//! Unique-right callable join execution and ABI entry points.

use super::*;

#[allow(clippy::too_many_arguments)]
/// Shared callable-key unique-right join after its versioned type capabilities are parsed.
fn join_hashable_unique_records<const DIRECT_FIELDS: bool>(
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    left_key: &Bound<'_, PyAny>,
    right_key: &Bound<'_, PyAny>,
    record_adapter: &Bound<'_, PyAny>,
    left_join: bool,
    suffix: &Bound<'_, PyAny>,
    shared_names: &Bound<'_, PyAny>,
    fallback_left_key: Option<&Bound<'_, PyAny>>,
    fallback_right_key: Option<&Bound<'_, PyAny>>,
    record_capabilities: &CallableJoinRecordCapabilities,
) -> PyResult<Option<Vec<Py<PyDict>>>> {
    let Some(record_adapters) = CallableJoinRecordAdapters::parse(record_adapter)? else {
        return Ok(None);
    };
    if DIRECT_FIELDS {
        let left_field = match left_key.cast_exact::<PyString>() {
            Ok(field) => field,
            Err(_) => return Ok(None),
        };
        let right_field = match right_key.cast_exact::<PyString>() {
            Ok(field) => field,
            Err(_) => return Ok(None),
        };
        if exact_string_contains_dot(left.py(), left_field.as_ptr())?
            || exact_string_contains_dot(left.py(), right_field.as_ptr())?
        {
            return Ok(None);
        }
    } else if !left_key.is_callable() || !right_key.is_callable() {
        return Ok(None);
    }
    let suffix = match suffix.cast_exact::<PyString>() {
        Ok(suffix) => suffix,
        Err(_) => return Ok(None),
    };
    let shared_names = match shared_names.cast_exact::<PyFrozenSet>() {
        Ok(names) => names,
        Err(_) => return Ok(None),
    };
    for name in shared_names.iter() {
        if name.cast_exact::<PyString>().is_err() {
            return Ok(None);
        }
    }
    let right_count = if !DIRECT_FIELDS && record_adapters.fallback.is_some() {
        exact_callable_join_source_len(right)
    } else {
        preflight_callable_join_source::<true>(right, record_capabilities)?
    };
    let Some(right_count) = right_count else {
        return Ok(None);
    };
    let left_count = if !DIRECT_FIELDS && record_adapters.fallback.is_some() {
        exact_callable_join_source_len(left)
    } else {
        preflight_callable_join_source::<true>(left, record_capabilities)?
    };
    let Some(left_count) = left_count else {
        return Ok(None);
    };

    // From this point forward selectors may have observable effects. Every later shape uses
    // either the exact-dict branch or record_adapter; no path may return None and replay them.
    let py = left.py();
    let index = PyDict::new(py);
    let seen_columns = PySet::empty(py)?;
    // A preflighted live list may replace later rows from an earlier selector callback. Check each
    // actual row below: exact dictionaries are copied, while only the built-in `dict` adapter is
    // trusted to return a fresh snapshot for any replacement record type.
    let mapping_proxy = mapping_proxy_snapshot_capability(
        record_adapters.preflighted.bind(py),
        &record_capabilities.record_types,
    )?;
    let namedtuple = standard_namedtuple_snapshot_capability(record_adapters.preflighted.bind(py))?;
    let mut can_collapse_right_lookup = true;
    let mut right_columns = Vec::new();
    right_columns
        .try_reserve(RECORD_JOIN_V1_MAX_FIELDS.min(right_count))
        .map_err(join_allocation_error)?;
    let mut right_schema_cache =
        CallableRightSchemaCache::new(right_count, CallableRightSchemaMode::Unique);
    let mut right_iterator = right.try_iter()?;
    for row in &mut right_iterator {
        let row = row?;
        let snapshot_fallback =
            callable_join_uses_fallback(&row, &record_adapters, record_capabilities);
        let snapshot = snapshot_callable_join_record(
            &row,
            &record_adapters,
            snapshot_fallback,
            mapping_proxy.as_ref(),
            namedtuple.as_ref(),
        )?;
        if !snapshot.trusted {
            can_collapse_right_lookup = false;
        }
        let record = snapshot.record;
        // Callable selectors always receive the live row directly, so their fallback flag is
        // intentionally unused. Only direct-field joins must revalidate after the snapshot
        // callback, which may have changed the row's type or MRO.
        let selector_fallback = DIRECT_FIELDS
            && callable_join_uses_fallback(&row, &record_adapters, record_capabilities);
        let key = select_live_hashable_join_key::<DIRECT_FIELDS>(
            &row,
            right_key,
            fallback_right_key,
            selector_fallback,
        )?;
        remember_callable_join_columns_with_cache(
            &record,
            &mut right_columns,
            &seen_columns,
            &mut can_collapse_right_lookup,
            &mut right_schema_cache,
            py,
        )?;
        let existing = index
            .get_item(&key)
            .map_err(|error| callable_join_key_error(py, error))?;
        if existing.is_some() {
            return Err(callable_join_duplicate_error(&key)?);
        }
        index
            .set_item(&key, record.bind(py))
            .map_err(|error| callable_join_key_error(py, error))?;
    }

    let mut joined = Vec::new();
    joined
        .try_reserve(left_count)
        .map_err(join_allocation_error)?;
    let mut target_cache: Option<CallableJoinTargetCache> = None;
    let mut left_iterator = left.try_iter()?;
    for row in &mut left_iterator {
        let row = row?;
        let snapshot_fallback =
            callable_join_uses_fallback(&row, &record_adapters, record_capabilities);
        let output = snapshot_callable_join_record(
            &row,
            &record_adapters,
            snapshot_fallback,
            mapping_proxy.as_ref(),
            namedtuple.as_ref(),
        )?
        .record;
        let selector_fallback = DIRECT_FIELDS
            && callable_join_uses_fallback(&row, &record_adapters, record_capabilities);
        let key = select_live_hashable_join_key::<DIRECT_FIELDS>(
            &row,
            left_key,
            fallback_left_key,
            selector_fallback,
        )?;
        let matched = index
            .get_item(&key)
            .map_err(|error| callable_join_key_error(py, error))?;
        let Some(right_record) = matched else {
            if left_join {
                let cache_hit = target_cache
                    .as_ref()
                    .map(|cached| callable_join_same_left_shape(&output, cached, py))
                    .transpose()?
                    .unwrap_or(false);
                if cache_hit {
                    merge_callable_join_plan_unmatched(
                        &output,
                        target_cache
                            .as_ref()
                            .expect("a cache hit requires a retained target plan"),
                        suffix,
                        py,
                    )?;
                } else {
                    let (targets, new_cache) =
                        callable_join_targets(&output, &right_columns, suffix, shared_names, py)?;
                    merge_callable_join_unmatched(&output, &targets, shared_names, py)?;
                    target_cache = new_cache;
                }
                if joined.len() == joined.capacity() {
                    joined.try_reserve(1).map_err(join_allocation_error)?;
                }
                joined.push(output);
            }
            continue;
        };
        let right_record = right_record
            .cast_into_exact::<PyDict>()
            .expect("the private join index stores only exact dictionary snapshots");
        let cache_hit = target_cache
            .as_ref()
            .map(|cached| callable_join_same_left_shape(&output, cached, py))
            .transpose()?
            .unwrap_or(false);
        if cache_hit {
            merge_callable_join_plan_match(
                &output,
                &right_record,
                target_cache
                    .as_ref()
                    .expect("a cache hit requires a retained target plan"),
                suffix,
                can_collapse_right_lookup,
                py,
            )?;
        } else {
            let (targets, new_cache) =
                callable_join_targets(&output, &right_columns, suffix, shared_names, py)?;
            merge_callable_join_match(
                &output,
                &right_record,
                &targets,
                shared_names,
                can_collapse_right_lookup,
                py,
            )?;
            target_cache = new_cache;
        }
        if joined.len() == joined.capacity() {
            joined.try_reserve(1).map_err(join_allocation_error)?;
        }
        joined.push(output);
    }
    Ok(Some(joined))
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
/// Materialize an exact-dict callable-key join without replaying an invoked callback.
pub(crate) fn join_hashable_unique_records_v1(
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    left_key: &Bound<'_, PyAny>,
    right_key: &Bound<'_, PyAny>,
    record_adapter: &Bound<'_, PyAny>,
    left_join: bool,
    suffix: &Bound<'_, PyAny>,
    shared_names: &Bound<'_, PyAny>,
) -> PyResult<Option<Vec<Py<PyDict>>>> {
    join_hashable_unique_records::<false>(
        left,
        right,
        left_key,
        right_key,
        record_adapter,
        left_join,
        suffix,
        shared_names,
        None,
        None,
        &CallableJoinRecordCapabilities::default(),
    )
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
/// Extend v1 with exact row-type capabilities validated before callback ownership transfers.
pub(crate) fn join_hashable_unique_records_v2(
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    left_key: &Bound<'_, PyAny>,
    right_key: &Bound<'_, PyAny>,
    record_adapter: &Bound<'_, PyAny>,
    left_join: bool,
    suffix: &Bound<'_, PyAny>,
    shared_names: &Bound<'_, PyAny>,
    allowed_record_types: &Bound<'_, PyAny>,
) -> PyResult<Option<Vec<Py<PyDict>>>> {
    let Some(record_capabilities) = callable_join_record_type_tokens(allowed_record_types)? else {
        return Ok(None);
    };
    join_hashable_unique_records::<false>(
        left,
        right,
        left_key,
        right_key,
        record_adapter,
        left_join,
        suffix,
        shared_names,
        None,
        None,
        &record_capabilities,
    )
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
/// Select exact-string fields after an exact-dict-or-declared-record preflight, without callbacks.
pub(crate) fn join_hashable_unique_direct_records_v1(
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    left_field: &Bound<'_, PyAny>,
    right_field: &Bound<'_, PyAny>,
    left_join: bool,
    suffix: &Bound<'_, PyAny>,
    shared_names: &Bound<'_, PyAny>,
    allowed_record_types: &Bound<'_, PyAny>,
) -> PyResult<Option<Vec<Py<PyDict>>>> {
    let Some(capabilities) = callable_join_direct_capabilities(allowed_record_types)? else {
        return Ok(None);
    };
    let CallableJoinDirectCapabilities {
        record_capabilities,
        fallback,
    } = capabilities;
    let py = left.py();
    let record_adapter = py.get_type::<PyDict>();
    if record_capabilities
        .record_types
        .iter()
        .any(|row_type| row_type.bind(py).is(&record_adapter))
    {
        return Ok(None);
    }
    let adapter_pair = fallback
        .as_ref()
        .map(|fallback| {
            PyTuple::new(
                py,
                [record_adapter.as_any(), fallback.record_adapter.bind(py)],
            )
        })
        .transpose()?;
    let adapter = adapter_pair
        .as_ref()
        .map_or(record_adapter.as_any(), Bound::as_any);
    join_hashable_unique_records::<true>(
        left,
        right,
        left_field,
        right_field,
        adapter,
        left_join,
        suffix,
        shared_names,
        fallback
            .as_ref()
            .map(|fallback| fallback.left_selector.bind(py)),
        fallback
            .as_ref()
            .map(|fallback| fallback.right_selector.bind(py)),
        &record_capabilities,
    )
}
