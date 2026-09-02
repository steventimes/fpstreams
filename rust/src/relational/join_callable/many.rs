//! Many-right callable join execution and ABI entry points.

use super::*;

#[allow(clippy::too_many_arguments)]
/// Shared callable-key many-right join after its versioned type capabilities are parsed.
fn join_hashable_many_records<const DIRECT_FIELDS: bool>(
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

    // All fallbacks end here. From the first snapshot onward, adapters and selectors own their
    // observable effects and every error must propagate without replaying either source.
    let py = left.py();
    let index = PyDict::new(py);
    let seen_columns = PySet::empty(py)?;
    // See the unique-right path: validate ownership against each post-callback live row rather
    // than relying on the earlier source preflight.
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
        CallableRightSchemaCache::new(right_count, CallableRightSchemaMode::Many);
    let mut right_records = Vec::new();
    right_records
        .try_reserve_exact(right_count)
        .map_err(join_allocation_error)?;
    let mut right_codes = Vec::new();
    right_codes
        .try_reserve_exact(right_count)
        .map_err(join_allocation_error)?;
    let mut group_counts = Vec::new();
    group_counts
        .try_reserve_exact(right_count)
        .map_err(join_allocation_error)?;

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
        // Callable selectors do not consume the direct-field fallback flag. Preserve the second
        // live capability check only for direct-field joins, where snapshot callbacks may have
        // changed the row's type or MRO before selection.
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
        let code = match index.get_item(&key)? {
            Some(code) => code
                .extract::<usize>()
                .expect("the private many-right index stores only usize codes"),
            None => {
                let code = group_counts.len();
                index.set_item(&key, code)?;
                if group_counts.len() == group_counts.capacity() {
                    group_counts.try_reserve(1).map_err(join_allocation_error)?;
                }
                group_counts.push(0);
                code
            }
        };
        group_counts[code] = checked_join_output_size(group_counts[code], 1)?;
        if right_codes.len() == right_codes.capacity() {
            right_codes.try_reserve(1).map_err(join_allocation_error)?;
        }
        if right_records.len() == right_records.capacity() {
            right_records
                .try_reserve(1)
                .map_err(join_allocation_error)?;
        }
        right_codes.push(code);
        right_records.push(record);
    }
    let bulk_merge_right_field_count = right_schema_cache.identity_homogeneous_field_count();
    let (group_offsets, right_positions) = factorized_right_positions(&right_codes, &group_counts)?;

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
        let snapshot = snapshot_callable_join_record(
            &row,
            &record_adapters,
            snapshot_fallback,
            mapping_proxy.as_ref(),
            namedtuple.as_ref(),
        )?;
        let left_snapshot_trusted = snapshot.trusted;
        let left_record = snapshot.record;
        let selector_fallback = DIRECT_FIELDS
            && callable_join_uses_fallback(&row, &record_adapters, record_capabilities);
        let key = select_live_hashable_join_key::<DIRECT_FIELDS>(
            &row,
            left_key,
            fallback_left_key,
            selector_fallback,
        )?;
        let matched = index.get_item(&key)?;
        let Some(code) = matched else {
            if left_join {
                let output_count = checked_join_output_size(joined.len(), 1)?;
                joined.try_reserve(1).map_err(join_allocation_error)?;
                let cache_hit = target_cache
                    .as_ref()
                    .map(|cached| callable_join_same_left_shape(&left_record, cached, py))
                    .transpose()?
                    .unwrap_or(false);
                if cache_hit {
                    merge_callable_join_plan_unmatched(
                        &left_record,
                        target_cache
                            .as_ref()
                            .expect("a cache hit requires a retained target plan"),
                        suffix,
                        py,
                    )?;
                } else {
                    let (targets, new_cache) = callable_join_targets(
                        &left_record,
                        &right_columns,
                        suffix,
                        shared_names,
                        py,
                    )?;
                    merge_callable_join_unmatched(&left_record, &targets, shared_names, py)?;
                    target_cache = new_cache;
                }
                joined.push(left_record);
                debug_assert_eq!(joined.len(), output_count);
            }
            continue;
        };
        let code = code
            .extract::<usize>()
            .expect("the private many-right index stores only usize codes");
        let start = group_offsets[code];
        let end = group_offsets[code + 1];
        let matches = &right_positions[start..end];
        let output_count = checked_join_output_size(joined.len(), matches.len())?;
        joined
            .try_reserve(matches.len())
            .map_err(join_allocation_error)?;
        let cache_hit = target_cache
            .as_ref()
            .map(|cached| callable_join_same_left_shape(&left_record, cached, py))
            .transpose()?
            .unwrap_or(false);
        let (targets, new_cache) = if cache_hit {
            (
                callable_join_targets_from_plan(
                    target_cache
                        .as_ref()
                        .expect("a cache hit requires a retained target plan"),
                    suffix,
                    py,
                )?,
                None,
            )
        } else {
            callable_join_targets(&left_record, &right_columns, suffix, shared_names, py)?
        };
        let bulk_suffix_prefix = if cache_hit {
            let cached = target_cache
                .as_ref()
                .expect("a cache hit requires a retained target plan");
            bulk_merge_right_field_count
                .filter(|field_count| *field_count == cached.plan.len())
                .and(cached.bulk_merge_suffix_prefix)
                .filter(|_| left_snapshot_trusted)
        } else {
            None
        };
        let mut original = Some(left_record);
        for (match_index, &right_position) in matches.iter().enumerate() {
            let output = if match_index + 1 == matches.len() {
                original
                    .take()
                    .expect("the final match owns the original left snapshot")
            } else {
                copy_join_record(
                    py,
                    original
                        .as_ref()
                        .expect("earlier matches retain the original left snapshot"),
                )?
            };
            if let Some(suffix_prefix) = bulk_suffix_prefix {
                merge_callable_join_targets_bulk_match(
                    &output,
                    right_records[right_position].bind(py),
                    &targets,
                    suffix_prefix,
                    py,
                )?;
            } else {
                merge_callable_join_match(
                    &output,
                    right_records[right_position].bind(py),
                    &targets,
                    shared_names,
                    can_collapse_right_lookup,
                    py,
                )?;
            }
            joined.push(output);
        }
        if !cache_hit {
            target_cache = new_cache;
        }
        debug_assert_eq!(joined.len(), output_count);
    }
    Ok(Some(joined))
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
/// Materialize an exact-dict callable-key many-right join without replaying callbacks.
pub(crate) fn join_hashable_many_records_v1(
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    left_key: &Bound<'_, PyAny>,
    right_key: &Bound<'_, PyAny>,
    record_adapter: &Bound<'_, PyAny>,
    left_join: bool,
    suffix: &Bound<'_, PyAny>,
    shared_names: &Bound<'_, PyAny>,
) -> PyResult<Option<Vec<Py<PyDict>>>> {
    join_hashable_many_records::<false>(
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
/// Extend callable many v1 with exact row-type capabilities checked before callbacks.
pub(crate) fn join_hashable_many_records_v2(
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
    join_hashable_many_records::<false>(
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
/// Select exact-string fields for a many join after an exact-dict-or-declared-record preflight.
pub(crate) fn join_hashable_many_direct_records_v1(
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
    join_hashable_many_records::<true>(
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
