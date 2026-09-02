//! Single-lane fixed-schema count and sum kernels.

use super::*;

/// Count one exact-i64 key, reusing a retained first-key identity when possible.
#[inline(always)]
fn add_fixed_exact_i64_key<const USE_OBJECT_CACHE: bool>(
    py: Python<'_>,
    key_object: *mut ffi::PyObject,
    state: &mut ObjectKeyGroupState,
    counts: &mut Vec<usize>,
) -> PyResult<Option<()>> {
    if USE_OBJECT_CACHE && let Some(position) = state.cached_position(key_object) {
        return Ok(ObjectKeyGroupState::add_fixed_count_at_position(
            counts, position,
        ));
    }
    let Some(key) = exact_i64(py, key_object)? else {
        return Ok(None);
    };
    state.add_fixed_count::<USE_OBJECT_CACHE>(counts, py, key_object, key)
}

/// Count and sum one exact-i64 row, validating the value even on an identity-cache hit.
#[inline(always)]
fn add_fixed_exact_i64_key_value<const USE_OBJECT_CACHE: bool>(
    py: Python<'_>,
    key_object: *mut ffi::PyObject,
    value_object: *mut ffi::PyObject,
    state: &mut ObjectKeyGroupState,
    counts: &mut Vec<usize>,
) -> PyResult<Option<()>> {
    if USE_OBJECT_CACHE && let Some(position) = state.cached_position(key_object) {
        let Some(value) = exact_i64(py, value_object)? else {
            return Ok(None);
        };
        return Ok(state.add_fixed_count_sum_at_position(counts, position, value));
    }
    let Some(key) = exact_i64(py, key_object)? else {
        return Ok(None);
    };
    let Some(value) = exact_i64(py, value_object)? else {
        return Ok(None);
    };
    state.add_fixed_count_sum::<USE_OBJECT_CACHE>(counts, py, key_object, key, value)
}

/// Validate and consume one exact tuple row in a compile-time-selected fixed aggregation mode.
#[inline(always)]
fn group_fixed_exact_tuple_row<const WITH_SUM: bool, const USE_OBJECT_CACHE: bool>(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    key_index: isize,
    value_index: isize,
    cached_layout: &mut Option<(usize, usize, usize)>,
    state: &mut ObjectKeyGroupState,
    counts: &mut Vec<usize>,
) -> PyResult<Option<()>> {
    // SAFETY: row is live through the GIL, the immutable outer tuple, or the owned exact-list
    // snapshot retained by the caller.
    if unsafe { ffi::PyTuple_CheckExact(row) } == 0 {
        return Ok(None);
    }
    // SAFETY: row was proven to be an exact tuple.
    let width = unsafe { ffi::PyTuple_Size(row) };
    if width < 0 {
        return Err(PyErr::fetch(py));
    }
    let width = width as usize;
    let (key_position, value_position) = match *cached_layout {
        Some((cached_width, key_position, value_position)) if cached_width == width => {
            (key_position, value_position)
        }
        _ => {
            let Some(key_position) = normalize_index(key_index, width) else {
                return Ok(None);
            };
            let value_position = if WITH_SUM {
                let Some(value_position) = normalize_index(value_index, width) else {
                    return Ok(None);
                };
                value_position
            } else {
                0
            };
            *cached_layout = Some((width, key_position, value_position));
            (key_position, value_position)
        }
    };
    // SAFETY: key_position was normalized against this exact tuple's fixed width.
    let key_object = unsafe { ffi::PyTuple_GetItem(row, key_position as ffi::Py_ssize_t) };
    if key_object.is_null() {
        return Err(PyErr::fetch(py));
    }
    if WITH_SUM {
        // SAFETY: the compile-time sum branch proves value_position was normalized above.
        let value_object = unsafe { ffi::PyTuple_GetItem(row, value_position as ffi::Py_ssize_t) };
        if value_object.is_null() {
            return Err(PyErr::fetch(py));
        }
        add_fixed_exact_i64_key_value::<USE_OBJECT_CACHE>(
            py,
            key_object,
            value_object,
            state,
            counts,
        )
    } else {
        add_fixed_exact_i64_key::<USE_OBJECT_CACHE>(py, key_object, state, counts)
    }
}

/// Scan exact tuple rows through a count-only or count+sum monomorphized hot loop.
fn group_fixed_exact_tuple_sequence<const WITH_SUM: bool>(
    py: Python<'_>,
    row_count: usize,
    mut get_row: impl FnMut(usize) -> *mut ffi::PyObject,
    key_index: isize,
    value_index: isize,
) -> PyResult<Option<(ObjectKeyGroups, Vec<usize>)>> {
    let mut state = ObjectKeyGroupState::new(row_count);
    let mut counts = Vec::new();
    let mut cached_layout = None;
    let mut row_index = 0;
    while row_index < row_count && state.groups.len() <= OBJECT_KEY_CACHE_SLOTS {
        let row = get_row(row_index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
        if group_fixed_exact_tuple_row::<WITH_SUM, true>(
            py,
            row,
            key_index,
            value_index,
            &mut cached_layout,
            &mut state,
            &mut counts,
        )?
        .is_none()
        {
            return Ok(None);
        }
        row_index += 1;
    }
    while row_index < row_count {
        let row = get_row(row_index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
        if group_fixed_exact_tuple_row::<WITH_SUM, false>(
            py,
            row,
            key_index,
            value_index,
            &mut cached_layout,
            &mut state,
            &mut counts,
        )?
        .is_none()
        {
            return Ok(None);
        }
        row_index += 1;
    }
    debug_assert_eq!(state.groups.len(), counts.len());
    Ok(Some((state.groups, counts)))
}

/// Select the free-thread-safe outer-container path for fixed exact tuple rows.
fn group_fixed_i64_tuple_source<const WITH_SUM: bool>(
    source: &Bound<'_, PyAny>,
    key_index: isize,
    value_index: isize,
) -> PyResult<Option<(ObjectKeyGroups, Vec<usize>)>> {
    if let Ok(rows) = source.cast_exact::<PyList>() {
        #[cfg(not(Py_GIL_DISABLED))]
        return group_fixed_exact_tuple_sequence::<WITH_SUM>(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: a GIL build prevents exact-list mutation during this attached call.
                unsafe { ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            key_index,
            value_index,
        );
        #[cfg(Py_GIL_DISABLED)]
        {
            let snapshot = snapshot_exact_list_rows(source.py(), source, rows)?;
            return group_fixed_exact_tuple_sequence::<WITH_SUM>(
                source.py(),
                snapshot.len(),
                |index| snapshot[index].bind(source.py()).as_ptr(),
                key_index,
                value_index,
            );
        }
    }
    if let Ok(rows) = source.cast_exact::<PyTuple>() {
        return group_fixed_exact_tuple_sequence::<WITH_SUM>(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: exact outer tuples are immutable and index is within fixed length.
                unsafe { ffi::PyTuple_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            key_index,
            value_index,
        );
    }
    Ok(None)
}

/// Validate and consume one exact dict row in a compile-time-selected fixed mode.
#[inline]
fn group_fixed_exact_dict_row<const WITH_SUM: bool, const USE_OBJECT_CACHE: bool>(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    key_field: *mut ffi::PyObject,
    value_field: *mut ffi::PyObject,
    state: &mut ObjectKeyGroupState,
    counts: &mut Vec<usize>,
) -> PyResult<Option<()>> {
    // SAFETY: row is live through the GIL, immutable outer tuple, or strong list snapshot.
    if unsafe { ffi::PyDict_CheckExact(row) } == 0 {
        return Ok(None);
    }
    // SAFETY: row remains live for this entire call.
    let row_bound = unsafe { Borrowed::from_ptr(py, row) };
    with_critical_section(row_bound.as_any(), || {
        let mut position = 0;
        let mut field = core::ptr::null_mut();
        let mut field_value = core::ptr::null_mut();
        let mut key_object = core::ptr::null_mut();
        let mut value_object = core::ptr::null_mut();
        // SAFETY: row is an exact dict protected by this critical section.
        let field_count = unsafe { ffi::PyDict_Size(row) };
        if field_count < 0 {
            return Err(PyErr::fetch(py));
        }
        let field_count = usize::try_from(field_count)
            .map_err(|_| PyMemoryError::new_err("native record field count is too large"))?;
        if field_count > RECORD_GROUP_SUM_MAX_FIELDS {
            return Ok(None);
        }
        for _ in 0..field_count {
            // SAFETY: the locked exact dict has the fixed size read above.
            if unsafe { ffi::PyDict_Next(row, &mut position, &mut field, &mut field_value) } == 0 {
                return Ok(None);
            }
            // Reject every dispatch-capable field before selector lookup.
            // SAFETY: PyDict_Next returned a non-null live key.
            if unsafe { ffi::PyUnicode_CheckExact(field) } == 0 {
                return Ok(None);
            }
            if field == key_field {
                key_object = field_value;
            }
            if WITH_SUM && field == value_field {
                value_object = field_value;
            }
        }
        if key_object.is_null() {
            let Some(found) = dict_item(py, row, key_field)? else {
                return Ok(None);
            };
            key_object = found;
        }
        if WITH_SUM {
            if value_object.is_null() {
                let Some(found) = dict_item(py, row, value_field)? else {
                    return Ok(None);
                };
                value_object = found;
            }
            add_fixed_exact_i64_key_value::<USE_OBJECT_CACHE>(
                py,
                key_object,
                value_object,
                state,
                counts,
            )
        } else {
            add_fixed_exact_i64_key::<USE_OBJECT_CACHE>(py, key_object, state, counts)
        }
    })
}

/// Scan exact dict rows through a count-only or count+sum monomorphized hot loop.
fn group_fixed_exact_dict_sequence<const WITH_SUM: bool>(
    py: Python<'_>,
    row_count: usize,
    mut get_row: impl FnMut(usize) -> *mut ffi::PyObject,
    key_field: *mut ffi::PyObject,
    value_field: *mut ffi::PyObject,
) -> PyResult<Option<(ObjectKeyGroups, Vec<usize>)>> {
    let mut state = ObjectKeyGroupState::new(row_count);
    let mut counts = Vec::new();
    let mut row_index = 0;
    while row_index < row_count && state.groups.len() <= OBJECT_KEY_CACHE_SLOTS {
        let row = get_row(row_index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
        if group_fixed_exact_dict_row::<WITH_SUM, true>(
            py,
            row,
            key_field,
            value_field,
            &mut state,
            &mut counts,
        )?
        .is_none()
        {
            return Ok(None);
        }
        row_index += 1;
    }
    while row_index < row_count {
        let row = get_row(row_index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
        if group_fixed_exact_dict_row::<WITH_SUM, false>(
            py,
            row,
            key_field,
            value_field,
            &mut state,
            &mut counts,
        )?
        .is_none()
        {
            return Ok(None);
        }
        row_index += 1;
    }
    debug_assert_eq!(state.groups.len(), counts.len());
    Ok(Some((state.groups, counts)))
}

/// Select the free-thread-safe outer-container path for fixed exact dict rows.
fn group_fixed_i64_dict_source<const WITH_SUM: bool>(
    source: &Bound<'_, PyAny>,
    key_field: *mut ffi::PyObject,
    value_field: *mut ffi::PyObject,
) -> PyResult<Option<(ObjectKeyGroups, Vec<usize>)>> {
    if let Ok(rows) = source.cast_exact::<PyList>() {
        #[cfg(not(Py_GIL_DISABLED))]
        return group_fixed_exact_dict_sequence::<WITH_SUM>(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: a GIL build prevents exact-list mutation during this attached call.
                unsafe { ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            key_field,
            value_field,
        );
        #[cfg(Py_GIL_DISABLED)]
        {
            let snapshot = snapshot_exact_list_rows(source.py(), source, rows)?;
            return group_fixed_exact_dict_sequence::<WITH_SUM>(
                source.py(),
                snapshot.len(),
                |index| snapshot[index].bind(source.py()).as_ptr(),
                key_field,
                value_field,
            );
        }
    }
    if let Ok(rows) = source.cast_exact::<PyTuple>() {
        return group_fixed_exact_dict_sequence::<WITH_SUM>(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: exact outer tuples are immutable and index is within fixed length.
                unsafe { ffi::PyTuple_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            key_field,
            value_field,
        );
    }
    Ok(None)
}

/// Build the count-only ABI payload while preserving first-key identity and encounter order.
fn materialize_fixed_count_payload(
    py: Python<'_>,
    groups: ObjectKeyGroups,
    counts: Vec<usize>,
    key_name: &Bound<'_, PyString>,
    count_name: &Bound<'_, PyString>,
) -> PyResult<(bool, Py<PyAny>)> {
    debug_assert_eq!(groups.len(), counts.len());
    if groups.len() < GROUP_SUM_FINAL_ROWS_THRESHOLD {
        let pairs = groups
            .into_iter()
            .zip(counts)
            .map(|((key, _unused_total), count)| (key, count));
        let pairs = PyList::new(py, pairs)?;
        return Ok((false, pairs.into_any().unbind()));
    }

    let mut rows = Vec::new();
    rows.try_reserve(groups.len())
        .map_err(group_allocation_error)?;
    for ((key, _unused_total), count) in groups.into_iter().zip(counts) {
        let row = new_dict_fallible(py)?;
        row.set_item(key_name, key)?;
        row.set_item(count_name, count)?;
        rows.push(row.unbind());
    }
    let rows = PyList::new(py, rows)?;
    Ok((true, rows.into_any().unbind()))
}

/// Build the count+sum ABI payload with a widened sum lane.
fn materialize_fixed_count_sum_payload(
    py: Python<'_>,
    groups: ObjectKeyGroups,
    counts: Vec<usize>,
    key_name: &Bound<'_, PyString>,
    count_name: &Bound<'_, PyString>,
    sum_name: &Bound<'_, PyString>,
) -> PyResult<(bool, Py<PyAny>)> {
    debug_assert_eq!(groups.len(), counts.len());
    if groups.len() < GROUP_SUM_FINAL_ROWS_THRESHOLD {
        let triples = groups
            .into_iter()
            .zip(counts)
            .map(|((key, total), count)| (key, count, total));
        let triples = PyList::new(py, triples)?;
        return Ok((false, triples.into_any().unbind()));
    }

    let mut rows = Vec::new();
    rows.try_reserve(groups.len())
        .map_err(group_allocation_error)?;
    for ((key, total), count) in groups.into_iter().zip(counts) {
        let row = new_dict_fallible(py)?;
        row.set_item(key_name, key)?;
        row.set_item(count_name, count)?;
        set_widened_i64_item(&row, sum_name, total)?;
        rows.push(row.unbind());
    }
    let rows = PyList::new(py, rows)?;
    Ok((true, rows.into_any().unbind()))
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
/// Group exact tuple rows into fixed count-only or count+sum lanes.
pub(crate) fn group_fixed_i64_rows_v1(
    source: &Bound<'_, PyAny>,
    key_index: isize,
    value_index_or_none: &Bound<'_, PyAny>,
    key_name: &Bound<'_, PyAny>,
    count_name: &Bound<'_, PyAny>,
    sum_name_or_none: &Bound<'_, PyAny>,
) -> PyResult<Option<(bool, Py<PyAny>)>> {
    let key_name = match key_name.cast_exact::<PyString>() {
        Ok(name) => name,
        Err(_) => return Ok(None),
    };
    let count_name = match count_name.cast_exact::<PyString>() {
        Ok(name) => name,
        Err(_) => return Ok(None),
    };

    if value_index_or_none.is_none() {
        if !sum_name_or_none.is_none() {
            return Ok(None);
        }
        let Some((groups, counts)) = group_fixed_i64_tuple_source::<false>(source, key_index, 0)?
        else {
            return Ok(None);
        };
        return materialize_fixed_count_payload(source.py(), groups, counts, key_name, count_name)
            .map(Some);
    }
    if sum_name_or_none.is_none() {
        return Ok(None);
    }
    let Some(value_index) = exact_i64(source.py(), value_index_or_none.as_ptr())? else {
        return Ok(None);
    };
    let Ok(value_index) = isize::try_from(value_index) else {
        return Ok(None);
    };
    let sum_name = match sum_name_or_none.cast_exact::<PyString>() {
        Ok(name) => name,
        Err(_) => return Ok(None),
    };
    let Some((groups, counts)) =
        group_fixed_i64_tuple_source::<true>(source, key_index, value_index)?
    else {
        return Ok(None);
    };
    materialize_fixed_count_sum_payload(source.py(), groups, counts, key_name, count_name, sum_name)
        .map(Some)
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
/// Group exact dict rows into fixed count-only or count+sum lanes.
pub(crate) fn group_fixed_i64_dict_rows_v1(
    source: &Bound<'_, PyAny>,
    key_field: &Bound<'_, PyAny>,
    value_field_or_none: &Bound<'_, PyAny>,
    key_name: &Bound<'_, PyAny>,
    count_name: &Bound<'_, PyAny>,
    sum_name_or_none: &Bound<'_, PyAny>,
) -> PyResult<Option<(bool, Py<PyAny>)>> {
    let key_field = match key_field.cast_exact::<PyString>() {
        Ok(field) => field,
        Err(_) => return Ok(None),
    };
    let key_name = match key_name.cast_exact::<PyString>() {
        Ok(name) => name,
        Err(_) => return Ok(None),
    };
    let count_name = match count_name.cast_exact::<PyString>() {
        Ok(name) => name,
        Err(_) => return Ok(None),
    };

    if value_field_or_none.is_none() {
        if !sum_name_or_none.is_none() {
            return Ok(None);
        }
        let Some((groups, counts)) = group_fixed_i64_dict_source::<false>(
            source,
            key_field.as_ptr(),
            core::ptr::null_mut(),
        )?
        else {
            return Ok(None);
        };
        return materialize_fixed_count_payload(source.py(), groups, counts, key_name, count_name)
            .map(Some);
    }
    if sum_name_or_none.is_none() {
        return Ok(None);
    }
    let value_field = match value_field_or_none.cast_exact::<PyString>() {
        Ok(field) => field,
        Err(_) => return Ok(None),
    };
    let sum_name = match sum_name_or_none.cast_exact::<PyString>() {
        Ok(name) => name,
        Err(_) => return Ok(None),
    };
    let Some((groups, counts)) =
        group_fixed_i64_dict_source::<true>(source, key_field.as_ptr(), value_field.as_ptr())?
    else {
        return Ok(None);
    };
    materialize_fixed_count_sum_payload(source.py(), groups, counts, key_name, count_name, sum_name)
        .map(Some)
}
