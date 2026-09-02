//! Exact integer reductions over retained dictionary rows.

#[cfg(Py_GIL_DISABLED)]
use super::group_numeric::snapshot_exact_list_rows;
use super::group_numeric::{dict_item, exact_i64};
use super::*;

/// Validate and add one borrowed exact dictionary without observable speculative callbacks.
#[inline]
fn add_exact_dict_value(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    value_field: *mut ffi::PyObject,
    total: &mut i128,
) -> PyResult<Option<()>> {
    // SAFETY: the caller keeps row live through the GIL, an owned list snapshot, or the
    // immutable source tuple for this call. The exact check cannot dispatch Python code.
    if unsafe { ffi::PyDict_CheckExact(row) } == 0 {
        return Ok(None);
    }
    // SAFETY: row is a live exact dictionary for the duration of this critical section.
    let row_bound = unsafe { Borrowed::from_ptr(py, row) };
    with_critical_section(row_bound.as_any(), || {
        let mut position = 0;
        let mut field = core::ptr::null_mut();
        let mut field_value = core::ptr::null_mut();
        let mut selected = core::ptr::null_mut();
        // SAFETY: row is an exact dictionary protected by its critical section.
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
            // SAFETY: the exact dictionary is locked and its size fixes the number of items.
            if unsafe { ffi::PyDict_Next(row, &mut position, &mut field, &mut field_value) } == 0 {
                return Ok(None);
            }
            // Reject custom keys before any fallback lookup could invoke equality hooks.
            if unsafe { ffi::PyUnicode_CheckExact(field) } == 0 {
                return Ok(None);
            }
            if field == value_field {
                selected = field_value;
            }
        }
        if selected.is_null() {
            let Some(found) = dict_item(py, row, value_field)? else {
                return Ok(None);
            };
            selected = found;
        }
        let Some(value) = exact_i64(py, selected)? else {
            return Ok(None);
        };
        let Some(next) = total.checked_add(i128::from(value)) else {
            return Ok(None);
        };
        *total = next;
        Ok(Some(()))
    })
}

/// Reduce borrowed rows from a GIL-protected list, locked snapshot, or immutable tuple.
fn sum_exact_dict_sequence(
    py: Python<'_>,
    row_count: usize,
    mut get_row: impl FnMut(usize) -> *mut ffi::PyObject,
    value_field: *mut ffi::PyObject,
) -> PyResult<Option<i128>> {
    let mut total = 0_i128;
    for index in 0..row_count {
        let row = get_row(index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
        if add_exact_dict_value(py, row, value_field, &mut total)?.is_none() {
            return Ok(None);
        }
    }
    Ok(Some(total))
}

#[pyfunction]
/// Sum one exact i64 field, returning None when canonical Python fallback is required.
pub(crate) fn global_sum_i64_dict_rows_v1<'py>(
    source: &Bound<'py, PyAny>,
    value_field: &Bound<'py, PyAny>,
) -> PyResult<Option<i128>> {
    let value_field = match value_field.cast_exact::<PyString>() {
        Ok(field) => field,
        Err(_) => return Ok(None),
    };

    if let Ok(rows) = source.cast_exact::<PyList>() {
        #[cfg(not(Py_GIL_DISABLED))]
        return sum_exact_dict_sequence(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: the GIL prevents exact-list mutation and index is below its length.
                unsafe { ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            value_field.as_ptr(),
        );
        #[cfg(Py_GIL_DISABLED)]
        {
            let snapshot = snapshot_exact_list_rows(source.py(), source, rows)?;
            return sum_exact_dict_sequence(
                source.py(),
                snapshot.len(),
                |index| snapshot[index].bind(source.py()).as_ptr(),
                value_field.as_ptr(),
            );
        }
    }
    if let Ok(rows) = source.cast_exact::<PyTuple>() {
        return sum_exact_dict_sequence(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: exact tuples are immutable and index is below their fixed length.
                unsafe { ffi::PyTuple_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            value_field.as_ptr(),
        );
    }
    Ok(None)
}
