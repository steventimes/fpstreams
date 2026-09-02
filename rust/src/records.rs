//! Exact column batches assembled directly into Python record dictionaries.

use pyo3::prelude::*;

#[cfg(not(Py_GIL_DISABLED))]
use pyo3::{ffi, types::PyTuple};

/// Assemble private, equal-length exact-list columns into exact dictionaries.
///
/// ``None`` is a side-effect-free compatibility decline.  The endpoint deliberately accepts
/// only exact tuples, exact string names, and exact list columns so callers can retain their
/// ordinary Python materializer for every externally observable container protocol.
#[pyfunction]
pub(crate) fn records_from_exact_columns_v1(
    names: &Bound<'_, PyAny>,
    columns: &Bound<'_, PyAny>,
) -> PyResult<Option<Py<PyAny>>> {
    #[cfg(Py_GIL_DISABLED)]
    {
        let _ = (names, columns);
        Ok(None)
    }

    #[cfg(not(Py_GIL_DISABLED))]
    {
        let (Ok(names), Ok(columns)) = (
            names.cast_exact::<PyTuple>(),
            columns.cast_exact::<PyTuple>(),
        ) else {
            return Ok(None);
        };
        let field_count = names.len();
        if field_count == 0 || columns.len() != field_count {
            return Ok(None);
        }

        let mut fields = Vec::new();
        fields.try_reserve_exact(field_count).map_err(|_| {
            pyo3::exceptions::PyMemoryError::new_err("native record assembly allocation failed")
        })?;
        let mut row_count: Option<ffi::Py_ssize_t> = None;
        for index in 0..field_count {
            let index = index as ffi::Py_ssize_t;
            // SAFETY: both inputs are exact tuples and index is within their equal fixed length.
            let name = unsafe { ffi::PyTuple_GetItem(names.as_ptr(), index) };
            let column = unsafe { ffi::PyTuple_GetItem(columns.as_ptr(), index) };
            if name.is_null() || column.is_null() {
                return Err(PyErr::fetch(names.py()));
            }
            if unsafe { ffi::PyUnicode_CheckExact(name) } == 0
                || unsafe { ffi::PyList_CheckExact(column) } == 0
            {
                return Ok(None);
            }
            // SAFETY: column is an exact list and PyList_Size cannot invoke Python code.
            let length = unsafe { ffi::PyList_Size(column) };
            if length < 0 {
                return Err(PyErr::fetch(names.py()));
            }
            if row_count.is_some_and(|expected| expected != length) {
                return Ok(None);
            }
            row_count = Some(length);
            fields.push((name, column));
        }

        let py = names.py();
        let row_count = row_count.expect("a non-empty field tuple has one column length");
        py.check_signals()?;
        // SAFETY: PyList_New returns one owned exact list or sets a Python exception. Bound safely
        // releases both initialized entries and remaining null slots if a later allocation fails.
        let output = unsafe { Bound::from_owned_ptr_or_err(py, ffi::PyList_New(row_count))? };
        for row_index in 0..row_count {
            if row_index != 0 && row_index & 4095 == 0 {
                py.check_signals()?;
            }
            // SAFETY: PyDict_New returns one owned exact dictionary or sets an exception.
            let record = unsafe { Bound::from_owned_ptr_or_err(py, ffi::PyDict_New())? };
            for &(name, column) in &fields {
                // SAFETY: every validated exact list has row_count entries and stays live through
                // the columns tuple. The borrowed value remains live while PyDict_SetItem retains
                // its own reference.
                let value = unsafe { ffi::PyList_GetItem(column, row_index) };
                if value.is_null() {
                    return Err(PyErr::fetch(py));
                }
                if unsafe { ffi::PyDict_SetItem(record.as_ptr(), name, value) } != 0 {
                    return Err(PyErr::fetch(py));
                }
            }
            // SAFETY: row_index names a null slot in this new list. Stable-ABI PyList_SetItem
            // steals record on both success and failure.
            if unsafe { ffi::PyList_SetItem(output.as_ptr(), row_index, record.into_ptr()) } != 0 {
                return Err(PyErr::fetch(py));
            }
        }
        py.check_signals()?;
        Ok(Some(output.unbind()))
    }
}

#[cfg(all(test, not(Py_GIL_DISABLED)))]
mod tests {
    use super::*;
    use pyo3::types::{PyDict, PyDictMethods, PyList, PyListMethods, PyTuple};

    #[test]
    fn exact_columns_build_fresh_ordered_records() {
        Python::initialize();
        Python::attach(|py| {
            let names = PyTuple::new(py, ["key", "value"]).unwrap();
            let keys = PyList::new(py, [1, 2]).unwrap();
            let values = PyList::new(py, ["a", "b"]).unwrap();
            let columns = PyTuple::new(py, [keys.as_any(), values.as_any()]).unwrap();

            let output = records_from_exact_columns_v1(names.as_any(), columns.as_any())
                .unwrap()
                .unwrap();
            let output = output.bind(py).cast_exact::<PyList>().unwrap();
            assert_eq!(output.len(), 2);
            let first = output.get_item(0).unwrap().cast_into::<PyDict>().unwrap();
            let second = output.get_item(1).unwrap().cast_into::<PyDict>().unwrap();
            assert_eq!(
                first
                    .get_item("key")
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                1
            );
            assert_eq!(
                first
                    .get_item("value")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "a"
            );
            assert_eq!(
                second
                    .get_item("key")
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                2
            );
            assert_eq!(
                second
                    .get_item("value")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "b"
            );
            assert!(!first.is(&second));
        });
    }

    #[test]
    fn malformed_column_envelopes_decline_without_protocol_calls() {
        Python::initialize();
        Python::attach(|py| {
            let names = PyTuple::new(py, ["left", "right"]).unwrap();
            let short = PyList::new(py, [1]).unwrap();
            let long = PyList::new(py, [2, 3]).unwrap();
            let mismatched = PyTuple::new(py, [short.as_any(), long.as_any()]).unwrap();
            assert!(
                records_from_exact_columns_v1(names.as_any(), mismatched.as_any())
                    .unwrap()
                    .is_none()
            );

            let non_string_names = PyTuple::new(py, [1]).unwrap();
            let one_column = PyTuple::new(py, [short.as_any()]).unwrap();
            assert!(
                records_from_exact_columns_v1(non_string_names.as_any(), one_column.as_any(),)
                    .unwrap()
                    .is_none()
            );
            assert!(
                records_from_exact_columns_v1(names.as_any(), PyList::empty(py).as_any())
                    .unwrap()
                    .is_none()
            );
        });
    }
}
