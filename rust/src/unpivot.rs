//! Streaming exact-dictionary unpivot with a lossless Python fallback boundary.

#[cfg(not(Py_GIL_DISABLED))]
use crate::common::{exact_dict_has_only_string_keys, is_exact_sequence_iterator};

use pyo3::prelude::*;
#[cfg(not(Py_GIL_DISABLED))]
use pyo3::{
    exceptions::PyMemoryError,
    ffi,
    types::{PyList, PyTuple},
};

type UnpivotPrefix = (Option<Py<PyAny>>, bool);

#[cfg(not(Py_GIL_DISABLED))]
struct ExactUnpivotPlan {
    output: *mut ffi::PyObject,
    columns: *mut ffi::PyObject,
    column_count: ffi::Py_ssize_t,
    names_to: *mut ffi::PyObject,
    values_to: *mut ffi::PyObject,
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn validate_unpivot_plan(
    output: &Bound<'_, PyAny>,
    source: &Bound<'_, PyAny>,
    columns: &Bound<'_, PyAny>,
    names_to: &Bound<'_, PyAny>,
    values_to: &Bound<'_, PyAny>,
) -> PyResult<Option<ffi::Py_ssize_t>> {
    if output.cast_exact::<PyList>().is_err()
        || unsafe { ffi::PyUnicode_CheckExact(names_to.as_ptr()) } == 0
        || unsafe { ffi::PyUnicode_CheckExact(values_to.as_ptr()) } == 0
    {
        return Ok(None);
    }
    let Ok(columns) = columns.cast_exact::<PyTuple>() else {
        return Ok(None);
    };
    // SAFETY: columns is a live exact tuple, so its length cannot change or dispatch Python.
    let column_count = unsafe { ffi::PyTuple_Size(columns.as_ptr()) };
    if column_count < 0 {
        return Err(PyErr::fetch(columns.py()));
    }
    if column_count == 0 {
        return Ok(None);
    }
    for index in 0..column_count {
        // SAFETY: index is within the fixed exact-tuple length and the returned field is a
        // borrowed reference retained by columns throughout this call.
        let field = unsafe { ffi::PyTuple_GetItem(columns.as_ptr(), index) };
        if field.is_null() {
            return Err(PyErr::fetch(columns.py()));
        }
        if unsafe { ffi::PyUnicode_CheckExact(field) } == 0 {
            return Ok(None);
        }
    }

    // Public Rows.unpivot rejects equal output names before execution. Keep the native ABI
    // independently safe when called directly, without consuming the source on an invalid plan.
    let equal =
        unsafe { ffi::PyObject_RichCompareBool(names_to.as_ptr(), values_to.as_ptr(), ffi::Py_EQ) };
    if equal < 0 {
        return Err(PyErr::fetch(output.py()));
    }
    if equal != 0 || !is_exact_sequence_iterator(output.py(), source)? {
        return Ok(None);
    }
    Ok(Some(column_count))
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn dict_contains_exact_string(
    py: Python<'_>,
    dictionary: *mut ffi::PyObject,
    field: *mut ffi::PyObject,
) -> PyResult<bool> {
    // SAFETY: dictionary is exact and all its keys and field are exact strings, so containment
    // cannot invoke user code.
    let contains = unsafe { ffi::PyDict_Contains(dictionary, field) };
    if contains < 0 {
        return Err(PyErr::fetch(py));
    }
    Ok(contains != 0)
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn set_exact_dict_item(
    py: Python<'_>,
    dictionary: *mut ffi::PyObject,
    field: *mut ffi::PyObject,
    value: *mut ffi::PyObject,
) -> PyResult<()> {
    // SAFETY: all pointers are live for this call. PyDict_SetItem retains its own references.
    if unsafe { ffi::PyDict_SetItem(dictionary, field, value) } != 0 {
        return Err(PyErr::fetch(py));
    }
    Ok(())
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn append_exact_unpivot_row(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    plan: &ExactUnpivotPlan,
    selected_values: &mut Vec<Py<PyAny>>,
) -> PyResult<bool> {
    // A noncanonical row must reach Python untouched: even a dictionary lookup could invoke a
    // custom colliding key's equality method before the compatibility path takes ownership.
    if unsafe { ffi::PyDict_CheckExact(row) } == 0 || !exact_dict_has_only_string_keys(py, row)? {
        return Ok(false);
    }

    // SAFETY: PyDict_Copy returns one owned exact dictionary or sets an exception.
    let base = unsafe { Bound::from_owned_ptr_or_err(py, ffi::PyDict_Copy(row))? };
    if selected_values.capacity() < plan.column_count as usize {
        selected_values
            .try_reserve_exact(plan.column_count as usize - selected_values.capacity())
            .map_err(|_| {
                PyMemoryError::new_err("could not allocate unpivot field scratch space")
            })?;
    }
    selected_values.clear();
    for index in 0..plan.column_count {
        if index != 0 && index & 4095 == 0 {
            // No tuple or dictionary borrow crosses this callback-capable checkpoint.
            py.check_signals()?;
        }
        // SAFETY: index is within the live exact tuple's fixed length.
        let field = unsafe { ffi::PyTuple_GetItem(plan.columns, index) };
        if field.is_null() {
            return Err(PyErr::fetch(py));
        }
        // SAFETY: base contains only exact string keys and field is an exact string. A null
        // result without an exception is a missing field and therefore a Python fallback row.
        let selected = unsafe { ffi::PyDict_GetItemWithError(base.as_ptr(), field) };
        if selected.is_null() {
            if unsafe { !ffi::PyErr_Occurred().is_null() } {
                return Err(PyErr::fetch(py));
            }
            return Ok(false);
        }
        selected_values.push(unsafe { Borrowed::from_ptr(py, selected).to_owned().unbind() });
        // SAFETY: the preceding lookup proved the exact key exists in this private copy.
        if unsafe { ffi::PyDict_DelItem(base.as_ptr(), field) } != 0 {
            return Err(PyErr::fetch(py));
        }
    }

    if dict_contains_exact_string(py, base.as_ptr(), plan.names_to)?
        || dict_contains_exact_string(py, base.as_ptr(), plan.values_to)?
    {
        return Ok(false);
    }

    // Reuse the private base for the first output, exactly matching the Python materializer.
    let first_column = unsafe { ffi::PyTuple_GetItem(plan.columns, 0) };
    if first_column.is_null() {
        return Err(PyErr::fetch(py));
    }
    set_exact_dict_item(py, base.as_ptr(), plan.names_to, first_column)?;
    set_exact_dict_item(
        py,
        base.as_ptr(),
        plan.values_to,
        selected_values[0].bind(py).as_ptr(),
    )?;
    // SAFETY: output is a live exact list and PyList_Append takes its own strong reference.
    if unsafe { ffi::PyList_Append(plan.output, base.as_ptr()) } != 0 {
        return Err(PyErr::fetch(py));
    }

    for index in 1..plan.column_count {
        if index & 4095 == 0 {
            // selected_values owns its elements; acquire the next tuple borrow only after the
            // signal handler has returned.
            py.check_signals()?;
        }
        // Copy the first completed output so output-field insertion order stays identical while
        // replacing only the two expansion values.
        let reshaped =
            unsafe { Bound::from_owned_ptr_or_err(py, ffi::PyDict_Copy(base.as_ptr()))? };
        let column = unsafe { ffi::PyTuple_GetItem(plan.columns, index) };
        if column.is_null() {
            return Err(PyErr::fetch(py));
        }
        set_exact_dict_item(py, reshaped.as_ptr(), plan.names_to, column)?;
        set_exact_dict_item(
            py,
            reshaped.as_ptr(),
            plan.values_to,
            selected_values[index as usize].bind(py).as_ptr(),
        )?;
        if unsafe { ffi::PyList_Append(plan.output, reshaped.as_ptr()) } != 0 {
            return Err(PyErr::fetch(py));
        }
    }
    Ok(true)
}

/// Append the compatible exact-dictionary prefix of one unpivot expansion.
///
/// The first incompatible, missing-field, or colliding-output row is returned unprocessed so
/// Python can resume with its canonical protocol and exception behavior. ``completed``
/// distinguishes iterator exhaustion from that fallback boundary.
#[pyfunction]
pub(crate) fn unpivot_exact_dict_prefix_v1(
    output: &Bound<'_, PyAny>,
    source: &Bound<'_, PyAny>,
    columns: &Bound<'_, PyAny>,
    names_to: &Bound<'_, PyAny>,
    values_to: &Bound<'_, PyAny>,
) -> PyResult<Option<UnpivotPrefix>> {
    #[cfg(Py_GIL_DISABLED)]
    {
        let _ = (output, source, columns, names_to, values_to);
        Ok(None)
    }

    #[cfg(not(Py_GIL_DISABLED))]
    {
        let Some(column_count) =
            validate_unpivot_plan(output, source, columns, names_to, values_to)?
        else {
            return Ok(None);
        };
        let py = output.py();
        let plan = ExactUnpivotPlan {
            output: output.as_ptr(),
            columns: columns.as_ptr(),
            column_count,
            names_to: names_to.as_ptr(),
            values_to: values_to.as_ptr(),
        };
        let mut selected_values = Vec::new();
        let mut previous: Option<Py<PyAny>> = None;
        let mut rows_since_signal_check = 0_u16;

        loop {
            if rows_since_signal_check == 0 {
                py.check_signals()?;
            }
            // Keep the previous loop target and its selected values alive while requesting the
            // next row, matching Python for-loop lifetime at the source boundary.
            let row = unsafe { ffi::PyIter_Next(source.as_ptr()) };
            if row.is_null() {
                if unsafe { !ffi::PyErr_Occurred().is_null() } {
                    return Err(PyErr::fetch(py));
                }
                drop(previous);
                return Ok(Some((None, true)));
            }
            let row = unsafe { Bound::from_owned_ptr(py, row) };
            drop(previous.take());

            if !append_exact_unpivot_row(py, row.as_ptr(), &plan, &mut selected_values)? {
                return Ok(Some((Some(row.unbind()), false)));
            }
            previous = Some(row.unbind());
            rows_since_signal_check = rows_since_signal_check.wrapping_add(1) & 4095;
        }
    }
}

#[cfg(all(test, not(Py_GIL_DISABLED)))]
mod tests {
    use super::*;
    use pyo3::types::{PyDict, PyDictMethods, PyListMethods};

    #[test]
    fn unpivot_prefix_expands_arbitrary_exact_string_fields() {
        Python::initialize();
        Python::attach(|py| {
            let row = PyDict::new(py);
            row.set_item("account", 7).unwrap();
            row.set_item("january", 10).unwrap();
            row.set_item("february", py.None()).unwrap();
            let rows = PyList::new(py, [&row]).unwrap();
            let source = rows.as_any().call_method0("__iter__").unwrap();
            let columns = PyTuple::new(py, ["january", "february"]).unwrap();
            let output = PyList::empty(py);

            let (boundary, completed) = unpivot_exact_dict_prefix_v1(
                output.as_any(),
                &source,
                columns.as_any(),
                "month".into_pyobject(py).unwrap().as_any(),
                "amount".into_pyobject(py).unwrap().as_any(),
            )
            .unwrap()
            .unwrap();

            assert!(completed);
            assert!(boundary.is_none());
            assert_eq!(output.len(), 2);
            let first = output
                .get_item(0)
                .unwrap()
                .cast_into_exact::<PyDict>()
                .unwrap();
            let second = output
                .get_item(1)
                .unwrap()
                .cast_into_exact::<PyDict>()
                .unwrap();
            assert_eq!(
                first
                    .get_item("account")
                    .unwrap()
                    .unwrap()
                    .extract::<i32>()
                    .unwrap(),
                7
            );
            assert_eq!(
                first
                    .get_item("month")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "january"
            );
            assert_eq!(
                first
                    .get_item("amount")
                    .unwrap()
                    .unwrap()
                    .extract::<i32>()
                    .unwrap(),
                10
            );
            assert_eq!(
                second
                    .get_item("month")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "february"
            );
            assert!(second.get_item("amount").unwrap().unwrap().is_none());
            assert!(!first.is(&second));
        });
    }

    #[test]
    fn unpivot_prefix_returns_an_incompatible_row_without_consuming_the_tail() {
        Python::initialize();
        Python::attach(|py| {
            let first = PyDict::new(py);
            first.set_item("id", 1).unwrap();
            first.set_item("north", 10).unwrap();
            let incompatible = PyDict::new(py);
            incompatible.set_item("id", 2).unwrap();
            incompatible.set_item("north", 20).unwrap();
            incompatible.set_item(7, "custom key").unwrap();
            let tail = PyDict::new(py);
            tail.set_item("id", 3).unwrap();
            tail.set_item("north", 30).unwrap();
            let rows = PyList::new(py, [&first, &incompatible, &tail]).unwrap();
            let source = rows.as_any().call_method0("__iter__").unwrap();
            let columns = PyTuple::new(py, ["north"]).unwrap();
            let output = PyList::empty(py);

            let (boundary, completed) = unpivot_exact_dict_prefix_v1(
                output.as_any(),
                &source,
                columns.as_any(),
                "axis".into_pyobject(py).unwrap().as_any(),
                "measure".into_pyobject(py).unwrap().as_any(),
            )
            .unwrap()
            .unwrap();

            assert!(!completed);
            assert_eq!(output.len(), 1);
            assert!(boundary.unwrap().bind(py).is(&incompatible));
            assert!(source.call_method0("__next__").unwrap().is(&tail));
        });
    }

    #[test]
    fn unpivot_prefix_leaves_missing_or_colliding_rows_for_python() {
        Python::initialize();
        Python::attach(|py| {
            for row in [PyDict::new(py), {
                let row = PyDict::new(py);
                row.set_item("north", 10).unwrap();
                row.set_item("axis", "existing").unwrap();
                row
            }] {
                let rows = PyList::new(py, [&row]).unwrap();
                let source = rows.as_any().call_method0("__iter__").unwrap();
                let columns = PyTuple::new(py, ["north"]).unwrap();
                let output = PyList::empty(py);

                let (boundary, completed) = unpivot_exact_dict_prefix_v1(
                    output.as_any(),
                    &source,
                    columns.as_any(),
                    "axis".into_pyobject(py).unwrap().as_any(),
                    "measure".into_pyobject(py).unwrap().as_any(),
                )
                .unwrap()
                .unwrap();

                assert!(!completed);
                assert_eq!(output.len(), 0);
                assert!(boundary.unwrap().bind(py).is(&row));
            }
        });
    }
}
