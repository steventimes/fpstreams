//! Exact-dictionary pivot materialization with a lossless Python fallback boundary.

#[cfg(not(Py_GIL_DISABLED))]
use crate::common::exact_dict_has_only_string_keys;

#[cfg(not(Py_GIL_DISABLED))]
use pyo3::{
    exceptions::{PyMemoryError, PyValueError},
    ffi,
    types::{PyDict, PyString, PyTuple, PyType},
};
use pyo3::{prelude::*, types::PyList};

#[cfg(not(Py_GIL_DISABLED))]
struct PivotPlan {
    index_fields: Vec<*mut ffi::PyObject>,
    column_field: *mut ffi::PyObject,
    value_field: *mut ffi::PyObject,
    key_names: Vec<*mut ffi::PyObject>,
}

#[cfg(not(Py_GIL_DISABLED))]
enum PivotGroupKey {
    Single(Py<PyAny>),
    Multiple(Py<PyTuple>),
}

#[cfg(not(Py_GIL_DISABLED))]
struct PivotGroup {
    key: PivotGroupKey,
    cells: Py<PyDict>,
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn pivot_allocation_error() -> PyErr {
    PyMemoryError::new_err("native pivot allocation failed")
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn new_dict<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
    // SAFETY: PyDict_New returns either one owned exact dictionary or sets an exception.
    let dictionary = unsafe { Bound::<PyAny>::from_owned_ptr_or_err(py, ffi::PyDict_New())? };
    // SAFETY: the non-null result of PyDict_New is always an exact dictionary.
    Ok(unsafe { dictionary.cast_into_unchecked() })
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn set_dict_item(
    py: Python<'_>,
    dictionary: *mut ffi::PyObject,
    key: *mut ffi::PyObject,
    value: *mut ffi::PyObject,
) -> PyResult<()> {
    // SAFETY: every pointer is live and PyDict_SetItem retains its own references.
    if unsafe { ffi::PyDict_SetItem(dictionary, key, value) } != 0 {
        return Err(PyErr::fetch(py));
    }
    Ok(())
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn dict_item(
    py: Python<'_>,
    dictionary: *mut ffi::PyObject,
    key: *mut ffi::PyObject,
) -> PyResult<Option<*mut ffi::PyObject>> {
    // SAFETY: dictionary is exact and the admitted key types have side-effect-free hashing and
    // equality. The returned reference stays borrowed from dictionary.
    let value = unsafe { ffi::PyDict_GetItemWithError(dictionary, key) };
    if value.is_null() {
        if unsafe { !ffi::PyErr_Occurred().is_null() } {
            return Err(PyErr::fetch(py));
        }
        return Ok(None);
    }
    Ok(Some(value))
}

#[cfg(not(Py_GIL_DISABLED))]
fn exact_string_tuple(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<Option<Vec<*mut ffi::PyObject>>> {
    let Ok(tuple) = value.cast_exact::<PyTuple>() else {
        return Ok(None);
    };
    let width = tuple.len();
    let mut fields = Vec::new();
    fields
        .try_reserve(width)
        .map_err(|_| pivot_allocation_error())?;
    for index in 0..width {
        // SAFETY: tuple is exact and immutable, and index is below its fixed length.
        let field = unsafe { ffi::PyTuple_GetItem(tuple.as_ptr(), index as ffi::Py_ssize_t) };
        if field.is_null() {
            return Err(PyErr::fetch(py));
        }
        if unsafe { ffi::PyUnicode_CheckExact(field) } == 0 {
            return Ok(None);
        }
        fields.push(field);
    }
    Ok(Some(fields))
}

#[cfg(not(Py_GIL_DISABLED))]
fn validate_plan<'py>(
    source: &Bound<'py, PyAny>,
    index_fields: &Bound<'py, PyAny>,
    column_field: &Bound<'py, PyAny>,
    value_field: &Bound<'py, PyAny>,
    key_names: &Bound<'py, PyAny>,
    duplicate_error_type: &Bound<'py, PyAny>,
) -> PyResult<Option<(PivotPlan, Bound<'py, PyType>)>> {
    if source.cast_exact::<PyList>().is_err() && source.cast_exact::<PyTuple>().is_err() {
        return Ok(None);
    }
    let py = source.py();
    let Some(index_fields) = exact_string_tuple(py, index_fields)? else {
        return Ok(None);
    };
    let Some(key_names) = exact_string_tuple(py, key_names)? else {
        return Ok(None);
    };
    if index_fields.is_empty() || index_fields.len() != key_names.len() {
        return Ok(None);
    }
    if column_field.cast_exact::<PyString>().is_err()
        || value_field.cast_exact::<PyString>().is_err()
    {
        return Ok(None);
    }
    let Ok(duplicate_error_type) = duplicate_error_type.cast::<PyType>() else {
        return Ok(None);
    };

    // Public Rows.pivot already rejects duplicate output names. Keep the direct ABI lossless too:
    // exact strings make this validation incapable of invoking user protocols.
    let seen_names = new_dict(py)?;
    let none = py.None();
    for &name in &key_names {
        if dict_item(py, seen_names.as_ptr(), name)?.is_some() {
            return Ok(None);
        }
        set_dict_item(py, seen_names.as_ptr(), name, none.bind(py).as_ptr())?;
    }

    Ok(Some((
        PivotPlan {
            index_fields,
            column_field: column_field.as_ptr(),
            value_field: value_field.as_ptr(),
            key_names,
        },
        duplicate_error_type.clone(),
    )))
}

#[cfg(not(Py_GIL_DISABLED))]
fn snapshot_exact_source(py: Python<'_>, source: &Bound<'_, PyAny>) -> PyResult<Vec<Py<PyAny>>> {
    let (row_count, get_row): (usize, fn(*mut ffi::PyObject, usize) -> *mut ffi::PyObject) =
        if let Ok(rows) = source.cast_exact::<PyList>() {
            (rows.len(), |container, index| {
                // SAFETY: called only while attached to Python, with an exact list and an
                // index below the length captured before this callback-free snapshot.
                unsafe { ffi::PyList_GetItem(container, index as ffi::Py_ssize_t) }
            })
        } else {
            let rows = source.cast_exact::<PyTuple>()?;
            (rows.len(), |container, index| {
                // SAFETY: exact tuples are immutable and index is below their fixed length.
                unsafe { ffi::PyTuple_GetItem(container, index as ffi::Py_ssize_t) }
            })
        };
    let mut snapshot = Vec::new();
    snapshot
        .try_reserve(row_count)
        .map_err(|_| pivot_allocation_error())?;
    for index in 0..row_count {
        let row = get_row(source.as_ptr(), index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
        // SAFETY: source owns this borrowed row for the callback-free snapshot. The new strong
        // reference lets signal checkpoints run without exposing a dangling list-item borrow.
        snapshot.push(unsafe { Borrowed::from_ptr(py, row).to_owned().unbind() });
    }
    Ok(snapshot)
}

#[cfg(not(Py_GIL_DISABLED))]
fn tuple_from_borrowed<'py>(
    py: Python<'py>,
    values: &[*mut ffi::PyObject],
) -> PyResult<Bound<'py, PyTuple>> {
    PyTuple::new(
        py,
        values.iter().map(|&value| {
            // SAFETY: every value is retained by the exact row for this tuple construction.
            unsafe { Borrowed::from_ptr(py, value) }
        }),
    )
}

#[cfg(not(Py_GIL_DISABLED))]
fn duplicate_error(
    py: Python<'_>,
    error_type: &Bound<'_, PyType>,
    key_values: &[*mut ffi::PyObject],
    current_key: Option<&Bound<'_, PyTuple>>,
    column: *mut ffi::PyObject,
) -> PyResult<PyErr> {
    let owned_key;
    let key = if let Some(key) = current_key {
        key
    } else {
        owned_key = tuple_from_borrowed(py, key_values)?;
        &owned_key
    };
    let key_repr = key.repr()?.extract::<String>()?;
    // SAFETY: column is borrowed from the current exact dictionary and is an exact string.
    let column = unsafe { Borrowed::from_ptr(py, column) };
    let column_repr = column.repr()?.extract::<String>()?;
    Ok(PyErr::from_type(
        error_type.clone(),
        format!("multiple values for pivot key {key_repr}, column {column_repr}"),
    ))
}

#[cfg(not(Py_GIL_DISABLED))]
fn materialize_output(
    py: Python<'_>,
    plan: &PivotPlan,
    groups: Vec<PivotGroup>,
    columns: &[Py<PyAny>],
    fill: &Bound<'_, PyAny>,
) -> PyResult<Py<PyList>> {
    let output = PyList::empty(py);
    if groups.is_empty() {
        return Ok(output.unbind());
    }

    // Multiple groups share the sparse-wide fill layout through one exact template. A single
    // group uses the canonical one-dictionary path to avoid doubling peak record width.
    let template = if groups.len() > 1 {
        let template = new_dict(py)?;
        for &name in &plan.key_names {
            set_dict_item(py, template.as_ptr(), name, fill.as_ptr())?;
        }
        for column in columns {
            set_dict_item(
                py,
                template.as_ptr(),
                column.bind(py).as_ptr(),
                fill.as_ptr(),
            )?;
        }
        Some(template)
    } else {
        None
    };

    for (position, group) in groups.into_iter().enumerate() {
        if position != 0 && position & 4095 == 0 {
            py.check_signals()?;
        }
        let record = if let Some(template) = &template {
            // SAFETY: PyDict_Copy returns one owned exact dictionary or sets an exception.
            let record = unsafe {
                Bound::<PyAny>::from_owned_ptr_or_err(py, ffi::PyDict_Copy(template.as_ptr()))?
            };
            // SAFETY: PyDict_Copy of an exact dictionary is exact.
            unsafe { record.cast_into_unchecked::<PyDict>() }
        } else {
            new_dict(py)?
        };

        match group.key {
            PivotGroupKey::Single(value) => {
                set_dict_item(
                    py,
                    record.as_ptr(),
                    plan.key_names[0],
                    value.bind(py).as_ptr(),
                )?;
            }
            PivotGroupKey::Multiple(values) => {
                for (index, &name) in plan.key_names.iter().enumerate() {
                    // SAFETY: values has the same fixed width as plan.key_names.
                    let value = unsafe {
                        ffi::PyTuple_GetItem(values.bind(py).as_ptr(), index as ffi::Py_ssize_t)
                    };
                    if value.is_null() {
                        return Err(PyErr::fetch(py));
                    }
                    set_dict_item(py, record.as_ptr(), name, value)?;
                }
            }
        }

        if template.is_some() {
            // SAFETY: both operands are exact dictionaries and overriding existing values keeps
            // the template's key order while retaining each selected value's identity.
            if unsafe { ffi::PyDict_Merge(record.as_ptr(), group.cells.bind(py).as_ptr(), 1) } != 0
            {
                return Err(PyErr::fetch(py));
            }
        } else {
            for column in columns {
                let column = column.bind(py).as_ptr();
                let selected =
                    dict_item(py, group.cells.bind(py).as_ptr(), column)?.unwrap_or(fill.as_ptr());
                set_dict_item(py, record.as_ptr(), column, selected)?;
            }
        }
        output.append(record)?;
    }
    Ok(output.unbind())
}

/// Materialize the strict, direct-field ``aggregate="error"`` pivot specialization.
///
/// ``None`` is a lossless request for the Python implementation: the accepted source is always a
/// retained exact list/tuple and this function never mutates or iterates it through protocols.
#[pyfunction]
pub(crate) fn pivot_exact_dict_rows_v1(
    source: &Bound<'_, PyAny>,
    index_fields: &Bound<'_, PyAny>,
    column_field: &Bound<'_, PyAny>,
    value_field: &Bound<'_, PyAny>,
    key_names: &Bound<'_, PyAny>,
    fill: &Bound<'_, PyAny>,
    duplicate_error_type: &Bound<'_, PyAny>,
) -> PyResult<Option<Py<PyList>>> {
    #[cfg(Py_GIL_DISABLED)]
    {
        let _ = (
            source,
            index_fields,
            column_field,
            value_field,
            key_names,
            fill,
            duplicate_error_type,
        );
        Ok(None)
    }

    #[cfg(not(Py_GIL_DISABLED))]
    {
        let Some((plan, duplicate_error_type)) = validate_plan(
            source,
            index_fields,
            column_field,
            value_field,
            key_names,
            duplicate_error_type,
        )?
        else {
            return Ok(None);
        };
        let py = source.py();
        let snapshot = snapshot_exact_source(py, source)?;
        let group_index = new_dict(py)?;
        let seen_columns = new_dict(py)?;
        let key_name_set = new_dict(py)?;
        let none = py.None();
        for &name in &plan.key_names {
            set_dict_item(py, key_name_set.as_ptr(), name, none.bind(py).as_ptr())?;
        }

        let mut groups = Vec::new();
        groups
            .try_reserve(snapshot.len())
            .map_err(|_| pivot_allocation_error())?;
        let mut columns = Vec::new();
        let mut key_values = Vec::new();
        key_values
            .try_reserve(plan.index_fields.len())
            .map_err(|_| pivot_allocation_error())?;

        for (row_index, owned_row) in snapshot.iter().enumerate() {
            if row_index & 4095 == 0 {
                py.check_signals()?;
            }
            let row = owned_row.bind(py).as_ptr();
            if unsafe { ffi::PyDict_CheckExact(row) } == 0
                || !exact_dict_has_only_string_keys(py, row)?
            {
                return Ok(None);
            }

            key_values.clear();
            for &field in &plan.index_fields {
                let Some(value) = dict_item(py, row, field)? else {
                    return Ok(None);
                };
                if unsafe { ffi::PyLong_CheckExact(value) } == 0
                    && unsafe { ffi::PyUnicode_CheckExact(value) } == 0
                {
                    return Ok(None);
                }
                key_values.push(value);
            }
            let Some(column) = dict_item(py, row, plan.column_field)? else {
                return Ok(None);
            };
            if unsafe { ffi::PyUnicode_CheckExact(column) } == 0 {
                return Ok(None);
            }
            if dict_item(py, key_name_set.as_ptr(), column)?.is_some() {
                // SAFETY: column is retained by the current exact row and is an exact string.
                let column = unsafe { Borrowed::from_ptr(py, column) };
                return Err(PyValueError::new_err(format!(
                    "pivot column {} collides with an index column",
                    column.repr()?.extract::<String>()?
                )));
            }
            if dict_item(py, seen_columns.as_ptr(), column)?.is_none() {
                set_dict_item(py, seen_columns.as_ptr(), column, none.bind(py).as_ptr())?;
                // SAFETY: column is retained by the current row. Own the first encountered
                // exact-string object so output dictionaries preserve its identity and order.
                columns.push(unsafe { Borrowed::from_ptr(py, column).to_owned().unbind() });
            }

            let current_tuple = if key_values.len() == 1 {
                None
            } else {
                Some(tuple_from_borrowed(py, &key_values)?)
            };
            let lookup_key = current_tuple
                .as_ref()
                .map_or(key_values[0], |values| values.as_ptr());
            let cells = if let Some(cells) = dict_item(py, group_index.as_ptr(), lookup_key)? {
                cells
            } else {
                let cells = new_dict(py)?;
                set_dict_item(py, group_index.as_ptr(), lookup_key, cells.as_ptr())?;
                let group_key = if let Some(values) = &current_tuple {
                    PivotGroupKey::Multiple(values.clone().unbind())
                } else {
                    // SAFETY: the exact row retains the selected scalar; own the first equal key
                    // object so output preserves Python dictionary insertion identity.
                    PivotGroupKey::Single(unsafe {
                        Borrowed::from_ptr(py, key_values[0]).to_owned().unbind()
                    })
                };
                let cells = cells.unbind();
                let cells_ptr = cells.bind(py).as_ptr();
                groups.push(PivotGroup {
                    key: group_key,
                    cells,
                });
                cells_ptr
            };

            // Python selects the value before applying the duplicate policy. Missing values must
            // therefore decline even when the cell itself already exists.
            let Some(value) = dict_item(py, row, plan.value_field)? else {
                return Ok(None);
            };
            if dict_item(py, cells, column)?.is_some() {
                return Err(duplicate_error(
                    py,
                    &duplicate_error_type,
                    &key_values,
                    current_tuple.as_ref(),
                    column,
                )?);
            }
            set_dict_item(py, cells, column, value)?;
        }

        materialize_output(py, &plan, groups, &columns, fill).map(Some)
    }
}

#[cfg(all(test, not(Py_GIL_DISABLED)))]
mod tests {
    use super::*;
    use pyo3::{
        exceptions::{PyRuntimeError, PyValueError},
        types::{PyDictMethods, PyListMethods, PyString},
    };

    fn fields<'py>(py: Python<'py>, names: &[&str]) -> Bound<'py, PyTuple> {
        PyTuple::new(py, names).unwrap()
    }

    #[test]
    fn exact_pivot_preserves_group_column_field_and_value_identity() {
        Python::initialize();
        Python::attach(|py| {
            let fill = PyDict::new(py);
            let cpu = PyString::new(py, "cpu");
            let memory = PyString::new(py, "memory");
            let first_value = PyList::new(py, [10]).unwrap();
            let second_value = PyList::new(py, [20]).unwrap();

            let first = PyDict::new(py);
            first.set_item("site", "north").unwrap();
            first.set_item("rack", 1).unwrap();
            first.set_item("metric", &cpu).unwrap();
            first.set_item("value", &first_value).unwrap();
            let second = PyDict::new(py);
            second.set_item("site", "south").unwrap();
            second.set_item("rack", 2).unwrap();
            second.set_item("metric", &memory).unwrap();
            second.set_item("value", &second_value).unwrap();
            let third = PyDict::new(py);
            third.set_item("site", "north").unwrap();
            third.set_item("rack", 1).unwrap();
            third.set_item("metric", "disk").unwrap();
            third.set_item("value", 30).unwrap();
            let source = PyList::new(py, [&first, &second, &third]).unwrap();

            let output = pivot_exact_dict_rows_v1(
                source.as_any(),
                fields(py, &["site", "rack"]).as_any(),
                PyString::new(py, "metric").as_any(),
                PyString::new(py, "value").as_any(),
                fields(py, &["site", "rack"]).as_any(),
                fill.as_any(),
                py.get_type::<PyValueError>().as_any(),
            )
            .unwrap()
            .unwrap();

            let output = output.bind(py);
            assert_eq!(output.len(), 2);
            let north = output.get_item(0).unwrap().cast_into::<PyDict>().unwrap();
            let south = output.get_item(1).unwrap().cast_into::<PyDict>().unwrap();
            assert_eq!(
                north.keys().extract::<Vec<String>>().unwrap(),
                ["site", "rack", "cpu", "memory", "disk"]
            );
            assert_eq!(
                south.keys().extract::<Vec<String>>().unwrap(),
                ["site", "rack", "cpu", "memory", "disk"]
            );
            assert!(north.get_item(&cpu).unwrap().unwrap().is(&first_value));
            assert!(south.get_item(&memory).unwrap().unwrap().is(&second_value));
            assert!(north.get_item(&memory).unwrap().unwrap().is(&fill));
            assert!(south.get_item(&cpu).unwrap().unwrap().is(&fill));
            assert!(north.keys().get_item(2).unwrap().is(&cpu));
            assert!(north.keys().get_item(3).unwrap().is(&memory));
            assert!(source.get_item(0).unwrap().is(&first));
        });
    }

    #[test]
    fn exact_pivot_accepts_tuple_sources_and_arbitrary_exact_integers() {
        Python::initialize();
        Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                c"value = 1 << 200\nother = int(str(value))\n",
                c"pivot_big_int.py",
                c"pivot_big_int",
            )
            .unwrap();
            let value = module.getattr("value").unwrap();
            let other = module.getattr("other").unwrap();
            assert!(!value.is(&other));
            let first = PyDict::new(py);
            first.set_item("id", &value).unwrap();
            first.set_item("axis", "left").unwrap();
            first.set_item("amount", 1).unwrap();
            let second = PyDict::new(py);
            second.set_item("id", &other).unwrap();
            second.set_item("axis", "right").unwrap();
            second.set_item("amount", 2).unwrap();
            let source = PyTuple::new(py, [&first, &second]).unwrap();

            let output = pivot_exact_dict_rows_v1(
                source.as_any(),
                fields(py, &["id"]).as_any(),
                PyString::new(py, "axis").as_any(),
                PyString::new(py, "amount").as_any(),
                fields(py, &["id"]).as_any(),
                py.None().bind(py),
                py.get_type::<PyValueError>().as_any(),
            )
            .unwrap()
            .unwrap();
            let output = output.bind(py);
            assert_eq!(output.len(), 1);
            let row = output.get_item(0).unwrap().cast_into::<PyDict>().unwrap();
            assert!(row.get_item("id").unwrap().unwrap().is(&value));
            assert_eq!(
                row.get_item("left")
                    .unwrap()
                    .unwrap()
                    .extract::<i32>()
                    .unwrap(),
                1
            );
            assert_eq!(
                row.get_item("right")
                    .unwrap()
                    .unwrap()
                    .extract::<i32>()
                    .unwrap(),
                2
            );
        });
    }

    #[test]
    fn exact_pivot_declines_incompatible_rows_without_mutating_the_source() {
        Python::initialize();
        Python::attach(|py| {
            let invalid_rows = [
                {
                    let row = PyDict::new(py);
                    row.set_item("id", true).unwrap();
                    row.set_item("axis", "left").unwrap();
                    row.set_item("amount", 1).unwrap();
                    row
                },
                {
                    let row = PyDict::new(py);
                    row.set_item("id", 1).unwrap();
                    row.set_item("axis", 7).unwrap();
                    row.set_item("amount", 1).unwrap();
                    row
                },
                {
                    let row = PyDict::new(py);
                    row.set_item("id", 1).unwrap();
                    row.set_item("axis", "left").unwrap();
                    row.set_item(7, "unsafe key").unwrap();
                    row
                },
            ];
            for row in invalid_rows {
                let source = PyList::new(py, [&row]).unwrap();
                assert!(
                    pivot_exact_dict_rows_v1(
                        source.as_any(),
                        fields(py, &["id"]).as_any(),
                        PyString::new(py, "axis").as_any(),
                        PyString::new(py, "amount").as_any(),
                        fields(py, &["id"]).as_any(),
                        py.None().bind(py),
                        py.get_type::<PyValueError>().as_any(),
                    )
                    .unwrap()
                    .is_none()
                );
                assert_eq!(source.len(), 1);
                assert!(source.get_item(0).unwrap().is(&row));
            }
        });
    }

    #[test]
    fn exact_pivot_uses_the_supplied_duplicate_error_and_rejects_name_collisions() {
        Python::initialize();
        Python::attach(|py| {
            let first = PyDict::new(py);
            first.set_item("id", 1).unwrap();
            first.set_item("axis", "left").unwrap();
            first.set_item("amount", 10).unwrap();
            let duplicate = PyDict::new(py);
            duplicate.set_item("id", 1).unwrap();
            duplicate.set_item("axis", "left").unwrap();
            duplicate.set_item("amount", 20).unwrap();
            let source = PyList::new(py, [&first, &duplicate]).unwrap();

            let error = pivot_exact_dict_rows_v1(
                source.as_any(),
                fields(py, &["id"]).as_any(),
                PyString::new(py, "axis").as_any(),
                PyString::new(py, "amount").as_any(),
                fields(py, &["id"]).as_any(),
                py.None().bind(py),
                py.get_type::<PyRuntimeError>().as_any(),
            )
            .unwrap_err();
            assert!(error.is_instance_of::<PyRuntimeError>(py));
            assert_eq!(
                error.value(py).to_string(),
                "multiple values for pivot key (1,), column 'left'"
            );

            first.set_item("axis", "id").unwrap();
            let source = PyList::new(py, [&first]).unwrap();
            let collision = pivot_exact_dict_rows_v1(
                source.as_any(),
                fields(py, &["id"]).as_any(),
                PyString::new(py, "axis").as_any(),
                PyString::new(py, "amount").as_any(),
                fields(py, &["id"]).as_any(),
                py.None().bind(py),
                py.get_type::<PyRuntimeError>().as_any(),
            )
            .unwrap_err();
            assert!(collision.is_instance_of::<PyValueError>(py));
            assert_eq!(
                collision.value(py).to_string(),
                "pivot column 'id' collides with an index column"
            );
        });
    }
}
