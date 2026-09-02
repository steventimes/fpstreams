//! One-pass exact-dictionary projection with a lossless Python fallback boundary.

#[cfg(not(Py_GIL_DISABLED))]
use crate::common::{
    KernelError, direct_field_selection_error, exact_dict_has_only_string_keys_up_to,
    is_exact_sequence_iterator,
};
use crate::integer::Instruction;
#[cfg(not(Py_GIL_DISABLED))]
use crate::integer::prepare_expression;
use pyo3::prelude::*;
#[cfg(not(Py_GIL_DISABLED))]
use pyo3::{
    exceptions::{PyAttributeError, PyKeyError, PyStopIteration, PyTypeError},
    ffi,
    types::{PyList, PyTuple},
};

type SelectPrefix = (Option<Py<PyAny>>, bool);

#[cfg(not(Py_GIL_DISABLED))]
const I64_ROW_FILTER_MAX_FIELDS: ffi::Py_ssize_t = 2;

#[cfg(not(Py_GIL_DISABLED))]
struct ExactSelectPlan {
    output: *mut ffi::PyObject,
    output_names: *mut ffi::PyObject,
    input_fields: *mut ffi::PyObject,
    field_count: ffi::Py_ssize_t,
    selection_error_type: *mut ffi::PyObject,
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn validate_select_plan(
    output: &Bound<'_, PyAny>,
    source: &Bound<'_, PyAny>,
    output_names: &Bound<'_, PyAny>,
    input_fields: &Bound<'_, PyAny>,
    selection_error_type: &Bound<'_, PyAny>,
) -> PyResult<Option<ffi::Py_ssize_t>> {
    if output.cast_exact::<PyList>().is_err() || !selection_error_type.is_callable() {
        return Ok(None);
    }
    let (Ok(output_names), Ok(input_fields)) = (
        output_names.cast_exact::<PyTuple>(),
        input_fields.cast_exact::<PyTuple>(),
    ) else {
        return Ok(None);
    };
    let output_count = output_names.len();
    let input_count = input_fields.len();
    if output_count != input_count || !is_exact_sequence_iterator(output.py(), source)? {
        return Ok(None);
    }
    for index in 0..input_count {
        // SAFETY: both tuples are exact and index is within their fixed, equal lengths.
        let tuple_index = index as ffi::Py_ssize_t;
        let output_name = unsafe { ffi::PyTuple_GetItem(output_names.as_ptr(), tuple_index) };
        let input_field = unsafe { ffi::PyTuple_GetItem(input_fields.as_ptr(), tuple_index) };
        if output_name.is_null() || input_field.is_null() {
            return Err(PyErr::fetch(output.py()));
        }
        if unsafe { ffi::PyUnicode_CheckExact(output_name) } == 0
            || unsafe { ffi::PyUnicode_CheckExact(input_field) } == 0
        {
            return Ok(None);
        }
    }
    Ok(Some(input_count as ffi::Py_ssize_t))
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn select_exact_dict_row(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    plan: &ExactSelectPlan,
) -> PyResult<Option<Py<PyAny>>> {
    if unsafe { ffi::PyDict_CheckExact(row) } == 0 {
        return Ok(None);
    }
    // SAFETY: PyDict_New returns one owned exact dictionary or sets an exception.
    let record = unsafe { Bound::from_owned_ptr_or_err(py, ffi::PyDict_New())? };
    for index in 0..plan.field_count {
        if index != 0 && index & 4095 == 0 {
            py.check_signals()?;
        }
        // SAFETY: index is within both live exact tuples' fixed lengths.
        let output_name = unsafe { ffi::PyTuple_GetItem(plan.output_names, index) };
        let input_field = unsafe { ffi::PyTuple_GetItem(plan.input_fields, index) };
        if output_name.is_null() || input_field.is_null() {
            return Err(PyErr::fetch(py));
        }

        // SAFETY: this kernel is disabled on free-threaded builds, the GIL is held, row stays
        // live for the call, and input_field is owned by the plan tuple.  A borrowed lookup
        // avoids one redundant incref/decref pair before PyDict_SetItem retains the value.
        let selected = unsafe { ffi::PyDict_GetItemWithError(row, input_field) };
        if selected.is_null() {
            let error = if unsafe { ffi::PyErr_Occurred().is_null() } {
                // SAFETY: input_field remains live through the plan tuple.
                let field = unsafe { Borrowed::from_ptr(py, input_field) }.to_owned();
                PyKeyError::new_err(field.unbind())
            } else {
                PyErr::fetch(py)
            };
            // SAFETY: both pointers remain owned by their plan tuples for this call.
            let field = unsafe { Borrowed::from_ptr(py, input_field) };
            let selection_error_type = unsafe { Borrowed::from_ptr(py, plan.selection_error_type) };
            return Err(direct_field_selection_error(
                field.as_any(),
                selection_error_type.as_any(),
                error,
            )?);
        }
        // SAFETY: record is private; output_name is owned by the plan tuple, and the borrowed
        // selected value remains live through row while PyDict_SetItem retains its own reference.
        if unsafe { ffi::PyDict_SetItem(record.as_ptr(), output_name, selected) } != 0 {
            return Err(PyErr::fetch(py));
        }
    }
    Ok(Some(record.unbind()))
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn exact_dict_field_is_not_none(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    field: *mut ffi::PyObject,
) -> PyResult<Option<bool>> {
    if unsafe { ffi::PyDict_CheckExact(row) } == 0 {
        return Ok(None);
    }
    // SAFETY: this helper is reachable only from the GIL-held kernel below, row remains live,
    // and field is a live exact string. The borrowed value is inspected before either can drop.
    let selected = unsafe { ffi::PyDict_GetItemWithError(row, field) };
    if selected.is_null() {
        if unsafe { ffi::PyErr_Occurred().is_null() } {
            return Ok(Some(false));
        }
        let error = PyErr::fetch(py);
        if error.is_instance_of::<PyAttributeError>(py)
            || error.is_instance_of::<PyKeyError>(py)
            || error.is_instance_of::<PyTypeError>(py)
        {
            return Ok(Some(false));
        }
        return Err(error);
    }
    // SAFETY: selected is a borrowed value owned by the still-live exact row dictionary.
    Ok(Some(selected != unsafe { ffi::Py_None() }))
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn exact_dict_i64_field(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    field: *mut ffi::PyObject,
) -> PyResult<Option<i64>> {
    if unsafe { ffi::PyDict_CheckExact(row) } == 0 {
        return Ok(None);
    }
    // A custom key could run equality during lookup. Decline before that observable protocol so
    // replaying a Python boundary never executes the same lookup twice.
    if !exact_dict_has_only_string_keys_up_to(py, row, I64_ROW_FILTER_MAX_FIELDS)? {
        return Ok(None);
    }
    // SAFETY: this helper is reachable only from the GIL-held kernel below, row remains live,
    // and field is a live exact string. The borrowed value is consumed before either can drop.
    let selected = unsafe { ffi::PyDict_GetItemWithError(row, field) };
    if selected.is_null() {
        return if unsafe { ffi::PyErr_Occurred().is_null() } {
            Ok(None)
        } else {
            Err(PyErr::fetch(py))
        };
    }
    if unsafe { ffi::PyLong_CheckExact(selected) } == 0 {
        return Ok(None);
    }
    let mut overflow = 0;
    // SAFETY: exact Python integers support direct signed extraction without protocol dispatch.
    let value = unsafe { ffi::PyLong_AsLongLongAndOverflow(selected, &mut overflow) };
    if overflow != 0 {
        return Ok(None);
    }
    if value == -1 && unsafe { !ffi::PyErr_Occurred().is_null() } {
        return Err(PyErr::fetch(py));
    }
    Ok(Some(value))
}

/// Append the exact-dictionary prefix accepted by one closed i64 row expression.
///
/// A row that needs Python integer width, object protocols, or exceptional arithmetic is returned
/// untouched. The caller can then replay that boundary and the remaining iterator through the
/// canonical generated Rows sink.
#[pyfunction]
pub(crate) fn filter_i64_expr_exact_dict_prefix_v1(
    output: &Bound<'_, PyAny>,
    source: &Bound<'_, PyAny>,
    field: &Bound<'_, PyAny>,
    instructions: Vec<Instruction>,
    negate: bool,
) -> PyResult<Option<SelectPrefix>> {
    #[cfg(Py_GIL_DISABLED)]
    {
        let _ = (output, source, field, instructions, negate);
        Ok(None)
    }

    #[cfg(not(Py_GIL_DISABLED))]
    {
        if output.cast_exact::<PyList>().is_err()
            || unsafe { ffi::PyUnicode_CheckExact(field.as_ptr()) } == 0
            || !is_exact_sequence_iterator(output.py(), source)?
        {
            return Ok(None);
        }
        let py = output.py();
        let expression = prepare_expression(instructions);
        let mut stack = Vec::new();
        let mut previous: Option<Py<PyAny>> = None;
        let mut rows_since_signal_check = 0_u16;

        loop {
            if rows_since_signal_check == 0 {
                py.check_signals()?;
            }
            // Keep the previous target alive until the next pull, matching Python's for-loop
            // target lifetime for both accepted and rejected rows.
            let row = unsafe { ffi::PyIter_Next(source.as_ptr()) };
            if row.is_null() {
                if unsafe { !ffi::PyErr_Occurred().is_null() } {
                    return Err(PyErr::fetch(py));
                }
                drop(previous);
                return Ok(Some((None, true)));
            }
            // SAFETY: PyIter_Next returned one owned reference.
            let row = unsafe { Bound::from_owned_ptr(py, row) };
            drop(previous.take());

            let Some(value) = exact_dict_i64_field(py, row.as_ptr(), field.as_ptr())? else {
                return Ok(Some((Some(row.unbind()), false)));
            };
            let accepted = match expression.evaluate(value, &mut stack) {
                Ok(result) => result != 0,
                Err(
                    KernelError::DivisionByZero
                    | KernelError::InvalidProgram(_)
                    | KernelError::Overflow,
                ) => return Ok(Some((Some(row.unbind()), false))),
            };
            if accepted != negate
                && unsafe { ffi::PyList_Append(output.as_ptr(), row.as_ptr()) } != 0
            {
                return Err(PyErr::fetch(py));
            }
            previous = Some(row.unbind());
            rows_since_signal_check = rows_since_signal_check.wrapping_add(1) & 4095;
        }
    }
}

/// Append an exact-dictionary prefix whose selected field is not None.
///
/// Missing fields and the lookup failures suppressed by Rows.drop_nulls are treated as null. A
/// non-exact dictionary row is returned untouched so Python can resume the canonical Mapping and
/// selector protocols for it and the remaining suffix.
#[pyfunction]
pub(crate) fn drop_nulls_exact_dict_prefix_v1(
    output: &Bound<'_, PyAny>,
    source: &Bound<'_, PyAny>,
    field: &Bound<'_, PyAny>,
) -> PyResult<Option<SelectPrefix>> {
    #[cfg(Py_GIL_DISABLED)]
    {
        let _ = (output, source, field);
        Ok(None)
    }

    #[cfg(not(Py_GIL_DISABLED))]
    {
        if output.cast_exact::<PyList>().is_err()
            || unsafe { ffi::PyUnicode_CheckExact(field.as_ptr()) } == 0
            || !is_exact_sequence_iterator(output.py(), source)?
        {
            return Ok(None);
        }
        let py = output.py();
        let mut previous: Option<Py<PyAny>> = None;
        let mut rows_since_signal_check = 0_u16;

        loop {
            if rows_since_signal_check == 0 {
                py.check_signals()?;
            }
            // Keep the previous target live until the next pull, matching Python's for-loop
            // lifetime even for a dropped row.
            let row = unsafe { ffi::PyIter_Next(source.as_ptr()) };
            if row.is_null() {
                if unsafe { !ffi::PyErr_Occurred().is_null() } {
                    return Err(PyErr::fetch(py));
                }
                drop(previous);
                return Ok(Some((None, true)));
            }
            // SAFETY: PyIter_Next returned one owned reference.
            let row = unsafe { Bound::from_owned_ptr(py, row) };
            drop(previous.take());

            let Some(keep) = exact_dict_field_is_not_none(py, row.as_ptr(), field.as_ptr())? else {
                return Ok(Some((Some(row.unbind()), false)));
            };
            if keep {
                // SAFETY: output is an exact list and PyList_Append retains the original row.
                if unsafe { ffi::PyList_Append(output.as_ptr(), row.as_ptr()) } != 0 {
                    return Err(PyErr::fetch(py));
                }
            }
            previous = Some(row.unbind());
            rows_since_signal_check = rows_since_signal_check.wrapping_add(1) & 4095;
        }
    }
}

/// Append the compatible exact-dictionary prefix of a direct Rows.select projection.
///
/// Field validation and projection happen in the same traversal. A non-exact dictionary row is
/// returned untouched so Python can resume its canonical Mapping/attribute behavior; missing
/// exact-dictionary fields raise the canonical SelectionError immediately.
#[pyfunction]
pub(crate) fn select_exact_dict_prefix_v1(
    output: &Bound<'_, PyAny>,
    source: &Bound<'_, PyAny>,
    output_names: &Bound<'_, PyAny>,
    input_fields: &Bound<'_, PyAny>,
    selection_error_type: &Bound<'_, PyAny>,
) -> PyResult<Option<SelectPrefix>> {
    #[cfg(Py_GIL_DISABLED)]
    {
        let _ = (
            output,
            source,
            output_names,
            input_fields,
            selection_error_type,
        );
        Ok(None)
    }

    #[cfg(not(Py_GIL_DISABLED))]
    {
        let Some(field_count) = validate_select_plan(
            output,
            source,
            output_names,
            input_fields,
            selection_error_type,
        )?
        else {
            return Ok(None);
        };
        let py = output.py();
        let plan = ExactSelectPlan {
            output: output.as_ptr(),
            output_names: output_names.as_ptr(),
            input_fields: input_fields.as_ptr(),
            field_count,
            selection_error_type: selection_error_type.as_ptr(),
        };
        let mut previous: Option<Py<PyAny>> = None;
        let mut rows_since_signal_check = 0_u16;

        loop {
            if rows_since_signal_check == 0 {
                py.check_signals()?;
            }
            // Keep the previous target live while obtaining the next, matching the generated
            // Python for-loop used by the canonical Rows fusion path.
            let row = unsafe { ffi::PyIter_Next(source.as_ptr()) };
            if row.is_null() {
                if unsafe { !ffi::PyErr_Occurred().is_null() } {
                    return Err(PyErr::fetch(py));
                }
                drop(previous);
                return Ok(Some((None, true)));
            }
            // SAFETY: PyIter_Next returned one owned reference.
            let row = unsafe { Bound::from_owned_ptr(py, row) };
            drop(previous.take());

            let record = match select_exact_dict_row(py, row.as_ptr(), &plan) {
                Ok(Some(record)) => record,
                Ok(None) => return Ok(Some((Some(row.unbind()), false))),
                Err(error) if error.is_instance_of::<PyStopIteration>(py) => {
                    drop(row);
                    drop(previous);
                    return Ok(Some((None, true)));
                }
                Err(error) => return Err(error),
            };
            // SAFETY: output is a live exact list and PyList_Append retains its own reference.
            if unsafe { ffi::PyList_Append(plan.output, record.bind(py).as_ptr()) } != 0 {
                return Err(PyErr::fetch(py));
            }
            previous = Some(row.unbind());
            rows_since_signal_check = rows_since_signal_check.wrapping_add(1) & 4095;
        }
    }
}

#[cfg(all(test, not(Py_GIL_DISABLED)))]
mod tests {
    use super::*;
    use pyo3::types::{PyDict, PyDictMethods, PyListMethods, PyString};

    fn selection_error<'py>(py: Python<'py>) -> Bound<'py, PyAny> {
        let package_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("src");
        PyModule::import(py, "sys")
            .unwrap()
            .getattr("path")
            .unwrap()
            .call_method1("insert", (0, package_root.to_str().unwrap()))
            .unwrap();
        PyModule::import(py, "fpstreams.errors")
            .unwrap()
            .getattr("SelectionError")
            .unwrap()
    }

    #[test]
    fn select_projects_and_validates_in_one_pass() {
        Python::initialize();
        Python::attach(|py| {
            let first = PyDict::new(py);
            first.set_item("id", 1).unwrap();
            first.set_item("value", 2).unwrap();
            let second = PyDict::new(py);
            second.set_item("id", 3).unwrap();
            second.set_item("value", 4).unwrap();
            let rows = PyList::new(py, [&first, &second]).unwrap();
            let source = rows.as_any().call_method0("__iter__").unwrap();
            let output = PyList::empty(py);
            let outputs = PyTuple::new(py, ["key", "amount"]).unwrap();
            let inputs = PyTuple::new(py, ["id", "value"]).unwrap();

            let result = select_exact_dict_prefix_v1(
                output.as_any(),
                &source,
                outputs.as_any(),
                inputs.as_any(),
                &selection_error(py),
            )
            .unwrap()
            .unwrap();

            assert!(result.1);
            assert!(result.0.is_none());
            assert_eq!(output.len(), 2);
            assert_eq!(
                output
                    .get_item(1)
                    .unwrap()
                    .get_item("amount")
                    .unwrap()
                    .extract::<i32>()
                    .unwrap(),
                4
            );
            assert_eq!(first.len(), 2);
            assert_eq!(second.len(), 2);
        });
    }

    #[test]
    fn select_returns_non_dict_boundary_and_translates_missing_field() {
        Python::initialize();
        Python::attach(|py| {
            let first = PyDict::new(py);
            first.set_item("id", 1).unwrap();
            let boundary = PyList::new(py, [2]).unwrap();
            let tail = PyDict::new(py);
            tail.set_item("id", 3).unwrap();
            let rows =
                PyList::new(py, [&first.as_any(), &boundary.as_any(), &tail.as_any()]).unwrap();
            let source = rows.as_any().call_method0("__iter__").unwrap();
            let output = PyList::empty(py);
            let names = PyTuple::new(py, ["id"]).unwrap();

            let (returned, completed) = select_exact_dict_prefix_v1(
                output.as_any(),
                &source,
                names.as_any(),
                names.as_any(),
                &selection_error(py),
            )
            .unwrap()
            .unwrap();

            assert!(!completed);
            assert!(returned.unwrap().bind(py).is(&boundary));
            assert!(source.call_method0("__next__").unwrap().is(&tail));
            assert_eq!(output.len(), 1);

            let missing = PyDict::new(py);
            let rows = PyList::new(py, [&missing]).unwrap();
            let source = rows.as_any().call_method0("__iter__").unwrap();
            let error = select_exact_dict_prefix_v1(
                PyList::empty(py).as_any(),
                &source,
                names.as_any(),
                names.as_any(),
                &selection_error(py),
            )
            .unwrap_err();
            assert!(error.is_instance(py, &selection_error(py)));
            assert!(error.cause(py).unwrap().is_instance_of::<PyKeyError>(py));
        });
    }

    #[test]
    fn drop_nulls_filters_missing_and_none_while_preserving_rows() {
        Python::initialize();
        Python::attach(|py| {
            let first = PyDict::new(py);
            first.set_item("nullable", 1).unwrap();
            let null = PyDict::new(py);
            null.set_item("nullable", py.None()).unwrap();
            let missing = PyDict::new(py);
            let last = PyDict::new(py);
            last.set_item("nullable", 2).unwrap();
            let rows = PyTuple::new(py, [&first, &null, &missing, &last]).unwrap();
            let source = rows.as_any().call_method0("__iter__").unwrap();
            let output = PyList::empty(py);

            let (boundary, completed) = drop_nulls_exact_dict_prefix_v1(
                output.as_any(),
                &source,
                PyString::new(py, "nullable").as_any(),
            )
            .unwrap()
            .unwrap();

            assert!(completed);
            assert!(boundary.is_none());
            assert_eq!(output.len(), 2);
            assert!(output.get_item(0).unwrap().is(&first));
            assert!(output.get_item(1).unwrap().is(&last));
        });
    }

    #[test]
    fn drop_nulls_returns_the_first_non_dict_boundary() {
        Python::initialize();
        Python::attach(|py| {
            let first = PyDict::new(py);
            first.set_item("nullable", 1).unwrap();
            let boundary = PyList::new(py, [2]).unwrap();
            let tail = PyDict::new(py);
            tail.set_item("nullable", 3).unwrap();
            let rows =
                PyList::new(py, [&first.as_any(), &boundary.as_any(), &tail.as_any()]).unwrap();
            let source = rows.as_any().call_method0("__iter__").unwrap();
            let output = PyList::empty(py);

            let (returned, completed) = drop_nulls_exact_dict_prefix_v1(
                output.as_any(),
                &source,
                PyString::new(py, "nullable").as_any(),
            )
            .unwrap()
            .unwrap();

            assert!(!completed);
            assert!(returned.unwrap().bind(py).is(&boundary));
            assert!(source.call_method0("__next__").unwrap().is(&tail));
            assert_eq!(output.len(), 1);
            assert!(output.get_item(0).unwrap().is(&first));
        });
    }

    #[test]
    fn drop_nulls_matches_lookup_exception_boundaries() {
        Python::initialize();
        Python::attach(|py| {
            let fixture = PyModule::from_code(
                py,
                c"class Trap:\n    def __init__(self, error):\n        self.error = error\n    def __hash__(self):\n        return hash('nullable')\n    def __eq__(self, other):\n        raise self.error('lookup failed')\n",
                c"drop_nulls_lookup.py",
                c"drop_nulls_lookup",
            )
            .unwrap();
            let trap_type = fixture.getattr("Trap").unwrap();
            let field = PyString::new(py, "nullable");

            for name in ["AttributeError", "KeyError", "TypeError"] {
                let exception = PyModule::import(py, "builtins")
                    .unwrap()
                    .getattr(name)
                    .unwrap();
                let row = PyDict::new(py);
                row.set_item(trap_type.call1((exception,)).unwrap(), 1)
                    .unwrap();
                let rows = PyList::new(py, [&row]).unwrap();
                let source = rows.as_any().call_method0("__iter__").unwrap();
                let output = PyList::empty(py);
                let result =
                    drop_nulls_exact_dict_prefix_v1(output.as_any(), &source, field.as_any())
                        .unwrap()
                        .unwrap();
                assert!(result.1);
                assert!(output.is_empty());
            }

            for name in ["StopIteration", "ValueError"] {
                let exception = PyModule::import(py, "builtins")
                    .unwrap()
                    .getattr(name)
                    .unwrap();
                let row = PyDict::new(py);
                row.set_item(trap_type.call1((exception,)).unwrap(), 1)
                    .unwrap();
                let rows = PyList::new(py, [&row]).unwrap();
                let source = rows.as_any().call_method0("__iter__").unwrap();
                let error = drop_nulls_exact_dict_prefix_v1(
                    PyList::empty(py).as_any(),
                    &source,
                    field.as_any(),
                )
                .unwrap_err();
                assert_eq!(error.get_type(py).name().unwrap(), name);
            }
        });
    }
}
