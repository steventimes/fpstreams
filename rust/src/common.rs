//! Shared native-kernel errors and compensated statistics for one-pass aggregate terminals.

use pyo3::exceptions::{
    PyAttributeError, PyKeyError, PyMemoryError, PyOverflowError, PyTypeError, PyValueError,
    PyZeroDivisionError,
};
use pyo3::ffi;
use pyo3::prelude::*;
#[cfg(Py_GIL_DISABLED)]
use pyo3::sync::critical_section::with_critical_section;
use pyo3::types::{PyFloat, PyInt, PyList, PySet, PyString, PyTuple};
use std::sync::atomic::{AtomicPtr, Ordering};

// Keep these bit positions synchronized with ``NativeAggregateField`` in
// ``collecting/aggregate_program.py``.  The masked ABI still returns the
// established eight-slot snapshot; unrequested slots contain their empty
// sentinel and must never be read by a finalizer.
pub(crate) const AGGREGATE_COUNT: u8 = 1 << 0;
pub(crate) const AGGREGATE_TOTAL: u8 = 1 << 1;
pub(crate) const AGGREGATE_MINIMUM: u8 = 1 << 2;
pub(crate) const AGGREGATE_MAXIMUM: u8 = 1 << 3;
pub(crate) const AGGREGATE_FIRST: u8 = 1 << 4;
pub(crate) const AGGREGATE_LAST: u8 = 1 << 5;
pub(crate) const AGGREGATE_MEAN: u8 = 1 << 6;
pub(crate) const AGGREGATE_M2: u8 = 1 << 7;

pub(crate) fn validate_aggregate_mask(mask: u8) -> Result<u8, KernelError> {
    if mask == 0 {
        return Err(KernelError::InvalidProgram(
            "native aggregate mask must request at least one field",
        ));
    }
    Ok(mask)
}

/// Report the exact-container ABI used by current wheels.
///
/// Planning checks this positive marker before sending a Python container to
/// native code.  Wheels released before this contract accepted ``Vec``
/// arguments directly, which let PyO3 invoke user-defined numeric conversion
/// protocols while parsing an argument.  A named marker lets new Python code
/// keep those older wheels on the canonical interpreter path instead.
#[pyfunction]
pub(crate) const fn exact_container_extraction_v1() -> bool {
    true
}

/// Prove an exact list contains only exact dictionaries without protocol dispatch.
#[pyfunction]
pub(crate) fn all_exact_dict_rows_v1(source: &Bound<'_, PyAny>) -> PyResult<bool> {
    let Ok(rows) = source.cast_exact::<PyList>() else {
        return Ok(false);
    };

    #[cfg(not(Py_GIL_DISABLED))]
    {
        let row_count = rows.len();
        for index in 0..row_count {
            // SAFETY: an attached GIL build prevents exact-list mutation during this call, and
            // index is below the unchanged list length. PyDict_CheckExact cannot dispatch.
            let row = unsafe { ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) };
            if row.is_null() {
                return Err(PyErr::fetch(source.py()));
            }
            if unsafe { ffi::PyDict_CheckExact(row) } == 0 {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[cfg(Py_GIL_DISABLED)]
    with_critical_section(source, || {
        let row_count = rows.len();
        for index in 0..row_count {
            // SAFETY: the exact list stays locked and index is below its locked length. Take a
            // strong reference before any later API access so a suspended free-threaded critical
            // section cannot turn this list-item borrow into a dangling pointer.
            let row = unsafe { ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) };
            if row.is_null() {
                return Err(PyErr::fetch(source.py()));
            }
            let row = unsafe { Borrowed::from_ptr(source.py(), row).to_owned() };
            if unsafe { ffi::PyDict_CheckExact(row.as_ptr()) } == 0 {
                return Ok(false);
            }
        }
        Ok(true)
    })
}

/// Translate only failures raised while the direct dict lookup callable is active.
fn direct_field_selection_error(
    field: &Bound<'_, PyAny>,
    selection_error_type: &Bound<'_, PyAny>,
    error: PyErr,
) -> PyResult<PyErr> {
    let py = field.py();
    if !error.is_instance_of::<PyAttributeError>(py)
        && !error.is_instance_of::<PyKeyError>(py)
        && !error.is_instance_of::<PyTypeError>(py)
    {
        return Ok(error);
    }

    let prefix = PyString::new(py, "Could not resolve selector ");
    let middle = PyString::new(py, "; failed at ");
    let representation = field.repr()?;
    // SAFETY: every operand is a live Unicode object. PyUnicode_Concat returns a new owned
    // reference or sets an exception, preserving the canonical Python f-string representation.
    let message = unsafe { ffi::PyUnicode_Concat(prefix.as_ptr(), representation.as_ptr()) };
    let message = unsafe { Bound::from_owned_ptr_or_err(py, message)? };
    let message = unsafe { ffi::PyUnicode_Concat(message.as_ptr(), middle.as_ptr()) };
    let message = unsafe { Bound::from_owned_ptr_or_err(py, message)? };
    let message = unsafe { ffi::PyUnicode_Concat(message.as_ptr(), representation.as_ptr()) };
    let message = unsafe { Bound::from_owned_ptr_or_err(py, message)? };
    let translated = PyErr::from_value(selection_error_type.call1((message,))?);
    translated.set_context(py, Some(error.clone_ref(py)));
    translated.set_cause(py, Some(error));
    Ok(translated)
}

/// METH_O key callback: exact-dict lookup failures are translated here, before comparisons begin.
unsafe fn direct_dict_field_key_call(
    py: Python<'_>,
    state: *mut ffi::PyObject,
    row: *mut ffi::PyObject,
) -> PyResult<*mut ffi::PyObject> {
    // SAFETY: PyCFunction owns its exact two-item tuple self for the full call.
    let field = unsafe { ffi::PyTuple_GetItem(state, 0) };
    if field.is_null() {
        return Err(PyErr::fetch(py));
    }
    // SAFETY: the state tuple and Python call keep these borrowed objects live. The caller uses
    // this key only after proving an exact dict, and PyDict_GetItemRef returns an owned reference
    // while providing the dict's synchronization on no-GIL builds.
    let field = unsafe { Borrowed::from_ptr(py, field) };
    let mut selected = core::ptr::null_mut();
    let status = unsafe { ffi::compat::PyDict_GetItemRef(row, field.as_ptr(), &mut selected) };
    let error = match status {
        1..=core::ffi::c_int::MAX => return Ok(selected),
        0 => PyKeyError::new_err(field.to_owned().unbind()),
        core::ffi::c_int::MIN..=-1 => PyErr::fetch(py),
    };
    // The error class is cold-path state: avoid a second tuple lookup for every successful row.
    let selection_error_type = unsafe { ffi::PyTuple_GetItem(state, 1) };
    if selection_error_type.is_null() {
        return Err(PyErr::fetch(py));
    }
    let selection_error_type = unsafe { Borrowed::from_ptr(py, selection_error_type) };
    Err(direct_field_selection_error(
        field.as_any(),
        selection_error_type.as_any(),
        error,
    )?)
}

// PyCFunction retains its method-definition pointer for the callable's lifetime. Allocate one
// immutable process-lifetime definition through Python's fallible allocator; AtomicPtr avoids a
// mutable Rust static and makes concurrent free-threaded first use race-free.
static DIRECT_DICT_FIELD_KEY_METHOD: AtomicPtr<ffi::PyMethodDef> =
    AtomicPtr::new(core::ptr::null_mut());

fn direct_dict_field_key_method() -> PyResult<*mut ffi::PyMethodDef> {
    let retained = DIRECT_DICT_FIELD_KEY_METHOD.load(Ordering::Acquire);
    if !retained.is_null() {
        return Ok(retained);
    }
    // SAFETY: PyMem_Malloc returns storage suitably aligned for any Python/C object, or null.
    let candidate =
        unsafe { ffi::PyMem_Malloc(size_of::<ffi::PyMethodDef>()) }.cast::<ffi::PyMethodDef>();
    if candidate.is_null() {
        return Err(PyMemoryError::new_err(
            "could not allocate native direct-field method definition",
        ));
    }
    // SAFETY: candidate points to writable storage for exactly one PyMethodDef.
    unsafe {
        candidate.write(ffi::PyMethodDef {
            ml_name: c"_fpstreams_direct_dict_field_key".as_ptr(),
            ml_meth: ffi::PyMethodDefPointer {
                PyCFunction: pyo3::get_trampoline_function!(binaryfunc, direct_dict_field_key_call),
            },
            ml_flags: ffi::METH_O,
            ml_doc: core::ptr::null(),
        });
    }
    match DIRECT_DICT_FIELD_KEY_METHOD.compare_exchange(
        core::ptr::null_mut(),
        candidate,
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        Ok(_) => Ok(candidate),
        Err(retained) => {
            // SAFETY: this losing candidate was never published or passed to Python.
            unsafe { ffi::PyMem_Free(candidate.cast()) };
            Ok(retained)
        }
    }
}

/// Build the direct exact-dict key callable used only after all_exact_dict_rows_v1 succeeds.
#[pyfunction]
pub(crate) fn direct_dict_field_key_v1(
    py: Python<'_>,
    field: &Bound<'_, PyAny>,
    selection_error_type: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let state = PyTuple::new(py, [field, selection_error_type])?;
    let method = direct_dict_field_key_method()?;
    // SAFETY: the static method definition matches METH_O, and PyCFunction_NewEx takes a strong
    // reference to the exact tuple that owns field and the canonical SelectionError class.
    let callable = unsafe { ffi::PyCFunction_NewEx(method, state.as_ptr(), core::ptr::null_mut()) };
    // SAFETY: a successful PyCFunction_NewEx returns one owned reference.
    Ok(unsafe { Bound::from_owned_ptr_or_err(py, callable)? }.unbind())
}

fn extract_exact_i64_item(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    if !value.is_exact_instance_of::<PyInt>() {
        return Err(PyTypeError::new_err(
            "native i64 containers require exact integers",
        ));
    }
    // Exact PyInt extraction preserves PyO3's OverflowError for values
    // outside signed i64 without permitting subclass protocol dispatch.
    value.extract()
}

fn extract_exact_f64_item(value: &Bound<'_, PyAny>, allow_integers: bool) -> PyResult<f64> {
    let exact_float = value.is_exact_instance_of::<PyFloat>();
    let permitted_integer = allow_integers && value.is_exact_instance_of::<PyInt>();
    if !(exact_float || permitted_integer) {
        return Err(PyTypeError::new_err(if allow_integers {
            "native f64 containers require exact floats or integers"
        } else {
            "native f64 containers require exact floats"
        }));
    }
    // PyFloat_AsDouble also raises OverflowError for an exact integer that
    // cannot be represented as f64; exact-type validation prevents hooks.
    value.extract()
}

/// Copy an exact built-in integer container without invoking ``__index__``.
///
/// The full snapshot is deliberately taken while the GIL is held.  Exact
/// container and item checks make every indexed read non-reentrant; only after
/// the owned Rust vector exists may the compute-heavy kernel detach.
pub(crate) fn extract_i64_container(values: &Bound<'_, PyAny>) -> PyResult<Vec<i64>> {
    if values.is_exact_instance_of::<PyList>() {
        let list = values.cast::<PyList>()?;
        let mut output = Vec::with_capacity(list.len());
        for value in list.iter() {
            output.push(extract_exact_i64_item(&value)?);
        }
        return Ok(output);
    }
    if values.is_exact_instance_of::<PyTuple>() {
        let tuple = values.cast::<PyTuple>()?;
        let mut output = Vec::with_capacity(tuple.len());
        for value in tuple.iter() {
            output.push(extract_exact_i64_item(&value)?);
        }
        return Ok(output);
    }
    Err(PyTypeError::new_err(
        "native numeric sources require an exact list or tuple",
    ))
}

/// Copy an exact built-in floating container without invoking ``__float__``.
///
/// Integer inputs are legal only when a leading fitem map is the first stage
/// that evaluates source values.  Predicate-first and identity programs must
/// retain Python's original element type, so they accept exact floats only.
pub(crate) fn extract_f64_container(
    values: &Bound<'_, PyAny>,
    allow_integers: bool,
) -> PyResult<Vec<f64>> {
    if values.is_exact_instance_of::<PyList>() {
        let list = values.cast::<PyList>()?;
        let mut output = Vec::with_capacity(list.len());
        for value in list.iter() {
            output.push(extract_exact_f64_item(&value, allow_integers)?);
        }
        return Ok(output);
    }
    if values.is_exact_instance_of::<PyTuple>() {
        let tuple = values.cast::<PyTuple>()?;
        let mut output = Vec::with_capacity(tuple.len());
        for value in tuple.iter() {
            output.push(extract_exact_f64_item(&value, allow_integers)?);
        }
        return Ok(output);
    }
    Err(PyTypeError::new_err(
        "native numeric sources require an exact list or tuple",
    ))
}

/// Retain a bounded prefix from an exact built-in sequence before item conversion.
///
/// Owning every selected item first prevents a conversion hook from invalidating a later
/// borrowed list slot. Integer and float probes deliberately share this exact snapshot rule.
#[inline]
pub(crate) fn snapshot_exact_container_prefix(
    values: &Bound<'_, PyAny>,
    max_items: usize,
) -> PyResult<(Vec<Py<PyAny>>, bool)> {
    let (length, items) = if values.is_exact_instance_of::<PyList>() {
        let container = values.cast::<PyList>()?;
        let length = container.len();
        let count = length.min(max_items);
        let mut items = Vec::with_capacity(count);
        for index in 0..count {
            items.push(container.get_item(index)?.unbind());
        }
        (length, items)
    } else if values.is_exact_instance_of::<PyTuple>() {
        let container = values.cast::<PyTuple>()?;
        let length = container.len();
        let count = length.min(max_items);
        let mut items = Vec::with_capacity(count);
        for index in 0..count {
            items.push(container.get_item(index)?.unbind());
        }
        (length, items)
    } else {
        return Err(PyTypeError::new_err(
            "native container probes require an exact list or tuple",
        ));
    };
    Ok((items, length <= max_items))
}

#[derive(Debug)]
pub(crate) enum KernelError {
    DivisionByZero,
    InvalidProgram(&'static str),
    Overflow,
}

/// Neumaier-style compensation for a masked float total.
///
/// Keeping this state separate lets a total-only aggregate avoid the variance
/// arithmetic in ``OnlineStatistics`` while matching its established handling
/// of finite and non-finite inputs.
#[derive(Default)]
pub(crate) struct CompensatedSum {
    total: f64,
    compensation: f64,
}

impl CompensatedSum {
    #[inline]
    pub(crate) fn accept(&mut self, value: f64) {
        let combined = self.total + value;
        if self.total.is_finite() && value.is_finite() && combined.is_finite() {
            self.compensation += if self.total.abs() >= value.abs() {
                self.total - combined + value
            } else {
                value - combined + self.total
            };
        } else {
            self.compensation = 0.0;
        }
        self.total = combined;
    }

    #[inline]
    pub(crate) fn value(&self) -> f64 {
        self.total + self.compensation
    }
}

#[derive(Default)]
pub(crate) struct OnlineStatistics {
    count: u64,
    total: f64,
    compensation: f64,
    rolling_mean: f64,
    squared_deviations: f64,
}

impl OnlineStatistics {
    pub(crate) fn accept(&mut self, value: f64) -> Result<(), KernelError> {
        self.count = self.count.checked_add(1).ok_or(KernelError::Overflow)?;

        let combined = self.total + value;
        if self.total.is_finite() && value.is_finite() && combined.is_finite() {
            self.compensation += if self.total.abs() >= value.abs() {
                self.total - combined + value
            } else {
                value - combined + self.total
            };
        } else {
            self.compensation = 0.0;
        }
        self.total = combined;

        let delta = value - self.rolling_mean;
        self.rolling_mean += delta / (self.count as f64);
        self.squared_deviations += delta * (value - self.rolling_mean);
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> (u64, f64, f64) {
        let mean = if self.count == 0 {
            0.0
        } else {
            (self.total + self.compensation) / (self.count as f64)
        };
        (self.count, mean, self.squared_deviations)
    }

    pub(crate) fn sum(&self) -> f64 {
        self.total + self.compensation
    }
}

pub(crate) fn kernel_error(error: KernelError) -> PyErr {
    match error {
        KernelError::DivisionByZero => PyZeroDivisionError::new_err("integer division by zero"),
        KernelError::InvalidProgram(message) => PyValueError::new_err(message),
        KernelError::Overflow => PyOverflowError::new_err("native i64 expression overflowed"),
    }
}

/// The three public collection terminals that can be built after detached execution.
pub(crate) enum MaterializeTarget {
    List,
    Tuple,
    Set,
}

/// Validate the target before a materializer reads or converts its source.
pub(crate) fn materialize_target(target: u8) -> PyResult<MaterializeTarget> {
    match target {
        0 => Ok(MaterializeTarget::List),
        1 => Ok(MaterializeTarget::Tuple),
        2 => Ok(MaterializeTarget::Set),
        _ => Err(PyValueError::new_err("unknown native materialize target")),
    }
}

/// Build exactly one requested Python container from detached numeric output.
pub(crate) fn materialize_values<'py, T>(
    py: Python<'py>,
    values: Vec<T>,
    target: MaterializeTarget,
) -> PyResult<Py<PyAny>>
where
    T: IntoPyObject<'py>,
{
    match target {
        MaterializeTarget::List => Ok(PyList::new(py, values)?.into_any().unbind()),
        MaterializeTarget::Tuple => Ok(PyTuple::new(py, values)?.into_any().unbind()),
        MaterializeTarget::Set => Ok(PySet::new(py, values)?.into_any().unbind()),
    }
}
