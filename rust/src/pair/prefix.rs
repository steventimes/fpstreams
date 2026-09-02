//! Shared structural proofs for exact-pair prefix kernels.

use pyo3::prelude::*;
#[cfg(not(Py_GIL_DISABLED))]
use pyo3::{
    exceptions::PyOverflowError,
    ffi,
    types::{PyDict, PyDictMethods},
};

pub(crate) type PairPrefix = (Option<Py<PyAny>>, bool);

/// Borrow the two fields of an exact pair without invoking sequence protocols.
#[cfg(not(Py_GIL_DISABLED))]
#[inline]
pub(crate) fn exact_pair_parts<'a, 'py>(
    py: Python<'py>,
    row: &'a Bound<'py, PyAny>,
) -> PyResult<Option<(Borrowed<'a, 'py, PyAny>, Borrowed<'a, 'py, PyAny>)>> {
    if unsafe { ffi::PyTuple_CheckExact(row.as_ptr()) } == 0 {
        return Ok(None);
    }
    let width = unsafe { ffi::PyTuple_Size(row.as_ptr()) };
    if width < 0 {
        return Err(PyErr::fetch(py));
    }
    if width != 2 {
        return Ok(None);
    }
    // SAFETY: row is an exact two-item tuple kept live by its owned Bound.
    let key = unsafe { ffi::PyTuple_GetItem(row.as_ptr(), 0) };
    let value = unsafe { ffi::PyTuple_GetItem(row.as_ptr(), 1) };
    if key.is_null() || value.is_null() {
        return Err(PyErr::fetch(py));
    }
    // SAFETY: both items remain owned by the live row tuple for this iteration.
    Ok(Some(unsafe {
        (Borrowed::from_ptr(py, key), Borrowed::from_ptr(py, value))
    }))
}

/// Borrow an exact pair only when both fields are exact Python integers.
#[cfg(not(Py_GIL_DISABLED))]
#[inline]
pub(crate) fn exact_i64_pair_parts<'a, 'py>(
    py: Python<'py>,
    row: &'a Bound<'py, PyAny>,
) -> PyResult<Option<(Borrowed<'a, 'py, PyAny>, Borrowed<'a, 'py, PyAny>)>> {
    let Some((key, value)) = exact_pair_parts(py, row)? else {
        return Ok(None);
    };
    if unsafe { ffi::PyLong_CheckExact(key.as_ptr()) } == 0
        || unsafe { ffi::PyLong_CheckExact(value.as_ptr()) } == 0
    {
        return Ok(None);
    }
    Ok(Some((key, value)))
}

/// Return whether a pair key is safe to hash without invoking Python protocols.
#[cfg(not(Py_GIL_DISABLED))]
#[inline]
pub(crate) fn exact_pair_key_is_supported(key: Borrowed<'_, '_, PyAny>) -> bool {
    // SAFETY: key is a live tuple borrow and exact-type checks cannot dispatch Python code.
    unsafe {
        ffi::PyLong_CheckExact(key.as_ptr()) != 0 || ffi::PyUnicode_CheckExact(key.as_ptr()) != 0
    }
}

/// Extract an exact integer, treating arbitrary precision as a Python fallback boundary.
#[cfg(not(Py_GIL_DISABLED))]
#[inline]
pub(crate) fn exact_i64_value(
    py: Python<'_>,
    value: Borrowed<'_, '_, PyAny>,
) -> PyResult<Option<i64>> {
    if unsafe { ffi::PyLong_CheckExact(value.as_ptr()) } == 0 {
        return Ok(None);
    }
    // SAFETY: exact integers cannot dispatch conversion hooks.
    let extracted = unsafe { ffi::PyLong_AsLongLong(value.as_ptr()) };
    if extracted == -1 && unsafe { !ffi::PyErr_Occurred().is_null() } {
        let error = PyErr::fetch(py);
        if error.is_instance_of::<PyOverflowError>(py) {
            return Ok(None);
        }
        return Err(error);
    }
    Ok(Some(extracted))
}

/// Extract an exact float or integer, treating oversized integers as a Python boundary.
#[cfg(not(Py_GIL_DISABLED))]
#[inline]
pub(crate) fn exact_f64_value(
    py: Python<'_>,
    value: Borrowed<'_, '_, PyAny>,
) -> PyResult<Option<f64>> {
    if unsafe { ffi::PyFloat_CheckExact(value.as_ptr()) } == 0
        && unsafe { ffi::PyLong_CheckExact(value.as_ptr()) } == 0
    {
        return Ok(None);
    }
    // SAFETY: exact-type validation excludes conversion hooks.
    let extracted = unsafe { ffi::PyFloat_AsDouble(value.as_ptr()) };
    if unsafe { !ffi::PyErr_Occurred().is_null() } {
        let error = PyErr::fetch(py);
        if error.is_instance_of::<PyOverflowError>(py) {
            return Ok(None);
        }
        return Err(error);
    }
    Ok(Some(extracted))
}

/// Insert an unchanged exact pair using the caller's first- or last-wins policy.
#[cfg(not(Py_GIL_DISABLED))]
#[inline]
pub(crate) fn insert_borrowed_pair(
    output: &Bound<'_, PyDict>,
    key: Borrowed<'_, '_, PyAny>,
    value: Borrowed<'_, '_, PyAny>,
    keep_first: bool,
) -> PyResult<()> {
    if keep_first {
        output.set_default(key, value)?;
    } else {
        output.set_item(key, value)?;
    }
    Ok(())
}
