//! Streaming exact-dictionary unnest with a lossless Python fallback boundary.

#[cfg(not(Py_GIL_DISABLED))]
use crate::common::{exact_dict_has_only_string_keys_interruptible, is_exact_sequence_iterator};

use pyo3::prelude::*;
#[cfg(not(Py_GIL_DISABLED))]
use pyo3::{
    ffi,
    types::{PyList, PyString, PyTuple},
};

type UnnestPrefix = (Option<Py<PyAny>>, bool);

#[cfg(not(Py_GIL_DISABLED))]
const UNNEST_WIDE_COPY_MAX_CODE_POINTS: isize = 32;

#[cfg(not(Py_GIL_DISABLED))]
struct ExactUnnestRow {
    nested: *mut ffi::PyObject,
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn concatenate_fstring_parts(
    prefix: &Bound<'_, PyAny>,
    name: *mut ffi::PyObject,
) -> PyResult<Py<PyAny>> {
    let py = prefix.py();
    // SAFETY: both operands are live exact strings and these length queries cannot dispatch.
    let prefix_length = unsafe { ffi::PyUnicode_GetLength(prefix.as_ptr()) };
    if prefix_length < 0 {
        return Err(PyErr::fetch(py));
    }
    let name_length = unsafe { ffi::PyUnicode_GetLength(name) };
    if name_length < 0 {
        return Err(PyErr::fetch(py));
    }

    let target = if (prefix_length == 0) != (name_length == 0) {
        // BUILD_STRING mints a distinct result when exactly one f-string part is empty, while
        // PyUnicode_Concat and PyUnicode_FromFormat may return their non-empty operand. A stable-
        // ABI wide-character round trip copies every code point, including lone surrogates,
        // without passing through UTF-8. CPython caches one-character results created by
        // PyUnicode_FromWideChar, so retain PyUnicode_Join for that narrow identity boundary.
        // Join also scales better once the copied payload is no longer small.
        let nonempty = if prefix_length == 0 {
            name
        } else {
            prefix.as_ptr()
        };
        let nonempty_length = prefix_length.max(name_length);
        if nonempty_length == 1 || nonempty_length > UNNEST_WIDE_COPY_MAX_CODE_POINTS {
            let separator = PyString::new(py, "");
            // SAFETY: name is a live exact string borrowed from the live nested dictionary.
            let name = unsafe { Bound::from_borrowed_ptr(py, name) };
            let parts = PyTuple::new(py, [prefix.as_any(), &name])?;
            // SAFETY: separator and both tuple entries are live exact Unicode objects.
            unsafe { ffi::PyUnicode_Join(separator.as_ptr(), parts.as_ptr()) }
        } else {
            let mut wide_length = 0;
            // SAFETY: nonempty is a live exact Unicode object. The returned buffer is allocated
            // with PyMem_Malloc and remains valid until the matching PyMem_Free below.
            let wide = unsafe { ffi::PyUnicode_AsWideCharString(nonempty, &mut wide_length) };
            if wide.is_null() {
                return Err(PyErr::fetch(py));
            }
            // SAFETY: wide contains exactly wide_length initialized wchar_t values. FromWideChar
            // copies them into a fresh exact Unicode object before the buffer is released.
            let target = unsafe { ffi::PyUnicode_FromWideChar(wide, wide_length) };
            unsafe { ffi::PyMem_Free(wide.cast()) };
            target
        }
    } else {
        // SAFETY: both operands are live exact Unicode objects.
        unsafe { ffi::PyUnicode_Concat(prefix.as_ptr(), name) }
    };
    // SAFETY: both constructors return one owned reference or set an exception.
    Ok(unsafe { Bound::from_owned_ptr_or_err(py, target)? }.unbind())
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn probe_exact_unnest_row(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    column: *mut ffi::PyObject,
) -> PyResult<Option<ExactUnnestRow>> {
    // Prove the complete outer key set before lookup so replaying an incompatible row cannot
    // repeat custom hash/equality protocols.
    if unsafe { ffi::PyDict_CheckExact(row) } == 0
        || !exact_dict_has_only_string_keys_interruptible(py, row)?
    {
        return Ok(None);
    }

    // SAFETY: row is exact and column is an exact string. A null result without an exception is
    // the missing-column boundary that Python must translate canonically.
    let nested = unsafe { ffi::PyDict_GetItemWithError(row, column) };
    if nested.is_null() {
        if unsafe { !ffi::PyErr_Occurred().is_null() } {
            return Err(PyErr::fetch(py));
        }
        return Ok(None);
    }
    if unsafe { ffi::PyDict_CheckExact(nested) } == 0 {
        return Ok(None);
    }

    Ok(Some(ExactUnnestRow { nested }))
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn append_probed_exact_unnest_row(
    py: Python<'_>,
    output: *mut ffi::PyObject,
    row: *mut ffi::PyObject,
    nested: *mut ffi::PyObject,
    column: *mut ffi::PyObject,
    prefix: &Bound<'_, PyAny>,
) -> PyResult<bool> {
    // Build privately and append only after the whole nested row is known compatible. This
    // keeps every row atomic if allocation, signal handling, or collision fallback interrupts it.
    let record = unsafe { Bound::from_owned_ptr_or_err(py, ffi::PyDict_Copy(row))? };
    // SAFETY: the preceding lookup proved column exists in this private exact copy.
    if unsafe { ffi::PyDict_DelItem(record.as_ptr(), column) } != 0 {
        return Err(PyErr::fetch(py));
    }

    // SAFETY: the attached GIL keeps the exact nested dictionary stable for this non-callback
    // loop. PyDict_Next returns borrowed entries and never invokes key protocols.
    let nested_size = unsafe { ffi::PyDict_Size(nested) };
    if nested_size < 0 {
        return Err(PyErr::fetch(py));
    }
    let mut position = 0;
    let mut name = core::ptr::null_mut();
    let mut value = core::ptr::null_mut();
    for field_index in 0..nested_size {
        if field_index != 0 && field_index % 4096 == 0 {
            py.check_signals()?;
        }
        if unsafe { ffi::PyDict_Next(nested, &mut position, &mut name, &mut value) } == 0 {
            return Ok(false);
        }
        if unsafe { ffi::PyUnicode_CheckExact(name) } == 0 {
            return Ok(false);
        }
        let target = concatenate_fstring_parts(prefix, name)?;
        // SAFETY: record is exact and private, and target is an exact generated string.
        let collision = unsafe { ffi::PyDict_Contains(record.as_ptr(), target.bind(py).as_ptr()) };
        if collision < 0 {
            return Err(PyErr::fetch(py));
        }
        if collision != 0 {
            return Ok(false);
        }
        // SAFETY: record is private; target and value are live, and PyDict_SetItem retains them.
        if unsafe { ffi::PyDict_SetItem(record.as_ptr(), target.bind(py).as_ptr(), value) } != 0 {
            return Err(PyErr::fetch(py));
        }
    }

    // SAFETY: output is an exact list and PyList_Append retains its own reference.
    if unsafe { ffi::PyList_Append(output, record.as_ptr()) } != 0 {
        return Err(PyErr::fetch(py));
    }
    Ok(true)
}

/// Append the compatible exact-dictionary prefix of one unnest transform.
///
/// The first noncanonical, missing-column, nested-record, or colliding row is returned
/// unprocessed so Python can resume with its public conversion and exception semantics.
#[pyfunction]
pub(crate) fn unnest_exact_dict_prefix_v1(
    output: &Bound<'_, PyAny>,
    source: &Bound<'_, PyAny>,
    column: &Bound<'_, PyAny>,
    prefix: &Bound<'_, PyAny>,
) -> PyResult<Option<UnnestPrefix>> {
    #[cfg(Py_GIL_DISABLED)]
    {
        let _ = (output, source, column, prefix);
        Ok(None)
    }

    #[cfg(not(Py_GIL_DISABLED))]
    {
        if output.cast_exact::<PyList>().is_err()
            || unsafe { ffi::PyUnicode_CheckExact(column.as_ptr()) } == 0
            || unsafe { ffi::PyUnicode_CheckExact(prefix.as_ptr()) } == 0
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
            // Retain the previous target until the next pull, matching a Python for-loop's
            // target lifetime at the source boundary.
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

            let Some(probed) = probe_exact_unnest_row(py, row.as_ptr(), column.as_ptr())? else {
                return Ok(Some((Some(row.unbind()), false)));
            };
            if !append_probed_exact_unnest_row(
                py,
                output.as_ptr(),
                row.as_ptr(),
                probed.nested,
                column.as_ptr(),
                prefix,
            )? {
                return Ok(Some((Some(row.unbind()), false)));
            }
            previous = Some(row.unbind());
            rows_since_signal_check = rows_since_signal_check.wrapping_add(1) & 4095;
        }
    }
}
