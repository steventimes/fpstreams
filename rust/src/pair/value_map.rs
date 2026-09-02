//! Exact-pair scalar-expression mapping into a caller-owned Python dictionary.

use super::prefix::PairPrefix;
#[cfg(not(Py_GIL_DISABLED))]
use super::prefix::{
    exact_f64_value, exact_i64_value, exact_pair_key_is_supported, exact_pair_parts,
};
#[cfg(not(Py_GIL_DISABLED))]
use crate::common::is_exact_sequence_iterator;
use crate::float::FloatInstruction;
#[cfg(not(Py_GIL_DISABLED))]
use crate::float::prepare_float_expression;
use crate::integer::Instruction;
#[cfg(not(Py_GIL_DISABLED))]
use crate::integer::prepare_expression;
use pyo3::prelude::*;
#[cfg(not(Py_GIL_DISABLED))]
use pyo3::{
    ffi,
    types::{PyBool, PyDict, PyDictMethods, PyFloat, PyInt},
};

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn insert_mapped(
    output: &Bound<'_, PyDict>,
    key: Borrowed<'_, '_, PyAny>,
    mapped: &Bound<'_, PyAny>,
    keep_first: bool,
) -> PyResult<()> {
    if keep_first {
        output.set_default(key, mapped)?;
    } else {
        output.set_item(key, mapped)?;
    }
    Ok(())
}

/// Map exact i64 pair values and collect the compatible prefix into `output`.
#[pyfunction]
pub(crate) fn pair_i64_value_map_to_dict_exact_prefix_v1(
    output: &Bound<'_, PyAny>,
    source: &Bound<'_, PyAny>,
    instructions: Vec<Instruction>,
    keep_first: bool,
    result_is_bool: bool,
) -> PyResult<Option<PairPrefix>> {
    #[cfg(Py_GIL_DISABLED)]
    {
        let _ = (output, source, instructions, keep_first, result_is_bool);
        Ok(None)
    }

    #[cfg(not(Py_GIL_DISABLED))]
    {
        let Ok(output) = output.cast_exact::<PyDict>() else {
            return Ok(None);
        };
        if instructions.is_empty() || !is_exact_sequence_iterator(output.py(), source)? {
            return Ok(None);
        }
        let identity = instructions.as_slice() == [(0, 0)];
        let py = output.py();
        let expression = prepare_expression(instructions);
        let mut stack = Vec::new();
        let mut rows_since_signal_check = 0_u16;

        loop {
            if rows_since_signal_check == 0 {
                py.check_signals()?;
            }
            // SAFETY: the validated exact iterator returns one owned reference or a clean end.
            let row = unsafe { ffi::PyIter_Next(source.as_ptr()) };
            if row.is_null() {
                if unsafe { !ffi::PyErr_Occurred().is_null() } {
                    return Err(PyErr::fetch(py));
                }
                return Ok(Some((None, true)));
            }
            // SAFETY: PyIter_Next returned one owned reference.
            let row = unsafe { Bound::from_owned_ptr(py, row) };
            let Some((key, value)) = exact_pair_parts(py, &row)? else {
                return Ok(Some((Some(row.unbind()), false)));
            };
            if !exact_pair_key_is_supported(key) {
                return Ok(Some((Some(row.unbind()), false)));
            }
            let Some(value_i64) = exact_i64_value(py, value)? else {
                return Ok(Some((Some(row.unbind()), false)));
            };
            let Ok(mapped) = expression.evaluate(value_i64, &mut stack) else {
                return Ok(Some((Some(row.unbind()), false)));
            };
            if identity && !result_is_bool {
                insert_mapped(output, key, value.as_any(), keep_first)?;
            } else if result_is_bool {
                insert_mapped(
                    output,
                    key,
                    PyBool::new(py, mapped != 0).as_any(),
                    keep_first,
                )?;
            } else {
                insert_mapped(output, key, PyInt::new(py, mapped).as_any(), keep_first)?;
            }
            rows_since_signal_check = rows_since_signal_check.wrapping_add(1) & 4095;
        }
    }
}

/// Map exact f64-compatible pair values and collect the compatible prefix into `output`.
#[pyfunction]
pub(crate) fn pair_f64_value_map_to_dict_exact_prefix_v1(
    output: &Bound<'_, PyAny>,
    source: &Bound<'_, PyAny>,
    instructions: Vec<FloatInstruction>,
    keep_first: bool,
    result_is_bool: bool,
) -> PyResult<Option<PairPrefix>> {
    #[cfg(Py_GIL_DISABLED)]
    {
        let _ = (output, source, instructions, keep_first, result_is_bool);
        Ok(None)
    }

    #[cfg(not(Py_GIL_DISABLED))]
    {
        let Ok(output) = output.cast_exact::<PyDict>() else {
            return Ok(None);
        };
        if instructions.is_empty() || !is_exact_sequence_iterator(output.py(), source)? {
            return Ok(None);
        }
        let identity = instructions.as_slice() == [(0, 0.0)];
        let py = output.py();
        let expression = prepare_float_expression(instructions);
        let mut stack = Vec::new();
        let mut rows_since_signal_check = 0_u16;

        loop {
            if rows_since_signal_check == 0 {
                py.check_signals()?;
            }
            // SAFETY: the validated exact iterator returns one owned reference or a clean end.
            let row = unsafe { ffi::PyIter_Next(source.as_ptr()) };
            if row.is_null() {
                if unsafe { !ffi::PyErr_Occurred().is_null() } {
                    return Err(PyErr::fetch(py));
                }
                return Ok(Some((None, true)));
            }
            // SAFETY: PyIter_Next returned one owned reference.
            let row = unsafe { Bound::from_owned_ptr(py, row) };
            let Some((key, value)) = exact_pair_parts(py, &row)? else {
                return Ok(Some((Some(row.unbind()), false)));
            };
            if !exact_pair_key_is_supported(key) {
                return Ok(Some((Some(row.unbind()), false)));
            }
            let Some(value_f64) = exact_f64_value(py, value)? else {
                return Ok(Some((Some(row.unbind()), false)));
            };
            let Ok(mapped) = expression.evaluate(value_f64, &mut stack) else {
                return Ok(Some((Some(row.unbind()), false)));
            };
            if identity
                && !result_is_bool
                && unsafe { ffi::PyFloat_CheckExact(value.as_ptr()) } != 0
            {
                insert_mapped(output, key, value.as_any(), keep_first)?;
            } else if result_is_bool {
                insert_mapped(
                    output,
                    key,
                    PyBool::new(py, mapped != 0.0).as_any(),
                    keep_first,
                )?;
            } else {
                insert_mapped(output, key, PyFloat::new(py, mapped).as_any(), keep_first)?;
            }
            rows_since_signal_check = rows_since_signal_check.wrapping_add(1) & 4095;
        }
    }
}
