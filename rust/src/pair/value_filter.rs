//! Exact-pair value-expression filtering into a caller-owned Python dictionary.

use super::prefix::PairPrefix;
#[cfg(not(Py_GIL_DISABLED))]
use super::prefix::{
    exact_f64_value, exact_i64_value, exact_pair_key_is_supported, exact_pair_parts,
    insert_borrowed_pair,
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
use pyo3::{ffi, types::PyDict};

/// Filter exact i64 pair values and collect the compatible prefix into `output`.
#[pyfunction]
pub(crate) fn pair_i64_value_filter_to_dict_exact_prefix_v1(
    output: &Bound<'_, PyAny>,
    source: &Bound<'_, PyAny>,
    instructions: Vec<Instruction>,
    keep_first: bool,
) -> PyResult<Option<PairPrefix>> {
    #[cfg(Py_GIL_DISABLED)]
    {
        let _ = (output, source, instructions, keep_first);
        Ok(None)
    }

    #[cfg(not(Py_GIL_DISABLED))]
    {
        let Ok(output) = output.cast_exact::<PyDict>() else {
            return Ok(None);
        };
        if !is_exact_sequence_iterator(output.py(), source)? {
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

            let Some((key, value)) = exact_pair_parts(py, &row)? else {
                return Ok(Some((Some(row.unbind()), false)));
            };
            if !exact_pair_key_is_supported(key) {
                return Ok(Some((Some(row.unbind()), false)));
            }
            let Some(value_i64) = exact_i64_value(py, value)? else {
                return Ok(Some((Some(row.unbind()), false)));
            };
            let Ok(accepted) = expression.evaluate(value_i64, &mut stack) else {
                return Ok(Some((Some(row.unbind()), false)));
            };
            if accepted != 0 {
                insert_borrowed_pair(output, key, value, keep_first)?;
            }
            previous = Some(row.unbind());
            rows_since_signal_check = rows_since_signal_check.wrapping_add(1) & 4095;
        }
    }
}

/// Filter exact f64-compatible pair values and collect the compatible prefix into `output`.
#[pyfunction]
pub(crate) fn pair_f64_value_filter_to_dict_exact_prefix_v1(
    output: &Bound<'_, PyAny>,
    source: &Bound<'_, PyAny>,
    instructions: Vec<FloatInstruction>,
    keep_first: bool,
) -> PyResult<Option<PairPrefix>> {
    #[cfg(Py_GIL_DISABLED)]
    {
        let _ = (output, source, instructions, keep_first);
        Ok(None)
    }

    #[cfg(not(Py_GIL_DISABLED))]
    {
        let Ok(output) = output.cast_exact::<PyDict>() else {
            return Ok(None);
        };
        if !is_exact_sequence_iterator(output.py(), source)? {
            return Ok(None);
        }
        let py = output.py();
        let expression = prepare_float_expression(instructions);
        let mut stack = Vec::new();
        let mut previous: Option<Py<PyAny>> = None;
        let mut rows_since_signal_check = 0_u16;

        loop {
            if rows_since_signal_check == 0 {
                py.check_signals()?;
            }
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

            let Some((key, value)) = exact_pair_parts(py, &row)? else {
                return Ok(Some((Some(row.unbind()), false)));
            };
            if !exact_pair_key_is_supported(key) {
                return Ok(Some((Some(row.unbind()), false)));
            }
            let Some(value_f64) = exact_f64_value(py, value)? else {
                return Ok(Some((Some(row.unbind()), false)));
            };
            let Ok(accepted) = expression.evaluate(value_f64, &mut stack) else {
                return Ok(Some((Some(row.unbind()), false)));
            };
            if accepted != 0.0 {
                insert_borrowed_pair(output, key, value, keep_first)?;
            }
            previous = Some(row.unbind());
            rows_since_signal_check = rows_since_signal_check.wrapping_add(1) & 4095;
        }
    }
}
