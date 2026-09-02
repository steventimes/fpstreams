//! Exact i64 pair-row expression filtering into a caller-owned Python dictionary.

#[cfg(not(Py_GIL_DISABLED))]
use super::expr::prepare_pair_predicate_expression;
use super::prefix::PairPrefix;
#[cfg(not(Py_GIL_DISABLED))]
use super::prefix::{exact_i64_pair_parts, exact_i64_value, insert_borrowed_pair};
#[cfg(not(Py_GIL_DISABLED))]
use crate::common::is_exact_sequence_iterator;
use crate::integer::Instruction;
use pyo3::prelude::*;
#[cfg(not(Py_GIL_DISABLED))]
use pyo3::{ffi, types::PyDict};

/// Filter exact `(i64, i64)` rows and collect the compatible prefix into `output`.
#[pyfunction]
pub(crate) fn pair_i64_row_filter_to_dict_exact_prefix_v1(
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
        let Some(expression) = prepare_pair_predicate_expression(instructions) else {
            return Ok(None);
        };

        let py = output.py();
        let mut stack = expression.stack();
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

            let Some((key, value)) = exact_i64_pair_parts(py, &row)? else {
                return Ok(Some((Some(row.unbind()), false)));
            };
            let Some(key_i64) = exact_i64_value(py, key)? else {
                return Ok(Some((Some(row.unbind()), false)));
            };
            let Some(value_i64) = exact_i64_value(py, value)? else {
                return Ok(Some((Some(row.unbind()), false)));
            };
            let Some(accepted) = expression.evaluate(key_i64, value_i64, &mut stack) else {
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
