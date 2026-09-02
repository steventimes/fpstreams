//! Python-facing adapters for floating-point kernels.

use super::*;
#[cfg(not(Py_GIL_DISABLED))]
use crate::common::acquire_f64_buffer;
use crate::common::{
    extract_f64_buffer, extract_f64_container, kernel_error, materialize_target,
    materialize_values, snapshot_exact_container_prefix,
};
use crate::integer::I64Range;
use crate::numeric_mean::mean_f64_container;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyFloat, PyInt};

#[pyfunction]
pub(crate) fn execute_f64(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: FloatProgram,
) -> PyResult<Vec<f64>> {
    let allow_integers = source_allows_integer_conversion(&program);
    let values = extract_f64_container(values, allow_integers)?;
    py.detach(move || run_f64(values, program))
        .map_err(kernel_error)
}

/// Promise that aggregate total slots use Python collector-compatible sequential addition.
#[pyfunction]
pub(crate) fn sequential_f64_aggregate_total_v1() -> bool {
    true
}

/// Snapshot an external f64 buffer before running a fused pipeline.
#[pyfunction]
pub(crate) fn execute_f64_buffer_v1(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: FloatProgram,
) -> PyResult<Vec<f64>> {
    let values = extract_f64_buffer(values)?;
    py.detach(move || run_f64(values, program))
        .map_err(kernel_error)
}

#[pyfunction]
pub(crate) fn execute_f64_range(
    py: Python<'_>,
    start: i64,
    stop: i64,
    step: i64,
    program: FloatProgram,
) -> PyResult<Vec<f64>> {
    if step == 0 {
        return Err(PyValueError::new_err("range step cannot be zero"));
    }
    let values = (I64Range {
        current: start,
        stop,
        step,
    })
    .map(|value| value as f64);
    py.detach(move || run_f64(values, program))
        .map_err(kernel_error)
}

/// Run a fused f64 program detached, then build the requested terminal container once.
#[pyfunction]
pub(crate) fn materialize_f64(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: FloatProgram,
    target: u8,
) -> PyResult<Py<PyAny>> {
    let target = materialize_target(target)?;
    let allow_integers = source_allows_integer_conversion(&program);
    let values = extract_f64_container(values, allow_integers)?;
    let output = py
        .detach(move || run_f64(values, program))
        .map_err(kernel_error)?;
    materialize_values(py, output, target)
}

/// Snapshot an external f64 buffer and build its fused terminal collection once.
#[pyfunction]
pub(crate) fn materialize_f64_buffer_v1(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: FloatProgram,
    target: u8,
) -> PyResult<Py<PyAny>> {
    let target = materialize_target(target)?;
    let values = extract_f64_buffer(values)?;
    let output = py
        .detach(move || run_f64_buffer_materialization(values, program))
        .map_err(kernel_error)?;
    materialize_values(py, output, target)
}

/// Range-specialized counterpart of ``materialize_f64``.
#[pyfunction]
pub(crate) fn materialize_f64_range(
    py: Python<'_>,
    start: i64,
    stop: i64,
    step: i64,
    program: FloatProgram,
    target: u8,
) -> PyResult<Py<PyAny>> {
    let target = materialize_target(target)?;
    if step == 0 {
        return Err(PyValueError::new_err("range step cannot be zero"));
    }
    let values = (I64Range {
        current: start,
        stop,
        step,
    })
    .map(|value| value as f64);
    let output = py
        .detach(move || run_f64(values, program))
        .map_err(kernel_error)?;
    materialize_values(py, output, target)
}

#[pyfunction]
pub(crate) fn terminal_f64(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: FloatProgram,
    terminal: u8,
) -> PyResult<Option<f64>> {
    let allow_integers = source_allows_integer_conversion(&program);
    let values = extract_f64_container(values, allow_integers)?;
    py.detach(move || run_f64_terminal(values, program, terminal))
        .map_err(kernel_error)
}

/// Reduce a validated f64 buffer and report how many values the fused program emitted.
///
/// The count lets Python preserve its exact empty-sum start value. Terminal opcode 8 remains
/// available for CPython 3.11's sequential sum, while opcode 1 uses compensated summation.
/// Every terminal snapshots the exporter before detaching, preventing Python callbacks from
/// invalidating borrowed buffer storage during reduction.
#[pyfunction]
pub(crate) fn terminal_f64_buffer_v1(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: FloatProgram,
    terminal: u8,
) -> PyResult<(u64, Option<f64>)> {
    let values = extract_f64_buffer(values)?;
    if program.is_empty() {
        return py
            .detach(move || run_f64_identity_terminal(values, terminal))
            .map_err(kernel_error);
    }
    py.detach(move || run_f64_terminal_state::<_, true>(values, program, terminal))
        .map_err(kernel_error)
}

/// Reduce an f64 buffer directly through its live export on GIL-enabled builds.
///
/// Free-threaded builds retain the owned-snapshot path because another Python thread may mutate a
/// writable exporter while the native loop is running. Attached builds keep both the GIL and the
/// export until every compiled stage finishes, then release the export before checking signals.
#[pyfunction]
pub(crate) fn terminal_f64_buffer_v2(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: FloatProgram,
    terminal: u8,
) -> PyResult<(u64, Option<f64>)> {
    #[cfg(not(Py_GIL_DISABLED))]
    {
        let result = match acquire_f64_buffer(values)? {
            Some(buffer) => {
                let slice = buffer.as_slice(py).ok_or_else(|| {
                    PyTypeError::new_err("native f64 buffers require one C-contiguous dimension")
                })?;
                if program.is_empty() {
                    run_f64_identity_terminal(slice.iter().map(|value| value.get()), terminal)
                } else {
                    run_f64_terminal_state::<_, true>(
                        slice.iter().map(|value| value.get()),
                        program,
                        terminal,
                    )
                }
            }
            None if program.is_empty() => run_f64_identity_terminal(std::iter::empty(), terminal),
            None => run_f64_terminal_state::<_, true>(std::iter::empty(), program, terminal),
        };
        py.check_signals()?;
        result.map_err(kernel_error)
    }
    #[cfg(Py_GIL_DISABLED)]
    {
        terminal_f64_buffer_v1(py, values, program, terminal)
    }
}

fn extract_f64_item(value: &Bound<'_, PyAny>, allow_integers: bool) -> PyResult<f64> {
    let exact_float = value.is_exact_instance_of::<PyFloat>();
    if !(exact_float || (allow_integers && value.is_exact_instance_of::<PyInt>())) {
        return Err(PyTypeError::new_err(if allow_integers {
            "native f64 container probes require exact floats or integers"
        } else {
            "native f64 container probes require exact floats"
        }));
    }
    value.extract()
}

/// Probe a small exact list/tuple prefix while retaining the fused stage state.
///
/// Holding the GIL for this bounded path is intentional: it avoids PyO3's eager
/// whole-container extraction. A non-decision returns ``(false, None)`` so the
/// Python adapter can restart the detached bulk f64 kernel for a full traversal.
#[pyfunction]
pub(crate) fn terminal_f64_probe(
    values: &Bound<'_, PyAny>,
    program: FloatProgram,
    terminal: u8,
    max_items: usize,
) -> PyResult<(bool, Option<f64>)> {
    if !(5..=7).contains(&terminal) {
        return Err(kernel_error(KernelError::InvalidProgram(
            "container probes require first, any, or all",
        )));
    }
    let allow_integers = source_allows_integer_conversion(&program);
    let (items, exhausted) = snapshot_exact_container_prefix(values, max_items)?;
    let mut stages = prepare_f64(program).map_err(kernel_error)?;
    let mut result = match terminal {
        5 => None,
        6 => Some(0.0),
        7 => Some(1.0),
        _ => unreachable!(),
    };
    if stages
        .iter()
        .any(|stage| stage.kind == 3 && stage.remaining == 0)
    {
        return Ok((true, result));
    }

    let mut stack = Vec::new();
    for item in items {
        let value = extract_f64_item(item.bind(values.py()), allow_integers)?;
        let mut emit = |value| {
            let stop = match terminal {
                5 => {
                    result = Some(value);
                    true
                }
                6 if value != 0.0 => {
                    result = Some(1.0);
                    true
                }
                7 if value == 0.0 => {
                    result = Some(0.0);
                    true
                }
                6 | 7 => false,
                _ => unreachable!(),
            };
            Ok(if stop {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            })
        };
        if process_f64_value(&mut stages, value, &mut stack, &mut emit)
            .map_err(kernel_error)?
            .is_break()
        {
            return Ok((true, result));
        }
    }
    Ok((exhausted, if exhausted { result } else { None }))
}

#[pyfunction]
pub(crate) fn terminal_f64_range(
    py: Python<'_>,
    start: i64,
    stop: i64,
    step: i64,
    program: FloatProgram,
    terminal: u8,
) -> PyResult<Option<f64>> {
    if step == 0 {
        return Err(PyValueError::new_err("range step cannot be zero"));
    }
    let values = (I64Range {
        current: start,
        stop,
        step,
    })
    .map(|value| value as f64);
    py.detach(move || run_f64_terminal(values, program, terminal))
        .map_err(kernel_error)
}

#[pyfunction]
pub(crate) fn statistics_f64(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: FloatProgram,
) -> PyResult<(u64, f64, f64)> {
    let allow_integers = source_allows_integer_conversion(&program);
    let values = extract_f64_container(values, allow_integers)?;
    py.detach(move || run_f64_statistics(values, program))
        .map_err(kernel_error)
}

#[pyfunction]
pub(crate) fn statistics_f64_range(
    py: Python<'_>,
    start: i64,
    stop: i64,
    step: i64,
    program: FloatProgram,
) -> PyResult<(u64, f64, f64)> {
    if step == 0 {
        return Err(PyValueError::new_err("range step cannot be zero"));
    }
    let values = (I64Range {
        current: start,
        stop,
        step,
    })
    .map(|value| value as f64);
    py.detach(move || run_f64_statistics(values, program))
        .map_err(kernel_error)
}

#[pyfunction]
pub(crate) fn mean_f64(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: FloatProgram,
) -> PyResult<Option<f64>> {
    if program.is_empty() {
        return mean_f64_container(values);
    }
    let allow_integers = source_allows_integer_conversion(&program);
    let values = extract_f64_container(values, allow_integers)?;
    py.detach(move || run_f64_mean(values, program))
        .map_err(kernel_error)
}

/// Reduce a validated external f64 buffer through the compensated mean state.
#[pyfunction]
pub(crate) fn mean_f64_buffer_v1(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: FloatProgram,
) -> PyResult<Option<f64>> {
    let values = extract_f64_buffer(values)?;
    if program.is_empty() {
        return py
            .detach(move || run_f64_identity_mean(values))
            .map_err(kernel_error);
    }
    py.detach(move || run_f64_mean(values, program))
        .map_err(kernel_error)
}

/// Reduce an f64 buffer directly through its live export on GIL-enabled builds.
///
/// Attached builds keep the GIL and exporter alive through every compiled stage. Free-threaded
/// builds retain the owned-snapshot path because a writable exporter may be mutated concurrently.
#[cfg(not(Py_GIL_DISABLED))]
#[inline(never)]
fn mean_f64_staged_buffer(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: FloatProgram,
) -> PyResult<Option<f64>> {
    let result = match acquire_f64_buffer(values)? {
        Some(buffer) => {
            let slice = buffer.as_slice(py).ok_or_else(|| {
                PyTypeError::new_err("native f64 buffers require one C-contiguous dimension")
            })?;
            run_f64_mean(slice.iter().map(|value| value.get()), program)
        }
        None => run_f64_mean(std::iter::empty(), program),
    };
    py.check_signals()?;
    result.map_err(kernel_error)
}

#[pyfunction]
pub(crate) fn mean_f64_buffer_v2(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: FloatProgram,
) -> PyResult<Option<f64>> {
    #[cfg(not(Py_GIL_DISABLED))]
    {
        if program.is_empty() {
            let result = match acquire_f64_buffer(values)? {
                Some(buffer) => {
                    let slice = buffer.as_slice(py).ok_or_else(|| {
                        PyTypeError::new_err(
                            "native f64 buffers require one C-contiguous dimension",
                        )
                    })?;
                    run_f64_identity_mean(slice.iter().map(|value| value.get()))
                }
                None => run_f64_identity_mean(std::iter::empty()),
            };
            py.check_signals()?;
            return result.map_err(kernel_error);
        }
        mean_f64_staged_buffer(py, values, program)
    }
    #[cfg(Py_GIL_DISABLED)]
    {
        mean_f64_buffer_v1(py, values, program)
    }
}

#[pyfunction]
pub(crate) fn mean_f64_range(
    py: Python<'_>,
    start: i64,
    stop: i64,
    step: i64,
    program: FloatProgram,
) -> PyResult<Option<f64>> {
    if step == 0 {
        return Err(PyValueError::new_err("range step cannot be zero"));
    }
    let values = (I64Range {
        current: start,
        stop,
        step,
    })
    .map(|value| value as f64);
    py.detach(move || run_f64_mean(values, program))
        .map_err(kernel_error)
}

#[pyfunction]
pub(crate) fn aggregate_f64(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: FloatProgram,
) -> PyResult<F64AggregateSnapshot> {
    let allow_integers = source_allows_integer_conversion(&program);
    let values = extract_f64_container(values, allow_integers)?;
    py.detach(move || run_f64_aggregate(values, program))
        .map_err(kernel_error)
}

#[pyfunction]
pub(crate) fn aggregate_f64_range(
    py: Python<'_>,
    start: i64,
    stop: i64,
    step: i64,
    program: FloatProgram,
) -> PyResult<F64AggregateSnapshot> {
    if step == 0 {
        return Err(PyValueError::new_err("range step cannot be zero"));
    }
    let values = (I64Range {
        current: start,
        stop,
        step,
    })
    .map(|value| value as f64);
    py.detach(move || run_f64_aggregate(values, program))
        .map_err(kernel_error)
}

/// Compute only requested aggregate fields while retaining the eight-slot ABI schema.
#[pyfunction]
pub(crate) fn aggregate_f64_masked(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: FloatProgram,
    mask: u8,
) -> PyResult<F64AggregateSnapshot> {
    let allow_integers = source_allows_integer_conversion(&program);
    let values = extract_f64_container(values, allow_integers)?;
    py.detach(move || run_f64_aggregate_masked(values, program, mask))
        .map_err(kernel_error)
}

/// Reduce a validated external f64 buffer into only the requested aggregate fields.
///
/// The exporter is snapshotted before detaching so Python callbacks can never invalidate a borrowed
/// buffer while native reduction is in progress.
#[pyfunction]
pub(crate) fn aggregate_f64_buffer_masked_v1(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: FloatProgram,
    mask: u8,
) -> PyResult<F64AggregateSnapshot> {
    let values = extract_f64_buffer(values)?;
    if program.is_empty() {
        return py
            .detach(move || run_f64_identity_aggregate_masked(values, mask))
            .map_err(kernel_error);
    }
    py.detach(move || run_f64_aggregate_masked(values, program, mask))
        .map_err(kernel_error)
}

/// Reduce an identity f64 buffer directly through its live export on GIL-enabled builds.
#[pyfunction]
pub(crate) fn aggregate_f64_buffer_masked_v2(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: FloatProgram,
    mask: u8,
) -> PyResult<F64AggregateSnapshot> {
    #[cfg(not(Py_GIL_DISABLED))]
    if program.is_empty() {
        let result = match acquire_f64_buffer(values)? {
            Some(buffer) => {
                let slice = buffer.as_slice(py).ok_or_else(|| {
                    PyTypeError::new_err("native f64 buffers require one C-contiguous dimension")
                })?;
                run_f64_identity_aggregate_masked(slice.iter().map(|value| value.get()), mask)
            }
            None => run_f64_identity_aggregate_masked(std::iter::empty(), mask),
        };
        py.check_signals()?;
        return result.map_err(kernel_error);
    }
    aggregate_f64_buffer_masked_v1(py, values, program, mask)
}

/// Range-specialized counterpart of ``aggregate_f64_masked``.
#[pyfunction]
pub(crate) fn aggregate_f64_range_masked(
    py: Python<'_>,
    start: i64,
    stop: i64,
    step: i64,
    program: FloatProgram,
    mask: u8,
) -> PyResult<F64AggregateSnapshot> {
    if step == 0 {
        return Err(PyValueError::new_err("range step cannot be zero"));
    }
    let values = (I64Range {
        current: start,
        stop,
        step,
    })
    .map(|value| value as f64);
    py.detach(move || run_f64_aggregate_masked(values, program, mask))
        .map_err(kernel_error)
}

#[pyfunction]
pub(crate) fn count_f64(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: FloatProgram,
) -> PyResult<u64> {
    let allow_integers = source_allows_integer_conversion(&program);
    let values = extract_f64_container(values, allow_integers)?;
    py.detach(move || run_f64_count(values, program))
        .map_err(kernel_error)
}

#[pyfunction]
pub(crate) fn count_f64_range(
    py: Python<'_>,
    start: i64,
    stop: i64,
    step: i64,
    program: FloatProgram,
) -> PyResult<u64> {
    if step == 0 {
        return Err(PyValueError::new_err("range step cannot be zero"));
    }
    let values = (I64Range {
        current: start,
        stop,
        step,
    })
    .map(|value| value as f64);
    py.detach(move || run_f64_count(values, program))
        .map_err(kernel_error)
}
