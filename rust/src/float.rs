//! Floating-point kernels that fuse pipeline stages into streaming materializers and terminals.

use crate::common::{
    AGGREGATE_COUNT, AGGREGATE_FIRST, AGGREGATE_LAST, AGGREGATE_M2, AGGREGATE_MAXIMUM,
    AGGREGATE_MEAN, AGGREGATE_MINIMUM, AGGREGATE_TOTAL, CompensatedSum, KernelError,
    OnlineStatistics, extract_f64_container, kernel_error, materialize_target, materialize_values,
    snapshot_exact_container_prefix, validate_aggregate_mask,
};
use crate::integer::I64Range;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyFloat, PyInt};
use std::ops::ControlFlow;

pub(crate) type FloatInstruction = (u8, f64);
pub(crate) type FloatProgram = Vec<(u8, Vec<FloatInstruction>)>;
pub(crate) type F64AggregateSnapshot = (
    u64,
    f64,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    f64,
    f64,
);

/// Return whether exact Python integers may cross the f64 source boundary.
///
/// Take/drop stages do not evaluate a scalar expression and therefore do not
/// establish a representation.  Only a map as the first expression-bearing
/// stage converts Python integer inputs to float before their type can be
/// observed; identity and predicate-first pipelines must start from floats.
fn source_allows_integer_conversion(program: &FloatProgram) -> bool {
    program
        .iter()
        .find(|(kind, _code)| matches!(kind, 0 | 1 | 2 | 6 | 7 | 8))
        .is_some_and(|(kind, _code)| *kind == 0)
}

struct FloatStage {
    kind: u8,
    code: Vec<FloatInstruction>,
    remaining: u64,
    dropping: bool,
}

fn evaluate_f64(
    value: f64,
    code: &[FloatInstruction],
    stack: &mut Vec<f64>,
) -> Result<f64, KernelError> {
    stack.clear();
    for &(opcode, operand) in code {
        match opcode {
            0 => stack.push(value),
            1 => stack.push(operand),
            2..=4 | 8..=15 | 18 => {
                let right = stack
                    .pop()
                    .ok_or(KernelError::InvalidProgram("expression stack underflow"))?;
                let left = stack
                    .pop()
                    .ok_or(KernelError::InvalidProgram("expression stack underflow"))?;
                let result = match opcode {
                    2 => left + right,
                    3 => left - right,
                    4 => left * right,
                    8 => f64::from(left == right),
                    9 => f64::from(left != right),
                    10 => f64::from(left < right),
                    11 => f64::from(left <= right),
                    12 => f64::from(left > right),
                    13 => f64::from(left >= right),
                    14 => f64::from(left != 0.0 && right != 0.0),
                    15 => f64::from(left != 0.0 || right != 0.0),
                    18 => {
                        if right == 0.0 {
                            return Err(KernelError::DivisionByZero);
                        }
                        left / right
                    }
                    _ => unreachable!(),
                };
                stack.push(result);
            }
            7 | 16 | 17 => {
                let operand = stack
                    .pop()
                    .ok_or(KernelError::InvalidProgram("expression stack underflow"))?;
                stack.push(match opcode {
                    7 => -operand,
                    16 => f64::from(operand == 0.0),
                    17 => operand.abs(),
                    _ => unreachable!(),
                });
            }
            _ => {
                return Err(KernelError::InvalidProgram(
                    "unknown float expression opcode",
                ));
            }
        }
    }
    if stack.len() != 1 {
        return Err(KernelError::InvalidProgram(
            "expression must leave exactly one value",
        ));
    }
    stack
        .pop()
        .ok_or(KernelError::InvalidProgram("expression stack underflow"))
}

fn prepare_f64(program: FloatProgram) -> Result<Vec<FloatStage>, KernelError> {
    program
        .into_iter()
        .map(|(kind, code)| {
            if kind > 8 || kind == 5 {
                return Err(KernelError::InvalidProgram("unknown pipeline stage"));
            }
            let remaining = if kind == 3 || kind == 4 {
                let count = code
                    .first()
                    .ok_or(KernelError::InvalidProgram("missing take/drop count"))?
                    .1;
                if !count.is_finite() || count < 0.0 || count.fract() != 0.0 {
                    return Err(KernelError::InvalidProgram("invalid take/drop count"));
                }
                count as u64
            } else {
                0
            };
            Ok(FloatStage {
                kind,
                code,
                remaining,
                dropping: kind == 7,
            })
        })
        .collect()
}

enum FloatProcessStop {
    Consumer,
    SourceComplete,
}

fn process_f64_value<F>(
    stages: &mut [FloatStage],
    source_value: f64,
    stack: &mut Vec<f64>,
    emit: &mut F,
) -> Result<ControlFlow<FloatProcessStop>, KernelError>
where
    F: FnMut(f64) -> Result<ControlFlow<()>, KernelError>,
{
    let mut value = source_value;
    let mut emit_value = true;
    let mut stop_after_item = false;
    for stage in stages {
        match stage.kind {
            0 => {
                value = evaluate_f64(value, &stage.code, stack)?;
            }
            1 => {
                if evaluate_f64(value, &stage.code, stack)? == 0.0 {
                    emit_value = false;
                    break;
                }
            }
            2 => {
                if evaluate_f64(value, &stage.code, stack)? != 0.0 {
                    emit_value = false;
                    break;
                }
            }
            3 => {
                if stage.remaining == 0 {
                    return Ok(ControlFlow::Break(FloatProcessStop::SourceComplete));
                }
                stage.remaining -= 1;
                stop_after_item |= stage.remaining == 0;
            }
            4 => {
                if stage.remaining > 0 {
                    stage.remaining -= 1;
                    emit_value = false;
                    break;
                }
            }
            6 => {
                if evaluate_f64(value, &stage.code, stack)? == 0.0 {
                    return Ok(ControlFlow::Break(FloatProcessStop::SourceComplete));
                }
            }
            7 => {
                if stage.dropping {
                    if evaluate_f64(value, &stage.code, stack)? != 0.0 {
                        emit_value = false;
                        break;
                    }
                    stage.dropping = false;
                }
            }
            8 => {
                if evaluate_f64(value, &stage.code, stack)? == 0.0 {
                    stop_after_item = true;
                }
            }
            _ => unreachable!(),
        }
    }
    if emit_value && emit(value)?.is_break() {
        return Ok(ControlFlow::Break(FloatProcessStop::Consumer));
    }
    if stop_after_item {
        return Ok(ControlFlow::Break(FloatProcessStop::SourceComplete));
    }
    Ok(ControlFlow::Continue(()))
}

fn process_f64<I, F>(values: I, program: FloatProgram, mut emit: F) -> Result<(), KernelError>
where
    I: IntoIterator<Item = f64>,
    F: FnMut(f64) -> Result<ControlFlow<()>, KernelError>,
{
    let mut stages = prepare_f64(program)?;
    if stages
        .iter()
        .any(|stage| stage.kind == 3 && stage.remaining == 0)
    {
        return Ok(());
    }

    let mut stack = Vec::new();
    for source_value in values {
        if process_f64_value(&mut stages, source_value, &mut stack, &mut emit)?.is_break() {
            return Ok(());
        }
    }
    Ok(())
}

pub(crate) fn run_f64<I>(values: I, program: FloatProgram) -> Result<Vec<f64>, KernelError>
where
    I: IntoIterator<Item = f64>,
{
    let mut output = Vec::new();
    process_f64(values, program, |value| {
        output.push(value);
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(output)
}

pub(crate) fn run_f64_count<I>(values: I, program: FloatProgram) -> Result<u64, KernelError>
where
    I: IntoIterator<Item = f64>,
{
    let mut count = 0_u64;
    process_f64(values, program, |_value| {
        count = count.checked_add(1).ok_or(KernelError::Overflow)?;
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(count)
}

pub(crate) fn run_f64_terminal<I>(
    values: I,
    program: FloatProgram,
    terminal: u8,
) -> Result<Option<f64>, KernelError>
where
    I: IntoIterator<Item = f64>,
{
    let mut result = match terminal {
        1 | 6 | 8 => Some(0.0),
        7 => Some(1.0),
        2..=5 => None,
        _ => {
            return Err(KernelError::InvalidProgram("unknown float terminal"));
        }
    };
    let mut compensation = 0.0;
    process_f64(values, program, |value| {
        let control = match terminal {
            1 | 8 => {
                let current = result.unwrap_or_default();
                let total = current + value;
                if terminal == 1 && current.is_finite() && value.is_finite() && total.is_finite() {
                    compensation += if current.abs() >= value.abs() {
                        current - total + value
                    } else {
                        value - total + current
                    };
                } else if terminal == 1 {
                    compensation = 0.0;
                }
                result = Some(total);
                ControlFlow::Continue(())
            }
            2 => {
                result = Some(result.map_or(
                    value,
                    |current| {
                        if value < current { value } else { current }
                    },
                ));
                ControlFlow::Continue(())
            }
            3 => {
                result = Some(result.map_or(
                    value,
                    |current| {
                        if value > current { value } else { current }
                    },
                ));
                ControlFlow::Continue(())
            }
            4 => {
                result = Some(value);
                ControlFlow::Continue(())
            }
            5 => {
                result = Some(value);
                ControlFlow::Break(())
            }
            6 if value != 0.0 => {
                result = Some(1.0);
                ControlFlow::Break(())
            }
            7 if value == 0.0 => {
                result = Some(0.0);
                ControlFlow::Break(())
            }
            6 | 7 => ControlFlow::Continue(()),
            _ => unreachable!(),
        };
        Ok(control)
    })?;
    if terminal == 1 {
        result = result.map(|value| value + compensation);
    }
    Ok(result)
}

pub(crate) fn run_f64_statistics<I>(
    values: I,
    program: FloatProgram,
) -> Result<(u64, f64, f64), KernelError>
where
    I: IntoIterator<Item = f64>,
{
    let mut statistics = OnlineStatistics::default();
    process_f64(values, program, |value| {
        statistics.accept(value)?;
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(statistics.snapshot())
}

pub(crate) fn run_f64_aggregate<I>(
    values: I,
    program: FloatProgram,
) -> Result<F64AggregateSnapshot, KernelError>
where
    I: IntoIterator<Item = f64>,
{
    let mut minimum = None;
    let mut maximum = None;
    let mut first = None;
    let mut last = None;
    let mut statistics = OnlineStatistics::default();
    process_f64(values, program, |value| {
        minimum = Some(minimum.map_or(
            value,
            |current: f64| {
                if value < current { value } else { current }
            },
        ));
        maximum = Some(maximum.map_or(
            value,
            |current: f64| {
                if value > current { value } else { current }
            },
        ));
        first.get_or_insert(value);
        last = Some(value);
        statistics.accept(value)?;
        Ok(ControlFlow::Continue(()))
    })?;
    let total = statistics.sum();
    let (count, mean, squared_deviations) = statistics.snapshot();
    Ok((
        count,
        total,
        minimum,
        maximum,
        first,
        last,
        mean,
        squared_deviations,
    ))
}

pub(crate) fn run_f64_aggregate_masked<I>(
    values: I,
    program: FloatProgram,
    mask: u8,
) -> Result<F64AggregateSnapshot, KernelError>
where
    I: IntoIterator<Item = f64>,
{
    let mask = validate_aggregate_mask(mask)?;
    let needs_statistics = mask & (AGGREGATE_MEAN | AGGREGATE_M2) != 0;
    let mut count = 0_u64;
    let mut total = CompensatedSum::default();
    let mut minimum = None;
    let mut maximum = None;
    let mut first = None;
    let mut last = None;
    let mut statistics = OnlineStatistics::default();

    process_f64(values, program, |value| {
        if needs_statistics {
            statistics.accept(value)?;
        } else {
            if mask & AGGREGATE_COUNT != 0 {
                count = count.checked_add(1).ok_or(KernelError::Overflow)?;
            }
            if mask & AGGREGATE_TOTAL != 0 {
                total.accept(value);
            }
        }
        if mask & AGGREGATE_MINIMUM != 0 {
            minimum = Some(minimum.map_or(
                value,
                |current: f64| {
                    if value < current { value } else { current }
                },
            ));
        }
        if mask & AGGREGATE_MAXIMUM != 0 {
            maximum = Some(maximum.map_or(
                value,
                |current: f64| {
                    if value > current { value } else { current }
                },
            ));
        }
        if mask & AGGREGATE_FIRST != 0 {
            first.get_or_insert(value);
        }
        if mask & AGGREGATE_LAST != 0 {
            last = Some(value);
        }
        Ok(ControlFlow::Continue(()))
    })?;

    let (statistics_count, mean, squared_deviations) = if needs_statistics {
        statistics.snapshot()
    } else {
        (0, 0.0, 0.0)
    };
    Ok((
        if mask & AGGREGATE_COUNT != 0 {
            if needs_statistics {
                statistics_count
            } else {
                count
            }
        } else {
            0
        },
        if mask & AGGREGATE_TOTAL != 0 {
            if needs_statistics {
                statistics.sum()
            } else {
                total.value()
            }
        } else {
            0.0
        },
        minimum,
        maximum,
        first,
        last,
        if mask & AGGREGATE_MEAN != 0 {
            mean
        } else {
            0.0
        },
        if mask & AGGREGATE_M2 != 0 {
            squared_deviations
        } else {
            0.0
        },
    ))
}

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
