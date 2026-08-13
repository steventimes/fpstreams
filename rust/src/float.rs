//! Fused floating-point expression and pipeline kernels.

use crate::common::{KernelError, OnlineStatistics, kernel_error};
use crate::integer::I64Range;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
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
        let mut value = source_value;
        let mut emit_value = true;
        let mut stop_after_item = false;
        for stage in &mut stages {
            match stage.kind {
                0 => {
                    value = evaluate_f64(value, &stage.code, &mut stack)?;
                }
                1 => {
                    if evaluate_f64(value, &stage.code, &mut stack)? == 0.0 {
                        emit_value = false;
                        break;
                    }
                }
                2 => {
                    if evaluate_f64(value, &stage.code, &mut stack)? != 0.0 {
                        emit_value = false;
                        break;
                    }
                }
                3 => {
                    if stage.remaining == 0 {
                        return Ok(());
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
                    if evaluate_f64(value, &stage.code, &mut stack)? == 0.0 {
                        return Ok(());
                    }
                }
                7 => {
                    if stage.dropping {
                        if evaluate_f64(value, &stage.code, &mut stack)? != 0.0 {
                            emit_value = false;
                            break;
                        }
                        stage.dropping = false;
                    }
                }
                8 => {
                    if evaluate_f64(value, &stage.code, &mut stack)? == 0.0 {
                        stop_after_item = true;
                    }
                }
                _ => unreachable!(),
            }
        }
        if emit_value && emit(value)?.is_break() {
            return Ok(());
        }
        if stop_after_item {
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

#[pyfunction]
pub(crate) fn execute_f64(
    py: Python<'_>,
    values: Vec<f64>,
    program: FloatProgram,
) -> PyResult<Vec<f64>> {
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

#[pyfunction]
pub(crate) fn terminal_f64(
    py: Python<'_>,
    values: Vec<f64>,
    program: FloatProgram,
    terminal: u8,
) -> PyResult<Option<f64>> {
    py.detach(move || run_f64_terminal(values, program, terminal))
        .map_err(kernel_error)
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
    values: Vec<f64>,
    program: FloatProgram,
) -> PyResult<(u64, f64, f64)> {
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
    values: Vec<f64>,
    program: FloatProgram,
) -> PyResult<F64AggregateSnapshot> {
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

#[pyfunction]
pub(crate) fn count_f64(py: Python<'_>, values: Vec<f64>, program: FloatProgram) -> PyResult<u64> {
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
