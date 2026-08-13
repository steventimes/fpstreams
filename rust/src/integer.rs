//! Fused signed-integer expression and pipeline kernels.

use crate::common::{KernelError, OnlineStatistics, kernel_error};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::HashSet;
use std::ops::ControlFlow;

pub(crate) type Instruction = (u8, i64);
pub(crate) type Program = Vec<(u8, Vec<Instruction>)>;
pub(crate) type I64AggregateSnapshot = (
    u64,
    i128,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    f64,
    f64,
);

struct Stage {
    kind: u8,
    code: Vec<Instruction>,
    remaining: u64,
    seen: Option<HashSet<i64>>,
    dropping: bool,
}

fn pop(stack: &mut Vec<i64>) -> Result<i64, KernelError> {
    stack
        .pop()
        .ok_or(KernelError::InvalidProgram("expression stack underflow"))
}

pub(crate) fn floor_div(left: i64, right: i64) -> Result<i64, KernelError> {
    if right == 0 {
        return Err(KernelError::DivisionByZero);
    }
    let mut quotient = left.checked_div(right).ok_or(KernelError::Overflow)?;
    let remainder = left.checked_rem(right).ok_or(KernelError::Overflow)?;
    if remainder != 0 && (remainder < 0) != (right < 0) {
        quotient = quotient.checked_sub(1).ok_or(KernelError::Overflow)?;
    }
    Ok(quotient)
}

pub(crate) fn modulo(left: i64, right: i64) -> Result<i64, KernelError> {
    if right == 0 {
        return Err(KernelError::DivisionByZero);
    }
    if right == -1 {
        return Ok(0);
    }
    let mut remainder = left.checked_rem(right).ok_or(KernelError::Overflow)?;
    if remainder != 0 && (remainder < 0) != (right < 0) {
        remainder = remainder.checked_add(right).ok_or(KernelError::Overflow)?;
    }
    Ok(remainder)
}

pub(crate) fn evaluate(
    value: i64,
    code: &[Instruction],
    stack: &mut Vec<i64>,
) -> Result<i64, KernelError> {
    stack.clear();
    for &(opcode, operand) in code {
        match opcode {
            0 => stack.push(value),
            1 => stack.push(operand),
            2..=6 | 8..=15 => {
                let right = pop(stack)?;
                let left = pop(stack)?;
                let result = match opcode {
                    2 => left.checked_add(right).ok_or(KernelError::Overflow)?,
                    3 => left.checked_sub(right).ok_or(KernelError::Overflow)?,
                    4 => left.checked_mul(right).ok_or(KernelError::Overflow)?,
                    5 => floor_div(left, right)?,
                    6 => modulo(left, right)?,
                    8 => i64::from(left == right),
                    9 => i64::from(left != right),
                    10 => i64::from(left < right),
                    11 => i64::from(left <= right),
                    12 => i64::from(left > right),
                    13 => i64::from(left >= right),
                    14 => i64::from(left != 0 && right != 0),
                    15 => i64::from(left != 0 || right != 0),
                    _ => unreachable!(),
                };
                stack.push(result);
            }
            7 | 16 | 17 => {
                let operand = pop(stack)?;
                let result = match opcode {
                    7 => operand.checked_neg().ok_or(KernelError::Overflow)?,
                    16 => i64::from(operand == 0),
                    17 => operand.checked_abs().ok_or(KernelError::Overflow)?,
                    _ => unreachable!(),
                };
                stack.push(result);
            }
            _ => {
                return Err(KernelError::InvalidProgram("unknown expression opcode"));
            }
        }
    }
    if stack.len() != 1 {
        return Err(KernelError::InvalidProgram(
            "expression must leave exactly one value",
        ));
    }
    pop(stack)
}

fn prepare(program: Program) -> Result<Vec<Stage>, KernelError> {
    program
        .into_iter()
        .map(|(kind, code)| {
            if kind > 8 {
                return Err(KernelError::InvalidProgram("unknown pipeline stage"));
            }
            let remaining = if kind == 3 || kind == 4 {
                let count = code
                    .first()
                    .ok_or(KernelError::InvalidProgram("missing take/drop count"))?
                    .1;
                u64::try_from(count)
                    .map_err(|_| KernelError::InvalidProgram("negative take/drop count"))?
            } else {
                0
            };
            Ok(Stage {
                kind,
                code,
                remaining,
                seen: (kind == 5).then(HashSet::new),
                dropping: kind == 7,
            })
        })
        .collect()
}

fn process_values<I, F>(values: I, program: Program, mut emit: F) -> Result<(), KernelError>
where
    I: IntoIterator<Item = i64>,
    F: FnMut(i64) -> Result<ControlFlow<()>, KernelError>,
{
    let mut stages = prepare(program)?;
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
                    value = evaluate(value, &stage.code, &mut stack)?;
                }
                1 => {
                    if evaluate(value, &stage.code, &mut stack)? == 0 {
                        emit_value = false;
                        break;
                    }
                }
                2 => {
                    if evaluate(value, &stage.code, &mut stack)? != 0 {
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
                5 => {
                    let seen = stage
                        .seen
                        .as_mut()
                        .ok_or(KernelError::InvalidProgram("missing distinct state"))?;
                    if !seen.insert(value) {
                        emit_value = false;
                        break;
                    }
                }
                6 => {
                    if evaluate(value, &stage.code, &mut stack)? == 0 {
                        return Ok(());
                    }
                }
                7 => {
                    if stage.dropping {
                        if evaluate(value, &stage.code, &mut stack)? != 0 {
                            emit_value = false;
                            break;
                        }
                        stage.dropping = false;
                    }
                }
                8 => {
                    if evaluate(value, &stage.code, &mut stack)? == 0 {
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

pub(crate) fn run_values<I>(values: I, program: Program) -> Result<Vec<i64>, KernelError>
where
    I: IntoIterator<Item = i64>,
{
    let mut output = Vec::new();
    process_values(values, program, |value| {
        output.push(value);
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(output)
}

pub(crate) fn run_terminal<I>(
    values: I,
    program: Program,
    terminal: u8,
) -> Result<Option<i64>, KernelError>
where
    I: IntoIterator<Item = i64>,
{
    let mut result: Option<i64> = match terminal {
        0 | 1 | 6 => Some(0),
        7 => Some(1),
        2..=5 => None,
        _ => {
            return Err(KernelError::InvalidProgram("unknown terminal"));
        }
    };
    process_values(values, program, |value| {
        let control = match terminal {
            0 => {
                result = Some(
                    result
                        .unwrap_or_default()
                        .checked_add(1)
                        .ok_or(KernelError::Overflow)?,
                );
                ControlFlow::Continue(())
            }
            1 => {
                result = Some(
                    result
                        .unwrap_or_default()
                        .checked_add(value)
                        .ok_or(KernelError::Overflow)?,
                );
                ControlFlow::Continue(())
            }
            2 => {
                result = Some(result.map_or(value, |current| current.min(value)));
                ControlFlow::Continue(())
            }
            3 => {
                result = Some(result.map_or(value, |current| current.max(value)));
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
            6 if value != 0 => {
                result = Some(1);
                ControlFlow::Break(())
            }
            7 if value == 0 => {
                result = Some(0);
                ControlFlow::Break(())
            }
            6 | 7 => ControlFlow::Continue(()),
            _ => unreachable!(),
        };
        Ok(control)
    })?;
    Ok(result)
}

pub(crate) fn run_i64_statistics<I>(
    values: I,
    program: Program,
) -> Result<(u64, f64, f64), KernelError>
where
    I: IntoIterator<Item = i64>,
{
    let mut statistics = OnlineStatistics::default();
    process_values(values, program, |value| {
        statistics.accept(value as f64)?;
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(statistics.snapshot())
}

pub(crate) fn run_i64_aggregate<I>(
    values: I,
    program: Program,
) -> Result<I64AggregateSnapshot, KernelError>
where
    I: IntoIterator<Item = i64>,
{
    let mut total = 0_i128;
    let mut minimum = None;
    let mut maximum = None;
    let mut first = None;
    let mut last = None;
    let mut statistics = OnlineStatistics::default();
    process_values(values, program, |value| {
        total = total
            .checked_add(i128::from(value))
            .ok_or(KernelError::Overflow)?;
        minimum = Some(minimum.map_or(value, |current: i64| current.min(value)));
        maximum = Some(maximum.map_or(value, |current: i64| current.max(value)));
        first.get_or_insert(value);
        last = Some(value);
        statistics.accept(value as f64)?;
        Ok(ControlFlow::Continue(()))
    })?;
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

pub(crate) struct I64Range {
    pub(crate) current: i64,
    pub(crate) stop: i64,
    pub(crate) step: i64,
}

impl Iterator for I64Range {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        let finished = if self.step > 0 {
            self.current >= self.stop
        } else {
            self.current <= self.stop
        };
        if finished {
            return None;
        }
        let value = self.current;
        self.current = self.current.checked_add(self.step).unwrap_or(self.stop);
        Some(value)
    }
}

#[pyfunction]
pub(crate) fn execute_i64(
    py: Python<'_>,
    values: Vec<i64>,
    program: Program,
) -> PyResult<Vec<i64>> {
    py.detach(move || run_values(values, program))
        .map_err(kernel_error)
}

#[pyfunction]
pub(crate) fn execute_i64_range(
    py: Python<'_>,
    start: i64,
    stop: i64,
    step: i64,
    program: Program,
) -> PyResult<Vec<i64>> {
    if step == 0 {
        return Err(PyValueError::new_err("range step cannot be zero"));
    }
    let values = I64Range {
        current: start,
        stop,
        step,
    };
    py.detach(move || run_values(values, program))
        .map_err(kernel_error)
}

#[pyfunction]
pub(crate) fn terminal_i64(
    py: Python<'_>,
    values: Vec<i64>,
    program: Program,
    terminal: u8,
) -> PyResult<Option<i64>> {
    py.detach(move || run_terminal(values, program, terminal))
        .map_err(kernel_error)
}

#[pyfunction]
pub(crate) fn terminal_i64_range(
    py: Python<'_>,
    start: i64,
    stop: i64,
    step: i64,
    program: Program,
    terminal: u8,
) -> PyResult<Option<i64>> {
    if step == 0 {
        return Err(PyValueError::new_err("range step cannot be zero"));
    }
    let values = I64Range {
        current: start,
        stop,
        step,
    };
    py.detach(move || run_terminal(values, program, terminal))
        .map_err(kernel_error)
}

#[pyfunction]
pub(crate) fn statistics_i64(
    py: Python<'_>,
    values: Vec<i64>,
    program: Program,
) -> PyResult<(u64, f64, f64)> {
    py.detach(move || run_i64_statistics(values, program))
        .map_err(kernel_error)
}

#[pyfunction]
pub(crate) fn statistics_i64_range(
    py: Python<'_>,
    start: i64,
    stop: i64,
    step: i64,
    program: Program,
) -> PyResult<(u64, f64, f64)> {
    if step == 0 {
        return Err(PyValueError::new_err("range step cannot be zero"));
    }
    let values = I64Range {
        current: start,
        stop,
        step,
    };
    py.detach(move || run_i64_statistics(values, program))
        .map_err(kernel_error)
}

#[pyfunction]
pub(crate) fn aggregate_i64(
    py: Python<'_>,
    values: Vec<i64>,
    program: Program,
) -> PyResult<I64AggregateSnapshot> {
    py.detach(move || run_i64_aggregate(values, program))
        .map_err(kernel_error)
}

#[pyfunction]
pub(crate) fn aggregate_i64_range(
    py: Python<'_>,
    start: i64,
    stop: i64,
    step: i64,
    program: Program,
) -> PyResult<I64AggregateSnapshot> {
    if step == 0 {
        return Err(PyValueError::new_err("range step cannot be zero"));
    }
    let values = I64Range {
        current: start,
        stop,
        step,
    };
    py.detach(move || run_i64_aggregate(values, program))
        .map_err(kernel_error)
}
