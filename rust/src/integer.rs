//! Signed-integer kernels that fuse pipeline stages into streaming materializers and terminals.

use crate::common::{
    AGGREGATE_COUNT, AGGREGATE_FIRST, AGGREGATE_LAST, AGGREGATE_M2, AGGREGATE_MAXIMUM,
    AGGREGATE_MEAN, AGGREGATE_MINIMUM, AGGREGATE_TOTAL, KernelError, OnlineStatistics,
    extract_i64_container, kernel_error, materialize_target, materialize_values,
    snapshot_exact_container_prefix, validate_aggregate_mask,
};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyInt;
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
    expression: Option<PreparedExpression>,
    remaining: u64,
    seen: Option<HashSet<i64>>,
    dropping: bool,
}

pub(crate) enum PreparedExpression {
    /// A checked ``item * multiplier + offset`` map without interpreter dispatch.
    Affine { multiplier: i64, offset: i64 },
    /// An exact divisibility predicate using a mask instead of integer division.
    DivisibleByPowerOfTwo { mask: i64 },
    /// Every expression outside the conservative peephole vocabulary.
    Bytecode(Vec<Instruction>),
}

pub(crate) fn prepare_expression(code: Vec<Instruction>) -> PreparedExpression {
    // These two shapes dominate numeric map/filter pipelines. Both preserve the
    // checked arithmetic and signed-modulo behavior of the generic evaluator.
    if let [(0, _), (1, multiplier), (4, _), (1, offset), (2, _)] = code.as_slice() {
        return PreparedExpression::Affine {
            multiplier: *multiplier,
            offset: *offset,
        };
    }
    if let [(0, _), (1, divisor), (6, _), (1, 0), (8, _)] = code.as_slice() {
        let magnitude = divisor.unsigned_abs();
        if magnitude.is_power_of_two() {
            return PreparedExpression::DivisibleByPowerOfTwo {
                mask: (magnitude - 1) as i64,
            };
        }
    }
    PreparedExpression::Bytecode(code)
}

impl PreparedExpression {
    #[inline]
    fn evaluate(&self, value: i64, stack: &mut Vec<i64>) -> Result<i64, KernelError> {
        match self {
            Self::Affine { multiplier, offset } => value
                .checked_mul(*multiplier)
                .and_then(|mapped| mapped.checked_add(*offset))
                .ok_or(KernelError::Overflow),
            Self::DivisibleByPowerOfTwo { mask } => Ok(i64::from(value & mask == 0)),
            Self::Bytecode(code) => evaluate(value, code, stack),
        }
    }
}

impl Stage {
    #[inline]
    fn evaluate(&self, value: i64, stack: &mut Vec<i64>) -> Result<i64, KernelError> {
        self.expression
            .as_ref()
            .ok_or(KernelError::InvalidProgram("missing stage expression"))?
            .evaluate(value, stack)
    }
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
            let expression =
                matches!(kind, 0 | 1 | 2 | 6 | 7 | 8).then(|| prepare_expression(code));
            Ok(Stage {
                kind,
                expression,
                remaining,
                seen: (kind == 5).then(HashSet::new),
                dropping: kind == 7,
            })
        })
        .collect()
}

/// Tell the caller why processing a source value cannot continue.
///
/// This distinction lets bounded PyO3 probes keep stage state between values while
/// still recognize a take/take-while pipeline that has exhausted its own source.
enum ProcessStop {
    Consumer,
    SourceComplete,
}

fn process_value<F>(
    stages: &mut [Stage],
    source_value: i64,
    stack: &mut Vec<i64>,
    emit: &mut F,
) -> Result<ControlFlow<ProcessStop>, KernelError>
where
    F: FnMut(i64) -> Result<ControlFlow<()>, KernelError>,
{
    let mut value = source_value;
    let mut emit_value = true;
    let mut stop_after_item = false;
    for stage in stages {
        match stage.kind {
            0 => {
                value = stage.evaluate(value, stack)?;
            }
            1 => {
                if stage.evaluate(value, stack)? == 0 {
                    emit_value = false;
                    break;
                }
            }
            2 => {
                if stage.evaluate(value, stack)? != 0 {
                    emit_value = false;
                    break;
                }
            }
            3 => {
                if stage.remaining == 0 {
                    return Ok(ControlFlow::Break(ProcessStop::SourceComplete));
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
                if stage.evaluate(value, stack)? == 0 {
                    return Ok(ControlFlow::Break(ProcessStop::SourceComplete));
                }
            }
            7 => {
                if stage.dropping {
                    if stage.evaluate(value, stack)? != 0 {
                        emit_value = false;
                        break;
                    }
                    stage.dropping = false;
                }
            }
            8 => {
                if stage.evaluate(value, stack)? == 0 {
                    stop_after_item = true;
                }
            }
            _ => unreachable!(),
        }
    }
    if emit_value && emit(value)?.is_break() {
        return Ok(ControlFlow::Break(ProcessStop::Consumer));
    }
    if stop_after_item {
        return Ok(ControlFlow::Break(ProcessStop::SourceComplete));
    }
    Ok(ControlFlow::Continue(()))
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
        if process_value(&mut stages, source_value, &mut stack, &mut emit)?.is_break() {
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

pub(crate) fn run_i64_aggregate_masked<I>(
    values: I,
    program: Program,
    mask: u8,
) -> Result<I64AggregateSnapshot, KernelError>
where
    I: IntoIterator<Item = i64>,
{
    let mask = validate_aggregate_mask(mask)?;
    let needs_statistics = mask & (AGGREGATE_MEAN | AGGREGATE_M2) != 0;
    let mut count = 0_u64;
    let mut total = 0_i128;
    let mut minimum = None;
    let mut maximum = None;
    let mut first = None;
    let mut last = None;
    let mut statistics = OnlineStatistics::default();

    process_values(values, program, |value| {
        if needs_statistics {
            statistics.accept(value as f64)?;
        } else if mask & AGGREGATE_COUNT != 0 {
            count = count.checked_add(1).ok_or(KernelError::Overflow)?;
        }
        if mask & AGGREGATE_TOTAL != 0 {
            total = total
                .checked_add(i128::from(value))
                .ok_or(KernelError::Overflow)?;
        }
        if mask & AGGREGATE_MINIMUM != 0 {
            minimum = Some(minimum.map_or(value, |current: i64| current.min(value)));
        }
        if mask & AGGREGATE_MAXIMUM != 0 {
            maximum = Some(maximum.map_or(value, |current: i64| current.max(value)));
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
        total,
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
    values: &Bound<'_, PyAny>,
    program: Program,
) -> PyResult<Vec<i64>> {
    let values = extract_i64_container(values)?;
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

/// Run a fused i64 program detached, then build the requested terminal container once.
#[pyfunction]
pub(crate) fn materialize_i64(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: Program,
    target: u8,
) -> PyResult<Py<PyAny>> {
    let target = materialize_target(target)?;
    let values = extract_i64_container(values)?;
    let output = py
        .detach(move || run_values(values, program))
        .map_err(kernel_error)?;
    materialize_values(py, output, target)
}

/// Range-specialized counterpart of ``materialize_i64``.
#[pyfunction]
pub(crate) fn materialize_i64_range(
    py: Python<'_>,
    start: i64,
    stop: i64,
    step: i64,
    program: Program,
    target: u8,
) -> PyResult<Py<PyAny>> {
    let target = materialize_target(target)?;
    if step == 0 {
        return Err(PyValueError::new_err("range step cannot be zero"));
    }
    let values = I64Range {
        current: start,
        stop,
        step,
    };
    let output = py
        .detach(move || run_values(values, program))
        .map_err(kernel_error)?;
    materialize_values(py, output, target)
}

#[pyfunction]
pub(crate) fn terminal_i64(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: Program,
    terminal: u8,
) -> PyResult<Option<i64>> {
    let values = extract_i64_container(values)?;
    py.detach(move || run_terminal(values, program, terminal))
        .map_err(kernel_error)
}

fn extract_i64_item(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    if !value.is_exact_instance_of::<PyInt>() {
        return Err(PyTypeError::new_err(
            "native i64 container probes require exact integers",
        ));
    }
    value.extract()
}

/// Probe only a bounded prefix of an exact Python container without building a Vec.
///
/// The return value is ``(completed, result)``. ``completed`` is false only when
/// the budget ended before first/any/all could be decided; callers then restart
/// the existing detached bulk kernel from the beginning to retain full-scan speed.
#[pyfunction]
pub(crate) fn terminal_i64_probe(
    values: &Bound<'_, PyAny>,
    program: Program,
    terminal: u8,
    max_items: usize,
) -> PyResult<(bool, Option<i64>)> {
    if !(5..=7).contains(&terminal) {
        return Err(kernel_error(KernelError::InvalidProgram(
            "container probes require first, any, or all",
        )));
    }
    let (items, exhausted) = snapshot_exact_container_prefix(values, max_items)?;
    let mut stages = prepare(program).map_err(kernel_error)?;
    let mut result = match terminal {
        5 => None,
        6 => Some(0),
        7 => Some(1),
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
        let value = extract_i64_item(item.bind(values.py()))?;
        let mut emit = |value| {
            let stop = match terminal {
                5 => {
                    result = Some(value);
                    true
                }
                6 if value != 0 => {
                    result = Some(1);
                    true
                }
                7 if value == 0 => {
                    result = Some(0);
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
        if process_value(&mut stages, value, &mut stack, &mut emit)
            .map_err(kernel_error)?
            .is_break()
        {
            return Ok((true, result));
        }
    }
    Ok((exhausted, if exhausted { result } else { None }))
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
    values: &Bound<'_, PyAny>,
    program: Program,
) -> PyResult<(u64, f64, f64)> {
    let values = extract_i64_container(values)?;
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
    values: &Bound<'_, PyAny>,
    program: Program,
) -> PyResult<I64AggregateSnapshot> {
    let values = extract_i64_container(values)?;
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

/// Compute only requested aggregate fields while preserving the established snapshot schema.
#[pyfunction]
pub(crate) fn aggregate_i64_masked(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: Program,
    mask: u8,
) -> PyResult<I64AggregateSnapshot> {
    let values = extract_i64_container(values)?;
    py.detach(move || run_i64_aggregate_masked(values, program, mask))
        .map_err(kernel_error)
}

/// Range-specialized counterpart of ``aggregate_i64_masked``.
#[pyfunction]
pub(crate) fn aggregate_i64_range_masked(
    py: Python<'_>,
    start: i64,
    stop: i64,
    step: i64,
    program: Program,
    mask: u8,
) -> PyResult<I64AggregateSnapshot> {
    if step == 0 {
        return Err(PyValueError::new_err("range step cannot be zero"));
    }
    let values = I64Range {
        current: start,
        stop,
        step,
    };
    py.detach(move || run_i64_aggregate_masked(values, program, mask))
        .map_err(kernel_error)
}
