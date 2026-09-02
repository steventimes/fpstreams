//! Signed-integer kernels that fuse pipeline stages into streaming materializers and terminals.

use crate::common::{
    AGGREGATE_COUNT, AGGREGATE_FIRST, AGGREGATE_LAST, AGGREGATE_M2, AGGREGATE_MAXIMUM,
    AGGREGATE_MEAN, AGGREGATE_MINIMUM, AGGREGATE_TOTAL, KernelError, OnlineStatistics,
    validate_aggregate_mask,
};
use crate::numeric_mean::{CompensatedMean, next_exact_i64_mean_total};
use crate::relational::SeededI64BuildHasher;
use std::collections::HashSet;
use std::ops::ControlFlow;

mod endpoints;

pub(crate) use endpoints::{
    I64Range, aggregate_i64, aggregate_i64_buffer_masked_v1, aggregate_i64_buffer_masked_v2,
    aggregate_i64_masked, aggregate_i64_range, aggregate_i64_range_masked, execute_i64,
    execute_i64_buffer_v1, execute_i64_range, frequencies_i64_exact_v1, materialize_i64,
    materialize_i64_buffer_v1, materialize_i64_range, mean_i64, mean_i64_buffer_v1,
    mean_i64_buffer_v2, mean_i64_range, statistics_i64, statistics_i64_range, terminal_i64,
    terminal_i64_probe, terminal_i64_range,
};
#[cfg(not(Py_GIL_DISABLED))]
pub(crate) use endpoints::{
    materialize_i64_filter_exact_list_v1, materialize_i64_map_exact_list_v1,
};

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
    seen: Option<HashSet<i64, SeededI64BuildHasher>>,
    dropping: bool,
}

struct StatelessMapFilter {
    mapper: PreparedExpression,
    predicate: PreparedExpression,
    negated: bool,
}

pub(crate) enum PreparedExpression {
    /// A checked ``item * multiplier + offset`` map without interpreter dispatch.
    Affine { multiplier: i64, offset: i64 },
    /// An exact divisibility predicate using a mask instead of integer division.
    DivisibleByPowerOfTwo { mask: i64 },
    /// Every expression outside the conservative peephole vocabulary.
    Bytecode(Vec<Instruction>),
}

#[derive(Clone, Copy)]
enum AffineCandidate {
    Constant(i64),
    Item,
    Scaled(i64),
    Affine { multiplier: i64, offset: i64 },
}

impl AffineCandidate {
    fn shifted(self, offset: i64) -> Option<Self> {
        let multiplier = match self {
            Self::Item => 1,
            Self::Scaled(multiplier) => multiplier,
            Self::Constant(_) | Self::Affine { .. } => return None,
        };
        Some(Self::Affine { multiplier, offset })
    }
}

fn prepare_affine_expression(code: &[Instruction]) -> Option<PreparedExpression> {
    // Checked arithmetic makes algebraic reassociation observable. Accept only one item,
    // an optional item/constant multiplication, then one constant translation. The fixed stack
    // covers that whole grammar; deeper or nested arithmetic stays on the bytecode evaluator.
    let mut stack = [AffineCandidate::Item; 3];
    let mut depth = 0;
    for &(opcode, operand) in code {
        match opcode {
            0 | 1 => {
                if depth == stack.len() {
                    return None;
                }
                stack[depth] = if opcode == 0 {
                    AffineCandidate::Item
                } else {
                    AffineCandidate::Constant(operand)
                };
                depth += 1;
            }
            2..=4 => {
                if depth < 2 {
                    return None;
                }
                let right = stack[depth - 1];
                let left = stack[depth - 2];
                depth -= 2;
                let candidate = match (opcode, left, right) {
                    (2, linear, AffineCandidate::Constant(offset))
                    | (2, AffineCandidate::Constant(offset), linear) => linear.shifted(offset)?,
                    (3, linear, AffineCandidate::Constant(subtrahend)) => {
                        linear.shifted(subtrahend.checked_neg()?)?
                    }
                    (4, AffineCandidate::Item, AffineCandidate::Constant(multiplier))
                    | (4, AffineCandidate::Constant(multiplier), AffineCandidate::Item) => {
                        AffineCandidate::Scaled(multiplier)
                    }
                    _ => return None,
                };
                stack[depth] = candidate;
                depth += 1;
            }
            _ => return None,
        }
    }
    if depth != 1 {
        return None;
    }
    match stack[0] {
        AffineCandidate::Scaled(multiplier) => Some(PreparedExpression::Affine {
            multiplier,
            offset: 0,
        }),
        AffineCandidate::Affine { multiplier, offset } => {
            Some(PreparedExpression::Affine { multiplier, offset })
        }
        _ => None,
    }
}

pub(crate) fn prepare_expression(code: Vec<Instruction>) -> PreparedExpression {
    // These common shapes dominate numeric map/filter pipelines. Each preserves the
    // checked arithmetic and signed-modulo behavior of the generic evaluator.
    if let Some(expression) = prepare_affine_expression(&code) {
        return expression;
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
    pub(crate) fn evaluate(&self, value: i64, stack: &mut Vec<i64>) -> Result<i64, KernelError> {
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
                // Exact integers do not need a general-purpose SipHash round per item. A fresh
                // seed per stage prevents reusable collision layouts across executions.
                seen: (kind == 5).then(|| HashSet::with_hasher(SeededI64BuildHasher::random())),
                dropping: kind == 7,
            })
        })
        .collect()
}

fn prepare_stateless_map_filter(program: Program) -> Result<StatelessMapFilter, KernelError> {
    let mut stages = program.into_iter();
    let (_, mapper) = stages
        .next()
        .ok_or(KernelError::InvalidProgram("missing map stage"))?;
    let (filter_kind, predicate) = stages
        .next()
        .ok_or(KernelError::InvalidProgram("missing filter stage"))?;
    Ok(StatelessMapFilter {
        mapper: prepare_expression(mapper),
        predicate: prepare_expression(predicate),
        negated: filter_kind == 2,
    })
}

/// Tell the caller why processing a source value cannot continue.
///
/// This distinction lets bounded PyO3 probes keep stage state between values while
/// still recognize a take/take-while pipeline that has exhausted its own source.
enum ProcessStop {
    Consumer,
    SourceComplete,
}

#[inline(always)]
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

#[inline]
fn process_values<I, F>(values: I, program: Program, mut emit: F) -> Result<(), KernelError>
where
    I: IntoIterator<Item = i64>,
    F: FnMut(i64) -> Result<ControlFlow<()>, KernelError>,
{
    if matches!(program.as_slice(), [(0, _), (1 | 2, _)]) {
        let map_filter = prepare_stateless_map_filter(program)?;
        return process_stateless_map_filter(values, map_filter, &mut emit);
    }
    process_general_values(values, program, emit)
}

#[inline]
fn process_general_values<I, F>(values: I, program: Program, mut emit: F) -> Result<(), KernelError>
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

#[inline(never)]
fn process_stateless_map_filter<I, F>(
    values: I,
    map_filter: StatelessMapFilter,
    emit: &mut F,
) -> Result<(), KernelError>
where
    I: IntoIterator<Item = i64>,
    F: FnMut(i64) -> Result<ControlFlow<()>, KernelError>,
{
    let StatelessMapFilter {
        mapper,
        predicate,
        negated,
    } = map_filter;
    if let (
        PreparedExpression::Affine { multiplier, offset },
        PreparedExpression::DivisibleByPowerOfTwo { mask },
    ) = (&mapper, &predicate)
    {
        for source_value in values {
            let value = source_value
                .checked_mul(*multiplier)
                .and_then(|mapped| mapped.checked_add(*offset))
                .ok_or(KernelError::Overflow)?;
            if (value & *mask == 0) == negated {
                continue;
            }
            if emit(value)?.is_break() {
                return Ok(());
            }
        }
        return Ok(());
    }

    let mut stack = Vec::new();
    for source_value in values {
        let value = mapper.evaluate(source_value, &mut stack)?;
        let matches = predicate.evaluate(value, &mut stack)? != 0;
        if matches == negated {
            continue;
        }
        if emit(value)?.is_break() {
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

pub(crate) fn run_i64_buffer_materialization(
    values: Vec<i64>,
    program: Program,
) -> Result<Vec<i64>, KernelError> {
    if program.is_empty() {
        return Ok(values);
    }
    run_values(values, program)
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

pub(crate) fn run_i64_mean<I>(values: I, program: Program) -> Result<Option<f64>, KernelError>
where
    I: IntoIterator<Item = i64>,
{
    let mut mean = CompensatedMean::default();
    process_values(values, program, |value| {
        mean.accept(value as f64)?;
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(mean.value())
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

#[cfg(not(Py_GIL_DISABLED))]
#[inline(always)]
fn select_i64_extreme<const MAXIMUM: bool>(left: i64, right: i64) -> i64 {
    if MAXIMUM {
        left.max(right)
    } else {
        left.min(right)
    }
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn reduce_i64_extreme_by<T, F, const MAXIMUM: bool>(values: &[T], value: F) -> Option<i64>
where
    F: Fn(&T) -> i64,
{
    let (first, remaining) = values.split_first()?;
    let first = value(first);
    let mut lane_0 = first;
    let mut lane_1 = first;
    let mut lane_2 = first;
    let mut lane_3 = first;
    let mut chunks = remaining.chunks_exact(4);
    for chunk in &mut chunks {
        lane_0 = select_i64_extreme::<MAXIMUM>(lane_0, value(&chunk[0]));
        lane_1 = select_i64_extreme::<MAXIMUM>(lane_1, value(&chunk[1]));
        lane_2 = select_i64_extreme::<MAXIMUM>(lane_2, value(&chunk[2]));
        lane_3 = select_i64_extreme::<MAXIMUM>(lane_3, value(&chunk[3]));
    }
    for item in chunks.remainder() {
        lane_0 = select_i64_extreme::<MAXIMUM>(lane_0, value(item));
    }
    Some(select_i64_extreme::<MAXIMUM>(
        select_i64_extreme::<MAXIMUM>(lane_0, lane_1),
        select_i64_extreme::<MAXIMUM>(lane_2, lane_3),
    ))
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
pub(crate) fn reduce_i64_min_by<T, F>(values: &[T], value: F) -> Option<i64>
where
    F: Fn(&T) -> i64,
{
    reduce_i64_extreme_by::<_, _, false>(values, value)
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
pub(crate) fn reduce_i64_max_by<T, F>(values: &[T], value: F) -> Option<i64>
where
    F: Fn(&T) -> i64,
{
    reduce_i64_extreme_by::<_, _, true>(values, value)
}

pub(crate) fn run_i64_identity_aggregate_masked<I>(
    values: I,
    mask: u8,
) -> Result<I64AggregateSnapshot, KernelError>
where
    I: IntoIterator<Item = i64>,
    I::IntoIter: ExactSizeIterator,
{
    let mut values = values.into_iter();
    let empty = || (0, 0, None, None, None, None, 0.0, 0.0);
    match validate_aggregate_mask(mask)? {
        AGGREGATE_COUNT => {
            let mut snapshot = empty();
            snapshot.0 = u64::try_from(values.len()).map_err(|_| KernelError::Overflow)?;
            Ok(snapshot)
        }
        AGGREGATE_TOTAL => {
            let mut snapshot = empty();
            // Every physically representable sequence of i64 values has an exact sum that fits
            // i128: even `usize::MAX * i64::MIN` remains above `i128::MIN`. Avoiding an
            // impossible per-item overflow branch lets LLVM keep this identity reduction tight.
            snapshot.1 = values.map(i128::from).sum();
            Ok(snapshot)
        }
        AGGREGATE_MINIMUM => {
            let mut snapshot = empty();
            snapshot.2 = values.min();
            Ok(snapshot)
        }
        AGGREGATE_MAXIMUM => {
            let mut snapshot = empty();
            snapshot.3 = values.max();
            Ok(snapshot)
        }
        AGGREGATE_FIRST => {
            let mut snapshot = empty();
            snapshot.4 = values.next();
            Ok(snapshot)
        }
        AGGREGATE_LAST => {
            let mut snapshot = empty();
            snapshot.5 = values.last();
            Ok(snapshot)
        }
        statistics_mask if statistics_mask == AGGREGATE_COUNT | AGGREGATE_MEAN | AGGREGATE_M2 => {
            let mut statistics = OnlineStatistics::default();
            for value in values {
                statistics.accept(value as f64)?;
            }
            let (count, mean, squared_deviations) = statistics.snapshot();
            Ok((count, 0, None, None, None, None, mean, squared_deviations))
        }
        validated => run_i64_aggregate_masked(values, Vec::new(), validated),
    }
}

#[cfg(any(not(Py_GIL_DISABLED), test))]
pub(crate) fn run_i64_identity_mean<I>(values: I) -> Result<Option<f64>, KernelError>
where
    I: IntoIterator<Item = i64>,
{
    let mut values = values.into_iter();
    let mut count = 0_u64;
    let mut total = 0_i64;
    while let Some(value) = values.next() {
        let next_count = count.checked_add(1).ok_or(KernelError::Overflow)?;
        if let Some(next_total) = next_exact_i64_mean_total(total, value) {
            count = next_count;
            total = next_total;
            continue;
        }

        let mut mean = CompensatedMean::from_state(count, total as f64, 0.0);
        mean.accept(value as f64)?;
        for value in values {
            mean.accept(value as f64)?;
        }
        return Ok(mean.value());
    }
    Ok((count != 0).then(|| (total as f64) / (count as f64)))
}

/// Compute an identity-slice mean while deriving the common-path count from its known length.
#[inline]
pub(crate) fn run_i64_identity_mean_by<T, F>(
    values: &[T],
    value: F,
) -> Result<Option<f64>, KernelError>
where
    F: Fn(&T) -> i64,
{
    let mut total = 0_i64;
    for (index, item) in values.iter().enumerate() {
        let item = value(item);
        if let Some(next_total) = next_exact_i64_mean_total(total, item) {
            total = next_total;
            continue;
        }

        let count = u64::try_from(index).map_err(|_| KernelError::Overflow)?;
        let mut mean = CompensatedMean::from_state(count, total as f64, 0.0);
        mean.accept(item as f64)?;
        for item in &values[index + 1..] {
            mean.accept(value(item) as f64)?;
        }
        return Ok(mean.value());
    }
    let count = u64::try_from(values.len()).map_err(|_| KernelError::Overflow)?;
    Ok((count != 0).then(|| (total as f64) / (count as f64)))
}
