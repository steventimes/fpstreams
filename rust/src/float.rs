//! Floating-point kernels that fuse pipeline stages into streaming materializers and terminals.

use crate::common::{
    AGGREGATE_COUNT, AGGREGATE_FIRST, AGGREGATE_LAST, AGGREGATE_M2, AGGREGATE_MAXIMUM,
    AGGREGATE_MEAN, AGGREGATE_MINIMUM, AGGREGATE_TOTAL, CompensatedSum, KernelError,
    OnlineStatistics, validate_aggregate_mask,
};
use crate::numeric_mean::CompensatedMean;
use std::ops::ControlFlow;

mod affine_pair;
mod endpoints;

pub(crate) use affine_pair::run_f64_affine_comparison_pair_sum;
pub(crate) use endpoints::{
    aggregate_f64, aggregate_f64_buffer_masked_v1, aggregate_f64_buffer_masked_v2,
    aggregate_f64_masked, aggregate_f64_range, aggregate_f64_range_masked, count_f64,
    count_f64_range, execute_f64, execute_f64_buffer_v1, execute_f64_range, materialize_f64,
    materialize_f64_buffer_v1, materialize_f64_range, mean_f64, mean_f64_buffer_v1,
    mean_f64_buffer_v2, mean_f64_range, sequential_f64_aggregate_total_v1, statistics_f64,
    statistics_f64_range, terminal_f64, terminal_f64_buffer_v1, terminal_f64_buffer_v2,
    terminal_f64_probe, terminal_f64_range,
};

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
    expression: Option<PreparedFloatExpression>,
    remaining: u64,
    dropping: bool,
}

struct StatelessFloatMapFilter {
    mapper: PreparedFloatExpression,
    predicate: PreparedFloatExpression,
    negated: bool,
}

pub(crate) struct FloatComparison {
    pub(crate) item_left: bool,
    pub(crate) opcode: u8,
    pub(crate) operand: f64,
}

pub(crate) enum PreparedFloatExpression {
    /// An exact ``fitem * multiplier + offset`` map without interpreter dispatch.
    Affine { multiplier: f64, offset: f64 },
    /// One direct item/constant comparison.
    Comparison(FloatComparison),
    /// Two direct comparisons combined by boolean and/or.
    ComparisonPair {
        left: FloatComparison,
        right: FloatComparison,
        boolean_opcode: u8,
    },
    /// Every expression outside the conservative peephole vocabulary.
    Bytecode(Vec<FloatInstruction>),
}

fn parse_float_comparison(code: &[FloatInstruction]) -> Option<FloatComparison> {
    match code {
        [(0, _), (1, operand), (opcode @ 8..=13, _)] => Some(FloatComparison {
            item_left: true,
            opcode: *opcode,
            operand: *operand,
        }),
        [(1, operand), (0, _), (opcode @ 8..=13, _)] => Some(FloatComparison {
            item_left: false,
            opcode: *opcode,
            operand: *operand,
        }),
        _ => None,
    }
}

pub(crate) fn prepare_float_expression(code: Vec<FloatInstruction>) -> PreparedFloatExpression {
    if let [(0, _), (1, multiplier), (4, _), (1, offset), (2, _)] = code.as_slice() {
        return PreparedFloatExpression::Affine {
            multiplier: *multiplier,
            offset: *offset,
        };
    }
    if let Some(comparison) = parse_float_comparison(&code) {
        return PreparedFloatExpression::Comparison(comparison);
    }
    if let [left @ .., (boolean_opcode @ (14 | 15), _)] = code.as_slice()
        && left.len() == 6
        && let (Some(left), Some(right)) = (
            parse_float_comparison(&left[..3]),
            parse_float_comparison(&left[3..]),
        )
    {
        return PreparedFloatExpression::ComparisonPair {
            left,
            right,
            boolean_opcode: *boolean_opcode,
        };
    }
    PreparedFloatExpression::Bytecode(code)
}

impl FloatComparison {
    #[inline]
    fn evaluate(&self, value: f64) -> bool {
        let (left, right) = if self.item_left {
            (value, self.operand)
        } else {
            (self.operand, value)
        };
        match self.opcode {
            8 => left == right,
            9 => left != right,
            10 => left < right,
            11 => left <= right,
            12 => left > right,
            13 => left >= right,
            _ => unreachable!("prepared float comparison has an invalid opcode"),
        }
    }
}

impl PreparedFloatExpression {
    #[inline]
    pub(crate) fn evaluate(&self, value: f64, stack: &mut Vec<f64>) -> Result<f64, KernelError> {
        match self {
            Self::Affine { multiplier, offset } => Ok(value * *multiplier + *offset),
            Self::Comparison(comparison) => Ok(f64::from(comparison.evaluate(value))),
            Self::ComparisonPair {
                left,
                right,
                boolean_opcode,
            } => {
                let left = left.evaluate(value);
                let right = right.evaluate(value);
                Ok(f64::from(match boolean_opcode {
                    14 => left && right,
                    15 => left || right,
                    _ => unreachable!("prepared float boolean has an invalid opcode"),
                }))
            }
            Self::Bytecode(code) => evaluate_f64(value, code, stack),
        }
    }
}

impl FloatStage {
    #[inline]
    fn evaluate(&self, value: f64, stack: &mut Vec<f64>) -> Result<f64, KernelError> {
        self.expression
            .as_ref()
            .ok_or(KernelError::InvalidProgram("missing stage expression"))?
            .evaluate(value, stack)
    }
}

pub(crate) fn evaluate_f64(
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
            let expression =
                matches!(kind, 0 | 1 | 2 | 6 | 7 | 8).then(|| prepare_float_expression(code));
            Ok(FloatStage {
                kind,
                expression,
                remaining,
                dropping: kind == 7,
            })
        })
        .collect()
}

fn prepare_stateless_f64_map_filter(
    program: FloatProgram,
) -> Result<StatelessFloatMapFilter, KernelError> {
    let mut stages = program.into_iter();
    let (_, mapper) = stages
        .next()
        .ok_or(KernelError::InvalidProgram("missing float map stage"))?;
    let (filter_kind, predicate) = stages
        .next()
        .ok_or(KernelError::InvalidProgram("missing float filter stage"))?;
    Ok(StatelessFloatMapFilter {
        mapper: prepare_float_expression(mapper),
        predicate: prepare_float_expression(predicate),
        negated: filter_kind == 2,
    })
}

enum FloatProcessStop {
    Consumer,
    SourceComplete,
}

#[inline(always)]
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
                value = stage.evaluate(value, stack)?;
            }
            1 => {
                if stage.evaluate(value, stack)? == 0.0 {
                    emit_value = false;
                    break;
                }
            }
            2 => {
                if stage.evaluate(value, stack)? != 0.0 {
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
                if stage.evaluate(value, stack)? == 0.0 {
                    return Ok(ControlFlow::Break(FloatProcessStop::SourceComplete));
                }
            }
            7 => {
                if stage.dropping {
                    if stage.evaluate(value, stack)? != 0.0 {
                        emit_value = false;
                        break;
                    }
                    stage.dropping = false;
                }
            }
            8 => {
                if stage.evaluate(value, stack)? == 0.0 {
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

#[inline]
fn process_f64<I, F>(values: I, program: FloatProgram, mut emit: F) -> Result<(), KernelError>
where
    I: IntoIterator<Item = f64>,
    F: FnMut(f64) -> Result<ControlFlow<()>, KernelError>,
{
    if matches!(program.as_slice(), [(0, _), (1 | 2, _)]) {
        let map_filter = prepare_stateless_f64_map_filter(program)?;
        return process_stateless_f64_map_filter(values, map_filter, &mut emit);
    }
    process_general_f64(values, program, emit)
}

#[inline]
fn process_general_f64<I, F>(
    values: I,
    program: FloatProgram,
    mut emit: F,
) -> Result<(), KernelError>
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

#[inline(never)]
fn process_stateless_f64_map_filter<I, F>(
    values: I,
    map_filter: StatelessFloatMapFilter,
    emit: &mut F,
) -> Result<(), KernelError>
where
    I: IntoIterator<Item = f64>,
    F: FnMut(f64) -> Result<ControlFlow<()>, KernelError>,
{
    let StatelessFloatMapFilter {
        mapper,
        predicate,
        negated,
    } = map_filter;
    match (&mapper, &predicate) {
        (
            PreparedFloatExpression::Affine { multiplier, offset },
            PreparedFloatExpression::Comparison(comparison),
        ) => {
            for source_value in values {
                let value = source_value * *multiplier + *offset;
                if comparison.evaluate(value) == negated {
                    continue;
                }
                if emit(value)?.is_break() {
                    return Ok(());
                }
            }
            return Ok(());
        }
        (
            PreparedFloatExpression::Affine { multiplier, offset },
            PreparedFloatExpression::ComparisonPair {
                left,
                right,
                boolean_opcode,
            },
        ) => {
            for source_value in values {
                let value = source_value * *multiplier + *offset;
                let left_matches = left.evaluate(value);
                let right_matches = right.evaluate(value);
                let matches = match boolean_opcode {
                    14 => left_matches && right_matches,
                    15 => left_matches || right_matches,
                    _ => unreachable!("prepared float boolean has an invalid opcode"),
                };
                if matches == negated {
                    continue;
                }
                if emit(value)?.is_break() {
                    return Ok(());
                }
            }
            return Ok(());
        }
        _ => {}
    }

    let mut stack = Vec::new();
    for source_value in values {
        let value = mapper.evaluate(source_value, &mut stack)?;
        let matches = predicate.evaluate(value, &mut stack)? != 0.0;
        if matches == negated {
            continue;
        }
        if emit(value)?.is_break() {
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

pub(crate) fn run_f64_buffer_materialization(
    values: Vec<f64>,
    program: FloatProgram,
) -> Result<Vec<f64>, KernelError> {
    if program.is_empty() {
        return Ok(values);
    }
    run_f64(values, program)
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
    run_f64_terminal_state::<_, false>(values, program, terminal).map(|(_emitted, result)| result)
}

fn run_f64_terminal_state<I, const TRACK_EMITTED: bool>(
    values: I,
    program: FloatProgram,
    terminal: u8,
) -> Result<(u64, Option<f64>), KernelError>
where
    I: IntoIterator<Item = f64>,
{
    let values = if terminal == 1 {
        match run_f64_affine_comparison_pair_sum::<_, TRACK_EMITTED>(values, &program) {
            Ok(result) => return result,
            Err(values) => values,
        }
    } else {
        values
    };
    let mut result = match terminal {
        1 | 6 | 8 => Some(0.0),
        7 => Some(1.0),
        0 | 2..=5 => None,
        _ => {
            return Err(KernelError::InvalidProgram("unknown float terminal"));
        }
    };
    let mut compensation = 0.0;
    let mut emitted = 0_u64;
    process_f64(values, program, |value| {
        if TRACK_EMITTED {
            emitted = emitted.checked_add(1).ok_or(KernelError::Overflow)?;
        }
        let control = match terminal {
            0 => ControlFlow::Continue(()),
            1 | 8 => {
                let current = result.unwrap_or_default();
                let total = current + value;
                if terminal == 1 && total.is_finite() {
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
    Ok((emitted, result))
}

pub(crate) fn run_f64_identity_terminal<I>(
    values: I,
    terminal: u8,
) -> Result<(u64, Option<f64>), KernelError>
where
    I: IntoIterator<Item = f64>,
{
    let mut result = match terminal {
        1 | 6 | 8 => Some(0.0),
        7 => Some(1.0),
        0 | 2..=5 => None,
        _ => {
            return Err(KernelError::InvalidProgram("unknown float terminal"));
        }
    };
    let mut compensation = 0.0;
    let mut emitted = 0_u64;
    for value in values {
        emitted = emitted.checked_add(1).ok_or(KernelError::Overflow)?;
        let stop = match terminal {
            0 => false,
            1 | 8 => {
                let current = result.unwrap_or_default();
                let total = current + value;
                if terminal == 1 && total.is_finite() {
                    compensation += if current.abs() >= value.abs() {
                        current - total + value
                    } else {
                        value - total + current
                    };
                } else if terminal == 1 {
                    compensation = 0.0;
                }
                result = Some(total);
                false
            }
            2 => {
                result = Some(result.map_or(
                    value,
                    |current| {
                        if value < current { value } else { current }
                    },
                ));
                false
            }
            3 => {
                result = Some(result.map_or(
                    value,
                    |current| {
                        if value > current { value } else { current }
                    },
                ));
                false
            }
            4 => {
                result = Some(value);
                false
            }
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
        if stop {
            break;
        }
    }
    if terminal == 1 {
        result = result.map(|value| value + compensation);
    }
    Ok((emitted, result))
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

pub(crate) fn run_f64_mean<I>(values: I, program: FloatProgram) -> Result<Option<f64>, KernelError>
where
    I: IntoIterator<Item = f64>,
{
    let mut mean = CompensatedMean::default();
    process_f64(values, program, |value| {
        mean.accept(value)?;
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(mean.value())
}

pub(crate) fn run_f64_identity_mean<I>(values: I) -> Result<Option<f64>, KernelError>
where
    I: IntoIterator<Item = f64>,
{
    let mut mean = CompensatedMean::default();
    for value in values {
        mean.accept(value)?;
    }
    Ok(mean.value())
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
    let mut total = 0.0;
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
        total += value;
        statistics.accept(value)?;
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
    let mut total = 0.0;
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
        }
        if mask & AGGREGATE_TOTAL != 0 {
            total += value;
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
            total
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

pub(crate) fn run_f64_identity_aggregate_masked<I>(
    values: I,
    mask: u8,
) -> Result<F64AggregateSnapshot, KernelError>
where
    I: IntoIterator<Item = f64>,
{
    let mask = validate_aggregate_mask(mask)?;
    let needs_statistics = mask & (AGGREGATE_MEAN | AGGREGATE_M2) != 0;
    let mut count = 0_u64;
    let mut total = 0.0;
    let mut minimum = None;
    let mut maximum = None;
    let mut first = None;
    let mut last = None;
    let mut statistics = OnlineStatistics::default();

    for value in values {
        if needs_statistics {
            statistics.accept(value)?;
        } else {
            if mask & AGGREGATE_COUNT != 0 {
                count = count.checked_add(1).ok_or(KernelError::Overflow)?;
            }
        }
        if mask & AGGREGATE_TOTAL != 0 {
            total += value;
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
    }

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
            total
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
