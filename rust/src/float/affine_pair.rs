//! Affine-map and comparison-pair specialization for compensated sums.

use super::*;

struct AffineComparisonPair {
    multiplier: f64,
    offset: f64,
    left: FloatComparison,
    right: FloatComparison,
    negated: bool,
}

fn normalize_float_comparison(mut comparison: FloatComparison) -> FloatComparison {
    if comparison.item_left {
        return comparison;
    }
    comparison.item_left = true;
    comparison.opcode = match comparison.opcode {
        8 | 9 => comparison.opcode,
        10 => 12,
        11 => 13,
        12 => 10,
        13 => 11,
        _ => unreachable!("prepared float comparison has an invalid opcode"),
    };
    comparison
}

fn prepare_affine_comparison_pair(program: &FloatProgram) -> Option<AffineComparisonPair> {
    let [(0, mapper), (filter_kind @ (1 | 2), predicate)] = program.as_slice() else {
        return None;
    };
    let [(0, _), (1, multiplier), (4, _), (1, offset), (2, _)] = mapper.as_slice() else {
        return None;
    };
    let [comparisons @ .., (14, _)] = predicate.as_slice() else {
        return None;
    };
    if comparisons.len() != 6 {
        return None;
    }
    let left = normalize_float_comparison(parse_float_comparison(&comparisons[..3])?);
    let right = normalize_float_comparison(parse_float_comparison(&comparisons[3..])?);
    Some(AffineComparisonPair {
        multiplier: *multiplier,
        offset: *offset,
        left,
        right,
        negated: *filter_kind == 2,
    })
}

#[inline(always)]
fn run_affine_comparison_pair_sum_loop<I, L, R, const TRACK_EMITTED: bool>(
    values: I,
    prepared: AffineComparisonPair,
    left: L,
    right: R,
) -> Result<(u64, Option<f64>), KernelError>
where
    I: IntoIterator<Item = f64>,
    L: Fn(f64, f64) -> bool,
    R: Fn(f64, f64) -> bool,
{
    let mut total = CompensatedSum::default();
    let mut emitted = 0_u64;
    for source_value in values {
        let value = source_value * prepared.multiplier + prepared.offset;
        let left_matches = left(value, prepared.left.operand);
        let right_matches = right(value, prepared.right.operand);
        if (left_matches && right_matches) == prepared.negated {
            continue;
        }
        if TRACK_EMITTED {
            emitted = emitted.checked_add(1).ok_or(KernelError::Overflow)?;
        }
        total.accept(value);
    }
    Ok((emitted, Some(total.value())))
}

#[inline(always)]
fn dispatch_affine_comparison_pair_sum_right<I, L, const TRACK_EMITTED: bool>(
    values: I,
    prepared: AffineComparisonPair,
    left: L,
) -> Result<(u64, Option<f64>), KernelError>
where
    I: IntoIterator<Item = f64>,
    L: Fn(f64, f64) -> bool,
{
    match prepared.right.opcode {
        8 => run_affine_comparison_pair_sum_loop::<_, _, _, TRACK_EMITTED>(
            values,
            prepared,
            left,
            |value, operand| value == operand,
        ),
        9 => run_affine_comparison_pair_sum_loop::<_, _, _, TRACK_EMITTED>(
            values,
            prepared,
            left,
            |value, operand| value != operand,
        ),
        10 => run_affine_comparison_pair_sum_loop::<_, _, _, TRACK_EMITTED>(
            values,
            prepared,
            left,
            |value, operand| value < operand,
        ),
        11 => run_affine_comparison_pair_sum_loop::<_, _, _, TRACK_EMITTED>(
            values,
            prepared,
            left,
            |value, operand| value <= operand,
        ),
        12 => run_affine_comparison_pair_sum_loop::<_, _, _, TRACK_EMITTED>(
            values,
            prepared,
            left,
            |value, operand| value > operand,
        ),
        13 => run_affine_comparison_pair_sum_loop::<_, _, _, TRACK_EMITTED>(
            values,
            prepared,
            left,
            |value, operand| value >= operand,
        ),
        _ => unreachable!("prepared float comparison has an invalid opcode"),
    }
}

fn dispatch_affine_comparison_pair_sum<I, const TRACK_EMITTED: bool>(
    values: I,
    prepared: AffineComparisonPair,
) -> Result<(u64, Option<f64>), KernelError>
where
    I: IntoIterator<Item = f64>,
{
    match prepared.left.opcode {
        8 => dispatch_affine_comparison_pair_sum_right::<_, _, TRACK_EMITTED>(
            values,
            prepared,
            |value, operand| value == operand,
        ),
        9 => dispatch_affine_comparison_pair_sum_right::<_, _, TRACK_EMITTED>(
            values,
            prepared,
            |value, operand| value != operand,
        ),
        10 => dispatch_affine_comparison_pair_sum_right::<_, _, TRACK_EMITTED>(
            values,
            prepared,
            |value, operand| value < operand,
        ),
        11 => dispatch_affine_comparison_pair_sum_right::<_, _, TRACK_EMITTED>(
            values,
            prepared,
            |value, operand| value <= operand,
        ),
        12 => dispatch_affine_comparison_pair_sum_right::<_, _, TRACK_EMITTED>(
            values,
            prepared,
            |value, operand| value > operand,
        ),
        13 => dispatch_affine_comparison_pair_sum_right::<_, _, TRACK_EMITTED>(
            values,
            prepared,
            |value, operand| value >= operand,
        ),
        _ => unreachable!("prepared float comparison has an invalid opcode"),
    }
}

/// Attempt the allocation-free affine-map/comparison-pair compensated-sum loop.
///
/// The outer result returns the untouched iterator when the program shape is unsupported, so the
/// generic terminal path can retain its exact behavior without reopening the source.
pub(crate) fn run_f64_affine_comparison_pair_sum<I, const TRACK_EMITTED: bool>(
    values: I,
    program: &FloatProgram,
) -> Result<Result<(u64, Option<f64>), KernelError>, I>
where
    I: IntoIterator<Item = f64>,
{
    let Some(prepared) = prepare_affine_comparison_pair(program) else {
        return Err(values);
    };
    Ok(dispatch_affine_comparison_pair_sum::<_, TRACK_EMITTED>(
        values, prepared,
    ))
}
