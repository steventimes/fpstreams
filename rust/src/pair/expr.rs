//! Checked postfix expressions over exact `(i64, i64)` pair rows.

#[cfg(not(Py_GIL_DISABLED))]
use crate::integer::{Instruction, floor_div, modulo};

#[cfg(not(Py_GIL_DISABLED))]
pub(crate) const PAIR_KEY_OPCODE: u8 = 19;

#[cfg(not(Py_GIL_DISABLED))]
pub(crate) struct PreparedPairExpression {
    instructions: Vec<Instruction>,
    stack_capacity: usize,
}

#[cfg(not(Py_GIL_DISABLED))]
impl PreparedPairExpression {
    pub(crate) fn stack(&self) -> Vec<i64> {
        Vec::with_capacity(self.stack_capacity)
    }

    #[inline]
    pub(crate) fn evaluate(&self, key: i64, value: i64, stack: &mut Vec<i64>) -> Option<i64> {
        stack.clear();
        for &(opcode, operand) in &self.instructions {
            match opcode {
                0 => stack.push(value),
                1 => stack.push(operand),
                PAIR_KEY_OPCODE => stack.push(key),
                2..=6 | 8..=13 => {
                    let right = stack.pop()?;
                    let left = stack.pop()?;
                    let result = match opcode {
                        2 => left.checked_add(right)?,
                        3 => left.checked_sub(right)?,
                        4 => left.checked_mul(right)?,
                        5 => floor_div(left, right).ok()?,
                        6 => modulo(left, right).ok()?,
                        8 => i64::from(left == right),
                        9 => i64::from(left != right),
                        10 => i64::from(left < right),
                        11 => i64::from(left <= right),
                        12 => i64::from(left > right),
                        13 => i64::from(left >= right),
                        _ => unreachable!(),
                    };
                    stack.push(result);
                }
                7 | 16 | 17 => {
                    let value = stack.pop()?;
                    let result = match opcode {
                        7 => value.checked_neg()?,
                        16 => i64::from(value == 0),
                        17 => value.checked_abs()?,
                        _ => unreachable!(),
                    };
                    stack.push(result);
                }
                _ => return None,
            }
        }
        stack.pop()
    }
}

#[cfg(not(Py_GIL_DISABLED))]
fn prepare_pair_expression(
    instructions: Vec<Instruction>,
    arithmetic_only: bool,
    require_pair_reference: bool,
) -> Option<PreparedPairExpression> {
    if instructions.is_empty() {
        return None;
    }

    let mut depth = 0_usize;
    let mut stack_capacity = 0_usize;
    let mut references_pair = false;
    for &(opcode, _) in &instructions {
        match opcode {
            0 | PAIR_KEY_OPCODE => {
                references_pair = true;
                depth = depth.checked_add(1)?;
                stack_capacity = stack_capacity.max(depth);
            }
            1 => {
                depth = depth.checked_add(1)?;
                stack_capacity = stack_capacity.max(depth);
            }
            2..=6 => {
                if depth < 2 {
                    return None;
                }
                depth -= 1;
            }
            8..=13 if !arithmetic_only => {
                if depth < 2 {
                    return None;
                }
                depth -= 1;
            }
            7 | 17 => {
                if depth == 0 {
                    return None;
                }
            }
            16 if !arithmetic_only => {
                if depth == 0 {
                    return None;
                }
            }
            _ => return None,
        }
    }
    if depth != 1 || (require_pair_reference && !references_pair) {
        return None;
    }
    Some(PreparedPairExpression {
        instructions,
        stack_capacity,
    })
}

/// Prepare the arithmetic/relational grammar accepted by pair-row filtering.
#[cfg(not(Py_GIL_DISABLED))]
pub(crate) fn prepare_pair_predicate_expression(
    instructions: Vec<Instruction>,
) -> Option<PreparedPairExpression> {
    prepare_pair_expression(instructions, false, true)
}

/// Prepare a pure integer-arithmetic expression for a grouped key or value.
#[cfg(not(Py_GIL_DISABLED))]
pub(crate) fn prepare_pair_arithmetic_expression(
    instructions: Vec<Instruction>,
) -> Option<PreparedPairExpression> {
    prepare_pair_expression(instructions, true, false)
}
