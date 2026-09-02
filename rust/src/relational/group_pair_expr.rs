//! Exact integer pair-expression grouping kernels.

#[cfg(not(Py_GIL_DISABLED))]
use super::group_numeric::{
    I64GroupPositions, exact_i64, group_allocation_error, new_dict_fallible,
};
use super::*;
use crate::integer::Instruction;
#[cfg(not(Py_GIL_DISABLED))]
use crate::pair::{PAIR_KEY_OPCODE, PreparedPairExpression, prepare_pair_arithmetic_expression};

#[cfg(not(Py_GIL_DISABLED))]
const ITEM_OPCODE: u8 = 0;
#[cfg(not(Py_GIL_DISABLED))]
const CONSTANT_OPCODE: u8 = 1;
#[cfg(not(Py_GIL_DISABLED))]
const MODULO_OPCODE: u8 = 6;

#[cfg(not(Py_GIL_DISABLED))]
type ComputedKeyGroups = Vec<(i64, Option<Py<PyAny>>, i128)>;

#[cfg(not(Py_GIL_DISABLED))]
#[derive(Clone, Copy)]
struct ModuloIdentityInput {
    index: usize,
    divisor: i64,
}

#[cfg(not(Py_GIL_DISABLED))]
fn modulo_identity_input(instructions: &[Instruction]) -> Option<ModuloIdentityInput> {
    match instructions {
        [
            (PAIR_KEY_OPCODE, 0),
            (CONSTANT_OPCODE, divisor),
            (MODULO_OPCODE, 0),
        ] => Some(ModuloIdentityInput {
            index: 0,
            divisor: *divisor,
        }),
        [
            (ITEM_OPCODE, 0),
            (CONSTANT_OPCODE, divisor),
            (MODULO_OPCODE, 0),
        ] => Some(ModuloIdentityInput {
            index: 1,
            divisor: *divisor,
        }),
        _ => None,
    }
}

#[pyfunction]
/// Group exact pair rows by two checked integer-expression programs.
pub(crate) fn group_sum_i64_pair_expr_rows_v1(
    source: &Bound<'_, PyAny>,
    key_instructions: Vec<Instruction>,
    value_instructions: Vec<Instruction>,
    key_name: &Bound<'_, PyAny>,
    output_name: &Bound<'_, PyAny>,
) -> PyResult<Option<(bool, Py<PyAny>)>> {
    #[cfg(Py_GIL_DISABLED)]
    {
        let _ = (
            source,
            key_instructions,
            value_instructions,
            key_name,
            output_name,
        );
        Ok(None)
    }

    #[cfg(not(Py_GIL_DISABLED))]
    {
        // Every speculative guard is exact and callback-free. Validate names and programs before
        // touching the retained source so a decline can replay the whole container in Python.
        let key_name = match key_name.cast_exact::<PyString>() {
            Ok(name) => name,
            Err(_) => return Ok(None),
        };
        let output_name = match output_name.cast_exact::<PyString>() {
            Ok(name) => name,
            Err(_) => return Ok(None),
        };
        let modulo_identity = modulo_identity_input(&key_instructions);
        if key_instructions
            .last()
            .is_some_and(|&(opcode, _)| opcode == MODULO_OPCODE)
            && modulo_identity.is_none()
        {
            return Ok(None);
        }
        let Some(key_expression) = prepare_pair_arithmetic_expression(key_instructions) else {
            return Ok(None);
        };
        let Some(value_expression) = prepare_pair_arithmetic_expression(value_instructions) else {
            return Ok(None);
        };
        let Some(groups) = group_sum_i64_pair_expr_rows(
            source,
            &key_expression,
            &value_expression,
            modulo_identity,
        )?
        else {
            return Ok(None);
        };

        let py = source.py();
        if groups.len() < GROUP_SUM_FINAL_ROWS_THRESHOLD {
            let mut pairs = Vec::new();
            pairs
                .try_reserve(groups.len())
                .map_err(group_allocation_error)?;
            for (key, key_object, total) in groups {
                let key_object =
                    key_object.unwrap_or_else(|| PyInt::new(py, key).into_any().unbind());
                pairs.push((key_object, total));
            }
            let pairs = PyList::new(py, pairs)?;
            return Ok(Some((false, pairs.into_any().unbind())));
        }

        let mut rows = Vec::new();
        rows.try_reserve(groups.len())
            .map_err(group_allocation_error)?;
        for (key, key_object, total) in groups {
            let row = new_dict_fallible(py)?;
            match key_object {
                Some(key_object) => row.set_item(key_name, key_object.bind(py))?,
                None => row.set_item(key_name, key)?,
            }
            set_widened_i64_item(&row, output_name, total)?;
            rows.push(row.unbind());
        }
        let rows = PyList::new(py, rows)?;
        Ok(Some((true, rows.into_any().unbind())))
    }
}

#[cfg(not(Py_GIL_DISABLED))]
fn group_sum_i64_pair_expr_rows(
    source: &Bound<'_, PyAny>,
    key_expression: &PreparedPairExpression,
    value_expression: &PreparedPairExpression,
    modulo_identity: Option<ModuloIdentityInput>,
) -> PyResult<Option<ComputedKeyGroups>> {
    if let Ok(rows) = source.cast_exact::<PyList>() {
        return group_exact_pair_expr_sequence(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: this kernel is GIL-only, so the exact list cannot mutate while the
                // attached native call borrows its rows.
                unsafe { ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            key_expression,
            value_expression,
            modulo_identity,
        );
    }
    if let Ok(rows) = source.cast_exact::<PyTuple>() {
        return group_exact_pair_expr_sequence(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: exact tuples are immutable and index is below their fixed length.
                unsafe { ffi::PyTuple_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            key_expression,
            value_expression,
            modulo_identity,
        );
    }
    Ok(None)
}

#[cfg(not(Py_GIL_DISABLED))]
fn group_exact_pair_expr_sequence(
    py: Python<'_>,
    row_count: usize,
    mut get_row: impl FnMut(usize) -> *mut ffi::PyObject,
    key_expression: &PreparedPairExpression,
    value_expression: &PreparedPairExpression,
    modulo_identity: Option<ModuloIdentityInput>,
) -> PyResult<Option<ComputedKeyGroups>> {
    let mut state = ComputedKeyGroupState::new(row_count);
    let mut key_stack = key_expression.stack();
    let mut value_stack = value_expression.stack();
    let modulo_divisor = modulo_identity.map(|identity| PyInt::new(py, identity.divisor));
    for index in 0..row_count {
        let row = get_row(index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
        // SAFETY: every row is held live by the GIL-protected exact list or immutable tuple.
        if unsafe { ffi::PyTuple_CheckExact(row) } == 0 {
            return Ok(None);
        }
        // SAFETY: row was proven to be an exact tuple.
        let width = unsafe { ffi::PyTuple_Size(row) };
        if width < 0 {
            return Err(PyErr::fetch(py));
        }
        if width != 2 {
            return Ok(None);
        }
        // SAFETY: the exact tuple has exactly two live items.
        let first = unsafe { ffi::PyTuple_GetItem(row, 0) };
        // SAFETY: as above for the second item.
        let second = unsafe { ffi::PyTuple_GetItem(row, 1) };
        if first.is_null() || second.is_null() {
            return Err(PyErr::fetch(py));
        }
        let Some(first_value) = exact_i64(py, first)? else {
            return Ok(None);
        };
        let Some(second_value) = exact_i64(py, second)? else {
            return Ok(None);
        };
        // Preserve Python's left-to-right selector evaluation: the key expression is evaluated
        // before the aggregate value expression for every admitted row.
        let Some(key) = key_expression.evaluate(first_value, second_value, &mut key_stack) else {
            return Ok(None);
        };
        // Canonical grouping locates or creates the key before it evaluates the aggregate input.
        // Exact integers cannot dispatch Python hashing code, but retaining that order also keeps
        // native allocation failures aligned with the Python execution boundary.
        let (position, inserted) = state.position(key)?;
        if inserted && let Some(identity) = modulo_identity {
            let (dividend_object, dividend_value) = if identity.index == 0 {
                (first, first_value)
            } else {
                (second, second_value)
            };
            if key == dividend_value {
                let divisor = modulo_divisor
                    .as_ref()
                    .expect("a modulo identity input always prepares its divisor");
                // SAFETY: both operands are live exact Python integers. PyNumber_Remainder
                // returns one owned reference or null with an exception set.
                let key_object =
                    unsafe { ffi::PyNumber_Remainder(dividend_object, divisor.as_ptr()) };
                if key_object.is_null() {
                    return Err(PyErr::fetch(py));
                }
                // SAFETY: the non-null result is one owned reference from PyNumber_Remainder.
                let key_object = unsafe { Bound::from_owned_ptr(py, key_object) };
                if exact_i64(py, key_object.as_ptr())? != Some(key) {
                    return Ok(None);
                }
                state.retain_key_object(position, key_object.unbind());
            }
        }
        let Some(value) = value_expression.evaluate(first_value, second_value, &mut value_stack)
        else {
            return Ok(None);
        };
        if state.add_at(position, value).is_none() {
            return Ok(None);
        }
    }
    Ok(Some(state.groups))
}

#[cfg(not(Py_GIL_DISABLED))]
struct ComputedKeyGroupState {
    positions: I64GroupPositions,
    groups: ComputedKeyGroups,
    dense_limit: usize,
}

#[cfg(not(Py_GIL_DISABLED))]
impl ComputedKeyGroupState {
    fn new(row_count: usize) -> Self {
        Self {
            positions: I64GroupPositions::Dense(Vec::new()),
            groups: Vec::new(),
            dense_limit: row_count
                .saturating_mul(MAX_DENSE_SLOTS_PER_ROW)
                .min(MAX_DENSE_GROUP_SLOTS),
        }
    }

    #[inline]
    fn position(&mut self, key: i64) -> PyResult<(usize, bool)> {
        if let Some(position) = self.positions.position(
            key,
            self.dense_limit,
            self.groups.iter().map(|(existing, _, _)| *existing),
        )? {
            return Ok((position, false));
        }

        self.positions.try_reserve_group()?;
        self.groups.try_reserve(1).map_err(group_allocation_error)?;
        let position = self.groups.len();
        self.positions.insert(key, position);
        self.groups.push((key, None, 0));
        Ok((position, true))
    }

    fn retain_key_object(&mut self, position: usize, key: Py<PyAny>) {
        debug_assert!(self.groups[position].1.is_none());
        self.groups[position].1 = Some(key);
    }

    #[inline]
    fn add_at(&mut self, position: usize, value: i64) -> Option<()> {
        let total = self.groups[position].2.checked_add(i128::from(value))?;
        self.groups[position].2 = total;
        Some(())
    }
}
