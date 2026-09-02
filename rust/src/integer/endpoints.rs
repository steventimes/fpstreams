//! Python-facing adapters for signed-integer kernels.

use super::*;
#[cfg(not(Py_GIL_DISABLED))]
use crate::common::acquire_i64_buffer;
use crate::common::{
    extract_i64_buffer, extract_i64_container, kernel_error, materialize_target,
    materialize_values, snapshot_exact_container_prefix,
};
use crate::numeric_mean::{mean_i64_container, sum_i64_container};
#[cfg(not(Py_GIL_DISABLED))]
use pyo3::exceptions::PyOverflowError;
use pyo3::exceptions::{PyMemoryError, PyTypeError, PyValueError};
#[cfg(not(Py_GIL_DISABLED))]
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyDictMethods, PyInt, PyIterator, PyList, PyTuple};
use std::collections::HashMap;

pub(crate) struct I64Range {
    pub(crate) current: i64,
    pub(crate) stop: i64,
    pub(crate) step: i64,
}

struct I64FrequencyGroup {
    first_key: Py<PyAny>,
    count: usize,
}

const MAX_NATIVE_FREQUENCY_GROUPS: usize = 256;

fn frequency_allocation_error(_error: std::collections::TryReserveError) -> PyErr {
    PyMemoryError::new_err("could not allocate native frequency groups")
}

fn add_exact_i64_frequency(
    item: &Bound<'_, PyAny>,
    positions: &mut HashMap<i64, usize, SeededI64BuildHasher>,
    groups: &mut Vec<I64FrequencyGroup>,
) -> PyResult<bool> {
    if !item.is_exact_instance_of::<PyInt>() {
        return Ok(false);
    }
    let Ok(value) = item.extract::<i64>() else {
        return Ok(false);
    };
    if let Some(&position) = positions.get(&value) {
        groups[position].count += 1;
        return Ok(true);
    }
    if groups.len() == MAX_NATIVE_FREQUENCY_GROUPS {
        return Ok(false);
    }

    positions
        .try_reserve(1)
        .map_err(frequency_allocation_error)?;
    groups.try_reserve(1).map_err(frequency_allocation_error)?;
    let position = groups.len();
    positions.insert(value, position);
    groups.push(I64FrequencyGroup {
        first_key: item.clone().unbind(),
        count: 1,
    });
    Ok(true)
}

fn materialize_frequency_groups<'py>(
    py: Python<'py>,
    groups: Vec<I64FrequencyGroup>,
) -> PyResult<Bound<'py, PyDict>> {
    let counts = PyDict::new(py);
    for group in groups {
        counts.set_item(group.first_key, group.count)?;
    }
    Ok(counts)
}

fn partial_frequency_result(
    source: &Bound<'_, PyAny>,
    groups: Vec<I64FrequencyGroup>,
    resume_at: usize,
) -> PyResult<Option<Py<PyAny>>> {
    let py = source.py();
    let counts = materialize_frequency_groups(py, groups)?;
    let remainder = PyIterator::from_object(source)?;
    remainder.call_method1("__setstate__", (resume_at,))?;
    let partial = PyTuple::new(py, [counts.as_any(), remainder.as_any()])?;
    Ok(Some(partial.into_any().unbind()))
}

/// Count a bounded exact-i64 prefix, retaining resumable state for a canonical fallback.
#[pyfunction]
pub(crate) fn frequencies_i64_exact_v1(source: &Bound<'_, PyAny>) -> PyResult<Option<Py<PyAny>>> {
    let mut positions = HashMap::with_hasher(SeededI64BuildHasher::random());
    let mut groups = Vec::new();
    if let Ok(values) = source.cast_exact::<PyList>() {
        for (index, item) in values.iter().enumerate() {
            if !add_exact_i64_frequency(&item, &mut positions, &mut groups)? {
                return partial_frequency_result(source, groups, index);
            }
        }
    } else if let Ok(values) = source.cast_exact::<PyTuple>() {
        for (index, item) in values.iter().enumerate() {
            if !add_exact_i64_frequency(&item, &mut positions, &mut groups)? {
                return partial_frequency_result(source, groups, index);
            }
        }
    } else {
        return Ok(None);
    }

    let counts = materialize_frequency_groups(source.py(), groups)?;
    Ok(Some(counts.into_any().unbind()))
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

/// Snapshot an external i64 buffer before running a fused pipeline.
#[pyfunction]
pub(crate) fn execute_i64_buffer_v1(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: Program,
) -> PyResult<Vec<i64>> {
    let values = extract_i64_buffer(values)?;
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

#[cfg(not(Py_GIL_DISABLED))]
fn validate_direct_i64_expression(
    instructions: &[Instruction],
) -> Result<(usize, bool), KernelError> {
    let mut depth = 0_usize;
    let mut stack_capacity = 0_usize;
    let mut contains_item = false;
    for &(opcode, _) in instructions {
        match opcode {
            0 | 1 => {
                depth = depth.checked_add(1).ok_or(KernelError::InvalidProgram(
                    "expression stack capacity overflow",
                ))?;
                stack_capacity = stack_capacity.max(depth);
                contains_item |= opcode == 0;
            }
            2..=6 | 8..=15 => {
                if depth < 2 {
                    return Err(KernelError::InvalidProgram("expression stack underflow"));
                }
                depth -= 1;
            }
            7 | 16 | 17 => {
                if depth == 0 {
                    return Err(KernelError::InvalidProgram("expression stack underflow"));
                }
            }
            _ => return Err(KernelError::InvalidProgram("unknown expression opcode")),
        }
    }
    if depth != 1 {
        return Err(KernelError::InvalidProgram(
            "expression must leave exactly one value",
        ));
    }
    Ok((stack_capacity, contains_item))
}

#[cfg(not(Py_GIL_DISABLED))]
fn validate_direct_i64_map(instructions: &[Instruction]) -> Result<usize, KernelError> {
    let (stack_capacity, contains_item) = validate_direct_i64_expression(instructions)?;
    if !contains_item {
        return Err(KernelError::InvalidProgram(
            "direct map expression must reference item",
        ));
    }
    if !matches!(instructions.last(), Some((2..=5 | 7, _))) {
        return Err(KernelError::InvalidProgram(
            "direct map expression root must be add, subtract, multiply, floor-divide, or negate",
        ));
    }
    Ok(stack_capacity)
}

#[cfg(not(Py_GIL_DISABLED))]
fn direct_i64_expression_stack(
    expression: &PreparedExpression,
    stack_capacity: usize,
) -> PyResult<Vec<i64>> {
    let mut stack = Vec::new();
    if matches!(expression, PreparedExpression::Bytecode(_)) {
        stack.try_reserve_exact(stack_capacity).map_err(|_| {
            PyMemoryError::new_err("could not allocate direct i64 expression stack")
        })?;
    }
    Ok(stack)
}

/// Materialize one validated exact-integer map directly into a fresh Python list.
#[cfg(not(Py_GIL_DISABLED))]
#[pyfunction]
pub(crate) fn materialize_i64_map_exact_list_v1(
    py: Python<'_>,
    source: &Bound<'_, PyAny>,
    instructions: Vec<Instruction>,
) -> PyResult<Option<Py<PyAny>>> {
    let stack_capacity = validate_direct_i64_map(&instructions).map_err(kernel_error)?;
    let is_list = if source.cast_exact::<PyList>().is_ok() {
        true
    } else if source.cast_exact::<PyTuple>().is_ok() {
        false
    } else {
        return Ok(None);
    };
    let expression = prepare_expression(instructions);
    let mut stack = direct_i64_expression_stack(&expression, stack_capacity)?;

    // Signal handlers run only while no private result exists or after it is fully initialized.
    // The attached fill itself contains no callbacks, signal checks, or detached work.
    py.check_signals()?;
    let length = unsafe {
        if is_list {
            ffi::PyList_Size(source.as_ptr())
        } else {
            ffi::PyTuple_Size(source.as_ptr())
        }
    };
    if length < 0 {
        return Err(PyErr::fetch(py));
    }
    // SAFETY: PyList_New returns one owned reference or sets a Python exception. Keeping the
    // result in Bound makes every successfully filled slot and every remaining null slot safe
    // to release together on a later decline.
    let output = unsafe { Bound::from_owned_ptr_or_err(py, ffi::PyList_New(length))? };

    for index in 0..length {
        // SAFETY: source is an exact list/tuple held under the GIL for the entire fill.
        let item = unsafe {
            if is_list {
                ffi::PyList_GetItem(source.as_ptr(), index)
            } else {
                ffi::PyTuple_GetItem(source.as_ptr(), index)
            }
        };
        if item.is_null() {
            return Err(PyErr::fetch(py));
        }
        if unsafe { ffi::PyLong_CheckExact(item) } == 0 {
            return Ok(None);
        }
        let value = unsafe { ffi::PyLong_AsLongLong(item) };
        if value == -1 && !unsafe { ffi::PyErr_Occurred() }.is_null() {
            let error = PyErr::fetch(py);
            if error.is_instance_of::<PyOverflowError>(py) {
                return Ok(None);
            }
            return Err(error);
        }
        let mapped = match expression.evaluate(value, &mut stack) {
            Ok(mapped) => mapped,
            Err(KernelError::Overflow | KernelError::DivisionByZero) => return Ok(None),
            Err(error @ KernelError::InvalidProgram(_)) => return Err(kernel_error(error)),
        };
        // LP64 exposes the narrower PyLong constructor while LLP64 retains the full-width ABI.
        let mapped_pointer = unsafe {
            if std::mem::size_of::<std::os::raw::c_long>() == std::mem::size_of::<i64>() {
                ffi::PyLong_FromLong(mapped as std::os::raw::c_long)
            } else {
                ffi::PyLong_FromLongLong(mapped)
            }
        };
        let mapped_object = unsafe { Bound::from_owned_ptr_or_err(py, mapped_pointer)? };
        // SAFETY: index names a null slot in this new list. Stable-ABI PyList_SetItem steals
        // mapped_object on both success and failure, so no Rust owner may wrap that pointer.
        if unsafe { ffi::PyList_SetItem(output.as_ptr(), index, mapped_object.into_ptr()) } != 0 {
            return Err(PyErr::fetch(py));
        }
    }
    py.check_signals()?;
    Ok(Some(output.unbind()))
}

/// Filter one exact-integer sequence directly into a list of its original Python objects.
#[cfg(not(Py_GIL_DISABLED))]
#[pyfunction]
pub(crate) fn materialize_i64_filter_exact_list_v1(
    py: Python<'_>,
    source: &Bound<'_, PyAny>,
    instructions: Vec<Instruction>,
    negated: bool,
) -> PyResult<Option<Py<PyAny>>> {
    let (stack_capacity, _) =
        validate_direct_i64_expression(&instructions).map_err(kernel_error)?;
    let is_list = if source.cast_exact::<PyList>().is_ok() {
        true
    } else if source.cast_exact::<PyTuple>().is_ok() {
        false
    } else {
        return Ok(None);
    };
    let expression = prepare_expression(instructions);
    let mut stack = direct_i64_expression_stack(&expression, stack_capacity)?;

    py.check_signals()?;
    let length = unsafe {
        if is_list {
            ffi::PyList_Size(source.as_ptr())
        } else {
            ffi::PyTuple_Size(source.as_ptr())
        }
    };
    if length < 0 {
        return Err(PyErr::fetch(py));
    }
    // SAFETY: PyList_New returns one owned reference or sets a Python exception. An empty list
    // remains valid after every successful append, so Bound can release it on any later error.
    let output = unsafe { Bound::from_owned_ptr_or_err(py, ffi::PyList_New(0))? };

    for index in 0..length {
        // SAFETY: source is an exact list/tuple held under the GIL. Both getters return a
        // borrowed reference; PyList_Append increments it when the predicate keeps the item.
        let item = unsafe {
            if is_list {
                ffi::PyList_GetItem(source.as_ptr(), index)
            } else {
                ffi::PyTuple_GetItem(source.as_ptr(), index)
            }
        };
        if item.is_null() {
            return Err(PyErr::fetch(py));
        }
        if unsafe { ffi::PyLong_CheckExact(item) } == 0 {
            return Ok(None);
        }
        let value = unsafe { ffi::PyLong_AsLongLong(item) };
        if value == -1 && !unsafe { ffi::PyErr_Occurred() }.is_null() {
            let error = PyErr::fetch(py);
            if error.is_instance_of::<PyOverflowError>(py) {
                return Ok(None);
            }
            return Err(error);
        }
        let predicate = match expression.evaluate(value, &mut stack) {
            Ok(predicate) => predicate != 0,
            Err(KernelError::Overflow | KernelError::DivisionByZero) => return Ok(None),
            Err(error @ KernelError::InvalidProgram(_)) => return Err(kernel_error(error)),
        };
        if predicate == negated {
            continue;
        }
        // SAFETY: output is the private exact list created above. Stable-ABI PyList_Append
        // borrows item and increments its reference on success; it never steals this pointer.
        if unsafe { ffi::PyList_Append(output.as_ptr(), item) } != 0 {
            return Err(PyErr::fetch(py));
        }
    }
    py.check_signals()?;
    Ok(Some(output.unbind()))
}

/// Snapshot an external i64 buffer and build its fused terminal collection once.
#[pyfunction]
pub(crate) fn materialize_i64_buffer_v1(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: Program,
    target: u8,
) -> PyResult<Py<PyAny>> {
    let target = materialize_target(target)?;
    let values = extract_i64_buffer(values)?;
    let output = py
        .detach(move || run_i64_buffer_materialization(values, program))
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
) -> PyResult<Option<i128>> {
    if program.is_empty() && terminal == 1 {
        return sum_i64_container(values).map(Some);
    }
    let values = extract_i64_container(values)?;
    let result = py
        .detach(move || run_terminal(values, program, terminal))
        .map_err(kernel_error)?;
    Ok(result.map(i128::from))
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
pub(crate) fn mean_i64(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: Program,
) -> PyResult<Option<f64>> {
    if program.is_empty() {
        return mean_i64_container(values);
    }
    let values = extract_i64_container(values)?;
    py.detach(move || run_i64_mean(values, program))
        .map_err(kernel_error)
}

#[pyfunction]
pub(crate) fn mean_i64_range(
    py: Python<'_>,
    start: i64,
    stop: i64,
    step: i64,
    program: Program,
) -> PyResult<Option<f64>> {
    if step == 0 {
        return Err(PyValueError::new_err("range step cannot be zero"));
    }
    let values = I64Range {
        current: start,
        stop,
        step,
    };
    py.detach(move || run_i64_mean(values, program))
        .map_err(kernel_error)
}

/// Reduce an owned snapshot of a validated external i64 buffer through the compensated mean state.
#[pyfunction]
pub(crate) fn mean_i64_buffer_v1(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: Program,
) -> PyResult<Option<f64>> {
    let values = extract_i64_buffer(values)?;
    if program.is_empty() {
        return py
            .detach(move || run_i64_identity_mean_by(&values, |value| *value))
            .map_err(kernel_error);
    }
    py.detach(move || run_i64_mean(values, program))
        .map_err(kernel_error)
}

/// Reduce an i64 buffer without allocating on GIL-enabled builds.
///
/// The buffer protocol keeps the export valid while every compiled stage and the terminal run
/// under the GIL. Free-threaded Python retains the owned-snapshot path because attachment alone
/// does not exclude concurrent writers.
#[pyfunction]
pub(crate) fn mean_i64_buffer_v2(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: Program,
) -> PyResult<Option<f64>> {
    #[cfg(not(Py_GIL_DISABLED))]
    {
        if program.is_empty() {
            let result = {
                let buffer = acquire_i64_buffer(values)?;
                if buffer.item_count() == 0 {
                    run_i64_identity_mean(std::iter::empty())
                } else {
                    let slice = buffer.as_slice(py).ok_or_else(|| {
                        PyTypeError::new_err(
                            "native i64 buffers require one C-contiguous dimension",
                        )
                    })?;
                    run_i64_identity_mean_by(slice, |value| value.get())
                }
            };
            py.check_signals()?;
            return result.map_err(kernel_error);
        }
        let result = {
            let buffer = acquire_i64_buffer(values)?;
            if buffer.item_count() == 0 {
                run_i64_mean(std::iter::empty(), program)
            } else {
                let slice = buffer.as_slice(py).ok_or_else(|| {
                    PyTypeError::new_err("native i64 buffers require one C-contiguous dimension")
                })?;
                run_i64_mean(slice.iter().map(|value| value.get()), program)
            }
        };
        py.check_signals()?;
        result.map_err(kernel_error)
    }
    #[cfg(Py_GIL_DISABLED)]
    {
        mean_i64_buffer_v1(py, values, program)
    }
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

/// Reduce an owned snapshot of a validated external i64 buffer into requested aggregate fields.
#[pyfunction]
pub(crate) fn aggregate_i64_buffer_masked_v1(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: Program,
    mask: u8,
) -> PyResult<I64AggregateSnapshot> {
    let values = extract_i64_buffer(values)?;
    if program.is_empty() {
        return py
            .detach(move || run_i64_identity_aggregate_masked(values, mask))
            .map_err(kernel_error);
    }
    py.detach(move || run_i64_aggregate_masked(values, program, mask))
        .map_err(kernel_error)
}

/// Reduce an i64 buffer directly through its pinned export on GIL-enabled builds.
///
/// Attached builds keep both the GIL and the export until every compiled stage finishes, then
/// release the export before checking signals. Free-threaded builds retain the owned snapshot.
#[pyfunction]
pub(crate) fn aggregate_i64_buffer_masked_v2(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    program: Program,
    mask: u8,
) -> PyResult<I64AggregateSnapshot> {
    #[cfg(not(Py_GIL_DISABLED))]
    {
        if program.is_empty() {
            let result = {
                let buffer = acquire_i64_buffer(values)?;
                if buffer.item_count() == 0 {
                    run_i64_identity_aggregate_masked(std::iter::empty(), mask)
                } else {
                    let slice = buffer.as_slice(py).ok_or_else(|| {
                        PyTypeError::new_err(
                            "native i64 buffers require one C-contiguous dimension",
                        )
                    })?;
                    match mask {
                        AGGREGATE_MINIMUM => Ok((
                            0,
                            0,
                            reduce_i64_min_by(slice, |value| value.get()),
                            None,
                            None,
                            None,
                            0.0,
                            0.0,
                        )),
                        AGGREGATE_MAXIMUM => Ok((
                            0,
                            0,
                            None,
                            reduce_i64_max_by(slice, |value| value.get()),
                            None,
                            None,
                            0.0,
                            0.0,
                        )),
                        _ => run_i64_identity_aggregate_masked(
                            slice.iter().map(|value| value.get()),
                            mask,
                        ),
                    }
                }
            };
            py.check_signals()?;
            return result.map_err(kernel_error);
        }
        let result = {
            let buffer = acquire_i64_buffer(values)?;
            if buffer.item_count() == 0 {
                run_i64_aggregate_masked(std::iter::empty(), program, mask)
            } else {
                let slice = buffer.as_slice(py).ok_or_else(|| {
                    PyTypeError::new_err("native i64 buffers require one C-contiguous dimension")
                })?;
                run_i64_aggregate_masked(slice.iter().map(|value| value.get()), program, mask)
            }
        };
        py.check_signals()?;
        result.map_err(kernel_error)
    }
    #[cfg(Py_GIL_DISABLED)]
    {
        aggregate_i64_buffer_masked_v1(py, values, program, mask)
    }
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
