//! Mean-only state and exact-container reductions.

use crate::common::{KernelError, kernel_error};
#[cfg(not(Py_GIL_DISABLED))]
use crate::common::{acquire_f64_buffer, acquire_i64_buffer};
#[cfg(Py_GIL_DISABLED)]
use crate::common::{extract_f64_buffer, extract_i64_buffer};
#[cfg(not(Py_GIL_DISABLED))]
use crate::relational::exact_python_function_code;
#[cfg(not(Py_GIL_DISABLED))]
use pyo3::PyTypeInfo;
use pyo3::exceptions::PyTypeError;
use pyo3::ffi;
use pyo3::prelude::*;
#[cfg(not(Py_GIL_DISABLED))]
use pyo3::sync::PyOnceLock;
#[cfg(Py_GIL_DISABLED)]
use pyo3::sync::critical_section::with_critical_section;
#[cfg(not(Py_GIL_DISABLED))]
use pyo3::types::{PyDict, PyFunction, PyInt, PyRange, PyString, PyType};
use pyo3::types::{PyList, PyTuple};

#[cfg(not(Py_GIL_DISABLED))]
const EXACT_ITERATOR_MEAN_CHUNK_ROWS: usize = 4096;

/// Count values while retaining only the compensated sum needed by a mean.
///
/// Variance terminals use ``OnlineStatistics``. Keeping a smaller state for
/// mean-only terminals avoids two Welford updates per emitted value.
#[derive(Default)]
pub(crate) struct CompensatedMean {
    count: u64,
    total: f64,
    compensation: f64,
}

impl CompensatedMean {
    #[inline]
    pub(crate) fn from_state(count: u64, total: f64, compensation: f64) -> Self {
        Self {
            count,
            total,
            compensation,
        }
    }

    #[inline]
    pub(crate) fn accept(&mut self, value: f64) -> Result<(), KernelError> {
        self.count = self.count.checked_add(1).ok_or(KernelError::Overflow)?;
        let combined = self.total + value;
        if combined.is_finite() {
            self.compensation += if self.total.abs() >= value.abs() {
                self.total - combined + value
            } else {
                value - combined + self.total
            };
        } else {
            self.compensation = 0.0;
        }
        self.total = combined;
        Ok(())
    }

    #[inline]
    pub(crate) fn value(&self) -> Option<f64> {
        (self.count != 0).then(|| (self.total + self.compensation) / (self.count as f64))
    }

    #[inline]
    pub(crate) fn state(&self) -> (u64, f64, f64) {
        (self.count, self.total, self.compensation)
    }
}

type ExactIteratorMeanChunk = (u8, u64, f64, f64, Option<Py<PyAny>>);

#[cfg(not(Py_GIL_DISABLED))]
static EXACT_BUILTIN_ITERATOR_TYPES: PyOnceLock<[Py<PyType>; 4]> = PyOnceLock::new();

#[cfg(not(Py_GIL_DISABLED))]
fn exact_builtin_iterator_types(py: Python<'_>) -> PyResult<&[Py<PyType>; 4]> {
    EXACT_BUILTIN_ITERATOR_TYPES.get_or_try_init(py, || {
        let list_iterator = PyList::empty(py).try_iter()?.get_type().clone().unbind();
        let tuple_iterator = PyTuple::empty(py).try_iter()?.get_type().clone().unbind();
        let range_iterator = PyRange::new(py, 0, 1)?
            .try_iter()?
            .get_type()
            .clone()
            .unbind();
        let large_start = PyInt::new(py, 1_i128 << 100);
        let large_stop = PyInt::new(py, (1_i128 << 100) + 1);
        let long_range_iterator = PyRange::type_object(py)
            .call1((large_start, large_stop))?
            .try_iter()?
            .get_type()
            .clone()
            .unbind();
        Ok([
            list_iterator,
            tuple_iterator,
            range_iterator,
            long_range_iterator,
        ])
    })
}

#[cfg(not(Py_GIL_DISABLED))]
fn exact_mean_dependencies_are_live(
    py: Python<'_>,
    bindings: &Bound<'_, PyTuple>,
    mean_function: &Bound<'_, PyAny>,
    mean_code: &Bound<'_, PyAny>,
) -> PyResult<bool> {
    let mean_function = mean_function.cast_exact::<PyFunction>();
    if !bindings.get_type().is(PyTuple::type_object(py)) || mean_function.is_err() {
        return Err(PyTypeError::new_err(
            "native iterator mean requires canonical dependency metadata",
        ));
    }
    let mean_function = mean_function.expect("the exact function check succeeded above");
    for binding in bindings.iter() {
        let binding = binding.cast_exact::<PyTuple>().map_err(|_| {
            PyTypeError::new_err("native iterator mean dependency entries must be exact tuples")
        })?;
        if binding.len() != 4 {
            return Err(PyTypeError::new_err(
                "native iterator mean dependency entries require four fields",
            ));
        }
        let primary_item = binding.get_item(0)?;
        let primary = primary_item.cast_exact::<PyDict>().map_err(|_| {
            PyTypeError::new_err("native iterator mean dependency namespaces must be exact dicts")
        })?;
        let fallback = binding.get_item(1)?;
        let name_item = binding.get_item(2)?;
        let name = name_item.cast_exact::<PyString>().map_err(|_| {
            PyTypeError::new_err("native iterator mean dependency names must be exact strings")
        })?;
        let expected = binding.get_item(3)?;
        let live = match primary.get_item(name)? {
            Some(value) => Some(value),
            None if fallback.is_none() => None,
            None => {
                let fallback = fallback.cast_exact::<PyDict>().map_err(|_| {
                    PyTypeError::new_err(
                        "native iterator mean dependency fallbacks must be exact dicts",
                    )
                })?;
                fallback.get_item(name)?
            }
        };
        if !live.is_some_and(|value| value.is(&expected)) {
            return Ok(false);
        }
    }
    let code_type = mean_code.get_type();
    Ok(exact_python_function_code(mean_function, &code_type)?
        .is_some_and(|code| code.bind(py).is(mean_code)))
}

/// Consume at most one callback-free chunk from an exact built-in iterator.
///
/// The dependency snapshot is checked under the GIL immediately before the first pull. Exact
/// ``bool``, ``int``, and ``float`` values cannot invoke user code; a subclass or custom value is
/// returned untouched so Python can resume from the compensated prefix with live global lookup.
/// Free-threaded builds conservatively decline because another thread can replace those globals.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub(crate) fn mean_exact_iterator_chunk_v1(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    count: u64,
    total: f64,
    compensation: f64,
    bindings: &Bound<'_, PyTuple>,
    mean_function: &Bound<'_, PyAny>,
    mean_code: &Bound<'_, PyAny>,
) -> PyResult<ExactIteratorMeanChunk> {
    #[cfg(Py_GIL_DISABLED)]
    {
        let _ = (py, values, bindings, mean_function, mean_code);
        Ok((3, count, total, compensation, None))
    }

    #[cfg(not(Py_GIL_DISABLED))]
    {
        let iterator_types = exact_builtin_iterator_types(py)?;
        if !iterator_types
            .iter()
            .any(|iterator_type| values.get_type().is(iterator_type.bind(py)))
        {
            return Err(PyTypeError::new_err(
                "native iterator mean requires an exact built-in list, tuple, or range iterator",
            ));
        }
        if !exact_mean_dependencies_are_live(py, bindings, mean_function, mean_code)? {
            return Ok((3, count, total, compensation, None));
        }

        let mut mean = CompensatedMean::from_state(count, total, compensation);
        for _ in 0..EXACT_ITERATOR_MEAN_CHUNK_ROWS {
            // SAFETY: the exact iterator-type guard restricts this call to CPython's callback-free
            // list, tuple, range, and long-range iterator implementations.
            let item = unsafe { ffi::PyIter_Next(values.as_ptr()) };
            if item.is_null() {
                if unsafe { !ffi::PyErr_Occurred().is_null() } {
                    return Err(PyErr::fetch(py));
                }
                let (count, total, compensation) = mean.state();
                return Ok((0, count, total, compensation, None));
            }
            // SAFETY: PyIter_Next returned one owned reference.
            let item = unsafe { Bound::from_owned_ptr(py, item) };
            let pointer = item.as_ptr();
            let value = if unsafe { ffi::PyFloat_CheckExact(pointer) } != 0 {
                unsafe { ffi::PyFloat_AsDouble(pointer) }
            } else if unsafe { ffi::PyLong_CheckExact(pointer) } != 0
                || unsafe { ffi::PyBool_Check(pointer) } != 0
            {
                unsafe { ffi::PyLong_AsDouble(pointer) }
            } else {
                let (count, total, compensation) = mean.state();
                return Ok((2, count, total, compensation, Some(item.unbind())));
            };
            if value == -1.0 && unsafe { !ffi::PyErr_Occurred().is_null() } {
                return Err(PyErr::fetch(py));
            }
            mean.accept(value).map_err(kernel_error)?;
        }
        let (count, total, compensation) = mean.state();
        Ok((1, count, total, compensation, None))
    }
}

const MAX_EXACT_F64_INTEGER: i64 = 1_i64 << 53;
const EXACT_F64_INTEGER_RANGE_WIDTH: u64 = 1_u64 << 54;

/// Return the exact integer sum when converting and adding ``value`` would be lossless in f64.
///
/// Shifting maps ``[-2**53, 2**53]`` to ``[0, 2**54]``. Since ``a | b`` is never smaller than
/// either operand, this single conservative comparison proves both the input and next total are
/// inside that interval. Rare boundary combinations may decline, but none can be accepted unsafely.
#[inline(always)]
pub(crate) fn next_exact_i64_mean_total(total: i64, value: i64) -> Option<i64> {
    let next_total = total.wrapping_add(value);
    let shifted_value = value.wrapping_add(MAX_EXACT_F64_INTEGER) as u64;
    let shifted_total = next_total.wrapping_add(MAX_EXACT_F64_INTEGER) as u64;
    (shifted_value | shifted_total <= EXACT_F64_INTEGER_RANGE_WIDTH).then_some(next_total)
}

#[inline]
fn update_mean_state<I>(
    count: u64,
    total: f64,
    compensation: f64,
    values: I,
) -> Result<(u64, f64, f64), KernelError>
where
    I: IntoIterator<Item = f64>,
{
    let mut mean = CompensatedMean::from_state(count, total, compensation);
    for value in values {
        mean.accept(value)?;
    }
    Ok(mean.state())
}

/// Continue a plain floating-point sum without changing encounter order.
///
/// This deliberately does not use compensated or pairwise summation: record aggregation starts
/// from Python's zero identity and applies ``total + value`` once per row. Carrying the returned
/// accumulator into the next buffer therefore preserves that exact chunk-independent state.
#[inline]
pub(crate) fn update_f64_sum_state<I>(mut total: f64, values: I) -> f64
where
    I: IntoIterator<Item = f64>,
{
    for value in values {
        total += value;
    }
    total
}

#[inline]
pub(crate) fn update_i64_mean_state<I>(
    count: u64,
    total: f64,
    compensation: f64,
    values: I,
) -> Result<(u64, f64, f64), KernelError>
where
    I: IntoIterator<Item = i64>,
{
    let positive_zero = 0.0_f64.to_bits();
    let exact_total = total.is_finite()
        && total.trunc() == total
        && total.abs() <= MAX_EXACT_F64_INTEGER as f64
        && (total != 0.0 || total.to_bits() == positive_zero)
        && compensation.to_bits() == positive_zero;
    let mut values = values.into_iter();
    if !exact_total {
        let mut mean = CompensatedMean::from_state(count, total, compensation);
        for value in values {
            mean.accept(value as f64)?;
        }
        return Ok(mean.state());
    }

    let mut count = count;
    let mut exact_total = total as i64;
    while let Some(value) = values.next() {
        let next_count = count.checked_add(1).ok_or(KernelError::Overflow)?;
        if let Some(next_total) = next_exact_i64_mean_total(exact_total, value) {
            count = next_count;
            exact_total = next_total;
            continue;
        }

        let mut mean = CompensatedMean::from_state(count, exact_total as f64, 0.0);
        mean.accept(value as f64)?;
        for value in values {
            mean.accept(value as f64)?;
        }
        return Ok(mean.state());
    }
    Ok((count, exact_total as f64, 0.0))
}

/// Continue one compensated mean state from a validated external i64 buffer.
///
/// GIL-enabled builds borrow the exported buffer for one attached scan. Free-threaded builds
/// retain the existing owned-snapshot boundary because attachment does not exclude native writers.
#[pyfunction]
pub(crate) fn update_mean_i64_buffer_v1(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    count: u64,
    total: f64,
    compensation: f64,
) -> PyResult<(u64, f64, f64)> {
    #[cfg(not(Py_GIL_DISABLED))]
    {
        let result = {
            let buffer = acquire_i64_buffer(values)?;
            if buffer.item_count() == 0 {
                update_i64_mean_state(count, total, compensation, std::iter::empty())
            } else {
                let slice = buffer.as_slice(py).ok_or_else(|| {
                    PyTypeError::new_err("native i64 buffers require one C-contiguous dimension")
                })?;
                update_i64_mean_state(
                    count,
                    total,
                    compensation,
                    slice.iter().map(|value| value.get()),
                )
            }
        };
        py.check_signals()?;
        result.map_err(kernel_error)
    }

    #[cfg(Py_GIL_DISABLED)]
    {
        let values = extract_i64_buffer(values)?;
        py.detach(move || update_i64_mean_state(count, total, compensation, values))
            .map_err(kernel_error)
    }
}

/// Continue one compensated mean state from a validated external f64 buffer.
///
/// The returned tuple is the raw ``(count, total, compensation)`` state, so a caller can preserve
/// the exact sequential update order across independently owned Arrow record batches.
#[pyfunction]
pub(crate) fn update_mean_f64_buffer_v1(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    count: u64,
    total: f64,
    compensation: f64,
) -> PyResult<(u64, f64, f64)> {
    #[cfg(not(Py_GIL_DISABLED))]
    {
        let result = match acquire_f64_buffer(values)? {
            Some(buffer) => {
                let slice = buffer.as_slice(py).ok_or_else(|| {
                    PyTypeError::new_err("native f64 buffers require one C-contiguous dimension")
                })?;
                update_mean_state(
                    count,
                    total,
                    compensation,
                    slice.iter().map(|value| value.get()),
                )
            }
            None => update_mean_state(count, total, compensation, std::iter::empty()),
        };
        py.check_signals()?;
        result.map_err(kernel_error)
    }

    #[cfg(Py_GIL_DISABLED)]
    {
        let values = extract_f64_buffer(values)?;
        py.detach(move || update_mean_state(count, total, compensation, values))
            .map_err(kernel_error)
    }
}

/// Continue one sequential sum from a validated external f64 buffer.
///
/// GIL-enabled builds keep the exporter and GIL for the full borrowed scan. Free-threaded builds
/// first take the same owned snapshot used by the mean continuation before detaching, so a native
/// writer cannot race a borrowed pointer. The caller owns empty-input accounting; an empty buffer
/// returns ``total`` bit-for-bit unchanged.
#[pyfunction]
pub(crate) fn update_sum_f64_buffer_v1(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    total: f64,
) -> PyResult<f64> {
    #[cfg(not(Py_GIL_DISABLED))]
    {
        let result = match acquire_f64_buffer(values)? {
            Some(buffer) => {
                let slice = buffer.as_slice(py).ok_or_else(|| {
                    PyTypeError::new_err("native f64 buffers require one C-contiguous dimension")
                })?;
                update_f64_sum_state(total, slice.iter().map(|value| value.get()))
            }
            None => total,
        };
        py.check_signals()?;
        Ok(result)
    }

    #[cfg(Py_GIL_DISABLED)]
    {
        let values = extract_f64_buffer(values)?;
        Ok(py.detach(move || update_f64_sum_state(total, values)))
    }
}

#[inline]
fn visit_exact_items_with<F>(
    values: &Bound<'_, PyAny>,
    length: usize,
    is_list: bool,
    mut visit: F,
) -> PyResult<bool>
where
    F: FnMut(*mut ffi::PyObject) -> PyResult<bool>,
{
    let py = values.py();
    for index in 0..length {
        // SAFETY: callers hold the GIL or the exact list's critical section for the whole scan.
        // Exact tuples are immutable. This speculative scan deliberately does not run Python or
        // check signals: a decline must leave one atomic container view for canonical replay.
        let item = unsafe {
            if is_list {
                ffi::PyList_GetItem(values.as_ptr(), index as ffi::Py_ssize_t)
            } else {
                ffi::PyTuple_GetItem(values.as_ptr(), index as ffi::Py_ssize_t)
            }
        };
        if item.is_null() {
            return Err(PyErr::fetch(py));
        }
        if !visit(item)? {
            return Ok(false);
        }
    }
    Ok(true)
}

#[inline]
fn mean_exact_i64_items(
    values: &Bound<'_, PyAny>,
    length: usize,
    is_list: bool,
) -> PyResult<Option<f64>> {
    let py = values.py();
    let mut mean = CompensatedMean::default();
    visit_exact_items_with(values, length, is_list, |item| {
        // Exact-type validation prevents ``__index__`` or subclass dispatch.
        if unsafe { ffi::PyLong_CheckExact(item) } == 0 {
            return Err(PyTypeError::new_err(
                "native i64 containers require exact integers",
            ));
        }
        let value = unsafe { ffi::PyLong_AsLongLong(item) };
        if value == -1 && !unsafe { ffi::PyErr_Occurred() }.is_null() {
            return Err(PyErr::fetch(py));
        }
        mean.accept(value as f64).map_err(kernel_error)?;
        Ok(true)
    })?;
    Ok(mean.value())
}

#[inline]
fn sum_exact_i64_items(values: &Bound<'_, PyAny>, length: usize, is_list: bool) -> PyResult<i128> {
    let py = values.py();
    let mut total = 0_i128;
    visit_exact_items_with(values, length, is_list, |item| {
        // Exact-type validation prevents ``__index__`` or subclass dispatch.
        if unsafe { ffi::PyLong_CheckExact(item) } == 0 {
            return Err(PyTypeError::new_err(
                "native i64 containers require exact integers",
            ));
        }
        let value = unsafe { ffi::PyLong_AsLongLong(item) };
        if value == -1 && !unsafe { ffi::PyErr_Occurred() }.is_null() {
            return Err(PyErr::fetch(py));
        }
        // A Python container cannot hold enough signed i64 values to overflow i128: even an
        // address-space-sized sequence stays below the widened accumulator's range.
        total += i128::from(value);
        Ok(true)
    })?;
    Ok(total)
}

/// Sum an operation-free exact integer list/tuple in one attached scan.
pub(crate) fn sum_i64_container(values: &Bound<'_, PyAny>) -> PyResult<i128> {
    if let Ok(list) = values.cast_exact::<PyList>() {
        #[cfg(not(Py_GIL_DISABLED))]
        return sum_exact_i64_items(values, list.len(), true);

        #[cfg(Py_GIL_DISABLED)]
        return with_critical_section(values, || sum_exact_i64_items(values, list.len(), true));
    }
    if let Ok(tuple) = values.cast_exact::<PyTuple>() {
        return sum_exact_i64_items(values, tuple.len(), false);
    }
    Err(PyTypeError::new_err(
        "native numeric sources require an exact list or tuple",
    ))
}

/// Reduce an operation-free exact integer list/tuple in one attached scan.
///
/// General programs still snapshot into an owned vector so their heavier work can detach.
/// Mean-only identity reduction is cheaper than that copy plus a second traversal, and exact
/// numeric objects cannot re-enter Python while the source is borrowed.
pub(crate) fn mean_i64_container(values: &Bound<'_, PyAny>) -> PyResult<Option<f64>> {
    if let Ok(list) = values.cast_exact::<PyList>() {
        #[cfg(not(Py_GIL_DISABLED))]
        return mean_exact_i64_items(values, list.len(), true);

        #[cfg(Py_GIL_DISABLED)]
        return with_critical_section(values, || mean_exact_i64_items(values, list.len(), true));
    }
    if let Ok(tuple) = values.cast_exact::<PyTuple>() {
        return mean_exact_i64_items(values, tuple.len(), false);
    }
    Err(PyTypeError::new_err(
        "native numeric sources require an exact list or tuple",
    ))
}

#[inline]
fn mean_exact_f64_items(
    values: &Bound<'_, PyAny>,
    length: usize,
    is_list: bool,
) -> PyResult<Option<f64>> {
    let py = values.py();
    let mut mean = CompensatedMean::default();
    visit_exact_items_with(values, length, is_list, |item| {
        // Exact-type validation prevents ``__float__`` or subclass dispatch.
        if unsafe { ffi::PyFloat_CheckExact(item) } == 0 {
            return Err(PyTypeError::new_err(
                "native f64 containers require exact floats",
            ));
        }
        let value = unsafe { ffi::PyFloat_AsDouble(item) };
        if value == -1.0 && !unsafe { ffi::PyErr_Occurred() }.is_null() {
            return Err(PyErr::fetch(py));
        }
        mean.accept(value).map_err(kernel_error)?;
        Ok(true)
    })?;
    Ok(mean.value())
}

/// Reduce an operation-free exact floating list/tuple without an intermediate vector.
pub(crate) fn mean_f64_container(values: &Bound<'_, PyAny>) -> PyResult<Option<f64>> {
    if let Ok(list) = values.cast_exact::<PyList>() {
        #[cfg(not(Py_GIL_DISABLED))]
        return mean_exact_f64_items(values, list.len(), true);

        #[cfg(Py_GIL_DISABLED)]
        return with_critical_section(values, || mean_exact_f64_items(values, list.len(), true));
    }
    if let Ok(tuple) = values.cast_exact::<PyTuple>() {
        return mean_exact_f64_items(values, tuple.len(), false);
    }
    Err(PyTypeError::new_err(
        "native numeric sources require an exact list or tuple",
    ))
}

#[inline]
fn mean_exact_number_items(
    values: &Bound<'_, PyAny>,
    length: usize,
    is_list: bool,
) -> PyResult<(bool, Option<f64>)> {
    let py = values.py();
    let mut mean = CompensatedMean::default();
    let handled = visit_exact_items_with(values, length, is_list, |item| {
        let value = if unsafe { ffi::PyFloat_CheckExact(item) } != 0 {
            unsafe { ffi::PyFloat_AsDouble(item) }
        } else if unsafe { ffi::PyLong_CheckExact(item) } != 0
            || unsafe { ffi::PyBool_Check(item) } != 0
        {
            // PyLong_AsDouble supports arbitrary-size exact integers and reports overflow.
            unsafe { ffi::PyLong_AsDouble(item) }
        } else {
            // Declining is side-effect free: exact list/tuple reads do not consume the source,
            // and no numeric conversion protocol has been invoked.
            return Ok(false);
        };
        if value == -1.0 && !unsafe { ffi::PyErr_Occurred() }.is_null() {
            return Err(PyErr::fetch(py));
        }
        mean.accept(value).map_err(kernel_error)?;
        Ok(true)
    })?;
    Ok((handled, handled.then(|| mean.value()).flatten()))
}

/// Attempt a single-pass identity mean for an exact list/tuple of built-in real numbers.
///
/// The handled flag distinguishes an accepted empty container from a safe decline. This endpoint
/// deliberately refuses subclasses and user protocols so automatic execution can replay the
/// untouched container through the canonical Python semantics.
#[pyfunction]
pub(crate) fn mean_exact_numbers_v1(values: &Bound<'_, PyAny>) -> PyResult<(bool, Option<f64>)> {
    if let Ok(list) = values.cast_exact::<PyList>() {
        #[cfg(not(Py_GIL_DISABLED))]
        return mean_exact_number_items(values, list.len(), true);

        #[cfg(Py_GIL_DISABLED)]
        return with_critical_section(values, || mean_exact_number_items(values, list.len(), true));
    }
    if let Ok(tuple) = values.cast_exact::<PyTuple>() {
        return mean_exact_number_items(values, tuple.len(), false);
    }
    Ok((false, None))
}
