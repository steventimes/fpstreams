//! Stable PEP 3118 buffer preparation for transactional NumPy grouping.

use super::{GroupData, TypedGroupData};
use crate::common::{AGGREGATE_MAXIMUM, AGGREGATE_MINIMUM, AGGREGATE_TOTAL};
use pyo3::buffer::{Element, ElementType, PyBuffer, PyUntypedBuffer, ReadOnlyCell};
use pyo3::exceptions::PyMemoryError;
use pyo3::prelude::*;
use std::ffi::CStr;
#[cfg(not(Py_GIL_DISABLED))]
use std::marker::PhantomData;
#[cfg(not(Py_GIL_DISABLED))]
use std::mem;

#[derive(Clone, Copy)]
#[repr(transparent)]
struct BufferBool(u8);

// SAFETY: PEP 3118 '?' elements occupy one byte. BufferBool accepts every byte pattern and the
// grouped scan canonicalizes zero/non-zero to Rust bool before retaining aggregate state.
unsafe impl Element for BufferBool {
    fn is_compatible_format(format: &CStr) -> bool {
        ElementType::from_format(format) == ElementType::Bool
    }
}

enum NumericBuffer {
    Bool(Option<PyBuffer<BufferBool>>),
    I64(Option<PyBuffer<i64>>),
    U64(Option<PyBuffer<u64>>),
}

#[cfg(not(Py_GIL_DISABLED))]
struct StridedBuffer<T: Element> {
    buffer: Option<PyBuffer<T>>,
    stride: isize,
}

#[cfg(not(Py_GIL_DISABLED))]
enum StridedNumericBuffer {
    Bool(StridedBuffer<BufferBool>),
    I64(StridedBuffer<i64>),
    U64(StridedBuffer<u64>),
}

#[cfg(not(Py_GIL_DISABLED))]
impl StridedNumericBuffer {
    fn len(&self) -> usize {
        match self {
            Self::Bool(values) => values
                .buffer
                .as_ref()
                .map_or(0, |buffer| buffer.item_count()),
            Self::I64(values) => values
                .buffer
                .as_ref()
                .map_or(0, |buffer| buffer.item_count()),
            Self::U64(values) => values
                .buffer
                .as_ref()
                .map_or(0, |buffer| buffer.item_count()),
        }
    }

    fn kind_name(&self) -> &'static str {
        match self {
            Self::Bool(_) => "bool",
            Self::I64(_) => "i64",
            Self::U64(_) => "u64",
        }
    }
}

#[cfg(not(Py_GIL_DISABLED))]
struct StridedCells<'a, T: Element> {
    pointer: *const ReadOnlyCell<T>,
    stride: isize,
    remaining: usize,
    _buffer: PhantomData<&'a PyBuffer<T>>,
}

#[cfg(not(Py_GIL_DISABLED))]
impl<'a, T: Element> StridedCells<'a, T> {
    fn new(values: &'a StridedBuffer<T>) -> Self {
        let (pointer, remaining) = values.buffer.as_ref().map_or_else(
            || (std::ptr::null(), 0),
            |buffer| {
                (
                    buffer.buf_ptr().cast::<ReadOnlyCell<T>>(),
                    buffer.item_count(),
                )
            },
        );
        Self {
            pointer,
            stride: values.stride,
            remaining,
            _buffer: PhantomData,
        }
    }
}

#[cfg(not(Py_GIL_DISABLED))]
impl<T: Element> Iterator for StridedCells<'_, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        // SAFETY: acquisition rejects indirect and misaligned buffers. The exporter guarantees
        // every logical position described by its one-dimensional shape and signed stride is a
        // valid T element while PyBuffer keeps the export alive. This scan never detaches.
        let value = unsafe { (&*self.pointer).get() };
        self.remaining -= 1;
        if self.remaining != 0 {
            self.pointer = self
                .pointer
                .cast::<u8>()
                .wrapping_offset(self.stride)
                .cast::<ReadOnlyCell<T>>();
        }
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

#[cfg(not(Py_GIL_DISABLED))]
impl<T: Element> ExactSizeIterator for StridedCells<'_, T> {}

impl NumericBuffer {
    fn len(&self) -> usize {
        match self {
            Self::Bool(values) => values.as_ref().map_or(0, |values| values.item_count()),
            Self::I64(values) => values.as_ref().map_or(0, |values| values.item_count()),
            Self::U64(values) => values.as_ref().map_or(0, |values| values.item_count()),
        }
    }

    fn kind_name(&self) -> &'static str {
        match self {
            Self::Bool(_) => "bool",
            Self::I64(_) => "i64",
            Self::U64(_) => "u64",
        }
    }
}

fn is_native_endian(format: &CStr) -> bool {
    match format.to_bytes().first().copied() {
        Some(b'<') => cfg!(target_endian = "little"),
        Some(b'>') | Some(b'!') => cfg!(target_endian = "big"),
        _ => true,
    }
}

#[cfg(test)]
pub(super) fn native_endian_for_test(format: &CStr) -> bool {
    is_native_endian(format)
}

fn acquire_numeric_buffer(values: &Bound<'_, PyAny>) -> PyResult<Option<NumericBuffer>> {
    let py = values.py();
    let buffer = match PyUntypedBuffer::get(values) {
        Ok(buffer) => buffer,
        Err(error) if error.is_instance_of::<PyMemoryError>(py) => return Err(error),
        Err(_) => return Ok(None),
    };
    if buffer.dimensions() != 1 || !buffer.is_c_contiguous() || !is_native_endian(buffer.format()) {
        return Ok(None);
    }
    let kind = match ElementType::from_format(buffer.format()) {
        ElementType::Bool if buffer.item_size() == 1 => 0,
        ElementType::SignedInteger { bytes: 8 } if buffer.item_size() == 8 => 1,
        ElementType::UnsignedInteger { bytes: 8 } if buffer.item_size() == 8 => 2,
        _ => return Ok(None),
    };
    if buffer.item_count() == 0 {
        return Ok(Some(match kind {
            0 => NumericBuffer::Bool(None),
            1 => NumericBuffer::I64(None),
            _ => NumericBuffer::U64(None),
        }));
    }
    match kind {
        0 => {
            let buffer = match buffer.into_typed::<BufferBool>() {
                Ok(buffer) => buffer,
                Err(_) => return Ok(None),
            };
            Ok(Some(NumericBuffer::Bool(Some(buffer))))
        }
        1 => {
            let buffer = match buffer.into_typed::<i64>() {
                Ok(buffer) => buffer,
                Err(_) => return Ok(None),
            };
            Ok(Some(NumericBuffer::I64(Some(buffer))))
        }
        _ => {
            let buffer = match buffer.into_typed::<u64>() {
                Ok(buffer) => buffer,
                Err(_) => return Ok(None),
            };
            Ok(Some(NumericBuffer::U64(Some(buffer))))
        }
    }
}

#[cfg(not(Py_GIL_DISABLED))]
fn acquire_strided_numeric_buffer(
    values: &Bound<'_, PyAny>,
) -> PyResult<Option<StridedNumericBuffer>> {
    let py = values.py();
    let buffer = match PyUntypedBuffer::get(values) {
        Ok(buffer) => buffer,
        Err(error) if error.is_instance_of::<PyMemoryError>(py) => return Err(error),
        Err(_) => return Ok(None),
    };
    if buffer.dimensions() != 1
        || buffer.suboffsets().is_some()
        || !is_native_endian(buffer.format())
    {
        return Ok(None);
    }
    let stride = buffer.strides()[0];
    let kind = match ElementType::from_format(buffer.format()) {
        ElementType::Bool if buffer.item_size() == 1 => 0,
        ElementType::SignedInteger { bytes: 8 } if buffer.item_size() == 8 => 1,
        ElementType::UnsignedInteger { bytes: 8 } if buffer.item_size() == 8 => 2,
        _ => return Ok(None),
    };
    let alignment = match kind {
        0 => mem::align_of::<BufferBool>(),
        1 => mem::align_of::<i64>(),
        _ => mem::align_of::<u64>(),
    };
    if stride.unsigned_abs() % alignment != 0 {
        return Ok(None);
    }
    if buffer.item_count() == 0 {
        return Ok(Some(match kind {
            0 => StridedNumericBuffer::Bool(StridedBuffer {
                buffer: None,
                stride,
            }),
            1 => StridedNumericBuffer::I64(StridedBuffer {
                buffer: None,
                stride,
            }),
            _ => StridedNumericBuffer::U64(StridedBuffer {
                buffer: None,
                stride,
            }),
        }));
    }
    match kind {
        0 => {
            let buffer = match buffer.into_typed::<BufferBool>() {
                Ok(buffer) => buffer,
                Err(_) => return Ok(None),
            };
            Ok(Some(StridedNumericBuffer::Bool(StridedBuffer {
                buffer: Some(buffer),
                stride,
            })))
        }
        1 => {
            let buffer = match buffer.into_typed::<i64>() {
                Ok(buffer) => buffer,
                Err(_) => return Ok(None),
            };
            Ok(Some(StridedNumericBuffer::I64(StridedBuffer {
                buffer: Some(buffer),
                stride,
            })))
        }
        _ => {
            let buffer = match buffer.into_typed::<u64>() {
                Ok(buffer) => buffer,
                Err(_) => return Ok(None),
            };
            Ok(Some(StridedNumericBuffer::U64(StridedBuffer {
                buffer: Some(buffer),
                stride,
            })))
        }
    }
}

fn bool_slice<'a>(
    buffer: &'a Option<PyBuffer<BufferBool>>,
    py: Python<'a>,
) -> &'a [ReadOnlyCell<BufferBool>] {
    buffer
        .as_ref()
        .map(|buffer| {
            buffer
                .as_slice(py)
                .expect("a validated one-dimensional C buffer has a typed slice")
        })
        .unwrap_or(&[])
}

fn i64_slice<'a>(buffer: &'a Option<PyBuffer<i64>>, py: Python<'a>) -> &'a [ReadOnlyCell<i64>] {
    buffer
        .as_ref()
        .map(|buffer| {
            buffer
                .as_slice(py)
                .expect("a validated one-dimensional C buffer has a typed slice")
        })
        .unwrap_or(&[])
}

fn u64_slice<'a>(buffer: &'a Option<PyBuffer<u64>>, py: Python<'a>) -> &'a [ReadOnlyCell<u64>] {
    buffer
        .as_ref()
        .map(|buffer| {
            buffer
                .as_slice(py)
                .expect("a validated one-dimensional C buffer has a typed slice")
        })
        .unwrap_or(&[])
}

fn build_partial(
    py: Python<'_>,
    keys: NumericBuffer,
    values: Option<NumericBuffer>,
    mask: u8,
) -> PyResult<Option<TypedGroupData>> {
    if let Some(values) = &values
        && (values.kind_name() != keys.kind_name() || values.len() != keys.len())
    {
        return Ok(None);
    }
    if mask & (AGGREGATE_TOTAL | AGGREGATE_MINIMUM | AGGREGATE_MAXIMUM) != 0 && values.is_none() {
        return Ok(None);
    }
    match (keys, values) {
        (NumericBuffer::Bool(keys), Some(NumericBuffer::Bool(values))) => {
            let keys = bool_slice(&keys, py);
            let values = bool_slice(&values, py);
            GroupData::from_iterators(
                keys.iter().map(|value| value.get().0 != 0),
                Some(values.iter().map(|value| value.get().0 != 0)),
                mask,
            )
            .map(TypedGroupData::Bool)
            .map(Some)
        }
        (NumericBuffer::I64(keys), Some(NumericBuffer::I64(values))) => {
            let keys = i64_slice(&keys, py);
            let values = i64_slice(&values, py);
            GroupData::from_iterators(
                keys.iter().map(ReadOnlyCell::get),
                Some(values.iter().map(ReadOnlyCell::get)),
                mask,
            )
            .map(TypedGroupData::I64)
            .map(Some)
        }
        (NumericBuffer::U64(keys), Some(NumericBuffer::U64(values))) => {
            let keys = u64_slice(&keys, py);
            let values = u64_slice(&values, py);
            GroupData::from_iterators(
                keys.iter().map(ReadOnlyCell::get),
                Some(values.iter().map(ReadOnlyCell::get)),
                mask,
            )
            .map(TypedGroupData::U64)
            .map(Some)
        }
        (NumericBuffer::Bool(keys), None) => {
            let keys = bool_slice(&keys, py);
            GroupData::from_iterators(
                keys.iter().map(|value| value.get().0 != 0),
                None::<std::iter::Empty<bool>>,
                mask,
            )
            .map(TypedGroupData::Bool)
            .map(Some)
        }
        (NumericBuffer::I64(keys), None) => {
            let keys = i64_slice(&keys, py);
            GroupData::from_iterators(
                keys.iter().map(ReadOnlyCell::get),
                None::<std::iter::Empty<i64>>,
                mask,
            )
            .map(TypedGroupData::I64)
            .map(Some)
        }
        (NumericBuffer::U64(keys), None) => {
            let keys = u64_slice(&keys, py);
            GroupData::from_iterators(
                keys.iter().map(ReadOnlyCell::get),
                None::<std::iter::Empty<u64>>,
                mask,
            )
            .map(TypedGroupData::U64)
            .map(Some)
        }
        _ => Ok(None),
    }
}

#[cfg(not(Py_GIL_DISABLED))]
fn build_strided_partial(
    keys: StridedNumericBuffer,
    values: Option<StridedNumericBuffer>,
    mask: u8,
) -> PyResult<Option<TypedGroupData>> {
    if let Some(values) = &values
        && (values.kind_name() != keys.kind_name() || values.len() != keys.len())
    {
        return Ok(None);
    }
    if mask & (AGGREGATE_TOTAL | AGGREGATE_MINIMUM | AGGREGATE_MAXIMUM) != 0 && values.is_none() {
        return Ok(None);
    }
    match (keys, values) {
        (StridedNumericBuffer::Bool(keys), Some(StridedNumericBuffer::Bool(values))) => {
            GroupData::from_iterators(
                StridedCells::new(&keys).map(|value| value.0 != 0),
                Some(StridedCells::new(&values).map(|value| value.0 != 0)),
                mask,
            )
            .map(TypedGroupData::Bool)
            .map(Some)
        }
        (StridedNumericBuffer::I64(keys), Some(StridedNumericBuffer::I64(values))) => {
            GroupData::from_iterators(
                StridedCells::new(&keys),
                Some(StridedCells::new(&values)),
                mask,
            )
            .map(TypedGroupData::I64)
            .map(Some)
        }
        (StridedNumericBuffer::U64(keys), Some(StridedNumericBuffer::U64(values))) => {
            GroupData::from_iterators(
                StridedCells::new(&keys),
                Some(StridedCells::new(&values)),
                mask,
            )
            .map(TypedGroupData::U64)
            .map(Some)
        }
        (StridedNumericBuffer::Bool(keys), None) => GroupData::from_iterators(
            StridedCells::new(&keys).map(|value| value.0 != 0),
            None::<std::iter::Empty<bool>>,
            mask,
        )
        .map(TypedGroupData::Bool)
        .map(Some),
        (StridedNumericBuffer::I64(keys), None) => GroupData::from_iterators(
            StridedCells::new(&keys),
            None::<std::iter::Empty<i64>>,
            mask,
        )
        .map(TypedGroupData::I64)
        .map(Some),
        (StridedNumericBuffer::U64(keys), None) => GroupData::from_iterators(
            StridedCells::new(&keys),
            None::<std::iter::Empty<u64>>,
            mask,
        )
        .map(TypedGroupData::U64)
        .map(Some),
        _ => Ok(None),
    }
}

pub(super) fn prepare_contiguous_partial(
    keys: &Bound<'_, PyAny>,
    values: Option<&Bound<'_, PyAny>>,
    mask: u8,
) -> PyResult<Option<TypedGroupData>> {
    let py = keys.py();
    let Some(keys) = acquire_numeric_buffer(keys)? else {
        return Ok(None);
    };
    let values = match values {
        Some(values) => {
            let Some(values) = acquire_numeric_buffer(values)? else {
                return Ok(None);
            };
            Some(values)
        }
        None => None,
    };
    build_partial(py, keys, values, mask)
}

#[cfg(not(Py_GIL_DISABLED))]
pub(super) fn prepare_strided_partial(
    keys: &Bound<'_, PyAny>,
    values: Option<&Bound<'_, PyAny>>,
    mask: u8,
) -> PyResult<Option<TypedGroupData>> {
    let Some(keys) = acquire_strided_numeric_buffer(keys)? else {
        return Ok(None);
    };
    let values = match values {
        Some(values) => {
            let Some(values) = acquire_strided_numeric_buffer(values)? else {
                return Ok(None);
            };
            Some(values)
        }
        None => None,
    };
    build_strided_partial(keys, values, mask)
}
