//! Transactional grouped-state ABI tests for stable integer buffers.

use crate::common::{AGGREGATE_COUNT, AGGREGATE_MAXIMUM, AGGREGATE_MINIMUM, AGGREGATE_TOTAL};
#[cfg(not(Py_GIL_DISABLED))]
use crate::numpy_group::numpy_group_strided_partial_v2;
use crate::numpy_group::{
    numpy_group_commit_v1, numpy_group_finalize_v1, numpy_group_partial_v1, numpy_group_state_v1,
};
#[cfg(not(Py_GIL_DISABLED))]
use pyo3::exceptions::PyBufferError;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
#[cfg(not(Py_GIL_DISABLED))]
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyAnyMethods, PyBytes, PyTupleMethods};
#[cfg(not(Py_GIL_DISABLED))]
use std::ffi::{CStr, c_int, c_void};
#[cfg(not(Py_GIL_DISABLED))]
use std::ptr;

#[cfg(not(Py_GIL_DISABLED))]
#[derive(Clone, Copy)]
enum TestBufferFormat {
    Bool,
    I64,
}

#[cfg(not(Py_GIL_DISABLED))]
impl TestBufferFormat {
    fn as_c_str(self) -> &'static CStr {
        match self {
            Self::Bool => c"?",
            Self::I64 => c"q",
        }
    }
}

#[cfg(not(Py_GIL_DISABLED))]
struct TestBufferMetadata {
    shape: isize,
    stride: isize,
    suboffset: isize,
}

/// Minimal direct/indirect exporter used to exercise buffer layouts stdlib memoryview cannot make.
#[cfg(not(Py_GIL_DISABLED))]
#[pyclass]
struct TestStridedBuffer {
    storage: Vec<u64>,
    start_bytes: usize,
    length: isize,
    item_size: isize,
    stride: isize,
    format: TestBufferFormat,
    indirect: bool,
}

#[cfg(not(Py_GIL_DISABLED))]
#[pymethods]
impl TestStridedBuffer {
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(PyBufferError::new_err("buffer view is null"));
        }
        if flags & ffi::PyBUF_WRITABLE == ffi::PyBUF_WRITABLE {
            return Err(PyBufferError::new_err("test buffer is read-only"));
        }

        let exporter = slf.borrow();
        let data = unsafe {
            exporter
                .storage
                .as_ptr()
                .cast::<u8>()
                .add(exporter.start_bytes)
        };
        let length = exporter.length;
        let item_size = exporter.item_size;
        let stride = exporter.stride;
        let format = exporter.format.as_c_str();
        let indirect = exporter.indirect;
        drop(exporter);

        let metadata = Box::new(TestBufferMetadata {
            shape: length,
            stride,
            suboffset: 0,
        });
        let metadata = Box::into_raw(metadata);
        unsafe {
            (*view).obj = slf.into_ptr();
            (*view).buf = data.cast_mut().cast::<c_void>();
            (*view).len = length * item_size;
            (*view).readonly = 1;
            (*view).itemsize = item_size;
            (*view).format = format.as_ptr().cast_mut();
            (*view).ndim = 1;
            (*view).shape = ptr::addr_of_mut!((*metadata).shape);
            (*view).strides = ptr::addr_of_mut!((*metadata).stride);
            (*view).suboffsets = if indirect {
                ptr::addr_of_mut!((*metadata).suboffset)
            } else {
                ptr::null_mut()
            };
            (*view).internal = metadata.cast::<c_void>();
        }
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, view: *mut ffi::Py_buffer) {
        if !view.is_null() {
            let metadata = unsafe { (*view).internal.cast::<TestBufferMetadata>() };
            if !metadata.is_null() {
                drop(unsafe { Box::from_raw(metadata) });
                unsafe { (*view).internal = ptr::null_mut() };
            }
        }
    }
}

fn i64_array<'py>(py: Python<'py>, values: &[i64]) -> Bound<'py, PyAny> {
    PyModule::import(py, "array")
        .unwrap()
        .getattr("array")
        .unwrap()
        .call1(("q", values.to_vec()))
        .unwrap()
}

fn u64_array<'py>(py: Python<'py>, values: &[u64]) -> Bound<'py, PyAny> {
    PyModule::import(py, "array")
        .unwrap()
        .getattr("array")
        .unwrap()
        .call1(("Q", values.to_vec()))
        .unwrap()
}

fn bool_buffer<'py>(py: Python<'py>, values: &[bool]) -> Bound<'py, PyAny> {
    let bytes = values
        .iter()
        .map(|&value| u8::from(value))
        .collect::<Vec<_>>();
    PyModule::import(py, "builtins")
        .unwrap()
        .getattr("memoryview")
        .unwrap()
        .call1((PyBytes::new(py, &bytes),))
        .unwrap()
        .call_method1("cast", ("?",))
        .unwrap()
}

#[cfg(not(Py_GIL_DISABLED))]
fn strided_view<'py>(
    py: Python<'py>,
    values: &Bound<'py, PyAny>,
    step: isize,
) -> Bound<'py, PyAny> {
    let builtins = PyModule::import(py, "builtins").unwrap();
    let view = builtins
        .getattr("memoryview")
        .unwrap()
        .call1((values,))
        .unwrap();
    let slice = builtins
        .getattr("slice")
        .unwrap()
        .call1((py.None(), py.None(), step))
        .unwrap();
    view.get_item(slice).unwrap()
}

fn state<'py>(py: Python<'py>, mask: u8) -> Py<NumpyGroupStateForTests> {
    // Keep the test helper's concrete type local to this module while constructing through
    // the public ABI. The alias below deliberately exposes no implementation fields.
    Py::new(py, numpy_group_state_v1(mask).unwrap()).unwrap()
}

type NumpyGroupStateForTests = crate::numpy_group::NumpyGroupState;

#[test]
fn i64_partials_are_discardable_transactional_and_first_seen() {
    Python::initialize();
    Python::attach(|py| {
        let mask = AGGREGATE_COUNT | AGGREGATE_TOTAL | AGGREGATE_MINIMUM | AGGREGATE_MAXIMUM;
        let state = state(py, mask);

        let first_keys = i64_array(py, &[2, 1, 2]);
        let first_values = i64_array(py, &[10, 20, -3]);
        let first = Py::new(
            py,
            numpy_group_partial_v1(&first_keys, Some(&first_values), mask)
                .unwrap()
                .unwrap(),
        )
        .unwrap();

        let discarded_keys = i64_array(py, &[99]);
        let discarded_values = i64_array(py, &[999]);
        let _discarded = numpy_group_partial_v1(&discarded_keys, Some(&discarded_values), mask)
            .unwrap()
            .unwrap();

        // The partial owns a stable snapshot; mutating the exporter before commit is harmless.
        first_keys.call_method1("__setitem__", (0, 999)).unwrap();
        first_values.call_method1("__setitem__", (0, 999)).unwrap();
        numpy_group_commit_v1(state.bind(py).borrow_mut(), first.bind(py).borrow_mut()).unwrap();

        let second_keys = i64_array(py, &[1, 3, 2]);
        let second_values = i64_array(py, &[5, 8, 100]);
        let second = Py::new(
            py,
            numpy_group_partial_v1(&second_keys, Some(&second_values), mask)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        numpy_group_commit_v1(state.bind(py).borrow_mut(), second.bind(py).borrow_mut()).unwrap();

        let result = numpy_group_finalize_v1(py, state.bind(py).borrow_mut()).unwrap();
        let result = result.bind(py);
        assert_eq!(
            result.get_item(0).unwrap().extract::<Vec<i64>>().unwrap(),
            vec![2, 1, 3]
        );
        assert_eq!(
            result.get_item(1).unwrap().extract::<Vec<u128>>().unwrap(),
            vec![3, 2, 1]
        );
        assert_eq!(
            result.get_item(2).unwrap().extract::<Vec<i128>>().unwrap(),
            vec![107, 25, 8]
        );
        assert_eq!(
            result.get_item(3).unwrap().extract::<Vec<i64>>().unwrap(),
            vec![-3, 5, 8]
        );
        assert_eq!(
            result.get_item(4).unwrap().extract::<Vec<i64>>().unwrap(),
            vec![100, 20, 8]
        );

        let finalized = numpy_group_finalize_v1(py, state.bind(py).borrow_mut()).unwrap_err();
        assert!(finalized.is_instance_of::<PyRuntimeError>(py));
    });
}

#[test]
fn uint64_sum_uses_widened_unsigned_state() {
    Python::initialize();
    Python::attach(|py| {
        let mask = AGGREGATE_COUNT | AGGREGATE_TOTAL | AGGREGATE_MINIMUM | AGGREGATE_MAXIMUM;
        let state = state(py, mask);
        let keys = u64_array(py, &[u64::MAX, 0, u64::MAX]);
        let values = u64_array(py, &[u64::MAX, u64::MAX, u64::MAX]);
        let partial = Py::new(
            py,
            numpy_group_partial_v1(&keys, Some(&values), mask)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        numpy_group_commit_v1(state.bind(py).borrow_mut(), partial.bind(py).borrow_mut()).unwrap();

        let result = numpy_group_finalize_v1(py, state.bind(py).borrow_mut()).unwrap();
        let result = result.bind(py);
        assert_eq!(
            result.get_item(0).unwrap().extract::<Vec<u64>>().unwrap(),
            vec![u64::MAX, 0]
        );
        assert_eq!(
            result.get_item(2).unwrap().extract::<Vec<u128>>().unwrap(),
            vec![u128::from(u64::MAX) * 2, u128::from(u64::MAX)]
        );
    });
}

#[test]
fn bool_keys_and_extrema_remain_python_booleans() {
    Python::initialize();
    Python::attach(|py| {
        let mask = AGGREGATE_COUNT | AGGREGATE_TOTAL | AGGREGATE_MINIMUM | AGGREGATE_MAXIMUM;
        let state = state(py, mask);
        let keys = bool_buffer(py, &[true, false, true]);
        let values = bool_buffer(py, &[true, true, false]);
        let partial = Py::new(
            py,
            numpy_group_partial_v1(&keys, Some(&values), mask)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        numpy_group_commit_v1(state.bind(py).borrow_mut(), partial.bind(py).borrow_mut()).unwrap();

        let result = numpy_group_finalize_v1(py, state.bind(py).borrow_mut()).unwrap();
        let result = result.bind(py);
        assert_eq!(
            result.get_item(0).unwrap().extract::<Vec<bool>>().unwrap(),
            vec![true, false]
        );
        assert_eq!(
            result.get_item(1).unwrap().extract::<Vec<u128>>().unwrap(),
            vec![2, 1]
        );
        assert_eq!(
            result.get_item(2).unwrap().extract::<Vec<u128>>().unwrap(),
            vec![1, 1]
        );
        assert_eq!(
            result.get_item(3).unwrap().extract::<Vec<bool>>().unwrap(),
            vec![false, true]
        );
        assert_eq!(
            result.get_item(4).unwrap().extract::<Vec<bool>>().unwrap(),
            vec![true, true]
        );
    });
}

#[test]
fn count_only_accepts_no_values_and_unsupported_buffers_decline() {
    Python::initialize();
    Python::attach(|py| {
        let state = state(py, AGGREGATE_COUNT);
        let keys = i64_array(py, &[3, 3, -1]);
        let partial = Py::new(
            py,
            numpy_group_partial_v1(&keys, None, AGGREGATE_COUNT)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        numpy_group_commit_v1(state.bind(py).borrow_mut(), partial.bind(py).borrow_mut()).unwrap();
        let result = numpy_group_finalize_v1(py, state.bind(py).borrow_mut()).unwrap();
        let result = result.bind(py);
        assert_eq!(
            result.get_item(0).unwrap().extract::<Vec<i64>>().unwrap(),
            vec![3, -1]
        );
        assert_eq!(
            result.get_item(1).unwrap().extract::<Vec<u128>>().unwrap(),
            vec![2, 1]
        );
        for index in 2..5 {
            assert!(result.get_item(index).unwrap().is_none());
        }

        let sum_without_values = numpy_group_partial_v1(&keys, None, AGGREGATE_TOTAL).unwrap();
        assert!(sum_without_values.is_none());

        let i32_keys = PyModule::import(py, "array")
            .unwrap()
            .getattr("array")
            .unwrap()
            .call1(("i", vec![1_i32, 2]))
            .unwrap();
        assert!(
            numpy_group_partial_v1(&i32_keys, None, AGGREGATE_COUNT)
                .unwrap()
                .is_none()
        );

        let short_values = i64_array(py, &[1, 2]);
        assert!(
            numpy_group_partial_v1(&keys, Some(&short_values), AGGREGATE_TOTAL)
                .unwrap()
                .is_none()
        );
    });
}

#[test]
fn failed_or_duplicate_commit_leaves_state_logically_unchanged() {
    Python::initialize();
    Python::attach(|py| {
        let mask = AGGREGATE_TOTAL;
        let state = state(py, mask);
        let keys = i64_array(py, &[7]);
        let values = i64_array(py, &[10]);
        let partial = Py::new(
            py,
            numpy_group_partial_v1(&keys, Some(&values), mask)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        numpy_group_commit_v1(state.bind(py).borrow_mut(), partial.bind(py).borrow_mut()).unwrap();

        let duplicate =
            numpy_group_commit_v1(state.bind(py).borrow_mut(), partial.bind(py).borrow_mut())
                .unwrap_err();
        assert!(duplicate.is_instance_of::<PyRuntimeError>(py));

        let unsigned_keys = u64_array(py, &[8]);
        let unsigned_values = u64_array(py, &[20]);
        let incompatible = Py::new(
            py,
            numpy_group_partial_v1(&unsigned_keys, Some(&unsigned_values), mask)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let mismatch = numpy_group_commit_v1(
            state.bind(py).borrow_mut(),
            incompatible.bind(py).borrow_mut(),
        )
        .unwrap_err();
        assert!(mismatch.is_instance_of::<PyValueError>(py));

        let next_keys = i64_array(py, &[7]);
        let next_values = i64_array(py, &[5]);
        let next = Py::new(
            py,
            numpy_group_partial_v1(&next_keys, Some(&next_values), mask)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        numpy_group_commit_v1(state.bind(py).borrow_mut(), next.bind(py).borrow_mut()).unwrap();
        let result = numpy_group_finalize_v1(py, state.bind(py).borrow_mut()).unwrap();
        assert_eq!(
            result
                .bind(py)
                .get_item(2)
                .unwrap()
                .extract::<Vec<i128>>()
                .unwrap(),
            vec![15]
        );
    });
}

#[test]
fn invalid_lane_masks_are_rejected_before_state_or_partial_work() {
    Python::initialize();
    Python::attach(|py| {
        for mask in [0, 1 << 4, u8::MAX] {
            let state_error = numpy_group_state_v1(mask).err().unwrap();
            assert!(state_error.is_instance_of::<PyValueError>(py));

            let keys = i64_array(py, &[1]);
            let partial_error = numpy_group_partial_v1(&keys, None, mask).err().unwrap();
            assert!(partial_error.is_instance_of::<PyValueError>(py));
        }
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn strided_partial_v2_scans_positive_stride_and_owns_its_snapshot() {
    Python::initialize();
    Python::attach(|py| {
        let mask = AGGREGATE_COUNT | AGGREGATE_TOTAL | AGGREGATE_MINIMUM | AGGREGATE_MAXIMUM;
        let state = state(py, mask);
        let key_exporter = i64_array(py, &[2, 99, 1, 99, 2, 99]);
        let value_exporter = i64_array(py, &[10, 0, 20, 0, -3, 0]);
        let keys = strided_view(py, &key_exporter, 2);
        let values = strided_view(py, &value_exporter, 2);

        let partial = Py::new(
            py,
            numpy_group_strided_partial_v2(&keys, Some(&values), mask)
                .unwrap()
                .unwrap(),
        )
        .unwrap();

        // The endpoint must finish the attached scan before returning, not retain exporter data.
        key_exporter.call_method1("__setitem__", (0, 777)).unwrap();
        value_exporter
            .call_method1("__setitem__", (0, 777))
            .unwrap();
        numpy_group_commit_v1(state.bind(py).borrow_mut(), partial.bind(py).borrow_mut()).unwrap();

        let result = numpy_group_finalize_v1(py, state.bind(py).borrow_mut()).unwrap();
        let result = result.bind(py);
        assert_eq!(
            result.get_item(0).unwrap().extract::<Vec<i64>>().unwrap(),
            vec![2, 1]
        );
        assert_eq!(
            result.get_item(1).unwrap().extract::<Vec<u128>>().unwrap(),
            vec![2, 1]
        );
        assert_eq!(
            result.get_item(2).unwrap().extract::<Vec<i128>>().unwrap(),
            vec![7, 20]
        );
        assert_eq!(
            result.get_item(3).unwrap().extract::<Vec<i64>>().unwrap(),
            vec![-3, 20]
        );
        assert_eq!(
            result.get_item(4).unwrap().extract::<Vec<i64>>().unwrap(),
            vec![10, 20]
        );
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn strided_partial_v2_scans_negative_stride_in_logical_order() {
    Python::initialize();
    Python::attach(|py| {
        let mask = AGGREGATE_COUNT | AGGREGATE_TOTAL;
        let group_state = state(py, mask);
        let key_exporter = u64_array(py, &[1, 2, 3]);
        let value_exporter = u64_array(py, &[10, 20, 30]);
        let keys = strided_view(py, &key_exporter, -1);
        let values = strided_view(py, &value_exporter, -1);
        let partial = Py::new(
            py,
            numpy_group_strided_partial_v2(&keys, Some(&values), mask)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        numpy_group_commit_v1(
            group_state.bind(py).borrow_mut(),
            partial.bind(py).borrow_mut(),
        )
        .unwrap();

        let result = numpy_group_finalize_v1(py, group_state.bind(py).borrow_mut()).unwrap();
        let result = result.bind(py);
        assert_eq!(
            result.get_item(0).unwrap().extract::<Vec<u64>>().unwrap(),
            vec![3, 2, 1]
        );
        assert_eq!(
            result.get_item(2).unwrap().extract::<Vec<u128>>().unwrap(),
            vec![30, 20, 10]
        );

        let count_state = state(py, AGGREGATE_COUNT);
        let count_partial = Py::new(
            py,
            numpy_group_strided_partial_v2(&keys, None, AGGREGATE_COUNT)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        numpy_group_commit_v1(
            count_state.bind(py).borrow_mut(),
            count_partial.bind(py).borrow_mut(),
        )
        .unwrap();
        let counts = numpy_group_finalize_v1(py, count_state.bind(py).borrow_mut()).unwrap();
        assert_eq!(
            counts
                .bind(py)
                .get_item(1)
                .unwrap()
                .extract::<Vec<u128>>()
                .unwrap(),
            vec![1, 1, 1]
        );
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn strided_partial_v2_accepts_direct_zero_stride_bool_buffers() {
    Python::initialize();
    Python::attach(|py| {
        let mask = AGGREGATE_COUNT | AGGREGATE_TOTAL | AGGREGATE_MINIMUM | AGGREGATE_MAXIMUM;
        let state = state(py, mask);
        let keys = Py::new(
            py,
            TestStridedBuffer {
                storage: vec![1],
                start_bytes: 0,
                length: 3,
                item_size: 1,
                stride: 0,
                format: TestBufferFormat::Bool,
                indirect: false,
            },
        )
        .unwrap();
        let values = Py::new(
            py,
            TestStridedBuffer {
                storage: vec![0],
                start_bytes: 0,
                length: 3,
                item_size: 1,
                stride: 0,
                format: TestBufferFormat::Bool,
                indirect: false,
            },
        )
        .unwrap();
        let partial = Py::new(
            py,
            numpy_group_strided_partial_v2(
                keys.bind(py).as_any(),
                Some(values.bind(py).as_any()),
                mask,
            )
            .unwrap()
            .unwrap(),
        )
        .unwrap();
        numpy_group_commit_v1(state.bind(py).borrow_mut(), partial.bind(py).borrow_mut()).unwrap();

        let result = numpy_group_finalize_v1(py, state.bind(py).borrow_mut()).unwrap();
        let result = result.bind(py);
        assert_eq!(
            result.get_item(0).unwrap().extract::<Vec<bool>>().unwrap(),
            vec![true]
        );
        assert_eq!(
            result.get_item(1).unwrap().extract::<Vec<u128>>().unwrap(),
            vec![3]
        );
        assert_eq!(
            result.get_item(2).unwrap().extract::<Vec<u128>>().unwrap(),
            vec![0]
        );
        assert_eq!(
            result.get_item(3).unwrap().extract::<Vec<bool>>().unwrap(),
            vec![false]
        );
        assert_eq!(
            result.get_item(4).unwrap().extract::<Vec<bool>>().unwrap(),
            vec![false]
        );
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn strided_partial_v2_declines_indirect_misaligned_or_mismatched_buffers() {
    Python::initialize();
    Python::attach(|py| {
        let indirect = Py::new(
            py,
            TestStridedBuffer {
                storage: vec![1],
                start_bytes: 0,
                length: 1,
                item_size: 1,
                stride: 1,
                format: TestBufferFormat::Bool,
                indirect: true,
            },
        )
        .unwrap();
        assert!(
            numpy_group_strided_partial_v2(indirect.bind(py).as_any(), None, AGGREGATE_COUNT,)
                .unwrap()
                .is_none()
        );

        let misaligned = Py::new(
            py,
            TestStridedBuffer {
                storage: vec![0; 2],
                start_bytes: 1,
                length: 1,
                item_size: 8,
                stride: 8,
                format: TestBufferFormat::I64,
                indirect: false,
            },
        )
        .unwrap();
        assert!(
            numpy_group_strided_partial_v2(misaligned.bind(py).as_any(), None, AGGREGATE_COUNT,)
                .unwrap()
                .is_none()
        );

        let misaligned_stride = Py::new(
            py,
            TestStridedBuffer {
                storage: vec![0; 2],
                start_bytes: 0,
                length: 2,
                item_size: 8,
                stride: 4,
                format: TestBufferFormat::I64,
                indirect: false,
            },
        )
        .unwrap();
        assert!(
            numpy_group_strided_partial_v2(
                misaligned_stride.bind(py).as_any(),
                None,
                AGGREGATE_COUNT,
            )
            .unwrap()
            .is_none()
        );

        let keys = i64_array(py, &[1, 2, 3]);
        let short_values = i64_array(py, &[10, 20]);
        assert!(
            numpy_group_strided_partial_v2(&keys, Some(&short_values), AGGREGATE_TOTAL)
                .unwrap()
                .is_none()
        );

        let unsigned_values = u64_array(py, &[10, 20, 30]);
        assert!(
            numpy_group_strided_partial_v2(&keys, Some(&unsigned_values), AGGREGATE_TOTAL)
                .unwrap()
                .is_none()
        );
    });
}
