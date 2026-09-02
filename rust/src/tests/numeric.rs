//! Fused numeric pipeline and terminal tests.

use super::*;
use pyo3::types::PyBytesMethods;
#[cfg(not(Py_GIL_DISABLED))]
use std::sync::Mutex;

mod buffers;
mod kernels;
mod reductions;

use buffers::{assert_same_f64, assert_same_optional_f64};
use reductions::legacy_compensated_total;

#[cfg(not(Py_GIL_DISABLED))]
static BUFFER_GIL_TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn exact_i64_numpy_pack_is_native_endian_and_declines_without_protocol_dispatch() {
    Python::initialize();
    Python::attach(|py| {
        let values = PyList::new(py, [i64::MIN, -2, 0, i64::MAX]).unwrap();
        let packed = crate::numpy_export::pack_i64_exact_sequence_v1(py, values.as_any())
            .unwrap()
            .unwrap();
        let bytes = packed.bind(py).as_bytes();
        let decoded = bytes
            .chunks_exact(size_of::<i64>())
            .map(|chunk| i64::from_ne_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(decoded, vec![i64::MIN, -2, 0, i64::MAX]);

        let fixture = PyModule::from_code(
            py,
            c"class Integer(int):\n    calls = 0\n    def __index__(self):\n        type(self).calls += 1\n        raise AssertionError('integer protocol called')\ninteger = Integer(1)\n",
            c"exact_i64_numpy_pack.py",
            c"exact_i64_numpy_pack",
        )
        .unwrap();
        for incompatible in [
            PyList::new(py, [fixture.getattr("integer").unwrap()]).unwrap(),
            PyList::new(py, [true]).unwrap(),
            PyList::new(py, [py.eval(c"1 << 100", None, None).unwrap()]).unwrap(),
        ] {
            assert!(
                crate::numpy_export::pack_i64_exact_sequence_v1(py, incompatible.as_any())
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(
            fixture
                .getattr("Integer")
                .unwrap()
                .getattr("calls")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            0
        );
    });
}

fn f64_array<'py>(py: Python<'py>, values: Vec<f64>) -> Bound<'py, PyAny> {
    PyModule::import(py, "array")
        .unwrap()
        .getattr("array")
        .unwrap()
        .call1(("d", values))
        .unwrap()
}

fn i64_array<'py>(py: Python<'py>, values: Vec<i64>) -> Bound<'py, PyAny> {
    PyModule::import(py, "array")
        .unwrap()
        .getattr("array")
        .unwrap()
        .call1(("q", values))
        .unwrap()
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn exact_i64_sequence_snapshot_accepts_only_builtin_ints_without_protocol_dispatch() {
    Python::initialize();
    Python::attach(|py| {
        let list = PyList::new(py, [i64::MIN, -1, 0, i64::MAX]).unwrap();
        assert_eq!(
            crate::common::snapshot_exact_i64_sequence(list.as_any()).unwrap(),
            vec![i64::MIN, -1, 0, i64::MAX]
        );

        let tuple = PyTuple::new(py, [3_i64, 2, 1]).unwrap();
        assert_eq!(
            crate::common::snapshot_exact_i64_sequence(tuple.as_any()).unwrap(),
            vec![3, 2, 1]
        );

        let fixture = PyModule::from_code(
            py,
            c"class Integer(int):\n    calls = 0\n    def __index__(self):\n        type(self).calls += 1\n        raise AssertionError('integer protocol called')\ninteger = Integer(1)\n",
            c"exact_i64_sequence_snapshot.py",
            c"exact_i64_sequence_snapshot",
        )
        .unwrap();
        let subclass = PyList::new(py, [fixture.getattr("integer").unwrap()]).unwrap();
        let subclass_error =
            crate::common::snapshot_exact_i64_sequence(subclass.as_any()).unwrap_err();
        assert!(subclass_error.is_instance_of::<pyo3::exceptions::PyTypeError>(py));
        assert_eq!(
            fixture
                .getattr("Integer")
                .unwrap()
                .getattr("calls")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            0
        );

        let booleans = PyTuple::new(py, [true]).unwrap();
        let boolean_error =
            crate::common::snapshot_exact_i64_sequence(booleans.as_any()).unwrap_err();
        assert!(boolean_error.is_instance_of::<pyo3::exceptions::PyTypeError>(py));

        let huge = py.eval(c"1 << 100", None, None).unwrap();
        let overflowing = PyList::new(py, [huge]).unwrap();
        let overflow_error =
            crate::common::snapshot_exact_i64_sequence(overflowing.as_any()).unwrap_err();
        assert!(overflow_error.is_instance_of::<pyo3::exceptions::PyOverflowError>(py));
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn direct_exact_i64_map_materializer_accepts_only_arithmetic_roots() {
    Python::initialize();
    Python::attach(|py| {
        let list = PyList::new(py, [-5_i64, -1, 0, 5]).unwrap();
        let tuple = PyTuple::new(py, [-5_i64, -1, 0, 5]).unwrap();
        let cases = [
            (vec![(0, 0), (1, 2), (2, 0)], vec![-3_i64, 1, 2, 7]),
            (vec![(0, 0), (1, 2), (3, 0)], vec![-7_i64, -3, -2, 3]),
            (vec![(0, 0), (1, -2), (4, 0)], vec![10_i64, 2, 0, -10]),
            (vec![(0, 0), (1, 2), (5, 0)], vec![-3_i64, -1, 0, 2]),
            (vec![(0, 0), (7, 0)], vec![5_i64, 1, 0, -5]),
            (
                vec![(0, 0), (1, 2), (1, 3), (4, 0), (2, 0)],
                vec![1_i64, 5, 6, 11],
            ),
        ];

        for source in [list.as_any(), tuple.as_any()] {
            for (instructions, expected) in &cases {
                let output = crate::integer::materialize_i64_map_exact_list_v1(
                    py,
                    source,
                    instructions.clone(),
                )
                .unwrap()
                .unwrap();
                assert!(output.bind(py).is_exact_instance_of::<PyList>());
                assert_eq!(output.extract::<Vec<i64>>(py).unwrap(), *expected);
            }
        }

        let empty = PyList::empty(py);
        let output = crate::integer::materialize_i64_map_exact_list_v1(
            py,
            empty.as_any(),
            vec![(0, 0), (1, 1), (2, 0)],
        )
        .unwrap()
        .unwrap();
        assert!(output.extract::<Vec<i64>>(py).unwrap().is_empty());
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn direct_exact_i64_map_materializer_validates_postfix_before_materializing() {
    Python::initialize();
    Python::attach(|py| {
        let source = PyList::new(py, [1_i64, 2, 3]).unwrap();
        let invalid_programs = [
            vec![],
            vec![(99, 0)],
            vec![(2, 0)],
            vec![(7, 0)],
            vec![(0, 0), (1, 1)],
            vec![(1, 2), (1, 3), (2, 0)],
            vec![(0, 0)],
            vec![(1, 7)],
            vec![(0, 0), (17, 0)],
            vec![(0, 0), (1, 2), (6, 0)],
            vec![(0, 0), (1, 2), (8, 0)],
        ];

        for instructions in invalid_programs {
            let error = crate::integer::materialize_i64_map_exact_list_v1(
                py,
                source.as_any(),
                instructions,
            )
            .unwrap_err();
            assert!(error.is_instance_of::<pyo3::exceptions::PyValueError>(py));
        }

        let nonsequence = PyDict::new(py);
        let error = crate::integer::materialize_i64_map_exact_list_v1(
            py,
            nonsequence.as_any(),
            vec![(2, 0)],
        )
        .unwrap_err();
        assert!(error.is_instance_of::<pyo3::exceptions::PyValueError>(py));

        let empty = PyList::empty(py);
        let error =
            crate::integer::materialize_i64_map_exact_list_v1(py, empty.as_any(), vec![(0, 0)])
                .unwrap_err();
        assert!(error.is_instance_of::<pyo3::exceptions::PyValueError>(py));
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn direct_exact_i64_map_materializer_declines_without_touching_the_source() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"class Integer(int):\n    calls = 0\n    def __index__(self):\n        type(self).calls += 1\n        raise AssertionError('integer protocol called')\nclass Sequence(list):\n    pass\ninteger = Integer(7)\nmixed = [1, integer, 3]\nhuge_tail = [1, 1 << 100]\nfloat_tail = [1, 2.5]\nboolean_tail = [1, True]\nsubclass_source = Sequence([1, 2])\n",
            c"direct_exact_i64_map.py",
            c"direct_exact_i64_map",
        )
        .unwrap();
        let add_one = vec![(0, 0), (1, 1), (2, 0)];

        for name in [
            "mixed",
            "huge_tail",
            "float_tail",
            "boolean_tail",
            "subclass_source",
        ] {
            let source = fixture.getattr(name).unwrap();
            assert!(
                crate::integer::materialize_i64_map_exact_list_v1(py, &source, add_one.clone())
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(
            fixture
                .getattr("Integer")
                .unwrap()
                .getattr("calls")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            0
        );
        assert_eq!(
            fixture
                .getattr("mixed")
                .unwrap()
                .repr()
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "[1, 7, 3]"
        );

        let overflow = PyList::new(py, [1_i64, i64::MAX]).unwrap();
        assert!(
            crate::integer::materialize_i64_map_exact_list_v1(py, overflow.as_any(), add_one)
                .unwrap()
                .is_none()
        );
        assert_eq!(overflow.extract::<Vec<i64>>().unwrap(), vec![1, i64::MAX]);

        let negation_overflow = PyList::new(py, [1_i64, i64::MIN]).unwrap();
        assert!(
            crate::integer::materialize_i64_map_exact_list_v1(
                py,
                negation_overflow.as_any(),
                vec![(0, 0), (7, 0)],
            )
            .unwrap()
            .is_none()
        );

        let division_overflow = PyList::new(py, [1_i64, i64::MIN]).unwrap();
        assert!(
            crate::integer::materialize_i64_map_exact_list_v1(
                py,
                division_overflow.as_any(),
                vec![(0, 0), (1, -1), (5, 0)],
            )
            .unwrap()
            .is_none()
        );

        let division_by_zero = PyTuple::new(py, [4_i64, 0]).unwrap();
        assert!(
            crate::integer::materialize_i64_map_exact_list_v1(
                py,
                division_by_zero.as_any(),
                vec![(1, 10), (0, 0), (5, 0)],
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(division_by_zero.extract::<Vec<i64>>().unwrap(), vec![4, 0]);
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn direct_exact_i64_filter_materializer_retains_selected_object_identity() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"values = [int(str(value)) for value in (1000, 1001, 1002, 1003)]\ntuple_values = tuple(values)\n",
            c"direct_exact_i64_filter.py",
            c"direct_exact_i64_filter",
        )
        .unwrap();
        let even = vec![(0, 0), (1, 2), (6, 0), (1, 0), (8, 0)];

        for source_name in ["values", "tuple_values"] {
            let source = fixture.getattr(source_name).unwrap();
            for (negated, expected, positions) in [
                (false, vec![1000_i64, 1002], vec![0_usize, 2]),
                (true, vec![1001_i64, 1003], vec![1_usize, 3]),
            ] {
                let output = crate::integer::materialize_i64_filter_exact_list_v1(
                    py,
                    &source,
                    even.clone(),
                    negated,
                )
                .unwrap()
                .unwrap();
                let output = output.bind(py).cast_exact::<PyList>().unwrap();
                assert_eq!(output.extract::<Vec<i64>>().unwrap(), expected);
                for (output_index, source_index) in positions.into_iter().enumerate() {
                    assert!(
                        output
                            .get_item(output_index)
                            .unwrap()
                            .is(source.get_item(source_index).unwrap())
                    );
                }
            }
        }
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn direct_exact_i64_filter_materializer_accepts_numeric_and_constant_predicates() {
    Python::initialize();
    Python::attach(|py| {
        let source = PyList::new(py, [0_i64, 2, 0, -3]).unwrap();
        let cases = [
            (vec![(0, 0)], false, vec![2_i64, -3]),
            (vec![(1, 1)], false, vec![0_i64, 2, 0, -3]),
            (vec![(1, 0)], false, vec![]),
            (vec![(1, 0)], true, vec![0_i64, 2, 0, -3]),
        ];
        for (instructions, negated, expected) in cases {
            let output = crate::integer::materialize_i64_filter_exact_list_v1(
                py,
                source.as_any(),
                instructions,
                negated,
            )
            .unwrap()
            .unwrap();
            assert_eq!(output.extract::<Vec<i64>>(py).unwrap(), expected);
        }

        for instructions in [
            vec![],
            vec![(99, 0)],
            vec![(2, 0)],
            vec![(7, 0)],
            vec![(0, 0), (1, 1)],
        ] {
            let error = crate::integer::materialize_i64_filter_exact_list_v1(
                py,
                source.as_any(),
                instructions,
                false,
            )
            .unwrap_err();
            assert!(error.is_instance_of::<pyo3::exceptions::PyValueError>(py));
        }
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn direct_exact_i64_filter_materializer_declines_without_mutating_the_source() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"class Integer(int):\n    calls = 0\n    def __index__(self):\n        type(self).calls += 1\n        raise AssertionError('integer protocol called')\nclass Sequence(list):\n    pass\nmixed = [2, Integer(3), 4]\nhuge_tail = [2, 1 << 100]\nfloat_tail = [2, 3.5]\nboolean_tail = [2, True]\nsubclass_source = Sequence([2, 4])\n",
            c"direct_exact_i64_filter_decline.py",
            c"direct_exact_i64_filter_decline",
        )
        .unwrap();
        let predicate = vec![(0, 0), (1, 0), (9, 0)];

        for name in [
            "mixed",
            "huge_tail",
            "float_tail",
            "boolean_tail",
            "subclass_source",
        ] {
            let source = fixture.getattr(name).unwrap();
            assert!(
                crate::integer::materialize_i64_filter_exact_list_v1(
                    py,
                    &source,
                    predicate.clone(),
                    false,
                )
                .unwrap()
                .is_none()
            );
        }
        assert_eq!(
            fixture
                .getattr("Integer")
                .unwrap()
                .getattr("calls")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            0
        );
        assert_eq!(
            fixture
                .getattr("mixed")
                .unwrap()
                .repr()
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "[2, 3, 4]"
        );

        let overflow = PyList::new(py, [1_i64, i64::MAX]).unwrap();
        assert!(
            crate::integer::materialize_i64_filter_exact_list_v1(
                py,
                overflow.as_any(),
                vec![(0, 0), (1, 1), (2, 0)],
                false,
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(overflow.extract::<Vec<i64>>().unwrap(), vec![1, i64::MAX]);

        let division_by_zero = PyTuple::new(py, [4_i64, 0]).unwrap();
        assert!(
            crate::integer::materialize_i64_filter_exact_list_v1(
                py,
                division_by_zero.as_any(),
                vec![(1, 10), (0, 0), (5, 0)],
                false,
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(division_by_zero.extract::<Vec<i64>>().unwrap(), vec![4, 0]);
    });
}
