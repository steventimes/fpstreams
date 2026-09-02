//! Exact-pair and pair-expression grouping coverage.

use super::*;

#[test]
fn exact_tuple_row_group_sum_accepts_lists_and_preserves_first_key_identity() {
    Python::initialize();
    Python::attach(|py| {
        let first_key = pyo3::types::PyInt::new(py, 1_000_i64);
        let equal_key = pyo3::types::PyInt::new(py, 1_000_i64);
        let later_key = pyo3::types::PyInt::new(py, -7_i64);
        assert!(!first_key.is(&equal_key));

        let first_value = pyo3::types::PyInt::new(py, 2_i64);
        let second_value = pyo3::types::PyInt::new(py, 5_i64);
        let third_value = pyo3::types::PyInt::new(py, 3_i64);
        let first = PyTuple::new(py, [first_key.as_any(), first_value.as_any()]).unwrap();
        let second = PyTuple::new(py, [later_key.as_any(), second_value.as_any()]).unwrap();
        let third = PyTuple::new(py, [equal_key.as_any(), third_value.as_any()]).unwrap();

        let list_source = PyList::new(py, [&first, &second, &third]).unwrap();
        let list_groups = group_sum_i64_pairs(list_source.as_any(), 0, 1)
            .unwrap()
            .unwrap();
        assert_eq!(list_groups.len(), 2);
        assert!(list_groups[0].0.bind(py).is(&first_key));
        assert_eq!(list_groups[0].1, 5);
        assert!(list_groups[1].0.bind(py).is(&later_key));
        assert_eq!(list_groups[1].1, 5);

        let tuple_source = PyTuple::new(py, [&first, &second, &third]).unwrap();
        let tuple_groups = group_sum_i64_pairs(tuple_source.as_any(), 0, 1)
            .unwrap()
            .unwrap();
        assert_eq!(tuple_groups.len(), 2);
        assert!(tuple_groups[0].0.bind(py).is(&first_key));
        assert_eq!(tuple_groups[0].1, 5);
        assert!(tuple_groups[1].0.bind(py).is(&later_key));
        assert_eq!(tuple_groups[1].1, 5);
    });
}

#[test]
fn exact_pair_group_sum_v1_accepts_only_two_tuple_i64_rows_and_preserves_order_identity() {
    Python::initialize();
    Python::attach(|py| {
        let first_key = pyo3::types::PyInt::new(py, 1_000_i64);
        let equal_key = pyo3::types::PyInt::new(py, 1_000_i64);
        let later_key = pyo3::types::PyInt::new(py, -7_i64);
        assert!(!first_key.is(&equal_key));

        let first = PyTuple::new(
            py,
            [first_key.as_any(), pyo3::types::PyInt::new(py, 2).as_any()],
        )
        .unwrap();
        let second = PyTuple::new(
            py,
            [later_key.as_any(), pyo3::types::PyInt::new(py, 5).as_any()],
        )
        .unwrap();
        let third = PyTuple::new(
            py,
            [equal_key.as_any(), pyo3::types::PyInt::new(py, 3).as_any()],
        )
        .unwrap();

        for source in [
            PyList::new(py, [&first, &second, &third])
                .unwrap()
                .into_any(),
            PyTuple::new(py, [&first, &second, &third])
                .unwrap()
                .into_any(),
        ] {
            let groups = group_sum_i64_exact_pairs_v1(source.as_any())
                .unwrap()
                .unwrap();
            assert_eq!(groups.len(), 2);
            assert!(groups[0].0.bind(py).is(&first_key));
            assert_eq!(groups[0].1, 5);
            assert!(groups[1].0.bind(py).is(&later_key));
            assert_eq!(groups[1].1, 5);
        }

        let empty = PyList::empty(py);
        assert!(
            group_sum_i64_exact_pairs_v1(empty.as_any())
                .unwrap()
                .unwrap()
                .is_empty()
        );
        let empty = PyTuple::empty(py);
        assert!(
            group_sum_i64_exact_pairs_v1(empty.as_any())
                .unwrap()
                .unwrap()
                .is_empty()
        );

        let maximum = PyTuple::new(py, [1_i64, i64::MAX]).unwrap();
        let one = PyTuple::new(py, [1_i64, 1_i64]).unwrap();
        let widened_source = PyList::new(py, [&maximum, &one]).unwrap();
        assert_eq!(
            group_sum_i64_exact_pairs_v1(widened_source.as_any())
                .unwrap()
                .unwrap()[0]
                .1,
            i128::from(i64::MAX) + 1
        );
    });
}

#[test]
fn exact_pair_group_sum_v2_switches_to_the_final_dictionary_at_4096_groups() {
    Python::initialize();
    Python::attach(|py| {
        let output_name = PyString::new(py, "total");
        let small = PyList::empty(py);
        for key in 0..4_095_i64 {
            small.append((key, 1_i64)).unwrap();
        }
        let (is_final, payload) =
            group_sum_i64_exact_pairs_v2(small.as_any(), output_name.as_any())
                .unwrap()
                .unwrap();
        assert!(!is_final);
        assert_eq!(
            payload.bind(py).cast_exact::<PyList>().unwrap().len(),
            4_095
        );

        let first_key = pyo3::types::PyInt::new(py, 1_000_000_i64);
        let equal_key = pyo3::types::PyInt::new(py, 1_000_000_i64);
        assert!(!first_key.is(&equal_key));
        let large = PyList::empty(py);
        large.append((&first_key, i64::MAX)).unwrap();
        for key in 0..4_095_i64 {
            large.append((key, 1_i64)).unwrap();
        }
        large.append((&equal_key, 1_i64)).unwrap();

        let (is_final, payload) =
            group_sum_i64_exact_pairs_v2(large.as_any(), output_name.as_any())
                .unwrap()
                .unwrap();

        assert!(is_final);
        let result = payload.bind(py).cast_exact::<PyDict>().unwrap();
        assert_eq!(result.len(), 4_096);
        assert!(result.keys().get_item(0).unwrap().is(&first_key));
        let first_values = result
            .get_item(&first_key)
            .unwrap()
            .unwrap()
            .cast_into::<PyDict>()
            .unwrap();
        assert!(first_values.keys().get_item(0).unwrap().is(&output_name));
        assert_eq!(
            first_values
                .get_item(&output_name)
                .unwrap()
                .unwrap()
                .extract::<i128>()
                .unwrap(),
            i128::from(i64::MAX) + 1
        );
        assert_eq!(
            result.keys().get_item(1).unwrap().extract::<i64>().unwrap(),
            0
        );
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn pair_expression_group_sum_preserves_arithmetic_order_and_widens_totals() {
    Python::initialize();
    Python::attach(|py| {
        let key_name = PyString::new(py, "key");
        let output_name = PyString::new(py, "total");
        let first = PyTuple::new(py, [3_i64, i64::MAX]).unwrap();
        let second = PyTuple::new(py, [1_i64, i64::MAX]).unwrap();
        let third = PyTuple::new(py, [4_i64, 1_i64]).unwrap();
        let fourth = PyTuple::new(py, [8_i64, 2_i64]).unwrap();
        let key_modulo_three = vec![(19, 0), (1, 3), (6, 0)];
        let value_identity = vec![(0, 0)];
        let expected = vec![
            (0_i64, i128::from(i64::MAX)),
            (1_i64, i128::from(i64::MAX) + 1),
            (2_i64, 2_i128),
        ];

        for source in [
            PyList::new(py, [&first, &second, &third, &fourth])
                .unwrap()
                .into_any(),
            PyTuple::new(py, [&first, &second, &third, &fourth])
                .unwrap()
                .into_any(),
        ] {
            let (is_final_rows, payload) = crate::relational::group_sum_i64_pair_expr_rows_v1(
                source.as_any(),
                key_modulo_three.clone(),
                value_identity.clone(),
                key_name.as_any(),
                output_name.as_any(),
            )
            .unwrap()
            .unwrap();
            assert!(!is_final_rows);
            assert_eq!(
                payload.bind(py).extract::<Vec<(i64, i128)>>().unwrap(),
                expected
            );
        }

        let first_modulo_key = pyo3::types::PyInt::new(py, 5_000_i64);
        let equal_modulo_key = pyo3::types::PyInt::new(py, 5_000_i64);
        assert!(!first_modulo_key.is(&equal_modulo_key));
        let first_identity_row = PyTuple::new(
            py,
            [
                first_modulo_key.as_any(),
                pyo3::types::PyInt::new(py, 2_i64).as_any(),
            ],
        )
        .unwrap();
        let equal_identity_row = PyTuple::new(
            py,
            [
                equal_modulo_key.as_any(),
                pyo3::types::PyInt::new(py, 3_i64).as_any(),
            ],
        )
        .unwrap();
        let identity_source = PyList::new(py, [&first_identity_row, &equal_identity_row]).unwrap();
        let (is_final_rows, payload) = crate::relational::group_sum_i64_pair_expr_rows_v1(
            identity_source.as_any(),
            vec![(19, 0), (1, 1_000_000_000_000), (6, 0)],
            value_identity.clone(),
            key_name.as_any(),
            output_name.as_any(),
        )
        .unwrap()
        .unwrap();
        assert!(!is_final_rows);
        let pairs = payload.bind(py).cast_exact::<PyList>().unwrap();
        let first_pair = pairs.get_item(0).unwrap();
        let first_pair = first_pair.cast_exact::<PyTuple>().unwrap();
        assert!(first_pair.get_item(0).unwrap().is(&first_modulo_key));

        let negatives = PyList::new(
            py,
            [(-7_i64, -7_i64), (-8, -8), (-1, -1), (7, 7), (8, 8), (1, 1)],
        )
        .unwrap();
        let (is_final_rows, payload) = crate::relational::group_sum_i64_pair_expr_rows_v1(
            negatives.as_any(),
            vec![(19, 0), (1, 3), (5, 0)],
            vec![(0, 0), (1, -3), (6, 0)],
            key_name.as_any(),
            output_name.as_any(),
        )
        .unwrap()
        .unwrap();
        assert!(!is_final_rows);
        assert_eq!(
            payload.bind(py).extract::<Vec<(i64, i128)>>().unwrap(),
            vec![(-3, -3), (-1, -1), (2, -3), (0, -2)]
        );
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn pair_expression_group_sum_declines_unsafe_rows_and_checked_programs() {
    Python::initialize();
    Python::attach(|py| {
        let key_name = PyString::new(py, "key");
        let output_name = PyString::new(py, "total");
        let key_identity = vec![(19, 0)];
        let value_identity = vec![(0, 0)];
        let valid = PyTuple::new(py, [1_i64, 2_i64]).unwrap();
        let list_row = PyList::new(py, [3_i64, 4_i64]).unwrap();
        let late_list = PyList::new(py, [valid.as_any(), list_row.as_any()]).unwrap();
        let boolean = PyTuple::new(py, [true, false]).unwrap();
        let boolean_source = PyList::new(py, [&boolean]).unwrap();
        let huge = py.eval(c"1 << 100", None, None).unwrap();
        let huge_row =
            PyTuple::new(py, [huge.as_any(), pyo3::types::PyInt::new(py, 1).as_any()]).unwrap();
        let huge_source = PyTuple::new(py, [&huge_row]).unwrap();

        for source in [
            late_list.into_any(),
            boolean_source.into_any(),
            huge_source.into_any(),
        ] {
            assert!(
                crate::relational::group_sum_i64_pair_expr_rows_v1(
                    source.as_any(),
                    key_identity.clone(),
                    value_identity.clone(),
                    key_name.as_any(),
                    output_name.as_any(),
                )
                .unwrap()
                .is_none()
            );
        }

        let key_overflow = PyList::new(py, [(i64::MAX, 1_i64)]).unwrap();
        assert!(
            crate::relational::group_sum_i64_pair_expr_rows_v1(
                key_overflow.as_any(),
                vec![(19, 0), (1, 1), (2, 0)],
                value_identity.clone(),
                key_name.as_any(),
                output_name.as_any(),
            )
            .unwrap()
            .is_none()
        );
        let value_overflow = PyList::new(py, [(1_i64, i64::MIN)]).unwrap();
        assert!(
            crate::relational::group_sum_i64_pair_expr_rows_v1(
                value_overflow.as_any(),
                key_identity.clone(),
                vec![(0, 0), (7, 0)],
                key_name.as_any(),
                output_name.as_any(),
            )
            .unwrap()
            .is_none()
        );
        let valid_source = PyList::new(py, [&valid]).unwrap();
        assert!(
            crate::relational::group_sum_i64_pair_expr_rows_v1(
                valid_source.as_any(),
                vec![(19, 0), (1, 0), (6, 0)],
                value_identity.clone(),
                key_name.as_any(),
                output_name.as_any(),
            )
            .unwrap()
            .is_none()
        );

        let empty = PyList::empty(py);
        for boolean_program in [vec![(19, 0), (1, 0), (8, 0)], vec![(19, 0), (16, 0)]] {
            assert!(
                crate::relational::group_sum_i64_pair_expr_rows_v1(
                    empty.as_any(),
                    boolean_program,
                    value_identity.clone(),
                    key_name.as_any(),
                    output_name.as_any(),
                )
                .unwrap()
                .is_none()
            );
        }
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn pair_expression_group_sum_switches_to_final_rows_at_4096_groups() {
    Python::initialize();
    Python::attach(|py| {
        let key_name = PyString::new(py, "key");
        let output_name = PyString::new(py, "total");
        let small = PyList::empty(py);
        for key in 0..4_095_i64 {
            small.append((key, 0_i64)).unwrap();
        }
        let (is_final_rows, payload) = crate::relational::group_sum_i64_pair_expr_rows_v1(
            small.as_any(),
            vec![(19, 0), (1, 1), (2, 0)],
            vec![(1, 1)],
            key_name.as_any(),
            output_name.as_any(),
        )
        .unwrap()
        .unwrap();
        assert!(!is_final_rows);
        let pairs = payload.bind(py).cast_exact::<PyList>().unwrap();
        assert_eq!(pairs.len(), 4_095);
        assert_eq!(
            pairs.get_item(0).unwrap().extract::<(i64, i64)>().unwrap(),
            (1, 1)
        );
        assert_eq!(
            pairs
                .get_item(4_094)
                .unwrap()
                .extract::<(i64, i64)>()
                .unwrap(),
            (4_095, 1)
        );

        let large = PyList::empty(py);
        for key in 0..4_096_i64 {
            large.append((key, 0_i64)).unwrap();
        }
        let (is_final_rows, payload) = crate::relational::group_sum_i64_pair_expr_rows_v1(
            large.as_any(),
            vec![(19, 0), (1, 1), (2, 0)],
            vec![(1, 1)],
            key_name.as_any(),
            output_name.as_any(),
        )
        .unwrap()
        .unwrap();
        assert!(is_final_rows);
        let rows = payload.bind(py).cast_exact::<PyList>().unwrap();
        assert_eq!(rows.len(), 4_096);
        for (index, expected_key) in [(0, 1_i64), (4_095, 4_096_i64)] {
            let row = rows.get_item(index).unwrap().cast_into::<PyDict>().unwrap();
            assert_eq!(
                row.get_item(&key_name)
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                expected_key
            );
            assert_eq!(
                row.get_item(&output_name)
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                1
            );
        }
    });
}

#[test]
fn exact_pair_group_sum_v1_declines_nonexact_or_non_i64_pair_shapes_without_protocol_calls() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"class Row(tuple):\n    calls = 0\n    def __iter__(self):\n        type(self).calls += 1\n        raise AssertionError('row protocol called')\nclass Integer(int):\n    calls = 0\n    def __index__(self):\n        type(self).calls += 1\n        raise AssertionError('integer protocol called')\nclass OuterList(list):\n    calls = 0\n    def __iter__(self):\n        type(self).calls += 1\n        raise AssertionError('outer list protocol called')\nclass OuterTuple(tuple):\n    calls = 0\n    def __iter__(self):\n        type(self).calls += 1\n        raise AssertionError('outer tuple protocol called')\nrow = Row((1, 2))\ninteger = Integer(1)\nouter_list = OuterList([(1, 2)])\nouter_tuple = OuterTuple(((1, 2),))\n",
            c"strict_pair_shapes.py",
            c"strict_pair_shapes",
        )
        .unwrap();
        let row_subclass = fixture.getattr("row").unwrap();
        let integer_subclass = fixture.getattr("integer").unwrap();
        let outer_list_subclass = fixture.getattr("outer_list").unwrap();
        let outer_tuple_subclass = fixture.getattr("outer_tuple").unwrap();
        let huge = py.eval(c"1 << 100", None, None).unwrap();
        let true_value = py.eval(c"True", None, None).unwrap();
        let false_value = py.eval(c"False", None, None).unwrap();

        let short = PyTuple::new(py, [1_i64]).unwrap();
        let wide = PyTuple::new(py, [1_i64, 2_i64, 3_i64]).unwrap();
        let valid = PyTuple::new(py, [1_i64, 2_i64]).unwrap();
        let list_row = PyList::new(py, [1_i64, 2_i64]).unwrap();
        let boolean_key = PyTuple::new(
            py,
            [true_value.as_any(), pyo3::types::PyInt::new(py, 2).as_any()],
        )
        .unwrap();
        let boolean_value = PyTuple::new(
            py,
            [
                pyo3::types::PyInt::new(py, 1).as_any(),
                false_value.as_any(),
            ],
        )
        .unwrap();
        let subclass_key = PyTuple::new(
            py,
            [&integer_subclass, pyo3::types::PyInt::new(py, 2).as_any()],
        )
        .unwrap();
        let huge_value =
            PyTuple::new(py, [pyo3::types::PyInt::new(py, 1).as_any(), &huge]).unwrap();

        let loose_wide = PyList::new(py, [&wide]).unwrap();
        assert_eq!(
            group_sum_i64_pairs(loose_wide.as_any(), 0, 1)
                .unwrap()
                .unwrap()[0]
                .1,
            2
        );
        let late_wide = PyList::new(py, [&valid, &wide]).unwrap();
        assert!(
            group_sum_i64_exact_pairs_v1(late_wide.as_any())
                .unwrap()
                .is_none()
        );

        for row in [
            short.into_any(),
            wide.into_any(),
            list_row.into_any(),
            row_subclass,
            boolean_key.into_any(),
            boolean_value.into_any(),
            subclass_key.into_any(),
            huge_value.into_any(),
        ] {
            let source = PyList::new(py, [&row]).unwrap();
            assert!(
                group_sum_i64_exact_pairs_v1(source.as_any())
                    .unwrap()
                    .is_none()
            );
        }

        assert!(
            group_sum_i64_exact_pairs_v1(outer_list_subclass.as_any())
                .unwrap()
                .is_none()
        );
        assert!(
            group_sum_i64_exact_pairs_v1(outer_tuple_subclass.as_any())
                .unwrap()
                .is_none()
        );

        assert_eq!(
            fixture
                .getattr("Row")
                .unwrap()
                .getattr("calls")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            0
        );
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
                .getattr("OuterList")
                .unwrap()
                .getattr("calls")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            0
        );
        assert_eq!(
            fixture
                .getattr("OuterTuple")
                .unwrap()
                .getattr("calls")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            0
        );
        assert!(
            group_sum_i64_exact_pairs_v1(py.None().bind(py))
                .unwrap()
                .is_none()
        );
    });
}
