//! Fixed-schema grouping tests.

use super::*;

#[test]
fn fixed_tuple_group_returns_count_pairs_and_count_sum_triples_with_identity_and_order() {
    Python::initialize();
    Python::attach(|py| {
        let first_key = pyo3::types::PyInt::new(py, 1_000_i64);
        let equal_key = pyo3::types::PyInt::new(py, 1_000_i64);
        let later_key = pyo3::types::PyInt::new(py, -7_i64);
        assert!(!first_key.is(&equal_key));
        let first = PyTuple::new(
            py,
            [
                first_key.as_any(),
                pyo3::types::PyInt::new(py, i64::MAX).as_any(),
            ],
        )
        .unwrap();
        let second = PyTuple::new(
            py,
            [
                later_key.as_any(),
                pyo3::types::PyInt::new(py, 5_i64).as_any(),
            ],
        )
        .unwrap();
        let third = PyTuple::new(
            py,
            [
                equal_key.as_any(),
                pyo3::types::PyInt::new(py, 1_i64).as_any(),
            ],
        )
        .unwrap();
        let key_name = PyString::new(py, "group");
        let count_name = PyString::new(py, "rows");
        let sum_name = PyString::new(py, "total");
        let value_index = pyo3::types::PyInt::new(py, 1_i64);
        let none = py.None();

        for source in [
            PyList::new(py, [&first, &second, &third])
                .unwrap()
                .into_any(),
            PyTuple::new(py, [&first, &second, &third])
                .unwrap()
                .into_any(),
        ] {
            let (is_final_rows, payload) = group_fixed_i64_rows_v1(
                source.as_any(),
                0,
                none.bind(py),
                key_name.as_any(),
                count_name.as_any(),
                none.bind(py),
            )
            .unwrap()
            .unwrap();
            assert!(!is_final_rows);
            let pairs = payload.bind(py).cast_exact::<PyList>().unwrap();
            assert_eq!(pairs.len(), 2);
            let first_pair = pairs.get_item(0).unwrap().cast_into::<PyTuple>().unwrap();
            assert!(first_pair.get_item(0).unwrap().is(&first_key));
            assert_eq!(
                first_pair.get_item(1).unwrap().extract::<usize>().unwrap(),
                2
            );
            let second_pair = pairs.get_item(1).unwrap().cast_into::<PyTuple>().unwrap();
            assert!(second_pair.get_item(0).unwrap().is(&later_key));
            assert_eq!(
                second_pair.get_item(1).unwrap().extract::<usize>().unwrap(),
                1
            );

            let (is_final_rows, payload) = group_fixed_i64_rows_v1(
                source.as_any(),
                0,
                value_index.as_any(),
                key_name.as_any(),
                count_name.as_any(),
                sum_name.as_any(),
            )
            .unwrap()
            .unwrap();
            assert!(!is_final_rows);
            let triples = payload.bind(py).cast_exact::<PyList>().unwrap();
            assert_eq!(triples.len(), 2);
            let first_triple = triples.get_item(0).unwrap().cast_into::<PyTuple>().unwrap();
            assert!(first_triple.get_item(0).unwrap().is(&first_key));
            assert_eq!(
                first_triple
                    .get_item(1)
                    .unwrap()
                    .extract::<usize>()
                    .unwrap(),
                2
            );
            assert_eq!(
                first_triple.get_item(2).unwrap().extract::<i128>().unwrap(),
                i128::from(i64::MAX) + 1
            );
            let second_triple = triples.get_item(1).unwrap().cast_into::<PyTuple>().unwrap();
            assert!(second_triple.get_item(0).unwrap().is(&later_key));
            assert_eq!(
                second_triple
                    .get_item(1)
                    .unwrap()
                    .extract::<usize>()
                    .unwrap(),
                1
            );
            assert_eq!(
                second_triple
                    .get_item(2)
                    .unwrap()
                    .extract::<i128>()
                    .unwrap(),
                5
            );
        }
    });
}

#[test]
fn fixed_dict_group_returns_count_and_sum_payloads_for_equal_field_names() {
    Python::initialize();
    Python::attach(|py| {
        let stored_key_field = PyString::new(py, "__fixed_group_key__");
        let stored_value_field = PyString::new(py, "__fixed_group_value__");
        let selected_key_field = PyString::new(py, "__fixed_group_key__");
        let selected_value_field = PyString::new(py, "__fixed_group_value__");
        assert!(!stored_key_field.is(&selected_key_field));
        assert!(!stored_value_field.is(&selected_value_field));
        let first_key = pyo3::types::PyInt::new(py, 2_000_i64);
        let equal_key = pyo3::types::PyInt::new(py, 2_000_i64);
        let later_key = pyo3::types::PyInt::new(py, -3_i64);
        let make_row = |key: &Bound<'_, PyAny>, value: i64| {
            let row = PyDict::new(py);
            row.set_item(&stored_key_field, key).unwrap();
            row.set_item(&stored_value_field, value).unwrap();
            row
        };
        let first = make_row(first_key.as_any(), i64::MAX);
        let second = make_row(later_key.as_any(), 7);
        let third = make_row(equal_key.as_any(), 1);
        let source = PyTuple::new(py, [&first, &second, &third]).unwrap();
        let key_name = PyString::new(py, "group");
        let count_name = PyString::new(py, "rows");
        let sum_name = PyString::new(py, "total");
        let none = py.None();

        let (is_final_rows, payload) = group_fixed_i64_dict_rows_v1(
            source.as_any(),
            selected_key_field.as_any(),
            none.bind(py),
            key_name.as_any(),
            count_name.as_any(),
            none.bind(py),
        )
        .unwrap()
        .unwrap();
        assert!(!is_final_rows);
        let pairs = payload.bind(py).cast_exact::<PyList>().unwrap();
        assert_eq!(pairs.len(), 2);
        let first_pair = pairs.get_item(0).unwrap().cast_into::<PyTuple>().unwrap();
        assert!(first_pair.get_item(0).unwrap().is(&first_key));
        assert_eq!(
            first_pair.get_item(1).unwrap().extract::<usize>().unwrap(),
            2
        );

        let (is_final_rows, payload) = group_fixed_i64_dict_rows_v1(
            source.as_any(),
            selected_key_field.as_any(),
            selected_value_field.as_any(),
            key_name.as_any(),
            count_name.as_any(),
            sum_name.as_any(),
        )
        .unwrap()
        .unwrap();
        assert!(!is_final_rows);
        let triples = payload.bind(py).cast_exact::<PyList>().unwrap();
        assert_eq!(triples.len(), 2);
        let first_triple = triples.get_item(0).unwrap().cast_into::<PyTuple>().unwrap();
        assert!(first_triple.get_item(0).unwrap().is(&first_key));
        assert_eq!(
            first_triple
                .get_item(1)
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            2
        );
        assert_eq!(
            first_triple.get_item(2).unwrap().extract::<i128>().unwrap(),
            i128::from(i64::MAX) + 1
        );
        let second_triple = triples.get_item(1).unwrap().cast_into::<PyTuple>().unwrap();
        assert!(second_triple.get_item(0).unwrap().is(&later_key));
        assert_eq!(
            second_triple
                .get_item(1)
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            1
        );
        assert_eq!(
            second_triple
                .get_item(2)
                .unwrap()
                .extract::<i128>()
                .unwrap(),
            7
        );
    });
}

#[test]
fn fixed_group_count_only_ignores_values_while_count_sum_validates_every_value() {
    Python::initialize();
    Python::attach(|py| {
        let key = pyo3::types::PyInt::new(py, 1_000_i64);
        let valid_tuple = PyTuple::new(
            py,
            [key.as_any(), pyo3::types::PyInt::new(py, 2_i64).as_any()],
        )
        .unwrap();
        let invalid_tuple = PyTuple::new(
            py,
            [key.as_any(), pyo3::types::PyBool::new(py, true).as_any()],
        )
        .unwrap();
        let tuple_source = PyList::new(py, [&valid_tuple, &invalid_tuple]).unwrap();
        let key_name = PyString::new(py, "group");
        let count_name = PyString::new(py, "rows");
        let sum_name = PyString::new(py, "total");
        let value_index = pyo3::types::PyInt::new(py, 1_i64);
        let none = py.None();

        let (_, payload) = group_fixed_i64_rows_v1(
            tuple_source.as_any(),
            0,
            none.bind(py),
            key_name.as_any(),
            count_name.as_any(),
            none.bind(py),
        )
        .unwrap()
        .unwrap();
        let pair = payload
            .bind(py)
            .cast_exact::<PyList>()
            .unwrap()
            .get_item(0)
            .unwrap()
            .cast_into::<PyTuple>()
            .unwrap();
        assert_eq!(pair.get_item(1).unwrap().extract::<usize>().unwrap(), 2);
        assert!(
            group_fixed_i64_rows_v1(
                tuple_source.as_any(),
                0,
                value_index.as_any(),
                key_name.as_any(),
                count_name.as_any(),
                sum_name.as_any(),
            )
            .unwrap()
            .is_none()
        );

        let valid_dict = PyDict::new(py);
        valid_dict.set_item("group", &key).unwrap();
        valid_dict.set_item("value", 2_i64).unwrap();
        let invalid_dict = PyDict::new(py);
        invalid_dict.set_item("group", &key).unwrap();
        invalid_dict.set_item("value", true).unwrap();
        let dict_source = PyList::new(py, [&valid_dict, &invalid_dict]).unwrap();
        let key_field = PyString::new(py, "group");
        let value_field = PyString::new(py, "value");
        let (_, payload) = group_fixed_i64_dict_rows_v1(
            dict_source.as_any(),
            key_field.as_any(),
            none.bind(py),
            key_name.as_any(),
            count_name.as_any(),
            none.bind(py),
        )
        .unwrap()
        .unwrap();
        let pair = payload
            .bind(py)
            .cast_exact::<PyList>()
            .unwrap()
            .get_item(0)
            .unwrap()
            .cast_into::<PyTuple>()
            .unwrap();
        assert_eq!(pair.get_item(1).unwrap().extract::<usize>().unwrap(), 2);
        assert!(
            group_fixed_i64_dict_rows_v1(
                dict_source.as_any(),
                key_field.as_any(),
                value_field.as_any(),
                key_name.as_any(),
                count_name.as_any(),
                sum_name.as_any(),
            )
            .unwrap()
            .is_none()
        );
    });
}

#[test]
fn fixed_group_rejects_mismatched_modes_and_nonexact_names_before_scanning() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"class Index(int):\n    calls = 0\n    def __index__(self):\n        type(self).calls += 1\n        raise AssertionError('index protocol called')\nclass Name(str):\n    pass\nclass Field:\n    calls = 0\n    def __hash__(self):\n        type(self).calls += 1\n        return 0\n    def __eq__(self, other):\n        type(self).calls += 1\n        return False\nfield = Field()\nrow = {'group': 1, 'value': 2, field: 3}\nField.calls = 0\nindex = Index(1)\nname = Name('derived')\n",
            c"fixed_group_preflight.py",
            c"fixed_group_preflight",
        )
        .unwrap();
        let tuple_source = PyList::new(py, [(1_i64, 2_i64)]).unwrap();
        let dict_source = PyList::new(py, [fixture.getattr("row").unwrap()]).unwrap();
        let key_name = PyString::new(py, "group");
        let count_name = PyString::new(py, "rows");
        let sum_name = PyString::new(py, "total");
        let key_field = PyString::new(py, "group");
        let value_field = PyString::new(py, "value");
        let value_index = pyo3::types::PyInt::new(py, 1_i64);
        let none = py.None();

        assert!(
            group_fixed_i64_rows_v1(
                tuple_source.as_any(),
                0,
                value_index.as_any(),
                key_name.as_any(),
                count_name.as_any(),
                none.bind(py),
            )
            .unwrap()
            .is_none()
        );
        assert!(
            group_fixed_i64_rows_v1(
                tuple_source.as_any(),
                0,
                none.bind(py),
                key_name.as_any(),
                count_name.as_any(),
                sum_name.as_any(),
            )
            .unwrap()
            .is_none()
        );
        assert!(
            group_fixed_i64_rows_v1(
                tuple_source.as_any(),
                0,
                &fixture.getattr("index").unwrap(),
                key_name.as_any(),
                count_name.as_any(),
                sum_name.as_any(),
            )
            .unwrap()
            .is_none()
        );
        assert!(
            group_fixed_i64_dict_rows_v1(
                dict_source.as_any(),
                key_field.as_any(),
                value_field.as_any(),
                key_name.as_any(),
                count_name.as_any(),
                sum_name.as_any(),
            )
            .unwrap()
            .is_none()
        );
        assert!(
            group_fixed_i64_dict_rows_v1(
                dict_source.as_any(),
                key_field.as_any(),
                none.bind(py),
                key_name.as_any(),
                &fixture.getattr("name").unwrap(),
                none.bind(py),
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(
            fixture
                .getattr("Index")
                .unwrap()
                .getattr("calls")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            0
        );
        assert_eq!(
            fixture
                .getattr("Field")
                .unwrap()
                .getattr("calls")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            0
        );
    });
}

#[test]
fn fixed_group_keeps_4095_groups_in_compact_payloads() {
    Python::initialize();
    Python::attach(|py| {
        let tuple_source = PyList::empty(py);
        let dict_source = PyList::empty(py);
        for key in 0..4_095_i64 {
            tuple_source.append((key, 1_i64)).unwrap();
            let row = PyDict::new(py);
            row.set_item("group", key).unwrap();
            row.set_item("value", 1_i64).unwrap();
            dict_source.append(row).unwrap();
        }
        let key_name = PyString::new(py, "alias");
        let count_name = PyString::new(py, "rows");
        let sum_name = PyString::new(py, "total");
        let key_field = PyString::new(py, "group");
        let value_field = PyString::new(py, "value");
        let value_index = pyo3::types::PyInt::new(py, 1_i64);
        let none = py.None();

        for (width, (value_selector, sum_output)) in [
            (2, (none.bind(py), none.bind(py))),
            (3, (value_index.as_any(), sum_name.as_any())),
        ] {
            let (is_final_rows, payload) = group_fixed_i64_rows_v1(
                tuple_source.as_any(),
                0,
                value_selector,
                key_name.as_any(),
                count_name.as_any(),
                sum_output,
            )
            .unwrap()
            .unwrap();
            assert!(!is_final_rows);
            let entries = payload.bind(py).cast_exact::<PyList>().unwrap();
            assert_eq!(entries.len(), 4_095);
            assert_eq!(
                entries
                    .get_item(0)
                    .unwrap()
                    .cast_into::<PyTuple>()
                    .unwrap()
                    .len(),
                width
            );
        }

        for (width, (value_selector, sum_output)) in [
            (2, (none.bind(py), none.bind(py))),
            (3, (value_field.as_any(), sum_name.as_any())),
        ] {
            let (is_final_rows, payload) = group_fixed_i64_dict_rows_v1(
                dict_source.as_any(),
                key_field.as_any(),
                value_selector,
                key_name.as_any(),
                count_name.as_any(),
                sum_output,
            )
            .unwrap()
            .unwrap();
            assert!(!is_final_rows);
            let entries = payload.bind(py).cast_exact::<PyList>().unwrap();
            assert_eq!(entries.len(), 4_095);
            assert_eq!(
                entries
                    .get_item(0)
                    .unwrap()
                    .cast_into::<PyTuple>()
                    .unwrap()
                    .len(),
                width
            );
        }
    });
}

#[test]
fn fixed_group_materializes_count_and_count_sum_rows_at_4096_groups() {
    Python::initialize();
    Python::attach(|py| {
        let first_key = pyo3::types::PyInt::new(py, 1_000_000_i64);
        let equal_key = pyo3::types::PyInt::new(py, 1_000_000_i64);
        let tuple_source = PyList::empty(py);
        tuple_source.append((&first_key, 1_i64)).unwrap();
        for key in 0..4_095_i64 {
            tuple_source.append((key, 1_i64)).unwrap();
        }
        tuple_source.append((&equal_key, 1_i64)).unwrap();
        let key_name = PyString::new(py, "alias");
        let count_name = PyString::new(py, "rows");
        let sum_name = PyString::new(py, "total");
        let value_index = pyo3::types::PyInt::new(py, 1_i64);
        let none = py.None();

        let (is_final_rows, payload) = group_fixed_i64_rows_v1(
            tuple_source.as_any(),
            0,
            none.bind(py),
            key_name.as_any(),
            count_name.as_any(),
            none.bind(py),
        )
        .unwrap()
        .unwrap();
        assert!(is_final_rows);
        let rows = payload.bind(py).cast_exact::<PyList>().unwrap();
        assert_eq!(rows.len(), 4_096);
        let first = rows.get_item(0).unwrap().cast_into::<PyDict>().unwrap();
        assert_eq!(
            first.keys().extract::<Vec<String>>().unwrap(),
            vec!["alias", "rows"]
        );
        assert!(first.get_item("alias").unwrap().unwrap().is(&first_key));
        assert_eq!(
            first
                .get_item("rows")
                .unwrap()
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            2
        );

        let (is_final_rows, payload) = group_fixed_i64_rows_v1(
            tuple_source.as_any(),
            0,
            value_index.as_any(),
            key_name.as_any(),
            count_name.as_any(),
            sum_name.as_any(),
        )
        .unwrap()
        .unwrap();
        assert!(is_final_rows);
        let rows = payload.bind(py).cast_exact::<PyList>().unwrap();
        assert_eq!(rows.len(), 4_096);
        let first = rows.get_item(0).unwrap().cast_into::<PyDict>().unwrap();
        assert_eq!(
            first.keys().extract::<Vec<String>>().unwrap(),
            vec!["alias", "rows", "total"]
        );
        assert_eq!(
            first
                .get_item("total")
                .unwrap()
                .unwrap()
                .extract::<i128>()
                .unwrap(),
            2
        );

        let dict_source = PyList::empty(py);
        let append_dict = |key: &Bound<'_, PyAny>, value: i64| {
            let row = PyDict::new(py);
            row.set_item("group", key).unwrap();
            row.set_item("value", value).unwrap();
            dict_source.append(row).unwrap();
        };
        append_dict(first_key.as_any(), i64::MAX);
        for key in 0..4_095_i64 {
            let key = pyo3::types::PyInt::new(py, key);
            append_dict(key.as_any(), 1);
        }
        append_dict(equal_key.as_any(), 1);
        let key_field = PyString::new(py, "group");
        let value_field = PyString::new(py, "value");

        let (is_final_rows, payload) = group_fixed_i64_dict_rows_v1(
            dict_source.as_any(),
            key_field.as_any(),
            none.bind(py),
            key_name.as_any(),
            count_name.as_any(),
            none.bind(py),
        )
        .unwrap()
        .unwrap();
        assert!(is_final_rows);
        let rows = payload.bind(py).cast_exact::<PyList>().unwrap();
        assert_eq!(rows.len(), 4_096);
        let first = rows.get_item(0).unwrap().cast_into::<PyDict>().unwrap();
        assert_eq!(
            first.keys().extract::<Vec<String>>().unwrap(),
            vec!["alias", "rows"]
        );

        let (is_final_rows, payload) = group_fixed_i64_dict_rows_v1(
            dict_source.as_any(),
            key_field.as_any(),
            value_field.as_any(),
            key_name.as_any(),
            count_name.as_any(),
            sum_name.as_any(),
        )
        .unwrap()
        .unwrap();
        assert!(is_final_rows);
        let rows = payload.bind(py).cast_exact::<PyList>().unwrap();
        assert_eq!(rows.len(), 4_096);
        let first = rows.get_item(0).unwrap().cast_into::<PyDict>().unwrap();
        assert_eq!(
            first.keys().extract::<Vec<String>>().unwrap(),
            vec!["alias", "rows", "total"]
        );
        assert!(first.get_item("alias").unwrap().unwrap().is(&first_key));
        assert_eq!(
            first
                .get_item("rows")
                .unwrap()
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            2
        );
        assert_eq!(
            first
                .get_item("total")
                .unwrap()
                .unwrap()
                .extract::<i128>()
                .unwrap(),
            i128::from(i64::MAX) + 1
        );
    });
}
