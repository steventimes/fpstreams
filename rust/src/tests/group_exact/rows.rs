//! Exact tuple- and dictionary-row grouping coverage.

use super::*;

#[test]
fn tuple_row_group_sum_v1_returns_identity_safe_pairs_below_the_final_rows_threshold() {
    Python::initialize();
    Python::attach(|py| {
        let first_key = pyo3::types::PyInt::new(py, 1_000_000_i64);
        let equal_key = pyo3::types::PyInt::new(py, 1_000_000_i64);
        assert!(!first_key.is(&equal_key));
        let first = PyTuple::new(
            py,
            [first_key.as_any(), pyo3::types::PyInt::new(py, 2).as_any()],
        )
        .unwrap();
        let second = PyTuple::new(
            py,
            [equal_key.as_any(), pyo3::types::PyInt::new(py, 3).as_any()],
        )
        .unwrap();
        let source = PyList::empty(py);
        source.append(&first).unwrap();
        for key in 0..4_094_i64 {
            source.append((key, 1_i64)).unwrap();
        }
        source.append(&second).unwrap();
        let key_name = PyString::new(py, "key");
        let output_name = PyString::new(py, "total");

        let (is_final_rows, payload) = group_sum_i64_rows_v1(
            source.as_any(),
            0,
            1,
            key_name.as_any(),
            output_name.as_any(),
        )
        .unwrap()
        .unwrap();

        assert!(!is_final_rows);
        let pairs = payload.bind(py).cast_exact::<PyList>().unwrap();
        assert_eq!(pairs.len(), 4_095);
        let pair = pairs.get_item(0).unwrap().cast_into::<PyTuple>().unwrap();
        assert!(pair.get_item(0).unwrap().is(&first_key));
        assert_eq!(pair.get_item(1).unwrap().extract::<i128>().unwrap(), 5);
    });
}

#[test]
fn tuple_row_group_sum_v1_returns_final_rows_at_4096_groups_with_identity_and_order() {
    Python::initialize();
    Python::attach(|py| {
        let first_key = pyo3::types::PyInt::new(py, 1_000_000_i64);
        let equal_key = pyo3::types::PyInt::new(py, 1_000_000_i64);
        assert!(!first_key.is(&equal_key));
        let source = PyList::empty(py);
        source
            .append(
                PyTuple::new(
                    py,
                    [first_key.as_any(), pyo3::types::PyInt::new(py, 2).as_any()],
                )
                .unwrap(),
            )
            .unwrap();
        for key in 0..4_095_i64 {
            source.append((key, 1_i64)).unwrap();
        }
        source
            .append(
                PyTuple::new(
                    py,
                    [equal_key.as_any(), pyo3::types::PyInt::new(py, 3).as_any()],
                )
                .unwrap(),
            )
            .unwrap();
        let key_name = PyString::new(py, "key");
        let output_name = PyString::new(py, "total");

        let (is_final_rows, payload) = group_sum_i64_rows_v1(
            source.as_any(),
            0,
            1,
            key_name.as_any(),
            output_name.as_any(),
        )
        .unwrap()
        .unwrap();

        assert!(is_final_rows);
        let rows = payload.bind(py).cast_exact::<PyList>().unwrap();
        assert_eq!(rows.len(), 4_096);
        let first = rows.get_item(0).unwrap().cast_into::<PyDict>().unwrap();
        assert!(first.get_item("key").unwrap().unwrap().is(&first_key));
        assert_eq!(
            first
                .get_item("total")
                .unwrap()
                .unwrap()
                .extract::<i128>()
                .unwrap(),
            5
        );
        assert_eq!(
            rows.get_item(1)
                .unwrap()
                .cast_into::<PyDict>()
                .unwrap()
                .get_item("key")
                .unwrap()
                .unwrap()
                .extract::<i64>()
                .unwrap(),
            0
        );
        assert_eq!(
            rows.get_item(4_095)
                .unwrap()
                .cast_into::<PyDict>()
                .unwrap()
                .get_item("key")
                .unwrap()
                .unwrap()
                .extract::<i64>()
                .unwrap(),
            4_094
        );
    });
}

#[test]
fn tuple_row_group_sum_v1_rejects_nonexact_field_names() {
    Python::initialize();
    Python::attach(|py| {
        let source = PyList::new(py, [(1_i64, 2_i64)]).unwrap();
        let key_name = PyString::new(py, "key");
        let output_name = PyString::new(py, "total");
        let field_subclass = py
            .eval(c"type('Field', (str,), {})('total')", None, None)
            .unwrap();

        assert!(
            group_sum_i64_rows_v1(source.as_any(), 0, 1, &field_subclass, output_name.as_any(),)
                .unwrap()
                .is_none()
        );
        assert!(
            group_sum_i64_rows_v1(source.as_any(), 0, 1, key_name.as_any(), &field_subclass)
                .unwrap()
                .is_none()
        );
    });
}

#[test]
fn dict_row_group_sum_v1_returns_identity_safe_pairs_below_the_final_rows_threshold() {
    Python::initialize();
    Python::attach(|py| {
        let first_key = pyo3::types::PyInt::new(py, 1_000_000_i64);
        let equal_key = pyo3::types::PyInt::new(py, 1_000_000_i64);
        assert!(!first_key.is(&equal_key));
        let first = PyDict::new(py);
        first.set_item("group", &first_key).unwrap();
        first.set_item("amount", 2_i64).unwrap();
        let second = PyDict::new(py);
        second.set_item("group", &equal_key).unwrap();
        second.set_item("amount", 3_i64).unwrap();
        let source = PyList::empty(py);
        source.append(&first).unwrap();
        for key in 0..4_094_i64 {
            let row = PyDict::new(py);
            row.set_item("group", key).unwrap();
            row.set_item("amount", 1_i64).unwrap();
            source.append(row).unwrap();
        }
        source.append(&second).unwrap();
        let key_field = PyString::new(py, "group");
        let value_field = PyString::new(py, "amount");
        let key_name = PyString::new(py, "alias");
        let output_name = PyString::new(py, "total");

        let (is_final_rows, payload) = group_sum_i64_dict_rows_v1(
            source.as_any(),
            key_field.as_any(),
            value_field.as_any(),
            key_name.as_any(),
            output_name.as_any(),
        )
        .unwrap()
        .unwrap();

        assert!(!is_final_rows);
        let pairs = payload.bind(py).cast_exact::<PyList>().unwrap();
        assert_eq!(pairs.len(), 4_095);
        let pair = pairs.get_item(0).unwrap().cast_into::<PyTuple>().unwrap();
        assert!(pair.get_item(0).unwrap().is(&first_key));
        assert_eq!(pair.get_item(1).unwrap().extract::<i128>().unwrap(), 5);
    });
}

#[test]
fn dict_row_group_sum_v1_returns_aliased_final_rows_at_4096_groups() {
    Python::initialize();
    Python::attach(|py| {
        let first_key = pyo3::types::PyInt::new(py, 1_000_000_i64);
        let equal_key = pyo3::types::PyInt::new(py, 1_000_000_i64);
        assert!(!first_key.is(&equal_key));
        let source = PyList::empty(py);
        let first = PyDict::new(py);
        first.set_item("group", &first_key).unwrap();
        first.set_item("amount", i64::MAX).unwrap();
        source.append(&first).unwrap();
        for key in 0..4_095_i64 {
            let row = PyDict::new(py);
            row.set_item("group", key).unwrap();
            row.set_item("amount", 1_i64).unwrap();
            source.append(row).unwrap();
        }
        let duplicate = PyDict::new(py);
        duplicate.set_item("group", &equal_key).unwrap();
        duplicate.set_item("amount", i64::MAX).unwrap();
        source.append(duplicate).unwrap();
        let key_field = PyString::new(py, "group");
        let value_field = PyString::new(py, "amount");
        let key_name = PyString::new(py, "alias");
        let output_name = PyString::new(py, "sum");

        let (is_final_rows, payload) = group_sum_i64_dict_rows_v1(
            source.as_any(),
            key_field.as_any(),
            value_field.as_any(),
            key_name.as_any(),
            output_name.as_any(),
        )
        .unwrap()
        .unwrap();

        assert!(is_final_rows);
        let rows = payload.bind(py).cast_exact::<PyList>().unwrap();
        assert_eq!(rows.len(), 4_096);
        let first = rows.get_item(0).unwrap().cast_into::<PyDict>().unwrap();
        assert_eq!(
            first.keys().extract::<Vec<String>>().unwrap(),
            vec!["alias", "sum"]
        );
        assert!(first.get_item("alias").unwrap().unwrap().is(&first_key));
        assert_eq!(
            first
                .get_item("sum")
                .unwrap()
                .unwrap()
                .extract::<i128>()
                .unwrap(),
            18_446_744_073_709_551_614_i128
        );
        assert_eq!(
            rows.get_item(1)
                .unwrap()
                .cast_into::<PyDict>()
                .unwrap()
                .get_item("alias")
                .unwrap()
                .unwrap()
                .extract::<i64>()
                .unwrap(),
            0
        );
        assert_eq!(
            rows.get_item(4_095)
                .unwrap()
                .cast_into::<PyDict>()
                .unwrap()
                .get_item("alias")
                .unwrap()
                .unwrap()
                .extract::<i64>()
                .unwrap(),
            4_094
        );
    });
}

#[test]
fn dict_row_group_sum_v1_rejects_nonexact_output_field_names() {
    Python::initialize();
    Python::attach(|py| {
        let row = PyDict::new(py);
        row.set_item("group", 1_i64).unwrap();
        row.set_item("amount", 2_i64).unwrap();
        let source = PyList::new(py, [&row]).unwrap();
        let key_field = PyString::new(py, "group");
        let value_field = PyString::new(py, "amount");
        let key_name = PyString::new(py, "alias");
        let output_name = PyString::new(py, "total");
        let field_subclass = py
            .eval(c"type('Field', (str,), {})('derived')", None, None)
            .unwrap();

        assert!(
            group_sum_i64_dict_rows_v1(
                source.as_any(),
                key_field.as_any(),
                value_field.as_any(),
                &field_subclass,
                output_name.as_any(),
            )
            .unwrap()
            .is_none()
        );
        assert!(
            group_sum_i64_dict_rows_v1(
                source.as_any(),
                key_field.as_any(),
                value_field.as_any(),
                key_name.as_any(),
                &field_subclass,
            )
            .unwrap()
            .is_none()
        );
    });
}

#[test]
fn exact_dict_group_sum_preserves_first_key_identity_and_order() {
    Python::initialize();
    Python::attach(|py| {
        let first_key = pyo3::types::PyInt::new(py, 1_000_i64);
        let first = PyDict::new(py);
        first.set_item("key", &first_key).unwrap();
        first.set_item("value", 5_i64).unwrap();
        let second = PyDict::new(py);
        second.set_item("key", 1_i64).unwrap();
        second.set_item("value", 7_i64).unwrap();
        let third = PyDict::new(py);
        third.set_item("key", 1_000_i64).unwrap();
        third.set_item("value", 11_i64).unwrap();
        let source = PyList::new(py, [&first, &second, &third]).unwrap();
        let key_field = PyString::new(py, "key");
        let value_field = PyString::new(py, "value");

        let groups =
            group_sum_i64_dict_rows(source.as_any(), key_field.as_any(), value_field.as_any())
                .unwrap()
                .unwrap();

        assert_eq!(groups.len(), 2);
        assert!(groups[0].0.bind(py).is(&first_key));
        assert_eq!(groups[0].1, 16);
        assert_eq!(groups[1].0.bind(py).extract::<i64>().unwrap(), 1);
        assert_eq!(groups[1].1, 7);

        let tuple_source = PyTuple::new(py, [&first, &second, &third]).unwrap();
        let tuple_groups = group_sum_i64_dict_rows(
            tuple_source.as_any(),
            key_field.as_any(),
            value_field.as_any(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(tuple_groups.len(), 2);
        assert!(tuple_groups[0].0.bind(py).is(&first_key));
        assert_eq!(tuple_groups[0].1, 16);
    });
}

#[test]
fn exact_dict_group_sum_handles_same_and_equal_nonidentical_field_names() {
    Python::initialize();
    Python::attach(|py| {
        let amount = PyString::new(py, "__group_amount_field__");
        let first = PyDict::new(py);
        first.set_item(&amount, 2_i64).unwrap();
        let second = PyDict::new(py);
        second.set_item(&amount, 3_i64).unwrap();
        let same_source = PyList::new(py, [&first, &second]).unwrap();

        let same_groups =
            group_sum_i64_dict_rows(same_source.as_any(), amount.as_any(), amount.as_any())
                .unwrap()
                .unwrap();
        assert_eq!(
            same_groups
                .iter()
                .map(|(key, total)| (key.bind(py).extract::<i64>().unwrap(), *total))
                .collect::<Vec<_>>(),
            vec![(2, 2), (3, 3)]
        );

        let stored_key = PyString::new(py, "__noninterned_group_key_field__");
        let stored_value = PyString::new(py, "__noninterned_group_value_field__");
        let selected_key = PyString::new(py, "__noninterned_group_key_field__");
        let selected_value = PyString::new(py, "__noninterned_group_value_field__");
        assert!(!stored_key.is(&selected_key));
        assert!(!stored_value.is(&selected_value));
        let row = PyDict::new(py);
        row.set_item(&stored_key, 7_i64).unwrap();
        row.set_item(&stored_value, 11_i64).unwrap();
        let source = PyList::new(py, [&row]).unwrap();

        let groups = group_sum_i64_dict_rows(
            source.as_any(),
            selected_key.as_any(),
            selected_value.as_any(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].0.bind(py).extract::<i64>().unwrap(), 7);
        assert_eq!(groups[0].1, 11);
    });
}

#[test]
fn exact_dict_group_sum_identity_cache_threshold_switches_to_numeric_grouping() {
    Python::initialize();
    Python::attach(|py| {
        let keys = (0..256)
            .map(|index| {
                pyo3::types::PyInt::new(py, 1_000_i64 + index)
                    .into_any()
                    .unbind()
            })
            .collect::<Vec<_>>();
        let mut rows = Vec::with_capacity(keys.len() * 2);
        for value in [1_i64, 2_i64] {
            for key in &keys {
                let row = PyDict::new(py);
                row.set_item("key", key.bind(py)).unwrap();
                row.set_item("value", value).unwrap();
                rows.push(row);
            }
        }
        let source = PyList::new(py, &rows).unwrap();
        let key_field = PyString::new(py, "key");
        let value_field = PyString::new(py, "value");

        let groups =
            group_sum_i64_dict_rows(source.as_any(), key_field.as_any(), value_field.as_any())
                .unwrap()
                .unwrap();

        assert_eq!(groups.len(), keys.len());
        for (index, (key, total)) in groups.iter().enumerate() {
            assert!(key.bind(py).is(keys[index].bind(py)));
            assert_eq!(*total, 3);
        }
    });
}

#[test]
fn exact_dict_group_sum_identity_cache_collisions_remain_numeric_misses() {
    Python::initialize();
    Python::attach(|py| {
        let mut slots = (0..128)
            .map(|_| None)
            .collect::<Vec<Option<(Py<PyAny>, i64)>>>();
        let (first_key, first_value, second_key, second_value) = (1_000_i64..)
            .find_map(|value| {
                let key = pyo3::types::PyInt::new(py, value).into_any().unbind();
                let slot = ((key.bind(py).as_ptr() as usize) >> 4) & 127;
                if let Some((previous, previous_value)) = slots[slot].take() {
                    Some((previous, previous_value, key, value))
                } else {
                    slots[slot] = Some((key, value));
                    None
                }
            })
            .expect("129 live objects must collide in a 128-slot direct-mapped cache");
        let make_row = |key: &Py<PyAny>, value: i64| {
            let row = PyDict::new(py);
            row.set_item("key", key.bind(py)).unwrap();
            row.set_item("value", value).unwrap();
            row
        };
        let rows = [
            make_row(&first_key, 1),
            make_row(&second_key, 2),
            make_row(&first_key, 4),
            make_row(&second_key, 8),
        ];
        let source = PyList::new(py, &rows).unwrap();
        let key_field = PyString::new(py, "key");
        let value_field = PyString::new(py, "value");

        let groups =
            group_sum_i64_dict_rows(source.as_any(), key_field.as_any(), value_field.as_any())
                .unwrap()
                .unwrap();

        assert_eq!(groups.len(), 2);
        assert!(groups[0].0.bind(py).is(first_key.bind(py)));
        assert_eq!(groups[0].0.bind(py).extract::<i64>().unwrap(), first_value);
        assert_eq!(groups[0].1, 5);
        assert!(groups[1].0.bind(py).is(second_key.bind(py)));
        assert_eq!(groups[1].0.bind(py).extract::<i64>().unwrap(), second_value);
        assert_eq!(groups[1].1, 10);
    });
}

#[test]
fn exact_dict_group_sum_identity_cache_hit_still_validates_each_value() {
    Python::initialize();
    Python::attach(|py| {
        let key = pyo3::types::PyInt::new(py, 1_000_i64);
        let first = PyDict::new(py);
        first.set_item("key", &key).unwrap();
        first.set_item("value", 2_i64).unwrap();
        let second = PyDict::new(py);
        second.set_item("key", &key).unwrap();
        second.set_item("value", 3_i64).unwrap();
        let source = PyList::new(py, [&first, &second]).unwrap();
        let key_field = PyString::new(py, "key");
        let value_field = PyString::new(py, "value");

        let groups =
            group_sum_i64_dict_rows(source.as_any(), key_field.as_any(), value_field.as_any())
                .unwrap()
                .unwrap();
        assert_eq!(groups.len(), 1);
        assert!(groups[0].0.bind(py).is(&key));
        assert_eq!(groups[0].1, 5);

        second.set_item("value", true).unwrap();
        assert!(
            group_sum_i64_dict_rows(source.as_any(), key_field.as_any(), value_field.as_any())
                .unwrap()
                .is_none()
        );
    });
}

#[test]
fn exact_dict_group_sum_migrates_compact_slots_to_sparse_extreme_keys() {
    Python::initialize();
    Python::attach(|py| {
        let rows = [(0_i64, 1_i64), (3, 2), (i64::MAX, 4), (-7, 5), (0, 6)]
            .into_iter()
            .map(|(key, value)| {
                let row = PyDict::new(py);
                row.set_item("key", key).unwrap();
                row.set_item("value", value).unwrap();
                row
            })
            .collect::<Vec<_>>();
        let source = PyList::new(py, &rows).unwrap();
        let key_field = PyString::new(py, "key");
        let value_field = PyString::new(py, "value");

        let groups =
            group_sum_i64_dict_rows(source.as_any(), key_field.as_any(), value_field.as_any())
                .unwrap()
                .unwrap();
        let values = groups
            .iter()
            .map(|(key, total)| (key.bind(py).extract::<i64>().unwrap(), *total))
            .collect::<Vec<_>>();

        assert_eq!(values, vec![(0, 7), (3, 2), (i64::MAX, 4), (-7, 5)]);
    });
}

#[test]
fn exact_dict_group_sum_rejects_wide_rows_at_every_position() {
    Python::initialize();
    Python::attach(|py| {
        let make_row = |extra_fields: usize| {
            let row = PyDict::new(py);
            row.set_item("key", 1_i64).unwrap();
            row.set_item("value", 2_i64).unwrap();
            for index in 0..extra_fields {
                row.set_item(format!("field_{index}"), index).unwrap();
            }
            row
        };
        let boundary = make_row(22);
        let wide = make_row(23);
        let key_field = PyString::new(py, "key");
        let value_field = PyString::new(py, "value");

        let boundary_source = PyList::new(py, [&boundary]).unwrap();
        assert!(
            group_sum_i64_dict_rows(
                boundary_source.as_any(),
                key_field.as_any(),
                value_field.as_any(),
            )
            .unwrap()
            .is_some()
        );

        let wide_first = PyList::new(py, [&wide]).unwrap();
        assert!(
            group_sum_i64_dict_rows(
                wide_first.as_any(),
                key_field.as_any(),
                value_field.as_any(),
            )
            .unwrap()
            .is_none()
        );

        let late_wide = PyList::new(py, [&boundary, &wide]).unwrap();
        assert!(
            group_sum_i64_dict_rows(late_wide.as_any(), key_field.as_any(), value_field.as_any(),)
                .unwrap()
                .is_none()
        );
    });
}

#[test]
fn exact_dict_group_sum_rejects_unsupported_values_and_shapes() {
    Python::initialize();
    Python::attach(|py| {
        let key_field = PyString::new(py, "key");
        let value_field = PyString::new(py, "value");

        let boolean = PyDict::new(py);
        boolean.set_item("key", true).unwrap();
        boolean.set_item("value", 1_i64).unwrap();
        let boolean_source = PyList::new(py, [&boolean]).unwrap();
        assert!(
            group_sum_i64_dict_rows(
                boolean_source.as_any(),
                key_field.as_any(),
                value_field.as_any(),
            )
            .unwrap()
            .is_none()
        );

        let huge = py.eval(c"1 << 100", None, None).unwrap();
        let oversized = PyDict::new(py);
        oversized.set_item("key", 1_i64).unwrap();
        oversized.set_item("value", huge).unwrap();
        let oversized_source = PyTuple::new(py, [&oversized]).unwrap();
        assert!(
            group_sum_i64_dict_rows(
                oversized_source.as_any(),
                key_field.as_any(),
                value_field.as_any(),
            )
            .unwrap()
            .is_none()
        );

        let non_record_source = PyList::new(py, [1_i64]).unwrap();
        assert!(
            group_sum_i64_dict_rows(
                non_record_source.as_any(),
                key_field.as_any(),
                value_field.as_any(),
            )
            .unwrap()
            .is_none()
        );

        let selector_subclass = py
            .eval(c"type('Field', (str,), {})('key')", None, None)
            .unwrap();
        assert!(
            group_sum_i64_dict_rows(
                boolean_source.as_any(),
                &selector_subclass,
                value_field.as_any(),
            )
            .unwrap()
            .is_none()
        );
    });
}

#[test]
fn exact_dict_group_sum_checks_all_keys_before_selector_lookup() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"class Trap:\n    calls = 0\n    def __hash__(self):\n        return hash('key')\n    def __eq__(self, other):\n        type(self).calls += 1\n        raise AssertionError('selector lookup touched custom equality')\ntrap = Trap()\nrow = {trap: 7}\nTrap.calls = 0\n",
            c"native_group_safety.py",
            c"native_group_safety",
        )
        .unwrap();
        let row = fixture.getattr("row").unwrap();
        let source = PyList::new(py, [&row]).unwrap();
        let key_field = PyString::new(py, "key");
        let value_field = PyString::new(py, "value");

        assert!(
            group_sum_i64_dict_rows(source.as_any(), key_field.as_any(), value_field.as_any(),)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            fixture
                .getattr("Trap")
                .unwrap()
                .getattr("calls")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            0
        );
    });
}
