//! Callable grouping behavior and hash-probe tests.

use super::*;

#[test]
fn callable_key_group_preserves_callback_order_first_key_and_python_addition() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"calls = []\nfallback_calls = []\nclass Total:\n    def __init__(self, text):\n        self.text = text\n    def __radd__(self, other):\n        assert other == 0\n        return Total(self.text)\n    def __add__(self, other):\n        return Total(self.text + other.text)\ndef select_key(row):\n    calls.append(row['tag'])\n    return row['key']\ndef fallback(row):\n    fallback_calls.append(row)\n    return row['value']\n",
            c"callable_key_group.py",
            c"callable_key_group",
        )
        .unwrap();
        let first_key = pyo3::types::PyInt::new(py, 1_000_i64);
        let equal_key = pyo3::types::PyInt::new(py, 1_000_i64);
        let later_key = pyo3::types::PyInt::new(py, -7_i64);
        assert!(!first_key.is(&equal_key));

        let total_type = fixture.getattr("Total").unwrap();
        let first = PyDict::new(py);
        first.set_item("key", &first_key).unwrap();
        first.set_item("tag", "first").unwrap();
        first
            .set_item("value", total_type.call1(("a",)).unwrap())
            .unwrap();
        let second = PyDict::new(py);
        second.set_item("key", &later_key).unwrap();
        second.set_item("tag", "second").unwrap();
        second
            .set_item("value", total_type.call1(("b",)).unwrap())
            .unwrap();
        let third = PyDict::new(py);
        third.set_item("key", &equal_key).unwrap();
        third.set_item("tag", "third").unwrap();
        third
            .set_item("value", total_type.call1(("c",)).unwrap())
            .unwrap();
        let source = PyList::new(py, [&first, &second, &third]).unwrap();

        let groups = group_count_sum_callable_key_dict_rows_v1(
            source.as_any(),
            &fixture.getattr("select_key").unwrap(),
            PyString::new(py, "value").as_any(),
            &fixture.getattr("fallback").unwrap(),
        )
        .unwrap()
        .unwrap();

        assert_eq!(groups.len(), 2);
        assert!(groups[0].0.bind(py).is(&first_key));
        assert_eq!(groups[0].1, 2);
        assert_eq!(
            groups[0]
                .2
                .bind(py)
                .getattr("text")
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "ac"
        );
        assert!(groups[1].0.bind(py).is(&later_key));
        assert_eq!(groups[1].1, 1);
        assert_eq!(
            groups[1]
                .2
                .bind(py)
                .getattr("text")
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "b"
        );
        assert_eq!(
            fixture
                .getattr("calls")
                .unwrap()
                .extract::<Vec<String>>()
                .unwrap(),
            ["first", "second", "third"]
        );
        assert!(
            fixture
                .getattr("fallback_calls")
                .unwrap()
                .extract::<Vec<Py<PyAny>>>()
                .unwrap()
                .is_empty()
        );
    });
}

#[test]
fn callable_value_group_keeps_live_list_mutation_in_native_loop() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"calls = []\naccessor_calls = []\nsource = None\nclass Record:\n    def __init__(self, key, value, tag):\n        self.key = key\n        self.value = value\n        self.tag = tag\n    def __getitem__(self, field):\n        return getattr(self, field)\ndef key_accessor(row):\n    accessor_calls.append(row.tag)\n    return row['key']\ndef select_value(row):\n    tag = row['tag'] if type(row) is dict else row.tag\n    calls.append(tag)\n    if tag == 'first':\n        source[1] = Record(2, 5, 'replacement')\n    return row['value']\n",
            c"callable_value_group.py",
            c"callable_value_group",
        )
        .unwrap();
        let first = PyDict::new(py);
        first.set_item("key", 1_i64).unwrap();
        first.set_item("value", 2_i64).unwrap();
        first.set_item("tag", "first").unwrap();
        let second = PyDict::new(py);
        second.set_item("key", 9_i64).unwrap();
        second.set_item("value", 99_i64).unwrap();
        second.set_item("tag", "stale").unwrap();
        let source = PyList::new(py, [&first, &second]).unwrap();
        fixture.setattr("source", &source).unwrap();

        let groups = group_count_sum_callable_value_dict_rows_v1(
            source.as_any(),
            PyString::new(py, "key").as_any(),
            &fixture.getattr("key_accessor").unwrap(),
            &fixture.getattr("select_value").unwrap(),
        )
        .unwrap()
        .unwrap();

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].0.bind(py).extract::<i64>().unwrap(), 1);
        assert_eq!(groups[0].1, 1);
        assert_eq!(groups[0].2.bind(py).extract::<i64>().unwrap(), 2);
        assert_eq!(groups[1].0.bind(py).extract::<i64>().unwrap(), 2);
        assert_eq!(groups[1].1, 1);
        assert_eq!(groups[1].2.bind(py).extract::<i64>().unwrap(), 5);
        assert_eq!(
            fixture
                .getattr("calls")
                .unwrap()
                .extract::<Vec<String>>()
                .unwrap(),
            ["first", "replacement"]
        );
        assert_eq!(
            fixture
                .getattr("accessor_calls")
                .unwrap()
                .extract::<Vec<String>>()
                .unwrap(),
            ["replacement"]
        );
    });
}

#[test]
fn callable_group_declines_non_exact_initial_rows_before_callbacks() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"calls = 0\ndef callback(row):\n    global calls\n    calls += 1\n    return 1\ndef accessor(row):\n    raise AssertionError('accessor must not run during preflight')\n",
            c"callable_group_preflight.py",
            c"callable_group_preflight",
        )
        .unwrap();
        let source = PyList::new(py, [py.None()]).unwrap();
        let result = group_count_sum_callable_key_dict_rows_v1(
            source.as_any(),
            &fixture.getattr("callback").unwrap(),
            PyString::new(py, "value").as_any(),
            &fixture.getattr("accessor").unwrap(),
        )
        .unwrap();

        assert!(result.is_none());
        assert_eq!(
            fixture
                .getattr("calls")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            0
        );
    });
}

#[test]
fn callable_group_translates_unhashable_callback_keys_once() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"calls = 0\ndef key(row):\n    global calls\n    calls += 1\n    return []\ndef accessor(row):\n    raise AssertionError('value access follows successful grouping')\n",
            c"callable_group_hash_error.py",
            c"callable_group_hash_error",
        )
        .unwrap();
        let row = PyDict::new(py);
        row.set_item("value", 1_i64).unwrap();
        let source = PyList::new(py, [&row]).unwrap();

        let error = group_count_sum_callable_key_dict_rows_v1(
            source.as_any(),
            &fixture.getattr("key").unwrap(),
            PyString::new(py, "value").as_any(),
            &fixture.getattr("accessor").unwrap(),
        )
        .unwrap_err();

        assert!(error.is_instance_of::<pyo3::exceptions::PyTypeError>(py));
        assert_eq!(
            error.value(py).str().unwrap().extract::<String>().unwrap(),
            "group_by keys must be hashable"
        );
        assert!(
            error
                .value(py)
                .getattr("__suppress_context__")
                .unwrap()
                .is_truthy()
                .unwrap()
        );
        assert!(error.cause(py).is_none());
        assert!(
            error
                .context(py)
                .unwrap()
                .is_instance_of::<pyo3::exceptions::PyTypeError>(py)
        );
        assert_eq!(
            fixture
                .getattr("calls")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            1
        );
    });
}

#[test]
fn callable_group_never_declines_after_callback_removes_direct_field() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"calls = 0\ndef key(row):\n    global calls\n    calls += 1\n    row.pop('value')\n    return row['key']\ndef accessor(row):\n    raise AssertionError('exact dict uses the native direct field')\n",
            c"callable_group_selection_error.py",
            c"callable_group_selection_error",
        )
        .unwrap();
        let row = PyDict::new(py);
        row.set_item("key", 1_i64).unwrap();
        row.set_item("value", 2_i64).unwrap();
        let source = PyList::new(py, [&row]).unwrap();

        let error = group_count_sum_callable_key_dict_rows_v1(
            source.as_any(),
            &fixture.getattr("key").unwrap(),
            PyString::new(py, "value").as_any(),
            &fixture.getattr("accessor").unwrap(),
        )
        .unwrap_err();
        let selection_error = PyModule::import(py, "fpstreams.errors")
            .unwrap()
            .getattr("SelectionError")
            .unwrap();

        assert!(error.matches(py, &selection_error).unwrap());
        assert!(
            error
                .cause(py)
                .unwrap()
                .is_instance_of::<pyo3::exceptions::PyKeyError>(py)
        );
        assert_eq!(
            fixture
                .getattr("calls")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            1
        );
    });
}

#[test]
fn callable_group_wraps_exact_dict_lookup_type_error_as_selection_error() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"primary = TypeError('lookup equality failed')\nclass Evil:\n    def __hash__(self):\n        return hash('value')\n    def __eq__(self, other):\n        raise primary\nevil = Evil()\ndef key(row):\n    return row['key']\ndef accessor(row):\n    raise AssertionError('exact dict uses the native direct field')\n",
            c"callable_group_lookup_type_error.py",
            c"callable_group_lookup_type_error",
        )
        .unwrap();
        let row = PyDict::new(py);
        row.set_item(fixture.getattr("evil").unwrap(), 9_i64)
            .unwrap();
        row.set_item("key", 1_i64).unwrap();
        let source = PyList::new(py, [&row]).unwrap();

        let error = group_count_sum_callable_key_dict_rows_v1(
            source.as_any(),
            &fixture.getattr("key").unwrap(),
            PyString::new(py, "value").as_any(),
            &fixture.getattr("accessor").unwrap(),
        )
        .unwrap_err();
        let selection_error = PyModule::import(py, "fpstreams.errors")
            .unwrap()
            .getattr("SelectionError")
            .unwrap();

        assert!(error.matches(py, &selection_error).unwrap());
        assert!(
            error
                .cause(py)
                .unwrap()
                .value(py)
                .is(fixture.getattr("primary").unwrap())
        );
        assert!(
            error
                .context(py)
                .unwrap()
                .value(py)
                .is(fixture.getattr("primary").unwrap())
        );
        assert!(
            error
                .value(py)
                .getattr("__suppress_context__")
                .unwrap()
                .is_truthy()
                .unwrap()
        );
    });
}

#[test]
fn callable_group_hash_warmup_elides_only_same_exact_builtin_tail_keys() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"def key(row):\n    return row['key']\ndef value(row):\n    return row['value']\n",
            c"callable_group_hash_warmup.py",
            c"callable_group_hash_warmup",
        )
        .unwrap();

        for (keys, expected) in [
            (
                (0_i64..34)
                    .map(|index| pyo3::types::PyInt::new(py, index).into_any().unbind())
                    .collect::<Vec<_>>(),
                CallableGroupHashProbeCounts {
                    explicit_hashes: 32,
                    elided_hashes: 2,
                    successful_warmup_rows: 32,
                },
            ),
            (
                (0_i64..32)
                    .map(|index| pyo3::types::PyInt::new(py, index).into_any().unbind())
                    .chain([PyString::new(py, "drift").into_any().unbind()])
                    .chain([pyo3::types::PyInt::new(py, 34_i64).into_any().unbind()])
                    .collect::<Vec<_>>(),
                CallableGroupHashProbeCounts {
                    explicit_hashes: 33,
                    elided_hashes: 1,
                    successful_warmup_rows: 32,
                },
            ),
            (
                (0_i64..31)
                    .map(|index| pyo3::types::PyInt::new(py, index).into_any().unbind())
                    .chain([PyString::new(py, "warmup-drift").into_any().unbind()])
                    .chain([pyo3::types::PyInt::new(py, 32_i64).into_any().unbind()])
                    .collect::<Vec<_>>(),
                CallableGroupHashProbeCounts {
                    explicit_hashes: 33,
                    elided_hashes: 0,
                    successful_warmup_rows: 32,
                },
            ),
        ] {
            let rows = PyList::empty(py);
            for key in keys {
                let row = PyDict::new(py);
                row.set_item("key", key).unwrap();
                row.set_item("value", 1_i64).unwrap();
                rows.append(row).unwrap();
            }
            begin_callable_group_hash_probe_count();
            group_count_sum_callable_key_dict_rows_v1(
                rows.as_any(),
                &fixture.getattr("key").unwrap(),
                PyString::new(py, "value").as_any(),
                &fixture.getattr("value").unwrap(),
            )
            .unwrap()
            .unwrap();
            assert_eq!(end_callable_group_hash_probe_count(), expected);
        }

        let rows = PyList::empty(py);
        for index in 0_i64..34 {
            let row = PyDict::new(py);
            row.set_item("key", index).unwrap();
            row.set_item("value", 1_i64).unwrap();
            rows.append(row).unwrap();
        }
        begin_callable_group_hash_probe_count();
        group_count_sum_callable_value_dict_rows_v1(
            rows.as_any(),
            PyString::new(py, "key").as_any(),
            &fixture.getattr("key").unwrap(),
            &fixture.getattr("value").unwrap(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            end_callable_group_hash_probe_count(),
            CallableGroupHashProbeCounts {
                explicit_hashes: 32,
                elided_hashes: 2,
                successful_warmup_rows: 32,
            }
        );
    });
}

#[test]
fn callable_group_hash_warmup_observes_a_key_only_after_python_add_succeeds() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"class Bad:\n    def __radd__(self, other):\n        raise TypeError('addition failed')\ndef key(row):\n    return row['key']\ndef value(row):\n    return row['value']\n",
            c"callable_group_hash_add_failure.py",
            c"callable_group_hash_add_failure",
        )
        .unwrap();
        let rows = PyList::empty(py);
        for index in 0..32 {
            let row = PyDict::new(py);
            row.set_item("key", index).unwrap();
            if index == 31 {
                row.set_item("value", fixture.getattr("Bad").unwrap().call0().unwrap())
                    .unwrap();
            } else {
                row.set_item("value", 1_i64).unwrap();
            }
            rows.append(row).unwrap();
        }

        begin_callable_group_hash_probe_count();
        let error = group_count_sum_callable_key_dict_rows_v1(
            rows.as_any(),
            &fixture.getattr("key").unwrap(),
            PyString::new(py, "value").as_any(),
            &fixture.getattr("value").unwrap(),
        )
        .unwrap_err();
        assert!(error.is_instance_of::<pyo3::exceptions::PyTypeError>(py));
        assert_eq!(
            end_callable_group_hash_probe_count(),
            CallableGroupHashProbeCounts {
                explicit_hashes: 32,
                elided_hashes: 0,
                successful_warmup_rows: 31,
            }
        );
    });
}
