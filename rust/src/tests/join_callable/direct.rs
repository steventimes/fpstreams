//! Direct-field callable join coverage.

use super::*;

#[test]
fn direct_unique_exact_dict_join_keeps_nonshared_same_field_suffix_and_left_order() {
    Python::initialize();
    Python::attach(|py| {
        let unmatched = PyDict::new(py);
        unmatched.set_item("id", 2_i64).unwrap();
        unmatched.set_item("payload", "left-2").unwrap();
        let matched = PyDict::new(py);
        matched.set_item("id", 1_i64).unwrap();
        matched.set_item("payload", "left-1").unwrap();
        let left = PyList::new(py, [&unmatched, &matched]).unwrap();

        let right_row = PyDict::new(py);
        right_row.set_item("id", 1_i64).unwrap();
        right_row.set_item("payload", "right-1").unwrap();
        right_row.set_item("tail", "R").unwrap();
        let right = PyTuple::new(py, [&right_row]).unwrap();
        let field = PyString::new(py, "id");
        let shared = PyFrozenSet::empty(py).unwrap();
        let exact_dict_only = PyTuple::empty(py);

        let joined = join_hashable_unique_direct_records_v1(
            left.as_any(),
            right.as_any(),
            field.as_any(),
            field.as_any(),
            true,
            PyString::new(py, "_right").as_any(),
            shared.as_any(),
            exact_dict_only.as_any(),
        )
        .unwrap()
        .unwrap();

        assert_eq!(joined.len(), 2);
        assert_eq!(
            joined[0].bind(py).keys().extract::<Vec<String>>().unwrap(),
            vec!["id", "payload", "id_right", "payload_right", "tail"]
        );
        assert_eq!(
            joined[0]
                .bind(py)
                .get_item("id")
                .unwrap()
                .unwrap()
                .extract::<i64>()
                .unwrap(),
            2
        );
        assert!(
            joined[0]
                .bind(py)
                .get_item("id_right")
                .unwrap()
                .unwrap()
                .is_none()
        );
        assert_eq!(
            joined[1]
                .bind(py)
                .get_item("id_right")
                .unwrap()
                .unwrap()
                .extract::<i64>()
                .unwrap(),
            1
        );
        assert_eq!(
            joined[1]
                .bind(py)
                .get_item("payload_right")
                .unwrap()
                .unwrap()
                .extract::<&str>()
                .unwrap(),
            "right-1"
        );
    });
}

#[test]
fn direct_many_exact_dict_join_preserves_multiplicity_order_and_collisions() {
    Python::initialize();
    Python::attach(|py| {
        let first_left = PyDict::new(py);
        first_left.set_item("id", 1_i64).unwrap();
        first_left.set_item("payload", "L1").unwrap();
        let second_left = PyDict::new(py);
        second_left.set_item("id", 2_i64).unwrap();
        second_left.set_item("payload", "L2").unwrap();
        let left = PyList::new(py, [&first_left, &second_left]).unwrap();

        let first_right = PyDict::new(py);
        first_right.set_item("id", 1_i64).unwrap();
        first_right.set_item("payload", "R1").unwrap();
        let second_right = PyDict::new(py);
        second_right.set_item("id", 1_i64).unwrap();
        second_right.set_item("payload", "R2").unwrap();
        let right = PyTuple::new(py, [&first_right, &second_right]).unwrap();
        let field = PyString::new(py, "id");
        let shared = PyFrozenSet::empty(py).unwrap();
        let exact_dict_only = PyTuple::empty(py);

        let joined = join_hashable_many_direct_records_v1(
            left.as_any(),
            right.as_any(),
            field.as_any(),
            field.as_any(),
            true,
            PyString::new(py, "_right").as_any(),
            shared.as_any(),
            exact_dict_only.as_any(),
        )
        .unwrap()
        .unwrap();

        assert_eq!(joined.len(), 3);
        assert_eq!(
            joined
                .iter()
                .map(|row| {
                    row.bind(py)
                        .get_item("payload_right")
                        .unwrap()
                        .unwrap()
                        .extract::<Option<String>>()
                        .unwrap()
                })
                .collect::<Vec<_>>(),
            vec![Some("R1".to_owned()), Some("R2".to_owned()), None]
        );
        assert_eq!(
            joined[2]
                .bind(py)
                .get_item("payload")
                .unwrap()
                .unwrap()
                .extract::<&str>()
                .unwrap(),
            "L2"
        );
    });
}

#[test]
fn direct_exact_dict_join_handles_empty_schema_and_selection_error() {
    Python::initialize();
    Python::attach(|py| {
        let left_row = PyDict::new(py);
        left_row.set_item("id", 1_i64).unwrap();
        left_row.set_item("left", "L").unwrap();
        let left = PyList::new(py, [&left_row]).unwrap();
        let empty_right = PyTuple::empty(py);
        let field = PyString::new(py, "id");
        let shared = PyFrozenSet::empty(py).unwrap();
        let exact_dict_only = PyTuple::empty(py);
        let selection_error = fpstreams_error_type(py, "SelectionError");

        let joined = join_hashable_unique_direct_records_v1(
            left.as_any(),
            empty_right.as_any(),
            field.as_any(),
            field.as_any(),
            true,
            PyString::new(py, "_right").as_any(),
            shared.as_any(),
            exact_dict_only.as_any(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(joined.len(), 1);
        assert_eq!(
            joined[0].bind(py).keys().extract::<Vec<String>>().unwrap(),
            vec!["id", "left"]
        );

        let missing = PyDict::new(py);
        missing.set_item("right", "missing id").unwrap();
        let invalid_right = PyTuple::new(py, [&missing]).unwrap();
        let error = join_hashable_unique_direct_records_v1(
            left.as_any(),
            invalid_right.as_any(),
            field.as_any(),
            field.as_any(),
            false,
            PyString::new(py, "_right").as_any(),
            shared.as_any(),
            exact_dict_only.as_any(),
        )
        .unwrap_err();
        assert!(error.matches(py, &selection_error).unwrap());
        assert!(
            error
                .cause(py)
                .unwrap()
                .is_instance_of::<pyo3::exceptions::PyKeyError>(py)
        );
    });
}

#[test]
fn direct_empty_type_capability_remains_exact_dict_only() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"from types import MappingProxyType\nrecord = MappingProxyType({'id': 1})\n",
            c"direct_exact_dict_only_fixture.py",
            c"direct_exact_dict_only_fixture",
        )
        .unwrap();
        let source = PyList::new(py, [fixture.getattr("record").unwrap()]).unwrap();
        let field = PyString::new(py, "id");
        let shared = PyFrozenSet::empty(py).unwrap();
        let exact_dict_only = PyTuple::empty(py);

        assert!(
            join_hashable_unique_direct_records_v1(
                source.as_any(),
                source.as_any(),
                field.as_any(),
                field.as_any(),
                false,
                PyString::new(py, "_right").as_any(),
                shared.as_any(),
                exact_dict_only.as_any(),
            )
            .unwrap()
            .is_none()
        );
    });
}
