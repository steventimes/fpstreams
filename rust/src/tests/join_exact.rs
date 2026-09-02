//! Exact dictionary join tests.

use super::*;

#[test]
fn exact_dict_join_preserves_left_order_and_builds_independent_snapshots() {
    Python::initialize();
    Python::attach(|py| {
        let first_key = pyo3::types::PyInt::new(py, 2_000_i64);
        let first = PyDict::new(py);
        first.set_item("id", &first_key).unwrap();
        first.set_item("left", "b").unwrap();
        let second_key = pyo3::types::PyInt::new(py, 1_000_i64);
        let second = PyDict::new(py);
        second.set_item("id", &second_key).unwrap();
        second.set_item("left", "a").unwrap();
        let left = PyList::new(py, [&first, &second]).unwrap();

        let right_key = pyo3::types::PyInt::new(py, 1_000_i64);
        let payload = PyDict::new(py);
        payload.set_item("identity", true).unwrap();
        let right_row = PyDict::new(py);
        right_row.set_item("id", &right_key).unwrap();
        right_row.set_item("right", &payload).unwrap();
        right_row.set_item("tail", "last").unwrap();
        let right = PyTuple::new(py, [&right_row]).unwrap();
        let field = PyString::new(py, "id");

        let joined = join_i64_unique_dict_rows_v1(
            left.as_any(),
            right.as_any(),
            field.as_any(),
            field.as_any(),
            true,
        )
        .unwrap()
        .unwrap();

        assert_eq!(joined.len(), 2);
        assert_eq!(
            joined[0]
                .bind(py)
                .get_item("id")
                .unwrap()
                .unwrap()
                .extract::<i64>()
                .unwrap(),
            2_000
        );
        assert!(
            joined[0]
                .bind(py)
                .get_item("right")
                .unwrap()
                .unwrap()
                .is_none()
        );
        assert!(
            joined[0]
                .bind(py)
                .get_item("tail")
                .unwrap()
                .unwrap()
                .is_none()
        );
        assert!(
            joined[1]
                .bind(py)
                .get_item("id")
                .unwrap()
                .unwrap()
                .is(&second_key)
        );
        assert!(
            joined[1]
                .bind(py)
                .get_item("right")
                .unwrap()
                .unwrap()
                .is(&payload)
        );
        assert_eq!(
            joined[1].bind(py).keys().extract::<Vec<String>>().unwrap(),
            vec!["id", "left", "right", "tail"]
        );

        first.set_item("left", "mutated").unwrap();
        right_row.set_item("right", "mutated").unwrap();
        assert_eq!(
            joined[0]
                .bind(py)
                .get_item("left")
                .unwrap()
                .unwrap()
                .extract::<&str>()
                .unwrap(),
            "b"
        );
        assert!(
            joined[1]
                .bind(py)
                .get_item("right")
                .unwrap()
                .unwrap()
                .is(&payload)
        );
    });
}

#[test]
fn exact_dict_join_v2_preserves_borrowed_source_semantics() {
    Python::initialize();
    Python::attach(|py| {
        let first = PyDict::new(py);
        first.set_item("id", 2_i64).unwrap();
        first.set_item("left", "unmatched").unwrap();
        let second = PyDict::new(py);
        second.set_item("id", 1_i64).unwrap();
        second.set_item("left", "matched").unwrap();
        let left = PyTuple::new(py, [&first, &second]).unwrap();

        let payload = PyDict::new(py);
        payload.set_item("owned", false).unwrap();
        let right_row = PyDict::new(py);
        right_row.set_item("id", 1_i64).unwrap();
        right_row.set_item("right", &payload).unwrap();
        let right = PyList::new(py, [&right_row]).unwrap();
        let field = PyString::new(py, "id");

        let joined = join_i64_unique_dict_rows_v2(
            left.as_any(),
            right.as_any(),
            field.as_any(),
            field.as_any(),
            true,
        )
        .unwrap()
        .unwrap();

        assert_eq!(joined.len(), 2);
        assert!(
            joined[0]
                .bind(py)
                .get_item("right")
                .unwrap()
                .unwrap()
                .is_none()
        );
        assert!(
            joined[1]
                .bind(py)
                .get_item("right")
                .unwrap()
                .unwrap()
                .is(&payload)
        );
        second.set_item("left", "changed").unwrap();
        right_row.set_item("right", "changed").unwrap();
        assert_eq!(
            joined[1]
                .bind(py)
                .get_item("left")
                .unwrap()
                .unwrap()
                .extract::<&str>()
                .unwrap(),
            "matched"
        );
        assert!(
            joined[1]
                .bind(py)
                .get_item("right")
                .unwrap()
                .unwrap()
                .is(&payload)
        );
    });
}

#[test]
fn exact_dict_join_supports_inner_join_and_distinct_key_fields() {
    Python::initialize();
    Python::attach(|py| {
        let first = PyDict::new(py);
        first.set_item("id", 1_i64).unwrap();
        first.set_item("left", "unmatched").unwrap();
        let second = PyDict::new(py);
        second.set_item("id", 2_i64).unwrap();
        second.set_item("left", "matched").unwrap();
        let left = PyTuple::new(py, [&first, &second]).unwrap();

        let right_row = PyDict::new(py);
        right_row.set_item("rid", 2_i64).unwrap();
        right_row.set_item("right", "R").unwrap();
        let right = PyList::new(py, [&right_row]).unwrap();
        let left_field = PyString::new(py, "id");
        let right_field = PyString::new(py, "rid");

        let joined = join_i64_unique_dict_rows_v1(
            left.as_any(),
            right.as_any(),
            left_field.as_any(),
            right_field.as_any(),
            false,
        )
        .unwrap()
        .unwrap();

        assert_eq!(joined.len(), 1);
        assert_eq!(
            joined[0].bind(py).keys().extract::<Vec<String>>().unwrap(),
            vec!["id", "left", "rid", "right"]
        );
        assert_eq!(
            joined[0]
                .bind(py)
                .get_item("left")
                .unwrap()
                .unwrap()
                .extract::<&str>()
                .unwrap(),
            "matched"
        );
    });
}

#[test]
fn exact_dict_join_canonicalizes_equal_nonidentical_right_field_objects() {
    Python::initialize();
    Python::attach(|py| {
        let left_id = PyString::new(py, "left_join_identifier_that_is_not_interned_123456789");
        let left_payload = PyString::new(py, "left_join_payload_that_is_not_interned_123456789");
        let right_id_first =
            PyString::new(py, "right_join_identifier_that_is_not_interned_123456789");
        let right_id_second =
            PyString::new(py, "right_join_identifier_that_is_not_interned_123456789");
        let right_payload_first =
            PyString::new(py, "right_join_payload_that_is_not_interned_123456789");
        let right_payload_second =
            PyString::new(py, "right_join_payload_that_is_not_interned_123456789");
        assert!(!right_id_first.is(&right_id_second));
        assert!(!right_payload_first.is(&right_payload_second));

        let left_first = PyDict::new(py);
        left_first.set_item(&left_id, 1_i64).unwrap();
        left_first.set_item(&left_payload, "L1").unwrap();
        let left_second = PyDict::new(py);
        left_second.set_item(&left_id, 2_i64).unwrap();
        left_second.set_item(&left_payload, "L2").unwrap();
        let left = PyList::new(py, [&left_first, &left_second]).unwrap();

        let right_first = PyDict::new(py);
        right_first.set_item(&right_id_first, 1_i64).unwrap();
        right_first.set_item(&right_payload_first, "R1").unwrap();
        let right_second = PyDict::new(py);
        right_second.set_item(&right_id_second, 2_i64).unwrap();
        right_second.set_item(&right_payload_second, "R2").unwrap();
        let right = PyList::new(py, [&right_first, &right_second]).unwrap();

        let joined = join_i64_unique_dict_rows_v1(
            left.as_any(),
            right.as_any(),
            left_id.as_any(),
            right_id_first.as_any(),
            false,
        )
        .unwrap()
        .unwrap();

        assert_eq!(joined.len(), 2);
        for output in &joined {
            let keys = output.bind(py).keys();
            assert_eq!(keys.len(), 4);
            assert!(keys.get_item(2).unwrap().is(&right_id_first));
            assert!(keys.get_item(3).unwrap().is(&right_payload_first));
        }

        let joined_many = join_i64_many_dict_rows_v1(
            left.as_any(),
            right.as_any(),
            left_id.as_any(),
            right_id_first.as_any(),
            false,
        )
        .unwrap()
        .unwrap();
        assert_eq!(joined_many.len(), 2);
        for output in &joined_many {
            let keys = output.bind(py).keys();
            assert!(keys.get_item(2).unwrap().is(&right_id_first));
            assert!(keys.get_item(3).unwrap().is(&right_payload_first));
        }
    });
}

#[test]
fn exact_dict_join_accepts_compact_and_bulk_width_boundaries_and_declines_wider_rows() {
    Python::initialize();
    Python::attach(|py| {
        let id = PyString::new(py, "id");
        let left_payload = PyString::new(py, "left");
        let left_first = PyDict::new(py);
        left_first.set_item(&id, 1_i64).unwrap();
        left_first.set_item(&left_payload, "L1").unwrap();
        let left_second = PyDict::new(py);
        left_second.set_item(&id, 2_i64).unwrap();
        left_second.set_item(&left_payload, "L2").unwrap();
        let left = PyList::new(py, [&left_first, &left_second]).unwrap();

        for field_count in [23_usize, 24, 63, 64] {
            let payload_fields = (1..field_count)
                .map(|index| PyString::new(py, &format!("right_{index}")))
                .collect::<Vec<_>>();
            let first = PyDict::new(py);
            first.set_item(&id, 1_i64).unwrap();
            let second = PyDict::new(py);
            second.set_item(&id, 2_i64).unwrap();
            for (index, field) in payload_fields.iter().enumerate() {
                first.set_item(field, index as i64).unwrap();
                second.set_item(field, 100_i64 + index as i64).unwrap();
            }
            let right = PyList::new(py, [&first, &second]).unwrap();
            let joined = join_i64_unique_dict_rows_v1(
                left.as_any(),
                right.as_any(),
                id.as_any(),
                id.as_any(),
                false,
            )
            .unwrap()
            .unwrap();
            assert_eq!(joined.len(), 2);
            let last_field = payload_fields.last().unwrap();
            assert_eq!(
                joined[0]
                    .bind(py)
                    .get_item(last_field)
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                field_count as i64 - 2
            );
            assert_eq!(
                joined[1]
                    .bind(py)
                    .get_item(last_field)
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                100_i64 + field_count as i64 - 2
            );

            second.set_item(&id, 1_i64).unwrap();
            let many = join_i64_many_dict_rows_v1(
                PyList::new(py, [&left_first]).unwrap().as_any(),
                right.as_any(),
                id.as_any(),
                id.as_any(),
                false,
            )
            .unwrap()
            .unwrap();
            assert_eq!(many.len(), 2);
            assert_eq!(
                many[0]
                    .bind(py)
                    .get_item(last_field)
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                field_count as i64 - 2
            );
            assert_eq!(
                many[1]
                    .bind(py)
                    .get_item(last_field)
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                100_i64 + field_count as i64 - 2
            );
        }

        let too_wide = PyDict::new(py);
        too_wide.set_item(&id, 1_i64).unwrap();
        for index in 1..65 {
            too_wide
                .set_item(format!("too_wide_{index}"), index)
                .unwrap();
        }
        let too_wide_rows = PyList::new(py, [&too_wide]).unwrap();
        assert!(
            join_i64_unique_dict_rows_v1(
                PyList::new(py, [&left_first]).unwrap().as_any(),
                too_wide_rows.as_any(),
                id.as_any(),
                id.as_any(),
                false,
            )
            .unwrap()
            .is_none()
        );
        assert!(
            join_i64_many_dict_rows_v1(
                PyList::new(py, [&left_first]).unwrap().as_any(),
                too_wide_rows.as_any(),
                id.as_any(),
                id.as_any(),
                false,
            )
            .unwrap()
            .is_none()
        );
    });
}

#[test]
fn exact_dict_join_bulk_merges_wide_canonical_schemas_across_versions() {
    const FIELD_COUNT: usize = 24;

    Python::initialize();
    Python::attach(|py| {
        let id = PyString::new(py, "id");
        let left = PyDict::new(py);
        left.set_item(&id, 1_i64).unwrap();
        left.set_item("left", true).unwrap();

        let right = PyDict::new(py);
        right.set_item(&id, 1_i64).unwrap();
        for index in 1..FIELD_COUNT {
            right.set_item(format!("right_{index}"), index).unwrap();
        }

        begin_record_join_probe_count();
        let joined = join_i64_unique_dict_rows_v1(
            PyList::new(py, [&left]).unwrap().as_any(),
            PyList::new(py, [&right]).unwrap().as_any(),
            id.as_any(),
            id.as_any(),
            false,
        )
        .unwrap()
        .unwrap();
        let probes = end_record_join_probe_count();

        assert_eq!(joined.len(), 1);
        assert_eq!(probes.bulk_merge_hits, 1);

        begin_record_join_probe_count();
        let joined_many = join_i64_many_dict_rows_v1(
            PyList::new(py, [&left]).unwrap().as_any(),
            PyList::new(py, [&right]).unwrap().as_any(),
            id.as_any(),
            id.as_any(),
            false,
        )
        .unwrap()
        .unwrap();
        let many_probes = end_record_join_probe_count();

        assert_eq!(joined_many.len(), 1);
        assert_eq!(many_probes.bulk_merge_hits, 1);
    });
}

#[test]
fn exact_dict_join_declines_duplicates_collisions_and_non_i64_keys() {
    Python::initialize();
    Python::attach(|py| {
        let field = PyString::new(py, "id");
        let left_row = PyDict::new(py);
        left_row.set_item("id", 1_i64).unwrap();
        left_row.set_item("value", "left").unwrap();
        let left = PyList::new(py, [&left_row]).unwrap();

        let duplicate_a = PyDict::new(py);
        duplicate_a.set_item("id", 1_i64).unwrap();
        duplicate_a.set_item("right", "a").unwrap();
        let duplicate_b = PyDict::new(py);
        duplicate_b.set_item("id", 1_i64).unwrap();
        duplicate_b.set_item("right", "b").unwrap();
        let duplicates = PyTuple::new(py, [&duplicate_a, &duplicate_b]).unwrap();
        assert!(
            join_i64_unique_dict_rows_v1(
                left.as_any(),
                duplicates.as_any(),
                field.as_any(),
                field.as_any(),
                false,
            )
            .unwrap()
            .is_none()
        );

        let collision = PyDict::new(py);
        collision.set_item("id", 1_i64).unwrap();
        collision.set_item("value", "right").unwrap();
        let collisions = PyList::new(py, [&collision]).unwrap();
        assert!(
            join_i64_unique_dict_rows_v1(
                left.as_any(),
                collisions.as_any(),
                field.as_any(),
                field.as_any(),
                false,
            )
            .unwrap()
            .is_none()
        );

        let boolean = PyDict::new(py);
        boolean.set_item("id", true).unwrap();
        boolean.set_item("value", "left").unwrap();
        let boolean_left = PyList::new(py, [&boolean]).unwrap();
        assert!(
            join_i64_unique_dict_rows_v1(
                boolean_left.as_any(),
                collisions.as_any(),
                field.as_any(),
                field.as_any(),
                false,
            )
            .unwrap()
            .is_none()
        );

        let huge = py.eval(c"1 << 100", None, None).unwrap();
        let oversized = PyDict::new(py);
        oversized.set_item("id", huge).unwrap();
        oversized.set_item("value", "left").unwrap();
        let oversized_left = PyTuple::new(py, [&oversized]).unwrap();
        assert!(
            join_i64_unique_dict_rows_v1(
                oversized_left.as_any(),
                collisions.as_any(),
                field.as_any(),
                field.as_any(),
                false,
            )
            .unwrap()
            .is_none()
        );
    });
}

#[test]
fn exact_dict_join_declines_non_fixed_schemas_wide_rows_and_unsafe_shapes() {
    Python::initialize();
    Python::attach(|py| {
        let field = PyString::new(py, "id");
        let first = PyDict::new(py);
        first.set_item("id", 1_i64).unwrap();
        first.set_item("left", "a").unwrap();
        let reordered = PyDict::new(py);
        reordered.set_item("left", "b").unwrap();
        reordered.set_item("id", 2_i64).unwrap();
        let non_fixed = PyList::new(py, [&first, &reordered]).unwrap();
        let right_row = PyDict::new(py);
        right_row.set_item("id", 1_i64).unwrap();
        right_row.set_item("right", "A").unwrap();
        let right = PyTuple::new(py, [&right_row]).unwrap();
        assert!(
            join_i64_unique_dict_rows_v1(
                non_fixed.as_any(),
                right.as_any(),
                field.as_any(),
                field.as_any(),
                false,
            )
            .unwrap()
            .is_none()
        );

        let wide = PyDict::new(py);
        wide.set_item("id", 1_i64).unwrap();
        for index in 0..64 {
            wide.set_item(format!("field_{index}"), index).unwrap();
        }
        let wide_left = PyTuple::new(py, [&wide]).unwrap();
        assert!(
            join_i64_unique_dict_rows_v1(
                wide_left.as_any(),
                right.as_any(),
                field.as_any(),
                field.as_any(),
                false,
            )
            .unwrap()
            .is_none()
        );

        let dotted = PyString::new(py, "nested.id");
        assert!(
            join_i64_unique_dict_rows_v1(
                PyList::new(py, [&first]).unwrap().as_any(),
                right.as_any(),
                dotted.as_any(),
                field.as_any(),
                false,
            )
            .unwrap()
            .is_none()
        );

        let fixture = PyModule::from_code(
            py,
            c"class Rows(list): pass\nclass Record(dict): pass\nclass Field(str): pass\nrows = Rows([{'id': 1}])\nrecords = [Record(id=1)]\nfield = Field('id')\n",
            c"native_join_shapes.py",
            c"native_join_shapes",
        )
        .unwrap();
        assert!(
            join_i64_unique_dict_rows_v1(
                &fixture.getattr("rows").unwrap(),
                right.as_any(),
                field.as_any(),
                field.as_any(),
                false,
            )
            .unwrap()
            .is_none()
        );
        assert!(
            join_i64_unique_dict_rows_v1(
                &fixture.getattr("records").unwrap(),
                right.as_any(),
                field.as_any(),
                field.as_any(),
                false,
            )
            .unwrap()
            .is_none()
        );
        assert!(
            join_i64_unique_dict_rows_v1(
                PyList::new(py, [&first]).unwrap().as_any(),
                right.as_any(),
                &fixture.getattr("field").unwrap(),
                field.as_any(),
                false,
            )
            .unwrap()
            .is_none()
        );
    });
}

#[test]
fn exact_dict_many_join_preserves_duplicate_order_and_independent_snapshots() {
    Python::initialize();
    Python::attach(|py| {
        let make_left = |id: i64, value: &str| {
            let row = PyDict::new(py);
            row.set_item("id", id).unwrap();
            row.set_item("left", value).unwrap();
            row
        };
        let first = make_left(1, "l0");
        let second = make_left(2, "l1");
        let third = make_left(1, "l2");
        let left = PyList::new(py, [&first, &second, &third]).unwrap();

        let make_right = |id: i64, value: &str| {
            let row = PyDict::new(py);
            row.set_item("id", id).unwrap();
            row.set_item("right", value).unwrap();
            row
        };
        let right_a = make_right(1, "a");
        let right_b = make_right(1, "b");
        let right_c = make_right(2, "c");
        let right = PyTuple::new(py, [&right_a, &right_b, &right_c]).unwrap();
        let field = PyString::new(py, "id");

        let joined = join_i64_many_dict_rows_v1(
            left.as_any(),
            right.as_any(),
            field.as_any(),
            field.as_any(),
            false,
        )
        .unwrap()
        .unwrap();
        let values = joined
            .iter()
            .map(|row| {
                let row = row.bind(py);
                (
                    row.get_item("left")
                        .unwrap()
                        .unwrap()
                        .extract::<String>()
                        .unwrap(),
                    row.get_item("right")
                        .unwrap()
                        .unwrap()
                        .extract::<String>()
                        .unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            vec![
                ("l0".to_owned(), "a".to_owned()),
                ("l0".to_owned(), "b".to_owned()),
                ("l1".to_owned(), "c".to_owned()),
                ("l2".to_owned(), "a".to_owned()),
                ("l2".to_owned(), "b".to_owned()),
            ]
        );

        assert!(!joined[0].bind(py).is(joined[1].bind(py)));
        joined[0].bind(py).set_item("left", "changed").unwrap();
        assert_eq!(
            joined[1]
                .bind(py)
                .get_item("left")
                .unwrap()
                .unwrap()
                .extract::<&str>()
                .unwrap(),
            "l0"
        );
        first.set_item("left", "source-changed").unwrap();
        right_b.set_item("right", "source-changed").unwrap();
        assert_eq!(
            joined[1]
                .bind(py)
                .get_item("right")
                .unwrap()
                .unwrap()
                .extract::<&str>()
                .unwrap(),
            "b"
        );
    });
}

#[test]
fn exact_dict_many_join_left_fills_unmatched_right_fields() {
    Python::initialize();
    Python::attach(|py| {
        let unmatched = PyDict::new(py);
        unmatched.set_item("id", 7_i64).unwrap();
        unmatched.set_item("left", "missing").unwrap();
        let matched = PyDict::new(py);
        matched.set_item("id", 1_i64).unwrap();
        matched.set_item("left", "present").unwrap();
        let left = PyTuple::new(py, [&unmatched, &matched]).unwrap();

        let first = PyDict::new(py);
        first.set_item("id", 1_i64).unwrap();
        first.set_item("right", "a").unwrap();
        first.set_item("tail", 10_i64).unwrap();
        let second = PyDict::new(py);
        second.set_item("id", 1_i64).unwrap();
        second.set_item("right", "b").unwrap();
        second.set_item("tail", 20_i64).unwrap();
        let right = PyList::new(py, [&first, &second]).unwrap();
        let field = PyString::new(py, "id");

        let joined = join_i64_many_dict_rows_v1(
            left.as_any(),
            right.as_any(),
            field.as_any(),
            field.as_any(),
            true,
        )
        .unwrap()
        .unwrap();

        assert_eq!(joined.len(), 3);
        assert!(
            joined[0]
                .bind(py)
                .get_item("right")
                .unwrap()
                .unwrap()
                .is_none()
        );
        assert!(
            joined[0]
                .bind(py)
                .get_item("tail")
                .unwrap()
                .unwrap()
                .is_none()
        );
        assert_eq!(
            joined[1]
                .bind(py)
                .get_item("right")
                .unwrap()
                .unwrap()
                .extract::<&str>()
                .unwrap(),
            "a"
        );
        assert_eq!(
            joined[2]
                .bind(py)
                .get_item("right")
                .unwrap()
                .unwrap()
                .extract::<&str>()
                .unwrap(),
            "b"
        );
    });
}

#[test]
fn exact_dict_many_join_handles_empty_sides_and_declines_unsafe_rows() {
    Python::initialize();
    Python::attach(|py| {
        let field = PyString::new(py, "id");
        let left_row = PyDict::new(py);
        left_row.set_item("id", 1_i64).unwrap();
        left_row.set_item("left", "value").unwrap();
        let left = PyList::new(py, [&left_row]).unwrap();
        let empty = PyTuple::empty(py);

        let inner = join_i64_many_dict_rows_v1(
            left.as_any(),
            empty.as_any(),
            field.as_any(),
            field.as_any(),
            false,
        )
        .unwrap()
        .unwrap();
        assert!(inner.is_empty());
        let outer = join_i64_many_dict_rows_v1(
            left.as_any(),
            empty.as_any(),
            field.as_any(),
            field.as_any(),
            true,
        )
        .unwrap()
        .unwrap();
        assert_eq!(outer.len(), 1);
        assert_eq!(
            outer[0].bind(py).keys().extract::<Vec<String>>().unwrap(),
            vec!["id", "left"]
        );

        let right_row = PyDict::new(py);
        right_row.set_item("id", 1_i64).unwrap();
        right_row.set_item("right", "value").unwrap();
        let right = PyList::new(py, [&right_row]).unwrap();
        assert!(
            join_i64_many_dict_rows_v1(
                empty.as_any(),
                right.as_any(),
                field.as_any(),
                field.as_any(),
                true,
            )
            .unwrap()
            .unwrap()
            .is_empty()
        );

        let collision = PyDict::new(py);
        collision.set_item("id", 1_i64).unwrap();
        collision.set_item("left", "collision").unwrap();
        let collisions = PyList::new(py, [&collision]).unwrap();
        assert!(
            join_i64_many_dict_rows_v1(
                left.as_any(),
                collisions.as_any(),
                field.as_any(),
                field.as_any(),
                false,
            )
            .unwrap()
            .is_none()
        );

        let invalid = PyDict::new(py);
        invalid.set_item("id", true).unwrap();
        invalid.set_item("right", "value").unwrap();
        let invalid_right = PyList::new(py, [&invalid]).unwrap();
        assert!(
            join_i64_many_dict_rows_v1(
                left.as_any(),
                invalid_right.as_any(),
                field.as_any(),
                field.as_any(),
                false,
            )
            .unwrap()
            .is_none()
        );
    });
}

#[test]
fn exact_dict_many_join_checks_output_size_without_allocating_the_boundary() {
    let limit = usize::try_from(pyo3::ffi::PY_SSIZE_T_MAX).unwrap();
    assert_eq!(checked_join_output_size(limit - 1, 1).unwrap(), limit);

    Python::initialize();
    Python::attach(|py| {
        let list_limit_error = checked_join_output_size(limit, 1).unwrap_err();
        assert!(list_limit_error.is_instance_of::<pyo3::exceptions::PyMemoryError>(py));
        let usize_error = checked_join_output_size(usize::MAX, 1).unwrap_err();
        assert!(usize_error.is_instance_of::<pyo3::exceptions::PyMemoryError>(py));
    });
}
