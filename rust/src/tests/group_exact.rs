//! Exact tuple and dictionary grouping tests.

use super::*;

mod pairs;
mod rows;

#[test]
fn grouped_i64_sum_preserves_first_seen_key_order() {
    let rows = vec![(3, 5), (1, 7), (3, 11), (2, -4), (1, 2)];

    assert_eq!(
        group_sum_pairs(rows, Some((1, 3))),
        Some(vec![(3, 16), (1, 9), (2, -4)])
    );
}

#[test]
fn grouped_i64_sum_handles_sparse_extreme_keys_without_dense_allocation() {
    let rows = vec![(i64::MIN, 1), (i64::MAX, 2), (i64::MIN, 3)];

    assert_eq!(
        group_sum_pairs(rows, Some((i64::MIN, i64::MAX))),
        Some(vec![(i64::MIN, 4), (i64::MAX, 2)])
    );
}

#[test]
fn exact_dict_global_sum_accepts_i64_rows_and_widens_the_total() {
    Python::initialize();
    Python::attach(|py| {
        let value_field = PyString::new(py, "value");
        let first = PyDict::new(py);
        first.set_item(&value_field, i64::MAX).unwrap();
        let second = PyDict::new(py);
        second.set_item(&value_field, i64::MAX).unwrap();
        let list_source = PyList::new(py, [&first, &second]).unwrap();
        let tuple_source = PyTuple::new(py, [&first, &second]).unwrap();
        let expected = i128::from(i64::MAX) * 2;

        assert_eq!(
            global_sum_i64_dict_rows_v1(list_source.as_any(), value_field.as_any()).unwrap(),
            Some(expected)
        );
        assert_eq!(
            global_sum_i64_dict_rows_v1(tuple_source.as_any(), value_field.as_any()).unwrap(),
            Some(expected)
        );
    });
}

#[test]
fn exact_dict_global_sum_declines_unsafe_values_without_protocol_dispatch() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"class Trap:\n    calls = 0\n    def __hash__(self):\n        return hash('value')\n    def __eq__(self, other):\n        type(self).calls += 1\n        raise AssertionError('lookup touched custom equality')\ntrap = Trap()\nrow = {trap: 1}\n",
            c"native_global_sum_safety.py",
            c"native_global_sum_safety",
        )
        .unwrap();
        let value_field = PyString::new(py, "value");
        let row = fixture.getattr("row").unwrap();
        let trap_source = PyList::new(py, [&row]).unwrap();
        assert!(
            global_sum_i64_dict_rows_v1(trap_source.as_any(), value_field.as_any())
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

        let boolean = PyDict::new(py);
        boolean.set_item(&value_field, true).unwrap();
        let boolean_source = PyList::new(py, [&boolean]).unwrap();
        assert!(
            global_sum_i64_dict_rows_v1(boolean_source.as_any(), value_field.as_any())
                .unwrap()
                .is_none()
        );

        let huge = py.eval(c"1 << 100", None, None).unwrap();
        let oversized = PyDict::new(py);
        oversized.set_item(&value_field, huge).unwrap();
        let oversized_source = PyList::new(py, [&oversized]).unwrap();
        assert!(
            global_sum_i64_dict_rows_v1(oversized_source.as_any(), value_field.as_any())
                .unwrap()
                .is_none()
        );
    });
}
