//! Parity tests for fused native stages, streaming terminals, short-circuiting, and aggregation.

use crate::common::{
    AGGREGATE_COUNT, AGGREGATE_LAST, AGGREGATE_M2, AGGREGATE_MAXIMUM, AGGREGATE_MEAN,
    AGGREGATE_MINIMUM, AGGREGATE_TOTAL, CompensatedSum, OnlineStatistics, all_exact_dict_rows_v1,
    materialize_target, materialize_values,
};
use crate::float::{
    F64AggregateSnapshot, FloatInstruction, PreparedFloatExpression,
    aggregate_f64_buffer_masked_v1, aggregate_f64_buffer_masked_v2, evaluate_f64,
    execute_f64_buffer_v1, materialize_f64_buffer_v1, mean_f64_buffer_v1, mean_f64_buffer_v2,
    prepare_float_expression, run_f64, run_f64_affine_comparison_pair_sum,
    run_f64_aggregate_masked, run_f64_buffer_materialization, run_f64_count,
    run_f64_identity_aggregate_masked, run_f64_identity_mean, run_f64_identity_terminal,
    run_f64_mean, run_f64_statistics, run_f64_terminal, terminal_f64_buffer_v1,
    terminal_f64_buffer_v2,
};
use crate::integer::{
    PreparedExpression, aggregate_i64_buffer_masked_v1, aggregate_i64_buffer_masked_v2, evaluate,
    floor_div, frequencies_i64_exact_v1, mean_i64_buffer_v1, mean_i64_buffer_v2, modulo,
    prepare_expression, run_i64_aggregate, run_i64_aggregate_masked,
    run_i64_buffer_materialization, run_i64_identity_aggregate_masked, run_i64_identity_mean,
    run_i64_identity_mean_by, run_i64_mean, run_i64_statistics, run_terminal, run_values,
};
use crate::numeric_mean::{update_mean_f64_buffer_v1, update_mean_i64_buffer_v1};
#[cfg(not(Py_GIL_DISABLED))]
use crate::relational::standard_namedtuple_record_adapter_v1;
use crate::relational::{
    CallableRightSchemaProbeCounts, begin_callable_right_schema_probe_count,
    begin_record_join_probe_count, checked_join_output_size, end_callable_right_schema_probe_count,
    end_record_join_probe_count, global_sum_i64_dict_rows_v1, group_sum_i64_dict_rows,
    group_sum_i64_dict_rows_v1, group_sum_i64_exact_pairs_v1, group_sum_i64_exact_pairs_v2,
    group_sum_i64_pairs, group_sum_i64_rows_v1, group_sum_pairs,
    join_hashable_many_direct_records_v1, join_hashable_many_records_v1,
    join_hashable_unique_direct_records_v1, join_hashable_unique_records_v1,
    join_hashable_unique_records_v2, join_i64_many_dict_rows_v1, join_i64_unique_dict_rows_v1,
    join_i64_unique_dict_rows_v2,
};
use crate::relational_fixed::{group_fixed_i64_dict_rows_v1, group_fixed_i64_rows_v1};
#[cfg(not(Py_GIL_DISABLED))]
use pyo3::IntoPyObject;
use pyo3::types::{
    PyAnyMethods, PyDict, PyDictMethods, PyFrozenSet, PyList, PyListMethods, PyModule, PyString,
    PyTuple, PyTupleMethods,
};
use pyo3::{Bound, Py, PyAny, Python};
#[cfg(not(Py_GIL_DISABLED))]
use std::sync::Mutex;

#[cfg(not(Py_GIL_DISABLED))]
static STANDARD_NAMEDTUPLE_TEST_LOCK: Mutex<()> = Mutex::new(());

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn pair_i64_value_filter_collects_exact_pairs_with_first_policy() {
    Python::initialize();
    Python::attach(|py| {
        let rows = PyList::new(
            py,
            [
                PyTuple::new(py, [1, 1]).unwrap(),
                PyTuple::new(py, [1, 2]).unwrap(),
                PyTuple::new(py, [2, 4]).unwrap(),
            ],
        )
        .unwrap();
        let source = rows.as_any().call_method0("__iter__").unwrap();
        let output = PyDict::new(py);

        let result = crate::pair::pair_i64_value_filter_to_dict_exact_prefix_v1(
            output.as_any(),
            &source,
            vec![(0, 0), (1, 2), (6, 0), (1, 0), (8, 0)],
            true,
        )
        .unwrap()
        .unwrap();

        assert!(result.1);
        assert!(result.0.is_none());
        assert_eq!(output.len(), 2);
        assert_eq!(
            output
                .get_item(1)
                .unwrap()
                .unwrap()
                .extract::<i32>()
                .unwrap(),
            2
        );
        assert_eq!(
            output
                .get_item(2)
                .unwrap()
                .unwrap()
                .extract::<i32>()
                .unwrap(),
            4
        );
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn pair_f64_value_filter_accepts_exact_ints_and_floats_with_last_policy() {
    Python::initialize();
    Python::attach(|py| {
        let rows = PyList::new(
            py,
            [
                PyTuple::new(
                    py,
                    [
                        "a".into_pyobject(py).unwrap().as_any(),
                        1_i32.into_pyobject(py).unwrap().as_any(),
                    ],
                )
                .unwrap(),
                PyTuple::new(
                    py,
                    [
                        "a".into_pyobject(py).unwrap().as_any(),
                        2.5_f64.into_pyobject(py).unwrap().as_any(),
                    ],
                )
                .unwrap(),
                PyTuple::new(
                    py,
                    [
                        "b".into_pyobject(py).unwrap().as_any(),
                        3_i32.into_pyobject(py).unwrap().as_any(),
                    ],
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let source = rows.as_any().call_method0("__iter__").unwrap();
        let output = PyDict::new(py);

        let result = crate::pair::pair_f64_value_filter_to_dict_exact_prefix_v1(
            output.as_any(),
            &source,
            vec![(0, 0.0), (1, 1.5), (12, 0.0)],
            false,
        )
        .unwrap()
        .unwrap();

        assert!(result.1);
        assert!(result.0.is_none());
        assert_eq!(output.len(), 2);
        assert_eq!(
            output
                .get_item("a")
                .unwrap()
                .unwrap()
                .extract::<f64>()
                .unwrap(),
            2.5
        );
        assert_eq!(
            output
                .get_item("b")
                .unwrap()
                .unwrap()
                .extract::<i32>()
                .unwrap(),
            3
        );
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn pair_i64_row_filter_collects_tuple_iterator_with_last_policy() {
    Python::initialize();
    Python::attach(|py| {
        let rows = PyTuple::new(
            py,
            [
                PyTuple::new(py, [1_i64, 1001_i64]).unwrap(),
                PyTuple::new(py, [1_i64, 1002_i64]).unwrap(),
                PyTuple::new(py, [1_i64, 1003_i64]).unwrap(),
                PyTuple::new(py, [2_i64, 1000_i64]).unwrap(),
            ],
        )
        .unwrap();
        let source = rows.as_any().call_method0("__iter__").unwrap();
        let output = PyDict::new(py);

        let result = crate::pair::pair_i64_row_filter_to_dict_exact_prefix_v1(
            output.as_any(),
            &source,
            vec![(19, 0), (0, 0), (2, 0), (1, 2), (6, 0), (1, 0), (8, 0)],
            false,
        )
        .unwrap()
        .unwrap();

        assert!(result.1);
        assert!(result.0.is_none());
        assert_eq!(output.len(), 2);
        assert_eq!(
            output
                .get_item(1)
                .unwrap()
                .unwrap()
                .extract::<i64>()
                .unwrap(),
            1003
        );
        assert_eq!(
            output
                .get_item(2)
                .unwrap()
                .unwrap()
                .extract::<i64>()
                .unwrap(),
            1000
        );
        let expected_value = rows
            .get_item(2)
            .unwrap()
            .cast_into::<PyTuple>()
            .unwrap()
            .get_item(1)
            .unwrap();
        assert!(output.get_item(1).unwrap().unwrap().is(&expected_value));
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn pair_i64_row_filter_collects_list_iterator_with_first_policy() {
    Python::initialize();
    Python::attach(|py| {
        let rows = PyList::new(
            py,
            [
                PyTuple::new(py, [7_i64, 1_i64]).unwrap(),
                PyTuple::new(py, [7_i64, 2_i64]).unwrap(),
                PyTuple::new(py, [8_i64, 9_i64]).unwrap(),
            ],
        )
        .unwrap();
        let source = rows.as_any().call_method0("__iter__").unwrap();
        let output = PyDict::new(py);

        let result = crate::pair::pair_i64_row_filter_to_dict_exact_prefix_v1(
            output.as_any(),
            &source,
            vec![(19, 0), (0, 0), (13, 0)],
            true,
        )
        .unwrap()
        .unwrap();

        assert!(result.1);
        assert!(result.0.is_none());
        assert_eq!(output.len(), 1);
        assert_eq!(
            output
                .get_item(7)
                .unwrap()
                .unwrap()
                .extract::<i64>()
                .unwrap(),
            1
        );
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn pair_i64_row_filter_returns_incompatible_row_without_consuming_suffix() {
    Python::initialize();
    Python::attach(|py| {
        let first = PyTuple::new(py, [1_i64, 1_i64]).unwrap();
        let boundary_key = 2_i64.into_pyobject(py).unwrap();
        let boundary_value = true.into_pyobject(py).unwrap();
        let boundary = PyTuple::new(py, [boundary_key.as_any(), boundary_value.as_any()]).unwrap();
        let tail = PyTuple::new(py, [3_i64, 3_i64]).unwrap();
        let rows = PyList::new(py, [&first, &boundary, &tail]).unwrap();
        let source = rows.as_any().call_method0("__iter__").unwrap();
        let output = PyDict::new(py);

        let (returned, completed) = crate::pair::pair_i64_row_filter_to_dict_exact_prefix_v1(
            output.as_any(),
            &source,
            vec![(19, 0), (0, 0), (8, 0)],
            false,
        )
        .unwrap()
        .unwrap();

        assert!(!completed);
        assert!(returned.unwrap().bind(py).is(&boundary));
        assert_eq!(output.len(), 1);
        assert_eq!(
            output
                .get_item(1)
                .unwrap()
                .unwrap()
                .extract::<i64>()
                .unwrap(),
            1
        );
        assert!(source.call_method0("__next__").unwrap().is(&tail));
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn pair_i64_row_filter_turns_arithmetic_overflow_into_a_boundary() {
    Python::initialize();
    Python::attach(|py| {
        let first = PyTuple::new(py, [1_i64, 1_i64]).unwrap();
        let boundary = PyTuple::new(py, [i64::MAX, 1_i64]).unwrap();
        let tail = PyTuple::new(py, [3_i64, 3_i64]).unwrap();
        let rows = PyList::new(py, [&first, &boundary, &tail]).unwrap();
        let source = rows.as_any().call_method0("__iter__").unwrap();
        let output = PyDict::new(py);

        let (returned, completed) = crate::pair::pair_i64_row_filter_to_dict_exact_prefix_v1(
            output.as_any(),
            &source,
            vec![(19, 0), (0, 0), (2, 0), (1, 0), (12, 0)],
            false,
        )
        .unwrap()
        .unwrap();

        assert!(!completed);
        assert!(returned.unwrap().bind(py).is(&boundary));
        assert_eq!(output.len(), 1);
        assert!(source.call_method0("__next__").unwrap().is(&tail));
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn pair_i64_row_filter_rejects_invalid_programs_without_consuming_input() {
    Python::initialize();
    Python::attach(|py| {
        let invalid_programs = [
            vec![],
            vec![(2, 0)],
            vec![(1, 1)],
            vec![(19, 0), (0, 0), (14, 0)],
            vec![(19, 0), (18, 0)],
            vec![(19, 0), (99, 0)],
        ];

        for instructions in invalid_programs {
            let row = PyTuple::new(py, [1_i64, 1_i64]).unwrap();
            let rows = PyList::new(py, [&row]).unwrap();
            let source = rows.as_any().call_method0("__iter__").unwrap();
            let output = PyDict::new(py);

            let result = crate::pair::pair_i64_row_filter_to_dict_exact_prefix_v1(
                output.as_any(),
                &source,
                instructions,
                false,
            )
            .unwrap();

            assert!(result.is_none());
            assert!(output.is_empty());
            assert!(source.call_method0("__next__").unwrap().is(&row));
        }
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn pair_i64_row_filter_rejects_inexact_endpoints_without_consuming_input() {
    Python::initialize();
    Python::attach(|py| {
        let row = PyTuple::new(py, [1_i64, 1_i64]).unwrap();
        let rows = PyList::new(py, [&row]).unwrap();
        let source = rows.as_any().call_method0("__iter__").unwrap();
        let invalid_output = PyList::empty(py);

        let result = crate::pair::pair_i64_row_filter_to_dict_exact_prefix_v1(
            invalid_output.as_any(),
            &source,
            vec![(19, 0), (0, 0), (8, 0)],
            false,
        )
        .unwrap();

        assert!(result.is_none());
        assert!(source.call_method0("__next__").unwrap().is(&row));

        let output = PyDict::new(py);
        let result = crate::pair::pair_i64_row_filter_to_dict_exact_prefix_v1(
            output.as_any(),
            rows.as_any(),
            vec![(19, 0), (0, 0), (8, 0)],
            false,
        )
        .unwrap();
        assert!(result.is_none());
        assert!(output.is_empty());
    });
}

#[cfg(Py_GIL_DISABLED)]
#[test]
fn pair_i64_row_filter_declines_without_consuming_on_free_threaded_python() {
    Python::initialize();
    Python::attach(|py| {
        let row = PyTuple::new(py, [1_i64, 1_i64]).unwrap();
        let rows = PyList::new(py, [&row]).unwrap();
        let source = rows.as_any().call_method0("__iter__").unwrap();
        let output = PyDict::new(py);

        let result = crate::pair::pair_i64_row_filter_to_dict_exact_prefix_v1(
            output.as_any(),
            &source,
            vec![(19, 0), (0, 0), (8, 0)],
            false,
        )
        .unwrap();

        assert!(result.is_none());
        assert!(output.is_empty());
        assert!(source.call_method0("__next__").unwrap().is(&row));
    });
}

fn fpstreams_error_type<'py>(py: Python<'py>, name: &str) -> Bound<'py, PyAny> {
    let package_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the Rust crate must live below the repository root")
        .join("src");
    PyModule::import(py, "sys")
        .unwrap()
        .getattr("path")
        .unwrap()
        .call_method1("insert", (0, package_root.to_str().unwrap()))
        .unwrap();
    PyModule::import(py, "fpstreams.errors")
        .unwrap()
        .getattr(name)
        .unwrap()
}

mod adapters;
mod group_exact;
mod group_fixed;
mod join_callable;
mod join_exact;
mod numeric;
mod numpy_group;
