//! Parity tests for fused native stages, streaming terminals, short-circuiting, and aggregation.

use crate::common::{
    AGGREGATE_COUNT, AGGREGATE_LAST, AGGREGATE_M2, AGGREGATE_MEAN, AGGREGATE_MINIMUM,
    AGGREGATE_TOTAL, all_exact_dict_rows_v1, materialize_target, materialize_values,
};
use crate::float::{
    run_f64, run_f64_aggregate_masked, run_f64_count, run_f64_statistics, run_f64_terminal,
};
use crate::integer::{
    PreparedExpression, evaluate, floor_div, modulo, prepare_expression, run_i64_aggregate,
    run_i64_aggregate_masked, run_i64_statistics, run_terminal, run_values,
};
#[cfg(not(Py_GIL_DISABLED))]
use crate::relational::standard_namedtuple_record_adapter_v1;
use crate::relational::{
    CallableGroupHashProbeCounts, CallableRightSchemaProbeCounts,
    begin_callable_group_hash_probe_count, begin_callable_right_schema_probe_count,
    checked_join_output_size, end_callable_group_hash_probe_count,
    end_callable_right_schema_probe_count, group_count_sum_callable_key_dict_rows_v1,
    group_count_sum_callable_value_dict_rows_v1, group_sum_i64_dict_rows,
    group_sum_i64_dict_rows_v1, group_sum_i64_exact_pairs_v1, group_sum_i64_pairs,
    group_sum_i64_rows_v1, group_sum_pairs, join_hashable_many_direct_records_v1,
    join_hashable_many_records_v1, join_hashable_unique_records_v1,
    join_hashable_unique_records_v2, join_i64_many_dict_rows_v1, join_i64_unique_dict_rows_v1,
};
use crate::relational_fixed::{group_fixed_i64_dict_rows_v1, group_fixed_i64_rows_v1};
use pyo3::types::{
    PyAnyMethods, PyDict, PyDictMethods, PyFrozenSet, PyList, PyListMethods, PyModule, PyString,
    PyTuple, PyTupleMethods,
};
use pyo3::{Bound, Py, PyAny, Python};
#[cfg(not(Py_GIL_DISABLED))]
use std::sync::Mutex;

#[cfg(not(Py_GIL_DISABLED))]
static STANDARD_NAMEDTUPLE_TEST_LOCK: Mutex<()> = Mutex::new(());

mod adapters;
mod group_callable;
mod group_exact;
mod group_fixed;
mod join_callable;
mod join_exact;
mod numeric;
