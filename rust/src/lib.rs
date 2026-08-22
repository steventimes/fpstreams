//! PyO3 registration for GIL-detached native stage executors and fused terminal kernels.

mod common;
mod float;
mod integer;
mod relational;
mod relational_fixed;

use crate::common::{
    all_exact_dict_rows_v1, direct_dict_field_key_v1, exact_container_extraction_v1,
};
use crate::float::{
    aggregate_f64, aggregate_f64_masked, aggregate_f64_range, aggregate_f64_range_masked,
    count_f64, count_f64_range, execute_f64, execute_f64_range, materialize_f64,
    materialize_f64_range, statistics_f64, statistics_f64_range, terminal_f64, terminal_f64_probe,
    terminal_f64_range,
};
use crate::integer::{
    aggregate_i64, aggregate_i64_masked, aggregate_i64_range, aggregate_i64_range_masked,
    execute_i64, execute_i64_range, materialize_i64, materialize_i64_range, statistics_i64,
    statistics_i64_range, terminal_i64, terminal_i64_probe, terminal_i64_range,
};
use crate::relational::{
    RECORD_GROUP_SUM_MAX_FIELDS, RECORD_JOIN_V1_MAX_FIELDS,
    group_count_sum_callable_key_dict_rows_v1, group_count_sum_callable_value_dict_rows_v1,
    group_sum_i64_dict_rows, group_sum_i64_dict_rows_v1, group_sum_i64_exact_pairs_v1,
    group_sum_i64_pairs, group_sum_i64_rows_v1, join_hashable_many_direct_records_v1,
    join_hashable_many_records_v1, join_hashable_many_records_v2,
    join_hashable_unique_direct_records_v1, join_hashable_unique_records_v1,
    join_hashable_unique_records_v2, join_i64_many_dict_rows_v1, join_i64_unique_dict_rows_v1,
    standard_namedtuple_record_adapter_v1,
};
use pyo3::prelude::*;

#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pyfunction]
fn build_profile() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

macro_rules! register_pyfunctions {
    ($module:ident, $($function:ident),+ $(,)?) => {
        $(
            $module.add_function(wrap_pyfunction!($function, $module)?)?;
        )+
    };
}

#[pymodule(gil_used = false)]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_pyfunctions!(
        module,
        version,
        build_profile,
        all_exact_dict_rows_v1,
        direct_dict_field_key_v1,
        exact_container_extraction_v1,
    );
    register_pyfunctions!(
        module,
        execute_i64,
        execute_i64_range,
        materialize_i64,
        materialize_i64_range,
        terminal_i64,
        terminal_i64_probe,
        terminal_i64_range,
        statistics_i64,
        statistics_i64_range,
        aggregate_i64,
        aggregate_i64_range,
        aggregate_i64_masked,
        aggregate_i64_range_masked,
    );
    register_pyfunctions!(
        module,
        execute_f64,
        execute_f64_range,
        materialize_f64,
        materialize_f64_range,
        terminal_f64,
        terminal_f64_probe,
        terminal_f64_range,
        statistics_f64,
        statistics_f64_range,
        aggregate_f64,
        aggregate_f64_range,
        aggregate_f64_masked,
        aggregate_f64_range_masked,
        count_f64,
        count_f64_range,
    );
    register_pyfunctions!(
        module,
        group_count_sum_callable_key_dict_rows_v1,
        group_count_sum_callable_value_dict_rows_v1,
        group_sum_i64_exact_pairs_v1,
        group_sum_i64_pairs,
        group_sum_i64_rows_v1,
        group_sum_i64_dict_rows,
        group_sum_i64_dict_rows_v1,
    );
    module.add("record_group_sum_max_fields", RECORD_GROUP_SUM_MAX_FIELDS)?;
    register_pyfunctions!(
        module,
        join_hashable_many_direct_records_v1,
        join_hashable_many_records_v1,
        join_hashable_many_records_v2,
        join_hashable_unique_direct_records_v1,
        join_hashable_unique_records_v1,
        join_hashable_unique_records_v2,
        join_i64_many_dict_rows_v1,
        join_i64_unique_dict_rows_v1,
        standard_namedtuple_record_adapter_v1,
    );
    module.add("record_join_v1_max_fields", RECORD_JOIN_V1_MAX_FIELDS)?;
    relational_fixed::register(module)?;
    Ok(())
}

#[cfg(test)]
mod tests;
