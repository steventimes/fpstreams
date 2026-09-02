//! PyO3 registration for GIL-detached native stage executors and fused terminal kernels.

mod common;
mod float;
mod integer;
mod numeric_mean;
mod numpy_export;
mod numpy_group;
mod pair;
mod pivot;
mod records;
mod relational;
mod relational_fixed;
mod scalar_sort;
mod scalar_unique;
mod select;
mod unnest;
mod unpivot;

use crate::common::{
    all_exact_dict_rows_v1, direct_dict_field_key_v1, exact_container_extraction_v1,
};
use crate::float::{
    aggregate_f64, aggregate_f64_buffer_masked_v1, aggregate_f64_buffer_masked_v2,
    aggregate_f64_masked, aggregate_f64_range, aggregate_f64_range_masked, count_f64,
    count_f64_range, execute_f64, execute_f64_buffer_v1, execute_f64_range, materialize_f64,
    materialize_f64_buffer_v1, materialize_f64_range, mean_f64, mean_f64_buffer_v1,
    mean_f64_buffer_v2, mean_f64_range, sequential_f64_aggregate_total_v1, statistics_f64,
    statistics_f64_range, terminal_f64, terminal_f64_buffer_v1, terminal_f64_buffer_v2,
    terminal_f64_probe, terminal_f64_range,
};
use crate::integer::{
    aggregate_i64, aggregate_i64_buffer_masked_v1, aggregate_i64_buffer_masked_v2,
    aggregate_i64_masked, aggregate_i64_range, aggregate_i64_range_masked, execute_i64,
    execute_i64_buffer_v1, execute_i64_range, frequencies_i64_exact_v1, materialize_i64,
    materialize_i64_buffer_v1, materialize_i64_range, mean_i64, mean_i64_buffer_v1,
    mean_i64_buffer_v2, mean_i64_range, statistics_i64, statistics_i64_range, terminal_i64,
    terminal_i64_probe, terminal_i64_range,
};
#[cfg(not(Py_GIL_DISABLED))]
use crate::integer::{materialize_i64_filter_exact_list_v1, materialize_i64_map_exact_list_v1};
use crate::numeric_mean::{
    mean_exact_iterator_chunk_v1, mean_exact_numbers_v1, update_mean_f64_buffer_v1,
    update_mean_i64_buffer_v1, update_sum_f64_buffer_v1,
};
use crate::numpy_export::pack_i64_exact_sequence_v1;
#[cfg(not(Py_GIL_DISABLED))]
use crate::numpy_group::numpy_group_strided_partial_v2;
use crate::numpy_group::{
    NumpyGroupPartial, NumpyGroupState, numpy_group_commit_v1, numpy_group_finalize_v1,
    numpy_group_partial_v1, numpy_group_state_v1,
};
use crate::pair::{
    pair_f64_value_filter_to_dict_exact_prefix_v1, pair_f64_value_map_to_dict_exact_prefix_v1,
    pair_i64_row_filter_to_dict_exact_prefix_v1, pair_i64_value_filter_to_dict_exact_prefix_v1,
    pair_i64_value_map_to_dict_exact_prefix_v1, pair_unique_exact_prefix_v1,
};
use crate::pivot::pivot_exact_dict_rows_v1;
use crate::records::records_from_exact_columns_v1;
use crate::relational::{
    RECORD_GROUP_SUM_MAX_FIELDS, RECORD_JOIN_V1_MAX_FIELDS, global_sum_i64_dict_rows_v1,
    group_sum_i64_dict_rows, group_sum_i64_dict_rows_v1, group_sum_i64_exact_pairs_v1,
    group_sum_i64_exact_pairs_v2, group_sum_i64_pair_expr_rows_v1, group_sum_i64_pairs,
    group_sum_i64_rows_v1, join_hashable_many_direct_records_v1, join_hashable_many_records_v1,
    join_hashable_many_records_v2, join_hashable_unique_direct_records_v1,
    join_hashable_unique_records_v1, join_hashable_unique_records_v2, join_i64_many_dict_rows_v1,
    join_i64_unique_dict_rows_v1, join_i64_unique_dict_rows_v2,
    standard_namedtuple_record_adapter_v1,
};
use crate::scalar_sort::sort_i64_exact_sequence_v1;
use crate::scalar_unique::{
    unique_i64_exact_prefix_cached_v1, unique_i64_exact_prefix_identity_cached_v1,
    unique_i64_exact_prefix_v1,
};
use crate::select::{
    drop_nulls_exact_dict_prefix_v1, filter_i64_expr_exact_dict_prefix_v1,
    select_exact_dict_prefix_v1,
};
use crate::unnest::unnest_exact_dict_prefix_v1;
use crate::unpivot::unpivot_exact_dict_prefix_v1;
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
    module.add_class::<NumpyGroupState>()?;
    module.add_class::<NumpyGroupPartial>()?;
    register_pyfunctions!(
        module,
        version,
        build_profile,
        all_exact_dict_rows_v1,
        direct_dict_field_key_v1,
        exact_container_extraction_v1,
        mean_exact_iterator_chunk_v1,
        mean_exact_numbers_v1,
        pack_i64_exact_sequence_v1,
        records_from_exact_columns_v1,
        update_mean_f64_buffer_v1,
        update_mean_i64_buffer_v1,
        update_sum_f64_buffer_v1,
        sort_i64_exact_sequence_v1,
        unique_i64_exact_prefix_cached_v1,
        unique_i64_exact_prefix_identity_cached_v1,
        unique_i64_exact_prefix_v1,
        numpy_group_state_v1,
        numpy_group_partial_v1,
        numpy_group_commit_v1,
        numpy_group_finalize_v1,
    );
    register_pyfunctions!(
        module,
        execute_i64,
        execute_i64_buffer_v1,
        execute_i64_range,
        frequencies_i64_exact_v1,
        materialize_i64,
        materialize_i64_buffer_v1,
        materialize_i64_range,
        terminal_i64,
        terminal_i64_probe,
        terminal_i64_range,
        mean_i64,
        mean_i64_range,
        statistics_i64,
        statistics_i64_range,
        aggregate_i64,
        aggregate_i64_range,
        aggregate_i64_masked,
        aggregate_i64_range_masked,
        aggregate_i64_buffer_masked_v1,
        aggregate_i64_buffer_masked_v2,
        mean_i64_buffer_v1,
        mean_i64_buffer_v2,
    );
    #[cfg(not(Py_GIL_DISABLED))]
    register_pyfunctions!(
        module,
        materialize_i64_filter_exact_list_v1,
        materialize_i64_map_exact_list_v1,
        numpy_group_strided_partial_v2,
    );
    register_pyfunctions!(
        module,
        execute_f64,
        execute_f64_buffer_v1,
        execute_f64_range,
        materialize_f64,
        materialize_f64_buffer_v1,
        materialize_f64_range,
        terminal_f64,
        terminal_f64_buffer_v1,
        terminal_f64_buffer_v2,
        terminal_f64_probe,
        terminal_f64_range,
        mean_f64,
        mean_f64_buffer_v1,
        mean_f64_buffer_v2,
        mean_f64_range,
        statistics_f64,
        statistics_f64_range,
        aggregate_f64,
        aggregate_f64_range,
        aggregate_f64_masked,
        aggregate_f64_buffer_masked_v1,
        aggregate_f64_buffer_masked_v2,
        aggregate_f64_range_masked,
        sequential_f64_aggregate_total_v1,
        count_f64,
        count_f64_range,
    );
    register_pyfunctions!(
        module,
        global_sum_i64_dict_rows_v1,
        group_sum_i64_exact_pairs_v1,
        group_sum_i64_exact_pairs_v2,
        group_sum_i64_pair_expr_rows_v1,
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
        join_i64_unique_dict_rows_v2,
        standard_namedtuple_record_adapter_v1,
    );
    register_pyfunctions!(
        module,
        drop_nulls_exact_dict_prefix_v1,
        filter_i64_expr_exact_dict_prefix_v1,
        select_exact_dict_prefix_v1,
        unnest_exact_dict_prefix_v1,
        unpivot_exact_dict_prefix_v1,
    );
    register_pyfunctions!(module, pivot_exact_dict_rows_v1,);
    register_pyfunctions!(module, pair_i64_row_filter_to_dict_exact_prefix_v1,);
    register_pyfunctions!(module, pair_unique_exact_prefix_v1,);
    register_pyfunctions!(
        module,
        pair_i64_value_filter_to_dict_exact_prefix_v1,
        pair_f64_value_filter_to_dict_exact_prefix_v1,
        pair_i64_value_map_to_dict_exact_prefix_v1,
        pair_f64_value_map_to_dict_exact_prefix_v1,
    );
    module.add("record_join_v1_max_fields", RECORD_JOIN_V1_MAX_FIELDS)?;
    relational_fixed::register(module)?;
    Ok(())
}

#[cfg(test)]
mod tests;
