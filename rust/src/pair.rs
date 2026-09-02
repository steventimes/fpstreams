//! Exact-pair expression and lossless prefix kernels.

mod expr;
mod prefix;
mod row_filter;
mod unique;
mod value_filter;
mod value_map;

#[cfg(not(Py_GIL_DISABLED))]
pub(crate) use expr::{
    PAIR_KEY_OPCODE, PreparedPairExpression, prepare_pair_arithmetic_expression,
};
pub(crate) use row_filter::pair_i64_row_filter_to_dict_exact_prefix_v1;
pub(crate) use unique::pair_unique_exact_prefix_v1;
pub(crate) use value_filter::{
    pair_f64_value_filter_to_dict_exact_prefix_v1, pair_i64_value_filter_to_dict_exact_prefix_v1,
};
pub(crate) use value_map::{
    pair_f64_value_map_to_dict_exact_prefix_v1, pair_i64_value_map_to_dict_exact_prefix_v1,
};
