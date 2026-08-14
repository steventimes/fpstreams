//! PyO3 module registration for fpstreams native kernels.

mod common;
mod float;
mod integer;

use crate::float::{
    aggregate_f64, aggregate_f64_range, count_f64, count_f64_range, execute_f64, execute_f64_range,
    statistics_f64, statistics_f64_range, terminal_f64, terminal_f64_range,
};
use crate::integer::{
    aggregate_i64, aggregate_i64_range, execute_i64, execute_i64_range, statistics_i64,
    statistics_i64_range, terminal_i64, terminal_i64_range,
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

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(version, module)?)?;
    module.add_function(wrap_pyfunction!(build_profile, module)?)?;
    module.add_function(wrap_pyfunction!(execute_i64, module)?)?;
    module.add_function(wrap_pyfunction!(execute_i64_range, module)?)?;
    module.add_function(wrap_pyfunction!(terminal_i64, module)?)?;
    module.add_function(wrap_pyfunction!(terminal_i64_range, module)?)?;
    module.add_function(wrap_pyfunction!(statistics_i64, module)?)?;
    module.add_function(wrap_pyfunction!(statistics_i64_range, module)?)?;
    module.add_function(wrap_pyfunction!(aggregate_i64, module)?)?;
    module.add_function(wrap_pyfunction!(aggregate_i64_range, module)?)?;
    module.add_function(wrap_pyfunction!(execute_f64, module)?)?;
    module.add_function(wrap_pyfunction!(execute_f64_range, module)?)?;
    module.add_function(wrap_pyfunction!(terminal_f64, module)?)?;
    module.add_function(wrap_pyfunction!(terminal_f64_range, module)?)?;
    module.add_function(wrap_pyfunction!(statistics_f64, module)?)?;
    module.add_function(wrap_pyfunction!(statistics_f64_range, module)?)?;
    module.add_function(wrap_pyfunction!(aggregate_f64, module)?)?;
    module.add_function(wrap_pyfunction!(aggregate_f64_range, module)?)?;
    module.add_function(wrap_pyfunction!(count_f64, module)?)?;
    module.add_function(wrap_pyfunction!(count_f64_range, module)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
