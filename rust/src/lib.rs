use pyo3::prelude::*;
use pyo3::types::PyModule;

mod list_ops;

use list_ops::{
    batch_list, distinct_list, limit_list, max_list, min_list, skip_list, sorted_list, sum_list,
    window_list,
};

#[pymodule]
fn fpstreams_rust(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(distinct_list, m)?)?;
    m.add_function(wrap_pyfunction!(batch_list, m)?)?;
    m.add_function(wrap_pyfunction!(limit_list, m)?)?;
    m.add_function(wrap_pyfunction!(skip_list, m)?)?;
    m.add_function(wrap_pyfunction!(window_list, m)?)?;
    m.add_function(wrap_pyfunction!(sorted_list, m)?)?;
    m.add_function(wrap_pyfunction!(min_list, m)?)?;
    m.add_function(wrap_pyfunction!(max_list, m)?)?;
    m.add_function(wrap_pyfunction!(sum_list, m)?)?;
    Ok(())
}
