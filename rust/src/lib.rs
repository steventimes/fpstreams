use std::collections::HashMap;

use pyo3::basic::CompareOp;
use pyo3::prelude::*;
use pyo3::types::{PyList, PySequence};

#[pyfunction]
fn distinct_list(py: Python<'_>, seq: &PyAny) -> PyResult<PyObject> {
    let seq = PySequence::fast(seq, "expected a sequence")?;
    let mut buckets: HashMap<isize, Vec<PyObject>> = HashMap::new();
    let mut output: Vec<PyObject> = Vec::with_capacity(seq.len()? as usize);

    for item in seq.iter()? {
        let item = item?;
        let hash = item.hash()?;
        let bucket = buckets.entry(hash).or_insert_with(Vec::new);

        let mut is_dup = false;
        for existing in bucket.iter() {
            if item
                .rich_compare(existing.as_ref(py), CompareOp::Eq)?
                .is_true()?
            {
                is_dup = true;
                break;
            }
        }

        if !is_dup {
            let obj = item.to_object(py);
            bucket.push(obj.clone());
            output.push(obj);
        }
    }

    Ok(PyList::new(py, output).to_object(py))
}

#[pyfunction]
fn batch_list(py: Python<'_>, seq: &PyAny, size: usize) -> PyResult<PyObject> {
    let seq = PySequence::fast(seq, "expected a sequence")?;
    let mut batches: Vec<PyObject> = Vec::new();
    let mut current: Vec<PyObject> = Vec::with_capacity(size.max(1));

    for item in seq.iter()? {
        current.push(item?.to_object(py));
        if current.len() >= size {
            batches.push(PyList::new(py, &current).to_object(py));
            current.clear();
        }
    }

    if !current.is_empty() {
        batches.push(PyList::new(py, &current).to_object(py));
    }

    Ok(PyList::new(py, batches).to_object(py))
}

#[pymodule]
fn fpstreams_rust(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(distinct_list, m)?)?;
    m.add_function(wrap_pyfunction!(batch_list, m)?)?;
    Ok(())
}
