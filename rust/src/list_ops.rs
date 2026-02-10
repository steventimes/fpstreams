use std::collections::HashMap;

use pyo3::basic::CompareOp;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyLong, PySequence};

#[pyfunction]
pub fn distinct_list(py: Python<'_>, seq: &PyAny) -> PyResult<PyObject> {
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
pub fn batch_list(py: Python<'_>, seq: &PyAny, size: usize) -> PyResult<PyObject> {
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

#[pyfunction]
pub fn limit_list(py: Python<'_>, seq: &PyAny, max_size: usize) -> PyResult<PyObject> {
    let seq = PySequence::fast(seq, "expected a sequence")?;
    let mut output: Vec<PyObject> = Vec::with_capacity(max_size.min(seq.len()? as usize));
    let mut count = 0usize;

    for item in seq.iter()? {
        if count >= max_size {
            break;
        }
        output.push(item?.to_object(py));
        count += 1;
    }

    Ok(PyList::new(py, output).to_object(py))
}

#[pyfunction]
pub fn skip_list(py: Python<'_>, seq: &PyAny, skip: usize) -> PyResult<PyObject> {
    let seq = PySequence::fast(seq, "expected a sequence")?;
    let mut output: Vec<PyObject> = Vec::new();
    let mut index = 0usize;

    for item in seq.iter()? {
        if index >= skip {
            output.push(item?.to_object(py));
        }
        index += 1;
    }

    Ok(PyList::new(py, output).to_object(py))
}

#[pyfunction]
pub fn window_list(py: Python<'_>, seq: &PyAny, size: usize, step: usize) -> PyResult<PyObject> {
    let seq = PySequence::fast(seq, "expected a sequence")?;
    let mut values: Vec<PyObject> = Vec::with_capacity(seq.len()? as usize);

    for item in seq.iter()? {
        values.push(item?.to_object(py));
    }

    if size == 0 || values.is_empty() {
        return Ok(PyList::empty(py).to_object(py));
    }

    let step = if step == 0 { 1 } else { step };
    let mut windows: Vec<PyObject> = Vec::new();
    let mut index = 0usize;

    while index + size <= values.len() {
        let slice = &values[index..index + size];
        windows.push(PyList::new(py, slice).to_object(py));
        index += step;
    }

    Ok(PyList::new(py, windows).to_object(py))
}

#[pyfunction]
pub fn sorted_list(py: Python<'_>, seq: &PyAny, reverse: bool) -> PyResult<PyObject> {
    let seq = PySequence::fast(seq, "expected a sequence")?;
    let mut values: Vec<PyObject> = Vec::with_capacity(seq.len()? as usize);

    for item in seq.iter()? {
        values.push(item?.to_object(py));
    }

    let list = PyList::new(py, values);
    if reverse {
        let kwargs = PyDict::new(py);
        kwargs.set_item("reverse", true)?;
        list.call_method("sort", (), Some(kwargs))?;
    } else {
        list.call_method0("sort")?;
    }
    Ok(list.to_object(py))
}

#[pyfunction]
pub fn min_list(py: Python<'_>, seq: &PyAny) -> PyResult<PyObject> {
    let seq = PySequence::fast(seq, "expected a sequence")?;
    let mut iter = seq.iter()?;
    let first = match iter.next() {
        Some(item) => item?,
        None => return Ok(py.None()),
    };
    let mut best = first.to_object(py);

    for item in iter {
        let item = item?;
        if item
            .rich_compare(best.as_ref(py), CompareOp::Lt)?
            .is_true()?
        {
            best = item.to_object(py);
        }
    }

    Ok(best)
}

#[pyfunction]
pub fn max_list(py: Python<'_>, seq: &PyAny) -> PyResult<PyObject> {
    let seq = PySequence::fast(seq, "expected a sequence")?;
    let mut iter = seq.iter()?;
    let first = match iter.next() {
        Some(item) => item?,
        None => return Ok(py.None()),
    };
    let mut best = first.to_object(py);

    for item in iter {
        let item = item?;
        if item
            .rich_compare(best.as_ref(py), CompareOp::Gt)?
            .is_true()?
        {
            best = item.to_object(py);
        }
    }

    Ok(best)
}

#[pyfunction]
pub fn sum_list(py: Python<'_>, seq: &PyAny) -> PyResult<PyObject> {
    let seq = PySequence::fast(seq, "expected a sequence")?;
    let mut acc = PyLong::new(py, 0).to_object(py);

    for item in seq.iter()? {
        let item = item?;
        acc = acc.as_ref(py).call_method1("__add__", (item,))?.to_object(py);
    }

    Ok(acc)
}
