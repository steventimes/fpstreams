//! One-pass first-key dictionary collection with a lossless Python fallback boundary.

use super::prefix::PairPrefix;
#[cfg(not(Py_GIL_DISABLED))]
use super::prefix::{exact_pair_key_is_supported, exact_pair_parts};
#[cfg(not(Py_GIL_DISABLED))]
use crate::common::is_exact_sequence_iterator;
use pyo3::prelude::*;
#[cfg(not(Py_GIL_DISABLED))]
use pyo3::{
    ffi,
    types::{PyDict, PyDictMethods},
};

/// Collect the compatible prefix of exact pairs into an existing first-wins dictionary.
///
/// A non-exact pair or non-exact int/string key is returned untouched. Python can seed its
/// canonical uniqueness state from `output` and resume at that boundary without hashing the
/// incompatible key inside this endpoint.
#[pyfunction]
pub(crate) fn pair_unique_exact_prefix_v1(
    output: &Bound<'_, PyAny>,
    source: &Bound<'_, PyAny>,
) -> PyResult<Option<PairPrefix>> {
    #[cfg(Py_GIL_DISABLED)]
    {
        let _ = (output, source);
        Ok(None)
    }

    #[cfg(not(Py_GIL_DISABLED))]
    {
        let Ok(output) = output.cast_exact::<PyDict>() else {
            return Ok(None);
        };
        if !is_exact_sequence_iterator(output.py(), source)? {
            return Ok(None);
        }
        let py = output.py();
        let mut previous: Option<Py<PyAny>> = None;
        let mut rows_since_signal_check = 0_u16;

        loop {
            if rows_since_signal_check == 0 {
                py.check_signals()?;
            }
            // Keep the previous source row alive until the next pull, matching a Python for-loop.
            let row = unsafe { ffi::PyIter_Next(source.as_ptr()) };
            if row.is_null() {
                if unsafe { !ffi::PyErr_Occurred().is_null() } {
                    return Err(PyErr::fetch(py));
                }
                drop(previous);
                return Ok(Some((None, true)));
            }
            // SAFETY: PyIter_Next returned one owned reference.
            let row = unsafe { Bound::from_owned_ptr(py, row) };
            drop(previous.take());

            let Some((key, value)) = exact_pair_parts(py, &row)? else {
                return Ok(Some((Some(row.unbind()), false)));
            };
            // Only exact builtins can reach dictionary hashing. This returns the boundary before
            // a custom __hash__ or __eq__ implementation can run.
            if !exact_pair_key_is_supported(key) {
                return Ok(Some((Some(row.unbind()), false)));
            }
            output.set_default(key, value)?;
            previous = Some(row.unbind());
            rows_since_signal_check = rows_since_signal_check.wrapping_add(1) & 4095;
        }
    }
}

#[cfg(all(test, not(Py_GIL_DISABLED)))]
mod tests {
    use super::*;
    use pyo3::types::{PyDict, PyList, PyString, PyTuple};

    #[test]
    fn exact_pairs_keep_the_first_value_and_key_identity() {
        Python::initialize();
        Python::attach(|py| {
            let first_key = PyString::new(py, "key");
            let equal_key = PyString::new(py, "key");
            let first = PyTuple::new(
                py,
                [
                    first_key.as_any(),
                    1_i32.into_pyobject(py).unwrap().as_any(),
                ],
            )
            .unwrap();
            let duplicate = PyTuple::new(
                py,
                [
                    equal_key.as_any(),
                    2_i32.into_pyobject(py).unwrap().as_any(),
                ],
            )
            .unwrap();
            let integer = PyTuple::new(
                py,
                [
                    3_i32.into_pyobject(py).unwrap().as_any(),
                    4_i32.into_pyobject(py).unwrap().as_any(),
                ],
            )
            .unwrap();
            let rows = PyList::new(py, [&first, &duplicate, &integer]).unwrap();
            let source = rows.as_any().call_method0("__iter__").unwrap();
            let output = PyDict::new(py);

            let result = pair_unique_exact_prefix_v1(output.as_any(), &source)
                .unwrap()
                .unwrap();

            assert!(result.1);
            assert!(result.0.is_none());
            assert_eq!(output.len(), 2);
            assert_eq!(
                output
                    .get_item("key")
                    .unwrap()
                    .unwrap()
                    .extract::<i32>()
                    .unwrap(),
                1
            );
            assert!(output.keys().get_item(0).unwrap().is(&first_key));
        });
    }

    #[test]
    fn incompatible_boundary_is_returned_before_custom_hashing() {
        Python::initialize();
        Python::attach(|py| {
            let fixture = PyModule::from_code(
                py,
                c"class Key:\n    calls = 0\n    def __hash__(self):\n        type(self).calls += 1\n        return 1\n",
                c"pair_unique.py",
                c"pair_unique",
            )
            .unwrap();
            let key_type = fixture.getattr("Key").unwrap();
            let custom_key = key_type.call0().unwrap();
            let first = PyTuple::new(py, [1, 10]).unwrap();
            let boundary_value = 20_i32.into_pyobject(py).unwrap();
            let boundary =
                PyTuple::new(py, [custom_key.as_any(), boundary_value.as_any()]).unwrap();
            let tail = PyTuple::new(py, [3, 30]).unwrap();
            let rows = PyList::new(py, [&first, &boundary, &tail]).unwrap();
            let source = rows.as_any().call_method0("__iter__").unwrap();
            let output = PyDict::new(py);

            let (returned, completed) = pair_unique_exact_prefix_v1(output.as_any(), &source)
                .unwrap()
                .unwrap();

            assert!(!completed);
            assert!(returned.unwrap().bind(py).is(&boundary));
            assert!(source.call_method0("__next__").unwrap().is(&tail));
            assert_eq!(
                key_type
                    .getattr("calls")
                    .unwrap()
                    .extract::<usize>()
                    .unwrap(),
                0
            );
            assert_eq!(
                output
                    .get_item(1)
                    .unwrap()
                    .unwrap()
                    .extract::<i32>()
                    .unwrap(),
                10
            );
        });
    }

    #[test]
    fn invalid_output_or_iterator_declines_without_consuming() {
        Python::initialize();
        Python::attach(|py| {
            let rows = PyList::new(py, [PyTuple::new(py, [1, 2]).unwrap()]).unwrap();
            let source = rows.as_any().call_method0("__iter__").unwrap();
            assert!(
                pair_unique_exact_prefix_v1(PyList::empty(py).as_any(), &source)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                source
                    .call_method0("__next__")
                    .unwrap()
                    .extract::<(i32, i32)>()
                    .unwrap(),
                (1, 2)
            );

            let output = PyDict::new(py);
            assert!(
                pair_unique_exact_prefix_v1(output.as_any(), rows.as_any())
                    .unwrap()
                    .is_none()
            );
            assert!(output.is_empty());
        });
    }
}
