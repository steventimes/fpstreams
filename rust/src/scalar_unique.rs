//! One-pass stable integer uniqueness with a lossless Python fallback boundary.

#[cfg(not(Py_GIL_DISABLED))]
use crate::common::is_exact_sequence_iterator;
#[cfg(not(Py_GIL_DISABLED))]
use crate::relational::SeededI64BuildHasher;
use pyo3::prelude::*;
#[cfg(not(Py_GIL_DISABLED))]
use pyo3::{
    exceptions::{PyMemoryError, PyOverflowError},
    ffi,
    types::PyList,
};
#[cfg(not(Py_GIL_DISABLED))]
use std::collections::HashSet;

type UniquePrefix = (Option<Py<PyAny>>, bool);

#[cfg(not(Py_GIL_DISABLED))]
const RECENT_INTEGER_SLOTS: usize = 256;

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn recent_integer_slot(integer: i64) -> usize {
    const GOLDEN_RATIO: u64 = 0x9e37_79b9_7f4a_7c15;
    ((integer as u64).wrapping_mul(GOLDEN_RATIO) >> 56) as usize
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn recent_object_slot(value: *mut ffi::PyObject) -> usize {
    const GOLDEN_RATIO: u64 = 0x9e37_79b9_7f4a_7c15;
    ((((value as usize >> 4) as u64).wrapping_mul(GOLDEN_RATIO) >> 56) as usize)
        & (RECENT_INTEGER_SLOTS - 1)
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn exact_i64(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Option<i64>> {
    if unsafe { ffi::PyLong_CheckExact(value.as_ptr()) } == 0 {
        return Ok(None);
    }
    // SAFETY: value is a live exact Python integer. Overflow is an incompatibility boundary,
    // not a native execution error; the Python continuation handles arbitrary-size integers.
    let extracted = unsafe { ffi::PyLong_AsLongLong(value.as_ptr()) };
    if extracted == -1 && unsafe { !ffi::PyErr_Occurred().is_null() } {
        let error = PyErr::fetch(py);
        if error.is_instance_of::<PyOverflowError>(py) {
            return Ok(None);
        }
        return Err(error);
    }
    Ok(Some(extracted))
}

#[cfg(not(Py_GIL_DISABLED))]
#[cold]
#[inline(never)]
fn exact_boolean(value: &Bound<'_, PyAny>) -> Option<i64> {
    // SAFETY: bool has exactly the two immortal singleton instances. Normalizing them to 0/1
    // preserves Python's cross-type int equality and first identity.
    if unsafe { ffi::PyBool_Check(value.as_ptr()) } == 0 {
        return None;
    }
    Some(i64::from(value.as_ptr() == unsafe { ffi::Py_True() }))
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn insert_unique_integer(
    seen: &mut HashSet<i64, SeededI64BuildHasher>,
    spare_capacity: &mut usize,
    integer: i64,
) -> PyResult<bool> {
    // Track spare slots locally so ordinary inserts do not need two HashSet metadata reads.
    // Only probe a full table: duplicate keys at an expansion boundary must not spuriously
    // allocate or raise MemoryError.
    if *spare_capacity == 0 {
        if seen.contains(&integer) {
            return Ok(false);
        }
        seen.try_reserve(1)
            .map_err(|_| PyMemoryError::new_err("native unique allocation failed"))?;
        *spare_capacity = seen.capacity() - seen.len();
    }
    let inserted = seen.insert(integer);
    if inserted {
        *spare_capacity -= 1;
    }
    Ok(inserted)
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn unique_i64_exact_prefix_impl<const CACHE_IDENTITIES: bool, F>(
    output: &Bound<'_, PyAny>,
    source: &Bound<'_, PyAny>,
    recent_objects: &mut [Option<Py<PyAny>>],
    mut insert: F,
) -> PyResult<Option<UniquePrefix>>
where
    F: FnMut(i64) -> PyResult<bool>,
{
    let Ok(output) = output.cast_exact::<PyList>() else {
        return Ok(None);
    };
    if !output.is_empty() || !is_exact_sequence_iterator(output.py(), source)? {
        return Ok(None);
    }

    let py = output.py();
    let mut previous: Option<Py<PyAny>> = None;
    let mut rows_since_signal_check = 0_u16;

    loop {
        if rows_since_signal_check == 0 {
            py.check_signals()?;
        }
        // Keep the previous source value alive until the next pull, matching Python's
        // for-loop assignment lifetime even when a signal handler mutates the source.
        let value = unsafe { ffi::PyIter_Next(source.as_ptr()) };
        if value.is_null() {
            if unsafe { !ffi::PyErr_Occurred().is_null() } {
                return Err(PyErr::fetch(py));
            }
            drop(previous);
            return Ok(Some((None, true)));
        }
        // SAFETY: PyIter_Next returned one owned reference.
        let value = unsafe { Bound::from_owned_ptr(py, value) };
        drop(previous.take());

        let object_slot = if CACHE_IDENTITIES {
            recent_object_slot(value.as_ptr())
        } else {
            0
        };
        if CACHE_IDENTITIES
            && recent_objects[object_slot]
                .as_ref()
                .is_some_and(|cached| cached.as_ptr() == value.as_ptr())
        {
            previous = Some(value.unbind());
            rows_since_signal_check = rows_since_signal_check.wrapping_add(1) & 4095;
            continue;
        }

        let integer = match exact_i64(py, &value)? {
            Some(integer) => integer,
            None => match exact_boolean(&value) {
                Some(integer) => integer,
                None => return Ok(Some((Some(value.unbind()), false))),
            },
        };
        if insert(integer)? {
            output.append(&value)?;
            if CACHE_IDENTITIES {
                // The fixed-size cache owns each referent independently. A direct ABI caller may
                // expose and clear ``output`` from a signal handler while this scan is in flight.
                recent_objects[object_slot] = Some(value.clone().unbind());
            }
        }
        previous = Some(value.unbind());
        rows_since_signal_check = rows_since_signal_check.wrapping_add(1) & 4095;
    }
}

/// Append the unique exact-integer prefix of an exact sequence iterator to ``output``.
///
/// Machine-size values use the native hash table. Booleans share the i64 keys 0 and 1, matching
/// Python numeric equality. The first other or arbitrary-precision value is returned untouched so
/// the Python caller can resume canonical mixed-type semantics.
#[pyfunction]
pub(crate) fn unique_i64_exact_prefix_v1(
    output: &Bound<'_, PyAny>,
    source: &Bound<'_, PyAny>,
) -> PyResult<Option<UniquePrefix>> {
    #[cfg(Py_GIL_DISABLED)]
    {
        let _ = (output, source);
        Ok(None)
    }

    #[cfg(not(Py_GIL_DISABLED))]
    {
        let mut seen = HashSet::with_hasher(SeededI64BuildHasher::random());
        let mut spare_capacity = 0_usize;
        unique_i64_exact_prefix_impl::<false, _>(output, source, &mut [], |integer| {
            insert_unique_integer(&mut seen, &mut spare_capacity, integer)
        })
    }
}

/// Deduplicate a sampled low-cardinality exact-integer prefix with a tiny direct cache.
#[pyfunction]
pub(crate) fn unique_i64_exact_prefix_cached_v1(
    output: &Bound<'_, PyAny>,
    source: &Bound<'_, PyAny>,
) -> PyResult<Option<UniquePrefix>> {
    #[cfg(Py_GIL_DISABLED)]
    {
        let _ = (output, source);
        Ok(None)
    }

    #[cfg(not(Py_GIL_DISABLED))]
    {
        let mut seen = HashSet::with_hasher(SeededI64BuildHasher::random());
        let mut spare_capacity = 0_usize;
        let mut recent_integers = [None; RECENT_INTEGER_SLOTS];
        unique_i64_exact_prefix_impl::<false, _>(output, source, &mut [], |integer| {
            let slot = recent_integer_slot(integer);
            if recent_integers[slot] == Some(integer) {
                return Ok(false);
            }
            let inserted = insert_unique_integer(&mut seen, &mut spare_capacity, integer)?;
            recent_integers[slot] = Some(integer);
            Ok(inserted)
        })
    }
}

/// Deduplicate low-cardinality exact integers whose repeated values reuse object identities.
#[pyfunction]
pub(crate) fn unique_i64_exact_prefix_identity_cached_v1(
    output: &Bound<'_, PyAny>,
    source: &Bound<'_, PyAny>,
) -> PyResult<Option<UniquePrefix>> {
    #[cfg(Py_GIL_DISABLED)]
    {
        let _ = (output, source);
        Ok(None)
    }

    #[cfg(not(Py_GIL_DISABLED))]
    {
        let mut seen = HashSet::with_hasher(SeededI64BuildHasher::random());
        let mut spare_capacity = 0_usize;
        let mut recent_integers = [None; RECENT_INTEGER_SLOTS];
        let mut recent_objects: [Option<Py<PyAny>>; RECENT_INTEGER_SLOTS] =
            std::array::from_fn(|_| None);
        unique_i64_exact_prefix_impl::<true, _>(output, source, &mut recent_objects, |integer| {
            let slot = recent_integer_slot(integer);
            if recent_integers[slot] == Some(integer) {
                return Ok(false);
            }
            let inserted = insert_unique_integer(&mut seen, &mut spare_capacity, integer)?;
            recent_integers[slot] = Some(integer);
            Ok(inserted)
        })
    }
}

#[cfg(all(test, not(Py_GIL_DISABLED)))]
mod tests {
    use super::*;
    use pyo3::types::{PyInt, PyList, PyString};

    #[test]
    fn exact_integer_prefix_keeps_first_identity_and_returns_boundary() {
        Python::initialize();
        Python::attach(|py| {
            let first = PyInt::new(py, 1_000_i64);
            let equal = PyInt::new(py, 1_000_i64);
            let later = PyInt::new(py, -7_i64);
            let boundary = PyString::new(py, "python");
            let values = PyList::new(
                py,
                [
                    first.as_any(),
                    equal.as_any(),
                    later.as_any(),
                    boundary.as_any(),
                ],
            )
            .unwrap();
            let source = values.as_any().try_iter().unwrap();
            let output = PyList::empty(py);

            let (incompatible, completed) =
                unique_i64_exact_prefix_v1(output.as_any(), source.as_any())
                    .unwrap()
                    .unwrap();

            assert!(!completed);
            assert!(incompatible.unwrap().bind(py).is(&boundary));
            assert_eq!(output.len(), 2);
            assert!(output.get_item(0).unwrap().is(&first));
            assert!(output.get_item(1).unwrap().is(&later));
        });
    }

    #[test]
    fn arbitrary_precision_integer_is_returned_as_the_python_boundary() {
        Python::initialize();
        Python::attach(|py| {
            let first = PyInt::new(py, 1_000_i64);
            let boundary = py.eval(c"1 << 100", None, None).unwrap();
            let tail = PyInt::new(py, -7_i64);
            let values = PyList::new(py, [first.as_any(), &boundary, tail.as_any()]).unwrap();
            let source = values.as_any().try_iter().unwrap();
            let output = PyList::empty(py);

            let (incompatible, completed) =
                unique_i64_exact_prefix_v1(output.as_any(), source.as_any())
                    .unwrap()
                    .unwrap();

            assert!(!completed);
            assert!(incompatible.unwrap().bind(py).is(&boundary));
            assert_eq!(output.len(), 1);
            assert!(output.get_item(0).unwrap().is(&first));
            assert!(source.call_method0("__next__").unwrap().is(&tail));
        });
    }

    #[test]
    fn booleans_share_integer_keys_without_replacing_first_objects() {
        Python::initialize();
        Python::attach(|py| {
            let one = PyInt::new(py, 1_i64);
            let zero = PyInt::new(py, 0_i64);
            let true_value = py.eval(c"True", None, None).unwrap();
            let false_value = py.eval(c"False", None, None).unwrap();
            let values =
                PyList::new(py, [&true_value, one.as_any(), &false_value, zero.as_any()]).unwrap();
            let source = values.as_any().try_iter().unwrap();
            let output = PyList::empty(py);

            let (incompatible, completed) =
                unique_i64_exact_prefix_v1(output.as_any(), source.as_any())
                    .unwrap()
                    .unwrap();

            assert!(completed);
            assert!(incompatible.is_none());
            assert_eq!(output.len(), 2);
            assert!(output.get_item(0).unwrap().is(&true_value));
            assert!(output.get_item(1).unwrap().is(&false_value));
        });
    }

    #[test]
    fn cached_prefix_keeps_first_objects_and_returns_the_mixed_boundary() {
        Python::initialize();
        Python::attach(|py| {
            let first = PyInt::new(py, 1_000_i64);
            let equal = PyInt::new(py, 1_000_i64);
            let true_value = py.eval(c"True", None, None).unwrap();
            let one = PyInt::new(py, 1_i64);
            let boundary = PyString::new(py, "python");
            let tail = PyInt::new(py, -7_i64);
            let values = PyList::new(
                py,
                [
                    first.as_any(),
                    equal.as_any(),
                    &true_value,
                    one.as_any(),
                    boundary.as_any(),
                    tail.as_any(),
                ],
            )
            .unwrap();
            let source = values.as_any().try_iter().unwrap();
            let output = PyList::empty(py);

            let (incompatible, completed) =
                unique_i64_exact_prefix_cached_v1(output.as_any(), source.as_any())
                    .unwrap()
                    .unwrap();

            assert!(!completed);
            assert!(incompatible.unwrap().bind(py).is(&boundary));
            assert_eq!(output.len(), 2);
            assert!(output.get_item(0).unwrap().is(&first));
            assert!(output.get_item(1).unwrap().is(&true_value));
            assert!(source.call_method0("__next__").unwrap().is(&tail));
        });
    }

    #[test]
    fn identity_cached_prefix_keeps_owned_first_objects_and_exact_numeric_equality() {
        Python::initialize();
        Python::attach(|py| {
            let first = PyInt::new(py, 1_000_i64);
            let equal = PyInt::new(py, 1_000_i64);
            let true_value = py.eval(c"True", None, None).unwrap();
            let one = PyInt::new(py, 1_i64);
            let boundary = PyString::new(py, "python");
            let tail = PyInt::new(py, -7_i64);
            let values = PyList::new(
                py,
                [
                    first.as_any(),
                    first.as_any(),
                    equal.as_any(),
                    &true_value,
                    &true_value,
                    one.as_any(),
                    boundary.as_any(),
                    tail.as_any(),
                ],
            )
            .unwrap();
            let source = values.as_any().try_iter().unwrap();
            let output = PyList::empty(py);

            let (incompatible, completed) =
                unique_i64_exact_prefix_identity_cached_v1(output.as_any(), source.as_any())
                    .unwrap()
                    .unwrap();

            assert!(!completed);
            assert!(incompatible.unwrap().bind(py).is(&boundary));
            assert_eq!(output.len(), 2);
            assert!(output.get_item(0).unwrap().is(&first));
            assert!(output.get_item(1).unwrap().is(&true_value));
            assert!(source.call_method0("__next__").unwrap().is(&tail));
        });
    }
}
