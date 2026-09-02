//! Stable identity sorting for retained exact integer sequences.

#[cfg(not(Py_GIL_DISABLED))]
use pyo3::exceptions::{PyMemoryError, PyOverflowError};
use pyo3::prelude::*;
#[cfg(not(Py_GIL_DISABLED))]
use pyo3::types::{PyInt, PyList, PyTuple};
#[cfg(not(Py_GIL_DISABLED))]
use std::cmp::{Ordering, Reverse};

#[cfg(not(Py_GIL_DISABLED))]
struct SortEntry {
    value: i64,
    item: Py<PyAny>,
}

#[cfg(not(Py_GIL_DISABLED))]
enum DenseSortResult {
    Sorted(Vec<Py<PyAny>>),
    Fallback(Vec<SortEntry>),
}

#[cfg(not(Py_GIL_DISABLED))]
const ADAPTIVE_SORT_MIN_ROWS: usize = 32_768;
#[cfg(not(Py_GIL_DISABLED))]
const ORDER_SAMPLE_INTERVALS: usize = 512;
#[cfg(not(Py_GIL_DISABLED))]
const LOCAL_RUN_SAMPLES: usize = 128;
#[cfg(not(Py_GIL_DISABLED))]
const LOCAL_RUN_STRIDE: usize = 16;
// Keep the random-write counting table inside a modest cache footprint. Beyond this width the
// comparison sorter wins even when the integer range is technically dense.
#[cfg(not(Py_GIL_DISABLED))]
const DENSE_SORT_MAX_BUCKETS: usize = 524_288;
#[cfg(not(Py_GIL_DISABLED))]
const DENSE_SORT_MAX_RANGE_RATIO: usize = 4;
#[cfg(not(Py_GIL_DISABLED))]
const DENSE_SORT_SAMPLE_ITEMS: usize = 512;

#[cfg(not(Py_GIL_DISABLED))]
fn sequence_len(source: &Bound<'_, PyAny>) -> Option<usize> {
    source
        .cast_exact::<PyList>()
        .map(|values| values.len())
        .or_else(|_| source.cast_exact::<PyTuple>().map(|values| values.len()))
        .ok()
}

#[cfg(not(Py_GIL_DISABLED))]
fn sequence_item<'py>(source: &Bound<'py, PyAny>, index: usize) -> PyResult<Bound<'py, PyAny>> {
    if let Ok(values) = source.cast_exact::<PyList>() {
        values.get_item(index)
    } else {
        source.cast_exact::<PyTuple>()?.get_item(index)
    }
}

#[cfg(not(Py_GIL_DISABLED))]
fn exact_i64_value(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Option<i64>> {
    if !value.is_exact_instance_of::<PyInt>() {
        return Ok(None);
    }
    match value.extract::<i64>() {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.is_instance_of::<PyOverflowError>(py) => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(not(Py_GIL_DISABLED))]
fn direction(left: i64, right: i64) -> i8 {
    match right.cmp(&left) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

/// Detect inputs for which CPython's adaptive Timsort is already the better engine.
///
/// Evenly spaced comparisons distinguish random order from monotone, nearly monotone, and
/// few-run inputs without allocating or scanning the whole source. False negatives only choose
/// the canonical fallback; the full native validator still protects against false positives.
#[cfg(not(Py_GIL_DISABLED))]
fn prefers_python_adaptive_sort(source: &Bound<'_, PyAny>) -> PyResult<bool> {
    let Some(length) = sequence_len(source) else {
        return Ok(true);
    };
    if length < 2 {
        return Ok(false);
    }
    if length < ADAPTIVE_SORT_MIN_ROWS {
        return Ok(false);
    }
    let intervals = (length - 1).min(ORDER_SAMPLE_INTERVALS);
    let py = source.py();
    let Some(mut previous) = exact_i64_value(py, &sequence_item(source, 0)?)? else {
        return Ok(true);
    };
    let mut previous_direction = 0_i8;
    let mut non_equal = 0_usize;
    let mut direction_changes = 0_usize;
    for sample in 1..=intervals {
        let index = sample * (length - 1) / intervals;
        let Some(current) = exact_i64_value(py, &sequence_item(source, index)?)? else {
            return Ok(true);
        };
        let current_direction = direction(previous, current);
        if current_direction != 0 {
            non_equal += 1;
            if previous_direction != 0 && current_direction != previous_direction {
                direction_changes += 1;
            }
            previous_direction = current_direction;
        }
        previous = current;
    }
    if non_equal < 16 || direction_changes < 8 || direction_changes * 16 < non_equal {
        return Ok(true);
    }

    // Widely spaced samples can make shuffled long runs look random. Probe short local windows
    // as well: Timsort can reuse those runs, while random order changes direction frequently.
    let local_span = LOCAL_RUN_STRIDE * 2;
    let local_samples = (length - local_span).min(LOCAL_RUN_SAMPLES);
    let mut informative = 0_usize;
    let mut local_changes = 0_usize;
    for sample in 0..local_samples {
        let index = (sample * 2 + 1) * (length - local_span) / (local_samples * 2);
        let Some(first) = exact_i64_value(py, &sequence_item(source, index)?)? else {
            return Ok(true);
        };
        let Some(second) = exact_i64_value(py, &sequence_item(source, index + LOCAL_RUN_STRIDE)?)?
        else {
            return Ok(true);
        };
        let Some(third) = exact_i64_value(py, &sequence_item(source, index + local_span)?)? else {
            return Ok(true);
        };
        let first_direction = direction(first, second);
        let second_direction = direction(second, third);
        if first_direction != 0 && second_direction != 0 {
            informative += 1;
            if first_direction != second_direction {
                local_changes += 1;
            }
        }
    }
    Ok(informative >= 16 && local_changes * 4 < informative)
}

#[cfg(not(Py_GIL_DISABLED))]
fn exact_sequence_items(source: &Bound<'_, PyAny>) -> PyResult<Option<Vec<Py<PyAny>>>> {
    let Some(length) = sequence_len(source) else {
        return Ok(None);
    };
    let mut items = Vec::new();
    items
        .try_reserve_exact(length)
        .map_err(|_| PyMemoryError::new_err("native sort allocation failed"))?;
    if let Ok(values) = source.cast_exact::<PyList>() {
        for item in values.iter() {
            items.push(item.unbind());
        }
    } else {
        let values = source.cast_exact::<PyTuple>()?;
        for item in values.iter() {
            items.push(item.unbind());
        }
    }
    Ok(Some(items))
}

#[cfg(not(Py_GIL_DISABLED))]
fn exact_i64_entries(py: Python<'_>, items: Vec<Py<PyAny>>) -> PyResult<Option<Vec<SortEntry>>> {
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(items.len())
        .map_err(|_| PyMemoryError::new_err("native sort allocation failed"))?;
    for item in items {
        let Some(value) = exact_i64_value(py, item.bind(py))? else {
            return Ok(None);
        };
        entries.push(SortEntry { value, item });
    }
    Ok(Some(entries))
}

#[cfg(not(Py_GIL_DISABLED))]
fn dense_sort_items(entries: Vec<SortEntry>, reverse: bool) -> Result<DenseSortResult, ()> {
    if entries.is_empty() {
        return Ok(DenseSortResult::Sorted(Vec::new()));
    }
    let sample = &entries[..entries.len().min(DENSE_SORT_SAMPLE_ITEMS)];
    let sampled_minimum = sample
        .iter()
        .map(|entry| entry.value)
        .min()
        .expect("a non-empty sample has a minimum");
    let sampled_maximum = sample
        .iter()
        .map(|entry| entry.value)
        .max()
        .expect("a non-empty sample has a maximum");
    let sampled_width = i128::from(sampled_maximum) - i128::from(sampled_minimum) + 1;
    let maximum_width = entries
        .len()
        .saturating_mul(DENSE_SORT_MAX_RANGE_RATIO)
        .min(DENSE_SORT_MAX_BUCKETS);
    if sampled_width <= 0 || sampled_width > maximum_width as i128 {
        return Ok(DenseSortResult::Fallback(entries));
    }
    let minimum = entries
        .iter()
        .map(|entry| entry.value)
        .min()
        .expect("the non-empty sample proves entries are non-empty");
    let maximum = entries
        .iter()
        .map(|entry| entry.value)
        .max()
        .expect("non-empty entries have a maximum");
    let width = i128::from(maximum) - i128::from(minimum) + 1;
    if width <= 0 || width > maximum_width as i128 {
        return Ok(DenseSortResult::Fallback(entries));
    }
    let width = width as usize;
    let mut offsets = Vec::new();
    offsets.try_reserve_exact(width).map_err(|_| ())?;
    offsets.resize(width, 0_usize);
    for entry in &entries {
        offsets[entry.value.wrapping_sub(minimum) as usize] += 1;
    }
    let mut position = 0_usize;
    if reverse {
        for count in offsets.iter_mut().rev() {
            let next = position + *count;
            *count = position;
            position = next;
        }
    } else {
        for count in &mut offsets {
            let next = position + *count;
            *count = position;
            position = next;
        }
    }
    let mut output = Vec::new();
    output.try_reserve_exact(entries.len()).map_err(|_| ())?;
    output.resize_with(entries.len(), || None);
    for entry in entries {
        let bucket = entry.value.wrapping_sub(minimum) as usize;
        let index = offsets[bucket];
        offsets[bucket] += 1;
        output[index] = Some(entry.item);
    }
    Ok(DenseSortResult::Sorted(
        output
            .into_iter()
            .map(|item| item.expect("dense sort fills every reserved output slot"))
            .collect(),
    ))
}

#[cfg(not(Py_GIL_DISABLED))]
fn sort_items(mut entries: Vec<SortEntry>, reverse: bool) -> Result<Vec<Py<PyAny>>, ()> {
    match dense_sort_items(entries, reverse)? {
        DenseSortResult::Sorted(items) => Ok(items),
        DenseSortResult::Fallback(returned) => {
            entries = returned;
            if reverse {
                entries.sort_by_key(|entry| Reverse(entry.value));
            } else {
                entries.sort_by_key(|entry| entry.value);
            }
            Ok(entries.into_iter().map(|entry| entry.item).collect())
        }
    }
}

/// Sort an exact list or tuple of exact signed-64-bit integers while preserving item identity.
///
/// ``None`` is a lossless compatibility decline: the caller can still run built-in ``sorted``
/// because this endpoint never invokes item protocols or mutates the retained source.
#[pyfunction]
pub(crate) fn sort_i64_exact_sequence_v1(
    source: &Bound<'_, PyAny>,
    reverse: bool,
) -> PyResult<Option<Py<PyAny>>> {
    #[cfg(Py_GIL_DISABLED)]
    {
        let _ = (source, reverse);
        Ok(None)
    }

    #[cfg(not(Py_GIL_DISABLED))]
    {
        if prefers_python_adaptive_sort(source)? {
            return Ok(None);
        }
        let py = source.py();
        let Some(items) = exact_sequence_items(source)? else {
            return Ok(None);
        };
        let Some(entries) = exact_i64_entries(py, items)? else {
            return Ok(None);
        };
        let items = py
            .detach(move || sort_items(entries, reverse))
            .map_err(|()| PyMemoryError::new_err("native sort allocation failed"))?;
        Ok(Some(PyList::new(py, items)?.into_any().unbind()))
    }
}

#[cfg(all(test, not(Py_GIL_DISABLED)))]
mod tests {
    use super::*;

    #[test]
    fn stable_sort_preserves_equal_integer_identity_in_both_directions() {
        Python::initialize();
        Python::attach(|py| {
            let first = PyInt::new(py, 1_000_i64);
            let equal = PyInt::new(py, 1_000_i64);
            let lower = PyInt::new(py, -1_i64);
            let values = PyList::new(py, [first.as_any(), lower.as_any(), equal.as_any()]).unwrap();

            for reverse in [false, true] {
                let sorted = sort_i64_exact_sequence_v1(values.as_any(), reverse)
                    .unwrap()
                    .unwrap();
                let sorted = sorted.bind(py).cast::<PyList>().unwrap();
                let equal_offset = usize::from(!reverse);
                assert!(sorted.get_item(equal_offset).unwrap().is(&first));
                assert!(sorted.get_item(equal_offset + 1).unwrap().is(&equal));
            }
        });
    }

    #[test]
    fn incompatible_values_decline_without_protocol_dispatch() {
        Python::initialize();
        Python::attach(|py| {
            let fixture = PyModule::from_code(
                py,
                c"class Integer(int):\n    def __lt__(self, other):\n        raise AssertionError('comparison dispatched')\n",
                c"scalar_sort.py",
                c"scalar_sort",
            )
            .unwrap();
            let subclass = fixture.getattr("Integer").unwrap().call1((2,)).unwrap();
            let values =
                PyList::new(py, [1_i32.into_pyobject(py).unwrap().as_any(), &subclass]).unwrap();

            assert!(
                sort_i64_exact_sequence_v1(values.as_any(), false)
                    .unwrap()
                    .is_none()
            );
        });
    }

    #[test]
    fn dense_sort_handles_signed_minimum_and_keeps_reverse_ties_stable() {
        Python::initialize();
        Python::attach(|py| {
            let first = PyInt::new(py, i64::MIN);
            let higher = PyInt::new(py, i64::MIN + 1);
            let equal = PyInt::new(py, i64::MIN);
            let values =
                PyList::new(py, [first.as_any(), higher.as_any(), equal.as_any()]).unwrap();

            let sorted = sort_i64_exact_sequence_v1(values.as_any(), true)
                .unwrap()
                .unwrap();
            let sorted = sorted.bind(py).cast::<PyList>().unwrap();
            assert!(sorted.get_item(0).unwrap().is(&higher));
            assert!(sorted.get_item(1).unwrap().is(&first));
            assert!(sorted.get_item(2).unwrap().is(&equal));
        });
    }

    #[test]
    fn empty_and_singleton_sequences_return_fresh_lists() {
        Python::initialize();
        Python::attach(|py| {
            let empty = PyTuple::empty(py);
            let item = PyInt::new(py, 7_i64);
            let singleton = PyTuple::new(py, [item.as_any()]).unwrap();

            let sorted_empty = sort_i64_exact_sequence_v1(empty.as_any(), false)
                .unwrap()
                .unwrap();
            let sorted_singleton = sort_i64_exact_sequence_v1(singleton.as_any(), false)
                .unwrap()
                .unwrap();

            assert!(sorted_empty.bind(py).cast::<PyList>().unwrap().is_empty());
            assert!(
                sorted_singleton
                    .bind(py)
                    .cast::<PyList>()
                    .unwrap()
                    .get_item(0)
                    .unwrap()
                    .is(&item)
            );
        });
    }

    #[test]
    fn shuffled_long_natural_runs_decline_to_python_timsort() {
        Python::initialize();
        Python::attach(|py| {
            let chunk_size = 512_i64;
            let chunks = 64_i64;
            let mut values = Vec::with_capacity((chunk_size * chunks) as usize);
            for position in 0..chunks {
                let chunk = (position * 37) % chunks;
                values.extend((chunk * chunk_size)..((chunk + 1) * chunk_size));
            }
            let values = PyList::new(py, values).unwrap();

            assert!(
                sort_i64_exact_sequence_v1(values.as_any(), false)
                    .unwrap()
                    .is_none()
            );
        });
    }

    #[test]
    fn modular_permutation_is_not_mistaken_for_long_natural_runs() {
        Python::initialize();
        Python::attach(|py| {
            let size = 524_288_i64;
            let values =
                PyList::new(py, (0..size).map(|position| (position * 48_271) % size)).unwrap();

            assert!(!prefers_python_adaptive_sort(values.as_any()).unwrap());
        });
    }
}
