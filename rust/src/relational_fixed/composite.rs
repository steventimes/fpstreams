//! Ordered count/sum over two exact integer keys in retained tuple rows.

use super::*;

struct CompositeGroup {
    first: Py<PyAny>,
    second: Py<PyAny>,
    count: usize,
    total: i128,
}

fn group_composite_sequence(
    py: Python<'_>,
    row_count: usize,
    mut get_row: impl FnMut(usize) -> *mut ffi::PyObject,
    indices: [isize; 3],
) -> PyResult<Option<Vec<CompositeGroup>>> {
    // The single-i64 hasher overwrites its state on each integer write. A pair
    // needs a hasher that combines both components, including a repeated second key.
    let mut positions: HashMap<(i64, i64), usize> = HashMap::new();
    let mut groups: Vec<CompositeGroup> = Vec::new();
    let mut layout: Option<(usize, [usize; 3])> = None;
    for row_index in 0..row_count {
        let row = get_row(row_index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
        // SAFETY: the caller retains the exact outer tuple/list or a locked-list
        // snapshot. Tuple rows are immutable; subclasses are rejected before access.
        if unsafe { ffi::PyTuple_CheckExact(row) } == 0 {
            return Ok(None);
        }
        // SAFETY: row is an exact tuple retained by the source or snapshot.
        let width = unsafe { ffi::PyTuple_Size(row) };
        if width < 0 {
            return Err(PyErr::fetch(py));
        }
        let width = width as usize;
        let slots = match layout {
            Some((cached_width, slots)) if cached_width == width => slots,
            _ => {
                let mut slots = [0; 3];
                for (slot, index) in slots.iter_mut().zip(indices) {
                    let Some(position) = normalize_index(index, width) else {
                        return Ok(None);
                    };
                    *slot = position;
                }
                layout = Some((width, slots));
                slots
            }
        };
        let mut objects = [core::ptr::null_mut(); 3];
        let mut values = [0_i64; 3];
        for index in 0..3 {
            // SAFETY: each slot was normalized for this exact tuple's fixed width.
            let object = unsafe { ffi::PyTuple_GetItem(row, slots[index] as ffi::Py_ssize_t) };
            if object.is_null() {
                return Err(PyErr::fetch(py));
            }
            let Some(value) = exact_i64(py, object)? else {
                return Ok(None);
            };
            objects[index] = object;
            values[index] = value;
        }
        let key = (values[0], values[1]);
        if let Some(&position) = positions.get(&key) {
            let group = &mut groups[position];
            let Some(count) = group.count.checked_add(1) else {
                return Ok(None);
            };
            let Some(total) = group.total.checked_add(i128::from(values[2])) else {
                return Ok(None);
            };
            group.count = count;
            group.total = total;
        } else {
            positions.try_reserve(1).map_err(group_allocation_error)?;
            groups.try_reserve(1).map_err(group_allocation_error)?;
            // SAFETY: both borrowed keys are owned by the live immutable row.
            // Retaining its first key objects preserves Python identity on output.
            let first = unsafe { Borrowed::from_ptr(py, objects[0]).to_owned().unbind() };
            let second = unsafe { Borrowed::from_ptr(py, objects[1]).to_owned().unbind() };
            positions.insert(key, groups.len());
            groups.push(CompositeGroup {
                first,
                second,
                count: 1,
                total: i128::from(values[2]),
            });
        }
    }
    Ok(Some(groups))
}

fn group_composite_source(
    source: &Bound<'_, PyAny>,
    indices: [isize; 3],
) -> PyResult<Option<Vec<CompositeGroup>>> {
    if let Ok(rows) = source.cast_exact::<PyList>() {
        #[cfg(not(Py_GIL_DISABLED))]
        return group_composite_sequence(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: attached GIL execution prevents list mutation during the scan.
                unsafe { ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            indices,
        );
        #[cfg(Py_GIL_DISABLED)]
        {
            let snapshot = snapshot_exact_list_rows(source.py(), source, rows)?;
            return group_composite_sequence(
                source.py(),
                snapshot.len(),
                |index| snapshot[index].bind(source.py()).as_ptr(),
                indices,
            );
        }
    }
    if let Ok(rows) = source.cast_exact::<PyTuple>() {
        return group_composite_sequence(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: the exact outer tuple is immutable and index is in bounds.
                unsafe { ffi::PyTuple_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            indices,
        );
    }
    Ok(None)
}

#[pyfunction]
/// Return ordered count/sum rows, or decline before invoking any user protocol.
pub(crate) fn group_count_sum_i64_two_key_rows_v1(
    source: &Bound<'_, PyAny>,
    indices: &Bound<'_, PyAny>,
    output_names: &Bound<'_, PyAny>,
) -> PyResult<Option<Py<PyList>>> {
    let Ok(indices) = indices.cast_exact::<PyTuple>() else {
        return Ok(None);
    };
    let Ok(output_names) = output_names.cast_exact::<PyTuple>() else {
        return Ok(None);
    };
    if indices.len() != 3 || output_names.len() != 4 {
        return Ok(None);
    }
    let mut positions = [0_isize; 3];
    for (slot, value) in positions.iter_mut().zip(indices.iter()) {
        let Some(value) = exact_i64(source.py(), value.as_ptr())? else {
            return Ok(None);
        };
        let Ok(value) = isize::try_from(value) else {
            return Ok(None);
        };
        *slot = value;
    }
    let mut names = Vec::new();
    names.try_reserve_exact(4).map_err(group_allocation_error)?;
    for value in output_names.iter() {
        let Ok(name) = value.cast_into_exact::<PyString>() else {
            return Ok(None);
        };
        names.push(name);
    }
    let Some(groups) = group_composite_source(source, positions)? else {
        return Ok(None);
    };
    let mut rows = Vec::new();
    rows.try_reserve_exact(groups.len())
        .map_err(group_allocation_error)?;
    for group in groups {
        let row = new_dict_fallible(source.py())?;
        row.set_item(&names[0], group.first)?;
        row.set_item(&names[1], group.second)?;
        row.set_item(&names[2], group.count)?;
        set_widened_i64_item(&row, &names[3], group.total)?;
        rows.push(row.unbind());
    }
    PyList::new(source.py(), rows).map(|rows| Some(rows.unbind()))
}
