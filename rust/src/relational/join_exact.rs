//! Exact-dictionary schema, snapshot, and merge primitives shared by joins.

use super::group_numeric::{dict_item, exact_i64, snapshot_exact_list_rows};
use super::*;

/// One fixed exact-dict field layout, retaining the first row's key objects in order.
pub(super) struct ExactDictSchema {
    pub(super) fields: Vec<Py<PyAny>>,
}

/// Exact integer positions for a unique-right join.
///
/// Compact non-negative keys stay in direct slots. A negative or sparse key migrates the
/// already-built prefix to hashing without declining the native join.
pub(super) enum DictJoinPositions {
    Dense {
        slots: Vec<usize>,
        keys: Vec<i64>,
        dense_limit: usize,
        hash_capacity: usize,
    },
    Hash(HashMap<i64, usize>),
}

impl DictJoinPositions {
    pub(super) fn new(row_count: usize) -> Self {
        Self::Dense {
            slots: Vec::new(),
            keys: Vec::new(),
            dense_limit: row_count
                .saturating_mul(MAX_DENSE_SLOTS_PER_ROW)
                .min(MAX_DENSE_GROUP_SLOTS),
            hash_capacity: row_count,
        }
    }

    /// Insert a unique key, returning false when the right relation contains a duplicate.
    pub(super) fn insert_unique(&mut self, key: i64, position: usize) -> PyResult<bool> {
        if let Ok(index) = usize::try_from(key)
            && let Self::Dense {
                slots,
                keys,
                dense_limit,
                ..
            } = self
        {
            let required = index.saturating_add(1);
            let growth_limit = slots
                .len()
                .saturating_mul(MAX_RECORD_DENSE_GROWTH_FACTOR)
                .max(MAX_INITIAL_RECORD_DENSE_SLOTS);
            if required <= *dense_limit && required <= growth_limit {
                keys.try_reserve(1).map_err(join_allocation_error)?;
                if required > slots.len() {
                    slots
                        .try_reserve(required - slots.len())
                        .map_err(join_allocation_error)?;
                    let new_len = slots.capacity().min(*dense_limit).max(required);
                    slots.resize(new_len, usize::MAX);
                }
                if slots[index] != usize::MAX {
                    return Ok(false);
                }
                slots[index] = position;
                keys.push(key);
                return Ok(true);
            }
        }

        if matches!(self, Self::Dense { .. }) {
            let previous = std::mem::replace(self, Self::Hash(HashMap::new()));
            let Self::Dense {
                keys,
                hash_capacity,
                ..
            } = previous
            else {
                unreachable!("the dense join index was just replaced")
            };
            let mut positions = HashMap::new();
            positions
                .try_reserve(hash_capacity)
                .map_err(join_allocation_error)?;
            for (position, existing_key) in keys.into_iter().enumerate() {
                positions.insert(existing_key, position);
            }
            *self = Self::Hash(positions);
        }
        let Self::Hash(positions) = self else {
            unreachable!("dense join positions must migrate before hashed insertion")
        };
        Ok(positions.insert(key, position).is_none())
    }

    /// Look up a right position without changing the chosen representation.
    #[inline]
    pub(super) fn get(&self, key: i64) -> Option<usize> {
        match self {
            Self::Dense { slots, .. } => {
                let index = usize::try_from(key).ok()?;
                slots
                    .get(index)
                    .copied()
                    .filter(|&position| position != usize::MAX)
            }
            Self::Hash(positions) => positions.get(&key).copied(),
        }
    }
}

/// Convert a failed native join reservation into a recoverable Python allocation error.
pub(super) fn join_allocation_error(_error: TryReserveError) -> PyErr {
    PyMemoryError::new_err("native record join allocation failed")
}

/// Add one contribution to a Python-list-sized join result without wrapping.
pub(crate) fn checked_join_output_size(current: usize, additional: usize) -> PyResult<usize> {
    let total = current
        .checked_add(additional)
        .ok_or_else(|| PyMemoryError::new_err("native record join output size overflowed"))?;
    let py_list_limit = usize::try_from(ffi::PY_SSIZE_T_MAX)
        .expect("Py_ssize_t maximum must fit in the platform usize");
    if total > py_list_limit {
        return Err(PyMemoryError::new_err(
            "native record join output exceeds Python list capacity",
        ));
    }
    Ok(total)
}

/// Compare two already-proven exact Python strings without invoking user code.
pub(super) fn exact_string_equal(
    py: Python<'_>,
    left: *mut ffi::PyObject,
    right: *mut ffi::PyObject,
) -> PyResult<bool> {
    if left == right {
        return Ok(true);
    }
    // SAFETY: both pointers are live exact unicode objects, so comparison cannot dispatch to
    // Python overrides. PyUnicode_Compare may still report a genuine interpreter error.
    let comparison = unsafe { ffi::PyUnicode_Compare(left, right) };
    // PyUnicode_Compare reports errors as -1, which is also a valid ordering result. Avoid the
    // thread-local exception lookup on the much hotter equal-string result.
    // SAFETY: reading the attached thread's exception indicator has no ownership effect.
    if comparison == -1 && !unsafe { ffi::PyErr_Occurred() }.is_null() {
        return Err(PyErr::fetch(py));
    }
    Ok(comparison == 0)
}

/// Reject dotted selectors without UTF-8 conversion, including strings with lone surrogates.
pub(super) fn exact_string_contains_dot(
    py: Python<'_>,
    value: *mut ffi::PyObject,
) -> PyResult<bool> {
    // SAFETY: value is a live exact unicode object. Searching for one code point invokes no
    // Python callback and accepts the entire Py_ssize_t range as the end position.
    let position =
        unsafe { ffi::PyUnicode_FindChar(value, u32::from('.'), 0, ffi::PY_SSIZE_T_MAX, 1) };
    // A missing character returns -1 without an exception; a genuine failure sets one.
    // SAFETY: reading the attached thread's exception indicator has no ownership effect.
    if !unsafe { ffi::PyErr_Occurred() }.is_null() {
        return Err(PyErr::fetch(py));
    }
    Ok(position >= 0)
}

/// Validate one exact dict's field layout and capture its selected value while scanning.
fn validate_exact_dict_schema(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    selected_field: *mut ffi::PyObject,
    schema: &mut Option<ExactDictSchema>,
) -> PyResult<Option<(*mut ffi::PyObject, bool)>> {
    // SAFETY: the caller holds the exact dict's critical section.
    let field_count = unsafe { ffi::PyDict_Size(row) };
    if field_count < 0 {
        return Err(PyErr::fetch(py));
    }
    let field_count = field_count as usize;
    if field_count > RECORD_JOIN_V1_MAX_FIELDS {
        return Ok(None);
    }

    let mut new_fields = if schema.is_none() {
        let mut fields = Vec::new();
        fields
            .try_reserve(field_count)
            .map_err(join_allocation_error)?;
        Some(fields)
    } else {
        None
    };
    let mut canonical_field_objects = true;
    let mut position = 0;
    let mut field = core::ptr::null_mut();
    let mut field_value = core::ptr::null_mut();
    let mut selected_value = core::ptr::null_mut();
    for field_index in 0..field_count {
        // SAFETY: row is an exact dict protected by its critical section. PyDict_Next returns
        // borrowed references in insertion order and invokes no Python callbacks.
        if unsafe { ffi::PyDict_Next(row, &mut position, &mut field, &mut field_value) } == 0 {
            return Ok(None);
        }
        // SAFETY: field is non-null for an item returned by PyDict_Next.
        if unsafe { ffi::PyUnicode_CheckExact(field) } == 0 {
            return Ok(None);
        }
        if let Some(expected) = schema.as_ref() {
            let Some(expected_field) = expected.fields.get(field_index) else {
                return Ok(None);
            };
            let expected_field = expected_field.bind(py).as_ptr();
            if field != expected_field {
                canonical_field_objects = false;
                if !exact_string_equal(py, field, expected_field)? {
                    return Ok(None);
                }
            }
        } else {
            let fields = new_fields
                .as_mut()
                .expect("a first exact-dict schema owns its field buffer");
            // SAFETY: field is a borrowed key kept live by the locked row. Taking one strong
            // reference keeps the schema field alive independently of later row mutation.
            fields.push(unsafe { Borrowed::from_ptr(py, field).to_owned().unbind() });
        }
        if field == selected_field {
            selected_value = field_value;
        }
    }

    if let Some(expected) = schema.as_ref()
        && expected.fields.len() != field_count
    {
        return Ok(None);
    }
    if let Some(fields) = new_fields {
        *schema = Some(ExactDictSchema { fields });
    }
    if selected_value.is_null() {
        let Some(found) = dict_item(py, row, selected_field)? else {
            return Ok(None);
        };
        selected_value = found;
    }
    Ok(Some((selected_value, canonical_field_objects)))
}

/// Copy one exact row coherently, then validate and select from the private snapshot.
pub(super) fn snapshot_exact_join_row(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    key_field: *mut ffi::PyObject,
    schema: &mut Option<ExactDictSchema>,
) -> PyResult<Option<(Py<PyDict>, i64, bool)>> {
    // SAFETY: the source container or its snapshot owns a strong reference to row for this call.
    if unsafe { ffi::PyDict_CheckExact(row) } == 0 {
        return Ok(None);
    }
    // Reject a statically wide row before allocating a speculative copy. PyDict_Size performs
    // its own synchronization on free-threaded CPython and cannot dispatch to user code for an
    // exact dict. The private snapshot is still checked again below because another thread may
    // change the source after this inexpensive preflight.
    // SAFETY: row is a live exact dict owned by the source snapshot.
    let field_count = unsafe { ffi::PyDict_Size(row) };
    if field_count < 0 {
        return Err(PyErr::fetch(py));
    }
    if field_count as usize > RECORD_JOIN_V1_MAX_FIELDS {
        return Ok(None);
    }
    // PyDict_Copy performs its own synchronization on free-threaded CPython. It must not run
    // inside another critical section for the same dictionary: nested critical sections may
    // suspend the outer lock and would leave a gap between validation and the copied contents.
    // SAFETY: row is a live exact dict owned by the source snapshot. PyDict_Copy returns a new
    // exact dict or a null pointer with a Python error.
    let copied = unsafe { ffi::PyDict_Copy(row) };
    // SAFETY: copied is an owned reference returned by PyDict_Copy.
    let snapshot = unsafe { Bound::from_owned_ptr_or_err(py, copied)? }
        .cast_into::<PyDict>()
        .expect("PyDict_Copy must return an exact dict");
    let snapshot_ptr = snapshot.as_ptr();
    let key = with_critical_section(snapshot.as_any(), || -> PyResult<Option<(i64, bool)>> {
        let Some((key_object, canonical_field_objects)) =
            validate_exact_dict_schema(py, snapshot_ptr, key_field, schema)?
        else {
            return Ok(None);
        };
        let Some(key) = exact_i64(py, key_object)? else {
            return Ok(None);
        };
        Ok(Some((key, canonical_field_objects)))
    })?;
    Ok(key.map(|(key, canonical_field_objects)| (snapshot.unbind(), key, canonical_field_objects)))
}

/// Return one exact row's current bounded width without invoking mapping protocols.
pub(super) fn exact_join_row_field_count(
    py: Python<'_>,
    row: *mut ffi::PyObject,
) -> PyResult<Option<usize>> {
    // SAFETY: the outer source snapshot keeps row live for this call. Exact-dict size reads
    // synchronize internally on free-threaded CPython and cannot invoke user code.
    if unsafe { ffi::PyDict_CheckExact(row) } == 0 {
        return Ok(None);
    }
    let field_count = unsafe { ffi::PyDict_Size(row) };
    if field_count < 0 {
        return Err(PyErr::fetch(py));
    }
    let field_count = field_count as usize;
    if field_count > RECORD_JOIN_V1_MAX_FIELDS {
        return Ok(None);
    }
    Ok(Some(field_count))
}

/// Capture one validated right row's values in canonical schema order.
fn collect_exact_join_values<T>(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    key_field: *mut ffi::PyObject,
    schema: &mut Option<ExactDictSchema>,
    values: &mut Vec<T>,
    mut capture: impl FnMut(Python<'_>, *mut ffi::PyObject) -> T,
) -> PyResult<Option<i64>> {
    if unsafe { ffi::PyDict_CheckExact(row) } == 0 {
        return Ok(None);
    }
    // SAFETY: the source container or its snapshot owns row strongly throughout this function.
    let row_bound = unsafe { Borrowed::from_ptr(py, row) };
    with_critical_section(row_bound.as_any(), || {
        // SAFETY: row is an exact dict protected by this critical section.
        let field_count = unsafe { ffi::PyDict_Size(row) };
        if field_count < 0 {
            return Err(PyErr::fetch(py));
        }
        let field_count = field_count as usize;
        if field_count > RECORD_JOIN_V1_MAX_FIELDS {
            return Ok(None);
        }
        if let Some(expected) = schema.as_ref()
            && expected.fields.len() != field_count
        {
            return Ok(None);
        }
        values
            .try_reserve(field_count)
            .map_err(join_allocation_error)?;
        let mut new_fields = if schema.is_none() {
            let mut fields = Vec::new();
            fields
                .try_reserve(field_count)
                .map_err(join_allocation_error)?;
            Some(fields)
        } else {
            None
        };

        let mut position = 0;
        let mut field = core::ptr::null_mut();
        let mut field_value = core::ptr::null_mut();
        let mut selected_value = core::ptr::null_mut();
        for field_index in 0..field_count {
            // SAFETY: the locked size fixes the successful iteration count. PyDict_Next only
            // returns borrowed references and cannot invoke Python code for this exact dict.
            if unsafe { ffi::PyDict_Next(row, &mut position, &mut field, &mut field_value) } == 0 {
                return Ok(None);
            }
            // SAFETY: field is non-null for an item returned by PyDict_Next.
            if unsafe { ffi::PyUnicode_CheckExact(field) } == 0 {
                return Ok(None);
            }
            if let Some(expected) = schema.as_ref() {
                let expected_field = expected.fields[field_index].bind(py).as_ptr();
                if field != expected_field && !exact_string_equal(py, field, expected_field)? {
                    return Ok(None);
                }
            } else {
                // SAFETY: the row lock keeps this borrowed exact string live while the strong
                // reference is taken for the canonical first-row schema.
                new_fields
                    .as_mut()
                    .expect("a first compact snapshot owns its schema field buffer")
                    .push(unsafe { Borrowed::from_ptr(py, field).to_owned().unbind() });
            }
            if field == key_field {
                selected_value = field_value;
            }
            values.push(capture(py, field_value));
        }

        if let Some(fields) = new_fields {
            *schema = Some(ExactDictSchema { fields });
        }
        if selected_value.is_null() {
            let Some(found) = dict_item(py, row, key_field)? else {
                return Ok(None);
            };
            selected_value = found;
        }
        exact_i64(py, selected_value)
    })
}

/// Capture one validated right row as strong values in canonical schema order.
pub(super) fn snapshot_exact_join_values(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    key_field: *mut ffi::PyObject,
    schema: &mut Option<ExactDictSchema>,
    values: &mut Vec<Py<PyAny>>,
) -> PyResult<Option<i64>> {
    collect_exact_join_values(py, row, key_field, schema, values, |py, value| {
        // SAFETY: value is borrowed from the locked row. The new strong reference freezes this
        // build-side value independently of later source mutation.
        unsafe { Borrowed::from_ptr(py, value).to_owned().unbind() }
    })
}

/// Borrow one validated right row's values while a GIL-protected source owns them.
#[cfg(not(Py_GIL_DISABLED))]
pub(super) fn borrow_exact_join_values(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    key_field: *mut ffi::PyObject,
    schema: &mut Option<ExactDictSchema>,
    values: &mut Vec<*mut ffi::PyObject>,
) -> PyResult<Option<i64>> {
    collect_exact_join_values(py, row, key_field, schema, values, |_py, value| value)
}

/// Bound speculative allocation while reserving enough compact slots for common narrow tables.
pub(super) fn reserve_compact_join_values<T>(
    values: &mut Vec<T>,
    row_count: usize,
    field_count: usize,
) -> PyResult<()> {
    let total_values = row_count
        .checked_mul(field_count)
        .ok_or_else(|| PyMemoryError::new_err("native record join snapshot is too large"))?;
    let initial_values = total_values.min(MAX_INITIAL_RECORD_JOIN_VALUES);
    values
        .try_reserve_exact(initial_values.saturating_sub(values.len()))
        .map_err(join_allocation_error)
}

/// Retain stable row references from one exact list or tuple without opening Python iteration.
pub(super) fn snapshot_exact_join_source(
    source: &Bound<'_, PyAny>,
) -> PyResult<Option<Vec<Py<PyAny>>>> {
    if let Ok(rows) = source.cast_exact::<PyList>() {
        return snapshot_exact_list_rows(source.py(), source, rows).map(Some);
    }
    let rows = match source.cast_exact::<PyTuple>() {
        Ok(rows) => rows,
        Err(_) => return Ok(None),
    };
    let mut snapshot = Vec::new();
    snapshot
        .try_reserve(rows.len())
        .map_err(join_allocation_error)?;
    for index in 0..rows.len() {
        // SAFETY: exact tuples are immutable and index is within their fixed length.
        let row = unsafe { ffi::PyTuple_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) };
        if row.is_null() {
            return Err(PyErr::fetch(source.py()));
        }
        // SAFETY: row is a borrowed tuple item kept live by source. Take a stable strong ref.
        snapshot.push(unsafe { Borrowed::from_ptr(source.py(), row).to_owned().unbind() });
    }
    Ok(Some(snapshot))
}

/// Identify which right fields are the one shared same-name join key.
pub(super) fn shared_right_fields(
    py: Python<'_>,
    schema: Option<&ExactDictSchema>,
    right_field: *mut ffi::PyObject,
    shared_key: bool,
) -> PyResult<Vec<bool>> {
    let Some(schema) = schema else {
        return Ok(Vec::new());
    };
    let mut shared = Vec::new();
    shared
        .try_reserve(schema.fields.len())
        .map_err(join_allocation_error)?;
    for field in &schema.fields {
        shared.push(shared_key && exact_string_equal(py, field.bind(py).as_ptr(), right_field)?);
    }
    Ok(shared)
}

/// Return false when a non-shared right field would require Python suffix semantics.
pub(super) fn schemas_have_no_collision(
    py: Python<'_>,
    left: &ExactDictSchema,
    right: Option<&ExactDictSchema>,
    shared_right: &[bool],
) -> PyResult<bool> {
    let Some(right) = right else {
        return Ok(true);
    };
    for (right_index, right_field) in right.fields.iter().enumerate() {
        if shared_right[right_index] {
            continue;
        }
        for left_field in &left.fields {
            if exact_string_equal(
                py,
                left_field.bind(py).as_ptr(),
                right_field.bind(py).as_ptr(),
            )? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Insert one exact field/value pair and preserve genuine dictionary allocation errors.
pub(super) fn set_dict_item(
    py: Python<'_>,
    target: *mut ffi::PyObject,
    field: *mut ffi::PyObject,
    value: *mut ffi::PyObject,
) -> PyResult<()> {
    // SAFETY: target is an executor-owned exact dict; field is an exact string; value is live.
    if unsafe { ffi::PyDict_SetItem(target, field, value) } == 0 {
        Ok(())
    } else {
        Err(PyErr::fetch(py))
    }
}

/// Append right fields to one executor-owned left snapshot in canonical field order.
pub(super) fn merge_exact_join_match(
    py: Python<'_>,
    output: &Py<PyDict>,
    right: &Py<PyDict>,
    right_schema: Option<&ExactDictSchema>,
    shared_right: &[bool],
    bulk_merge_right: bool,
) -> PyResult<Option<()>> {
    let Some(schema) = right_schema else {
        return Ok(Some(()));
    };
    if bulk_merge_right {
        // SAFETY: both operands are executor-owned exact dictionaries with exact-string keys.
        // Schema collision checks prove that override=0 only suppresses the shared join field.
        // Pointer-identical right layouts also preserve the first row's canonical key objects.
        if unsafe { ffi::PyDict_Merge(output.bind(py).as_ptr(), right.bind(py).as_ptr(), 0) } != 0 {
            return Err(PyErr::fetch(py));
        }
        #[cfg(test)]
        record_join_bulk_merge_hit();
        return Ok(Some(()));
    }
    for (field_index, field) in schema.fields.iter().enumerate() {
        if shared_right[field_index] {
            continue;
        }
        let field = field.bind(py).as_ptr();
        let Some(value) = dict_item(py, right.bind(py).as_ptr(), field)? else {
            return Ok(None);
        };
        set_dict_item(py, output.bind(py).as_ptr(), field, value)?;
    }
    Ok(Some(()))
}

/// Append one compact right-value snapshot with the first row's canonical field objects.
pub(super) fn merge_exact_join_values(
    py: Python<'_>,
    output: &Py<PyDict>,
    right_values: &[Py<PyAny>],
    right_schema: Option<&ExactDictSchema>,
    shared_right: &[bool],
) -> PyResult<()> {
    let Some(schema) = right_schema else {
        debug_assert!(right_values.is_empty());
        return Ok(());
    };
    debug_assert_eq!(right_values.len(), schema.fields.len());
    debug_assert_eq!(shared_right.len(), schema.fields.len());
    for (field_index, (field, value)) in schema.fields.iter().zip(right_values).enumerate() {
        if !shared_right[field_index] {
            set_dict_item(
                py,
                output.bind(py).as_ptr(),
                field.bind(py).as_ptr(),
                value.bind(py).as_ptr(),
            )?;
        }
    }
    Ok(())
}

/// Append borrowed right values while the exact source remains protected by the GIL.
#[cfg(not(Py_GIL_DISABLED))]
pub(super) fn merge_borrowed_exact_join_values(
    py: Python<'_>,
    output: &Py<PyDict>,
    right_values: &[*mut ffi::PyObject],
    right_schema: Option<&ExactDictSchema>,
    shared_right: &[bool],
) -> PyResult<()> {
    let Some(schema) = right_schema else {
        debug_assert!(right_values.is_empty());
        return Ok(());
    };
    debug_assert_eq!(right_values.len(), schema.fields.len());
    debug_assert_eq!(shared_right.len(), schema.fields.len());
    for (field_index, (field, &value)) in schema.fields.iter().zip(right_values).enumerate() {
        if !shared_right[field_index] {
            set_dict_item(py, output.bind(py).as_ptr(), field.bind(py).as_ptr(), value)?;
        }
    }
    Ok(())
}

/// Append unmatched right fields as None in the fixed right schema's encounter order.
pub(super) fn merge_exact_join_unmatched(
    py: Python<'_>,
    output: &Py<PyDict>,
    right_schema: Option<&ExactDictSchema>,
    shared_right: &[bool],
    none: *mut ffi::PyObject,
) -> PyResult<()> {
    let Some(schema) = right_schema else {
        return Ok(());
    };
    for (field_index, field) in schema.fields.iter().enumerate() {
        if !shared_right[field_index] {
            set_dict_item(py, output.bind(py).as_ptr(), field.bind(py).as_ptr(), none)?;
        }
    }
    Ok(())
}

/// Build stable contiguous right-row positions for factorized join-key codes.
pub(super) fn factorized_right_positions(
    right_codes: &[usize],
    group_counts: &[usize],
) -> PyResult<(Vec<usize>, Vec<usize>)> {
    let offset_count = checked_join_output_size(group_counts.len(), 1)?;
    let mut offsets = Vec::new();
    offsets
        .try_reserve_exact(offset_count)
        .map_err(join_allocation_error)?;
    offsets.push(0);
    for &count in group_counts {
        let next =
            checked_join_output_size(*offsets.last().expect("offset zero was inserted"), count)?;
        offsets.push(next);
    }
    debug_assert_eq!(offsets.last().copied(), Some(right_codes.len()));

    let mut cursors = Vec::new();
    cursors
        .try_reserve_exact(group_counts.len())
        .map_err(join_allocation_error)?;
    cursors.extend_from_slice(&offsets[..group_counts.len()]);

    let mut positions = Vec::new();
    positions
        .try_reserve_exact(right_codes.len())
        .map_err(join_allocation_error)?;
    positions.resize(right_codes.len(), usize::MAX);
    for (right_position, &code) in right_codes.iter().enumerate() {
        let slot = *cursors
            .get(code)
            .expect("every factorized code has one group cursor");
        positions[slot] = right_position;
        cursors[code] = slot
            .checked_add(1)
            .expect("a right-row cursor cannot exceed its source length");
    }
    debug_assert!(positions.iter().all(|&position| position != usize::MAX));
    Ok((offsets, positions))
}

/// Copy one executor-owned exact dictionary while preserving Python allocation errors.
pub(super) fn copy_join_record(py: Python<'_>, source: &Py<PyDict>) -> PyResult<Py<PyDict>> {
    // SAFETY: source is a live exact dict. PyDict_Copy returns a new exact dict or null with
    // the interpreter's allocation exception set.
    let copied = unsafe { ffi::PyDict_Copy(source.bind(py).as_ptr()) };
    // SAFETY: copied is an owned reference returned by PyDict_Copy.
    Ok(unsafe { Bound::from_owned_ptr_or_err(py, copied)? }
        .cast_into::<PyDict>()
        .expect("PyDict_Copy must return an exact dict")
        .unbind())
}
