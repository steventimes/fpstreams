//! Exact-i64 unique and many-side dictionary join kernels.

use super::join_exact::{
    DictJoinPositions, copy_join_record, exact_join_row_field_count, exact_string_contains_dot,
    exact_string_equal, factorized_right_positions, join_allocation_error, merge_exact_join_match,
    merge_exact_join_unmatched, merge_exact_join_values, reserve_compact_join_values,
    schemas_have_no_collision, shared_right_fields, snapshot_exact_join_row,
    snapshot_exact_join_source, snapshot_exact_join_values,
};
#[cfg(not(Py_GIL_DISABLED))]
use super::join_exact::{borrow_exact_join_values, merge_borrowed_exact_join_values};
use super::*;

#[cfg(not(Py_GIL_DISABLED))]
#[derive(Clone, Copy)]
enum BorrowedJoinSourceKind {
    List,
    Tuple,
}

/// Admit one exact eager source whose rows remain live throughout an attached GIL call.
#[cfg(not(Py_GIL_DISABLED))]
fn borrowed_join_source(source: &Bound<'_, PyAny>) -> Option<(BorrowedJoinSourceKind, usize)> {
    if let Ok(rows) = source.cast_exact::<PyList>() {
        return Some((BorrowedJoinSourceKind::List, rows.len()));
    }
    source
        .cast_exact::<PyTuple>()
        .ok()
        .map(|rows| (BorrowedJoinSourceKind::Tuple, rows.len()))
}

/// Read one borrowed source row without creating a table-sized strong-reference snapshot.
#[cfg(not(Py_GIL_DISABLED))]
fn borrowed_join_row(
    source: &Bound<'_, PyAny>,
    kind: BorrowedJoinSourceKind,
    index: usize,
) -> PyResult<*mut ffi::PyObject> {
    // SAFETY: a GIL build cannot mutate an exact list while this attached call is running; exact
    // tuples are immutable. The caller bounds index by the source length captured under the GIL.
    let row = unsafe {
        match kind {
            BorrowedJoinSourceKind::List => {
                ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t)
            }
            BorrowedJoinSourceKind::Tuple => {
                ffi::PyTuple_GetItem(source.as_ptr(), index as ffi::Py_ssize_t)
            }
        }
    };
    if row.is_null() {
        Err(PyErr::fetch(source.py()))
    } else {
        Ok(row)
    }
}

#[pyfunction]
/// Speculatively materialize an exact-i64 unique-right join or return None for fallback.
pub(crate) fn join_i64_unique_dict_rows_v1(
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    left_field: &Bound<'_, PyAny>,
    right_field: &Bound<'_, PyAny>,
    left_join: bool,
) -> PyResult<Option<Vec<Py<PyDict>>>> {
    let left_field = match left_field.cast_exact::<PyString>() {
        Ok(field) => field,
        Err(_) => return Ok(None),
    };
    let right_field = match right_field.cast_exact::<PyString>() {
        Ok(field) => field,
        Err(_) => return Ok(None),
    };
    if exact_string_contains_dot(left.py(), left_field.as_ptr())?
        || exact_string_contains_dot(left.py(), right_field.as_ptr())?
    {
        return Ok(None);
    }
    let Some(right_rows) = snapshot_exact_join_source(right)? else {
        return Ok(None);
    };

    let shared_key = exact_string_equal(left.py(), left_field.as_ptr(), right_field.as_ptr())?;
    let compact_right = if let Some(first) = right_rows.first() {
        let Some(field_count) =
            exact_join_row_field_count(left.py(), first.bind(left.py()).as_ptr())?
        else {
            return Ok(None);
        };
        !record_join_prefers_bulk_merge(field_count)
    } else {
        true
    };
    let mut right_schema = None;
    let mut right_records: Vec<Py<PyDict>> = Vec::new();
    if !compact_right {
        right_records
            .try_reserve(right_rows.len())
            .map_err(join_allocation_error)?;
    }
    let mut right_values = Vec::new();
    let mut canonical_right_field_objects = true;
    let mut right_index = DictJoinPositions::new(right_rows.len());
    for (right_position, row) in right_rows.iter().enumerate() {
        let key = if compact_right {
            let Some(key) = snapshot_exact_join_values(
                left.py(),
                row.bind(left.py()).as_ptr(),
                right_field.as_ptr(),
                &mut right_schema,
                &mut right_values,
            )?
            else {
                return Ok(None);
            };
            if right_position == 0 {
                let field_count = right_schema
                    .as_ref()
                    .expect("a validated compact right row establishes its schema")
                    .fields
                    .len();
                reserve_compact_join_values(&mut right_values, right_rows.len(), field_count)?;
            }
            key
        } else {
            let Some((snapshot, key, canonical_field_objects)) = snapshot_exact_join_row(
                left.py(),
                row.bind(left.py()).as_ptr(),
                right_field.as_ptr(),
                &mut right_schema,
            )?
            else {
                return Ok(None);
            };
            canonical_right_field_objects &= canonical_field_objects;
            right_records.push(snapshot);
            key
        };
        if !right_index.insert_unique(key, right_position)? {
            return Ok(None);
        }
    }
    let right_field_count = right_schema
        .as_ref()
        .map_or(0, |schema| schema.fields.len());
    debug_assert!(!compact_right || right_values.len() == right_rows.len() * right_field_count);
    let shared_right = shared_right_fields(
        left.py(),
        right_schema.as_ref(),
        right_field.as_ptr(),
        shared_key,
    )?;
    let bulk_merge_right = !compact_right && canonical_right_field_objects;
    let Some(left_rows) = snapshot_exact_join_source(left)? else {
        return Ok(None);
    };

    let mut left_schema = None;
    let mut collision_checked = false;
    let mut joined = Vec::new();
    joined
        .try_reserve(left_rows.len())
        .map_err(join_allocation_error)?;
    let none = left.py().None();
    for row in &left_rows {
        let Some((snapshot, key, _)) = snapshot_exact_join_row(
            left.py(),
            row.bind(left.py()).as_ptr(),
            left_field.as_ptr(),
            &mut left_schema,
        )?
        else {
            return Ok(None);
        };
        if !collision_checked {
            let schema = left_schema
                .as_ref()
                .expect("a validated left row establishes its schema");
            if !schemas_have_no_collision(left.py(), schema, right_schema.as_ref(), &shared_right)?
            {
                return Ok(None);
            }
            collision_checked = true;
        }
        if let Some(right_position) = right_index.get(key) {
            if compact_right {
                let start = right_position * right_field_count;
                merge_exact_join_values(
                    left.py(),
                    &snapshot,
                    &right_values[start..start + right_field_count],
                    right_schema.as_ref(),
                    &shared_right,
                )?;
            } else if merge_exact_join_match(
                left.py(),
                &snapshot,
                &right_records[right_position],
                right_schema.as_ref(),
                &shared_right,
                bulk_merge_right,
            )?
            .is_none()
            {
                return Ok(None);
            }
            joined.push(snapshot);
        } else if left_join {
            merge_exact_join_unmatched(
                left.py(),
                &snapshot,
                right_schema.as_ref(),
                &shared_right,
                none.as_ptr(),
            )?;
            joined.push(snapshot);
        }
    }
    Ok(Some(joined))
}

#[cfg(not(Py_GIL_DISABLED))]
fn join_i64_unique_dict_rows_borrowed(
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    left_field: &Bound<'_, PyAny>,
    right_field: &Bound<'_, PyAny>,
    left_join: bool,
) -> PyResult<Option<Vec<Py<PyDict>>>> {
    let left_field = match left_field.cast_exact::<PyString>() {
        Ok(field) => field,
        Err(_) => return Ok(None),
    };
    let right_field = match right_field.cast_exact::<PyString>() {
        Ok(field) => field,
        Err(_) => return Ok(None),
    };
    if exact_string_contains_dot(left.py(), left_field.as_ptr())?
        || exact_string_contains_dot(left.py(), right_field.as_ptr())?
    {
        return Ok(None);
    }
    let Some((right_kind, right_count)) = borrowed_join_source(right) else {
        return Ok(None);
    };

    // Wide rows retain v1's proven bulk-merge path. The borrowed value lane targets the common
    // narrow schema where it can remove both whole-source and whole-value strong snapshots.
    if right_count != 0 {
        let first = borrowed_join_row(right, right_kind, 0)?;
        let Some(field_count) = exact_join_row_field_count(left.py(), first)? else {
            return Ok(None);
        };
        if record_join_prefers_bulk_merge(field_count) {
            return Ok(None);
        }
    }

    let shared_key = exact_string_equal(left.py(), left_field.as_ptr(), right_field.as_ptr())?;
    let mut right_schema = None;
    let mut right_values = Vec::new();
    let mut right_index = DictJoinPositions::new(right_count);
    for right_position in 0..right_count {
        let row = borrowed_join_row(right, right_kind, right_position)?;
        let Some(key) = borrow_exact_join_values(
            left.py(),
            row,
            right_field.as_ptr(),
            &mut right_schema,
            &mut right_values,
        )?
        else {
            return Ok(None);
        };
        if right_position == 0 {
            let field_count = right_schema
                .as_ref()
                .expect("a validated borrowed right row establishes its schema")
                .fields
                .len();
            reserve_compact_join_values(&mut right_values, right_count, field_count)?;
        }
        if !right_index.insert_unique(key, right_position)? {
            return Ok(None);
        }
    }
    let right_field_count = right_schema
        .as_ref()
        .map_or(0, |schema| schema.fields.len());
    debug_assert_eq!(right_values.len(), right_count * right_field_count);
    let shared_right = shared_right_fields(
        left.py(),
        right_schema.as_ref(),
        right_field.as_ptr(),
        shared_key,
    )?;
    let Some((left_kind, left_count)) = borrowed_join_source(left) else {
        return Ok(None);
    };

    let mut left_schema = None;
    let mut collision_checked = false;
    let mut joined = Vec::new();
    joined
        .try_reserve(left_count)
        .map_err(join_allocation_error)?;
    let none = left.py().None();
    for left_position in 0..left_count {
        let row = borrowed_join_row(left, left_kind, left_position)?;
        let Some((snapshot, key, _)) =
            snapshot_exact_join_row(left.py(), row, left_field.as_ptr(), &mut left_schema)?
        else {
            return Ok(None);
        };
        if !collision_checked {
            let schema = left_schema
                .as_ref()
                .expect("a validated left row establishes its schema");
            if !schemas_have_no_collision(left.py(), schema, right_schema.as_ref(), &shared_right)?
            {
                return Ok(None);
            }
            collision_checked = true;
        }
        if let Some(right_position) = right_index.get(key) {
            let start = right_position * right_field_count;
            merge_borrowed_exact_join_values(
                left.py(),
                &snapshot,
                &right_values[start..start + right_field_count],
                right_schema.as_ref(),
                &shared_right,
            )?;
            joined.push(snapshot);
        } else if left_join {
            merge_exact_join_unmatched(
                left.py(),
                &snapshot,
                right_schema.as_ref(),
                &shared_right,
                none.as_ptr(),
            )?;
            joined.push(snapshot);
        }
    }
    Ok(Some(joined))
}

#[pyfunction]
/// Borrow exact GIL-owned rows for a unique-right i64 join; free-threaded builds retain v1.
pub(crate) fn join_i64_unique_dict_rows_v2(
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    left_field: &Bound<'_, PyAny>,
    right_field: &Bound<'_, PyAny>,
    left_join: bool,
) -> PyResult<Option<Vec<Py<PyDict>>>> {
    #[cfg(not(Py_GIL_DISABLED))]
    {
        join_i64_unique_dict_rows_borrowed(left, right, left_field, right_field, left_join)
    }
    #[cfg(Py_GIL_DISABLED)]
    {
        join_i64_unique_dict_rows_v1(left, right, left_field, right_field, left_join)
    }
}

#[pyfunction]
/// Speculatively materialize a stable exact-i64 many-right join or return None for fallback.
pub(crate) fn join_i64_many_dict_rows_v1(
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    left_field: &Bound<'_, PyAny>,
    right_field: &Bound<'_, PyAny>,
    left_join: bool,
) -> PyResult<Option<Vec<Py<PyDict>>>> {
    let left_field = match left_field.cast_exact::<PyString>() {
        Ok(field) => field,
        Err(_) => return Ok(None),
    };
    let right_field = match right_field.cast_exact::<PyString>() {
        Ok(field) => field,
        Err(_) => return Ok(None),
    };
    if exact_string_contains_dot(left.py(), left_field.as_ptr())?
        || exact_string_contains_dot(left.py(), right_field.as_ptr())?
    {
        return Ok(None);
    }

    let Some(right_rows) = snapshot_exact_join_source(right)? else {
        return Ok(None);
    };
    let shared_key = exact_string_equal(left.py(), left_field.as_ptr(), right_field.as_ptr())?;
    let compact_right = if let Some(first) = right_rows.first() {
        let Some(field_count) =
            exact_join_row_field_count(left.py(), first.bind(left.py()).as_ptr())?
        else {
            return Ok(None);
        };
        !record_join_prefers_bulk_merge(field_count)
    } else {
        true
    };
    let mut right_schema = None;
    let mut right_records: Vec<Py<PyDict>> = Vec::new();
    if !compact_right {
        right_records
            .try_reserve_exact(right_rows.len())
            .map_err(join_allocation_error)?;
    }
    let mut right_values = Vec::new();
    let mut right_codes = Vec::new();
    right_codes
        .try_reserve_exact(right_rows.len())
        .map_err(join_allocation_error)?;
    let mut group_counts = Vec::new();
    let mut right_groups = DictJoinPositions::new(right_rows.len());
    let mut canonical_right_field_objects = true;

    for (right_position, row) in right_rows.iter().enumerate() {
        let key = if compact_right {
            let Some(key) = snapshot_exact_join_values(
                left.py(),
                row.bind(left.py()).as_ptr(),
                right_field.as_ptr(),
                &mut right_schema,
                &mut right_values,
            )?
            else {
                return Ok(None);
            };
            if right_position == 0 {
                let field_count = right_schema
                    .as_ref()
                    .expect("a validated compact right row establishes its schema")
                    .fields
                    .len();
                reserve_compact_join_values(&mut right_values, right_rows.len(), field_count)?;
            }
            key
        } else {
            let Some((snapshot, key, canonical_field_objects)) = snapshot_exact_join_row(
                left.py(),
                row.bind(left.py()).as_ptr(),
                right_field.as_ptr(),
                &mut right_schema,
            )?
            else {
                return Ok(None);
            };
            canonical_right_field_objects &= canonical_field_objects;
            right_records.push(snapshot);
            key
        };
        let code = if let Some(code) = right_groups.get(key) {
            code
        } else {
            let code = group_counts.len();
            group_counts.try_reserve(1).map_err(join_allocation_error)?;
            if !right_groups.insert_unique(key, code)? {
                unreachable!("a missing factorized key must insert exactly once");
            }
            group_counts.push(0);
            code
        };
        group_counts[code] = checked_join_output_size(group_counts[code], 1)?;
        right_codes.push(code);
    }
    let right_field_count = right_schema
        .as_ref()
        .map_or(0, |schema| schema.fields.len());
    debug_assert!(!compact_right || right_values.len() == right_rows.len() * right_field_count);
    let (group_offsets, right_positions) = factorized_right_positions(&right_codes, &group_counts)?;
    let shared_right = shared_right_fields(
        left.py(),
        right_schema.as_ref(),
        right_field.as_ptr(),
        shared_key,
    )?;
    let bulk_merge_right = !compact_right && canonical_right_field_objects;

    let Some(left_rows) = snapshot_exact_join_source(left)? else {
        return Ok(None);
    };
    let mut left_schema = None;
    let mut collision_checked = false;
    let mut left_records = Vec::new();
    left_records
        .try_reserve_exact(left_rows.len())
        .map_err(join_allocation_error)?;
    for row in &left_rows {
        let Some((snapshot, key, _)) = snapshot_exact_join_row(
            left.py(),
            row.bind(left.py()).as_ptr(),
            left_field.as_ptr(),
            &mut left_schema,
        )?
        else {
            return Ok(None);
        };
        if !collision_checked {
            let schema = left_schema
                .as_ref()
                .expect("a validated left row establishes its schema");
            if !schemas_have_no_collision(left.py(), schema, right_schema.as_ref(), &shared_right)?
            {
                return Ok(None);
            }
            collision_checked = true;
        }
        left_records.push((snapshot, key));
    }

    let mut output_count = 0;
    for (_, key) in &left_records {
        let additional = right_groups
            .get(*key)
            .map_or(usize::from(left_join), |code| group_counts[code]);
        output_count = checked_join_output_size(output_count, additional)?;
    }
    let mut joined = Vec::new();
    joined
        .try_reserve_exact(output_count)
        .map_err(join_allocation_error)?;
    let none = left.py().None();

    for (left_record, key) in left_records {
        if let Some(code) = right_groups.get(key) {
            let start = group_offsets[code];
            let end = group_offsets[code + 1];
            let matches = &right_positions[start..end];
            let mut original = Some(left_record);
            for (match_index, &right_position) in matches.iter().enumerate() {
                let output = if match_index + 1 == matches.len() {
                    original
                        .take()
                        .expect("the final match owns the original left snapshot")
                } else {
                    copy_join_record(
                        left.py(),
                        original
                            .as_ref()
                            .expect("earlier matches retain the original left snapshot"),
                    )?
                };
                if compact_right {
                    let start = right_position * right_field_count;
                    merge_exact_join_values(
                        left.py(),
                        &output,
                        &right_values[start..start + right_field_count],
                        right_schema.as_ref(),
                        &shared_right,
                    )?;
                } else if merge_exact_join_match(
                    left.py(),
                    &output,
                    &right_records[right_position],
                    right_schema.as_ref(),
                    &shared_right,
                    bulk_merge_right,
                )?
                .is_none()
                {
                    return Ok(None);
                }
                joined.push(output);
            }
        } else if left_join {
            merge_exact_join_unmatched(
                left.py(),
                &left_record,
                right_schema.as_ref(),
                &shared_right,
                none.as_ptr(),
            )?;
            joined.push(left_record);
        }
    }
    debug_assert_eq!(joined.len(), output_count);
    Ok(Some(joined))
}
