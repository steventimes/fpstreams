//! Exact native kernels for structurally proven relational operations.

use pyo3::exceptions::{PyAttributeError, PyKeyError, PyMemoryError, PyTypeError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::critical_section::with_critical_section;
use pyo3::types::{
    PyDict, PyDictMethods, PyFrozenSet, PyFrozenSetMethods, PyList, PySet, PySetMethods, PyString,
    PyTuple, PyTupleMethods, PyType, PyTypeMethods,
};
#[cfg(not(Py_GIL_DISABLED))]
use pyo3::types::{PyFunction, PyInt};
#[cfg(not(Py_GIL_DISABLED))]
use pyo3::{PyTraverseError, PyVisit};
#[cfg(test)]
use std::cell::Cell;
use std::collections::HashMap;
use std::collections::TryReserveError;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
#[cfg(not(Py_GIL_DISABLED))]
use std::sync::atomic::{AtomicPtr, Ordering};

mod adapters;
mod global_numeric;
mod group_numeric;
mod group_pair_expr;
mod join_callable;
mod join_exact;
mod join_i64;

#[cfg(not(Py_GIL_DISABLED))]
pub(crate) use adapters::exact_python_function_code;
pub(crate) use adapters::standard_namedtuple_record_adapter_v1;
pub(crate) use global_numeric::global_sum_i64_dict_rows_v1;
#[cfg(test)]
pub(crate) use group_numeric::group_sum_pairs;
pub(crate) use group_numeric::{
    SeededI64BuildHasher, group_sum_i64_dict_rows, group_sum_i64_dict_rows_v1,
    group_sum_i64_exact_pairs_v1, group_sum_i64_exact_pairs_v2, group_sum_i64_pairs,
    group_sum_i64_rows_v1,
};
pub(crate) use group_pair_expr::group_sum_i64_pair_expr_rows_v1;
pub(crate) use join_callable::{
    join_hashable_many_direct_records_v1, join_hashable_many_records_v1,
    join_hashable_many_records_v2, join_hashable_unique_direct_records_v1,
    join_hashable_unique_records_v1, join_hashable_unique_records_v2,
};
pub(crate) use join_exact::checked_join_output_size;
pub(crate) use join_i64::{
    join_i64_many_dict_rows_v1, join_i64_unique_dict_rows_v1, join_i64_unique_dict_rows_v2,
};

// Dense slots are selected only when their allocation stays proportional to the
// already-extracted pair buffer. The absolute cap prevents a large transient allocation
// even for a very large input with a wide key range.
const MAX_DENSE_GROUP_SLOTS: usize = 1 << 20;
const MAX_DENSE_SLOTS_PER_ROW: usize = 2;
const MAX_INITIAL_RECORD_DENSE_SLOTS: usize = 1 << 14;
const MAX_RECORD_DENSE_GROWTH_FACTOR: usize = 4;
const MAX_INITIAL_RECORD_JOIN_VALUES: usize = 1 << 20;
const OBJECT_KEY_CACHE_SLOTS: usize = 128;
const GROUP_SUM_FINAL_ROWS_THRESHOLD: usize = 1 << 12;
pub(crate) const RECORD_GROUP_SUM_MAX_FIELDS: usize = 24;
pub(crate) const RECORD_JOIN_V1_MAX_FIELDS: usize = 64;
const CALLABLE_RIGHT_SCHEMA_CACHE_MIN_ROWS: usize = 20_000;
const CALLABLE_RIGHT_SCHEMA_CACHE_MIN_FIELDS: usize = 8;
const CALLABLE_RIGHT_SCHEMA_VALUE_CACHE_MIN_ROWS: usize = 40_000;
const CALLABLE_RIGHT_SCHEMA_VALUE_CACHE_MIN_FIELDS: usize = 16;
// Bulk merge is reserved for wide schemas; callable joins additionally validate the collision
// layout before selecting it.
const CALLABLE_JOIN_BULK_MERGE_MIN_FIELDS: usize = 24;
// Exact record joins use the same cross-version width boundary and require a canonical schema at
// the call site before bulk merge is admitted.
const RECORD_JOIN_BULK_MERGE_MIN_FIELDS: usize = 24;

#[inline]
pub(crate) fn set_widened_i64_item(
    row: &Bound<'_, PyDict>,
    name: &Bound<'_, PyString>,
    total: i128,
) -> PyResult<()> {
    if let Ok(narrow) = i64::try_from(total) {
        return row.set_item(name, narrow);
    }
    row.set_item(name, total)
}

#[inline]
fn record_join_prefers_bulk_merge(field_count: usize) -> bool {
    field_count >= RECORD_JOIN_BULK_MERGE_MIN_FIELDS
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct CallableRightSchemaProbeCounts {
    pub(crate) full_field_probes: usize,
    pub(crate) identity_cache_hits: usize,
    pub(crate) value_cache_hits: usize,
    pub(crate) bulk_merge_hits: usize,
    pub(crate) mapping_proxy_snapshot_hits: usize,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RecordJoinProbeCounts {
    pub(crate) bulk_merge_hits: usize,
}

#[cfg(test)]
thread_local! {
    static CALLABLE_RIGHT_SCHEMA_PROBE_COUNTS: Cell<Option<CallableRightSchemaProbeCounts>> =
        const { Cell::new(None) };
}

#[cfg(test)]
thread_local! {
    static RECORD_JOIN_PROBE_COUNTS: Cell<Option<RecordJoinProbeCounts>> =
        const { Cell::new(None) };
}

#[cfg(test)]
pub(crate) fn begin_record_join_probe_count() {
    RECORD_JOIN_PROBE_COUNTS.with(|counts| {
        assert!(
            counts
                .replace(Some(RecordJoinProbeCounts::default()))
                .is_none(),
            "record join probe counting cannot be nested"
        );
    });
}

#[cfg(test)]
pub(crate) fn end_record_join_probe_count() -> RecordJoinProbeCounts {
    RECORD_JOIN_PROBE_COUNTS.with(|counts| {
        counts
            .take()
            .expect("record join probe counting must be started before it is ended")
    })
}

#[cfg(test)]
fn record_join_bulk_merge_hit() {
    RECORD_JOIN_PROBE_COUNTS.with(|counts| {
        if let Some(mut current) = counts.get() {
            current.bulk_merge_hits += 1;
            counts.set(Some(current));
        }
    });
}

#[cfg(test)]
pub(crate) fn begin_callable_right_schema_probe_count() {
    CALLABLE_RIGHT_SCHEMA_PROBE_COUNTS.with(|counts| {
        assert!(
            counts
                .replace(Some(CallableRightSchemaProbeCounts::default()))
                .is_none(),
            "callable right-schema probe counting cannot be nested"
        );
    });
}

#[cfg(test)]
pub(crate) fn end_callable_right_schema_probe_count() -> CallableRightSchemaProbeCounts {
    CALLABLE_RIGHT_SCHEMA_PROBE_COUNTS.with(|counts| {
        counts
            .take()
            .expect("callable right-schema probe counting must be started before it is ended")
    })
}

#[cfg(test)]
fn record_callable_right_schema_full_field_probe() {
    CALLABLE_RIGHT_SCHEMA_PROBE_COUNTS.with(|counts| {
        if let Some(mut current) = counts.get() {
            current.full_field_probes += 1;
            counts.set(Some(current));
        }
    });
}

#[cfg(test)]
fn record_callable_right_schema_identity_cache_hit() {
    CALLABLE_RIGHT_SCHEMA_PROBE_COUNTS.with(|counts| {
        if let Some(mut current) = counts.get() {
            current.identity_cache_hits += 1;
            counts.set(Some(current));
        }
    });
}

#[cfg(test)]
fn record_callable_right_schema_value_cache_hit() {
    CALLABLE_RIGHT_SCHEMA_PROBE_COUNTS.with(|counts| {
        if let Some(mut current) = counts.get() {
            current.value_cache_hits += 1;
            counts.set(Some(current));
        }
    });
}

#[cfg(test)]
fn record_callable_join_bulk_merge_hit() {
    CALLABLE_RIGHT_SCHEMA_PROBE_COUNTS.with(|counts| {
        if let Some(mut current) = counts.get() {
            current.bulk_merge_hits += 1;
            counts.set(Some(current));
        }
    });
}

#[cfg(test)]
fn record_mapping_proxy_snapshot_hit() {
    CALLABLE_RIGHT_SCHEMA_PROBE_COUNTS.with(|counts| {
        if let Some(mut current) = counts.get() {
            current.mapping_proxy_snapshot_hits += 1;
            counts.set(Some(current));
        }
    });
}
