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
mod group_callable;
mod group_numeric;
mod join_callable;
mod join_exact;
mod join_i64;

pub(crate) use adapters::standard_namedtuple_record_adapter_v1;
pub(crate) use group_callable::{
    group_count_sum_callable_key_dict_rows_v1, group_count_sum_callable_value_dict_rows_v1,
};
#[cfg(test)]
pub(crate) use group_numeric::group_sum_pairs;
pub(crate) use group_numeric::{
    group_sum_i64_dict_rows, group_sum_i64_dict_rows_v1, group_sum_i64_exact_pairs_v1,
    group_sum_i64_pairs, group_sum_i64_rows_v1,
};
pub(crate) use join_callable::{
    join_hashable_many_direct_records_v1, join_hashable_many_records_v1,
    join_hashable_many_records_v2, join_hashable_unique_direct_records_v1,
    join_hashable_unique_records_v1, join_hashable_unique_records_v2,
};
pub(crate) use join_exact::checked_join_output_size;
pub(crate) use join_i64::{join_i64_many_dict_rows_v1, join_i64_unique_dict_rows_v1};

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
pub(crate) const RECORD_JOIN_V1_MAX_FIELDS: usize = 24;
const CALLABLE_RIGHT_SCHEMA_CACHE_MIN_ROWS: usize = 20_000;
const CALLABLE_RIGHT_SCHEMA_CACHE_MIN_FIELDS: usize = 8;
const CALLABLE_RIGHT_SCHEMA_VALUE_CACHE_MIN_ROWS: usize = 40_000;
const CALLABLE_RIGHT_SCHEMA_VALUE_CACHE_MIN_FIELDS: usize = 16;
const CALLABLE_GROUP_HASH_WARMUP_ROWS: usize = 32;
// Callable suffix-prefix width-8 many A/B regressed 2.54%; admit only the measured wide layout.
const CALLABLE_JOIN_BULK_MERGE_MIN_FIELDS: usize = 24;
// CPython 3.11/3.13/3.14 A/B only found a consistent PyDict_Merge win at the maximum width.
// Narrower build rows use the lower-allocation compact value snapshot instead.
const RECORD_JOIN_BULK_MERGE_MIN_FIELDS: usize = 24;

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
pub(crate) struct CallableGroupHashProbeCounts {
    pub(crate) explicit_hashes: usize,
    pub(crate) elided_hashes: usize,
    pub(crate) successful_warmup_rows: usize,
}

#[cfg(test)]
thread_local! {
    static CALLABLE_RIGHT_SCHEMA_PROBE_COUNTS: Cell<Option<CallableRightSchemaProbeCounts>> =
        const { Cell::new(None) };
}

#[cfg(test)]
thread_local! {
    static CALLABLE_GROUP_HASH_PROBE_COUNTS: Cell<Option<CallableGroupHashProbeCounts>> =
        const { Cell::new(None) };
}

#[cfg(test)]
pub(crate) fn begin_callable_group_hash_probe_count() {
    CALLABLE_GROUP_HASH_PROBE_COUNTS.with(|counts| {
        assert!(
            counts
                .replace(Some(CallableGroupHashProbeCounts::default()))
                .is_none(),
            "callable group hash probe counting cannot be nested"
        );
    });
}

#[cfg(test)]
pub(crate) fn end_callable_group_hash_probe_count() -> CallableGroupHashProbeCounts {
    CALLABLE_GROUP_HASH_PROBE_COUNTS.with(|counts| {
        counts
            .take()
            .expect("callable group hash probe counting must be started before it is ended")
    })
}

#[cfg(test)]
fn record_callable_group_explicit_hash() {
    CALLABLE_GROUP_HASH_PROBE_COUNTS.with(|counts| {
        if let Some(mut current) = counts.get() {
            current.explicit_hashes += 1;
            counts.set(Some(current));
        }
    });
}

#[cfg(test)]
fn record_callable_group_elided_hash() {
    CALLABLE_GROUP_HASH_PROBE_COUNTS.with(|counts| {
        if let Some(mut current) = counts.get() {
            current.elided_hashes += 1;
            counts.set(Some(current));
        }
    });
}

#[cfg(test)]
fn record_callable_group_successful_warmup_row() {
    CALLABLE_GROUP_HASH_PROBE_COUNTS.with(|counts| {
        if let Some(mut current) = counts.get() {
            current.successful_warmup_rows += 1;
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
