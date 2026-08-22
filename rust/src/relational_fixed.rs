//! Fixed-schema exact native grouping kernels isolated from legacy single-sum codegen.

use crate::relational::RECORD_GROUP_SUM_MAX_FIELDS;
use pyo3::exceptions::PyMemoryError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::critical_section::with_critical_section;
use pyo3::types::{PyDict, PyDictMethods, PyList, PyModule, PyString, PyTuple, PyTupleMethods};
use std::collections::hash_map::{Entry, RandomState};
use std::collections::{HashMap, TryReserveError};
use std::hash::{BuildHasher, Hasher};

#[cfg(test)]
use std::cell::Cell;

const MAX_DENSE_GROUP_SLOTS: usize = 1 << 20;
const MAX_DENSE_SLOTS_PER_ROW: usize = 2;
const MAX_INITIAL_RECORD_DENSE_SLOTS: usize = 1 << 14;
const MAX_RECORD_DENSE_GROWTH_FACTOR: usize = 4;
const OBJECT_KEY_CACHE_SLOTS: usize = 128;
const GROUP_SUM_FINAL_ROWS_THRESHOLD: usize = 1 << 12;

#[cfg(test)]
thread_local! {
    static I64_HASH_PROBE_COUNT: Cell<Option<usize>> = const { Cell::new(None) };
}

#[cfg(test)]
fn begin_i64_hash_probe_count() {
    I64_HASH_PROBE_COUNT.with(|count| {
        assert!(count.replace(Some(0)).is_none());
    });
}

#[cfg(test)]
fn end_i64_hash_probe_count() -> usize {
    I64_HASH_PROBE_COUNT.with(|count| {
        count
            .replace(None)
            .expect("hash probe counting must be started before it is ended")
    })
}

/// Per-table randomized, non-cryptographic hashing specialized for proven i64 keys.
///
/// The per-table random seed makes precomputed collision patterns less reusable across
/// tables; this intentionally does not claim SipHash's adaptive denial-of-service resistance.
#[derive(Clone)]
struct SeededI64BuildHasher {
    seed_a: u64,
    seed_b: u64,
}

impl SeededI64BuildHasher {
    fn random() -> Self {
        let entropy = RandomState::new();
        Self {
            seed_a: entropy.hash_one(0x6a09_e667_f3bc_c909_u64),
            seed_b: entropy.hash_one(0xbb67_ae85_84ca_a73b_u64),
        }
    }
}

struct SeededI64Hasher {
    seed_a: u64,
    seed_b: u64,
    state: u64,
}

impl SeededI64Hasher {
    #[inline]
    fn avalanche(mut value: u64) -> u64 {
        value ^= value >> 30;
        value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value ^= value >> 27;
        value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    #[inline]
    fn hash_integer(&mut self, value: u64) {
        #[cfg(test)]
        I64_HASH_PROBE_COUNT.with(|count| {
            if let Some(current) = count.get() {
                count.set(Some(current + 1));
            }
        });
        let product = u128::from(value ^ self.seed_a) * u128::from(self.seed_b | 1);
        self.state = product as u64 ^ (product >> 64) as u64;
    }
}

impl Hasher for SeededI64Hasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.state
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut value =
            self.state ^ (bytes.len() as u64).wrapping_mul(self.seed_b | 1) ^ self.seed_a;
        for &byte in bytes {
            value ^= u64::from(byte);
            value = value.wrapping_mul(0x100_0000_01b3);
        }
        self.state = Self::avalanche(value ^ self.seed_b);
    }

    #[inline]
    fn write_i64(&mut self, value: i64) {
        self.hash_integer(value as u64);
    }

    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.hash_integer(value);
    }
}

impl BuildHasher for SeededI64BuildHasher {
    type Hasher = SeededI64Hasher;

    #[inline]
    fn build_hasher(&self) -> Self::Hasher {
        SeededI64Hasher {
            seed_a: self.seed_a,
            seed_b: self.seed_b,
            state: 0,
        }
    }
}

type FastI64PositionMap = HashMap<i64, usize, SeededI64BuildHasher>;

/// First Python key object and its widened integer sum, in encounter order.
type ObjectKeyGroups = Vec<(Py<PyAny>, i128)>;

/// Translate a Python-style positive or negative tuple index without invoking Python code.
fn normalize_index(index: isize, width: usize) -> Option<usize> {
    let width = isize::try_from(width).ok()?;
    let normalized = if index < 0 {
        width.checked_add(index)?
    } else {
        index
    };
    (0..width)
        .contains(&normalized)
        .then_some(normalized as usize)
}

/// Convert a fallible Rust container reservation into Python's allocation error.
fn group_allocation_error(_error: TryReserveError) -> PyErr {
    PyMemoryError::new_err("native grouping allocation failed")
}

/// Allocate a dictionary without converting a genuine Python allocation error into a panic.
fn new_dict_fallible<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
    // SAFETY: PyDict_New returns either a new owned exact dict or null with an exception set.
    let dictionary = unsafe { Bound::<PyAny>::from_owned_ptr_or_err(py, ffi::PyDict_New())? };
    // SAFETY: the non-null result of PyDict_New is always an exact dictionary.
    Ok(unsafe { dictionary.cast_into_unchecked() })
}

/// Adaptive exact-integer index kept outside Python containers and critical sections.
enum I64GroupPositions {
    Dense(Vec<usize>),
    Hash(FastI64PositionMap),
}

impl I64GroupPositions {
    /// Find a key while adaptively retaining dense slots only for a compact non-negative range.
    fn position<I>(
        &mut self,
        key: i64,
        dense_limit: usize,
        group_keys: I,
    ) -> PyResult<Option<usize>>
    where
        I: ExactSizeIterator<Item = i64>,
    {
        if let Ok(index) = usize::try_from(key)
            && let Self::Dense(slots) = self
        {
            let required = index.saturating_add(1);
            let growth_limit = slots
                .len()
                .saturating_mul(MAX_RECORD_DENSE_GROWTH_FACTOR)
                .max(MAX_INITIAL_RECORD_DENSE_SLOTS);
            if required <= dense_limit && required <= growth_limit {
                if required > slots.len() {
                    slots
                        .try_reserve(required - slots.len())
                        .map_err(group_allocation_error)?;
                    let new_len = slots.capacity().min(dense_limit).max(required);
                    slots.resize(new_len, usize::MAX);
                }
                let position = slots[index];
                return Ok((position != usize::MAX).then_some(position));
            }
        }

        if matches!(self, Self::Dense(_)) {
            let mut positions = FastI64PositionMap::with_hasher(SeededI64BuildHasher::random());
            positions
                .try_reserve(group_keys.len())
                .map_err(group_allocation_error)?;
            for (position, existing_key) in group_keys.enumerate() {
                positions.insert(existing_key, position);
            }
            *self = Self::Hash(positions);
        }
        let Self::Hash(positions) = self else {
            unreachable!("dense positions must migrate before a sparse lookup")
        };
        Ok(positions.get(&key).copied())
    }

    /// Reserve storage for one new hashed group; dense storage is prepared by `position`.
    fn try_reserve_group(&mut self) -> PyResult<()> {
        if let Self::Hash(positions) = self {
            positions.try_reserve(1).map_err(group_allocation_error)?;
        }
        Ok(())
    }

    /// Record a position after all fallible reservations have succeeded.
    fn insert(&mut self, key: i64, position: usize) {
        match self {
            Self::Dense(slots) => {
                let index =
                    usize::try_from(key).expect("a dense key was validated before insertion");
                debug_assert_eq!(slots[index], usize::MAX);
                slots[index] = position;
            }
            Self::Hash(positions) => {
                positions.insert(key, position);
            }
        }
    }
}

/// Mutable aggregation state kept outside Python containers and their critical sections.
#[derive(Clone, Copy)]
struct ObjectKeyCacheEntry {
    object: *mut ffi::PyObject,
    position: usize,
}

struct ObjectKeyGroupState {
    positions: I64GroupPositions,
    group_keys: Vec<i64>,
    groups: ObjectKeyGroups,
    dense_limit: usize,
    object_cache: [ObjectKeyCacheEntry; OBJECT_KEY_CACHE_SLOTS],
}

impl ObjectKeyGroupState {
    fn new(row_count: usize) -> Self {
        Self {
            positions: I64GroupPositions::Dense(Vec::new()),
            group_keys: Vec::new(),
            groups: Vec::new(),
            dense_limit: row_count
                .saturating_mul(MAX_DENSE_SLOTS_PER_ROW)
                .min(MAX_DENSE_GROUP_SLOTS),
            object_cache: [ObjectKeyCacheEntry {
                object: core::ptr::null_mut(),
                position: usize::MAX,
            }; OBJECT_KEY_CACHE_SLOTS],
        }
    }

    /// Return a retained first-key position through a tiny direct-mapped identity cache.
    #[inline]
    fn cached_position(&self, key_object: *mut ffi::PyObject) -> Option<usize> {
        let slot = ((key_object as usize) >> 4) & (OBJECT_KEY_CACHE_SLOTS - 1);
        let entry = self.object_cache[slot];
        (entry.object == key_object).then_some(entry.position)
    }

    /// Remember only a key object already held strongly by `groups`.
    #[inline]
    fn remember_first_object(&mut self, key_object: *mut ffi::PyObject, position: usize) {
        debug_assert!(position < OBJECT_KEY_CACHE_SLOTS);
        let slot = ((key_object as usize) >> 4) & (OBJECT_KEY_CACHE_SLOTS - 1);
        self.object_cache[slot] = ObjectKeyCacheEntry {
            object: key_object,
            position,
        };
    }
}

/// Fetch a borrowed exact-dict value, preserving genuine Python errors.
fn dict_item(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    field: *mut ffi::PyObject,
) -> PyResult<Option<*mut ffi::PyObject>> {
    // SAFETY: both pointers are live Python objects and the caller holds the row's critical
    // section. PyDict_GetItemWithError returns a borrowed pointer or sets a Python exception.
    let item = unsafe { ffi::PyDict_GetItemWithError(row, field) };
    if !item.is_null() {
        return Ok(Some(item));
    }
    // SAFETY: querying the attached thread's error indicator has no ownership effect.
    if unsafe { ffi::PyErr_Occurred() }.is_null() {
        Ok(None)
    } else {
        Err(PyErr::fetch(py))
    }
}

/// Convert one exact Python int to i64 without allocating an intermediate Bound object.
fn exact_i64(py: Python<'_>, value: *mut ffi::PyObject) -> PyResult<Option<i64>> {
    // SAFETY: value is a live borrowed Python object. The exact check excludes bool and every
    // int subclass before conversion can invoke any subclass behavior.
    if unsafe { ffi::PyLong_CheckExact(value) } == 0 {
        return Ok(None);
    }
    let mut overflow = 0;
    // SAFETY: PyLong_AsLongLongAndOverflow accepts the exact PyLong proven above.
    let extracted = unsafe { ffi::PyLong_AsLongLongAndOverflow(value, &mut overflow) };
    // The overflow channel deliberately does not set an exception. Exact PyLongs cannot invoke
    // __index__, so the only remaining error sentinel worth disambiguating is a returned -1.
    if overflow != 0 {
        return Ok(None);
    }
    // SAFETY: reading the current exception indicator is valid while attached to Python. C-API
    // conversion errors return -1, while every other value proves that no error was raised.
    if extracted == -1 && !unsafe { ffi::PyErr_Occurred() }.is_null() {
        return Err(PyErr::fetch(py));
    }
    Ok(Some(extracted))
}

/// Take stable strong references while an exact list is locked.
#[cfg(Py_GIL_DISABLED)]
fn snapshot_exact_list_rows(
    py: Python<'_>,
    source: &Bound<'_, PyAny>,
    rows: &Bound<'_, PyList>,
) -> PyResult<Vec<Py<PyAny>>> {
    with_critical_section(source, || {
        let row_count = rows.len();
        let mut snapshot = Vec::new();
        snapshot
            .try_reserve(row_count)
            .map_err(group_allocation_error)?;
        for index in 0..row_count {
            // SAFETY: the exact list stays locked for the entire snapshot and index is below
            // its locked length. Taking an owned reference before unlocking prevents a
            // concurrent deletion or replacement from invalidating the row pointer.
            let row = unsafe { ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) };
            if row.is_null() {
                return Err(PyErr::fetch(py));
            }
            // SAFETY: row is a borrowed reference held live by the locked list. to_owned takes
            // the strong reference that keeps it live while individual row dicts are processed.
            snapshot.push(unsafe { Borrowed::from_ptr(py, row).to_owned().unbind() });
        }
        Ok(snapshot)
    })
}

impl ObjectKeyGroupState {
    /// Count one row in an existing group without exposing a partially-updated lane.
    #[inline]
    fn add_fixed_count_at_position(counts: &mut [usize], position: usize) -> Option<()> {
        let next = counts[position].checked_add(1)?;
        counts[position] = next;
        Some(())
    }

    /// Count and sum one row in an existing group after both overflow checks succeed.
    #[inline]
    fn add_fixed_count_sum_at_position(
        &mut self,
        counts: &mut [usize],
        position: usize,
        value: i64,
    ) -> Option<()> {
        let next_count = counts[position].checked_add(1)?;
        let next_total = self.groups[position].1.checked_add(i128::from(value))?;
        counts[position] = next_count;
        self.groups[position].1 = next_total;
        Some(())
    }

    /// Add one exact-i64 key to the fixed count lane.
    #[inline(always)]
    fn add_fixed_count<const USE_OBJECT_CACHE: bool>(
        &mut self,
        counts: &mut Vec<usize>,
        py: Python<'_>,
        key_object: *mut ffi::PyObject,
        key: i64,
    ) -> PyResult<Option<()>> {
        if let I64GroupPositions::Hash(positions) = &mut self.positions {
            let needs_hash_reserve = positions.len() == positions.capacity();
            match positions.entry(key) {
                Entry::Occupied(entry) => {
                    let position = *entry.get();
                    let result = Self::add_fixed_count_at_position(counts, position);
                    if USE_OBJECT_CACHE && self.groups[position].0.bind(py).as_ptr() == key_object {
                        self.remember_first_object(key_object, position);
                    }
                    return Ok(result);
                }
                Entry::Vacant(entry) if !needs_hash_reserve => {
                    self.group_keys
                        .try_reserve(1)
                        .map_err(group_allocation_error)?;
                    self.groups.try_reserve(1).map_err(group_allocation_error)?;
                    counts.try_reserve(1).map_err(group_allocation_error)?;
                    // SAFETY: the caller keeps key_object live until the retained reference is
                    // created; groups then keeps the identity-cache pointer live.
                    let first_key =
                        unsafe { Borrowed::from_ptr(py, key_object).to_owned().unbind() };
                    let position = self.groups.len();
                    entry.insert(position);
                    self.group_keys.push(key);
                    self.groups.push((first_key, 0));
                    counts.push(1);
                    if USE_OBJECT_CACHE && position < OBJECT_KEY_CACHE_SLOTS {
                        self.remember_first_object(key_object, position);
                    }
                    return Ok(Some(()));
                }
                Entry::Vacant(entry) => {
                    let vacant_key = entry.into_key();
                    positions.try_reserve(1).map_err(group_allocation_error)?;
                    self.group_keys
                        .try_reserve(1)
                        .map_err(group_allocation_error)?;
                    self.groups.try_reserve(1).map_err(group_allocation_error)?;
                    counts.try_reserve(1).map_err(group_allocation_error)?;
                    // SAFETY: see the allocation-free vacant branch above.
                    let first_key =
                        unsafe { Borrowed::from_ptr(py, key_object).to_owned().unbind() };
                    let position = self.groups.len();
                    positions.insert(vacant_key, position);
                    self.group_keys.push(key);
                    self.groups.push((first_key, 0));
                    counts.push(1);
                    if USE_OBJECT_CACHE && position < OBJECT_KEY_CACHE_SLOTS {
                        self.remember_first_object(key_object, position);
                    }
                    return Ok(Some(()));
                }
            }
        }

        if let Some(position) =
            self.positions
                .position(key, self.dense_limit, self.group_keys.iter().copied())?
        {
            let result = Self::add_fixed_count_at_position(counts, position);
            if USE_OBJECT_CACHE && self.groups[position].0.bind(py).as_ptr() == key_object {
                self.remember_first_object(key_object, position);
            }
            return Ok(result);
        }

        self.positions.try_reserve_group()?;
        self.group_keys
            .try_reserve(1)
            .map_err(group_allocation_error)?;
        self.groups.try_reserve(1).map_err(group_allocation_error)?;
        counts.try_reserve(1).map_err(group_allocation_error)?;
        // SAFETY: the exact tuple owns key_object or the caller holds the exact row dict's
        // critical section. The retained key keeps the identity cache pointer live.
        let first_key = unsafe { Borrowed::from_ptr(py, key_object).to_owned().unbind() };
        let position = self.groups.len();
        self.positions.insert(key, position);
        self.group_keys.push(key);
        self.groups.push((first_key, 0));
        counts.push(1);
        if USE_OBJECT_CACHE && position < OBJECT_KEY_CACHE_SLOTS {
            self.remember_first_object(key_object, position);
        }
        Ok(Some(()))
    }

    /// Add one exact-i64 key/value row to parallel fixed count and widened-sum lanes.
    #[inline(always)]
    fn add_fixed_count_sum<const USE_OBJECT_CACHE: bool>(
        &mut self,
        counts: &mut Vec<usize>,
        py: Python<'_>,
        key_object: *mut ffi::PyObject,
        key: i64,
        value: i64,
    ) -> PyResult<Option<()>> {
        if let I64GroupPositions::Hash(positions) = &mut self.positions {
            let needs_hash_reserve = positions.len() == positions.capacity();
            match positions.entry(key) {
                Entry::Occupied(entry) => {
                    let position = *entry.get();
                    let result = self.add_fixed_count_sum_at_position(counts, position, value);
                    if USE_OBJECT_CACHE && self.groups[position].0.bind(py).as_ptr() == key_object {
                        self.remember_first_object(key_object, position);
                    }
                    return Ok(result);
                }
                Entry::Vacant(entry) if !needs_hash_reserve => {
                    self.group_keys
                        .try_reserve(1)
                        .map_err(group_allocation_error)?;
                    self.groups.try_reserve(1).map_err(group_allocation_error)?;
                    counts.try_reserve(1).map_err(group_allocation_error)?;
                    // SAFETY: the caller keeps key_object live until the retained reference is
                    // created; groups then keeps the identity-cache pointer live.
                    let first_key =
                        unsafe { Borrowed::from_ptr(py, key_object).to_owned().unbind() };
                    let position = self.groups.len();
                    entry.insert(position);
                    self.group_keys.push(key);
                    self.groups.push((first_key, i128::from(value)));
                    counts.push(1);
                    if USE_OBJECT_CACHE && position < OBJECT_KEY_CACHE_SLOTS {
                        self.remember_first_object(key_object, position);
                    }
                    return Ok(Some(()));
                }
                Entry::Vacant(entry) => {
                    let vacant_key = entry.into_key();
                    positions.try_reserve(1).map_err(group_allocation_error)?;
                    self.group_keys
                        .try_reserve(1)
                        .map_err(group_allocation_error)?;
                    self.groups.try_reserve(1).map_err(group_allocation_error)?;
                    counts.try_reserve(1).map_err(group_allocation_error)?;
                    // SAFETY: see the allocation-free vacant branch above.
                    let first_key =
                        unsafe { Borrowed::from_ptr(py, key_object).to_owned().unbind() };
                    let position = self.groups.len();
                    positions.insert(vacant_key, position);
                    self.group_keys.push(key);
                    self.groups.push((first_key, i128::from(value)));
                    counts.push(1);
                    if USE_OBJECT_CACHE && position < OBJECT_KEY_CACHE_SLOTS {
                        self.remember_first_object(key_object, position);
                    }
                    return Ok(Some(()));
                }
            }
        }

        if let Some(position) =
            self.positions
                .position(key, self.dense_limit, self.group_keys.iter().copied())?
        {
            let result = self.add_fixed_count_sum_at_position(counts, position, value);
            if USE_OBJECT_CACHE && self.groups[position].0.bind(py).as_ptr() == key_object {
                self.remember_first_object(key_object, position);
            }
            return Ok(result);
        }

        self.positions.try_reserve_group()?;
        self.group_keys
            .try_reserve(1)
            .map_err(group_allocation_error)?;
        self.groups.try_reserve(1).map_err(group_allocation_error)?;
        counts.try_reserve(1).map_err(group_allocation_error)?;
        // SAFETY: the exact tuple owns key_object or the caller holds the exact row dict's
        // critical section. The retained key keeps the identity cache pointer live.
        let first_key = unsafe { Borrowed::from_ptr(py, key_object).to_owned().unbind() };
        let position = self.groups.len();
        self.positions.insert(key, position);
        self.group_keys.push(key);
        self.groups.push((first_key, i128::from(value)));
        counts.push(1);
        if USE_OBJECT_CACHE && position < OBJECT_KEY_CACHE_SLOTS {
            self.remember_first_object(key_object, position);
        }
        Ok(Some(()))
    }
}

/// Count one exact-i64 key, reusing a retained first-key identity when possible.
#[inline(always)]
fn add_fixed_exact_i64_key<const USE_OBJECT_CACHE: bool>(
    py: Python<'_>,
    key_object: *mut ffi::PyObject,
    state: &mut ObjectKeyGroupState,
    counts: &mut Vec<usize>,
) -> PyResult<Option<()>> {
    if USE_OBJECT_CACHE && let Some(position) = state.cached_position(key_object) {
        return Ok(ObjectKeyGroupState::add_fixed_count_at_position(
            counts, position,
        ));
    }
    let Some(key) = exact_i64(py, key_object)? else {
        return Ok(None);
    };
    state.add_fixed_count::<USE_OBJECT_CACHE>(counts, py, key_object, key)
}

/// Count and sum one exact-i64 row, validating the value even on an identity-cache hit.
#[inline(always)]
fn add_fixed_exact_i64_key_value<const USE_OBJECT_CACHE: bool>(
    py: Python<'_>,
    key_object: *mut ffi::PyObject,
    value_object: *mut ffi::PyObject,
    state: &mut ObjectKeyGroupState,
    counts: &mut Vec<usize>,
) -> PyResult<Option<()>> {
    if USE_OBJECT_CACHE && let Some(position) = state.cached_position(key_object) {
        let Some(value) = exact_i64(py, value_object)? else {
            return Ok(None);
        };
        return Ok(state.add_fixed_count_sum_at_position(counts, position, value));
    }
    let Some(key) = exact_i64(py, key_object)? else {
        return Ok(None);
    };
    let Some(value) = exact_i64(py, value_object)? else {
        return Ok(None);
    };
    state.add_fixed_count_sum::<USE_OBJECT_CACHE>(counts, py, key_object, key, value)
}

/// Validate and consume one exact tuple row in a compile-time-selected fixed aggregation mode.
#[inline(always)]
fn group_fixed_exact_tuple_row<const WITH_SUM: bool, const USE_OBJECT_CACHE: bool>(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    key_index: isize,
    value_index: isize,
    cached_layout: &mut Option<(usize, usize, usize)>,
    state: &mut ObjectKeyGroupState,
    counts: &mut Vec<usize>,
) -> PyResult<Option<()>> {
    // SAFETY: row is live through the GIL, the immutable outer tuple, or the owned exact-list
    // snapshot retained by the caller.
    if unsafe { ffi::PyTuple_CheckExact(row) } == 0 {
        return Ok(None);
    }
    // SAFETY: row was proven to be an exact tuple.
    let width = unsafe { ffi::PyTuple_Size(row) };
    if width < 0 {
        return Err(PyErr::fetch(py));
    }
    let width = width as usize;
    let (key_position, value_position) = match *cached_layout {
        Some((cached_width, key_position, value_position)) if cached_width == width => {
            (key_position, value_position)
        }
        _ => {
            let Some(key_position) = normalize_index(key_index, width) else {
                return Ok(None);
            };
            let value_position = if WITH_SUM {
                let Some(value_position) = normalize_index(value_index, width) else {
                    return Ok(None);
                };
                value_position
            } else {
                0
            };
            *cached_layout = Some((width, key_position, value_position));
            (key_position, value_position)
        }
    };
    // SAFETY: key_position was normalized against this exact tuple's fixed width.
    let key_object = unsafe { ffi::PyTuple_GetItem(row, key_position as ffi::Py_ssize_t) };
    if key_object.is_null() {
        return Err(PyErr::fetch(py));
    }
    if WITH_SUM {
        // SAFETY: the compile-time sum branch proves value_position was normalized above.
        let value_object = unsafe { ffi::PyTuple_GetItem(row, value_position as ffi::Py_ssize_t) };
        if value_object.is_null() {
            return Err(PyErr::fetch(py));
        }
        add_fixed_exact_i64_key_value::<USE_OBJECT_CACHE>(
            py,
            key_object,
            value_object,
            state,
            counts,
        )
    } else {
        add_fixed_exact_i64_key::<USE_OBJECT_CACHE>(py, key_object, state, counts)
    }
}

/// Scan exact tuple rows through a count-only or count+sum monomorphized hot loop.
fn group_fixed_exact_tuple_sequence<const WITH_SUM: bool>(
    py: Python<'_>,
    row_count: usize,
    mut get_row: impl FnMut(usize) -> *mut ffi::PyObject,
    key_index: isize,
    value_index: isize,
) -> PyResult<Option<(ObjectKeyGroups, Vec<usize>)>> {
    let mut state = ObjectKeyGroupState::new(row_count);
    let mut counts = Vec::new();
    let mut cached_layout = None;
    let mut row_index = 0;
    while row_index < row_count && state.groups.len() <= OBJECT_KEY_CACHE_SLOTS {
        let row = get_row(row_index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
        if group_fixed_exact_tuple_row::<WITH_SUM, true>(
            py,
            row,
            key_index,
            value_index,
            &mut cached_layout,
            &mut state,
            &mut counts,
        )?
        .is_none()
        {
            return Ok(None);
        }
        row_index += 1;
    }
    while row_index < row_count {
        let row = get_row(row_index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
        if group_fixed_exact_tuple_row::<WITH_SUM, false>(
            py,
            row,
            key_index,
            value_index,
            &mut cached_layout,
            &mut state,
            &mut counts,
        )?
        .is_none()
        {
            return Ok(None);
        }
        row_index += 1;
    }
    debug_assert_eq!(state.groups.len(), counts.len());
    Ok(Some((state.groups, counts)))
}

/// Select the free-thread-safe outer-container path for fixed exact tuple rows.
fn group_fixed_i64_tuple_source<const WITH_SUM: bool>(
    source: &Bound<'_, PyAny>,
    key_index: isize,
    value_index: isize,
) -> PyResult<Option<(ObjectKeyGroups, Vec<usize>)>> {
    if let Ok(rows) = source.cast_exact::<PyList>() {
        #[cfg(not(Py_GIL_DISABLED))]
        return group_fixed_exact_tuple_sequence::<WITH_SUM>(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: a GIL build prevents exact-list mutation during this attached call.
                unsafe { ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            key_index,
            value_index,
        );
        #[cfg(Py_GIL_DISABLED)]
        {
            let snapshot = snapshot_exact_list_rows(source.py(), source, rows)?;
            return group_fixed_exact_tuple_sequence::<WITH_SUM>(
                source.py(),
                snapshot.len(),
                |index| snapshot[index].bind(source.py()).as_ptr(),
                key_index,
                value_index,
            );
        }
    }
    if let Ok(rows) = source.cast_exact::<PyTuple>() {
        return group_fixed_exact_tuple_sequence::<WITH_SUM>(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: exact outer tuples are immutable and index is within fixed length.
                unsafe { ffi::PyTuple_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            key_index,
            value_index,
        );
    }
    Ok(None)
}

/// Validate and consume one exact dict row in a compile-time-selected fixed mode.
#[inline]
fn group_fixed_exact_dict_row<const WITH_SUM: bool, const USE_OBJECT_CACHE: bool>(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    key_field: *mut ffi::PyObject,
    value_field: *mut ffi::PyObject,
    state: &mut ObjectKeyGroupState,
    counts: &mut Vec<usize>,
) -> PyResult<Option<()>> {
    // SAFETY: row is live through the GIL, immutable outer tuple, or strong list snapshot.
    if unsafe { ffi::PyDict_CheckExact(row) } == 0 {
        return Ok(None);
    }
    // SAFETY: row remains live for this entire call.
    let row_bound = unsafe { Borrowed::from_ptr(py, row) };
    with_critical_section(row_bound.as_any(), || {
        let mut position = 0;
        let mut field = core::ptr::null_mut();
        let mut field_value = core::ptr::null_mut();
        let mut key_object = core::ptr::null_mut();
        let mut value_object = core::ptr::null_mut();
        // SAFETY: row is an exact dict protected by this critical section.
        let field_count = unsafe { ffi::PyDict_Size(row) };
        if field_count < 0 {
            return Err(PyErr::fetch(py));
        }
        let field_count = usize::try_from(field_count)
            .map_err(|_| PyMemoryError::new_err("native record field count is too large"))?;
        if field_count > RECORD_GROUP_SUM_MAX_FIELDS {
            return Ok(None);
        }
        for _ in 0..field_count {
            // SAFETY: the locked exact dict has the fixed size read above.
            if unsafe { ffi::PyDict_Next(row, &mut position, &mut field, &mut field_value) } == 0 {
                return Ok(None);
            }
            // Reject every dispatch-capable field before selector lookup.
            // SAFETY: PyDict_Next returned a non-null live key.
            if unsafe { ffi::PyUnicode_CheckExact(field) } == 0 {
                return Ok(None);
            }
            if field == key_field {
                key_object = field_value;
            }
            if WITH_SUM && field == value_field {
                value_object = field_value;
            }
        }
        if key_object.is_null() {
            let Some(found) = dict_item(py, row, key_field)? else {
                return Ok(None);
            };
            key_object = found;
        }
        if WITH_SUM {
            if value_object.is_null() {
                let Some(found) = dict_item(py, row, value_field)? else {
                    return Ok(None);
                };
                value_object = found;
            }
            add_fixed_exact_i64_key_value::<USE_OBJECT_CACHE>(
                py,
                key_object,
                value_object,
                state,
                counts,
            )
        } else {
            add_fixed_exact_i64_key::<USE_OBJECT_CACHE>(py, key_object, state, counts)
        }
    })
}

/// Scan exact dict rows through a count-only or count+sum monomorphized hot loop.
fn group_fixed_exact_dict_sequence<const WITH_SUM: bool>(
    py: Python<'_>,
    row_count: usize,
    mut get_row: impl FnMut(usize) -> *mut ffi::PyObject,
    key_field: *mut ffi::PyObject,
    value_field: *mut ffi::PyObject,
) -> PyResult<Option<(ObjectKeyGroups, Vec<usize>)>> {
    let mut state = ObjectKeyGroupState::new(row_count);
    let mut counts = Vec::new();
    let mut row_index = 0;
    while row_index < row_count && state.groups.len() <= OBJECT_KEY_CACHE_SLOTS {
        let row = get_row(row_index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
        if group_fixed_exact_dict_row::<WITH_SUM, true>(
            py,
            row,
            key_field,
            value_field,
            &mut state,
            &mut counts,
        )?
        .is_none()
        {
            return Ok(None);
        }
        row_index += 1;
    }
    while row_index < row_count {
        let row = get_row(row_index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
        if group_fixed_exact_dict_row::<WITH_SUM, false>(
            py,
            row,
            key_field,
            value_field,
            &mut state,
            &mut counts,
        )?
        .is_none()
        {
            return Ok(None);
        }
        row_index += 1;
    }
    debug_assert_eq!(state.groups.len(), counts.len());
    Ok(Some((state.groups, counts)))
}

/// Select the free-thread-safe outer-container path for fixed exact dict rows.
fn group_fixed_i64_dict_source<const WITH_SUM: bool>(
    source: &Bound<'_, PyAny>,
    key_field: *mut ffi::PyObject,
    value_field: *mut ffi::PyObject,
) -> PyResult<Option<(ObjectKeyGroups, Vec<usize>)>> {
    if let Ok(rows) = source.cast_exact::<PyList>() {
        #[cfg(not(Py_GIL_DISABLED))]
        return group_fixed_exact_dict_sequence::<WITH_SUM>(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: a GIL build prevents exact-list mutation during this attached call.
                unsafe { ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            key_field,
            value_field,
        );
        #[cfg(Py_GIL_DISABLED)]
        {
            let snapshot = snapshot_exact_list_rows(source.py(), source, rows)?;
            return group_fixed_exact_dict_sequence::<WITH_SUM>(
                source.py(),
                snapshot.len(),
                |index| snapshot[index].bind(source.py()).as_ptr(),
                key_field,
                value_field,
            );
        }
    }
    if let Ok(rows) = source.cast_exact::<PyTuple>() {
        return group_fixed_exact_dict_sequence::<WITH_SUM>(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: exact outer tuples are immutable and index is within fixed length.
                unsafe { ffi::PyTuple_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            key_field,
            value_field,
        );
    }
    Ok(None)
}

/// Build the count-only ABI payload while preserving first-key identity and encounter order.
fn materialize_fixed_count_payload(
    py: Python<'_>,
    groups: ObjectKeyGroups,
    counts: Vec<usize>,
    key_name: &Bound<'_, PyString>,
    count_name: &Bound<'_, PyString>,
) -> PyResult<(bool, Py<PyAny>)> {
    debug_assert_eq!(groups.len(), counts.len());
    if groups.len() < GROUP_SUM_FINAL_ROWS_THRESHOLD {
        let pairs = groups
            .into_iter()
            .zip(counts)
            .map(|((key, _unused_total), count)| (key, count));
        let pairs = PyList::new(py, pairs)?;
        return Ok((false, pairs.into_any().unbind()));
    }

    let mut rows = Vec::new();
    rows.try_reserve(groups.len())
        .map_err(group_allocation_error)?;
    for ((key, _unused_total), count) in groups.into_iter().zip(counts) {
        let row = new_dict_fallible(py)?;
        row.set_item(key_name, key)?;
        row.set_item(count_name, count)?;
        rows.push(row.unbind());
    }
    let rows = PyList::new(py, rows)?;
    Ok((true, rows.into_any().unbind()))
}

/// Build the count+sum ABI payload with a widened sum lane.
fn materialize_fixed_count_sum_payload(
    py: Python<'_>,
    groups: ObjectKeyGroups,
    counts: Vec<usize>,
    key_name: &Bound<'_, PyString>,
    count_name: &Bound<'_, PyString>,
    sum_name: &Bound<'_, PyString>,
) -> PyResult<(bool, Py<PyAny>)> {
    debug_assert_eq!(groups.len(), counts.len());
    if groups.len() < GROUP_SUM_FINAL_ROWS_THRESHOLD {
        let triples = groups
            .into_iter()
            .zip(counts)
            .map(|((key, total), count)| (key, count, total));
        let triples = PyList::new(py, triples)?;
        return Ok((false, triples.into_any().unbind()));
    }

    let mut rows = Vec::new();
    rows.try_reserve(groups.len())
        .map_err(group_allocation_error)?;
    for ((key, total), count) in groups.into_iter().zip(counts) {
        let row = new_dict_fallible(py)?;
        row.set_item(key_name, key)?;
        row.set_item(count_name, count)?;
        row.set_item(sum_name, total)?;
        rows.push(row.unbind());
    }
    let rows = PyList::new(py, rows)?;
    Ok((true, rows.into_any().unbind()))
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
/// Group exact tuple rows into fixed count-only or count+sum lanes.
pub(crate) fn group_fixed_i64_rows_v1(
    source: &Bound<'_, PyAny>,
    key_index: isize,
    value_index_or_none: &Bound<'_, PyAny>,
    key_name: &Bound<'_, PyAny>,
    count_name: &Bound<'_, PyAny>,
    sum_name_or_none: &Bound<'_, PyAny>,
) -> PyResult<Option<(bool, Py<PyAny>)>> {
    let key_name = match key_name.cast_exact::<PyString>() {
        Ok(name) => name,
        Err(_) => return Ok(None),
    };
    let count_name = match count_name.cast_exact::<PyString>() {
        Ok(name) => name,
        Err(_) => return Ok(None),
    };

    if value_index_or_none.is_none() {
        if !sum_name_or_none.is_none() {
            return Ok(None);
        }
        let Some((groups, counts)) = group_fixed_i64_tuple_source::<false>(source, key_index, 0)?
        else {
            return Ok(None);
        };
        return materialize_fixed_count_payload(source.py(), groups, counts, key_name, count_name)
            .map(Some);
    }
    if sum_name_or_none.is_none() {
        return Ok(None);
    }
    let Some(value_index) = exact_i64(source.py(), value_index_or_none.as_ptr())? else {
        return Ok(None);
    };
    let Ok(value_index) = isize::try_from(value_index) else {
        return Ok(None);
    };
    let sum_name = match sum_name_or_none.cast_exact::<PyString>() {
        Ok(name) => name,
        Err(_) => return Ok(None),
    };
    let Some((groups, counts)) =
        group_fixed_i64_tuple_source::<true>(source, key_index, value_index)?
    else {
        return Ok(None);
    };
    materialize_fixed_count_sum_payload(source.py(), groups, counts, key_name, count_name, sum_name)
        .map(Some)
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
/// Group exact dict rows into fixed count-only or count+sum lanes.
pub(crate) fn group_fixed_i64_dict_rows_v1(
    source: &Bound<'_, PyAny>,
    key_field: &Bound<'_, PyAny>,
    value_field_or_none: &Bound<'_, PyAny>,
    key_name: &Bound<'_, PyAny>,
    count_name: &Bound<'_, PyAny>,
    sum_name_or_none: &Bound<'_, PyAny>,
) -> PyResult<Option<(bool, Py<PyAny>)>> {
    let key_field = match key_field.cast_exact::<PyString>() {
        Ok(field) => field,
        Err(_) => return Ok(None),
    };
    let key_name = match key_name.cast_exact::<PyString>() {
        Ok(name) => name,
        Err(_) => return Ok(None),
    };
    let count_name = match count_name.cast_exact::<PyString>() {
        Ok(name) => name,
        Err(_) => return Ok(None),
    };

    if value_field_or_none.is_none() {
        if !sum_name_or_none.is_none() {
            return Ok(None);
        }
        let Some((groups, counts)) = group_fixed_i64_dict_source::<false>(
            source,
            key_field.as_ptr(),
            core::ptr::null_mut(),
        )?
        else {
            return Ok(None);
        };
        return materialize_fixed_count_payload(source.py(), groups, counts, key_name, count_name)
            .map(Some);
    }
    if sum_name_or_none.is_none() {
        return Ok(None);
    }
    let value_field = match value_field_or_none.cast_exact::<PyString>() {
        Ok(field) => field,
        Err(_) => return Ok(None),
    };
    let sum_name = match sum_name_or_none.cast_exact::<PyString>() {
        Ok(name) => name,
        Err(_) => return Ok(None),
    };
    let Some((groups, counts)) =
        group_fixed_i64_dict_source::<true>(source, key_field.as_ptr(), value_field.as_ptr())?
    else {
        return Ok(None);
    };
    materialize_fixed_count_sum_payload(source.py(), groups, counts, key_name, count_name, sum_name)
        .map(Some)
}

/// Keep optional fixed-schema PyO3 definitions out of the legacy module initializer body.
#[inline(never)]
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(group_fixed_i64_rows_v1, module)?)?;
    module.add_function(wrap_pyfunction!(group_fixed_i64_dict_rows_v1, module)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_fixed_group_inserts_hash_once_between_growth_boundaries() {
        const ROW_COUNT: usize = 4_096;

        Python::initialize();
        Python::attach(|py| {
            let key_at = |index: usize| i64::MIN + index as i64 * 1_000_003;
            let mut single_entry_reference =
                FastI64PositionMap::with_hasher(SeededI64BuildHasher::random());
            begin_i64_hash_probe_count();
            for index in 0..ROW_COUNT {
                match single_entry_reference.entry(key_at(index)) {
                    Entry::Vacant(entry) => {
                        entry.insert(index);
                    }
                    Entry::Occupied(_) => panic!("generated sparse keys must be unique"),
                }
            }
            let single_entry_probes = end_i64_hash_probe_count();
            // The fallible production path must release a vacant Entry before growing and then
            // hash that boundary key once more. HashMap doubles geometrically, so allow one extra
            // probe per growth boundary plus the initial dense-to-hash migration.
            let max_probes = single_entry_probes + ROW_COUNT.ilog2() as usize + 1;

            let source =
                PyList::new(py, (0..ROW_COUNT).map(|index| (key_at(index), 1_i64))).unwrap();
            let key_name = PyString::new(py, "key");
            let count_name = PyString::new(py, "count");
            let sum_name = PyString::new(py, "total");
            let value_index = pyo3::types::PyInt::new(py, 1_i64);
            let none = py.None();

            for (value_index_or_none, sum_name_or_none) in [
                (none.bind(py).as_any(), none.bind(py).as_any()),
                (value_index.as_any(), sum_name.as_any()),
            ] {
                begin_i64_hash_probe_count();
                let (_, payload) = group_fixed_i64_rows_v1(
                    source.as_any(),
                    0,
                    value_index_or_none,
                    key_name.as_any(),
                    count_name.as_any(),
                    sum_name_or_none,
                )
                .unwrap()
                .unwrap();
                let probes = end_i64_hash_probe_count();

                assert_eq!(
                    payload.bind(py).cast_exact::<PyList>().unwrap().len(),
                    ROW_COUNT
                );
                assert!(
                    probes >= single_entry_probes,
                    "sparse fixed grouping unexpectedly skipped reference hash work"
                );
                assert!(
                    probes <= max_probes,
                    "sparse fixed grouping used {probes} probes; single-Entry growth used \
                     {single_entry_probes} and the boundary-aware maximum is {max_probes}"
                );
            }
        });
    }
}
