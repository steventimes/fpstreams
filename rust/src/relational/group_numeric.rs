//! Exact integer tuple and dictionary grouping kernels.

use super::*;

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

/// Sum signed values by key using direct slots when the observed key span is compact.
#[cfg(test)]
pub(crate) fn group_sum_pairs(
    rows: Vec<(i64, i64)>,
    key_bounds: Option<(i64, i64)>,
) -> Option<Vec<(i64, i128)>> {
    if let Some((minimum, maximum)) = key_bounds
        && let Some(slot_count) = dense_slot_count(rows.len(), minimum, maximum)
    {
        return group_sum_dense(rows, minimum, slot_count);
    }
    let mut positions: HashMap<i64, usize> = HashMap::new();
    let mut groups: Vec<(i64, i128)> = Vec::new();
    for (key, value) in rows {
        if let Some(&position) = positions.get(&key) {
            groups[position].1 = groups[position].1.checked_add(i128::from(value))?;
        } else {
            positions.insert(key, groups.len());
            groups.push((key, i128::from(value)));
        }
    }
    Some(groups)
}

/// Return a safe dense-index width, or select hashing for sparse integer keys.
#[cfg(test)]
fn dense_slot_count(row_count: usize, minimum: i64, maximum: i64) -> Option<usize> {
    let span = i128::from(maximum)
        .checked_sub(i128::from(minimum))?
        .checked_add(1)?;
    let slots = usize::try_from(span).ok()?;
    (slots <= MAX_DENSE_GROUP_SLOTS && slots <= row_count.saturating_mul(MAX_DENSE_SLOTS_PER_ROW))
        .then_some(slots)
}

/// Aggregate a compact integer key range through direct slots instead of hashing.
#[cfg(test)]
fn group_sum_dense(
    rows: Vec<(i64, i64)>,
    minimum: i64,
    slot_count: usize,
) -> Option<Vec<(i64, i128)>> {
    let mut positions = vec![usize::MAX; slot_count];
    let mut groups: Vec<(i64, i128)> = Vec::new();
    for (key, value) in rows {
        let offset = usize::try_from(i128::from(key) - i128::from(minimum)).ok()?;
        let position = *positions.get(offset)?;
        if position == usize::MAX {
            positions[offset] = groups.len();
            groups.push((key, i128::from(value)));
        } else {
            groups[position].1 = groups[position].1.checked_add(i128::from(value))?;
        }
    }
    Some(groups)
}

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

#[pyfunction]
/// Speculatively aggregate exact tuple rows in an exact list/tuple container.
pub(crate) fn group_sum_i64_pairs(
    source: &Bound<'_, PyAny>,
    key_index: isize,
    value_index: isize,
) -> PyResult<Option<ObjectKeyGroups>> {
    group_sum_i64_tuple_rows(source, key_index, value_index, None)
}

#[pyfunction]
/// Speculatively aggregate exact two-item tuple rows in an exact list/tuple container.
pub(crate) fn group_sum_i64_exact_pairs_v1(
    source: &Bound<'_, PyAny>,
) -> PyResult<Option<ObjectKeyGroups>> {
    group_sum_i64_tuple_rows(source, 0, 1, Some(2))
}

/// Aggregate exact tuple rows, optionally requiring one fixed row width.
fn group_sum_i64_tuple_rows(
    source: &Bound<'_, PyAny>,
    key_index: isize,
    value_index: isize,
    required_width: Option<usize>,
) -> PyResult<Option<ObjectKeyGroups>> {
    // A GIL build may borrow exact-list rows for the whole attached call. A free-threaded build
    // instead takes one locked snapshot of strong row references before scanning tuple contents.
    if let Ok(rows) = source.cast_exact::<PyList>() {
        #[cfg(not(Py_GIL_DISABLED))]
        return group_exact_tuple_sequence(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: a GIL build cannot mutate the exact list while this native call stays
                // attached to Python, and index is below the list's unchanged length.
                unsafe { ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            key_index,
            value_index,
            required_width,
        );
        #[cfg(Py_GIL_DISABLED)]
        {
            let snapshot = snapshot_exact_list_rows(source.py(), source, rows)?;
            return group_exact_tuple_sequence(
                source.py(),
                snapshot.len(),
                |index| snapshot[index].bind(source.py()).as_ptr(),
                key_index,
                value_index,
                required_width,
            );
        }
    }
    if let Ok(rows) = source.cast_exact::<PyTuple>() {
        return group_exact_tuple_sequence(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: exact tuples are immutable and index is below their fixed length.
                unsafe { ffi::PyTuple_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            key_index,
            value_index,
            required_width,
        );
    }
    Ok(None)
}

#[pyfunction]
/// Aggregate once, returning pairs for small outputs and final dictionaries for large outputs.
pub(crate) fn group_sum_i64_rows_v1(
    source: &Bound<'_, PyAny>,
    key_index: isize,
    value_index: isize,
    key_name: &Bound<'_, PyAny>,
    output_name: &Bound<'_, PyAny>,
) -> PyResult<Option<(bool, Py<PyAny>)>> {
    // Validate both names before opening the speculative row scan. Exact strings have
    // side-effect-free hashing/equality when final dictionaries are materialized.
    let key_name = match key_name.cast_exact::<PyString>() {
        Ok(name) => name,
        Err(_) => return Ok(None),
    };
    let output_name = match output_name.cast_exact::<PyString>() {
        Ok(name) => name,
        Err(_) => return Ok(None),
    };
    let Some(groups) = group_sum_i64_pairs(source, key_index, value_index)? else {
        return Ok(None);
    };
    let py = source.py();
    if groups.len() < GROUP_SUM_FINAL_ROWS_THRESHOLD {
        let pairs = PyList::new(py, groups)?;
        return Ok(Some((false, pairs.into_any().unbind())));
    }

    let mut rows = Vec::new();
    rows.try_reserve(groups.len())
        .map_err(group_allocation_error)?;
    for (key, total) in groups {
        let row = new_dict_fallible(py)?;
        row.set_item(key_name, key)?;
        row.set_item(output_name, total)?;
        rows.push(row.unbind());
    }
    let rows = PyList::new(py, rows)?;
    Ok(Some((true, rows.into_any().unbind())))
}

/// Aggregate exact tuple rows whose outer container keeps every borrowed row stable.
fn group_exact_tuple_sequence(
    py: Python<'_>,
    row_count: usize,
    mut get_row: impl FnMut(usize) -> *mut ffi::PyObject,
    key_index: isize,
    value_index: isize,
    required_width: Option<usize>,
) -> PyResult<Option<ObjectKeyGroups>> {
    let mut state = ObjectKeyGroupState::new(row_count);
    let mut cached_layout: Option<(usize, usize, usize)> = None;
    let mut row_index = 0;
    while row_index < row_count && state.groups.len() <= OBJECT_KEY_CACHE_SLOTS {
        let row = get_row(row_index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
        if group_exact_tuple_row::<true>(
            py,
            row,
            key_index,
            value_index,
            required_width,
            &mut cached_layout,
            &mut state,
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
        if group_exact_tuple_row::<false>(
            py,
            row,
            key_index,
            value_index,
            required_width,
            &mut cached_layout,
            &mut state,
        )?
        .is_none()
        {
            return Ok(None);
        }
        row_index += 1;
    }
    Ok(Some(state.groups))
}

/// Validate and aggregate one exact tuple row in either cached or uncached key mode.
#[inline(always)]
fn group_exact_tuple_row<const USE_OBJECT_CACHE: bool>(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    key_index: isize,
    value_index: isize,
    required_width: Option<usize>,
    cached_layout: &mut Option<(usize, usize, usize)>,
    state: &mut ObjectKeyGroupState,
) -> PyResult<Option<()>> {
    // SAFETY: row is a live borrowed Python object from the GIL-protected list, immutable
    // outer tuple, or owned exact-list snapshot.
    if unsafe { ffi::PyTuple_CheckExact(row) } == 0 {
        return Ok(None);
    }
    // Reuse normalized indexes for the usual fixed-width table, but invalidate the cache on a
    // heterogeneous row so negative indexes retain Python semantics.
    // SAFETY: row was proven to be an exact tuple.
    let width = unsafe { ffi::PyTuple_Size(row) };
    if width < 0 {
        return Err(PyErr::fetch(py));
    }
    let width = width as usize;
    if required_width.is_some_and(|required| width != required) {
        return Ok(None);
    }
    let (key_position, value_position) = match *cached_layout {
        Some((cached_width, key_position, value_position)) if cached_width == width => {
            (key_position, value_position)
        }
        _ => {
            let Some(key_position) = normalize_index(key_index, width) else {
                return Ok(None);
            };
            let Some(value_position) = normalize_index(value_index, width) else {
                return Ok(None);
            };
            *cached_layout = Some((width, key_position, value_position));
            (key_position, value_position)
        }
    };
    // SAFETY: both normalized positions are below the exact tuple's fixed width.
    let key_object = unsafe { ffi::PyTuple_GetItem(row, key_position as ffi::Py_ssize_t) };
    // SAFETY: both normalized positions are below the exact tuple's fixed width.
    let value_object = unsafe { ffi::PyTuple_GetItem(row, value_position as ffi::Py_ssize_t) };
    if key_object.is_null() || value_object.is_null() {
        return Err(PyErr::fetch(py));
    }
    add_exact_i64_objects::<USE_OBJECT_CACHE>(py, key_object, value_object, state)
}

/// Convert a fallible Rust container reservation into Python's allocation error.
pub(super) fn group_allocation_error(_error: TryReserveError) -> PyErr {
    PyMemoryError::new_err("native grouping allocation failed")
}

/// Allocate a dictionary without converting a genuine Python allocation error into a panic.
pub(super) fn new_dict_fallible<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
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

    /// Add to an existing group, retaining the widened overflow guard.
    #[inline]
    fn add_at_position(&mut self, position: usize, value: i64) -> Option<()> {
        let total = self.groups[position].1.checked_add(i128::from(value))?;
        self.groups[position].1 = total;
        Some(())
    }

    /// Add a proven exact-int pair, cloning the Python key only for a new group.
    #[inline(always)]
    fn add<const USE_OBJECT_CACHE: bool>(
        &mut self,
        py: Python<'_>,
        key_object: *mut ffi::PyObject,
        key: i64,
        value: i64,
    ) -> PyResult<Option<()>> {
        if let Some(position) =
            self.positions
                .position(key, self.dense_limit, self.group_keys.iter().copied())?
        {
            let result = self.add_at_position(position, value);
            if USE_OBJECT_CACHE && self.groups[position].0.bind(py).as_ptr() == key_object {
                self.remember_first_object(key_object, position);
            }
            return Ok(result);
        }

        // Reserve both containers explicitly so genuine allocation failures become a Python
        // MemoryError rather than an abort inside Rust's infallible growth path.
        self.positions.try_reserve_group()?;
        self.group_keys
            .try_reserve(1)
            .map_err(group_allocation_error)?;
        self.groups.try_reserve(1).map_err(group_allocation_error)?;
        // SAFETY: key_object is a live borrowed reference owned by an immutable tuple or by a
        // row dict whose critical section the caller holds. Converting it takes one strong ref.
        let first_key = unsafe { Borrowed::from_ptr(py, key_object).to_owned().unbind() };
        let position = self.groups.len();
        self.positions.insert(key, position);
        self.group_keys.push(key);
        self.groups.push((first_key, i128::from(value)));
        if USE_OBJECT_CACHE && position < OBJECT_KEY_CACHE_SLOTS {
            self.remember_first_object(key_object, position);
        }
        Ok(Some(()))
    }
}

/// Fetch a borrowed exact-dict value, preserving genuine Python errors.
pub(super) fn dict_item(
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
pub(super) fn exact_i64(py: Python<'_>, value: *mut ffi::PyObject) -> PyResult<Option<i64>> {
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

/// Validate and aggregate one exact-int key/value pair, reusing retained key identities.
#[inline(always)]
fn add_exact_i64_objects<const USE_OBJECT_CACHE: bool>(
    py: Python<'_>,
    key_object: *mut ffi::PyObject,
    value_object: *mut ffi::PyObject,
    state: &mut ObjectKeyGroupState,
) -> PyResult<Option<()>> {
    if USE_OBJECT_CACHE {
        if let Some(position) = state.cached_position(key_object) {
            // A cache hit proves this is the same immutable exact int that `groups` owns
            // strongly. Only the changing value still needs exact-int validation/extraction.
            let Some(value) = exact_i64(py, value_object)? else {
                return Ok(None);
            };
            return Ok(state.add_at_position(position, value));
        }
    }
    let Some(key) = exact_i64(py, key_object)? else {
        return Ok(None);
    };
    let Some(value) = exact_i64(py, value_object)? else {
        return Ok(None);
    };
    state.add::<USE_OBJECT_CACHE>(py, key_object, key, value)
}

/// Validate and aggregate one borrowed row without observable speculative callbacks.
#[inline]
fn group_exact_dict_row<const USE_OBJECT_CACHE: bool>(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    key_field: *mut ffi::PyObject,
    value_field: *mut ffi::PyObject,
    state: &mut ObjectKeyGroupState,
) -> PyResult<Option<()>> {
    // SAFETY: the caller keeps row live through the GIL, an owned list snapshot, or the
    // immutable source tuple for the duration of this call.
    if unsafe { ffi::PyDict_CheckExact(row) } == 0 {
        return Ok(None);
    }
    // SAFETY: row is non-null and remains owned by the GIL-protected list, caller snapshot, or
    // immutable tuple throughout this scope. Borrowed avoids one INCREF/DECREF per record.
    let row_bound = unsafe { Borrowed::from_ptr(py, row) };

    // Keep validation, both lookups, and cloning the first key under one object lock. This closes
    // the gap in which free-threaded Python could replace a proven string key with a custom key.
    with_critical_section(row_bound.as_any(), || {
        let mut position = 0;
        let mut field = core::ptr::null_mut();
        let mut field_value = core::ptr::null_mut();
        let mut key_object = core::ptr::null_mut();
        let mut value_object = core::ptr::null_mut();
        // SAFETY: row is the exact dict protected by this critical section. Reading its size
        // cannot invoke Python code. Knowing the fixed count avoids one terminal PyDict_Next
        // call per row on the common narrow-record path.
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
            // SAFETY: row is an exact dict protected by its critical section. PyDict_Next only
            // returns borrowed references and does not invoke Python code. The locked size fixes
            // the number of successful iterations.
            if unsafe { ffi::PyDict_Next(row, &mut position, &mut field, &mut field_value) } == 0 {
                return Ok(None);
            }
            // Reject before either selector lookup if any key could run custom equality.
            // SAFETY: field is non-null whenever PyDict_Next reports an item.
            if unsafe { ffi::PyUnicode_CheckExact(field) } == 0 {
                return Ok(None);
            }
            // Dict literals and records built from stable schemas normally retain the exact
            // selector-name objects. Capture their borrowed values during the mandatory safety
            // scan so the common path avoids two subsequent hashed dictionary lookups. Equal
            // but non-identical exact strings retain canonical lookup semantics below.
            if field == key_field {
                key_object = field_value;
            }
            if field == value_field {
                value_object = field_value;
            }
        }

        if key_object.is_null() {
            let Some(found) = dict_item(py, row, key_field)? else {
                return Ok(None);
            };
            key_object = found;
        }
        if value_object.is_null() {
            let Some(found) = dict_item(py, row, value_field)? else {
                return Ok(None);
            };
            value_object = found;
        };
        add_exact_i64_objects::<USE_OBJECT_CACHE>(py, key_object, value_object, state)
    })
}

/// Aggregate borrowed rows from a GIL-protected list, locked snapshot, or immutable tuple.
fn group_exact_dict_sequence(
    py: Python<'_>,
    row_count: usize,
    mut get_row: impl FnMut(usize) -> *mut ffi::PyObject,
    key_field: *mut ffi::PyObject,
    value_field: *mut ffi::PyObject,
) -> PyResult<Option<ObjectKeyGroups>> {
    let mut state = ObjectKeyGroupState::new(row_count);
    let mut index = 0;
    while index < row_count && state.groups.len() <= OBJECT_KEY_CACHE_SLOTS {
        let row = get_row(index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
        if group_exact_dict_row::<true>(py, row, key_field, value_field, &mut state)?.is_none() {
            return Ok(None);
        }
        index += 1;
    }
    while index < row_count {
        let row = get_row(index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
        if group_exact_dict_row::<false>(py, row, key_field, value_field, &mut state)?.is_none() {
            return Ok(None);
        }
        index += 1;
    }
    Ok(Some(state.groups))
}

/// Take stable strong references while an exact list is locked.
pub(super) fn snapshot_exact_list_rows(
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

#[pyfunction]
/// Speculatively group exact dict records, returning None for a semantics-safe fallback.
pub(crate) fn group_sum_i64_dict_rows<'py>(
    source: &Bound<'py, PyAny>,
    key_field: &Bound<'py, PyAny>,
    value_field: &Bound<'py, PyAny>,
) -> PyResult<Option<ObjectKeyGroups>> {
    // Exact strings are immutable and have side-effect-free hashing/equality. Subclasses are
    // deliberately rejected because their methods would make a failed speculative scan visible.
    let key_field = match key_field.cast_exact::<PyString>() {
        Ok(field) => field,
        Err(_) => return Ok(None),
    };
    let value_field = match value_field.cast_exact::<PyString>() {
        Ok(field) => field,
        Err(_) => return Ok(None),
    };

    if let Ok(rows) = source.cast_exact::<PyList>() {
        #[cfg(not(Py_GIL_DISABLED))]
        return group_exact_dict_sequence(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: on a GIL build, exact-list mutation cannot race this attached native
                // call. Free-threaded builds retain the strong-reference snapshot below.
                unsafe { ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            key_field.as_ptr(),
            value_field.as_ptr(),
        );
        #[cfg(Py_GIL_DISABLED)]
        {
            // Nested one-object critical sections suspend the outer lock. Snapshot strong row
            // references first so locking each dict cannot expose a dangling list-item borrow.
            let snapshot = snapshot_exact_list_rows(source.py(), source, rows)?;
            return group_exact_dict_sequence(
                source.py(),
                snapshot.len(),
                |index| snapshot[index].bind(source.py()).as_ptr(),
                key_field.as_ptr(),
                value_field.as_ptr(),
            );
        }
    }
    if let Ok(rows) = source.cast_exact::<PyTuple>() {
        return group_exact_dict_sequence(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: exact tuples are immutable and index is below their fixed length.
                unsafe { ffi::PyTuple_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            key_field.as_ptr(),
            value_field.as_ptr(),
        );
    }
    Ok(None)
}

#[pyfunction]
/// Aggregate once, returning pairs for small record outputs and final dictionaries for large ones.
pub(crate) fn group_sum_i64_dict_rows_v1<'py>(
    source: &Bound<'py, PyAny>,
    key_field: &Bound<'py, PyAny>,
    value_field: &Bound<'py, PyAny>,
    key_name: &Bound<'py, PyAny>,
    output_name: &Bound<'py, PyAny>,
) -> PyResult<Option<(bool, Py<PyAny>)>> {
    // Validate every field name before opening the speculative record scan.
    let key_name = match key_name.cast_exact::<PyString>() {
        Ok(name) => name,
        Err(_) => return Ok(None),
    };
    let output_name = match output_name.cast_exact::<PyString>() {
        Ok(name) => name,
        Err(_) => return Ok(None),
    };
    let key_field = match key_field.cast_exact::<PyString>() {
        Ok(field) => field,
        Err(_) => return Ok(None),
    };
    let value_field = match value_field.cast_exact::<PyString>() {
        Ok(field) => field,
        Err(_) => return Ok(None),
    };

    // Keep this scan local to the v1 wrapper. Sharing the legacy #[pyfunction] body prevents
    // LLVM from specializing its hot row loop into either Python trampoline and measurably
    // regresses low-cardinality workloads.
    let groups = if let Ok(rows) = source.cast_exact::<PyList>() {
        #[cfg(not(Py_GIL_DISABLED))]
        {
            group_exact_dict_sequence(
                source.py(),
                rows.len(),
                |index| {
                    // SAFETY: on a GIL build, exact-list mutation cannot race this attached
                    // native call. Free-threaded builds use the strong-reference snapshot below.
                    unsafe { ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
                },
                key_field.as_ptr(),
                value_field.as_ptr(),
            )?
        }
        #[cfg(Py_GIL_DISABLED)]
        {
            let snapshot = snapshot_exact_list_rows(source.py(), source, rows)?;
            group_exact_dict_sequence(
                source.py(),
                snapshot.len(),
                |index| snapshot[index].bind(source.py()).as_ptr(),
                key_field.as_ptr(),
                value_field.as_ptr(),
            )?
        }
    } else if let Ok(rows) = source.cast_exact::<PyTuple>() {
        group_exact_dict_sequence(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: exact tuples are immutable and index is below their fixed length.
                unsafe { ffi::PyTuple_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            key_field.as_ptr(),
            value_field.as_ptr(),
        )?
    } else {
        return Ok(None);
    };
    let Some(groups) = groups else {
        return Ok(None);
    };
    let py = source.py();
    if groups.len() < GROUP_SUM_FINAL_ROWS_THRESHOLD {
        let pairs = PyList::new(py, groups)?;
        return Ok(Some((false, pairs.into_any().unbind())));
    }

    let mut rows = Vec::new();
    rows.try_reserve(groups.len())
        .map_err(group_allocation_error)?;
    for (key, total) in groups {
        let row = new_dict_fallible(py)?;
        row.set_item(key_name, key)?;
        row.set_item(output_name, total)?;
        rows.push(row.unbind());
    }
    let rows = PyList::new(py, rows)?;
    Ok(Some((true, rows.into_any().unbind())))
}
