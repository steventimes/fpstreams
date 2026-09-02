//! Transactional grouped aggregation over stable NumPy-compatible integer buffers.

mod buffer;

#[cfg(test)]
use self::buffer::native_endian_for_test as is_native_endian;
use self::buffer::prepare_contiguous_partial;
#[cfg(not(Py_GIL_DISABLED))]
use self::buffer::prepare_strided_partial;
use crate::common::{AGGREGATE_COUNT, AGGREGATE_MAXIMUM, AGGREGATE_MINIMUM, AGGREGATE_TOTAL};
use pyo3::exceptions::{PyMemoryError, PyOverflowError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyInt, PyList, PyTuple};
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, TryReserveError};
use std::convert::Infallible;
use std::hash::{BuildHasher, Hash, Hasher};

const SUPPORTED_LANES: u8 =
    AGGREGATE_COUNT | AGGREGATE_TOTAL | AGGREGATE_MINIMUM | AGGREGATE_MAXIMUM;
const INITIAL_DENSE_SLOTS: usize = 64;
const MAX_DENSE_SLOTS: usize = 1 << 20;
const DENSE_SLOTS_PER_ROW: usize = 2;

fn allocation_error(_error: TryReserveError) -> PyErr {
    PyMemoryError::new_err("native NumPy grouping allocation failed")
}

fn overflow_error() -> PyErr {
    PyOverflowError::new_err("native NumPy grouped state exceeded its widened integer capacity")
}

fn validate_lane_mask(mask: u8) -> PyResult<u8> {
    if mask == 0 || mask & !SUPPORTED_LANES != 0 {
        return Err(PyValueError::new_err(
            "native NumPy group mask must contain only count, sum, min, or max",
        ));
    }
    Ok(mask)
}

fn dense_limit(row_count: usize) -> usize {
    row_count
        .saturating_mul(DENSE_SLOTS_PER_ROW)
        .clamp(INITIAL_DENSE_SLOTS, MAX_DENSE_SLOTS)
}

/// Per-state randomized integer hashing without SipHash's cost on proven numeric keys.
#[derive(Clone)]
struct NumericBuildHasher {
    seed_a: u64,
    seed_b: u64,
}

impl NumericBuildHasher {
    fn random() -> Self {
        let entropy = RandomState::new();
        Self {
            seed_a: entropy.hash_one(0x6a09_e667_f3bc_c909_u64),
            seed_b: entropy.hash_one(0xbb67_ae85_84ca_a73b_u64),
        }
    }
}

struct NumericHasher {
    seed_a: u64,
    seed_b: u64,
    state: u64,
}

impl NumericHasher {
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

impl Hasher for NumericHasher {
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

    #[inline]
    fn write_u8(&mut self, value: u8) {
        self.hash_integer(u64::from(value));
    }
}

impl BuildHasher for NumericBuildHasher {
    type Hasher = NumericHasher;

    fn build_hasher(&self) -> Self::Hasher {
        NumericHasher {
            seed_a: self.seed_a,
            seed_b: self.seed_b,
            state: 0,
        }
    }
}

trait GroupScalar: Copy + Eq + Hash + Ord {
    type Sum: Copy;

    fn coordinate(self) -> i128;
    fn widened(self) -> Self::Sum;
    fn checked_sum(left: Self::Sum, right: Self::Sum) -> Option<Self::Sum>;
}

impl GroupScalar for i64 {
    type Sum = i128;

    fn coordinate(self) -> i128 {
        i128::from(self)
    }

    fn widened(self) -> Self::Sum {
        i128::from(self)
    }

    fn checked_sum(left: Self::Sum, right: Self::Sum) -> Option<Self::Sum> {
        left.checked_add(right)
    }
}

impl GroupScalar for u64 {
    type Sum = u128;

    fn coordinate(self) -> i128 {
        i128::from(self)
    }

    fn widened(self) -> Self::Sum {
        u128::from(self)
    }

    fn checked_sum(left: Self::Sum, right: Self::Sum) -> Option<Self::Sum> {
        left.checked_add(right)
    }
}

impl GroupScalar for bool {
    type Sum = u128;

    fn coordinate(self) -> i128 {
        i128::from(u8::from(self))
    }

    fn widened(self) -> Self::Sum {
        u128::from(u8::from(self))
    }

    fn checked_sum(left: Self::Sum, right: Self::Sum) -> Option<Self::Sum> {
        left.checked_add(right)
    }
}

type FastPositionMap<T> = HashMap<T, usize, NumericBuildHasher>;

enum AdaptiveIndex<T> {
    Empty,
    Dense { minimum: i128, slots: Vec<usize> },
    Hash(FastPositionMap<T>),
}

impl<T: GroupScalar> AdaptiveIndex<T> {
    fn get(&self, key: T) -> Option<usize> {
        match self {
            Self::Empty => None,
            Self::Dense { minimum, slots } => {
                let offset = key.coordinate().checked_sub(*minimum)?;
                let offset = usize::try_from(offset).ok()?;
                let position = *slots.get(offset)?;
                (position != usize::MAX).then_some(position)
            }
            Self::Hash(positions) => positions.get(&key).copied(),
        }
    }

    fn try_reserve_new(
        &mut self,
        new_keys: &[T],
        limit: usize,
        existing_keys: &[T],
    ) -> PyResult<()> {
        if new_keys.is_empty() {
            return Ok(());
        }
        if let Self::Hash(positions) = self {
            positions
                .try_reserve(new_keys.len())
                .map_err(allocation_error)?;
            return Ok(());
        }
        if let Self::Dense { minimum, slots } = self {
            let maximum =
                *minimum + i128::try_from(slots.len().saturating_sub(1)).unwrap_or(i128::MAX);
            if new_keys.iter().all(|key| {
                let coordinate = key.coordinate();
                (*minimum..=maximum).contains(&coordinate)
            }) {
                return Ok(());
            }
        }

        let mut new_minimum = new_keys[0].coordinate();
        let mut new_maximum = new_minimum;
        for &key in &new_keys[1..] {
            let coordinate = key.coordinate();
            new_minimum = new_minimum.min(coordinate);
            new_maximum = new_maximum.max(coordinate);
        }
        let (current_minimum, current_maximum, current_len) = match self {
            Self::Empty => (None, None, 0),
            Self::Dense { minimum, slots } => {
                let maximum =
                    *minimum + i128::try_from(slots.len().saturating_sub(1)).unwrap_or(i128::MAX);
                (Some(*minimum), Some(maximum), slots.len())
            }
            Self::Hash(_) => unreachable!("hash indexes return before dense preparation"),
        };
        let required_minimum = current_minimum.map_or(new_minimum, |value| value.min(new_minimum));
        let required_maximum = current_maximum.map_or(new_maximum, |value| value.max(new_maximum));
        let required_span = required_maximum
            .checked_sub(required_minimum)
            .and_then(|span| span.checked_add(1))
            .and_then(|span| usize::try_from(span).ok());

        if required_span.is_none_or(|span| span > limit) {
            let mut positions = FastPositionMap::with_hasher(NumericBuildHasher::random());
            positions
                .try_reserve(existing_keys.len().saturating_add(new_keys.len()))
                .map_err(allocation_error)?;
            for (position, &key) in existing_keys.iter().enumerate() {
                positions.insert(key, position);
            }
            *self = Self::Hash(positions);
            return Ok(());
        }

        let required_span = required_span.expect("the dense span was validated above");
        let target_len = required_span
            .max(INITIAL_DENSE_SLOTS.min(limit))
            .max(current_len.saturating_mul(2).min(limit));
        let slack = target_len - required_span;
        let slack = i128::try_from(slack).unwrap_or(0);
        let next_minimum = match (current_minimum, current_maximum) {
            (Some(current_minimum), Some(current_maximum))
                if new_minimum >= current_minimum && new_maximum > current_maximum =>
            {
                current_minimum
            }
            (Some(current_minimum), Some(current_maximum))
                if new_minimum < current_minimum && new_maximum <= current_maximum =>
            {
                required_minimum - slack
            }
            (None, None)
                if required_minimum >= 0
                    && required_maximum < i128::try_from(target_len).unwrap_or(0) =>
            {
                0
            }
            _ => required_minimum - slack / 2,
        };
        let mut next_slots = Vec::new();
        next_slots
            .try_reserve_exact(target_len)
            .map_err(allocation_error)?;
        next_slots.resize(target_len, usize::MAX);
        if let Self::Dense { minimum, slots } = self {
            let offset = usize::try_from(*minimum - next_minimum)
                .expect("the expanded dense range contains the previous range");
            next_slots[offset..offset + slots.len()].copy_from_slice(slots);
        }
        *self = Self::Dense {
            minimum: next_minimum,
            slots: next_slots,
        };
        Ok(())
    }

    fn insert(&mut self, key: T, position: usize) {
        match self {
            Self::Empty => unreachable!("an index must reserve a new key before insertion"),
            Self::Dense { minimum, slots } => {
                let offset = usize::try_from(key.coordinate() - *minimum)
                    .expect("the dense range was prepared before insertion");
                debug_assert_eq!(slots[offset], usize::MAX);
                slots[offset] = position;
            }
            Self::Hash(positions) => {
                positions.insert(key, position);
            }
        }
    }
}

struct GroupData<T: GroupScalar> {
    index: AdaptiveIndex<T>,
    keys: Vec<T>,
    counts: Option<Vec<u128>>,
    sums: Option<Vec<T::Sum>>,
    minima: Option<Vec<T>>,
    maxima: Option<Vec<T>>,
    rows: usize,
}

impl<T: GroupScalar> GroupData<T> {
    fn empty(mask: u8) -> Self {
        Self {
            index: AdaptiveIndex::Empty,
            keys: Vec::new(),
            counts: (mask & AGGREGATE_COUNT != 0).then(Vec::new),
            sums: (mask & AGGREGATE_TOTAL != 0).then(Vec::new),
            minima: (mask & AGGREGATE_MINIMUM != 0).then(Vec::new),
            maxima: (mask & AGGREGATE_MAXIMUM != 0).then(Vec::new),
            rows: 0,
        }
    }

    fn from_iterators<K, V>(keys: K, values: Option<V>, mask: u8) -> PyResult<Self>
    where
        K: ExactSizeIterator<Item = T>,
        V: Iterator<Item = T>,
    {
        let row_count = keys.len();
        let mut groups = Self::empty(mask);
        let limit = dense_limit(row_count);
        let mut values = values;
        for key in keys {
            let value = values
                .as_mut()
                .map(|values| values.next().expect("equal buffer lengths were validated"));
            if let Some(position) = groups.index.get(key) {
                groups.update(position, value)?;
                continue;
            }

            groups.try_reserve_groups(1)?;
            groups
                .index
                .try_reserve_new(std::slice::from_ref(&key), limit, &groups.keys)?;
            let position = groups.keys.len();
            groups.index.insert(key, position);
            groups.keys.push(key);
            groups.push_lanes(value);
        }
        groups.rows = row_count;
        Ok(groups)
    }

    #[cfg(test)]
    fn from_snapshots(keys: Vec<T>, values: Option<Vec<T>>, mask: u8) -> PyResult<Self> {
        Self::from_iterators(keys.into_iter(), values.map(Vec::into_iter), mask)
    }

    fn try_reserve_groups(&mut self, additional: usize) -> PyResult<()> {
        self.keys
            .try_reserve(additional)
            .map_err(allocation_error)?;
        if let Some(values) = &mut self.counts {
            values.try_reserve(additional).map_err(allocation_error)?;
        }
        if let Some(values) = &mut self.sums {
            values.try_reserve(additional).map_err(allocation_error)?;
        }
        if let Some(values) = &mut self.minima {
            values.try_reserve(additional).map_err(allocation_error)?;
        }
        if let Some(values) = &mut self.maxima {
            values.try_reserve(additional).map_err(allocation_error)?;
        }
        Ok(())
    }

    fn update(&mut self, position: usize, value: Option<T>) -> PyResult<()> {
        if let Some(counts) = &mut self.counts {
            counts[position] = counts[position].checked_add(1).ok_or_else(overflow_error)?;
        }
        if let Some(sums) = &mut self.sums {
            sums[position] = T::checked_sum(
                sums[position],
                value.expect("sum lanes require a value buffer").widened(),
            )
            .ok_or_else(overflow_error)?;
        }
        if let Some(minima) = &mut self.minima {
            minima[position] =
                minima[position].min(value.expect("minimum lanes require a value buffer"));
        }
        if let Some(maxima) = &mut self.maxima {
            maxima[position] =
                maxima[position].max(value.expect("maximum lanes require a value buffer"));
        }
        Ok(())
    }

    fn push_lanes(&mut self, value: Option<T>) {
        if let Some(counts) = &mut self.counts {
            counts.push(1);
        }
        if let Some(sums) = &mut self.sums {
            sums.push(value.expect("sum lanes require a value buffer").widened());
        }
        if let Some(minima) = &mut self.minima {
            minima.push(value.expect("minimum lanes require a value buffer"));
        }
        if let Some(maxima) = &mut self.maxima {
            maxima.push(value.expect("maximum lanes require a value buffer"));
        }
    }

    fn merge(&mut self, partial: &Self) -> PyResult<()> {
        let next_rows = self
            .rows
            .checked_add(partial.rows)
            .ok_or_else(overflow_error)?;
        let mut positions = Vec::new();
        positions
            .try_reserve_exact(partial.keys.len())
            .map_err(allocation_error)?;
        let mut new_keys = Vec::new();
        new_keys
            .try_reserve(partial.keys.len())
            .map_err(allocation_error)?;

        for (partial_position, &key) in partial.keys.iter().enumerate() {
            let position = self.index.get(key);
            if let Some(position) = position {
                self.preflight_merge(position, partial, partial_position)?;
            } else {
                new_keys.push(key);
            }
            positions.push(position);
        }

        self.try_reserve_groups(new_keys.len())?;
        self.index
            .try_reserve_new(&new_keys, dense_limit(next_rows), &self.keys)?;

        for (partial_position, (&key, position)) in partial.keys.iter().zip(positions).enumerate() {
            if let Some(position) = position {
                self.apply_merge(position, partial, partial_position);
            } else {
                let position = self.keys.len();
                self.index.insert(key, position);
                self.keys.push(key);
                self.push_partial_lanes(partial, partial_position);
            }
        }
        self.rows = next_rows;
        Ok(())
    }

    fn preflight_merge(
        &self,
        position: usize,
        partial: &Self,
        partial_position: usize,
    ) -> PyResult<()> {
        if let (Some(current), Some(incoming)) = (&self.counts, &partial.counts) {
            current[position]
                .checked_add(incoming[partial_position])
                .ok_or_else(overflow_error)?;
        }
        if let (Some(current), Some(incoming)) = (&self.sums, &partial.sums) {
            T::checked_sum(current[position], incoming[partial_position])
                .ok_or_else(overflow_error)?;
        }
        Ok(())
    }

    fn apply_merge(&mut self, position: usize, partial: &Self, partial_position: usize) {
        if let (Some(current), Some(incoming)) = (&mut self.counts, &partial.counts) {
            current[position] = current[position]
                .checked_add(incoming[partial_position])
                .expect("commit preflight proved the count merge fits");
        }
        if let (Some(current), Some(incoming)) = (&mut self.sums, &partial.sums) {
            current[position] = T::checked_sum(current[position], incoming[partial_position])
                .expect("commit preflight proved the sum merge fits");
        }
        if let (Some(current), Some(incoming)) = (&mut self.minima, &partial.minima) {
            current[position] = current[position].min(incoming[partial_position]);
        }
        if let (Some(current), Some(incoming)) = (&mut self.maxima, &partial.maxima) {
            current[position] = current[position].max(incoming[partial_position]);
        }
    }

    fn push_partial_lanes(&mut self, partial: &Self, position: usize) {
        if let (Some(current), Some(incoming)) = (&mut self.counts, &partial.counts) {
            current.push(incoming[position]);
        }
        if let (Some(current), Some(incoming)) = (&mut self.sums, &partial.sums) {
            current.push(incoming[position]);
        }
        if let (Some(current), Some(incoming)) = (&mut self.minima, &partial.minima) {
            current.push(incoming[position]);
        }
        if let (Some(current), Some(incoming)) = (&mut self.maxima, &partial.maxima) {
            current.push(incoming[position]);
        }
    }
}

enum TypedGroupData {
    Bool(GroupData<bool>),
    I64(GroupData<i64>),
    U64(GroupData<u64>),
}

impl TypedGroupData {
    fn kind_name(&self) -> &'static str {
        match self {
            Self::Bool(_) => "bool",
            Self::I64(_) => "i64",
            Self::U64(_) => "u64",
        }
    }
}

#[pyclass(module = "fpstreams._native")]
pub(crate) struct NumpyGroupState {
    mask: u8,
    data: Option<TypedGroupData>,
    finalized: bool,
}

#[pyclass(module = "fpstreams._native")]
pub(crate) struct NumpyGroupPartial {
    mask: u8,
    data: Option<TypedGroupData>,
    committed: bool,
}

#[pyfunction]
pub(crate) fn numpy_group_state_v1(lane_mask: u8) -> PyResult<NumpyGroupState> {
    Ok(NumpyGroupState {
        mask: validate_lane_mask(lane_mask)?,
        data: None,
        finalized: false,
    })
}

#[pyfunction]
pub(crate) fn numpy_group_partial_v1(
    keys: &Bound<'_, PyAny>,
    values: Option<&Bound<'_, PyAny>>,
    lane_mask: u8,
) -> PyResult<Option<NumpyGroupPartial>> {
    let mask = validate_lane_mask(lane_mask)?;
    Ok(
        prepare_contiguous_partial(keys, values, mask)?.map(|data| NumpyGroupPartial {
            mask,
            data: Some(data),
            committed: false,
        }),
    )
}

/// Scans a direct one-dimensional signed-stride buffer while the GIL remains attached.
#[cfg(not(Py_GIL_DISABLED))]
#[pyfunction]
pub(crate) fn numpy_group_strided_partial_v2(
    keys: &Bound<'_, PyAny>,
    values: Option<&Bound<'_, PyAny>>,
    lane_mask: u8,
) -> PyResult<Option<NumpyGroupPartial>> {
    let mask = validate_lane_mask(lane_mask)?;
    Ok(
        prepare_strided_partial(keys, values, mask)?.map(|data| NumpyGroupPartial {
            mask,
            data: Some(data),
            committed: false,
        }),
    )
}

#[pyfunction]
pub(crate) fn numpy_group_commit_v1(
    mut state: PyRefMut<'_, NumpyGroupState>,
    mut partial: PyRefMut<'_, NumpyGroupPartial>,
) -> PyResult<()> {
    if state.finalized {
        return Err(PyRuntimeError::new_err(
            "native NumPy grouped state is already finalized",
        ));
    }
    if partial.committed {
        return Err(PyRuntimeError::new_err(
            "native NumPy grouped partial is already committed",
        ));
    }
    if state.mask != partial.mask {
        return Err(PyValueError::new_err(
            "native NumPy grouped state and partial masks differ",
        ));
    }

    if state.data.is_none() {
        state.data = partial.data.take();
        partial.committed = true;
        return Ok(());
    }

    let state_kind = state
        .data
        .as_ref()
        .expect("the non-empty state was checked above")
        .kind_name();
    let partial_kind = partial
        .data
        .as_ref()
        .expect("an uncommitted partial retains its grouped data")
        .kind_name();
    if state_kind != partial_kind {
        return Err(PyValueError::new_err(
            "native NumPy grouped state and partial dtypes differ",
        ));
    }

    match (
        state.data.as_mut().expect("state data exists"),
        partial.data.as_ref().expect("partial data exists"),
    ) {
        (TypedGroupData::Bool(state), TypedGroupData::Bool(partial)) => state.merge(partial)?,
        (TypedGroupData::I64(state), TypedGroupData::I64(partial)) => state.merge(partial)?,
        (TypedGroupData::U64(state), TypedGroupData::U64(partial)) => state.merge(partial)?,
        _ => unreachable!("matching kind names imply matching typed group variants"),
    }
    partial.data = None;
    partial.committed = true;
    Ok(())
}

fn list_object<'py, T>(py: Python<'py>, values: &[T]) -> PyResult<Py<PyAny>>
where
    T: Copy + IntoPyObject<'py>,
{
    Ok(PyList::new(py, values.iter().copied())?.into_any().unbind())
}

fn lane_object<'py, T>(py: Python<'py>, values: Option<&Vec<T>>) -> PyResult<Py<PyAny>>
where
    T: Copy + IntoPyObject<'py>,
{
    match values {
        Some(values) => list_object(py, values),
        None => Ok(py.None()),
    }
}

#[derive(Clone, Copy)]
struct FastI128(i128);

impl<'py> IntoPyObject<'py> for FastI128 {
    type Target = PyInt;
    type Output = Bound<'py, PyInt>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match i64::try_from(self.0) {
            Ok(value) => value.into_pyobject(py),
            Err(_) => self.0.into_pyobject(py),
        }
    }
}

#[derive(Clone, Copy)]
struct FastU128(u128);

impl<'py> IntoPyObject<'py> for FastU128 {
    type Target = PyInt;
    type Output = Bound<'py, PyInt>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match u64::try_from(self.0) {
            Ok(value) => value.into_pyobject(py),
            Err(_) => self.0.into_pyobject(py),
        }
    }
}

trait WidePythonInteger: Copy {
    fn list_object(py: Python<'_>, values: &[Self]) -> PyResult<Py<PyAny>>;
}

impl WidePythonInteger for i128 {
    fn list_object(py: Python<'_>, values: &[Self]) -> PyResult<Py<PyAny>> {
        Ok(PyList::new(py, values.iter().copied().map(FastI128))?
            .into_any()
            .unbind())
    }
}

impl WidePythonInteger for u128 {
    fn list_object(py: Python<'_>, values: &[Self]) -> PyResult<Py<PyAny>> {
        Ok(PyList::new(py, values.iter().copied().map(FastU128))?
            .into_any()
            .unbind())
    }
}

fn wide_lane_object<T: WidePythonInteger>(
    py: Python<'_>,
    values: Option<&Vec<T>>,
) -> PyResult<Py<PyAny>> {
    match values {
        Some(values) => T::list_object(py, values),
        None => Ok(py.None()),
    }
}

fn materialize_data<'py, T>(py: Python<'py>, data: &GroupData<T>) -> PyResult<Py<PyTuple>>
where
    T: GroupScalar + IntoPyObject<'py>,
    T::Sum: WidePythonInteger,
{
    let keys = list_object(py, &data.keys)?;
    let counts = wide_lane_object(py, data.counts.as_ref())?;
    let sums = wide_lane_object(py, data.sums.as_ref())?;
    let minima = lane_object(py, data.minima.as_ref())?;
    let maxima = lane_object(py, data.maxima.as_ref())?;
    Ok(PyTuple::new(py, [keys, counts, sums, minima, maxima])?.unbind())
}

fn materialize_empty(py: Python<'_>, mask: u8) -> PyResult<Py<PyTuple>> {
    let keys = PyList::empty(py).into_any().unbind();
    let lane = |requested: bool| {
        if requested {
            PyList::empty(py).into_any().unbind()
        } else {
            py.None()
        }
    };
    let counts = lane(mask & AGGREGATE_COUNT != 0);
    let sums = lane(mask & AGGREGATE_TOTAL != 0);
    let minima = lane(mask & AGGREGATE_MINIMUM != 0);
    let maxima = lane(mask & AGGREGATE_MAXIMUM != 0);
    Ok(PyTuple::new(py, [keys, counts, sums, minima, maxima])?.unbind())
}

#[pyfunction]
pub(crate) fn numpy_group_finalize_v1(
    py: Python<'_>,
    mut state: PyRefMut<'_, NumpyGroupState>,
) -> PyResult<Py<PyTuple>> {
    if state.finalized {
        return Err(PyRuntimeError::new_err(
            "native NumPy grouped state is already finalized",
        ));
    }
    let output = match state.data.as_ref() {
        Some(TypedGroupData::Bool(data)) => materialize_data(py, data)?,
        Some(TypedGroupData::I64(data)) => materialize_data(py, data)?,
        Some(TypedGroupData::U64(data)) => materialize_data(py, data)?,
        None => materialize_empty(py, state.mask)?,
    };
    state.finalized = true;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn widened_sum_overflow_is_detected_before_mutation() {
        let mut state = GroupData::<u64>::empty(AGGREGATE_TOTAL);
        state.keys.push(1);
        state.sums.as_mut().unwrap().push(u128::MAX);
        state.rows = 1;
        state
            .index
            .try_reserve_new(&[1], dense_limit(1), &[])
            .unwrap();
        state.index.insert(1, 0);

        let partial =
            GroupData::from_snapshots(vec![1_u64], Some(vec![1_u64]), AGGREGATE_TOTAL).unwrap();
        assert!(state.merge(&partial).is_err());
        assert_eq!(state.sums.unwrap(), vec![u128::MAX]);
        assert_eq!(state.rows, 1);
    }

    #[test]
    fn endian_gate_rejects_the_opposite_byte_order() {
        #[cfg(target_endian = "little")]
        {
            assert!(!is_native_endian(c">q"));
            assert!(is_native_endian(c"<q"));
        }
        #[cfg(target_endian = "big")]
        {
            assert!(!is_native_endian(c"<q"));
            assert!(is_native_endian(c">q"));
        }
    }

    #[test]
    fn reserved_dense_slots_do_not_reallocate_for_each_new_group() {
        let mut index = AdaptiveIndex::<i64>::Empty;
        let mut keys = Vec::new();
        for key in 0..INITIAL_DENSE_SLOTS as i64 {
            index
                .try_reserve_new(&[key], 4096, &keys)
                .expect("a compact integer domain should stay dense");
            index.insert(key, keys.len());
            keys.push(key);
        }
        let AdaptiveIndex::Dense { slots, .. } = index else {
            panic!("compact keys unexpectedly migrated to hashing");
        };
        assert_eq!(slots.len(), INITIAL_DENSE_SLOTS);
    }
}
