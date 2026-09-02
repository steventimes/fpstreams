//! Ordered count/sum/min/max kernels for exact tuple and dictionary record sources.

use super::*;

#[derive(Clone, Copy)]
pub(super) enum MultiI64LaneKind {
    Count,
    Sum,
    Min,
    Max,
}

impl MultiI64LaneKind {
    pub(super) fn from_code(code: i64) -> Option<Self> {
        match code {
            0 => Some(Self::Count),
            1 => Some(Self::Sum),
            2 => Some(Self::Min),
            3 => Some(Self::Max),
            _ => None,
        }
    }
}

pub(super) struct MultiTupleLane<'py> {
    pub(super) kind: MultiI64LaneKind,
    pub(super) value_index: Option<isize>,
    pub(super) output_name: Bound<'py, PyString>,
}

pub(super) struct MultiDictLane<'py> {
    pub(super) kind: MultiI64LaneKind,
    pub(super) value_field: Option<Bound<'py, PyString>>,
    pub(super) output_name: Bound<'py, PyString>,
}

enum MultiI64LaneValues {
    Count(Vec<usize>),
    Sum(Vec<i128>),
    Min {
        values: Vec<i64>,
        objects: Vec<Py<PyAny>>,
    },
    Max {
        values: Vec<i64>,
        objects: Vec<Py<PyAny>>,
    },
}

impl MultiI64LaneValues {
    fn new(kind: MultiI64LaneKind) -> Self {
        match kind {
            MultiI64LaneKind::Count => Self::Count(Vec::new()),
            MultiI64LaneKind::Sum => Self::Sum(Vec::new()),
            MultiI64LaneKind::Min => Self::Min {
                values: Vec::new(),
                objects: Vec::new(),
            },
            MultiI64LaneKind::Max => Self::Max {
                values: Vec::new(),
                objects: Vec::new(),
            },
        }
    }

    fn try_reserve_group(&mut self) -> PyResult<()> {
        match self {
            Self::Count(values) => values.try_reserve(1).map_err(group_allocation_error),
            Self::Sum(values) => values.try_reserve(1).map_err(group_allocation_error),
            Self::Min { values, objects } | Self::Max { values, objects } => {
                values.try_reserve(1).map_err(group_allocation_error)?;
                objects.try_reserve(1).map_err(group_allocation_error)
            }
        }
    }

    fn push_group(&mut self, py: Python<'_>, value: i64, value_object: *mut ffi::PyObject) {
        match self {
            Self::Count(values) => values.push(1),
            Self::Sum(values) => values.push(i128::from(value)),
            Self::Min { values, objects } | Self::Max { values, objects } => {
                debug_assert!(!value_object.is_null());
                // SAFETY: the current exact row keeps value_object live while it is retained.
                let object = unsafe { Borrowed::from_ptr(py, value_object).to_owned().unbind() };
                values.push(value);
                objects.push(object);
            }
        }
    }

    fn update_group(
        &mut self,
        py: Python<'_>,
        position: usize,
        value: i64,
        value_object: *mut ffi::PyObject,
    ) -> Option<()> {
        match self {
            Self::Count(values) => {
                values[position] = values[position].checked_add(1)?;
            }
            Self::Sum(values) => {
                values[position] = values[position].checked_add(i128::from(value))?;
            }
            Self::Min { values, objects } => {
                if value < values[position] {
                    debug_assert!(!value_object.is_null());
                    values[position] = value;
                    // SAFETY: the current exact row keeps value_object live while it is retained.
                    objects[position] =
                        unsafe { Borrowed::from_ptr(py, value_object).to_owned().unbind() };
                }
            }
            Self::Max { values, objects } => {
                if value > values[position] {
                    debug_assert!(!value_object.is_null());
                    values[position] = value;
                    // SAFETY: the current exact row keeps value_object live while it is retained.
                    objects[position] =
                        unsafe { Borrowed::from_ptr(py, value_object).to_owned().unbind() };
                }
            }
        }
        Some(())
    }

    fn set_item(
        &self,
        row: &Bound<'_, PyDict>,
        name: &Bound<'_, PyString>,
        position: usize,
    ) -> PyResult<()> {
        match self {
            Self::Count(values) => row.set_item(name, values[position]),
            Self::Sum(values) => set_widened_i64_item(row, name, values[position]),
            Self::Min { objects, .. } | Self::Max { objects, .. } => {
                row.set_item(name, &objects[position])
            }
        }
    }
}

struct MultiI64GroupState {
    positions: I64GroupPositions,
    group_keys: Vec<i64>,
    keys: Vec<Py<PyAny>>,
    lanes: Vec<MultiI64LaneValues>,
    dense_limit: usize,
    object_cache: [ObjectKeyCacheEntry; OBJECT_KEY_CACHE_SLOTS],
}

impl MultiI64GroupState {
    fn new(
        row_count: usize,
        kinds: impl ExactSizeIterator<Item = MultiI64LaneKind>,
    ) -> PyResult<Self> {
        let mut lanes = Vec::new();
        lanes
            .try_reserve_exact(kinds.len())
            .map_err(group_allocation_error)?;
        lanes.extend(kinds.map(MultiI64LaneValues::new));
        Ok(Self {
            positions: I64GroupPositions::Dense(Vec::new()),
            group_keys: Vec::new(),
            keys: Vec::new(),
            lanes,
            dense_limit: row_count
                .saturating_mul(MAX_DENSE_SLOTS_PER_ROW)
                .min(MAX_DENSE_GROUP_SLOTS),
            object_cache: [ObjectKeyCacheEntry {
                object: core::ptr::null_mut(),
                position: usize::MAX,
            }; OBJECT_KEY_CACHE_SLOTS],
        })
    }

    #[inline]
    fn cached_position(&self, key_object: *mut ffi::PyObject) -> Option<usize> {
        let slot = ((key_object as usize) >> 4) & (OBJECT_KEY_CACHE_SLOTS - 1);
        let entry = self.object_cache[slot];
        (entry.object == key_object).then_some(entry.position)
    }

    #[inline]
    fn remember_first_object(&mut self, key_object: *mut ffi::PyObject, position: usize) {
        debug_assert!(position < OBJECT_KEY_CACHE_SLOTS);
        let slot = ((key_object as usize) >> 4) & (OBJECT_KEY_CACHE_SLOTS - 1);
        self.object_cache[slot] = ObjectKeyCacheEntry {
            object: key_object,
            position,
        };
    }

    #[inline]
    fn update_group(
        &mut self,
        py: Python<'_>,
        position: usize,
        lane_slots: &[Option<usize>],
        values: &[i64],
        value_objects: &[*mut ffi::PyObject],
    ) -> Option<()> {
        debug_assert_eq!(self.lanes.len(), lane_slots.len());
        for (lane, slot) in self.lanes.iter_mut().zip(lane_slots) {
            let (value, value_object) = match slot {
                Some(slot) => (values[*slot], value_objects[*slot]),
                None => (0, core::ptr::null_mut()),
            };
            lane.update_group(py, position, value, value_object)?;
        }
        Some(())
    }

    #[inline(always)]
    fn add<const USE_OBJECT_CACHE: bool>(
        &mut self,
        py: Python<'_>,
        key_object: *mut ffi::PyObject,
        key: i64,
        lane_slots: &[Option<usize>],
        values: &[i64],
        value_objects: &[*mut ffi::PyObject],
    ) -> PyResult<Option<()>> {
        if USE_OBJECT_CACHE && let Some(position) = self.cached_position(key_object) {
            return Ok(self.update_group(py, position, lane_slots, values, value_objects));
        }
        if let Some(position) =
            self.positions
                .position(key, self.dense_limit, self.group_keys.iter().copied())?
        {
            let result = self.update_group(py, position, lane_slots, values, value_objects);
            if USE_OBJECT_CACHE && self.keys[position].bind(py).as_ptr() == key_object {
                self.remember_first_object(key_object, position);
            }
            return Ok(result);
        }

        self.positions.try_reserve_group()?;
        self.group_keys
            .try_reserve(1)
            .map_err(group_allocation_error)?;
        self.keys.try_reserve(1).map_err(group_allocation_error)?;
        for lane in &mut self.lanes {
            lane.try_reserve_group()?;
        }
        // SAFETY: the row or exact tuple keeps key_object live until this strong reference exists.
        let first_key = unsafe { Borrowed::from_ptr(py, key_object).to_owned().unbind() };
        let position = self.keys.len();
        self.positions.insert(key, position);
        self.group_keys.push(key);
        self.keys.push(first_key);
        for (lane, slot) in self.lanes.iter_mut().zip(lane_slots) {
            let (value, value_object) = match slot {
                Some(slot) => (values[*slot], value_objects[*slot]),
                None => (0, core::ptr::null_mut()),
            };
            lane.push_group(py, value, value_object);
        }
        if USE_OBJECT_CACHE && position < OBJECT_KEY_CACHE_SLOTS {
            self.remember_first_object(key_object, position);
        }
        Ok(Some(()))
    }
}

struct MultiScratch {
    values: Vec<i64>,
    value_objects: Vec<*mut ffi::PyObject>,
}

impl MultiScratch {
    const fn new() -> Self {
        Self {
            values: Vec::new(),
            value_objects: Vec::new(),
        }
    }

    fn resize(&mut self, slots: usize) -> PyResult<()> {
        self.values
            .try_reserve(slots.saturating_sub(self.values.len()))
            .map_err(group_allocation_error)?;
        self.value_objects
            .try_reserve(slots.saturating_sub(self.value_objects.len()))
            .map_err(group_allocation_error)?;
        self.values.resize(slots, 0);
        self.value_objects.resize(slots, core::ptr::null_mut());
        Ok(())
    }
}

pub(super) fn parse_multi_tuple_lanes<'py>(
    lanes: &Bound<'py, PyAny>,
) -> PyResult<Option<Vec<MultiTupleLane<'py>>>> {
    let lanes = match lanes.cast_exact::<PyTuple>() {
        Ok(lanes) if !lanes.is_empty() => lanes,
        Ok(_) | Err(_) => return Ok(None),
    };
    let mut parsed = Vec::new();
    parsed
        .try_reserve_exact(lanes.len())
        .map_err(group_allocation_error)?;
    for raw_lane in lanes.iter() {
        let descriptor = match raw_lane.cast_exact::<PyTuple>() {
            Ok(descriptor) if descriptor.len() == 3 => descriptor,
            Ok(_) | Err(_) => return Ok(None),
        };
        let Some(kind) = exact_i64(lanes.py(), descriptor.get_item(0)?.as_ptr())?
            .and_then(MultiI64LaneKind::from_code)
        else {
            return Ok(None);
        };
        let selector = descriptor.get_item(1)?;
        let value_index = if matches!(kind, MultiI64LaneKind::Count) {
            if !selector.is_none() {
                return Ok(None);
            }
            None
        } else {
            let Some(value_index) = exact_i64(lanes.py(), selector.as_ptr())? else {
                return Ok(None);
            };
            let Ok(value_index) = isize::try_from(value_index) else {
                return Ok(None);
            };
            Some(value_index)
        };
        let output_name = match descriptor.get_item(2)?.cast_into_exact::<PyString>() {
            Ok(name) => name,
            Err(_) => return Ok(None),
        };
        parsed.push(MultiTupleLane {
            kind,
            value_index,
            output_name,
        });
    }
    Ok(Some(parsed))
}

pub(super) fn parse_multi_dict_lanes<'py>(
    lanes: &Bound<'py, PyAny>,
) -> PyResult<Option<Vec<MultiDictLane<'py>>>> {
    let lanes = match lanes.cast_exact::<PyTuple>() {
        Ok(lanes) if !lanes.is_empty() => lanes,
        Ok(_) | Err(_) => return Ok(None),
    };
    let mut parsed = Vec::new();
    parsed
        .try_reserve_exact(lanes.len())
        .map_err(group_allocation_error)?;
    for raw_lane in lanes.iter() {
        let descriptor = match raw_lane.cast_exact::<PyTuple>() {
            Ok(descriptor) if descriptor.len() == 3 => descriptor,
            Ok(_) | Err(_) => return Ok(None),
        };
        let Some(kind) = exact_i64(lanes.py(), descriptor.get_item(0)?.as_ptr())?
            .and_then(MultiI64LaneKind::from_code)
        else {
            return Ok(None);
        };
        let selector = descriptor.get_item(1)?;
        let value_field = if matches!(kind, MultiI64LaneKind::Count) {
            if !selector.is_none() {
                return Ok(None);
            }
            None
        } else {
            match selector.cast_into_exact::<PyString>() {
                Ok(field) => Some(field),
                Err(_) => return Ok(None),
            }
        };
        let output_name = match descriptor.get_item(2)?.cast_into_exact::<PyString>() {
            Ok(name) => name,
            Err(_) => return Ok(None),
        };
        parsed.push(MultiDictLane {
            kind,
            value_field,
            output_name,
        });
    }
    Ok(Some(parsed))
}

struct MultiTupleLayout {
    width: usize,
    key_position: usize,
    lane_slots: Vec<Option<usize>>,
    slot_positions: Vec<usize>,
}

struct MultiTuplePlan<'lanes, 'py> {
    key_index: isize,
    lanes: &'lanes [MultiTupleLane<'py>],
    required_width: Option<usize>,
}

#[inline(always)]
fn group_multi_exact_tuple_row<const USE_OBJECT_CACHE: bool>(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    plan: &MultiTuplePlan<'_, '_>,
    cached_layout: &mut Option<MultiTupleLayout>,
    state: &mut MultiI64GroupState,
    scratch: &mut MultiScratch,
) -> PyResult<Option<()>> {
    // SAFETY: the outer exact sequence or owned free-threaded snapshot keeps row live.
    if unsafe { ffi::PyTuple_CheckExact(row) } == 0 {
        return Ok(None);
    }
    // SAFETY: row was proven to be an exact tuple.
    let width = unsafe { ffi::PyTuple_Size(row) };
    if width < 0 {
        return Err(PyErr::fetch(py));
    }
    let width = width as usize;
    if plan
        .required_width
        .is_some_and(|required| width != required)
    {
        return Ok(None);
    }
    if cached_layout
        .as_ref()
        .is_none_or(|layout| layout.width != width)
    {
        let Some(key_position) = normalize_index(plan.key_index, width) else {
            return Ok(None);
        };
        let mut lane_slots = Vec::new();
        lane_slots
            .try_reserve_exact(plan.lanes.len())
            .map_err(group_allocation_error)?;
        let mut slot_positions = Vec::new();
        slot_positions
            .try_reserve_exact(plan.lanes.len())
            .map_err(group_allocation_error)?;
        for lane in plan.lanes {
            let slot = match lane.value_index {
                Some(index) => {
                    let Some(position) = normalize_index(index, width) else {
                        return Ok(None);
                    };
                    match slot_positions.iter().position(|&item| item == position) {
                        Some(slot) => Some(slot),
                        None => {
                            let slot = slot_positions.len();
                            slot_positions.push(position);
                            Some(slot)
                        }
                    }
                }
                None => None,
            };
            lane_slots.push(slot);
        }
        *cached_layout = Some(MultiTupleLayout {
            width,
            key_position,
            lane_slots,
            slot_positions,
        });
        scratch.resize(
            cached_layout
                .as_ref()
                .expect("the tuple layout was just initialized")
                .slot_positions
                .len(),
        )?;
    }
    let layout = cached_layout
        .as_ref()
        .expect("the current exact tuple width has a normalized layout");
    // SAFETY: key_position was normalized against this tuple width.
    let key_object = unsafe { ffi::PyTuple_GetItem(row, layout.key_position as ffi::Py_ssize_t) };
    if key_object.is_null() {
        return Err(PyErr::fetch(py));
    }
    let Some(key) = exact_i64(py, key_object)? else {
        return Ok(None);
    };
    for ((position, target), target_object) in layout
        .slot_positions
        .iter()
        .zip(scratch.values.iter_mut())
        .zip(scratch.value_objects.iter_mut())
    {
        // SAFETY: every value position was normalized against this exact tuple width.
        let value_object = unsafe { ffi::PyTuple_GetItem(row, *position as ffi::Py_ssize_t) };
        if value_object.is_null() {
            return Err(PyErr::fetch(py));
        }
        let Some(value) = exact_i64(py, value_object)? else {
            return Ok(None);
        };
        *target = value;
        *target_object = value_object;
    }
    state.add::<USE_OBJECT_CACHE>(
        py,
        key_object,
        key,
        &layout.lane_slots,
        &scratch.values,
        &scratch.value_objects,
    )
}

fn group_multi_exact_tuple_sequence(
    py: Python<'_>,
    row_count: usize,
    mut get_row: impl FnMut(usize) -> *mut ffi::PyObject,
    key_index: isize,
    lanes: &[MultiTupleLane<'_>],
    required_width: Option<usize>,
) -> PyResult<Option<MultiI64GroupState>> {
    let mut state = MultiI64GroupState::new(row_count, lanes.iter().map(|lane| lane.kind))?;
    let mut scratch = MultiScratch::new();
    let mut cached_layout = None;
    let plan = MultiTuplePlan {
        key_index,
        lanes,
        required_width,
    };
    let mut row_index = 0;
    while row_index < row_count && state.keys.len() <= OBJECT_KEY_CACHE_SLOTS {
        let row = get_row(row_index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
        if group_multi_exact_tuple_row::<true>(
            py,
            row,
            &plan,
            &mut cached_layout,
            &mut state,
            &mut scratch,
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
        if group_multi_exact_tuple_row::<false>(
            py,
            row,
            &plan,
            &mut cached_layout,
            &mut state,
            &mut scratch,
        )?
        .is_none()
        {
            return Ok(None);
        }
        row_index += 1;
    }
    Ok(Some(state))
}

fn group_multi_i64_tuple_source(
    source: &Bound<'_, PyAny>,
    key_index: isize,
    lanes: &[MultiTupleLane<'_>],
    required_width: Option<usize>,
) -> PyResult<Option<MultiI64GroupState>> {
    if let Ok(rows) = source.cast_exact::<PyList>() {
        #[cfg(not(Py_GIL_DISABLED))]
        return group_multi_exact_tuple_sequence(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: a GIL build prevents exact-list mutation during this attached call.
                unsafe { ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            key_index,
            lanes,
            required_width,
        );
        #[cfg(Py_GIL_DISABLED)]
        {
            let snapshot = snapshot_exact_list_rows(source.py(), source, rows)?;
            return group_multi_exact_tuple_sequence(
                source.py(),
                snapshot.len(),
                |index| snapshot[index].bind(source.py()).as_ptr(),
                key_index,
                lanes,
                required_width,
            );
        }
    }
    if let Ok(rows) = source.cast_exact::<PyTuple>() {
        return group_multi_exact_tuple_sequence(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: exact outer tuples are immutable and index is within fixed length.
                unsafe { ffi::PyTuple_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            key_index,
            lanes,
            required_width,
        );
    }
    Ok(None)
}

struct MultiDictLayout<'py> {
    lane_slots: Vec<Option<usize>>,
    selectors: Vec<Bound<'py, PyString>>,
}

fn multi_dict_layout<'py>(lanes: &[MultiDictLane<'py>]) -> PyResult<MultiDictLayout<'py>> {
    let mut lane_slots = Vec::new();
    lane_slots
        .try_reserve_exact(lanes.len())
        .map_err(group_allocation_error)?;
    let mut selectors: Vec<Bound<'py, PyString>> = Vec::new();
    selectors
        .try_reserve_exact(lanes.len())
        .map_err(group_allocation_error)?;
    for lane in lanes {
        let Some(field) = &lane.value_field else {
            lane_slots.push(None);
            continue;
        };
        let mut matching_slot = None;
        for (slot, selector) in selectors.iter().enumerate() {
            // Parsed selectors are exact strings, so equality cannot dispatch user code.
            if selector.as_any().eq(field.as_any())? {
                matching_slot = Some(slot);
                break;
            }
        }
        let slot = matching_slot.unwrap_or_else(|| {
            let slot = selectors.len();
            selectors.push(field.clone());
            slot
        });
        lane_slots.push(Some(slot));
    }
    Ok(MultiDictLayout {
        lane_slots,
        selectors,
    })
}

#[inline(always)]
fn group_multi_exact_dict_row<const USE_OBJECT_CACHE: bool>(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    key_field: *mut ffi::PyObject,
    layout: &MultiDictLayout<'_>,
    state: &mut MultiI64GroupState,
    scratch: &mut MultiScratch,
) -> PyResult<Option<()>> {
    // SAFETY: the outer exact sequence or owned free-threaded snapshot keeps row live.
    if unsafe { ffi::PyDict_CheckExact(row) } == 0 {
        return Ok(None);
    }
    // SAFETY: row remains live for this entire call.
    let row_bound = unsafe { Borrowed::from_ptr(py, row) };
    with_critical_section(row_bound.as_any(), || {
        let field_count = unsafe { ffi::PyDict_Size(row) };
        if field_count < 0 {
            return Err(PyErr::fetch(py));
        }
        let field_count = usize::try_from(field_count)
            .map_err(|_| PyMemoryError::new_err("native record field count is too large"))?;
        if field_count > RECORD_GROUP_SUM_MAX_FIELDS {
            return Ok(None);
        }
        let mut position = 0;
        let mut field = core::ptr::null_mut();
        let mut field_value = core::ptr::null_mut();
        for _ in 0..field_count {
            // SAFETY: the exact dict is protected by its critical section and size is fixed.
            if unsafe { ffi::PyDict_Next(row, &mut position, &mut field, &mut field_value) } == 0 {
                return Ok(None);
            }
            // SAFETY: PyDict_Next returned one live key.
            if unsafe { ffi::PyUnicode_CheckExact(field) } == 0 {
                return Ok(None);
            }
        }
        let Some(key_object) = dict_item(py, row, key_field)? else {
            return Ok(None);
        };
        let Some(key) = exact_i64(py, key_object)? else {
            return Ok(None);
        };
        for ((value_field, target), target_object) in layout
            .selectors
            .iter()
            .zip(scratch.values.iter_mut())
            .zip(scratch.value_objects.iter_mut())
        {
            let Some(value_object) = dict_item(py, row, value_field.as_ptr())? else {
                return Ok(None);
            };
            let Some(value) = exact_i64(py, value_object)? else {
                return Ok(None);
            };
            *target = value;
            *target_object = value_object;
        }
        state.add::<USE_OBJECT_CACHE>(
            py,
            key_object,
            key,
            &layout.lane_slots,
            &scratch.values,
            &scratch.value_objects,
        )
    })
}

fn group_multi_exact_dict_sequence(
    py: Python<'_>,
    row_count: usize,
    mut get_row: impl FnMut(usize) -> *mut ffi::PyObject,
    key_field: *mut ffi::PyObject,
    lanes: &[MultiDictLane<'_>],
) -> PyResult<Option<MultiI64GroupState>> {
    let mut state = MultiI64GroupState::new(row_count, lanes.iter().map(|lane| lane.kind))?;
    let layout = multi_dict_layout(lanes)?;
    let mut scratch = MultiScratch::new();
    scratch.resize(layout.selectors.len())?;
    let mut row_index = 0;
    while row_index < row_count && state.keys.len() <= OBJECT_KEY_CACHE_SLOTS {
        let row = get_row(row_index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
        if group_multi_exact_dict_row::<true>(
            py,
            row,
            key_field,
            &layout,
            &mut state,
            &mut scratch,
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
        if group_multi_exact_dict_row::<false>(
            py,
            row,
            key_field,
            &layout,
            &mut state,
            &mut scratch,
        )?
        .is_none()
        {
            return Ok(None);
        }
        row_index += 1;
    }
    Ok(Some(state))
}

fn group_multi_i64_dict_source(
    source: &Bound<'_, PyAny>,
    key_field: *mut ffi::PyObject,
    lanes: &[MultiDictLane<'_>],
) -> PyResult<Option<MultiI64GroupState>> {
    if let Ok(rows) = source.cast_exact::<PyList>() {
        #[cfg(not(Py_GIL_DISABLED))]
        return group_multi_exact_dict_sequence(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: a GIL build prevents exact-list mutation during this attached call.
                unsafe { ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            key_field,
            lanes,
        );
        #[cfg(Py_GIL_DISABLED)]
        {
            let snapshot = snapshot_exact_list_rows(source.py(), source, rows)?;
            return group_multi_exact_dict_sequence(
                source.py(),
                snapshot.len(),
                |index| snapshot[index].bind(source.py()).as_ptr(),
                key_field,
                lanes,
            );
        }
    }
    if let Ok(rows) = source.cast_exact::<PyTuple>() {
        return group_multi_exact_dict_sequence(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: exact outer tuples are immutable and index is within fixed length.
                unsafe { ffi::PyTuple_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            key_field,
            lanes,
        );
    }
    Ok(None)
}

fn materialize_multi_group_rows(
    py: Python<'_>,
    state: MultiI64GroupState,
    key_name: &Bound<'_, PyString>,
    output_names: &[Bound<'_, PyString>],
) -> PyResult<Py<PyAny>> {
    debug_assert_eq!(state.lanes.len(), output_names.len());
    let mut rows = Vec::new();
    rows.try_reserve(state.keys.len())
        .map_err(group_allocation_error)?;
    for (position, key) in state.keys.into_iter().enumerate() {
        let row = new_dict_fallible(py)?;
        row.set_item(key_name, key)?;
        for (lane, output_name) in state.lanes.iter().zip(output_names) {
            lane.set_item(&row, output_name, position)?;
        }
        rows.push(row.unbind());
    }
    Ok(PyList::new(py, rows)?.into_any().unbind())
}

#[pyfunction]
/// Group exact tuple rows through ordered count/sum/min/max lanes in one scan.
pub(crate) fn group_multi_i64_rows_v1(
    source: &Bound<'_, PyAny>,
    key_index: isize,
    key_name: &Bound<'_, PyAny>,
    lanes: &Bound<'_, PyAny>,
) -> PyResult<Option<Py<PyAny>>> {
    let key_name = match key_name.cast_exact::<PyString>() {
        Ok(name) => name,
        Err(_) => return Ok(None),
    };
    let Some(lanes) = parse_multi_tuple_lanes(lanes)? else {
        return Ok(None);
    };
    let Some(state) = group_multi_i64_tuple_source(source, key_index, &lanes, None)? else {
        return Ok(None);
    };
    let mut output_names = Vec::new();
    output_names
        .try_reserve_exact(lanes.len())
        .map_err(group_allocation_error)?;
    output_names.extend(lanes.iter().map(|lane| lane.output_name.clone()));
    materialize_multi_group_rows(source.py(), state, key_name, &output_names).map(Some)
}

#[pyfunction]
/// Group strict exact key/value tuples through ordered count/sum/min/max lanes in one scan.
pub(crate) fn group_multi_i64_exact_pairs_v1(
    source: &Bound<'_, PyAny>,
    key_name: &Bound<'_, PyAny>,
    lanes: &Bound<'_, PyAny>,
) -> PyResult<Option<Py<PyAny>>> {
    let key_name = match key_name.cast_exact::<PyString>() {
        Ok(name) => name,
        Err(_) => return Ok(None),
    };
    let Some(lanes) = parse_multi_tuple_lanes(lanes)? else {
        return Ok(None);
    };
    if lanes
        .iter()
        .any(|lane| lane.value_index.is_some_and(|index| index != 1))
    {
        return Ok(None);
    }
    let Some(state) = group_multi_i64_tuple_source(source, 0, &lanes, Some(2))? else {
        return Ok(None);
    };
    let mut output_names = Vec::new();
    output_names
        .try_reserve_exact(lanes.len())
        .map_err(group_allocation_error)?;
    output_names.extend(lanes.iter().map(|lane| lane.output_name.clone()));
    materialize_multi_group_rows(source.py(), state, key_name, &output_names).map(Some)
}

#[pyfunction]
/// Group exact dictionary rows through ordered count/sum/min/max lanes in one scan.
pub(crate) fn group_multi_i64_dict_rows_v1(
    source: &Bound<'_, PyAny>,
    key_field: &Bound<'_, PyAny>,
    key_name: &Bound<'_, PyAny>,
    lanes: &Bound<'_, PyAny>,
) -> PyResult<Option<Py<PyAny>>> {
    let key_field = match key_field.cast_exact::<PyString>() {
        Ok(field) => field,
        Err(_) => return Ok(None),
    };
    let key_name = match key_name.cast_exact::<PyString>() {
        Ok(name) => name,
        Err(_) => return Ok(None),
    };
    let Some(lanes) = parse_multi_dict_lanes(lanes)? else {
        return Ok(None);
    };
    let Some(state) = group_multi_i64_dict_source(source, key_field.as_ptr(), &lanes)? else {
        return Ok(None);
    };
    let mut output_names = Vec::new();
    output_names
        .try_reserve_exact(lanes.len())
        .map_err(group_allocation_error)?;
    output_names.extend(lanes.iter().map(|lane| lane.output_name.clone()));
    materialize_multi_group_rows(source.py(), state, key_name, &output_names).map(Some)
}

#[inline(never)]
pub(super) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(group_multi_i64_rows_v1, module)?)?;
    module.add_function(wrap_pyfunction!(group_multi_i64_exact_pairs_v1, module)?)?;
    module.add_function(wrap_pyfunction!(group_multi_i64_dict_rows_v1, module)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dictionary_layout_reuses_equal_nonidentical_selector_strings() {
        Python::initialize();
        Python::attach(|py| {
            let first = PyString::new(py, "selected");
            let second = PyString::new(py, "selected");
            assert_ne!(first.as_ptr(), second.as_ptr());
            let lanes = vec![
                MultiDictLane {
                    kind: MultiI64LaneKind::Count,
                    value_field: None,
                    output_name: PyString::new(py, "rows"),
                },
                MultiDictLane {
                    kind: MultiI64LaneKind::Sum,
                    value_field: Some(first),
                    output_name: PyString::new(py, "total"),
                },
                MultiDictLane {
                    kind: MultiI64LaneKind::Min,
                    value_field: Some(second),
                    output_name: PyString::new(py, "low"),
                },
            ];

            let layout = multi_dict_layout(&lanes).unwrap();

            assert_eq!(layout.selectors.len(), 1);
            assert_eq!(layout.lane_slots, [None, Some(0), Some(0)]);
        });
    }

    #[test]
    fn exact_pair_multi_group_requires_two_item_rows() {
        Python::initialize();
        Python::attach(|py| {
            let none = py.None();
            let count_code = pyo3::types::PyInt::new(py, 0);
            let sum_code = pyo3::types::PyInt::new(py, 1);
            let value_index = pyo3::types::PyInt::new(py, 1);
            let count_name = PyString::new(py, "count");
            let sum_name = PyString::new(py, "total");
            let count_lane = PyTuple::new(
                py,
                [count_code.as_any(), none.bind(py), count_name.as_any()],
            )
            .unwrap();
            let sum_lane = PyTuple::new(
                py,
                [sum_code.as_any(), value_index.as_any(), sum_name.as_any()],
            )
            .unwrap();
            let lanes = PyTuple::new(py, [count_lane.as_any(), sum_lane.as_any()]).unwrap();
            let key_name = PyString::new(py, "key");

            let pair = PyTuple::new(py, [1_i32, 2_i32]).unwrap();
            let pairs = PyList::new(py, [&pair]).unwrap();
            assert!(
                group_multi_i64_exact_pairs_v1(pairs.as_any(), key_name.as_any(), lanes.as_any(),)
                    .unwrap()
                    .is_some()
            );

            for row in [
                PyTuple::new(py, [1_i32]).unwrap().into_any(),
                PyTuple::new(py, [1_i32, 2_i32, 3_i32]).unwrap().into_any(),
            ] {
                let invalid = PyList::new(py, [&row]).unwrap();
                assert!(
                    group_multi_i64_exact_pairs_v1(
                        invalid.as_any(),
                        key_name.as_any(),
                        lanes.as_any(),
                    )
                    .unwrap()
                    .is_none()
                );
            }
        });
    }
}
