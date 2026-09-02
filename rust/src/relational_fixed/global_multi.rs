//! Global count/sum/min/max kernels for exact tuple and dictionary record sources.

use super::group_multi::{
    MultiDictLane, MultiI64LaneKind, MultiTupleLane, parse_multi_dict_lanes,
    parse_multi_tuple_lanes,
};
use super::*;

#[path = "global_multi/same_field.rs"]
mod same_field;

use same_field::{
    materialize_same_dict_global, materialize_same_tuple_global, reduce_same_dict_global,
    reduce_same_tuple_global, same_dict_field_plan, same_tuple_field_plan,
};

#[cfg(test)]
use same_field::{GLOBAL_COUNT, GLOBAL_MAX, GLOBAL_MIN, GLOBAL_SUM, SameFieldGlobalPlan};

struct GlobalI64Extremum {
    value: Option<i64>,
    object: Option<Py<PyAny>>,
    #[cfg(not(Py_GIL_DISABLED))]
    pending_object: *mut ffi::PyObject,
}

impl GlobalI64Extremum {
    const fn new() -> Self {
        Self {
            value: None,
            object: None,
            #[cfg(not(Py_GIL_DISABLED))]
            pending_object: core::ptr::null_mut(),
        }
    }

    #[inline]
    fn accept<const MAXIMUM: bool>(
        &mut self,
        _py: Python<'_>,
        value: i64,
        value_object: *mut ffi::PyObject,
    ) {
        if self.value.is_some_and(|current| {
            if MAXIMUM {
                value <= current
            } else {
                value >= current
            }
        }) {
            return;
        }
        debug_assert!(!value_object.is_null());
        #[cfg(not(Py_GIL_DISABLED))]
        {
            // The attached ABI wrapper retains the exact source until it promotes the final
            // pointers immediately after a successful scan. No Python callback or detach occurs
            // between this borrow and that promotion.
            self.pending_object = value_object;
        }
        #[cfg(Py_GIL_DISABLED)]
        {
            // A row dictionary can change after its critical section on free-threaded Python, so
            // every selected extremum must remain independently owned during the scan.
            self.object =
                Some(unsafe { Borrowed::from_ptr(_py, value_object).to_owned().unbind() });
        }
        self.value = Some(value);
    }

    #[inline]
    fn promote(&mut self, _py: Python<'_>) {
        #[cfg(not(Py_GIL_DISABLED))]
        {
            if self.pending_object.is_null() {
                debug_assert!(self.value.is_none() || self.object.is_some());
                return;
            }
            debug_assert!(self.value.is_some());
            debug_assert!(self.object.is_none());
            // SAFETY: a successful attached scan still holds the GIL and the source Bound that
            // owns every row. Py_INCREF cannot call Python, so promotion completes before result
            // allocation introduces any possible callback or collection boundary.
            self.object = Some(unsafe {
                Borrowed::from_ptr(_py, self.pending_object)
                    .to_owned()
                    .unbind()
            });
            self.pending_object = core::ptr::null_mut();
        }
        #[cfg(Py_GIL_DISABLED)]
        {
            debug_assert_eq!(self.value.is_some(), self.object.is_some());
        }
    }

    fn set_item(
        &self,
        py: Python<'_>,
        row: &Bound<'_, PyDict>,
        name: &Bound<'_, PyString>,
    ) -> PyResult<()> {
        #[cfg(not(Py_GIL_DISABLED))]
        debug_assert!(self.pending_object.is_null());
        match &self.object {
            Some(object) => row.set_item(name, object),
            None => row.set_item(name, py.None()),
        }
    }
}

enum GlobalI64LaneValue {
    Count(usize),
    Sum(i128),
    Min(GlobalI64Extremum),
    Max(GlobalI64Extremum),
}

impl GlobalI64LaneValue {
    fn new(kind: MultiI64LaneKind) -> Self {
        match kind {
            MultiI64LaneKind::Count => Self::Count(0),
            MultiI64LaneKind::Sum => Self::Sum(0),
            MultiI64LaneKind::Min => Self::Min(GlobalI64Extremum::new()),
            MultiI64LaneKind::Max => Self::Max(GlobalI64Extremum::new()),
        }
    }

    #[inline]
    fn accept(
        &mut self,
        py: Python<'_>,
        value: i64,
        value_object: *mut ffi::PyObject,
    ) -> Option<()> {
        match self {
            Self::Count(count) => {
                *count = count.checked_add(1)?;
            }
            Self::Sum(total) => {
                *total = total.checked_add(i128::from(value))?;
            }
            Self::Min(extremum) => extremum.accept::<false>(py, value, value_object),
            Self::Max(extremum) => extremum.accept::<true>(py, value, value_object),
        }
        Some(())
    }

    #[inline]
    fn promote_extremum(&mut self, py: Python<'_>) {
        match self {
            Self::Min(extremum) | Self::Max(extremum) => extremum.promote(py),
            Self::Count(_) | Self::Sum(_) => {}
        }
    }

    fn set_item(
        &self,
        py: Python<'_>,
        row: &Bound<'_, PyDict>,
        name: &Bound<'_, PyString>,
    ) -> PyResult<()> {
        match self {
            Self::Count(count) => row.set_item(name, count),
            Self::Sum(total) => set_widened_i64_item(row, name, *total),
            Self::Min(extremum) | Self::Max(extremum) => extremum.set_item(py, row, name),
        }
    }
}

struct GlobalI64State {
    lanes: Vec<GlobalI64LaneValue>,
}

impl GlobalI64State {
    fn new<I>(kinds: I) -> PyResult<Self>
    where
        I: ExactSizeIterator<Item = MultiI64LaneKind>,
    {
        let mut lanes = Vec::new();
        lanes
            .try_reserve_exact(kinds.len())
            .map_err(group_allocation_error)?;
        lanes.extend(kinds.map(GlobalI64LaneValue::new));
        Ok(Self { lanes })
    }

    #[inline]
    fn accept(
        &mut self,
        py: Python<'_>,
        lane_index: usize,
        value: i64,
        value_object: *mut ffi::PyObject,
    ) -> Option<()> {
        self.lanes[lane_index].accept(py, value, value_object)
    }

    #[inline]
    fn promote_extrema(&mut self, py: Python<'_>) {
        for lane in &mut self.lanes {
            lane.promote_extremum(py);
        }
    }
}

struct GlobalTupleLayout {
    width: usize,
    lane_slots: Vec<Option<usize>>,
    slot_positions: Vec<usize>,
}

struct GlobalDictLayout<'py> {
    lane_slots: Vec<Option<usize>>,
    selectors: Vec<Bound<'py, PyString>>,
}

struct GlobalValueScratch {
    values: Vec<i64>,
    objects: Vec<*mut ffi::PyObject>,
}

impl GlobalValueScratch {
    fn resize(&mut self, slots: usize) -> PyResult<()> {
        self.values
            .try_reserve(slots.saturating_sub(self.values.len()))
            .map_err(group_allocation_error)?;
        self.objects
            .try_reserve(slots.saturating_sub(self.objects.len()))
            .map_err(group_allocation_error)?;
        self.values.resize(slots, 0);
        self.objects.resize(slots, core::ptr::null_mut());
        Ok(())
    }
}

#[inline(always)]
fn global_multi_exact_tuple_row(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    lanes: &[MultiTupleLane<'_>],
    layout: &mut GlobalTupleLayout,
    state: &mut GlobalI64State,
    scratch: &mut GlobalValueScratch,
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
    if layout.width != width {
        let mut value_positions = Vec::new();
        value_positions
            .try_reserve_exact(lanes.len())
            .map_err(group_allocation_error)?;
        let mut lane_slots = Vec::new();
        lane_slots
            .try_reserve_exact(lanes.len())
            .map_err(group_allocation_error)?;
        for lane in lanes {
            let position = match lane.value_index {
                Some(index) => {
                    let Some(position) = normalize_index(index, width) else {
                        return Ok(None);
                    };
                    Some(position)
                }
                None => None,
            };
            let slot = match position {
                Some(position) => match value_positions.iter().position(|&item| item == position) {
                    Some(slot) => Some(slot),
                    None => {
                        let slot = value_positions.len();
                        value_positions.push(position);
                        Some(slot)
                    }
                },
                None => None,
            };
            lane_slots.push(slot);
        }
        *layout = GlobalTupleLayout {
            width,
            lane_slots,
            slot_positions: value_positions,
        };
        scratch.resize(layout.slot_positions.len())?;
    }
    for (slot, &position) in layout.slot_positions.iter().enumerate() {
        // SAFETY: every value position was normalized against this exact tuple width.
        let value_object = unsafe { ffi::PyTuple_GetItem(row, position as ffi::Py_ssize_t) };
        if value_object.is_null() {
            return Err(PyErr::fetch(py));
        }
        let Some(value) = exact_i64(py, value_object)? else {
            return Ok(None);
        };
        scratch.values[slot] = value;
        scratch.objects[slot] = value_object;
    }
    for (lane_index, slot) in layout.lane_slots.iter().enumerate() {
        let Some(slot) = slot else {
            debug_assert!(matches!(lanes[lane_index].kind, MultiI64LaneKind::Count));
            if state
                .accept(py, lane_index, 0, core::ptr::null_mut())
                .is_none()
            {
                return Ok(None);
            }
            continue;
        };
        if state
            .accept(
                py,
                lane_index,
                scratch.values[*slot],
                scratch.objects[*slot],
            )
            .is_none()
        {
            return Ok(None);
        }
    }
    Ok(Some(()))
}

fn global_multi_exact_tuple_sequence(
    py: Python<'_>,
    row_count: usize,
    mut get_row: impl FnMut(usize) -> *mut ffi::PyObject,
    lanes: &[MultiTupleLane<'_>],
) -> PyResult<Option<GlobalI64State>> {
    let mut state = GlobalI64State::new(lanes.iter().map(|lane| lane.kind))?;
    let mut layout = GlobalTupleLayout {
        width: usize::MAX,
        lane_slots: Vec::new(),
        slot_positions: Vec::new(),
    };
    let mut scratch = GlobalValueScratch {
        values: Vec::new(),
        objects: Vec::new(),
    };
    for index in 0..row_count {
        let row = get_row(index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
        if global_multi_exact_tuple_row(py, row, lanes, &mut layout, &mut state, &mut scratch)?
            .is_none()
        {
            return Ok(None);
        }
    }
    Ok(Some(state))
}

fn global_multi_i64_tuple_source(
    source: &Bound<'_, PyAny>,
    lanes: &[MultiTupleLane<'_>],
) -> PyResult<Option<GlobalI64State>> {
    if let Ok(rows) = source.cast_exact::<PyList>() {
        #[cfg(not(Py_GIL_DISABLED))]
        return global_multi_exact_tuple_sequence(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: a GIL build prevents exact-list mutation during this attached call.
                unsafe { ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            lanes,
        );
        #[cfg(Py_GIL_DISABLED)]
        {
            let snapshot = snapshot_exact_list_rows(source.py(), source, rows)?;
            return global_multi_exact_tuple_sequence(
                source.py(),
                snapshot.len(),
                |index| snapshot[index].bind(source.py()).as_ptr(),
                lanes,
            );
        }
    }
    if let Ok(rows) = source.cast_exact::<PyTuple>() {
        return global_multi_exact_tuple_sequence(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: exact outer tuples are immutable and index is within fixed length.
                unsafe { ffi::PyTuple_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            lanes,
        );
    }
    Ok(None)
}

fn global_multi_dict_layout<'py>(lanes: &[MultiDictLane<'py>]) -> PyResult<GlobalDictLayout<'py>> {
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
    Ok(GlobalDictLayout {
        lane_slots,
        selectors,
    })
}

#[inline(always)]
fn global_multi_exact_dict_row(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    lanes: &[MultiDictLane<'_>],
    layout: &GlobalDictLayout<'_>,
    state: &mut GlobalI64State,
    selected: &mut [*mut ffi::PyObject],
    scratch: &mut GlobalValueScratch,
) -> PyResult<Option<()>> {
    // SAFETY: the outer exact sequence or owned free-threaded snapshot keeps row live.
    if unsafe { ffi::PyDict_CheckExact(row) } == 0 {
        return Ok(None);
    }
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
        selected.fill(core::ptr::null_mut());
        let mut position = 0;
        let mut field = core::ptr::null_mut();
        let mut field_value = core::ptr::null_mut();
        for _ in 0..field_count {
            // SAFETY: the exact dict is protected by its critical section and its size is fixed.
            if unsafe { ffi::PyDict_Next(row, &mut position, &mut field, &mut field_value) } == 0 {
                return Ok(None);
            }
            // SAFETY: PyDict_Next returned a live key.
            if unsafe { ffi::PyUnicode_CheckExact(field) } == 0 {
                return Ok(None);
            }
            for (slot, selector) in layout.selectors.iter().enumerate() {
                if selector.as_ptr() == field {
                    selected[slot] = field_value;
                }
            }
        }

        for (slot, selector) in layout.selectors.iter().enumerate() {
            let value_object = if selected[slot].is_null() {
                let Some(found) = dict_item(py, row, selector.as_ptr())? else {
                    return Ok(None);
                };
                found
            } else {
                selected[slot]
            };
            let Some(value) = exact_i64(py, value_object)? else {
                return Ok(None);
            };
            scratch.values[slot] = value;
            scratch.objects[slot] = value_object;
        }

        for (lane_index, lane) in lanes.iter().enumerate() {
            let Some(slot) = layout.lane_slots[lane_index] else {
                debug_assert!(lane.value_field.is_none());
                if state
                    .accept(py, lane_index, 0, core::ptr::null_mut())
                    .is_none()
                {
                    return Ok(None);
                }
                continue;
            };
            if state
                .accept(py, lane_index, scratch.values[slot], scratch.objects[slot])
                .is_none()
            {
                return Ok(None);
            }
        }
        Ok(Some(()))
    })
}

fn global_multi_exact_dict_sequence(
    py: Python<'_>,
    row_count: usize,
    mut get_row: impl FnMut(usize) -> *mut ffi::PyObject,
    lanes: &[MultiDictLane<'_>],
) -> PyResult<Option<GlobalI64State>> {
    let mut state = GlobalI64State::new(lanes.iter().map(|lane| lane.kind))?;
    let layout = global_multi_dict_layout(lanes)?;
    let mut selected = Vec::new();
    selected
        .try_reserve_exact(layout.selectors.len())
        .map_err(group_allocation_error)?;
    selected.resize(layout.selectors.len(), core::ptr::null_mut());
    let mut scratch = GlobalValueScratch {
        values: Vec::new(),
        objects: Vec::new(),
    };
    scratch.resize(layout.selectors.len())?;
    for index in 0..row_count {
        let row = get_row(index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
        if global_multi_exact_dict_row(
            py,
            row,
            lanes,
            &layout,
            &mut state,
            &mut selected,
            &mut scratch,
        )?
        .is_none()
        {
            return Ok(None);
        }
    }
    Ok(Some(state))
}

fn global_multi_i64_dict_source(
    source: &Bound<'_, PyAny>,
    lanes: &[MultiDictLane<'_>],
) -> PyResult<Option<GlobalI64State>> {
    if let Ok(rows) = source.cast_exact::<PyList>() {
        #[cfg(not(Py_GIL_DISABLED))]
        return global_multi_exact_dict_sequence(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: a GIL build prevents exact-list mutation during this attached call.
                unsafe { ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            lanes,
        );
        #[cfg(Py_GIL_DISABLED)]
        {
            let snapshot = snapshot_exact_list_rows(source.py(), source, rows)?;
            return global_multi_exact_dict_sequence(
                source.py(),
                snapshot.len(),
                |index| snapshot[index].bind(source.py()).as_ptr(),
                lanes,
            );
        }
    }
    if let Ok(rows) = source.cast_exact::<PyTuple>() {
        return global_multi_exact_dict_sequence(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: exact outer tuples are immutable and index is within fixed length.
                unsafe { ffi::PyTuple_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            lanes,
        );
    }
    Ok(None)
}

fn materialize_global_multi<'py>(
    py: Python<'py>,
    state: GlobalI64State,
    output_names: &[Bound<'py, PyString>],
) -> PyResult<Py<PyAny>> {
    let row = new_dict_fallible(py)?;
    for (value, name) in state.lanes.iter().zip(output_names) {
        value.set_item(py, &row, name)?;
    }
    Ok(row.into_any().unbind())
}

#[pyfunction]
/// Reduce exact tuple rows through ordered count/sum/min/max lanes in one scan.
pub(crate) fn global_multi_i64_rows_v1(
    source: &Bound<'_, PyAny>,
    lanes: &Bound<'_, PyAny>,
) -> PyResult<Option<Py<PyAny>>> {
    let Some(lanes) = parse_multi_tuple_lanes(lanes)? else {
        return Ok(None);
    };
    if let Some(plan) = same_tuple_field_plan(&lanes) {
        let Some(state) = reduce_same_tuple_global(source, plan)? else {
            return Ok(None);
        };
        return materialize_same_tuple_global(source.py(), state, &lanes).map(Some);
    }
    let Some(mut state) = global_multi_i64_tuple_source(source, &lanes)? else {
        return Ok(None);
    };
    state.promote_extrema(source.py());
    let mut output_names = Vec::new();
    output_names
        .try_reserve_exact(lanes.len())
        .map_err(group_allocation_error)?;
    output_names.extend(lanes.iter().map(|lane| lane.output_name.clone()));
    materialize_global_multi(source.py(), state, &output_names).map(Some)
}

#[pyfunction]
/// Reduce exact dictionary rows through ordered count/sum/min/max lanes in one scan.
pub(crate) fn global_multi_i64_dict_rows_v1(
    source: &Bound<'_, PyAny>,
    lanes: &Bound<'_, PyAny>,
) -> PyResult<Option<Py<PyAny>>> {
    let Some(lanes) = parse_multi_dict_lanes(lanes)? else {
        return Ok(None);
    };
    if let Some(plan) = same_dict_field_plan(&lanes)? {
        let Some(state) = reduce_same_dict_global(source, plan)? else {
            return Ok(None);
        };
        return materialize_same_dict_global(source.py(), state, &lanes).map(Some);
    }
    let Some(mut state) = global_multi_i64_dict_source(source, &lanes)? else {
        return Ok(None);
    };
    state.promote_extrema(source.py());
    let mut output_names = Vec::new();
    output_names
        .try_reserve_exact(lanes.len())
        .map_err(group_allocation_error)?;
    output_names.extend(lanes.iter().map(|lane| lane.output_name.clone()));
    materialize_global_multi(source.py(), state, &output_names).map(Some)
}

#[inline(never)]
pub(super) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(global_multi_i64_rows_v1, module)?)?;
    module.add_function(wrap_pyfunction!(global_multi_i64_dict_rows_v1, module)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(Py_GIL_DISABLED))]
    fn ref_count(value: &Bound<'_, PyAny>) -> ffi::Py_ssize_t {
        // SAFETY: Bound proves that value is a live Python object for this attached test scope.
        unsafe { ffi::Py_REFCNT(value.as_ptr()) }
    }

    #[test]
    fn same_dict_field_plan_reuses_duplicate_lane_kinds() {
        Python::initialize();
        Python::attach(|py| {
            let value = PyString::new(py, "value");
            let equal_value = PyString::new(py, "value");
            assert_ne!(value.as_ptr(), equal_value.as_ptr());
            let lanes = vec![
                MultiDictLane {
                    kind: MultiI64LaneKind::Count,
                    value_field: None,
                    output_name: PyString::new(py, "rows"),
                },
                MultiDictLane {
                    kind: MultiI64LaneKind::Sum,
                    value_field: Some(value.clone()),
                    output_name: PyString::new(py, "total"),
                },
                MultiDictLane {
                    kind: MultiI64LaneKind::Sum,
                    value_field: Some(value.clone()),
                    output_name: PyString::new(py, "total_again"),
                },
                MultiDictLane {
                    kind: MultiI64LaneKind::Min,
                    value_field: Some(equal_value),
                    output_name: PyString::new(py, "low"),
                },
                MultiDictLane {
                    kind: MultiI64LaneKind::Max,
                    value_field: Some(value.clone()),
                    output_name: PyString::new(py, "high"),
                },
            ];

            let plan = same_dict_field_plan(&lanes)
                .unwrap()
                .expect("one shared field should specialize");

            assert_eq!(plan.selector, value.as_ptr());
            assert_eq!(
                plan.mask,
                GLOBAL_COUNT | GLOBAL_SUM | GLOBAL_MIN | GLOBAL_MAX
            );
        });
    }

    #[test]
    fn same_tuple_field_plan_reuses_duplicate_lane_kinds() {
        Python::initialize();
        Python::attach(|py| {
            let lanes = vec![
                MultiTupleLane {
                    kind: MultiI64LaneKind::Count,
                    value_index: None,
                    output_name: PyString::new(py, "rows"),
                },
                MultiTupleLane {
                    kind: MultiI64LaneKind::Sum,
                    value_index: Some(-1),
                    output_name: PyString::new(py, "total"),
                },
                MultiTupleLane {
                    kind: MultiI64LaneKind::Min,
                    value_index: Some(-1),
                    output_name: PyString::new(py, "low"),
                },
                MultiTupleLane {
                    kind: MultiI64LaneKind::Max,
                    value_index: Some(-1),
                    output_name: PyString::new(py, "high"),
                },
            ];

            let plan = same_tuple_field_plan(&lanes).expect("one shared index should specialize");

            assert_eq!(plan.selector, -1);
            assert_eq!(
                plan.mask,
                GLOBAL_COUNT | GLOBAL_SUM | GLOBAL_MIN | GLOBAL_MAX
            );
        });
    }

    #[test]
    fn same_field_plans_reject_multiple_selectors() {
        Python::initialize();
        Python::attach(|py| {
            let left = PyString::new(py, "left");
            let right = PyString::new(py, "right");
            let dict_lanes = vec![
                MultiDictLane {
                    kind: MultiI64LaneKind::Sum,
                    value_field: Some(left),
                    output_name: PyString::new(py, "total"),
                },
                MultiDictLane {
                    kind: MultiI64LaneKind::Min,
                    value_field: Some(right),
                    output_name: PyString::new(py, "low"),
                },
            ];
            let tuple_lanes = vec![
                MultiTupleLane {
                    kind: MultiI64LaneKind::Sum,
                    value_index: Some(0),
                    output_name: PyString::new(py, "total"),
                },
                MultiTupleLane {
                    kind: MultiI64LaneKind::Min,
                    value_index: Some(1),
                    output_name: PyString::new(py, "low"),
                },
            ];

            assert!(same_dict_field_plan(&dict_lanes).unwrap().is_none());
            assert!(same_tuple_field_plan(&tuple_lanes).is_none());
        });
    }

    #[test]
    fn same_field_reducers_dispatch_every_value_mask() {
        Python::initialize();
        Python::attach(|py| {
            let field = PyString::new(py, "value");
            let first_dict = PyDict::new(py);
            first_dict.set_item(&field, -3_i64).unwrap();
            let second_dict = PyDict::new(py);
            second_dict.set_item(&field, 7_i64).unwrap();
            let dict_source = PyList::new(py, [&first_dict, &second_dict]).unwrap();

            let first_tuple = PyTuple::new(py, [-3_i64]).unwrap();
            let second_tuple = PyTuple::new(py, [7_i64]).unwrap();
            let tuple_source = PyList::new(py, [&first_tuple, &second_tuple]).unwrap();

            for value_mask in [
                GLOBAL_SUM,
                GLOBAL_MIN,
                GLOBAL_MAX,
                GLOBAL_SUM | GLOBAL_MIN,
                GLOBAL_SUM | GLOBAL_MAX,
                GLOBAL_MIN | GLOBAL_MAX,
                GLOBAL_SUM | GLOBAL_MIN | GLOBAL_MAX,
            ] {
                let dict_state = reduce_same_dict_global(
                    dict_source.as_any(),
                    SameFieldGlobalPlan {
                        selector: field.as_ptr(),
                        mask: GLOBAL_COUNT | value_mask,
                    },
                )
                .unwrap()
                .expect("every dictionary value mask should specialize");
                let tuple_state = reduce_same_tuple_global(
                    tuple_source.as_any(),
                    SameFieldGlobalPlan {
                        selector: 0,
                        mask: GLOBAL_COUNT | value_mask,
                    },
                )
                .unwrap()
                .expect("every tuple value mask should specialize");

                for state in [&dict_state, &tuple_state] {
                    assert_eq!(state.row_count, 2);
                    assert_eq!(
                        state.total,
                        if value_mask & GLOBAL_SUM != 0 { 4 } else { 0 }
                    );
                    assert_eq!(
                        state.minimum.as_ref().map(|(value, _object)| *value),
                        (value_mask & GLOBAL_MIN != 0).then_some(-3)
                    );
                    assert_eq!(
                        state.maximum.as_ref().map(|(value, _object)| *value),
                        (value_mask & GLOBAL_MAX != 0).then_some(7)
                    );
                }
            }
        });
    }

    #[test]
    fn same_dict_field_reducer_keeps_wide_sum_and_first_extrema_identity() {
        Python::initialize();
        Python::attach(|py| {
            let value = PyString::new(py, "value");
            let lanes = vec![
                MultiDictLane {
                    kind: MultiI64LaneKind::Count,
                    value_field: None,
                    output_name: PyString::new(py, "rows"),
                },
                MultiDictLane {
                    kind: MultiI64LaneKind::Sum,
                    value_field: Some(value.clone()),
                    output_name: PyString::new(py, "total"),
                },
                MultiDictLane {
                    kind: MultiI64LaneKind::Min,
                    value_field: Some(value.clone()),
                    output_name: PyString::new(py, "low"),
                },
                MultiDictLane {
                    kind: MultiI64LaneKind::Max,
                    value_field: Some(value.clone()),
                    output_name: PyString::new(py, "high"),
                },
            ];
            let plan = same_dict_field_plan(&lanes).unwrap().unwrap();
            let first_value = pyo3::types::PyInt::new(py, i64::MAX);
            let equal_value = pyo3::types::PyInt::new(py, i64::MAX);
            assert!(!first_value.is(&equal_value));
            let first = PyDict::new(py);
            first.set_item("id", 1).unwrap();
            first.set_item(&value, &first_value).unwrap();
            let second = PyDict::new(py);
            second.set_item("id", 2).unwrap();
            second.set_item(&value, &equal_value).unwrap();
            let source = PyList::new(py, [&first, &second]).unwrap();

            let state = reduce_same_dict_global(source.as_any(), plan)
                .unwrap()
                .expect("exact dict rows should specialize");

            assert_eq!(state.row_count, 2);
            assert_eq!(state.total, i128::from(i64::MAX) * 2);
            assert!(state.minimum.as_ref().unwrap().1.bind(py).is(&first_value));
            assert!(state.maximum.as_ref().unwrap().1.bind(py).is(&first_value));
        });
    }

    #[test]
    fn same_tuple_field_reducer_renormalizes_negative_index_per_row() {
        Python::initialize();
        Python::attach(|py| {
            let lanes = vec![
                MultiTupleLane {
                    kind: MultiI64LaneKind::Count,
                    value_index: None,
                    output_name: PyString::new(py, "rows"),
                },
                MultiTupleLane {
                    kind: MultiI64LaneKind::Sum,
                    value_index: Some(-1),
                    output_name: PyString::new(py, "total"),
                },
                MultiTupleLane {
                    kind: MultiI64LaneKind::Min,
                    value_index: Some(-1),
                    output_name: PyString::new(py, "low"),
                },
                MultiTupleLane {
                    kind: MultiI64LaneKind::Max,
                    value_index: Some(-1),
                    output_name: PyString::new(py, "high"),
                },
            ];
            let plan = same_tuple_field_plan(&lanes).unwrap();
            let first_value = pyo3::types::PyInt::new(py, i64::MAX);
            let equal_value = pyo3::types::PyInt::new(py, i64::MAX);
            assert!(!first_value.is(&equal_value));
            let first =
                PyTuple::new(py, [pyo3::types::PyInt::new(py, 1), first_value.clone()]).unwrap();
            let second = PyTuple::new(
                py,
                [
                    pyo3::types::PyInt::new(py, 2),
                    pyo3::types::PyInt::new(py, 3),
                    equal_value.clone(),
                ],
            )
            .unwrap();
            let source = PyList::new(py, [&first, &second]).unwrap();

            let state = reduce_same_tuple_global(source.as_any(), plan)
                .unwrap()
                .expect("exact tuple rows should specialize");

            assert_eq!(state.row_count, 2);
            assert_eq!(state.total, i128::from(i64::MAX) * 2);
            assert!(state.minimum.as_ref().unwrap().1.bind(py).is(&first_value));
            assert!(state.maximum.as_ref().unwrap().1.bind(py).is(&first_value));
        });
    }

    #[test]
    fn same_dict_field_materializer_reuses_duplicate_outputs() {
        Python::initialize();
        Python::attach(|py| {
            let value = PyString::new(py, "value");
            let rows_name = PyString::new(py, "rows");
            let total_name = PyString::new(py, "total");
            let repeated_total_name = PyString::new(py, "total_again");
            let low_name = PyString::new(py, "low");
            let high_name = PyString::new(py, "high");
            let lanes = vec![
                MultiDictLane {
                    kind: MultiI64LaneKind::Count,
                    value_field: None,
                    output_name: rows_name.clone(),
                },
                MultiDictLane {
                    kind: MultiI64LaneKind::Sum,
                    value_field: Some(value.clone()),
                    output_name: total_name.clone(),
                },
                MultiDictLane {
                    kind: MultiI64LaneKind::Sum,
                    value_field: Some(value.clone()),
                    output_name: repeated_total_name.clone(),
                },
                MultiDictLane {
                    kind: MultiI64LaneKind::Min,
                    value_field: Some(value.clone()),
                    output_name: low_name.clone(),
                },
                MultiDictLane {
                    kind: MultiI64LaneKind::Max,
                    value_field: Some(value.clone()),
                    output_name: high_name.clone(),
                },
            ];
            let first_value = pyo3::types::PyInt::new(py, i64::MAX);
            let equal_value = pyo3::types::PyInt::new(py, i64::MAX);
            let first = PyDict::new(py);
            first.set_item(&value, &first_value).unwrap();
            let second = PyDict::new(py);
            second.set_item(&value, &equal_value).unwrap();
            let source = PyList::new(py, [&first, &second]).unwrap();
            let state = reduce_same_dict_global(
                source.as_any(),
                same_dict_field_plan(&lanes).unwrap().unwrap(),
            )
            .unwrap()
            .unwrap();

            let result = materialize_same_dict_global(py, state, &lanes).unwrap();
            let result = result.bind(py).cast_exact::<PyDict>().unwrap();

            assert_eq!(
                result
                    .get_item(&rows_name)
                    .unwrap()
                    .unwrap()
                    .extract::<usize>()
                    .unwrap(),
                2
            );
            let expected_total = i128::from(i64::MAX) * 2;
            for name in [&total_name, &repeated_total_name] {
                assert_eq!(
                    result
                        .get_item(name)
                        .unwrap()
                        .unwrap()
                        .extract::<i128>()
                        .unwrap(),
                    expected_total
                );
            }
            assert!(
                result
                    .get_item(&low_name)
                    .unwrap()
                    .unwrap()
                    .is(&first_value)
            );
            assert!(
                result
                    .get_item(&high_name)
                    .unwrap()
                    .unwrap()
                    .is(&first_value)
            );
        });
    }

    #[test]
    fn same_tuple_field_materializer_keeps_empty_lane_values() {
        Python::initialize();
        Python::attach(|py| {
            let rows_name = PyString::new(py, "rows");
            let total_name = PyString::new(py, "total");
            let low_name = PyString::new(py, "low");
            let high_name = PyString::new(py, "high");
            let lanes = vec![
                MultiTupleLane {
                    kind: MultiI64LaneKind::Count,
                    value_index: None,
                    output_name: rows_name.clone(),
                },
                MultiTupleLane {
                    kind: MultiI64LaneKind::Sum,
                    value_index: Some(-1),
                    output_name: total_name.clone(),
                },
                MultiTupleLane {
                    kind: MultiI64LaneKind::Min,
                    value_index: Some(-1),
                    output_name: low_name.clone(),
                },
                MultiTupleLane {
                    kind: MultiI64LaneKind::Max,
                    value_index: Some(-1),
                    output_name: high_name.clone(),
                },
            ];
            let source = PyTuple::empty(py);
            let state =
                reduce_same_tuple_global(source.as_any(), same_tuple_field_plan(&lanes).unwrap())
                    .unwrap()
                    .unwrap();

            let result = materialize_same_tuple_global(py, state, &lanes).unwrap();
            let result = result.bind(py).cast_exact::<PyDict>().unwrap();

            assert_eq!(
                result
                    .get_item(&rows_name)
                    .unwrap()
                    .unwrap()
                    .extract::<usize>()
                    .unwrap(),
                0
            );
            assert_eq!(
                result
                    .get_item(&total_name)
                    .unwrap()
                    .unwrap()
                    .extract::<i128>()
                    .unwrap(),
                0
            );
            assert!(result.get_item(&low_name).unwrap().unwrap().is_none());
            assert!(result.get_item(&high_name).unwrap().unwrap().is_none());
        });
    }

    #[cfg(not(Py_GIL_DISABLED))]
    #[test]
    fn generic_extrema_borrow_during_scan_then_promote_only_final_objects() {
        Python::initialize();
        Python::attach(|py| {
            let initial_minimum = pyo3::types::PyInt::new(py, 10_000_i64);
            let final_minimum = pyo3::types::PyInt::new(py, -10_000_i64);
            let initial_maximum = pyo3::types::PyInt::new(py, -20_000_i64);
            let final_maximum = pyo3::types::PyInt::new(py, 20_000_i64);
            let initial_minimum_refs = ref_count(initial_minimum.as_any());
            let final_minimum_refs = ref_count(final_minimum.as_any());
            let initial_maximum_refs = ref_count(initial_maximum.as_any());
            let final_maximum_refs = ref_count(final_maximum.as_any());
            let mut state =
                GlobalI64State::new([MultiI64LaneKind::Min, MultiI64LaneKind::Max].into_iter())
                    .unwrap();

            assert!(
                state
                    .accept(py, 0, 10_000, initial_minimum.as_ptr())
                    .is_some()
            );
            assert!(
                state
                    .accept(py, 1, -20_000, initial_maximum.as_ptr())
                    .is_some()
            );
            assert!(
                state
                    .accept(py, 0, -10_000, final_minimum.as_ptr())
                    .is_some()
            );
            assert!(
                state
                    .accept(py, 1, 20_000, final_maximum.as_ptr())
                    .is_some()
            );

            assert_eq!(ref_count(initial_minimum.as_any()), initial_minimum_refs);
            assert_eq!(ref_count(final_minimum.as_any()), final_minimum_refs);
            assert_eq!(ref_count(initial_maximum.as_any()), initial_maximum_refs);
            assert_eq!(ref_count(final_maximum.as_any()), final_maximum_refs);

            state.promote_extrema(py);

            assert_eq!(ref_count(initial_minimum.as_any()), initial_minimum_refs);
            assert_eq!(ref_count(final_minimum.as_any()), final_minimum_refs + 1);
            assert_eq!(ref_count(initial_maximum.as_any()), initial_maximum_refs);
            assert_eq!(ref_count(final_maximum.as_any()), final_maximum_refs + 1);
            drop(state);
            assert_eq!(ref_count(final_minimum.as_any()), final_minimum_refs);
            assert_eq!(ref_count(final_maximum.as_any()), final_maximum_refs);
        });
    }

    #[test]
    fn generic_extrema_late_decline_releases_scan_state_without_changing_identity() {
        Python::initialize();
        Python::attach(|py| {
            let min = pyo3::types::PyInt::new(py, 2);
            let max = pyo3::types::PyInt::new(py, 3);
            let low = PyString::new(py, "low");
            let high = PyString::new(py, "high");
            let low_name = PyString::new(py, "minimum");
            let high_name = PyString::new(py, "maximum");
            let lanes = PyTuple::new(
                py,
                [
                    PyTuple::new(py, [min.as_any(), low.as_any(), low_name.as_any()]).unwrap(),
                    PyTuple::new(py, [max.as_any(), high.as_any(), high_name.as_any()]).unwrap(),
                ],
            )
            .unwrap();
            let first_low = pyo3::types::PyInt::new(py, -10_000_i64);
            let first_high = pyo3::types::PyInt::new(py, 10_000_i64);
            let first = PyDict::new(py);
            first.set_item(&low, &first_low).unwrap();
            first.set_item(&high, &first_high).unwrap();
            let invalid = PyDict::new(py);
            invalid.set_item(&low, -20_000_i64).unwrap();
            invalid.set_item(&high, "not-an-exact-int").unwrap();
            let source = PyList::new(py, [&first, &invalid]).unwrap();
            assert!(
                global_multi_i64_dict_rows_v1(source.as_any(), lanes.as_any())
                    .unwrap()
                    .is_none()
            );
            assert!(first.get_item(&low).unwrap().unwrap().is(&first_low));
            assert!(first.get_item(&high).unwrap().unwrap().is(&first_high));
        });
    }

    #[test]
    fn exact_global_multi_keeps_wide_sums_empty_values_and_first_extrema() {
        Python::initialize();
        Python::attach(|py| {
            let count = pyo3::types::PyInt::new(py, 0);
            let sum = pyo3::types::PyInt::new(py, 1);
            let min = pyo3::types::PyInt::new(py, 2);
            let max = pyo3::types::PyInt::new(py, 3);
            let amount = PyString::new(py, "amount");
            let low = PyString::new(py, "low");
            let high = PyString::new(py, "high");
            let rows_name = PyString::new(py, "rows");
            let total_name = PyString::new(py, "total");
            let low_name = PyString::new(py, "minimum");
            let high_name = PyString::new(py, "maximum");
            let none = py.None();
            let lanes = PyTuple::new(
                py,
                [
                    PyTuple::new(py, [count.as_any(), none.bind(py), rows_name.as_any()]).unwrap(),
                    PyTuple::new(py, [sum.as_any(), amount.as_any(), total_name.as_any()]).unwrap(),
                    PyTuple::new(py, [min.as_any(), low.as_any(), low_name.as_any()]).unwrap(),
                    PyTuple::new(py, [max.as_any(), high.as_any(), high_name.as_any()]).unwrap(),
                ],
            )
            .unwrap();
            let first_low = pyo3::types::PyInt::new(py, -1_000_i64);
            let equal_low = pyo3::types::PyInt::new(py, -1_000_i64);
            let first_high = pyo3::types::PyInt::new(py, 1_000_i64);
            let equal_high = pyo3::types::PyInt::new(py, 1_000_i64);
            assert!(!first_high.is(&equal_high));
            let first = PyDict::new(py);
            first.set_item(&amount, i64::MAX).unwrap();
            first.set_item(&low, &first_low).unwrap();
            first.set_item(&high, &first_high).unwrap();
            let second = PyDict::new(py);
            second.set_item(&amount, i64::MAX).unwrap();
            second.set_item(&low, &equal_low).unwrap();
            second.set_item(&high, &equal_high).unwrap();
            let source = PyList::new(py, [&first, &second]).unwrap();
            let result = global_multi_i64_dict_rows_v1(source.as_any(), lanes.as_any())
                .unwrap()
                .unwrap();
            let result = result.bind(py).cast_exact::<PyDict>().unwrap();

            assert_eq!(
                result
                    .get_item(&total_name)
                    .unwrap()
                    .unwrap()
                    .extract::<i128>()
                    .unwrap(),
                i128::from(i64::MAX) * 2
            );
            assert!(result.get_item(&low_name).unwrap().unwrap().is(&first_low));
            assert!(
                result
                    .get_item(&high_name)
                    .unwrap()
                    .unwrap()
                    .is(&first_high)
            );

            let empty = PyTuple::empty(py);
            let empty_result = global_multi_i64_dict_rows_v1(empty.as_any(), lanes.as_any())
                .unwrap()
                .unwrap();
            let empty_result = empty_result.bind(py).cast_exact::<PyDict>().unwrap();
            assert_eq!(
                empty_result
                    .get_item(&rows_name)
                    .unwrap()
                    .unwrap()
                    .extract::<usize>()
                    .unwrap(),
                0
            );
            assert!(empty_result.get_item(&low_name).unwrap().unwrap().is_none());
        });
    }
}
