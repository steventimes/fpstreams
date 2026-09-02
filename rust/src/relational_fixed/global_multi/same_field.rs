//! Fixed-state global aggregation when every value lane shares one selector.

use super::*;

pub(super) const GLOBAL_COUNT: u8 = 1 << 0;
pub(super) const GLOBAL_SUM: u8 = 1 << 1;
pub(super) const GLOBAL_MIN: u8 = 1 << 2;
pub(super) const GLOBAL_MAX: u8 = 1 << 3;
const GLOBAL_VALUE_MASK: u8 = GLOBAL_SUM | GLOBAL_MIN | GLOBAL_MAX;

#[derive(Clone, Copy)]
pub(super) struct SameFieldGlobalPlan<S> {
    pub(super) selector: S,
    pub(super) mask: u8,
}

#[inline]
const fn global_lane_mask(kind: MultiI64LaneKind) -> u8 {
    match kind {
        MultiI64LaneKind::Count => GLOBAL_COUNT,
        MultiI64LaneKind::Sum => GLOBAL_SUM,
        MultiI64LaneKind::Min => GLOBAL_MIN,
        MultiI64LaneKind::Max => GLOBAL_MAX,
    }
}

pub(super) fn same_dict_field_plan(
    lanes: &[MultiDictLane<'_>],
) -> PyResult<Option<SameFieldGlobalPlan<*mut ffi::PyObject>>> {
    let mut selector: Option<Bound<'_, PyString>> = None;
    let mut mask = 0;
    for lane in lanes {
        mask |= global_lane_mask(lane.kind);
        let Some(field) = &lane.value_field else {
            continue;
        };
        if let Some(current) = &selector {
            // Parsed selectors are exact strings, so equality cannot dispatch user code.
            if !current.as_any().eq(field.as_any())? {
                return Ok(None);
            }
        } else {
            selector = Some(field.clone());
        }
    }
    Ok(selector.map(|selector| SameFieldGlobalPlan {
        selector: selector.as_ptr(),
        mask,
    }))
}

pub(super) fn same_tuple_field_plan(
    lanes: &[MultiTupleLane<'_>],
) -> Option<SameFieldGlobalPlan<isize>> {
    let mut selector = None;
    let mut mask = 0;
    for lane in lanes {
        mask |= global_lane_mask(lane.kind);
        let Some(index) = lane.value_index else {
            continue;
        };
        if selector.is_some_and(|current| current != index) {
            return None;
        }
        selector = Some(index);
    }
    selector.map(|selector| SameFieldGlobalPlan { selector, mask })
}

#[cfg(not(Py_GIL_DISABLED))]
type ScannedExtremum = *mut ffi::PyObject;
#[cfg(Py_GIL_DISABLED)]
type ScannedExtremum = Py<PyAny>;

struct SameFieldScanState {
    pub(super) row_count: usize,
    pub(super) total: i128,
    minimum: Option<(i64, ScannedExtremum)>,
    maximum: Option<(i64, ScannedExtremum)>,
}

pub(super) struct SameFieldGlobalState {
    pub(super) row_count: usize,
    pub(super) total: i128,
    pub(super) minimum: Option<(i64, Py<PyAny>)>,
    pub(super) maximum: Option<(i64, Py<PyAny>)>,
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline(always)]
fn scan_extremum(_py: Python<'_>, value_object: *mut ffi::PyObject) -> ScannedExtremum {
    value_object
}

#[cfg(Py_GIL_DISABLED)]
#[inline(always)]
fn scan_extremum(py: Python<'_>, value_object: *mut ffi::PyObject) -> ScannedExtremum {
    // SAFETY: the current row's critical section keeps value_object live until this reference
    // has been retained for use after the row is unlocked.
    unsafe { Borrowed::from_ptr(py, value_object).to_owned().unbind() }
}

#[cfg(not(Py_GIL_DISABLED))]
#[inline]
fn own_scanned_extremum(py: Python<'_>, object: ScannedExtremum) -> Py<PyAny> {
    // SAFETY: the attached scan has not invoked callbacks, and the exact outer sequence still
    // owns every row. Retain the final extremum before result allocation can trigger GC.
    unsafe { Borrowed::from_ptr(py, object).to_owned().unbind() }
}

#[cfg(Py_GIL_DISABLED)]
#[inline]
fn own_scanned_extremum(_py: Python<'_>, object: ScannedExtremum) -> Py<PyAny> {
    object
}

impl SameFieldScanState {
    const fn new() -> Self {
        Self {
            row_count: 0,
            total: 0,
            minimum: None,
            maximum: None,
        }
    }

    #[inline(always)]
    fn accept<const MASK: u8>(
        &mut self,
        py: Python<'_>,
        value: i64,
        value_object: *mut ffi::PyObject,
    ) -> Option<()> {
        if MASK & GLOBAL_SUM != 0 {
            self.total = self.total.checked_add(i128::from(value))?;
        }
        if MASK & GLOBAL_MIN != 0
            && self
                .minimum
                .as_ref()
                .is_none_or(|(current, _object)| value < *current)
        {
            let object = scan_extremum(py, value_object);
            self.minimum = Some((value, object));
        }
        if MASK & GLOBAL_MAX != 0
            && self
                .maximum
                .as_ref()
                .is_none_or(|(current, _object)| value > *current)
        {
            let object = scan_extremum(py, value_object);
            self.maximum = Some((value, object));
        }
        Some(())
    }

    fn finish(self, py: Python<'_>) -> SameFieldGlobalState {
        SameFieldGlobalState {
            row_count: self.row_count,
            total: self.total,
            minimum: self
                .minimum
                .map(|(value, object)| (value, own_scanned_extremum(py, object))),
            maximum: self
                .maximum
                .map(|(value, object)| (value, own_scanned_extremum(py, object))),
        }
    }
}

impl SameFieldGlobalState {
    fn set_item(
        &self,
        py: Python<'_>,
        row: &Bound<'_, PyDict>,
        name: &Bound<'_, PyString>,
        kind: MultiI64LaneKind,
    ) -> PyResult<()> {
        match kind {
            MultiI64LaneKind::Count => row.set_item(name, self.row_count),
            MultiI64LaneKind::Sum => set_widened_i64_item(row, name, self.total),
            MultiI64LaneKind::Min => match &self.minimum {
                Some((_value, object)) => row.set_item(name, object),
                None => row.set_item(name, py.None()),
            },
            MultiI64LaneKind::Max => match &self.maximum {
                Some((_value, object)) => row.set_item(name, object),
                None => row.set_item(name, py.None()),
            },
        }
    }
}

fn reduce_same_tuple_sequence<const MASK: u8>(
    py: Python<'_>,
    row_count: usize,
    mut get_row: impl FnMut(usize) -> *mut ffi::PyObject,
    plan: SameFieldGlobalPlan<isize>,
) -> PyResult<Option<SameFieldGlobalState>> {
    let mut state = SameFieldScanState::new();
    let mut cached_layout: Option<(usize, usize)> = None;
    for index in 0..row_count {
        let row = get_row(index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
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
        let value_position = match cached_layout {
            Some((cached_width, position)) if cached_width == width => position,
            _ => {
                let Some(position) = normalize_index(plan.selector, width) else {
                    return Ok(None);
                };
                cached_layout = Some((width, position));
                position
            }
        };
        // SAFETY: value_position was normalized against this exact tuple's current width.
        let value_object = unsafe { ffi::PyTuple_GetItem(row, value_position as ffi::Py_ssize_t) };
        if value_object.is_null() {
            return Err(PyErr::fetch(py));
        }
        let Some(value) = exact_i64(py, value_object)? else {
            return Ok(None);
        };
        if state.accept::<MASK>(py, value, value_object).is_none() {
            return Ok(None);
        }
    }
    state.row_count = row_count;
    Ok(Some(state.finish(py)))
}

fn reduce_same_tuple_global_mask<const MASK: u8>(
    source: &Bound<'_, PyAny>,
    plan: SameFieldGlobalPlan<isize>,
) -> PyResult<Option<SameFieldGlobalState>> {
    if let Ok(rows) = source.cast_exact::<PyList>() {
        #[cfg(not(Py_GIL_DISABLED))]
        return reduce_same_tuple_sequence::<MASK>(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: the GIL prevents exact-list mutation and index is below its length.
                unsafe { ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            plan,
        );
        #[cfg(Py_GIL_DISABLED)]
        {
            let snapshot = snapshot_exact_list_rows(source.py(), source, rows)?;
            return reduce_same_tuple_sequence::<MASK>(
                source.py(),
                snapshot.len(),
                |index| snapshot[index].bind(source.py()).as_ptr(),
                plan,
            );
        }
    }
    if let Ok(rows) = source.cast_exact::<PyTuple>() {
        return reduce_same_tuple_sequence::<MASK>(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: exact tuples are immutable and index is below their fixed length.
                unsafe { ffi::PyTuple_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            plan,
        );
    }
    Ok(None)
}

pub(super) fn reduce_same_tuple_global(
    source: &Bound<'_, PyAny>,
    plan: SameFieldGlobalPlan<isize>,
) -> PyResult<Option<SameFieldGlobalState>> {
    match plan.mask & GLOBAL_VALUE_MASK {
        GLOBAL_SUM => reduce_same_tuple_global_mask::<GLOBAL_SUM>(source, plan),
        GLOBAL_MIN => reduce_same_tuple_global_mask::<GLOBAL_MIN>(source, plan),
        GLOBAL_MAX => reduce_same_tuple_global_mask::<GLOBAL_MAX>(source, plan),
        mask if mask == GLOBAL_SUM | GLOBAL_MIN => {
            reduce_same_tuple_global_mask::<{ GLOBAL_SUM | GLOBAL_MIN }>(source, plan)
        }
        mask if mask == GLOBAL_SUM | GLOBAL_MAX => {
            reduce_same_tuple_global_mask::<{ GLOBAL_SUM | GLOBAL_MAX }>(source, plan)
        }
        mask if mask == GLOBAL_MIN | GLOBAL_MAX => {
            reduce_same_tuple_global_mask::<{ GLOBAL_MIN | GLOBAL_MAX }>(source, plan)
        }
        mask if mask == GLOBAL_VALUE_MASK => {
            reduce_same_tuple_global_mask::<GLOBAL_VALUE_MASK>(source, plan)
        }
        _ => Ok(None),
    }
}

#[inline(always)]
fn accept_same_dict_row<const MASK: u8>(
    py: Python<'_>,
    row: *mut ffi::PyObject,
    plan: SameFieldGlobalPlan<*mut ffi::PyObject>,
    state: &mut SameFieldScanState,
) -> PyResult<Option<()>> {
    // SAFETY: the outer exact sequence or owned free-threaded snapshot keeps row live.
    if unsafe { ffi::PyDict_CheckExact(row) } == 0 {
        return Ok(None);
    }
    // SAFETY: row remains live for the critical section through its outer owner.
    let row_bound = unsafe { Borrowed::from_ptr(py, row) };
    with_critical_section(row_bound.as_any(), || {
        // The complete key scan is required even after finding the selected pointer. A custom
        // colliding key could otherwise make a declined shortcut skip canonical equality hooks.
        let field_count = unsafe { ffi::PyDict_Size(row) };
        if field_count < 0 {
            return Err(PyErr::fetch(py));
        }
        let field_count = usize::try_from(field_count)
            .map_err(|_| PyMemoryError::new_err("native record field count is too large"))?;
        if field_count > RECORD_GROUP_SUM_MAX_FIELDS {
            return Ok(None);
        }
        let mut selected = core::ptr::null_mut();
        let mut position = 0;
        let mut field = core::ptr::null_mut();
        let mut field_value = core::ptr::null_mut();
        for _ in 0..field_count {
            // SAFETY: the exact dict is locked and its size fixes the successful iterations.
            if unsafe { ffi::PyDict_Next(row, &mut position, &mut field, &mut field_value) } == 0 {
                return Ok(None);
            }
            // SAFETY: PyDict_Next returned a live key.
            if unsafe { ffi::PyUnicode_CheckExact(field) } == 0 {
                return Ok(None);
            }
            if field == plan.selector {
                selected = field_value;
            }
        }
        if selected.is_null() {
            let Some(found) = dict_item(py, row, plan.selector)? else {
                return Ok(None);
            };
            selected = found;
        }
        let Some(value) = exact_i64(py, selected)? else {
            return Ok(None);
        };
        Ok(state.accept::<MASK>(py, value, selected))
    })
}

fn reduce_same_dict_sequence<const MASK: u8>(
    py: Python<'_>,
    row_count: usize,
    mut get_row: impl FnMut(usize) -> *mut ffi::PyObject,
    plan: SameFieldGlobalPlan<*mut ffi::PyObject>,
) -> PyResult<Option<SameFieldGlobalState>> {
    let mut state = SameFieldScanState::new();
    for index in 0..row_count {
        let row = get_row(index);
        if row.is_null() {
            return Err(PyErr::fetch(py));
        }
        if accept_same_dict_row::<MASK>(py, row, plan, &mut state)?.is_none() {
            return Ok(None);
        }
    }
    state.row_count = row_count;
    Ok(Some(state.finish(py)))
}

fn reduce_same_dict_global_mask<const MASK: u8>(
    source: &Bound<'_, PyAny>,
    plan: SameFieldGlobalPlan<*mut ffi::PyObject>,
) -> PyResult<Option<SameFieldGlobalState>> {
    if let Ok(rows) = source.cast_exact::<PyList>() {
        #[cfg(not(Py_GIL_DISABLED))]
        return reduce_same_dict_sequence::<MASK>(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: the GIL prevents exact-list mutation and index is below its length.
                unsafe { ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            plan,
        );
        #[cfg(Py_GIL_DISABLED)]
        {
            let snapshot = snapshot_exact_list_rows(source.py(), source, rows)?;
            return reduce_same_dict_sequence::<MASK>(
                source.py(),
                snapshot.len(),
                |index| snapshot[index].bind(source.py()).as_ptr(),
                plan,
            );
        }
    }
    if let Ok(rows) = source.cast_exact::<PyTuple>() {
        return reduce_same_dict_sequence::<MASK>(
            source.py(),
            rows.len(),
            |index| {
                // SAFETY: exact tuples are immutable and index is below their fixed length.
                unsafe { ffi::PyTuple_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) }
            },
            plan,
        );
    }
    Ok(None)
}

pub(super) fn reduce_same_dict_global(
    source: &Bound<'_, PyAny>,
    plan: SameFieldGlobalPlan<*mut ffi::PyObject>,
) -> PyResult<Option<SameFieldGlobalState>> {
    match plan.mask & GLOBAL_VALUE_MASK {
        GLOBAL_SUM => reduce_same_dict_global_mask::<GLOBAL_SUM>(source, plan),
        GLOBAL_MIN => reduce_same_dict_global_mask::<GLOBAL_MIN>(source, plan),
        GLOBAL_MAX => reduce_same_dict_global_mask::<GLOBAL_MAX>(source, plan),
        mask if mask == GLOBAL_SUM | GLOBAL_MIN => {
            reduce_same_dict_global_mask::<{ GLOBAL_SUM | GLOBAL_MIN }>(source, plan)
        }
        mask if mask == GLOBAL_SUM | GLOBAL_MAX => {
            reduce_same_dict_global_mask::<{ GLOBAL_SUM | GLOBAL_MAX }>(source, plan)
        }
        mask if mask == GLOBAL_MIN | GLOBAL_MAX => {
            reduce_same_dict_global_mask::<{ GLOBAL_MIN | GLOBAL_MAX }>(source, plan)
        }
        mask if mask == GLOBAL_VALUE_MASK => {
            reduce_same_dict_global_mask::<GLOBAL_VALUE_MASK>(source, plan)
        }
        _ => Ok(None),
    }
}

pub(super) fn materialize_same_dict_global(
    py: Python<'_>,
    state: SameFieldGlobalState,
    lanes: &[MultiDictLane<'_>],
) -> PyResult<Py<PyAny>> {
    let row = new_dict_fallible(py)?;
    for lane in lanes {
        state.set_item(py, &row, &lane.output_name, lane.kind)?;
    }
    Ok(row.into_any().unbind())
}

pub(super) fn materialize_same_tuple_global(
    py: Python<'_>,
    state: SameFieldGlobalState,
    lanes: &[MultiTupleLane<'_>],
) -> PyResult<Py<PyAny>> {
    let row = new_dict_fallible(py)?;
    for lane in lanes {
        state.set_item(py, &row, &lane.output_name, lane.kind)?;
    }
    Ok(row.into_any().unbind())
}
