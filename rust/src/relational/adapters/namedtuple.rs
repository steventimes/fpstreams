//! Guarded standard-NamedTuple capability discovery and snapshot adapters.

#[cfg(not(Py_GIL_DISABLED))]
use super::super::group_numeric::new_dict_fallible;
#[cfg(not(Py_GIL_DISABLED))]
use super::super::join_exact::{join_allocation_error, set_dict_item};
use super::super::*;
#[cfg(not(Py_GIL_DISABLED))]
use super::{
    ExpectedReferentSequence, FixedReferents, collect_fixed_referents,
    match_expected_referent_sequence,
};
#[cfg(not(Py_GIL_DISABLED))]
use super::{SingleReferent, call_one_arg, collect_single_referent};

#[cfg(not(Py_GIL_DISABLED))]
struct StandardNamedTupleTypeCapability {
    row_type: Py<PyType>,
    namespace: Py<PyDict>,
    mro: Py<PyTuple>,
    fields: Py<PyTuple>,
    field_count: usize,
    asdict: Py<PyAny>,
    closure: Py<PyTuple>,
    function_referents: Vec<Py<PyAny>>,
}

#[cfg(not(Py_GIL_DISABLED))]
impl StandardNamedTupleTypeCapability {
    fn try_clone_ref(&self, py: Python<'_>) -> PyResult<Self> {
        let mut function_referents = Vec::new();
        function_referents
            .try_reserve_exact(self.function_referents.len())
            .map_err(join_allocation_error)?;
        function_referents.extend(
            self.function_referents
                .iter()
                .map(|referent| referent.clone_ref(py)),
        );
        Ok(Self {
            row_type: self.row_type.clone_ref(py),
            namespace: self.namespace.clone_ref(py),
            mro: self.mro.clone_ref(py),
            fields: self.fields.clone_ref(py),
            field_count: self.field_count,
            asdict: self.asdict.clone_ref(py),
            closure: self.closure.clone_ref(py),
            function_referents,
        })
    }
}

#[cfg(not(Py_GIL_DISABLED))]
fn borrowed_exact_dict_item(
    dictionary: &Bound<'_, PyDict>,
    key: &Bound<'_, PyString>,
) -> PyResult<*mut ffi::PyObject> {
    // SAFETY: both objects are live under the GIL. The exact dictionary owns any non-null
    // borrowed result for the remainder of this non-reentrant identity check.
    let item = unsafe { ffi::PyDict_GetItemWithError(dictionary.as_ptr(), key.as_ptr()) };
    if item.is_null() && unsafe { !ffi::PyErr_Occurred().is_null() } {
        Err(PyErr::fetch(dictionary.py()))
    } else {
        Ok(item)
    }
}

#[cfg(not(Py_GIL_DISABLED))]
pub(in crate::relational) struct StandardNamedTupleSnapshotCapability {
    fallback_adapter: Py<PyAny>,
    mapping_abc: Py<PyAny>,
    record_globals: Py<PyDict>,
    record_continuations: Py<PyTuple>,
    record_continuations_name: Py<PyString>,
    function_traverse: ffi::traverseproc,
    cell_type: Py<PyType>,
    cell_traverse: ffi::traverseproc,
    dict_cell_index: usize,
    zip_cell_index: usize,
    guard_names: StandardNamedTupleGuardNames,
    record_types: Vec<StandardNamedTupleTypeCapability>,
}

#[cfg(not(Py_GIL_DISABLED))]
struct StandardNamedTupleGuardNames {
    mro: Py<PyString>,
    dataclass_fields: Py<PyString>,
    iter: Py<PyString>,
    getattribute: Py<PyString>,
    fields: Py<PyString>,
    asdict: Py<PyString>,
}

#[cfg(not(Py_GIL_DISABLED))]
impl StandardNamedTupleGuardNames {
    fn clone_ref(&self, py: Python<'_>) -> Self {
        Self {
            mro: self.mro.clone_ref(py),
            dataclass_fields: self.dataclass_fields.clone_ref(py),
            iter: self.iter.clone_ref(py),
            getattribute: self.getattribute.clone_ref(py),
            fields: self.fields.clone_ref(py),
            asdict: self.asdict.clone_ref(py),
        }
    }
}

#[cfg(not(Py_GIL_DISABLED))]
impl StandardNamedTupleSnapshotCapability {
    fn try_clone_ref(&self, py: Python<'_>) -> PyResult<Self> {
        let mut record_types = Vec::new();
        record_types
            .try_reserve_exact(self.record_types.len())
            .map_err(join_allocation_error)?;
        for capability in &self.record_types {
            record_types.push(capability.try_clone_ref(py)?);
        }
        Ok(Self {
            fallback_adapter: self.fallback_adapter.clone_ref(py),
            mapping_abc: self.mapping_abc.clone_ref(py),
            record_globals: self.record_globals.clone_ref(py),
            record_continuations: self.record_continuations.clone_ref(py),
            record_continuations_name: self.record_continuations_name.clone_ref(py),
            function_traverse: self.function_traverse,
            cell_type: self.cell_type.clone_ref(py),
            cell_traverse: self.cell_traverse,
            dict_cell_index: self.dict_cell_index,
            zip_cell_index: self.zip_cell_index,
            guard_names: self.guard_names.clone_ref(py),
            record_types,
        })
    }

    fn traverse(&self, visit: &PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&self.fallback_adapter)?;
        visit.call(&self.mapping_abc)?;
        visit.call(&self.record_globals)?;
        visit.call(&self.record_continuations)?;
        visit.call(&self.record_continuations_name)?;
        visit.call(&self.cell_type)?;
        visit.call(&self.guard_names.mro)?;
        visit.call(&self.guard_names.dataclass_fields)?;
        visit.call(&self.guard_names.iter)?;
        visit.call(&self.guard_names.getattribute)?;
        visit.call(&self.guard_names.fields)?;
        visit.call(&self.guard_names.asdict)?;
        for capability in &self.record_types {
            visit.call(&capability.row_type)?;
            visit.call(&capability.namespace)?;
            visit.call(&capability.mro)?;
            visit.call(&capability.fields)?;
            visit.call(&capability.asdict)?;
            visit.call(&capability.closure)?;
            for referent in &capability.function_referents {
                visit.call(referent)?;
            }
        }
        Ok(())
    }
}

#[cfg(not(Py_GIL_DISABLED))]
#[pyclass(
    module = "fpstreams._native",
    name = "_StandardNamedTupleSnapshotState"
)]
struct StandardNamedTupleSnapshotState {
    capability: Option<StandardNamedTupleSnapshotCapability>,
}

#[cfg(not(Py_GIL_DISABLED))]
#[pymethods]
impl StandardNamedTupleSnapshotState {
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        if let Some(capability) = &self.capability {
            capability.traverse(&visit)?;
        }
        Ok(())
    }

    fn __clear__(&mut self) {
        self.capability = None;
    }
}

#[cfg(Py_GIL_DISABLED)]
pub(in crate::relational) struct StandardNamedTupleSnapshotCapability;

#[cfg(not(Py_GIL_DISABLED))]
fn exact_python_function_traverse(
    function: &Bound<'_, PyFunction>,
) -> PyResult<Option<ffi::traverseproc>> {
    // SAFETY: the exact built-in Python function type exposes this Stable-ABI slot.
    let slot =
        unsafe { ffi::PyType_GetSlot(function.get_type().as_type_ptr(), ffi::Py_tp_traverse) };
    if slot.is_null() {
        if unsafe { !ffi::PyErr_Occurred().is_null() } {
            unsafe { ffi::PyErr_Clear() };
        }
        return Ok(None);
    }
    // SAFETY: CPython returned this pointer specifically for Py_tp_traverse.
    Ok(Some(unsafe {
        core::mem::transmute::<*mut core::ffi::c_void, ffi::traverseproc>(slot)
    }))
}

#[cfg(not(Py_GIL_DISABLED))]
fn exact_cell_traverse(cell: &Bound<'_, PyAny>) -> PyResult<Option<ffi::traverseproc>> {
    // The canonical closure proves this exact runtime type is a cell before the resulting slot is
    // retained. PyType_GetSlot and Py_tp_traverse are both Stable ABI.
    let slot = unsafe { ffi::PyType_GetSlot(cell.get_type().as_type_ptr(), ffi::Py_tp_traverse) };
    if slot.is_null() {
        if unsafe { !ffi::PyErr_Occurred().is_null() } {
            unsafe { ffi::PyErr_Clear() };
        }
        return Ok(None);
    }
    // SAFETY: CPython returned this pointer specifically for Py_tp_traverse.
    Ok(Some(unsafe {
        core::mem::transmute::<*mut core::ffi::c_void, ffi::traverseproc>(slot)
    }))
}

#[cfg(not(Py_GIL_DISABLED))]
pub(crate) fn exact_python_function_code(
    function: &Bound<'_, PyFunction>,
    code_type: &Bound<'_, PyType>,
) -> PyResult<Option<Py<PyAny>>> {
    let py = function.py();
    let Some(traverse) = exact_python_function_traverse(function)? else {
        return Ok(None);
    };
    let mut referents = FixedReferents {
        items: [core::ptr::null_mut(); 32],
        count: 0,
        overflowed: false,
    };
    // SAFETY: function owns every referent while the synchronous visitor uses stack-live state.
    let status = unsafe {
        traverse(
            function.as_ptr(),
            collect_fixed_referents,
            (&raw mut referents).cast(),
        )
    };
    if status != 0 || referents.overflowed {
        if unsafe { !ffi::PyErr_Occurred().is_null() } {
            unsafe { ffi::PyErr_Clear() };
        }
        return Ok(None);
    }
    let mut code = None;
    for pointer in referents.items[..referents.count].iter().copied() {
        // SAFETY: the live exact function retains every pointer reported by its traversal.
        let referent = unsafe { Bound::from_borrowed_ptr(py, pointer) };
        if referent.get_type().is(code_type) {
            if code.is_some() {
                return Ok(None);
            }
            code = Some(referent.clone().unbind());
        }
    }
    Ok(code)
}

#[cfg(not(Py_GIL_DISABLED))]
fn exact_python_function_referents(
    function: &Bound<'_, PyFunction>,
    traverse: ffi::traverseproc,
) -> PyResult<Option<Vec<Py<PyAny>>>> {
    let mut referents = FixedReferents {
        items: [core::ptr::null_mut(); 32],
        count: 0,
        overflowed: false,
    };
    // SAFETY: the exact function retains every borrowed referent during synchronous traversal.
    let status = unsafe {
        traverse(
            function.as_ptr(),
            collect_fixed_referents,
            (&raw mut referents).cast(),
        )
    };
    if status != 0 || referents.overflowed {
        if unsafe { !ffi::PyErr_Occurred().is_null() } {
            unsafe { ffi::PyErr_Clear() };
        }
        return Ok(None);
    }
    let mut retained = Vec::new();
    retained
        .try_reserve_exact(referents.count)
        .map_err(join_allocation_error)?;
    for pointer in referents.items[..referents.count].iter().copied() {
        // SAFETY: the live function owns each traversal referent while it is cloned.
        retained.push(
            unsafe { Bound::from_borrowed_ptr(function.py(), pointer) }
                .clone()
                .unbind(),
        );
    }
    Ok(Some(retained))
}

#[cfg(not(Py_GIL_DISABLED))]
fn exact_python_function_has_referents(
    function: &Bound<'_, PyFunction>,
    traverse: ffi::traverseproc,
    expected: &[Py<PyAny>],
) -> PyResult<bool> {
    let mut state = ExpectedReferentSequence {
        expected: expected.as_ptr(),
        expected_count: expected.len(),
        observed_count: 0,
        matches: true,
    };
    // SAFETY: the exact function and every owned expected referent stay live throughout the
    // synchronous traversal; the visitor only compares their object pointers in captured order.
    let status = unsafe {
        traverse(
            function.as_ptr(),
            match_expected_referent_sequence,
            (&raw mut state).cast(),
        )
    };
    if status != 0 {
        if unsafe { !ffi::PyErr_Occurred().is_null() } {
            return Err(PyErr::fetch(function.py()));
        }
        return Ok(false);
    }
    Ok(state.matches && state.observed_count == state.expected_count)
}

#[cfg(not(Py_GIL_DISABLED))]
fn standard_namedtuple_freevar_indices(
    freevars: &Bound<'_, PyTuple>,
) -> PyResult<Option<(usize, usize)>> {
    if freevars.len() != 2 {
        return Ok(None);
    }
    let mut dict_index = None;
    let mut zip_index = None;
    for (index, name) in freevars.iter().enumerate() {
        let name = match name.cast_exact::<PyString>() {
            Ok(name) => name,
            Err(_) => return Ok(None),
        };
        match name.to_str()? {
            "_dict" if dict_index.is_none() => dict_index = Some(index),
            "_zip" if zip_index.is_none() => zip_index = Some(index),
            _ => return Ok(None),
        }
    }
    Ok(dict_index.zip(zip_index))
}

#[cfg(not(Py_GIL_DISABLED))]
fn standard_namedtuple_closure_is_canonical(
    closure: &Bound<'_, PyTuple>,
    dict_cell_index: usize,
    zip_cell_index: usize,
    cell_type: &Bound<'_, PyType>,
    cell_traverse: ffi::traverseproc,
) -> PyResult<bool> {
    if closure.len() != 2 {
        return Ok(false);
    }
    // SAFETY: both indices were derived from the canonical two-name freevar tuple and are in
    // bounds for this exact two-item closure tuple.
    let dict_cell =
        unsafe { ffi::PyTuple_GetItem(closure.as_ptr(), dict_cell_index as ffi::Py_ssize_t) };
    let zip_cell =
        unsafe { ffi::PyTuple_GetItem(closure.as_ptr(), zip_cell_index as ffi::Py_ssize_t) };
    if dict_cell.is_null()
        || zip_cell.is_null()
        || unsafe { ffi::Py_TYPE(dict_cell) } != cell_type.as_type_ptr()
        || unsafe { ffi::Py_TYPE(zip_cell) } != cell_type.as_type_ptr()
    {
        return Ok(false);
    }
    let mut dict_referent = SingleReferent {
        first: core::ptr::null_mut(),
        count: 0,
    };
    let mut zip_referent = SingleReferent {
        first: core::ptr::null_mut(),
        count: 0,
    };
    // SAFETY: both objects have the exact type that supplied this slot, retain their contents for
    // the synchronous visitors, and the two visitor states remain stack-live for each call.
    let dict_status = unsafe {
        cell_traverse(
            dict_cell,
            collect_single_referent,
            (&raw mut dict_referent).cast(),
        )
    };
    let zip_status = unsafe {
        cell_traverse(
            zip_cell,
            collect_single_referent,
            (&raw mut zip_referent).cast(),
        )
    };
    if dict_status != 0 || zip_status != 0 {
        if unsafe { !ffi::PyErr_Occurred().is_null() } {
            return Err(PyErr::fetch(closure.py()));
        }
        return Ok(false);
    }
    let expected_zip = (&raw mut ffi::PyZip_Type).cast::<ffi::PyObject>();
    Ok(dict_referent.count == 1
        && dict_referent.first
            == closure
                .py()
                .get_type::<PyDict>()
                .as_type_ptr()
                .cast::<ffi::PyObject>()
        && zip_referent.count == 1
        && zip_referent.first == expected_zip)
}

#[cfg(not(Py_GIL_DISABLED))]
fn exact_mapping_proxy_referent_dict(proxy: &Bound<'_, PyAny>) -> PyResult<Option<Py<PyDict>>> {
    let py = proxy.py();
    let probe_mapping = PyDict::new(py);
    // SAFETY: probe_mapping is an exact live dict and PyDictProxy_New returns an owned reference.
    let probe_proxy = unsafe { ffi::PyDictProxy_New(probe_mapping.as_ptr()) };
    // SAFETY: probe_proxy is the owned result of PyDictProxy_New.
    let probe_proxy = unsafe { Bound::from_owned_ptr_or_err(py, probe_proxy)? };
    let proxy_type = probe_proxy.get_type();
    if !proxy.get_type().is(&proxy_type) {
        return Ok(None);
    }
    // SAFETY: the exact built-in mappingproxy type exposes this Stable-ABI slot.
    let slot = unsafe { ffi::PyType_GetSlot(proxy_type.as_type_ptr(), ffi::Py_tp_traverse) };
    if slot.is_null() {
        // SAFETY: optional capability discovery must not leak a CPython slot error.
        if unsafe { !ffi::PyErr_Occurred().is_null() } {
            unsafe { ffi::PyErr_Clear() };
        }
        return Ok(None);
    }
    // SAFETY: CPython returned this pointer specifically for Py_tp_traverse.
    let traverse =
        unsafe { core::mem::transmute::<*mut core::ffi::c_void, ffi::traverseproc>(slot) };
    let mut referent = SingleReferent {
        first: core::ptr::null_mut(),
        count: 0,
    };
    // SAFETY: proxy has the exact type that owns this slot and the visitor state is stack-live.
    let status = unsafe {
        traverse(
            proxy.as_ptr(),
            collect_single_referent,
            (&raw mut referent).cast(),
        )
    };
    if status != 0 {
        if unsafe { !ffi::PyErr_Occurred().is_null() } {
            // SAFETY: the infallible visitor cannot intentionally leave an exception set.
            unsafe { ffi::PyErr_Clear() };
        }
        return Ok(None);
    }
    if referent.count != 1 || referent.first.is_null() {
        return Ok(None);
    }
    // SAFETY: the live mappingproxy retains its backing namespace dict while we take a reference.
    let namespace = unsafe { Bound::from_borrowed_ptr(py, referent.first) };
    Ok(namespace
        .cast_into_exact::<PyDict>()
        .ok()
        .map(Bound::unbind))
}

#[cfg(not(Py_GIL_DISABLED))]
fn standard_namedtuple_capability_from_state(
    state: &Bound<'_, PyAny>,
) -> PyResult<Option<StandardNamedTupleSnapshotCapability>> {
    let state = match state.cast_exact::<StandardNamedTupleSnapshotState>() {
        Ok(state) => state,
        Err(_) => return Ok(None),
    };
    let state = state.try_borrow()?;
    match &state.capability {
        Some(capability) => Ok(Some(capability.try_clone_ref(state.py())?)),
        None => Ok(None),
    }
}

#[cfg(not(Py_GIL_DISABLED))]
impl StandardNamedTupleSnapshotCapability {
    fn matching_type<'py>(
        &'py self,
        row: &Bound<'py, PyAny>,
    ) -> Option<&'py StandardNamedTupleTypeCapability> {
        // SAFETY: row is live and every capability retains its exact PyType.
        let row_type = unsafe { ffi::Py_TYPE(row.as_ptr()) };
        self.record_types
            .iter()
            .find(|capability| row_type == capability.row_type.bind(row.py()).as_type_ptr())
    }

    fn proof_is_live(
        &self,
        capability: &StandardNamedTupleTypeCapability,
        py: Python<'_>,
    ) -> PyResult<bool> {
        let fields = capability.fields.bind(py);
        let row_type = capability.row_type.bind(py);
        if !row_type
            .getattr(self.guard_names.mro.bind(py))?
            .is(capability.mro.bind(py).as_any())
        {
            return Ok(false);
        }
        let namespace = capability.namespace.bind(py);
        if !borrowed_exact_dict_item(namespace, self.guard_names.dataclass_fields.bind(py))?
            .is_null()
            || !borrowed_exact_dict_item(namespace, self.guard_names.iter.bind(py))?.is_null()
            || !borrowed_exact_dict_item(namespace, self.guard_names.getattribute.bind(py))?
                .is_null()
        {
            return Ok(false);
        }
        if borrowed_exact_dict_item(namespace, self.guard_names.fields.bind(py))? != fields.as_ptr()
        {
            return Ok(false);
        }
        let asdict = capability.asdict.bind(py);
        if borrowed_exact_dict_item(namespace, self.guard_names.asdict.bind(py))? != asdict.as_ptr()
        {
            return Ok(false);
        }
        let asdict_function = match asdict.cast_exact::<PyFunction>() {
            Ok(function) => function,
            Err(_) => return Ok(false),
        };
        if !exact_python_function_has_referents(
            asdict_function,
            self.function_traverse,
            &capability.function_referents,
        )? {
            return Ok(false);
        }
        if !standard_namedtuple_closure_is_canonical(
            capability.closure.bind(py),
            self.dict_cell_index,
            self.zip_cell_index,
            self.cell_type.bind(py),
            self.cell_traverse,
        )? {
            return Ok(false);
        }
        Ok(true)
    }

    fn continuation_record(
        &self,
        continuations: &Bound<'_, PyTuple>,
        index: usize,
        row: &Bound<'_, PyAny>,
    ) -> PyResult<Option<Py<PyDict>>> {
        call_one_arg(&continuations.get_item(index)?, row)?
            .cast_into_exact::<PyDict>()
            .map(Bound::unbind)
            .map(Some)
            .map_err(Into::into)
    }

    fn fast_snapshot(&self, row: &Bound<'_, PyAny>) -> PyResult<Option<Py<PyDict>>> {
        let Some(capability) = self.matching_type(row) else {
            return Ok(None);
        };
        let py = row.py();

        let continuations = self.record_continuations.bind(py);
        if borrowed_exact_dict_item(
            self.record_globals.bind(py),
            self.record_continuations_name.bind(py),
        )? != continuations.as_ptr()
        {
            return Ok(None);
        }
        // Run dynamic Mapping dispatch before validating the snapshot proof, matching canonical
        // protocol order. A false subclass hook may mutate any proof before returning; validate
        // once afterward and continue after Mapping rather than replaying the hook on a miss.
        let is_mapping = row.is_instance(self.mapping_abc.bind(py))?;
        if is_mapping {
            return self.continuation_record(continuations, 0, row);
        }
        if !self.proof_is_live(capability, py)? {
            return self.continuation_record(continuations, 1, row);
        }

        // The exact row type is a proven tuple subtype. Stable-ABI tuple access bypasses any
        // inherited protocol overrides and deliberately follows dict(zip(fields, row)) truncation.
        // SAFETY: row's exact type identity matched a type whose live MRO and namespace guards pass.
        let row_size = unsafe { ffi::PyTuple_Size(row.as_ptr()) };
        if row_size < 0 {
            return Err(PyErr::fetch(py));
        }
        let fields = capability.fields.bind(py);
        let item_count = capability.field_count.min(row_size as usize);
        let snapshot = new_dict_fallible(py)?;
        for index in 0..item_count {
            // SAFETY: index is within both the cached exact fields tuple and live tuple storage.
            let value = unsafe { ffi::PyTuple_GetItem(row.as_ptr(), index as ffi::Py_ssize_t) };
            if value.is_null() {
                return Err(PyErr::fetch(py));
            }
            let field = unsafe { ffi::PyTuple_GetItem(fields.as_ptr(), index as ffi::Py_ssize_t) };
            if field.is_null() {
                return Err(PyErr::fetch(py));
            }
            set_dict_item(py, snapshot.as_ptr(), field, value)?;
        }
        Ok(Some(snapshot.unbind()))
    }

    fn snapshot_or_fallback(&self, row: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        if let Some(snapshot) = self.fast_snapshot(row)? {
            return Ok(snapshot.into_any());
        }
        Ok(call_one_arg(self.fallback_adapter.bind(row.py()), row)?.unbind())
    }

    pub(super) fn snapshot_or_fallback_dict(&self, row: &Bound<'_, PyAny>) -> PyResult<Py<PyDict>> {
        self.snapshot_or_fallback(row)?
            .into_bound(row.py())
            .cast_into_exact::<PyDict>()
            .map(Bound::unbind)
            .map_err(Into::into)
    }
}

#[cfg(Py_GIL_DISABLED)]
impl StandardNamedTupleSnapshotCapability {
    pub(super) fn snapshot_or_fallback_dict(
        &self,
        _row: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyDict>> {
        unreachable!("free-threaded builds never admit the NamedTuple snapshot capability")
    }
}

#[cfg(not(Py_GIL_DISABLED))]
unsafe fn standard_namedtuple_record_adapter_call(
    py: Python<'_>,
    state: *mut ffi::PyObject,
    row: *mut ffi::PyObject,
) -> PyResult<*mut ffi::PyObject> {
    // SAFETY: this private PyCFunction retains its GC-visible state for the active call.
    let state = unsafe { Bound::from_borrowed_ptr(py, state) };
    // SAFETY: row is a non-null positional argument owned by the active Python call.
    let row = unsafe { Bound::from_borrowed_ptr(py, row) };
    let state = state
        .cast_exact::<StandardNamedTupleSnapshotState>()
        .map_err(|_| PyTypeError::new_err("invalid native NamedTuple adapter state"))?;
    let state = state.try_borrow()?;
    let capability = state
        .capability
        .as_ref()
        .ok_or_else(|| PyTypeError::new_err("cleared native NamedTuple adapter state"))?;
    Ok(capability.snapshot_or_fallback(&row)?.into_ptr())
}

#[cfg(not(Py_GIL_DISABLED))]
static STANDARD_NAMEDTUPLE_RECORD_ADAPTER_METHOD: AtomicPtr<ffi::PyMethodDef> =
    AtomicPtr::new(core::ptr::null_mut());

#[cfg(not(Py_GIL_DISABLED))]
fn standard_namedtuple_record_adapter_method() -> PyResult<*mut ffi::PyMethodDef> {
    let retained = STANDARD_NAMEDTUPLE_RECORD_ADAPTER_METHOD.load(Ordering::Acquire);
    if !retained.is_null() {
        return Ok(retained);
    }
    // SAFETY: PyMem_Malloc returns suitably aligned storage or null without publishing it.
    let candidate =
        unsafe { ffi::PyMem_Malloc(size_of::<ffi::PyMethodDef>()) }.cast::<ffi::PyMethodDef>();
    if candidate.is_null() {
        return Err(PyMemoryError::new_err(
            "could not allocate native NamedTuple method definition",
        ));
    }
    // SAFETY: candidate points to writable storage for exactly one PyMethodDef.
    unsafe {
        candidate.write(ffi::PyMethodDef {
            ml_name: c"_fpstreams_standard_namedtuple_record_adapter_v1".as_ptr(),
            ml_meth: ffi::PyMethodDefPointer {
                PyCFunction: pyo3::get_trampoline_function!(
                    binaryfunc,
                    standard_namedtuple_record_adapter_call
                ),
            },
            ml_flags: ffi::METH_O,
            ml_doc: core::ptr::null(),
        });
    }
    match STANDARD_NAMEDTUPLE_RECORD_ADAPTER_METHOD.compare_exchange(
        core::ptr::null_mut(),
        candidate,
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        Ok(_) => Ok(candidate),
        Err(retained) => {
            // SAFETY: this losing candidate was never published or passed to Python.
            unsafe { ffi::PyMem_Free(candidate.cast()) };
            Ok(retained)
        }
    }
}

#[cfg(not(Py_GIL_DISABLED))]
pub(in crate::relational) fn standard_namedtuple_snapshot_capability(
    record_adapter: &Bound<'_, PyAny>,
) -> PyResult<Option<StandardNamedTupleSnapshotCapability>> {
    // SAFETY: these Stable-ABI predicates/accessors accept every live Python object.
    if unsafe { ffi::PyCFunction_CheckExact(record_adapter.as_ptr()) } == 0
        || unsafe { ffi::PyCFunction_GetFlags(record_adapter.as_ptr()) } != ffi::METH_O
    {
        return Ok(None);
    }
    // Read the exact trampoline stored in the same process-lifetime method definition used by
    // construction; a second macro expansion need not have a comparable function address.
    let method = standard_namedtuple_record_adapter_method()?;
    // SAFETY: method is the immutable process-lifetime definition published above.
    let expected = unsafe { (*method).ml_meth.PyCFunction };
    let Some(actual) = (unsafe { ffi::PyCFunction_GetFunction(record_adapter.as_ptr()) }) else {
        return Ok(None);
    };
    if !std::ptr::fn_addr_eq(actual, expected) {
        return Ok(None);
    }
    // SAFETY: this exact private function was constructed with a non-null private state self.
    let state = unsafe { ffi::PyCFunction_GetSelf(record_adapter.as_ptr()) };
    if state.is_null() {
        return Ok(None);
    }
    // SAFETY: PyCFunction owns the state for at least record_adapter's lifetime.
    let state = unsafe { Bound::from_borrowed_ptr(record_adapter.py(), state) };
    standard_namedtuple_capability_from_state(&state)
}

#[cfg(Py_GIL_DISABLED)]
pub(in crate::relational) fn standard_namedtuple_snapshot_capability(
    _record_adapter: &Bound<'_, PyAny>,
) -> PyResult<Option<StandardNamedTupleSnapshotCapability>> {
    Ok(None)
}

#[cfg(not(Py_GIL_DISABLED))]
#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub(crate) fn standard_namedtuple_record_adapter_v1(
    record_types: &Bound<'_, PyAny>,
    fallback_adapter: &Bound<'_, PyAny>,
    get_cache_token: &Bound<'_, PyAny>,
    abc_token: &Bound<'_, PyAny>,
    canonical_namedtuple_factory: &Bound<'_, PyAny>,
    code_type: &Bound<'_, PyAny>,
    mapping_abc: &Bound<'_, PyAny>,
    record_continuations: &Bound<'_, PyAny>,
    record_globals: &Bound<'_, PyAny>,
) -> PyResult<Option<Py<PyAny>>> {
    let py = record_types.py();
    let record_types = match record_types.cast_exact::<PyTuple>() {
        Ok(record_types) if matches!(record_types.len(), 1 | 2) => record_types,
        _ => return Ok(None),
    };
    if !fallback_adapter.is_callable() || !get_cache_token.is_callable() {
        return Ok(None);
    }
    let record_continuations = match record_continuations.cast_exact::<PyTuple>() {
        Ok(continuations)
            if continuations.len() == 2
                && continuations.get_item(0)?.is_callable()
                && continuations.get_item(1)?.is_callable() =>
        {
            continuations
        }
        _ => return Ok(None),
    };
    let record_globals = match record_globals.cast_exact::<PyDict>() {
        Ok(globals) => globals,
        Err(_) => return Ok(None),
    };
    let record_continuations_name = PyString::intern(py, "_RECORD_CONTINUATIONS");
    if !record_globals
        .get_item(&record_continuations_name)?
        .is_some_and(|live| live.is(record_continuations.as_any()))
    {
        return Ok(None);
    }
    let abc_token = match abc_token.cast_exact::<PyInt>() {
        Ok(token) => token.extract::<u64>()?,
        Err(_) => return Ok(None),
    };
    let current_token = get_cache_token.call0()?;
    if current_token.cast_exact::<PyInt>()?.extract::<u64>()? != abc_token {
        return Ok(None);
    }
    let code_type = match code_type.cast_exact::<PyType>() {
        Ok(code_type) => code_type,
        Err(_) => return Ok(None),
    };
    let canonical_namedtuple_factory = match canonical_namedtuple_factory.cast_exact::<PyFunction>()
    {
        Ok(factory) => factory,
        Err(_) => return Ok(None),
    };
    let Some(function_traverse) = exact_python_function_traverse(canonical_namedtuple_factory)?
    else {
        return Ok(None);
    };
    let Some(factory_code) = exact_python_function_code(canonical_namedtuple_factory, code_type)?
    else {
        return Ok(None);
    };
    let constants = match factory_code
        .bind(py)
        .getattr("co_consts")?
        .cast_into_exact::<PyTuple>()
    {
        Ok(constants) => constants,
        Err(_) => return Ok(None),
    };
    let mut canonical_asdict = None;
    for constant in constants.iter() {
        if !constant.get_type().is(code_type) {
            continue;
        }
        let name = match constant.getattr("co_name")?.cast_into_exact::<PyString>() {
            Ok(name) => name,
            Err(_) => return Ok(None),
        };
        if name.to_str()? != "_asdict" {
            continue;
        }
        let freevars = match constant
            .getattr("co_freevars")?
            .cast_into_exact::<PyTuple>()
        {
            Ok(freevars) => freevars,
            Err(_) => return Ok(None),
        };
        let Some(indices) = standard_namedtuple_freevar_indices(&freevars)? else {
            continue;
        };
        if canonical_asdict.is_some() {
            return Ok(None);
        }
        canonical_asdict = Some((constant.unbind(), indices));
    }
    let Some((canonical_asdict_code, (dict_cell_index, zip_cell_index))) = canonical_asdict else {
        return Ok(None);
    };
    let staticmethod_type = match py
        .import("builtins")?
        .getattr("staticmethod")?
        .cast_into_exact::<PyType>()
    {
        Ok(staticmethod_type) => staticmethod_type,
        Err(_) => return Ok(None),
    };
    let tuple_type = py.get_type::<PyTuple>();
    let mut canonical_cell_type: Option<Py<PyType>> = None;
    let mut canonical_cell_traverse: Option<ffi::traverseproc> = None;
    let mut seen_types = Vec::new();
    let mut capabilities = Vec::new();
    seen_types
        .try_reserve_exact(record_types.len())
        .map_err(join_allocation_error)?;
    capabilities
        .try_reserve_exact(record_types.len())
        .map_err(join_allocation_error)?;
    for row_type in record_types.iter() {
        let row_type = match row_type.cast_exact::<PyType>() {
            Ok(row_type) => row_type,
            Err(_) => return Ok(None),
        };
        if row_type.is_subclass(mapping_abc)? {
            return Ok(None);
        }
        if seen_types
            .iter()
            .any(|seen: &Py<PyType>| seen.bind(py).is(row_type))
        {
            return Ok(None);
        }
        let bases = match row_type.getattr("__bases__")?.cast_into_exact::<PyTuple>() {
            Ok(bases) if bases.len() == 1 && bases.get_item(0)?.is(tuple_type.as_any()) => bases,
            _ => return Ok(None),
        };
        let _ = bases;
        let mro = match row_type.getattr("__mro__")?.cast_into_exact::<PyTuple>() {
            Ok(mro)
                if mro.len() == 3
                    && mro.get_item(0)?.is(row_type)
                    && mro.get_item(1)?.is(tuple_type.as_any())
                    && mro.get_item(2)?.as_ptr()
                        == (&raw mut ffi::PyBaseObject_Type).cast::<ffi::PyObject>() =>
            {
                mro
            }
            _ => return Ok(None),
        };
        let namespace_proxy = row_type.getattr("__dict__")?;
        let Some(namespace) = exact_mapping_proxy_referent_dict(&namespace_proxy)? else {
            return Ok(None);
        };
        let namespace = namespace.bind(py);
        let slots = match namespace.get_item("__slots__")? {
            Some(slots) => match slots.cast_into_exact::<PyTuple>() {
                Ok(slots) if slots.is_empty() => slots,
                _ => return Ok(None),
            },
            None => return Ok(None),
        };
        let _ = slots;
        if namespace.get_item("__dataclass_fields__")?.is_some()
            || namespace.get_item("__iter__")?.is_some()
            || namespace.get_item("__getattribute__")?.is_some()
        {
            return Ok(None);
        }
        let fields = match namespace.get_item("_fields")? {
            Some(fields) => match fields.cast_into_exact::<PyTuple>() {
                Ok(fields) => fields,
                Err(_) => return Ok(None),
            },
            None => return Ok(None),
        };
        for field in fields.iter() {
            if field.cast_exact::<PyString>().is_err() {
                return Ok(None);
            }
        }
        let raw_new = match namespace.get_item("__new__")? {
            Some(raw_new) if raw_new.get_type().is(&staticmethod_type) => raw_new,
            _ => return Ok(None),
        };
        let _ = raw_new;
        let asdict = match namespace.get_item("_asdict")? {
            Some(asdict) => match asdict.cast_into_exact::<PyFunction>() {
                Ok(asdict) => asdict,
                Err(_) => return Ok(None),
            },
            None => return Ok(None),
        };
        if !exact_python_function_code(&asdict, code_type)?
            .is_some_and(|code| code.bind(py).is(canonical_asdict_code.bind(py)))
        {
            return Ok(None);
        }
        let closure = match asdict.getattr("__closure__")?.cast_into_exact::<PyTuple>() {
            Ok(closure) => closure,
            Err(_) => return Ok(None),
        };
        if closure.len() != 2 {
            return Ok(None);
        }
        let dict_cell = closure.get_item(dict_cell_index)?;
        let zip_cell = closure.get_item(zip_cell_index)?;
        let live_cell_type = dict_cell.get_type();
        if !zip_cell.get_type().is(&live_cell_type) {
            return Ok(None);
        }
        let Some(live_cell_traverse) = exact_cell_traverse(&dict_cell)? else {
            return Ok(None);
        };
        if let Some(expected) = &canonical_cell_type {
            if !live_cell_type.is(expected.bind(py))
                || !std::ptr::fn_addr_eq(
                    live_cell_traverse,
                    canonical_cell_traverse.expect("a retained cell type has its traverse slot"),
                )
            {
                return Ok(None);
            }
        } else {
            canonical_cell_type = Some(live_cell_type.clone().unbind());
            canonical_cell_traverse = Some(live_cell_traverse);
        }
        if !standard_namedtuple_closure_is_canonical(
            &closure,
            dict_cell_index,
            zip_cell_index,
            &live_cell_type,
            live_cell_traverse,
        )? {
            return Ok(None);
        }
        let Some(function_referents) = exact_python_function_referents(&asdict, function_traverse)?
        else {
            return Ok(None);
        };
        seen_types.push(row_type.clone().unbind());
        capabilities.push(StandardNamedTupleTypeCapability {
            row_type: row_type.clone().unbind(),
            namespace: namespace.clone().unbind(),
            mro: mro.unbind(),
            field_count: fields.len(),
            fields: fields.unbind(),
            asdict: asdict.unbind().into_any(),
            closure: closure.unbind(),
            function_referents,
        });
    }
    let final_token = get_cache_token.call0()?;
    if final_token.cast_exact::<PyInt>()?.extract::<u64>()? != abc_token {
        return Ok(None);
    }
    let cell_type = canonical_cell_type.expect("at least one NamedTuple type was admitted");
    let cell_traverse =
        canonical_cell_traverse.expect("an admitted NamedTuple cell type has a traverse slot");
    let capability = StandardNamedTupleSnapshotCapability {
        fallback_adapter: fallback_adapter.clone().unbind(),
        mapping_abc: mapping_abc.clone().unbind(),
        record_globals: record_globals.clone().unbind(),
        record_continuations: record_continuations.clone().unbind(),
        record_continuations_name: record_continuations_name.unbind(),
        function_traverse,
        cell_type,
        cell_traverse,
        dict_cell_index,
        zip_cell_index,
        guard_names: StandardNamedTupleGuardNames {
            mro: PyString::intern(py, "__mro__").unbind(),
            dataclass_fields: PyString::intern(py, "__dataclass_fields__").unbind(),
            iter: PyString::intern(py, "__iter__").unbind(),
            getattribute: PyString::intern(py, "__getattribute__").unbind(),
            fields: PyString::intern(py, "_fields").unbind(),
            asdict: PyString::intern(py, "_asdict").unbind(),
        },
        record_types: capabilities,
    };
    // A private GC-tracked carrier keeps the payload opaque while exposing all strong Python
    // references to cycle detection. Its __clear__ breaks adapter/type/fallback back-references.
    let state = Bound::new(
        py,
        StandardNamedTupleSnapshotState {
            capability: Some(capability),
        },
    )?;
    let method = standard_namedtuple_record_adapter_method()?;
    // SAFETY: the process-lifetime method definition matches METH_O and PyCFunction_NewEx takes
    // a strong reference to the exact private GC-tracked state.
    let callable = unsafe { ffi::PyCFunction_NewEx(method, state.as_ptr(), core::ptr::null_mut()) };
    // SAFETY: a successful PyCFunction_NewEx returns one owned reference.
    Ok(Some(
        unsafe { Bound::from_owned_ptr_or_err(py, callable)? }.unbind(),
    ))
}

#[cfg(Py_GIL_DISABLED)]
#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub(crate) fn standard_namedtuple_record_adapter_v1(
    _record_types: &Bound<'_, PyAny>,
    _fallback_adapter: &Bound<'_, PyAny>,
    _get_cache_token: &Bound<'_, PyAny>,
    _abc_token: &Bound<'_, PyAny>,
    _canonical_namedtuple_factory: &Bound<'_, PyAny>,
    _code_type: &Bound<'_, PyAny>,
    _mapping_abc: &Bound<'_, PyAny>,
    _record_continuations: &Bound<'_, PyAny>,
    _record_globals: &Bound<'_, PyAny>,
) -> PyResult<Option<Py<PyAny>>> {
    Ok(None)
}
