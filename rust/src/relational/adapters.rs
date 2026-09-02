//! Guarded snapshots for MappingProxy and standard NamedTuple records.

use super::*;

mod namedtuple;

#[cfg(not(Py_GIL_DISABLED))]
pub(crate) use namedtuple::exact_python_function_code;
pub(crate) use namedtuple::standard_namedtuple_record_adapter_v1;
pub(super) use namedtuple::{
    StandardNamedTupleSnapshotCapability, standard_namedtuple_snapshot_capability,
};

/// Invoke one Python callable without allocating a one-element argument tuple.
#[inline(always)]
pub(super) fn call_one_arg<'py>(
    callable: &Bound<'py, PyAny>,
    argument: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    // SAFETY: both objects stay live for the duration of the stable-ABI varargs call, and the
    // trailing null pointer terminates its borrowed argument list. The result is a new reference
    // or a null pointer with a Python exception set.
    let result = unsafe {
        ffi::PyObject_CallFunctionObjArgs(
            callable.as_ptr(),
            argument.as_ptr(),
            core::ptr::null_mut::<ffi::PyObject>(),
        )
    };
    unsafe { Bound::from_owned_ptr_or_err(callable.py(), result) }
}

/// Fast adapter for preflighted rows plus an optional canonical live-replacement fallback.
pub(super) struct CallableJoinRecordAdapters {
    pub(super) preflighted: Py<PyAny>,
    pub(super) fallback: Option<Py<PyAny>>,
}

impl CallableJoinRecordAdapters {
    /// Accept legacy single-callable adapters or a private `(fast, canonical)` exact tuple.
    pub(super) fn parse(value: &Bound<'_, PyAny>) -> PyResult<Option<Self>> {
        if value.is_callable() {
            return Ok(Some(Self {
                preflighted: value.clone().unbind(),
                fallback: None,
            }));
        }
        let pair = match value.cast_exact::<PyTuple>() {
            Ok(pair) if pair.len() == 2 => pair,
            Ok(_) | Err(_) => return Ok(None),
        };
        let preflighted = pair.get_item(0)?;
        let fallback = pair.get_item(1)?;
        if !preflighted.is_callable() || !fallback.is_callable() {
            return Ok(None);
        }
        Ok(Some(Self {
            preflighted: preflighted.unbind(),
            fallback: Some(fallback.unbind()),
        }))
    }
}

pub(super) struct CallableJoinSnapshot {
    pub(super) record: Py<PyDict>,
    pub(super) trusted: bool,
}

/// Snapshot one current row before invoking its opaque selector.
#[inline(always)]
pub(super) fn snapshot_callable_join_record(
    row: &Bound<'_, PyAny>,
    record_adapters: &CallableJoinRecordAdapters,
    use_fallback: bool,
    mapping_proxy: Option<&MappingProxySnapshotCapability>,
    namedtuple: Option<&StandardNamedTupleSnapshotCapability>,
) -> PyResult<CallableJoinSnapshot> {
    if let Some(capability) = namedtuple {
        return capability
            .snapshot_or_fallback_dict(row)
            .map(|record| CallableJoinSnapshot {
                record,
                trusted: false,
            });
    }
    if let Ok(record) = row.cast_exact::<PyDict>() {
        return Ok(CallableJoinSnapshot {
            record: record.copy()?.unbind(),
            trusted: true,
        });
    }
    if let Some(record) = snapshot_exact_string_mapping_proxy(row, mapping_proxy)? {
        return Ok(CallableJoinSnapshot {
            record,
            trusted: true,
        });
    }
    let adapter = if use_fallback {
        record_adapters
            .fallback
            .as_ref()
            .expect("a fallback snapshot requires a parsed canonical adapter")
    } else {
        &record_adapters.preflighted
    };
    let trusted = !use_fallback && adapter.bind(row.py()).is(row.py().get_type::<PyDict>());
    call_one_arg(adapter.bind(row.py()), row)?
        .cast_into_exact::<PyDict>()
        .map(|record| CallableJoinSnapshot {
            record: record.unbind(),
            trusted,
        })
        .map_err(Into::into)
}

pub(super) struct MappingProxySnapshotCapability {
    row_type: Py<PyType>,
    traverse: ffi::traverseproc,
}

struct SingleReferent {
    first: *mut ffi::PyObject,
    count: usize,
}

#[cfg(not(Py_GIL_DISABLED))]
struct FixedReferents {
    items: [*mut ffi::PyObject; 32],
    count: usize,
    overflowed: bool,
}

#[cfg(not(Py_GIL_DISABLED))]
struct ExpectedReferentSequence {
    expected: *const Py<PyAny>,
    expected_count: usize,
    observed_count: usize,
    matches: bool,
}

#[cfg(not(Py_GIL_DISABLED))]
unsafe extern "C" fn collect_fixed_referents(
    object: *mut ffi::PyObject,
    argument: *mut core::ffi::c_void,
) -> core::ffi::c_int {
    // SAFETY: exact_python_function_code passes stack-live storage and tp_traverse invokes this
    // visitor synchronously while retaining every borrowed referent through the function object.
    let state = unsafe { &mut *argument.cast::<FixedReferents>() };
    if state.count < state.items.len() {
        state.items[state.count] = object;
        state.count += 1;
    } else {
        state.overflowed = true;
    }
    0
}

#[cfg(not(Py_GIL_DISABLED))]
unsafe extern "C" fn match_expected_referent_sequence(
    object: *mut ffi::PyObject,
    argument: *mut core::ffi::c_void,
) -> core::ffi::c_int {
    // SAFETY: the exact function traversal synchronously retains object, the expected owned
    // references, and the stack-live state. Factory-time discovery captured this runtime's own
    // traversal order, so one pointer comparison proves code and closure identity together.
    let state = unsafe { &mut *argument.cast::<ExpectedReferentSequence>() };
    if state.observed_count >= state.expected_count {
        state.matches = false;
    } else {
        // SAFETY: observed_count is within the retained expected slice proven above.
        let expected = unsafe { &*state.expected.add(state.observed_count) };
        state.matches &= expected.as_ptr() == object;
    }
    state.observed_count = state.observed_count.saturating_add(1);
    0
}

unsafe extern "C" fn collect_single_referent(
    object: *mut ffi::PyObject,
    argument: *mut core::ffi::c_void,
) -> core::ffi::c_int {
    // SAFETY: snapshot_exact_string_mapping_proxy passes a live stack state and tp_traverse calls
    // the visitor synchronously. The borrowed object stays owned by the live mappingproxy.
    let state = unsafe { &mut *argument.cast::<SingleReferent>() };
    if state.count == 0 {
        state.first = object;
    }
    state.count = state.count.saturating_add(1);
    0
}

/// Resolve exact MappingProxyType's public stable-ABI traverse slot once per native join.
pub(super) fn mapping_proxy_snapshot_capability(
    record_adapter: &Bound<'_, PyAny>,
    allowed_record_types: &[Py<PyType>],
) -> PyResult<Option<MappingProxySnapshotCapability>> {
    let py = record_adapter.py();
    if !record_adapter.is(py.get_type::<PyDict>().as_any()) || allowed_record_types.is_empty() {
        return Ok(None);
    }
    // Construct the actual built-in type through Stable ABI instead of trusting the mutable
    // ``types.MappingProxyType`` module attribute.
    let probe_mapping = PyDict::new(py);
    // SAFETY: probe_mapping is a live exact dict. PyDictProxy_New returns a new reference or null
    // with a Python error, and the owned Bound below assumes that documented ownership.
    let probe_proxy = unsafe { ffi::PyDictProxy_New(probe_mapping.as_ptr()) };
    // SAFETY: probe_proxy is the owned result of PyDictProxy_New.
    let probe_proxy = unsafe { Bound::from_owned_ptr_or_err(py, probe_proxy)? };
    let row_type = probe_proxy.get_type();
    if !allowed_record_types
        .iter()
        .any(|allowed| allowed.bind(py).is(&row_type))
    {
        return Ok(None);
    }
    // PyType_GetSlot is Stable ABI since 3.4 and Py_tp_traverse is a stable slot id. The returned
    // pointer has traverseproc's documented signature for this slot.
    // SAFETY: row_type is a live built-in type and Py_tp_traverse identifies a public type slot.
    let slot = unsafe { ffi::PyType_GetSlot(row_type.as_type_ptr(), ffi::Py_tp_traverse) };
    if slot.is_null() {
        // SAFETY: checking and clearing the thread's active Python error is valid while attached.
        if unsafe { !ffi::PyErr_Occurred().is_null() } {
            // This optional proof must not add a failure absent from canonical dict(row).
            unsafe { ffi::PyErr_Clear() };
        }
        return Ok(None);
    }
    // SAFETY: CPython returned this non-null function pointer specifically for Py_tp_traverse.
    let traverse =
        unsafe { core::mem::transmute::<*mut core::ffi::c_void, ffi::traverseproc>(slot) };
    Ok(Some(MappingProxySnapshotCapability {
        row_type: row_type.unbind(),
        traverse,
    }))
}

/// Recover one exact proxy referent without audit events, then copy only exact-string dict rows.
fn snapshot_exact_string_mapping_proxy(
    row: &Bound<'_, PyAny>,
    capability: Option<&MappingProxySnapshotCapability>,
) -> PyResult<Option<Py<PyDict>>> {
    let Some(capability) = capability else {
        return Ok(None);
    };
    let py = row.py();
    if !row.get_type().is(capability.row_type.bind(py)) {
        return Ok(None);
    }
    let mut referent = SingleReferent {
        first: core::ptr::null_mut(),
        count: 0,
    };
    // SAFETY: row is the exact type that owns this slot. The visitor and stack argument stay live
    // for the synchronous call and never retain a borrowed reference after row's Bound lifetime.
    let status = unsafe {
        (capability.traverse)(
            row.as_ptr(),
            collect_single_referent,
            (&raw mut referent).cast(),
        )
    };
    if status != 0 {
        // SAFETY: checking and clearing the thread's active Python error is valid while attached.
        if unsafe { !ffi::PyErr_Occurred().is_null() } {
            // The exact built-in currently cannot reach this branch with our infallible visitor.
            // Treat a future optional-slot failure as a guarded decline, not a new public error.
            unsafe { ffi::PyErr_Clear() };
        }
        return Ok(None);
    }
    if referent.count != 1 || referent.first.is_null() {
        return Ok(None);
    }
    // SAFETY: the live exact mappingproxy retains its sole traversed referent through this scope.
    let mapping = unsafe { Bound::from_borrowed_ptr(py, referent.first) };
    let mapping = match mapping.cast_into_exact::<PyDict>() {
        Ok(mapping) => mapping,
        Err(_) => return Ok(None),
    };
    // PyDict_Copy owns its free-threaded synchronization. Scan the private copy afterwards so a
    // non-string-key row returns to canonical Mapping iteration without nested locks.
    // SAFETY: mapping is a live exact dict; PyDict_Copy returns an owned reference or null.
    let copied = unsafe { ffi::PyDict_Copy(mapping.as_ptr()) };
    // SAFETY: copied is the owned result of PyDict_Copy.
    let copied = unsafe { Bound::from_owned_ptr_or_err(py, copied)? }
        .cast_into_exact::<PyDict>()
        .expect("PyDict_Copy must return an exact dict");
    let exact_strings = with_critical_section(copied.as_any(), || {
        let mut position = 0;
        let mut field = core::ptr::null_mut();
        let mut ignored_value = core::ptr::null_mut();
        while unsafe {
            ffi::PyDict_Next(
                copied.as_ptr(),
                &mut position,
                &mut field,
                &mut ignored_value,
            )
        } != 0
        {
            // SAFETY: PyDict_Next returned a live borrowed key from the locked private snapshot.
            if unsafe { ffi::PyUnicode_CheckExact(field) } == 0 {
                return false;
            }
        }
        true
    });
    if !exact_strings {
        return Ok(None);
    }
    #[cfg(test)]
    record_mapping_proxy_snapshot_hit();
    Ok(Some(copied.unbind()))
}
