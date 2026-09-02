//! Callable and Mapping join kernels for hashable Python keys.

use super::adapters::{
    CallableJoinRecordAdapters, call_one_arg, mapping_proxy_snapshot_capability,
    snapshot_callable_join_record, standard_namedtuple_snapshot_capability,
};
use super::join_exact::{
    checked_join_output_size, copy_join_record, exact_string_contains_dot, exact_string_equal,
    factorized_right_positions, join_allocation_error,
};
use super::*;

mod many;
mod unique;

pub(crate) use many::{
    join_hashable_many_direct_records_v1, join_hashable_many_records_v1,
    join_hashable_many_records_v2,
};
pub(crate) use unique::{
    join_hashable_unique_direct_records_v1, join_hashable_unique_records_v1,
    join_hashable_unique_records_v2,
};

/// Exact row types plus an optional retained MRO identity for mutable nominal Mapping classes.
#[derive(Default)]
struct CallableJoinRecordCapabilities {
    record_types: Vec<Py<PyType>>,
    expected_mros: Vec<Option<Py<PyTuple>>>,
    mro_getter: Option<CallableJoinMroGetter>,
}

struct CallableJoinMroGetter {
    descriptor: Py<PyAny>,
    get: ffi::descrgetfunc,
}

impl CallableJoinMroGetter {
    /// Retain the built-in ``type.__mro__`` descriptor and its non-dispatching getter slot.
    fn new(py: Python<'_>) -> PyResult<Option<Self>> {
        // The process-lifetime built-in type and its immutable dictionary use only canonical
        // C slots. Resolve that exact descriptor once, then bypass every supplied metaclass.
        // SAFETY: PyType_Type is a process-lifetime built-in object.
        let type_object = unsafe {
            Bound::<PyAny>::from_borrowed_ptr(
                py,
                (&raw mut ffi::PyType_Type).cast::<ffi::PyObject>(),
            )
        };
        let namespace = type_object.getattr("__dict__")?;
        let descriptor = namespace.get_item("__mro__")?;
        // SAFETY: descriptor is retained by namespace and PyType_GetSlot only reads its exact
        // built-in type. A null optional slot makes this specialization unavailable.
        let slot =
            unsafe { ffi::PyType_GetSlot(ffi::Py_TYPE(descriptor.as_ptr()), ffi::Py_tp_descr_get) };
        if slot.is_null() {
            if unsafe { !ffi::PyErr_Occurred().is_null() } {
                unsafe { ffi::PyErr_Clear() };
            }
            return Ok(None);
        }
        // SAFETY: CPython returned this pointer specifically for Py_tp_descr_get.
        let get =
            unsafe { core::mem::transmute::<*mut core::ffi::c_void, ffi::descrgetfunc>(slot) };
        Ok(Some(Self {
            descriptor: descriptor.unbind(),
            get,
        }))
    }

    /// Compare one type's current internal MRO without metaclass attribute dispatch.
    fn matches(&self, row_type: &Bound<'_, PyType>, expected_mro: &Bound<'_, PyTuple>) -> bool {
        let py = row_type.py();
        // SAFETY: descriptor is the retained built-in type.__mro__ getset descriptor. Calling
        // its exact descr_get slot directly bypasses arbitrary metaclass namespaces and returns
        // a new owned reference or sets a Python error.
        let actual = unsafe {
            (self.get)(
                self.descriptor.bind(py).as_ptr(),
                row_type.as_ptr(),
                ffi::Py_TYPE(row_type.as_ptr()).cast::<ffi::PyObject>(),
            )
        };
        if actual.is_null() {
            if unsafe { !ffi::PyErr_Occurred().is_null() } {
                unsafe { ffi::PyErr_Clear() };
            }
            return false;
        }
        // SAFETY: descr_get returned a new owned reference proven non-null above.
        unsafe { Bound::<PyAny>::from_owned_ptr(py, actual) }.is(expected_mro.as_any())
    }
}

/// The exact token shape that admits one live row.
#[derive(Clone, Copy)]
enum CallableJoinRowTypeCapability {
    ExactType,
    StableMro,
}

/// Match one live row using exact C-level type identity and any retained MRO proof.
fn callable_join_row_type_capability(
    row: *mut ffi::PyObject,
    capabilities: &CallableJoinRecordCapabilities,
    py: Python<'_>,
) -> Option<CallableJoinRowTypeCapability> {
    // SAFETY: callers retain row either directly or through an exact container during this check.
    // Py_TYPE is a non-dispatching pointer read.
    let row_type = unsafe { ffi::Py_TYPE(row) };
    capabilities
        .record_types
        .iter()
        .zip(&capabilities.expected_mros)
        .find_map(|(expected_type, expected_mro)| {
            if expected_type.bind(py).as_type_ptr() != row_type {
                return None;
            }
            let Some(expected_mro) = expected_mro else {
                return Some(CallableJoinRowTypeCapability::ExactType);
            };
            capabilities
                .mro_getter
                .as_ref()
                .is_some_and(|getter| getter.matches(expected_type.bind(py), expected_mro.bind(py)))
                .then_some(CallableJoinRowTypeCapability::StableMro)
        })
}

/// Test one live row using exact C-level type identity and any retained MRO proof.
fn callable_join_row_type_allowed<const ALLOW_EXACT_DICT: bool>(
    row: *mut ffi::PyObject,
    capabilities: &CallableJoinRecordCapabilities,
    py: Python<'_>,
) -> bool {
    // SAFETY: row is retained as documented by callable_join_row_type_capability.
    (ALLOW_EXACT_DICT && unsafe { ffi::PyDict_CheckExact(row) != 0 })
        || callable_join_row_type_capability(row, capabilities, py).is_some()
}

/// Route a callback-replaced live row through the canonical adapter after preflight ownership.
#[inline(always)]
fn callable_join_uses_fallback(
    row: &Bound<'_, PyAny>,
    record_adapters: &CallableJoinRecordAdapters,
    capabilities: &CallableJoinRecordCapabilities,
) -> bool {
    record_adapters.fallback.is_some()
        && !callable_join_row_type_allowed::<true>(row.as_ptr(), capabilities, row.py())
}

/// Parse legacy type tokens or `(type, exact_mro)` capabilities without invoking user code.
fn callable_join_record_type_tokens(
    value: &Bound<'_, PyAny>,
) -> PyResult<Option<CallableJoinRecordCapabilities>> {
    let tokens = match value.cast_exact::<PyTuple>() {
        Ok(tokens) => tokens,
        Err(_) => return Ok(None),
    };
    let mut record_types = Vec::new();
    let mut expected_mros = Vec::new();
    let mut mro_getter = None;
    record_types
        .try_reserve_exact(tokens.len())
        .map_err(join_allocation_error)?;
    expected_mros
        .try_reserve_exact(tokens.len())
        .map_err(join_allocation_error)?;
    for token in tokens.iter() {
        if let Ok(row_type) = token.clone().cast_into::<PyType>() {
            record_types.push(row_type.unbind());
            expected_mros.push(None);
            continue;
        }
        let pair = match token.cast_exact::<PyTuple>() {
            Ok(pair) if pair.len() == 2 => pair,
            Ok(_) | Err(_) => return Ok(None),
        };
        let row_type = match pair.get_item(0)?.cast_into::<PyType>() {
            Ok(row_type) => row_type,
            Err(_) => return Ok(None),
        };
        let expected_mro = match pair.get_item(1)?.cast_into_exact::<PyTuple>() {
            Ok(expected_mro) => expected_mro,
            Err(_) => return Ok(None),
        };
        if mro_getter.is_none() {
            mro_getter = CallableJoinMroGetter::new(value.py())?;
            if mro_getter.is_none() {
                return Ok(None);
            }
        }
        if !mro_getter
            .as_ref()
            .is_some_and(|getter| getter.matches(&row_type, &expected_mro))
        {
            return Ok(None);
        }
        record_types.push(row_type.unbind());
        expected_mros.push(Some(expected_mro.unbind()));
    }
    Ok(Some(CallableJoinRecordCapabilities {
        record_types,
        expected_mros,
        mro_getter,
    }))
}

/// Canonical callbacks carried only by the private direct-field capability envelope.
struct CallableJoinDirectFallback {
    record_adapter: Py<PyAny>,
    left_selector: Py<PyAny>,
    right_selector: Py<PyAny>,
}

struct CallableJoinDirectCapabilities {
    record_capabilities: CallableJoinRecordCapabilities,
    fallback: Option<CallableJoinDirectFallback>,
}

/// Parse legacy exact type tokens or `(tokens, record_adapter, left_selector, right_selector)`.
fn callable_join_direct_capabilities(
    value: &Bound<'_, PyAny>,
) -> PyResult<Option<CallableJoinDirectCapabilities>> {
    if let Some(tokens) = callable_join_record_type_tokens(value)? {
        return Ok(Some(CallableJoinDirectCapabilities {
            record_capabilities: tokens,
            fallback: None,
        }));
    }
    let capabilities = match value.cast_exact::<PyTuple>() {
        Ok(capabilities) if capabilities.len() == 4 => capabilities,
        Ok(_) | Err(_) => return Ok(None),
    };
    let Some(tokens) = callable_join_record_type_tokens(&capabilities.get_item(0)?)? else {
        return Ok(None);
    };
    let record_adapter = capabilities.get_item(1)?;
    let left_selector = capabilities.get_item(2)?;
    let right_selector = capabilities.get_item(3)?;
    if !record_adapter.is_callable()
        || !left_selector.is_callable()
        || !right_selector.is_callable()
    {
        return Ok(None);
    }
    Ok(Some(CallableJoinDirectCapabilities {
        record_capabilities: tokens,
        fallback: Some(CallableJoinDirectFallback {
            record_adapter: record_adapter.unbind(),
            left_selector: left_selector.unbind(),
            right_selector: right_selector.unbind(),
        }),
    }))
}

/// Check the initial record shape without invoking a selector or record protocol.
fn preflight_callable_join_source<const ALLOW_EXACT_DICT: bool>(
    source: &Bound<'_, PyAny>,
    capabilities: &CallableJoinRecordCapabilities,
) -> PyResult<Option<usize>> {
    if let Ok(rows) = source.cast_exact::<PyList>() {
        return with_critical_section(source, || {
            let row_count = rows.len();
            for index in 0..row_count {
                // SAFETY: the exact list stays locked and index is below its locked length.
                // No row reference escapes this callback, so preflight does not need the
                // million-row strong-reference snapshot used by speculative row kernels.
                let row = unsafe { ffi::PyList_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) };
                if row.is_null() {
                    return Err(PyErr::fetch(source.py()));
                }
                // SAFETY: row remains live under the source list's critical section.
                if !callable_join_row_type_allowed::<ALLOW_EXACT_DICT>(
                    row,
                    capabilities,
                    source.py(),
                ) {
                    return Ok(None);
                }
            }
            Ok(Some(row_count))
        });
    }
    let rows = match source.cast_exact::<PyTuple>() {
        Ok(rows) => rows,
        Err(_) => return Ok(None),
    };
    for index in 0..rows.len() {
        // SAFETY: exact tuples are immutable and index is within their fixed length.
        let row = unsafe { ffi::PyTuple_GetItem(source.as_ptr(), index as ffi::Py_ssize_t) };
        if row.is_null() {
            return Err(PyErr::fetch(source.py()));
        }
        // SAFETY: row is a live tuple item for the duration of this check.
        if !callable_join_row_type_allowed::<ALLOW_EXACT_DICT>(row, capabilities, source.py()) {
            return Ok(None);
        }
    }
    Ok(Some(rows.len()))
}

/// Return the size of an exact eager source when every live row has a canonical fallback.
#[inline]
pub(super) fn exact_callable_join_source_len(source: &Bound<'_, PyAny>) -> Option<usize> {
    if let Ok(rows) = source.cast_exact::<PyList>() {
        return Some(rows.len());
    }
    source.cast_exact::<PyTuple>().ok().map(|rows| rows.len())
}

/// Append newly discovered right fields using Python set hashing and equality semantics.
fn remember_callable_join_columns(
    record: &Py<PyDict>,
    columns: &mut Vec<Py<PyAny>>,
    seen: &Bound<'_, PySet>,
    can_collapse_right_lookup: &mut bool,
    py: Python<'_>,
) -> PyResult<()> {
    for (name, _value) in record.bind(py).iter() {
        #[cfg(test)]
        record_callable_right_schema_full_field_probe();
        if *can_collapse_right_lookup
            // SAFETY: name is retained by the locked exact-dict iterator for this check.
            && unsafe { ffi::PyUnicode_CheckExact(name.as_ptr()) } == 0
        {
            *can_collapse_right_lookup = false;
        }
        if !seen.contains(&name)? {
            seen.add(&name)?;
            columns.try_reserve(1).map_err(join_allocation_error)?;
            columns.push(name.unbind());
        }
    }
    Ok(())
}

/// Per-query admission for skipping repeated Python set probes on one exact right layout.
#[derive(Clone, Copy)]
enum CallableRightSchemaMode {
    Unique,
    Many,
}

enum CallableRightSchemaCache {
    Disabled,
    ObserveFirst {
        right_count: usize,
        mode: CallableRightSchemaMode,
    },
    ActiveIdentity {
        field_count: usize,
    },
    // This state proves equal exact-string values and order, not pointer identity. Merge
    // specializations must never treat it as an identity-homogeneous layout.
    ActiveValue {
        field_count: usize,
    },
    // Keep the wide many candidate only while every row passes stricter pointer-identity checks.
    // Its first equal-but-distinct layout transitions permanently to ActiveValue.
    ActiveValueIdentityCandidate {
        field_count: usize,
    },
}

impl CallableRightSchemaCache {
    fn new(right_count: usize, mode: CallableRightSchemaMode) -> Self {
        if right_count >= CALLABLE_RIGHT_SCHEMA_CACHE_MIN_ROWS {
            Self::ObserveFirst { right_count, mode }
        } else {
            Self::Disabled
        }
    }

    fn identity_homogeneous_field_count(&self) -> Option<usize> {
        match self {
            Self::ActiveIdentity { field_count }
            | Self::ActiveValueIdentityCandidate { field_count } => Some(*field_count),
            Self::Disabled | Self::ObserveFirst { .. } | Self::ActiveValue { .. } => None,
        }
    }
}

/// Compare a private exact-dict snapshot with the first row's exact-string layout.
#[inline(always)]
fn callable_join_same_right_shape<const ALLOW_VALUE_MATCH: bool>(
    record: &Py<PyDict>,
    columns: &[Py<PyAny>],
    field_count: usize,
    py: Python<'_>,
) -> PyResult<bool> {
    if field_count > columns.len() {
        return Ok(false);
    }
    let record = record.bind(py);
    with_critical_section(record.as_any(), || {
        let record_ptr = record.as_ptr();
        // SAFETY: record is a live exact dict protected by its critical section.
        let actual_count = unsafe { ffi::PyDict_Size(record_ptr) };
        if actual_count < 0 {
            return Err(PyErr::fetch(py));
        }
        if actual_count as usize != field_count {
            return Ok(false);
        }

        let mut position = 0;
        let mut field = core::ptr::null_mut();
        let mut ignored_value = core::ptr::null_mut();
        #[cfg(test)]
        let mut matched_by_value = false;
        for expected in &columns[..field_count] {
            // SAFETY: the locked size bounds successful iteration; PyDict_Next cannot dispatch.
            if unsafe {
                ffi::PyDict_Next(record_ptr, &mut position, &mut field, &mut ignored_value)
            } == 0
            {
                return Ok(false);
            }
            let expected = expected.bind(py).as_ptr();
            if field == expected {
                continue;
            }
            if !ALLOW_VALUE_MATCH
                // SAFETY: `field` is retained by the locked exact-dict iterator for this check.
                || unsafe { ffi::PyUnicode_CheckExact(field) } == 0
            {
                return Ok(false);
            }
            // Active admission already proved every first-row column is an exact string.
            // The current field belongs to the locked exact dict, the expected field is retained
            // by `columns`, and exact Unicode objects are immutable.
            if !exact_string_equal(py, field, expected)? {
                return Ok(false);
            }
            #[cfg(test)]
            {
                matched_by_value = true;
            }
        }
        #[cfg(test)]
        if matched_by_value {
            record_callable_right_schema_value_cache_hit();
        } else {
            record_callable_right_schema_identity_cache_hit();
        }
        Ok(true)
    })
}

/// Preserve the original field-discovery loop unless a fully guarded schema cache hits.
#[inline(always)]
fn remember_callable_join_columns_with_cache(
    record: &Py<PyDict>,
    columns: &mut Vec<Py<PyAny>>,
    seen: &Bound<'_, PySet>,
    can_collapse_right_lookup: &mut bool,
    cache: &mut CallableRightSchemaCache,
    py: Python<'_>,
) -> PyResult<()> {
    match cache {
        CallableRightSchemaCache::Disabled => {
            remember_callable_join_columns(record, columns, seen, can_collapse_right_lookup, py)
        }
        CallableRightSchemaCache::ObserveFirst { right_count, mode } => {
            remember_callable_join_columns(record, columns, seen, can_collapse_right_lookup, py)?;
            let field_count = record.bind(py).len();
            if *can_collapse_right_lookup
                && field_count >= CALLABLE_RIGHT_SCHEMA_CACHE_MIN_FIELDS
                && columns.len() == field_count
            {
                let allow_value_match = matches!(*mode, CallableRightSchemaMode::Many)
                    || *right_count >= CALLABLE_RIGHT_SCHEMA_VALUE_CACHE_MIN_ROWS
                    || field_count >= CALLABLE_RIGHT_SCHEMA_VALUE_CACHE_MIN_FIELDS;
                *cache = if allow_value_match {
                    let track_identity = field_count >= CALLABLE_JOIN_BULK_MERGE_MIN_FIELDS
                        && matches!(*mode, CallableRightSchemaMode::Many);
                    if track_identity {
                        CallableRightSchemaCache::ActiveValueIdentityCandidate { field_count }
                    } else {
                        CallableRightSchemaCache::ActiveValue { field_count }
                    }
                } else {
                    CallableRightSchemaCache::ActiveIdentity { field_count }
                };
            } else {
                *cache = CallableRightSchemaCache::Disabled;
            }
            Ok(())
        }
        CallableRightSchemaCache::ActiveIdentity { field_count } => {
            if *can_collapse_right_lookup
                && callable_join_same_right_shape::<false>(record, columns, *field_count, py)?
            {
                return Ok(());
            }
            *cache = CallableRightSchemaCache::Disabled;
            remember_callable_join_columns(record, columns, seen, can_collapse_right_lookup, py)
        }
        CallableRightSchemaCache::ActiveValue { field_count } => {
            if *can_collapse_right_lookup
                && callable_join_same_right_shape::<true>(record, columns, *field_count, py)?
            {
                return Ok(());
            }
            *cache = CallableRightSchemaCache::Disabled;
            remember_callable_join_columns(record, columns, seen, can_collapse_right_lookup, py)
        }
        CallableRightSchemaCache::ActiveValueIdentityCandidate { field_count } => {
            if *can_collapse_right_lookup
                && callable_join_same_right_shape::<false>(record, columns, *field_count, py)?
            {
                return Ok(());
            }
            if *can_collapse_right_lookup
                && callable_join_same_right_shape::<true>(record, columns, *field_count, py)?
            {
                *cache = CallableRightSchemaCache::ActiveValue {
                    field_count: *field_count,
                };
                return Ok(());
            }
            *cache = CallableRightSchemaCache::Disabled;
            remember_callable_join_columns(record, columns, seen, can_collapse_right_lookup, py)
        }
    }
}

/// Translate only hash/equality TypeErrors to the public join-key boundary.
fn callable_join_key_error(py: Python<'_>, error: PyErr) -> PyErr {
    if error.is_instance_of::<PyTypeError>(py) {
        PyTypeError::new_err("join keys must be hashable")
    } else {
        error
    }
}

/// Match the public direct-string selector boundary without returning to a Python callback.
fn direct_join_selection_error(field: &Bound<'_, PyAny>, error: PyErr) -> PyResult<PyErr> {
    let py = field.py();
    if !error.is_instance_of::<PyAttributeError>(py)
        && !error.is_instance_of::<PyKeyError>(py)
        && !error.is_instance_of::<PyTypeError>(py)
    {
        return Ok(error);
    }

    let prefix = PyString::new(py, "Could not resolve selector ");
    let middle = PyString::new(py, "; failed at ");
    let representation = field.repr()?;
    // SAFETY: every operand is a live Unicode object. PyUnicode_Concat returns a new owned
    // reference or sets an exception; building inside Python preserves lone surrogate reprs.
    let message = unsafe { ffi::PyUnicode_Concat(prefix.as_ptr(), representation.as_ptr()) };
    let message = unsafe { Bound::from_owned_ptr_or_err(py, message)? };
    let message = unsafe { ffi::PyUnicode_Concat(message.as_ptr(), middle.as_ptr()) };
    let message = unsafe { Bound::from_owned_ptr_or_err(py, message)? };
    let message = unsafe { ffi::PyUnicode_Concat(message.as_ptr(), representation.as_ptr()) };
    let message = unsafe { Bound::from_owned_ptr_or_err(py, message)? };
    let error_type = PyModule::import(py, "fpstreams.errors")?.getattr("SelectionError")?;
    let translated = PyErr::from_value(error_type.call1((message,))?);
    translated.set_context(py, Some(error.clone_ref(py)));
    translated.set_cause(py, Some(error));
    Ok(translated)
}

/// Select a callable key or a preflighted direct field without a runtime mode branch.
#[inline(always)]
fn select_hashable_join_key<'py, const DIRECT_FIELDS: bool>(
    row: &Bound<'py, PyAny>,
    selector: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    if DIRECT_FIELDS {
        match row.get_item(selector) {
            Ok(key) => Ok(key),
            Err(error) => Err(direct_join_selection_error(selector, error)?),
        }
    } else {
        call_one_arg(selector, row)
    }
}

/// Restore canonical selection when a snapshot callback replaces a preflighted direct row.
#[inline(always)]
fn select_live_hashable_join_key<'py, const DIRECT_FIELDS: bool>(
    row: &Bound<'py, PyAny>,
    selector: &Bound<'py, PyAny>,
    fallback_selector: Option<&Bound<'py, PyAny>>,
    use_fallback: bool,
) -> PyResult<Bound<'py, PyAny>> {
    if DIRECT_FIELDS && use_fallback {
        return select_hashable_join_key::<false>(
            row,
            fallback_selector.expect("direct fallback snapshots require a canonical selector"),
        );
    }
    select_hashable_join_key::<DIRECT_FIELDS>(row, selector)
}

/// Build the exact duplicate-cardinality error after the canonical first index lookup.
fn callable_join_duplicate_error(key: &Bound<'_, PyAny>) -> PyResult<PyErr> {
    let py = key.py();
    let prefix = PyString::new(
        py,
        "join validate='m:1' requires unique right keys; found duplicate ",
    );
    let representation = key.repr()?;
    // SAFETY: both operands are live Unicode objects. Keeping the message inside Python avoids
    // lossy UTF-8 conversion of a valid repr containing lone surrogate code points.
    let message = unsafe { ffi::PyUnicode_Concat(prefix.as_ptr(), representation.as_ptr()) };
    let message = unsafe { Bound::from_owned_ptr_or_err(py, message)? };
    Ok(PyErr::from_value(
        py.get_type::<PyValueError>().call1((message,))?,
    ))
}

/// Format one generated right target with the same default formatting used by an f-string.
fn concatenate_callable_join_strings(
    left: &Bound<'_, PyAny>,
    suffix: &Bound<'_, PyString>,
) -> PyResult<Py<PyAny>> {
    let py = left.py();
    // SAFETY: both values are exact Unicode objects. PyUnicode_GetLength sets an exception on
    // failure and otherwise has no observable Python dispatch.
    let left_length = unsafe { ffi::PyUnicode_GetLength(left.as_ptr()) };
    if left_length < 0 {
        return Err(PyErr::fetch(py));
    }
    let suffix_length = unsafe { ffi::PyUnicode_GetLength(suffix.as_ptr()) };
    if suffix_length < 0 {
        return Err(PyErr::fetch(py));
    }

    let target = if (left_length == 0) != (suffix_length == 0) {
        // PyUnicode_Concat is permitted to return its non-empty operand when the other operand
        // is empty. BUILD_STRING for a two-part f-string instead mints a distinct result. A
        // two-element join preserves that identity rule on this rare boundary.
        let separator = PyString::new(py, "");
        let parts = PyTuple::new(py, [left, suffix.as_any()])?;
        // SAFETY: separator is exact Unicode and parts is a live two-element exact tuple.
        unsafe { ffi::PyUnicode_Join(separator.as_ptr(), parts.as_ptr()) }
    } else {
        // SAFETY: both operands are live exact Unicode objects.
        unsafe { ffi::PyUnicode_Concat(left.as_ptr(), suffix.as_ptr()) }
    };
    // SAFETY: both Unicode constructors return a new owned reference or set an exception.
    Ok(unsafe { Bound::from_owned_ptr_or_err(py, target)? }.unbind())
}

/// Format one generated right target with the same default formatting used by an f-string.
fn callable_join_suffix_target(
    name: &Bound<'_, PyAny>,
    suffix: &Bound<'_, PyString>,
) -> PyResult<Py<PyAny>> {
    let py = name.py();
    let empty = PyString::new(py, "");
    // SAFETY: PyObject_Format and PyUnicode_Concat return owned references or set an error.
    let formatted = unsafe { ffi::PyObject_Format(name.as_ptr(), empty.as_ptr()) };
    let formatted = unsafe { Bound::from_owned_ptr_or_err(py, formatted)? };
    concatenate_callable_join_strings(&formatted, suffix)
}

/// Raise the package's canonical collision error without making it part of the hot ABI.
fn callable_join_duplicate_target_error(
    name: &Bound<'_, PyAny>,
    target: &Bound<'_, PyAny>,
) -> PyResult<PyErr> {
    let py = name.py();
    let prefix = PyString::new(py, "join maps right column ");
    let middle = PyString::new(py, " to existing output column ");
    let name = name.repr()?;
    let target = target.repr()?;
    // SAFETY: every operand is a live Unicode object and each concatenation either returns an
    // owned reference or sets a Python exception. Avoiding Rust text conversion preserves the
    // exact repr, including lone surrogate code points.
    let message = unsafe { ffi::PyUnicode_Concat(prefix.as_ptr(), name.as_ptr()) };
    let message = unsafe { Bound::from_owned_ptr_or_err(py, message)? };
    let message = unsafe { ffi::PyUnicode_Concat(message.as_ptr(), middle.as_ptr()) };
    let message = unsafe { Bound::from_owned_ptr_or_err(py, message)? };
    let message = unsafe { ffi::PyUnicode_Concat(message.as_ptr(), target.as_ptr()) };
    let message = unsafe { Bound::from_owned_ptr_or_err(py, message)? };
    let error_type = PyModule::import(py, "fpstreams.errors")?.getattr("DuplicateKeyError")?;
    Ok(PyErr::from_value(error_type.call1((message,))?))
}

/// One per-left-row right-field plan, retaining first-seen right name objects.
type CallableJoinTargets = Vec<(Py<PyAny>, Py<PyAny>, bool)>;

/// A reusable target operation for one proven exact-string left field layout.
enum CallableJoinTargetOperation {
    /// Preserve a shared right field already present on the left.
    Shared,
    /// Insert under the first-seen right field object itself.
    Fixed,
    /// Mint the suffixed exact string separately for every output row.
    Suffixed,
}

struct CallableJoinPlanEntry {
    name: Py<PyAny>,
    operation: CallableJoinTargetOperation,
}

/// The overwhelmingly common homogeneous-row plan, guarded by key identity and order.
struct CallableJoinTargetCache {
    left_shape: Vec<Py<PyAny>>,
    plan: Vec<CallableJoinPlanEntry>,
    bulk_merge_suffix_prefix: Option<usize>,
}

/// Admit one order-preserving PyDict_Merge plan.
///
/// A suffixed source name must collide with the owned left snapshot. Inserting a short strict
/// prefix of suffixed targets first lets override=0 skip their originals while the bulk merge
/// appends every remaining field in canonical right order. A prefix wider than one quarter of
/// the plan or any later suffixed operation is rejected because manual collision work dominates.
fn callable_join_bulk_merge_suffix_prefix(plan: &[CallableJoinPlanEntry]) -> Option<usize> {
    if plan.len() < CALLABLE_JOIN_BULK_MERGE_MIN_FIELDS {
        return None;
    }
    let suffix_prefix = plan
        .iter()
        .take_while(|entry| matches!(entry.operation, CallableJoinTargetOperation::Suffixed))
        .count();
    if suffix_prefix > plan.len() / 4
        || suffix_prefix == plan.len()
        || plan[suffix_prefix..]
            .iter()
            .any(|entry| matches!(entry.operation, CallableJoinTargetOperation::Suffixed))
    {
        return None;
    }
    Some(suffix_prefix)
}

/// Compare one private exact dict with a cached exact-string layout without Python hashing.
fn callable_join_same_left_shape(
    left: &Py<PyDict>,
    cached: &CallableJoinTargetCache,
    py: Python<'_>,
) -> PyResult<bool> {
    let left = left.bind(py);
    with_critical_section(left.as_any(), || {
        let left_ptr = left.as_ptr();
        // SAFETY: left is a live exact dict protected by its critical section.
        let field_count = unsafe { ffi::PyDict_Size(left_ptr) };
        if field_count < 0 {
            return Err(PyErr::fetch(py));
        }
        if field_count as usize != cached.left_shape.len() {
            return Ok(false);
        }

        let mut position = 0;
        let mut field = core::ptr::null_mut();
        let mut ignored_value = core::ptr::null_mut();
        for expected in &cached.left_shape {
            // SAFETY: the dict is protected by its critical section, and PyDict_Next neither
            // hashes nor dispatches to user code.
            if unsafe { ffi::PyDict_Next(left_ptr, &mut position, &mut field, &mut ignored_value) }
                == 0
            {
                return Ok(false);
            }
            if field != expected.bind(py).as_ptr() {
                return Ok(false);
            }
        }
        Ok(true)
    })
}

/// Resolve collision targets only after a match (or a left-join miss) is known.
fn callable_join_targets(
    left: &Py<PyDict>,
    right_columns: &[Py<PyAny>],
    suffix: &Bound<'_, PyString>,
    shared_names: &Bound<'_, PyFrozenSet>,
    py: Python<'_>,
) -> PyResult<(CallableJoinTargets, Option<CallableJoinTargetCache>)> {
    // Python's canonical `set(left_dict)` reuses the exact dict's cached key hashes. Building an
    // empty set and adding each key would re-run protocol-sensitive `__hash__` callbacks.
    // SAFETY: left is a live exact dict and PySet_New returns a new owned exact set or sets an
    // exception. It performs the same dict-specialized construction as `set(left)`.
    let left_names = unsafe { ffi::PySet_New(left.bind(py).as_ptr()) };
    let left_names = unsafe { Bound::from_owned_ptr_or_err(py, left_names)? }
        .cast_into_exact::<PySet>()
        .expect("PySet_New must return an exact set");
    let mut cacheable = right_columns
        .iter()
        // SAFETY: every column is a live Python object retained by right_columns.
        .all(|name| unsafe { ffi::PyUnicode_CheckExact(name.bind(py).as_ptr()) } != 0);
    let mut left_shape = Vec::new();
    if cacheable {
        let field_count = left.bind(py).len();
        left_shape
            .try_reserve_exact(field_count)
            .map_err(join_allocation_error)?;
    }
    for (name, _value) in left.bind(py).iter() {
        if cacheable {
            // Only exact strings can bypass later Python set hashing/equality observably.
            if unsafe { ffi::PyUnicode_CheckExact(name.as_ptr()) } != 0 {
                left_shape.push(name.unbind());
            } else {
                cacheable = false;
                left_shape.clear();
            }
        }
    }
    let used = left_names
        .call_method0("copy")?
        .cast_into_exact::<PySet>()?;
    let mut targets = Vec::new();
    targets
        .try_reserve_exact(right_columns.len())
        .map_err(join_allocation_error)?;
    let mut plan = Vec::new();
    if cacheable {
        plan.try_reserve_exact(right_columns.len())
            .map_err(join_allocation_error)?;
    }
    for name in right_columns {
        let name = name.bind(py);
        let shared = shared_names.contains(name)?;
        if shared {
            targets.push((name.clone().unbind(), name.clone().unbind(), true));
            if cacheable {
                plan.push(CallableJoinPlanEntry {
                    name: name.clone().unbind(),
                    operation: CallableJoinTargetOperation::Shared,
                });
            }
            continue;
        }
        let collides = left_names.contains(name)?;
        let target = if collides {
            callable_join_suffix_target(name, suffix)?
        } else {
            name.clone().unbind()
        };
        if used.contains(target.bind(py))? {
            return Err(callable_join_duplicate_target_error(name, target.bind(py))?);
        }
        used.add(target.bind(py))?;
        targets.push((name.clone().unbind(), target, false));
        if cacheable {
            plan.push(CallableJoinPlanEntry {
                name: name.clone().unbind(),
                operation: if collides {
                    CallableJoinTargetOperation::Suffixed
                } else {
                    CallableJoinTargetOperation::Fixed
                },
            });
        }
    }
    let cache = cacheable.then(|| {
        let bulk_merge_suffix_prefix = callable_join_bulk_merge_suffix_prefix(&plan);
        CallableJoinTargetCache {
            left_shape,
            plan,
            bulk_merge_suffix_prefix,
        }
    });
    Ok((targets, cache))
}

/// Merge a cached many-right plan while retaining one suffix target per left row.
fn merge_callable_join_targets_bulk_match(
    output: &Py<PyDict>,
    right: &Bound<'_, PyDict>,
    targets: &CallableJoinTargets,
    suffix_prefix: usize,
    py: Python<'_>,
) -> PyResult<()> {
    let output = output.bind(py);
    for (name, target, shared) in &targets[..suffix_prefix] {
        debug_assert!(!shared);
        let value = right
            .get_item(name.bind(py))?
            .expect("identity-homogeneous right rows retain every planned field");
        output.set_item(target.bind(py), value)?;
    }
    // SAFETY: both operands are private exact dictionaries. Right-layout identity proves every
    // iterated key is the canonical plan object. Exact-string left-layout identity proves each
    // manually suffixed original exists, so override=0 skips it. The strict-prefix guard keeps
    // output order and targets were minted once per left row, as in the canonical loop.
    if unsafe { ffi::PyDict_Merge(output.as_ptr(), right.as_ptr(), 0) } != 0 {
        return Err(PyErr::fetch(py));
    }
    #[cfg(test)]
    record_callable_join_bulk_merge_hit();
    Ok(())
}

/// Merge through a proven homogeneous exact-string plan without rebuilding Python sets.
fn merge_callable_join_plan_match(
    output: &Py<PyDict>,
    right: &Bound<'_, PyDict>,
    cached: &CallableJoinTargetCache,
    suffix: &Bound<'_, PyString>,
    can_collapse_right_lookup: bool,
    py: Python<'_>,
) -> PyResult<()> {
    let output = output.bind(py);
    for entry in &cached.plan {
        let name = entry.name.bind(py);
        if matches!(entry.operation, CallableJoinTargetOperation::Shared)
            && output.contains(name)?
        {
            continue;
        }
        let value = if can_collapse_right_lookup {
            // Every name and every key in these private snapshots is an immutable exact string.
            // No selector can mutate the snapshots, so the canonical membership probe cannot be
            // observed and one lookup preserves its result.
            let Some(value) = right.get_item(name)? else {
                continue;
            };
            value
        } else {
            if !right.contains(name)? {
                continue;
            }
            // Canonical merging performs `name in right` followed by `right[name]`. Keep the
            // second subscription observable and let callback-driven removal raise KeyError.
            right.as_any().get_item(name)?
        };
        match entry.operation {
            CallableJoinTargetOperation::Suffixed => {
                // Cache admission proves name is an exact string, so its default formatting is
                // itself and cannot invoke user code. The helper also preserves the two-part
                // f-string identity rule when exactly one operand is empty.
                let target = concatenate_callable_join_strings(name, suffix)?;
                output.set_item(target.bind(py), value)?;
            }
            CallableJoinTargetOperation::Shared | CallableJoinTargetOperation::Fixed => {
                output.set_item(name, value)?;
            }
        }
    }
    Ok(())
}

/// Resolve one cached plan into targets once for every many-match left row.
fn callable_join_targets_from_plan(
    cached: &CallableJoinTargetCache,
    suffix: &Bound<'_, PyString>,
    py: Python<'_>,
) -> PyResult<CallableJoinTargets> {
    let mut targets = Vec::new();
    targets
        .try_reserve_exact(cached.plan.len())
        .map_err(join_allocation_error)?;
    for entry in &cached.plan {
        let name = entry.name.bind(py);
        let (target, shared) = match entry.operation {
            CallableJoinTargetOperation::Shared => (name.clone().unbind(), true),
            CallableJoinTargetOperation::Fixed => (name.clone().unbind(), false),
            CallableJoinTargetOperation::Suffixed => {
                (concatenate_callable_join_strings(name, suffix)?, false)
            }
        };
        targets.push((name.clone().unbind(), target, shared));
    }
    Ok(targets)
}

/// Fill an unmatched row through a proven homogeneous exact-string plan.
fn merge_callable_join_plan_unmatched(
    output: &Py<PyDict>,
    cached: &CallableJoinTargetCache,
    suffix: &Bound<'_, PyString>,
    py: Python<'_>,
) -> PyResult<()> {
    let output = output.bind(py);
    for entry in &cached.plan {
        match entry.operation {
            CallableJoinTargetOperation::Shared => {}
            CallableJoinTargetOperation::Fixed => {
                output.set_item(entry.name.bind(py), py.None())?;
            }
            CallableJoinTargetOperation::Suffixed => {
                let name = entry.name.bind(py);
                let target = concatenate_callable_join_strings(name, suffix)?;
                output.set_item(target.bind(py), py.None())?;
            }
        }
    }
    Ok(())
}

/// Merge one unique-right match into the already-owned left snapshot.
fn merge_callable_join_match(
    output: &Py<PyDict>,
    right: &Bound<'_, PyDict>,
    targets: &CallableJoinTargets,
    shared_names: &Bound<'_, PyFrozenSet>,
    can_collapse_right_lookup: bool,
    py: Python<'_>,
) -> PyResult<()> {
    let output = output.bind(py);
    for (name, target, _shared) in targets {
        let name = name.bind(py);
        let target = target.bind(py);
        // Canonical merging re-evaluates `name in shared_names` for every output instead of
        // reusing the result that selected the collision target. Preserve observable hash/equality
        // callbacks and any callback-driven membership changes here.
        if shared_names.contains(name)? && output.contains(target)? {
            continue;
        }
        if can_collapse_right_lookup {
            if let Some(value) = right.get_item(name)? {
                output.set_item(target, value)?;
            }
        } else if right.contains(name)? {
            // Preserve the canonical second lookup instead of collapsing membership and
            // subscription into one PyDict_GetItemRef call.
            let value = right.as_any().get_item(name)?;
            output.set_item(target, value)?;
        }
    }
    Ok(())
}

/// Fill every non-shared right column on one unmatched left output.
fn merge_callable_join_unmatched(
    output: &Py<PyDict>,
    targets: &CallableJoinTargets,
    shared_names: &Bound<'_, PyFrozenSet>,
    py: Python<'_>,
) -> PyResult<()> {
    let output = output.bind(py);
    for (name, target, _shared) in targets {
        // Match the canonical left-join miss loop's fresh `name not in shared_names` probe.
        if !shared_names.contains(name.bind(py))? {
            output.set_item(target.bind(py), py.None())?;
        }
    }
    Ok(())
}
