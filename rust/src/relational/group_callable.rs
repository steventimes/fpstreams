//! Callable grouping kernels that preserve Python callback semantics.

use super::group_numeric::group_allocation_error;
use super::join_callable::{direct_join_selection_error, preflight_callable_join_source};
use super::join_exact::exact_string_contains_dot;
use super::*;

/// One encounter-ordered callable group backed by a dense Rust slot.
struct CallableCountSumGroup {
    first_key: Py<PyAny>,
    count: usize,
    total: Py<PyAny>,
}

type CallableCountSumRows = Vec<(Py<PyAny>, usize, Py<PyAny>)>;

/// Mirror the Python callable-group PIC without making custom key behavior speculative.
#[derive(Default)]
struct CallableGroupHashWarmup {
    completed_rows: usize,
    candidate_type: *mut ffi::PyTypeObject,
    exact_builtin_candidate: bool,
}

impl CallableGroupHashWarmup {
    /// Return whether this row still needs Python's explicit leading `hash(key)` call.
    #[inline(always)]
    fn requires_explicit_hash<const ELIDE_EXACT_BUILTIN_HASH: bool>(
        &self,
        key: &Bound<'_, PyAny>,
    ) -> bool {
        if !ELIDE_EXACT_BUILTIN_HASH
            || self.completed_rows < CALLABLE_GROUP_HASH_WARMUP_ROWS
            || !self.exact_builtin_candidate
        {
            return true;
        }
        // SAFETY: the candidate pointer is retained only for immortal exact builtin types, and
        // key is live for this comparison. Py_TYPE is a non-dispatching identity read.
        unsafe { ffi::Py_TYPE(key.as_ptr()) != self.candidate_type }
    }

    /// Observe one key only after grouping, selection, and Python addition all succeeded.
    #[inline(always)]
    fn observe_success<const ELIDE_EXACT_BUILTIN_HASH: bool>(&mut self, key: &Bound<'_, PyAny>) {
        if !ELIDE_EXACT_BUILTIN_HASH || self.completed_rows >= CALLABLE_GROUP_HASH_WARMUP_ROWS {
            return;
        }
        // SAFETY: key is a live Python object and reading its exact type cannot invoke user code.
        let key_type = unsafe { ffi::Py_TYPE(key.as_ptr()) };
        if self.completed_rows == 0 {
            self.candidate_type = key_type;
            self.exact_builtin_candidate = exact_builtin_group_key(key);
        } else if key_type != self.candidate_type {
            self.exact_builtin_candidate = false;
        }
        self.completed_rows += 1;
        #[cfg(test)]
        record_callable_group_successful_warmup_row();
    }
}

/// Exact immutable builtin keys have no observable Python-level `__hash__` override.
#[inline(always)]
fn exact_builtin_group_key(key: &Bound<'_, PyAny>) -> bool {
    let key_ptr = key.as_ptr();
    key.is_none()
        // SAFETY: every check is an exact/non-dispatching C-level type identity test.
        || unsafe {
            ffi::PyLong_CheckExact(key_ptr) != 0
                || ffi::PyUnicode_CheckExact(key_ptr) != 0
                || ffi::PyBytes_CheckExact(key_ptr) != 0
                || ffi::PyBool_Check(key_ptr) != 0
                || ffi::PyFloat_CheckExact(key_ptr) != 0
                || ffi::PyComplex_CheckExact(key_ptr) != 0
        }
}

/// Translate only the explicit hash/index lookup boundary used by Python grouping.
fn callable_group_key_error(py: Python<'_>, error: PyErr) -> PyErr {
    if error.is_instance_of::<PyTypeError>(py) {
        let translated = PyTypeError::new_err("group_by keys must be hashable");
        // Match `raise TypeError(...) from None`: retain the active error as context while
        // suppressing its display through an explicit null cause.
        translated.set_context(py, Some(error));
        translated.set_cause(py, None);
        translated
    } else {
        error
    }
}

/// Resolve one field directly on an exact dict, retaining the compiled accessor for live drift.
#[inline(always)]
fn select_callable_group_direct_or_fallback<'py>(
    row: &Bound<'py, PyAny>,
    field: &Bound<'py, PyString>,
    accessor: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    if let Ok(record) = row.cast_exact::<PyDict>() {
        match record.get_item(field) {
            Ok(Some(value)) => Ok(value),
            Ok(None) => {
                let error = PyErr::from_value(row.py().get_type::<PyKeyError>().call1((field,))?);
                Err(direct_join_selection_error(field.as_any(), error)?)
            }
            Err(error) => Err(direct_join_selection_error(field.as_any(), error)?),
        }
    } else {
        accessor.call1((row,))
    }
}

/// Insert or find one Python key while storing only a dense integer code in the dictionary.
fn callable_group_position<const ELIDE_EXACT_BUILTIN_HASH: bool>(
    index: &Bound<'_, PyDict>,
    groups: &mut Vec<CallableCountSumGroup>,
    key: &Bound<'_, PyAny>,
    hash_warmup: &CallableGroupHashWarmup,
) -> PyResult<(usize, bool)> {
    let py = key.py();
    if hash_warmup.requires_explicit_hash::<ELIDE_EXACT_BUILTIN_HASH>(key) {
        #[cfg(test)]
        record_callable_group_explicit_hash();
        key.hash()
            .map_err(|error| callable_group_key_error(py, error))?;
    } else {
        #[cfg(test)]
        record_callable_group_elided_hash();
    }
    let existing = index
        .get_item(key)
        .map_err(|error| callable_group_key_error(py, error))?;
    if let Some(code) = existing {
        return Ok((code.extract::<usize>()?, false));
    }

    groups.try_reserve(1).map_err(group_allocation_error)?;
    let code = groups.len();
    // Keep insertion outside the translated lookup boundary. A second custom __hash__ failure
    // here is likewise outside Python's try/except around hash(key) and groups.get(key).
    index.set_item(key, code)?;
    Ok((code, true))
}

/// Validate the non-observable portion of either callable count/sum ABI.
fn preflight_callable_group<'py>(
    source: &Bound<'py, PyAny>,
    callback: &Bound<'py, PyAny>,
    field: &Bound<'py, PyAny>,
    direct_accessor: &Bound<'py, PyAny>,
) -> PyResult<Option<(usize, Bound<'py, PyString>)>> {
    if !callback.is_callable() || !direct_accessor.is_callable() {
        return Ok(None);
    }
    let field = match field.cast_exact::<PyString>() {
        Ok(field) => field.clone(),
        Err(_) => return Ok(None),
    };
    if exact_string_contains_dot(source.py(), field.as_ptr())? {
        return Ok(None);
    }
    let Some(row_count) = preflight_callable_join_source::<true>(source, &[])? else {
        return Ok(None);
    };
    Ok(Some((row_count, field)))
}

/// Group exact-dict records with an opaque key callback and one direct value field.
fn group_count_sum_callable_key_dict_rows<const ELIDE_EXACT_BUILTIN_HASH: bool>(
    source: &Bound<'_, PyAny>,
    key_selector: &Bound<'_, PyAny>,
    value_field: &Bound<'_, PyAny>,
    value_accessor: &Bound<'_, PyAny>,
) -> PyResult<Option<CallableCountSumRows>> {
    let Some((row_count, value_field)) =
        preflight_callable_group(source, key_selector, value_field, value_accessor)?
    else {
        return Ok(None);
    };

    // Callback ownership transfers here. Exact-list mutation can expose a replacement or appended
    // non-dict row, so every such row stays in this loop and uses the supplied canonical accessor.
    let py = source.py();
    let index = PyDict::new(py);
    let zero = 0_i64.into_pyobject(py)?;
    let mut groups = Vec::new();
    let mut hash_warmup = CallableGroupHashWarmup::default();
    groups
        .try_reserve(row_count.min(MAX_INITIAL_RECORD_DENSE_SLOTS))
        .map_err(group_allocation_error)?;
    let mut iterator = source.try_iter()?;
    for row in &mut iterator {
        let row = row?;
        let key = key_selector.call1((&row,))?;
        let (position, is_new) = callable_group_position::<ELIDE_EXACT_BUILTIN_HASH>(
            &index,
            &mut groups,
            &key,
            &hash_warmup,
        )?;
        let selected =
            select_callable_group_direct_or_fallback(&row, &value_field, value_accessor)?;
        if is_new {
            let total = zero.as_any().add(&selected)?.unbind();
            hash_warmup.observe_success::<ELIDE_EXACT_BUILTIN_HASH>(&key);
            groups.push(CallableCountSumGroup {
                first_key: key.unbind(),
                count: 1,
                total,
            });
        } else {
            let state = &mut groups[position];
            state.count = state
                .count
                .checked_add(1)
                .ok_or_else(|| PyMemoryError::new_err("native group count is too large"))?;
            state.total = state.total.bind(py).add(&selected)?.unbind();
            hash_warmup.observe_success::<ELIDE_EXACT_BUILTIN_HASH>(&key);
        }
    }
    Ok(Some(
        groups
            .into_iter()
            .map(|group| (group.first_key, group.count, group.total))
            .collect(),
    ))
}

/// Group exact-dict records with one direct key field and an opaque value callback.
fn group_count_sum_callable_value_dict_rows<const ELIDE_EXACT_BUILTIN_HASH: bool>(
    source: &Bound<'_, PyAny>,
    key_field: &Bound<'_, PyAny>,
    key_accessor: &Bound<'_, PyAny>,
    value_selector: &Bound<'_, PyAny>,
) -> PyResult<Option<CallableCountSumRows>> {
    let Some((row_count, key_field)) =
        preflight_callable_group(source, value_selector, key_field, key_accessor)?
    else {
        return Ok(None);
    };

    // Callback ownership transfers here. No later shape drift can return None and replay it.
    let py = source.py();
    let index = PyDict::new(py);
    let zero = 0_i64.into_pyobject(py)?;
    let mut groups = Vec::new();
    let mut hash_warmup = CallableGroupHashWarmup::default();
    groups
        .try_reserve(row_count.min(MAX_INITIAL_RECORD_DENSE_SLOTS))
        .map_err(group_allocation_error)?;
    let mut iterator = source.try_iter()?;
    for row in &mut iterator {
        let row = row?;
        let key = select_callable_group_direct_or_fallback(&row, &key_field, key_accessor)?;
        let (position, is_new) = callable_group_position::<ELIDE_EXACT_BUILTIN_HASH>(
            &index,
            &mut groups,
            &key,
            &hash_warmup,
        )?;
        let selected = value_selector.call1((&row,))?;
        if is_new {
            let total = zero.as_any().add(&selected)?.unbind();
            hash_warmup.observe_success::<ELIDE_EXACT_BUILTIN_HASH>(&key);
            groups.push(CallableCountSumGroup {
                first_key: key.unbind(),
                count: 1,
                total,
            });
        } else {
            let state = &mut groups[position];
            state.count = state
                .count
                .checked_add(1)
                .ok_or_else(|| PyMemoryError::new_err("native group count is too large"))?;
            state.total = state.total.bind(py).add(&selected)?.unbind();
            hash_warmup.observe_success::<ELIDE_EXACT_BUILTIN_HASH>(&key);
        }
    }
    Ok(Some(
        groups
            .into_iter()
            .map(|group| (group.first_key, group.count, group.total))
            .collect(),
    ))
}

#[pyfunction]
pub(crate) fn group_count_sum_callable_key_dict_rows_v1(
    source: &Bound<'_, PyAny>,
    key_selector: &Bound<'_, PyAny>,
    value_field: &Bound<'_, PyAny>,
    value_accessor: &Bound<'_, PyAny>,
) -> PyResult<Option<CallableCountSumRows>> {
    group_count_sum_callable_key_dict_rows::<true>(
        source,
        key_selector,
        value_field,
        value_accessor,
    )
}

#[pyfunction]
pub(crate) fn group_count_sum_callable_value_dict_rows_v1(
    source: &Bound<'_, PyAny>,
    key_field: &Bound<'_, PyAny>,
    key_accessor: &Bound<'_, PyAny>,
    value_selector: &Bound<'_, PyAny>,
) -> PyResult<Option<CallableCountSumRows>> {
    group_count_sum_callable_value_dict_rows::<true>(
        source,
        key_field,
        key_accessor,
        value_selector,
    )
}
