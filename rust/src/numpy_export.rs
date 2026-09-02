//! Stable-ABI scalar snapshots for NumPy's buffer boundary.

use crate::common::extract_i64_container;
use pyo3::exceptions::{PyOverflowError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// Pack an exact built-in integer sequence as native-endian signed i64 bytes.
///
/// Type or range mismatches decline atomically so NumPy can perform the public
/// conversion.  The returned immutable buffer is private and safe for NumPy to
/// copy without retaining references to the source container.
#[pyfunction]
pub(crate) fn pack_i64_exact_sequence_v1(
    py: Python<'_>,
    source: &Bound<'_, PyAny>,
) -> PyResult<Option<Py<PyBytes>>> {
    let values = match extract_i64_container(source) {
        Ok(values) => values,
        Err(error)
            if error.is_instance_of::<PyTypeError>(py)
                || error.is_instance_of::<PyOverflowError>(py) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let byte_len = values
        .len()
        .checked_mul(size_of::<i64>())
        .filter(|length| *length <= isize::MAX as usize)
        .ok_or_else(|| PyOverflowError::new_err("native i64 NumPy export is too large"))?;
    let packed = PyBytes::new_with(py, byte_len, |output| {
        // SAFETY: `values` owns `len` initialized i64 values. Viewing that
        // allocation as bytes for the duration of this copy is valid.
        let input = unsafe { std::slice::from_raw_parts(values.as_ptr().cast::<u8>(), byte_len) };
        output.copy_from_slice(input);
        Ok(())
    })?;
    Ok(Some(packed.unbind()))
}
