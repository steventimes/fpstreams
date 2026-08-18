//! Shared native-kernel errors and compensated statistics for one-pass aggregate terminals.

use pyo3::exceptions::{PyOverflowError, PyValueError, PyZeroDivisionError};
use pyo3::prelude::*;

#[derive(Debug)]
pub(crate) enum KernelError {
    DivisionByZero,
    InvalidProgram(&'static str),
    Overflow,
}

#[derive(Default)]
pub(crate) struct OnlineStatistics {
    count: u64,
    total: f64,
    compensation: f64,
    rolling_mean: f64,
    squared_deviations: f64,
}

impl OnlineStatistics {
    pub(crate) fn accept(&mut self, value: f64) -> Result<(), KernelError> {
        self.count = self.count.checked_add(1).ok_or(KernelError::Overflow)?;

        let combined = self.total + value;
        if self.total.is_finite() && value.is_finite() && combined.is_finite() {
            self.compensation += if self.total.abs() >= value.abs() {
                self.total - combined + value
            } else {
                value - combined + self.total
            };
        } else {
            self.compensation = 0.0;
        }
        self.total = combined;

        let delta = value - self.rolling_mean;
        self.rolling_mean += delta / (self.count as f64);
        self.squared_deviations += delta * (value - self.rolling_mean);
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> (u64, f64, f64) {
        let mean = if self.count == 0 {
            0.0
        } else {
            (self.total + self.compensation) / (self.count as f64)
        };
        (self.count, mean, self.squared_deviations)
    }

    pub(crate) fn sum(&self) -> f64 {
        self.total + self.compensation
    }
}

pub(crate) fn kernel_error(error: KernelError) -> PyErr {
    match error {
        KernelError::DivisionByZero => PyZeroDivisionError::new_err("integer division by zero"),
        KernelError::InvalidProgram(message) => PyValueError::new_err(message),
        KernelError::Overflow => PyOverflowError::new_err("native i64 expression overflowed"),
    }
}
