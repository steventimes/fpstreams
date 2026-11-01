//! Shifted rolling moments over values converted at the Python numeric boundary.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::VecDeque;

type RollingSnapshot = (usize, Option<f64>, Option<f64>);

#[pyclass(module = "fpstreams._native")]
pub(crate) struct RollingStatistics {
    size: usize,
    ddof: usize,
    values: VecDeque<Option<f64>>,
    count: usize,
    origin: f64,
    mean: f64,
    m2: f64,
    total: f64,
    compensation: f64,
    updates: usize,
}

#[pymethods]
impl RollingStatistics {
    #[new]
    fn new(size: usize, ddof: usize) -> PyResult<Self> {
        if size == 0 {
            return Err(PyValueError::new_err(
                "rolling statistics size must be positive",
            ));
        }
        Ok(Self {
            size,
            ddof,
            values: VecDeque::new(),
            count: 0,
            origin: 0.0,
            mean: 0.0,
            m2: 0.0,
            total: 0.0,
            compensation: 0.0,
            updates: 0,
        })
    }

    fn accept(&mut self, value: Option<f64>) -> RollingSnapshot {
        let mut repair = false;
        if self.values.len() == self.size {
            if let Some(old) = self.values.pop_front().flatten() {
                repair = self.remove(old);
            }
        }
        self.values.push_back(value);
        if let Some(value) = value {
            self.add(value);
        }
        self.updates += 1;
        if repair || self.updates >= self.size || !self.m2.is_finite() {
            self.rebuild();
        }
        let mean = (self.count != 0).then(|| (self.total + self.compensation) / self.count as f64);
        let variance = (self.count > self.ddof).then(|| {
            let value = self.m2 / (self.count - self.ddof) as f64;
            if value < 0.0 { 0.0 } else { value }
        });
        (self.count, mean, variance)
    }
}

impl RollingStatistics {
    fn reset(&mut self) {
        self.count = 0;
        self.origin = 0.0;
        self.mean = 0.0;
        self.m2 = 0.0;
        self.total = 0.0;
        self.compensation = 0.0;
    }

    fn sum(&mut self, value: f64) {
        let combined = self.total + value;
        if self.total.is_finite() && value.is_finite() && combined.is_finite() {
            self.compensation += if self.total.abs() >= value.abs() {
                (self.total - combined) + value
            } else {
                (value - combined) + self.total
            };
        } else {
            self.compensation = 0.0;
        }
        self.total = combined;
    }

    fn add(&mut self, value: f64) {
        self.sum(value);
        if self.count == 0 {
            self.origin = value;
        }
        let shifted = value - self.origin;
        self.count += 1;
        let delta = shifted - self.mean;
        self.mean += delta / self.count as f64;
        self.m2 += delta * (shifted - self.mean);
    }

    fn remove(&mut self, value: f64) -> bool {
        if self.count == 1 {
            self.reset();
            return false;
        }
        self.sum(-value);
        let shifted = value - self.origin;
        let new_mean = self.mean + (self.mean - shifted) / (self.count - 1) as f64;
        let new_m2 = self.m2 - (shifted - self.mean) * (shifted - new_mean);
        let repair = new_m2 < 0.0 || new_m2 < self.m2.abs() * 1e-12;
        self.count -= 1;
        self.mean = new_mean;
        self.m2 = new_m2;
        repair
    }

    fn rebuild(&mut self) {
        self.reset();
        for index in 0..self.values.len() {
            if let Some(value) = self.values[index] {
                self.add(value);
            }
        }
        self.updates = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::RollingStatistics;

    #[test]
    fn missing_values_occupy_window_slots_and_ddof_controls_variance() {
        let mut state = RollingStatistics::new(2, 1).unwrap();
        assert_eq!(state.accept(Some(1.0)), (1, Some(1.0), None));
        assert_eq!(state.accept(Some(3.0)), (2, Some(2.0), Some(2.0)));
        assert_eq!(state.accept(None), (1, Some(3.0), None));
        assert_eq!(state.accept(None), (0, None, None));
        assert_eq!(state.accept(Some(5.0)), (1, Some(5.0), None));
    }

    #[test]
    fn outlier_eviction_repairs_the_small_remaining_variance() {
        let mut state = RollingStatistics::new(3, 0).unwrap();
        for value in [1e15, 1.0, 2.0, 3.0] {
            state.accept(Some(value));
        }
        assert_eq!(state.accept(Some(4.0)), (3, Some(3.0), Some(2.0 / 3.0)));
    }
}
