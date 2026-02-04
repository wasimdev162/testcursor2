use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static ORDER_COUNTER: AtomicUsize = AtomicUsize::new(1);

pub fn now_millis() -> u64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    duration.as_millis() as u64
}

pub fn generate_client_id(prefix: &str) -> String {
    let counter = ORDER_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}-{}-{}", prefix, now_millis(), counter)
}

#[derive(Clone, Debug)]
pub struct RollingWindow {
    capacity: usize,
    values: VecDeque<f64>,
}

impl RollingWindow {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            values: VecDeque::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, value: f64) {
        if self.values.len() == self.capacity {
            self.values.pop_front();
        }
        self.values.push_back(value);
    }

    pub fn mean(&self) -> Option<f64> {
        if self.values.is_empty() {
            None
        } else {
            Some(self.values.iter().sum::<f64>() / self.values.len() as f64)
        }
    }

    pub fn sum(&self) -> f64 {
        self.values.iter().sum()
    }

    pub fn abs_sum(&self) -> f64 {
        self.values.iter().map(|value| value.abs()).sum()
    }

    pub fn last(&self) -> Option<f64> {
        self.values.back().copied()
    }

    pub fn first(&self) -> Option<f64> {
        self.values.front().copied()
    }

    pub fn slope(&self) -> Option<f64> {
        if self.values.len() < 2 {
            None
        } else {
            let first = self.values.front().copied().unwrap_or_default();
            let last = self.values.back().copied().unwrap_or_default();
            Some((last - first) / (self.values.len() as f64 - 1.0))
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }
}

pub fn clamp(value: f64, min: f64, max: f64) -> f64 {
    if value < min {
        min
    } else if value > max {
        max
    } else {
        value
    }
}
