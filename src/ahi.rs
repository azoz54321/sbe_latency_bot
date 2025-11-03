use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::types::Symbol;

#[derive(Debug)]
pub struct AhiAggregator {
    per_symbol: HashMap<Symbol, f64>,
    timeline: Vec<AhiPoint>,
    max_window: Duration,
    latest_value: f64,
}

#[derive(Clone, Copy, Debug)]
struct AhiPoint {
    ts: Instant,
    value: f64,
}

impl AhiAggregator {
    pub fn new(max_window: Duration) -> Self {
        Self {
            per_symbol: HashMap::with_capacity(512),
            timeline: Vec::with_capacity(4 * 60),
            max_window,
            latest_value: 0.0,
        }
    }

    pub fn update(&mut self, ts: Instant, symbol: Symbol, ret_60s: f64) -> f64 {
        let score = normalize_ret(ret_60s);
        if score.is_finite() {
            self.per_symbol.insert(symbol, score);
        }

        let aggregate = if self.per_symbol.is_empty() {
            0.0
        } else {
            self.per_symbol.values().sum::<f64>() / self.per_symbol.len() as f64
        };

        self.latest_value = aggregate;
        self.record(ts, aggregate);
        aggregate
    }

    pub fn average_over(&self, now: Instant, window: Duration) -> f64 {
        if window.is_zero() {
            return self.latest_value;
        }
        let cutoff = now.checked_sub(window).unwrap_or(now);

        let mut acc = 0.0;
        let mut count = 0;
        for point in self.iter_recent(cutoff) {
            acc += point.value;
            count += 1;
        }

        if count == 0 {
            self.latest_value
        } else {
            acc / (count as f64)
        }
    }

    pub fn drop_within(&self, now: Instant, window: Duration) -> f64 {
        let cutoff = now.checked_sub(window).unwrap_or(now);
        let mut max_val = f64::NEG_INFINITY;
        for point in self.iter_recent(cutoff) {
            if point.value > max_val {
                max_val = point.value;
            }
        }

        if max_val.is_finite() {
            (max_val - self.latest_value).max(0.0)
        } else {
            0.0
        }
    }

    fn record(&mut self, ts: Instant, value: f64) {
        self.timeline.push(AhiPoint { ts, value });
        self.prune(ts);
    }

    fn prune(&mut self, now: Instant) {
        let cutoff = now.checked_sub(self.max_window).unwrap_or(now);
        if self.timeline.is_empty() {
            return;
        }

        let first_idx = self
            .timeline
            .iter()
            .position(|point| point.ts >= cutoff)
            .unwrap_or(self.timeline.len());

        if first_idx > 0 {
            self.timeline.drain(0..first_idx);
        }
    }

    fn iter_recent(&self, cutoff: Instant) -> impl Iterator<Item = &AhiPoint> {
        self.timeline
            .iter()
            .rev()
            .take_while(move |point| point.ts >= cutoff)
    }
}

fn normalize_ret(ret_60s: f64) -> f64 {
    let clipped = ret_60s.clamp(-0.2, 0.2);
    clipped * 1_000.0
}
