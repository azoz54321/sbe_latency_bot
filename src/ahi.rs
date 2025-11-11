use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct AhiHistory {
    timeline: Vec<AhiPoint>,
    max_window: Duration,
    latest_value: f64,
}

#[derive(Clone, Copy, Debug)]
struct AhiPoint {
    ts: Instant,
    value: f64,
}

impl AhiHistory {
    pub fn new(max_window: Duration) -> Self {
        Self {
            timeline: Vec::with_capacity(128),
            max_window,
            latest_value: 0.0,
        }
    }

    pub fn record(&mut self, ts: Instant, value: f64) {
        self.latest_value = value;
        self.timeline.push(AhiPoint { ts, value });
        self.prune(ts);
    }

    pub fn latest(&self) -> f64 {
        self.latest_value
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
            acc / count as f64
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

    pub fn reset(&mut self, now: Instant, value: f64) {
        self.timeline.clear();
        self.latest_value = value;
        self.timeline.push(AhiPoint { ts: now, value });
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
