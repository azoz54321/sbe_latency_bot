use arc_swap::ArcSwap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::universe::Universe;

#[derive(Default)]
#[allow(dead_code)]
pub struct Rings;

#[derive(Clone)]
#[allow(dead_code)]
pub struct RingsHandle {
    inner: Arc<ArcSwap<Rings>>,
}

#[allow(dead_code)]
impl RingsHandle {
    pub fn new(rings: Rings) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(rings)),
        }
    }

    pub fn get(&self) -> Arc<Rings> {
        self.inner.load_full()
    }

    pub fn reinit_for(&self, universe: &Universe) {
        let new_rings = Arc::new(Rings::for_universe(universe));
        self.inner.store(new_rings);
    }
}

impl Rings {
    pub fn for_universe(_u: &Universe) -> Rings {
        Rings
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TimedValue {
    pub ts: Instant,
    pub value: f64,
}

#[derive(Debug)]
pub struct RingBuffer {
    window: Duration,
    entries: VecDeque<TimedValue>,
}

impl RingBuffer {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            entries: VecDeque::with_capacity(128),
        }
    }

    pub fn push(&mut self, ts: Instant, value: f64) {
        self.entries.push_back(TimedValue { ts, value });
        self.prune(ts);
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn head(&self) -> Option<TimedValue> {
        self.entries.back().copied()
    }

    pub fn value_at_offset(&self, offset: Duration) -> Option<f64> {
        if self.entries.is_empty() {
            return None;
        }
        let latest = self.entries.back()?;
        let target_ts = latest.ts.checked_sub(offset)?;
        self.entries
            .iter()
            .rev()
            .find(|entry| entry.ts <= target_ts)
            .map(|entry| entry.value)
    }

    pub fn prune(&mut self, now: Instant) {
        let cutoff = now.checked_sub(self.window).unwrap_or(now);
        while let Some(entry) = self.entries.front() {
            if entry.ts >= cutoff {
                break;
            }
            self.entries.pop_front();
        }
    }

    pub fn is_saturated(&self) -> bool {
        let Some(oldest) = self.entries.front() else {
            return false;
        };
        let Some(latest) = self.entries.back() else {
            return false;
        };
        latest
            .ts
            .checked_duration_since(oldest.ts)
            .map(|elapsed| elapsed >= self.window)
            .unwrap_or(false)
    }
}

#[derive(Debug)]
pub struct SymbolRings {
    pub ring_60s: RingBuffer,
    ring_15m: RingBuffer,
    ring_1h: RingBuffer,
    last_price: Option<f64>,
    last_update: Option<Instant>,
    boot_until: Option<Instant>,
}

impl SymbolRings {
    pub fn new(window_60s: Duration) -> Self {
        Self {
            ring_60s: RingBuffer::new(window_60s),
            ring_15m: RingBuffer::new(Duration::from_secs(15 * 60)),
            ring_1h: RingBuffer::new(Duration::from_secs(60 * 60)),
            last_price: None,
            last_update: None,
            boot_until: None,
        }
    }

    pub fn record(&mut self, ts: Instant, price: f64) {
        self.last_price = Some(price);
        self.last_update = Some(ts);
        self.ring_60s.push(ts, price);
        self.ring_15m.push(ts, price);
        self.ring_1h.push(ts, price);
    }

    pub fn snapshot(&mut self, ts: Instant) {
        if let (Some(price), Some(last_ts)) = (self.last_price, self.last_update) {
            if ts > last_ts {
                self.ring_60s.push(ts, price);
                self.ring_15m.push(ts, price);
                self.ring_1h.push(ts, price);
            }
        }
    }

    pub fn ret_60s(&self) -> Option<f64> {
        return_over(&self.ring_60s, Duration::from_secs(60))
    }

    pub fn ret_15m(&self) -> Option<f64> {
        return_over(&self.ring_15m, Duration::from_secs(15 * 60))
    }

    pub fn ret_1h(&self) -> Option<f64> {
        return_over(&self.ring_1h, Duration::from_secs(60 * 60))
    }

    pub fn warm(&mut self, now: Instant) -> bool {
        if let Some(until) = self.boot_until {
            if now < until {
                return false;
            }
            self.boot_until = None;
            return true;
        }
        self.ring_60s.is_saturated()
    }

    pub fn bootstrap(&mut self, ts: Instant, price: Option<f64>, hold: Duration) {
        self.ring_60s.clear();
        self.ring_15m.clear();
        self.ring_1h.clear();
        if let Some(value) = price {
            self.ring_60s.push(ts, value);
            self.ring_15m.push(ts, value);
            self.ring_1h.push(ts, value);
            self.last_price = Some(value);
            self.last_update = Some(ts);
        }
        self.boot_until = Some(ts + hold);
    }
}

pub fn return_over(ring: &RingBuffer, window: Duration) -> Option<f64> {
    let latest = ring.head()?;
    let past = ring.value_at_offset(window)?;
    if past <= 0.0 {
        return None;
    }
    Some((latest.value / past) - 1.0)
}

pub fn abs_return_over(ring: &RingBuffer, window: Duration) -> f64 {
    return_over(ring, window).map(f64::abs).unwrap_or(0.0)
}
