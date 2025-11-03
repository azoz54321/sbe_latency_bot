use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;

use crate::clock::Clock;
use crate::types::LogMessage;

const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(2);

pub struct LatencyMetrics {
    enabled: bool,
    flush_interval: Duration,
    last_flush: Instant,
    signal_to_buy: Histogram,
    buy_to_limit: Histogram,
    signal_to_limit: Histogram,
    auto_heal_attempts: u64,
    auto_heal_success: u64,
    disable_for_today: u64,
    clock: Arc<dyn Clock>,
}

impl LatencyMetrics {
    pub fn new(enabled: bool, clock: Arc<dyn Clock>) -> Self {
        Self {
            enabled,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            last_flush: clock.now_instant(),
            signal_to_buy: Histogram::new(),
            buy_to_limit: Histogram::new(),
            signal_to_limit: Histogram::new(),
            auto_heal_attempts: 0,
            auto_heal_success: 0,
            disable_for_today: 0,
            clock,
        }
    }

    pub fn record_signal_to_buy(&mut self, dur: Duration) {
        if self.enabled {
            self.signal_to_buy.record(dur);
        }
    }

    pub fn record_buy_to_limit(&mut self, dur: Duration) {
        if self.enabled {
            self.buy_to_limit.record(dur);
        }
    }

    pub fn record_signal_to_limit(&mut self, dur: Duration) {
        if self.enabled {
            self.signal_to_limit.record(dur);
        }
    }

    pub fn inc_auto_heal_attempts(&mut self) {
        if self.enabled {
            self.auto_heal_attempts = self.auto_heal_attempts.saturating_add(1);
        }
    }

    pub fn inc_auto_heal_success(&mut self) {
        if self.enabled {
            self.auto_heal_success = self.auto_heal_success.saturating_add(1);
        }
    }

    pub fn inc_disable_for_today(&mut self) {
        if self.enabled {
            self.disable_for_today = self.disable_for_today.saturating_add(1);
        }
    }

    pub fn maybe_flush(&mut self, log_tx: &Sender<LogMessage>) {
        if !self.enabled {
            return;
        }
        let now = self.clock.now_instant();
        if now
            .checked_duration_since(self.last_flush)
            .unwrap_or_default()
            < self.flush_interval
        {
            return;
        }

        self.last_flush = now;
        if let Some(snapshot) = self.take_snapshot() {
            let mut message = format!(
                "[METRICS] signal?buy(ms): {}/{} signal?limit(ms): {}/{} buy?limit(ms): {}/{} count={}",
                snapshot.signal_to_buy.p50_ms,
                snapshot.signal_to_buy.p95_ms,
                snapshot.signal_to_limit.p50_ms,
                snapshot.signal_to_limit.p95_ms,
                snapshot.buy_to_limit.p50_ms,
                snapshot.buy_to_limit.p95_ms,
                snapshot.signal_to_buy.count
            );
            if self.auto_heal_attempts > 0
                || self.auto_heal_success > 0
                || self.disable_for_today > 0
            {
                message.push_str(&format!(
                    " heal_attempts={} heal_success={} disables={}",
                    self.auto_heal_attempts, self.auto_heal_success, self.disable_for_today
                ));
            }
            let _ = log_tx.send(LogMessage::Info(message.into()));
            self.auto_heal_attempts = 0;
            self.auto_heal_success = 0;
            self.disable_for_today = 0;
        }
    }

    pub fn reset_daily(&mut self) {
        if !self.enabled {
            return;
        }
        self.signal_to_buy.clear();
        self.buy_to_limit.clear();
        self.signal_to_limit.clear();
        self.last_flush = self.clock.now_instant();
        self.auto_heal_attempts = 0;
        self.auto_heal_success = 0;
        self.disable_for_today = 0;
    }

    fn take_snapshot(&mut self) -> Option<LatencySnapshot> {
        if self.signal_to_buy.count == 0 {
            return None;
        }
        let snapshot = LatencySnapshot {
            signal_to_buy: self.signal_to_buy.percentiles(),
            buy_to_limit: self.buy_to_limit.percentiles(),
            signal_to_limit: self.signal_to_limit.percentiles(),
        };
        self.signal_to_buy.clear();
        self.buy_to_limit.clear();
        self.signal_to_limit.clear();
        Some(snapshot)
    }
}

#[derive(Debug, Clone, Copy)]
struct HistogramStats {
    pub count: u64,
    pub p50_ms: f64,
    pub p95_ms: f64,
}

struct Histogram {
    values: Vec<u64>,
    count: u64,
}

impl Histogram {
    fn new() -> Self {
        Self {
            values: Vec::with_capacity(64),
            count: 0,
        }
    }

    fn record(&mut self, dur: Duration) {
        self.values.push(dur.as_micros() as u64);
        self.count += 1;
    }

    fn percentiles(&self) -> HistogramStats {
        if self.count == 0 {
            return HistogramStats {
                count: 0,
                p50_ms: 0.0,
                p95_ms: 0.0,
            };
        }
        let mut sorted = self.values.clone();
        sorted.sort_unstable();
        let idx50 = ((sorted.len() as f64) * 0.5).floor() as usize;
        let idx95 = ((sorted.len() as f64) * 0.95).floor() as usize;
        HistogramStats {
            count: self.count,
            p50_ms: micros_to_ms(sorted[idx50]),
            p95_ms: micros_to_ms(sorted[idx95.min(sorted.len() - 1)]),
        }
    }

    fn clear(&mut self) {
        self.values.clear();
        self.count = 0;
    }
}

struct LatencySnapshot {
    signal_to_buy: HistogramStats,
    buy_to_limit: HistogramStats,
    signal_to_limit: HistogramStats,
}

fn micros_to_ms(value: u64) -> f64 {
    value as f64 / 1_000.0
}

#[cfg(feature = "test-mode")]
#[derive(Clone, Copy, Debug, Default)]
pub struct LatencyCountersSnapshot {
    pub auto_heal_attempts: u64,
    pub auto_heal_success: u64,
    pub disable_for_today: u64,
}

#[cfg(feature = "test-mode")]
impl LatencyMetrics {
    pub fn counters_snapshot(&self) -> LatencyCountersSnapshot {
        LatencyCountersSnapshot {
            auto_heal_attempts: self.auto_heal_attempts,
            auto_heal_success: self.auto_heal_success,
            disable_for_today: self.disable_for_today,
        }
    }
}
