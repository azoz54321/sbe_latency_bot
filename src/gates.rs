use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::clock::Clock;

#[derive(Default)]
#[allow(dead_code)]
pub struct TradingGate {
    enabled: AtomicBool,
    epoch: AtomicU64,
}

#[allow(dead_code)]
impl TradingGate {
    pub fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            epoch: AtomicU64::new(0),
        }
    }

    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Release);
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Release);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    pub fn bump_epoch(&self) -> u64 {
        self.epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }
}

pub struct WarmupGate {
    clock: Arc<dyn Clock>,
    until_epoch_ms: AtomicU64,
}

impl WarmupGate {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            until_epoch_ms: AtomicU64::new(0),
        }
    }

    #[allow(dead_code)]
    pub fn arm_for(&self, dur: Duration) {
        let now_ms = self.clock.unix_now_ms();
        self.until_epoch_ms
            .store(now_ms + dur.as_millis() as u64, Ordering::Release);
    }

    pub fn is_warm(&self) -> bool {
        let now_ms = self.clock.unix_now_ms();
        now_ms >= self.until_epoch_ms.load(Ordering::Acquire)
    }
}
