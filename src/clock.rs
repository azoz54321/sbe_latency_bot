#[cfg(test)]
use std::sync::Mutex;
#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

#[cfg(test)]
use chrono::Duration as ChronoDuration;
use chrono::{DateTime, Utc};

use crate::time_utils::instant_to_ns;

pub trait Clock: Send + Sync {
    fn now_instant(&self) -> Instant;
    fn now_utc(&self) -> DateTime<Utc>;
    fn instant_to_ns(&self, instant: Instant) -> u64;

    fn monotonic_now_ns(&self) -> u64 {
        self.instant_to_ns(self.now_instant())
    }

    fn unix_now_ns(&self) -> u64 {
        let utc = self.now_utc();
        let secs = utc.timestamp() as i128;
        let nanos = utc.timestamp_subsec_nanos() as i128;
        let total = secs.saturating_mul(1_000_000_000).saturating_add(nanos);
        if total < 0 {
            0
        } else {
            total as u64
        }
    }

    fn unix_now_ms(&self) -> u64 {
        self.unix_now_ns() / 1_000_000
    }
}

#[derive(Debug, Clone, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_instant(&self) -> Instant {
        Instant::now()
    }

    fn now_utc(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn instant_to_ns(&self, instant: Instant) -> u64 {
        instant_to_ns(instant)
    }
}

#[cfg(test)]
#[derive(Debug)]
pub struct TestClock {
    base_instant: Instant,
    base_utc: DateTime<Utc>,
    offset: Mutex<Duration>,
}

#[cfg(test)]
impl TestClock {
    pub fn new() -> Self {
        Self::from_datetime(Utc::now())
    }

    pub fn from_datetime(start: DateTime<Utc>) -> Self {
        Self {
            base_instant: Instant::now(),
            base_utc: start,
            offset: Mutex::new(Duration::ZERO),
        }
    }

    pub fn advance(&self, delta: Duration) {
        let mut offset = self.offset.lock().expect("test clock poisoned");
        *offset = offset.saturating_add(delta);
    }

    pub fn set_elapsed(&self, elapsed: Duration) {
        let mut offset = self.offset.lock().expect("test clock poisoned");
        *offset = elapsed;
    }

    pub fn set_datetime(&self, when: DateTime<Utc>) {
        let mut offset = self.offset.lock().expect("test clock poisoned");
        let delta = when
            .signed_duration_since(self.base_utc)
            .to_std()
            .unwrap_or(Duration::ZERO);
        *offset = delta;
    }

    fn chrono_offset(&self) -> ChronoDuration {
        let offset = self.offset.lock().expect("test clock poisoned");
        ChronoDuration::from_std(*offset).unwrap_or_else(|_| ChronoDuration::zero())
    }
}

#[cfg(test)]
impl Clock for TestClock {
    fn now_instant(&self) -> Instant {
        let offset = self.offset.lock().expect("test clock poisoned");
        self.base_instant + *offset
    }

    fn now_utc(&self) -> DateTime<Utc> {
        self.base_utc + self.chrono_offset()
    }

    fn instant_to_ns(&self, instant: Instant) -> u64 {
        instant
            .checked_duration_since(self.base_instant)
            .unwrap_or_default()
            .as_nanos() as u64
    }

    fn unix_now_ns(&self) -> u64 {
        let now = self.now_utc();
        let secs = now.timestamp() as i128;
        let nanos = now.timestamp_subsec_nanos() as i128;
        let total = secs.saturating_mul(1_000_000_000).saturating_add(nanos);
        if total < 0 {
            0
        } else {
            total as u64
        }
    }
}

#[cfg(test)]
impl Default for TestClock {
    fn default() -> Self {
        Self::new()
    }
}
