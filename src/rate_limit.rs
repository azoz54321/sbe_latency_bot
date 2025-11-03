use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct TokenBucket {
    capacity: f64,
    tokens: f64,
    refill_per_sec: f64,
    last_refill: Instant,
}

impl TokenBucket {
    pub fn new(capacity: u32, refill_per_sec: f64) -> Self {
        let capacity = capacity as f64;
        Self {
            capacity,
            tokens: capacity,
            refill_per_sec,
            last_refill: Instant::now(),
        }
    }

    pub fn try_acquire(&mut self, tokens: f64) -> bool {
        self.refill();
        if self.tokens >= tokens {
            self.tokens -= tokens;
            true
        } else {
            false
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now
            .checked_duration_since(self.last_refill)
            .unwrap_or_default();
        let to_add = elapsed.as_secs_f64() * self.refill_per_sec;
        if to_add > 0.0 {
            self.tokens = (self.tokens + to_add).min(self.capacity);
            self.last_refill = now;
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct ExponentialBackoff {
    base: Duration,
    max: Duration,
    jitter: Duration,
    attempt: u32,
}

#[allow(dead_code)]
impl ExponentialBackoff {
    pub fn new(base: Duration, max: Duration, jitter: Duration) -> Self {
        Self {
            base,
            max,
            jitter,
            attempt: 0,
        }
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    pub fn next_delay(&mut self) -> Duration {
        let shift = self.attempt.min(10);
        let multiplier = 1u32 << shift;
        let delay = self.base.saturating_mul(multiplier);
        self.attempt = self.attempt.saturating_add(1);
        let capped = if delay > self.max { self.max } else { delay };
        capped + self.random_jitter()
    }

    fn random_jitter(&self) -> Duration {
        if self.jitter.is_zero() {
            return Duration::ZERO;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        let nanos = now.subsec_nanos() as u64;
        let jitter_ns = nanos % (self.jitter.as_nanos() as u64 + 1);
        Duration::from_nanos(jitter_ns)
    }
}
