use once_cell::sync::Lazy;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

static MONO_ZERO: Lazy<Instant> = Lazy::new(Instant::now);

#[inline]
pub fn monotonic_now_ns() -> u64 {
    MONO_ZERO.elapsed().as_nanos() as u64
}

#[inline]
pub fn instant_to_ns(instant: Instant) -> u64 {
    instant.duration_since(*MONO_ZERO).as_nanos() as u64
}

#[inline]
pub fn wall_clock_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|dur| dur.as_nanos() as u64)
        .unwrap_or(0)
}
