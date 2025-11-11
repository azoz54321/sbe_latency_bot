use rand::Rng;
use std::time::Duration;

const MAX_SHIFT: u32 = 10;

pub fn full_jitter(attempt: u32, base: Duration, cap: Duration, rng: &mut impl Rng) -> Duration {
    let base_ms = duration_to_millis(base).max(1);
    let cap_ms = duration_to_millis(cap);
    let shift = attempt.min(MAX_SHIFT);
    let multiplier = 1u64.checked_shl(shift).unwrap_or(u64::MAX);
    let mut expo_ms = base_ms.saturating_mul(multiplier);
    if expo_ms == 0 {
        expo_ms = base_ms;
    }
    let capped_ms = expo_ms.min(cap_ms);
    if capped_ms == 0 {
        return Duration::from_millis(0);
    }
    let sleep_ms = rng.gen_range(0..=capped_ms);
    Duration::from_millis(sleep_ms)
}

fn duration_to_millis(duration: Duration) -> u64 {
    let millis = duration.as_millis();
    if millis == 0 {
        return 0;
    }
    if millis >= u128::from(u64::MAX) {
        u64::MAX
    } else {
        millis as u64
    }
}
