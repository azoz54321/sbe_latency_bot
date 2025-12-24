use std::ops::Sub;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct HotCounters {
    pub ws_in: AtomicU64,
    pub ws_text_in: AtomicU64,
    pub price_in: AtomicU64,
    pub decode_ok: AtomicU64,
    pub decode_need_more: AtomicU64,
    pub decode_corrupt: AtomicU64,
    pub decode_schema_mismatch: AtomicU64,
    pub decode_truncated: AtomicU64,
    pub drop_skew: AtomicU64,
}

impl HotCounters {
    pub fn snapshot(&self) -> HotSnapshot {
        HotSnapshot {
            ws_in: self.ws_in.load(Ordering::Relaxed),
            ws_text_in: self.ws_text_in.load(Ordering::Relaxed),
            price_in: self.price_in.load(Ordering::Relaxed),
            decode_ok: self.decode_ok.load(Ordering::Relaxed),
            decode_need_more: self.decode_need_more.load(Ordering::Relaxed),
            decode_corrupt: self.decode_corrupt.load(Ordering::Relaxed),
            decode_schema_mismatch: self.decode_schema_mismatch.load(Ordering::Relaxed),
            decode_truncated: self.decode_truncated.load(Ordering::Relaxed),
            drop_skew: self.drop_skew.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default, Copy, Clone)]
pub struct HotSnapshot {
    pub ws_in: u64,
    pub ws_text_in: u64,
    pub price_in: u64,
    pub decode_ok: u64,
    pub decode_need_more: u64,
    pub decode_corrupt: u64,
    pub decode_schema_mismatch: u64,
    pub decode_truncated: u64,
    pub drop_skew: u64,
}

impl HotSnapshot {
    pub fn saturating_sub(&self, other: &Self) -> Self {
        Self {
            ws_in: self.ws_in.saturating_sub(other.ws_in),
            ws_text_in: self.ws_text_in.saturating_sub(other.ws_text_in),
            price_in: self.price_in.saturating_sub(other.price_in),
            decode_ok: self.decode_ok.saturating_sub(other.decode_ok),
            decode_need_more: self.decode_need_more.saturating_sub(other.decode_need_more),
            decode_corrupt: self.decode_corrupt.saturating_sub(other.decode_corrupt),
            decode_schema_mismatch: self
                .decode_schema_mismatch
                .saturating_sub(other.decode_schema_mismatch),
            decode_truncated: self.decode_truncated.saturating_sub(other.decode_truncated),
            drop_skew: self.drop_skew.saturating_sub(other.drop_skew),
        }
    }
}

impl Sub for HotSnapshot {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            ws_in: self.ws_in.saturating_sub(rhs.ws_in),
            ws_text_in: self.ws_text_in.saturating_sub(rhs.ws_text_in),
            price_in: self.price_in.saturating_sub(rhs.price_in),
            decode_ok: self.decode_ok.saturating_sub(rhs.decode_ok),
            decode_need_more: self.decode_need_more.saturating_sub(rhs.decode_need_more),
            decode_corrupt: self.decode_corrupt.saturating_sub(rhs.decode_corrupt),
            decode_schema_mismatch: self
                .decode_schema_mismatch
                .saturating_sub(rhs.decode_schema_mismatch),
            decode_truncated: self.decode_truncated.saturating_sub(rhs.decode_truncated),
            drop_skew: self.drop_skew.saturating_sub(rhs.drop_skew),
        }
    }
}
