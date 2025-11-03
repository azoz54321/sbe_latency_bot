use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::time::Instant;

use rust_decimal::Decimal;

pub const MAX_SYMBOL_LEN: usize = 16;

pub type SymbolId = u64;

#[derive(Copy, Clone, Eq)]
pub struct Symbol {
    len: u8,
    data: [u8; MAX_SYMBOL_LEN],
}

impl Symbol {
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > MAX_SYMBOL_LEN {
            return None;
        }
        let mut data = [0u8; MAX_SYMBOL_LEN];
        for (idx, byte) in bytes.iter().enumerate() {
            data[idx] = byte.to_ascii_uppercase();
        }
        Some(Self {
            len: bytes.len() as u8,
            data,
        })
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> Option<Self> {
        Self::from_bytes(value.as_bytes())
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data[..self.len as usize]
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        debug_assert!(std::str::from_utf8(self.as_bytes()).is_ok());
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }
}

impl PartialEq for Symbol {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Hash for Symbol {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl fmt::Debug for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[inline]
pub fn symbol_id_from_str(symbol: &str) -> SymbolId {
    let mut hasher = DefaultHasher::new();
    symbol.to_ascii_uppercase().hash(&mut hasher);
    hasher.finish()
}

#[inline]
pub fn symbol_id(symbol: Symbol) -> SymbolId {
    symbol_id_from_str(symbol.as_str())
}

#[derive(Copy, Clone, Debug)]
pub struct PriceEvent {
    pub symbol: Symbol,
    pub price: f64,
    pub received_instant: Instant,
    pub ts_mono_ns: u64,
    #[allow(dead_code)]
    pub exch_ts_ns: u64,
    #[allow(dead_code)]
    pub seq: u64,
}

#[derive(Clone, Debug)]
pub struct TriggerEvent {
    pub symbol: Symbol,
    pub price_now: f64,
    pub ret_60s: f64,
    pub target_notional: Decimal,
    pub trigger_ts_mono_ns: u64,
    pub signal_ts_mono_ns: u64,
}

#[derive(Clone, Debug, Default)]
pub struct OrderIds {
    #[allow(dead_code)]
    pub buy: Option<String>,
    pub limit: Option<String>,
}

pub enum LogMessage {
    Metric(MetricEvent),
    Info(Cow<'static, str>),
    Warn(Cow<'static, str>),
    Error(String),
    ResetDaily,
}

#[derive(Copy, Clone, Debug)]
pub struct TickToSendMetric {
    pub symbol: Symbol,
    pub price_to_send_ns: u64,
    pub trigger_to_send_ns: u64,
}

#[derive(Copy, Clone, Debug)]
pub enum MetricEvent {
    WsMsgIn,
    WsTextIn,
    PriceEventIn,
    DecodeErr,
    QueueDropMarket {
        symbol: Symbol,
    },
    QueueDropTrigger {
        symbol: Symbol,
    },
    SeqAnomaly {
        symbol: Symbol,
        last_seq: u64,
        observed_seq: u64,
    },
    TriggerEmitted {
        symbol: Symbol,
    },
    TriggerDropLate {
        symbol: Symbol,
        latency_ns: u64,
    },
    BuyOk {
        symbol: Symbol,
    },
    BuyErr {
        symbol: Symbol,
    },
    TickToSend(TickToSendMetric),
    SignalSuppressed {
        symbol: Symbol,
    },
}

impl From<MetricEvent> for LogMessage {
    fn from(metric: MetricEvent) -> Self {
        LogMessage::Metric(metric)
    }
}
