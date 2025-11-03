use std::collections::HashMap;
use std::time::Instant;

use crate::capital::SlotId;
use crate::types::Symbol;

const STOP_LOSS_PCT: f64 = 0.05;
const TAKE_PROFIT_PCT: f64 = 0.10;
const PROFIT_PROTECT_PCT: f64 = 0.05;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitReason {
    StopLoss,
    ProfitProtect,
}

#[derive(Debug)]
pub struct Position {
    pub symbol: Symbol,
    pub entry_price: f64,
    pub qty: f64,
    pub take_profit: f64,
    pub slot: SlotId,
    pub opened_at: Instant,
    reach_protect: bool,
}

#[derive(Debug)]
pub struct PositionBook {
    positions: HashMap<Symbol, Position>,
}

#[allow(clippy::new_without_default)]
impl PositionBook {
    pub fn reset_daily_view(&mut self) {}

    pub fn new() -> Self {
        Self {
            positions: HashMap::with_capacity(16),
        }
    }

    pub fn open(&mut self, symbol: Symbol, entry_price: f64, qty: f64, slot: SlotId, now: Instant) {
        let take_profit = entry_price * (1.0 + TAKE_PROFIT_PCT);
        let position = Position {
            symbol,
            entry_price,
            qty,
            take_profit,
            slot,
            opened_at: now,
            reach_protect: false,
        };
        self.positions.insert(symbol, position);
    }

    pub fn close(&mut self, symbol: Symbol) -> Option<Position> {
        self.positions.remove(&symbol)
    }

    pub fn contains(&self, symbol: Symbol) -> bool {
        self.positions.contains_key(&symbol)
    }

    pub fn evaluate_price(&mut self, symbol: Symbol, price: f64) -> Option<ExitReason> {
        let position = self.positions.get_mut(&symbol)?;
        let stop_loss_price = position.entry_price * (1.0 - STOP_LOSS_PCT);
        if price <= stop_loss_price {
            return Some(ExitReason::StopLoss);
        }

        if price >= position.take_profit {
            position.reach_protect = true;
            return None;
        }

        let protect_threshold = position.entry_price * (1.0 + PROFIT_PROTECT_PCT);
        if price >= protect_threshold {
            position.reach_protect = true;
            return None;
        }

        if position.reach_protect && price <= position.entry_price {
            return Some(ExitReason::ProfitProtect);
        }

        None
    }

    #[cfg(feature = "test-mode")]
    pub fn snapshot(&self) -> PositionsSnapshot {
        let mut open = self
            .positions
            .values()
            .map(|pos| PositionEntry {
                symbol: pos.symbol,
                qty: pos.qty,
                entry_price: pos.entry_price,
                slot: pos.slot,
            })
            .collect::<Vec<_>>();
        open.sort_by(|a, b| a.symbol.as_bytes().cmp(b.symbol.as_bytes()));
        PositionsSnapshot { open }
    }
}

#[cfg(feature = "test-mode")]
#[derive(Clone, Debug, Default)]
pub struct PositionsSnapshot {
    pub open: Vec<PositionEntry>,
}

#[cfg(feature = "test-mode")]
#[derive(Clone, Debug)]
pub struct PositionEntry {
    pub symbol: Symbol,
    pub qty: f64,
    pub entry_price: f64,
    pub slot: SlotId,
}
