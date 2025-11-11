use std::collections::HashMap;
use std::time::Instant;

use rust_decimal::Decimal;

use crate::capital::SlotId;
use crate::config::StrategyConfig;
use crate::types::Symbol;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitReason {
    TakeProfitLimit,
    StopLoss,
    ReturnToEntry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitDecision {
    Hold,
    LimitFilled,
    Market { reason: ExitReason },
}

#[derive(Debug, Clone)]
pub struct Position {
    pub symbol: Symbol,
    pub qty: Decimal,
    pub entry_price: Decimal,
    pub entry_ts: Instant,
    pub high_water: Decimal,
    pub bounce_armed: bool,
    pub closing: bool,
    pub slot: SlotId,
    pub take_profit: Decimal,
    pub stop_loss: Decimal,
    pub bounce_break_even: Decimal,
    pub tp_order_id: Option<String>,
    pub tp_order_qty: Decimal,
    pub tp_adjust_done: bool,
    pub tp_initial_price: Decimal,
    pub tp_current_price: Decimal,
}

impl Position {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        symbol: Symbol,
        qty: Decimal,
        entry_price: Decimal,
        slot: SlotId,
        now: Instant,
        take_profit: Decimal,
        stop_loss: Decimal,
        bounce_break_even: Decimal,
        tp_order_id: Option<String>,
        tp_order_qty: Decimal,
    ) -> Self {
        Self {
            symbol,
            qty,
            entry_price,
            entry_ts: now,
            high_water: entry_price,
            bounce_armed: false,
            closing: false,
            slot,
            take_profit,
            stop_loss,
            bounce_break_even,
            tp_order_id,
            tp_order_qty,
            tp_adjust_done: false,
            tp_initial_price: take_profit,
            tp_current_price: take_profit,
        }
    }

    pub fn on_tick(&mut self, px: Decimal, cfg: &StrategyConfig) -> ExitDecision {
        if self.closing {
            return ExitDecision::Hold;
        }

        if px > self.high_water {
            self.high_water = px;
        }

        if px <= self.stop_loss {
            self.closing = true;
            return ExitDecision::Market {
                reason: ExitReason::StopLoss,
            };
        }

        if !self.bounce_armed {
            let arm_threshold = self.entry_price * (Decimal::ONE + cfg.bounce_arm_pct);
            if self.high_water >= arm_threshold {
                self.bounce_armed = true;
            }
        }

        if px >= self.take_profit {
            self.closing = true;
            return ExitDecision::LimitFilled;
        }

        if self.bounce_armed && px <= self.bounce_break_even {
            self.closing = true;
            return ExitDecision::Market {
                reason: ExitReason::ReturnToEntry,
            };
        }

        ExitDecision::Hold
    }

    pub fn mark_closing_failed(&mut self) {
        self.closing = false;
    }

    pub fn clear_tp_target(&mut self) {
        self.tp_order_id = None;
        self.tp_order_qty = Decimal::ZERO;
        self.tp_current_price = Decimal::ZERO;
    }

    pub fn apply_tp_adjust(&mut self, new_price: Decimal, new_order_id: String) {
        self.take_profit = new_price;
        self.tp_current_price = new_price;
        self.tp_order_id = Some(new_order_id);
        self.tp_adjust_done = true;
    }
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

    #[allow(clippy::too_many_arguments)]
    pub fn open(
        &mut self,
        symbol: Symbol,
        qty: Decimal,
        entry_price: Decimal,
        slot: SlotId,
        now: Instant,
        take_profit: Decimal,
        stop_loss: Decimal,
        bounce_break_even: Decimal,
        tp_order_id: Option<String>,
        tp_order_qty: Decimal,
    ) {
        let position = Position::new(
            symbol,
            qty,
            entry_price,
            slot,
            now,
            take_profit,
            stop_loss,
            bounce_break_even,
            tp_order_id,
            tp_order_qty,
        );
        self.positions.insert(symbol, position);
    }

    pub fn close(&mut self, symbol: Symbol) -> Option<Position> {
        self.positions.remove(&symbol)
    }

    pub fn contains(&self, symbol: Symbol) -> bool {
        self.positions.contains_key(&symbol)
    }

    pub fn on_tick(
        &mut self,
        symbol: Symbol,
        px: Decimal,
        cfg: &StrategyConfig,
    ) -> Option<ExitDecision> {
        let position = self.positions.get_mut(&symbol)?;
        let decision = position.on_tick(px, cfg);
        match decision {
            ExitDecision::Hold => None,
            other => Some(other),
        }
    }

    pub fn mark_closing_failed(&mut self, symbol: Symbol) {
        if let Some(position) = self.positions.get_mut(&symbol) {
            position.mark_closing_failed();
        }
    }

    pub fn record_tp_adjust(
        &mut self,
        symbol: Symbol,
        new_price: Decimal,
        new_order_id: String,
    ) -> Option<SlotId> {
        if let Some(position) = self.positions.get_mut(&symbol) {
            position.apply_tp_adjust(new_price, new_order_id);
            Some(position.slot)
        } else {
            None
        }
    }

    pub fn get(&self, symbol: Symbol) -> Option<&Position> {
        self.positions.get(&symbol)
    }

    pub fn get_mut(&mut self, symbol: Symbol) -> Option<&mut Position> {
        self.positions.get_mut(&symbol)
    }
}
