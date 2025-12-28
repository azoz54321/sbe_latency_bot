use std::time::{Duration, Instant};

use rust_decimal::Decimal;

use crate::capital::SlotId;
use crate::execution::{OrderFill, OrderStatus, OrderSubmitError, ProviderError};
use crate::filters::SymbolFilters;
use crate::types::Symbol;

#[derive(Debug, Clone)]
pub struct PendingTpState {
    pub slot: SlotId,
    pub symbol: Symbol,
    pub buy_client_id: String,
    pub tp_client_id: String,
    pub started_at: Instant,
    pub attempts: u32,
    pub last_err: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingTpClearReason {
    OpenOrders,
}

#[derive(Debug, Clone)]
pub struct KillSwitchEvent {
    pub symbol: Symbol,
    pub buy_client_id: String,
    pub qty: Decimal,
    pub elapsed_ms: u128,
    pub last_err: Option<String>,
    pub emergency_client_id: String,
    pub emergency_attempted: bool,
    pub emergency_ok: bool,
}

#[derive(Debug, Clone)]
pub enum PendingTpOutcome {
    NoAction,
    Cleared { reason: PendingTpClearReason },
    Kill { event: KillSwitchEvent },
}

pub trait PendingTpOps {
    fn tp_exists(&mut self, symbol: Symbol, tp_client_id: &str) -> Result<bool, ProviderError>;
    fn position_qty(&mut self, symbol: Symbol) -> Option<Decimal>;
    fn query_order(&mut self, symbol: Symbol, order_id: &str) -> Result<OrderFill, ProviderError>;
    fn query_balance(&mut self, asset: &str) -> Result<Decimal, ProviderError>;
    fn filters_for(&mut self, symbol: Symbol) -> Option<SymbolFilters>;
    fn last_price(&mut self, symbol: Symbol) -> Option<Decimal>;
    fn place_market_sell(
        &mut self,
        symbol: Symbol,
        qty: Decimal,
        client_order_id: String,
    ) -> Result<String, OrderSubmitError>;
}

#[derive(Debug, Clone)]
pub struct PendingTpGate {
    timeout: Duration,
    state: Option<PendingTpState>,
}

impl PendingTpGate {
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            state: None,
        }
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn is_active(&self) -> bool {
        self.state.is_some()
    }

    pub fn state(&self) -> Option<&PendingTpState> {
        self.state.as_ref()
    }

    pub fn enter(
        &mut self,
        now: Instant,
        slot: SlotId,
        symbol: Symbol,
        buy_client_id: String,
        tp_client_id: String,
    ) {
        if let Some(state) = self.state.as_ref() {
            if state.symbol == symbol && state.buy_client_id == buy_client_id {
                return;
            }
        }
        self.state = Some(PendingTpState {
            slot,
            symbol,
            buy_client_id,
            tp_client_id,
            started_at: now,
            attempts: 0,
            last_err: None,
        });
    }

    pub fn clear(&mut self) -> Option<PendingTpState> {
        self.state.take()
    }

    pub fn clear_if_tp(&mut self, tp_client_id: &str) -> bool {
        if let Some(state) = self.state.as_ref() {
            if state.tp_client_id == tp_client_id {
                self.state = None;
                return true;
            }
        }
        false
    }

    pub fn handle_timeout<P: PendingTpOps>(
        &mut self,
        now: Instant,
        order_lookup_id: String,
        base_asset: Option<String>,
        emergency_client_id: String,
        ops: &mut P,
    ) -> PendingTpOutcome {
        let Some(state) = self.state.as_mut() else {
            return PendingTpOutcome::NoAction;
        };

        let elapsed = now.duration_since(state.started_at);
        if elapsed < self.timeout {
            return PendingTpOutcome::NoAction;
        }

        state.attempts = state.attempts.saturating_add(1);
        let elapsed_ms = elapsed.as_millis();

        match ops.tp_exists(state.symbol, &state.tp_client_id) {
            Ok(true) => {
                self.state = None;
                return PendingTpOutcome::Cleared {
                    reason: PendingTpClearReason::OpenOrders,
                };
            }
            Ok(false) => {}
            Err(err) => {
                state.last_err = Some(err.to_string());
            }
        }

        let mut qty = ops.position_qty(state.symbol).unwrap_or(Decimal::ZERO);
        if qty <= Decimal::ZERO {
            match ops.query_order(state.symbol, &order_lookup_id) {
                Ok(fill) => {
                    if matches!(
                        fill.status,
                        OrderStatus::Filled | OrderStatus::PartiallyFilled
                    ) {
                        if fill.cum_filled > Decimal::ZERO {
                            qty = fill.cum_filled;
                        }
                    }
                }
                Err(err) => {
                    state.last_err = Some(err.to_string());
                }
            }
        }

        if qty <= Decimal::ZERO {
            if let Some(asset) = base_asset.as_deref() {
                match ops.query_balance(asset) {
                    Ok(balance_qty) => {
                        if balance_qty > Decimal::ZERO {
                            qty = balance_qty;
                        }
                    }
                    Err(err) => {
                        state.last_err = Some(err.to_string());
                    }
                }
            } else {
                state.last_err = Some("base asset missing".to_string());
            }
        }

        let aligned_qty = if let Some(filters) = ops.filters_for(state.symbol) {
            let aligned = floor_to_step(qty, filters.step);
            if aligned <= Decimal::ZERO {
                state.last_err = Some("qty_zero_after_alignment".to_string());
                Decimal::ZERO
            } else if filters.min_notional > Decimal::ZERO {
                if let Some(last_px) = ops.last_price(state.symbol) {
                    let notional = (aligned * last_px).normalize();
                    if notional < filters.min_notional {
                        state.last_err = Some(format!(
                            "min_notional not met min={} actual={}",
                            filters.min_notional, notional
                        ));
                        Decimal::ZERO
                    } else {
                        aligned
                    }
                } else {
                    aligned
                }
            } else {
                aligned
            }
        } else {
            state.last_err = Some("filters missing".to_string());
            Decimal::ZERO
        };

        let mut emergency_attempted = false;
        let mut emergency_ok = false;
        if aligned_qty > Decimal::ZERO {
            emergency_attempted = true;
            match ops.place_market_sell(state.symbol, aligned_qty, emergency_client_id.clone()) {
                Ok(_) => {
                    emergency_ok = true;
                }
                Err(err) => {
                    state.last_err = Some(err.to_string());
                }
            }
        } else if state.last_err.is_none() {
            state.last_err = Some("position_flat".to_string());
        }

        let event = KillSwitchEvent {
            symbol: state.symbol,
            buy_client_id: state.buy_client_id.clone(),
            qty: aligned_qty,
            elapsed_ms,
            last_err: state.last_err.clone(),
            emergency_client_id,
            emergency_attempted,
            emergency_ok,
        };

        self.state = None;
        PendingTpOutcome::Kill { event }
    }
}

fn floor_to_step(qty: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return qty;
    }
    let steps = (qty / step).floor();
    (steps * step).normalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    use rust_decimal::Decimal;

    struct MockOps {
        tp_exists: Result<bool, ProviderError>,
        position_qty: Option<Decimal>,
        order_fill: Result<OrderFill, ProviderError>,
        balance_qty: Result<Decimal, ProviderError>,
        filters: Option<SymbolFilters>,
        last_price: Option<Decimal>,
        sell_result: Result<String, OrderSubmitError>,
        sell_calls: usize,
    }

    impl MockOps {
        fn new() -> Self {
            Self {
                tp_exists: Ok(false),
                position_qty: None,
                order_fill: Ok(OrderFill {
                    cum_filled: Decimal::ZERO,
                    cum_quote: Decimal::ZERO,
                    status: OrderStatus::New,
                }),
                balance_qty: Ok(Decimal::ZERO),
                filters: Some(SymbolFilters {
                    step: Decimal::new(1, 0),
                    tick: Decimal::new(1, 2),
                    min_notional: Decimal::ZERO,
                }),
                last_price: Some(Decimal::new(1, 0)),
                sell_result: Ok("ok".to_string()),
                sell_calls: 0,
            }
        }
    }

    impl PendingTpOps for MockOps {
        fn tp_exists(
            &mut self,
            _symbol: Symbol,
            _tp_client_id: &str,
        ) -> Result<bool, ProviderError> {
            self.tp_exists.clone()
        }

        fn position_qty(&mut self, _symbol: Symbol) -> Option<Decimal> {
            self.position_qty
        }

        fn query_order(
            &mut self,
            _symbol: Symbol,
            _order_id: &str,
        ) -> Result<OrderFill, ProviderError> {
            self.order_fill.clone()
        }

        fn query_balance(&mut self, _asset: &str) -> Result<Decimal, ProviderError> {
            self.balance_qty.clone()
        }

        fn filters_for(&mut self, _symbol: Symbol) -> Option<SymbolFilters> {
            self.filters
        }

        fn last_price(&mut self, _symbol: Symbol) -> Option<Decimal> {
            self.last_price
        }

        fn place_market_sell(
            &mut self,
            _symbol: Symbol,
            _qty: Decimal,
            _client_order_id: String,
        ) -> Result<String, OrderSubmitError> {
            self.sell_calls += 1;
            self.sell_result.clone()
        }
    }

    #[test]
    fn pending_tp_clears_on_tp_match() {
        let symbol = Symbol::from_str("ETHUSDT").expect("symbol");
        let mut gate = PendingTpGate::new(Duration::from_secs(5));
        gate.enter(
            Instant::now(),
            SlotId::A,
            symbol,
            "BUY1".to_string(),
            "TP1".to_string(),
        );
        assert!(gate.is_active());
        assert!(gate.clear_if_tp("TP1"));
        assert!(!gate.is_active());
    }

    #[test]
    fn pending_tp_clears_on_open_orders() {
        let symbol = Symbol::from_str("ETHUSDT").expect("symbol");
        let mut gate = PendingTpGate::new(Duration::from_secs(1));
        let start = Instant::now() - Duration::from_secs(5);
        gate.enter(
            start,
            SlotId::A,
            symbol,
            "BUY1".to_string(),
            "TP1".to_string(),
        );

        let mut ops = MockOps::new();
        ops.tp_exists = Ok(true);

        let outcome = gate.handle_timeout(
            Instant::now(),
            "BUY1".to_string(),
            Some("ETH".to_string()),
            "EMERG-OPEN".to_string(),
            &mut ops,
        );

        match outcome {
            PendingTpOutcome::Cleared { reason } => {
                assert_eq!(reason, PendingTpClearReason::OpenOrders);
            }
            _ => panic!("expected cleared outcome"),
        }
        assert!(!gate.is_active());
    }

    #[test]
    fn duplicate_enter_is_idempotent() {
        let symbol = Symbol::from_str("ETHUSDT").expect("symbol");
        let mut gate = PendingTpGate::new(Duration::from_secs(5));
        let start = Instant::now() - Duration::from_secs(3);
        gate.enter(
            start,
            SlotId::A,
            symbol,
            "BUY1".to_string(),
            "TP1".to_string(),
        );
        let first_started = gate.state().unwrap().started_at;
        let later = start + Duration::from_secs(2);

        gate.enter(
            later,
            SlotId::A,
            symbol,
            "BUY1".to_string(),
            "TP1".to_string(),
        );

        let state = gate.state().expect("state present");
        assert_eq!(state.started_at, first_started);
        assert_eq!(state.buy_client_id, "BUY1");
        assert_eq!(state.tp_client_id, "TP1");
    }

    #[test]
    fn timeout_triggers_emergency_and_kill() {
        let symbol = Symbol::from_str("ETHUSDT").expect("symbol");
        let mut gate = PendingTpGate::new(Duration::from_secs(1));
        let start = Instant::now() - Duration::from_secs(10);
        gate.enter(
            start,
            SlotId::A,
            symbol,
            "BUY1".to_string(),
            "TP1".to_string(),
        );

        let mut ops = MockOps::new();
        ops.order_fill = Ok(OrderFill {
            cum_filled: Decimal::new(10, 0),
            cum_quote: Decimal::ZERO,
            status: OrderStatus::Filled,
        });
        ops.filters = Some(SymbolFilters {
            step: Decimal::new(1, 0),
            tick: Decimal::new(1, 2),
            min_notional: Decimal::ZERO,
        });

        let outcome = gate.handle_timeout(
            Instant::now(),
            "BUY1".to_string(),
            Some("ETH".to_string()),
            "EMERG-1".to_string(),
            &mut ops,
        );

        match outcome {
            PendingTpOutcome::Kill { event } => {
                assert!(event.emergency_attempted);
                assert!(event.emergency_ok);
                assert_eq!(event.qty, Decimal::new(10, 0));
            }
            _ => panic!("expected kill outcome"),
        }
        assert_eq!(ops.sell_calls, 1);
        assert!(!gate.is_active());
    }

    #[test]
    fn order_not_found_triggers_kill() {
        let symbol = Symbol::from_str("ETHUSDT").expect("symbol");
        let mut gate = PendingTpGate::new(Duration::from_secs(1));
        let start = Instant::now() - Duration::from_secs(5);
        gate.enter(
            start,
            SlotId::A,
            symbol,
            "BUY1".to_string(),
            "TP1".to_string(),
        );

        let mut ops = MockOps::new();
        ops.order_fill = Err(ProviderError::not_found("order missing".to_string()));
        ops.balance_qty = Ok(Decimal::new(2, 0));

        let outcome = gate.handle_timeout(
            Instant::now(),
            "BUY1".to_string(),
            Some("ETH".to_string()),
            "EMERG-2".to_string(),
            &mut ops,
        );

        match outcome {
            PendingTpOutcome::Kill { event } => {
                assert!(event.emergency_attempted);
            }
            _ => panic!("expected kill outcome"),
        }
        assert!(!gate.is_active());
        assert_eq!(ops.sell_calls, 1);
    }

    #[test]
    fn pending_tp_blocks_until_timeout() {
        let symbol = Symbol::from_str("ETHUSDT").expect("symbol");
        let mut gate = PendingTpGate::new(Duration::from_secs(10));
        let start = Instant::now();
        gate.enter(
            start,
            SlotId::A,
            symbol,
            "BUY1".to_string(),
            "TP1".to_string(),
        );

        let mut ops = MockOps::new();
        let outcome = gate.handle_timeout(
            start + Duration::from_secs(1),
            "BUY1".to_string(),
            Some("ETH".to_string()),
            "EMERG-3".to_string(),
            &mut ops,
        );

        assert!(matches!(outcome, PendingTpOutcome::NoAction));
        assert!(gate.is_active());
    }
}
