use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::thread;
use std::time::Duration;

use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use hmac::{Hmac, Mac};
use reqwest::blocking::Client;
use reqwest::StatusCode;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde::Deserialize;
use sha2::Sha256;

use dashmap::DashSet;
use std::sync::{Arc, Mutex};

use crate::affinity;
use crate::channels::SpscReceiver;
use crate::clock::Clock;
use crate::config::{Config, ExecutionMode, ServerSpec};
use crate::fees::tp_target_px;
use crate::filters::{FilterCache, SymbolFilters};
use crate::metrics::LatencyMetrics;
use crate::positions::ExitReason;
use crate::rate_limit::TokenBucket;
use crate::risk::{DisableReason, RiskHandle};

use crate::types::{
    symbol_id, LogMessage, MetricEvent, OrderIds, Symbol, SymbolId, TickToSendMetric, TriggerEvent,
};

type HmacSha256 = Hmac<Sha256>;

mod ws_orders;
use ws_orders::WsOrderClient;

const RATE_LIMIT_CAPACITY: u32 = 20;
const RATE_LIMIT_REFILL_PER_SEC: f64 = 20.0;

type ExecResult = Result<OrderIds, PlaceOrderError>;

#[derive(Clone)]
pub struct ProviderOrder {
    pub symbol: Symbol,
    pub side: &'static str,
    pub order_type: &'static str,
    pub quantity: Decimal,
    pub quantity_scale: u32,
    pub price: Option<Decimal>,
    pub price_scale: u32,
    pub client_order_id: String,
    pub trigger_ts_mono_ns: u64,
}

#[derive(Clone)]
pub struct TpAdjustSpec {
    pub min_diff_ticks: u32,
    pub timeout: Duration,
    pub tick_size: Decimal,
    pub tp_pct: Decimal,
    pub maker_fee_pct: Decimal,
    pub taker_fee_pct: Decimal,
    pub quantity: Decimal,
    pub quantity_scale: u32,
    pub price_scale: u32,
    pub current_tp: Decimal,
    pub client_order_id: String,
    pub symbol: Symbol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    Canceled,
    NotFound,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Expired,
    Unknown,
}

impl OrderStatus {
    fn from_str(value: &str) -> Self {
        match value {
            "NEW" => OrderStatus::New,
            "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
            "FILLED" => OrderStatus::Filled,
            "CANCELED" => OrderStatus::Canceled,
            "REJECTED" => OrderStatus::Rejected,
            "EXPIRED" => OrderStatus::Expired,
            _ => OrderStatus::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct OrderFill {
    pub cum_filled: Decimal,
    pub status: OrderStatus,
}

#[derive(Debug, Clone)]
pub struct ProviderError {
    kind: ProviderErrorKind,
    detail: String,
}

impl ProviderError {
    pub fn new(kind: ProviderErrorKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }

    pub fn transient(detail: impl Into<String>) -> Self {
        Self::new(ProviderErrorKind::Transient, detail)
    }

    pub fn fatal(detail: impl Into<String>) -> Self {
        Self::new(ProviderErrorKind::Fatal, detail)
    }

    pub fn not_found(detail: impl Into<String>) -> Self {
        Self::new(ProviderErrorKind::NotFound, detail)
    }

    pub fn is_transient(&self) -> bool {
        matches!(self.kind, ProviderErrorKind::Transient)
    }

    pub fn is_not_found(&self) -> bool {
        matches!(self.kind, ProviderErrorKind::NotFound)
    }
}

impl fmt::Display for ProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.detail)
    }
}

impl std::error::Error for ProviderError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderErrorKind {
    Transient,
    NotFound,
    Fatal,
}

pub trait ExecutionProvider: Send + Sync {
    fn submit_pair(
        &self,
        buy: &ProviderOrder,
        limit: &ProviderOrder,
        adjust: Option<TpAdjustSpec>,
    ) -> Result<OrderIds, OrderSubmitError>;
    fn submit_order(&self, order: &ProviderOrder) -> Result<String, OrderSubmitError>;
    fn cancel_order(&self, symbol: Symbol, order_id: &str) -> Result<CancelOutcome, ProviderError>;
    fn query_order(&self, symbol: Symbol, order_id: &str) -> Result<OrderFill, ProviderError>;
}

#[derive(Clone)]
pub struct ExecGuards {
    bucket: Arc<Mutex<TokenBucket>>,
    inflight: Arc<DashSet<SymbolId>>,
    tokens_per_order: f64,
}

impl ExecGuards {
    pub fn new(capacity: u32, refill_per_sec: f64, tokens_per_order: f64) -> Self {
        Self {
            bucket: Arc::new(Mutex::new(TokenBucket::new(capacity, refill_per_sec))),
            inflight: Arc::new(DashSet::new()),
            tokens_per_order,
        }
    }

    pub fn with_binance_defaults() -> Self {
        Self::new(RATE_LIMIT_CAPACITY, RATE_LIMIT_REFILL_PER_SEC, 1.0)
    }

    pub fn enter(&self, symbol_id: SymbolId) -> Result<GuardPermit<'_>, GuardError> {
        if !self.inflight.insert(symbol_id) {
            return Err(GuardError::InFlight);
        }

        let mut bucket = self.bucket.lock().expect("token bucket mutex poisoned");
        if bucket.try_acquire(self.tokens_per_order) {
            drop(bucket);
            Ok(GuardPermit {
                guards: self,
                symbol_id,
                active: true,
            })
        } else {
            drop(bucket);
            self.inflight.remove(&symbol_id);
            Err(GuardError::RateLimited)
        }
    }

    fn release(&self, symbol_id: SymbolId) {
        self.inflight.remove(&symbol_id);
    }
}

pub struct GuardPermit<'a> {
    guards: &'a ExecGuards,
    symbol_id: SymbolId,
    active: bool,
}

impl Drop for GuardPermit<'_> {
    fn drop(&mut self) {
        if self.active {
            self.guards.release(self.symbol_id);
        }
    }
}

#[derive(Debug)]
pub enum GuardError {
    RateLimited,
    InFlight,
}

impl fmt::Display for GuardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GuardError::RateLimited => write!(f, "rate limited"),
            GuardError::InFlight => write!(f, "order already in flight"),
        }
    }
}

impl std::error::Error for GuardError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderError {
    FilterPriceTick,
    FilterLotStep,
    FilterMinNotional,
    Transient,
    Fatal,
}

impl OrderError {
    fn is_filter(self) -> bool {
        matches!(
            self,
            OrderError::FilterPriceTick | OrderError::FilterLotStep | OrderError::FilterMinNotional
        )
    }
}

#[derive(Debug, Clone)]
pub struct OrderSubmitError {
    pub kind: OrderError,
    pub detail: String,
}

impl OrderSubmitError {
    #[allow(clippy::too_many_arguments)]
    fn new(kind: OrderError, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }
}

impl fmt::Display for OrderSubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({:?})", self.detail, self.kind)
    }
}

#[derive(Debug)]
pub enum PlaceOrderError {
    MissingCredentials,
    InvalidPrice,
    InvalidQuantity,
    FiltersUnavailable { symbol: Symbol, symbol_id: SymbolId },
    Guard(GuardError),
    Order(OrderSubmitError),
}

impl fmt::Display for PlaceOrderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlaceOrderError::MissingCredentials => write!(f, "missing REST credentials"),
            PlaceOrderError::InvalidPrice => write!(f, "invalid price"),
            PlaceOrderError::InvalidQuantity => write!(f, "invalid quantity"),
            PlaceOrderError::FiltersUnavailable { symbol, .. } => {
                write!(f, "filters unavailable for symbol {}", symbol)
            }
            PlaceOrderError::Guard(err) => write!(f, "guard rejected order: {err}"),
            PlaceOrderError::Order(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for PlaceOrderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PlaceOrderError::Guard(err) => Some(err),
            PlaceOrderError::Order(_) => None,
            _ => None,
        }
    }
}

impl From<GuardError> for PlaceOrderError {
    fn from(value: GuardError) -> Self {
        PlaceOrderError::Guard(value)
    }
}

impl PlaceOrderError {
    fn order_kind(&self) -> Option<OrderError> {
        match self {
            PlaceOrderError::Order(err) => Some(err.kind),
            _ => None,
        }
    }

    fn detail(&self) -> Option<&str> {
        match self {
            PlaceOrderError::Order(err) => Some(&err.detail),
            _ => None,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn spawn_execution(
    config: &'static Config,
    server: &'static ServerSpec,
    clock: Arc<dyn Clock>,
    trigger_rx: SpscReceiver<TriggerEvent>,
    log_tx: Sender<LogMessage>,
    filter_cache: FilterCache,
    guards: ExecGuards,
    risk: RiskHandle,
) -> (thread::JoinHandle<()>, ExecutionHandle) {
    let (cmd_tx, cmd_rx) = unbounded();
    let engine_filter_cache = filter_cache.clone();
    let engine_guards = guards.clone();
    let engine_risk = risk.clone();
    let provider: Arc<dyn ExecutionProvider> = {
        let rest = RestExecutionProvider::new(config, clock.clone());
        let ws_client = WsOrderClient::spawn(
            config,
            clock.clone(),
            log_tx.clone(),
            server.execution_core,
            cmd_tx.clone(),
        );
        Arc::new(WsExecutionProvider::new(rest, ws_client))
    };
    let provider_for_engine = provider.clone();
    let clock_for_engine = clock.clone();
    let log_tx_engine = log_tx.clone();

    let handle = thread::Builder::new()
        .name(format!("execution-{:?}", server.id))
        .spawn(move || {
            affinity::bind_to_core(server.execution_core);
            let mut engine = ExecutionEngine::new(
                config,
                clock_for_engine,
                provider_for_engine,
                log_tx_engine,
                cmd_rx,
                engine_filter_cache,
                engine_guards,
                engine_risk,
            );
            engine.run(trigger_rx);
        })
        .expect("failed to spawn execution thread");

    (
        handle,
        ExecutionHandle {
            command_tx: cmd_tx,
            filter_cache,
            guards,
            risk,
        },
    )
}

pub struct TargetInfo {
    pub order_id: Option<String>,
    pub qty: Decimal,
}

pub struct ExecutionHandle {
    command_tx: Sender<ExecutionCommand>,
    filter_cache: FilterCache,
    guards: ExecGuards,
    risk: RiskHandle,
}

impl Clone for ExecutionHandle {
    fn clone(&self) -> Self {
        Self {
            command_tx: self.command_tx.clone(),
            filter_cache: self.filter_cache.clone(),
            guards: self.guards.clone(),
            risk: self.risk.clone(),
        }
    }
}
impl ExecutionHandle {
    pub fn reset_metrics_daily(&self) -> anyhow::Result<()> {
        let (ack_tx, ack_rx) = bounded(1);
        self.command_tx
            .send(ExecutionCommand::ResetDaily { ack: ack_tx })
            .map_err(|err| anyhow::anyhow!("failed to send execution command: {err}"))?;
        ack_rx
            .recv()
            .map_err(|err| anyhow::anyhow!("execution command acknowledgement failed: {err}"))?;
        Ok(())
    }

    pub fn install_filters(&self, snapshot: HashMap<SymbolId, SymbolFilters>) {
        self.filter_cache.install(snapshot);
    }

    pub fn record_limit_fill(&self, symbol: Symbol) {
        let _ = self
            .command_tx
            .send(ExecutionCommand::RecordLimitFill { symbol });
    }

    pub fn submit_close_with_cancel(
        &self,
        symbol: Symbol,
        expected_qty: Decimal,
        reason: ExitReason,
        target: TargetInfo,
    ) -> Result<(), OrderSubmitError> {
        let (ack_tx, ack_rx) = bounded(1);
        self.command_tx
            .send(ExecutionCommand::CloseWithCancel {
                symbol,
                expected_qty,
                reason,
                target,
                ack: ack_tx,
            })
            .map_err(|_| {
                OrderSubmitError::new(OrderError::Fatal, "execution command channel closed")
            })?;
        ack_rx.recv().unwrap_or_else(|_| {
            Err(OrderSubmitError::new(
                OrderError::Fatal,
                "execution close acknowledgement failed",
            ))
        })
    }
}

enum ExecutionCommand {
    ResetDaily {
        ack: Sender<()>,
    },
    RecordLimitFill {
        symbol: Symbol,
    },
    CloseWithCancel {
        symbol: Symbol,
        expected_qty: Decimal,
        reason: ExitReason,
        target: TargetInfo,
        ack: Sender<Result<(), OrderSubmitError>>,
    },
    TpAdjust {
        symbol: Symbol,
        old_order_id: String,
        new_order_id: String,
        new_price: Decimal,
        ticks_diff: u32,
    },
}

struct ExecutionEngine {
    config: &'static Config,
    clock: Arc<dyn Clock>,
    log_tx: Sender<LogMessage>,
    latency_budget_ns: u64,
    filter_cache: FilterCache,
    guards: ExecGuards,
    risk: RiskHandle,
    metrics: LatencyMetrics,
    command_rx: Receiver<ExecutionCommand>,
    provider: Arc<dyn ExecutionProvider>,
    open_limits: HashMap<Symbol, Option<String>>,
    target_notional: Decimal,
}

impl ExecutionEngine {
    #[allow(clippy::too_many_arguments)]
    fn new(
        config: &'static Config,
        clock: Arc<dyn Clock>,
        provider: Arc<dyn ExecutionProvider>,
        log_tx: Sender<LogMessage>,
        command_rx: Receiver<ExecutionCommand>,
        filter_cache: FilterCache,
        guards: ExecGuards,
        risk: RiskHandle,
    ) -> Self {
        let metrics = LatencyMetrics::new(config.strategy.enable_metrics_test_only, clock.clone());

        let latency_budget_ns = config.trigger.latency_budget.as_nanos() as u64;
        let target_notional =
            Decimal::from_f64(config.execution.order_quote_size_usdt).unwrap_or(Decimal::ZERO);

        let engine = Self {
            config,
            clock,
            log_tx,
            latency_budget_ns,
            filter_cache,
            guards,
            risk,
            metrics,
            command_rx,
            provider,
            open_limits: HashMap::new(),
            target_notional,
        };
        tracing::debug!(target: "bot", "[BOOT] execution runtime ready");
        engine
    }

    fn run(&mut self, trigger_rx: SpscReceiver<TriggerEvent>) {
        let trigger_rx = trigger_rx.into_inner();
        loop {
            crossbeam_channel::select! {
                recv(self.command_rx) -> cmd => match cmd {
                    Ok(command) => self.handle_command(command),
                    Err(_) => break,
                },
                recv(trigger_rx) -> msg => match msg {
                    Ok(trigger) => self.handle_trigger(trigger),
                    Err(_) => break,
                },
            }
        }
    }

    fn handle_command(&mut self, command: ExecutionCommand) {
        match command {
            ExecutionCommand::ResetDaily { ack } => {
                self.metrics.reset_daily();
                self.open_limits.clear();
                let _ = ack.send(());
            }
            ExecutionCommand::RecordLimitFill { symbol } => {
                self.record_limit_fill(symbol);
            }
            ExecutionCommand::CloseWithCancel {
                symbol,
                expected_qty,
                reason,
                target,
                ack,
            } => {
                let result = self.handle_close_with_cancel(symbol, expected_qty, reason, target);
                let _ = ack.send(result);
            }
            ExecutionCommand::TpAdjust {
                symbol,
                old_order_id,
                new_order_id,
                new_price,
                ticks_diff,
            } => {
                self.handle_tp_adjust(symbol, old_order_id, new_order_id, new_price, ticks_diff);
            }
        }
    }

    fn record_limit_fill(&mut self, symbol: Symbol) {
        self.open_limits.remove(&symbol);
        self.metrics.inc_exit_limit_tp();
        self.metrics.maybe_flush();
    }

    fn handle_tp_adjust(
        &mut self,
        symbol: Symbol,
        old_order_id: String,
        new_order_id: String,
        _new_price: Decimal,
        ticks_diff: u32,
    ) {
        self.open_limits.insert(symbol, Some(new_order_id.clone()));

        let slot_label = slot_from_client_id(&new_order_id)
            .or_else(|| slot_from_client_id(&old_order_id))
            .unwrap_or_else(|| "NA".to_string());
        let _ = self.log_tx.send(LogMessage::Info(
            format!(
                "[TP_ADJUST] sym={} old={} new={} ticks={} slot={}",
                symbol, old_order_id, new_order_id, ticks_diff, slot_label
            )
            .into(),
        ));
    }

    fn handle_close_with_cancel(
        &mut self,
        symbol: Symbol,
        expected_qty: Decimal,
        reason: ExitReason,
        target: TargetInfo,
    ) -> Result<(), OrderSubmitError> {
        let mut remaining = if expected_qty > Decimal::ZERO {
            expected_qty
        } else {
            Decimal::ZERO
        };

        if let Some(ref order_id) = target.order_id {
            self.metrics.inc_cancel_attempts();

            let cancel_once = |engine: &mut ExecutionEngine| -> Result<(), ProviderError> {
                match engine.provider.cancel_order(symbol, order_id) {
                    Ok(CancelOutcome::Canceled) => {
                        engine.metrics.inc_cancel_success();
                        Ok(())
                    }
                    Ok(CancelOutcome::NotFound) => {
                        engine.metrics.inc_cancel_not_found();
                        Ok(())
                    }
                    Ok(CancelOutcome::Unknown) => {
                        engine.metrics.inc_cancel_failed();
                        Ok(())
                    }
                    Err(err) => {
                        if err.is_transient() {
                            return Err(err);
                        }
                        if err.is_not_found() {
                            engine.metrics.inc_cancel_not_found();
                        } else {
                            engine.metrics.inc_cancel_failed();
                        }
                        Ok(())
                    }
                }
            };

            if cancel_once(self).is_err() {
                thread::sleep(Duration::from_millis(40));
                let _ = cancel_once(self);
            }

            if let Ok(fill) = self.provider.query_order(symbol, order_id) {
                let filled = if fill.cum_filled > Decimal::ZERO {
                    fill.cum_filled
                } else {
                    Decimal::ZERO
                };
                let mut rem = if target.qty > filled {
                    target.qty - filled
                } else {
                    Decimal::ZERO
                };
                if rem < Decimal::ZERO {
                    rem = Decimal::ZERO;
                }
                let extra = if expected_qty > target.qty {
                    expected_qty - target.qty
                } else {
                    Decimal::ZERO
                };
                let mut candidate = rem + extra;
                if candidate < Decimal::ZERO {
                    candidate = Decimal::ZERO;
                }
                if candidate < remaining {
                    remaining = candidate;
                }
            }
        }

        self.open_limits.remove(&symbol);

        let symbol_id = symbol_id(symbol);
        let filters = self.filter_cache.get(symbol_id).ok_or_else(|| {
            OrderSubmitError::new(
                OrderError::Fatal,
                format!("filters unavailable for {}", symbol),
            )
        })?;

        let aligned_qty = floor_to_step(remaining, filters.step);
        if aligned_qty <= Decimal::ZERO {
            return Ok(());
        }

        if filters.min_notional > Decimal::ZERO {
            let approx_notional = self.approx_close_notional(aligned_qty, &target);
            if approx_notional > Decimal::ZERO && approx_notional < filters.min_notional {
                return Ok(());
            }
        }

        self.submit_market_close(symbol, filters, aligned_qty, reason)
    }

    fn submit_market_close(
        &mut self,
        symbol: Symbol,
        filters: SymbolFilters,
        quantity: Decimal,
        reason: ExitReason,
    ) -> Result<(), OrderSubmitError> {
        let symbol_id = symbol_id(symbol);
        let _permit = self.guards.enter(symbol_id).map_err(|err| match err {
            GuardError::RateLimited => OrderSubmitError::new(
                OrderError::Transient,
                format!("rate limited closing order for {}", symbol),
            ),
            GuardError::InFlight => OrderSubmitError::new(
                OrderError::Transient,
                format!("close already in-flight for {}", symbol),
            ),
        })?;

        if quantity <= Decimal::ZERO {
            return Ok(());
        }

        let qty_scale = filters.step.scale();
        let price_scale = filters.tick.scale();
        let now_ns = self.clock.monotonic_now_ns();

        self.provider.submit_order(&ProviderOrder {
            symbol,
            side: "SELL",
            order_type: "MARKET",
            quantity,
            quantity_scale: qty_scale,
            price: None,
            price_scale,
            client_order_id: format!("CLOSE-{}-{}", symbol, now_ns),
            trigger_ts_mono_ns: now_ns,
        })?;

        let qty_str = format_decimal(quantity, qty_scale);
        let reason_label = match reason {
            ExitReason::StopLoss => {
                self.metrics.inc_exit_market_sl();
                "stop"
            }
            ExitReason::ReturnToEntry => {
                self.metrics.inc_exit_market_bounce();
                "bounce"
            }
            ExitReason::TakeProfitLimit => {
                self.metrics.inc_exit_limit_tp();
                "tp"
            }
        };

        let _ = self.log_tx.send(LogMessage::Info(
            format!(
                "[SELL] market close reason={reason_label} {} qty={qty_str}",
                symbol
            )
            .into(),
        ));

        self.metrics.maybe_flush();
        Ok(())
    }

    fn approx_close_notional(&self, qty: Decimal, target: &TargetInfo) -> Decimal {
        if qty <= Decimal::ZERO || self.target_notional <= Decimal::ZERO {
            return Decimal::ZERO;
        }

        if target.qty <= Decimal::ZERO {
            return self.target_notional;
        }

        let mut ratio = qty / target.qty;
        if ratio < Decimal::ZERO {
            ratio = Decimal::ZERO;
        }
        if ratio > Decimal::ONE {
            ratio = Decimal::ONE;
        }

        (self.target_notional * ratio).normalize()
    }

    fn handle_trigger(&mut self, trigger: TriggerEvent) {
        let now_ns = self.clock.monotonic_now_ns();
        let elapsed_ns = now_ns.saturating_sub(trigger.signal_ts_mono_ns);

        if elapsed_ns > self.latency_budget_ns {
            let _ = self.log_tx.send(
                MetricEvent::TriggerDropLate {
                    symbol: trigger.symbol,
                    latency_ns: elapsed_ns,
                }
                .into(),
            );
            return;
        }

        let result = match self.config.execution.mode {
            ExecutionMode::Shadow => self.execute_shadow(&trigger),
            ExecutionMode::Live => self.place_buy_and_limit_live(&trigger),
        };

        match result {
            Ok(order_ids) => {
                self.open_limits
                    .insert(trigger.symbol, order_ids.limit.clone());
                let send_ns = self.clock.monotonic_now_ns();
                let _ = self.log_tx.send(
                    MetricEvent::BuyOk {
                        symbol: trigger.symbol,
                    }
                    .into(),
                );
                let _ = self.log_tx.send(
                    MetricEvent::TickToSend(TickToSendMetric {
                        symbol: trigger.symbol,
                        price_to_send_ns: send_ns.saturating_sub(trigger.signal_ts_mono_ns),
                        trigger_to_send_ns: send_ns.saturating_sub(trigger.trigger_ts_mono_ns),
                    })
                    .into(),
                );
                self.metrics.record_signal_to_buy(Duration::from_nanos(
                    send_ns.saturating_sub(trigger.signal_ts_mono_ns),
                ));
                if order_ids.limit.is_some() {
                    self.metrics.record_buy_to_limit(Duration::from_millis(0));
                    self.metrics.record_signal_to_limit(Duration::from_nanos(
                        send_ns.saturating_sub(trigger.signal_ts_mono_ns),
                    ));
                }
                self.metrics.maybe_flush();
            }
            Err(err) => match err {
                PlaceOrderError::Guard(GuardError::RateLimited) => {
                    let _ = self.log_tx.send(
                        MetricEvent::QueueDropTrigger {
                            symbol: trigger.symbol,
                        }
                        .into(),
                    );
                }
                PlaceOrderError::FiltersUnavailable { symbol_id, .. } => {
                    let _ = self.log_tx.send(LogMessage::Warn(
                        format!(
                            "[WARN] missing filters {} symbol_id={}",
                            trigger.symbol, symbol_id
                        )
                        .into(),
                    ));
                    let _ = self.log_tx.send(
                        MetricEvent::BuyErr {
                            symbol: trigger.symbol,
                        }
                        .into(),
                    );
                }
                PlaceOrderError::Guard(GuardError::InFlight) => {
                    let _ = self.log_tx.send(LogMessage::Warn(
                        format!("[WARN] guard_inflight {}", trigger.symbol).into(),
                    ));
                    let _ = self.log_tx.send(
                        MetricEvent::BuyErr {
                            symbol: trigger.symbol,
                        }
                        .into(),
                    );
                }
                other => {
                    let detail = other
                        .detail()
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| other.to_string());
                    let _ = self.log_tx.send(LogMessage::Error(format!(
                        "[EXEC] order_error {}: {}",
                        trigger.symbol, detail
                    )));
                    let _ = self.log_tx.send(
                        MetricEvent::BuyErr {
                            symbol: trigger.symbol,
                        }
                        .into(),
                    );
                }
            },
        }
    }

    fn execute_shadow(&self, trigger: &TriggerEvent) -> ExecResult {
        let _ = self.log_tx.send(LogMessage::Info(
            format!(
                "[BUY] SHADOW {} price={:.6} ret60s={:.2}% notional={}",
                trigger.symbol,
                trigger.price_now,
                trigger.ret_60s * 100.0,
                trigger.target_notional
            )
            .into(),
        ));
        Ok(OrderIds::default())
    }

    fn place_buy_and_limit_live(&mut self, trigger: &TriggerEvent) -> ExecResult {
        if self.config.credentials.rest_api_key.is_empty()
            || self.config.credentials.rest_api_secret.is_empty()
        {
            return Err(PlaceOrderError::MissingCredentials);
        }

        let symbol = trigger.symbol;
        let symbol_id = symbol_id(symbol);
        let mut filters = self
            .filter_cache
            .get(symbol_id)
            .ok_or(PlaceOrderError::FiltersUnavailable { symbol, symbol_id })?;

        let _permit = self
            .guards
            .enter(symbol_id)
            .map_err(PlaceOrderError::from)?;

        let mut auto_heal_used = false;

        loop {
            match self.submit_with_filters(trigger, filters) {
                Ok(outcome) => {
                    if auto_heal_used {
                        self.metrics.inc_auto_heal_success();
                        if self.config.strategy.enable_metrics_test_only {
                            tracing::debug!(target: "bot", "[HEAL] success {}", symbol);
                        }
                    }

                    if self.config.strategy.enable_metrics_test_only {
                        let qty_str = format_decimal(outcome.qty, outcome.qty_scale);
                        let tp_str = format_decimal(outcome.tp_price, outcome.price_scale);
                        let _ = self.log_tx.send(LogMessage::Info(
                            format!("[BUY] {} qty={qty_str} type=MARKET", symbol).into(),
                        ));
                        let _ = self.log_tx.send(LogMessage::Info(
                            format!("[SELL] {} qty={qty_str} tp={tp_str}", symbol).into(),
                        ));
                    }

                    return Ok(outcome.ids);
                }
                Err(mut err) => {
                    if !auto_heal_used {
                        if err
                            .order_kind()
                            .map(|kind| kind.is_filter())
                            .unwrap_or(false)
                        {
                            self.metrics.inc_auto_heal_attempts();
                            auto_heal_used = true;
                            if self.config.strategy.enable_metrics_test_only {
                                tracing::debug!(target: "bot", "[HEAL] refresh {}", symbol);
                            }
                            match self.filter_cache.refresh_symbol(symbol.as_str()) {
                                Ok(updated) => {
                                    filters = updated;
                                    continue;
                                }
                                Err(refresh_err) => {
                                    err = PlaceOrderError::Order(OrderSubmitError::new(
                                        OrderError::Fatal,
                                        format!("filter refresh failed: {refresh_err:?}"),
                                    ));
                                }
                            }
                        } else if matches!(err.order_kind(), Some(OrderError::Fatal)) {
                            drop(_permit);
                            self.disable_symbol(symbol, symbol_id, DisableReason::FatalError);
                            return Err(err);
                        } else {
                            return Err(err);
                        }
                    }

                    drop(_permit);
                    self.disable_symbol(symbol, symbol_id, DisableReason::OrderReject);
                    return Err(err);
                }
            }
        }
    }

    fn submit_with_filters(
        &self,
        trigger: &TriggerEvent,
        filters: SymbolFilters,
    ) -> Result<OrderAttempt, PlaceOrderError> {
        let symbol = trigger.symbol;
        let price_now =
            Decimal::from_f64(trigger.price_now).ok_or(PlaceOrderError::InvalidPrice)?;
        if price_now <= Decimal::ZERO {
            return Err(PlaceOrderError::InvalidPrice);
        }
        if filters.step <= Decimal::ZERO || filters.tick <= Decimal::ZERO {
            return Err(PlaceOrderError::InvalidQuantity);
        }

        let raw_qty = trigger.target_notional / price_now;
        let qty = floor_to_step(raw_qty, filters.step);
        if qty <= Decimal::ZERO {
            return Err(PlaceOrderError::Order(OrderSubmitError::new(
                OrderError::FilterLotStep,
                format!("computed quantity zero after alignment for {}", symbol),
            )));
        }

        let actual_notional = qty * price_now;
        if actual_notional < filters.min_notional {
            return Err(PlaceOrderError::Order(OrderSubmitError::new(
                OrderError::FilterMinNotional,
                format!(
                    "notional below minimum for {}: min={} actual={}",
                    symbol, filters.min_notional, actual_notional
                ),
            )));
        }

        let tp_price = align_tick_up(
            tp_target_px(
                price_now,
                self.config.strategy.tp_pct,
                self.config.strategy.maker_fee_pct,
                self.config.strategy.taker_fee_pct,
            ),
            filters.tick,
        );
        let qty_scale = filters.step.scale();
        let price_scale = filters.tick.scale();

        let buy_order = ProviderOrder {
            symbol,
            side: "BUY",
            order_type: "MARKET",
            quantity: qty,
            quantity_scale: qty_scale,
            price: None,
            price_scale,
            client_order_id: format!(
                "{}-buy-{}",
                trigger.slot.label(),
                trigger.trigger_ts_mono_ns
            ),
            trigger_ts_mono_ns: trigger.trigger_ts_mono_ns,
        };

        let limit_order = ProviderOrder {
            symbol,
            side: "SELL",
            order_type: "LIMIT",
            quantity: qty,
            quantity_scale: qty_scale,
            price: Some(tp_price),
            price_scale,
            client_order_id: format!("{}-tp-{}", trigger.slot.label(), trigger.trigger_ts_mono_ns),
            trigger_ts_mono_ns: trigger.trigger_ts_mono_ns,
        };

        let adjust_spec = if self.config.tp_adjust.enabled {
            Some(TpAdjustSpec {
                min_diff_ticks: self.config.tp_adjust.min_diff_ticks,
                timeout: self.config.tp_adjust.timeout,
                tick_size: filters.tick,
                tp_pct: self.config.strategy.tp_pct,
                maker_fee_pct: self.config.strategy.maker_fee_pct,
                taker_fee_pct: self.config.strategy.taker_fee_pct,
                quantity: qty,
                quantity_scale: qty_scale,
                price_scale,
                current_tp: tp_price,
                client_order_id: limit_order.client_order_id.clone(),
                symbol,
            })
        } else {
            None
        };

        let ids = self
            .provider
            .submit_pair(&buy_order, &limit_order, adjust_spec)
            .map_err(PlaceOrderError::Order)?;

        Ok(OrderAttempt {
            ids,
            qty,
            tp_price,
            qty_scale,
            price_scale,
        })
    }

    fn disable_symbol(&mut self, symbol: Symbol, symbol_id: SymbolId, reason: DisableReason) {
        self.risk.disable_for_today(symbol_id, reason);
        self.metrics.inc_disable_for_today();
        let reason_label = match reason {
            DisableReason::OrderReject => "OrderReject",
            DisableReason::FatalError => "FatalError",
            DisableReason::DailyLoss => "DailyLoss",
        };
        let _ = self.log_tx.send(LogMessage::Warn(
            format!("[RISK] disabled symbol={} reason={reason_label}", symbol).into(),
        ));
    }
}

fn floor_to_step(qty: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return qty;
    }
    let steps = (qty / step).floor();
    (steps * step).normalize()
}

fn align_tick_up(price: Decimal, tick: Decimal) -> Decimal {
    if tick <= Decimal::ZERO {
        return price;
    }
    let steps = (price / tick).ceil();
    (steps * tick).normalize()
}

fn slot_from_client_id(client_id: &str) -> Option<String> {
    client_id.split('-').next().map(|value| value.to_string())
}

fn format_decimal(value: Decimal, decimals: u32) -> String {
    value
        .round_dp_with_strategy(decimals, rust_decimal::RoundingStrategy::ToZero)
        .normalize()
        .to_string()
}

struct OrderAttempt {
    ids: OrderIds,
    qty: Decimal,
    tp_price: Decimal,
    qty_scale: u32,
    price_scale: u32,
}

fn classify_order_error(status: StatusCode, body: &str) -> OrderError {
    let upper = body.to_ascii_uppercase();
    if upper.contains("PRICE_FILTER") {
        return OrderError::FilterPriceTick;
    }
    if upper.contains("LOT_SIZE") {
        return OrderError::FilterLotStep;
    }
    if upper.contains("MIN_NOTIONAL") {
        return OrderError::FilterMinNotional;
    }

    if status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::SERVICE_UNAVAILABLE
        || status == StatusCode::GATEWAY_TIMEOUT
        || status == StatusCode::REQUEST_TIMEOUT
        || status.is_server_error()
    {
        return OrderError::Transient;
    }

    OrderError::Fatal
}

fn map_reqwest_error(err: reqwest::Error) -> ProviderError {
    if err.is_timeout() || err.is_connect() {
        ProviderError::transient(err.to_string())
    } else {
        ProviderError::fatal(err.to_string())
    }
}

fn is_unknown_order(body: &str) -> bool {
    body.to_ascii_uppercase().contains("UNKNOWN_ORDER")
}
struct RestExecutionProvider {
    config: &'static Config,
    client: Client,
    clock: Arc<dyn Clock>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RestQueryOrderResponse {
    #[serde(default)]
    status: String,
    #[serde(default)]
    executed_qty: String,
}

impl RestExecutionProvider {
    #[allow(clippy::too_many_arguments)]
    fn new(config: &'static Config, clock: Arc<dyn Clock>) -> Self {
        let client = Client::builder()
            .tcp_nodelay(true)
            .timeout(config.execution.request_timeout)
            .build()
            .expect("failed to build reqwest client");

        Self {
            config,
            client,
            clock,
        }
    }

    fn sign_payload(&self, payload: &str) -> Result<String, OrderSubmitError> {
        let secret = self.config.credentials.rest_api_secret;
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
            .map_err(|err| OrderSubmitError::new(OrderError::Fatal, err.to_string()))?;
        mac.update(payload.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }

    fn sign_payload_provider(&self, payload: &str) -> Result<String, ProviderError> {
        self.sign_payload(payload)
            .map_err(|err| ProviderError::fatal(err.to_string()))
    }

    fn signed_order_url(&self, symbol: Symbol, order_id: &str) -> Result<String, ProviderError> {
        let recv_window = self.config.execution.recv_window_ms;
        let timestamp_ms = self.clock.unix_now_ns() / 1_000_000;
        let payload = format!(
            "symbol={symbol}&origClientOrderId={order_id}&recvWindow={recv_window}&timestamp={timestamp}",
            symbol = symbol.as_str(),
            order_id = order_id,
            recv_window = recv_window,
            timestamp = timestamp_ms,
        );
        let signature = self.sign_payload_provider(&payload)?;
        Ok(format!(
            "{base}/api/v3/order?{payload}&signature={signature}",
            base = self.config.transport.rest_base_url,
            payload = payload,
            signature = signature,
        ))
    }

    fn http_error(&self, op: &str, status: StatusCode, body: String) -> ProviderError {
        let detail = format!("[REST] {op} status={status} body={body}");
        if status == StatusCode::TOO_MANY_REQUESTS
            || status == StatusCode::SERVICE_UNAVAILABLE
            || status == StatusCode::GATEWAY_TIMEOUT
            || status == StatusCode::REQUEST_TIMEOUT
            || status.is_server_error()
        {
            ProviderError::transient(detail)
        } else {
            ProviderError::fatal(detail)
        }
    }
}

impl ExecutionProvider for RestExecutionProvider {
    fn submit_pair(
        &self,
        buy: &ProviderOrder,
        limit: &ProviderOrder,
        _adjust: Option<TpAdjustSpec>,
    ) -> Result<OrderIds, OrderSubmitError> {
        let buy_id = self.submit_order(buy)?;
        let limit_id = self.submit_order(limit)?;
        Ok(OrderIds {
            buy: Some(buy_id),
            limit: Some(limit_id),
        })
    }

    fn submit_order(&self, order: &ProviderOrder) -> Result<String, OrderSubmitError> {
        let api_key = self.config.credentials.rest_api_key;
        let endpoint = format!("{}/api/v3/order", self.config.transport.rest_base_url);
        let recv_window = self.config.execution.recv_window_ms;
        let timestamp_ms = self.clock.unix_now_ns() / 1_000_000;
        let symbol_str = order.symbol.as_str();
        let qty_str = format_decimal(order.quantity, order.quantity_scale);

        let mut payload = format!(
            "symbol={symbol}&side={side}&type={order_type}&newClientOrderId={client_id}&newOrderRespType=ACK&recvWindow={recv_window}&timestamp={timestamp}&quantity={qty}",
            symbol = symbol_str,
            side = order.side,
            order_type = order.order_type,
            client_id = order.client_order_id,
            recv_window = recv_window,
            timestamp = timestamp_ms,
            qty = qty_str,
        );

        if let Some(price_val) = order.price {
            payload.push_str(&format!(
                "&price={}&timeInForce=GTC",
                format_decimal(price_val, order.price_scale)
            ));
        }

        let signature = self.sign_payload(&payload)?;
        let body = format!("{payload}&signature={signature}");

        let response = self
            .client
            .post(&endpoint)
            .header("X-MBX-APIKEY", api_key)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .map_err(|err| {
                if err.is_timeout() || err.is_connect() {
                    OrderSubmitError::new(OrderError::Transient, err.to_string())
                } else {
                    OrderSubmitError::new(OrderError::Fatal, err.to_string())
                }
            })?;

        let status = response.status();
        if status.is_success() {
            return Ok(order.client_order_id.clone());
        }

        let body = response.text().unwrap_or_default();
        let kind = classify_order_error(status, &body);
        Err(OrderSubmitError::new(
            kind,
            format!("binance rejected order: status={status} body={body}"),
        ))
    }

    fn cancel_order(&self, symbol: Symbol, order_id: &str) -> Result<CancelOutcome, ProviderError> {
        let url = self.signed_order_url(symbol, order_id)?;
        let response = self
            .client
            .delete(&url)
            .header("X-MBX-APIKEY", self.config.credentials.rest_api_key)
            .send()
            .map_err(map_reqwest_error)?;
        let status = response.status();
        if status.is_success() {
            return Ok(CancelOutcome::Canceled);
        }
        let body = response.text().unwrap_or_default();
        if status == StatusCode::NOT_FOUND || is_unknown_order(&body) {
            return Ok(CancelOutcome::NotFound);
        }
        Err(self.http_error("cancel", status, body))
    }

    fn query_order(&self, symbol: Symbol, order_id: &str) -> Result<OrderFill, ProviderError> {
        let url = self.signed_order_url(symbol, order_id)?;
        let response = self
            .client
            .get(&url)
            .header("X-MBX-APIKEY", self.config.credentials.rest_api_key)
            .send()
            .map_err(map_reqwest_error)?;
        let status = response.status();
        if status.is_success() {
            let payload: RestQueryOrderResponse = response
                .json()
                .map_err(|err| ProviderError::fatal(err.to_string()))?;
            let qty = Decimal::from_str(&payload.executed_qty).unwrap_or(Decimal::ZERO);
            return Ok(OrderFill {
                cum_filled: qty,
                status: OrderStatus::from_str(&payload.status),
            });
        }
        let body = response.text().unwrap_or_default();
        if status == StatusCode::NOT_FOUND || is_unknown_order(&body) {
            return Err(ProviderError::not_found(format!(
                "order not found {} {}",
                symbol, order_id
            )));
        }
        Err(self.http_error("query", status, body))
    }
}

struct WsExecutionProvider {
    rest: RestExecutionProvider,
    ws: WsOrderClient,
}

impl WsExecutionProvider {
    fn new(rest: RestExecutionProvider, ws: WsOrderClient) -> Self {
        Self { rest, ws }
    }
}

impl ExecutionProvider for WsExecutionProvider {
    fn submit_pair(
        &self,
        buy: &ProviderOrder,
        limit: &ProviderOrder,
        adjust: Option<TpAdjustSpec>,
    ) -> Result<OrderIds, OrderSubmitError> {
        self.ws.place_pair(buy.clone(), limit.clone(), adjust)
    }

    fn submit_order(&self, order: &ProviderOrder) -> Result<String, OrderSubmitError> {
        self.rest.submit_order(order)
    }

    fn cancel_order(&self, symbol: Symbol, order_id: &str) -> Result<CancelOutcome, ProviderError> {
        self.rest.cancel_order(symbol, order_id)
    }

    fn query_order(&self, symbol: Symbol, order_id: &str) -> Result<OrderFill, ProviderError> {
        self.rest.query_order(symbol, order_id)
    }
}
