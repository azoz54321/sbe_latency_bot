use std::collections::HashMap;
#[cfg(feature = "test-mode")]
use std::collections::HashSet;
use std::fmt;
use std::thread;
use std::time::Duration;

use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
#[cfg(not(feature = "test-mode"))]
use hmac::{Hmac, Mac};
#[cfg(not(feature = "test-mode"))]
use reqwest::blocking::Client;
#[cfg(not(feature = "test-mode"))]
use reqwest::StatusCode;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
#[cfg(not(feature = "test-mode"))]
use sha2::Sha256;

use dashmap::DashSet;
#[cfg(feature = "test-mode")]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::affinity;
use crate::channels::SpscReceiver;
use crate::clock::Clock;
use crate::config::{Config, ExecutionMode, ServerSpec};
use crate::filters::{FilterCache, SymbolFilters};
#[cfg(feature = "test-mode")]
use crate::metrics::LatencyCountersSnapshot;
use crate::metrics::LatencyMetrics;
use crate::rate_limit::TokenBucket;
use crate::risk::{DisableReason, RiskHandle};

use crate::types::{
    symbol_id, LogMessage, MetricEvent, OrderIds, Symbol, SymbolId, TickToSendMetric, TriggerEvent,
};

#[cfg(not(feature = "test-mode"))]
type HmacSha256 = Hmac<Sha256>;

const RATE_LIMIT_CAPACITY: u32 = 20;
const RATE_LIMIT_REFILL_PER_SEC: f64 = 20.0;

type ExecResult = Result<OrderIds, PlaceOrderError>;

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

pub trait ExecutionProvider: Send + Sync {
    fn submit_order(&self, order: &ProviderOrder) -> Result<String, OrderSubmitError>;
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
    #[cfg(feature = "test-mode")] provider_events: Option<Sender<ProviderEvent>>,
) -> (thread::JoinHandle<()>, ExecutionHandle) {
    let (cmd_tx, cmd_rx) = unbounded();
    let engine_filter_cache = filter_cache.clone();
    let engine_guards = guards.clone();
    let engine_risk = risk.clone();
    #[cfg(feature = "test-mode")]
    let fake_provider = Arc::new(match provider_events {
        Some(tx) => FakeExecutionProvider::with_event_sink(clock.clone(), tx),
        None => FakeExecutionProvider::new(clock.clone()),
    });
    #[cfg(feature = "test-mode")]
    let provider: Arc<dyn ExecutionProvider> = fake_provider.clone();
    #[cfg(not(feature = "test-mode"))]
    let provider: Arc<dyn ExecutionProvider> =
        Arc::new(RestExecutionProvider::new(config, clock.clone()));
    let provider_for_engine = provider.clone();
    let clock_for_engine = clock.clone();

    let handle = thread::Builder::new()
        .name(format!("execution-{:?}", server.id))
        .spawn(move || {
            affinity::bind_to_core(server.execution_core);
            let mut engine = ExecutionEngine::new(
                config,
                clock_for_engine,
                provider_for_engine,
                log_tx,
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
            #[cfg(feature = "test-mode")]
            fake_provider,
        },
    )
}

pub struct ExecutionHandle {
    command_tx: Sender<ExecutionCommand>,
    filter_cache: FilterCache,
    guards: ExecGuards,
    risk: RiskHandle,
    #[cfg(feature = "test-mode")]
    fake_provider: Arc<FakeExecutionProvider>,
}

impl Clone for ExecutionHandle {
    fn clone(&self) -> Self {
        Self {
            command_tx: self.command_tx.clone(),
            filter_cache: self.filter_cache.clone(),
            guards: self.guards.clone(),
            risk: self.risk.clone(),
            #[cfg(feature = "test-mode")]
            fake_provider: self.fake_provider.clone(),
        }
    }
}
impl ExecutionHandle {
    #[cfg(feature = "test-mode")]
    pub fn fake_provider(&self) -> Arc<FakeExecutionProvider> {
        self.fake_provider.clone()
    }

    #[cfg(feature = "test-mode")]
    pub fn metrics_snapshot(&self) -> LatencyCountersSnapshot {
        let (tx, rx) = bounded(1);
        self.command_tx
            .send(ExecutionCommand::SnapshotMetrics { tx })
            .expect("execution command channel disconnected");
        rx.recv().expect("metrics snapshot channel closed")
    }

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
}

enum ExecutionCommand {
    ResetDaily {
        ack: Sender<()>,
    },
    #[cfg(feature = "test-mode")]
    SnapshotMetrics {
        tx: Sender<LatencyCountersSnapshot>,
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
        };
        let _ = engine
            .log_tx
            .send(LogMessage::Info("[BOOT] execution runtime ready".into()));
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
                let _ = ack.send(());
            }
            #[cfg(feature = "test-mode")]
            ExecutionCommand::SnapshotMetrics { tx } => {
                let _ = tx.send(self.metrics.counters_snapshot());
            }
        }
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
                self.metrics.maybe_flush(&self.log_tx);
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
                            let _ = self.log_tx.send(LogMessage::Info(
                                format!("[HEAL] success {}", symbol).into(),
                            ));
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
                                let _ = self.log_tx.send(LogMessage::Info(
                                    format!("[HEAL] refresh {}", symbol).into(),
                                ));
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

        let tp_multiplier = Decimal::from_i128_with_scale(110, 2);
        let tp_price = align_tick_up(price_now * tp_multiplier, filters.tick);
        let qty_scale = filters.step.scale();
        let price_scale = filters.tick.scale();

        let buy_order = self
            .provider
            .submit_order(&ProviderOrder {
                symbol,
                side: "BUY",
                order_type: "MARKET",
                quantity: qty,
                quantity_scale: qty_scale,
                price: None,
                price_scale,
                client_order_id: format!("BUY-{}-{}", symbol, trigger.trigger_ts_mono_ns),
                trigger_ts_mono_ns: trigger.trigger_ts_mono_ns,
            })
            .map_err(PlaceOrderError::Order)?;

        let limit_order = self
            .provider
            .submit_order(&ProviderOrder {
                symbol,
                side: "SELL",
                order_type: "LIMIT",
                quantity: qty,
                quantity_scale: qty_scale,
                price: Some(tp_price),
                price_scale,
                client_order_id: format!("SELL-{}-{}", symbol, trigger.trigger_ts_mono_ns),
                trigger_ts_mono_ns: trigger.trigger_ts_mono_ns,
            })
            .map_err(PlaceOrderError::Order)?;

        Ok(OrderAttempt {
            ids: OrderIds {
                buy: Some(buy_order),
                limit: Some(limit_order),
            },
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

#[cfg(not(feature = "test-mode"))]
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
#[cfg(not(feature = "test-mode"))]
struct RestExecutionProvider {
    config: &'static Config,
    client: Client,
    clock: Arc<dyn Clock>,
}

#[cfg(not(feature = "test-mode"))]
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
}

#[cfg(not(feature = "test-mode"))]
impl ExecutionProvider for RestExecutionProvider {
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
}

#[cfg(feature = "test-mode")]
pub struct FakeExecutionProvider {
    _clock: Arc<dyn Clock>,
    behaviors: Mutex<HashMap<Symbol, FakeBehaviorState>>,
    calls: Mutex<Vec<ProviderCall>>,
    active: Mutex<HashSet<Symbol>>,
    overlap: AtomicBool,
    events_tx: Sender<ProviderEvent>,
    events_rx: Option<Receiver<ProviderEvent>>,
}

#[cfg(feature = "test-mode")]
#[derive(Debug, Clone, Copy)]
pub enum FakeOrderBehavior {
    AlwaysOk,
    RejectLimitOnce(OrderError),
    RejectBuyOnce(OrderError),
    Always(OrderError),
    TransientOnce,
    FatalOnce,
    SlowSuccess { spins: usize },
}

#[cfg(feature = "test-mode")]
impl FakeExecutionProvider {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        let (events_tx, events_rx) = unbounded();
        Self::with_parts(clock, events_tx, Some(events_rx))
    }

    pub fn with_event_sink(clock: Arc<dyn Clock>, tx: Sender<ProviderEvent>) -> Self {
        Self::with_parts(clock, tx, None)
    }

    fn with_parts(
        clock: Arc<dyn Clock>,
        events_tx: Sender<ProviderEvent>,
        events_rx: Option<Receiver<ProviderEvent>>,
    ) -> Self {
        Self {
            _clock: clock,
            behaviors: Mutex::new(HashMap::new()),
            calls: Mutex::new(Vec::new()),
            active: Mutex::new(HashSet::new()),
            overlap: AtomicBool::new(false),
            events_tx,
            events_rx,
        }
    }

    pub fn set_behavior(&self, symbol: Symbol, behavior: FakeOrderBehavior) {
        let mut map = self.behaviors.lock().expect("fake provider poisoned");
        let state = match behavior {
            FakeOrderBehavior::AlwaysOk => FakeBehaviorState::AlwaysOk,
            FakeOrderBehavior::RejectLimitOnce(error) => {
                FakeBehaviorState::RejectLimitOnce { error, used: false }
            }
            FakeOrderBehavior::RejectBuyOnce(error) => {
                FakeBehaviorState::RejectBuyOnce { error, used: false }
            }
            FakeOrderBehavior::Always(error) => FakeBehaviorState::AlwaysReject { error },
            FakeOrderBehavior::TransientOnce => FakeBehaviorState::TransientOnce { used: false },
            FakeOrderBehavior::FatalOnce => FakeBehaviorState::FatalOnce { used: false },
            FakeOrderBehavior::SlowSuccess { spins } => FakeBehaviorState::SlowSuccess { spins },
        };
        map.insert(symbol, state);
    }

    pub fn reset(&self) {
        self.behaviors
            .lock()
            .expect("fake provider poisoned")
            .clear();
        self.clear_calls();
        self.active.lock().expect("fake provider poisoned").clear();
        self.overlap.store(false, Ordering::SeqCst);
    }

    pub fn clear_calls(&self) {
        self.calls.lock().expect("fake provider poisoned").clear();
        self.overlap.store(false, Ordering::SeqCst);
        if let Some(rx) = &self.events_rx {
            while rx.try_recv().is_ok() {}
        }
    }

    pub fn drain_calls(&self) -> Vec<ProviderCall> {
        let mut calls = self.calls.lock().expect("fake provider poisoned");
        let drained = calls.clone();
        calls.clear();
        drained
    }

    pub fn snapshot_calls(&self) -> Vec<ProviderCall> {
        self.calls.lock().expect("fake provider poisoned").clone()
    }

    pub fn overlap_detected(&self) -> bool {
        self.overlap.load(Ordering::SeqCst)
    }

    fn emit(&self, event: ProviderEvent) {
        let _ = self.events_tx.send(event);
    }
}

#[cfg(feature = "test-mode")]
#[derive(Debug, Clone)]
enum FakeBehaviorState {
    AlwaysOk,
    RejectLimitOnce { error: OrderError, used: bool },
    RejectBuyOnce { error: OrderError, used: bool },
    AlwaysReject { error: OrderError },
    TransientOnce { used: bool },
    FatalOnce { used: bool },
    SlowSuccess { spins: usize },
}

#[cfg(feature = "test-mode")]
impl FakeBehaviorState {
    fn apply(&mut self, order: &ProviderOrder) -> Result<String, OrderSubmitError> {
        match self {
            FakeBehaviorState::AlwaysOk => Ok(order.client_order_id.clone()),
            FakeBehaviorState::RejectLimitOnce { error, used } => {
                if order.order_type == "LIMIT" && !*used {
                    *used = true;
                    Err(OrderSubmitError::new(
                        *error,
                        format!("fake limit reject {:?} {}", error, order.symbol),
                    ))
                } else {
                    Ok(order.client_order_id.clone())
                }
            }
            FakeBehaviorState::RejectBuyOnce { error, used } => {
                if order.side == "BUY" && !*used {
                    *used = true;
                    Err(OrderSubmitError::new(
                        *error,
                        format!("fake buy reject {:?} {}", error, order.symbol),
                    ))
                } else {
                    Ok(order.client_order_id.clone())
                }
            }
            FakeBehaviorState::AlwaysReject { error } => Err(OrderSubmitError::new(
                *error,
                format!("fake permanent reject {:?} {}", error, order.symbol),
            )),
            FakeBehaviorState::TransientOnce { used } => {
                if !*used {
                    *used = true;
                    Err(OrderSubmitError::new(
                        OrderError::Transient,
                        format!("fake transient failure for {}", order.symbol),
                    ))
                } else {
                    Ok(order.client_order_id.clone())
                }
            }
            FakeBehaviorState::FatalOnce { used } => {
                if !*used {
                    *used = true;
                    Err(OrderSubmitError::new(
                        OrderError::Fatal,
                        format!("fake fatal failure for {}", order.symbol),
                    ))
                } else {
                    Ok(order.client_order_id.clone())
                }
            }
            FakeBehaviorState::SlowSuccess { spins } => {
                for _ in 0..*spins {
                    std::hint::spin_loop();
                }
                Ok(order.client_order_id.clone())
            }
        }
    }
}

#[cfg(feature = "test-mode")]
impl ExecutionProvider for FakeExecutionProvider {
    fn submit_order(&self, order: &ProviderOrder) -> Result<String, OrderSubmitError> {
        {
            let mut active = self.active.lock().expect("fake provider poisoned");
            if !active.insert(order.symbol) {
                self.overlap.store(true, Ordering::SeqCst);
            }
        }

        let mut map = self.behaviors.lock().expect("fake provider poisoned");
        let state = map
            .entry(order.symbol)
            .or_insert(FakeBehaviorState::AlwaysOk);
        let result = state.apply(order);
        drop(map);

        {
            let mut active = self.active.lock().expect("fake provider poisoned");
            active.remove(&order.symbol);
        }

        {
            let mut calls = self.calls.lock().expect("fake provider poisoned");
            calls.push(ProviderCall {
                symbol: order.symbol,
                side: order.side,
                order_type: order.order_type,
                success: result.is_ok(),
            });
        }

        let sym_id = symbol_id(order.symbol);
        let event = if order.order_type == "LIMIT" {
            ProviderEvent::Limit { sym: sym_id }
        } else {
            ProviderEvent::Buy { sym: sym_id }
        };
        self.emit(event);

        result
    }
}

#[cfg(feature = "test-mode")]
#[derive(Clone, Debug)]
pub struct ProviderCall {
    pub symbol: Symbol,
    pub side: &'static str,
    pub order_type: &'static str,
    pub success: bool,
}

#[cfg(feature = "test-mode")]
#[derive(Clone, Debug)]
pub enum ProviderEvent {
    Buy { sym: SymbolId },
    Limit { sym: SymbolId },
}
