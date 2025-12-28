use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use crossbeam_channel::{bounded, unbounded, Receiver, Sender, TryRecvError, TrySendError};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use tracing::info;

use crate::affinity;
use crate::ahi::AhiHistory;
use crate::alt_gate::{AhiCalculator, AhiSample, SymbolReturns};
use crate::capital::{CapitalSlots, OrderRole, SlotId};
use crate::channels::{SpscReceiver, SpscSender};
use crate::clock::Clock;
use crate::config::{Config, ExecutionMode, LogProfile, ServerSpec, ShardAssignment};
use crate::execution::{
    ExecutionHandle, OrderFill, OrderStatus, OrderSubmitError, ProviderError, TargetInfo,
};
use crate::fees::{breakeven_px, sl_trigger_px, tp_target_px};
use crate::gates::{TradingGate, WarmupGate};
use crate::mode::{Mode, ModeMachine};
use crate::pending_tp::{PendingTpClearReason, PendingTpGate, PendingTpOps, PendingTpOutcome};
use crate::positions::{ExitDecision, ExitReason, Position, PositionBook};
use crate::rings::{abs_return_over, return_over, RingBuffer, RingsHandle, SymbolRings};
use crate::risk::{RiskEngine, RiskHandle, TradeBlock};
use crate::types::{
    AccountEvent, AccountExecutionReport, BalanceSnapshot, LogMessage, MetricEvent, PriceEvent,
    ReconnectNotice, SignalSuppressReason, Symbol, TriggerEvent,
};
use crate::universe::{Universe, UniverseHandle};

const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(1);
const RECONNECT_WARMUP_SECS: u64 = 60;
const PENDING_TP_TIMEOUT_SECS: u64 = 5;

#[allow(clippy::too_many_arguments)]
pub fn spawn_processor(
    config: &'static Config,
    server: &'static ServerSpec,
    clock: Arc<dyn Clock>,
    slot_budget: Decimal,
    usdt_free_start: Decimal,
    universe: UniverseHandle,
    rings: RingsHandle,
    trading_gate: Arc<TradingGate>,
    warmup_gate: Arc<WarmupGate>,
    shard_inputs: Vec<(ShardAssignment, SpscReceiver<PriceEvent>)>,
    reconnect_rx: Receiver<ReconnectNotice>,
    account_rx: Receiver<AccountEvent>,
    trigger_tx: SpscSender<TriggerEvent>,
    log_tx: Sender<LogMessage>,
) -> (thread::JoinHandle<()>, ProcessorHandle) {
    let (cmd_tx, cmd_rx) = unbounded();
    let (risk_engine, risk_handle) =
        RiskEngine::new(config, clock.clone()).expect("failed to initialise risk engine");
    let reconnect_rx_thread = reconnect_rx;
    let handle = thread::Builder::new()
        .name(format!("processor-{:?}", server.id))
        .spawn({
            let universe_clone = universe.clone();
            let rings_clone = rings.clone();
            let trading_clone = trading_gate.clone();
            let warmup_clone = warmup_gate.clone();
            let risk_engine = risk_engine;
            let clock = clock.clone();
            let reconnect_rx = reconnect_rx_thread;
            move || {
                affinity::bind_to_core(server.processor_core);
                let mut processor = Processor::new(
                    config,
                    clock,
                    slot_budget,
                    usdt_free_start,
                    shard_inputs,
                    trigger_tx,
                    log_tx,
                    universe_clone,
                    rings_clone,
                    trading_clone,
                    warmup_clone,
                    cmd_rx,
                    risk_engine,
                    reconnect_rx,
                    account_rx,
                );
                processor.run();
            }
        })
        .expect("failed to spawn processor thread");

    let controller = ProcessorHandle {
        trading: trading_gate,
        warmup: warmup_gate,
        universe,
        rings,
        cmd_tx,
        risk: risk_handle.clone(),
    };

    (handle, controller)
}

#[derive(Clone)]
pub struct ProcessorHandle {
    trading: Arc<TradingGate>,
    warmup: Arc<WarmupGate>,
    universe: UniverseHandle,
    rings: RingsHandle,
    cmd_tx: Sender<ProcessorCommand>,
    risk: RiskHandle,
}

#[allow(dead_code)]
impl ProcessorHandle {
    pub fn force_watch_only(&self) {
        self.trading.disable();
    }

    pub fn enable_trading(&self) {
        self.trading.enable();
    }

    pub fn arm_warmup(&self, secs: u64) {
        self.warmup.arm_for(Duration::from_secs(secs));
    }

    pub fn install_universe_and_rings(&self, universe: crate::universe::Universe) {
        self.rings.reinit_for(&universe);
        self.universe.install(universe);
    }

    pub fn apply_schema_swap(&self, universe: Universe) -> anyhow::Result<()> {
        let (ack_tx, ack_rx) = bounded(1);
        self.cmd_tx
            .send(ProcessorCommand::SchemaSwap {
                universe,
                ack: ack_tx,
            })
            .map_err(|err| anyhow!("failed to send schema swap command: {err}"))?;
        ack_rx
            .recv()
            .map_err(|err| anyhow!("schema swap acknowledgement failed: {err}"))?;
        Ok(())
    }

    pub fn bump_epoch(&self) -> u64 {
        self.trading.bump_epoch()
    }

    pub fn epoch(&self) -> u64 {
        self.trading.epoch()
    }

    pub fn attach_execution_handle(&self, execution: ExecutionHandle) -> Result<()> {
        self.send_command(|ack| ProcessorCommand::AttachExecution {
            handle: execution,
            ack,
        })
    }

    pub fn reset_daily_state(&self) -> Result<()> {
        self.send_command(|ack| ProcessorCommand::ResetDaily { ack })
    }

    pub fn risk_handle(&self) -> RiskHandle {
        self.risk.clone()
    }

    fn send_command<F>(&self, build: F) -> Result<()>
    where
        F: FnOnce(Sender<()>) -> ProcessorCommand,
    {
        let (ack_tx, ack_rx) = crossbeam_channel::bounded(1);
        self.cmd_tx
            .send(build(ack_tx))
            .map_err(|err| anyhow!("failed to send processor command: {err}"))?;
        ack_rx
            .recv()
            .map_err(|err| anyhow!("processor command acknowledgement failed: {err}"))?;
        Ok(())
    }
}

struct Processor {
    config: &'static Config,
    clock: Arc<dyn Clock>,
    shards: Vec<ShardChannel>,
    shard_symbols: HashMap<usize, Vec<Symbol>>,
    trigger_tx: SpscSender<TriggerEvent>,
    log_tx: Sender<LogMessage>,
    symbols: HashMap<Symbol, SymbolState>,
    last_prices: HashMap<Symbol, f64>,
    queue_age: Duration,
    ahi_history: AhiHistory,
    ahi_calculator: AhiCalculator,
    last_ahi_sample: Option<AhiSample>,
    mode: ModeMachine,
    capital: CapitalSlots,
    positions: PositionBook,
    risk: RiskEngine,
    execution: Option<ExecutionHandle>,
    benchmarks: Benchmarks,
    last_snapshot: Instant,
    next_ahi_compute: Instant,
    #[allow(dead_code)]
    universe: UniverseHandle,
    #[allow(dead_code)]
    rings: RingsHandle,
    trading_gate: Arc<TradingGate>,
    warmup_gate: Arc<WarmupGate>,
    command_rx: Receiver<ProcessorCommand>,
    reconnect_rx: Receiver<ReconnectNotice>,
    account_rx: Receiver<AccountEvent>,
    haram_symbols: HashSet<Symbol>,
    haram_logged: HashSet<Symbol>,
    diag_next_log: Instant,
    balances: HashMap<String, BalanceSnapshot>,
    client_ids: ClientOrderIdGen,
    usdt_free: Decimal,
    usdt_free_fallback: Decimal,
    pending_recovery: HashMap<SlotId, PendingRecovery>,
    pending_tp: PendingTpGate,
}
struct ShardChannel {
    receiver: SpscReceiver<PriceEvent>,
}

#[derive(Debug, Default)]
struct ClientOrderIdGen {
    seq: u64,
}

impl ClientOrderIdGen {
    fn next(&mut self, slot: SlotId) -> (String, String) {
        self.seq = self.seq.wrapping_add(1);
        let base = format!("SB-{}-{:08}", slot.label(), self.seq);
        (base.clone(), format!("{base}T"))
    }
}

#[derive(Debug, Clone)]
struct PendingRecovery {
    attempts: u32,
    next_retry: Instant,
}

struct PendingTpRuntime<'a> {
    processor: &'a mut Processor,
}

impl PendingTpOps for PendingTpRuntime<'_> {
    fn tp_exists(&mut self, symbol: Symbol, tp_client_id: &str) -> Result<bool, ProviderError> {
        let Some(execution) = self.processor.execution.as_ref() else {
            return Err(ProviderError::fatal("execution handle missing"));
        };
        execution.find_open_order(symbol, tp_client_id.to_string())
    }

    fn position_qty(&mut self, symbol: Symbol) -> Option<Decimal> {
        self.processor.positions.get(symbol).map(|pos| pos.qty)
    }

    fn query_order(&mut self, symbol: Symbol, order_id: &str) -> Result<OrderFill, ProviderError> {
        let Some(execution) = self.processor.execution.as_ref() else {
            return Err(ProviderError::fatal("execution handle missing"));
        };
        execution.query_order(symbol, order_id.to_string())
    }

    fn query_balance(&mut self, asset: &str) -> Result<Decimal, ProviderError> {
        let Some(execution) = self.processor.execution.as_ref() else {
            return Err(ProviderError::fatal("execution handle missing"));
        };
        execution.query_balance(asset.to_string())
    }

    fn filters_for(&mut self, symbol: Symbol) -> Option<crate::filters::SymbolFilters> {
        let execution = self.processor.execution.as_ref()?;
        execution.filters_for(symbol)
    }

    fn last_price(&mut self, symbol: Symbol) -> Option<Decimal> {
        self.processor
            .last_prices
            .get(&symbol)
            .and_then(|px| Decimal::from_f64(*px))
    }

    fn place_market_sell(
        &mut self,
        symbol: Symbol,
        qty: Decimal,
        client_order_id: String,
    ) -> Result<String, OrderSubmitError> {
        let Some(execution) = self.processor.execution.as_ref() else {
            return Err(OrderSubmitError {
                kind: crate::execution::OrderError::Fatal,
                detail: "execution handle missing".to_string(),
            });
        };
        execution.place_market_sell(symbol, qty, client_order_id)
    }
}

impl Processor {
    fn enter_pending_tp(
        &mut self,
        slot: SlotId,
        symbol: Symbol,
        buy_client_order_id: String,
        source: &str,
    ) {
        if self.config.execution.mode != ExecutionMode::Live {
            return;
        }
        let Some(tp_client_order_id) = self.capital.tp_client_id(slot) else {
            let _ = self.log_tx.send(LogMessage::Warn(
                format!(
                    "[GATE] pending_tp skipped ({}) tp_cid missing symbol={} cid={}",
                    source, symbol, buy_client_order_id
                )
                .into(),
            ));
            return;
        };
        if let Some(state) = self.pending_tp.state() {
            if state.symbol == symbol && state.buy_client_id == buy_client_order_id {
                return;
            }
            let _ = self.log_tx.send(LogMessage::Warn(
                format!(
                    "[GATE] pending_tp replacing existing ({}) symbol={} cid={}",
                    source, symbol, buy_client_order_id
                )
                .into(),
            ));
        }
        let now = self.clock.now_instant();
        self.pending_tp.enter(
            now,
            slot,
            symbol,
            buy_client_order_id.clone(),
            tp_client_order_id.clone(),
        );
        let _ = self.log_tx.send(LogMessage::Warn(
            format!(
                "[GATE] pending_tp enter ({}) symbol={} cid={} tp_cid={} timeout_secs={}",
                source,
                symbol,
                buy_client_order_id,
                tp_client_order_id,
                self.pending_tp.timeout().as_secs()
            )
            .into(),
        ));
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        config: &'static Config,
        clock: Arc<dyn Clock>,
        slot_budget: Decimal,
        usdt_free_start: Decimal,
        shard_inputs: Vec<(ShardAssignment, SpscReceiver<PriceEvent>)>,
        trigger_tx: SpscSender<TriggerEvent>,
        log_tx: Sender<LogMessage>,
        universe: UniverseHandle,
        rings: RingsHandle,
        trading_gate: Arc<TradingGate>,
        warmup_gate: Arc<WarmupGate>,
        command_rx: Receiver<ProcessorCommand>,
        risk: RiskEngine,
        reconnect_rx: Receiver<ReconnectNotice>,
        account_rx: Receiver<AccountEvent>,
    ) -> Self {
        let mut shard_symbols = HashMap::new();
        let shards = shard_inputs
            .into_iter()
            .map(|(assignment, receiver)| {
                let symbols = assignment
                    .symbols
                    .iter()
                    .filter_map(|sym| Symbol::from_str(sym))
                    .collect::<Vec<_>>();
                shard_symbols.insert(assignment.shard_index, symbols);
                ShardChannel { receiver }
            })
            .collect();

        let now = clock.now_instant();
        let window = config
            .strategy
            .ahi
            .enter_window
            .max(config.strategy.ahi.drop_window);
        let ahi_history = AhiHistory::new(window);
        let ahi_calculator = AhiCalculator::new(
            config.strategy.ahi.breadth_pos_threshold_bp,
            config.strategy.ahi.ethbtc_linear_fullscale_bp,
        );
        let armed_live =
            config.execution.mode == ExecutionMode::Live && config.execution.live_armed;
        let initial_mode = if armed_live {
            Mode::LiveTrading
        } else {
            Mode::WatchOnly
        };
        let mode = ModeMachine::new_with_state(config, initial_mode, now);
        info!(target: "bot", "ModeMachine initial_state={:?}", initial_mode);
        if armed_live {
            info!(target: "bot", "ModeNotLive gate override active (Live+Armed)");
        }
        let capital = CapitalSlots::new(slot_budget, slot_budget);
        let positions = PositionBook::new();

        let mut haram_symbols = HashSet::new();
        for entry in &config.strategy.haram_symbols {
            if let Some(symbol) = Symbol::from_str(entry) {
                haram_symbols.insert(symbol);
            }
        }
        let next_ahi_compute = now;

        Self {
            config,
            clock,
            shards,
            shard_symbols,
            trigger_tx,
            log_tx,
            symbols: HashMap::with_capacity(512),
            last_prices: HashMap::with_capacity(512),
            queue_age: config.backpressure.max_queue_age,
            ahi_history,
            ahi_calculator,
            last_ahi_sample: None,
            mode,
            capital,
            positions,
            risk,
            execution: None,
            benchmarks: Benchmarks::new(),
            last_snapshot: now,
            next_ahi_compute,
            universe,
            rings,
            trading_gate,
            warmup_gate,
            command_rx,
            reconnect_rx,
            account_rx,
            haram_symbols,
            haram_logged: HashSet::new(),
            diag_next_log: now,
            balances: HashMap::new(),
            client_ids: ClientOrderIdGen::default(),
            usdt_free: usdt_free_start,
            usdt_free_fallback: usdt_free_start,
            pending_recovery: HashMap::new(),
            pending_tp: PendingTpGate::new(Duration::from_secs(PENDING_TP_TIMEOUT_SECS)),
        }
    }

    fn run(&mut self) {
        loop {
            self.drain_commands();
            self.drain_reconnects();
            self.drain_account_events();
            self.handle_pending_timeouts();
            self.handle_pending_tp_timeout();

            let mut made_progress = false;
            for idx in 0..self.shards.len() {
                match self.shards[idx].receiver.try_recv() {
                    Ok(event) => {
                        made_progress = true;
                        self.handle_event(event);
                    }
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => return,
                }
            }

            self.maybe_snapshot_idle();
            self.maybe_compute_ahi();

            if !made_progress {
                std::hint::spin_loop();
            }
        }
    }

    fn drain_commands(&mut self) {
        while let Ok(cmd) = self.command_rx.try_recv() {
            self.handle_command(cmd);
        }
    }

    fn drain_reconnects(&mut self) {
        while let Ok(notice) = self.reconnect_rx.try_recv() {
            self.handle_reconnect(notice);
        }
    }

    fn drain_account_events(&mut self) {
        while let Ok(event) = self.account_rx.try_recv() {
            self.handle_account_event(event);
        }
    }

    fn handle_pending_timeouts(&mut self) {
        let now = self.clock.now_instant();
        let timeouts = self.capital.expire_pending(now);
        for timeout in timeouts {
            let _ = self.log_tx.send(LogMessage::Warn(
                format!(
                    "[ACCT] pending_timeout slot={} symbol={} cid={:?}",
                    timeout.slot.label(),
                    timeout.symbol,
                    timeout.client_order_id
                )
                .into(),
            ));
            let Some(buy_client_id) = timeout.client_order_id.clone() else {
                let _ = self.log_tx.send(LogMessage::Warn(
                    format!(
                        "[ACCT] pending_timeout missing client id slot={} symbol={}",
                        timeout.slot.label(),
                        timeout.symbol
                    )
                    .into(),
                ));
                let _ = self.capital.release_slot(timeout.slot);
                self.clear_pending_recovery(timeout.slot);
                continue;
            };

            if !self.pending_recovery_ready(timeout.slot, now) {
                continue;
            }

            let Some(execution) = self.execution.as_ref() else {
                let _ = self.log_tx.send(LogMessage::Warn(
                    format!(
                        "[ACCT] pending_timeout query skipped: execution handle missing slot={} symbol={}",
                        timeout.slot.label(),
                        timeout.symbol
                    )
                    .into(),
                ));
                self.schedule_pending_recovery(timeout.slot, now);
                continue;
            };

            let lookup_id = self
                .capital
                .order_id(timeout.slot)
                .unwrap_or_else(|| buy_client_id.clone());
            match execution.query_order(timeout.symbol, lookup_id.clone()) {
                Ok(fill) => {
                    if fill.status == OrderStatus::Filled
                        || fill.status == OrderStatus::PartiallyFilled
                    {
                        if fill.cum_filled <= Decimal::ZERO {
                            let _ = self.log_tx.send(LogMessage::Warn(
                                format!(
                                    "[ACCT] pending_timeout fill qty_zero slot={} symbol={} cid={}",
                                    timeout.slot.label(),
                                    timeout.symbol,
                                    buy_client_id
                                )
                                .into(),
                            ));
                            self.schedule_pending_recovery(timeout.slot, now);
                            continue;
                        }
                        let avg_price = if fill.cum_quote > Decimal::ZERO {
                            (fill.cum_quote / fill.cum_filled).normalize()
                        } else {
                            Decimal::ZERO
                        };
                        if avg_price <= Decimal::ZERO {
                            let _ = self.log_tx.send(LogMessage::Warn(
                                format!(
                                    "[ACCT] pending_timeout avg_price_missing slot={} symbol={} cid={}",
                                    timeout.slot.label(),
                                    timeout.symbol,
                                    buy_client_id
                                )
                                .into(),
                            ));
                            self.schedule_pending_recovery(timeout.slot, now);
                            continue;
                        }

                        let _ = self.capital.mark_position_open(
                            &buy_client_id,
                            self.capital.order_id(timeout.slot),
                        );
                        self.ensure_position_open_from_fill(
                            timeout.slot,
                            timeout.symbol,
                            fill.cum_filled,
                            avg_price,
                        );
                        self.place_tp_for_fill(
                            timeout.slot,
                            timeout.symbol,
                            fill.cum_filled,
                            avg_price,
                            buy_client_id.clone(),
                            "pending_timeout",
                        );
                        self.clear_pending_recovery(timeout.slot);
                        continue;
                    }

                    if fill.status == OrderStatus::New {
                        let _ = self.capital.mark_reserved(
                            &buy_client_id,
                            self.capital.order_id(timeout.slot),
                            now,
                        );
                        let _ = self.log_tx.send(LogMessage::Warn(
                            format!(
                                "[ACCT] pending_timeout status=NEW slot={} symbol={} cid={}",
                                timeout.slot.label(),
                                timeout.symbol,
                                buy_client_id
                            )
                            .into(),
                        ));
                        self.clear_pending_recovery(timeout.slot);
                        continue;
                    }

                    if matches!(
                        fill.status,
                        OrderStatus::Canceled | OrderStatus::Rejected | OrderStatus::Expired
                    ) {
                        let _ = self.log_tx.send(LogMessage::Warn(
                            format!(
                                "[ACCT] pending_timeout status={:?} slot={} symbol={} cid={}",
                                fill.status,
                                timeout.slot.label(),
                                timeout.symbol,
                                buy_client_id
                            )
                            .into(),
                        ));
                        let _ = self.capital.release_by_client(&buy_client_id);
                        self.clear_pending_recovery(timeout.slot);
                        continue;
                    }

                    let _ = self.log_tx.send(LogMessage::Warn(
                        format!(
                            "[ACCT] pending_timeout status={:?} slot={} symbol={} cid={}",
                            fill.status,
                            timeout.slot.label(),
                            timeout.symbol,
                            buy_client_id
                        )
                        .into(),
                    ));
                    self.schedule_pending_recovery(timeout.slot, now);
                }
                Err(err) => {
                    if err.is_not_found() {
                        let _ = self.log_tx.send(LogMessage::Warn(
                            format!(
                                "[ACCT] pending_timeout order not found slot={} symbol={} cid={}",
                                timeout.slot.label(),
                                timeout.symbol,
                                buy_client_id
                            )
                            .into(),
                        ));
                        let _ = self.capital.release_by_client(&buy_client_id);
                        self.clear_pending_recovery(timeout.slot);
                    } else {
                        let _ = self.log_tx.send(LogMessage::Warn(
                            format!(
                                "[ACCT] pending_timeout query failed slot={} symbol={} cid={} err={:?}",
                                timeout.slot.label(),
                                timeout.symbol,
                                buy_client_id,
                                err
                            )
                            .into(),
                        ));
                        self.schedule_pending_recovery(timeout.slot, now);
                    }
                }
            }
        }
        self.rebalance_slots_if_free();
    }

    fn handle_pending_tp_timeout(&mut self) {
        if !self.pending_tp.is_active() {
            return;
        }
        let now = self.clock.now_instant();
        let state_snapshot = self.pending_tp.state().cloned();
        let Some(state) = state_snapshot else {
            return;
        };
        let order_lookup_id = self
            .capital
            .order_id(state.slot)
            .unwrap_or_else(|| state.buy_client_id.clone());
        let base_asset = self
            .universe
            .get()
            .symbol_meta(state.symbol)
            .map(|meta| meta.base.clone());
        let emergency_client_id = format!(
            "EMERGENCY-{}-{}",
            state.symbol,
            self.clock.monotonic_now_ns()
        );

        let timeout = self.pending_tp.timeout();
        let mut gate = std::mem::replace(&mut self.pending_tp, PendingTpGate::new(timeout));
        let mut ops = PendingTpRuntime { processor: self };
        let outcome = gate.handle_timeout(
            now,
            order_lookup_id,
            base_asset,
            emergency_client_id,
            &mut ops,
        );
        self.pending_tp = gate;

        match outcome {
            PendingTpOutcome::NoAction => {}
            PendingTpOutcome::Cleared { reason } => match reason {
                PendingTpClearReason::OpenOrders => {
                    let _ = self.log_tx.send(LogMessage::Warn(
                        format!(
                            "[GATE] pending_tp cleared proof=open_orders symbol={} cid={} tp_cid={}",
                            state.symbol, state.buy_client_id, state.tp_client_id
                        )
                        .into(),
                    ));
                }
            },
            PendingTpOutcome::Kill { event } => {
                let emergency_label = if event.emergency_ok { "ok" } else { "err" };
                if event.emergency_attempted {
                    let _ = self.log_tx.send(LogMessage::Warn(
                        format!(
                            "[EMERGENCY] market_sell {} symbol={} qty={} cid={}",
                            emergency_label, event.symbol, event.qty, event.emergency_client_id
                        )
                        .into(),
                    ));
                } else {
                    let _ = self.log_tx.send(LogMessage::Warn(
                        format!(
                            "[EMERGENCY] market_sell skipped symbol={} qty={} cid={}",
                            event.symbol, event.qty, event.emergency_client_id
                        )
                        .into(),
                    ));
                }
                let last_err = event.last_err.unwrap_or_else(|| "none".to_string());
                let _ = self.log_tx.send(LogMessage::Error(format!(
                    "[KILL] TP not placed within timeout; emergency_sell={} symbol={} cid={} qty={} elapsed_ms={} last_err={}",
                    emergency_label,
                    event.symbol,
                    event.buy_client_id,
                    event.qty,
                    event.elapsed_ms,
                    last_err
                )));
                std::process::exit(2);
            }
        }
    }

    fn pending_recovery_ready(&self, slot: SlotId, now: Instant) -> bool {
        match self.pending_recovery.get(&slot) {
            Some(state) => now >= state.next_retry,
            None => true,
        }
    }

    fn schedule_pending_recovery(&mut self, slot: SlotId, now: Instant) {
        let entry = self
            .pending_recovery
            .entry(slot)
            .or_insert(PendingRecovery {
                attempts: 0,
                next_retry: now,
            });
        entry.attempts = entry.attempts.saturating_add(1);
        entry.next_retry = now + pending_recovery_delay(entry.attempts);
    }

    fn clear_pending_recovery(&mut self, slot: SlotId) {
        self.pending_recovery.remove(&slot);
    }

    fn handle_reconnect(&mut self, notice: ReconnectNotice) {
        let Some(symbols) = self.shard_symbols.get(&notice.shard_index) else {
            return;
        };
        let now = self.clock.now_instant();
        let hold = Duration::from_secs(RECONNECT_WARMUP_SECS);
        for &symbol in symbols {
            let price = self.last_prices.get(&symbol).copied();
            let state = self
                .symbols
                .entry(symbol)
                .or_insert_with(|| SymbolState::new(self.config.trigger.window));
            state.bootstrap(now, price, hold);
            let msg = match price {
                Some(value) => format!(
                    "[WARMUP] bootstrap shard={} symbol={} last_px={:.6}",
                    notice.shard_index, symbol, value
                ),
                None => format!(
                    "[WARMUP] bootstrap shard={} symbol={} last_px=NA",
                    notice.shard_index, symbol
                ),
            };
            let _ = self.log_tx.send(LogMessage::Info(msg.into()));
        }
    }

    fn handle_command(&mut self, cmd: ProcessorCommand) {
        match cmd {
            ProcessorCommand::ResetDaily { ack } => {
                self.reset_daily_components();
                let _ = ack.send(());
            }
            ProcessorCommand::AttachExecution { handle, ack } => {
                self.execution = Some(handle);
                let _ = ack.send(());
            }
            ProcessorCommand::SchemaSwap { universe, ack } => {
                self.apply_schema_swap(universe);
                let _ = ack.send(());
            }
        }
    }

    fn handle_account_event(&mut self, event: AccountEvent) {
        match event {
            AccountEvent::Execution(report) => self.handle_execution_report(report),
            AccountEvent::OutboundAccountPosition { balances } => {
                self.handle_outbound_balances(balances)
            }
            AccountEvent::BalanceUpdate { asset, delta } => {
                self.handle_balance_update(asset, delta)
            }
            AccountEvent::AccountSnapshot { balances } => self.handle_balance_snapshot(balances),
            AccountEvent::OpenOrders(orders) => self.handle_open_orders(orders),
            AccountEvent::BuySubmitted {
                symbol,
                buy_client_order_id,
                tp_client_order_id,
            } => {
                self.handle_buy_submitted(symbol, buy_client_order_id, tp_client_order_id);
            }
            AccountEvent::LocalReject {
                client_order_id,
                symbol,
                reason,
            } => {
                let _ = self.log_tx.send(LogMessage::Warn(
                    format!(
                        "[ACCT] local_reject cid={} symbol={} reason={}",
                        client_order_id, symbol, reason
                    )
                    .into(),
                ));
                let _ = self.capital.release_by_client(&client_order_id);
                self.positions.close(symbol);
                self.rebalance_slots_if_free();
            }
            AccountEvent::StreamClosed => {}
        }
    }

    fn reset_daily_components(&mut self) {
        let usdt_available = self.current_usdt_free();
        let portfolio_value = self.portfolio_value_usd();
        self.capital.reset_daily();
        self.risk.reset_daily_counters_keep_long_bans();
        self.positions.reset_daily_view();
        self.symbols.clear();
        self.last_prices.clear();
        self.benchmarks = Benchmarks::new();
        self.haram_logged.clear();
        self.diag_next_log = self.clock.now_instant();
        self.balances.clear();
        self.client_ids = ClientOrderIdGen::default();
        let _ = self.pending_tp.clear();
        let spent = self.maybe_top_up_bnb(portfolio_value, usdt_available);
        let remaining_usdt = (usdt_available - spent).max(Decimal::ZERO);
        self.usdt_free = remaining_usdt;
        self.usdt_free_fallback = remaining_usdt;
        self.rebalance_slots_with_total(remaining_usdt);
        let _ = self.log_tx.send(LogMessage::ResetDaily);
    }

    fn handle_execution_report(&mut self, report: AccountExecutionReport) {
        self.log_execution_report(&report);
        let status_upper = report.status.to_ascii_uppercase();
        let client_id = if self
            .capital
            .slot_for_client(&report.client_order_id)
            .is_some()
        {
            report.client_order_id.clone()
        } else {
            report
                .orig_client_order_id
                .clone()
                .unwrap_or_else(|| report.client_order_id.clone())
        };
        let role_lookup = self.capital.slot_for_client(&client_id);

        let Some((slot, role)) = role_lookup else {
            return;
        };

        if status_upper == "NEW" {
            let _ = self.capital.mark_reserved(
                &client_id,
                report.order_id.clone(),
                self.clock.now_instant(),
            );
            if let OrderRole::Buy = role {
                self.enter_pending_tp(slot, report.symbol, client_id.clone(), "exec_new");
                if let Some(execution) = self.execution.as_ref() {
                    execution.notify_buy_confirmed(client_id.clone());
                }
            }
            if let OrderRole::TakeProfit = role {
                if let Some(execution) = self.execution.as_ref() {
                    execution.notify_tp_confirmed(client_id.clone());
                }
            }
            return;
        }

        if status_upper == "PARTIALLY_FILLED" || status_upper == "FILLED" {
            let _ = self
                .capital
                .mark_position_open(&client_id, report.order_id.clone());
            match role {
                OrderRole::Buy => {
                    self.ensure_position_open(slot, &report);
                    if status_upper == "FILLED" {
                        self.place_tp_after_fill(slot, &report);
                    }
                    if let Some(execution) = self.execution.as_ref() {
                        execution.notify_buy_confirmed(client_id.clone());
                    }
                }
                OrderRole::TakeProfit => {
                    if let Some(execution) = self.execution.as_ref() {
                        execution.notify_tp_confirmed(client_id.clone());
                    }
                    if status_upper == "FILLED" {
                        self.close_position_from_tp(&report);
                    }
                }
            }
            return;
        }

        if status_upper == "REJECTED" || status_upper == "CANCELED" || status_upper == "EXPIRED" {
            if let Some((_, symbol)) = self.capital.release_by_client(&client_id) {
                self.positions.close(symbol);
            }
            self.rebalance_slots_if_free();
            return;
        }
    }

    fn ensure_position_open(&mut self, slot: SlotId, report: &AccountExecutionReport) {
        let qty = if report.cum_qty > Decimal::ZERO {
            report.cum_qty
        } else {
            report.last_qty
        };
        let entry_price = if report.last_price > Decimal::ZERO {
            report.last_price
        } else {
            report.price
        };
        self.ensure_position_open_from_fill(slot, report.symbol, qty, entry_price);
    }

    fn ensure_position_open_from_fill(
        &mut self,
        slot: SlotId,
        symbol: Symbol,
        qty: Decimal,
        entry_price: Decimal,
    ) {
        if qty <= Decimal::ZERO || entry_price <= Decimal::ZERO {
            return;
        }
        let strategy = &self.config.strategy;
        let take_profit = tp_target_px(
            entry_price,
            strategy.tp_pct,
            strategy.maker_fee_pct,
            strategy.taker_fee_pct,
        );
        let stop_loss = sl_trigger_px(
            entry_price,
            strategy.sl_pct,
            strategy.maker_fee_pct,
            strategy.taker_fee_pct,
        );
        let bounce_break_even =
            breakeven_px(entry_price, strategy.maker_fee_pct, strategy.taker_fee_pct);
        let tp_order_id = self
            .positions
            .get(symbol)
            .and_then(|position| position.tp_order_id.clone());
        let now = self.clock.now_instant();
        let already_open = self.positions.contains(symbol);
        self.positions.open(
            symbol,
            qty,
            entry_price,
            slot,
            now,
            take_profit,
            stop_loss,
            bounce_break_even,
            tp_order_id,
            qty,
        );
        if !already_open {
            let now_ksa = self.config.ksa_now(self.clock.as_ref());
            self.risk.mark_trade_open(symbol, now_ksa);
        }
    }

    fn place_tp_after_fill(&mut self, slot: SlotId, report: &AccountExecutionReport) {
        let executed_qty = if report.cum_qty > Decimal::ZERO {
            report.cum_qty
        } else {
            report.last_qty
        };
        let avg_price = if report.cum_quote > Decimal::ZERO && report.cum_qty > Decimal::ZERO {
            (report.cum_quote / report.cum_qty).normalize()
        } else if report.last_price > Decimal::ZERO {
            report.last_price
        } else {
            report.price
        };
        self.place_tp_for_fill(
            slot,
            report.symbol,
            executed_qty,
            avg_price,
            report.client_order_id.clone(),
            "",
        );
    }

    fn place_tp_for_fill(
        &mut self,
        slot: SlotId,
        symbol: Symbol,
        executed_qty: Decimal,
        avg_price: Decimal,
        buy_client_order_id: String,
        source: &str,
    ) {
        let context = if source.is_empty() { "" } else { " recovery" };
        if executed_qty <= Decimal::ZERO {
            return;
        }
        if avg_price <= Decimal::ZERO {
            let _ = self.log_tx.send(LogMessage::Warn(
                format!(
                    "[TP]{context} skip placement avg_price_missing symbol={} cid={}",
                    symbol, buy_client_order_id
                )
                .into(),
            ));
            return;
        }
        let tp_client_id = match self.capital.tp_client_id(slot) {
            Some(id) => id,
            None => {
                let _ = self.log_tx.send(LogMessage::Warn(
                    format!(
                        "[TP]{context} skip placement tp_client_id missing slot={}",
                        slot.label()
                    )
                    .into(),
                ));
                return;
            }
        };
        if let Some(position) = self.positions.get(symbol) {
            if let Some(tp_order_id) = position.tp_order_id.as_ref() {
                if position.tp_order_qty > Decimal::ZERO {
                    let _ = self.log_tx.send(LogMessage::Info(
                        format!(
                            "[TP]{context} skip placement proof=tp_order_id symbol={} cid={} tp_cid={} tp_order_id={}",
                            symbol, buy_client_order_id, tp_client_id, tp_order_id
                        )
                        .into(),
                    ));
                    if self.pending_tp.clear_if_tp(&tp_client_id) {
                        let _ = self.log_tx.send(LogMessage::Warn(
                            format!(
                                "[GATE] pending_tp cleared proof=tp_order_id symbol={} cid={} tp_cid={} tp_order_id={}",
                                symbol, buy_client_order_id, tp_client_id, tp_order_id
                            )
                            .into(),
                        ));
                    }
                    return;
                }
            }
        }
        let Some(execution) = self.execution.as_ref() else {
            let _ = self.log_tx.send(LogMessage::Warn(
                format!(
                    "[TP]{context} skip placement execution handle missing symbol={} cid={}",
                    symbol, buy_client_order_id
                )
                .into(),
            ));
            return;
        };
        if self.pending_tp.is_active() {
            match execution.find_open_order(symbol, tp_client_id.clone()) {
                Ok(true) => {
                    let _ = self.log_tx.send(LogMessage::Info(
                        format!(
                            "[TP]{context} skip placement proof=open_orders symbol={} cid={} tp_cid={}",
                            symbol, buy_client_order_id, tp_client_id
                        )
                        .into(),
                    ));
                    if self.pending_tp.clear_if_tp(&tp_client_id) {
                        let _ = self.log_tx.send(LogMessage::Warn(
                            format!(
                                "[GATE] pending_tp cleared proof=open_orders symbol={} cid={} tp_cid={}",
                                symbol, buy_client_order_id, tp_client_id
                            )
                            .into(),
                        ));
                    }
                    return;
                }
                Ok(false) => {}
                Err(err) => {
                    let _ = self.log_tx.send(LogMessage::Warn(
                        format!(
                            "[TP]{context} tp_exists check failed symbol={} cid={} err={:?}",
                            symbol, buy_client_order_id, err
                        )
                        .into(),
                    ));
                }
            }
        }
        let Some(filters) = execution.filters_for(symbol) else {
            let _ = self.log_tx.send(LogMessage::Warn(
                format!(
                    "[TP]{context} skip placement filters missing symbol={} cid={}",
                    symbol, buy_client_order_id
                )
                .into(),
            ));
            return;
        };
        let qty_aligned = floor_to_step(executed_qty, filters.step);
        if qty_aligned <= Decimal::ZERO {
            let _ = self.log_tx.send(LogMessage::Warn(
                format!(
                    "[TP]{context} skip placement qty_zero symbol={} qty={}",
                    symbol, executed_qty
                )
                .into(),
            ));
            return;
        }
        let tp_price = align_tick_up(
            avg_price * (Decimal::ONE + self.config.strategy.tp_pct),
            filters.tick,
        );
        if tp_price <= Decimal::ZERO {
            let _ = self.log_tx.send(LogMessage::Warn(
                format!(
                    "[TP]{context} skip placement price_zero symbol={} avg_price={}",
                    symbol, avg_price
                )
                .into(),
            ));
            return;
        }
        match execution.place_tp_limit(
            symbol,
            executed_qty,
            avg_price,
            tp_client_id.clone(),
            buy_client_order_id.clone(),
        ) {
            Ok(order_id) => {
                if let Some(position) = self.positions.get_mut(symbol) {
                    position.take_profit = tp_price;
                    position.tp_order_id = Some(order_id.clone());
                    position.tp_order_qty = qty_aligned;
                    position.tp_current_price = tp_price;
                    position.tp_initial_price = tp_price;
                }
                if self.pending_tp.clear_if_tp(&tp_client_id) {
                    let _ = self.log_tx.send(LogMessage::Warn(
                        format!(
                            "[GATE] pending_tp cleared proof=tp_order_id symbol={} cid={} tp_cid={} tp_order_id={}",
                            symbol, buy_client_order_id, tp_client_id, order_id
                        )
                        .into(),
                    ));
                }
                if !source.is_empty() {
                    let _ = self.log_tx.send(LogMessage::Warn(
                        format!(
                            "[TP] recovery placed symbol={} cid={} qty={} avg_price={}",
                            symbol, buy_client_order_id, executed_qty, avg_price
                        )
                        .into(),
                    ));
                }
            }
            Err(err) => {
                let _ = self.log_tx.send(LogMessage::Warn(
                    format!(
                        "[TP]{context} place_failed symbol={} cid={} err={}",
                        symbol, tp_client_id, err
                    )
                    .into(),
                ));
            }
        }
    }

    fn close_position_from_tp(&mut self, report: &AccountExecutionReport) {
        let exit_price = if report.last_price > Decimal::ZERO {
            report.last_price
        } else {
            report.price
        };
        if let Some(position) = self.positions.close(report.symbol) {
            self.finalize_exit(position, exit_price, ExitReason::TakeProfitLimit);
        }
    }

    fn handle_outbound_balances(&mut self, balances: Vec<BalanceSnapshot>) {
        for bal in balances {
            match self.balances.get(&bal.asset) {
                Some(prev) => {
                    let delta_free = bal.free - prev.free;
                    let delta_locked = bal.locked - prev.locked;
                    if delta_free != Decimal::ZERO || delta_locked != Decimal::ZERO {
                        self.log_balance_change(&bal.asset, &bal, Some((delta_free, delta_locked)));
                    }
                }
                None => {
                    self.log_balance_change(&bal.asset, &bal, None);
                }
            }
            self.balances.insert(bal.asset.clone(), bal);
        }
        if let Some(usdt) = self.balances.get("USDT") {
            self.usdt_free = usdt.free;
        }
        self.usdt_free_fallback = self.usdt_free;
        self.rebalance_slots_if_free();
    }

    fn current_usdt_free(&self) -> Decimal {
        if self.usdt_free > Decimal::ZERO {
            self.usdt_free
        } else {
            self.usdt_free_fallback
        }
    }

    fn portfolio_value_usd(&self) -> Decimal {
        let mut total = Decimal::ZERO;
        for bal in self.balances.values() {
            let amount = (bal.free + bal.locked).normalize();
            if amount <= Decimal::ZERO {
                continue;
            }
            if bal.asset.eq_ignore_ascii_case("USDT") {
                total += amount;
                continue;
            }
            let symbol_name = format!("{}USDT", bal.asset.to_ascii_uppercase());
            if let Some(symbol) = Symbol::from_str(&symbol_name) {
                if let Some(px_f64) = self.last_prices.get(&symbol) {
                    if let Some(px) = Decimal::from_f64(*px_f64) {
                        total += (amount * px).normalize();
                    }
                }
            }
        }
        if total <= Decimal::ZERO {
            self.current_usdt_free()
        } else {
            total
        }
    }

    fn rebalance_slots_with_total(&mut self, total_usdt: Decimal) {
        if total_usdt <= Decimal::ZERO {
            return;
        }
        let half = floor_usdt_cents(total_usdt / Decimal::from_i64(2).unwrap_or(Decimal::ONE));
        let current_a = self.capital.slot_budget(SlotId::A);
        let current_b = self.capital.slot_budget(SlotId::B);
        if current_a == half && current_b == half {
            return;
        }
        self.capital.set_budgets(half, half);
        if let Some(execution) = self.execution.as_ref() {
            execution.update_target_notional(half);
        }
        let _ = self.log_tx.send(LogMessage::Info(
            format!(
                "[CAPITAL] rebalance slot_a={} slot_b={} total_usdt={}",
                half, half, total_usdt
            )
            .into(),
        ));
    }

    fn rebalance_slots_if_free(&mut self) {
        if self.capital.both_idle() {
            let total_usdt = self.current_usdt_free();
            self.rebalance_slots_with_total(total_usdt);
        }
    }

    fn maybe_top_up_bnb(&mut self, portfolio_value: Decimal, usdt_free: Decimal) -> Decimal {
        let Some(execution) = self.execution.as_ref() else {
            return Decimal::ZERO;
        };
        let Some(bnb_symbol) = Symbol::from_str("BNBUSDT") else {
            return Decimal::ZERO;
        };
        if portfolio_value <= Decimal::ZERO || usdt_free <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        let desired =
            (portfolio_value * Decimal::from_f64(0.01).unwrap_or(Decimal::ZERO)).normalize();
        let spend = floor_usdt_cents(desired.min(usdt_free));
        let min_spend = Decimal::from_f64(5.0).unwrap_or(Decimal::ZERO);
        if spend < min_spend {
            let _ = self
                .log_tx
                .send(LogMessage::Info("[FEE] skip: insufficient USDT".into()));
            return Decimal::ZERO;
        }
        let client_id = format!("FEE-{}", self.clock.monotonic_now_ns());
        match execution.place_quote_market(bnb_symbol, spend, client_id.clone()) {
            Ok(order_id) => {
                let _ = self.log_tx.send(LogMessage::Info(
                    format!(
                        "[FEE] topup market_buy symbol={} spend_usdt={} cid={} oid={}",
                        bnb_symbol, spend, client_id, order_id
                    )
                    .into(),
                ));
                spend
            }
            Err(err) => {
                let _ = self.log_tx.send(LogMessage::Warn(
                    format!(
                        "[FEE] topup_failed symbol={} spend_usdt={} cid={} err={}",
                        bnb_symbol, spend, client_id, err
                    )
                    .into(),
                ));
                Decimal::ZERO
            }
        }
    }

    fn handle_balance_update(&mut self, asset: String, delta: Decimal) {
        let entry = self
            .balances
            .entry(asset.clone())
            .or_insert(BalanceSnapshot {
                asset: asset.clone(),
                free: Decimal::ZERO,
                locked: Decimal::ZERO,
            });
        entry.free += delta;
        let snapshot = entry.clone();
        self.log_balance_change(&asset, &snapshot, Some((delta, Decimal::ZERO)));
        if asset.eq_ignore_ascii_case("USDT") {
            self.usdt_free = snapshot.free;
        }
        self.usdt_free_fallback = self.usdt_free;
        self.rebalance_slots_if_free();
    }

    fn handle_balance_snapshot(&mut self, balances: Vec<BalanceSnapshot>) {
        let initial = self.balances.is_empty();
        for bal in balances {
            let delta = self
                .balances
                .get(&bal.asset)
                .map(|prev| (bal.free - prev.free, bal.locked - prev.locked));
            if initial
                || delta
                    .as_ref()
                    .map(|(df, dl)| *df != Decimal::ZERO || *dl != Decimal::ZERO)
                    .unwrap_or(true)
            {
                self.log_balance_change(&bal.asset, &bal, delta);
            }
            self.balances.insert(bal.asset.clone(), bal);
        }
        if let Some(usdt) = self.balances.get("USDT") {
            self.usdt_free = usdt.free;
        }
        self.usdt_free_fallback = self.usdt_free;
        self.rebalance_slots_if_free();
    }

    fn handle_open_orders(&mut self, orders: Vec<crate::types::OpenOrderSnapshot>) {
        if orders.is_empty() {
            return;
        }
        let managed: Vec<_> = orders
            .iter()
            .filter(|o| o.client_order_id.starts_with("SB-"))
            .collect();
        let external = orders.len().saturating_sub(managed.len());
        let _ = self.log_tx.send(LogMessage::Info(
            format!(
                "[ACCT] open_orders total={} managed={} external={}",
                orders.len(),
                managed.len(),
                external
            )
            .into(),
        ));
        if let Some(state) = self.pending_tp.state() {
            let tp_client_id = state.tp_client_id.clone();
            let symbol = state.symbol;
            let buy_client_id = state.buy_client_id.clone();
            if orders.iter().any(|order| {
                order.client_order_id == tp_client_id
                    && order.symbol == symbol
                    && order.side.eq_ignore_ascii_case("SELL")
            }) {
                if self.pending_tp.clear_if_tp(&tp_client_id) {
                    let _ = self.log_tx.send(LogMessage::Warn(
                        format!(
                            "[GATE] pending_tp cleared proof=open_orders symbol={} cid={} tp_cid={}",
                            symbol, buy_client_id, tp_client_id
                        )
                        .into(),
                    ));
                }
            }
        }
    }

    fn handle_buy_submitted(
        &mut self,
        symbol: Symbol,
        buy_client_order_id: String,
        _tp_client_order_id: String,
    ) {
        let Some((slot, _)) = self.capital.slot_for_client(&buy_client_order_id) else {
            let _ = self.log_tx.send(LogMessage::Warn(
                format!(
                    "[GATE] pending_tp skipped: slot missing symbol={} cid={}",
                    symbol, buy_client_order_id
                )
                .into(),
            ));
            return;
        };
        self.enter_pending_tp(slot, symbol, buy_client_order_id, "buy_submitted");
    }

    fn log_balance_change(
        &self,
        asset: &str,
        bal: &BalanceSnapshot,
        delta: Option<(Decimal, Decimal)>,
    ) {
        let mut msg = format!(
            "[ACCT] BAL asset={} free={} locked={}",
            asset, bal.free, bal.locked
        );
        if let Some((df, dl)) = delta {
            msg.push_str(&format!(" (Δfree={} Δlocked={})", df, dl));
        }
        let _ = self.log_tx.send(LogMessage::Info(msg.into()));
    }

    fn log_execution_report(&self, report: &AccountExecutionReport) {
        let mut parts = Vec::new();
        parts.push(format!("symbol={}", report.symbol));
        parts.push(format!("side={}", report.side));
        parts.push(format!("status={}", report.status));
        parts.push(format!("exec_type={}", report.exec_type));
        parts.push(format!("cid={}", report.client_order_id));
        if let Some(orig) = &report.orig_client_order_id {
            parts.push(format!("orig_cid={orig}"));
        }
        if let Some(order_id) = &report.order_id {
            parts.push(format!("oid={order_id}"));
        }
        parts.push(format!("price={}", report.price));
        parts.push(format!("orig_qty={}", report.orig_qty));
        parts.push(format!("cum_qty={}", report.cum_qty));
        parts.push(format!("last_qty={}", report.last_qty));
        parts.push(format!("last_px={}", report.last_price));
        if let Some(fee) = report.commission {
            if let Some(asset) = &report.commission_asset {
                parts.push(format!("fee={fee} {asset}"));
            } else {
                parts.push(format!("fee={fee}"));
            }
        }
        if let Some(reason) = &report.reject_reason {
            parts.push(format!("reason={reason}"));
        }
        let line = format!("[ACCT] ORDER {}", parts.join(" "));
        let _ = self.log_tx.send(LogMessage::Info(line.into()));
    }
    fn handle_event(&mut self, event: PriceEvent) {
        let now = self.clock.now_instant();
        if now.duration_since(event.received_instant) > self.queue_age {
            let _ = self.log_tx.send(
                MetricEvent::QueueDropMarket {
                    symbol: event.symbol,
                }
                .into(),
            );
            return;
        }

        let event_time = now;

        self.benchmarks.record(event.symbol, now, event.price);
        self.last_prices.insert(event.symbol, event.price);

        let minute_id = if event.event_ts_ms > 0 {
            Some(event.event_ts_ms / 60_000)
        } else {
            None
        };

        let (trigger_ret, diag_snapshot) = {
            let state = self
                .symbols
                .entry(event.symbol)
                .or_insert_with(|| SymbolState::new(self.config.trigger.window));
            state.rings.record(event_time, event.price);
            let _ = state.rings.warm(now);

            let ret = minute_id.and_then(|mid| state.minute_ret(mid, event.price));
            let diag = state.diag_snapshot(minute_id, event.price);
            (ret, diag)
        };

        if self.config.logging.profile == LogProfile::Verbose && now >= self.diag_next_log {
            let ret_pct = diag_snapshot.ret_from_open * 100.0;
            info!(
                target: "diagnostic",
                "minute_diag symbol={} raw_ts_ns={} event_ts_ms={} minute_id={:?} open_px={:.8} last_px={:.8} ret_from_open={:.4}% triggered={}",
                event.symbol,
                event.exch_ts_ns,
                event.event_ts_ms,
                diag_snapshot.minute_id,
                diag_snapshot.open_price,
                event.price,
                ret_pct,
                diag_snapshot.triggered_this_minute
            );
            self.diag_next_log = now + Duration::from_secs(5);
        }

        if let Some(price_dec) = Decimal::from_f64(event.price) {
            if let Some(decision) =
                self.positions
                    .on_tick(event.symbol, price_dec, &self.config.strategy)
            {
                self.process_exit_decision(event.symbol, price_dec, decision);
            }
        }

        let Some(ret_from_open) = trigger_ret else {
            return;
        };

        if ret_from_open < self.config.trigger.trigger_pct {
            return;
        }

        if self.pending_tp.is_active() {
            if let Some(state) = self.pending_tp.state() {
                let _ = self.log_tx.send(LogMessage::Warn(
                    format!(
                        "[GATE] pending_tp suppress symbol={} pending_symbol={} cid={}",
                        event.symbol, state.symbol, state.buy_client_id
                    )
                    .into(),
                ));
            }
            let _ = self.log_tx.send(
                MetricEvent::SignalSuppressed {
                    symbol: event.symbol,
                    reason: SignalSuppressReason::PendingTp,
                }
                .into(),
            );
            return;
        }

        if self.positions.contains(event.symbol) || self.capital.contains(event.symbol) {
            return;
        }

        if let Some(state) = self.symbols.get_mut(&event.symbol) {
            state.mark_triggered();
        }

        let trigger_instant = now;
        let _ = self.log_tx.send(
            MetricEvent::TriggerEmitted {
                symbol: event.symbol,
            }
            .into(),
        );

        let now_ksa = self.config.ksa_now(self.clock.as_ref());
        let trading_gate_blocked =
            self.config.execution.trading_gate_enabled && !self.trading_gate.is_enabled();
        let warmup_blocked = !self.warmup_gate.is_warm();
        let armed_live =
            self.config.execution.mode == ExecutionMode::Live && self.config.execution.live_armed;

        if self.config.execution.mode == ExecutionMode::Live && !self.config.execution.live_armed {
            let _ = self.log_tx.send(
                MetricEvent::SignalSuppressed {
                    symbol: event.symbol,
                    reason: SignalSuppressReason::NotArmed,
                }
                .into(),
            );
            return;
        }

        if self.config.execution.mode == ExecutionMode::Live && trading_gate_blocked {
            let _ = self.log_tx.send(
                MetricEvent::SignalSuppressed {
                    symbol: event.symbol,
                    reason: SignalSuppressReason::TradingDisabled,
                }
                .into(),
            );
            return;
        }

        if self.config.execution.mode == ExecutionMode::Live && warmup_blocked {
            let _ = self.log_tx.send(
                MetricEvent::SignalSuppressed {
                    symbol: event.symbol,
                    reason: SignalSuppressReason::Warmup,
                }
                .into(),
            );
            return;
        }

        if self.config.execution.mode == ExecutionMode::Live && !armed_live && !self.mode.is_live()
        {
            let _ = self.log_tx.send(
                MetricEvent::SignalSuppressed {
                    symbol: event.symbol,
                    reason: SignalSuppressReason::ModeNotLive,
                }
                .into(),
            );
            return;
        }

        if self.is_haram_symbol(event.symbol) {
            if self.haram_logged.insert(event.symbol) {
                let _ = self.log_tx.send(LogMessage::Warn(
                    format!("[RISK] haram {}", event.symbol).into(),
                ));
            }
            let _ = self.log_tx.send(
                MetricEvent::RiskDenyHaram {
                    symbol: event.symbol,
                }
                .into(),
            );
            let _ = self.log_tx.send(
                MetricEvent::SignalSuppressed {
                    symbol: event.symbol,
                    reason: SignalSuppressReason::HaramSymbol,
                }
                .into(),
            );
            return;
        }

        match self.risk.evaluate_trade(event.symbol, now_ksa) {
            Ok(Some(block)) => {
                self.log_trade_block(event.symbol, block);
                let _ = self.log_tx.send(
                    MetricEvent::SignalSuppressed {
                        symbol: event.symbol,
                        reason: SignalSuppressReason::RiskBlocked,
                    }
                    .into(),
                );
                return;
            }
            Ok(None) => {}
            Err(err) => {
                let _ = self.log_tx.send(LogMessage::Error(format!(
                    "[RISK] evaluate_trade failed {} err={err:?}",
                    event.symbol
                )));
                return;
            }
        }

        let Some(slot) = self.capital.first_idle_slot(now) else {
            if self.capital.reserved_only() {
                let _ = self.log_tx.send(
                    MetricEvent::SignalSuppressed {
                        symbol: event.symbol,
                        reason: SignalSuppressReason::NoCapital,
                    }
                    .into(),
                );
            }
            return;
        };

        let slot_budget = self.capital.slot_budget(slot);
        let available_usdt = self.current_usdt_free();
        if available_usdt < slot_budget {
            let _ = self.log_tx.send(
                MetricEvent::SignalSuppressed {
                    symbol: event.symbol,
                    reason: SignalSuppressReason::NoCapital,
                }
                .into(),
            );
            let _ = self.capital.release_slot(slot);
            self.rebalance_slots_if_free();
            return;
        }

        let (buy_client_order_id, tp_client_order_id) = self.client_ids.next(slot);
        if !self.capital.begin_pending_on_slot(
            now,
            slot,
            event.symbol,
            buy_client_order_id.clone(),
            tp_client_order_id.clone(),
        ) {
            return;
        }

        let slot_budget = self.capital.slot_budget(slot);
        if slot_budget <= Decimal::ZERO {
            let _ = self.log_tx.send(
                MetricEvent::SignalSuppressed {
                    symbol: event.symbol,
                    reason: SignalSuppressReason::NoCapital,
                }
                .into(),
            );
            let _ = self.capital.release_slot(slot);
            return;
        }

        let trigger = TriggerEvent {
            symbol: event.symbol,
            price_now: event.price,
            ret_from_open,
            price_rx_instant: event.received_instant,
            trigger_instant,
            target_notional: slot_budget,
            trigger_ts_mono_ns: self.clock.instant_to_ns(trigger_instant),
            signal_ts_mono_ns: event.ts_mono_ns,
            slot,
            buy_client_order_id,
            tp_client_order_id,
        };

        match self.trigger_tx.try_send(trigger.clone()) {
            Ok(()) => {
                // Reserved on account once execution reports are received.
            }
            Err(TrySendError::Full(_)) => {
                let _ = self.capital.release_slot(slot);
                let _ = self.log_tx.send(
                    MetricEvent::QueueDropTrigger {
                        symbol: event.symbol,
                    }
                    .into(),
                );
            }
            Err(TrySendError::Disconnected(_)) => {
                let _ = self.capital.release_slot(slot);
                let _ = self.log_tx.send(LogMessage::Error(
                    "trigger channel disconnected".to_string(),
                ));
            }
        }
    }

    fn process_exit_decision(&mut self, symbol: Symbol, price: Decimal, decision: ExitDecision) {
        match decision {
            ExitDecision::Hold => {}
            ExitDecision::LimitFilled => self.handle_limit_filled(symbol),
            ExitDecision::Market { reason } => self.handle_market_exit(symbol, price, reason),
        }
    }

    fn handle_limit_filled(&mut self, symbol: Symbol) {
        if let Some(position) = self.positions.get_mut(symbol) {
            position.clear_tp_target();
        }
        if let Some(position) = self.positions.close(symbol) {
            if let Some(execution) = self.execution.as_ref() {
                execution.record_limit_fill(symbol);
            }
            let take_profit = position.take_profit;
            self.finalize_exit(position, take_profit, ExitReason::TakeProfitLimit);
        }
    }

    fn handle_market_exit(&mut self, symbol: Symbol, price: Decimal, reason: ExitReason) {
        let Some(position_snapshot) = self.positions.get(symbol).cloned() else {
            return;
        };

        let Some(execution) = self.execution.as_ref() else {
            self.positions.mark_closing_failed(symbol);
            let _ = self.log_tx.send(LogMessage::Warn(
                format!(
                    "[SELL] market close skipped execution handle missing {}",
                    symbol
                )
                .into(),
            ));
            return;
        };

        let target_qty = if position_snapshot.tp_order_qty > Decimal::ZERO {
            position_snapshot.tp_order_qty
        } else {
            position_snapshot.qty
        };
        let target = TargetInfo {
            order_id: position_snapshot.tp_order_id.clone(),
            qty: target_qty,
        };

        match execution.submit_close_with_cancel(symbol, position_snapshot.qty, reason, target) {
            Ok(()) => {
                if let Some(position) = self.positions.get_mut(symbol) {
                    position.clear_tp_target();
                }
                if let Some(position) = self.positions.close(symbol) {
                    self.finalize_exit(position, price, reason);
                }
            }
            Err(err) => {
                self.positions.mark_closing_failed(symbol);
                let _ = self.log_tx.send(LogMessage::Error(format!(
                    "[SELL] market close failed {} reason={reason:?} err={err}",
                    symbol
                )));
            }
        }
    }

    fn finalize_exit(&mut self, position: Position, exit_price: Decimal, reason: ExitReason) {
        let _ = self.capital.release_slot(position.slot);
        self.rebalance_slots_if_free();

        let strat = &self.config.strategy;
        let qty = position.qty;
        let entry_notional = (position.entry_price * qty).normalize();
        let buy_fee = (entry_notional * strat.taker_fee_pct).normalize();
        let exit_notional = (exit_price * qty).normalize();
        let sell_fee_rate = match reason {
            ExitReason::TakeProfitLimit => strat.maker_fee_pct,
            ExitReason::StopLoss | ExitReason::ReturnToEntry => strat.taker_fee_pct,
        };
        let sell_fee = (exit_notional * sell_fee_rate).normalize();
        let net_profit = (exit_notional - sell_fee) - (entry_notional + buy_fee);
        let pnl_return = if entry_notional <= Decimal::ZERO {
            Decimal::ZERO
        } else {
            (net_profit / entry_notional).normalize()
        };
        let pnl_f64 = pnl_return.to_f64().unwrap_or(0.0);
        if pnl_f64 < 0.0 {
            self.mode.mark_loss();
        }

        let now_ksa = self.config.ksa_now(self.clock.as_ref());
        match self.risk.mark_trade_close(
            position.symbol,
            pnl_return,
            pnl_return < Decimal::ZERO,
            now_ksa,
        ) {
            Ok(effects) => {
                if effects.freeze_triggered {
                    let _ = self
                        .log_tx
                        .send(LogMessage::Warn("[RISK] global freeze (daily loss)".into()));
                    let _ = self.log_tx.send(MetricEvent::RiskFreezeDailyLoss.into());
                }
                if effects.banned_until.is_some() {
                    let _ = self.log_tx.send(LogMessage::Warn(
                        format!(
                            "[RISK] banned {} for {}d ({} losses)",
                            position.symbol,
                            self.config.strategy.ban_window_days,
                            self.config.strategy.ban_losses_threshold
                        )
                        .into(),
                    ));
                    let _ = self.log_tx.send(
                        MetricEvent::RiskBanCreated {
                            symbol: position.symbol,
                        }
                        .into(),
                    );
                }
            }
            Err(err) => {
                let _ = self.log_tx.send(LogMessage::Error(format!(
                    "risk_mark_close_failed {}: {err:?}",
                    position.symbol
                )));
            }
        }

        let hold_secs = position.entry_ts.elapsed().as_secs();
        let qty_f64 = qty.to_f64().unwrap_or(0.0);
        let exit_f64 = exit_price.to_f64().unwrap_or(0.0);
        if matches!(reason, ExitReason::TakeProfitLimit) {
            let _ = self.log_tx.send(LogMessage::Info(
                format!(
                    "[SELL] limit fill reason=tp {} price={:.6} qty={:.4} hold_s={} pnl={:.2}%",
                    position.symbol,
                    exit_f64,
                    qty_f64,
                    hold_secs,
                    pnl_f64 * 100.0
                )
                .into(),
            ));
        }
    }

    fn is_haram_symbol(&self, symbol: Symbol) -> bool {
        self.haram_symbols.contains(&symbol)
    }

    fn log_trade_block(&mut self, symbol: Symbol, block: TradeBlock) {
        match block {
            TradeBlock::GlobalFreeze { .. } => {}
            TradeBlock::NoRebuyUntil { first, .. } => {
                if first {
                    let _ = self.log_tx.send(LogMessage::Warn(
                        format!("[RISK] no-rebuy-today {}", symbol).into(),
                    ));
                    let _ = self
                        .log_tx
                        .send(MetricEvent::RiskDenyRebuyToday { symbol }.into());
                }
            }
            TradeBlock::Banned { first, .. } => {
                if first {
                    let _ = self.log_tx.send(LogMessage::Warn(
                        format!(
                            "[RISK] banned {} for {}d ({} losses)",
                            symbol,
                            self.config.strategy.ban_window_days,
                            self.config.strategy.ban_losses_threshold
                        )
                        .into(),
                    ));
                }
            }
            TradeBlock::Disabled { .. } => {}
        }
    }
    fn maybe_snapshot_idle(&mut self) {
        let now = self.clock.now_instant();
        if now
            .checked_duration_since(self.last_snapshot)
            .unwrap_or_default()
            < SNAPSHOT_INTERVAL
        {
            return;
        }
        self.last_snapshot = now;
        for state in self.symbols.values_mut() {
            state.rings.snapshot(now);
        }
    }

    fn maybe_compute_ahi(&mut self) {
        let now = self.clock.now_instant();
        if now < self.next_ahi_compute {
            return;
        }
        self.next_ahi_compute = now + self.config.strategy.ahi.compute_interval;
        self.recompute_ahi(now);
    }

    fn recompute_ahi(&mut self, now: Instant) {
        let mut symbol_returns = Vec::with_capacity(self.symbols.len());
        for state in self.symbols.values() {
            let ret_15m = state.rings.ret_15m();
            let ret_1h = state.rings.ret_1h();
            if ret_15m.is_none() && ret_1h.is_none() {
                continue;
            }
            symbol_returns.push(SymbolReturns { ret_15m, ret_1h });
        }

        let ethbtc_ret = self.benchmarks.eth_ret_1h();
        let sample = self.ahi_calculator.compute(symbol_returns, ethbtc_ret);

        self.ahi_history.record(now, sample.value);
        self.last_ahi_sample = Some(sample);

        let ahi_avg = self
            .ahi_history
            .average_over(now, self.config.strategy.ahi.enter_window);
        let ahi_drop = self
            .ahi_history
            .drop_within(now, self.config.strategy.ahi.drop_window);
        let btc_ret_abs = self.benchmarks.btc_ret_15m_abs();

        self.mode
            .update(now, sample.value, ahi_avg, ahi_drop, btc_ret_abs);
    }

    fn apply_schema_swap(&mut self, universe: Universe) {
        let symbol_capacity = universe.symbols.len();
        self.rings.reinit_for(&universe);
        self.universe.install(universe);

        self.symbols.clear();
        if symbol_capacity > self.symbols.capacity() {
            self.symbols
                .reserve(symbol_capacity - self.symbols.capacity());
        }

        self.benchmarks = Benchmarks::new();
        let now = self.clock.now_instant();
        let hold = Duration::from_secs(RECONNECT_WARMUP_SECS);
        for (&symbol, &price) in &self.last_prices {
            let state = self
                .symbols
                .entry(symbol)
                .or_insert_with(|| SymbolState::new(self.config.trigger.window));
            state.bootstrap(now, Some(price), hold);
        }
        self.ahi_history
            .reset(now, self.last_ahi_sample.map(|s| s.value).unwrap_or(0.0));
        self.last_ahi_sample = None;
        self.next_ahi_compute = now;
        self.mode = ModeMachine::new(self.config, now);
        self.warmup_gate
            .arm_for(Duration::from_secs(RECONNECT_WARMUP_SECS));
        let _ = self
            .log_tx
            .send(LogMessage::Info("[SCHEMA] swap_done".into()));
    }
}

enum ProcessorCommand {
    ResetDaily {
        ack: Sender<()>,
    },
    AttachExecution {
        handle: ExecutionHandle,
        ack: Sender<()>,
    },
    SchemaSwap {
        universe: Universe,
        ack: Sender<()>,
    },
}

struct SymbolState {
    rings: SymbolRings,
    current_minute_id: Option<u64>,
    open_price: f64,
    triggered_this_minute: bool,
}

impl SymbolState {
    #[allow(clippy::too_many_arguments)]
    fn new(window: Duration) -> Self {
        Self {
            rings: SymbolRings::new(window),
            current_minute_id: None,
            open_price: 0.0,
            triggered_this_minute: false,
        }
    }

    fn bootstrap(&mut self, now: Instant, price: Option<f64>, hold: Duration) {
        self.rings.bootstrap(now, price, hold);
        self.current_minute_id = None;
        self.triggered_this_minute = false;
        self.open_price = 0.0;
    }

    fn minute_ret(&mut self, minute_id: u64, price: f64) -> Option<f64> {
        if price <= 0.0 {
            return None;
        }

        if self.current_minute_id != Some(minute_id) {
            self.current_minute_id = Some(minute_id);
            self.open_price = price;
            self.triggered_this_minute = false;
        }

        if self.triggered_this_minute || self.open_price <= 0.0 {
            return None;
        }

        Some(price / self.open_price - 1.0)
    }

    fn mark_triggered(&mut self) {
        self.triggered_this_minute = true;
    }

    fn diag_snapshot(&self, minute_id: Option<u64>, price: f64) -> DiagSnapshot {
        let open_price = self.open_price;
        let ret_from_open = if open_price > 0.0 {
            price / open_price - 1.0
        } else {
            0.0
        };
        DiagSnapshot {
            minute_id: minute_id.or(self.current_minute_id),
            open_price,
            ret_from_open,
            triggered_this_minute: self.triggered_this_minute,
        }
    }
}

#[derive(Clone, Copy)]
struct DiagSnapshot {
    minute_id: Option<u64>,
    open_price: f64,
    ret_from_open: f64,
    triggered_this_minute: bool,
}

struct Benchmarks {
    btc: Benchmark,
    eth: Benchmark,
    ethbtc: EthBtcRatio,
}

impl Benchmarks {
    #[allow(clippy::too_many_arguments)]
    fn new() -> Self {
        Self {
            btc: Benchmark::new(Symbol::from_str("BTCUSDT").expect("valid BTCUSDT symbol")),
            eth: Benchmark::new(Symbol::from_str("ETHUSDT").expect("valid ETHUSDT symbol")),
            ethbtc: EthBtcRatio::new(),
        }
    }

    fn record(&mut self, symbol: Symbol, ts: Instant, price: f64) {
        let mut updated = false;
        if symbol == self.btc.symbol {
            self.btc.record(ts, price);
            updated = true;
        } else if symbol == self.eth.symbol {
            self.eth.record(ts, price);
            updated = true;
        }

        if updated {
            if let (Some(btc_px), Some(eth_px)) = (self.btc.last_price(), self.eth.last_price()) {
                if btc_px > 0.0 && eth_px > 0.0 {
                    let ratio = eth_px / btc_px;
                    self.ethbtc.record(ts, ratio);
                }
            }
        }
    }

    fn btc_ret_15m_abs(&self) -> f64 {
        self.btc.ret_15m_abs()
    }

    fn eth_ret_1h(&self) -> Option<f64> {
        self.ethbtc.ret_1h()
    }
}

struct Benchmark {
    symbol: Symbol,
    ring_15m: RingBuffer,
    ring_1h: RingBuffer,
    last_price: Option<f64>,
}

impl Benchmark {
    #[allow(clippy::too_many_arguments)]
    fn new(symbol: Symbol) -> Self {
        let fifteen_min = Duration::from_secs(15 * 60);
        let one_hour = Duration::from_secs(60 * 60);
        Self {
            symbol,
            ring_15m: RingBuffer::new(fifteen_min),
            ring_1h: RingBuffer::new(one_hour),
            last_price: None,
        }
    }

    fn record(&mut self, ts: Instant, price: f64) {
        self.ring_15m.push(ts, price);
        self.ring_1h.push(ts, price);
        self.last_price = Some(price);
    }

    fn last_price(&self) -> Option<f64> {
        self.last_price
    }

    fn ret_15m_abs(&self) -> f64 {
        abs_return_over(&self.ring_15m, Duration::from_secs(15 * 60))
    }

    #[allow(dead_code)]
    fn ret_1h(&self) -> Option<f64> {
        return_over(&self.ring_1h, Duration::from_secs(60 * 60))
    }
}

struct EthBtcRatio {
    ring_1h: RingBuffer,
}

impl EthBtcRatio {
    fn new() -> Self {
        Self {
            ring_1h: RingBuffer::new(Duration::from_secs(60 * 60)),
        }
    }

    fn record(&mut self, ts: Instant, ratio: f64) {
        self.ring_1h.push(ts, ratio);
    }

    fn ret_1h(&self) -> Option<f64> {
        return_over(&self.ring_1h, Duration::from_secs(60 * 60))
    }
}

fn pending_recovery_delay(attempts: u32) -> Duration {
    let capped = attempts.saturating_sub(1).min(4);
    let base_secs = 5u64.saturating_mul(1u64 << capped);
    let jitter_ms = fastrand::u64(0..500);
    Duration::from_secs(base_secs.min(60)) + Duration::from_millis(jitter_ms)
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

fn floor_usdt_cents(value: Decimal) -> Decimal {
    let hundred = Decimal::from_i32(100).unwrap_or(Decimal::ONE);
    ((value * hundred).floor() / hundred).normalize()
}
