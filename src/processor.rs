use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use crossbeam_channel::{bounded, unbounded, Receiver, Sender, TryRecvError, TrySendError};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;

use crate::affinity;
use crate::ahi::AhiHistory;
use crate::alt_gate::{AhiCalculator, AhiSample, SymbolReturns};
use crate::capital::CapitalSlots;
use crate::channels::{SpscReceiver, SpscSender};
use crate::clock::Clock;
use crate::config::{Config, ServerSpec, ShardAssignment};
use crate::execution::{ExecutionHandle, TargetInfo};
use crate::fees::{breakeven_px, sl_trigger_px, tp_target_px};
use crate::gates::{TradingGate, WarmupGate};
use crate::mode::ModeMachine;
use crate::positions::{ExitDecision, ExitReason, Position, PositionBook};
use crate::rings::{abs_return_over, return_over, RingBuffer, RingsHandle, SymbolRings};
use crate::risk::{RiskEngine, RiskHandle, TradeBlock};
use crate::types::{LogMessage, MetricEvent, PriceEvent, ReconnectNotice, Symbol, TriggerEvent};
use crate::universe::{Universe, UniverseHandle};

const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(1);
const RET_THRESHOLD: f64 = 0.05;
const RECONNECT_WARMUP_SECS: u64 = 60;

#[allow(clippy::too_many_arguments)]
pub fn spawn_processor(
    config: &'static Config,
    server: &'static ServerSpec,
    clock: Arc<dyn Clock>,
    universe: UniverseHandle,
    rings: RingsHandle,
    trading_gate: Arc<TradingGate>,
    warmup_gate: Arc<WarmupGate>,
    shard_inputs: Vec<(ShardAssignment, SpscReceiver<PriceEvent>)>,
    reconnect_rx: Receiver<ReconnectNotice>,
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
    target_notional: Decimal,
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
    haram_symbols: HashSet<Symbol>,
    haram_logged: HashSet<Symbol>,
}
struct ShardChannel {
    receiver: SpscReceiver<PriceEvent>,
}

impl Processor {
    #[allow(clippy::too_many_arguments)]
    fn new(
        config: &'static Config,
        clock: Arc<dyn Clock>,
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
        let mode = ModeMachine::new(config, now);
        let capital = CapitalSlots::new();
        let positions = PositionBook::new();

        let target_notional = Decimal::from_f64(config.execution.order_quote_size_usdt)
            .unwrap_or_else(|| Decimal::from_f64(50.0).unwrap());
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
            target_notional,
            last_snapshot: now,
            next_ahi_compute,
            universe,
            rings,
            trading_gate,
            warmup_gate,
            command_rx,
            reconnect_rx,
            haram_symbols,
            haram_logged: HashSet::new(),
        }
    }

    fn run(&mut self) {
        loop {
            self.drain_commands();
            self.drain_reconnects();

            let mut made_progress = false;
            for idx in 0..self.shards.len() {
                match self.shards[idx].receiver.try_recv() {
                    Ok(event) => {
                        if !self.trading_gate.is_enabled() {
                            continue;
                        }
                        let allow_trading = self.warmup_gate.is_warm();
                        if !allow_trading {
                            continue;
                        }
                        made_progress = true;
                        self.handle_event(event, allow_trading);
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
                .or_insert_with(|| SymbolState::new(self.config.strategy.window_ret_60s));
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

    fn reset_daily_components(&mut self) {
        self.capital.reset_daily();
        self.risk.reset_daily_counters_keep_long_bans();
        self.positions.reset_daily_view();
        self.symbols.clear();
        self.last_prices.clear();
        self.benchmarks = Benchmarks::new();
        self.haram_logged.clear();
        let _ = self.log_tx.send(LogMessage::ResetDaily);
    }

    fn handle_event(&mut self, event: PriceEvent, allow_trading: bool) {
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

        let (ret_60s, symbol_warm) = {
            let state = self
                .symbols
                .entry(event.symbol)
                .or_insert_with(|| SymbolState::new(self.config.strategy.window_ret_60s));
            state.rings.record(event_time, event.price);
            let warm = state.rings.warm(now);
            (state.rings.ret_60s(), warm)
        };

        if !allow_trading {
            return;
        }

        if let Some(price_dec) = Decimal::from_f64(event.price) {
            if let Some(decision) =
                self.positions
                    .on_tick(event.symbol, price_dec, &self.config.strategy)
            {
                self.process_exit_decision(event.symbol, price_dec, decision);
            }
        }

        let Some(ret_60s) = ret_60s else {
            return;
        };

        if !self.mode.is_live() {
            let _ = self.log_tx.send(
                MetricEvent::SignalSuppressed {
                    symbol: event.symbol,
                }
                .into(),
            );
            return;
        }

        if !symbol_warm {
            return;
        }

        if ret_60s < RET_THRESHOLD {
            return;
        }

        if self.positions.contains(event.symbol) || self.capital.contains(event.symbol) {
            return;
        }

        let now_ksa = self.config.ksa_now(self.clock.as_ref());

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
            return;
        }

        match self.risk.evaluate_trade(event.symbol, now_ksa) {
            Ok(Some(block)) => {
                self.log_trade_block(event.symbol, block);
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

        let Some(slot) = self.capital.try_reserve_slot(now, event.symbol) else {
            return;
        };

        let Some(entry_price_dec) = Decimal::from_f64(event.price) else {
            self.capital.release_slot(slot);
            return;
        };
        let quantity_estimate = self.estimate_quantity(event.price);
        if quantity_estimate <= Decimal::ZERO {
            self.capital.release_slot(slot);
            return;
        }

        let strategy = &self.config.strategy;
        let take_profit = tp_target_px(
            entry_price_dec,
            strategy.tp_pct,
            strategy.maker_fee_pct,
            strategy.taker_fee_pct,
        );
        let stop_loss = sl_trigger_px(
            entry_price_dec,
            strategy.sl_pct,
            strategy.maker_fee_pct,
            strategy.taker_fee_pct,
        );
        let bounce_break_even = breakeven_px(
            entry_price_dec,
            strategy.maker_fee_pct,
            strategy.taker_fee_pct,
        );

        let trigger = TriggerEvent {
            symbol: event.symbol,
            price_now: event.price,
            ret_60s,
            target_notional: self.target_notional,
            trigger_ts_mono_ns: self.clock.instant_to_ns(now),
            signal_ts_mono_ns: event.ts_mono_ns,
            slot,
        };

        match self.trigger_tx.try_send(trigger.clone()) {
            Ok(()) => {
                let tp_order_id = format!("{}-tp-{}", slot.label(), trigger.trigger_ts_mono_ns);
                self.positions.open(
                    event.symbol,
                    quantity_estimate,
                    entry_price_dec,
                    slot,
                    now,
                    take_profit,
                    stop_loss,
                    bounce_break_even,
                    Some(tp_order_id),
                    quantity_estimate,
                );
                self.risk.mark_trade_open(event.symbol, now_ksa);
                let _ = self.log_tx.send(
                    MetricEvent::TriggerEmitted {
                        symbol: event.symbol,
                    }
                    .into(),
                );
            }
            Err(TrySendError::Full(_)) => {
                self.capital.release_slot(slot);
                let _ = self.log_tx.send(
                    MetricEvent::QueueDropTrigger {
                        symbol: event.symbol,
                    }
                    .into(),
                );
            }
            Err(TrySendError::Disconnected(_)) => {
                self.capital.release_slot(slot);
                let _ = self.log_tx.send(LogMessage::Error(
                    "trigger channel disconnected".to_string(),
                ));
            }
        }
    }

    fn estimate_quantity(&self, price: f64) -> Decimal {
        if price <= 0.0 {
            return Decimal::ZERO;
        }
        let price_dec = Decimal::from_f64(price).unwrap_or(Decimal::ZERO);
        if price_dec <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        (self.target_notional / price_dec).max(Decimal::ZERO)
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
        self.capital.release_slot(position.slot);

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
                .or_insert_with(|| SymbolState::new(self.config.strategy.window_ret_60s));
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
}

impl SymbolState {
    #[allow(clippy::too_many_arguments)]
    fn new(window: Duration) -> Self {
        Self {
            rings: SymbolRings::new(window),
        }
    }

    fn bootstrap(&mut self, now: Instant, price: Option<f64>, hold: Duration) {
        self.rings.bootstrap(now, price, hold);
    }
}

struct Benchmarks {
    btc: Benchmark,
    eth: Benchmark,
}

impl Benchmarks {
    #[allow(clippy::too_many_arguments)]
    fn new() -> Self {
        Self {
            btc: Benchmark::new(Symbol::from_str("BTCUSDT").expect("valid BTCUSDT symbol")),
            eth: Benchmark::new(Symbol::from_str("ETHBTC").expect("valid ETHBTC symbol")),
        }
    }

    fn record(&mut self, symbol: Symbol, ts: Instant, price: f64) {
        if symbol == self.btc.symbol {
            self.btc.record(ts, price);
        } else if symbol == self.eth.symbol {
            self.eth.record(ts, price);
        }
    }

    fn btc_ret_15m_abs(&self) -> f64 {
        self.btc.ret_15m_abs()
    }

    fn eth_ret_1h(&self) -> Option<f64> {
        self.eth.ret_1h()
    }
}

struct Benchmark {
    symbol: Symbol,
    ring_15m: RingBuffer,
    ring_1h: RingBuffer,
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
        }
    }

    fn record(&mut self, ts: Instant, price: f64) {
        self.ring_15m.push(ts, price);
        self.ring_1h.push(ts, price);
    }

    fn ret_15m_abs(&self) -> f64 {
        abs_return_over(&self.ring_15m, Duration::from_secs(15 * 60))
    }

    fn ret_1h(&self) -> Option<f64> {
        return_over(&self.ring_1h, Duration::from_secs(60 * 60))
    }
}
