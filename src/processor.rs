use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
#[cfg(feature = "test-mode")]
use crossbeam_channel::bounded;
use crossbeam_channel::{unbounded, Receiver, Sender, TryRecvError, TrySendError};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;

use crate::affinity;
use crate::ahi::AhiAggregator;
use crate::capital::CapitalSlots;
#[cfg(feature = "test-mode")]
use crate::capital::CapitalSnapshot;
use crate::channels::{SpscReceiver, SpscSender};
use crate::clock::Clock;
use crate::config::{Config, ServerSpec, ShardAssignment};
use crate::gates::{TradingGate, WarmupGate};
use crate::mode::ModeMachine;
#[cfg(feature = "test-mode")]
use crate::positions::PositionsSnapshot;
use crate::positions::{ExitReason, PositionBook};
use crate::rings::{abs_return_over, RingBuffer, RingsHandle, SymbolRings};
#[cfg(feature = "test-mode")]
use crate::risk::RiskSnapshot;
use crate::risk::{RiskEngine, RiskHandle};
use crate::types::{LogMessage, MetricEvent, PriceEvent, Symbol, TriggerEvent};
use crate::universe::UniverseHandle;

const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(1);
const RET_THRESHOLD: f64 = 0.05;

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
    trigger_tx: SpscSender<TriggerEvent>,
    log_tx: Sender<LogMessage>,
    #[cfg(feature = "test-mode")] price_event_notifier: Option<Sender<()>>,
) -> (thread::JoinHandle<()>, ProcessorHandle) {
    let (cmd_tx, cmd_rx) = unbounded();
    let (risk_engine, risk_handle) =
        RiskEngine::new(config, clock.clone()).expect("failed to initialise risk engine");
    let handle = thread::Builder::new()
        .name(format!("processor-{:?}", server.id))
        .spawn({
            let universe_clone = universe.clone();
            let rings_clone = rings.clone();
            let trading_clone = trading_gate.clone();
            let warmup_clone = warmup_gate.clone();
            let risk_engine = risk_engine;
            let clock = clock.clone();
            #[cfg(feature = "test-mode")]
            let price_event_notifier_clone = price_event_notifier.clone();
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
                    #[cfg(feature = "test-mode")]
                    price_event_notifier_clone,
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

    pub fn bump_epoch(&self) -> u64 {
        self.trading.bump_epoch()
    }

    pub fn epoch(&self) -> u64 {
        self.trading.epoch()
    }

    pub fn reset_daily_state(&self) -> Result<()> {
        self.send_command(|ack| ProcessorCommand::ResetDaily { ack })
    }

    pub fn risk_handle(&self) -> RiskHandle {
        self.risk.clone()
    }

    #[cfg(feature = "test-mode")]
    pub fn force_trading_enabled_for_tests(&self) {
        self.trading.enable();
    }

    #[cfg(feature = "test-mode")]
    pub fn force_live_for_tests(&self, live: bool) -> Result<()> {
        self.send_command(|ack| ProcessorCommand::ForceModeLiveForTests { value: live, ack })
    }

    #[cfg(feature = "test-mode")]
    pub fn snapshot(&self) -> ProcessorSnapshot {
        let (tx, rx) = bounded(1);
        self.cmd_tx
            .send(ProcessorCommand::Snapshot { tx })
            .expect("processor command channel disconnected");
        rx.recv().expect("snapshot channel disconnected")
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
    trigger_tx: SpscSender<TriggerEvent>,
    log_tx: Sender<LogMessage>,
    symbols: HashMap<Symbol, SymbolState>,
    queue_age: Duration,
    ahi: AhiAggregator,
    mode: ModeMachine,
    capital: CapitalSlots,
    positions: PositionBook,
    risk: RiskEngine,
    benchmarks: Benchmarks,
    target_notional: Decimal,
    last_snapshot: Instant,
    #[allow(dead_code)]
    universe: UniverseHandle,
    #[allow(dead_code)]
    rings: RingsHandle,
    trading_gate: Arc<TradingGate>,
    warmup_gate: Arc<WarmupGate>,
    command_rx: Receiver<ProcessorCommand>,
    #[cfg(feature = "test-mode")]
    price_event_notifier: Option<Sender<()>>,
}

struct ShardChannel {
    receiver: SpscReceiver<PriceEvent>,
}

#[cfg(feature = "test-mode")]
struct EventAckGuard {
    tx: Sender<()>,
}

#[cfg(feature = "test-mode")]
impl EventAckGuard {
    fn new(tx: Sender<()>) -> Self {
        Self { tx }
    }
}

#[cfg(feature = "test-mode")]
impl Drop for EventAckGuard {
    fn drop(&mut self) {
        let _ = self.tx.send(());
    }
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
        #[cfg(feature = "test-mode")] price_event_notifier: Option<Sender<()>>,
    ) -> Self {
        let shards = shard_inputs
            .into_iter()
            .map(|(_, receiver)| ShardChannel { receiver })
            .collect();

        let now = clock.now_instant();
        let window = config
            .strategy
            .ahi
            .enter_window
            .max(config.strategy.ahi.drop_window);
        let ahi = AhiAggregator::new(window);
        let mode = ModeMachine::new(config, now);
        let capital = CapitalSlots::new();
        let positions = PositionBook::new();

        let target_notional = Decimal::from_f64(config.execution.order_quote_size_usdt)
            .unwrap_or_else(|| Decimal::from_f64(50.0).unwrap());

        Self {
            config,
            clock,
            shards,
            trigger_tx,
            log_tx,
            symbols: HashMap::with_capacity(512),
            queue_age: config.backpressure.max_queue_age,
            ahi,
            mode,
            capital,
            positions,
            risk,
            benchmarks: Benchmarks::new(),
            target_notional,
            last_snapshot: now,
            universe,
            rings,
            trading_gate,
            warmup_gate,
            command_rx,
            #[cfg(feature = "test-mode")]
            price_event_notifier,
        }
    }

    fn run(&mut self) {
        loop {
            self.drain_commands();

            let mut made_progress = false;
            for idx in 0..self.shards.len() {
                match self.shards[idx].receiver.try_recv() {
                    Ok(event) => {
                        if !self.trading_gate.is_enabled() {
                            continue;
                        }
                        let allow_trading = self.warmup_gate.is_warm();
                        #[cfg(not(feature = "test-mode"))]
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

    fn handle_command(&mut self, cmd: ProcessorCommand) {
        match cmd {
            ProcessorCommand::ResetDaily { ack } => {
                self.reset_daily_components();
                let _ = ack.send(());
            }
            #[cfg(feature = "test-mode")]
            ProcessorCommand::ForceModeLiveForTests { value, ack } => {
                self.mode.force_live_for_tests(value);
                if value {
                    self.trading_gate.enable();
                }
                let _ = ack.send(());
            }
            #[cfg(feature = "test-mode")]
            ProcessorCommand::Snapshot { tx } => {
                let _ = tx.send(self.build_snapshot());
            }
        }
    }

    fn reset_daily_components(&mut self) {
        self.capital.reset_daily();
        self.risk.reset_daily_counters_keep_long_bans();
        self.positions.reset_daily_view();
        self.symbols.clear();
        self.benchmarks = Benchmarks::new();
        let _ = self.log_tx.send(LogMessage::ResetDaily);
    }

    #[cfg(feature = "test-mode")]
    fn build_snapshot(&self) -> ProcessorSnapshot {
        ProcessorSnapshot {
            capital: self.capital.snapshot(),
            positions: self.positions.snapshot(),
            risk: self.risk.snapshot(),
        }
    }

    fn handle_event(&mut self, event: PriceEvent, allow_trading: bool) {
        let now = self.clock.now_instant();
        #[cfg(feature = "test-mode")]
        let _event_ack = self
            .price_event_notifier
            .as_ref()
            .map(|tx| EventAckGuard::new(tx.clone()));
        if now.duration_since(event.received_instant) > self.queue_age {
            let _ = self.log_tx.send(
                MetricEvent::QueueDropMarket {
                    symbol: event.symbol,
                }
                .into(),
            );
            return;
        }

        #[cfg(feature = "test-mode")]
        let event_time = event.received_instant;
        #[cfg(not(feature = "test-mode"))]
        let event_time = now;

        self.benchmarks.record(event.symbol, now, event.price);

        let (ret_60s, symbol_warm) = {
            let state = self
                .symbols
                .entry(event.symbol)
                .or_insert_with(|| SymbolState::new(self.config.strategy.window_ret_60s));
            state.rings.record(event_time, event.price);
            (state.rings.ret_60s(), state.rings.warm())
        };

        if !allow_trading {
            return;
        }

        if self.positions.contains(event.symbol) {
            if let Some(reason) = self.positions.evaluate_price(event.symbol, event.price) {
                self.handle_exit(event.symbol, event.price, reason);
            }
        }

        let Some(ret_60s) = ret_60s else {
            return;
        };

        let ahi_value = self.ahi.update(now, event.symbol, ret_60s);
        let ahi_avg = self
            .ahi
            .average_over(now, self.config.strategy.ahi.enter_window);
        let ahi_drop = self
            .ahi
            .drop_within(now, self.config.strategy.ahi.drop_window);
        let btc_ret_abs = self.benchmarks.btc_ret_15m_abs();

        self.mode
            .update(now, ahi_value, ahi_avg, ahi_drop, btc_ret_abs);

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
        if !self.risk.can_trade(event.symbol, now_ksa) {
            return;
        }

        let Some(slot) = self.capital.try_reserve_slot(now, event.symbol) else {
            return;
        };

        let quantity_estimate = self.estimate_quantity(event.price);
        let trigger = TriggerEvent {
            symbol: event.symbol,
            price_now: event.price,
            ret_60s,
            target_notional: self.target_notional,
            trigger_ts_mono_ns: self.clock.instant_to_ns(now),
            signal_ts_mono_ns: event.ts_mono_ns,
        };

        match self.trigger_tx.try_send(trigger.clone()) {
            Ok(()) => {
                self.positions
                    .open(event.symbol, event.price, quantity_estimate, slot, now);
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

    fn estimate_quantity(&self, price: f64) -> f64 {
        if price <= 0.0 {
            return 0.0;
        }
        self.target_notional
            .checked_div(Decimal::from_f64(price).unwrap_or_default())
            .and_then(|d| d.to_f64())
            .unwrap_or(0.0)
    }

    fn handle_exit(&mut self, symbol: Symbol, price: f64, reason: ExitReason) {
        let now_ksa = self.config.ksa_now(self.clock.as_ref());
        if let Some(position) = self.positions.close(symbol) {
            self.capital.release_slot(position.slot);
            let pnl = price / position.entry_price - 1.0;
            let hold_secs = position.opened_at.elapsed().as_secs();
            let qty = position.qty;
            let entry_symbol = position.symbol;
            if pnl < 0.0 {
                self.mode.mark_loss();
            }
            if let Err(err) = self.risk.mark_trade_close(entry_symbol, pnl, now_ksa) {
                let _ = self.log_tx.send(LogMessage::Error(format!(
                    "risk_mark_close_failed {}: {err:?}",
                    entry_symbol
                )));
            }
            let label = match reason {
                ExitReason::StopLoss => "[SELL] STOP",
                ExitReason::ProfitProtect => "[SELL] PROTECT",
            };
            let _ = self.log_tx.send(LogMessage::Info(
                format!(
                    "{label} {} price={:.6} qty={:.4} hold_s={} pnl={:.2}%",
                    entry_symbol,
                    price,
                    qty,
                    hold_secs,
                    pnl * 100.0
                )
                .into(),
            ));
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
}

enum ProcessorCommand {
    ResetDaily {
        ack: Sender<()>,
    },
    #[cfg(feature = "test-mode")]
    ForceModeLiveForTests {
        value: bool,
        ack: Sender<()>,
    },
    #[cfg(feature = "test-mode")]
    Snapshot {
        tx: Sender<ProcessorSnapshot>,
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
}

#[cfg(feature = "test-mode")]
#[derive(Clone, Debug)]
pub struct ProcessorSnapshot {
    pub capital: CapitalSnapshot,
    pub positions: PositionsSnapshot,
    pub risk: RiskSnapshot,
}
