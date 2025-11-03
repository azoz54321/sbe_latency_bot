#![cfg(feature = "test-mode")]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, RecvTimeoutError};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;

use sbe_latency_bot::channels::{spsc_channel, SpscSender};
use sbe_latency_bot::clock::{Clock, TestClock};
use sbe_latency_bot::config::{Config, ShardAssignment};
use sbe_latency_bot::execution::{
    self, ExecGuards, ExecutionHandle, FakeExecutionProvider, FakeOrderBehavior, ProviderCall,
    ProviderEvent,
};
use sbe_latency_bot::filters::{FilterCache, SymbolFilters};
use sbe_latency_bot::gates::{TradingGate, WarmupGate};
use sbe_latency_bot::metrics::LatencyCountersSnapshot;
use sbe_latency_bot::processor::{self, ProcessorHandle, ProcessorSnapshot};
use sbe_latency_bot::reset;
use sbe_latency_bot::rings::{Rings, RingsHandle};
use sbe_latency_bot::types::{symbol_id, LogMessage, MetricEvent, PriceEvent, Symbol};
use sbe_latency_bot::universe::{SymbolMeta, Universe, UniverseHandle, UniverseShard};

const DEFAULT_SYMBOL: &str = "BTCUSDT";
const SIGNAL_WINDOW_SECS: u64 = 60;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsSummary {
    pub heal_attempts: u64,
    pub heal_success: u64,
    pub disables: u64,
}

#[allow(dead_code)]
pub struct TestHarness {
    config: &'static Config,
    pub clock: Arc<TestClock>,
    processor: ProcessorHandle,
    execution: ExecutionHandle,
    market_tx: SpscSender<PriceEvent>,
    log_rx: Receiver<LogMessage>,
    log_backlog: Mutex<Vec<LogMessage>>,
    provider: Arc<FakeExecutionProvider>,
    provider_events: Receiver<ProviderEvent>,
    price_event_acks: Receiver<()>,
    symbols: HashMap<String, Symbol>,
    prices: Mutex<HashMap<Symbol, f64>>,
    seq: AtomicU64,
    #[allow(dead_code)]
    processor_thread: thread::JoinHandle<()>,
    #[allow(dead_code)]
    execution_thread: thread::JoinHandle<()>,
}

#[allow(dead_code)]
impl TestHarness {
    pub fn new() -> Self {
        Self::with_symbols_and_warmup(&[DEFAULT_SYMBOL], 0)
    }

    pub fn new_with_warmup(warmup_secs: u64) -> Self {
        Self::with_symbols_and_warmup(&[DEFAULT_SYMBOL], warmup_secs)
    }

    pub fn with_symbols(symbols: &[&str]) -> Self {
        Self::with_symbols_and_warmup(symbols, 0)
    }

    pub fn with_symbols_and_warmup(symbols: &[&str], warmup_secs: u64) -> Self {
        let config = Config::load();
        let server = config.active_server_spec();

        let clock = Arc::new(TestClock::new());
        let clock_dyn: Arc<dyn Clock> = clock.clone();

        let symbol_list = if symbols.is_empty() {
            vec![DEFAULT_SYMBOL.to_string()]
        } else {
            symbols.iter().map(|s| s.to_uppercase()).collect()
        };

        let (metas, symbol_map, leaked_slice) = build_symbol_metas(&symbol_list);
        let universe = build_universe(config, clock_dyn.as_ref(), metas.clone());
        let filter_seed = universe.filter_seed();
        let universe_handle = UniverseHandle::new(universe);
        let rings = {
            let latest = universe_handle.get();
            Rings::for_universe(&latest)
        };
        let rings_handle = RingsHandle::new(rings);

        let filter_cache = FilterCache::new(filter_seed, config.transport.rest_base_url)
            .expect("filter cache init");
        let exec_guards = ExecGuards::with_binance_defaults();

        let trading_gate = Arc::new(TradingGate::new());
        trading_gate.disable();
        let warmup_gate = Arc::new(WarmupGate::new(clock_dyn.clone()));
        warmup_gate.arm_for(Duration::from_secs(warmup_secs));

        let (log_tx, log_rx) = crossbeam_channel::unbounded();
        let (market_tx, market_rx) = spsc_channel::<PriceEvent>(config.channel.market_capacity);
        let (trigger_tx, trigger_rx) = spsc_channel(config.channel.trigger_capacity);
        let (price_event_tx, price_event_acks) = crossbeam_channel::unbounded();
        let (provider_events_tx, provider_events) = crossbeam_channel::unbounded();

        let shard_assignment = ShardAssignment {
            shard_index: 0,
            symbols: leaked_slice,
            cpu_core: 0,
        };
        let shard_inputs = vec![(shard_assignment, market_rx)];

        let (processor_thread, processor_handle) = processor::spawn_processor(
            config,
            server,
            clock_dyn.clone(),
            universe_handle.clone(),
            rings_handle.clone(),
            trading_gate.clone(),
            warmup_gate.clone(),
            shard_inputs,
            trigger_tx,
            log_tx.clone(),
            Some(price_event_tx),
        );

        let (execution_thread, execution_handle) = execution::spawn_execution(
            config,
            server,
            clock_dyn.clone(),
            trigger_rx,
            log_tx,
            filter_cache,
            exec_guards,
            processor_handle.risk_handle(),
            Some(provider_events_tx),
        );

        processor_handle.arm_warmup(warmup_secs);
        processor_handle.enable_trading();

        let provider = execution_handle.fake_provider();

        Self {
            config,
            clock,
            processor: processor_handle,
            execution: execution_handle,
            market_tx,
            log_rx,
            log_backlog: Mutex::new(Vec::new()),
            provider,
            provider_events,
            price_event_acks,
            symbols: symbol_map,
            prices: Mutex::new(HashMap::new()),
            seq: AtomicU64::new(1),
            processor_thread,
            execution_thread,
        }
    }

    pub fn config(&self) -> &'static Config {
        self.config
    }

    pub fn advance_ms(&self, millis: u64) {
        self.clock.advance(Duration::from_millis(millis));
    }

    pub fn advance_secs(&self, secs: u64) {
        self.advance_ms(secs.saturating_mul(1_000));
    }

    pub fn arm_warmup_secs(&self, secs: u64) {
        self.processor.arm_warmup(secs);
    }

    pub fn enable_trading(&self) {
        self.processor.enable_trading();
    }

    pub fn disable_trading(&self) {
        self.processor.force_watch_only();
    }

    pub fn set_provider_behavior(&self, symbol: &str, behavior: FakeOrderBehavior) {
        let sym = self.lookup_symbol(symbol);
        self.provider.set_behavior(sym, behavior);
    }

    pub fn clear_provider_calls(&self) {
        self.provider.clear_calls();
        while self.provider_events.try_recv().is_ok() {}
    }

    pub fn provider_calls(&self, symbol: &str) -> Vec<ProviderCall> {
        let sym = self.lookup_symbol(symbol);
        self.provider
            .snapshot_calls()
            .into_iter()
            .filter(|call| call.symbol == sym)
            .collect()
    }

    pub fn provider_call_sides(&self, symbol: &str) -> Vec<&'static str> {
        self.provider_calls(symbol)
            .into_iter()
            .map(|call| call.side)
            .collect()
    }

    pub fn provider_overlap_detected(&self) -> bool {
        self.provider.overlap_detected()
    }

    pub fn wait_provider_calls_at_least(
        &self,
        symbol: &str,
        expected: usize,
        timeout: Duration,
    ) -> usize {
        let deadline = Instant::now() + timeout;
        let receiver = self.provider_events.clone();
        while Instant::now() < deadline {
            if self.provider_calls(symbol).len() >= expected {
                break;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match receiver.recv_timeout(remaining) {
                Ok(_) => {}
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        self.provider_calls(symbol).len()
    }

    pub fn provider_call_counts(&self, symbol: &str) -> (usize, usize) {
        let calls = self.provider_calls(symbol);
        let buys = calls.iter().filter(|call| call.side == "BUY").count();
        let limits = calls
            .iter()
            .filter(|call| call.order_type == "LIMIT")
            .count();
        (buys, limits)
    }

    pub fn assert_provider_sequence(
        &self,
        symbol: &str,
        expected: &[(&'static str, &'static str)],
    ) {
        let calls = self.provider_calls(symbol);
        let actual: Vec<(&str, &str)> = calls
            .iter()
            .map(|call| (call.side, call.order_type))
            .collect();
        assert_eq!(
            actual.len(),
            expected.len(),
            "unexpected provider call count for {}: {:?}",
            symbol,
            actual
        );
        for (idx, ((side, order_type), (exp_side, exp_type))) in
            actual.iter().zip(expected.iter()).enumerate()
        {
            assert_eq!(
                (side, order_type),
                (exp_side, exp_type),
                "provider call mismatch at index {} for {}",
                idx,
                symbol
            );
        }
    }

    pub fn send_price(&self, symbol: &str, price: f64) {
        let sym = self.lookup_symbol(symbol);
        let now = self.clock.now_instant();
        self.set_last_price(sym, price);
        self.enqueue_price_event_at(sym, now, price);
    }

    pub fn send_price_at(&self, symbol: &str, at: Instant, price: f64) {
        let sym = self.lookup_symbol(symbol);
        self.enqueue_price_event_at(sym, at, price);
    }

    pub fn emit_ret_without_advancing(&self, symbol: &str, pct: f64) {
        self.emit_ret_with_window(symbol, pct, Duration::from_secs(SIGNAL_WINDOW_SECS));
    }

    pub fn emit_ret_with_window_ms(&self, symbol: &str, pct: f64, window_ms: u64) {
        self.emit_ret_with_window(symbol, pct, Duration::from_millis(window_ms));
    }

    pub fn emit_benchmark_returns(&self, btc_ret: f64, eth_ret: f64) {
        let fifteen_minutes = Duration::from_secs(15 * 60);
        self.emit_ret_with_window("BTCUSDT", btc_ret, fifteen_minutes);
        self.emit_ret_with_window("ETHBTC", eth_ret, fifteen_minutes);
    }

    fn emit_ret_with_window(&self, symbol: &str, pct: f64, window: Duration) {
        let sym = self.lookup_symbol(symbol);
        let now_instant = self.clock.now_instant();
        let old_price = self.last_price(sym);
        let new_price = old_price * (1.0 + pct);
        let current_elapsed = Duration::from_nanos(self.clock.instant_to_ns(now_instant));
        let past_elapsed = current_elapsed.saturating_sub(window);

        self.clock.set_elapsed(past_elapsed);
        let past_instant = self.clock.now_instant();
        self.enqueue_price_event_at(sym, past_instant, old_price);
        self.spin();
        let _ = self.wait_price_event(Duration::from_millis(10));

        self.clock.set_elapsed(current_elapsed);
        self.enqueue_price_event_at(sym, now_instant, new_price);
        self.set_last_price(sym, new_price);
        self.spin();
        let _ = self.wait_price_event(Duration::from_millis(10));
    }

    fn wait_price_event(&self, timeout: Duration) -> bool {
        match self.price_event_acks.recv_timeout(timeout) {
            Ok(_) => true,
            Err(RecvTimeoutError::Timeout) => false,
            Err(RecvTimeoutError::Disconnected) => {
                panic!("price event notifier channel disconnected")
            }
        }
    }
    pub fn prewarm_symbol(&self, symbol: &str, price: f64) {
        self.send_price(symbol, price);
        self.spin();
        let _ = self.wait_price_event(Duration::from_millis(10));
        self.advance_secs(SIGNAL_WINDOW_SECS);
        self.send_price(symbol, price);
        self.spin();
        let _ = self.wait_price_event(Duration::from_millis(10));
    }

    pub fn emit_ret_signal(&self, symbol: &str, base_price: f64, ret: f64) {
        self.send_price(symbol, base_price);
        self.spin();
        self.advance_secs(SIGNAL_WINDOW_SECS);
        let new_price = base_price * (1.0 + ret);
        self.send_price(symbol, new_price);
        self.spin();
    }

    pub fn use_existing_price_and_emit_ret(&self, symbol: &str, ret: f64) {
        self.advance_secs(SIGNAL_WINDOW_SECS);
        let sym = self.lookup_symbol(symbol);
        let base = self.last_price(sym);
        let new_price = base * (1.0 + ret);
        self.send_price(symbol, new_price);
        self.spin();
    }

    pub fn drain_logs(&self) -> Vec<LogMessage> {
        self.take_logs()
    }

    pub fn drain_info_strings(&self) -> Vec<String> {
        self.take_logs()
            .into_iter()
            .filter_map(|msg| match msg {
                LogMessage::Info(text) => Some(text.into_owned()),
                LogMessage::Warn(text) => Some(text.into_owned()),
                LogMessage::Error(text) => Some(text),
                _ => None,
            })
            .collect()
    }

    pub fn collect_metric_events(&self) -> Vec<MetricEvent> {
        self.take_logs()
            .into_iter()
            .filter_map(|msg| match msg {
                LogMessage::Metric(event) => Some(event),
                _ => None,
            })
            .collect()
    }

    pub fn wait_for_metrics_summary(&self) -> Option<MetricsSummary> {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(2) {
            let logs = self.take_logs();
            if let Some(summary) = logs.iter().find_map(parse_metrics_summary) {
                self.push_back_logs(logs);
                return Some(summary);
            }
            if !logs.is_empty() {
                self.push_back_logs(logs);
            }
            thread::yield_now();
        }
        None
    }

    pub fn run_daily_reset(&self) -> Result<()> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("building tokio runtime for reset")?;
        rt.block_on(reset::perform_daily_reset_for_tests(
            self.config,
            self.clock.as_ref(),
            &self.processor,
            &self.execution,
        ))
    }

    pub fn provider(&self) -> Arc<FakeExecutionProvider> {
        self.provider.clone()
    }

    pub fn execution_handle(&self) -> &ExecutionHandle {
        &self.execution
    }

    pub fn processor_handle(&self) -> &ProcessorHandle {
        &self.processor
    }

    pub fn processor_snapshot(&self) -> ProcessorSnapshot {
        self.processor.snapshot()
    }

    pub fn metrics_snapshot(&self) -> LatencyCountersSnapshot {
        self.execution.metrics_snapshot()
    }

    pub fn scenario(&self) -> Scenario<'_> {
        Scenario::new(self)
    }

    pub fn install_default_filters(&self) {
        let mut snapshot = HashMap::new();
        for (name, symbol) in &self.symbols {
            snapshot.insert(symbol_id(*symbol), filters_for_symbol(name));
        }
        self.execution.install_filters(snapshot);
    }

    #[cfg(feature = "test-mode")]
    pub fn force_live_for_tests(&self, live: bool) {
        self.processor
            .force_live_for_tests(live)
            .expect("failed to force mode");
    }

    #[cfg(feature = "test-mode")]
    pub fn force_trading_enabled_for_tests(&self) {
        self.processor.force_trading_enabled_for_tests();
    }

    pub fn lookup_symbol(&self, symbol: &str) -> Symbol {
        let key = symbol.to_uppercase();
        *self
            .symbols
            .get(&key)
            .unwrap_or_else(|| panic!("symbol {symbol} not registered in harness"))
    }

    fn enqueue_price_event_at(&self, symbol: Symbol, when: Instant, price: f64) {
        let now = self.clock.now_instant();
        let now_mono = self.clock.instant_to_ns(now);
        let ts_mono = self.clock.instant_to_ns(when);
        let exch_now = self.clock.unix_now_ns();
        let exch_ts = if ts_mono <= now_mono {
            exch_now.saturating_sub(now_mono - ts_mono)
        } else {
            exch_now.saturating_add(ts_mono - now_mono)
        };
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);

        let event = PriceEvent {
            symbol,
            price,
            received_instant: when,
            ts_mono_ns: ts_mono,
            exch_ts_ns: exch_ts,
            seq,
        };

        self.market_tx.try_send(event).expect("market channel send");
    }

    fn record_price(&self, symbol: Symbol, price: f64) {
        self.prices
            .lock()
            .expect("price map poisoned")
            .insert(symbol, price);
    }

    fn set_last_price(&self, symbol: Symbol, price: f64) {
        self.record_price(symbol, price);
    }

    fn last_price(&self, symbol: Symbol) -> f64 {
        self.prices
            .lock()
            .expect("price map poisoned")
            .get(&symbol)
            .copied()
            .unwrap_or(100.0)
    }

    fn spin(&self) {
        for _ in 0..1_000 {
            thread::yield_now();
        }
    }

    fn take_logs(&self) -> Vec<LogMessage> {
        let mut backlog = self.log_backlog.lock().expect("log backlog mutex poisoned");
        let mut logs = backlog.drain(..).collect::<Vec<_>>();
        drop(backlog);
        while let Ok(msg) = self.log_rx.try_recv() {
            logs.push(msg);
        }
        logs
    }

    fn push_back_logs(&self, logs: Vec<LogMessage>) {
        if logs.is_empty() {
            return;
        }
        let mut backlog = self.log_backlog.lock().expect("log backlog mutex poisoned");
        backlog.extend(logs);
    }
}

#[allow(dead_code)]
pub struct Scenario<'a> {
    harness: &'a TestHarness,
    elapsed_ms: u64,
    warmup_unlock_ms: Option<u64>,
}

#[allow(dead_code)]
impl<'a> Scenario<'a> {
    pub fn new(harness: &'a TestHarness) -> Self {
        Self {
            harness,
            elapsed_ms: 0,
            warmup_unlock_ms: None,
        }
    }

    pub fn advance_ms(&mut self, millis: u64) -> &mut Self {
        self.elapsed_ms = self.elapsed_ms.saturating_add(millis);
        self.harness.advance_ms(millis);
        self.maybe_enable_trading();
        self
    }

    pub fn advance_secs(&mut self, secs: u64) -> &mut Self {
        self.advance_ms(secs.saturating_mul(1_000));
        self
    }

    pub fn emit_ret(&mut self, symbol: &str, pct: f64, window_ms: u64) -> &mut Self {
        self.harness.emit_ret_with_window_ms(symbol, pct, window_ms);
        self
    }

    pub fn emit_btc_eth(&mut self, btc_ret: f64, eth_ret: f64) -> &mut Self {
        self.harness.emit_benchmark_returns(btc_ret, eth_ret);
        self
    }

    pub fn drive_reset_now(&mut self) -> &mut Self {
        self.harness
            .run_daily_reset()
            .expect("daily reset should succeed");
        let warmup_secs = self.harness.config().strategy.warmup_secs;
        if warmup_secs == 0 {
            self.harness.enable_trading();
            self.warmup_unlock_ms = None;
        } else {
            self.warmup_unlock_ms = Some(
                self.elapsed_ms
                    .saturating_add(warmup_secs.saturating_mul(1_000)),
            );
        }
        self
    }

    pub fn wait_provider_calls(&self, symbol: &str, expected: usize, timeout_ms: u64) -> usize {
        self.harness.wait_provider_calls_at_least(
            symbol,
            expected,
            Duration::from_millis(timeout_ms),
        )
    }

    pub fn assert_log_seq(&mut self, expected: &[&str]) -> &mut Self {
        let mut attempts = 0;
        let mut collected: Vec<String> = Vec::new();
        loop {
            collected.extend(self.harness.drain_info_strings());
            if sequence_found(&collected, expected) {
                return self;
            }
            if attempts >= 20 {
                panic!(
                    "patterns {:?} not found in logs: {:?}",
                    expected, collected
                );
            }
            std::thread::sleep(Duration::from_millis(10));
            attempts += 1;
        }

        fn sequence_found(logs: &[String], expected: &[&str]) -> bool {
            let mut cursor = 0usize;
            for line in logs {
                if line.contains(expected[cursor]) {
                    cursor += 1;
                    if cursor == expected.len() {
                        return true;
                    }
                }
            }
            false
        }
    }

    pub fn processor_snapshot(&self) -> ProcessorSnapshot {
        self.harness.processor_snapshot()
    }

    pub fn metrics_snapshot(&self) -> LatencyCountersSnapshot {
        self.harness.metrics_snapshot()
    }

    fn maybe_enable_trading(&mut self) {
        if let Some(unlock) = self.warmup_unlock_ms {
            if self.elapsed_ms >= unlock {
                self.harness.enable_trading();
                self.warmup_unlock_ms = None;
            }
        }
    }
}

fn parse_metrics_summary(message: &LogMessage) -> Option<MetricsSummary> {
    let LogMessage::Info(text) = message else {
        return None;
    };
    let content = text.as_ref();
    if !content.starts_with("[METRICS]") {
        return None;
    }
    let mut heal_attempts = None;
    let mut heal_success = None;
    let mut disables = None;
    for token in content.split_whitespace() {
        if let Some(rest) = token.strip_prefix("heal_attempts=") {
            heal_attempts = rest.parse::<u64>().ok();
        } else if let Some(rest) = token.strip_prefix("heal_success=") {
            heal_success = rest.parse::<u64>().ok();
        } else if let Some(rest) = token.strip_prefix("disables=") {
            disables = rest.parse::<u64>().ok();
        }
    }
    Some(MetricsSummary {
        heal_attempts: heal_attempts.unwrap_or(0),
        heal_success: heal_success.unwrap_or(0),
        disables: disables.unwrap_or(0),
    })
}

fn build_symbol_metas(
    symbols: &[String],
) -> (
    Vec<SymbolMeta>,
    HashMap<String, Symbol>,
    &'static [&'static str],
) {
    let mut metas = Vec::with_capacity(symbols.len());
    let mut map = HashMap::with_capacity(symbols.len());
    let mut leaked_refs: Vec<&'static str> = Vec::with_capacity(symbols.len());

    for name in symbols {
        let upper = name.to_uppercase();
        let leaked = Box::leak(upper.clone().into_boxed_str());
        let symbol = Symbol::from_str(leaked).expect("invalid symbol in harness");
        map.insert(leaked.to_string(), symbol);
        let filters = filters_for_symbol(leaked);
        let (base, quote) = split_base_quote(leaked);
        metas.push(SymbolMeta {
            symbol,
            symbol_str: leaked.to_string(),
            base,
            quote,
            symbol_id: symbol_id(symbol),
            filters,
            onboard_date: None,
        });
        leaked_refs.push(leaked);
    }

    let leaked_slice: &'static [&'static str] = Box::leak(leaked_refs.into_boxed_slice());

    (metas, map, leaked_slice)
}

fn build_universe(config: &'static Config, clock: &dyn Clock, metas: Vec<SymbolMeta>) -> Universe {
    let generated_at = config.ksa_now(clock);
    let mut symbol_index = HashMap::with_capacity(metas.len());
    let mut members = Vec::with_capacity(metas.len());
    for (idx, meta) in metas.iter().enumerate() {
        symbol_index.insert(meta.symbol, idx);
        members.push(meta.symbol);
    }
    let shards = vec![UniverseShard { index: 0, members }];

    Universe {
        generated_at,
        symbols: metas,
        symbol_index,
        shards,
    }
}

fn filters_for_symbol(symbol: &str) -> SymbolFilters {
    let mut filters = SymbolFilters {
        step: Decimal::from_f64(0.001).expect("valid step"),
        tick: Decimal::from_f64(0.01).expect("valid tick"),
        min_notional: Decimal::from_f64(10.0).expect("valid min notional"),
    };

    if symbol.contains("SMALL") {
        filters.min_notional = Decimal::from_f64(100.0).expect("valid min notional override");
    }

    filters
}

fn split_base_quote(symbol: &str) -> (String, String) {
    if symbol.len() <= 4 {
        return (symbol.to_string(), "USDT".to_string());
    }
    let (base, quote) = symbol.split_at(symbol.len() - 4);
    (base.to_string(), quote.to_string())
}
