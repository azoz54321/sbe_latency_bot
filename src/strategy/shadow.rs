use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use chrono::Duration as ChronoDuration;
use crossbeam_channel::{Receiver, RecvTimeoutError};

use crate::clock::Clock;
use crate::config::Config;

const WINDOW_NS: u64 = 1_000_000_000;
const SPIKE_PCT: f64 = 0.005;
const TP_PCT: f64 = 0.2;
const SL_PCT: f64 = 0.01;
const REVERT_ARM_PCT: f64 = 0.01;
const PX_SCALE: f64 = 1e-8;
const METRICS_INTERVAL: Duration = Duration::from_secs(1);

pub const CHANNEL_CAPACITY: usize = 8_192;

#[derive(Clone, Copy, Debug)]
pub struct Tick {
    pub sym: u32,
    pub px_e8: i64,
    pub ts_ns: u64,
}

pub fn spawn_worker(
    config: &'static Config,
    clock: Arc<dyn Clock>,
    rx: Receiver<Tick>,
    drop_counter: Arc<AtomicU64>,
) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("shadow-strategy".into())
        .spawn(move || {
            let mut worker = ShadowWorker::new(config, clock, rx, drop_counter);
            worker.run();
        })
        .expect("failed to spawn shadow worker")
}

struct ShadowWorker {
    config: &'static Config,
    clock: Arc<dyn Clock>,
    rx: Receiver<Tick>,
    drop_counter: Arc<AtomicU64>,
    states: HashMap<u32, SymState>,
    banned: HashSet<u32>,
    metrics: Metrics,
    last_metrics: Instant,
    open_positions: usize,
    next_reset_deadline: Instant,
    syms_this_sec: HashSet<u32>,
    ticks_this_sec: u64,
}

impl ShadowWorker {
    fn new(
        config: &'static Config,
        clock: Arc<dyn Clock>,
        rx: Receiver<Tick>,
        drop_counter: Arc<AtomicU64>,
    ) -> Self {
        let next_reset_deadline = Self::compute_next_reset_deadline(clock.as_ref(), config);
        let worker = Self {
            config,
            clock,
            rx,
            drop_counter,
            states: HashMap::new(),
            banned: HashSet::new(),
            metrics: Metrics::default(),
            last_metrics: Instant::now(),
            open_positions: 0,
            next_reset_deadline,
            syms_this_sec: HashSet::with_capacity(256),
            ticks_this_sec: 0,
        };
        println!(
            "[SHADOW] cfg spike={:.5}% window_ms=1000 tp={:.2}% sl={:.2}%",
            SPIKE_PCT * 100.0,
            TP_PCT * 100.0,
            SL_PCT * 100.0
        );
        worker
    }

    fn run(&mut self) {
        loop {
            match self.rx.recv_timeout(Duration::from_millis(5)) {
                Ok(tick) => self.handle_tick(tick),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            self.maybe_emit_metrics();
            self.maybe_reset();
        }
    }

    fn handle_tick(&mut self, tick: Tick) {
        if tick.px_e8 <= 0 {
            return;
        }

        self.ticks_this_sec = self.ticks_this_sec.saturating_add(1);
        self.syms_this_sec.insert(tick.sym);

        let price = tick.px_e8 as f64 * PX_SCALE;
        let mut outcome: Option<CloseOutcome> = None;
        {
            let state_entry = self
                .states
                .entry(tick.sym)
                .or_insert_with(|| SymState::Flat(Window::default()));

            match state_entry {
                SymState::Flat(window) => {
                    window.push(tick.ts_ns, price);
                    if self.banned.contains(&tick.sym) {
                        return;
                    }
                    if let Some(min_px) = window.min_price() {
                        if min_px > 0.0 && price >= min_px * (1.0 + SPIKE_PCT) {
                            let jump_pct = ((price / min_px) - 1.0) * 100.0;
                            println!(
                                "[BUY-SIGNAL] symbol_id={} jump=+{:.2}% entry={:.8} t_ns={}",
                                tick.sym, jump_pct, price, tick.ts_ns
                            );

                            let position = Position::new(price, tick.ts_ns);
                            println!(
                                "[PAPER-OPEN] symbol_id={} entry={:.8} target={:.8} stop={:.8}",
                                tick.sym, position.entry_px, position.target, position.stop
                            );
                            self.metrics.new_signals += 1;
                            self.open_positions += 1;

                            let window_owned = std::mem::take(window);
                            *state_entry = SymState::Long {
                                window: window_owned,
                                position,
                            };
                        }
                    }
                }
                SymState::Long { window, position } => {
                    window.push(tick.ts_ns, price);
                    if !position.hit_plus5 && price >= position.entry_px * (1.0 + REVERT_ARM_PCT) {
                        position.hit_plus5 = true;
                    }

                    let mut close_reason = None;
                    if price >= position.target {
                        close_reason = Some((CloseReason::TakeProfit, position.entry_ts_ns));
                    } else if price <= position.stop {
                        close_reason = Some((CloseReason::StopLoss, position.entry_ts_ns));
                    } else if position.hit_plus5 && price <= position.entry_px {
                        close_reason = Some((CloseReason::Revert, position.entry_ts_ns));
                    }

                    if let Some((reason, entry_ts_ns)) = close_reason {
                        let window_owned = std::mem::take(window);
                        outcome = Some(CloseOutcome {
                            reason,
                            entry_ts_ns,
                            window: window_owned,
                        });
                    }
                }
            }
        }

        if let Some(mut close) = outcome {
            self.apply_close(tick.sym, tick.ts_ns, close.entry_ts_ns, close.reason);
            close.window.push(tick.ts_ns, price);
            self.states.insert(tick.sym, SymState::Flat(close.window));
        }
    }

    fn apply_close(&mut self, sym: u32, ts_ns: u64, entry_ts_ns: u64, reason: CloseReason) {
        let dur_ms = ts_ns.saturating_sub(entry_ts_ns) / 1_000_000;
        match reason {
            CloseReason::TakeProfit => {
                println!("[PAPER-TP] symbol_id={} pnl=+10.00% dur_ms={}", sym, dur_ms);
                self.metrics.closed_tp += 1;
            }
            CloseReason::StopLoss => {
                println!("[PAPER-SL] symbol_id={} pnl=-5.00% dur_ms={}", sym, dur_ms);
                self.metrics.closed_sl += 1;
            }
            CloseReason::Revert => {
                println!("[PAPER-REVERT] symbol_id={} dur_ms={}", sym, dur_ms);
                self.metrics.closed_rev += 1;
            }
        }

        self.banned.insert(sym);
        if self.open_positions > 0 {
            self.open_positions -= 1;
        }
    }

    fn maybe_emit_metrics(&mut self) {
        if self.last_metrics.elapsed() < METRICS_INTERVAL {
            return;
        }

        let dropped = self.drop_counter.swap(0, Ordering::Relaxed);
        println!(
            "[SHADOW] open_positions={} new_signals={} closed_tp={} closed_sl={} closed_rev={} dropped_ticks={} syms_sec={} ticks_sec={}",
            self.open_positions,
            self.metrics.new_signals,
            self.metrics.closed_tp,
            self.metrics.closed_sl,
            self.metrics.closed_rev,
            dropped,
            self.syms_this_sec.len(),
            self.ticks_this_sec
        );
        self.metrics.reset();
        self.last_metrics = Instant::now();
        self.syms_this_sec.clear();
        self.ticks_this_sec = 0;
    }

    fn maybe_reset(&mut self) {
        if Instant::now() < self.next_reset_deadline {
            return;
        }

        for state in self.states.values_mut() {
            if let SymState::Long { window, .. } = state {
                let window_owned = std::mem::take(window);
                *state = SymState::Flat(window_owned);
            }
        }
        self.open_positions = 0;
        self.banned.clear();
        println!("[RESET] 03:00 KSA — cleared shadow state");
        self.next_reset_deadline =
            Self::compute_next_reset_deadline(self.clock.as_ref(), self.config);
    }

    fn compute_next_reset_deadline(clock: &dyn Clock, config: &'static Config) -> Instant {
        let now_ksa = config.ksa_now(clock);
        let next_ksa = config.next_reset_time_ksa(now_ksa);
        let delta: ChronoDuration = next_ksa - now_ksa;
        let wait = delta.to_std().unwrap_or_else(|_| Duration::from_secs(0));
        clock.now_instant() + wait
    }
}

#[derive(Default)]
struct Metrics {
    new_signals: u64,
    closed_tp: u64,
    closed_sl: u64,
    closed_rev: u64,
}

impl Metrics {
    fn reset(&mut self) {
        self.new_signals = 0;
        self.closed_tp = 0;
        self.closed_sl = 0;
        self.closed_rev = 0;
    }
}

#[derive(Clone)]
struct Position {
    entry_px: f64,
    entry_ts_ns: u64,
    target: f64,
    stop: f64,
    hit_plus5: bool,
}

impl Position {
    fn new(entry_px: f64, entry_ts_ns: u64) -> Self {
        Self {
            entry_px,
            entry_ts_ns,
            target: entry_px * (1.0 + TP_PCT),
            stop: entry_px * (1.0 - SL_PCT),
            hit_plus5: false,
        }
    }
}

enum SymState {
    Flat(Window),
    Long { window: Window, position: Position },
}

#[derive(Default, Clone)]
struct Window {
    samples: VecDeque<(u64, f64)>,
}

impl Window {
    fn push(&mut self, ts_ns: u64, price: f64) {
        self.samples.push_back((ts_ns, price));
        self.prune(ts_ns);
    }

    fn prune(&mut self, ts_ns: u64) {
        let cutoff = ts_ns.saturating_sub(WINDOW_NS);
        while let Some(&(ts, _)) = self.samples.front() {
            if ts >= cutoff {
                break;
            }
            self.samples.pop_front();
        }
    }

    fn min_price(&self) -> Option<f64> {
        self.samples
            .iter()
            .map(|(_, price)| *price)
            .fold(None, |acc, price| match acc {
                Some(current) if current <= price => Some(current),
                _ => Some(price),
            })
    }
}

enum CloseReason {
    TakeProfit,
    StopLoss,
    Revert,
}

struct CloseOutcome {
    reason: CloseReason,
    entry_ts_ns: u64,
    window: Window,
}
