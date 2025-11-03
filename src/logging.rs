use std::collections::VecDeque;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError};
use tracing::{error, info, warn};

use crate::config::{Config, ServerSpec};
use crate::types::{LogMessage, MetricEvent, Symbol, TickToSendMetric};

const MAX_TICK_SAMPLES: usize = 2_048;

pub fn spawn_log_aggregator(
    config: &'static Config,
    rx: Receiver<LogMessage>,
) -> thread::JoinHandle<()> {
    let server_spec = config.active_server_spec();

    thread::Builder::new()
        .name("log-aggregator".to_string())
        .spawn(move || {
            let mut aggregator = LogAggregator::new(config, server_spec, rx);
            aggregator.run();
        })
        .expect("failed to spawn log aggregator")
}

struct LogAggregator {
    config: &'static Config,
    server: &'static ServerSpec,
    rx: Receiver<LogMessage>,
    stats: Stats,
    last_flush: Instant,
}

impl LogAggregator {
    fn new(config: &'static Config, server: &'static ServerSpec, rx: Receiver<LogMessage>) -> Self {
        info!(
            target: "bot",
            "log aggregator ready: active_server={:?} log_flush={}ms",
            server.id,
            config.logging.flush_interval.as_millis()
        );

        Self {
            config,
            server,
            rx,
            stats: Stats::default(),
            last_flush: Instant::now(),
        }
    }

    fn run(&mut self) {
        loop {
            match self.rx.recv_timeout(Duration::from_millis(10)) {
                Ok(message) => self.handle(message),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }

            if self.last_flush.elapsed() >= self.config.logging.flush_interval {
                self.flush_summary();
                self.last_flush = Instant::now();
            }
        }
    }

    fn handle(&mut self, message: LogMessage) {
        match message {
            LogMessage::Metric(event) => self.stats.apply(event),
            LogMessage::Info(text) => info!(target: "bot", "{}", text),
            LogMessage::Warn(text) => warn!(target: "bot", "{}", text),
            LogMessage::Error(err) => error!(target: "bot", "{}", err),
            LogMessage::ResetDaily => self.stats.reset_daily(),
        }
    }

    fn flush_summary(&mut self) {
        let (price_p50, price_p95) = self.stats.price_latency_percentiles();
        let (trigger_p50, trigger_p95) = self.stats.trigger_latency_percentiles();

        info!(
            target: "bot",
            "stats server={:?} ws_in={} ws_text_in={} price_in={} decode_err={} triggers={} late={} queue_drop_market={} queue_drop_trigger={} seq_anomaly={} buy_ok={} buy_err={} suppressed={} price_to_send_ms_p50={:.2} p95={:.2} trigger_to_send_ms_p50={:.2} p95={:.2}{}",
            self.server.id,
            self.stats.ws_in,
            self.stats.ws_text_in,
            self.stats.price_in,
            self.stats.decode_err,
            self.stats.trigger_emitted,
            self.stats.trigger_drop_late,
            self.stats.queue_drop_market,
            self.stats.queue_drop_trigger,
            self.stats.seq_anomaly,
            self.stats.buy_ok,
            self.stats.buy_err,
            self.stats.signal_suppressed,
            price_p50,
            price_p95,
            trigger_p50,
            trigger_p95,
            self.stats.format_tail_notes(),
        );
    }
}

#[derive(Default)]
struct Stats {
    ws_in: u64,
    ws_text_in: u64,
    price_in: u64,
    decode_err: u64,
    queue_drop_market: u64,
    queue_drop_trigger: u64,
    seq_anomaly: u64,
    trigger_emitted: u64,
    trigger_drop_late: u64,
    buy_ok: u64,
    buy_err: u64,
    signal_suppressed: u64,
    price_to_send_ms: VecDeque<f64>,
    trigger_to_send_ms: VecDeque<f64>,
    last_queue_drop_market: Option<Symbol>,
    last_queue_drop_trigger: Option<Symbol>,
    last_seq_anomaly: Option<(Symbol, u64, u64)>,
    last_trigger_drop: Option<(Symbol, f64)>,
    last_buy_err: Option<Symbol>,
    last_tick_symbol: Option<Symbol>,
    last_trigger_emitted: Option<Symbol>,
    last_buy_ok: Option<Symbol>,
    last_signal_suppressed: Option<Symbol>,
}

impl Stats {
    fn reset_daily(&mut self) {
        self.ws_in = 0;
        self.ws_text_in = 0;
        self.price_in = 0;
        self.decode_err = 0;
        self.queue_drop_market = 0;
        self.queue_drop_trigger = 0;
        self.seq_anomaly = 0;
        self.trigger_emitted = 0;
        self.trigger_drop_late = 0;
        self.buy_ok = 0;
        self.buy_err = 0;
        self.signal_suppressed = 0;
        self.price_to_send_ms.clear();
        self.trigger_to_send_ms.clear();
        self.last_queue_drop_market = None;
        self.last_queue_drop_trigger = None;
        self.last_seq_anomaly = None;
        self.last_trigger_drop = None;
        self.last_buy_err = None;
        self.last_tick_symbol = None;
        self.last_trigger_emitted = None;
        self.last_buy_ok = None;
        self.last_signal_suppressed = None;
    }

    fn apply(&mut self, event: MetricEvent) {
        match event {
            MetricEvent::WsMsgIn => self.ws_in += 1,
            MetricEvent::WsTextIn => self.ws_text_in += 1,
            MetricEvent::PriceEventIn => self.price_in += 1,
            MetricEvent::DecodeErr => self.decode_err += 1,
            MetricEvent::QueueDropMarket { symbol } => {
                self.queue_drop_market += 1;
                self.last_queue_drop_market = Some(symbol);
            }
            MetricEvent::QueueDropTrigger { symbol } => {
                self.queue_drop_trigger += 1;
                self.last_queue_drop_trigger = Some(symbol);
            }
            MetricEvent::SeqAnomaly {
                symbol,
                last_seq,
                observed_seq,
            } => {
                self.seq_anomaly += 1;
                self.last_seq_anomaly = Some((symbol, last_seq, observed_seq));
            }
            MetricEvent::TriggerEmitted { symbol } => {
                self.trigger_emitted += 1;
                self.last_trigger_emitted = Some(symbol);
            }
            MetricEvent::TriggerDropLate { symbol, latency_ns } => {
                self.trigger_drop_late += 1;
                self.last_trigger_drop = Some((symbol, latency_ns as f64 / 1_000_000.0));
            }
            MetricEvent::BuyOk { symbol } => {
                self.buy_ok += 1;
                self.last_buy_ok = Some(symbol);
            }
            MetricEvent::BuyErr { symbol } => {
                self.buy_err += 1;
                self.last_buy_err = Some(symbol);
            }
            MetricEvent::TickToSend(metric) => self.record_tick(metric),
            MetricEvent::SignalSuppressed { symbol } => {
                self.signal_suppressed += 1;
                self.last_signal_suppressed = Some(symbol);
            }
        }
    }

    fn record_tick(&mut self, metric: TickToSendMetric) {
        let price_ms = metric.price_to_send_ns as f64 / 1_000_000.0;
        let trigger_ms = metric.trigger_to_send_ns as f64 / 1_000_000.0;

        if self.price_to_send_ms.len() == MAX_TICK_SAMPLES {
            self.price_to_send_ms.pop_front();
        }
        if self.trigger_to_send_ms.len() == MAX_TICK_SAMPLES {
            self.trigger_to_send_ms.pop_front();
        }

        self.price_to_send_ms.push_back(price_ms);
        self.trigger_to_send_ms.push_back(trigger_ms);
        self.last_tick_symbol = Some(metric.symbol);
    }

    fn price_latency_percentiles(&self) -> (f64, f64) {
        (
            percentile(&self.price_to_send_ms, 0.50),
            percentile(&self.price_to_send_ms, 0.95),
        )
    }

    fn trigger_latency_percentiles(&self) -> (f64, f64) {
        (
            percentile(&self.trigger_to_send_ms, 0.50),
            percentile(&self.trigger_to_send_ms, 0.95),
        )
    }

    fn format_tail_notes(&self) -> String {
        let mut notes = Vec::new();
        if let Some(symbol) = self.last_queue_drop_market {
            notes.push(format!("last_market_drop={}", symbol));
        }
        if let Some(symbol) = self.last_queue_drop_trigger {
            notes.push(format!("last_trigger_drop_queue={}", symbol));
        }
        if let Some((symbol, last, observed)) = self.last_seq_anomaly {
            notes.push(format!(
                "last_seq_anomaly={} prev={} got={}",
                symbol, last, observed
            ));
        }
        if let Some((symbol, latency_ms)) = self.last_trigger_drop {
            notes.push(format!(
                "last_trigger_drop_late={} latency_ms={:.2}",
                symbol, latency_ms
            ));
        }
        if let Some(symbol) = self.last_buy_err {
            notes.push(format!("last_buy_err={}", symbol));
        }
        if let Some(symbol) = self.last_tick_symbol {
            notes.push(format!("last_tick_symbol={}", symbol));
        }
        if let Some(symbol) = self.last_trigger_emitted {
            notes.push(format!("last_trigger={}", symbol));
        }
        if let Some(symbol) = self.last_buy_ok {
            notes.push(format!("last_buy_ok={}", symbol));
        }
        if let Some(symbol) = self.last_signal_suppressed {
            notes.push(format!("last_signal_suppressed={}", symbol));
        }

        if notes.is_empty() {
            String::new()
        } else {
            format!(" {}", notes.join(" "))
        }
    }
}

fn percentile(samples: &VecDeque<f64>, quantile: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut values = samples.iter().copied().collect::<Vec<_>>();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((values.len() as f64 - 1.0) * quantile).round() as usize;
    values[idx]
}
