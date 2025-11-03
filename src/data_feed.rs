use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use byteorder::{ByteOrder, LittleEndian};
use crossbeam_channel::{Sender, TrySendError};
use futures_util::{SinkExt, StreamExt};
use http::header::{HeaderName, HeaderValue};
use tokio::runtime::Builder;
use tokio::time::{interval, sleep};
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};
use url::Url;

use crate::affinity;
use crate::channels::SpscSender;
use crate::config::{Config, ShardAssignment};
use crate::sbe::{DecodedMsg, SbeDecoder};
use crate::time_utils::{instant_to_ns, wall_clock_now_ns};
use crate::types::{LogMessage, MetricEvent, PriceEvent, Symbol};

const PING_INTERVAL_MS: u64 = 5_000;

static BTC_DEBUG_COUNT: AtomicUsize = AtomicUsize::new(0);
static LOGGED_SBE_HEADER: AtomicBool = AtomicBool::new(false);

pub fn spawn_shard_reader(
    config: &'static Config,
    assignment: ShardAssignment,
    market_tx: SpscSender<PriceEvent>,
    log_tx: Sender<LogMessage>,
) -> thread::JoinHandle<()> {
    let thread_name = format!("ws-shard-{}", assignment.shard_index);
    thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            affinity::bind_to_core(assignment.cpu_core);

            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime");

            runtime.block_on(async move {
                let mut runner = ShardRunner::new(config, assignment, market_tx, log_tx);
                runner.run().await;
            });
        })
        .expect("failed to spawn shard reader")
}

struct ShardRunner {
    config: &'static Config,
    assignment: ShardAssignment,
    market_tx: SpscSender<PriceEvent>,
    log_tx: Sender<LogMessage>,
    retry_backoff: Duration,
    seq_map: HashMap<Symbol, u64>,
    decoder: SbeDecoder,
    max_skew_ns: u64,
    stream_symbol: Symbol,
    stream_symbol_str: String,
    stream_channel: String,
}

impl ShardRunner {
    fn new(
        config: &'static Config,
        assignment: ShardAssignment,
        market_tx: SpscSender<PriceEvent>,
        log_tx: Sender<LogMessage>,
    ) -> Self {
        let stream_symbol_str = sanitize_stream_part("btcusdt");
        let stream_channel = sanitize_stream_part("trade");
        let stream_symbol =
            Symbol::from_str(&stream_symbol_str).expect("invalid sanitized stream symbol");

        Self {
            config,
            assignment,
            market_tx,
            log_tx,
            retry_backoff: Duration::from_millis(250),
            seq_map: HashMap::with_capacity(512),
            decoder: SbeDecoder,
            max_skew_ns: config.backpressure.max_queue_age.as_nanos() as u64,
            stream_symbol,
            stream_symbol_str,
            stream_channel,
        }
    }

    async fn run(&mut self) {
        loop {
            if let Err(err) = self.open_stream().await {
                self.log_tx
                    .send(LogMessage::Warn(
                        format!(
                            "shard {} stream error: {err:?}, retrying in {:?}",
                            self.assignment.shard_index, self.retry_backoff
                        )
                        .into(),
                    ))
                    .ok();
                sleep(self.retry_backoff).await;
                self.retry_backoff = (self.retry_backoff * 2).min(Duration::from_secs(5));
            } else {
                self.retry_backoff = Duration::from_millis(250);
            }
        }
    }

    async fn open_stream(&mut self) -> anyhow::Result<()> {
        let url = build_ws_url_single(&self.stream_symbol_str, &self.stream_channel);
        Url::parse(&url).expect("invalid WS URL");
        tracing::info!("connecting WS url={}", url);
        let mut request = url.clone().into_client_request()?;

        let header_name = HeaderName::from_static("x-mbx-apikey");
        let header_value =
            HeaderValue::from_str(self.config.credentials.sbe_ws_api_key).map_err(|err| {
                anyhow::anyhow!(
                    "invalid SBE API key header for shard {}: {err}",
                    self.assignment.shard_index
                )
            })?;
        request.headers_mut().insert(header_name, header_value);

        let (ws_stream, _) = tokio_tungstenite::connect_async(request).await?;
        self.log_tx
            .send(LogMessage::Info(
                format!(
                    "connected shard {} ({} symbols) -> {}",
                    self.assignment.shard_index,
                    self.assignment.symbols.len(),
                    url
                )
                .into(),
            ))
            .ok();

        let (mut ws_sink, mut ws_source) = ws_stream.split();
        let mut ping_timer = interval(Duration::from_millis(PING_INTERVAL_MS));

        loop {
            tokio::select! {
                _ = ping_timer.tick() => {
                    if let Err(err) = ws_sink.send(Message::Ping(Vec::new())).await {
                        return Err(err.into());
                    }
                }
                message = ws_source.next() => {
                    let Some(message) = message else {
                        break;
                    };
                    match message {
                        Ok(Message::Binary(payload)) => self.handle_payload(&payload),
                        Ok(Message::Text(_)) => {
                            self.log_tx
                                .send(MetricEvent::WsTextIn.into())
                                .ok();
                        }
                        Ok(Message::Pong(_)) => {}
                        Ok(Message::Ping(data)) => {
                            ws_sink.send(Message::Pong(data)).await?;
                        }
                        Ok(Message::Frame(_)) => {}
                        Ok(Message::Close(_)) => break,
                        Err(err) => return Err(err.into()),
                    }
                }
            }
        }

        Ok(())
    }

    fn handle_payload(&mut self, payload: &[u8]) {
        self.log_tx.send(MetricEvent::WsMsgIn.into()).ok();
        log_sbe_header_once(payload);

        match self.decoder.decode_frame(payload) {
            Some(DecodedMsg::Trade {
                symbol,
                price,
                qty,
                exch_ts_ns,
                seq,
            }) => {
                self.log_tx.send(MetricEvent::PriceEventIn.into()).ok();
                let symbol = symbol.unwrap_or(self.stream_symbol);
                self.forward_trade(symbol, price, qty, exch_ts_ns, seq);
            }
            _ => {
                self.log_tx.send(MetricEvent::DecodeErr.into()).ok();
            }
        }
    }

    fn forward_trade(&mut self, symbol: Symbol, price: f64, qty: f64, exch_ts_ns: u64, seq: u64) {
        let wall_ns = wall_clock_now_ns();
        if wall_ns > 0 && wall_ns.saturating_sub(exch_ts_ns) > self.max_skew_ns {
            return;
        }

        if seq > 0 {
            if let Some(prev) = self.seq_map.get(&symbol).copied() {
                if seq <= prev || seq > prev + 1 {
                    self.log_tx
                        .send(
                            MetricEvent::SeqAnomaly {
                                symbol,
                                last_seq: prev,
                                observed_seq: seq,
                            }
                            .into(),
                        )
                        .ok();
                }
            }
            self.seq_map.insert(symbol, seq);
        }

        let now = Instant::now();

        if symbol.as_bytes() == b"BTCUSDT" {
            let mut current = BTC_DEBUG_COUNT.load(Ordering::Relaxed);
            let mut should_log = false;
            while current < 3 {
                match BTC_DEBUG_COUNT.compare_exchange(
                    current,
                    current + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        should_log = true;
                        break;
                    }
                    Err(value) => current = value,
                }
            }

            if should_log {
                tracing::debug!(
                    target: "bot",
                    "first_trade btcusdt price={:.8} qty={:.8} exch_ts_ns={}",
                    price,
                    qty,
                    exch_ts_ns
                );
            }
        }

        let event = PriceEvent {
            symbol,
            price,
            received_instant: now,
            ts_mono_ns: instant_to_ns(now),
            exch_ts_ns,
            seq,
        };

        match self.market_tx.try_send(event) {
            Ok(()) => {}
            Err(TrySendError::Full(event)) => {
                self.log_tx
                    .send(
                        MetricEvent::QueueDropMarket {
                            symbol: event.symbol,
                        }
                        .into(),
                    )
                    .ok();
            }
            Err(TrySendError::Disconnected(_)) => {
                self.log_tx
                    .send(LogMessage::Error(format!(
                        "market channel disconnected for shard {}",
                        self.assignment.shard_index
                    )))
                    .ok();
            }
        }
    }
}
fn sanitize_stream_part(s: &str) -> String {
    let trimmed = s.trim_matches(|c: char| c == ' ' || c == '\r' || c == '\n' || c == '\t');
    trimmed.to_ascii_lowercase()
}

fn build_ws_url_single(symbol: &str, channel: &str) -> String {
    let sym = sanitize_stream_part(symbol);
    let chan = sanitize_stream_part(channel);

    assert!(sym
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
    assert!(chan.chars().all(|c| c.is_ascii_lowercase()));

    format!("wss://stream-sbe.binance.com/ws/{}@{}", sym, chan)
}

fn log_sbe_header_once(buf: &[u8]) {
    if LOGGED_SBE_HEADER.swap(true, Ordering::Relaxed) {
        return;
    }
    if buf.len() >= 8 {
        let block_len = LittleEndian::read_u16(&buf[0..2]);
        let template_id = LittleEndian::read_u16(&buf[2..4]);
        let schema_id = LittleEndian::read_u16(&buf[4..6]);
        let version = LittleEndian::read_u16(&buf[6..8]);
        tracing::info!(
            "SBE header: blockLen={} templateId={} schemaId={} version={}",
            block_len,
            template_id,
            schema_id,
            version
        );
    } else {
        tracing::warn!("SBE header: frame too short (len={})", buf.len());
    }
}
