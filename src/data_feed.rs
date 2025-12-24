use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use byteorder::{ByteOrder, LittleEndian};
use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use dashmap::DashMap;
use futures_util::{Sink, SinkExt, StreamExt};
use http::header::{HeaderName, HeaderValue};
use serde_json::json;
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::time::{interval, sleep};
use tokio_tungstenite::{
    client_async_tls_with_config,
    tungstenite::{client::IntoClientRequest, Message},
};
use url::Url;

mod schema_guard;
pub use schema_guard::{SchemaGuard, SchemaGuardHandle};

use crate::affinity;
use crate::channels::SpscSender;
use crate::config::{Config, ExecutionMode, ShardAssignment};
use crate::decoder_sbe::{DecodeStatus, SbeDecoder, MAX_TRADES_PER_FRAME};
use crate::ffi::BsbeTrade;
use crate::hot_counters::HotCounters;
use crate::strategy::shadow::Tick as ShadowTick;
use crate::time_utils::{instant_to_ns, wall_clock_now_ns};
use crate::types::{LogMessage, MetricEvent, PriceEvent, ReconnectNotice, Symbol};
use crate::universe::SymbolKey;

pub type IngestTx = Sender<PriceEvent>;
pub type IngestRx = Receiver<PriceEvent>;

pub const INGEST_CHANNEL_CAPACITY: usize = 65_536;
const PING_INTERVAL_MS: u64 = 5_000;
const MAX_CLIENT_MSGS_PER_SEC: usize = 5;
const MAX_PENDING_BYTES: usize = 1_048_576;

static BTC_DEBUG_COUNT: AtomicUsize = AtomicUsize::new(0);
static LOGGED_SBE_HEADER: AtomicBool = AtomicBool::new(false);

pub struct ShardHandle {
    pub join: thread::JoinHandle<()>,
    pub ctx: Arc<ShardContext>,
}

pub fn spawn_shard_reader(
    config: &'static Config,
    assignment: ShardAssignment,
    market_tx: SpscSender<PriceEvent>,
    log_tx: Sender<LogMessage>,
    hot: Arc<HotCounters>,
    schema_guard: SchemaGuardHandle,
    reconnect_tx: Sender<ReconnectNotice>,
    shadow_tx: Sender<ShadowTick>,
    shadow_drop_counter: Arc<AtomicU64>,
    symbols: Arc<Vec<SymbolKey>>,
    runtime: Handle,
) -> ShardHandle {
    let shard_index = assignment.shard_index;
    let cpu_core = assignment.cpu_core;
    let (ing_tx, ing_rx) = bounded(INGEST_CHANNEL_CAPACITY);

    let log_tx_for_ctx = log_tx.clone();
    let log_tx_for_agg = log_tx.clone();
    let (control_tx, control_rx) = unbounded_channel();

    let aggregator = thread::Builder::new()
        .name(format!("ing-bridge-{shard_index}"))
        .spawn(move || {
            affinity::bind_to_core(cpu_core);
            shard_aggregator_loop(shard_index, ing_rx, market_tx, log_tx_for_agg);
        })
        .expect("failed to spawn shard aggregator");

    let ctx = ShardContext::new(
        config,
        shard_index,
        log_tx_for_ctx,
        hot,
        schema_guard,
        reconnect_tx,
        shadow_tx,
        shadow_drop_counter,
        ing_tx,
        config.backpressure.max_exch_skew.as_nanos() as u64,
        control_tx,
    );

    let ctx_for_stream = ctx.clone();
    let initial_symbols = symbols.as_ref().clone();
    runtime.spawn(async move {
        run_shard_stream(ctx_for_stream, control_rx, initial_symbols).await;
    });

    ShardHandle {
        join: aggregator,
        ctx,
    }
}

pub struct RefreshStats {
    pub added: usize,
    pub removed: usize,
    pub kept: usize,
}

pub struct ShardContext {
    config: &'static Config,
    shard_index: usize,
    log_tx: Sender<LogMessage>,
    hot: Arc<HotCounters>,
    schema_guard: SchemaGuardHandle,
    reconnect_tx: Sender<ReconnectNotice>,
    shadow_tx: Sender<ShadowTick>,
    shadow_drop_counter: Arc<AtomicU64>,
    ingest_tx: IngestTx,
    max_exch_skew_ns: u64,
    symbols: Arc<DashMap<Symbol, SymbolMeta>>,
    control_tx: UnboundedSender<ControlMessage>,
    control_id: AtomicU64,
}

#[derive(Clone)]
struct SymbolMeta {
    key: SymbolKey,
    symbol: Symbol,
    stream: String,
}

impl SymbolMeta {
    fn new(key: SymbolKey) -> Option<Self> {
        let symbol = Symbol::from_str(&key.name)?;
        let stream = format!("{}@trade", key.name.to_ascii_lowercase());
        Some(Self {
            key,
            symbol,
            stream,
        })
    }
}

#[derive(Debug)]
enum ControlMessage {
    ApplyDiff {
        subscribe: Vec<String>,
        unsubscribe: Vec<String>,
    },
}

struct MessageRateLimiter {
    recent: VecDeque<Instant>,
}

impl MessageRateLimiter {
    fn new() -> Self {
        Self {
            recent: VecDeque::with_capacity(MAX_CLIENT_MSGS_PER_SEC + 1),
        }
    }

    async fn acquire(&mut self) {
        let window = Duration::from_secs(1);
        let now = Instant::now();
        while let Some(ts) = self.recent.front().copied() {
            if now.duration_since(ts) >= window {
                self.recent.pop_front();
            } else {
                break;
            }
        }

        if let Some(ts) = self.recent.front().copied() {
            if self.recent.len() >= MAX_CLIENT_MSGS_PER_SEC {
                let wait = window.saturating_sub(now.duration_since(ts));
                if wait > Duration::from_millis(0) {
                    sleep(wait).await;
                }
            }
        }

        self.recent.push_back(Instant::now());
    }
}

impl ShardContext {
    fn new(
        config: &'static Config,
        shard_index: usize,
        log_tx: Sender<LogMessage>,
        hot: Arc<HotCounters>,
        schema_guard: SchemaGuardHandle,
        reconnect_tx: Sender<ReconnectNotice>,
        shadow_tx: Sender<ShadowTick>,
        shadow_drop_counter: Arc<AtomicU64>,
        ingest_tx: IngestTx,
        max_exch_skew_ns: u64,
        control_tx: UnboundedSender<ControlMessage>,
    ) -> Arc<Self> {
        Arc::new(Self {
            config,
            shard_index,
            log_tx,
            hot,
            schema_guard,
            reconnect_tx,
            shadow_tx,
            shadow_drop_counter,
            ingest_tx,
            max_exch_skew_ns,
            symbols: Arc::new(DashMap::new()),
            control_tx,
            control_id: AtomicU64::new(1),
        })
    }

    pub async fn snapshot_symbols(self: &Arc<Self>) -> Vec<SymbolKey> {
        self.symbols
            .iter()
            .map(|entry| entry.value().key.clone())
            .collect()
    }

    pub async fn symbol_count(self: &Arc<Self>) -> usize {
        self.symbols.len()
    }

    pub async fn apply_universe_diff(
        self: &Arc<Self>,
        desired: BTreeSet<SymbolKey>,
    ) -> RefreshStats {
        let mut desired_symbols = HashMap::new();
        let mut to_add = Vec::new();
        for key in desired.iter() {
            if let Some(symbol) = Symbol::from_str(&key.name) {
                desired_symbols.insert(symbol, key.clone());
                if !self.symbols.contains_key(&symbol) {
                    to_add.push((symbol, key.clone()));
                }
            }
        }
        let mut to_remove = Vec::new();
        for entry in self.symbols.iter() {
            let symbol = *entry.key();
            if !desired_symbols.contains_key(&symbol) {
                to_remove.push(symbol);
            }
        }
        let kept = desired_symbols.len().saturating_sub(to_add.len());

        let mut added = 0;
        let mut removed = 0;
        let mut subscribe = Vec::new();
        let mut unsubscribe = Vec::new();

        for symbol in to_remove.into_iter() {
            if let Some((_, meta)) = self.symbols.remove(&symbol) {
                removed += 1;
                unsubscribe.push(meta.stream.clone());
                let _ = self.log_tx.send(LogMessage::Info(
                    format!(
                        "[BOOT] remove symbol={} shard={}",
                        meta.key.name, self.shard_index
                    )
                    .into(),
                ));
            }
        }

        for (symbol, key) in to_add.into_iter() {
            if let Some(meta) = SymbolMeta::new(key.clone()) {
                subscribe.push(meta.stream.clone());
                self.symbols.insert(symbol, meta.clone());
                added += 1;
                let _ = self.log_tx.send(LogMessage::Info(
                    format!("[BOOT] add symbol={} shard={}", key.name, self.shard_index).into(),
                ));
            }
        }

        if !subscribe.is_empty() || !unsubscribe.is_empty() {
            let _ = self.control_tx.send(ControlMessage::ApplyDiff {
                subscribe,
                unsubscribe,
            });
        }

        RefreshStats {
            added,
            removed,
            kept,
        }
    }

    fn notify_reconnect(&self) {
        let notice = ReconnectNotice {
            shard_index: self.shard_index,
            ts_mono_ns: instant_to_ns(Instant::now()),
        };
        let _ = self.reconnect_tx.try_send(notice);
    }

    fn send_shadow_tick(&self, meta: &SymbolMeta, trade: &BsbeTrade) {
        if self.config.execution.mode != ExecutionMode::Shadow {
            return;
        }
        if trade.px_e8 == 0 {
            return;
        }
        let tick = ShadowTick {
            symbol: meta.symbol,
            px_e8: trade.px_e8,
            ts_ns: trade.event_ts_ns,
            rx_instant: Instant::now(),
        };
        match self.shadow_tx.try_send(tick) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.shadow_drop_counter.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Disconnected(_)) => {}
        }
    }

    fn emit_price_event(&self, event: PriceEvent) {
        match self.ingest_tx.try_send(event) {
            Ok(()) => {}
            Err(TrySendError::Full(dropped)) => {
                let _ = self.log_tx.send(
                    MetricEvent::QueueDropMarket {
                        symbol: dropped.symbol,
                    }
                    .into(),
                );
            }
            Err(TrySendError::Disconnected(_)) => {
                let _ = self.log_tx.send(LogMessage::Warn(
                    format!("shard {} ingest channel closed", self.shard_index).into(),
                ));
            }
        }
    }

    fn lookup_symbol(&self, symbol: Symbol) -> Option<SymbolMeta> {
        self.symbols.get(&symbol).map(|entry| entry.value().clone())
    }

    fn streams_snapshot(&self) -> Vec<String> {
        self.symbols
            .iter()
            .map(|entry| entry.value().stream.clone())
            .collect()
    }

    fn next_control_id(&self) -> u64 {
        self.control_id.fetch_add(1, Ordering::Relaxed)
    }
}

async fn run_shard_stream(
    ctx: Arc<ShardContext>,
    mut control_rx: UnboundedReceiver<ControlMessage>,
    initial_symbols: Vec<SymbolKey>,
) {
    let initial_set: BTreeSet<SymbolKey> = initial_symbols.into_iter().collect();
    let _ = ctx.apply_universe_diff(initial_set).await;

    let mut decoder = SbeDecoder::default();
    let mut last_seq: HashMap<Symbol, u64> = HashMap::new();
    let mut pending_controls: VecDeque<ControlMessage> = VecDeque::new();
    let mut pending: Vec<u8> = Vec::new();
    let mut trades: Vec<BsbeTrade> = Vec::with_capacity(MAX_TRADES_PER_FRAME);
    let mut logged_need_more = false;
    let mut last_status_log: Option<Instant> = None;

    loop {
        while let Ok(msg) = control_rx.try_recv() {
            pending_controls.push_back(msg);
        }

        let streams = ctx.streams_snapshot();
        if streams.is_empty() {
            sleep(Duration::from_millis(200)).await;
            continue;
        }

        let url = match build_stream_url(&streams) {
            Some(url) => url,
            None => {
                sleep(Duration::from_millis(200)).await;
                continue;
            }
        };

        let sample_stream = streams.get(0).cloned().unwrap_or_default();
        tracing::debug!(
            "[WS] connect shard={} url={} api_key_len={} sample_stream={}",
            ctx.shard_index,
            url,
            ctx.config.credentials.sbe_ws_api_key.len(),
            sample_stream
        );

        match connect_shard_stream(&ctx, &url).await {
            Ok(ws_stream) => {
                tracing::info!(
                    shard = ctx.shard_index,
                    stream_count = streams.len(),
                    %url,
                    "combined shard stream connected"
                );
                ctx.notify_reconnect();
                let (mut ws_sink, mut ws_source) = ws_stream.split();
                let mut ping_timer = interval(Duration::from_millis(PING_INTERVAL_MS));
                let mut rate_limiter = MessageRateLimiter::new();

                if let Err(err) = flush_control_queue(
                    &ctx,
                    &mut ws_sink,
                    &mut rate_limiter,
                    &mut pending_controls,
                )
                .await
                {
                    tracing::warn!(
                        shard = ctx.shard_index,
                        error = %err,
                        "failed to flush pending control messages"
                    );
                    continue;
                }

                loop {
                    tokio::select! {
                        _ = ping_timer.tick() => {
                            if let Err(err) = send_rate_limited(&mut ws_sink, &mut rate_limiter, Message::Ping(Vec::new())).await {
                                tracing::warn!(shard = ctx.shard_index, error = %err, "ping failed");
                                break;
                            }
                        }
                        Some(ctrl) = control_rx.recv() => {
                            pending_controls.push_back(ctrl);
                            if let Err(err) = flush_control_queue(&ctx, &mut ws_sink, &mut rate_limiter, &mut pending_controls).await {
                                tracing::warn!(shard = ctx.shard_index, error = %err, "control flush failed");
                                break;
                            }
                        }
                        message = ws_source.next() => {
                            let Some(message) = message else { break };
                            match message {
                                Ok(Message::Binary(payload)) => {
                                    if ctx.schema_guard.is_paused() {
                                        continue;
                                    }
                                    ctx.hot.ws_in.fetch_add(1, Ordering::Relaxed);
                                    log_sbe_header_once(&payload);
                                    pending.extend_from_slice(&payload);
                                    if pending.len() > MAX_PENDING_BYTES {
                                        ctx.hot
                                            .decode_corrupt
                                            .fetch_add(1, Ordering::Relaxed);
                                        pending.clear();
                                        continue;
                                    }

                                    trades.clear();
                                    let (report, consumed) = decoder.decode_stream_with_pending(
                                        &pending,
                                        |trade| trades.push(*trade),
                                    );

                                    let status = report.status;
                                    let now = Instant::now();
                                    let should_log_status = match last_status_log {
                                        None => true,
                                        Some(prev) => now.duration_since(prev) > Duration::from_secs(5),
                                    };
                                    let need_pending_for_need_more =
                                        matches!(status, DecodeStatus::Incomplete)
                                            && !logged_need_more;
                                    let pending_len_before =
                                        if status != DecodeStatus::Complete
                                            && (should_log_status || need_pending_for_need_more)
                                        {
                                            Some(pending.len())
                                        } else {
                                            None
                                        };

                                    if status != DecodeStatus::Complete && should_log_status {
                                        let head_before = hex_head(&pending);
                                        let header_before = parse_header(&pending);
                                        let pending_bytes =
                                            pending_len_before.unwrap_or_else(|| pending.len());
                                        last_status_log = Some(now);
                                        tracing::debug!(
                                            "[SBE] status={:?} shard={} pending_bytes={} consumed={} head={} header={:?}",
                                            status,
                                            ctx.shard_index,
                                            pending_bytes,
                                            consumed,
                                            head_before,
                                            header_before
                                        );
                                    }

                                    if consumed > 0 {
                                        pending.drain(0..consumed);
                                    }

                                    for trade in &trades {
                                        process_trade(&ctx, trade, &mut last_seq);
                                    }

                                    match status {
                                        DecodeStatus::Complete => {
                                            ctx.hot.decode_ok.fetch_add(1, Ordering::Relaxed);
                                            logged_need_more = false;
                                        }
                                        DecodeStatus::Incomplete => {
                                            ctx.hot
                                            .decode_need_more
                                            .fetch_add(1, Ordering::Relaxed);
                                        if !logged_need_more {
                                            let pending_bytes =
                                                pending_len_before.unwrap_or_else(|| pending.len());
                                            tracing::debug!(
                                                "[SBE] need_more shard={} pending_bytes={}",
                                                ctx.shard_index,
                                                pending_bytes
                                            );
                                            logged_need_more = true;
                                        }
                                    }
                                        DecodeStatus::OutputTruncated => {
                                            ctx.hot
                                                .decode_truncated
                                                .fetch_add(1, Ordering::Relaxed);
                                        }
                                        DecodeStatus::Corrupt => {
                                            ctx.hot
                                                .decode_corrupt
                                                .fetch_add(1, Ordering::Relaxed);
                                            pending.clear();
                                        }
                                        DecodeStatus::SchemaMismatch => {
                                            ctx.hot
                                                .decode_schema_mismatch
                                                .fetch_add(1, Ordering::Relaxed);
                                            ctx.schema_guard.record_mismatch();
                                            pending.clear();
                                        }
                                    }

                                    if decoder.take_schema_mismatch() {
                                        ctx.schema_guard.record_mismatch();
                                    }
                                }
                                Ok(Message::Text(_)) => {
                                    ctx.hot.ws_text_in.fetch_add(1, Ordering::Relaxed);
                                }
                                Ok(Message::Ping(data)) => {
                                    if let Err(err) = send_rate_limited(&mut ws_sink, &mut rate_limiter, Message::Pong(data)).await {
                                        tracing::warn!(shard = ctx.shard_index, error = %err, "pong failed");
                                        break;
                                    }
                                }
                                Ok(Message::Pong(_)) => {}
                                Ok(Message::Close(_)) => {
                                    break;
                                }
                                Ok(Message::Frame(_)) => {}
                                Err(err) => {
                                    tracing::warn!(
                                        shard = ctx.shard_index,
                                        error = %err,
                                        "stream error; reconnecting"
                                    );
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(
                    shard = ctx.shard_index,
                    error = %err,
                    "shard stream connect failed; backing off"
                );
                let jitter = fastrand::u64(..500);
                sleep(Duration::from_millis(1_500 + jitter)).await;
            }
        }
    }
}

async fn connect_shard_stream(
    ctx: &ShardContext,
    url: &str,
) -> anyhow::Result<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>>
{
    let parsed = Url::parse(url).expect("invalid WS URL");
    let mut request = parsed.clone().into_client_request()?;

    let header_name = HeaderName::from_static("x-mbx-apikey");
    let api_key = ctx.config.credentials.sbe_ws_api_key;
    if api_key.is_empty() {
        return Err(anyhow!(
            "missing SBE API key for shard {}; set BINANCE_SBE_API_KEY",
            ctx.shard_index
        ));
    }
    let header_value = HeaderValue::from_str(api_key).map_err(|err| {
        anyhow!(
            "invalid SBE API key header for shard {}: {err}",
            ctx.shard_index
        )
    })?;
    request.headers_mut().insert(header_name, header_value);

    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow!("invalid host in {}", url))?;
    let port = parsed.port_or_known_default().unwrap_or(443);
    let addr = format!("{host}:{port}");
    let stream = TcpStream::connect(addr.clone())
        .await
        .with_context(|| format!("tcp connect {}", addr))?;
    stream
        .set_nodelay(true)
        .context("failed to set TCP_NODELAY")?;
    let (ws_stream, response) = client_async_tls_with_config(request, stream, None, None).await?;
    if let Some(ext) = response.headers().get("Sec-WebSocket-Extensions") {
        let value = ext.to_str().unwrap_or_default().to_ascii_lowercase();
        if value.contains("permessage-deflate") {
            return Err(anyhow!(
                "compression negotiated (permessage-deflate); refusing connection"
            ));
        }
    }

    Ok(ws_stream)
}

async fn flush_control_queue<S>(
    ctx: &ShardContext,
    ws_sink: &mut S,
    rate_limiter: &mut MessageRateLimiter,
    pending: &mut VecDeque<ControlMessage>,
) -> anyhow::Result<()>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    while let Some(msg) = pending.front() {
        send_control_message(ctx, ws_sink, rate_limiter, msg).await?;
        pending.pop_front();
    }
    Ok(())
}

async fn send_control_message<S>(
    ctx: &ShardContext,
    ws_sink: &mut S,
    rate_limiter: &mut MessageRateLimiter,
    message: &ControlMessage,
) -> anyhow::Result<()>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    match message {
        ControlMessage::ApplyDiff {
            subscribe,
            unsubscribe,
        } => {
            if !unsubscribe.is_empty() {
                let payload = json!({
                    "method": "UNSUBSCRIBE",
                    "params": unsubscribe,
                    "id": ctx.next_control_id(),
                })
                .to_string();
                send_rate_limited(ws_sink, rate_limiter, Message::Text(payload)).await?;
            }
            if !subscribe.is_empty() {
                let payload = json!({
                    "method": "SUBSCRIBE",
                    "params": subscribe,
                    "id": ctx.next_control_id(),
                })
                .to_string();
                send_rate_limited(ws_sink, rate_limiter, Message::Text(payload)).await?;
            }
        }
    }
    Ok(())
}

async fn send_rate_limited<S>(
    ws_sink: &mut S,
    rate_limiter: &mut MessageRateLimiter,
    message: Message,
) -> anyhow::Result<()>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    rate_limiter.acquire().await;
    ws_sink.send(message).await?;
    Ok(())
}

fn build_stream_url(streams: &[String]) -> Option<String> {
    if streams.is_empty() {
        return None;
    }
    let path = streams.join("/");
    Some(format!(
        "wss://stream-sbe.binance.com/stream?streams={}",
        path
    ))
}

fn process_trade(ctx: &ShardContext, trade: &BsbeTrade, last_seq: &mut HashMap<Symbol, u64>) {
    if trade.px_e8 == 0 || trade.symbol_len == 0 {
        return;
    }

    let symbol = match Symbol::from_bytes(&trade.symbol_bytes[..trade.symbol_len as usize]) {
        Some(sym) => sym,
        None => return,
    };
    let Some(meta) = ctx.lookup_symbol(symbol) else {
        return;
    };

    let wall_ns = wall_clock_now_ns();
    if wall_ns > 0 && wall_ns.saturating_sub(trade.event_ts_ns) > ctx.max_exch_skew_ns {
        ctx.hot.drop_skew.fetch_add(1, Ordering::Relaxed);
        return;
    }

    let seq = trade.trade_id;
    if seq > 0 {
        let entry = last_seq.entry(meta.symbol).or_insert(0);
        if *entry != 0 && (seq <= *entry || seq > *entry + 1) {
            let _ = ctx.log_tx.send(
                MetricEvent::SeqAnomaly {
                    symbol: meta.symbol,
                    last_seq: *entry,
                    observed_seq: seq,
                }
                .into(),
            );
        }
        *entry = seq;
    }

    if meta.symbol.as_bytes() == b"BTCUSDT" {
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
                e8_to_float(trade.px_e8),
                e8_to_float(trade.qty_e8),
                trade.event_ts_ns
            );
        }
    }

    ctx.send_shadow_tick(&meta, trade);

    let now = Instant::now();
    let event = PriceEvent {
        symbol: meta.symbol,
        price: e8_to_float(trade.px_e8),
        received_instant: now,
        ts_mono_ns: instant_to_ns(now),
        exch_ts_ns: trade.event_ts_ns,
        event_ts_ms: normalize_ts_to_ms(trade.event_ts_ns),
        seq,
    };
    ctx.hot.price_in.fetch_add(1, Ordering::Relaxed);
    ctx.emit_price_event(event);
}

fn normalize_ts_to_ms(raw: u64) -> u64 {
    if raw >= 10_000_000_000_000_000 {
        raw / 1_000_000
    } else if raw >= 10_000_000_000_000 {
        raw / 1_000
    } else {
        raw
    }
}

fn shard_aggregator_loop(
    shard_index: usize,
    rx: IngestRx,
    spsc: SpscSender<PriceEvent>,
    log_tx: Sender<LogMessage>,
) {
    tracing::info!(
        "shard {}: aggregator online (MPSC→SPSC bridge)",
        shard_index
    );
    while let Ok(event) = rx.recv() {
        match spsc.try_send(event) {
            Ok(()) => {}
            Err(TrySendError::Full(event)) => {
                let _ = log_tx.send(
                    MetricEvent::QueueDropMarket {
                        symbol: event.symbol,
                    }
                    .into(),
                );
            }
            Err(TrySendError::Disconnected(_)) => {
                tracing::warn!(
                    "shard {}: processor channel disconnected; stopping aggregator",
                    shard_index
                );
                break;
            }
        }
    }
    tracing::info!(
        "shard {}: aggregator offline (all producers dropped)",
        shard_index
    );
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
        tracing::debug!(
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

#[inline]
fn e8_to_float(value: i64) -> f64 {
    value as f64 / 100_000_000.0
}

fn hex_head(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(32);
    for byte in bytes.iter().take(16) {
        use std::fmt::Write;
        let _ = write!(out, "{:02x}", byte);
    }
    out
}

fn parse_header(buf: &[u8]) -> Option<(u16, u16, u16, u16)> {
    if buf.len() < 8 {
        return None;
    }
    let block_len = LittleEndian::read_u16(&buf[0..2]);
    let template_id = LittleEndian::read_u16(&buf[2..4]);
    let schema_id = LittleEndian::read_u16(&buf[4..6]);
    let version = LittleEndian::read_u16(&buf[6..8]);
    Some((block_len, template_id, schema_id, version))
}
