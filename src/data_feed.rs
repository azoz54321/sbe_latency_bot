use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use byteorder::{ByteOrder, LittleEndian};
use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use futures_util::{SinkExt, StreamExt};
use http::header::{HeaderName, HeaderValue};
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::time::{interval, sleep};
use tokio_tungstenite::{
    client_async_tls_with_config,
    tungstenite::{client::IntoClientRequest, Message},
};
use tokio_util::sync::CancellationToken;
use url::Url;

mod schema_guard;
pub use schema_guard::{SchemaGuard, SchemaGuardHandle};

use crate::affinity;
use crate::channels::SpscSender;
use crate::config::{Config, ShardAssignment};
use crate::decoder_sbe::{DecodeStatus, SbeDecoder};
use crate::ffi::BsbeTrade;
use crate::strategy::shadow::Tick as ShadowTick;
use crate::time_utils::{instant_to_ns, wall_clock_now_ns};
use crate::types::{
    symbol_id_from_str, LogMessage, MetricEvent, PriceEvent, ReconnectNotice, Symbol,
};
use crate::universe::SymbolKey;

pub type IngestTx = Sender<PriceEvent>;
pub type IngestRx = Receiver<PriceEvent>;

pub const INGEST_CHANNEL_CAPACITY: usize = 65_536;
pub const PER_SHARD_CONNECT_CONC: usize = 32;
pub const DIAL_SPREAD_MS: u64 = 8;
const PING_INTERVAL_MS: u64 = 5_000;

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
        schema_guard,
        reconnect_tx,
        shadow_tx,
        shadow_drop_counter,
        ing_tx,
        config.backpressure.max_queue_age.as_nanos() as u64,
    );

    let ctx_for_seed = ctx.clone();
    runtime.spawn(async move {
        let desired: BTreeSet<SymbolKey> = symbols.iter().cloned().collect();
        let _ = ctx_for_seed.apply_universe_diff(desired).await;
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
    schema_guard: SchemaGuardHandle,
    reconnect_tx: Sender<ReconnectNotice>,
    shadow_tx: Sender<ShadowTick>,
    shadow_drop_counter: Arc<AtomicU64>,
    ingest_tx: IngestTx,
    max_skew_ns: u64,
    dial_sem: Arc<Semaphore>,
    symbols: Mutex<BTreeMap<String, SymbolRuntime>>,
}

struct SymbolRuntime {
    key: SymbolKey,
    cancel: CancellationToken,
}

impl ShardContext {
    fn new(
        config: &'static Config,
        shard_index: usize,
        log_tx: Sender<LogMessage>,
        schema_guard: SchemaGuardHandle,
        reconnect_tx: Sender<ReconnectNotice>,
        shadow_tx: Sender<ShadowTick>,
        shadow_drop_counter: Arc<AtomicU64>,
        ingest_tx: IngestTx,
        max_skew_ns: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            config,
            shard_index,
            log_tx,
            schema_guard,
            reconnect_tx,
            shadow_tx,
            shadow_drop_counter,
            ingest_tx,
            max_skew_ns,
            dial_sem: Arc::new(Semaphore::new(PER_SHARD_CONNECT_CONC)),
            symbols: Mutex::new(BTreeMap::new()),
        })
    }

    pub async fn snapshot_symbols(self: &Arc<Self>) -> Vec<SymbolKey> {
        let guard = self.symbols.lock().await;
        guard.values().map(|entry| entry.key.clone()).collect()
    }

    pub async fn symbol_count(self: &Arc<Self>) -> usize {
        let guard = self.symbols.lock().await;
        guard.len()
    }

    pub async fn apply_universe_diff(
        self: &Arc<Self>,
        desired: BTreeSet<SymbolKey>,
    ) -> RefreshStats {
        let desired_names: BTreeSet<_> = desired.iter().map(|k| k.name.clone()).collect();
        let guard = self.symbols.lock().await;
        let mut to_add = Vec::new();
        for key in desired.iter() {
            if !guard.contains_key(&key.name) {
                to_add.push(key.clone());
            }
        }
        let mut to_remove = Vec::new();
        for name in guard.keys() {
            if !desired_names.contains(name) {
                to_remove.push(name.clone());
            }
        }
        let kept = desired.len().saturating_sub(to_add.len());
        drop(guard);

        let mut removed = 0;
        for name in to_remove {
            if self.stop_symbol(&name).await {
                removed += 1;
            }
        }

        for key in to_add.iter().cloned() {
            self.spawn_symbol_task(key).await;
        }

        RefreshStats {
            added: to_add.len(),
            removed,
            kept,
        }
    }

    async fn spawn_symbol_task(self: &Arc<Self>, key: SymbolKey) {
        let permit = self
            .dial_sem
            .clone()
            .acquire_owned()
            .await
            .expect("failed to acquire dial permit");
        sleep(Duration::from_millis(DIAL_SPREAD_MS)).await;
        let token = CancellationToken::new();
        {
            let mut guard = self.symbols.lock().await;
            guard.insert(
                key.name.clone(),
                SymbolRuntime {
                    key: key.clone(),
                    cancel: token.clone(),
                },
            );
        }
        let symbol_name = key.name.clone();
        let ctx = self.clone();
        tokio::spawn(async move {
            run_symbol_reader(ctx, key, token, Some(permit)).await;
        });
        let _ = self.log_tx.send(LogMessage::Info(
            format!(
                "[BOOT] add symbol={} shard={}",
                symbol_name, self.shard_index
            )
            .into(),
        ));
    }

    async fn stop_symbol(&self, name: &str) -> bool {
        let entry = {
            let mut guard = self.symbols.lock().await;
            guard.remove(name)
        };
        if let Some(entry) = entry {
            entry.cancel.cancel();
            let _ = self.log_tx.send(LogMessage::Info(
                format!(
                    "[BOOT] remove symbol={} shard={}",
                    entry.key.name, self.shard_index
                )
                .into(),
            ));
            true
        } else {
            false
        }
    }

    fn notify_reconnect(&self) {
        let notice = ReconnectNotice {
            shard_index: self.shard_index,
            ts_mono_ns: instant_to_ns(Instant::now()),
        };
        let _ = self.reconnect_tx.try_send(notice);
    }

    fn send_shadow_tick(&self, key: &SymbolKey, trade: &BsbeTrade) {
        if trade.px_e8 == 0 {
            return;
        }
        let sym = if key.symbol_id != 0 {
            key.symbol_id
        } else {
            symbol_id_from_str(&key.name) as u32
        };
        let tick = ShadowTick {
            sym,
            px_e8: trade.px_e8,
            ts_ns: trade.event_ts_ns,
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
}

async fn run_symbol_reader(
    ctx: Arc<ShardContext>,
    key: SymbolKey,
    token: CancellationToken,
    mut dial_permit: Option<OwnedSemaphorePermit>,
) {
    let symbol = Symbol::from_str(&key.name).expect("invalid symbol");
    let url = format!(
        "wss://stream-sbe.binance.com/ws/{}@trade",
        key.name.to_ascii_lowercase()
    );
    tracing::info!(
        shard = ctx.shard_index,
        symbol = %key.name,
        %url,
        "per-symbol ws url"
    );
    let mut decoder = SbeDecoder::default();
    let mut last_seq: Option<u64> = None;

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                tracing::info!(
                    "[REFRESH] stopped symbol={} shard={}",
                    key.name,
                    ctx.shard_index
                );
                break;
            }
            result = connect_symbol_stream(
                ctx.clone(),
                &key,
                symbol,
                &url,
                &mut decoder,
                &mut last_seq,
                &token,
            ) => {
                if let Some(permit) = dial_permit.take() {
                    drop(permit);
                }

                match result {
                    Ok(()) => {
                        sleep(Duration::from_millis(250)).await;
                    }
                    Err(err) => {
                        tracing::warn!(
                            shard = ctx.shard_index,
                            symbol = %key.name,
                            error = %err,
                            "symbol stream error; backing off"
                        );
                        let jitter = fastrand::u64(..500);
                        sleep(Duration::from_millis(1_250 + jitter)).await;
                    }
                }
            }
        }
    }
}

async fn connect_symbol_stream(
    ctx: Arc<ShardContext>,
    key: &SymbolKey,
    symbol: Symbol,
    url: &str,
    decoder: &mut SbeDecoder,
    last_seq: &mut Option<u64>,
    token: &CancellationToken,
) -> anyhow::Result<()> {
    let parsed = Url::parse(url).expect("invalid WS URL");
    let mut request = parsed.clone().into_client_request()?;

    let header_name = HeaderName::from_static("x-mbx-apikey");
    let header_value =
        HeaderValue::from_str(ctx.config.credentials.sbe_ws_api_key).map_err(|err| {
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

    tracing::info!(
        "connected per-symbol shard={} symbol={} -> {}",
        ctx.shard_index,
        key.name,
        url
    );
    ctx.notify_reconnect();

    let (mut ws_sink, mut ws_source) = ws_stream.split();
    let mut ping_timer = interval(Duration::from_millis(PING_INTERVAL_MS));

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                return Ok(());
            }
            _ = ping_timer.tick() => {
                ws_sink.send(Message::Ping(Vec::new())).await?;
            }
            message = ws_source.next() => {
                let Some(message) = message else { break };
                match message {
                    Ok(Message::Binary(payload)) => {
                        if ctx.schema_guard.is_paused() {
                            continue;
                        }
                        ctx.log_tx.send(MetricEvent::WsMsgIn.into()).ok();
                        log_sbe_header_once(&payload);
                        handle_payload(&ctx, key, symbol, decoder, &payload, last_seq);
                    }
                    Ok(Message::Text(_)) => {
                        ctx.log_tx.send(MetricEvent::WsTextIn.into()).ok();
                    }
                    Ok(Message::Ping(data)) => {
                        ws_sink.send(Message::Pong(data)).await?;
                    }
                    Ok(Message::Pong(_)) => {}
                    Ok(Message::Close(_)) => break,
                    Ok(Message::Frame(_)) => {}
                    Err(err) => return Err(err.into()),
                }
            }
        }
    }

    Ok(())
}

fn handle_payload(
    ctx: &Arc<ShardContext>,
    key: &SymbolKey,
    symbol: Symbol,
    decoder: &mut SbeDecoder,
    payload: &[u8],
    last_seq: &mut Option<u64>,
) {
    let mut trades: Vec<BsbeTrade> = Vec::with_capacity(4);
    let report = decoder.decode_stream(payload, |trade| {
        trades.push(*trade);
    });

    for trade in &trades {
        process_trade(ctx, key, symbol, trade, last_seq);
    }

    match report.status {
        DecodeStatus::Complete | DecodeStatus::Incomplete => {}
        DecodeStatus::Corrupt => {
            ctx.log_tx.send(MetricEvent::DecodeErr.into()).ok();
        }
        DecodeStatus::SchemaMismatch => {
            ctx.schema_guard.record_mismatch();
        }
    }

    if decoder.take_schema_mismatch() {
        ctx.schema_guard.record_mismatch();
    }
}

fn process_trade(
    ctx: &ShardContext,
    key: &SymbolKey,
    symbol: Symbol,
    trade: &BsbeTrade,
    last_seq: &mut Option<u64>,
) {
    if trade.px_e8 == 0 {
        return;
    }

    ctx.log_tx.send(MetricEvent::PriceEventIn.into()).ok();
    ctx.send_shadow_tick(key, trade);

    let seq = trade.trade_id;
    if seq > 0 {
        if let Some(prev) = *last_seq {
            if seq <= prev || seq > prev + 1 {
                let _ = ctx.log_tx.send(
                    MetricEvent::SeqAnomaly {
                        symbol,
                        last_seq: prev,
                        observed_seq: seq,
                    }
                    .into(),
                );
            }
        }
        *last_seq = Some(seq);
    }

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
                e8_to_float(trade.px_e8),
                e8_to_float(trade.qty_e8),
                trade.event_ts_ns
            );
        }
    }

    let wall_ns = wall_clock_now_ns();
    if wall_ns > 0 && wall_ns.saturating_sub(trade.event_ts_ns) > ctx.max_skew_ns {
        return;
    }

    let now = Instant::now();
    let event = PriceEvent {
        symbol,
        price: e8_to_float(trade.px_e8),
        received_instant: now,
        ts_mono_ns: instant_to_ns(now),
        exch_ts_ns: trade.event_ts_ns,
        seq,
    };
    ctx.emit_price_event(event);
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
