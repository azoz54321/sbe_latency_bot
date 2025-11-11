use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use crossbeam_channel::{bounded, Sender, TryRecvError, TrySendError};
use futures_util::{SinkExt, StreamExt};
use hmac::Mac;
use rand::thread_rng;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::runtime::Builder;
use tokio::time::sleep;
use tokio_tungstenite::{
    client_async_tls_with_config,
    tungstenite::{client::IntoClientRequest, Message},
    MaybeTlsStream,
};
use url::Url;

use crate::affinity;
use crate::channels::{spsc_channel, SpscReceiver, SpscSender};
use crate::clock::Clock;
use crate::config::Config;
use crate::fees::tp_target_px;
use crate::net::backoff;
use crate::types::{LogMessage, Symbol};

use super::{
    format_decimal, ExecutionCommand, HmacSha256, OrderError, OrderIds, OrderSubmitError,
    ProviderOrder, TpAdjustSpec,
};

const WS_PRIMARY_URL: &str = "wss://ws-api.binance.com:443/ws-api/v3?timeUnit=MICROSECOND";
const WS_FALLBACK_URL: &str = "wss://ws-api.binance.com:9443/ws-api/v3?timeUnit=MICROSECOND";
const WS_METHOD_ORDER_PLACE: &str = "order.place";
const WS_METHOD_ORDER_CANCEL: &str = "order.cancel";
const COMMAND_QUEUE_CAPACITY: usize = 256;
const COMMAND_POLL_DELAY_US: u64 = 50;
const MAX_BACKOFF_MS: u64 = 5_000;

type ResponseTx = Sender<Result<OrderIds, OrderSubmitError>>;

pub struct WsOrderClient {
    tx: SpscSender<ClientCommand>,
}

impl WsOrderClient {
    pub fn spawn(
        config: &'static Config,
        clock: Arc<dyn Clock>,
        log_tx: Sender<LogMessage>,
        bind_core: usize,
        command_tx: Sender<ExecutionCommand>,
    ) -> Self {
        let (tx, rx) = spsc_channel(COMMAND_QUEUE_CAPACITY);
        thread::Builder::new()
            .name("ws-orders".to_string())
            .spawn(move || {
                affinity::bind_to_core(bind_core);
                let runtime = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build ws-orders runtime");
                runtime.block_on(async move {
                    let mut runner = WsOrderRuntime::new(config, clock, log_tx, rx, command_tx);
                    runner.run().await;
                });
            })
            .expect("failed to spawn ws-orders thread");

        Self { tx }
    }

    pub fn place_pair(
        &self,
        buy: ProviderOrder,
        limit: ProviderOrder,
        adjust: Option<TpAdjustSpec>,
    ) -> Result<OrderIds, OrderSubmitError> {
        let (resp_tx, resp_rx) = bounded(1);
        let cmd = ClientCommand::PlacePair {
            buy,
            limit,
            adjust,
            respond_to: resp_tx,
        };
        self.tx.try_send(cmd).map_err(|err| match err {
            TrySendError::Full(_) => OrderSubmitError::new(
                OrderError::Transient,
                "ws-order queue is full, dropping request",
            ),
            TrySendError::Disconnected(_) => {
                OrderSubmitError::new(OrderError::Transient, "ws-order queue disconnected")
            }
        })?;

        resp_rx.recv().map_err(|_| {
            OrderSubmitError::new(
                OrderError::Fatal,
                "ws-order response channel closed unexpectedly",
            )
        })?
    }
}

#[derive(Clone)]
enum ClientCommand {
    PlacePair {
        buy: ProviderOrder,
        limit: ProviderOrder,
        adjust: Option<TpAdjustSpec>,
        respond_to: ResponseTx,
    },
}

struct WsOrderRuntime {
    config: &'static Config,
    clock: Arc<dyn Clock>,
    log_tx: Sender<LogMessage>,
    rx: SpscReceiver<ClientCommand>,
    pending: HashMap<String, PendingPair>,
    lookup: HashMap<String, PendingKey>,
    pair_seq: AtomicU64,
    recv_window: u64,
    command_tx: Sender<ExecutionCommand>,
}

impl WsOrderRuntime {
    fn new(
        config: &'static Config,
        clock: Arc<dyn Clock>,
        log_tx: Sender<LogMessage>,
        rx: SpscReceiver<ClientCommand>,
        command_tx: Sender<ExecutionCommand>,
    ) -> Self {
        let recv_window = config.execution.recv_window_ms.min(5_000);
        Self {
            config,
            clock,
            log_tx,
            rx,
            pending: HashMap::new(),
            lookup: HashMap::new(),
            pair_seq: AtomicU64::new(1),
            recv_window,
            command_tx,
        }
    }

    async fn run(&mut self) {
        let mut attempt: u32 = 0;
        let mut rng = thread_rng();
        let base = Duration::from_millis(150);
        let cap = Duration::from_millis(MAX_BACKOFF_MS);
        loop {
            let mut connected = false;
            let mut connected_for = Duration::ZERO;
            match self.open_connection().await {
                Ok(mut stream) => {
                    self.log_debug("[WS] connected to Spot WS-API");
                    connected = true;
                    let started = Instant::now();
                    if let Err(err) = self.process_stream(&mut stream).await {
                        self.log_error(format!("[WS] stream error: {err:?}"));
                    }
                    connected_for = started.elapsed();
                }
                Err(err) => {
                    self.log_error(format!("[WS] connect error: {err:?}"));
                }
            }

            self.fail_all_pending("ws-api disconnected");
            if self.drain_offline_commands("ws-api disconnected") {
                break;
            }

            let reset_attempt = connected && connected_for >= Duration::from_secs(60);
            if reset_attempt {
                attempt = 0;
            }
            let sleep_for = backoff::full_jitter(attempt, base, cap, &mut rng);
            sleep(sleep_for).await;
            if !reset_attempt {
                attempt = attempt.saturating_add(1);
            }
        }
    }

    async fn open_connection(
        &self,
    ) -> anyhow::Result<tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>> {
        for url in [WS_PRIMARY_URL, WS_FALLBACK_URL] {
            match self.connect_url(url).await {
                Ok(stream) => return Ok(stream),
                Err(err) => {
                    self.log_error(format!("[WS] failed to connect {url}: {err:?}"));
                }
            }
        }
        Err(anyhow!("all WS-API endpoints failed"))
    }

    async fn connect_url(
        &self,
        url: &str,
    ) -> anyhow::Result<tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>> {
        let parsed = Url::parse(url)?;
        let host = parsed
            .host_str()
            .ok_or_else(|| anyhow!("missing host in {}", url))?;
        let port = parsed.port_or_known_default().unwrap_or(443);
        let addr = format!("{host}:{port}");
        let stream = TcpStream::connect(addr.clone())
            .await
            .with_context(|| format!("tcp connect {}", addr))?;
        stream
            .set_nodelay(true)
            .with_context(|| "set TCP_NODELAY failed")?;
        let request = parsed.clone().into_client_request()?;
        let (ws_stream, response) = client_async_tls_with_config(request, stream, None, None)
            .await
            .with_context(|| format!("ws handshake {}", url))?;
        if let Some(ext) = response.headers().get("Sec-WebSocket-Extensions") {
            let value = ext.to_str().unwrap_or_default().to_ascii_lowercase();
            if value.contains("permessage-deflate") {
                return Err(anyhow!(
                    "compression negotiated (permessage-deflate); refusing connection"
                ));
            }
        }
        self.log_info("[WS] no_compression=true; TCP_NODELAY=true");
        Ok(ws_stream)
    }

    async fn process_stream(
        &mut self,
        stream: &mut tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                cmd = Self::recv_command(&self.rx) => {
                    match cmd {
                        Some(command) => self.handle_command(stream, command).await?,
                        None => return Ok(()),
                    }
                }
                msg = stream.next() => {
                    match msg {
                        Some(Ok(message)) => self.handle_message(stream, message).await?,
                        Some(Err(err)) => return Err(err.into()),
                        None => return Err(anyhow!("ws-api closed connection")),
                    }
                }
            }
            self.expire_adjusts();
        }
    }

    async fn recv_command(rx: &SpscReceiver<ClientCommand>) -> Option<ClientCommand> {
        loop {
            match rx.try_recv() {
                Ok(cmd) => return Some(cmd),
                Err(TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_micros(COMMAND_POLL_DELAY_US)).await;
                }
                Err(TryRecvError::Disconnected) => return None,
            }
        }
    }

    async fn handle_command(
        &mut self,
        stream: &mut tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
        command: ClientCommand,
    ) -> anyhow::Result<()> {
        match command {
            ClientCommand::PlacePair {
                buy,
                limit,
                adjust,
                respond_to,
            } => {
                self.enqueue_pair(stream, buy, limit, adjust, respond_to)
                    .await?;
            }
        }
        Ok(())
    }

    async fn handle_message(
        &mut self,
        stream: &mut tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
        message: Message,
    ) -> anyhow::Result<()> {
        match message {
            Message::Text(payload) => {
                if let Err(err) = self.handle_response_text(stream, &payload).await {
                    self.log_error(format!("[WS] parse error: {err:?} payload={payload}"));
                }
            }
            Message::Binary(payload) => {
                let text = String::from_utf8_lossy(&payload);
                if let Err(err) = self.handle_response_text(stream, text.as_ref()).await {
                    self.log_error(format!("[WS] binary parse error: {err:?}"));
                }
            }
            Message::Ping(payload) => {
                stream.send(Message::Pong(payload)).await?;
            }
            Message::Pong(_) => {}
            Message::Close(_) => {
                return Err(anyhow!("ws-api requested close"));
            }
            Message::Frame(_) => {}
        }
        Ok(())
    }

    async fn enqueue_pair(
        &mut self,
        stream: &mut tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
        buy: ProviderOrder,
        limit: ProviderOrder,
        adjust: Option<TpAdjustSpec>,
        respond_to: ResponseTx,
    ) -> anyhow::Result<()> {
        let pair_id = self.next_pair_id();
        let buy_msg_id = format!("{pair_id}-B");
        let limit_msg_id = format!("{pair_id}-L");

        let buy_frame = match self.build_request(&buy, &buy_msg_id) {
            Ok(frame) => frame,
            Err(err) => {
                let _ = respond_to.send(Err(err));
                return Ok(());
            }
        };
        let limit_frame = match self.build_request(&limit, &limit_msg_id) {
            Ok(frame) => frame,
            Err(err) => {
                let _ = respond_to.send(Err(err));
                return Ok(());
            }
        };

        let order_ids = OrderIds {
            buy: Some(buy.client_order_id.clone()),
            limit: Some(limit.client_order_id.clone()),
        };

        let adjust_state = adjust.map(TpAdjustState::from_spec);
        let symbol = buy.symbol;
        self.register_pair(
            pair_id.clone(),
            &buy_msg_id,
            &limit_msg_id,
            respond_to,
            order_ids,
            symbol,
            adjust_state,
        );

        let buy_sent_at = Instant::now();
        if let Err(err) = stream.send(Message::Text(buy_frame)).await {
            self.fail_pair(
                &pair_id,
                OrderSubmitError::new(OrderError::Fatal, err.to_string()),
            );
            return Err(err.into());
        }
        let limit_sent_at;
        if let Err(err) = stream.send(Message::Text(limit_frame)).await {
            self.fail_pair(
                &pair_id,
                OrderSubmitError::new(OrderError::Fatal, err.to_string()),
            );
            return Err(err.into());
        } else {
            limit_sent_at = Instant::now();
        }

        let delta = limit_sent_at.saturating_duration_since(buy_sent_at);
        self.log_debug(format!(
            "[WS] pair_send symbol={} buy_limit_gap_ns={}",
            symbol,
            delta.as_nanos()
        ));

        Ok(())
    }

    fn build_request(&self, order: &ProviderOrder, id: &str) -> Result<String, OrderSubmitError> {
        let mut params = Vec::with_capacity(12);
        params.push((
            "apiKey".to_string(),
            self.config.credentials.rest_api_key.to_string(),
        ));
        params.push((
            "newClientOrderId".to_string(),
            order.client_order_id.clone(),
        ));
        params.push((
            "quantity".to_string(),
            format_decimal(order.quantity, order.quantity_scale),
        ));
        params.push(("recvWindow".to_string(), self.recv_window.to_string()));
        params.push(("side".to_string(), order.side.to_string()));
        params.push(("symbol".to_string(), order.symbol.as_str().to_string()));
        params.push((
            "timestamp".to_string(),
            (self.clock.unix_now_ns() / 1_000).to_string(),
        ));
        params.push(("type".to_string(), order.order_type.to_string()));
        let resp_type = if order.order_type.eq_ignore_ascii_case("MARKET") {
            "FULL"
        } else {
            "ACK"
        };
        params.push(("newOrderRespType".to_string(), resp_type.to_string()));

        if let Some(price) = order.price {
            params.push((
                "price".to_string(),
                format_decimal(price, order.price_scale),
            ));
            params.push(("timeInForce".to_string(), "GTC".to_string()));
        }

        params.sort_by(|a, b| a.0.cmp(&b.0));
        let payload = params
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let signature = self.sign_payload(&payload)?;

        let mut json_params = serde_json::Map::new();
        for (k, v) in params {
            json_params.insert(k, serde_json::Value::String(v));
        }
        json_params.insert(
            "signature".to_string(),
            serde_json::Value::String(signature),
        );

        let request = serde_json::json!({
            "id": id,
            "method": WS_METHOD_ORDER_PLACE,
            "params": json_params,
        });
        Ok(request.to_string())
    }

    fn build_cancel_request(
        &self,
        symbol: Symbol,
        client_order_id: &str,
        id: &str,
    ) -> Result<String, OrderSubmitError> {
        let mut params = Vec::with_capacity(8);
        params.push((
            "apiKey".to_string(),
            self.config.credentials.rest_api_key.to_string(),
        ));
        params.push(("origClientOrderId".to_string(), client_order_id.to_string()));
        params.push(("recvWindow".to_string(), self.recv_window.to_string()));
        params.push(("symbol".to_string(), symbol.as_str().to_string()));
        params.push((
            "timestamp".to_string(),
            (self.clock.unix_now_ns() / 1_000).to_string(),
        ));

        params.sort_by(|a, b| a.0.cmp(&b.0));
        let payload = params
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let signature = self.sign_payload(&payload)?;

        let mut json_params = serde_json::Map::new();
        for (k, v) in params {
            json_params.insert(k, serde_json::Value::String(v));
        }
        json_params.insert(
            "signature".to_string(),
            serde_json::Value::String(signature),
        );

        let request = serde_json::json!({
            "id": id,
            "method": WS_METHOD_ORDER_CANCEL,
            "params": json_params,
        });
        Ok(request.to_string())
    }

    fn sign_payload(&self, payload: &str) -> Result<String, OrderSubmitError> {
        let secret = self.config.credentials.rest_api_secret.as_bytes();
        let mut mac = HmacSha256::new_from_slice(secret)
            .map_err(|err| OrderSubmitError::new(OrderError::Fatal, err.to_string()))?;
        mac.update(payload.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }

    fn register_pair(
        &mut self,
        pair_id: String,
        buy_msg_id: &str,
        limit_msg_id: &str,
        respond_to: ResponseTx,
        order_ids: OrderIds,
        symbol: Symbol,
        adjust: Option<TpAdjustState>,
    ) {
        let entry = PendingPair {
            respond_to: Some(respond_to),
            order_ids,
            buy_done: false,
            limit_done: false,
            symbol,
            adjust,
        };
        self.pending.insert(pair_id.clone(), entry);
        self.lookup.insert(
            buy_msg_id.to_string(),
            PendingKey {
                pair_id: pair_id.clone(),
                side: PendingSide::Buy,
            },
        );
        self.lookup.insert(
            limit_msg_id.to_string(),
            PendingKey {
                pair_id,
                side: PendingSide::Limit,
            },
        );
    }

    async fn handle_response_text(
        &mut self,
        stream: &mut tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
        payload: &str,
    ) -> anyhow::Result<()> {
        let resp: WsResponse = serde_json::from_str(payload)?;
        let id = resp
            .id
            .ok_or_else(|| anyhow!("response missing id: {}", payload))?;
        let status = resp.status.unwrap_or_default();

        let key = match self.lookup.remove(&id) {
            Some(key) => key,
            None => {
                self.log_error(format!("[WS] unknown response id={id}"));
                return Ok(());
            }
        };

        if status == 200 {
            self.handle_success(stream, key, resp.result.as_ref())
                .await?;
        } else {
            let code = resp.error.as_ref().and_then(|err| err.code);
            let msg = resp
                .error
                .as_ref()
                .and_then(|err| err.msg.as_deref())
                .unwrap_or("unknown error")
                .to_string();
            match key.side {
                PendingSide::Adjust => {
                    self.handle_adjust_error(&key.pair_id, code, &msg);
                }
                PendingSide::Cancel => {
                    self.handle_cancel_error(&key.pair_id, code, &msg);
                }
                _ => {
                    let err = OrderSubmitError::new(
                        classify_ws_error(code, &msg),
                        format!("ws-api rejected order: code={:?} msg={}", code, msg),
                    );
                    self.fail_pair(&key.pair_id, err);
                }
            }
        }
        Ok(())
    }

    async fn handle_success(
        &mut self,
        stream: &mut tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
        key: PendingKey,
        result: Option<&Value>,
    ) -> anyhow::Result<()> {
        if matches!(key.side, PendingSide::Adjust) {
            self.handle_adjust_success(&key.pair_id, result)?;
            return Ok(());
        }

        let pair_id = key.pair_id.clone();
        let mut log_message: Option<String> = None;
        {
            if let Some(pair) = self.pending.get_mut(&pair_id) {
                let symbol = pair.symbol;
                let client_id = extract_client_order_id(result);
                match key.side {
                    PendingSide::Buy => {
                        pair.buy_done = true;
                        if let Some(state) = pair.adjust.as_mut() {
                            let fill_price = extract_fill_price(result);
                            state.update_fill_price(fill_price);
                        }
                        if let Some(id) = client_id {
                            log_message = Some(format!(
                                "[BUY] WS-API acknowledged symbol={} client_id={}",
                                symbol, id
                            ));
                        } else {
                            log_message =
                                Some(format!("[BUY] WS-API acknowledged symbol={}", symbol));
                        }
                    }
                    PendingSide::Limit => {
                        pair.limit_done = true;
                        if let Some(id) = client_id {
                            log_message = Some(format!(
                                "[SELL] WS-API acknowledged symbol={} client_id={}",
                                symbol, id
                            ));
                        } else {
                            log_message =
                                Some(format!("[SELL] WS-API acknowledged symbol={}", symbol));
                        }
                    }
                    PendingSide::Cancel => {}
                    PendingSide::Adjust => {}
                }
            }
        }

        if let Some(msg) = log_message {
            self.log_info(msg);
        }

        self.after_ack(stream, &pair_id).await?;
        Ok(())
    }

    async fn after_ack(
        &mut self,
        stream: &mut tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
        pair_id: &str,
    ) -> anyhow::Result<()> {
        let mut plan: Option<AdjustPlan> = None;
        if let Some(pair) = self.pending.get_mut(pair_id) {
            if pair.buy_done && pair.limit_done {
                if let Some(respond_to) = pair.respond_to.take() {
                    let _ = respond_to.send(Ok(pair.order_ids.clone()));
                }
            }

            if let Some(state) = pair.adjust.as_mut() {
                if state.adjust_done || state.adjust_inflight {
                    // waiting for inflight adjust or already done
                } else if state.expired() {
                    state.mark_skipped();
                } else if let Some(fill_price) = state.fill_price {
                    let new_price = state.compute_ideal_tp(fill_price);
                    if new_price <= Decimal::ZERO {
                        state.mark_skipped();
                    } else {
                        let ticks_diff = state.diff_ticks(new_price);
                        if ticks_diff < state.min_diff_ticks || new_price == state.current_tp {
                            state.mark_skipped();
                        } else {
                            let new_client_id = build_adjust_client_id(&state.client_order_id);
                            plan = Some(AdjustPlan {
                                symbol: pair.symbol,
                                cancel_client_id: state.client_order_id.clone(),
                                new_client_id,
                                quantity: state.quantity,
                                quantity_scale: state.quantity_scale,
                                price_scale: state.price_scale,
                                new_price,
                                ticks_diff,
                            });
                        }
                    }
                }
            }
        }

        if let Some(plan) = plan {
            match self.send_adjust(stream, pair_id, &plan).await {
                Ok(()) => {
                    if let Some(pair) = self.pending.get_mut(pair_id) {
                        if let Some(state) = pair.adjust.as_mut() {
                            state.mark_pending_adjust(
                                plan.new_price,
                                plan.new_client_id.clone(),
                                plan.ticks_diff,
                            );
                        }
                    }
                }
                Err(err) => {
                    self.log_error(format!(
                        "[TP_ADJUST] send_failed symbol={} err={err}",
                        plan.symbol
                    ));
                    if let Some(pair) = self.pending.get_mut(pair_id) {
                        if let Some(state) = pair.adjust.as_mut() {
                            state.mark_failed();
                        }
                    }
                }
            }
        }

        self.cleanup_pair_if_done(pair_id);
        Ok(())
    }

    async fn send_adjust(
        &mut self,
        stream: &mut tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
        pair_id: &str,
        plan: &AdjustPlan,
    ) -> anyhow::Result<()> {
        let cancel_msg_id = format!("{pair_id}-C");
        let cancel_frame = self
            .build_cancel_request(plan.symbol, &plan.cancel_client_id, &cancel_msg_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        self.lookup.insert(
            cancel_msg_id.clone(),
            PendingKey {
                pair_id: pair_id.to_string(),
                side: PendingSide::Cancel,
            },
        );
        if let Err(err) = stream.send(Message::Text(cancel_frame)).await {
            self.lookup.remove(&cancel_msg_id);
            return Err(err.into());
        }

        let order = ProviderOrder {
            symbol: plan.symbol,
            side: "SELL",
            order_type: "LIMIT",
            quantity: plan.quantity,
            quantity_scale: plan.quantity_scale,
            price: Some(plan.new_price),
            price_scale: plan.price_scale,
            client_order_id: plan.new_client_id.clone(),
            trigger_ts_mono_ns: self.clock.monotonic_now_ns(),
        };
        let adjust_msg_id = format!("{pair_id}-A");
        let adjust_frame = self
            .build_request(&order, &adjust_msg_id)
            .map_err(|err| anyhow!(err.to_string()))?;
        self.lookup.insert(
            adjust_msg_id.clone(),
            PendingKey {
                pair_id: pair_id.to_string(),
                side: PendingSide::Adjust,
            },
        );
        if let Err(err) = stream.send(Message::Text(adjust_frame)).await {
            self.lookup.remove(&adjust_msg_id);
            return Err(err.into());
        }

        Ok(())
    }

    fn cleanup_pair_if_done(&mut self, pair_id: &str) {
        let remove = match self.pending.get(pair_id) {
            Some(pair) => {
                let response_done = pair.respond_to.is_none();
                let adjust_done = pair
                    .adjust
                    .as_ref()
                    .map(|state| state.adjust_done && !state.adjust_inflight)
                    .unwrap_or(true);
                response_done && adjust_done
            }
            None => false,
        };

        if remove {
            self.pending.remove(pair_id);
        }
    }

    fn expire_adjusts(&mut self) {
        let mut cleanup = Vec::new();
        for (pair_id, pair) in self.pending.iter_mut() {
            if let Some(state) = pair.adjust.as_mut() {
                if !state.adjust_done && !state.adjust_inflight && state.expired() {
                    state.mark_skipped();
                    cleanup.push(pair_id.clone());
                }
            }
        }

        for pair_id in cleanup {
            self.cleanup_pair_if_done(&pair_id);
        }
    }

    fn handle_adjust_success(
        &mut self,
        pair_id: &str,
        result: Option<&Value>,
    ) -> anyhow::Result<()> {
        let mut command: Option<ExecutionCommand> = None;
        if let Some(pair) = self.pending.get_mut(pair_id) {
            if let Some(state) = pair.adjust.as_mut() {
                if !state.adjust_inflight {
                    return Ok(());
                }
                let old_order_id = state.client_order_id.clone();
                if let Some(fill_price) = extract_fill_price(result) {
                    state.update_fill_price(Some(fill_price));
                }
                let ack_price = result
                    .and_then(|value| value.as_object())
                    .and_then(|map| map.get("price"))
                    .and_then(|value| value.as_str())
                    .and_then(parse_decimal_str);
                let pending_price = state.pending_new_price.unwrap_or(state.current_tp);
                let new_price = ack_price.unwrap_or(pending_price);
                let pending_client = state
                    .pending_new_client_id
                    .clone()
                    .unwrap_or_else(|| state.client_order_id.clone());
                let new_client_id = extract_client_order_id(result).unwrap_or(pending_client);
                let ticks_diff = state.pending_ticks_diff;
                pair.order_ids.limit = Some(new_client_id.clone());
                state.finalize_adjust(new_price, new_client_id.clone());
                command = Some(ExecutionCommand::TpAdjust {
                    symbol: state.symbol,
                    old_order_id,
                    new_order_id: new_client_id,
                    new_price,
                    ticks_diff,
                });
            }
        }

        if let Some(cmd) = command {
            let _ = self.command_tx.send(cmd);
        }

        self.cleanup_pair_if_done(pair_id);
        Ok(())
    }

    fn handle_adjust_error(&mut self, pair_id: &str, code: Option<i64>, msg: &str) {
        let mut log_entry: Option<String> = None;
        if let Some(pair) = self.pending.get_mut(pair_id) {
            let symbol = pair.symbol;
            if let Some(state) = pair.adjust.as_mut() {
                log_entry = Some(format!(
                    "[TP_ADJUST] adjust_error symbol={} code={:?} msg={}",
                    symbol, code, msg
                ));
                state.mark_failed();
            }
        } else {
            log_entry = Some(format!(
                "[TP_ADJUST] adjust_error pair={} code={:?} msg={}",
                pair_id, code, msg
            ));
        }
        if let Some(entry) = log_entry {
            self.log_error(entry);
        }
        self.cleanup_pair_if_done(pair_id);
    }

    fn handle_cancel_error(&mut self, pair_id: &str, code: Option<i64>, msg: &str) {
        if let Some(pair) = self.pending.get(pair_id) {
            self.log_error(format!(
                "[TP_ADJUST] cancel_error symbol={} code={:?} msg={}",
                pair.symbol, code, msg
            ));
        } else {
            self.log_error(format!(
                "[TP_ADJUST] cancel_error pair={} code={:?} msg={}",
                pair_id, code, msg
            ));
        }
    }

    fn fail_pair(&mut self, pair_id: &str, err: OrderSubmitError) {
        if let Some(mut entry) = self.pending.remove(pair_id) {
            if let Some(tx) = entry.respond_to.take() {
                let _ = tx.send(Err(err));
            }
        }
        self.lookup.retain(|_, pending| pending.pair_id != pair_id);
    }

    fn fail_all_pending(&mut self, reason: &str) {
        let reason_owned = reason.to_string();
        for (_, mut entry) in self.pending.drain() {
            let err = OrderSubmitError::new(OrderError::Transient, reason_owned.clone());
            if let Some(tx) = entry.respond_to.take() {
                let _ = tx.send(Err(err));
            }
        }
        self.lookup.clear();
    }

    fn drain_offline_commands(&mut self, reason: &str) -> bool {
        loop {
            match self.rx.try_recv() {
                Ok(command) => self.fail_command(command, reason),
                Err(TryRecvError::Empty) => break false,
                Err(TryRecvError::Disconnected) => break true,
            }
        }
    }

    fn fail_command(&self, command: ClientCommand, reason: &str) {
        let err = OrderSubmitError::new(OrderError::Transient, reason.to_string());
        match command {
            ClientCommand::PlacePair { respond_to, .. } => {
                let _ = respond_to.send(Err(err));
            }
        }
    }

    fn next_pair_id(&self) -> String {
        let seq = self.pair_seq.fetch_add(1, Ordering::Relaxed);
        format!("pair-{seq}")
    }

    fn log_debug(&self, msg: impl AsRef<str>) {
        tracing::debug!(target: "bot", "{}", msg.as_ref());
    }

    fn log_info(&self, msg: impl Into<String>) {
        let owned: String = msg.into();
        let _ = self.log_tx.send(LogMessage::Info(owned.into()));
    }

    fn log_error(&self, msg: impl Into<String>) {
        let owned: String = msg.into();
        let _ = self.log_tx.send(LogMessage::Error(owned));
    }
}

#[derive(Clone)]
struct PendingPair {
    respond_to: Option<ResponseTx>,
    order_ids: OrderIds,
    buy_done: bool,
    limit_done: bool,
    symbol: Symbol,
    adjust: Option<TpAdjustState>,
}

struct AdjustPlan {
    symbol: Symbol,
    cancel_client_id: String,
    new_client_id: String,
    quantity: Decimal,
    quantity_scale: u32,
    price_scale: u32,
    new_price: Decimal,
    ticks_diff: u32,
}

#[derive(Clone)]
struct TpAdjustState {
    min_diff_ticks: u32,
    timeout: Duration,
    tick_size: Decimal,
    tp_pct: Decimal,
    maker_fee_pct: Decimal,
    taker_fee_pct: Decimal,
    quantity: Decimal,
    quantity_scale: u32,
    price_scale: u32,
    current_tp: Decimal,
    client_order_id: String,
    symbol: Symbol,
    start: Instant,
    fill_price: Option<Decimal>,
    adjust_done: bool,
    adjust_inflight: bool,
    pending_new_price: Option<Decimal>,
    pending_new_client_id: Option<String>,
    pending_ticks_diff: u32,
}

impl TpAdjustState {
    fn from_spec(spec: TpAdjustSpec) -> Self {
        Self {
            min_diff_ticks: spec.min_diff_ticks,
            timeout: spec.timeout,
            tick_size: spec.tick_size,
            tp_pct: spec.tp_pct,
            maker_fee_pct: spec.maker_fee_pct,
            taker_fee_pct: spec.taker_fee_pct,
            quantity: spec.quantity,
            quantity_scale: spec.quantity_scale,
            price_scale: spec.price_scale,
            current_tp: spec.current_tp,
            client_order_id: spec.client_order_id,
            symbol: spec.symbol,
            start: Instant::now(),
            fill_price: None,
            adjust_done: false,
            adjust_inflight: false,
            pending_new_price: None,
            pending_new_client_id: None,
            pending_ticks_diff: 0,
        }
    }

    fn update_fill_price(&mut self, price: Option<Decimal>) {
        if self.fill_price.is_none() {
            self.fill_price = price;
        }
    }

    fn expired(&self) -> bool {
        self.start.elapsed() > self.timeout
    }

    fn compute_ideal_tp(&self, fill_price: Decimal) -> Decimal {
        let target = tp_target_px(
            fill_price,
            self.tp_pct,
            self.maker_fee_pct,
            self.taker_fee_pct,
        );
        super::align_tick_up(target, self.tick_size)
    }

    fn diff_ticks(&self, new_price: Decimal) -> u32 {
        if self.tick_size <= Decimal::ZERO {
            return 0;
        }
        let diff = (new_price - self.current_tp).abs();
        let ticks = diff / self.tick_size;
        ticks
            .to_u32()
            .or_else(|| ticks.round().to_u32())
            .unwrap_or(0)
    }

    fn mark_pending_adjust(&mut self, new_price: Decimal, new_client_id: String, ticks_diff: u32) {
        self.pending_new_price = Some(new_price);
        self.pending_new_client_id = Some(new_client_id);
        self.pending_ticks_diff = ticks_diff;
        self.adjust_inflight = true;
    }

    fn finalize_adjust(&mut self, new_price: Decimal, new_client_id: String) {
        self.current_tp = new_price;
        self.client_order_id = new_client_id;
        self.adjust_done = true;
        self.adjust_inflight = false;
        self.pending_new_price = None;
        self.pending_new_client_id = None;
        self.pending_ticks_diff = 0;
    }

    fn mark_skipped(&mut self) {
        self.adjust_done = true;
        self.adjust_inflight = false;
        self.pending_new_price = None;
        self.pending_new_client_id = None;
        self.pending_ticks_diff = 0;
    }

    fn mark_failed(&mut self) {
        self.mark_skipped();
    }
}

#[derive(Clone)]
struct PendingKey {
    pair_id: String,
    side: PendingSide,
}

#[derive(Clone, Copy)]
enum PendingSide {
    Buy,
    Limit,
    Cancel,
    Adjust,
}

#[derive(Debug, Deserialize)]
struct WsResponse {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    status: Option<u16>,
    #[serde(default)]
    result: Option<serde_json::Value>,
    #[serde(default)]
    error: Option<WsErrorPayload>,
}

#[derive(Debug, Deserialize, Clone)]
struct WsErrorPayload {
    #[serde(default)]
    code: Option<i64>,
    #[serde(default)]
    msg: Option<String>,
}

fn classify_ws_error(code: Option<i64>, msg: &str) -> OrderError {
    let upper = msg.to_ascii_uppercase();
    if upper.contains("PRICE_FILTER") {
        return OrderError::FilterPriceTick;
    }
    if upper.contains("LOT_SIZE") {
        return OrderError::FilterLotStep;
    }
    if upper.contains("MIN_NOTIONAL") {
        return OrderError::FilterMinNotional;
    }

    match code.unwrap_or_default() {
        -1013 | -1131 => OrderError::FilterLotStep,
        -1015 | -1003 => OrderError::Transient,
        _ => OrderError::Fatal,
    }
}

fn build_adjust_client_id(current: &str) -> String {
    if current.ends_with("-adj") {
        format!("{current}-1")
    } else {
        format!("{current}-adj")
    }
}

fn extract_client_order_id(result: Option<&Value>) -> Option<String> {
    let map = result?.as_object()?;
    map.get("clientOrderId")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
}

fn extract_fill_price(result: Option<&Value>) -> Option<Decimal> {
    let map = result?.as_object()?;

    if let Some(price) = map
        .get("avgPrice")
        .and_then(|value| value.as_str())
        .and_then(parse_decimal_str)
    {
        if price > Decimal::ZERO {
            return Some(price);
        }
    }

    if let Some(price) = map
        .get("price")
        .and_then(|value| value.as_str())
        .and_then(parse_decimal_str)
    {
        if price > Decimal::ZERO {
            return Some(price);
        }
    }

    if let (Some(qty), Some(quote)) = (
        map.get("executedQty")
            .and_then(|value| value.as_str())
            .and_then(parse_decimal_str),
        map.get("cummulativeQuoteQty")
            .and_then(|value| value.as_str())
            .and_then(parse_decimal_str),
    ) {
        if qty > Decimal::ZERO {
            let price = (quote / qty).normalize();
            if price > Decimal::ZERO {
                return Some(price);
            }
        }
    }

    if let Some(fills) = map.get("fills").and_then(Value::as_array) {
        for fill in fills {
            if let Some(price) = fill
                .get("price")
                .and_then(|value| value.as_str())
                .and_then(parse_decimal_str)
            {
                if price > Decimal::ZERO {
                    return Some(price);
                }
            }
        }
    }

    None
}

fn parse_decimal_str(value: &str) -> Option<Decimal> {
    Decimal::from_str(value).ok()
}
