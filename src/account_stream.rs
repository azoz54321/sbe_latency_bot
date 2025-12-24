use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Context};
use crossbeam_channel::Sender;
use futures_util::StreamExt;
use hmac::{Hmac, Mac};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use sha2::Sha256;
use tokio::net::TcpStream;
use tokio::runtime::Builder;
use tokio::time::interval;
use tokio_tungstenite::{client_async_tls_with_config, tungstenite::client::IntoClientRequest};
use url::Url;

use crate::config::{Config, ExecutionMode};
use crate::time_sync::TimeSync;
use crate::types::{
    AccountEvent, AccountExecutionReport, BalanceSnapshot, LogMessage, OpenOrderSnapshot, Symbol,
};

const WS_BASE_URL: &str = "wss://stream.binance.com:9443/ws/";
const KEEPALIVE_INTERVAL_SECS: u64 = 15 * 60;

pub fn spawn_account_stream(
    config: &'static Config,
    account_tx: Sender<AccountEvent>,
    log_tx: Sender<LogMessage>,
    time_sync: Arc<TimeSync>,
) -> Option<thread::JoinHandle<()>> {
    if config.execution.mode != ExecutionMode::Live || !config.execution.live_armed {
        return None;
    }
    if config.credentials.rest_api_key.is_empty() {
        let _ = log_tx.send(LogMessage::Warn(
            "[ACCT] REST API key missing; account stream disabled".into(),
        ));
        return None;
    }

    let base_url = config.transport.rest_base_url.to_string();
    let api_key = config.credentials.rest_api_key.to_string();
    let secret = config.credentials.rest_api_secret.to_string();
    let log_tx_clone = log_tx.clone();
    Some(
        thread::Builder::new()
            .name("account-stream".into())
            .spawn(move || {
                let runtime = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("account stream runtime");
                runtime.block_on(async move {
                    if let Err(err) =
                        run_stream(
                            config,
                            &base_url,
                            &api_key,
                            &secret,
                            account_tx,
                            log_tx,
                            time_sync,
                        )
                        .await
                    {
                        let _ = log_tx_clone
                            .send(LogMessage::Error(format!("[ACCT] stream stopped: {err:?}")));
                    }
                });
            })
            .expect("spawn account stream thread"),
    )
}

async fn run_stream(
    config: &'static Config,
    rest_base: &str,
    api_key: &str,
    api_secret: &str,
    account_tx: Sender<AccountEvent>,
    log_tx: Sender<LogMessage>,
    time_sync: Arc<TimeSync>,
) -> anyhow::Result<()> {
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("building account stream HTTP client")?;

    loop {
        let listen_key = match create_listen_key(&client, rest_base, api_key).await {
            Ok(key) => key,
            Err(err) => {
                let _ = log_tx.send(LogMessage::Error(format!(
                    "[ACCT] listen key create failed: {err:?}"
                )));
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        if let Ok(snapshot) =
            fetch_account_snapshot(&client, rest_base, api_key, api_secret, &time_sync).await
        {
            let _ = account_tx.send(AccountEvent::AccountSnapshot {
                balances: snapshot,
            });
        }

        if let Ok(open_orders) =
            fetch_open_orders(&client, rest_base, api_key, api_secret, &time_sync).await
        {
            let _ = account_tx.send(AccountEvent::OpenOrders(open_orders));
        }

        let ws_url = format!("{WS_BASE_URL}{listen_key}");
        let keepalive_interval = KEEPALIVE_INTERVAL_SECS.max(config.user_stream.keepalive_secs);
        let mut keepalive = interval(Duration::from_secs(keepalive_interval));
        let parsed = Url::parse(&ws_url).with_context(|| "parsing WS url")?;
        let host = parsed
            .host_str()
            .ok_or_else(|| anyhow!("missing host in ws url"))?;
        let port = parsed.port_or_known_default().unwrap_or(9443);
        let addr = format!("{host}:{port}");
        let tcp = TcpStream::connect(addr.clone())
            .await
            .with_context(|| format!("tcp connect {addr}"))?;
        tcp.set_nodelay(true)
            .with_context(|| "set TCP_NODELAY failed for account stream")?;
        let request = parsed.clone().into_client_request()?;
        let ws_result = client_async_tls_with_config(request, tcp, None, None).await;
        let (mut ws_stream, _) = match ws_result {
            Ok(parts) => parts,
            Err(err) => {
                let _ = log_tx.send(LogMessage::Error(format!(
                    "[ACCT] ws connect failed: {err:?}"
                )));
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        let _ = log_tx.send(LogMessage::Info("[ACCT] stream connected".into()));

        loop {
            tokio::select! {
                _ = keepalive.tick() => {
                    if let Err(err) = send_keepalive(&client, rest_base, api_key, &listen_key).await {
                        let _ = log_tx.send(LogMessage::Warn(format!("[ACCT] keepalive failed: {err:?}").into()));
                        break;
                    }
                }
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Text(txt))) => {
                            if let Err(err) = handle_ws_message(&txt, &account_tx) {
                                let _ = log_tx.send(LogMessage::Warn(format!("[ACCT] parse err: {err:?}").into()));
                            }
                        }
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(_))) => {}
                        Some(Ok(_)) => {}
                        Some(Err(err)) => {
                            let _ = log_tx.send(LogMessage::Warn(format!("[ACCT] ws error: {err:?}").into()));
                            break;
                        }
                        None => break,
                    }
                }
            }
        }

        let _ = account_tx.send(AccountEvent::StreamClosed);
        let _ = log_tx.send(LogMessage::Warn("[ACCT] stream reconnecting".into()));
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn create_listen_key(client: &Client, base_url: &str, api_key: &str) -> anyhow::Result<String> {
    let url = format!("{}/api/v3/userDataStream", base_url);
    let response = client
        .post(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .context("create listen key request failed")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("listen key rejected status={status} body={body}"));
    }
    let payload: ListenKeyResponse = response
        .json()
        .await
        .context("decoding listen key response")?;
    Ok(payload.listen_key)
}

async fn send_keepalive(
    client: &Client,
    base_url: &str,
    api_key: &str,
    listen_key: &str,
) -> anyhow::Result<()> {
    let url = format!("{}/api/v3/userDataStream", base_url);
    let response = client
        .put(&url)
        .query(&[("listenKey", listen_key)])
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .context("keepalive request failed")?;
    if response.status().is_success() {
        Ok(())
    } else {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(anyhow!("keepalive rejected status={status} body={body}"))
    }
}

async fn fetch_account_snapshot(
    client: &Client,
    base_url: &str,
    api_key: &str,
    api_secret: &str,
    time_sync: &TimeSync,
) -> anyhow::Result<Vec<BalanceSnapshot>> {
    let timestamp_ms = time_sync.now_ms_synced();
    let payload = format!("timestamp={timestamp_ms}");
    let signature = sign_payload(&payload, api_secret);
    let url = format!("{}/api/v3/account?{}&signature={}", base_url, payload, signature);
    let response = client
        .get(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .context("account snapshot request failed")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("account snapshot rejected status={status} body={body}"));
    }
    let payload: AccountSnapshot = response
        .json()
        .await
        .context("decoding account snapshot")?;
    Ok(payload
        .balances
        .into_iter()
        .map(|bal| BalanceSnapshot {
            asset: bal.asset,
            free: parse_decimal(&bal.free),
            locked: parse_decimal(&bal.locked),
        })
        .collect())
}

async fn fetch_open_orders(
    client: &Client,
    base_url: &str,
    api_key: &str,
    api_secret: &str,
    time_sync: &TimeSync,
) -> anyhow::Result<Vec<OpenOrderSnapshot>> {
    let timestamp_ms = time_sync.now_ms_synced();
    let payload = format!("timestamp={timestamp_ms}");
    let signature = sign_payload(&payload, api_secret);
    let url = format!(
        "{}/api/v3/openOrders?{}&signature={}",
        base_url, payload, signature
    );
    let response = client
        .get(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .context("open orders request failed")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("open orders rejected status={status} body={body}"));
    }
    let payload: Vec<RestOpenOrder> = response
        .json()
        .await
        .context("decoding open orders")?;
    let mut orders = Vec::with_capacity(payload.len());
    for order in payload {
        if let Some(symbol) = Symbol::from_str(&order.symbol) {
            orders.push(OpenOrderSnapshot {
                client_order_id: order.client_order_id,
                symbol,
                side: order.side,
                status: order.status,
                price: parse_decimal(&order.price),
                orig_qty: parse_decimal(&order.orig_qty),
                executed_qty: parse_decimal(&order.executed_qty),
            });
        }
    }
    Ok(orders)
}

fn handle_ws_message(text: &str, account_tx: &Sender<AccountEvent>) -> anyhow::Result<()> {
    let value: Value = serde_json::from_str(text)?;
    let Some(event_type) = value.get("e").and_then(|v| v.as_str()) else {
        return Ok(());
    };
    match event_type {
        "executionReport" => {
            let report: WsExecutionReport = serde_json::from_value(value)?;
            if let Some(symbol) = Symbol::from_str(&report.symbol) {
                let event = AccountExecutionReport {
                    symbol,
                    side: report.side,
                    order_type: report.order_type,
                    status: report.order_status,
                    exec_type: report.exec_type,
                    client_order_id: report.client_order_id,
                    orig_client_order_id: report.orig_client_order_id,
                    order_id: report.order_id.map(|id| id.to_string()),
                    price: parse_decimal(&report.price),
                    orig_qty: parse_decimal(&report.orig_qty),
                    cum_qty: parse_decimal(&report.cum_qty),
                    last_qty: parse_decimal(&report.last_qty),
                    last_price: parse_decimal(&report.last_price),
                    commission_asset: report.commission_asset,
                    commission: report.commission.map(|c| parse_decimal(&c)),
                    reject_reason: report.reject_reason,
                };
                let _ = account_tx.send(AccountEvent::Execution(event));
            }
        }
        "outboundAccountPosition" => {
            let pos: WsOutboundAccountPosition = serde_json::from_value(value)?;
            let balances = pos
                .balances
                .into_iter()
                .map(|bal| BalanceSnapshot {
                    asset: bal.asset,
                    free: parse_decimal(&bal.free),
                    locked: parse_decimal(&bal.locked),
                })
                .collect();
            let _ = account_tx.send(AccountEvent::OutboundAccountPosition { balances });
        }
        "balanceUpdate" => {
            let upd: WsBalanceUpdate = serde_json::from_value(value)?;
            let delta = parse_decimal(&upd.delta);
            let _ = account_tx.send(AccountEvent::BalanceUpdate {
                asset: upd.asset,
                delta,
            });
        }
        _ => {}
    }
    Ok(())
}

fn parse_decimal(value: &str) -> Decimal {
    Decimal::from_str_exact(value).unwrap_or_else(|_| Decimal::from_str(value).unwrap_or_default())
}

fn sign_payload(payload: &str, secret: &str) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(payload.as_bytes());
    let result = mac.finalize();
    hex::encode(result.into_bytes())
}

#[derive(Deserialize)]
struct ListenKeyResponse {
    #[serde(rename = "listenKey")]
    listen_key: String,
}

#[derive(Deserialize)]
struct AccountSnapshot {
    #[serde(default)]
    balances: Vec<AccountBalance>,
}

#[derive(Deserialize)]
struct AccountBalance {
    #[serde(rename = "asset")]
    asset: String,
    #[serde(rename = "free")]
    free: String,
    #[serde(rename = "locked")]
    locked: String,
}

#[derive(Deserialize)]
struct RestOpenOrder {
    #[serde(rename = "symbol")]
    symbol: String,
    #[serde(rename = "side")]
    side: String,
    #[serde(rename = "status")]
    status: String,
    #[serde(rename = "price")]
    price: String,
    #[serde(rename = "origQty")]
    orig_qty: String,
    #[serde(rename = "executedQty")]
    executed_qty: String,
    #[serde(rename = "clientOrderId")]
    client_order_id: String,
}

#[derive(Deserialize)]
struct WsExecutionReport {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "S")]
    side: String,
    #[serde(rename = "o")]
    order_type: String,
    #[serde(rename = "x")]
    exec_type: String,
    #[serde(rename = "X")]
    order_status: String,
    #[serde(rename = "c")]
    client_order_id: String,
    #[serde(rename = "C")]
    orig_client_order_id: Option<String>,
    #[serde(rename = "i")]
    order_id: Option<u64>,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    orig_qty: String,
    #[serde(rename = "z")]
    cum_qty: String,
    #[serde(rename = "l")]
    last_qty: String,
    #[serde(rename = "L")]
    last_price: String,
    #[serde(rename = "n")]
    commission: Option<String>,
    #[serde(rename = "N")]
    commission_asset: Option<String>,
    #[serde(rename = "r")]
    reject_reason: Option<String>,
}

#[derive(Deserialize)]
struct WsOutboundAccountPosition {
    #[serde(rename = "B")]
    balances: Vec<WsBalance>,
}

#[derive(Deserialize)]
struct WsBalance {
    #[serde(rename = "a")]
    asset: String,
    #[serde(rename = "f")]
    free: String,
    #[serde(rename = "l")]
    locked: String,
}

#[derive(Deserialize)]
struct WsBalanceUpdate {
    #[serde(rename = "a")]
    asset: String,
    #[serde(rename = "d")]
    delta: String,
}
