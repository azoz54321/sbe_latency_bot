use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Context};
use crossbeam_channel::Sender;
use reqwest::blocking::Client;
use reqwest::StatusCode;
use serde::Deserialize;

use crate::config::Config;
use crate::types::LogMessage;

const USER_STREAM_ENDPOINT: &str = "/api/v3/userDataStream";
const MIN_KEEPALIVE_SECS: u64 = 60;

pub struct UserStreamService {
    #[allow(dead_code)]
    handle: thread::JoinHandle<()>,
}

impl UserStreamService {
    pub fn maybe_spawn(config: &'static Config, log_tx: Sender<LogMessage>) -> Option<Self> {
        if !config.user_stream.enabled {
            return None;
        }

        let Some(initial_key) = config.user_stream.listen_key else {
            let _ = log_tx.send(LogMessage::Warn(
                "[USER_STREAM] enabled but listen_key missing; keepalive disabled".into(),
            ));
            return None;
        };

        let base_url = trim_base(config.transport.rest_base_url);
        let api_key = config.credentials.rest_api_key.to_string();
        let keepalive_secs = config.user_stream.keepalive_secs.max(MIN_KEEPALIVE_SECS);
        let log_tx_clone = log_tx.clone();
        let initial_key = initial_key.to_string();

        match thread::Builder::new()
            .name("user-stream-keepalive".to_string())
            .spawn(move || {
                run_loop(base_url, api_key, initial_key, keepalive_secs, log_tx_clone);
            }) {
            Ok(handle) => Some(Self { handle }),
            Err(err) => {
                let _ = log_tx.send(LogMessage::Error(format!(
                    "[USER_STREAM] failed to spawn keepalive thread: {err}"
                )));
                None
            }
        }
    }
}

fn run_loop(
    base_url: String,
    api_key: String,
    mut listen_key: String,
    keepalive_secs: u64,
    log_tx: Sender<LogMessage>,
) {
    let client = match Client::builder().timeout(Duration::from_secs(10)).build() {
        Ok(client) => client,
        Err(err) => {
            let _ = log_tx.send(LogMessage::Error(format!(
                "[USER_STREAM] failed to build HTTP client: {err}"
            )));
            return;
        }
    };

    let interval = Duration::from_secs(keepalive_secs);
    loop {
        thread::sleep(interval);
        match send_keepalive(&client, &base_url, &api_key, &listen_key) {
            Ok(()) => {}
            Err(err) => {
                let _ = log_tx.send(LogMessage::Warn(
                    format!("[USER_STREAM] keepalive failed: {err:?}, renewing listen key").into(),
                ));
                match create_listen_key(&client, &base_url, &api_key) {
                    Ok(new_key) => {
                        listen_key = new_key;
                        tracing::debug!("[USER_STREAM] obtained fresh listen key");
                    }
                    Err(renew_err) => {
                        let _ = log_tx.send(LogMessage::Error(format!(
                            "[USER_STREAM] listen key renewal failed: {renew_err:?}"
                        )));
                    }
                }
            }
        }
    }
}

fn send_keepalive(
    client: &Client,
    base_url: &str,
    api_key: &str,
    listen_key: &str,
) -> anyhow::Result<()> {
    let url = format!("{base_url}{USER_STREAM_ENDPOINT}");
    let response = client
        .put(&url)
        .query(&[("listenKey", listen_key)])
        .header("X-MBX-APIKEY", api_key)
        .send()
        .with_context(|| "user stream keepalive request failed")?;

    if response.status().is_success() {
        Ok(())
    } else {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        Err(anyhow!("keepalive rejected status={status} body={body}"))
    }
}

fn create_listen_key(client: &Client, base_url: &str, api_key: &str) -> anyhow::Result<String> {
    let url = format!("{base_url}{USER_STREAM_ENDPOINT}");
    let response = client
        .post(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .with_context(|| "user stream create request failed")?;

    if response.status() == StatusCode::OK {
        let payload: ListenKeyResponse = response
            .json()
            .with_context(|| "failed to parse listen key response")?;
        Ok(payload.listen_key)
    } else {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        Err(anyhow!(
            "create listen key rejected status={status} body={body}"
        ))
    }
}

#[derive(Deserialize)]
struct ListenKeyResponse {
    #[serde(rename = "listenKey")]
    listen_key: String,
}

fn trim_base(base: &str) -> String {
    base.trim_end_matches('/').to_string()
}
