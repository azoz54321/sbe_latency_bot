use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use reqwest::blocking::Client as BlockingClient;
use reqwest::Client;
use serde::Deserialize;
use tokio::time::sleep;

#[derive(Clone)]
pub struct TimeSync {
    offset_ms: Arc<AtomicI64>,
    rest_base_url: &'static str,
    client_blocking: BlockingClient,
    client_async: Client,
}

impl TimeSync {
    pub fn new(rest_base_url: &'static str) -> anyhow::Result<Self> {
        Ok(Self {
            offset_ms: Arc::new(AtomicI64::new(0)),
            rest_base_url,
            client_blocking: BlockingClient::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .context("building blocking timesync client")?,
            client_async: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .context("building async timesync client")?,
        })
    }

    #[inline]
    pub fn now_ms_synced(&self) -> u64 {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let offset = self.offset_ms.load(Ordering::Relaxed);
        now_ms.saturating_add(offset).max(0) as u64
    }

    pub fn resync_blocking(&self) -> anyhow::Result<()> {
        let url = format!("{}/api/v3/time", self.rest_base_url);
        let resp = self
            .client_blocking
            .get(&url)
            .send()
            .context("timesync GET (blocking)")?
            .error_for_status()
            .context("timesync status (blocking)")?;
        let payload: ServerTime = resp.json().context("decode server time (blocking)")?;
        let now_ms = chrono::Utc::now().timestamp_millis();
        let offset = payload.server_time as i64 - now_ms;
        self.offset_ms.store(offset, Ordering::Relaxed);
        Ok(())
    }

    pub async fn resync_async(&self) -> anyhow::Result<()> {
        let url = format!("{}/api/v3/time", self.rest_base_url);
        let resp = self
            .client_async
            .get(&url)
            .send()
            .await
            .context("timesync GET (async)")?
            .error_for_status()
            .context("timesync status (async)")?;
        let payload: ServerTime = resp.json().await.context("decode server time (async)")?;
        let now_ms = chrono::Utc::now().timestamp_millis();
        let offset = payload.server_time as i64 - now_ms;
        self.offset_ms.store(offset, Ordering::Relaxed);
        Ok(())
    }

    pub fn spawn_poll(self: Arc<Self>, period: Duration) {
        tokio::spawn(async move {
            loop {
                let _ = self.resync_async().await;
                sleep(period).await;
            }
        });
    }
}

#[derive(Deserialize)]
struct ServerTime {
    #[serde(rename = "serverTime")]
    server_time: u64,
}
