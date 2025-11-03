use std::collections::HashMap;
#[cfg(not(feature = "test-mode"))]
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
#[cfg(not(feature = "test-mode"))]
use anyhow::Context;
use arc_swap::ArcSwap;
#[cfg(not(feature = "test-mode"))]
use reqwest::blocking::Client;
use rust_decimal::Decimal;
#[cfg(not(feature = "test-mode"))]
use serde::Deserialize;

use crate::types::{symbol_id_from_str, SymbolId};

#[derive(Clone, Copy, Debug)]
pub struct SymbolFilters {
    pub step: Decimal,
    pub tick: Decimal,
    pub min_notional: Decimal,
}

#[cfg(not(feature = "test-mode"))]
#[derive(Clone)]
pub struct FilterCache {
    inner: Arc<ArcSwap<HashMap<SymbolId, SymbolFilters>>>,
    client: Client,
    rest_base_url: &'static str,
}

#[cfg(feature = "test-mode")]
#[derive(Clone)]
pub struct FilterCache {
    inner: Arc<ArcSwap<HashMap<SymbolId, SymbolFilters>>>,
}

impl FilterCache {
    #[cfg(not(feature = "test-mode"))]
    pub fn new(
        seed: HashMap<SymbolId, SymbolFilters>,
        rest_base_url: &'static str,
    ) -> anyhow::Result<Self> {
        let client = Client::builder()
            .user_agent("sbe-latency-bot/filter-cache")
            .tcp_nodelay(true)
            .build()
            .context("building filter cache HTTP client")?;
        Ok(Self {
            inner: Arc::new(ArcSwap::from_pointee(seed)),
            client,
            rest_base_url,
        })
    }

    #[cfg(feature = "test-mode")]
    pub fn new(
        seed: HashMap<SymbolId, SymbolFilters>,
        rest_base_url: &'static str,
    ) -> anyhow::Result<Self> {
        let _ = rest_base_url;
        Ok(Self {
            inner: Arc::new(ArcSwap::from_pointee(seed)),
        })
    }

    pub fn get(&self, id: SymbolId) -> Option<SymbolFilters> {
        self.inner.load_full().get(&id).copied()
    }

    pub fn install(&self, snapshot: HashMap<SymbolId, SymbolFilters>) {
        self.inner.store(Arc::new(snapshot));
    }

    #[cfg(not(feature = "test-mode"))]
    pub fn refresh_symbol(&self, sym: &str) -> anyhow::Result<SymbolFilters> {
        let url = format!("{}/api/v3/exchangeInfo?symbol={}", self.rest_base_url, sym);
        let response = self
            .client
            .get(&url)
            .send()
            .with_context(|| format!("refreshing filters for {sym}"))?;
        let status = response.status();
        let response = response
            .error_for_status()
            .with_context(|| format!("filter refresh failed: status={status} symbol={sym}"))?;
        let payload: ExchangeInfoResponse = response
            .json()
            .with_context(|| format!("decoding filter response for {sym}"))?;

        let info = payload
            .symbols
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("symbol {sym} not found in filter response"))?;

        let filters = extract_symbol_filters(&info.filters)?;
        let symbol_id = symbol_id_from_str(&info.symbol);

        let current = self.inner.load_full();
        let mut next = (*current).clone();
        next.insert(symbol_id, filters);
        self.inner.store(Arc::new(next));

        Ok(filters)
    }

    #[cfg(feature = "test-mode")]
    pub fn refresh_symbol(&self, sym: &str) -> anyhow::Result<SymbolFilters> {
        let current = self.inner.load_full();
        let sym_id = symbol_id_from_str(sym);
        let filters = current
            .get(&sym_id)
            .copied()
            .or_else(|| current.values().copied().next())
            .ok_or_else(|| anyhow!("no filters available for symbol {sym}"))?;
        let mut next = (*current).clone();
        next.insert(sym_id, filters);
        self.inner.store(Arc::new(next));
        Ok(filters)
    }
}

#[cfg(not(feature = "test-mode"))]
#[derive(Debug, Deserialize)]
struct ExchangeInfoResponse {
    #[serde(default)]
    symbols: Vec<SymbolInfo>,
}

#[cfg(not(feature = "test-mode"))]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SymbolInfo {
    symbol: String,
    #[serde(default)]
    filters: Vec<SymbolFilter>,
}

#[cfg(not(feature = "test-mode"))]
#[derive(Debug, Deserialize)]
#[serde(tag = "filterType", rename_all = "SCREAMING_SNAKE_CASE")]
enum SymbolFilter {
    PriceFilter {
        #[serde(default)]
        tick_size: String,
    },
    LotSize {
        #[serde(default)]
        step_size: String,
    },
    MinNotional {
        #[serde(default)]
        min_notional: String,
    },
    #[serde(other)]
    Other,
}

#[cfg(not(feature = "test-mode"))]
fn extract_symbol_filters(filters: &[SymbolFilter]) -> anyhow::Result<SymbolFilters> {
    let mut step = None;
    let mut tick = None;
    let mut min_notional = None;

    for filter in filters {
        match filter {
            SymbolFilter::PriceFilter { tick_size } => {
                if tick.is_none() {
                    tick = Decimal::from_str(tick_size).ok();
                }
            }
            SymbolFilter::LotSize { step_size } => {
                if step.is_none() {
                    step = Decimal::from_str(step_size).ok();
                }
            }
            SymbolFilter::MinNotional {
                min_notional: value,
            } => {
                if min_notional.is_none() {
                    min_notional = Decimal::from_str(value).ok();
                }
            }
            SymbolFilter::Other => {}
        }
    }

    match (step, tick, min_notional) {
        (Some(step), Some(tick), Some(min_notional))
            if step > Decimal::ZERO && tick > Decimal::ZERO =>
        {
            Ok(SymbolFilters {
                step,
                tick,
                min_notional,
            })
        }
        _ => Err(anyhow!(
            "missing filters: step={step:?} tick={tick:?} min_notional={min_notional:?}"
        )),
    }
}
