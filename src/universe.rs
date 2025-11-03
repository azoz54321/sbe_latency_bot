use arc_swap::ArcSwap;

use std::collections::HashMap;

use std::str::FromStr;

use std::sync::Arc;

use std::time::Duration;

use anyhow::{anyhow, Context};

use chrono::{DateTime, FixedOffset, Utc};

use reqwest::Client;

use rust_decimal::Decimal;

use serde::Deserialize;

use crate::clock::Clock;

use crate::config::Config;

use crate::filters::SymbolFilters;

use crate::types::{symbol_id_from_str, Symbol, SymbolId};

const NEW_LISTING_CUTOFF: Duration = Duration::from_secs(60 * 60);

const LEVERAGED_SUFFIXES: &[&str] = &["UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT", "HALFUSDT"];

const STABLE_BASES: &[&str] = &["USDT", "USDC", "BUSD", "FDUSD", "TUSD", "DAI"];

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SymbolMeta {
    pub symbol: Symbol,

    pub symbol_str: String,

    pub base: String,

    pub quote: String,

    pub symbol_id: SymbolId,

    pub onboard_date: Option<DateTime<Utc>>,

    pub filters: SymbolFilters,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct UniverseShard {
    pub index: usize,

    pub members: Vec<Symbol>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct Universe {
    pub generated_at: DateTime<FixedOffset>,

    pub symbols: Vec<SymbolMeta>,

    pub symbol_index: HashMap<Symbol, usize>,

    pub shards: Vec<UniverseShard>,
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct UniverseHandle {
    inner: Arc<ArcSwap<Universe>>,
}

#[allow(dead_code)]
impl UniverseHandle {
    pub fn new(universe: Universe) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(universe)),
        }
    }

    pub fn get(&self) -> Arc<Universe> {
        self.inner.load_full()
    }

    pub fn install(&self, universe: Universe) {
        self.inner.store(Arc::new(universe));
    }
}

impl Universe {
    #[allow(dead_code)]
    pub fn symbol_meta(&self, symbol: Symbol) -> Option<&SymbolMeta> {
        self.symbol_index
            .get(&symbol)
            .and_then(|idx| self.symbols.get(*idx))
    }

    pub fn filter_seed(&self) -> HashMap<SymbolId, SymbolFilters> {
        let mut map = HashMap::with_capacity(self.symbols.len());

        for meta in &self.symbols {
            map.insert(meta.symbol_id, meta.filters);
        }

        map
    }
}

pub async fn build_universe(
    config: &'static Config,

    clock: &dyn Clock,
) -> anyhow::Result<Universe> {
    let client = Client::builder()
        .user_agent("sbe-latency-bot/2.0")
        .tcp_nodelay(true)
        .build()
        .context("building universe HTTP client")?;

    let url = format!(
        "{}/api/v3/exchangeInfo?permissions=SPOT",
        config.transport.rest_base_url
    );

    let response = client
        .get(&url)
        .send()
        .await
        .context("fetching exchangeInfo")?;

    if !response.status().is_success() {
        return Err(anyhow!(
            "exchangeInfo request failed: {}",
            response.status()
        ));
    }

    let data: ExchangeInfoResponse = response
        .json()
        .await
        .context("decoding exchangeInfo response")?;

    let now_utc = clock.now_utc();

    let mut metas = Vec::new();

    for symbol_info in data.symbols.into_iter() {
        if !is_candidate(&symbol_info) {
            continue;
        }

        if symbol_info.quote_asset != "USDT" {
            continue;
        }

        if symbol_info.base_asset.eq_ignore_ascii_case("BTC") {
            continue;
        }

        if STABLE_BASES
            .iter()
            .any(|stable| symbol_info.base_asset.eq_ignore_ascii_case(stable))
        {
            continue;
        }

        if LEVERAGED_SUFFIXES
            .iter()
            .any(|suffix| symbol_info.symbol.ends_with(suffix))
        {
            continue;
        }

        if is_new_listing(&symbol_info, now_utc) {
            continue;
        }

        let Some(symbol) = Symbol::from_str(&symbol_info.symbol) else {
            continue;
        };

        let Some(filters) = parse_symbol_filters(&symbol_info) else {
            continue;
        };

        let symbol_id = symbol_id_from_str(&symbol_info.symbol);

        metas.push(SymbolMeta {
            symbol,

            symbol_str: symbol_info.symbol,

            base: symbol_info.base_asset,

            quote: symbol_info.quote_asset,

            symbol_id,

            onboard_date: symbol_info
                .onboard_date
                .map(|ms| DateTime::from_timestamp_millis(ms).unwrap_or(now_utc)),

            filters,
        });
    }

    metas.sort_by(|a, b| a.symbol_str.cmp(&b.symbol_str));

    let generated_at = config.ksa_now(clock);

    let mut symbol_index = HashMap::with_capacity(metas.len());

    for (idx, meta) in metas.iter().enumerate() {
        symbol_index.insert(meta.symbol, idx);
    }

    let shards = partition_shards(&metas, config.sharding.shard_size);

    Ok(Universe {
        generated_at,

        symbols: metas,

        symbol_index,

        shards,
    })
}

fn is_candidate(symbol: &SymbolInfo) -> bool {
    symbol.status == "TRADING" && symbol.is_spot_trading_allowed
}

fn is_new_listing(symbol: &SymbolInfo, now: DateTime<Utc>) -> bool {
    let onboard = match symbol.onboard_date {
        Some(ms) => match DateTime::from_timestamp_millis(ms) {
            Some(ts) => ts,

            None => return false,
        },

        None => return false,
    };

    (now - onboard)
        < chrono::Duration::from_std(NEW_LISTING_CUTOFF)
            .unwrap_or_else(|_| chrono::Duration::minutes(60))
}

fn partition_shards(metas: &[SymbolMeta], shard_size: usize) -> Vec<UniverseShard> {
    if shard_size == 0 {
        return vec![UniverseShard {
            index: 0,

            members: metas.iter().map(|meta| meta.symbol).collect(),
        }];
    }

    metas
        .chunks(shard_size)
        .enumerate()
        .map(|(idx, chunk)| UniverseShard {
            index: idx,

            members: chunk.iter().map(|meta| meta.symbol).collect(),
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct ExchangeInfoResponse {
    #[serde(default)]
    symbols: Vec<SymbolInfo>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SymbolInfo {
    symbol: String,
    status: String,
    base_asset: String,
    quote_asset: String,
    #[serde(default)]
    is_spot_trading_allowed: bool,
    #[serde(default)]
    onboard_date: Option<i64>,
    #[serde(default)]
    filters: Vec<SymbolFilter>,
}

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

fn parse_symbol_filters(info: &SymbolInfo) -> Option<SymbolFilters> {
    let mut step = None;

    let mut tick = None;

    let mut min_notional = None;

    for filter in &info.filters {
        match filter {
            SymbolFilter::PriceFilter { tick_size } => {
                tick = Decimal::from_str(tick_size).ok();
            }

            SymbolFilter::LotSize { step_size } => {
                step = Decimal::from_str(step_size).ok();
            }

            SymbolFilter::MinNotional {
                min_notional: value,
            } => {
                min_notional = Decimal::from_str(value).ok();
            }

            SymbolFilter::Other => {}
        }
    }

    match (step, tick, min_notional) {
        (Some(step), Some(tick), Some(min_notional))
            if step > Decimal::ZERO && tick > Decimal::ZERO =>
        {
            Some(SymbolFilters {
                step,

                tick,

                min_notional,
            })
        }

        _ => None,
    }
}
