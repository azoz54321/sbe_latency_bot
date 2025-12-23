use arc_swap::ArcSwap;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use chrono::{DateTime, FixedOffset, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::{info, warn};

use crate::clock::Clock;
use crate::config::Config;
use crate::filters::SymbolFilters;
use crate::types::{symbol_id_from_str, Symbol, SymbolId};

const NEW_LISTING_CUTOFF: Duration = Duration::from_secs(60 * 60);

const LEVERAGED_SUFFIXES: &[&str] = &["UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT", "HALFUSDT"];

const STABLE_BASES: &[&str] = &["USDT", "USDC", "BUSD", "FDUSD", "TUSD", "DAI"];

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SymbolKey {
    pub name: String,
    pub symbol_id: u32,
}

pub fn load_haram_list<P: AsRef<Path>>(path: P) -> BTreeSet<String> {
    let path_ref = path.as_ref();
    match fs::read_to_string(path_ref) {
        Ok(text) => {
            let mut set = BTreeSet::new();
            for line in text.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with('#') {
                    continue;
                }
                set.insert(trimmed.to_ascii_uppercase());
            }
            set
        }
        Err(err) => {
            warn!(
                ?path_ref,
                %err,
                "[HARAM] file not found or unreadable; proceeding with empty set"
            );
            BTreeSet::new()
        }
    }
}

pub async fn get_usdt_spot_universe(
    config: &'static Config,
    haram_external: Option<&BTreeSet<String>>,
) -> anyhow::Result<Vec<SymbolKey>> {
    let client = Client::builder()
        .user_agent("sbe-latency-bot/2.0")
        .tcp_nodelay(true)
        .build()
        .context("building universe HTTP client")?;

    let url = format!("{}/api/v3/exchangeInfo", config.transport.rest_base_url);
    let payload = client
        .get(&url)
        .send()
        .await
        .context("fetching exchangeInfo for USDT universe")?
        .error_for_status()
        .context("exchangeInfo HTTP status")?
        .json::<ExInfo>()
        .await
        .context("decoding exchangeInfo payload")?;

    let haram_config: HashSet<String> = config
        .strategy
        .haram_symbols
        .iter()
        .map(|s| s.to_ascii_uppercase())
        .collect();
    let haram_external = haram_external.map(|set| set.clone());

    let mut entries = Vec::with_capacity(payload.symbols.len());
    for info in payload.symbols.into_iter() {
        if info.status != "TRADING" {
            continue;
        }
        if info.quote_asset != "USDT" {
            continue;
        }
        if !(info.is_spot_trading_allowed
            || info
                .permissions
                .iter()
                .any(|perm| perm.eq_ignore_ascii_case("SPOT")))
        {
            continue;
        }
        let symbol_upper = info.symbol.to_ascii_uppercase();
        let is_haram = haram_config.contains(&symbol_upper)
            || haram_external
                .as_ref()
                .map(|set| set.contains(&symbol_upper))
                .unwrap_or(false);
        if is_haram {
            continue;
        }
        if LEVERAGED_SUFFIXES
            .iter()
            .any(|suffix| symbol_upper.ends_with(suffix))
        {
            continue;
        }
        if symbol_upper.ends_with("UP")
            || symbol_upper.ends_with("DOWN")
            || symbol_upper.ends_with("BULL")
            || symbol_upper.ends_with("BEAR")
        {
            continue;
        }

        let symbol_id = derive_symbol_key_id(&symbol_upper);
        entries.push(SymbolKey {
            name: symbol_upper,
            symbol_id,
        });
    }

    entries.sort_by(|a, b| a.name.cmp(&b.name));
    info!("[BOOT] universe size={}", entries.len());
    Ok(entries)
}

fn derive_symbol_key_id(symbol: &str) -> u32 {
    symbol_id_from_str(symbol) as u32
}

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

    let url = format!("{}/api/v3/exchangeInfo", config.transport.rest_base_url);

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

        if config
            .strategy
            .haram_symbols
            .iter()
            .any(|sym| sym.eq_ignore_ascii_case(&symbol_info.symbol))
        {
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
struct ExInfo {
    #[serde(default)]
    symbols: Vec<ExSymbol>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExSymbol {
    symbol: String,
    status: String,
    quote_asset: String,
    #[serde(default)]
    is_spot_trading_allowed: bool,
    #[serde(default)]
    permissions: Vec<String>,
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

pub async fn count_usdt_spot_symbols(
    client: &Client,
    rest_base_url: &str,
    haram: &[String],
) -> anyhow::Result<usize> {
    let url = format!("{}/api/v3/exchangeInfo", rest_base_url);
    let ex = client
        .get(&url)
        .send()
        .await?
        .error_for_status()?
        .json::<ExInfo>()
        .await?;

    let haram_set: HashSet<String> = haram.iter().map(|s| s.to_ascii_uppercase()).collect();

    let count = ex
        .symbols
        .into_iter()
        .filter(|s| {
            if s.status != "TRADING" || s.quote_asset != "USDT" {
                return false;
            }
            if !(s.is_spot_trading_allowed || s.permissions.iter().any(|p| p == "SPOT")) {
                return false;
            }
            if LEVERAGED_SUFFIXES
                .iter()
                .any(|suffix| s.symbol.ends_with(suffix))
            {
                return false;
            }
            let symbol_upper = s.symbol.to_ascii_uppercase();
            if haram_set.contains(&symbol_upper) {
                return false;
            }
            true
        })
        .count();

    Ok(count)
}
