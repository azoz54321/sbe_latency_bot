use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use chrono::{DateTime, Duration, FixedOffset, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::clock::Clock;
use crate::config::Config;
use crate::types::{symbol_id, Symbol, SymbolId};

const LOSS_BAN_WINDOW_DAYS: i64 = 30;
const DAILY_FREEZE_THRESHOLD: f64 = -0.10;

#[derive(Copy, Clone, Debug)]
pub enum DisableReason {
    OrderReject,
    FatalError,
}

#[derive(Clone)]
pub struct RiskHandle {
    config: &'static Config,
    clock: Arc<dyn Clock>,
    table: Arc<DisableTable>,
}

impl RiskHandle {
    fn new(config: &'static Config, clock: Arc<dyn Clock>, table: Arc<DisableTable>) -> Self {
        Self {
            config,
            clock,
            table,
        }
    }

    pub fn disable_for_today(&self, sym: SymbolId, reason: DisableReason) {
        let now = self.config.ksa_now(self.clock.as_ref());
        let until = self.config.next_reset_time_ksa(now);
        self.table.insert(sym, until, reason);
    }

    #[cfg(feature = "test-mode")]
    pub fn clear_disables_for_tests(&self) {
        self.table.clear_all_for_tests();
    }
}

pub struct RiskEngine {
    config: &'static Config,
    clock: Arc<dyn Clock>,
    daily_pnl: f64,
    frozen: bool,
    no_rebuy_until: HashMap<Symbol, DateTime<FixedOffset>>,
    penalties: HashMap<String, PenaltyRecord>,
    penalties_path: PathBuf,
    disable_table: Arc<DisableTable>,
}

impl RiskEngine {
    pub fn new(
        config: &'static Config,
        clock: Arc<dyn Clock>,
    ) -> anyhow::Result<(Self, RiskHandle)> {
        let penalties_path = PathBuf::from("data/penalties.json");
        let penalties = load_penalties(&penalties_path)?;
        let disable_table = Arc::new(DisableTable::default());

        let engine = Self {
            config,
            clock: clock.clone(),
            daily_pnl: 0.0,
            frozen: false,
            no_rebuy_until: HashMap::new(),
            penalties,
            penalties_path,
            disable_table: disable_table.clone(),
        };
        let handle = RiskHandle::new(config, clock, disable_table);

        Ok((engine, handle))
    }

    pub fn can_trade(&self, symbol: Symbol, now: DateTime<FixedOffset>) -> bool {
        if self.frozen {
            return false;
        }

        if let Some(until) = self.no_rebuy_until.get(&symbol) {
            if now < *until {
                return false;
            }
        }

        if self
            .disable_table
            .is_active(symbol_id(symbol), now)
            .is_some()
        {
            return false;
        }

        let key = symbol.to_string();
        if let Some(record) = self.penalties.get(&key) {
            if let Some(until) = record.banned_until {
                if now.with_timezone(&Utc) < until {
                    return false;
                }
            }
        }

        true
    }

    pub fn mark_trade_open(&mut self, symbol: Symbol, now: DateTime<FixedOffset>) {
        let next_reset = self.config.next_reset_time_ksa(now);
        self.no_rebuy_until.insert(symbol, next_reset);
    }

    pub fn mark_trade_close(
        &mut self,
        symbol: Symbol,
        pnl_pct: f64,
        now: DateTime<FixedOffset>,
    ) -> anyhow::Result<()> {
        self.daily_pnl += pnl_pct;
        if self.daily_pnl <= DAILY_FREEZE_THRESHOLD {
            self.frozen = true;
        }

        if pnl_pct < 0.0 {
            self.register_loss(symbol, now)?;
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub fn reset_daily_counters_keep_long_bans(&mut self) {
        self.daily_pnl = 0.0;
        self.frozen = false;
        self.no_rebuy_until.clear();
        let now = self.config.ksa_now(self.clock.as_ref());
        self.disable_table.clear_expired(now);
    }

    fn register_loss(&mut self, symbol: Symbol, now: DateTime<FixedOffset>) -> anyhow::Result<()> {
        let key = symbol.to_string();
        let record = self
            .penalties
            .entry(key.clone())
            .or_insert_with(|| PenaltyRecord {
                losses: Vec::with_capacity(4),
                banned_until: None,
            });

        let now_utc = now.with_timezone(&Utc);
        record.losses.push(now_utc);
        let cutoff = now_utc - Duration::days(LOSS_BAN_WINDOW_DAYS);
        record.losses.retain(|ts| *ts >= cutoff);

        if record.losses.len() >= 3 {
            record.banned_until = Some(now_utc + Duration::days(LOSS_BAN_WINDOW_DAYS));
        }

        self.persist_penalties()
            .with_context(|| "failed to persist penalties")?;
        Ok(())
    }

    fn persist_penalties(&self) -> anyhow::Result<()> {
        ensure_directory(&self.penalties_path)?;
        let data = serde_json::to_vec_pretty(&self.penalties)?;
        fs::write(&self.penalties_path, data)?;
        Ok(())
    }
}

fn ensure_directory(path: &Path) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)?;
        }
    }
    Ok(())
}

#[cfg(not(feature = "test-mode"))]
fn load_penalties(path: &Path) -> anyhow::Result<HashMap<String, PenaltyRecord>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let bytes = fs::read(path)?;
    let penalties = serde_json::from_slice(&bytes)?;
    Ok(penalties)
}

#[cfg(feature = "test-mode")]
fn load_penalties(_path: &Path) -> anyhow::Result<HashMap<String, PenaltyRecord>> {
    Ok(HashMap::new())
}

#[cfg(feature = "test-mode")]
impl RiskEngine {
    pub fn snapshot(&self) -> RiskSnapshot {
        RiskSnapshot {
            daily_pnl: self.daily_pnl,
            frozen: self.frozen,
            disabled: self.disable_table.snapshot(),
            no_rebuy: self.no_rebuy_until.keys().copied().collect(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PenaltyRecord {
    #[serde(default)]
    losses: Vec<DateTime<Utc>>,
    #[serde(default)]
    banned_until: Option<DateTime<Utc>>,
}

#[derive(Debug, Default)]
struct DisableTable {
    entries: DashMap<SymbolId, DisableEntry>,
}

impl DisableTable {
    fn insert(&self, symbol_id: SymbolId, until: DateTime<FixedOffset>, reason: DisableReason) {
        self.entries
            .insert(symbol_id, DisableEntry { until, reason });
    }

    fn is_active(&self, symbol_id: SymbolId, now: DateTime<FixedOffset>) -> Option<DisableReason> {
        if let Some(entry) = self.entries.get(&symbol_id) {
            if now < entry.until {
                return Some(entry.reason);
            }
        }
        if let Some(entry) = self.entries.get(&symbol_id) {
            if now >= entry.until {
                drop(entry);
                self.entries.remove(&symbol_id);
            }
        }
        None
    }

    fn clear_expired(&self, now: DateTime<FixedOffset>) {
        let expired: Vec<_> = self
            .entries
            .iter()
            .filter_map(|entry| {
                if now >= entry.until {
                    Some(*entry.key())
                } else {
                    None
                }
            })
            .collect();
        for key in expired {
            self.entries.remove(&key);
        }
    }

    #[cfg(feature = "test-mode")]
    fn clear_all_for_tests(&self) {
        self.entries.clear();
    }

    #[cfg(feature = "test-mode")]
    fn snapshot(&self) -> Vec<RiskDisabledEntry> {
        self.entries
            .iter()
            .map(|entry| RiskDisabledEntry {
                symbol_id: *entry.key(),
                reason: entry.value().reason,
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug)]
struct DisableEntry {
    until: DateTime<FixedOffset>,
    reason: DisableReason,
}

#[cfg(feature = "test-mode")]
#[derive(Clone, Debug, Default)]
pub struct RiskSnapshot {
    pub daily_pnl: f64,
    pub frozen: bool,
    pub disabled: Vec<RiskDisabledEntry>,
    pub no_rebuy: Vec<Symbol>,
}

#[cfg(feature = "test-mode")]
#[derive(Clone, Copy, Debug)]
pub struct RiskDisabledEntry {
    pub symbol_id: SymbolId,
    pub reason: DisableReason,
}
