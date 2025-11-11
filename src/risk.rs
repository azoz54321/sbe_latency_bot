use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, FixedOffset, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::clock::Clock;
use crate::config::Config;
use crate::types::{symbol_id, Symbol, SymbolId};

use rust_decimal::Decimal;

const ZERO_DEC: Decimal = Decimal::ZERO;

#[derive(Copy, Clone, Debug)]
pub enum DisableReason {
    OrderReject,
    FatalError,
    DailyLoss,
}

#[derive(Debug)]
pub enum TradeBlock {
    GlobalFreeze {
        first: bool,
    },
    NoRebuyUntil {
        until: DateTime<FixedOffset>,
        first: bool,
    },
    Disabled {
        reason: DisableReason,
    },
    Banned {
        until: DateTime<Utc>,
        first: bool,
    },
}

#[derive(Default)]
pub struct RiskEffects {
    pub freeze_triggered: bool,
    pub banned_until: Option<DateTime<Utc>>,
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
}

pub struct RiskEngine {
    config: &'static Config,
    clock: Arc<dyn Clock>,
    daily_pnl: Decimal,
    frozen: bool,
    freeze_logged: bool,
    no_rebuy_until: HashMap<Symbol, DateTime<FixedOffset>>,
    no_rebuy_logged: HashSet<Symbol>,
    penalties: HashMap<String, PenaltyRecord>,
    penalties_path: PathBuf,
    disable_table: Arc<DisableTable>,
    ban_logged: HashSet<Symbol>,
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
            daily_pnl: ZERO_DEC,
            frozen: false,
            freeze_logged: false,
            no_rebuy_until: HashMap::new(),
            no_rebuy_logged: HashSet::new(),
            penalties,
            penalties_path,
            disable_table: disable_table.clone(),
            ban_logged: HashSet::new(),
        };
        let handle = RiskHandle::new(config, clock, disable_table);

        Ok((engine, handle))
    }

    pub fn evaluate_trade(
        &mut self,
        symbol: Symbol,
        now: DateTime<FixedOffset>,
    ) -> Result<Option<TradeBlock>> {
        if self.frozen {
            let first = !self.freeze_logged;
            if first {
                self.freeze_logged = true;
            }
            return Ok(Some(TradeBlock::GlobalFreeze { first }));
        }

        if let Some(until) = self.no_rebuy_until.get(&symbol).copied() {
            if now < until {
                let first = self.no_rebuy_logged.insert(symbol);
                return Ok(Some(TradeBlock::NoRebuyUntil { until, first }));
            } else {
                self.no_rebuy_until.remove(&symbol);
                self.no_rebuy_logged.remove(&symbol);
            }
        }

        if let Some(reason) = self.disable_table.is_active(symbol_id(symbol), now) {
            return Ok(Some(TradeBlock::Disabled { reason }));
        }

        let key = symbol.to_string();
        if let Some(record) = self.penalties.get_mut(&key) {
            if let Some(until) = record.banned_until {
                let now_utc = now.with_timezone(&Utc);
                if now_utc < until {
                    let first = self.ban_logged.insert(symbol);
                    return Ok(Some(TradeBlock::Banned { until, first }));
                }
                record.banned_until = None;
                self.ban_logged.remove(&symbol);
                self.persist_penalties()
                    .with_context(|| "failed to persist penalties")?;
            }
        }

        Ok(None)
    }

    pub fn mark_trade_open(&mut self, symbol: Symbol, now: DateTime<FixedOffset>) {
        let next_reset = self.config.next_reset_time_ksa(now);
        self.no_rebuy_until.insert(symbol, next_reset);
        self.no_rebuy_logged.remove(&symbol);
    }

    pub fn mark_trade_close(
        &mut self,
        symbol: Symbol,
        pnl_return: Decimal,
        is_loss: bool,
        now: DateTime<FixedOffset>,
    ) -> anyhow::Result<RiskEffects> {
        self.daily_pnl += pnl_return;
        let freeze_threshold = self.config.strategy.daily_loss_freeze_pct;
        let mut effects = RiskEffects::default();

        if !self.frozen && self.daily_pnl <= -freeze_threshold {
            self.frozen = true;
            self.freeze_logged = true;
            effects.freeze_triggered = true;
        }

        if is_loss {
            if let Some(until) = self.register_loss(symbol, now)? {
                effects.banned_until = Some(until);
                self.ban_logged.insert(symbol);
            }
        }

        Ok(effects)
    }

    #[allow(dead_code)]
    pub fn reset_daily_counters_keep_long_bans(&mut self) {
        self.daily_pnl = ZERO_DEC;
        self.frozen = false;
        self.freeze_logged = false;
        self.no_rebuy_until.clear();
        self.no_rebuy_logged.clear();
        self.ban_logged.clear();
        let now = self.config.ksa_now(self.clock.as_ref());
        self.disable_table.clear_expired(now);
    }

    fn register_loss(
        &mut self,
        symbol: Symbol,
        now: DateTime<FixedOffset>,
    ) -> anyhow::Result<Option<DateTime<Utc>>> {
        let threshold = self.config.strategy.ban_losses_threshold;
        if threshold == 0 {
            return Ok(None);
        }

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
        let window_days = self.config.strategy.ban_window_days as i64;
        let cutoff = now_utc - Duration::days(window_days);
        record.losses.retain(|ts| *ts >= cutoff);

        if (record.losses.len() as u32) >= threshold {
            let until = now_utc + Duration::days(window_days);
            record.banned_until = Some(until);
            self.persist_penalties()
                .with_context(|| "failed to persist penalties")?;
            return Ok(Some(until));
        }

        self.persist_penalties()
            .with_context(|| "failed to persist penalties")?;
        Ok(None)
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

fn load_penalties(path: &Path) -> anyhow::Result<HashMap<String, PenaltyRecord>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let bytes = fs::read(path)?;
    let penalties = serde_json::from_slice(&bytes)?;
    Ok(penalties)
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
}

#[derive(Clone, Copy, Debug)]
struct DisableEntry {
    until: DateTime<FixedOffset>,
    reason: DisableReason,
}
