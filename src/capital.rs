use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::types::Symbol;
use rust_decimal::Decimal;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum SlotId {
    A,
    B,
}

impl SlotId {
    pub fn label(self) -> &'static str {
        match self {
            SlotId::A => "A",
            SlotId::B => "B",
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SlotPhase {
    Idle,
    PendingSend,
    ReservedOnAccount,
    PositionOpen,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum OrderRole {
    Buy,
    TakeProfit,
}

#[derive(Debug, Clone)]
struct SlotState {
    symbol: Option<Symbol>,
    phase: SlotPhase,
    buy_client_id: Option<String>,
    tp_client_id: Option<String>,
    order_id: Option<String>,
    pending_since: Option<Instant>,
    budget: Decimal,
}

impl SlotState {
    fn new(budget: Decimal) -> Self {
        Self {
            symbol: None,
            phase: SlotPhase::Idle,
            buy_client_id: None,
            tp_client_id: None,
            order_id: None,
            pending_since: None,
            budget,
        }
    }

    fn reset(&mut self) {
        self.symbol = None;
        self.phase = SlotPhase::Idle;
        self.buy_client_id = None;
        self.tp_client_id = None;
        self.order_id = None;
        self.pending_since = None;
    }
}

#[derive(Debug)]
pub struct PendingTimeout {
    pub slot: SlotId,
    pub symbol: Symbol,
    pub client_order_id: Option<String>,
}

#[derive(Debug)]
pub struct CapitalSlots {
    slots: [SlotState; 2],
    client_lookup: HashMap<String, (SlotId, OrderRole)>,
    pending_timeout: Duration,
}

#[allow(clippy::new_without_default)]
impl CapitalSlots {
    pub fn new(slot_a_budget: Decimal, slot_b_budget: Decimal) -> Self {
        Self {
            slots: [SlotState::new(slot_a_budget), SlotState::new(slot_b_budget)],
            client_lookup: HashMap::new(),
            pending_timeout: Duration::from_secs(5),
        }
    }

    pub fn try_begin_pending(
        &mut self,
        now: Instant,
        symbol: Symbol,
        buy_client_id: String,
        tp_client_id: String,
    ) -> Option<SlotId> {
        self.expire_pending(now);
        for (idx, slot) in self.slots.iter_mut().enumerate() {
            if slot.phase == SlotPhase::Idle {
                slot.phase = SlotPhase::PendingSend;
                slot.symbol = Some(symbol);
                slot.buy_client_id = Some(buy_client_id.clone());
                slot.tp_client_id = Some(tp_client_id.clone());
                slot.order_id = None;
                slot.pending_since = Some(now);

                let slot_id = match idx {
                    0 => SlotId::A,
                    _ => SlotId::B,
                };
                self.client_lookup
                    .insert(buy_client_id, (slot_id, OrderRole::Buy));
                self.client_lookup
                    .insert(tp_client_id, (slot_id, OrderRole::TakeProfit));
                return Some(slot_id);
            }
        }
        None
    }

    pub fn first_idle_slot(&mut self, now: Instant) -> Option<SlotId> {
        self.expire_pending(now);
        for (idx, slot) in self.slots.iter().enumerate() {
            if slot.phase == SlotPhase::Idle {
                return Some(match idx {
                    0 => SlotId::A,
                    _ => SlotId::B,
                });
            }
        }
        None
    }

    pub fn begin_pending_on_slot(
        &mut self,
        now: Instant,
        slot_id: SlotId,
        symbol: Symbol,
        buy_client_id: String,
        tp_client_id: String,
    ) -> bool {
        self.expire_pending(now);
        let Some(idx) = self.slot_index(slot_id) else {
            return false;
        };
        let slot = &mut self.slots[idx];
        if slot.phase != SlotPhase::Idle {
            return false;
        }
        slot.phase = SlotPhase::PendingSend;
        slot.symbol = Some(symbol);
        slot.buy_client_id = Some(buy_client_id.clone());
        slot.tp_client_id = Some(tp_client_id.clone());
        slot.order_id = None;
        slot.pending_since = Some(now);
        self.client_lookup
            .insert(buy_client_id, (slot_id, OrderRole::Buy));
        self.client_lookup
            .insert(tp_client_id, (slot_id, OrderRole::TakeProfit));
        true
    }

    pub fn mark_reserved(
        &mut self,
        client_order_id: &str,
        order_id: Option<String>,
        now: Instant,
    ) -> Option<(SlotId, Symbol)> {
        let (slot_id, _) = self.client_lookup.get(client_order_id).copied()?;
        let idx = self.slot_index(slot_id)?;
        let slot = &mut self.slots[idx];
        if let Some(symbol) = slot.symbol {
            slot.phase = SlotPhase::ReservedOnAccount;
            slot.order_id = order_id;
            slot.pending_since = Some(now);
            return Some((slot_id, symbol));
        }
        None
    }

    pub fn mark_position_open(
        &mut self,
        client_order_id: &str,
        order_id: Option<String>,
    ) -> Option<(SlotId, Symbol)> {
        let (slot_id, _) = self.client_lookup.get(client_order_id).copied()?;
        let idx = self.slot_index(slot_id)?;
        let slot = &mut self.slots[idx];
        if let Some(symbol) = slot.symbol {
            slot.phase = SlotPhase::PositionOpen;
            slot.order_id = order_id;
            return Some((slot_id, symbol));
        }
        None
    }

    pub fn release_by_client(&mut self, client_order_id: &str) -> Option<(SlotId, Symbol)> {
        let (slot_id, _) = self.client_lookup.get(client_order_id).copied()?;
        self.release_slot(slot_id)
    }

    pub fn release_slot(&mut self, slot_id: SlotId) -> Option<(SlotId, Symbol)> {
        let idx = self.slot_index(slot_id)?;
        let symbol = self.slots[idx].symbol;
        self.remove_mappings_for_slot(slot_id);
        self.slots[idx].reset();
        symbol.map(|sym| (slot_id, sym))
    }

    pub fn contains(&self, symbol: Symbol) -> bool {
        self.slots
            .iter()
            .any(|slot| slot.symbol == Some(symbol) && slot.phase != SlotPhase::Idle)
    }

    pub fn reset_daily(&mut self) {
        for slot in &mut self.slots {
            slot.reset();
        }
        self.client_lookup.clear();
    }

    pub fn expire_pending(&mut self, now: Instant) -> Vec<PendingTimeout> {
        let mut released = Vec::new();
        for idx in 0..self.slots.len() {
            let slot = &mut self.slots[idx];
            if slot.phase != SlotPhase::PendingSend {
                continue;
            }
            let Some(start) = slot.pending_since else {
                continue;
            };
            if now.duration_since(start) < self.pending_timeout {
                continue;
            }
            let slot_id = match idx {
                0 => SlotId::A,
                _ => SlotId::B,
            };
            if let Some(symbol) = slot.symbol {
                let buy_id = slot.buy_client_id.clone();
                released.push(PendingTimeout {
                    slot: slot_id,
                    symbol,
                    client_order_id: buy_id,
                });
            }
            slot.pending_since = Some(now);
        }
        released
    }

    pub fn slot_for_client(&self, client_order_id: &str) -> Option<(SlotId, OrderRole)> {
        self.client_lookup.get(client_order_id).copied()
    }

    pub fn slot_budget(&self, slot_id: SlotId) -> Decimal {
        let idx = self.slot_index(slot_id).unwrap_or(0);
        self.slots
            .get(idx)
            .map(|s| s.budget)
            .unwrap_or(Decimal::ZERO)
    }

    pub fn set_budget(&mut self, slot_id: SlotId, budget: Decimal) {
        if let Some(idx) = self.slot_index(slot_id) {
            if let Some(slot) = self.slots.get_mut(idx) {
                slot.budget = budget;
            }
        }
    }

    pub fn set_budgets(&mut self, slot_a_budget: Decimal, slot_b_budget: Decimal) {
        if let Some(slot) = self.slots.get_mut(0) {
            slot.budget = slot_a_budget;
        }
        if let Some(slot) = self.slots.get_mut(1) {
            slot.budget = slot_b_budget;
        }
    }

    pub fn both_idle(&self) -> bool {
        self.slots.iter().all(|slot| slot.phase == SlotPhase::Idle)
    }

    pub fn reserved_only(&self) -> bool {
        self.slots.iter().all(|slot| {
            matches!(
                slot.phase,
                SlotPhase::ReservedOnAccount | SlotPhase::PositionOpen
            )
        })
    }

    pub fn tp_client_id(&self, slot_id: SlotId) -> Option<String> {
        let idx = self.slot_index(slot_id)?;
        self.slots[idx].tp_client_id.clone()
    }

    pub fn buy_client_id(&self, slot_id: SlotId) -> Option<String> {
        let idx = self.slot_index(slot_id)?;
        self.slots[idx].buy_client_id.clone()
    }

    pub fn order_id(&self, slot_id: SlotId) -> Option<String> {
        let idx = self.slot_index(slot_id)?;
        self.slots[idx].order_id.clone()
    }

    fn slot_index(&self, slot_id: SlotId) -> Option<usize> {
        match slot_id {
            SlotId::A => Some(0),
            SlotId::B => Some(1),
        }
    }

    fn remove_mappings_for_slot(&mut self, slot_id: SlotId) {
        self.client_lookup
            .retain(|_, (mapped, _)| *mapped != slot_id);
    }
}
