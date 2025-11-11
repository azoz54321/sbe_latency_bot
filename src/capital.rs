use std::time::Instant;

use crate::types::Symbol;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
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

#[derive(Debug)]
struct SlotState {
    occupied: bool,
    symbol: Option<Symbol>,
    reserved_at: Option<Instant>,
}

impl SlotState {
    fn new() -> Self {
        Self {
            occupied: false,
            symbol: None,
            reserved_at: None,
        }
    }
}

#[derive(Debug)]
pub struct CapitalSlots {
    slots: [SlotState; 2],
}

#[allow(clippy::new_without_default)]
impl CapitalSlots {
    pub fn new() -> Self {
        Self {
            slots: [SlotState::new(), SlotState::new()],
        }
    }

    pub fn try_reserve_slot(&mut self, now: Instant, symbol: Symbol) -> Option<SlotId> {
        for (idx, slot) in self.slots.iter_mut().enumerate() {
            if !slot.occupied {
                slot.occupied = true;
                slot.symbol = Some(symbol);
                slot.reserved_at = Some(now);
                return Some(match idx {
                    0 => SlotId::A,
                    _ => SlotId::B,
                });
            }
        }
        None
    }

    pub fn release_slot(&mut self, slot_id: SlotId) {
        match slot_id {
            SlotId::A => self.reset_slot(0),
            SlotId::B => self.reset_slot(1),
        }
    }

    pub fn contains(&self, symbol: Symbol) -> bool {
        self.slots
            .iter()
            .any(|slot| slot.occupied && slot.symbol == Some(symbol))
    }

    pub fn reset_daily(&mut self) {
        for slot in &mut self.slots {
            slot.reserved_at = None;
        }
    }

    fn reset_slot(&mut self, idx: usize) {
        if let Some(slot) = self.slots.get_mut(idx) {
            slot.occupied = false;
            slot.symbol = None;
            slot.reserved_at = None;
        }
    }
}
