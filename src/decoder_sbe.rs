use crate::ffi::{bsbe_try_decode_one, BsbeTrade};
use std::array;
use std::mem::MaybeUninit;

const MAX_TRADES_PER_FRAME: usize = 32;
const ERR_INCOMPLETE: i32 = -1;
const ERR_CORRUPT: i32 = -2;
const ERR_SCHEMA_MISMATCH: i32 = -3;
const ERR_OUTPUT_TRUNCATED: i32 = -4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeStatus {
    Complete,
    Incomplete,
    Corrupt,
    SchemaMismatch,
}

#[derive(Debug, Clone, Copy)]
pub struct DecodeReport {
    pub frames: usize,
    pub trades: usize,
    pub status: DecodeStatus,
}

impl DecodeReport {
    pub fn empty() -> Self {
        Self {
            frames: 0,
            trades: 0,
            status: DecodeStatus::Complete,
        }
    }
}

#[derive(Default)]
pub struct SbeDecoder {
    schema_mismatch: bool,
}

impl SbeDecoder {
    pub fn decode_stream<F>(&mut self, mut src: &[u8], mut on_trade: F) -> DecodeReport
    where
        F: FnMut(&BsbeTrade),
    {
        let mut frames = 0usize;
        let mut trades = 0usize;
        let mut status = DecodeStatus::Complete;
        let mut slot: [MaybeUninit<BsbeTrade>; MAX_TRADES_PER_FRAME] =
            array::from_fn(|_| MaybeUninit::uninit());

        while !src.is_empty() {
            let mut consumed: usize = 0;
            let rc = unsafe {
                bsbe_try_decode_one(
                    src.as_ptr(),
                    src.len(),
                    slot.as_mut_ptr() as *mut BsbeTrade,
                    MAX_TRADES_PER_FRAME,
                    &mut consumed as *mut usize,
                )
            };

            if rc < 0 {
                match rc {
                    ERR_INCOMPLETE => {
                        status = promote(status, DecodeStatus::Incomplete);
                        break;
                    }
                    ERR_SCHEMA_MISMATCH => {
                        self.schema_mismatch = true;
                        status = DecodeStatus::SchemaMismatch;
                        break;
                    }
                    ERR_OUTPUT_TRUNCATED => {
                        status = promote(status, DecodeStatus::Corrupt);
                    }
                    ERR_CORRUPT | _ => {
                        status = promote(status, DecodeStatus::Corrupt);
                    }
                }

                if consumed == 0 || consumed > src.len() {
                    break;
                }
                src = &src[consumed..];
                continue;
            }

            if consumed == 0 || consumed > src.len() {
                status = promote(status, DecodeStatus::Corrupt);
                break;
            }

            frames = frames.saturating_add(1);
            let produced = rc as usize;
            for trade in slot.iter().take(produced) {
                let trade = unsafe { trade.assume_init() };
                trades = trades.saturating_add(1);
                on_trade(&trade);
            }

            src = &src[consumed..];
        }

        DecodeReport {
            frames,
            trades,
            status,
        }
    }

    pub fn take_schema_mismatch(&mut self) -> bool {
        let flag = self.schema_mismatch;
        self.schema_mismatch = false;
        flag
    }
}

fn promote(current: DecodeStatus, next: DecodeStatus) -> DecodeStatus {
    if severity(next) > severity(current) {
        next
    } else {
        current
    }
}

fn severity(status: DecodeStatus) -> u8 {
    match status {
        DecodeStatus::Complete => 0,
        DecodeStatus::Incomplete => 1,
        DecodeStatus::Corrupt => 2,
        DecodeStatus::SchemaMismatch => 3,
    }
}
