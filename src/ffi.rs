#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BsbeTrade {
    pub px_e8: i64,
    pub qty_e8: i64,
    pub side: u8,
    pub _pad0: u8,
    pub _pad1: u16,
    pub symbol_id: u32,
    pub event_ts_ns: u64,
    pub trade_id: u64,
}

extern "C" {
    pub fn bsbe_try_decode_one(
        buf: *const u8,
        len: usize,
        out: *mut BsbeTrade,
        out_cap: usize,
        consumed: *mut usize,
    ) -> i32;
}
