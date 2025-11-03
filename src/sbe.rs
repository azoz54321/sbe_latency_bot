use byteorder::{ByteOrder, LittleEndian};
#[allow(unused_imports)]
use spot_sbe as _;

use crate::types::Symbol;

#[derive(Default)]
pub struct SbeDecoder;

pub enum DecodedMsg {
    Trade {
        symbol: Option<Symbol>,
        price: f64,
        qty: f64,
        exch_ts_ns: u64,
        seq: u64,
    },
}

const HEADER_LEN: usize = 8;
const TRADE_TEMPLATE_ID: u16 = 201;
const WS_TEMPLATE_ID: u16 = 50;
const STREAM_TEMPLATE_ID: u16 = 10_000;
const STREAM_SCHEMA_ID: u16 = 1;
const STREAM_SCHEMA_VERSION: u16 = 0;
const RATE_LIMIT_HEADER_LEN: usize = 4;
const TRADES_GROUP_HEADER_LEN: usize = 6;
const TRADES_ENTRY_MIN_LEN: usize = 40;

#[derive(Clone, Copy)]
struct MessageHeader {
    block_length: u16,
    template_id: u16,
    schema_id: u16,
    version: u16,
}

impl SbeDecoder {
    pub fn decode_frame(&mut self, buf: &[u8]) -> Option<DecodedMsg> {
        let header = parse_header(buf)?;

        if header.template_id == STREAM_TEMPLATE_ID
            && header.schema_id == STREAM_SCHEMA_ID
            && header.version == STREAM_SCHEMA_VERSION
        {
            return self.decode_stream_envelope(buf.get(HEADER_LEN..)?);
        }

        let body = buf.get(HEADER_LEN..)?;
        match header.template_id {
            WS_TEMPLATE_ID => self.decode_ws_response(body),
            TRADE_TEMPLATE_ID => self.decode_trades(header, body, None),
            _ => None,
        }
    }

    fn decode_stream_envelope(&mut self, body: &[u8]) -> Option<DecodedMsg> {
        let inner_header = parse_header(body)?;
        if inner_header.template_id != TRADE_TEMPLATE_ID {
            return None;
        }
        let inner_body = body.get(HEADER_LEN..)?;
        self.decode_trades(inner_header, inner_body, None)
    }

    fn decode_ws_response(&mut self, body: &[u8]) -> Option<DecodedMsg> {
        let mut cursor = 0;
        if body.len() < 3 {
            return None;
        }
        cursor += 3; // skip schema flag + status

        if body.len() < cursor + RATE_LIMIT_HEADER_LEN {
            return None;
        }
        let rate_block = LittleEndian::read_u16(&body[cursor..cursor + 2]) as usize;
        let rate_count = LittleEndian::read_u16(&body[cursor + 2..cursor + 4]) as usize;
        cursor += RATE_LIMIT_HEADER_LEN;

        let rate_bytes = rate_block.checked_mul(rate_count)?;
        if body.len() < cursor + rate_bytes {
            return None;
        }
        cursor += rate_bytes;

        if body.len() < cursor + 1 {
            return None;
        }
        let id_len = body[cursor] as usize;
        cursor += 1;

        if body.len() < cursor + id_len {
            return None;
        }
        let stream_id = &body[cursor..cursor + id_len];
        cursor += id_len;

        if body.len() < cursor + 4 {
            return None;
        }
        let payload_len = LittleEndian::read_u32(&body[cursor..cursor + 4]) as usize;
        cursor += 4;

        if body.len() < cursor + payload_len {
            return None;
        }
        let payload = &body[cursor..cursor + payload_len];
        let symbol = parse_symbol_from_stream_id(stream_id)?;

        let inner_header = parse_header(payload)?;
        if inner_header.template_id != TRADE_TEMPLATE_ID {
            return None;
        }
        let inner_body = payload.get(HEADER_LEN..)?;
        self.decode_trades(inner_header, inner_body, Some(symbol))
    }

    fn decode_trades(
        &mut self,
        header: MessageHeader,
        body: &[u8],
        symbol_hint: Option<Symbol>,
    ) -> Option<DecodedMsg> {
        if header.block_length < 2 || body.len() < 2 + TRADES_GROUP_HEADER_LEN {
            return None;
        }

        let price_exp = body[0] as i8 as i32;
        let qty_exp = body[1] as i8 as i32;

        let group_block_len = LittleEndian::read_u16(&body[2..4]) as usize;
        let group_count = LittleEndian::read_u32(&body[4..8]) as usize;
        if group_block_len == 0 || group_count == 0 {
            return None;
        }

        let offset = 8;
        if body.len() < offset + group_block_len || group_block_len < TRADES_ENTRY_MIN_LEN {
            return None;
        }
        let trade = &body[offset..offset + group_block_len];

        let seq_raw = LittleEndian::read_i64(&trade[0..8]);
        if seq_raw < 0 {
            return None;
        }
        let price_raw = LittleEndian::read_i64(&trade[8..16]);
        let qty_raw = LittleEndian::read_i64(&trade[16..24]);
        let time_raw = LittleEndian::read_i64(&trade[32..40]);

        let seq = seq_raw as u64;
        let exch_ts_ns = if time_raw <= 0 {
            0
        } else {
            let nanos = (time_raw as u128).saturating_mul(1_000_000);
            nanos.min(u128::from(u64::MAX)) as u64
        };

        Some(DecodedMsg::Trade {
            symbol: symbol_hint,
            price: scale(price_raw, price_exp),
            qty: scale(qty_raw, qty_exp),
            exch_ts_ns,
            seq,
        })
    }
}

fn parse_header(buf: &[u8]) -> Option<MessageHeader> {
    if buf.len() < HEADER_LEN {
        return None;
    }
    Some(MessageHeader {
        block_length: LittleEndian::read_u16(&buf[0..2]),
        template_id: LittleEndian::read_u16(&buf[2..4]),
        schema_id: LittleEndian::read_u16(&buf[4..6]),
        version: LittleEndian::read_u16(&buf[6..8]),
    })
}

fn scale(value: i64, exponent: i32) -> f64 {
    if value == 0 {
        return 0.0;
    }
    let base = value as f64;
    if exponent == 0 {
        base
    } else {
        base * 10f64.powi(exponent)
    }
}

fn parse_symbol_from_stream_id(stream_id: &[u8]) -> Option<Symbol> {
    let symbol_len = stream_id
        .iter()
        .position(|&b| b == b'@')
        .unwrap_or(stream_id.len());
    Symbol::from_bytes(&stream_id[..symbol_len])
}
