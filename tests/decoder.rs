use sbe_latency_bot::decoder_sbe::{DecodeStatus, SbeDecoder};
use sbe_latency_bot::ffi::{bsbe_try_decode_one, BsbeTrade};
use std::path::PathBuf;

fn fixture_bytes(name: &str) -> Vec<u8> {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join(name);
    std::fs::read(base).expect("fixture should exist")
}

fn to_price(value: i64) -> f64 {
    value as f64 / 100_000_000.0
}

#[test]
fn decodes_without_errors() {
    let data = fixture_bytes("sbe_sample_01.bin");
    let mut decoder = SbeDecoder::default();
    let mut trades: Vec<BsbeTrade> = Vec::new();
    let report = decoder.decode_stream(&data, |trade| trades.push(*trade));

    assert_eq!(report.status, DecodeStatus::Complete);
    assert!(report.frames > 0, "no frames decoded");
    assert!(report.trades > 0, "no trades decoded");
    assert_eq!(trades.len(), report.trades);
}

#[test]
fn scales_and_side_match() {
    let data = fixture_bytes("sbe_sample_01.bin");
    let mut decoder = SbeDecoder::default();
    let mut trades: Vec<BsbeTrade> = Vec::new();
    let report = decoder.decode_stream(&data, |trade| trades.push(*trade));
    assert_eq!(report.status, DecodeStatus::Complete);
    assert!(trades.len() >= 2);

    let first = trades[0];
    assert!((to_price(first.px_e8) - 67_123.4567).abs() < 1e-6);
    assert!((to_price(first.qty_e8) - 0.25).abs() < 1e-9);
    assert_eq!(first.side, 1);
    assert_eq!(first.trade_id, 7_500_000_000);
    assert!(trades.iter().any(|trade| trade.side == 0));

    let sample2 = fixture_bytes("sbe_sample_02.bin");
    let mut decoder = SbeDecoder::default();
    let mut raw = Vec::new();
    let report = decoder.decode_stream(&sample2, |trade| raw.push(*trade));
    assert_eq!(report.status, DecodeStatus::Complete);
    assert_eq!(report.frames, 3);
    assert!(raw.iter().any(|trade| trade.side == 0));
    assert!(raw.iter().any(|trade| trade.side == 1));
    assert!((to_price(raw[1].px_e8) - 26_515.5).abs() < 1e-4);
}

#[test]
fn unknown_template_advances() {
    let block_len: u16 = 4;
    let template_id: u16 = 9_999;
    let schema_id: u16 = 3;
    let version: u16 = 1;

    let mut frame = Vec::with_capacity(block_len as usize + 8);
    for field in [block_len, template_id, schema_id, version] {
        frame.extend_from_slice(&field.to_le_bytes());
    }
    frame.resize(frame.capacity(), 0);

    let mut trade = BsbeTrade::default();
    let mut consumed = 0usize;
    let rc =
        unsafe { bsbe_try_decode_one(frame.as_ptr(), frame.len(), &mut trade, 1, &mut consumed) };
    assert_eq!(rc, 0);
    assert_eq!(consumed, frame.len());
}
