#![cfg(feature = "test-mode")]

#[path = "support.rs"]
mod support;

use std::time::Duration;

use rust_decimal::prelude::FromPrimitive;

use sbe_latency_bot::execution::{FakeOrderBehavior, OrderError};
use support::TestHarness;

fn wait_for_summary(harness: &TestHarness) -> support::MetricsSummary {
    harness
        .wait_for_metrics_summary()
        .expect("metrics summary available")
}

#[test]
fn test_warmup_blocks_signals() {
    let harness = TestHarness::with_symbols_and_warmup(&["ALTUSDT"], 10);
    harness.prewarm_symbol("ALTUSDT", 100.0);
    harness.clear_provider_calls();

    harness.emit_ret_without_advancing("ALTUSDT", 0.06);
    let received = harness.wait_provider_calls_at_least("ALTUSDT", 1, Duration::from_millis(200));
    assert_eq!(received, 0);
    assert!(harness.provider_calls("ALTUSDT").is_empty());

    harness.advance_secs(10);
    harness.force_trading_enabled_for_tests();
    harness.force_live_for_tests(true);
    harness.clear_provider_calls();
    harness.emit_ret_without_advancing("ALTUSDT", 0.06);
    let final_received = harness.wait_provider_calls_at_least("ALTUSDT", 2, Duration::from_secs(1));
    assert_eq!(final_received, 2);
    harness.assert_provider_sequence("ALTUSDT", &[("BUY", "MARKET"), ("SELL", "LIMIT")]);
}

#[test]
fn test_execution_auto_heal_then_success() {
    let harness = TestHarness::with_symbols_and_warmup(&["ALTUSDT"], 0);
    harness.prewarm_symbol("ALTUSDT", 100.0);
    harness.set_provider_behavior(
        "ALTUSDT",
        FakeOrderBehavior::RejectLimitOnce(OrderError::FilterPriceTick),
    );
    harness.force_trading_enabled_for_tests();
    harness.force_live_for_tests(true);
    harness.collect_metric_events();
    harness.drain_info_strings();
    harness.clear_provider_calls();

    harness.advance_secs(3);
    harness.emit_ret_without_advancing("ALTUSDT", 0.06);
    let received = harness.wait_provider_calls_at_least("ALTUSDT", 4, Duration::from_secs(1));
    assert_eq!(
        received,
        4,
        "unexpected provider call count: {:?}",
        harness.provider_calls("ALTUSDT")
    );
    harness.assert_provider_sequence(
        "ALTUSDT",
        &[
            ("BUY", "MARKET"),
            ("SELL", "LIMIT"),
            ("BUY", "MARKET"),
            ("SELL", "LIMIT"),
        ],
    );

    let summary = wait_for_summary(&harness);
    assert_eq!(summary.heal_attempts, 1);
    assert_eq!(summary.heal_success, 1);
    assert_eq!(summary.disables, 0);
    let logs = harness.drain_info_strings();
    assert!(!logs.iter().any(|line| line.contains("[RISK] disabled")));
}

#[test]
fn test_execution_min_notional_disables_for_today() {
    let harness = TestHarness::with_symbols_and_warmup(&["SMALLUSDT"], 0);
    harness.prewarm_symbol("SMALLUSDT", 25.0);
    harness.set_provider_behavior(
        "SMALLUSDT",
        FakeOrderBehavior::Always(OrderError::FilterMinNotional),
    );
    harness.force_trading_enabled_for_tests();
    harness.force_live_for_tests(true);
    harness.clear_provider_calls();

    harness.emit_ret_without_advancing("SMALLUSDT", 0.06);
    let received = harness.wait_provider_calls_at_least("SMALLUSDT", 1, Duration::from_millis(200));
    assert_eq!(received, 0);
    let (buys, limits) = harness.provider_call_counts("SMALLUSDT");
    assert_eq!((buys, limits), (0, 0));

    let logs = harness.drain_info_strings();
    assert!(logs
        .iter()
        .any(|line| line.contains("[RISK] disabled symbol=SMALLUSDT")));
}

#[test]
fn test_reset_scheduler_rebuilds_and_clears_daily() {
    let harness = TestHarness::with_symbols_and_warmup(&["ALTUSDT"], 3);
    harness.prewarm_symbol("ALTUSDT", 100.0);
    harness.clear_provider_calls();
    harness.force_trading_enabled_for_tests();
    harness.force_live_for_tests(true);
    harness.collect_metric_events();
    harness.drain_info_strings();
    harness.emit_ret_without_advancing("ALTUSDT", 0.06);
    let initial_calls = harness.wait_provider_calls_at_least("ALTUSDT", 2, Duration::from_secs(1));
    assert_eq!(initial_calls, 2);

    harness.set_provider_behavior(
        "ALTUSDT",
        FakeOrderBehavior::Always(OrderError::FilterMinNotional),
    );
    harness.clear_provider_calls();
    harness.emit_ret_without_advancing("ALTUSDT", 0.06);
    harness.wait_provider_calls_at_least("ALTUSDT", 1, Duration::from_millis(200));
    harness.drain_info_strings();
    harness.set_provider_behavior("ALTUSDT", FakeOrderBehavior::AlwaysOk);
    harness.clear_provider_calls();

    harness.run_daily_reset().expect("reset successful");
    let mut reset_filters = std::collections::HashMap::new();
    let alt_symbol = harness.lookup_symbol("ALTUSDT");
    reset_filters.insert(
        sbe_latency_bot::types::symbol_id(alt_symbol),
        sbe_latency_bot::filters::SymbolFilters {
            step: rust_decimal::Decimal::from_f64(0.001).unwrap(),
            tick: rust_decimal::Decimal::from_f64(0.01).unwrap(),
            min_notional: rust_decimal::Decimal::from_f64(10.0).unwrap(),
        },
    );
    harness.execution_handle().install_filters(reset_filters);

    harness.clear_provider_calls();
    harness.emit_ret_without_advancing("ALTUSDT", 0.06);
    let during_warmup =
        harness.wait_provider_calls_at_least("ALTUSDT", 1, Duration::from_millis(200));
    assert_eq!(during_warmup, 0);

    let warmup_secs = harness.config().strategy.warmup_secs;
    harness.advance_secs(warmup_secs + 1);
    harness.force_trading_enabled_for_tests();
    harness.force_live_for_tests(true);
    harness.collect_metric_events();
    harness.drain_info_strings();
    harness.clear_provider_calls();
    harness.prewarm_symbol("ALTUSDT", 100.0);
    harness.clear_provider_calls();
    harness.advance_secs(3);
    harness.emit_ret_without_advancing("ALTUSDT", 0.06);
    let post_warmup = harness.wait_provider_calls_at_least("ALTUSDT", 2, Duration::from_secs(1));
    assert_eq!(post_warmup, 2);
    harness.assert_provider_sequence("ALTUSDT", &[("BUY", "MARKET"), ("SELL", "LIMIT")]);

    let summary = wait_for_summary(&harness);
    assert_eq!(summary.heal_attempts, 0);
    assert_eq!(summary.heal_success, 0);
    assert_eq!(summary.disables, 0);
}

#[test]
fn test_guards_singleflight_rate_limit() {
    let harness = TestHarness::with_symbols_and_warmup(&["ALTUSDT", "BETAUSDT"], 0);
    harness.prewarm_symbol("ALTUSDT", 100.0);
    harness.prewarm_symbol("BETAUSDT", 50.0);
    harness.force_trading_enabled_for_tests();
    harness.force_live_for_tests(true);
    harness.collect_metric_events();
    harness.drain_info_strings();
    harness.set_provider_behavior(
        "ALTUSDT",
        FakeOrderBehavior::SlowSuccess { spins: 1_000_000 },
    );
    harness.set_provider_behavior("BETAUSDT", FakeOrderBehavior::AlwaysOk);
    harness.clear_provider_calls();

    harness.emit_ret_without_advancing("ALTUSDT", 0.05);
    harness.emit_ret_without_advancing("ALTUSDT", 0.05);
    harness.emit_ret_without_advancing("ALTUSDT", 0.05);
    harness.emit_ret_without_advancing("BETAUSDT", 0.06);

    harness.wait_provider_calls_at_least("ALTUSDT", 2, Duration::from_secs(2));
    harness.wait_provider_calls_at_least("BETAUSDT", 2, Duration::from_secs(2));

    let alt_calls = harness.provider_calls("ALTUSDT");
    assert_eq!(alt_calls.len(), 2);
    let alt_sides: Vec<_> = alt_calls.iter().map(|c| c.side).collect();
    assert_eq!(alt_sides, vec!["BUY", "SELL"]);

    let beta_calls = harness.provider_calls("BETAUSDT");
    assert_eq!(beta_calls.len(), 2);
    assert!(beta_calls.iter().all(|call| call.success));
    assert!(!harness.provider_overlap_detected());
}
