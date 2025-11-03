#![cfg(feature = "test-mode")]

#[path = "support.rs"]
mod support;

use sbe_latency_bot::execution::{FakeOrderBehavior, OrderError};
use sbe_latency_bot::types::symbol_id;

use support::TestHarness;

#[test]
fn e2e_min_notional_disables_for_today() {
    let harness = TestHarness::with_symbols_and_warmup(&["SMALLUSDT", "BTCUSDT", "ETHBTC"], 0);
    harness.prewarm_symbol("SMALLUSDT", 25.0);
    harness.prewarm_symbol("BTCUSDT", 30_000.0);
    harness.prewarm_symbol("ETHBTC", 0.07);
    harness.set_provider_behavior(
        "SMALLUSDT",
        FakeOrderBehavior::Always(OrderError::FilterMinNotional),
    );
    harness.clear_provider_calls();

    let mut scenario = harness.scenario();
    scenario.drive_reset_now().emit_btc_eth(0.0, 0.0);
    harness.install_default_filters();
    harness.install_default_filters();
    harness.install_default_filters();
    harness.prewarm_symbol("SMALLUSDT", 25.0);
    harness.clear_provider_calls();
    harness.force_live_for_tests(true);
    harness.force_trading_enabled_for_tests();

    scenario.emit_ret("SMALLUSDT", 0.06, 60_000);
    let provider_calls = scenario.wait_provider_calls("SMALLUSDT", 1, 300);
    assert_eq!(provider_calls, 0, "order should be blocked on min-notional");

    let metrics = scenario.metrics_snapshot();
    assert_eq!(metrics.disable_for_today, 1);

    let logs = harness.drain_info_strings();
    assert!(
        logs.iter()
            .any(|line| line.contains("[RISK] disabled symbol=SMALLUSDT")),
        "expected min-notional disable log, got {logs:?}"
    );

    let symbol = harness.lookup_symbol("SMALLUSDT");
    let snapshot = scenario.processor_snapshot();
    let disabled_ids: Vec<_> = snapshot
        .risk
        .disabled
        .iter()
        .map(|entry| entry.symbol_id)
        .collect();
    assert!(
        disabled_ids.contains(&symbol_id(symbol)),
        "symbol should be disabled after min-notional failure"
    );

    scenario.drive_reset_now();
    let post_reset = scenario.processor_snapshot();
    assert!(
        post_reset.risk.disabled.is_empty(),
        "reset should clear daily disables"
    );
}
