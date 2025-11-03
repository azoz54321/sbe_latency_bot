#![cfg(feature = "test-mode")]

#[path = "support.rs"]
mod support;

use sbe_latency_bot::execution::FakeOrderBehavior;

use support::TestHarness;

#[test]
fn e2e_reset_warmup_enable() {
    let harness = TestHarness::with_symbols_and_warmup(&["ALTUSDT", "BTCUSDT", "ETHBTC"], 3);
    harness.set_provider_behavior("ALTUSDT", FakeOrderBehavior::AlwaysOk);
    harness.prewarm_symbol("ALTUSDT", 100.0);
    harness.prewarm_symbol("BTCUSDT", 30_000.0);
    harness.prewarm_symbol("ETHBTC", 0.07);
    harness.clear_provider_calls();

    let mut scenario = harness.scenario();
    scenario.drive_reset_now();
    scenario.emit_btc_eth(0.0, 0.0);
    harness.install_default_filters();
    harness.prewarm_symbol("ALTUSDT", 100.0);
    harness.clear_provider_calls();
    scenario.emit_ret("ALTUSDT", 0.06, 60_000);

    let during_warmup = scenario.wait_provider_calls("ALTUSDT", 1, 200);
    assert_eq!(
        during_warmup, 0,
        "orders should be suppressed during warmup"
    );

    scenario.advance_secs(3);
    harness.force_live_for_tests(true);

    scenario.emit_ret("ALTUSDT", 0.06, 60_000);
    let post_warmup = scenario.wait_provider_calls("ALTUSDT", 2, 1_000);
    assert_eq!(
        post_warmup, 2,
        "expected one BUY and one LIMIT after warmup"
    );

    harness.assert_provider_sequence("ALTUSDT", &[("BUY", "MARKET"), ("SELL", "LIMIT")]);
}
