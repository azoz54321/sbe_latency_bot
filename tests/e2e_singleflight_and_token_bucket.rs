#![cfg(feature = "test-mode")]

#[path = "support.rs"]
mod support;

use sbe_latency_bot::execution::FakeOrderBehavior;

use support::TestHarness;

#[test]
fn e2e_singleflight_and_token_bucket() {
    let harness =
        TestHarness::with_symbols_and_warmup(&["ALTUSDT", "BETAUSDT", "BTCUSDT", "ETHBTC"], 0);
    harness.prewarm_symbol("ALTUSDT", 120.0);
    harness.prewarm_symbol("BETAUSDT", 45.0);
    harness.prewarm_symbol("BTCUSDT", 30_000.0);
    harness.prewarm_symbol("ETHBTC", 0.07);
    harness.set_provider_behavior(
        "ALTUSDT",
        FakeOrderBehavior::SlowSuccess { spins: 1_000_000 },
    );
    harness.set_provider_behavior("BETAUSDT", FakeOrderBehavior::AlwaysOk);
    harness.clear_provider_calls();

    let mut scenario = harness.scenario();
    scenario.drive_reset_now().emit_btc_eth(0.0, 0.0);
    harness.install_default_filters();
    harness.prewarm_symbol("ALTUSDT", 120.0);
    harness.prewarm_symbol("BETAUSDT", 45.0);
    harness.clear_provider_calls();
    harness.force_live_for_tests(true);
    harness.force_trading_enabled_for_tests();

    scenario.emit_ret("ALTUSDT", 0.06, 60_000);
    scenario.emit_ret("ALTUSDT", 0.06, 60_000);
    scenario.emit_ret("ALTUSDT", 0.06, 60_000);

    let alt_calls = scenario.wait_provider_calls("ALTUSDT", 2, 2_000);
    assert_eq!(
        alt_calls, 2,
        "singleflight should collapse duplicate triggers"
    );

    scenario.emit_ret("BETAUSDT", 0.06, 60_000);
    let beta_calls = scenario.wait_provider_calls("BETAUSDT", 2, 2_000);
    assert_eq!(
        beta_calls, 2,
        "token bucket should allow independent symbol"
    );

    assert!(
        !harness.provider_overlap_detected(),
        "provider recorded overlapping submissions"
    );

    let (alt_buys, alt_limits) = harness.provider_call_counts("ALTUSDT");
    assert_eq!((alt_buys, alt_limits), (1, 1));

    let (beta_buys, beta_limits) = harness.provider_call_counts("BETAUSDT");
    assert_eq!((beta_buys, beta_limits), (1, 1));
}
