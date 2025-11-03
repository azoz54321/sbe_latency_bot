#![cfg(feature = "test-mode")]

#[path = "support.rs"]
mod support;

use sbe_latency_bot::execution::{FakeOrderBehavior, OrderError};

use support::TestHarness;

#[test]
fn e2e_auto_heal_then_success() {
    let harness = TestHarness::with_symbols_and_warmup(&["ALTUSDT", "BTCUSDT", "ETHBTC"], 0);
    harness.prewarm_symbol("ALTUSDT", 100.0);
    harness.prewarm_symbol("BTCUSDT", 30_000.0);
    harness.prewarm_symbol("ETHBTC", 0.07);
    harness.set_provider_behavior(
        "ALTUSDT",
        FakeOrderBehavior::RejectLimitOnce(OrderError::FilterPriceTick),
    );
    harness.clear_provider_calls();

    let mut scenario = harness.scenario();
    scenario.drive_reset_now().emit_btc_eth(0.0, 0.0);
    harness.install_default_filters();
    harness.prewarm_symbol("ALTUSDT", 100.0);
    harness.clear_provider_calls();
    harness.force_live_for_tests(true);
    harness.force_trading_enabled_for_tests();

    scenario.emit_ret("ALTUSDT", 0.06, 60_000);
    let received = scenario.wait_provider_calls("ALTUSDT", 4, 1_000);
    let calls = harness.provider_calls("ALTUSDT");
    assert_eq!(received, 4, "unexpected provider call sequence: {:?}", calls);

    let buys = calls.iter().filter(|call| call.side == "BUY").count();
    let limits = calls
        .iter()
        .filter(|call| call.order_type == "LIMIT")
        .count();
    assert_eq!(buys, 2);
    assert_eq!(limits, 2);

    let logs = harness.drain_info_strings();
    assert!(
        logs.iter().any(|line| line.contains("[HEAL] refresh")),
        "missing heal refresh log: {:?}",
        logs
    );
    assert!(
        logs.iter().any(|line| line.contains("[HEAL] success")),
        "missing heal success log: {:?}",
        logs
    );
}
