#![cfg(feature = "test-mode")]

#[path = "support.rs"]
mod support;

use sbe_latency_bot::execution::FakeOrderBehavior;

use support::TestHarness;

#[test]
fn e2e_capital_slots_positions() {
    let harness = TestHarness::with_symbols_and_warmup(
        &["ALPHAUSDT", "BETAUSDT", "GAMMAUSDT", "BTCUSDT", "ETHBTC"],
        0,
    );

    harness.prewarm_symbol("ALPHAUSDT", 100.0);
    harness.prewarm_symbol("BETAUSDT", 80.0);
    harness.prewarm_symbol("GAMMAUSDT", 60.0);
    harness.prewarm_symbol("BTCUSDT", 30_000.0);
    harness.prewarm_symbol("ETHBTC", 0.07);

    harness.set_provider_behavior("ALPHAUSDT", FakeOrderBehavior::AlwaysOk);
    harness.set_provider_behavior("BETAUSDT", FakeOrderBehavior::AlwaysOk);
    harness.set_provider_behavior("GAMMAUSDT", FakeOrderBehavior::AlwaysOk);
    harness.clear_provider_calls();

    let mut scenario = harness.scenario();
    scenario.drive_reset_now().emit_btc_eth(0.0, 0.0);
    harness.install_default_filters();
    harness.prewarm_symbol("ALPHAUSDT", 100.0);
    harness.prewarm_symbol("BETAUSDT", 80.0);
    harness.prewarm_symbol("GAMMAUSDT", 60.0);
    harness.clear_provider_calls();
    harness.force_live_for_tests(true);
    harness.force_trading_enabled_for_tests();

    scenario.emit_ret("ALPHAUSDT", 0.07, 60_000);
    scenario.wait_provider_calls("ALPHAUSDT", 2, 1_000);
    let snapshot = scenario.processor_snapshot();
    let alpha = harness.lookup_symbol("ALPHAUSDT");
    let beta = harness.lookup_symbol("BETAUSDT");
    let gamma = harness.lookup_symbol("GAMMAUSDT");
    assert_eq!(snapshot.capital.slot_a.symbol, Some(alpha));
    assert!(snapshot.capital.slot_a.occupied);
    assert_eq!(snapshot.positions.open.len(), 1);

    scenario.emit_ret("BETAUSDT", 0.07, 60_000);
    scenario.wait_provider_calls("BETAUSDT", 2, 1_000);
    let snapshot = scenario.processor_snapshot();
    assert_eq!(snapshot.capital.slot_b.symbol, Some(beta));
    assert!(snapshot.capital.slot_b.occupied);
    assert_eq!(snapshot.positions.open.len(), 2);

    scenario.emit_ret("GAMMAUSDT", 0.07, 60_000);
    let gamma_attempt = scenario.wait_provider_calls("GAMMAUSDT", 1, 300);
    assert_eq!(
        gamma_attempt, 0,
        "third symbol should be rejected while slots busy"
    );

    harness.send_price("ALPHAUSDT", 94.0);
    let freed_snapshot = (0..50)
        .find_map(|_| {
            scenario.advance_ms(10);
            std::thread::sleep(std::time::Duration::from_millis(1));
            let snapshot = scenario.processor_snapshot();
            if !snapshot.capital.slot_a.occupied {
                Some(snapshot)
            } else {
                None
            }
        })
        .unwrap_or_else(|| {
            panic!(
                "slot A should be free after ALPHA exits, logs: {:?}",
                harness.drain_info_strings()
            )
        });
    assert_eq!(freed_snapshot.positions.open.len(), 1);
    assert_eq!(freed_snapshot.positions.open[0].symbol, beta);

    scenario.drive_reset_now().emit_btc_eth(0.0, 0.0);
    harness.install_default_filters();
    harness.prewarm_symbol("GAMMAUSDT", 60.0);
    harness.prewarm_symbol("BETAUSDT", 80.0);
    harness.clear_provider_calls();
    harness.force_live_for_tests(true);
    harness.force_trading_enabled_for_tests();

    scenario.emit_ret("GAMMAUSDT", 0.07, 60_000);
    let gamma_after_reset = scenario.wait_provider_calls("GAMMAUSDT", 2, 1_000);
    assert_eq!(
        gamma_after_reset, 2,
        "post-reset signal should reserve slot A for GAMMA"
    );

    scenario.emit_ret("BETAUSDT", 0.07, 60_000);
    let beta_after_reset = scenario.wait_provider_calls("BETAUSDT", 2, 1_000);
    assert_eq!(
        beta_after_reset, 2,
        "post-reset signal should reserve slot B for BETA"
    );

    let final_snapshot = scenario.processor_snapshot();
    assert_eq!(final_snapshot.capital.slot_a.symbol, Some(gamma));
    assert_eq!(final_snapshot.capital.slot_b.symbol, Some(beta));
    let open_symbols: Vec<_> = final_snapshot
        .positions
        .open
        .iter()
        .map(|entry| entry.symbol)
        .collect();
    assert_eq!(open_symbols, vec![beta, gamma]);
}
