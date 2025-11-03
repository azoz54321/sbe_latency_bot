#![cfg(feature = "test-mode")]

#[path = "support.rs"]
mod support;

use sbe_latency_bot::execution::FakeOrderBehavior;

use support::TestHarness;

#[test]
fn harness_initializes() {
    let harness = TestHarness::new();
    harness.set_provider_behavior("BTCUSDT", FakeOrderBehavior::AlwaysOk);
    harness.send_price("BTCUSDT", 100.0);
    harness.advance_secs(1);
    harness.send_price("BTCUSDT", 100.5);
}
