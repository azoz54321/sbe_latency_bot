use anyhow::Context;
use crossbeam_channel::bounded;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use sbe_latency_bot::channels::spsc_channel;
use sbe_latency_bot::clock::{Clock, SystemClock};
use sbe_latency_bot::config::Config;
use sbe_latency_bot::data_feed;
use sbe_latency_bot::execution::{self, ExecGuards};
use sbe_latency_bot::filters::FilterCache;
use sbe_latency_bot::gates::{TradingGate, WarmupGate};
use sbe_latency_bot::logging;
use sbe_latency_bot::processor;
use sbe_latency_bot::reset;
use sbe_latency_bot::rings::{Rings, RingsHandle};
use sbe_latency_bot::universe::{self, UniverseHandle};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config = Config::load();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);

    let initial_universe = match universe::build_universe(config, clock.as_ref()).await {
        Ok(universe) => {
            tracing::info!(
                "[BOOT] universe size={} shards={}",
                universe.symbols.len(),
                universe.shards.len()
            );
            universe
        }
        Err(err) => {
            tracing::error!("failed to build universe: {err:?}");
            universe::Universe {
                generated_at: config.ksa_now(clock.as_ref()),
                symbols: Vec::new(),
                symbol_index: HashMap::new(),
                shards: Vec::new(),
            }
        }
    };

    let filter_seed = initial_universe.filter_seed();
    let filter_cache = FilterCache::new(filter_seed, config.transport.rest_base_url)?;
    let exec_guards = ExecGuards::with_binance_defaults();

    let initial_rings = Rings::for_universe(&initial_universe);
    let universe_handle = UniverseHandle::new(initial_universe);
    let rings_handle = RingsHandle::new(initial_rings);

    let trading_gate = Arc::new(TradingGate::new());
    trading_gate.disable();
    let warmup_gate = Arc::new(WarmupGate::new(clock.clone()));
    warmup_gate.arm_for(Duration::from_secs(config.strategy.warmup_secs));

    tracing::info!(
        "[BOOT] gates trading_enabled=false warmup_secs={}",
        config.strategy.warmup_secs
    );

    tracing::info!(
        "trigger config: trigger_pct={:.6} window_ms={} queue_max_age_ms={}",
        config.trigger.trigger_pct,
        config.trigger.window.as_millis(),
        config.backpressure.max_queue_age.as_millis()
    );

    let server_spec = config.active_server_spec();
    let shard_assignments = config.sharding.shard_assignments(server_spec);
    assert!(
        shard_assignments.len() <= config.sharding.max_shards,
        "configured shard count {} exceeds SBE_MAX_SHARDS {}",
        shard_assignments.len(),
        config.sharding.max_shards
    );

    let (log_tx, log_rx) = bounded(config.channel.log_capacity);
    let log_handle = logging::spawn_log_aggregator(config, log_rx);

    let mut shard_receivers = Vec::with_capacity(shard_assignments.len());
    let mut shard_handles = Vec::with_capacity(shard_assignments.len());

    for assignment in shard_assignments {
        let (market_tx, market_rx) = spsc_channel(config.channel.market_capacity);
        let handle = data_feed::spawn_shard_reader(config, assignment, market_tx, log_tx.clone());
        shard_receivers.push((assignment, market_rx));
        shard_handles.push(handle);
    }

    let (trigger_tx, trigger_rx) = spsc_channel(config.channel.trigger_capacity);

    let (processor_thread, processor_handle) = processor::spawn_processor(
        config,
        server_spec,
        clock.clone(),
        universe_handle.clone(),
        rings_handle.clone(),
        trading_gate.clone(),
        warmup_gate.clone(),
        shard_receivers,
        trigger_tx,
        log_tx.clone(),
        #[cfg(feature = "test-mode")]
        None,
    );
    let risk_handle = processor_handle.risk_handle();

    processor_handle.arm_warmup(config.strategy.warmup_secs);
    let warmup_delay = config.strategy.warmup_secs;
    let processor_for_warmup = processor_handle.clone();
    tokio::spawn(async move {
        sleep(Duration::from_secs(warmup_delay)).await;
        processor_for_warmup.enable_trading();
    });

    let (execution_thread, execution_handle) = execution::spawn_execution(
        config,
        server_spec,
        clock.clone(),
        trigger_rx,
        log_tx.clone(),
        filter_cache.clone(),
        exec_guards.clone(),
        risk_handle.clone(),
        #[cfg(feature = "test-mode")]
        None,
    );

    reset::start_reset_scheduler(
        config,
        clock.clone(),
        processor_handle.clone(),
        execution_handle.clone(),
    );

    for handle in shard_handles {
        let join_res = tokio::task::spawn_blocking(move || handle.join())
            .await
            .context("failed to join shard thread")?;
        join_res.map_err(|_| anyhow::anyhow!("shard thread panicked"))?;
    }

    let processor_join = tokio::task::spawn_blocking(move || processor_thread.join())
        .await
        .context("failed to join processor thread")?;
    processor_join.map_err(|_| anyhow::anyhow!("processor thread panicked"))?;

    let execution_join = tokio::task::spawn_blocking(move || execution_thread.join())
        .await
        .context("failed to join execution thread")?;
    execution_join.map_err(|_| anyhow::anyhow!("execution thread panicked"))?;

    let log_join = tokio::task::spawn_blocking(move || log_handle.join())
        .await
        .context("failed to join log thread")?;
    log_join.map_err(|_| anyhow::anyhow!("log thread panicked"))?;

    Ok(())
}

fn init_tracing() {
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .expect("failed to create env filter");

    let fmt_layer = fmt::layer().with_ansi(false).with_target(false).compact();

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();
}
