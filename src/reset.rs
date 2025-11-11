use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::time::sleep;
use tracing::{debug, error};

use crate::clock::Clock;
use crate::config::Config;
use crate::execution::ExecutionHandle;
use crate::processor::ProcessorHandle;
use crate::universe;

pub fn start_reset_scheduler(
    config: &'static Config,
    clock: Arc<dyn Clock>,
    processor: ProcessorHandle,
    execution: ExecutionHandle,
) {
    tokio::spawn(async move {
        if let Err(err) = reset_loop(config, clock, processor, execution).await {
            error!("reset scheduler terminated: {err:?}");
        }
    });
}

async fn reset_loop(
    config: &'static Config,
    clock: Arc<dyn Clock>,
    processor: ProcessorHandle,
    execution: ExecutionHandle,
) -> Result<()> {
    let now = config.ksa_now(clock.as_ref());
    let todays_reset = config.todays_reset_time_ksa(now);
    if now >= todays_reset {
        if let Err(err) = perform_daily_reset(config, clock.as_ref(), &processor, &execution).await
        {
            error!("daily reset failed: {err:?}");
        }
    }

    loop {
        let now = config.ksa_now(clock.as_ref());
        let next = config.next_reset_time_ksa(now);
        let sleep_duration = match (next - now).to_std() {
            Ok(duration) => duration,
            Err(_) => Duration::from_secs(0),
        };

        sleep(sleep_duration).await;

        if let Err(err) = perform_daily_reset(config, clock.as_ref(), &processor, &execution).await
        {
            error!("daily reset failed: {err:?}");
        }
    }
}

async fn perform_daily_reset(
    config: &'static Config,
    clock: &dyn Clock,
    processor: &ProcessorHandle,
    execution: &ExecutionHandle,
) -> Result<()> {
    processor.force_watch_only();
    let epoch = processor.bump_epoch();

    debug!("[RESET] start epoch={}", epoch);

    processor.reset_daily_state()?;
    execution.reset_metrics_daily()?;
    debug!("[RESET] metrics cleared");

    {
        let rest_base = config.transport.rest_base_url;
        let haram = &config.strategy.haram_symbols;
        match reqwest::Client::builder()
            .user_agent("sbe-latency-bot/spot-usdt-count")
            .build()
        {
            Ok(http) => match universe::count_usdt_spot_symbols(&http, rest_base, haram).await {
                Ok(n) => debug!("[RESET] usdt_spot_count={}", n),
                Err(err) => debug!("[RESET] usdt_spot_count=error err={}", err),
            },
            Err(err) => debug!("[RESET] usdt_spot_count=error err={}", err),
        }
    }

    let universe = universe::build_universe(config, clock).await?;
    let filter_seed = universe.filter_seed();
    let symbol_count = universe.symbols.len();
    processor.install_universe_and_rings(universe);
    execution.install_filters(filter_seed);

    let warmup_secs = config.strategy.warmup_secs;
    processor.arm_warmup(warmup_secs);

    let processor_clone = processor.clone();
    tokio::spawn(async move {
        sleep(Duration::from_secs(warmup_secs)).await;
        processor_clone.enable_trading();
    });

    debug!(
        "[RESET] done symbols={} warmup={}",
        symbol_count, warmup_secs
    );

    Ok(())
}
