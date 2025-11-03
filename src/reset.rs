use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::time::sleep;
use tracing::{error, info};

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

    info!("[RESET] start epoch={}", epoch);

    processor.reset_daily_state()?;
    execution.reset_metrics_daily()?;
    info!("[RESET] metrics cleared");

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

    info!(
        "[RESET] done symbols={} warmup={}",
        symbol_count, warmup_secs
    );

    Ok(())
}
#[cfg(feature = "test-mode")]
pub async fn perform_daily_reset_for_tests(
    config: &'static Config,
    clock: &dyn Clock,
    processor: &ProcessorHandle,
    execution: &ExecutionHandle,
) -> Result<()> {
    perform_daily_reset(config, clock, processor, execution).await?;
    processor.risk_handle().clear_disables_for_tests();
    Ok(())
}
