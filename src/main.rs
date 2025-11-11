use anyhow::Context;
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use crossbeam_channel::bounded;
use std::collections::{BTreeSet, HashMap};
use std::sync::{atomic::AtomicU64, Arc};
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use sbe_latency_bot::channels::spsc_channel;
use sbe_latency_bot::clock::{Clock, SystemClock};
use sbe_latency_bot::config::{Config, ShardAssignment};
use sbe_latency_bot::data_feed::{self, ShardHandle};
use sbe_latency_bot::execution::{self, ExecGuards};
use sbe_latency_bot::filters::FilterCache;
use sbe_latency_bot::gates::{TradingGate, WarmupGate};
use sbe_latency_bot::logging;
use sbe_latency_bot::private_stream::UserStreamService;
use sbe_latency_bot::processor;
use sbe_latency_bot::reset;
use sbe_latency_bot::rings::{Rings, RingsHandle};
use sbe_latency_bot::strategy::shadow;
use sbe_latency_bot::universe::{self, load_haram_list, SymbolKey, UniverseHandle};

const SHADOW_SHARD_SIZE: usize = 120;
const HARAM_LIST_PATH: &str = "config/haram.list";
const KSA_TZ: Tz = chrono_tz::Asia::Riyadh;

#[derive(Copy, Clone, Debug)]
enum RefreshEvent {
    Reset03,
    Refresh15,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config = Config::load();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);

    let initial_universe = match universe::build_universe(config, clock.as_ref()).await {
        Ok(universe) => {
            tracing::debug!(
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

    tracing::debug!(
        "[BOOT] gates trading_enabled=false warmup_secs={}",
        config.strategy.warmup_secs
    );

    tracing::debug!(
        "trigger config: trigger_pct={:.6} window_ms={} queue_max_age_ms={}",
        config.trigger.trigger_pct,
        config.trigger.window.as_millis(),
        config.backpressure.max_queue_age.as_millis()
    );

    let haram_initial = load_haram_list(HARAM_LIST_PATH);
    log_haram_stats("[HARAM] loaded", &BTreeSet::new(), &haram_initial);
    let haram_state = Arc::new(RwLock::new(haram_initial.clone()));

    let shadow_universe = universe::get_usdt_spot_universe(config, Some(&haram_initial)).await?;
    tracing::info!("[BOOT] universe size={}", shadow_universe.len());
    let shard_chunks: Vec<Vec<SymbolKey>> = shadow_universe
        .chunks(SHADOW_SHARD_SIZE)
        .map(|chunk| chunk.to_vec())
        .collect();
    tracing::info!("[BOOT] shards={}", shard_chunks.len());
    assert!(!shard_chunks.is_empty(), "USDT shadow universe is empty");

    let server_spec = config.active_server_spec();
    assert!(
        shard_chunks.len() <= config.sharding.max_shards,
        "configured shard count {} exceeds SBE_MAX_SHARDS {}",
        shard_chunks.len(),
        config.sharding.max_shards
    );
    let cpu_fallback = *config
        .sharding
        .cpu_pin_per_shard
        .last()
        .expect("cpu pin config");
    let shard_plans: Vec<(ShardAssignment, Arc<Vec<SymbolKey>>)> = shard_chunks
        .into_iter()
        .enumerate()
        .map(|(idx, chunk)| {
            let cpu_core = *config
                .sharding
                .cpu_pin_per_shard
                .get(idx)
                .unwrap_or(&cpu_fallback);
            let names = chunk.iter().map(|key| key.name.clone()).collect::<Vec<_>>();
            let assignment = ShardAssignment {
                shard_index: idx,
                symbols: Arc::new(names),
                cpu_core,
            };
            (assignment, Arc::new(chunk))
        })
        .collect();

    let (log_tx, log_rx) = bounded(config.channel.log_capacity);
    let log_handle = logging::spawn_log_aggregator(config, log_rx);
    let _user_stream = UserStreamService::maybe_spawn(config, log_tx.clone());
    let (reconnect_tx, reconnect_rx) = crossbeam_channel::unbounded();

    let mut shard_receivers = Vec::with_capacity(shard_plans.len());
    let mut shard_handles: Vec<ShardHandle> = Vec::with_capacity(shard_plans.len());

    let (shadow_tx, shadow_rx) = crossbeam_channel::bounded(shadow::CHANNEL_CAPACITY);
    let shadow_drop_counter = Arc::new(AtomicU64::new(0));
    let _shadow_handle = shadow::spawn_worker(
        config,
        clock.clone(),
        shadow_rx,
        shadow_drop_counter.clone(),
    );

    let mut shard_sources = Vec::with_capacity(shard_plans.len());
    for (assignment, symbols) in shard_plans.iter() {
        let (market_tx, market_rx) = spsc_channel(config.channel.market_capacity);
        shard_receivers.push((assignment.clone(), market_rx));
        shard_sources.push((
            assignment.clone(),
            market_tx,
            shadow_tx.clone(),
            symbols.clone(),
        ));
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
        reconnect_rx,
        trigger_tx,
        log_tx.clone(),
    );
    let risk_handle = processor_handle.risk_handle();

    let runtime_handle = tokio::runtime::Handle::current();
    let schema_guard = data_feed::SchemaGuard::new(
        config,
        clock.clone(),
        trading_gate.clone(),
        warmup_gate.clone(),
        processor_handle.clone(),
        runtime_handle.clone(),
    );

    for (assignment, market_tx, strategy_tx, allow_symbols) in shard_sources.into_iter() {
        let handle = data_feed::spawn_shard_reader(
            config,
            assignment,
            market_tx,
            log_tx.clone(),
            schema_guard.handle(),
            reconnect_tx.clone(),
            strategy_tx,
            shadow_drop_counter.clone(),
            allow_symbols,
            runtime_handle.clone(),
        );
        shard_handles.push(handle);
    }

    let shard_contexts: Vec<_> = shard_handles.iter().map(|h| h.ctx.clone()).collect();
    spawn_universe_refresh_task(
        config,
        HARAM_LIST_PATH.to_string(),
        haram_state.clone(),
        shard_contexts.clone(),
    );

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
    );

    processor_handle
        .attach_execution_handle(execution_handle.clone())
        .expect("attach execution handle");

    reset::start_reset_scheduler(
        config,
        clock.clone(),
        processor_handle.clone(),
        execution_handle.clone(),
    );

    for handle in shard_handles {
        let join_res = tokio::task::spawn_blocking(move || handle.join.join())
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

fn spawn_universe_refresh_task(
    config: &'static Config,
    haram_path: String,
    haram_state: Arc<RwLock<BTreeSet<String>>>,
    shard_contexts: Vec<Arc<data_feed::ShardContext>>,
) {
    tokio::spawn(async move {
        loop {
            let now = Utc::now();
            let (event, at) = next_refresh_after(now);
            let secs = (at - now).num_seconds().max(0) as u64;
            tracing::info!(
                "[REFRESH] schedule=\"03:00 & 15:00 KSA\" next={} ev={:?}",
                at,
                event
            );
            sleep(Duration::from_secs(secs)).await;
            let haram_now = load_haram_list(&haram_path);
            let (added, removed) = {
                let mut guard = haram_state.write().await;
                let prev = guard.clone();
                let added = haram_now.difference(&prev).count();
                let removed = prev.difference(&haram_now).count();
                *guard = haram_now.clone();
                (added, removed)
            };
            tracing::info!(
                "[HARAM] refresh loaded={} added={} removed={} total={}",
                haram_now.len(),
                added,
                removed,
                haram_now.len()
            );
            if let Err(err) = run_universe_refresh(config, &shard_contexts, event, &haram_now).await
            {
                tracing::error!("[REFRESH] ev={:?} err={:?}", event, err);
            }
        }
    });
}

async fn run_universe_refresh(
    config: &'static Config,
    shard_contexts: &[Arc<data_feed::ShardContext>],
    event: RefreshEvent,
    haram: &BTreeSet<String>,
) -> anyhow::Result<()> {
    if shard_contexts.is_empty() {
        return Ok(());
    }

    let event_name = match event {
        RefreshEvent::Reset03 => "03:00",
        RefreshEvent::Refresh15 => "15:00",
    };

    let universe = universe::get_usdt_spot_universe(config, Some(haram)).await?;
    let total = universe.len();
    let now_ksa = Utc::now().with_timezone(&KSA_TZ);
    let label = now_ksa.format("%Y-%m-%dT%H:%M").to_string();

    tracing::info!(
        "[REFRESH] {} KSA event={} new_universe_size={}",
        label,
        event_name,
        total
    );

    let desired: BTreeSet<SymbolKey> = universe.into_iter().collect();
    let mut current_map = HashMap::new();
    let mut current_counts = Vec::with_capacity(shard_contexts.len());

    for (idx, ctx) in shard_contexts.iter().enumerate() {
        let symbols = ctx.snapshot_symbols().await;
        current_counts.push(symbols.len());
        for key in symbols {
            current_map.insert(key.name.clone(), idx);
        }
    }

    let mut assignments = vec![BTreeSet::new(); shard_contexts.len()];
    for key in desired.iter() {
        if let Some(&idx) = current_map.get(&key.name) {
            assignments[idx].insert(key.clone());
        } else {
            let (idx, _) = current_counts
                .iter()
                .enumerate()
                .min_by_key(|(_, count)| **count)
                .expect("non-empty shard list");
            assignments[idx].insert(key.clone());
            current_counts[idx] += 1;
        }
    }

    let mut added = 0;
    let mut removed = 0;
    let mut kept = 0;
    for (idx, ctx) in shard_contexts.iter().enumerate() {
        let stats = ctx.apply_universe_diff(assignments[idx].clone()).await;
        added += stats.added;
        removed += stats.removed;
        kept += stats.kept;
    }

    tracing::info!(
        "[REFRESH] {} KSA event={} added={} removed={} kept={} total={}",
        label,
        event_name,
        added,
        removed,
        kept,
        total
    );

    Ok(())
}

fn next_refresh_after(now_utc: DateTime<Utc>) -> (RefreshEvent, DateTime<Utc>) {
    use chrono::{Datelike, TimeZone};
    let now_ksa = now_utc.with_timezone(&KSA_TZ);
    let (y, m, d) = (now_ksa.year(), now_ksa.month(), now_ksa.day());
    let mk = |hour| {
        KSA_TZ
            .with_ymd_and_hms(y, m, d, hour, 0, 0)
            .single()
            .expect("valid reset time")
            .with_timezone(&Utc)
    };
    let t03 = mk(3);
    let t15 = mk(15);
    let mut candidates = Vec::new();
    if t03 > now_utc {
        candidates.push((RefreshEvent::Reset03, t03));
    }
    if t15 > now_utc {
        candidates.push((RefreshEvent::Refresh15, t15));
    }
    if let Some(next) = candidates.into_iter().min_by_key(|(_, ts)| *ts) {
        return next;
    }
    let tomorrow = now_ksa
        .date_naive()
        .succ_opt()
        .expect("valid tomorrow date");
    let next = KSA_TZ
        .with_ymd_and_hms(tomorrow.year(), tomorrow.month(), tomorrow.day(), 3, 0, 0)
        .single()
        .expect("valid next reset")
        .with_timezone(&Utc);
    (RefreshEvent::Reset03, next)
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

fn log_haram_stats(label: &str, prev: &BTreeSet<String>, current: &BTreeSet<String>) {
    let added = current.difference(prev).count();
    let removed = prev.difference(current).count();
    tracing::info!(
        "{} loaded={} added={} removed={} total={}",
        label,
        current.len(),
        added,
        removed,
        current.len()
    );
}
