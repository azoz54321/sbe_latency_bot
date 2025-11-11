use std::sync::{
    atomic::{AtomicBool, AtomicU8, Ordering},
    Arc,
};
use std::time::Duration;

use anyhow::Context;
use tokio::runtime::Handle;
use tokio::time::sleep;

use crate::clock::Clock;
use crate::config::Config;
use crate::gates::{TradingGate, WarmupGate};
use crate::processor::ProcessorHandle;
use crate::universe;

const MISMATCH_THRESHOLD: u8 = 3;
const SCHEMA_WARMUP_SECS: u64 = 60;

#[derive(Clone)]
pub struct SchemaGuardHandle {
    inner: Arc<SchemaGuardInner>,
}

pub struct SchemaGuard {
    inner: Arc<SchemaGuardInner>,
}

struct SchemaGuardInner {
    paused: AtomicBool,
    mismatch_counter: AtomicU8,
    rebuilding: AtomicBool,
    trading_gate: Arc<TradingGate>,
    warmup_gate: Arc<WarmupGate>,
    processor: ProcessorHandle,
    config: &'static Config,
    clock: Arc<dyn Clock>,
    runtime: Handle,
}

impl SchemaGuard {
    pub fn new(
        config: &'static Config,
        clock: Arc<dyn Clock>,
        trading_gate: Arc<TradingGate>,
        warmup_gate: Arc<WarmupGate>,
        processor: ProcessorHandle,
        runtime: Handle,
    ) -> Self {
        let inner = Arc::new(SchemaGuardInner {
            paused: AtomicBool::new(false),
            mismatch_counter: AtomicU8::new(0),
            rebuilding: AtomicBool::new(false),
            trading_gate,
            warmup_gate,
            processor,
            config,
            clock,
            runtime,
        });
        Self { inner }
    }

    pub fn handle(&self) -> SchemaGuardHandle {
        SchemaGuardHandle {
            inner: self.inner.clone(),
        }
    }
}

impl SchemaGuardHandle {
    pub fn is_paused(&self) -> bool {
        self.inner.paused.load(Ordering::Acquire)
    }

    pub fn record_mismatch(&self) {
        if self.inner.paused.load(Ordering::Acquire) {
            return;
        }
        let count = self.inner.mismatch_counter.fetch_add(1, Ordering::AcqRel) + 1;
        if count >= MISMATCH_THRESHOLD {
            self.inner.begin_rebuild();
        }
    }
}

impl SchemaGuardInner {
    fn begin_rebuild(self: &Arc<Self>) {
        if self.paused.swap(true, Ordering::AcqRel) {
            return;
        }
        if self.rebuilding.swap(true, Ordering::AcqRel) {
            return;
        }
        self.mismatch_counter.store(0, Ordering::Release);

        let this = self.clone();
        self.runtime.spawn(async move {
            let guard = this.clone();
            let rebuild_guard = guard.clone();
            if let Err(err) = rebuild_guard.rebuild().await {
                tracing::error!("[SCHEMA] rebuild failed: {err:?}");
                guard.processor.enable_trading();
                guard.trading_gate.enable();
                guard.warmup_gate.arm_for(Duration::from_secs(0));
                guard.rebuilding.store(false, Ordering::Release);
                guard.paused.store(false, Ordering::Release);
            }
        });
    }

    async fn rebuild(self: Arc<Self>) -> anyhow::Result<()> {
        tracing::warn!("[SCHEMA] mismatch detected, pausing decode");

        self.trading_gate.disable();
        self.processor.force_watch_only();

        self.warmup_gate
            .arm_for(Duration::from_secs(SCHEMA_WARMUP_SECS));
        self.processor.arm_warmup(SCHEMA_WARMUP_SECS);

        let universe = universe::build_universe(self.config, self.clock.as_ref())
            .await
            .context("rebuilding universe for schema change")?;

        self.processor
            .apply_schema_swap(universe)
            .context("applying processor schema swap")?;

        let processor = self.processor.clone();
        self.runtime.spawn(async move {
            sleep(Duration::from_secs(SCHEMA_WARMUP_SECS)).await;
            processor.enable_trading();
        });

        self.paused.store(false, Ordering::Release);
        self.rebuilding.store(false, Ordering::Release);
        Ok(())
    }
}
