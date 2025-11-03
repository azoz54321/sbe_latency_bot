sbe_latency_bot
================

Ultra-low-latency Binance SBE spot buyer targeting ≤15 ms p95 tick→send under a 361-symbol load.  
This iteration implements sharded SBE WebSocket ingestion, trigger detection, execution, and daily latency reporting with all runtime knobs centralized in `src/config.rs`.

Key Goals
---------
- Market BUY immediately when a symbol rallies ≥5 % within 1 s; sell logic is deferred.
- Enforce a hard latency budget: drop any trigger that cannot reach the execution thread within 15 ms of receipt.
- Cover 361 USDT symbols by sharding the SBE multi-stream feed (15 shards → 8 on Server A, 7 on Server B) with explicit CPU pinning.
- Persist all operational settings in a single immutable `Config` surface (`src/config.rs`); no environment variables or scattered constants.

Config Primer
-------------
- `Config::load()` exposes a read-only singleton populated from constants in `src/config.rs`.
- Transport credentials, SBE stream roster, shard size, CPU affinity, execution mode, quote sizing, and guardrails live here.
- To tweak deployments or activate `ExecutionMode::Live`, edit `src/config.rs` and rebuild—no other module reads configuration.

Build & Run
-----------
```bash
# From the repo root
cargo build --release

# Run the shadow trading loop (default is SHADOW mode)
cargo run --release
```

Architecture Overview
---------------------
| Component | Threads / placement | Notes |
|-----------|---------------------|-------|
| SBE ingest (`data_feed`) | One Tokio current-thread runtime per shard (`ws-shard-{id}`), pinned via `CPU_PIN_PER_SHARD` | Auto-builds multi-stream URLs, injects `X-MBX-APIKEY`, tracks ping RTT, reconnects when p95 ping >200 ms for 30 s, and drops newest frames if the lock-free queue saturates while recording queue-drop metrics. |
| Processor (`processor`) | One per server (`processor-ServerA` / `processor-ServerB`), pinned to config-specified core | Busy-spins across SPSC shard queues, maintains 1 s ring buffers per symbol, raises triggers when Δ≥5 %, and refuses triggers that breach the latency budget or execution back-pressure. |
| Execution (`execution`) | One per server (`execution-ServerA` / `execution-ServerB`) | Persistent TCP_NODELAY `reqwest` client. In SHADOW mode it timestamps only; LIVE mode (opt-in via config) signs MARKET BUY orders with the REST API key/secret. |
| Logging & metrics (`logging`) | Single `log-aggregator` thread | Batches human logs, aggregates queue/trigger/ping metrics, emits warnings when shard p95 exceeds 15 ms, and produces a daily per-shard/server/global summary table. |

Latency & Safety Guards
-----------------------
- **Tick→send budget:** 15 ms hard cap. Triggers that miss the window are dropped with structured metrics.
- **Queue hygiene:** Consumers discard ticks older than 150 ms; producers flag queue saturation events as `QueueDropMetric` samples.
- **Ping monitoring:** If shard WebSocket ping p95 stays above 200 ms for 30 s, the shard reconnects automatically with a warning log.
- **Hot shard alarms:** A 5-minute moving window drives latency alerts. Breaching 15 ms p95 raises “shrink shard” warnings; crossing 18 ms p95 issues “SPLIT SHARD NOW”. (Trigger throttling hooks are ready for future deployment controls.)
- **Daily roll-up:** Logs tick→send p50/p95/p99, ping p50/p95, trigger/dropped counts per shard/server plus global totals. Targets (p50≤6 ms, p95≤15 ms, p99≤20 ms) are reiterated in the summary for quick validation.

Deployment Notes
----------------
- 361 symbols are pre-split into 15 shards (size 25, last shard shorter) based on alphabetical ordering of active USDT pairs.
- Server A handles shards 0–7; Server B handles shards 8–14. Each server also pins a dedicated processor and execution thread.
- `USE_MULTI_SERVER` stays `true` to reflect the two-server production plan; switch `deployment.active_server` in config when building the binary for Server B.
- Start in SHADOW mode for 24–72 h to gather daily reports. If any shard p95 exceeds 15 ms, adjust shard size, move symbols, or add hardware before enabling `ExecutionMode::Live` with real keys.

Outstanding Work
----------------
- Integrate Binance’s official generated SBE decoder in `src/sbe.rs`.
- Wire an execution throttle that honours the “SPLIT SHARD NOW” guard by pausing triggers until re-balanced.
- Persist metrics beyond process lifetime (e.g., to disk or telemetry backend) once latency targets are satisfied.
- Expand execution sizing logic beyond the fixed quote size after LIVE smoke tests succeed.

Troubleshooting
---------------
- **WebSocket 404 / auth issues**: Confirm the `BINANCE_SBE_API_KEY` constant and endpoint in `config.rs` (Binance occasionally changes the SBE route). The code logs explicit reasons and retries with capped exponential backoff.
- **Queue drops**: Watch for `latency` logs referencing `queue_drops`; consider shrinking `SBE_SHARD_SIZE` or rebalancing symbols if drops persist.
- **Daily report gaps**: The log aggregator only emits summaries when samples exist. If a shard shows “no samples collected”, verify the shard’s stream list and WebSocket connectivity.
