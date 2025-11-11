# إرشادات Copilot — sbe_latency_bot
## أمثلة وسوم
[BOOT] [HARAM] [SHADOW] [BUY] [SELL]

> Quickstart: build cargo build --release  safe run cargo run --release --features shadow-mode  schema change  cargo clean && cargo build --release  وسوم السجلات ثابتة: [BOOT][HARAM][SHADOW][BUY][SELL]  Reset 03:00 KSA  Universe: USDT فقط مع استبعاد UP/DOWN/BULL/BEAR + HARAM.
# ط¥ط±ط´ط§ط¯ط§طھ Copilot â€” sbe_latency_bot
# Copilot Instructions â€” sbe_latency_bot

Goal: Help AI coding agents make safe, productive changes to a low-latency Binance Spot SBE streamer/trigger bot implemented primarily in Rust with a native C/C++ SBE decoder built via CMake and wired through FFI.

## Big Picture (what to know before you edit)
- Architecture:
  - Ingest: WebSocket (Binance SBE trade streams) â†’ native SBE decoder â†’ zero-copy structs â†’ Rust event loop.
  - Pipeline: Decode â†’ queue/window â†’ spike trigger (e.g., â‰¥X% within Y ms) â†’ (shadow/live) action â†’ structured logging `[BOOT] [HARAM] [SHADOW] [BUY] [SELL]`.
  - Modes: shadow-mode logs decisions only; live mode places orders (requires enabling in `src/config.rs`).
  - Daily reset: 03:00 Asia/Riyadh; rebuild universe, clear daily bans, reset counters.
  - Universe: Binance Spot USDT pairs with filters (excludes `UP/DOWN/BULL/BEAR`, plus a configurable `haram.list`).

Why this structure: Hard real-time constraints (target sub-15ms tickâ†’send) â€” parsing is offloaded to native code and Rust keeps the fast event loop minimal.

## Developer Workflows
- Build (Rust + native decoder):
  - `cargo build --release` (build script runs CMake for native decoder).
  - Optional features: build with `--features prod` or `--features shadow-mode` as used in CI/README.
- Run examples (prefer shadow):
  - `cargo run --release --features shadow-mode`
  - On boot look for logs: `[HARAM] loaded ... total=<N>` and `[BOOT] universe size=<N> shards=<M>` and a config echo like `cfg spike=0.50000% window_ms=1000 tp=20.00% sl=1.00%`.
- Tests & quick checks:
  - `cargo test --release`
  - Run capture helper for fixtures (see `docs/DECODER.md`): `cargo run --bin capture_sbe -- tests/data/sbe_sample_01.bin`.

## Regenerating / Updating the SBE Decoder
- Source of truth: `schemas/binance_sbe.xml`. Do not hand-edit generated decoder code.
- When XML changes: regenerate `native/generated/` with the exact `sbe-tool`/`agrona` versions in `native/VERSIONS.txt`, update that file, refresh `tests/data/` fixtures, and run `cargo test --release`.
- If CI/IDE skips native builds: `cargo clean && cargo build --release` to force a native rebuild.

## Conventions & Patterns
- Logging tags: `[BOOT]`, `[HARAM]`, `[SHADOW]`, `[BUY]`, `[SELL]` â€” ops scripts parse these; keep them stable.
- Time: use monotonic clocks for window math; printed timestamps are UTC.
- Symbol filters: exclude leveraged tokens (`UP/DOWN/BULL/BEAR`) and manage bans via `config/haram.list`.
- Sharding: deterministic shard assignment (see `src/config.rs` and `[BOOT] shards=<N>` logs). Maintain O(1) allocation.
- Resets: daily rebuild at 03:00 KSA clears daily bans and resets loss gates.

## Integration Points
- WebSocket: `wss://stream-sbe.binance.com/...` (SBE trade topics).
- Native bridge: `native/` + `build.rs` + `native/CMakeLists.txt`; Rust FFI wrappers are in `src/ffi.rs` and `src/decoder_sbe.rs`.
- Config surface: `src/config.rs` centralizes shard maps, CPU pins, ExecutionMode, spike/TP/SL, and queue ages.

## Safe Change Checklist (for AI agents)
- âœ… Change Rust event/window/trigger logic and logging. Prefer small, focused edits and unit tests.
- âœ… Update universe/filter logic (`config/haram.list`, shard map) but preserve semantics.
- âڑ ï¸ڈ When changing `schemas/*.xml`: regenerate native code, commit `native/generated/` and `native/VERSIONS.txt`, refresh fixtures, and run full tests.
- âڑ ï¸ڈ If renaming logging tags/fields, update any downstream log parsers or scripts that search for tags (search repo for `[BOOT]`, `[HARAM]`, `[SHADOW]`, `[BUY]`, `[SELL]`).
- â‌Œ Donâ€™t hand-edit generated SBE files in `native/generated/`.

## Examples (what to search for in logs / code)
- Boot: `INFO [BOOT] universe size=383 shards=4`.
- Shadow config echo: `[SHADOW] cfg spike=0.50000% window_ms=1000 tp=20.00% sl=1.00%`.
- WS connect: `connecting WS url=wss://stream-sbe.binance.com/...` then `connected shard 0`.

---

If youâ€™re an AI agent starting a task:
1) Build with `--release` to ensure native decoder is present.
2) Run in `shadow-mode`, confirm logs & trigger math.
3) Modify trigger/universe code paths only after validating shadow behavior; keep logging tags stable.
4) For schema changes, regenerate decoder and do a clean rebuild.

If any section is unclear or you want a 1-line quickstart or an expanded onboarding checklist, tell me which part to adjust.






