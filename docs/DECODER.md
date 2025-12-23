# Official SBE Decoder

## Layout

- `schemas/binance_sbe.xml` — pinned copy of the Binance **Spot** schema (`spot_3_1.xml` @ `bb13c52`).
- `schemas/binance_stream_sbe.xml` — pinned copy of the Binance **Stream** schema (`stream_1_0.xml` @ `bb13c52`).
- `native/generated/` — C++ headers emitted by `sbe-tool 1.31.0` + `agrona 1.21.1` (see `native/VERSIONS.txt`).
- `native/include/bsbe_bridge.h` / `native/src/bsbe_bridge.cc` – thin bridge that converts the generated structures into the flat `BsbeTrade`.
- `src/ffi.rs` / `src/decoder_sbe.rs` – safe Rust FFI wrapper and streaming decoder.
- `tests/data/*.bin` – short binary captures that are replayed in CI.

The bridge is compiled once per build through `cmake` and linked statically into the Rust crate.

## Regenerating the C++ bindings

1. Download a JRE (any 17+ build works, e.g. Temurin JRE) and place it on your `PATH`.
2. Download `sbe-tool` and `agrona` jars that match the versions listed in `native/VERSIONS.txt`.
3. Re-run the generator from the repo root for **both** schemas:

   ```powershell
   $env:SBE_JAR = ".tools\sbe-tool-1.31.0.jar"
   $env:AGRONA_JAR = ".tools\agrona-1.21.1.jar"
   $java = Join-Path $env:JAVA_HOME "bin\java.exe"
   foreach ($schema in @("schemas/binance_sbe.xml", "schemas/binance_stream_sbe.xml")) {
     & $java `
       "-Duser.language=en" "-Duser.country=US" `
       "-Dsbe.target.language=cpp" `
       "-Dsbe.output.dir=native/generated" `
       "-cp" "$env:SBE_JAR;$env:AGRONA_JAR" `
       "uk.co.real_logic.sbe.SbeTool" `
       $schema
   }
   ```

4. Update `native/VERSIONS.txt` with the new schema/tool commits (spot + stream).
5. Commit both `native/generated/` and `native/VERSIONS.txt`.

## Numeric scales and side mapping

The official schema exposes signed mantissas plus a runtime exponent for price and quantity. The bridge normalises every trade to a fixed `1e-8` scale before it crosses the FFI boundary:

```
px_e8  = mantissa_price * 10^(priceExponent + 8)
qty_e8 = mantissa_qty   * 10^(qtyExponent   + 8)
```

The Rust side converts back to `f64` by dividing by `1e8`. `BsbeTrade::side` is derived from `isBuyerMaker` (`0` = buy, `1` = sell) and `trade_id` is passed through so the existing sequence tracking keeps working.

## Updating fixtures

Binary fixtures live in `tests/data`. To refresh them after a schema or capture change:

1. Pipe new raw frames (concatenated SBE messages) into the helper so they can be written to disk:

   ```powershell
   Get-Content path\to\capture.bin -Encoding Byte | `
     cargo run --bin capture_sbe -- tests/data/sbe_sample_01.bin
   ```

2. Keep the files short (dozens of frames max) so they remain quick to replay in CI.
3. Re-run `cargo test --release` to make sure both decoder and fixtures stay in sync.

## Full update checklist

- [ ] Schema file bumped under `schemas/`.
- [ ] `native/generated/` re-generated with the pinned toolchain and committed.
- [ ] `native/VERSIONS.txt` updated.
- [ ] Fixtures refreshed and tests adjusted if the schema changed in a breaking way.
- [ ] CI (`.github/workflows/ci.yml`) still runs `cargo test --release`.
