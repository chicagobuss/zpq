# 02 — Lossless DECIMAL on the re-encode path (W3b)

**Status:** design (spiked 2026-06-18), implementation pending.

## Problem

Reading and *compacting* (byte-copy) decimals is already lossless. The gap is
the **re-encode path** — whenever a write goes through the encoder instead of a
byte-copy (a filter is present, a computed column, or `--select`). There a
DECIMAL column is decoded to `f64` (`decimal.decodeColumnAsF64`) and written
back out as **DOUBLE**, and `engine.coerceDecimalLeavesToDouble` rewrites the
footer to match. So `zpq query cur.parquet --filter "cost > 0" -o out.parquet`
turns a `DECIMAL(38,8)` into a `DOUBLE`: the logical type is lost and f64 can't
represent every 38-digit value exactly.

This is the `corpus_diff --mode write` "DECIMAL lost (… → DOUBLE)" finding.

## Why it's the architecture call

The runtime value lanes are *physical*: `i32 / i64 / f32 / f64 / string / bool`
(`filter_eval.Batch.Column`, `consumer.OutputAggregator.ColumnBuf`). Decimal has
no lane — on decode it's forced into `f64` so it can reuse the existing
filter/agg/expr evaluators with no new plumbing. Lossless re-encode needs the
*unscaled integer* to survive to the encoder.

## Spike findings (2026-06-18)

Three throwaway spikes (removed after measuring):

1. **Format truth (pyarrow).** pyarrow writes *all* decimals as
   `FIXED_LEN_BYTE_ARRAY` by default, but reads an **INT64-physical** decimal
   back as exact `DECIMAL` too. So emitting INT32/INT64 (precision ≤ 9 / ≤ 18)
   or FLBA(16) (≤ 38) with the DECIMAL logical annotation all round-trip in
   pyarrow and DuckDB.

2. **Encoder closed-loop (Zig).** Feeding the *existing* `encodeColumn` an `i64`
   column whose `schema_elem` keeps `logical_type = DECIMAL` writes
   `ColumnMetaData.type = INT64` and round-trips back through `decodeColumnAsF64`
   to the exact values. **So precision ≤ 18 needs ZERO new encoder code** — only
   the aggregator's decode + schema-capture change.

3. **Lane cost (ReleaseFast, 4M values, ns/value).**

   | path | ns/val | vs base |
   |---|---|---|
   | decode f64+scale (today) | 4.10 | — |
   | decode i128 raw (proposed) | 4.98 | 1.2× (perf-neutral) |
   | encode DOUBLE 8B (today) | 2.50 | — |
   | encode INT64 8B (≤18 lane) | 0.73 | 0.3× (cheaper) |
   | encode FLBA16 (>18 lane) | 5.76 | 2.3× (money case only) |

   i128 decode is perf-neutral; INT64 re-encode is *cheaper* than DOUBLE (and
   DELTA-compresses far better); FLBA(16) costs 2.3× but only for precision > 18
   and is still ~170 M values/s — nowhere near the snappy-bound hot path.

4. **Blast radius.** ~74 `switch`-arm sites over the column variants live in
   `filter/eval.zig`, `expr/eval.zig`, `expr/agg.zig`. Making decimal a
   first-class lane *in the evaluators* would touch all of them — large
   regression surface on the most-tested code.

## Decision: an **output-only** i128 lane

Keep filter/agg/expr **untouched** — decimals still decode to `f64` in the
shared batch, so the 74 evaluator sites and their semantics don't move (no
regression risk; decimal *filtering/aggregation* keep today's documented
f64-precision behavior). Add the lossless i128 lane **only** on the
passthrough → encoder path:

- `OutputAggregator.ColumnBuf` gains a `decimal` arm: `i128` values +
  def-levels + `{scale, precision, physical, byte_width}` (the source
  `decimal.Kind`).
- `decimal.zig` gains `decodeColumnAsI128` (the unscaled integers — no
  scale-apply), mirroring `decodeColumnAsF64`.
- `initOutputAggregator`: for a **passthrough** DECIMAL spec, build the decimal
  buf and **keep the source DECIMAL `SchemaElement`** (drop the DOUBLE synthesis).
- `appendProjectedRG`: for a `.passthrough` decimal, decode the chunk to i128,
  apply the selection vector, append to the decimal buf. (Decimals that are
  *also* a filter/expr input still decode to f64 in the batch — a second, cheap
  decode; see Follow-up.)
- `encoder.zig` gains a `.decimal` input arm: emit the unscaled integer by the
  **source physical type** — INT32/INT64 via the existing integer PLAIN/DELTA
  path (truncate i128 → i32/i64, exact for the precision), FLBA(n) via a new
  small big-endian PLAIN writer. `meta.type` = source physical type; the footer
  keeps the DECIMAL annotation.
- Remove `coerceDecimalLeavesToDouble` once decimals carry through.
- Computed expressions that *derive* a value from a decimal still output DOUBLE
  (unchanged) — only **passthrough** decimals are lossless.

### Scope ladder
- **D1** — INT32/INT64-backed decimals (precision ≤ 18). Reuses the integer
  encoder; the spike proves it works.
- **D2** — FLBA(16) / i128 decimals (precision ≤ 38; the `DECIMAL(38,8)` money
  case). Needs the new FLBA PLAIN writer.

### Verification
`corpus_diff --mode write` "DECIMAL lost" findings must go to **zero** across the
R2 corpus, and each output must read back value-exact under DuckDB + pyarrow
(the harness already does this). Plus a Zig unit test: i128/INT64/FLBA decimal
encode → decode round-trip.

## Follow-ups (not blocking)
- **Decode-once:** a passthrough decimal that isn't a filter/expr input is
  currently decoded twice (f64 for the batch, i128 for output). Skip the batch
  f64 decode for output-only decimals once we thread an "eval vs output" split.
- **Exact decimal filter/agg:** a future upgrade could carry i128 through the
  evaluators (the 74 sites) so `sum`/comparisons are exact, not f64. Out of
  scope here; the perf spike shows it's viable.
