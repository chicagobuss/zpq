# Type/operator perf — ZPQ vs DuckDB vs polars

Cross-engine wall-clock for the filter/aggregate surface across ZPQ's full
type set (decimal, string, date/timestamp, bool, IN). Two axes captured:
**cold** (full process invocation — ZPQ's serverless pitch) and **warm**
(import amortized — raw engine compute).

Reproduce: `just type-bench` (generates the fixture if absent, builds ZPQ
ReleaseFast, runs `benchmarks/type_bench.py`). Append a new dated block on each
meaningful run so this stays a history.

Column meanings (Tier-2 fairness — same file/queries/machine):
- `zpq`, `duckdb-cli` — native binaries, cold process, but startup is ~ms so
  the number ≈ compute.
- `polars-cold` — polars via a fresh python subprocess: pays interpreter +
  `import polars` (~100 ms) every time. The cold-start axis.
- `polars-warm` — polars in-process, import amortized = the Rust core's actual
  compute. (PyPolars wraps the same Rust kernels a standalone binary would
  call, so this *is* the native-compute number — no Rust toolchain needed.)

---

## 2026-06-13 — baseline

- Machine: Intel Core Ultra 9 386H, **16 logical CPUs**, Ubuntu 26.04.
- Engines: zpq @ Zig 0.16.0 ReleaseFast · DuckDB v1.5.3 (CLI) · polars 1.41.2 (py3.12).
- Fixture: `data/bench_types.parquet` — 5,000,000 rows, 68 MB, snappy, ~40 row
  groups. `id` INT32, `amt` DOUBLE, `price` DECIMAL(18,2) (INT64-backed),
  `name` UTF8 (1000 distinct), `d` DATE, `ts` TIMESTAMP(µs), `flag` BOOLEAN.
- Best of 5, wall-clock (ms):

| query                                  |     zpq | duckdb-cli | polars-cold | polars-warm |
|----------------------------------------|---------|------------|-------------|-------------|
| sum(price)  [decimal]                  |    52.5 |       19.7 |       146.7 |        10.4 |
| min/max(name)  [string]                |    49.9 |       17.7 |       127.5 |         9.1 |
| count WHERE d >= 2018  [date lit]      |    26.7 |       28.4 |       118.3 |         8.3 |
| count WHERE ts < 2015-06  [ts lit]     |    24.6 |       22.2 |       110.2 |         6.8 |
| sum(flag)  [bool agg]                  |    19.2 |       16.0 |       125.1 |         8.5 |
| count WHERE id IN (...)  [IN]          |    10.2 |       19.0 |       123.7 |         7.1 |
| sum(id)  [numeric baseline]            |    19.9 |       17.8 |       122.7 |         3.1 |

### Read

- **Cold-start: ZPQ dominates.** `polars-cold` is a flat ~110–147 ms wall (almost
  all Python + `import polars`); ZPQ answers in 10–52 ms with no runtime to spin
  up. This is exactly ZPQ's serverless thesis, and it holds clearly.
- **Warm compute: polars' Rust core is fastest** (3–10 ms), then `duckdb-cli`
  (16–28 ms), then `zpq` (10–52 ms). So once you strip the import tax, ZPQ is the
  slowest of the three on raw single-file compute — most on the decode-heavy
  `sum(price)` (52 vs polars 10) and `min/max(name)` (50 vs 9).
- **Two concrete levers for that warm gap, both honest:**
  1. **Single-file queries run on ONE thread.** `scan.zig`'s `n_workers =
     min(cpus, n_files)` → a single file = 1 worker, while polars uses all 16
     cores. That ~16× thread deficit is the biggest chunk of the warm gap. Lever:
     **row-group-level intra-file parallelism**.
  2. **Per-core decode throughput** on decimal/string (C4.y SIMD on the decode +
     accumulator loops) — the gap that remains after threading.
- **Crucial nuance — this does NOT dent the serverless story.** ZPQ's one-thread-
  per-file is *by design*: parallelism is the fan-out (N files across N Lambdas),
  where each worker handles one file and cold-start + minimal-work win. The warm
  single-file gap matters only for **long-running single-file CLI** use, not the
  S3-to-S3 fan-out ZPQ is built for.

**Takeaway:** the new filter/operator *semantics* carry no perf penalty (they
ride the existing int/f64/decode paths). The open perf levers, in order, are
**intra-file parallelism** (15 idle cores on a single-file query) then
**decimal/string decode SIMD** — re-measure both here after.

---

## Row-Group-Level Parallelism

Implemented intra-file parallelism: the scan work unit is now `(file, row_group)`,
not `(file)`, so a **single file saturates all cores** (was 1 worker). Same
machine/fixture/queries as baseline; best of 5, wall-clock (ms):

| query                                  |     zpq | duckdb-cli | polars-warm | (zpq before) |
|----------------------------------------|---------|------------|-------------|--------------|
| sum(price)  [decimal]                  |    16.0 |       20.6 |         8.1 |         52.5 |
| min/max(name)  [string]                |    14.2 |       13.8 |         9.1 |         49.9 |
| count WHERE d >= 2018  [date lit]      |    13.2 |       16.3 |         4.0 |         26.7 |
| count WHERE ts < 2015-06  [ts lit]     |    14.0 |       16.6 |         3.6 |         24.6 |
| sum(flag)  [bool agg]                  |     6.5 |       14.0 |         7.2 |         19.2 |
| count WHERE id IN (...)  [IN]          |    10.3 |       12.4 |         4.8 |         10.2 |
| sum(id)  [numeric baseline]            |    10.3 |       13.6 |         2.0 |         16.5 |

### Read

- **The big bite worked.** Single-file aggregates are 2–3.5× faster and now use
  every core. ZPQ **beats native duckdb-cli on most queries** (decimal, date,
  ts, bool, IN, numeric) and **beats polars-warm on bool agg** (6.5 vs 7.2).
- The "intra-file parallelism" lever is **closed** — single-file now
  scales like multi-file fan-out did.
- **Remaining gap to polars-warm is the per-core ceiling, not parallelism.**
  Scaling plateaus at ~4–8 workers (~14–20 ms wall) — bandwidth/decode-bound —
  and polars' Rust SIMD compute pulls ahead on the heaviest decode (decimal 8 ms
  vs 16) and pure numeric (2 ms vs 10). Closing that is the **decode-SIMD**
  lever (C4.y), not more threads.
- Tuning note: `-j 16` was marginally *slower* than `-j 8` (thread-spawn
  overhead past the bandwidth ceiling) — a worker-cap / persistent-pool tweak,
  not a blocker.

**Verified:** answers unchanged across all 15 duckdb cross-impl checks + 280
unit + 3 integration after the change.

---

## Decode SIMD: Where It Helps

Took the decode-SIMD lever to see what `@Vector` buys on the warm gap. First
finding: **the aggregate folds are already SIMD** (`simdSumF64`,
`simdMinMaxI64/F64` use `@Vector` + `@reduce`; `simdSumI64` keeps an i128 scalar
accumulator for overflow safety) and **PLAIN fixed-width decode is already a
`@memcpy`**. So the only clearly-scalar arithmetic left was the DECIMAL
scale-apply (one f64 divide per value). Vectorized it (`applyScaleSimd`: 8 raw
ints → f64 vector → one vector divide; bit-identical per lane).

Controlled A/B (same machine state, best-of-20, `sum(price)`):

| cores | scalar | SIMD | Δ      | note                                  |
|-------|--------|------|--------|---------------------------------------|
| -j1   |  50 ms | 47 ms| ~6%    | compute-bound, win visible            |
| -j2   |  30 ms | 28 ms| ~7%    | **Lambda's core count — win persists**|
| ~-j8  |  19 ms | 20 ms| ~0%    | overhead-bound, win washes out        |

Kept the change: ~6–7% on the decimal decode at the 1–2 CPU counts Lambda
actually runs (every tier reports 2 CPUs), bit-identical so DuckDB's exact
check stays green. On a 16-core workstation it's noise.

**Followup (2026-06-13 (d)) — perf-profiled the decode pipeline; snappy is the
floor.** Lowered `perf_event_paranoid` and captured a real CPU profile
(`task-clock`, 87k samples, saturated 400-file `sum(amt)` so no core sits idle).
Flat breakdown, on-CPU:

| function | % | meaning |
|---|--:|---|
| `snappy::DecompressBranchless` | **57%** | snappy decompression |
| `memcpy` | 7.5% | PLAIN decode → materialize |
| `scanRGForAgg` | 5.7% | the agg fold (already `@Vector`) |
| `HybridRleDecoder.decode` | 2.6% | def-level RLE |
| everything else (alloc, etc.) | <1% each | — |

This **rules out two levers**: decode→aggregate **fusion** can save at most the
7.5% memcpy (not worth the complexity), and more **arithmetic SIMD** is pointless
(the fold is 5.7% and already vectorized). The prize is snappy at 57%.

Tried the obvious snappy lever — **enabling its SSSE3/BMI2 intrinsic paths**
(target CPU features + `config.h` gate; the raw `-mssse3`/`-mbmi2` C flags get
overridden by Zig's target baseline, a real gotcha). Result: **zero wall-time
change** (42/25/1443 ms, identical to generic). `perf stat` explains why — IPC is
~3.3 with low cache-misses, but back-of-envelope the generic `DecompressBranchless`
is *already* doing ~1.6 GB/s (40 MB in ~24 ms), not the 250 MB/s the vendor
comment guessed. Google's generic branchless decompressor is already near-optimal;
intrinsics buy nothing here. **Reverted** (also: enabling BMI2 in the snappy
module would bake `bzhi` into the *Lambda* binary for no benefit — SIGILL risk).

**Net:** snappy decompress is a ~57% floor we can't cheaply lower. The only
remaining warm-path lever is **parallel efficiency** — the profile still showed
~15% idle even under a saturating workload, and scaling plateaus at ~4–8 workers
with a regression at -j16 (per-query thread-spawn tax → persistent worker pool).
ZPQ's cold-start axis (8–20 ms vs polars-cold's 95–110 ms) is unchallenged.

---

## 2026-06-13 (e) — apples-to-apples: the warm "5×" gap was mostly measurement

The warm table above compares **ZPQ's full CLI process** against **polars
in-process** (persistent thread pool, no startup). That's the right framing for
the *cold* axis but NOT for raw compute — it silently charges ZPQ for two things
unrelated to decode speed. Controlled re-measure on `sum(amt)` (the same snappy
DOUBLE column, best-of-12/15):

| measurement                          | polars | ZPQ          | ratio |
|--------------------------------------|-------:|-------------:|------:|
| **per-core** (1 thread, same column) | 30.6ms | 42 (−3 startup ≈ **39**) | **1.3×** |
| **multi-core compute** (strip startup)| 6.3ms | 13 (−3 ≈ **10**) | **1.6×** |
| scaling, 1→16 thread                 | 4.85×  | 3.9×         | ZPQ's ~15% idle |

Pure ZPQ process startup = **3 ms** (exec/link/init); polars-warm pays none of it.
For a 2 ms polars query (`sum(id)`), ZPQ's *startup alone* exceeds polars' whole
runtime — which is most of how a real ~1.3× per-core gap got reported as "5×".

**Conclusions:**
- **polars is NOT meaningfully faster at snappy.** Both run ~1.6 GB/s; the whole
  per-core pipeline (decompress+decode+sum) is ~1.3× apart, not 5×. The 57%
  snappy floor is shared, not a ZPQ-specific deficit.
- **Projection is correct (verified):** `sum(id)` [20 MB i32 col] = 15 ms vs
  `sum(amt)` [40 MB f64 col] = 42 ms — time tracks the *queried* column's size,
  not the 68 MB file. ZPQ decompresses only what the query touches; if it
  decompressed all columns these would be equal. (Apples-to-apples confirmed.)
- The honest residual gap is ~1.3–1.6×, from (a) ZPQ's per-query process startup
  — irrelevant to the serverless thesis, where cold-start IS the workload and ZPQ
  wins ~6× — and (b) ~20% worse parallel scaling (the persistent-pool lever).
- **Doc hygiene:** the headline warm table should be read as "full CLI process vs
  polars compute," not a like-for-like engine-compute comparison. `type_bench.py`
  has no in-process ZPQ mode (it's a CLI), so warm numbers carry an unavoidable
  startup tax; the single-thread / startup-adjusted figures here are the fair
  engine-compute comparison.

**The warm gap is the decode pipeline, not arithmetic.**
Single-thread cost breakdown (best-of-20, -j1):

| query                         | bytes | -j1   | isolates                       |
|-------------------------------|-------|-------|--------------------------------|
| `count(id)`                   |   0   |  3 ms | fixed overhead floor           |
| `sum(id)`   [INT32]           | 20 MB | 15 ms | i32 decode + sum               |
| `sum(amt)`  [DOUBLE, no scale]| 40 MB | 42 ms | f64 decode + (SIMD) sum        |
| `sum(price)`[DECIMAL + scale] | 40 MB | 48 ms | + scale-apply (6 ms of the 48) |

`sum(amt)` is 42 ms of pure decode for one 40 MB column (~1 GB/s) with an
already-vectorized sum — i.e. the cost is **snappy decompress + column
materialization**, not math. That's the real lever behind polars-warm's lead,
and it's *not* more SIMD. Candidates, in rough order: (1) **decompress
throughput** (snappy is the single biggest line item), (2) **decode→aggregate
fusion** to skip materializing the 40 MB f64/i64 intermediate, (3) a
**persistent worker pool** — scaling is only ~2–3× on 8 cores and *regresses* at
-j16 (per-query thread-spawn tax). None of these are arithmetic SIMD.
