# AGENTS.md — Working norms for this repo

Short, durable instructions for AI coding agents working on ZPQ.

## Ground every non-trivial decision in three things

Before writing more than ~50 LoC of new code or making a
load-bearing design choice, take five minutes for each of:

1. **Zig 0.16.0 stdlib & language features.** The release
   reorganised large parts of std (`std.posix`, `std.fs`,
   `std.Io`, `std.time`). Read the relevant module before
   re-implementing. Look for new primitives that match what
   you're about to build — `std.MultiArrayList`, `std.bit_set`,
   `std.Io.Threaded`, `std.Io.Group`, `std.Io.Queue`, etc. —
   and either use them or have a *specific* reason not to. Don't
   guess at APIs; inspect the pinned Zig stdlib from your local
   toolchain install.

2. **Comparables in `references/`.** Hardwood (Java),
   DuckDB (C++), Polars (Rust), parx (Rust). They've all hit
   the problem before. For a tricky case (concurrency model,
   nested decoding, predicate pushdown, footer thrift quirks)
   it is *almost always* faster to read 200 lines of one of
   them than to figure it out from spec. The hardwood pattern
   for nullable decode (Page → IntPage with parallel
   `definitionLevels` + `values`) is a recent example —
   ported in a few hours instead of designed from scratch.

3. **Light web research for best practices.** Especially for
   wire-format edge cases (parquet thrift IDL types vs runtime
   types, RLE/bit-packed-hybrid framing, definition-level
   semantics for nested), and for AWS-side behaviour
   (Lambda seccomp, S3 retry classes, SigV4 path encoding).
   The format docs are usually authoritative; mailing-list
   threads and parquet-mr issue tracker conversations fill in
   the "why is it this way" part. Don't trust LLM training-data
   recall on these — they drift.

If a change is "just plumbing" the three-step grounding can be
collapsed; if it's a load-bearing design choice (new primitive,
new wire-format support, allocator restructure, parallelism
model) it's mandatory.

## Don't trust correctness without verifying it

Wire-format engines are easy to get *almost right* in a way that
passes lenient readers and fails strict ones. ZPQ shipped a
broken `IntType.bitWidth` encoding for months because Polars was
lenient about it; pyarrow and DuckDB both rejected the output.

So:

- **Validate every parquet output with at least pyarrow + duckdb
  before claiming success.** "bytes_out > 0" is not the same as
  "valid parquet."
- **Run `apache/parquet-testing` corpus regularly** (wired into CI
  conformance). It surfaces unknowns you wouldn't have
  thought to test.
- **Never end a session with passing wallclock numbers but
  unvalidated outputs.** That's the trap that costs us the most.

### Test tiers (fastest first)

The workflow is tiered so the inner loop stays fast while cross-impl
validation still happens on a cadence. Don't skip the gauntlet before a
release.

- **Tier 1 — `just test`**: inner loop. Pure-Zig, no external deps,
  safety-checked. Known-output unit tests + the fuzz-lite PRNG loop.
  Seconds. Run constantly as you work. Checks our logic against
  *hand-known outputs* — fast, but self-referential by nature.
- **Tier 2 — `just check`**: before every commit. Tier 1 + lambda
  integration + the cross-impl smoke (ZPQ writes → pyarrow reads). Needs
  pyarrow. Mirrors CI.
- **Tier 3 — `just gauntlet`**: on a cadence and before releases. Fetches
  the `apache/parquet-testing` corpus into `data/parquet-testing` so the
  fixture-gated decode tests run against real Spark/foreign-writer files
  (the cross-impl decode validation), then the full suite + conformance +
  smoke. This is what catches the "almost right" wire-format bugs above —
  a Tier-1 self-round-trip will not.

## Documentation discipline

- Keep repository docs durable: design/reference notes, capability
  findings, measurements, and test truth.
- Avoid adding status snapshots, session journals, or forward-looking
  roadmap checklists to the public tree. If a note will go stale as soon
  as priorities change, keep it out of the repo.
- Other docs (`docs/<topic>_design.md`) are reference material edited
  when the underlying thing changes.

## The "Common 4" perf-tracking suite

`benchmarks/common_four.py` is the canonical end-to-end suite we
re-run every time we touch a hot path. Same four cells, same
fixtures, same query shapes — so wins/regressions show up in the
same shape across branches.

**The four cells:**

1. **`lambda_s3_to_s3`** — Lambda invocation that reads
   `benchmark_100mb.parquet` from S3, applies `int8 BETWEEN -10
   AND 10` + 3-column projection, writes back to S3 (the
   iceberg-compaction shape). Wall is end-to-end including invoke
   RTT.
2. **`local_from_s3`** — workstation `zpq query` against the same
   S3 file, decode-heavy aggregate (`count + sum(int64_sorted) +
   max(int8)`). No write.
3. **`local_from_local`** — same aggregate, against
   `data/benchmark_100mb.parquet` on local disk. Pure-decode
   ceiling — no network.
4. **`local_from_r2`** — workstation `zpq query` against
   `s3://${R2_BUCKET}/demo/nyc-taxi/yellow/yellow_tripdata_2023-01.parquet`
   on Cloudflare R2 (NYC taxi yellow, 47 MB / 3 M rows). Same
   shape aggregate but on the f64 fare/tip columns.

**Run it:**

```bash
just build
set -a; source .env; set +a   # creds + bucket names
python3 benchmarks/common_four.py --runs 5
# or to test a single cell:
python3 benchmarks/common_four.py --runs 5 --cells local_from_local
```

Output is TSV: `cell  min_ms  median_ms  p95_ms`. Saved to
`benchmarks/common_four_results.tsv` by default; pass `--out PATH`
to capture a baseline alongside a perf change.

**Required setup:**

- `.env` loaded with `AWS_S3_BUCKET`, `R2_*`, `LAMBDA_FUNCTION_NAME`.
- `data/benchmark_100mb.parquet` present locally.
- `s3://${AWS_S3_BUCKET}/zpq_test_data/benchmark/benchmark_100mb.parquet`
  uploaded.
- NYC taxi yellow_tripdata_2023-01 in R2 (or any S3-compat at
  `${R2_BUCKET}/demo/nyc-taxi/yellow/`).
- Lambda function `${LAMBDA_FUNCTION_NAME}` deployed with current
  binary, ≥1024 MB memory, ≥60 s timeout. (Re-deploy via
  `./tools/serverless/aws.sh deploy "$LAMBDA_FUNCTION_NAME"
  zig-out/lambda/zpq-lambda-arm64.zip arm64 1024` after any change
  to `src/lambda/` or `src/zpq.zig` shared core.)

**Discipline:**

- Always re-run **before AND after** a perf change; commit the
  baseline file alongside the change so the delta is on the record.
- AWS-side noise is real. If a single cell jumps and the others
  don't, suspect the network before suspecting the diff.
- Don't change query shapes between baseline and post-change runs.
  If you need a different shape for a specific investigation, add
  a *new* cell rather than mutating an existing one.

## Profiling

Frame pointers are on in both binaries (`omit_frame_pointer = false`
in `build.zig`); cost is in the noise. `just flamegraph <zpq args>`
records a perf trace with `--call-graph=fp -e cycles:u -F 4999`,
runs it through Brendan Gregg's stackcollapse + flamegraph (cloned
in `references/FlameGraph/`), and writes `prof/flame-<ts>.svg`.

```bash
just flamegraph query data/benchmark_100mb.parquet \
  --aggregate 'sum(int64_sorted) AS s, count(*) AS n'
```

Sub-100 ms runs are too short to sample — wrap in a 50× shell loop
or use a multi-file glob to get hundreds of samples. Open the SVG
in a browser; click frames to zoom. `just flamegraph-bpf <secs>`
attaches via `profile-bpfcc` for long-running processes (needs
sudo).

When picking a perf target, the flamegraph is the verdict. The
microbench harness (`just microbench`) is a complementary tool for
isolating per-(encoding × type × bit_width) decode cost from
glob/mmap noise.

## Tone and scope reminders

- The Tier 1 "grumpy elitism" mindset is real: skepticism over
  optimism, honesty over salesmanship, narrow primitive over
  bloated framework. When critiquing our own work, go ruthless —
  it's how we find what's actually broken.
- ZPQ is a **sharp narrow primitive** ("taco bell programming").
  Filter + project + S3-to-S3 passthrough on parquet. We don't
  compete with DuckDB on joins/aggs; we don't compete with
  Polars on dataframe ergonomics. We compete on cold-start,
  warm-pool throughput, and binary size *for our specific
  workload*.

## Git / PR conventions

- Don't add `Co-Authored-By` trailers unless the user asks.
- Commit message body explains *why*, not *what* — the diff has
  the what.
- Don't push or open PRs without explicit user approval.
