# Regression harness

Locks in current behavior so we can refactor without silent perf or correctness regressions. Three things asserted per
scenario:

1. **Result envelope** (timing fields stripped, paths normalized) matches committed golden in `golden/<name>.json`.
2. **Median wall time** stays within `baseline.json`'s `max_ms`.
3. **Peak RSS** stays within `max_rss_kb`. Lambda paths also assert `max_internal_ms` (response's `total_ms`) which
   strips network jitter.

Each scenario runs N times (default 5); we take the median to dampen first-call jitter while still catching consistent
slowdowns.

## Run

```bash
just bench-regression                  # CLI scenarios only (default)
just bench-regression --lambda         # also invoke deployed zpq-filter-r2
just bench-regression --filter glob    # only run scenarios with "glob" in name
just bench-regression --runs 10        # more samples for stable medians
```

Lambda scenarios need `R2_BUCKET` in env (`source .env`) and the `zpq-filter-r2` function deployed.

## When to update goldens / baseline

After an *intentional* behavior change — new feature, planned perf improvement, accepted output-shape change. The flow:

```bash
# Make your change, verify it's correct.
just test                              # unit tests pass
just bench-regression                  # see the failures, decide if intentional

# If intentional, capture the new state and commit.
just bench-regression-update --lambda  # writes new golden/*.json + baseline.json
git add benchmarks/regression/golden benchmarks/regression/baseline.json
git commit -m "..."
```

Don't commit baseline updates that came from "I think it ran slower because my laptop was hot." Re-run a few times with
`--runs 10` to get a stable median first.

## What's in the suite

| Scenario | What it covers |
|---|---|
| `cli_count_star_1file` | Stat short-circuit on a single mmap'd file. Should be ~ms; catches regressions that turn this into a decode path. |
| `cli_count_max_sum_1file` | Single-file aggregate with full decode (writer doesn't populate stats for these cols). Catches decode-loop slowdowns. |
| `cli_filtered_1file` | Filter eval folded into the decode pass + aggregate. Catches filter-eval regressions. |
| `cli_count_star_glob` | Multi-file metadata-only path. Catches regressions in glob expansion + per-file footer-parse. |
| `cli_count_max_sum_glob` | Multi-file aggregate via threaded per-file decode + `Accumulator.merge`. The most direct regression target for the upcoming Source/Storage refactor. |
| `cli_filtered_glob` | Multi-file filter+aggregate; full multi-file decode path. |
| `lambda_aggregate` | Real R2 round-trip + lambda decode. Catches network-stack regressions and lambda-specific bugs (libzstd link, env-var resolution, etc.). |
| `lambda_compaction` | Multi-file write path: parallel meta-fetch, schema validation, range fetch, decode+filter+encode, multipart upload. The biggest single chunk of `lambda/main.zig` that the refactor will lift out. |

Notably **not** in the suite:

- **Cold-start init time** — measured separately by `just probe-lambda`; re-run it after changes to lambda init code
  (libzstd, BoringSSL loader, etc.). Putting it in the regression harness would require forcing AWS to scale the
  function down between samples, which is fragile.
- **Microbench for isolated decode throughput** — the per-scenario `total_ms` and `decode_ms` proxies for this; if we
  want a number that's *just* decode rate we'd need an instrumented binary.
- **Cross-impl correctness** — that's `tools/conformance.py` against apache/parquet-testing.

## Fixtures

`data/benchmark_100mb.parquet` is the canonical single-file fixture (committed; 155 MB, 524 288 rows, 4 row groups,
mixed column types). For multi-file scenarios, the harness symlinks 4 copies into `benchmarks/regression/.tmp_glob/`
(gitignored) so glob queries see a 4-file dataset without 4× the disk.

## Tolerances

- Wall time: golden median × 1.3 + 5 ms (covers warm-cache jitter).
- RSS: peak × 1.25 + 1 MB (Linux's RSS measurement is noisy).
- Lambda internal: median × 1.2 + 50 ms (lambda-side scheduling jitter).

If your system is quieter or noisier than the one the baseline was captured on, re-capture with `just
bench-regression-update`. The absolute numbers aren't load-bearing; the *ratio* between current and baseline is.
