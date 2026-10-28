# Profiling a query

Moved from the README for 0.5.0. The full guide is [profiling.md](profiling.md).

`--profile` writes a per-thread trace of a query that opens in [Perfetto](https://ui.perfetto.dev): spans for every
phase (footer fetch and parse, planning and pruning, each S3 request with its time to first byte, each column chunk's
decode and each page's decompression, filter, aggregation, encode and write), counters for requests and bytes in flight
and RSS, and the run's metadata. `--profile-summary` prints the digest to stderr: time per phase, each phase's share of
the wall time where phases overlap, the costliest column chunks, I/O and pruning counts.

```bash
zpq query data/lineitem.parquet --aggregate 'sum(l_extendedprice * l_discount) AS revenue' \
  --filter "l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'" \
  --profile trace.json --profile-summary
```

The Lambda takes `"profile": true` (summary in the response) and `"profile_url": "s3://..."` (full trace uploaded).
Off, profiling costs a branch per span site; see [docs/profiling.md](profiling.md) for what each span means,
how the wall shares are computed, the measured cost, and a walkthrough.

Frame pointers are on in both binaries, so `perf record --call-graph=fp` gives usable stacks; `just flamegraph <zpq
args>` writes a flamegraph SVG (the contributor notes in `AGENTS.md` cover it and `just flamegraph-bpf`).
