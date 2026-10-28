# AGENTS.md

Instructions for coding agents working on zpq.

## Stance

- Grumpy elitism: skepticism over optimism, honesty over salesmanship. Critique our own work ruthlessly.
- Taco bell programming: zpq is one small Unix tool that does one job well and pipes into the next. The job is
  Parquet: query, filter, aggregate and rewrite it, locally or on S3, and emit NDJSON, CSV or Parquet. No joins
  (yet), no dataframe API (yet).

## Before you build something

For any new primitive, wire-format feature, allocator change or parallelism change:

1. Read the pinned Zig 0.17.0 stdlib for what you need (`std.Io`, `std.MultiArrayList`, `std.bit_set`, ...). Don't
   guess APIs from memory.
2. Read how a comparable does it, in `references/` (git-ignored): Hardwood, DuckDB, Polars, parx.
3. Check the Parquet spec (parquet-format 2.14.0) or the AWS docs for format and service edge cases. Don't trust
   recall.

## Correctness

- Every Parquet file zpq writes must read back in PyArrow and DuckDB. `bytes_out > 0` is not validation.
- Never report a speedup on outputs you haven't validated.

## Tests

Gates are build.zig steps (tools/check/README.md). A tier passes only on clean receipts for HEAD: nothing failed,
nothing skipped.

| tier | command | when |
|---|---|---|
| edit | `zig build check -Dtier=edit [-Dtest-filter=X]` (`just test-core`) | while editing |
| commit | `zig build check -Dtier=commit` (`just check`) | before every commit; mirrors CI |
| full | `zig build check -Dtier=full` (`just full`; `tools/check/fleet.sh` across hosts) | before a release |

The full tier is the one that catches wire-format bugs: the `apache/parquet-testing` corpus, ClickBench, path parity,
OS contracts, libzpq and the architecture check.

## Performance

- Measure with `python -m bench run --host H --lane cold|hot --prev ZPQ_PREV` (bench/README.md): zpq against DuckDB
  2.0 and Polars 2.0, a fresh process per query, the previous build as a same-host control.
- Run before and after a change, on the same host and with the same filters. Keep both records.
- An INCOMPLETE or UNCERTIFIED lane proves nothing. Re-run it on a quiet host.
- Profile with `just flamegraph <zpq args>` (frame pointers are on). For runs under 100 ms, loop or glob to get
  enough samples. The flamegraph picks the target.

## Code rules

- No dataset-, benchmark- or value-specific constants in execution heuristics. Derive facts from the input at run
  time and verify them before skipping work.
- Size batches and partitions from the runtime cache sizes (`src/core/hw.zig`), with forced-path controls for test
  coverage. Hardware choices must not change output bytes; floating-point reductions keep their canonical order.
- Every allocation a query holds draws from the memory ledger (`src/core/memory.zig`) before it allocates.
- Source lines: 120 columns, by convention (`zig fmt` doesn't wrap). Human-facing output (`src/cli/help.txt`, errors)
  wraps at 80. Never put line breaks in JSON output format strings.
- Comments say why, not what: invariants, lifetimes, determinism, and the bug a line guards against. Keep them short.

## Docs

- The name is lowercase **zpq** in prose. "Parquet" is capitalized. Write "incorrect", not "wrong".
- Repo docs are durable reference only: no status snapshots, session logs or roadmaps.

## Git

- No `Co-Authored-By` trailers unless asked.
- Commit bodies explain why; the diff shows what.
- Don't push or open PRs without explicit approval.
