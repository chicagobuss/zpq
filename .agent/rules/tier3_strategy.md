---
trigger: always_on
---

# Tier 3: The Current Strategy

**Phase: v2 sans-IO foundation, then S3-to-S3 dominance.**

## Where we are

The `v2-sans-io` branch is the active rewrite. It started by deleting the bulk of the pre-rewrite codebase — most of the old `src/core/*` and `src/io/*` is gone. What remains is a minimal skeleton that compiles cleanly on Zig 0.16.0 with two binary targets and one shared core surface:

```
src/
  zpq.zig            # Pure namespace — exports core.* and io.*
  core/              # Sans-IO logic. Currently: schema.zig, thrift.zig.
  io/                # I/O strategies. Currently: strategy.zig (MemoryReader).
  cli/main.zig       # Workstation binary (zpq).
  lambda/main.zig    # Lambda bootstrap binary (zpq-lambda).
```

Both binaries compile and run as placeholders. No real Parquet logic flows through them yet — that's the next milestone.

## Strategic goal (unchanged from before the rewrite)

Saturate the network with **S3-to-S3** workloads. ZPQ wins by doing the absolute minimum required, then doing it in parallel. The architecture has three layers:

1.  **Parallel S3 Sink** (the "Write Dominance" piece):
    *   **Morsel-driven**: processing units are sized to fit the multipart upload chunk size.
    *   **Bounded channel**: producers block when the sink is full — backpressure, not buffering.
    *   **8 concurrent multipart uploads** by default.
    *   **Async task pool**: serialization runs off the network-I/O thread.

2.  **Zero-Copy "Fast Path"**:
    *   When the query is `SELECT *` (no filter, no projection), ZPQ should look like `cp` — never decode Parquet, just stream the raw bytes from Source to Sink.
    *   The planner detects "identity transform" and dispatches directly to a byte-streaming path.

3.  **Late-materialization decode path** (when the query *does* need decoding):
    *   Filter pushdown evaluates predicates on the encoded representation when possible.
    *   Column projection skips entire column chunks at the I/O level.
    *   Row-group statistics prune before any bytes are read.

## Tactical priorities (in order)

1.  **Port `core/` modules into the new tree.** `schema.zig` and `thrift.zig` already survived the cleanup. The Parquet decoder, RLE/dict, and snappy/zstd glue need to come back as pure sans-IO modules with no allocator beyond what callers pass in.
2.  **Build out `io/` with explicit backend split.**
    *   `io/epoll.zig` — Lambda baseline. The Lambda binary imports only this.
    *   `io/iouring.zig` — CLI-only optimization. Excluded by `build_options.lambda` at compile time.
    *   `io/s3.zig` — SigV4 + HTTP/1.1 + BoringSSL, transport-agnostic.
    *   `io/sink.zig` — multipart upload coordinator (8 concurrent parts, bounded channel).
    *   `io/source.zig` — S3 + local file source.
3.  **Wire the planner.** Strict separation: `Planner` (decision — does this query need decoding? what columns? what predicates?) returns a plan. `Executor` (action) consumes the plan and runs it via the I/O strategy. Keep them in different files.
4.  **Lambda binary excludes io_uring.** Already enforced by the build system; preserve as we add code.
5.  **Re-introduce libxev for the CLI hot path.** Currently the CLI binary doesn't import libxev because the vendored fork's io_uring backend doesn't compile against Zig 0.16.0. Fix is either patching the fork or bumping to a newer commit. Do this before any CLI hot-path work — without it, the CLI is a placeholder.

## What was deferred

*   **DELTA_BINARY_PACKED, DELTA_BYTE_ARRAY, LZ4, GZIP** — needed for compatibility with Spark/PyArrow output. Tracked in `docs/COMPARISON_TO_HARDWOOD.md` as Milestone 1.
*   **Arrow C Data Interface** — for SQL-layer integration. Post-decode-path completeness.
*   **`std.http` for the Lambda runtime API loop** — current probe binary uses raw socket helpers; once the v2 Lambda binary needs the runtime API, evaluate whether `std.Io.Net` is mature enough or whether we keep the hand-rolled HTTP/1.1.

## Reference docs
*   `docs/tier_3_s3_pipeline_strategy.md` — detailed S3-to-S3 orchestration plan (the north star for this phase).
*   `docs/lambda_capabilities.md` — empirical probe of what Lambda actually allows.
*   `docs/COMPARISON_TO_HARDWOOD.md` — feature-completeness map for encoding/codec coverage.
