# Grand Context & Plan: The Consolidation

## Objective
Radically simplify project documentation into a cohesive, tiered knowledge base. eliminate the sprawl of `.txt`, `.md`, and random `PLAN_*.md` files. Establish a clear "Base Camp" for future architectural expeditions.

## Checkpoint (Fork Point)
**Commit**: `716c001` (branch: `clean-slate-review`)
**Date**: 2026-01-11
**State**: S3/Local benchmarking verified, `just bpftrace-bench` added, flamegraph methodology (wrapper scripts) established, Polars comparison baseline set.

To revert to this checkpoint:
```bash
git checkout 716c001
```

## 1. The New Structure (`.agent/rules/`)

We will enforce a 3-Tier Rule System.

### Tier 1: The Soul (`tier1_soul.md`)
**What is ZPQ?**
-   **Mission**: The fastest, leanest Serverless Parquet Engine.
-   **Core Philosophy**: **Laziness**. Do nothing until forced. Decode nothing unless requested. Copy nothing if you can borrow.
-   **The Stack**: Zig (Bleeding Edge 0.16), libxev (Sans-IO Event Loop), BoringSSL (Raw Crypto), AWS S3 (Native Protocol).
-   **The Mindset**: "Grumpy Elitism." We assume libraries are broken. We prove claims with benchmarks. We don't use SDKs.

### Tier 2: The Knowledge (`tier2_knowledge.md`)
**The Grimoire of Pain & Lessons**
-   **Zig 0.16 quirks**: `std.http` is dead to us. Use `std.ArrayListUnmanaged`. Allocators are explicit.
-   **libxev/BoringSSL**: Implicit state machines are traps. Explicit `flux` control. The "Memory BIO" pattern.
-   **Testing/Benchmarking**:
    -   *Never* compare `zpq` doing ETL vs `aws` doing Copy. Apples-to-Apples only.
    -   *Always* source `.env`.
    -   *Always* use `just build` (defaults to ReleaseFast) and `just` commands.
    -   Use `just build-debug` only when debugging with symbols needed.
-   **Performance Profiling (Flamegraphs)**:
    -   **When**: Use before major architectural changes (to find bottlenecks) and after (to verify fixes).
    -   **How**: Use the "Wrapper Script" pattern to preserve `.env` and `venv` under `sudo`.
    -   **Command**: `just bpftrace-bench <input> <output> <args>` for aggregate syscall stats.
    -   **Manual Perf**: `sudo perf record -F 997 -g -- /bin/bash /tmp/perf_wrapper.sh`.
    -   **What to expect**: Current `zpq` is CPU-bound by encoding (35%) and Memcpy (20%). Polars is bottlenecked by productive work (ZSTD compression).
-   **DNS/Networking**: Result of the DNS wars (The 3-tier resolver approach).
-   **Memory Management**: Track in-flight buffers (read_buf_ptr pattern). Always call `stop()` before connection teardown.

### The Meta-Process: Trace-Driven Development
We treat performance as a correctness constraint. Implementation follows this cycle:
1. **Predict**: Define expected thread, I/O, and memory traces *before* writing code.
2. **Instrument**: Capture real syscall/perf traces using `just bpftrace-bench` and `perf`.
3. **Validate**: Align the real execution with the prediction. Any divergence is a bug (even if output is correct).
4. **Iterate**: Refine until "Bad Waste" (Memcpy/Faults) is replaced by "Good Waste" (Compression/Compute).

### Tier 3: The Current Strategy (`tier3_strategy.md`)
**The expedition we are on RIGHT NOW.**
-   **Current Goal**: "Optimizer Dominance" (Unifying Fast & Slow Paths).
-   **Philosophy**: **Optimization via Subtraction**.
    -   We define a single pipeline (Executor -> RowGroupPipeline).
    -   The **Planner** subtracts steps (Decode, Filter, Encode) based on query semantics.
    -   **Identity Optimization**: For `SELECT *`, we detect that (Start, End) of columns are contiguous. We execute a single `CopyRange` op per RowGroup.
    -   **Subtractive Logic**:
        -   Start: "I must Decode RowGroup X".
        -   Check: "Do I need to filter?" No -> "Skip Filter".
        -   Check: "Do I need to modify?" No -> "Skip Decode".
        -   Result: "Copy Compressed Bytes".
-   **Architecture**:
    -   **Unified Executor**: Always used. Handles threading and S3 Prefetching (Main Thread) to avoid Event Loop conflicts.
    -   **RowGroupPipeline**:
        -   New `Config` struct determines mode (Scan vs Copy).
        -   `Copy Mode`: Reads contiguous chunks from MemorySource (pre-fetched), writes to Sink.
-   **Next Steps**:
    -   Remove `runFastPath` bifurcation in `engine.zig`.
    -   Enhance `RowGroupPipeline` to detect contiguous column chunks and perform bulk copy.
    -   Ensure `Executor` prefetching logic handles the raw byte ranges correctly.

## 2. Benchmark Results (100MB Parquet, ReleaseFast, 2026-01-11)

| Scenario | ZPQ | Polars | AWS CLI | Ratio (vs Polars) | Notes |
|----------|-----|--------|---------|-------------------|-------|
| Local → S3 | 13.49s | 7.64s | 6.84s | 1.76x slower | Polars uses parallel async threads |
| S3 → S3 | 20.46s | - | N/A | - | S3 read + decode/re-encode + S3 write |
| S3 → Local | 2.02s | - | N/A | - | Read-only path, very fast |

**Key Insight**: Polars achieves its speed via massive parallelism and optimized Arrow transformations. `zpq`'s Current bottleneck is row-by-row re-encoding and memory copies. The **Zero-Copy Fast Path** will allow `zpq` to **BEAT** Polars for `SELECT *` by streaming raw bits, which Polars cannot easily do.

### Methodology
- **Build**: `just build` (uses -Doptimize=ReleaseFast by default)
- **Data**: 100MB Parquet file, 524,288 rows, ~20 columns
- **ZPQ behavior**: Decodes ALL rows → `BenchmarkRow` struct (~18 columns) → re-encodes to Parquet → writes to sink
- **No filtering applied** (no `--filter` flag), all rows written
- **AWS CLI comparison**: Raw byte copy, no decoding (apples-to-oranges for ETL, but shows overhead)

**Key Insight**: The ~2x overhead vs AWS CLI is the cost of row-by-row decoding and re-encoding. The **Zero-Copy Fast Path** should eliminate this for `SELECT *` queries by streaming raw Parquet pages directly, targeting AWS CLI parity.

## 3. The Status (`STATUS.md`)
A single, living document tracking:
-   **Completed Milestones** (Read Dominance, Async Foundation, S3 Sink Stability).
-   **Current Active Task** (Fast Path Optimization).
-   **Future Roadmap** (SQL Layer, Arrow Interview).
-   **Latest Benchmark Numbers** (See table above).

## 3. The Purge
Once the above are created, we deleted:
-   `.cursor/`
-   `.claude/`
-   `hardening_plans/`
-   `BENCHMARK_RESULTS.md`
-   `MORSEL_ARCHITECTURE.md`
-   `PLAN_SQL_LAYER.md`
-   `step_back_architecture_01_05.md`
-   `STATUS_CURRENT_DETAIL.md`

## .cursor and .claude are now symlinks to .agent so rules stay in sync

## Execution Plan
1.  Create `.agent/grand_context_and_plan.md` (This file).
2.  Generate `.agent/rules/tier1_soul.md`.
3.  Generate `.agent/rules/tier2_knowledge.md`.
4.  Generate `.agent/rules/tier3_strategy.md`.
5.  Generate `STATUS.md`.
6.  Delete legacy files.
