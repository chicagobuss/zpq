# Grand Context & Plan: The Consolidation

## Objective
Radically simplify project documentation into a cohesive, tiered knowledge base. eliminate the sprawl of `.txt`, `.md`, and random `PLAN_*.md` files. Establish a clear "Base Camp" for future architectural expeditions.

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
    -   *Always* use `just` commands.
-   **DNS/Networking**: Result of the DNS wars (The 3-tier resolver approach).

### Tier 3: The Current Strategy (`tier3_strategy.md`)
**The expedition we are on RIGHT NOW.**
-   **Current Goal**: "Write Dominance" (S3 Parallel Sink).
-   **Architecture**:
    -   **Morsel-Driven parallel processing** (Go-style concurrency in Zig).
    -   **S3 Multipart Uploads** (8 concurrent parts).
    -   **Zero-Copy Pass-Through** (The "Fast Path" for `SELECT *`).
-   **Next Steps**:
    -   Implement the logic to bypass the `ParquetReader` when filtering is null.
    -   Refactor `main.zig` to use a `Planner` (Decider) vs `Executor` (Doer).

## 2. The Status (`STATUS.md`)
A single, living document tracking:
-   **Completed Milestones** (Read Dominance, Async Foundation).
-   **Current Active Task** (Parallel Sink Verification, Fast Path Optimization).
-   **Future Roadmap** (SQL Layer, Arrow Interview).
-   **Latest Benchmark Numbers** (The 100MB Scan results).

## 3. The Purge
Once the above are created, we DELETE:
-   `.cursor/`
-   `.claude/`
-   `hardening_plans/`
-   `BENCHMARK_RESULTS.md`
-   `MORSEL_ARCHITECTURE.md`
-   `PLAN_SQL_LAYER.md`
-   `step_back_architecture_01_05.md`
-   `STATUS_CURRENT_DETAIL.md`

## Execution Plan
1.  Create `.agent/grand_context_and_plan.md` (This file).
2.  Generate `.agent/rules/tier1_soul.md`.
3.  Generate `.agent/rules/tier2_knowledge.md`.
4.  Generate `.agent/rules/tier3_strategy.md`.
5.  Generate `STATUS.md`.
6.  Delete legacy files.
