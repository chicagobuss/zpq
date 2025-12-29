# ZPQ SQL Layer: Implementation Plan

This document synthesizes research into "Headless OLAP" frontends for the ZPQ S3-native Parquet reader. It outlines a phased roadmap to transition ZPQ from a "Raw Scanner" to a "Serverless Query Engine."

## 1. The Core Constraints
To maintain ZPQ's performance lead in AWS Lambda, the SQL layer must adhere to:
*   **Binary Overhead**: < 5MB (Target: ~1.5MB total deployment size).
*   **Memory Model**: Zero-copy using the **Apache Arrow C Data Interface**.
*   **Architecture**: "Sans-I/O" Query Planning. ZPQ handles all S3/Network logic; the SQL layer handles only AST parsing, planning, and expression evaluation.
*   **Execution**: Columnar/Vectorized by default.

---

## 2. Research Synthesis: The Three Paths

| Path | Frontend | Binary Size | Performance | Verdict |
| :--- | :--- | :--- | :--- | :--- |
| **A: SQLite VTable** | SQLite (C) | ~1MB | Moderate (Row-based) | **Pragmatic Start**. Battle-tested parser and planner. Requires "Pointer-as-BLOB" hacks for SIMD. |
| **B: Frankenstein** | `libpg_query` (C) | ~3-4MB | High (Vectorized) | **The Holy Grail**. Uses the real Postgres parser but executes via ZPQ's native SIMD kernels. |
| **C: Custom DSL** | mecha/Zitron (Zig) | < 100KB | Maximum | **The Lean Niche**. Great for simple filters, but fails on complex JOINs/Aggregates. |

---

## 3. Implementation Roadmap

### Phase 1: The Foundation (The "Universal Glue")
**Objective**: Decouple the decoder from the consumer.
*   Implement the **Arrow C Data Interface** (stable ABI C-structs).
*   Refactor ZPQ to emit `ArrowArray` and `ArrowSchema` objects.
*   *Why*: This makes ZPQ "pluggable" with any external engine (SQLite, DuckDB, or custom Zig kernels).

### Phase 2: Pragmatic SQL (The "SQLite Bridge")
**Objective**: Immediate SQL-92 compatibility for Lambda.
*   Integrate SQLite as a static library via `zig cc`.
*   Implement a **Zig-native Virtual Table** (using `Stanchion` as a code pattern).
*   **Optimization (The Secret Sauce)**: 
    *   Use `xBestIndex` to map SQL `WHERE` clauses to Parquet **Row Group Pruning**.
    *   Implement "Vectorized Aggregates": pass pointers to Arrow buffers into custom SQLite functions (e.g., `ZPQ_SUM(col)`) to bypass row-at-a-time bottlenecks.

### Phase 3: The Holy Grail (The "Postgres-on-Zig" Engine)
**Objective**: Bare-metal performance with industry-standard syntax.
*   Link `libpg_query` to turn SQL strings into JSON ASTs.
*   Build a **Rule-Based Planner** in Zig that translates the AST to ZPQ scan operations.
*   Execute queries directly over Arrow buffers using ZPQ's **SIMD bit-unpacking kernels**.

---

## 4. Technical Hurdles & Risks
1.  **C-FFI Fragility**: Both SQLite and `libpg_query` introduce C dependencies. We must maintain our "BoringSSL-style" pre-built binary workflow to keep build times low.
2.  **Blocking I/O**: SQLite's VTable API is synchronous. We must ensure our `libxev` loop isn't stalled by long-running SQL queries.
3.  **The "Planner Trap"**: Writing a SQL planner is complex. We will prioritize "Track A" (SQLite) to leverage their 20 years of optimization before attempting "Track B".

---

## 5. Decision Log
*   **2025-12-29**: Initial research complete. Consolidated Claude (SQLite focus) and Gemini (`libpg_query` discovery). Agreed on a "Two-Track" approach starting with Arrow compatibility.

