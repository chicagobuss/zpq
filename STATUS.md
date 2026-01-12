# ZPQ Project Status

**The Fastest Serverless Parquet Engine.**

## 🟢 Current Focus: Columnar Re-architecture
We are rewriting the core engine to use columnar batches and vectorized decoding, targeting Polars-competitive performance.

## 📊 Benchmarks (Latest - 100MB Parquet)
*   **Local -> S3**:
    *   **AWS CLI**: 6.84s (Raw Copy)
    *   **Polars**: 7.64s (Parallel Arrow)
    *   **ZPQ**: 13.49s (Serial Row-based)
*   **Bottleneck**: 35% Parquet encoding, 20% memcpy.

## 🗺️ Roadmap
*   **[COMPLETE] Phase 1: Read Dominance**
    *   Zero-Copy Page Reading, Async S3 Source.
*   **[COMPLETE] Phase 2: Write Dominance**
    *   Multipart S3 Sink, Parallel Row Group buffering.
*   **[ACTIVE] Phase 3: High Performance Architecture**
    *   **[WEEK 1]** Columnar Batch Container & Vectorized Reader.
    *   **[WEEK 2]** SIMD Filter Evaluation.
    *   **[WEEK 3]** Parallel Row Group Pipeline.
    *   **[WEEK 4]** Zero-Copy Fast Path (The Polars-Killer).
*   **[PLANNED] Phase 3: SQL Layer**
    *   Arrow C Data Interface
    *   SQLite VTable Integration

## 🛠️ Known Issues
*   Memory usage during massive parallel writes needs tuning (backpressure is working but aggressive).
*   "Fast Path" logic is designed but not yet wired into `main.zig`.

## 🧠 Knowledge Base
See `.agent/rules/` for the Tier 1-3 operating manuals.
