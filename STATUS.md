# ZPQ Project Status

**The Fastest Serverless Parquet Engine.**

## 🟢 Current Focus: Parallel S3 Writer
We are validating the robust parallel S3 sink and implementing the "Fast Path" for pass-through queries.

## 📊 Benchmarks (Latest)
*   **100MB Scan (S3 -> Local)**: ~5ms column fetch (ZPQ) vs ~260ms (PyArrow).
*   **100MB Upload (Local -> S3)**:
    *   **AWS CLI**: 6.86s (~14.5 MB/s) - *Baseline*
    *   **ZPQ (Parallel)**: 21.76s (~4.6 MB/s effective) - *Includes Parsing Overhead*
    *   *Next Goal*: Zero-Copy Fast Path to match AWS CLI.

## 🗺️ Roadmap
*   **[COMPLETE] Phase 1: Read Dominance**
    *   Zero-Copy Page Reading
    *   Async S3 Source
    *   Simd Bit-Unpacking
*   **[ACTIVE] Phase 2: Write Dominance**
    *   Multipart S3 Sink
    *   Parallel Part Uploads
    *   **[NEXT]** Pass-Through "Fast Path" (Zero-Decode)
*   **[PLANNED] Phase 3: SQL Layer**
    *   Arrow C Data Interface
    *   SQLite VTable Integration

## 🛠️ Known Issues
*   Memory usage during massive parallel writes needs tuning (backpressure is working but aggressive).
*   "Fast Path" logic is designed but not yet wired into `main.zig`.

## 🧠 Knowledge Base
See `.agent/rules/` for the Tier 1-3 operating manuals.
