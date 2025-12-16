## Phase 3: Benchmarking & Optimization (Detail)

### Benchmarking Strategy
We compare `zpq` against two industry standards:
1.  **PyArrow**: The standard Python binding for Apache Arrow (C++), highly optimized for data science.
2.  **Rust (`parquet` crate)**: The official Rust implementation, using the `arrow` feature for vectorized reading.

### Workload
*   **Data**: `data/skyway-export-00002.snappy.parquet` (Multiple row groups, Snappy compression, Dictionary encoding).
*   **Operation**: "Scan" (Read all pages, decompress, decode values, count totals).
*   **Hardware**: Apple M1 Max.

### Results (2024-12-15)

| Implementation | Tool / Method | Time (s) | Throughput (MB/s) | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **ZPQ (Zig)** | `zig build run ... scan` | **0.1601s** | **842.92** | Zero-copy page iteration, manual Snappy. |
| **PyArrow** | `pyarrow.parquet.read_table` | 0.2919s | 372.54 | C++ backend, highly optimized. |
| **Rust** | `ParquetRecordBatchReader` | 0.6500s | ~167.00 | Safe Arrow reader, overhead of Batch construction. |
| **Rust (CLI)** | `parquet-read` | 20.142s | ~5.00 | Formatting/Printing overhead dominates. |

### Analysis
*   `zpq` is **~1.8x faster** than PyArrow.
*   `zpq` is **~4x faster** than the safe Rust Arrow reader.
*   The performance gap comes from Zig's ability to easily manage memory manually (allocators) and perform zero-copy slicing on the memory-mapped or buffered data, whereas the Arrow implementations incur overhead building the standardized Arrow in-memory structures (`RecordBatch`, `ArrayData`).

### Conclusion
For "inspector" or "streaming" type workloads (Lambda functions, ETL filters), `zpq`'s lightweight approach is significantly more efficient than loading full Arrow tables.

