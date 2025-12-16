# ZPQ Project Status

## 🚀 High-Level Goals
*   **Performance**: Outperform PyArrow and Rust `parquet` crate (Standard Arrow Reader) in scan throughput.
*   **Safety**: Zero-copy where possible, safe memory management, robust error handling.
*   **Completeness**: Support all standard Parquet encodings and compression codecs.
*   **Portability**: Run as a standalone CLI, AWS Lambda (Zig's cross-compilation), or library.

## ✅ Current Progress (Phase 3: Optimization & Benchmarking)
*   [x] **Core Reading**: Thrift metadata parsing, Page iteration, Column chunk handling.
*   [x] **Encodings**:
    *   [x] `PLAIN`
    *   [x] `RLE` (Run-Length Encoding)
    *   [x] `BIT_PACKED` (Deprecated but present)
    *   [x] `RLE_DICTIONARY` / `PLAIN_DICTIONARY`
*   [x] **Decompression**:
    *   [x] **Snappy**: Native Zig implementation (verified & benchmarked).
*   [x] **Complex Features**:
    *   [x] **Definition Levels**: Handling NULL values via RLE decoding.
    *   [x] **Dictionary Resolution**: Reconstructing values from dictionary pages.
*   [x] **CLI Tools**:
    *   [x] `schema`: View file structure.
    *   [x] `meta`: View row group/compression stats.
    *   [x] `pages`: Deep inspection of page headers/stats.
    *   [x] `cat`: Dump values (partial CSV-like).
    *   [x] `scan`: High-performance throughput benchmark.
*   [x] **Benchmarking**:
    *   [x] **Throughput**: ~911 MB/s (ZPQ) vs ~370 MB/s (PyArrow) vs ~265 MB/s (Rust Arrow) on M1 Max.
    *   [x] **Validation**: Verified against `parquet-read` and `pyarrow`.

## 🚧 Upcoming (Phase 4: Cloud & Modernization)
*   [ ] **I/O Abstraction**:
    *   [ ] Refactor `ParquetFile` to use `RandomAccessSource` interface.
    *   [ ] Implement `LocalFileSource`.
    *   [ ] Implement `S3Source` (HTTP Range Requests).
*   [ ] **Nested Types**:
    *   [ ] Repetition Levels (Lists/Maps).
*   [ ] **Modern Encodings**:
    *   [ ] `DELTA_BINARY_PACKED`.
    *   [ ] `BYTE_STREAM_SPLIT`.

## 📉 Benchmarks (M1 Max)
| Implementation | Time (s) | Throughput (File) | Throughput (Values) |
| :--- | :--- | :--- | :--- |
| **ZPQ (Zig)** | **0.16s** | **~842 MB/s** | **~857 MVal/s** |
| PyArrow (Python) | 0.29s | ~372 MB/s | N/A |
| Rust (Arrow) | 0.65s | ~160 MB/s | N/A |
| Rust (CLI) | 20.14s | ~5 MB/s | N/A |

*Note: ZPQ is currently ~1.8x faster than PyArrow and ~4x faster than Rust (safe Arrow reader) for raw scanning.*
