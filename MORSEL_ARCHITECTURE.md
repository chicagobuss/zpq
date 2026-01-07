# Morsel Architecture: The ZPQ Core Engine

This document outlines the architectural blueprint for the unified ZPQ engine, addressing the "Trifecta" (Filter, Select, I/O Direction) and ensuring maximum efficiency for Zig 0.16.x.

## 1. The Core Abstractions

### 1.1 The "Morsel" (Unit of Work)
A **Morsel** is the fundamental unit of parallelism and memory management.
*   **Definition**: A subset of rows from a single Row Group (e.g., 64K rows).
*   **Input**: Byte ranges (compressed/encoded) from an `Accumulator`.
*   **Output**: A `VectorBatch` (decoded, filtered, selected) ready for the sink.
*   **Lifecycle**:
    1.  **Scan**: Peek footer/page-index to identify target Row Groups.
    2.  **Filter Phase**: Load *only* predicate columns. Evaluate. Generate `SelectionVector`.
    3.  **Project Phase**: Load payload columns *only* for selected rows (Range Coalescing).
    4.  **Decode**: SIMD-unpack directly into the `VectorBatch`.

### 1.2 IOD (Input/Output Direction) Matrix
We unify all I/O under two Traits: `ByteSource` and `ByteSink`.

| I/O Direction | implementation |
| :--- | :--- |
| **Local -> Local** | `AsyncFileSource` -> `AsyncFileSink` |
| **S3 -> Local** | `AsyncS3Source` -> `AsyncFileSink` (Streaming Download) |
| **Local -> S3** | `AsyncFileSource` -> `MultipartS3Sink` (Streaming Upload) |
| **S3 -> S3** | `AsyncS3Source` -> `MultipartS3Sink` (ETL/Remux) |
| **Memory -> Memory** | `MemorySource` -> `MemorySink` (Testing/Lambda) |

**The `ByteSource` Interface:**
```zig
pub const ByteSource = struct {
    /// Schedule a read for a list of disjoint ranges.
    /// The completion returns an `Accumulator` holding the data in memory.
    readAt: fn (ranges: []const Range, allocator: Allocator) !Accumulator,
};
```

## 2. The Trifecta Pipeline

The pipeline is "Lazy Pull". The Sink pulls batches from the Operator, which requests Morsels from the Scanner.

### 2.1 Filter (Predicate Pushdown)
*   **Strategy**: "Filter First, Decode Later."
*   **Mechanism**:
    1.  Read Page Index / Bloom Filter. Prune entire Row Groups.
    2.  For surviving Row Groups, read **only** the columns used in the `WHERE` clause.
    3.  Decode predicate columns into a temporary buffer (or use SIMD filter directly on RLE).
    4.  Result: `SelectionVector` (list of valid Row IDs).

### 2.2 Select (Projection)
*   **Strategy**: "Scatter/Gather I/O."
*   **Mechanism**:
    1.  Take `SelectionVector` from Filter.
    2.  Calculate byte ranges for the "Payload Columns" (SELECT * ...).
    3.  **Optimization**: If `SelectionVector` is sparse, do we skip pages? (Yes, if Page Index allows).
    4.  Issue `ByteSource.readAt` for these ranges.
    5.  Decode directly into the Output Batch using the SelectionVector (Gather).

### 2.3 I/O Direction (The Glue)
*   **S3 Optimization**:
    *   **Range Coalescing**: Merge adjacent reads (e.g., col A and col B are next to each other) into one HTTP request to minimize TTFB penalty.
    *   **Prefetch**: Predict next Morsel's headers? (Maybe future optimization).
*   **Local Optimization**:
    *   **io_uring**: Submit all read ops at once.
    *   **mmap**: Optional for local files (zero-copy), but explicit read often scales better with filter logic.

## 3. Implementation Plan (Phased)

We will build this "Inside Out" to ensure correctness before scale.

### Phase 1: Local Parquet Reader (Async) (`Step 1`)
*   **Goal**: Read `simple.parquet` from disk using the new `AsyncFileSource`.
*   **Components**:
    *   `src/io/local.zig`: `AsyncFileSource` (using `xev`).
    *   `src/parquet/metadata.zig`: Thrift decoding (Reuse legacy?).
    *   `src/parquet/reader.zig`: Basic Morsel loop (Read entire RG -> Decode).

### Phase 2: The S3 Source (`Step 2`)
*   **Goal**: Read `benchmark_100mb.parquet` from S3.
*   **Components**:
    *   `src/io/s3.zig`: `AsyncS3Source` (wrapping our new protocol layer).
        *   *Challenge*: Abstracting HTTP Range requests into the `ByteSource` API.
*   **Verification**: "S3 Cat" tool (stream parquet file to stdout).

### Phase 3: The Filter Engine (`Step 3`)
*   **Goal**: `SELECT * FROM table WHERE id > 100`.
*   **Components**:
    *   `src/engine/filter.zig`: Predicate evaluator.
    *   `src/parquet/page_reader.zig`: Lazy column reading.

### Phase 4: The Writer (Sink) (`Step 4`)
*   **Goal**: `S3 -> Filter -> S3` (ETL).
*   **Components**:
    *   `src/io/s3_writer.zig`: Multipart Upload Manager.
    *   `src/parquet/writer.zig`: RowGroup encoder.

## 4. Zig 0.16.x Specifics
*   **Memory**: Each Morsel has its own `ArenaAllocator`. Reset at end of Morsel processing.
*   **Async**: All I/O returns `xev.Completion` or internal future types. No blocking `read`.
*   **SIMD**: Use `@Vector` for bit-unpacking (RLE/BitPacked) in the Decoder.
