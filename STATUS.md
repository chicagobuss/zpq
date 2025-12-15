# ZPQ: Native Zig Parquet Library

## Mission
Build a **modern, high-performance, native Zig Parquet library**.
Goal: Feature parity with Apache Arrow, but with superior performance, lower memory footprint, and zero external dependencies.

## Architecture
*   **Zero-Dependency**: Custom Thrift Compact Protocol implementation (no libthrift).
*   **Zero-Copy (where possible)**: Direct mapping of memory-mapped files to structs.
*   **Allocator-Aware**: Explicit memory management for predictable scaling in Lambda/WASM.

## Roadmap

### Phase 1: The Foundation (Current)
*   [x] **Project Structure**: `zpq` module, `justfile` build system, `act` CI, `gen.py` fixtures.
*   [x] **Thrift Reader**: Custom Compact Protocol reader (VarInt, ZigZag, Field skipping).
*   [x] **Metadata Parsing**: File/RowGroup/Column headers, Schema traversal.
*   [x] **Encodings (Basic)**:
    *   [x] PLAIN (Integers, ByteArrays)
    *   [x] RLE / Bit-Packed Hybrid (for Dictionary Indices, Boolean)
*   [x] **Page Structure**: Iterating Data and Dictionary pages.
*   [x] **Handling Optional Fields**: Calculating Definition/Repetition levels and skipping them in Data Pages.

### Phase 2: Core Reader (Next Steps)
*   [ ] **Definition Levels (NULLs)**: Actually decode RLE definition levels to reconstruct `?T` (optional) values.
*   [ ] **Repetition Levels (Lists)**: Decode levels to reconstruct nested Lists/Arrays.
*   [ ] **Value Reconstruction**: Efficiently map Dictionary Indices -> Values using SIMD-friendly approaches.
*   [ ] **Decompression**:
    *   **Snappy**: Vendor Google's `snappy` (C++) and build with Zig (ensure `libc` linking).
    *   **Gzip**: Use `zlib` (often available on system, or vendor `miniz`).
    *   **Zstd**: Vendor `zstd` (C) for modern Parquet.
*   [ ] **Type Support**: Add `INT64`, `FLOAT`, `DOUBLE`, `INT96` (Timestamp), `FIXED_LEN_BYTE_ARRAY`.

### Phase 3: Advanced Reader
*   [ ] **Filter Pushdown**: Apply predicates *before* decoding values (Row Group skipping, Page skipping).
*   [ ] **Column Projection**: Only read IO for requested columns.
*   [ ] **Vectorized Decoding**: Decode batches of values directly into Arrow-compatible memory layouts.

### Phase 4: S3 / Cloud
*   [ ] **S3 Reader**: Async HTTP range requests to read footer + specific column chunks.
*   [ ] **Lambda Handler**: Main entry point for the original "zigaws" goal.

## Usage
```bash
# Quick test
just test

# Verify against official/generated fixtures
just verify

# Stress test (10k+ rows, edge cases)
just comprehensive

# Cross-compile check (Linux/Mac/ARM/x64)
just cross-check
```
