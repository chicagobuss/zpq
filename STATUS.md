# ZPQ: Native Zig Parquet Library

## Goals
1.  **Native Zig Parquet Reader**: Build a robust, zero-dependency Parquet reader in Zig.
2.  **AWS Lambda Integration**: Deploy as a lightweight Lambda function to query Parquet files directly from S3.
3.  **Performance**: Leverage Zig's manual memory management and compile-time features for high performance.

## Progress (as of Late 2025)

### Completed
*   **Project Infrastructure**:
    *   Renamed to **zpq** (Zig Parquet).
    *   Set up `build.zig` and project structure.
    *   Created Python-based fixture generator (`tools/fixtures/gen.py`) using PyArrow.
    *   Verified against official `parquet-testing` suite.
*   **Thrift Reader**:
    *   Implemented native `Thrift Compact Protocol` reader in Zig (`src/zpq/thrift.zig`).
    *   Supports `readVarInt`, `readZigZag`, `readStruct`, `skip`.
    *   Unit tested and verified against official metadata.
*   **Metadata Parsing**:
    *   Parsing `FileMetaData`, `RowGroup`, `ColumnChunk`, `PageHeader`, `DataPageHeader`, `DictionaryPageHeader`.
    *   Correctly traverses Row Groups and Columns.
*   **Encodings**:
    *   **PLAIN**: Implemented for Dictionary decoding (`src/zpq/decoder.zig`).
    *   **RLE / Bit-Packed**: Implemented and rigorously verified (`src/zpq/rle.zig`).
    *   Supports both RLE runs and Bit-Packed runs (including high bit widths).
*   **Page Reading**:
    *   Successfully reads and iterates `DICTIONARY_PAGE` and `DATA_PAGE`.
    *   Extracts values from Dictionary pages.
    *   Extracts indices from Data pages (RLE/Bit-Packed).

### In Progress / Next Steps
*   **Definition/Repetition Levels**:
    *   Implemented schema traversal to calculate Max Definition/Repetition levels.
    *   Implemented logic to skip RLE-encoded levels in Data Pages.
    *   Verified against `simple.parquet` (OPTIONAL fields).
    *   *Next*: Actually use definition levels to insert NULLs in output.
*   **Decompression**:
    *   Integrate Snappy/Gzip decompression. Currently assumes `UNCOMPRESSED`.
*   **Value Reconstruction**:
    *   Map decoded Data Page indices back to Dictionary values to produce final output.
*   **AWS Integration**:
    *   Add S3 fetching logic.

## Usage

### Generate Test Data
```bash
# Requires uv or python3 with pyarrow
source .venv/bin/activate
python3 tools/fixtures/gen.py
```

### Run Inspector
```bash
zig build run -- inspect data/required.parquet
```

### Run Tests
```bash
zig test src/zpq/rle.zig
zig test src/zpq/thrift_test.zig
```
