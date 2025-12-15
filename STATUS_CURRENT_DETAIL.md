# ZPQ Technical Context & Deep Dive

**Last Updated**: Dec 15, 2025
**Current State**: Stable, CI-verified, Parsing Metadata & skipping Levels correctly.

## Critical Implementation Details

### 1. Thrift Reader (`src/zpq/thrift.zig`)
*   **Status**: robust.
*   **Key Logic**: `readVarInt` uses `u64` accumulator to avoid overflow. `readFieldBegin` handles field deltas.
*   **Warning**: `readByte` must read a raw byte, *not* ZigZag. Do not confuse with `readByte` in some other implementations.
*   **Recent Fix**: Ensured `last_field_id` is saved/restored in nested struct parsing to preventing state corruption in `schema.zig`.

### 2. Schema Traversal (`src/zpq/schema.zig`)
*   **Status**: Working.
*   **Key Logic**: `FileMetaData` parses the flattened Thrift `SchemaElement` list.
*   **Helper**: `getColumnLevels(path)` uses `SchemaIterator` to walk the tree and calculate `max_def_level` and `max_rep_level` based on `REQUIRED` (0), `OPTIONAL` (+1), `REPEATED` (+1).
*   **Output**: Returns `Levels { max_def, max_rep }` which drives the Page Reader.

### 3. RLE Decoder (`src/zpq/rle.zig`)
*   **Status**: Rigorously tested (unit tests + `large_bitpacked.parquet`).
*   **Key Logic**: Handles both RLE runs and Bit-Packed runs.
*   **Bit-Packing**: `unpack8Values` unrolls the bit-shifting for performance.
*   **Next Challenge**: This same decoder will be used for Definition Levels (which are usually RLE encoded with small bit widths like 1).

### 4. Page Reader (`src/main.zig` / `src/zpq/column.zig`)
*   **Current Behavior**: 
    1. Reads Page Header.
    2. Checks `max_def_level` / `max_rep_level`.
    3. If > 0, reads 4-byte length prefix and **skips** that many bytes of encoded levels.
    4. Decodes remaining data as Values (Dictionary Indices).
*   **Immediate Technical Debt**: `src/main.zig` contains too much logic. The level reading/skipping and value decoding should be moved into `src/zpq/column.zig` or a new `src/zpq/page_reader.zig`.

## Next Session: Implementation Plan

### Step 1: Definition Level Decoding
**Goal**: Instead of skipping, *read* the definition levels.
*   **Logic**:
    *   Create an `RleDecoder` for the definition levels chunk.
    *   Decode all levels into a buffer (e.g., `[]u8` or `[]i16` - usually just 0 or 1).
    *   **Crucial**: Count how many `max_def_level`s exist. This is the number of *actual values* stored in the subsequent data stream.
    *   Use this count to initialize the Data/Index decoder.

### Step 2: Value Reconstruction
**Goal**: Produce a clean API like `reader.nextBatch(allocator) -> []?T`.
*   **Logic**:
    *   Iterate the definition levels.
    *   If `level == max_def`, pull one value from the Data Decoder.
    *   If `level < max_def`, insert `null`.

### Step 3: Decompression
**Goal**: Stop assuming `UNCOMPRESSED`.
*   **Task**: Check `page_header.codec`. If `SNAPPY` or `GZIP`, allocate a buffer `uncompressed_page_size` and decompress `p.data` before passing to decoders.
*   **Dependencies**: Look for `zig-snappy` or link against C snappy.

## Test Data Reference
*   `data/simple.parquet`: `OPTIONAL` fields. (Def Levels: 1).
*   `data/large_rle.parquet`: `REQUIRED` fields, RLE data.
*   `data/large_bitpacked.parquet`: `REQUIRED` fields, Bit-Packed data.

