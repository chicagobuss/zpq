# Morsel Architecture Implementation Status

**Created**: Jan 2, 2025  
**Status**: In Progress - Python PoC complete, Zig scaffolding started

---

## Vision

Fully pipelined parallel S3 I/O where each row group flows through independently:

```
Time →
────────────────────────────────────────────────────────────────────────────
S3 Read:    [RG1 bytes][RG2 bytes][RG3 bytes][RG4 bytes]
Decode:          [RG1]     [RG2]     [RG3]     [RG4]
Filter:            [RG1]     [RG2]     [RG3]     [RG4]
Encode:              [RG1]     [RG2]     [RG3]     [RG4]
S3 Write:              [part1]   [part2]   [part3]  [part4][footer]
```

### DuckDB vs zpq Parallelism

DuckDB is constrained by sequential file writes:

```
DuckDB:         [prepare RG1] [prepare RG2] [prepare RG3]
                     │             │             │
                     ▼             ▼             ▼
                [write RG1] → [write RG2] → [write RG3] → [footer]
                     └──────── sequential ────────┘
```

zpq leverages S3 multipart upload for parallel writes:

```
zpq:            [prepare RG1] [prepare RG2] [prepare RG3]
                     │             │             │
                     ▼             ▼             ▼
                [upload part1] [upload part2] [upload part3]  ← parallel!
                     │             │             │
                     └─────────────┼─────────────┘
                                   ▼
                            [footer part] → CompleteMultipartUpload
```

---

## What's Complete

### 1. Python Proof-of-Concept (`probes/morsel_poc/`)

Fully working Python implementation that validates the state machine and coordination logic.

| File | Purpose | Status |
|------|---------|--------|
| `coordinator.py` | MorselCoordinator state machine | ✅ 14 unit tests pass |
| `worker.py` | Morsel processing (decode → filter → encode → submit) | ✅ |
| `pipeline.py` | Full pipeline orchestration with ThreadPoolExecutor | ✅ |
| `parquet_footer.py` | Thrift-encoded footer with offset calculation | ✅ |
| `test_state_machine.py` | Unit tests for coordinator state transitions | ✅ |
| `test_s3_integration.py` | Real S3/MinIO integration tests | ✅ |

**Key validations from Python PoC:**
- State machine transitions: `INIT → UPLOADING → DRAINING → FINALIZING → COMPLETING → DONE`
- Backpressure: Blocks when `max_in_flight` reached (tested with threading)
- Concurrent submissions: Thread-safe with proper locking
- Byte offset calculation: Footer assembled after all parts complete
- Row group pruning: Stats-based skipping works
- Two-phase column fetch: Filter column first, then remaining columns

**Test Results:**
```
Pipeline Test 1 (no filter): 10,000 rows, 10 row groups, 51.5ms
Pipeline Test 2 (filter=cat_005): 1,000 rows, 1 row group pruned by stats, 26.2ms  
Pipeline Test 3 (filter=cat_999): 0 rows, all row groups pruned by stats
```

### 2. Zig Scaffolding (`src/zpq/core/morsel.zig`)

Basic structure created and compiles successfully.

**What exists:**
- `CoordinatorState` enum (init, uploading, draining, finalizing, completing, done, err)
- `ColumnChunkMeta` struct (column metadata with relative/absolute offsets)
- `RowGroupMeta` struct (row group metadata)
- `CompletedPart` struct (part_number, etag, size, metadata)
- `InFlightPart` struct (tracking in-flight uploads)
- `CoordinatorConfig` struct (bucket, key, region, credentials, max_in_flight)
- `MorselCoordinatorGen(XevApi)` generic struct parameterized by xev backend
- Basic `init()`, `deinit()`, `start()`, `submitMorsel()`, `drainCompleted()`, `finalize()`, `abort()`, `waitForCompletion()` methods

**What compiles:** `zig build` passes with no errors.

---

## What's Remaining

### 3. Complete MorselCoordinator Implementation

**File:** `src/zpq/core/morsel.zig`

#### 3a. Multipart Upload Initiation (`start()`)
- [x] Create S3Writer with loop/pool
- [x] Configure custom endpoint if provided
- [x] Set credentials
- [ ] **TODO:** Call `createMultipartUpload()` to get upload_id
- [ ] **TODO:** Store upload_id for subsequent part uploads

The S3Writer already has `createMultipartUpload()` but it's private. Options:
1. Make it public in writer.zig
2. Duplicate the HTTP request logic in morsel.zig
3. Use S3Writer's internal buffer but bypass it for direct part uploads

**Recommendation:** Add a new public method to S3Writer: `initMultipartUpload() -> []const u8` that returns the upload_id.

#### 3b. Part Submission (`submitMorsel()`)
- [x] Backpressure via event loop when max_in_flight reached
- [x] Assign part numbers
- [x] Track in-flight parts
- [ ] **TODO:** Actually upload the part data to S3
- [ ] **TODO:** Make uploads truly async (currently placeholder)

The S3Writer has `uploadPart()` and async `startPartUpload()` but they're private. Need to either:
1. Expose these methods
2. Implement direct HTTP part upload in morsel.zig using the same pattern

**Recommendation:** Add public methods to S3Writer:
- `uploadPartAsync(part_number: u32, data: []const u8, callback: *PartCallback) void`
- Or refactor to allow MorselCoordinator to drive the upload loop

#### 3c. Drain Completed Uploads (`drainCompleted()`)
- [x] Process completed parts
- [x] Handle errors
- [x] Move to completed list
- [x] Update statistics
- [ ] **TODO:** Actually integrate with S3Writer's async completion mechanism

#### 3d. Footer Building (`finalize()`)
- [x] Sort completed parts by part number
- [x] Calculate cumulative byte offsets
- [ ] **TODO:** Build actual Parquet FileMetaData (Thrift serialization)
- [ ] **TODO:** Include schema, row group metadata, column chunk offsets
- [ ] **TODO:** Upload footer as final part
- [ ] **TODO:** Call CompleteMultipartUpload with all ETags

**Key challenge:** The existing `writer.zig` in `src/zpq/core/` has Parquet footer writing but it's designed for local files. Need to extract or reuse:
- Schema serialization
- RowGroup metadata serialization  
- FileMetaData Thrift encoding
- The "PAR1" magic + length trailer format

**Files to reference:**
- `src/zpq/core/writer.zig` - existing Parquet writer (local files)
- `probes/morsel_poc/parquet_footer.py` - Python implementation of Thrift encoding

### 4. Integration with Pipeline

**File:** `src/zpq/core/pipeline.zig`

Currently `executeSlotParallel()` uses `SlotWriter` for local file output, then uploads to S3 as a second step. Need to add a new execution mode that uses MorselCoordinator for direct S3 streaming.

#### 4a. New Execution Mode
```zig
pub const ExecutionMode = enum {
    sequential,
    parallel,
    slot_parallel,
    morsel_parallel,  // NEW: Direct S3 streaming with morsel architecture
};
```

#### 4b. New Method: `executeMorselParallel()`
- Use MorselCoordinator instead of SlotWriter
- Each RowGroupWorker produces encoded bytes + metadata
- Submit to coordinator instead of writing to local file
- No temp file staging - direct S3 upload

#### 4c. Modify RowGroupWorker
**File:** `src/zpq/core/row_group_worker.zig`

Currently `SlotWriteCompletion` writes to local file via pwrite. Need a new completion type:
- `MorselWriteCompletion` - encodes row group and submits to MorselCoordinator

The worker already produces encoded bytes via the slot writer pattern. The change is:
1. Instead of `SlotWriter.writeRowGroup()` → call `MorselCoordinator.submitMorsel()`
2. The coordinator handles the async S3 upload

### 5. S3Writer Modifications

**File:** `src/zpq/io/s3/writer.zig`

To support the morsel architecture, need to expose some internals:

```zig
// New public methods needed:
pub fn createMultipartUploadPublic(self: *Self) ![]const u8;  // Returns upload_id
pub fn uploadPartDirect(self: *Self, upload_id: []const u8, part_number: u32, data: []const u8) ![]const u8;  // Returns ETag
pub fn completeMultipartUploadDirect(self: *Self, upload_id: []const u8, parts: []const PartInfo) !void;
pub fn abortMultipartUploadDirect(self: *Self, upload_id: []const u8) !void;
```

Alternatively, create a new `S3MultipartUploader` struct that exposes just the multipart operations without the buffering/streaming logic.

### 6. Footer Serialization

**Options:**

1. **Reuse existing writer.zig logic** - Extract footer building into a separate function that returns bytes instead of writing to file.

2. **Port from Python PoC** - The `parquet_footer.py` has working Thrift compact protocol serialization. Port to Zig.

3. **Use existing schema_mod** - The schema types already exist. Just need Thrift serialization.

**Key structures to serialize:**
```
FileMetaData {
    version: i32,
    schema: []SchemaElement,
    num_rows: i64,
    row_groups: []RowGroup,
    key_value_metadata: ?[]KeyValue,
    created_by: ?[]u8,
}

RowGroup {
    columns: []ColumnChunk,
    total_byte_size: i64,
    num_rows: i64,
    file_offset: i64,
}

ColumnChunk {
    file_offset: i64,
    meta_data: ColumnMetaData,
}
```

---

## File Summary

| File | Status | Description |
|------|--------|-------------|
| `probes/morsel_poc/coordinator.py` | ✅ Complete | Python state machine reference |
| `probes/morsel_poc/worker.py` | ✅ Complete | Python worker reference |
| `probes/morsel_poc/pipeline.py` | ✅ Complete | Python pipeline reference |
| `probes/morsel_poc/parquet_footer.py` | ✅ Complete | Python Thrift footer encoding |
| `probes/morsel_poc/test_state_machine.py` | ✅ Complete | 14 passing tests |
| `probes/morsel_poc/test_s3_integration.py` | ✅ Complete | S3 integration tests |
| `src/zpq/core/morsel.zig` | 🔶 Scaffolding | Types + basic structure, compiles |
| `src/zpq/core/pipeline.zig` | ⬜ Not started | Needs `executeMorselParallel()` |
| `src/zpq/core/row_group_worker.zig` | ⬜ Not started | Needs `MorselWriteCompletion` |
| `src/zpq/io/s3/writer.zig` | ⬜ Not started | Needs public multipart methods |

---

## Implementation Order (Recommended)

1. **Expose S3Writer multipart methods** - Make `createMultipartUpload`, `uploadPart`, `completeMultipartUpload` public or create wrapper methods.

2. **Complete MorselCoordinator.start()** - Actually call createMultipartUpload and store upload_id.

3. **Implement async part upload** - Either use S3Writer's existing async machinery or implement parallel HTTP requests directly.

4. **Implement drainCompleted()** - Integrate with xev event loop to process completions.

5. **Port footer serialization** - Either extract from writer.zig or port from parquet_footer.py.

6. **Implement finalize()** - Build footer, upload as final part, complete multipart.

7. **Add executeMorselParallel()** - New pipeline execution mode.

8. **Create MorselWriteCompletion** - Adapter for RowGroupWorker to submit to coordinator.

9. **End-to-end test** - Run full pipeline with S3 input → filter → S3 output.

---

## Key Design Decisions

### 1. Part Size and S3 Constraints
- S3 requires parts ≥5MB (except last part)
- Row groups may be smaller than 5MB
- **Decision needed:** Buffer small row groups together? Or ensure row groups are ≥5MB?
- Current S3Writer uses 8MB parts (AWS CLI default)

### 2. Memory Budget
- `max_in_flight` controls how many parts can be uploading simultaneously
- Each part holds encoded row group bytes in memory
- Default: 8 parts × ~10MB = ~80MB memory for uploads
- Lambda has 128MB-10GB memory options

### 3. Error Recovery
- If a part upload fails, can retry with same part number
- Need to track "failed" vs "in_progress" vs "completed"
- On unrecoverable error, call AbortMultipartUpload

### 4. Connection Pooling
- S3Writer already has GlobalConnectionPool
- Reuse connections across part uploads
- MorselCoordinator should leverage this, not create its own

---

## Testing Strategy

1. **Unit tests** - Test coordinator state machine in isolation (like Python tests)
2. **Integration with MockS3** - Use LocalStack or MinIO in Docker
3. **Real S3 test** - End-to-end with actual AWS S3
4. **Lambda test** - Deploy and run in Lambda environment

---

## References

- `STATUS_CURRENT_DETAIL.md` - Contains the full architecture vision and pseudocode
- `probes/morsel_poc/` - Python reference implementation
- DuckDB source: `references/duckdb/parquet_writer.cpp` - Their `PreparedRowGroup` → `FlushRowGroup()` → `Finalize()` pattern
