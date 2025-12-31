# ZPQ Parallel Write Pipeline - Implementation Plan

This document describes the "inverse square root" level optimizations for parallel Parquet writes.

## Executive Summary

We've proven through Python prototypes that:
1. **Padding between row groups is safe** - Parquet readers ignore zeros between data
2. **pwrite() enables parallel writes** - Multiple threads can write to different offsets
3. **S3 ETags are predictable** - Can pre-compute `CompleteMultipartUpload` request
4. **Footer offsets must be exact** - But can be pre-computed with slot-based allocation

This enables **embarrassingly parallel** writes with no coordination until the final footer.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        PARALLEL WRITE PIPELINE                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  PHASE 1: SLOT ALLOCATION (instant, single-threaded)                        │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ slot_size = max(input_rg_sizes) × (1 - selectivity) × 1.2              │ │
│  │ slot_offsets = [4 + i × slot_size for i in 0..N]                       │ │
│  │ footer_offset = 4 + N × slot_size                                      │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  PHASE 2: PARALLEL ROW GROUP PROCESSING (embarrassingly parallel)           │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐                        │
│  │ Worker 0 │ │ Worker 1 │ │ Worker 2 │ │ Worker N │                        │
│  │ Filter   │ │ Filter   │ │ Filter   │ │ Filter   │                        │
│  │ Encode   │ │ Encode   │ │ Encode   │ │ Encode   │                        │
│  │ Compress │ │ Compress │ │ Compress │ │ Compress │                        │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘                        │
│       │            │            │            │                               │
│       ▼            ▼            ▼            ▼                               │
│  [RG 0 bytes] [RG 1 bytes] [RG 2 bytes] [RG N bytes]                        │
│                                                                              │
│  PHASE 3: PARALLEL SLOT WRITES (via pwrite, no coordination)                │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ File: [PAR1][Slot 0][Slot 1][Slot 2]...[Slot N][Footer Space]       │    │
│  │              ↑        ↑        ↑          ↑                         │    │
│  │           pwrite   pwrite   pwrite     pwrite                       │    │
│  │         (parallel) (parallel) (parallel) (parallel)                 │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  PHASE 4: FOOTER WRITE (single-threaded, after all slots complete)          │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ Build FileMetaData with slot-based offsets                          │    │
│  │ Serialize via Thrift                                                │    │
│  │ Write: [footer_bytes][footer_len:u32][PAR1]                         │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Implementation Phases

### Phase 1: Slot-Based Writer Infrastructure

**Status: NOT STARTED**

**Goal**: Create `SlotWriter` that pre-allocates file and enables parallel pwrite.

```zig
/// Slot-based parallel Parquet writer.
/// Pre-computes all offsets, enabling embarrassingly parallel row group writes.
pub const SlotWriter = struct {
    allocator: std.mem.Allocator,
    file: std.fs.File,
    
    // Slot layout (computed at init)
    slot_size: u64,
    num_slots: usize,
    slot_offsets: []u64,
    footer_offset: u64,
    
    // Track actual sizes (filled during writes)
    actual_sizes: []u64,
    
    // Schema info
    schema_elements: []const schema.SchemaElement,
    
    pub fn init(
        allocator: std.mem.Allocator,
        path: []const u8,
        num_row_groups: usize,
        max_rg_size: u64,
        schema_elements: []const schema.SchemaElement,
    ) !SlotWriter {
        // Compute slot size with 20% margin
        const slot_size = max_rg_size + (max_rg_size / 5);
        
        // Pre-compute all offsets
        var slot_offsets = try allocator.alloc(u64, num_row_groups);
        for (0..num_row_groups) |i| {
            slot_offsets[i] = 4 + (i * slot_size);
        }
        const footer_offset = 4 + (num_row_groups * slot_size);
        
        // Create and pre-extend file
        const file = try std.fs.cwd().createFile(path, .{ .read = true });
        errdefer file.close();
        
        // Write PAR1 header
        try file.writeAll("PAR1");
        
        // Pre-extend file (sparse allocation)
        try file.seekTo(footer_offset + 65536);  // Footer space
        try file.writeAll(&[_]u8{0});
        
        return SlotWriter{
            .allocator = allocator,
            .file = file,
            .slot_size = slot_size,
            .num_slots = num_row_groups,
            .slot_offsets = slot_offsets,
            .footer_offset = footer_offset,
            .actual_sizes = try allocator.alloc(u64, num_row_groups),
            .schema_elements = schema_elements,
        };
    }
    
    /// Write row group data to a slot. Thread-safe via pwrite.
    pub fn writeSlot(self: *SlotWriter, slot_index: usize, data: []const u8) !void {
        const offset = self.slot_offsets[slot_index];
        
        // pwrite is atomic - no locking needed
        const written = try std.posix.pwrite(self.file.handle, data, offset);
        if (written != data.len) return error.PartialWrite;
        
        // Track actual size (atomic store for thread safety)
        @atomicStore(&self.actual_sizes[slot_index], data.len, .release);
    }
    
    /// Get the file offset for a slot (for building footer metadata).
    pub fn slotOffset(self: *const SlotWriter, slot_index: usize) u64 {
        return self.slot_offsets[slot_index];
    }
    
    /// Finish the file - build and write footer with slot-based offsets.
    pub fn finish(self: *SlotWriter, row_groups_meta: []const RowGroupMeta) !void {
        // Build FileMetaData with adjusted offsets
        var file_meta = schema.FileMetaData{
            .version = 2,
            .schema = self.schema_elements,
            .num_rows = 0,
            .created_by = "zpq",
            .row_groups = undefined,  // Will build below
        };
        
        // Build row groups with slot-adjusted offsets
        var row_groups = std.ArrayListUnmanaged(schema.RowGroup){};
        defer row_groups.deinit(self.allocator);
        
        for (row_groups_meta, 0..) |rg_meta, i| {
            const slot_start = self.slot_offsets[i];
            
            // Adjust column offsets to be relative to slot start
            var columns = std.ArrayListUnmanaged(schema.ColumnChunk){};
            var col_offset = slot_start;
            
            for (rg_meta.columns) |col_meta| {
                try columns.append(self.allocator, .{
                    .file_offset = col_offset,
                    .meta_data = .{
                        .type = col_meta.type,
                        .encodings = col_meta.encodings,
                        .path_in_schema = col_meta.path_in_schema,
                        .codec = col_meta.codec,
                        .num_values = col_meta.num_values,
                        .total_uncompressed_size = col_meta.uncompressed_size,
                        .total_compressed_size = col_meta.compressed_size,
                        .data_page_offset = col_offset,
                        .dictionary_page_offset = null,
                        .index_page_offset = null,
                    },
                });
                col_offset += col_meta.compressed_size;
            }
            
            try row_groups.append(self.allocator, .{
                .columns = columns,
                .total_byte_size = @atomicLoad(&self.actual_sizes[i], .acquire),
                .num_rows = rg_meta.num_rows,
            });
            
            file_meta.num_rows += rg_meta.num_rows;
        }
        
        file_meta.row_groups = row_groups;
        
        // Serialize footer via Thrift
        var writer = thrift.Writer.init(self.allocator);
        defer writer.deinit();
        try file_meta.write(&writer);
        
        const footer_bytes = writer.bytes();
        const footer_len: u32 = @intCast(footer_bytes.len);
        
        // Write footer at pre-computed offset
        try self.file.seekTo(self.footer_offset);
        try self.file.writeAll(footer_bytes);
        try self.file.writeAll(&std.mem.toBytes(footer_len));
        try self.file.writeAll("PAR1");
        
        // Truncate file to actual size
        const actual_end = self.footer_offset + footer_bytes.len + 8;
        try self.file.setEndPos(actual_end);
    }
    
    pub fn deinit(self: *SlotWriter) void {
        self.file.close();
        self.allocator.free(self.slot_offsets);
        self.allocator.free(self.actual_sizes);
    }
};
```

**Files to modify:**
- `src/zpq/core/slot_writer.zig` (NEW)
- `src/zpq/core.zig` - export SlotWriter

**Tasks:**
- [ ] Create SlotWriter struct
- [ ] Implement pwrite-based slot writing
- [ ] Implement footer construction with slot offsets
- [ ] Add unit tests

---

### Phase 2: Parallel Filter Pipeline Integration

**Status: NOT STARTED**

**Goal**: Modify filter.zig to use SlotWriter for parallel output.

```zig
/// Parallel filter with slot-based writes.
pub fn runParallelSlotWrite(ctx: *const Context, opts: Options) !void {
    // ... setup ...
    
    // PHASE 1: Estimate slot size from input
    const max_input_rg_size = blk: {
        var max: u64 = 0;
        for (pf.metadata.row_groups.items) |rg| {
            max = @max(max, @intCast(rg.total_byte_size));
        }
        break :blk max;
    };
    
    // PHASE 2: Pre-fetch all input data (already implemented)
    // ... existing batched read code ...
    
    // PHASE 3: Create SlotWriter
    var slot_writer = try SlotWriter.init(
        ctx.allocator,
        opts.output_path,
        pf.metadata.num_row_groups,
        max_input_rg_size,
        pf.metadata.schema.items,
    );
    defer slot_writer.deinit();
    
    // PHASE 4: Create workers with output buffers
    var workers: [MAX_RGS]*RowGroupWorker = undefined;
    var completions: [MAX_RGS]WorkerCompletion = undefined;
    var pending = std.atomic.Value(usize).init(num_rgs);
    
    for (0..num_rgs) |i| {
        workers[i] = try RowGroupWorker.init(ctx.allocator, &filter_ctx, &rg_data[i]);
        completions[i] = try WorkerCompletion.init(workers[i], &slot_writer, i, &pending);
    }
    
    // PHASE 5: Schedule all workers (parallel execution)
    for (&completions) |*c| {
        c.scheduleOn(ctx.loop, ctx.thread_pool);
    }
    
    // PHASE 6: Wait for all to complete
    while (pending.load(.acquire) > 0) {
        try ctx.loop.run(.once);
    }
    
    // PHASE 7: Collect metadata and finish
    var rg_metas: [MAX_RGS]RowGroupMeta = undefined;
    for (workers, 0..) |w, i| {
        rg_metas[i] = w.getOutputMeta();
    }
    
    try slot_writer.finish(rg_metas[0..num_rgs]);
}

/// Worker completion that writes to a slot.
const WorkerCompletion = struct {
    worker: *RowGroupWorker,
    slot_writer: *SlotWriter,
    slot_index: usize,
    task: xev.ThreadPool.Task,
    async_signal: xev.Async,
    completion: xev.Completion,
    pending: *std.atomic.Value(usize),
    
    // Output buffer (filled by worker)
    output_buffer: std.ArrayListUnmanaged(u8),
    
    pub fn init(
        worker: *RowGroupWorker,
        slot_writer: *SlotWriter,
        slot_index: usize,
        pending: *std.atomic.Value(usize),
    ) !WorkerCompletion {
        return .{
            .worker = worker,
            .slot_writer = slot_writer,
            .slot_index = slot_index,
            .task = .{ .callback = taskCallback },
            .async_signal = try xev.Async.init(),
            .completion = .{},
            .pending = pending,
            .output_buffer = .{},
        };
    }
    
    fn taskCallback(task: *xev.ThreadPool.Task) void {
        const self: *WorkerCompletion = @fieldParentPtr("task", task);
        
        // Execute filter (CPU work)
        self.worker.execute();
        
        if (self.worker.status == .done and self.worker.row_count > 0) {
            // Encode output to buffer (CPU work)
            self.worker.encodeToBuffer(&self.output_buffer) catch {
                self.worker.status = .failed;
            };
            
            // Write to slot via pwrite (parallel-safe)
            self.slot_writer.writeSlot(self.slot_index, self.output_buffer.items) catch {
                self.worker.status = .failed;
            };
        }
        
        // Signal completion
        self.async_signal.notify() catch {};
    }
    
    fn asyncCallback(ud: ?*WorkerCompletion, ...) xev.CallbackAction {
        const self = ud.?;
        _ = self.pending.fetchSub(1, .release);
        return .disarm;
    }
};
```

**Files to modify:**
- `src/cli/filter.zig` - add `runParallelSlotWrite`
- `src/zpq/core/row_group_worker.zig` - add `encodeToBuffer` method
- `src/main.zig` - add `--slot-parallel` flag

**Tasks:**
- [ ] Add `encodeToBuffer` to RowGroupWorker
- [ ] Integrate SlotWriter with filter pipeline
- [ ] Add WorkerCompletion with slot write callback
- [ ] Benchmark against sequential and `--parallel` modes

---

### Phase 3: S3 Multipart with Pre-computed ETags

**Status: NOT STARTED**

**Goal**: Enable parallel S3 uploads with pre-computed completion request.

```zig
/// S3 Multipart upload with pre-computed ETags.
pub const ParallelS3Uploader = struct {
    upload_id: []const u8,
    bucket: []const u8,
    key: []const u8,
    
    // Pre-computed data
    parts: []Part,
    complete_request_body: []const u8,  // Built before any upload!
    
    pub const Part = struct {
        number: u16,
        data: []const u8,
        md5_digest: [16]u8,
        etag: [32]u8,  // Hex-encoded MD5
        uploaded: std.atomic.Value(bool),
    };
    
    pub fn init(allocator: Allocator, bucket: []const u8, key: []const u8) !ParallelS3Uploader {
        // CreateMultipartUpload to get upload_id
        const upload_id = try s3.createMultipartUpload(bucket, key);
        
        return .{
            .upload_id = upload_id,
            .bucket = bucket,
            .key = key,
            .parts = &.{},
            .complete_request_body = &.{},
        };
    }
    
    /// Prepare all parts - computes MD5s and builds completion request BEFORE uploads.
    pub fn prepareParts(self: *ParallelS3Uploader, slot_data: []const []const u8) !void {
        self.parts = try self.allocator.alloc(Part, slot_data.len);
        
        // Compute MD5 for each part (can be parallelized)
        for (slot_data, 0..) |data, i| {
            var hasher = std.crypto.hash.Md5.init(.{});
            hasher.update(data);
            const digest = hasher.finalResult();
            
            self.parts[i] = .{
                .number = @intCast(i + 1),
                .data = data,
                .md5_digest = digest,
                .etag = hexEncode(digest),
                .uploaded = std.atomic.Value(bool).init(false),
            };
        }
        
        // Build CompleteMultipartUpload XML BEFORE any uploads
        self.complete_request_body = try self.buildCompleteRequest();
    }
    
    fn buildCompleteRequest(self: *ParallelS3Uploader) ![]const u8 {
        var xml = std.ArrayList(u8).init(self.allocator);
        try xml.appendSlice("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        try xml.appendSlice("<CompleteMultipartUpload>");
        
        for (self.parts) |part| {
            try xml.appendSlice("<Part>");
            try xml.writer().print("<PartNumber>{}</PartNumber>", .{part.number});
            try xml.writer().print("<ETag>\"{s}\"</ETag>", .{part.etag});
            try xml.appendSlice("</Part>");
        }
        
        try xml.appendSlice("</CompleteMultipartUpload>");
        return xml.toOwnedSlice();
    }
    
    /// Upload all parts in parallel. Order doesn't matter!
    pub fn uploadParallel(self: *ParallelS3Uploader, pool: *xev.ThreadPool) !void {
        var pending = std.atomic.Value(usize).init(self.parts.len);
        
        for (self.parts) |*part| {
            pool.schedule(xev.ThreadPool.Batch.from(&UploadTask{
                .uploader = self,
                .part = part,
                .pending = &pending,
            }.task));
        }
        
        // Wait for all uploads
        while (pending.load(.acquire) > 0) {
            std.time.sleep(1_000_000);  // 1ms
        }
    }
    
    /// Complete the upload - send pre-built request.
    pub fn complete(self: *ParallelS3Uploader) !void {
        // The completion request was built BEFORE uploads started!
        try s3.completeMultipartUpload(
            self.bucket,
            self.key,
            self.upload_id,
            self.complete_request_body,
        );
    }
};
```

**Files to modify:**
- `src/zpq/io/s3/multipart.zig` (NEW)
- `src/zpq/io/s3.zig` - export multipart

**Tasks:**
- [ ] Implement MD5 computation for parts
- [ ] Build CompleteMultipartUpload XML before uploads
- [ ] Parallel UploadPart with xev
- [ ] Integration with SlotWriter for S3 output

---

## Performance Expectations

| Scenario | Sequential | Slot-Parallel | Improvement |
|----------|------------|---------------|-------------|
| 100MB, 10 RGs, local SSD | 500ms | 80ms | **6x** |
| 1GB, 50 RGs, local SSD | 5s | 200ms | **25x** |
| 100MB, 10 RGs, S3 | 2s | 400ms | **5x** |
| 1GB, 50 RGs, S3 | 20s | 800ms | **25x** |

The speedup scales with row group count because:
1. **No sequential dependency** between row group writes
2. **Footer offsets pre-computed** - no waiting for sizes
3. **S3 ETags pre-computed** - completion request ready before uploads

---

## Verified Assumptions (Python Probes)

| Assumption | Test | Result |
|------------|------|--------|
| Padding between RGs is safe | `test_slot_padding.py` | ✅ 10KB zeros ignored |
| pwrite enables parallel writes | `test_slot_padding.py` | ✅ File integrity maintained |
| S3 ETags are predictable | `test_slot_padding.py` | ✅ MD5 formula confirmed |
| Footer offsets must be exact | `test_footer_reconstruction.py` | ✅ Shifted = read failure |
| Row group bytes are portable | `test_full_slot_assembly.py` | ✅ Extract/reassemble works |

---

## Implementation Order

1. **Phase 1: SlotWriter** - Foundation for parallel writes
   - Create `slot_writer.zig`
   - Implement pwrite-based slots
   - Footer with slot offsets
   - Unit tests

2. **Phase 2: Filter Integration** - Use SlotWriter in filter pipeline
   - Add `encodeToBuffer` to worker
   - Integrate with `--parallel` mode
   - Benchmark comparison

3. **Phase 3: S3 Multipart** - Parallel cloud uploads
   - MD5 pre-computation
   - Parallel UploadPart
   - Pre-built completion request

---

## References

- [Python Probes](/probes/python/) - Proof-of-concept implementations
- [Design Doc](/docs/design/speculative-parallel-writes.md) - Detailed theory
- [S3 Multipart Docs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)
- [S3 ETag Calculation](https://teppen.io/2018/06/23/aws_s3_etags/)
