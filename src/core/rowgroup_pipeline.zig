const std = @import("std");
const io = @import("../io/interface.zig");
const schema = @import("schema.zig");
const file = @import("file.zig");
const column_batch = @import("column_batch.zig");
const column_reader = @import("column_reader.zig");
const planner = @import("planner.zig");
const sink_mod = @import("../io/sink.zig");

pub const BatchQueue = struct {
    // Placeholder for now
    pub fn push(self: *BatchQueue, batch: column_batch.ColumnBatch) !void {
        _ = self;
        _ = batch;
    }
};

pub const RowGroupPipeline = struct {
    allocator: std.mem.Allocator,
    arena: *std.heap.ArenaAllocator,
    plan: *const planner.ExecutionPlan,
    row_group_idx: usize,
    source: io.RandomAccessSource,
    
    // Readers for this row group
    readers: []column_reader.AnyColumnReader,
    
    // Shared scan state
    current_row: usize,
    total_rows: usize,
    
    // Metadata for the row group
    row_group_metadata: *const schema.RowGroup,

    // Pre-allocated batch for morsel processing
    batch: column_batch.ColumnBatch,

    pub fn init(
        allocator: std.mem.Allocator,
        arena: *std.heap.ArenaAllocator,
        plan: *const planner.ExecutionPlan,
        source: io.RandomAccessSource,
        row_group_idx: usize,
        metadata: *const schema.RowGroup,
        file_metadata: *const schema.FileMetaData,
    ) !RowGroupPipeline {
        const readers = try allocator.alloc(column_reader.AnyColumnReader, plan.required_columns.len);
        
        const BATCH_SIZE = 4096;
        var batch = try column_batch.ColumnBatch.init(allocator, BATCH_SIZE);
        errdefer batch.deinit();

        // Initialize readers and setup batch columns
        for (plan.required_columns, 0..) |col_idx, i| {
            const col_chunk = metadata.columns.items[col_idx];
            // Assuming flat schema for now: root is 0, cols start at 1
            // TODO: robust schema mapping
            const col_schema = file_metadata.schema.items[col_idx + 1]; 
            
            const max_def: u8 = if (col_schema.repetition_type == .OPTIONAL) 1 else 0;
            const max_rep: u8 = if (col_schema.repetition_type == .REPEATED) 1 else 0;

            readers[i] = try column_reader.AnyColumnReader.init(
                allocator, 
                arena, 
                source, 
                col_chunk.meta_data.?, 
                max_def, 
                max_rep
            );
            
            _ = try batch.addColumn(col_schema);
        }

        return .{
            .allocator = allocator,
            .arena = arena,
            .plan = plan,
            .row_group_idx = row_group_idx,
            .source = source,
            .readers = readers,
            .batch = batch,
            .current_row = 0,
            .total_rows = @intCast(metadata.num_rows),
            .row_group_metadata = metadata,
        };
    }
    
    pub fn deinit(self: *RowGroupPipeline) void {
        self.allocator.free(self.readers);
        self.batch.deinit();
    }
    
    // Check if a column index is part of the filter set
    fn isFilterCol(self: *RowGroupPipeline, col_idx: usize) bool {
        for (self.plan.filter_columns) |f_idx| {
            if (f_idx == col_idx) return true;
        }
        return false;
    }
    
    // Helper to find the reader/batch index for a global column index
    fn getReaderIndex(self: *RowGroupPipeline, col_idx: usize) usize {
        for (self.plan.required_columns, 0..) |req_idx, i| {
            if (req_idx == col_idx) return i;
        }
        unreachable; // Planner guarantees required columns are present
    }

    pub fn next(self: *RowGroupPipeline) !?*const column_batch.ColumnBatch {
        const BATCH_SIZE = 4096;
        if (self.current_row >= self.total_rows) return null;

        // 1. Determine morsel size
        const count = @min(BATCH_SIZE, self.total_rows - self.current_row);
        
        // Re-use pre-allocated batch
        self.batch.reset();
        self.batch.num_rows = count; // Inform batch of current valid rows
        
        // 2. Read FILTER columns first
        for (self.plan.filter_columns) |col_idx| {
            const idx = self.getReaderIndex(col_idx);
            var reader = &self.readers[idx];
            const col = &self.batch.columns.items[idx];
            
            // Read without selection initially
            _ = try reader.readBatchInto(col, count, &self.batch.selection);
        }
        
        // 3. Evaluate Filter -> Updates batch.selection
        if (self.plan.filter) |filter| {
            filter.evaluate(&self.batch, &self.batch.selection, self.plan.required_columns); 
            // If no rows selected, we still return the batch, but selection is empty.
            // Optimization: if empty, we could skip reading projection columns?
            // Yes, let's do that check.
            if (!self.batch.selection.anySet(count)) {
                 self.current_row += count;
                 // But we must return *something* or recurse?
                 // If we return a batch with 0 selected, consumer sees 0 rows. Correct.
                 return &self.batch;
            }
        }
        
        // 4. Read PROJECTION columns (using selection)
        for (self.plan.output_columns) |col_idx| {
            if (!self.isFilterCol(col_idx)) {
                 const idx = self.getReaderIndex(col_idx);
                 var reader = &self.readers[idx];
                 const col = &self.batch.columns.items[idx];
                 
                 // Decode WITH selection (bitmask pushdown!)
                 _ = try reader.readBatchInto(col, count, &self.batch.selection);
            }
        }
        
        self.current_row += count;
        return &self.batch;
    }

    pub fn execute(self: *RowGroupPipeline, output_queue: anytype) !void {
        while (try self.next()) |batch| {
            // If completely filtered out (optimization), selection might be empty.
            // Queue implementation should handle that or we check here.
            // But we push anyway for now.
             try output_queue.push(batch.*);
        }
    }

    /// Fast Path: Copy raw compressed column chunks directly from source to sink.
    /// Returns the new RowGroup metadata (relative to the sink start).
    pub fn executeFastPath(self: *RowGroupPipeline, sink: sink_mod.Sink) !schema.RowGroup {
        var rg_cols = std.ArrayListUnmanaged(schema.ColumnChunk){};
        errdefer {
            for (rg_cols.items) |*c| {
                if (c.meta_data) |*m| {
                    m.encodings.deinit(self.allocator);
                    for (m.path_in_schema.items) |p| self.allocator.free(p);
                    m.path_in_schema.deinit(self.allocator);
                }
            }
            rg_cols.deinit(self.allocator);
        }

        var current_offset: i64 = 0;
        var total_rg_size: i64 = 0;

        // Iterate over required columns (which should be ALL columns in fast path)
        for (self.plan.required_columns) |col_idx| {
            const org_chunk = self.row_group_metadata.columns.items[col_idx];
            
            // 1. Determine read range
            if (org_chunk.meta_data) |meta| {
                const data_offset: u64 = @intCast(meta.data_page_offset);
                const col_len: u64 = @intCast(meta.total_compressed_size);
                
                // If dictionary page exists, it comes first
                const col_start = if (meta.dictionary_page_offset) |d| @min(@as(u64, @intCast(d)), data_offset) else data_offset;
                const io_len = col_len; // Assuming contiguous

                // 2. Read raw bytes
                // Optimization: getSlice could be zero-copy reference if MemorySource
                var buffer: []const u8 = undefined;
                var alloc_buf: []u8 = &[_]u8{};
                defer if (alloc_buf.len > 0) self.allocator.free(alloc_buf);

                if (self.source.getSlice(col_start, io_len)) |slice| {
                    buffer = slice;
                } else {
                    alloc_buf = try self.allocator.alloc(u8, io_len);
                    _ = try self.source.readAt(col_start, alloc_buf);
                    buffer = alloc_buf;
                }

                // 3. Write to sink
                var off: usize = 0;
                while (off < buffer.len) {
                    const n = try sink.write(buffer[off..]);
                    if (n == 0) return error.SinkWriteFailed;
                    off += n;
                }
                
                // 4. Create new metadata
                // Deep clone the metadata because we modify offsets and own lifecycle
                var new_meta = meta;

                // Adjust offsets to be relative to the start of this detached sink
                const shift = @as(i64, @intCast(col_start)) * -1 + current_offset; 
                new_meta.data_page_offset += shift;
                if (new_meta.index_page_offset) |*v| v.* += shift;
                if (new_meta.dictionary_page_offset) |*v| v.* += shift;

                // Deep copy arrays
                new_meta.encodings = .{};
                try new_meta.encodings.appendSlice(self.allocator, meta.encodings.items);
                
                new_meta.path_in_schema = .{};
                for (meta.path_in_schema.items) |p| {
                     try new_meta.path_in_schema.append(self.allocator, try self.allocator.dupe(u8, p));
                }

                const new_chunk = schema.ColumnChunk{
                    .file_path = null,
                    .file_offset = current_offset,
                    .meta_data = new_meta,
                };
                
                try rg_cols.append(self.allocator, new_chunk);
                
                current_offset += @intCast(buffer.len);
                total_rg_size += @intCast(buffer.len);
            }
        }

        return schema.RowGroup{
            .columns = rg_cols,
            .total_byte_size = total_rg_size,
            .num_rows = self.row_group_metadata.num_rows,
        };
    }
};
