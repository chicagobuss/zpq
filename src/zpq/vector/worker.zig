const std = @import("std");
const batch = @import("batch.zig");
const reader = @import("reader.zig");
const expr = @import("expr.zig");
const compute = @import("compute.zig");
const schema = @import("../core/schema.zig");
const column = @import("../core/column.zig");
const interface = @import("../io/interface.zig");

const Vector = batch.Vector;
const RecordBatch = batch.RecordBatch;
const VectorColumnReader = reader.VectorColumnReader;

/// Worker that processes a single row group using Vectorized Execution.
/// 
/// Workflow:
/// 1. Initialize VectorColumnReaders for all necessary columns (filter + output).
/// 2. Iterate in batches (e.g., 1024 rows).
/// 3. Read batches for filter columns.
/// 4. Evaluate Filter Expression -> Selection Vector (Bool Vector).
/// 5. If selection count > 0:
///    a. Read batches for other columns (skip if not selected? Random access or partial read?).
///       For Parquet, we mostly have to read sequentially within a page. 
///       Unless we implement "Skip" in VectorColumnReader.
///       (VectorColumnReader logic handles skipping logic if implemented).
///    b. Filter/Select data from read batches using Selection Vector.
///    c. Buffer/Yield the resulting filtered RecordBatch.
///
pub const VectorRowGroupWorker = struct {
    allocator: std.mem.Allocator,
    
    // Inputs
    row_group: schema.RowGroup,
    file: interface.RandomAccessSource, // Or ColumnChunk access
    
    // Schema / Plan
    filter_expr: ?expr.Expr,
    projection: []const usize, // Indices of columns to output
    
    // Readers
    readers: []VectorColumnReader,
    
    // State
    current_row: usize,
    total_rows: usize,

    // Prefetching State
    prefetched_buffers: std.ArrayList([]u8),
    prefetched_sources: std.ArrayList(*interface.local.MemorySource),
    
    pub fn init(
        allocator: std.mem.Allocator,
        file: interface.RandomAccessSource,
        meta: *const schema.FileMetaData,
        row_group: schema.RowGroup,
        filter_expr: ?expr.Expr,
        projection: []const usize,
    ) !VectorRowGroupWorker {
        var prefetched_buffers = std.ArrayList([]u8).init(allocator);
        errdefer {
            for (prefetched_buffers.items) |b| allocator.free(b);
            prefetched_buffers.deinit();
        }
        
        var prefetched_sources = std.ArrayList(*interface.local.MemorySource).init(allocator);
        errdefer {
            for (prefetched_sources.items) |s| allocator.destroy(s);
            prefetched_sources.deinit();
        }

        // Init readers
        const col_count = row_group.columns.items.len;
        var readers = try allocator.alloc(VectorColumnReader, col_count);
        errdefer allocator.free(readers); 
        
        // 1. Identification Pass: Collect ranges for prefetching
        // We need to track which columns are getting prefetched and where their buffer results will be.
        const PREFETCH_THRESHOLD = 64 * 1024 * 1024; // 64MB
        
        // Structures for bulk read
        var ranges = std.ArrayList(interface.Range).init(allocator);
        defer ranges.deinit();
        var buffers = std.ArrayList([]u8).init(allocator);
        defer buffers.deinit();
        
        // Map from col_index -> index_in_prefetched_buffers
        var col_prefetch_map = try allocator.alloc(?usize, col_count);
        defer allocator.free(col_prefetch_map);
        @memset(col_prefetch_map, null);
        
        for (row_group.columns.items, 0..) |chunk, i| {
            const col_meta = chunk.meta_data.?;
            const total_size: u64 = @intCast(col_meta.total_compressed_size);
            
            if (total_size > 0 and total_size < PREFETCH_THRESHOLD) {
                var start_offset: u64 = @intCast(col_meta.data_page_offset);
                if (col_meta.dictionary_page_offset) |dpo| {
                     if (dpo < start_offset) start_offset = @intCast(dpo);
                }
                
                // Allocate buffer
                const buf = try allocator.alloc(u8, total_size);
                // Track it for cleanup immediately in case of error
                try prefetched_buffers.append(buf); 
                
                try ranges.append(.{ .start = start_offset, .end = start_offset + total_size });
                try buffers.append(buf);
                
                col_prefetch_map[i] = buffers.items.len - 1;
                // std.debug.print("DEBUG: Queueing prefetch col {d} (size {d})\n", .{i, total_size});
            }
        }
        
        // 2. Execute Bulk Read
        if (ranges.items.len > 0) {
            // std.debug.print("DEBUG: Executing prefetch of {d} columns\n", .{ranges.items.len});
            try file.readRanges(ranges.items, buffers.items);
            // Note: readRanges ensures all completed or returns error.
        }
        
        // 3. Reader Initialization Pass
        for (row_group.columns.items, 0..) |chunk, i| {
            const col_meta = chunk.meta_data.?;
            const levels = meta.getColumnLevels(col_meta.path_in_schema.items);
            
            var source_to_use = file;
            
            if (col_prefetch_map[i]) |buf_idx| {
                const buf = buffers.items[buf_idx];
                
                var start_offset: u64 = @intCast(col_meta.data_page_offset);
                if (col_meta.dictionary_page_offset) |dpo| {
                     if (dpo < start_offset) start_offset = @intCast(dpo);
                }
                
                const mem_src = try allocator.create(interface.local.MemorySource);
                mem_src.* = interface.local.MemorySource.initWithOffset(buf, start_offset);
                try prefetched_sources.append(mem_src);
                source_to_use = mem_src.source();
            }
            
            const col_reader = try column.ColumnReader.init(source_to_use, chunk);
            readers[i] = VectorColumnReader.init(
                allocator, 
                col_reader, 
                col_meta.type, 
                @intCast(levels.max_def),
                @intCast(levels.max_rep)
            );
        }
        
        return VectorRowGroupWorker{
            .allocator = allocator,
            .row_group = row_group,
            .file = file,
            .filter_expr = filter_expr,
            .projection = try allocator.dupe(usize, projection),
            .readers = readers,
            .current_row = 0,
            .total_rows = @intCast(row_group.num_rows),
            .prefetched_buffers = prefetched_buffers,
            .prefetched_sources = prefetched_sources,
        };
    }
    
    pub fn deinit(self: *VectorRowGroupWorker) void {
        self.allocator.free(self.projection);
        for (self.readers) |*r| r.deinit();
        self.allocator.free(self.readers);
        
        for (self.prefetched_sources.items) |s| self.allocator.destroy(s);
        self.prefetched_sources.deinit();
        
        for (self.prefetched_buffers.items) |b| self.allocator.free(b);
        self.prefetched_buffers.deinit();
    }
    
    /// Process next batch of input rows.
    /// Returns a RecordBatch of *filtered* rows.
    /// Returns null when done.
    pub fn nextBatch(self: *VectorRowGroupWorker, batch_size: usize) !?RecordBatch {
        if (self.current_row >= self.total_rows) return null;
        
        const count = @min(batch_size, self.total_rows - self.current_row);
        
        // 1. Read Filter Columns & Evaluate
        // Need to identify which columns are needed for filter.
        // For now preventing sophisticated analysis, let's assume `filter_expr` refers to column indices.
        // We read those columns.
        
        var filter_mask: ?Vector = null; // Bool vector
        defer if (filter_mask) |*v| v.deinit();
        
        if (self.filter_expr) |e| {
            // We need to form a "batch" of just the columns needed for eval.
            // Or `evaluate` takes a provider/callback to get vectors?
            // Existing `evaluate` takes `RecordBatch`.
            // So we must read input columns into a RecordBatch first.
            
            // Collect needed columns (naive: read all involved in logic)
            // TODO: Analyze expr to find columns.
            // Prototype: assume only 1 column used for filter (id=0).
            
            // Read Batch for Filter Eval
            // We read ALL projected columns + filter columns?
            // Standard Vectorized Model:
            // 1. Read Filter Input Cols
            // 2. Eval Filter -> Selection
            // 3. Construct Selection Vector
            // 4. Read Output Cols (optimized with Selection)
            // 5. Construct Result Batch
            
            // For prototype: Read ALL projected columns + filter columns (Dense Read)
            // Then Filter. (No late materialization yet).
            
            // Let's implement: Read ALL, Filter, Return.
            
            var columns = try self.allocator.alloc(Vector, self.readers.len);
            // errdefer ... cleanup
            
            for (self.readers, 0..) |*r, i| {
                columns[i] = try r.nextBatch(count);
            }
            
            // Create Input Batch
            var input_batch = RecordBatch{
                .len = count,
                .columns = columns,
                .allocator = self.allocator,
            };
            defer input_batch.deinit(); // This frees the vectors!
            // Wait, we want to RETURN a subset of these vectors if they pass filter.
            // If we filter, we create NEW vectors (copied/compacted).
            // So input_batch ownership handling is correct to free "dense" source.
            
            // Eval
            filter_mask = try expr.evaluate(self.allocator, input_batch, e);
            
            // Filter Output
            var out_cols = try self.allocator.alloc(Vector, self.projection.len);
            errdefer self.allocator.free(out_cols);
            
            var batch_len: usize = 0;
            
            for (self.projection, 0..) |col_idx, out_i| {
                const src_vec = input_batch.column(col_idx);
                // Compute Filter
                // Note: filter returns a NEW allocated vector
                const filtered_vec = try compute.filter(self.allocator, src_vec, filter_mask.?);
                out_cols[out_i] = filtered_vec;
                batch_len = filtered_vec.len; // Assume all same length
            }
            
            self.current_row += count;
            
            if (batch_len == 0) {
                 // All filtered out.
                 // Should we recurse to find next batch? 
                 // Yes, otherwise we return empty batch which is fine but inefficient overhead.
                 // Ideally loop until we have data or done.
                 // Recursing for prototype simplicity (beware stack depth).
                 // Better: Loop logic.
                 self.allocator.free(out_cols); // vectors are empty but slice alloc exists (wait, filter returns valid empty vector with data ptr?)
                 // filter returns empty vector. deinit needed?
                 for (out_cols) |*v| v.deinit();
                 self.allocator.free(out_cols);
                 
                 return self.nextBatch(batch_size); 
            }
            
            return RecordBatch{
                .len = batch_len,
                .columns = out_cols,
                .allocator = self.allocator,
            };
            
        } else {
             // No filter
             var out_cols = try self.allocator.alloc(Vector, self.projection.len);
             for (self.projection, 0..) |col_idx, out_i| {
                 out_cols[out_i] = try self.readers[col_idx].nextBatch(count);
             }
             self.current_row += count;
             return RecordBatch{
                 .len = count,
                 .columns = out_cols,
                 .allocator = self.allocator,
             };
        }
    }
};
