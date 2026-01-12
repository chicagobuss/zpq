const std = @import("std");
const io = @import("../io/interface.zig");
const schema = @import("schema.zig");
const file = @import("file.zig");
const column_batch = @import("column_batch.zig");
const column_reader = @import("column_reader.zig");
const planner = @import("planner.zig");

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

    pub fn execute(self: *RowGroupPipeline, output_queue: anytype) !void {
        const BATCH_SIZE = 4096;

        while (self.current_row < self.total_rows) {
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
                filter.evaluate(&self.batch, &self.batch.selection); 
                // If no rows selected, skip the rest!
                if (!self.batch.selection.anySet(count)) {
                     self.current_row += count;
                     continue;
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
            
            // 5. Submit batch to output queue (copy or move?)
            // For now, we are reusing 'batch' so we can't push it directly if queue is async.
            // But if queue processes immediately or copies, it's fine.
            // 'BatchQueue' placeholder implies immediate processing or copy.
            try output_queue.push(self.batch);
            
            self.current_row += count;
        }
    }
};
