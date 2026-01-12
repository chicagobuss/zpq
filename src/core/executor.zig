const std = @import("std");
const xev = @import("xev");
const planner = @import("planner.zig");
const rowgroup_pipeline = @import("rowgroup_pipeline.zig");
const file = @import("file.zig");
const schema = @import("schema.zig");
const io = @import("../io/interface.zig");
const column_batch = @import("column_batch.zig");
const writer = @import("writer.zig");
const prefetching = @import("../io/prefetching_source.zig");
const memory_sink = @import("../io/memory_sink.zig");

pub const Executor = struct {
    allocator: std.mem.Allocator,
    plan: *planner.ExecutionPlan,
    file_metadata: *const schema.FileMetaData,
    source: io.RandomAccessSource,
    prefetch_source: prefetching.PrefetchingSource,
    thread_pool: *xev.ThreadPool,
    output_writer: ?*writer.ParquetWriter,
    
    // Shared state
    rows_scanned: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    rows_matched: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    write_mutex: std.Thread.Mutex = .{},
    
    const Self = @This();

    pub fn init(
        allocator: std.mem.Allocator,
        plan: *planner.ExecutionPlan,
        file_metadata: *const schema.FileMetaData,
        source: io.RandomAccessSource,
        thread_pool: *xev.ThreadPool,
        output_writer: ?*writer.ParquetWriter,
    ) Self {
        return .{
            .allocator = allocator,
            .plan = plan,
            .file_metadata = file_metadata,
            .source = source,
            .prefetch_source = prefetching.PrefetchingSource.init(allocator, source, thread_pool),
            .thread_pool = thread_pool,
            .output_writer = output_writer,
        };
    }
    
    pub fn deinit(self: *Self) void {
        self.prefetch_source.deinit();
    }

    pub fn execute(self: *Self) !void {
        // Simple parallel loop over row groups
        // We use an atomic counter or just the thread pool's task mechanism?
        // xev ThreadPool doesn't map directly to "wait for all".
        // We need a WaitGroup equivalent.
        
        var wg = std.Thread.WaitGroup{};
        
        for (self.file_metadata.row_groups.items, 0..) |*rg, rg_idx| {
            // Prefetch Logic Implementation:
            // Calculate range for this RG and next RG.
            // Since we iterate sequentially to spawn tasks, we can just issue prefetch for i+1 here.
            
            // Note: We should verify if 'prefetch' is non-blocking. Yes it is.
            if (rg_idx + 1 < self.file_metadata.row_groups.items.len) {
                 const next_rg = &self.file_metadata.row_groups.items[rg_idx + 1];
                 // Heuristic: Read from first column offset to ... size?
                 // Using 16MB constant from prefetcher for now if range calculation is hard.
                 // Better: iterate columns to find min_offset and max_offset+len.
                 
                 // Simpler: Just prefetch the *current* row group? 
                 // No, we want to prefetch *ahead* or at least start it immediately.
                 // Actually, if we spawn a task for RG[i], that task calls `pipeline.init` which calls `readAt`.
                 // If we call prefetch(RG[i]) *before* spawning the task, we are racing the task.
                 // If we call prefetch(RG[i+1]), we are ahead.
                 
                 // Let's iterate columns of next_rg to find bounds.
                 var min_offset: u64 = std.math.maxInt(u64);
                 var max_end: u64 = 0;
                 var valid = false;
                 
                 for (next_rg.columns.items) |*col| {
                     if (col.meta_data) |*meta| {
                         const start: u64 = @intCast(meta.data_page_offset); // or dictionary_page_offset
                         const len: u64 = @intCast(meta.total_compressed_size);
                         if (start < min_offset) min_offset = start;
                         if (start + len > max_end) max_end = start + len;
                         valid = true;
                     }
                 }
                 
                 if (valid) {
                     // Issue prefetch
                     self.prefetch_source.prefetch(min_offset, max_end - min_offset) catch {};
                 }
            }
            
            wg.start();
            
            // Allocate a task context
            const ctx = try self.allocator.create(TaskContext);
            ctx.* = .{
                .task = .{
                    .callback = workerCallback,
                },
                .executor = self,
                .row_group = rg,
                .row_group_idx = rg_idx,
                .wg = &wg,
            };
            
            // Queue task
            self.thread_pool.schedule(xev.ThreadPool.Batch.from(&ctx.task));
        }
        
        wg.wait();
    }
    
    const TaskContext = struct {
        task: xev.ThreadPool.Task,
        executor: *Executor,
        row_group: *schema.RowGroup,
        row_group_idx: usize,
        wg: *std.Thread.WaitGroup,
    };
    
    fn workerCallback(task: *xev.ThreadPool.Task) void {
        const ctx: *TaskContext = @fieldParentPtr("task", task);
        defer ctx.executor.allocator.destroy(ctx);
        defer ctx.wg.finish();
        
        // Setup Pipeline for this row group
        var arena = std.heap.ArenaAllocator.init(ctx.executor.allocator);
        defer arena.deinit();
        
        // We need to handle errors here, but callback is void.
        // For now, log and unexpected exit. Real system needs error propagation.
        processRowGroup(ctx, &arena) catch |err| {
            std.debug.print("Error processing row group {d}: {any}\n", .{ctx.row_group_idx, err});
        };
    }
    
    fn processRowGroup(ctx: *TaskContext, arena: *std.heap.ArenaAllocator) !void {
        var pipeline = try rowgroup_pipeline.RowGroupPipeline.init(
            ctx.executor.allocator,
            arena,
            ctx.executor.plan,
            ctx.executor.prefetch_source.randomAccessSource(),
            ctx.row_group_idx,
            ctx.row_group,
            ctx.executor.file_metadata,
        );
        defer pipeline.deinit();

        // Parallel Commit: Create a local writer with MemorySink
        var local_mem_sink = memory_sink.MemorySink.init(ctx.executor.allocator);
        defer local_mem_sink.deinit();

        const local_writer = if (ctx.executor.output_writer) |gw|
            try writer.ParquetWriter.initDetached(ctx.executor.allocator, local_mem_sink.sink(), gw.schema_elements.items)
        else
            null;

        defer if (local_writer) |lw| {
            // Note: we don't call lw.close() here because we only want the row group data,
            // not the footer.
            lw.deinit();
        };

        var matched_in_rg: usize = 0;
        var scanned_in_rg: usize = 0;

        while (try pipeline.next()) |batch| {
            var active_count: usize = 0;
            for (0..batch.num_rows) |i| {
                if (batch.selection.isActive(i)) active_count += 1;
            }

            scanned_in_rg += batch.num_rows;
            matched_in_rg += active_count;

            if (local_writer) |lw| {
                try lw.appendBatch(batch);
            }
        }

        // Update global atomic stats
        _ = ctx.executor.rows_scanned.fetchAdd(scanned_in_rg, .monotonic);
        _ = ctx.executor.rows_matched.fetchAdd(matched_in_rg, .monotonic);

        // Finalize Row Group locally (Compression happens here in parallel!)
        if (local_writer) |lw| {
            if (matched_in_rg > 0) {
                try lw.flushRowGroup();

                // Synchronized Merge
                ctx.executor.write_mutex.lock();
                defer ctx.executor.write_mutex.unlock();

                if (ctx.executor.output_writer) |gw| {
                    try gw.mergeDetached(local_mem_sink.data.items, lw.row_groups.items);
                }
            }
        }
    }
};
