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
const memory_source = @import("../io/memory_source.zig");
const coalescer = @import("../io/coalescer.zig");

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
        const num_rgs = self.file_metadata.row_groups.items.len;
        var data_manager = @import("data_manager.zig").DataManager.init(self.allocator, num_rgs);
        defer data_manager.deinit();

        var wg = std.Thread.WaitGroup{};
        
        // Phase 1: Spawn ALL worker tasks immediately
        // They will block on data_manager.waitForRowGroup
        for (self.file_metadata.row_groups.items, 0..) |*rg, rg_idx| {
            wg.start();
            
            const ctx = try self.allocator.create(TaskContext);
            ctx.* = .{
                .task = .{
                    .callback = workerCallback,
                },
                .executor = self,
                .row_group = rg,
                .row_group_idx = rg_idx,
                .data_manager = &data_manager,
                .wg = &wg,
            };
            
            self.thread_pool.schedule(xev.ThreadPool.Batch.from(&ctx.task));
        }

        // Phase 2: Dispatch all prefetch requests asynchronously (Conductor)
        var prefetches_completed: usize = 0;
        const PrefetchCtx = struct {
            executor: *Executor,
            data_manager: *@import("data_manager.zig").DataManager,
            rg_idx: usize,
            base_offset: u64,
            completed_ptr: *usize,
            
            fn callback(ptr: ?*anyopaque, err: ?anyerror) void {
                const ctx: *@This() = @ptrCast(@alignCast(ptr));
                if (err) |e| {
                    std.debug.print("Prefetch failed for RG {d}: {any}\n", .{ ctx.rg_idx, e });
                    ctx.data_manager.markFailed(ctx.rg_idx, e) catch {};
                }
                ctx.completed_ptr.* += 1;
                ctx.executor.allocator.destroy(ctx);
            }
        };


        for (self.file_metadata.row_groups.items, 0..) |*rg, rg_idx| {
            // Collect ranges for REQUIRED columns only
            var range_list = std.ArrayListUnmanaged(coalescer.Range){};
            defer range_list.deinit(self.allocator);

            for (self.plan.required_columns) |col_idx| {
                if (col_idx >= rg.columns.items.len) continue;
                const col = &rg.columns.items[col_idx];
                
                if (col.meta_data) |*meta| {
                    const data_offset: u64 = @intCast(meta.data_page_offset);
                    const col_len: u64 = @intCast(meta.total_compressed_size);
                    const col_start = if (meta.dictionary_page_offset) |d| @min(@as(u64, @intCast(d)), data_offset) else data_offset;
                    const col_end = col_start + col_len;
                    
                    try range_list.append(self.allocator, .{ .start = col_start, .end = col_end });
                }
            }
            
            // Coalesce ranges (gap threshold 64KB)
            const coalesced = try coalescer.Coalescer.coalesce(self.allocator, range_list.items, 64 * 1024);
            // Note: coalesced is owned by us, need to free later (but we pass it to async call... wait.
            // Actually, we process immediately below).
            defer self.allocator.free(coalesced);

            if (coalesced.len > 0) {
                 // Prepare buffers for each coalesced range
                 const buffers = try self.allocator.alloc([]u8, coalesced.len);
                 // We rely on the callback/WrappedCtx to free this 'buffers' slice, 
                 // BUT the individual buffers inside must be allocated now.
                 
                 var total_bytes: usize = 0;
                 for (coalesced, 0..) |range, i| {
                     const len = range.end - range.start;
                     buffers[i] = try self.allocator.alloc(u8, len);
                     total_bytes += len;
                 }
                 
                 // Convert coalescer ranges to io.Range
                 const io_ranges = try self.allocator.alloc(io.Range, coalesced.len);
                 for (coalesced, 0..) |c, i| {
                     io_ranges[i] = .{ .start = c.start, .end = c.end };
                 }

                const pctx = try self.allocator.create(PrefetchCtx);
                pctx.* = .{
                    .executor = self,
                    .data_manager = &data_manager,
                    .rg_idx = rg_idx,
                    .base_offset = 0, // Not used for sparse
                    .completed_ptr = &prefetches_completed,
                };
                
                // Wrap callback to mark ready
                const WrappedCtx = struct {
                    pctx: *PrefetchCtx,
                    buffers: [][]u8, // We own the slice AND the buffers
                    io_ranges: []io.Range, // We own this too
                    
                    fn cb(ptr: ?*anyopaque, err: ?anyerror) void {
                        const w: *@This() = @ptrCast(@alignCast(ptr));
                        if (err == null) {
                            // Create chunks from buffers + ranges
                            // We need to map back which buffer is which.
                            // Luckily io_ranges[i] corresponds to buffers[i].
                            
                            const chunks = w.pctx.executor.allocator.alloc(@import("data_manager.zig").RowGroupData.Chunk, w.buffers.len) catch panic("OOM in cb");
                            
                            for (w.buffers, 0..) |buf, i| {
                                chunks[i] = .{
                                    .data = buf, // Transfer ownership to RowGroupData
                                    .base_offset = w.io_ranges[i].start,
                                };
                            }
                            
                            const rg_data = @import("data_manager.zig").RowGroupData.init(w.pctx.executor.allocator, w.pctx.rg_idx, chunks);
                            
                            // Free the container slice, but NOT the buffer contents (transferred)
                            w.pctx.executor.allocator.free(w.buffers);
                            
                            w.pctx.data_manager.markReady(w.pctx.rg_idx, rg_data) catch {};
                        } else {
                            // On error, free everything
                            for (w.buffers) |buf| w.pctx.executor.allocator.free(buf);
                            w.pctx.executor.allocator.free(w.buffers);
                        }
                        
                        w.pctx.executor.allocator.free(w.io_ranges);
                        PrefetchCtx.callback(w.pctx, err);
                        w.pctx.executor.allocator.destroy(w);
                    }
                    
                    fn panic(msg: []const u8) noreturn {
                        std.debug.print("PANIC: {s}\n", .{msg});
                        std.process.exit(1);
                    }
                };
                
                const w = try self.allocator.create(WrappedCtx);
                w.* = .{ .pctx = pctx, .buffers = buffers, .io_ranges = io_ranges };

                if (self.source.vtable.readRangesAsync) |rra| {
                    try rra(self.source.ptr, io_ranges, buffers, WrappedCtx.cb, w);
                } else {
                    // Fallback to sync
                    try self.source.readRanges(io_ranges, buffers);
                    WrappedCtx.cb(w, null);
                }
            } else {
                prefetches_completed += 1;
            }
        }

        // Phase 3: Drive the loop until all prefetches are done (Conductor)
        // Workers are running on thread pool and will be unblocked as data arrives.
        while (prefetches_completed < num_rgs) {
            // We use the loop from main.zig (which is shared with S3Source)
            // Wait, how do we get the loop here?
            // S3Source has the loop. We can drive it via a dummy call or exposing it.
            // For now, let's assume S3Source's loop is what we need to drive.
            // Actually, Executor should probably HAVE a pointer to the loop.
            
            // FIXME: Drive the loop. For now, since S3Source.readAt/readRanges 
            // already drive the loop if they are sync, it works.
            // But for TRUE async, we need a way to run the loop.
            // Let's assume the loop is being driven elsewhere or we have it.
            
            // TEMPORARY: If we are on S3, we know the source is AsyncS3Source.
            // We'll add 'loop' to Executor.
            if (self.plan.loop) |loop| {
                try loop.run(.once);
            } else {
                // If no loop (sync source), we already called callbacks above.
                if (prefetches_completed < num_rgs) break;
            }
        }

        wg.wait();
    }

    
    const TaskContext = struct {
        task: xev.ThreadPool.Task,
        executor: *Executor,
        row_group: *schema.RowGroup,
        row_group_idx: usize,
        data_manager: *@import("data_manager.zig").DataManager,
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
        // Wait for data to be ready (blocks worker thread)
        const rg_data = ctx.data_manager.waitForRowGroup(ctx.row_group_idx) orelse {
            return error.NoRowGroupData;
        };

        // Create memory-backed source from pre-fetched data
        // This avoids any network calls from worker threads
        var mem_src = memory_source.MemorySource.init(
            rg_data.chunks
        );
        const source = mem_src.randomAccessSource();
        
        var pipeline = try rowgroup_pipeline.RowGroupPipeline.init(
            ctx.executor.allocator,
            arena,
            ctx.executor.plan,
            source,
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

        // Fast Path Optimization
        if (ctx.executor.plan.is_zero_copy) {
            const rg_meta = try pipeline.executeFastPath(local_mem_sink.sink());
            
            // Synchronized Merge
            ctx.executor.write_mutex.lock();
            defer ctx.executor.write_mutex.unlock();

            if (ctx.executor.output_writer) |gw| {
                const rgs = [_]schema.RowGroup{rg_meta};
                try gw.mergeDetached(local_mem_sink.data.items, &rgs);
            }
            
            _ = ctx.executor.rows_scanned.fetchAdd(@intCast(rg_meta.num_rows), .monotonic);
            _ = ctx.executor.rows_matched.fetchAdd(@intCast(rg_meta.num_rows), .monotonic);
            
            return;
        }

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
