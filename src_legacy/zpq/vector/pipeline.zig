const std = @import("std");
const xev = @import("xev");
const batch = @import("batch.zig");
const worker = @import("worker.zig");
const expr = @import("expr.zig");
const schema = @import("../core/schema.zig");
const file_mod = @import("../core/file.zig");
const factory = @import("../io/s3/factory.zig");

const VectorRowGroupWorker = worker.VectorRowGroupWorker;
const ParquetFile = file_mod.ParquetFile;

pub const VectorPipeline = struct {
    allocator: std.mem.Allocator,
    
    // Config
    input_path: ?[]const u8 = null,
    filter_expr: ?expr.Expr = null,
    projection: ?[]const usize = null,
    
    // Runtime
    loop: ?*xev.Dynamic.Loop = null,
    thread_pool: ?*xev.ThreadPool = null,
    
    // Iterator State
    current_rg_index: usize = 0,
    current_worker: ?VectorRowGroupWorker = null,
    is_prepared: bool = false,
    
    // State
    file: ?*ParquetFile = null,
    
    pub fn init(allocator: std.mem.Allocator) VectorPipeline {
        return .{ .allocator = allocator };
    }
    
    pub fn deinit(self: *VectorPipeline) void {
        if (self.current_worker) |*w| w.deinit();
        if (self.file) |f| {
            f.deinit();
            self.allocator.destroy(f);
        }
        if (self.projection) |p| self.allocator.free(p);
    }
    
    pub fn setInput(self: *VectorPipeline, path: []const u8) void {
        self.input_path = path;
    }
    
    pub fn setRuntime(self: *VectorPipeline, loop: *xev.Dynamic.Loop, pool: *xev.ThreadPool) void {
        self.loop = loop;
        self.thread_pool = pool;
    }
    
    pub fn prepare(self: *VectorPipeline) !void {
        if (self.is_prepared) return;
        
        const path = self.input_path orelse return error.NoInputPath;
        const loop = self.loop orelse return error.NoRuntime;
        const pool = self.thread_pool orelse return error.NoRuntime;
        
        // 1. Open File
        const pf = try self.allocator.create(ParquetFile);
        errdefer self.allocator.destroy(pf);
        
        pf.* = try factory.openFileWithOptions(self.allocator, path, .{
            .force_async = false,
            .loop = loop,
            .thread_pool = pool,
        });
        try pf.readFooter();
        self.file = pf;
        self.is_prepared = true;
        self.current_rg_index = 0;
    }
    
    pub fn nextBatch(self: *VectorPipeline, batch_size: usize) !?batch.RecordBatch {
        if (!self.is_prepared) try self.prepare();
        
        const pf = self.file.?;
        const meta = pf.metadata.?; // Checked in prepare via readFooter success? Actually readFooter can fail. 
        // If pf.readFooter() succeeded, metadata is set? 
        // ParquetFile impl sets it.
        const rg_count = meta.row_groups.items.len;
        
        while (true) {
            if (self.current_worker) |*w| {
                if (try w.nextBatch(batch_size)) |b| {
                    return b;
                } else {
                    // Worker finished
                    w.deinit();
                    self.current_worker = null;
                    self.current_rg_index += 1;
                }
            }
            
            // Check if done
            if (self.current_rg_index >= rg_count) return null;
            
            // Start next worker
            const rg = meta.row_groups.items[self.current_rg_index];
            
            // Projection
            // For optimized impl, projection should be pre-computed in prepare.
            // But here we need to copy logic.
            // TODO: Cache projection to avoid alloc per RG?
            // Existing logic allocated per RG.
            
            var free_proj: ?[]usize = null;
            defer if (free_proj) |p| self.allocator.free(p);
            
            const proj = self.projection orelse blk: {
                 // Create default projection (0..col_count)
                 const count = meta.schema.items.len - 1;
                 var p = try self.allocator.alloc(usize, count);
                 for (0..count) |j| p[j] = j;
                 free_proj = p;
                 break :blk p;
            };
            
            // We need to pass OWNERSHIP or COPY of projection to worker?
            // VectorRowGroupWorker takes `projection: []const usize`. It copies?
            // `worker.zig`: `self.projection = try allocator.dupe(usize, projection);`
            // Yes, it dupes. So we can use temporary `proj`.
            
            self.current_worker = try VectorRowGroupWorker.init(self.allocator, pf.source, &meta, rg, self.filter_expr, proj);
        }
    }

    /// Execute the pipeline to completion.
    /// Returns total rows matched.
    pub fn execute(self: *VectorPipeline) !usize {
        var total_rows: usize = 0;
        while (try self.nextBatch(4096)) |b| {
            var batch_mut = b;
            defer batch_mut.deinit();
            total_rows += b.len;
        }
        return total_rows;
    }
};

test "integration: vector pipeline simple.parquet" {
    const allocator = std.testing.allocator;
    
    // We need a loop/pool for the factory
    var loop = try xev.Dynamic.Loop.init(.{});
    defer loop.deinit();
    
    var pool = xev.ThreadPool.init(.{});
    defer {
        pool.shutdown();
        pool.deinit();
    }
    
    var vp = VectorPipeline.init(allocator);
    defer vp.deinit();
    
    vp.setInput("data/simple.parquet");
    vp.setRuntime(&loop, &pool);
    
    const count = try vp.execute();
    try std.testing.expectEqual(@as(usize, 3), count);
}

test "integration: vector pipeline s3 benchmark_1mb.parquet" {
    const allocator = std.testing.allocator;
    
    // Check if we should run S3 tests
    const bucket = std.posix.getenv("AWS_S3_BUCKET");
    if (bucket == null) {
        std.debug.print("Skipping S3 test (AWS_S3_BUCKET not set)\n", .{});
        return;
    }
    
    // We need a loop/pool for the factory
    var loop = try xev.Dynamic.Loop.init(.{});
    defer loop.deinit();
    
    var pool = xev.ThreadPool.init(.{});
    defer {
        pool.shutdown();
        pool.deinit();
    }
    
    var vp = VectorPipeline.init(allocator);
    defer vp.deinit();
    
    const path = try std.fmt.allocPrint(allocator, "s3://{s}/zpq_test_data/benchmark/benchmark_1mb.parquet", .{bucket.?});
    defer allocator.free(path);
    
    vp.setInput(path);
    vp.setRuntime(&loop, &pool);
    
    const count = try vp.execute();
    try std.testing.expectEqual(@as(usize, 5242), count);
}


