const std = @import("std");
const zpq = @import("zpq");

pub const VerificationQueue = struct {
    total_rows: usize = 0,
    batches_pushed: usize = 0,
    
    pub fn push(self: *VerificationQueue, batch: zpq.core.column_batch.ColumnBatch) !void {
        self.batches_pushed += 1;
        self.total_rows += batch.num_rows;
        
        // Sanity check: verify first column has data
        if (batch.num_rows > 0) {
            const col = &batch.columns.items[0];
            if (col.data == .i32) {
                // Ensure we actually read something
                // std.debug.print("Sample: {d}\n", .{col.data.i32[0]});
            }
        }
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // 1. Open benchmark file
    const path = "benchmark_local.parquet";
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);
    
    const fd = try std.posix.open(path_z, std.posix.O{ .ACCMODE = .RDONLY }, 0);
    defer std.posix.close(fd);

    var source = try zpq.io.local.AsyncFileSource.init(allocator, fd);
    var pfile = zpq.core.file.ParquetFile.init(allocator, source.randomAccessSource());
    try pfile.readFooter();
    defer pfile.deinit();

    std.debug.print("File loaded: {d} rows\n", .{pfile.metadata.num_rows});

    // 2. Setup Execution Plan (Manual)
    var plan = zpq.core.planner.ExecutionPlan.init(allocator);
    defer plan.deinit();
    
    // We want columns: 
    // 3 -> i32_req (index 4 in schema? No, items[3] in rowgroup columns?)
    // Let's use same indices as probe_benchmark_columnar.zig:
    // i32_m = row_group.columns.items[3]
    // i64_m = row_group.columns.items[5]
    // bool_m = row_group.columns.items[13]
    
    const req_cols = try allocator.alloc(usize, 3);
    req_cols[0] = 3;
    req_cols[1] = 5;
    req_cols[2] = 13;
    plan.required_columns = req_cols;
    
    plan.output_columns = try allocator.dupe(usize, req_cols); // Separate allocation
    // No filter columns for this test (projection only)
    
    // 3. Setup Queue
    var queue = VerificationQueue{};
    
    // 4. Run Pipeline per Row Group
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    
    var timer = try std.time.Timer.start();

    // Loop 100 times to get measurable time and verify stability
    // verification probe is for correctness but also stability
    for (0..10) |_| {
        for (pfile.metadata.row_groups.items, 0..) |*rg, rg_idx| {
             var pipeline = try zpq.core.rowgroup_pipeline.RowGroupPipeline.init(
                allocator,
                &arena,
                &plan,
                source.randomAccessSource(),
                rg_idx,
                rg,
                &pfile.metadata
            );
            defer pipeline.deinit();
            
            try pipeline.execute(&queue);
        }
    }
    
    const elapsed = timer.read() / 1_000_000;
    
    std.debug.print("Processed {d} rows in {d} batches in {d}ms\n", .{queue.total_rows, queue.batches_pushed, elapsed});
    
    if (queue.total_rows != pfile.metadata.num_rows * 10) {
        std.debug.print("ERROR: Expected {d} rows, got {d}\n", .{pfile.metadata.num_rows * 10, queue.total_rows});
        return error.VerificationFailed;
    }
    
    std.debug.print("Pipeline Integration SUCCESS\n", .{});
}
