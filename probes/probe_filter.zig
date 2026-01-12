const std = @import("std");
const zpq = @import("zpq");
const xev = @import("xev");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        std.debug.print("Usage: {s} <threads>\n", .{args[0]});
        return;
    }
    const num_threads = try std.fmt.parseInt(usize, args[1], 10);

    // 1. Open benchmark file
    const path = "benchmark_local.parquet";
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);
    
    // Check if file exists
    const fd = try std.posix.open(path_z, std.posix.O{ .ACCMODE = .RDONLY }, 0);
    // Keep FD open for the lifetime of the test
    defer std.posix.close(fd);

    var source = try zpq.io.local.AsyncFileSource.init(allocator, fd);
    // We cannot share `source` pointer directly if it was mutable, but `RandomAccessSource` 
    // interface is just vtable + pointer. `pread` is thread safe.
    // However, `AsyncFileSource` is allocated on stack here? No, .init returns struct.
    // wait, init returns struct by value.
    // Let's alloc it on heap to be safe or just keep it in stack of main.
    
    var pfile = zpq.core.file.ParquetFile.init(allocator, source.randomAccessSource());
    try pfile.readFooter();
    defer pfile.deinit();

    std.debug.print("Benchmark: {d} rows, {d} row groups, {d} threads\n", .{pfile.metadata.num_rows, pfile.metadata.row_groups.items.len, num_threads});

    // 2. Setup Plan
    var plan = zpq.core.planner.ExecutionPlan.init(allocator);
    defer plan.deinit();
    
    // Project: i32 (3), i64 (5), bool (13)
    // Project: i32 (3), i64 (5), bool (13) + Filter (2)
    const req_cols = try allocator.alloc(usize, 4);
    req_cols[0] = 3;
    req_cols[1] = 5;
    req_cols[2] = 13;
    req_cols[3] = 2; // int32_sorted for filter
    plan.required_columns = req_cols;
    
    // Output cols
    const out_cols = try allocator.alloc(usize, 3);
    out_cols[0] = 3;
    out_cols[1] = 5;
    out_cols[2] = 13;
    plan.output_columns = out_cols;

    // Filter cols
    const filter_cols = try allocator.alloc(usize, 1);
    filter_cols[0] = 2;
    plan.filter_columns = filter_cols;

    // Filter: int32_sorted < 250000
    plan.filter = zpq.core.filter.Filter{
        .int32 = .{ .col_idx = 2, .op = .Lt, .value = 250000 }
    };

    // 3. Thread Pool
    const Xev = xev;
    var thread_pool = Xev.ThreadPool.init(.{ .max_threads = @intCast(num_threads) });
    defer {
        thread_pool.shutdown();
        thread_pool.deinit();
    }

    // 4. Output Writer (Optional, but let's use null or a MemorySink to test Parallel Commit)
    // For this benchmark, we'll use a MemorySink to simulate writing overhead
    var mem_sink = zpq.io.memory_sink.MemorySink.init(allocator);
    defer mem_sink.deinit();

    var writer = try zpq.core.writer.ParquetWriter.init(allocator, mem_sink.sink(), pfile.metadata.schema.items);
    defer {
        writer.close() catch {};
        writer.deinit();
    }

    var timer = try std.time.Timer.start();

    // 5. Executor
    var executor = zpq.core.executor.Executor.init(
        allocator,
        &plan,
        &pfile.metadata,
        pfile.source,
        &thread_pool,
        writer,
    );
    try executor.execute();

    const total_rows = executor.rows_scanned.load(.monotonic);

    const elapsed_ns = timer.read();
    const elapsed_ms = elapsed_ns / 1_000_000;
    const mrows_sec = @as(f64, @floatFromInt(total_rows)) / @as(f64, @floatFromInt(elapsed_ms)) / 1000.0;

    std.debug.print("Processed {d} rows in {d}ms using {d} threads\n", .{total_rows, elapsed_ms, num_threads});
    std.debug.print("Throughput: {d:.2} Mrows/sec\n", .{mrows_sec});
}
