const std = @import("std");
const zpq = @import("zpq");

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

    // 3. Worker logic
    const Context = struct {
        allocator: std.mem.Allocator,
        pfile: *zpq.core.file.ParquetFile,
        plan: *zpq.core.planner.ExecutionPlan,
        source: zpq.io.RandomAccessSource,
        rg_start: usize,
        rg_end: usize,
        rows_processed: usize,
    };

    const worker_fn = struct {
        fn run(ctx: *Context) void {
            // Per-thread arena for batch allocations
            var arena = std.heap.ArenaAllocator.init(ctx.allocator);
            defer arena.deinit();
            const arena_alloc = arena.allocator();

            var dummy_queue = zpq.core.rowgroup_pipeline.BatchQueue{};

            // Loop 100 times to simulate a larger file (50MB -> 5GB equivalent)
            for (0..100) |_| {
                for (ctx.rg_start..ctx.rg_end) |rg_idx| {
                    const rg = &ctx.pfile.metadata.row_groups.items[rg_idx];
                    var pipeline = zpq.core.rowgroup_pipeline.RowGroupPipeline.init(
                        arena_alloc, // batch memory from arena
                        &arena,
                        ctx.plan,
                        ctx.source,
                        rg_idx,
                        rg,
                        &ctx.pfile.metadata
                    ) catch @panic("Failed to init pipeline");
                    defer pipeline.deinit();

                    pipeline.execute(&dummy_queue) catch @panic("Pipeline failed");
                    ctx.rows_processed += @intCast(rg.num_rows);
                }
            }
        }
    }.run;

    var threads = try allocator.alloc(std.Thread, num_threads);
    defer allocator.free(threads);
    var contexts = try allocator.alloc(Context, num_threads);
    defer allocator.free(contexts);

    const total_rgs = pfile.metadata.row_groups.items.len;
    const rgs_per_thread = total_rgs / num_threads;
    const extra = total_rgs % num_threads;
    var current_rg: usize = 0;

    var timer = try std.time.Timer.start();

    // Spawn threads
    for (0..num_threads) |i| {
        const count = rgs_per_thread + if (i < extra) @as(usize, 1) else 0;
        contexts[i] = Context{
            .allocator = allocator, // allocator is thread safe (GPA with mutex by default? Yes in std)
            .pfile = &pfile,
            .plan = &plan,
            .source = source.randomAccessSource(), // copy interface (fat pointer)
            .rg_start = current_rg,
            .rg_end = current_rg + count,
            .rows_processed = 0,
        };
        current_rg += count;
        threads[i] = try std.Thread.spawn(.{}, worker_fn, .{&contexts[i]});
    }

    // Join threads
    var total_rows: usize = 0;
    for (0..num_threads) |i| {
        threads[i].join();
        total_rows += contexts[i].rows_processed;
    }

    const elapsed_ns = timer.read();
    const elapsed_ms = elapsed_ns / 1_000_000;
    const mrows_sec = @as(f64, @floatFromInt(total_rows)) / @as(f64, @floatFromInt(elapsed_ms)) / 1000.0;

    std.debug.print("Processed {d} rows in {d}ms using {d} threads\n", .{total_rows, elapsed_ms, num_threads});
    std.debug.print("Throughput: {d:.2} Mrows/sec\n", .{mrows_sec});
}
