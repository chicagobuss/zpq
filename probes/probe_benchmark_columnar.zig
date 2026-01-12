const std = @import("std");
const zpq = @import("zpq");

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

    std.debug.print("Benchmark File loaded: {d} rows, {d} row groups\n", .{pfile.metadata.num_rows, pfile.metadata.row_groups.items.len});

    // 2. Setup Columnar Infrastructure
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const id_schema = pfile.metadata.schema.items[4];
    const i32_max_def: u8 = if (id_schema.repetition_type == .OPTIONAL) 1 else 0;
    const i64_schema = pfile.metadata.schema.items[6];
    const i64_max_def: u8 = if (i64_schema.repetition_type == .OPTIONAL) 1 else 0;
    const bool_schema = pfile.metadata.schema.items[14];
    const bool_max_def: u8 = if (bool_schema.repetition_type == .OPTIONAL) 1 else 0;

    var batch = try zpq.core.column_batch.ColumnBatch.init(allocator, 4096);
    defer batch.deinit();

    _ = try batch.addColumn(pfile.metadata.schema.items[4]);
    _ = try batch.addColumn(pfile.metadata.schema.items[6]);
    _ = try batch.addColumn(pfile.metadata.schema.items[14]);

    const c1 = &batch.columns.items[0];
    const c2 = &batch.columns.items[1];
    const c3 = &batch.columns.items[2];

    // 3. Read All Batches across all row groups - 1000 iterations for profiling
    var total_read: usize = 0;
    const iterations = 1000;
    var timer = try std.time.Timer.start();

    for (0..iterations) |_| {
        for (pfile.metadata.row_groups.items) |row_group| {
            const i32_m = row_group.columns.items[3].meta_data.?;
            const i64_m = row_group.columns.items[5].meta_data.?;
            const bool_m = row_group.columns.items[13].meta_data.?;

            var i32_reader = zpq.core.column_reader.ColumnReader(i32).init(
                allocator, &arena, pfile.source, i32_m, i32_max_def, 0
            );
            var i64_reader = zpq.core.column_reader.ColumnReader(i64).init(
                allocator, &arena, pfile.source, i64_m, i64_max_def, 0
            );
            var bool_reader = zpq.core.column_reader.ColumnReader(bool).init(
                allocator, &arena, pfile.source, bool_m, bool_max_def, 0
            );

            const rg_rows = row_group.num_rows;
            var rg_read: usize = 0;
            while (rg_read < rg_rows) {
                batch.reset();
                const n1 = try i32_reader.readBatch(c1.data.i32, null, &batch.selection);
                const n2 = try i64_reader.readBatch(c2.data.i64, null, &batch.selection);
                const n3 = try bool_reader.readBatch(c3.data.bool, null, &batch.selection);

                _ = n2;
                _ = n3;

                if (n1 == 0) break;
                rg_read += n1;
            }
            total_read += rg_read;
        }
    }

    const elapsed_ns = timer.read();
    const elapsed_ms = elapsed_ns / 1_000_000;

    std.debug.print("Read {d} total rows across {d} iterations in {d}ms\n", .{total_read, iterations, elapsed_ms});
    if (elapsed_ms > 0) {
        std.debug.print("Throughput: {d} Mrows/sec\n", .{@as(f64, @floatFromInt(total_read)) / @as(f64, @floatFromInt(elapsed_ms)) / 1000.0});
    }
    std.debug.print("Integration Probe SUCCESS\n", .{});
}
