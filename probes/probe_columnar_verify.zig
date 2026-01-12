const std = @import("std");
const zpq = @import("zpq");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // 1. Open local file
    const path = "references/duckdb/data/parquet-testing/arrow/alltypes_plain.parquet";
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);
    
    const fd = try std.posix.open(path_z, std.posix.O{ .ACCMODE = .RDONLY }, 0);
    defer std.posix.close(fd);

    var source = try zpq.io.local.AsyncFileSource.init(allocator, fd);
    var pfile = zpq.core.file.ParquetFile.init(allocator, source.randomAccessSource());
    try pfile.readFooter();
    defer pfile.deinit();

    std.debug.print("Parquet File loaded: {d} rows\n", .{pfile.metadata.num_rows});

    // 2. Setup Columnar Infrastructure
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    // We'll read the first two columns: id (0) and bool_col (1)
    // Actually let's do id (0) and bigint_col (5)
    const id_meta = pfile.metadata.row_groups.items[0].columns.items[0].meta_data.?;
    const bigint_meta = pfile.metadata.row_groups.items[0].columns.items[5].meta_data.?;

    var id_reader = zpq.core.column_reader.ColumnReader(i32).init(
        allocator, &arena, pfile.source, id_meta, 0, 0
    );
    var bigint_reader = zpq.core.column_reader.ColumnReader(i64).init(
        allocator, &arena, pfile.source, bigint_meta, 0, 0
    );

    var batch = try zpq.core.column_batch.ColumnBatch.init(allocator, 8);
    defer batch.deinit();

    const id_col = try batch.addColumn(pfile.metadata.schema.items[1]); // schema[0] is root
    const bigint_col = try batch.addColumn(pfile.metadata.schema.items[6]);

    // 3. Read Batch
    const n = try id_reader.readBatch(id_col.data.i32, null, &batch.selection);
    const n2 = try bigint_reader.readBatch(bigint_col.data.i64, null, &batch.selection);

    std.debug.print("Read {d} rows into ColumnBatch\n", .{n});
    try std.testing.expectEqual(n, n2);
    try std.testing.expectEqual(@as(usize, 8), n);

    // 4. Verify Values
    for (0..8) |i| {
        const id = id_col.data.i32[i];
        const bigint = bigint_col.data.i64[i];
        std.debug.print("Row {d}: id={d}, bigint={d}\n", .{i, id, bigint});
        // alltypes_plain has id = 0..7
        try std.testing.expectEqual(@as(i32, @intCast(i)), id);
        // bigint_col is often 10 * id in these test files
        try std.testing.expectEqual(@as(i64, @intCast(i * 10)), bigint);
    }

    std.debug.print("Integration Probe SUCCESS\n", .{});
}
