const std = @import("std");
const zpq = @import("zpq");

const AllTypes = struct {
    id: i32,
    bool_col: bool,
    tinyint_col: i32,
    smallint_col: i32,
    int_col: i32,
    bigint_col: i64,
    float_col: f32,
    double_col: f64,
    // date_string_col: []const u8, // Not supported yet (string/binary)
    // string_col: []const u8,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // 1. Open local file
    const path = "data/parquet-testing/data/alltypes_plain.parquet";
    var file = try std.fs.cwd().openFile(path, .{});
    defer file.close();

    var source = try zpq.io.local.AsyncFileSource.init(allocator, file);
    var pfile = zpq.core.file.ParquetFile.init(allocator, source.randomAccessSource());
    try pfile.readFooter();
    defer pfile.deinit();

    std.debug.print("Parquet File loaded: {d} rows\n", .{pfile.metadata.num_rows});

    // 2. Initialize ParquetReader
    var reader = try zpq.core.reader.ParquetReader(AllTypes).init(allocator, &pfile);
    defer reader.deinit();

    // 3. Scan and Filter
    // We want to find rows where id > 2
    // Field 0 is 'id' (i32)
    const filters = [_]zpq.core.filter.Filter{
        .{ .Int32 = .{ .col_idx = 0, .pred = .Gt, .val = 2 } },
    };

    var batch_buf: [1024]AllTypes = undefined;
    const count = try reader.nextBatch(&batch_buf, &filters);

    std.debug.print("Total matching rows: {d}\n", .{count});

    if (count != 5) { // alltypes_plain has 8 rows, id 0-7. id > 2 should find 3, 4, 5, 6, 7 (5 rows).
        std.debug.print("Verification Failed: expected 5, got {d}\n", .{count});
        return error.VerificationFailed;
    }

    // Verify values
    for (batch_buf[0..count]) |row| {
        std.debug.print("Row match: ID={d}, int_col={d}, bigint_col={d}, float={d}\n", .{
            row.id, row.int_col, row.bigint_col, row.float_col,
        });
        if (row.id <= 2) return error.FilterFailed;
    }
}
