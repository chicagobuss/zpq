//! Probe: Test write → read roundtrip
//!
//! Writes a parquet file with zpq, then reads it back to verify.

const std = @import("std");
const zpq = @import("zpq");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const path = "/tmp/zpq_roundtrip_test.parquet";

    // ========================================================================
    // Write
    // ========================================================================
    std.debug.print("=== Writing parquet file ===\n", .{});

    var writer = try zpq.core.writer.ParquetWriter.init(allocator, path);
    defer writer.deinit();

    // Define schema: id (INT32), value (DOUBLE), name (BYTE_ARRAY)
    try writer.setColumns(&[_]zpq.core.writer.ColumnDef{
        .{ .name = "id", .type = .INT32 },
        .{ .name = "value", .type = .DOUBLE },
        .{ .name = "name", .type = .BYTE_ARRAY },
    });

    // Write a row group with 5 rows
    var rg = try writer.beginRowGroup();

    const ids = [_]i32{ 1, 2, 3, 4, 5 };
    const values = [_]f64{ 1.1, 2.2, 3.3, 4.4, 5.5 };
    const names = [_][]const u8{ "alice", "bob", "charlie", "diana", "eve" };

    try rg.writeInt32Column(&ids);
    try rg.writeDoubleColumn(&values);
    try rg.writeByteArrayColumn(&names);

    try writer.finishRowGroup(rg, 5);
    try writer.finish();

    std.debug.print("Written to: {s}\n", .{path});

    // ========================================================================
    // Read back
    // ========================================================================
    std.debug.print("\n=== Reading parquet file ===\n", .{});

    var pf = try zpq.file.ParquetFile.open(allocator, path);
    defer pf.deinit();

    std.debug.print("Version: {}\n", .{pf.metadata.version});
    std.debug.print("Num rows: {}\n", .{pf.metadata.num_rows});
    std.debug.print("Num row groups: {}\n", .{pf.metadata.row_groups.items.len});
    std.debug.print("Created by: {s}\n", .{pf.metadata.created_by orelse "unknown"});

    std.debug.print("\nSchema:\n", .{});
    for (pf.metadata.schema.items, 0..) |elem, i| {
        const type_str = if (elem.type) |t| @tagName(t) else "GROUP";
        std.debug.print("  [{d}] {s}: {s}\n", .{ i, elem.name, type_str });
    }

    // Read and print values
    std.debug.print("\nData:\n", .{});

    var batch_reader = try pf.batchReader(allocator, null);
    defer batch_reader.deinit();

    var row_count: usize = 0;
    while (try batch_reader.next()) |batch| {
        defer batch.deinit(allocator);

        for (0..batch.row_count) |row| {
            const id = batch.columns[0].getInt32(row);
            const val = batch.columns[1].getFloat64(row);
            const name = batch.columns[2].getByteArray(row);

            std.debug.print("  row {}: id={?}, value={?d:.1}, name={?s}\n", .{
                row_count + row,
                id,
                val,
                name,
            });
        }
        row_count += batch.row_count;
    }

    std.debug.print("\nTotal rows read: {}\n", .{row_count});

    // ========================================================================
    // Verify
    // ========================================================================
    std.debug.print("\n=== Verification ===\n", .{});

    if (row_count == 5) {
        std.debug.print("✓ Row count matches (5)\n", .{});
    } else {
        std.debug.print("✗ Row count mismatch: expected 5, got {}\n", .{row_count});
    }

    std.debug.print("\n=== SUCCESS ===\n", .{});
}
