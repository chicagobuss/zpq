const std = @import("std");
const zpq = @import("zpq");
const BatchReader = zpq.core.batch_reader.BatchReader;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        std.debug.print("Usage: {s} <path>\n", .{args[0]});
        return;
    }

    var file = try zpq.file.ParquetFile.open(allocator, args[1]);
    defer file.deinit();
    try file.readFooter();

    const meta = file.metadata.?;
    for (meta.row_groups.items, 0..) |rg, rg_idx| {
        var rg_reader = try file.rowGroup(rg_idx);
        defer rg_reader.deinit();
        
        // Let's try to read the first column using BatchReader
        const col_idx = 0;
        const col = rg.columns.items[col_idx];
        const md = col.meta_data.?;
        const levels = meta.getColumnLevels(md.path_in_schema.items);
        const schema_elem = meta.getColumnSchema(md.path_in_schema.items);
        const type_length = if (schema_elem) |se| se.type_length else null;
        
        const reader = try rg_reader.columnReader(col_idx);
        
        switch (md.type) {
            .INT32 => try runTest(i32, allocator, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length),
            .INT64 => try runTest(i64, allocator, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length),
            .INT96 => try runTest([12]u8, allocator, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length),
            .FLOAT => try runTest(f32, allocator, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length),
            .DOUBLE => try runTest(f64, allocator, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length),
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => try runTest([]const u8, allocator, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length),
            else => std.debug.print("Skipping type {any}\n", .{md.type}),
        }
    }
}

fn runTest(comptime T: type, allocator: std.mem.Allocator, reader: zpq.column.ColumnReader, col_type: zpq.schema.Type, max_def: u16, max_rep: u16, type_length: ?i32) !void {
    var batch_reader = BatchReader(T).init(allocator, reader, col_type, max_def, max_rep, type_length);
    defer batch_reader.deinit();

    var batch: [1024]?T = undefined;
    var total: usize = 0;
    while (true) {
        const n = try batch_reader.nextBatch(&batch);
        if (n == 0) break;
        total += n;
    }
    std.debug.print("Read total of {d} values (type {any})\n", .{total, col_type});
}

