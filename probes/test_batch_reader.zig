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
        
        // Let's try to read the "name" column (index 1) using BatchReader
        const col_idx = 1;
        const col = rg.columns.items[col_idx];
        const md = col.meta_data.?;
        const levels = meta.getColumnLevels(md.path_in_schema.items);
        
        const reader = try rg_reader.columnReader(col_idx);
        var batch_reader = BatchReader([]const u8).init(allocator, reader, md.type, @intCast(levels.max_def));
        defer batch_reader.deinit();

        var batch: [1024]?[]const u8 = undefined;
        while (true) {
            const n = try batch_reader.nextBatch(&batch);
            if (n == 0) break;
            std.debug.print("Read batch of {d} values\n", .{n});
        }
    }
}

