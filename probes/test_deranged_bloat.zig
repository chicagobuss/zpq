const std = @import("std");
const zpq = @import("zpq");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const path = "data/deranged.parquet";
    var pf = try zpq.file.ParquetFile.open(allocator, path);
    defer pf.deinit();
    try pf.readFooter();

    const meta = pf.metadata.?;
    const col_idx = 1; // bloat_string
    
    std.debug.print("Testing column 'bloat_string' for large values...\n", .{});

    for (meta.row_groups.items, 0..) |rg, rg_idx| {
        var rg_reader = try pf.rowGroup(rg_idx);
        defer rg_reader.deinit();

        const md = rg.columns.items[col_idx].meta_data.?;
        const levels = meta.getColumnLevels(md.path_in_schema.items);
        const reader = try rg_reader.columnReader(col_idx);
        
        var batch_reader = zpq.core.batch_reader.BatchReader([]const u8).init(
            allocator, 
            reader, 
            md.type, 
            @intCast(levels.max_def), 
            @intCast(levels.max_rep),
            null
        );
        defer batch_reader.deinit();

        var batch: [1024]?[]const u8 = undefined;
        while (true) {
            const n = try batch_reader.nextBatch(&batch);
            if (n == 0) break;
            
            for (batch[0..n]) |val| {
                if (val) |s| {
                    if (s.len > 1000) {
                        std.debug.print("  Found large string: len={d} (starts with {s}...)\n", .{s.len, s[0..10]});
                    }
                }
            }
        }
    }
}

