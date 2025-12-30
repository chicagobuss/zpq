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
    std.debug.print("Skip Performance Benchmark on {s}\n", .{ path });

    const col_idx = 5; // repetitive_str (DICTIONARY)
    
    // --- 1. Measure Full Decode Time ---
    var timer = try std.time.Timer.start();
    var total_values: usize = 0;
    
    for (meta.row_groups.items, 0..) |_, rg_idx| {
        var rg_reader = try pf.rowGroup(rg_idx);
        defer rg_reader.deinit();
        
        const md = meta.row_groups.items[rg_idx].columns.items[col_idx].meta_data.?;
        const levels = meta.getColumnLevels(md.path_in_schema.items);
        const schema_elem = meta.getColumnSchema(md.path_in_schema.items).?;
        
        const col_reader = try rg_reader.columnReader(col_idx);
        var reader = zpq.core.batch_reader.BatchReader([]const u8).init(allocator, col_reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), schema_elem.type_length);
        defer reader.deinit();
        
        var buffer: [1024]?[]const u8 = undefined;
        while (true) {
            const n = try reader.nextBatch(&buffer);
            if (n == 0) break;
            total_values += n;
        }
    }
    const decode_time = timer.read();
    std.debug.print("Full Decode: {d} values in {d:.4}ms\n", .{ total_values, @as(f64, @floatFromInt(decode_time)) / 1_000_000.0 });

    // --- 2. Measure Skip Time ---
    timer.reset();
    var skipped_values: usize = 0;
    
    for (meta.row_groups.items, 0..) |rg_meta, rg_idx| {
        var rg_reader = try pf.rowGroup(rg_idx);
        defer rg_reader.deinit();
        
        const md = meta.row_groups.items[rg_idx].columns.items[col_idx].meta_data.?;
        const levels = meta.getColumnLevels(md.path_in_schema.items);
        const schema_elem = meta.getColumnSchema(md.path_in_schema.items).?;
        
        const col_reader = try rg_reader.columnReader(col_idx);
        var reader = zpq.core.batch_reader.BatchReader([]const u8).init(allocator, col_reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), schema_elem.type_length);
        defer reader.deinit();
        
        try reader.skip(@intCast(rg_meta.num_rows));
        skipped_values += @intCast(rg_meta.num_rows);
    }
    const skip_time = timer.read();
    std.debug.print("Full Skip:   {d} values in {d:.4}ms\n", .{ skipped_values, @as(f64, @floatFromInt(skip_time)) / 1_000_000.0 });
    
    const speedup = @as(f64, @floatFromInt(decode_time)) / @as(f64, @floatFromInt(skip_time));
    std.debug.print("Skip Speedup: {d:.2}x\n", .{ speedup });
}

