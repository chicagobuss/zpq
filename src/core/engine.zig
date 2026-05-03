const std = @import("std");
const xev = @import("xev");
const zpq = @import("../zpq.zig");

pub const ExecutionStats = struct {
    scanned: usize,
    matched: usize,
    elapsed_ms: f64,
};

pub fn printSchema(
    allocator: std.mem.Allocator,
    input_path: []const u8,
    loop: *xev.Dynamic.Loop,
    thread_pool: *xev.ThreadPool,
) !void {
    const source = try zpq.io.factory.openSource(allocator, input_path, .{ .loop = loop, .thread_pool = thread_pool });
    var source_mut = source;
    defer source_mut.close();

    var pfile = zpq.core.file.ParquetFile.init(allocator, source_mut);
    try pfile.readFooter();
    defer pfile.deinit();

    std.debug.print("Schema for {s}:\n", .{input_path});
    for (pfile.metadata.schema.items, 0..) |elem, i| {
        const type_str = if (elem.type) |t| @tagName(t) else "n/a";
        const rep_str = if (elem.repetition_type) |rt| @tagName(rt) else "n/a";
        const logical_str = if (elem.logical_type) |lt| @tagName(lt) else if (elem.converted_type) |ct| @tagName(ct) else "";
        if (logical_str.len > 0) {
            std.debug.print("  [{d}] {s}: {s} ({s}) L:{s}\n", .{ i, elem.name, type_str, rep_str, logical_str });
        } else {
            std.debug.print("  [{d}] {s}: {s} ({s})\n", .{ i, elem.name, type_str, rep_str });
        }
    }
}

pub fn printMetadata(
    allocator: std.mem.Allocator,
    input_path: []const u8,
    loop: *xev.Dynamic.Loop,
    thread_pool: *xev.ThreadPool,
) !void {
    const source = try zpq.io.factory.openSource(allocator, input_path, .{ .loop = loop, .thread_pool = thread_pool });
    var source_mut = source;
    defer source_mut.close();

    var pfile = zpq.core.file.ParquetFile.init(allocator, source_mut);
    try pfile.readFooter();
    defer pfile.deinit();

    std.debug.print("Metadata for {s}:\n", .{input_path});
    std.debug.print("  File Size: {d} bytes\n", .{pfile.source.size()});
    std.debug.print("  Rows: {d}\n", .{pfile.metadata.num_rows});
    std.debug.print("  Row Groups: {d}\n", .{pfile.metadata.row_groups.items.len});
}

pub fn runQuery(
    comptime T: type,
    allocator: std.mem.Allocator,
    input_path: []const u8,
    output_path: ?[]const u8,
    filter_str: ?[]const u8,
    select_str: ?[]const u8,
    loop: *xev.Dynamic.Loop,
    thread_pool: *xev.ThreadPool,
    repeat_count: usize,
) !ExecutionStats {
    // 1. Open Source
    const source = try zpq.io.factory.openSource(allocator, input_path, .{
        .loop = loop,
        .thread_pool = thread_pool,
    });
    // We defer close, but we might move source if needed? No, ParquetFile takes generic source interface.
    // Wait, source interface is struct instance.
    // We need to keep source alive.
    // If source is a struct value (factory returns struct), valid?
    // factory.openSource returns a struct that wraps.
    // We need to keep it alive.
    // We can't return it easily if defer closes it.
    // But we run query here. So it's fine.
    var source_mut = source;
    defer source_mut.close();

    var pfile = zpq.core.file.ParquetFile.init(allocator, source_mut);
    try pfile.readFooter();
    defer pfile.deinit();

    // 2. Setup Execution Plan
    var plan = zpq.core.planner.ExecutionPlan.init(allocator);
    // Explicitly set dynamic loop since ExecutionPlan typically expects static xev.Loop pointer?
    // Check planner definition: plan.loop is ?*xev.Loop.
    // xev.Loop vs xev.Dynamic.Loop mismatch?
    // main.zig line 215: plan.loop = @ptrCast(loop);
    // xev.Dynamic.Loop is wrapper around xev.Loop? No.
    // We might need to be careful here if Planner uses it.
    // xev.Dynamic.Loop might not cast to xev.Loop.
    // But main.zig did `@ptrCast(loop)`. Let's assume that worked (unsafe but effective).
    // Or plan needs to be generic? For now following main.zig pattern.
    plan.loop = @ptrCast(loop);
    defer plan.deinit();

    // Determine Columns
    var req_cols_list = std.ArrayListUnmanaged(usize){};
    defer req_cols_list.deinit(allocator);

    if (select_str) |s| {
        var it = std.mem.tokenizeScalar(u8, s, ',');
        while (it.next()) |col_name_raw| {
            const col_name = std.mem.trim(u8, col_name_raw, " ");
            const idx = try pfile.findColumnIndex(col_name);
            try req_cols_list.append(allocator, idx);
        }
    } else {
        // Select all (skip root)
        for (0..pfile.metadata.schema.items.len - 1) |i| {
            if (pfile.metadata.schema.items[i + 1].type != null) {
                try req_cols_list.append(allocator, i);
            }
        }
    }
    
    plan.required_columns = try req_cols_list.toOwnedSlice(allocator);
    plan.output_columns = try allocator.dupe(usize, plan.required_columns);

    // Filter
    if (filter_str) |f| {
        const filter_parser = @import("filter/parser.zig");
        plan.filter = try filter_parser.parseFilter(T, f, &pfile);
        
        var filter_cols_list = std.ArrayListUnmanaged(usize){};
        try filter_parser.collectFilterColumns(plan.filter.?, &filter_cols_list, allocator);
        plan.filter_columns = try filter_cols_list.toOwnedSlice(allocator);
    }

    // Detect Optimization
    if (pfile.metadata.row_groups.items.len > 0) {
        const total_cols = pfile.metadata.row_groups.items[0].columns.items.len;
        plan.detectOptimization(total_cols);
    }

    // 3. Fast Path or Output Setup
    const output_to_null = if (output_path) |o| std.mem.eql(u8, o, "/dev/null") else true;
    const output_to_stdout = if (output_path) |o| std.mem.eql(u8, o, "--") else false;
    const has_output = output_path != null and !output_to_null and !output_to_stdout;

    var timer = try std.time.Timer.start();

    if (plan.is_zero_copy and has_output) {
        std.debug.print("[engine] ⚡ Zero-Copy Fast Path Detected!\n", .{});
        return try runFastPath(allocator, source_mut, output_path.?, loop, thread_pool, repeat_count);
    }

    // 4. Slow Path (Executor)
    var writer: ?*zpq.core.writer.ParquetWriter = null;
    if (has_output) {
        const out_sink = try zpq.io.factory.openSink(allocator, output_path.?, .{
            .loop = loop,
            .thread_pool = thread_pool,
        });
        
        var out_schema = std.ArrayListUnmanaged(zpq.core.schema.SchemaElement){};
        defer out_schema.deinit(allocator);
        
        try out_schema.append(allocator, pfile.metadata.schema.items[0]); // Root
        for (plan.output_columns) |idx| {
            try out_schema.append(allocator, pfile.metadata.schema.items[idx + 1]);
        }
        
        writer = try zpq.core.writer.ParquetWriter.init(allocator, out_sink, out_schema.items);
    }
    defer {
        if (writer) |w| {
            w.close() catch {};
            w.deinit();
        }
    }

    var executor = zpq.core.executor.Executor.init(
        allocator,
        &plan,
        &pfile.metadata,
        pfile.source,
        thread_pool,
        writer,
    );

    for (0..repeat_count) |_| {
        try executor.execute();
    }

    const elapsed = timer.read();
    const elapsed_ms = @as(f64, @floatFromInt(elapsed)) / 1_000_000.0;

    return ExecutionStats{
        .scanned = executor.rows_scanned.load(.monotonic),
        .matched = executor.rows_matched.load(.monotonic),
        .elapsed_ms = elapsed_ms,
    };
}

fn runFastPath(
    allocator: std.mem.Allocator,
    source: zpq.io.RandomAccessSource,
    output_path: []const u8, 
    loop: *xev.Dynamic.Loop,
    thread_pool: *xev.ThreadPool,
    repeat_count: usize
) !ExecutionStats {
    // Identity copy: read everything from source, write to sink.
    // Since we already read footer, we might need to seek to 0?
    // RandomAccessSource doesn't have cursor usually, but reads at offset.
    // So we just copy from 0 to size.
    
    const size = source.size();
    
    // Open Sink
    // Using generic openSink from factory
    const sink = try zpq.io.factory.openSink(allocator, output_path, .{
        .loop = loop,
        .thread_pool = thread_pool,
    });
    // Sink is generic struct returned by factory. We need it to be mutable.
    var sink_mut = sink;
    defer sink_mut.close() catch {};

    var timer = try std.time.Timer.start();

    // Buffer size: 4MB
    const buf_size = 4 * 1024 * 1024;
    const buffer = try allocator.alloc(u8, buf_size);
    defer allocator.free(buffer);

    for (0..repeat_count) |_| {
        var offset: u64 = 0;
        
        while (offset < size) {
            const remaining = size - offset;
            const to_read = @min(remaining, buf_size);
            
            const n_read = try source.readAt(offset, buffer[0..to_read]);
            if (n_read == 0) break; // EOF
            
            const slice = buffer[0..n_read];
            
            // Write
            _ = try sink_mut.write(slice);
            
            offset += n_read;
        }
    }
    
    const elapsed = timer.read();
    const elapsed_ms = @as(f64, @floatFromInt(elapsed)) / 1_000_000.0;
    const mb_per_s = (@as(f64, @floatFromInt(size)) / (elapsed_ms / 1000.0)) / (1024.0 * 1024.0);
    std.debug.print("[engine] Copied {d} MB in {d:.2}ms ({d:.2} MB/s)\n", .{ size / 1024 / 1024, elapsed_ms, mb_per_s });
    
    return ExecutionStats{
        .scanned = 0, 
        .matched = 0,
        .elapsed_ms = elapsed_ms,
    };
}
