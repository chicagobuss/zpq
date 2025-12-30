const std = @import("std");
const zpq = @import("zpq");
const xev = @import("xev");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        printUsage(args[0]);
        return;
    }

    const command = args[1];

    const is_async = for (args) |arg| {
        if (std.mem.eql(u8, arg, "--async")) break true;
    } else false;

    const verify_tls = for (args) |arg| {
        if (std.mem.eql(u8, arg, "--tls-verify")) break true;
    } else false;

    // Initialize core xev infrastructure for all commands
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var thread_pool = xev.ThreadPool.init(.{ .max_threads = 4 });
    defer {
        thread_pool.shutdown();
        thread_pool.deinit();
    }

    var tp_resolver = zpq.s3.dns.ThreadPoolResolverGen(xev).init(&thread_pool, allocator);
    var sf_resolver = zpq.s3.dns.SingleFlightResolverGen(xev).init(allocator, tp_resolver.resolver());
    defer sf_resolver.deinit();
    var spec_resolver = zpq.s3.dns.SpeculativeResolverGen(xev).init(allocator, sf_resolver.resolver());

    const resolver = spec_resolver.resolver();

    if (std.mem.eql(u8, command, "schema")) {
        if (args.len < 3) {
            std.debug.print("Usage: {s} schema <parquet_file>\n", .{args[0]});
            return;
        }
        try cmdSchema(allocator, args[2], is_async, &loop, &thread_pool, resolver, verify_tls);
    } else if (std.mem.eql(u8, command, "meta")) {
        if (args.len < 3) {
            std.debug.print("Usage: {s} meta <parquet_file>\n", .{args[0]});
            return;
        }
        try cmdMeta(allocator, args[2], is_async, &loop, &thread_pool, resolver, verify_tls);
    } else if (std.mem.eql(u8, command, "cat")) {
        if (args.len < 3) {
            std.debug.print("Usage: {s} cat <parquet_file> [limit]\n", .{args[0]});
            return;
        }
        const limit = if (args.len > 3) try std.fmt.parseInt(usize, args[3], 10) else 10;
        try cmdCat(allocator, args[2], limit, is_async, &loop, &thread_pool, resolver, verify_tls);
    } else if (std.mem.eql(u8, command, "scan")) {
        if (args.len < 3) {
            std.debug.print("Usage: {s} scan <parquet_file>\n", .{args[0]});
            return;
        }
        try cmdScan(allocator, args[2], is_async, &loop, &thread_pool, resolver, verify_tls);
    } else if (std.mem.eql(u8, command, "pages")) {
        if (args.len < 3) {
            std.debug.print("Usage: {s} pages <parquet_file>\n", .{args[0]});
            return;
        }
        try cmdPages(allocator, args[2], is_async, &loop, &thread_pool, resolver, verify_tls);
    } else if (std.mem.eql(u8, command, "inspect")) {
        // Legacy support
        if (args.len < 3) {
            std.debug.print("Usage: {s} inspect <parquet_file> [--async]\n", .{args[0]});
            return;
        }
        try cmdPages(allocator, args[2], is_async, &loop, &thread_pool, resolver, verify_tls);
    } else if (std.mem.eql(u8, command, "debug-s3")) {
        if (args.len < 3) {
            std.debug.print("Usage: {s} debug-s3 <parquet_file> [--async]\n", .{args[0]});
            return;
        }
        try cmdDebugS3(allocator, args[2], is_async, &loop, &thread_pool, resolver, verify_tls);
    } else {
        printUsage(args[0]);
    }
}

const Resolver = zpq.s3.dns.ResolverGen(xev);

fn cmdDebugS3(allocator: std.mem.Allocator, path: []const u8, is_async: bool, loop: *xev.Loop, thread_pool: *xev.ThreadPool, resolver: Resolver, verify_tls: bool) !void {
    std.debug.print("Opening file: {s}\n", .{path});
    var pf = try openFile(allocator, path, is_async, loop, thread_pool, resolver, verify_tls);
    defer pf.deinit();

    // We assume it's S3Source.
    var buf: [1024]u8 = undefined;

    std.debug.print("Read 1 (offset 0, 1024 bytes)...\n", .{});
    var timer = try std.time.Timer.start();
    _ = try pf.source.readAt(0, &buf);
    var elapsed = timer.read();
    std.debug.print("Read 1 took: {d:.4}s\n", .{@as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0});

    std.debug.print("Read 2 (offset 1024, 1024 bytes)...\n", .{});
    timer.reset();
    _ = try pf.source.readAt(1024, &buf);
    elapsed = timer.read();
    std.debug.print("Read 2 took: {d:.4}s\n", .{@as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0});

    std.debug.print("Read 3 (offset 2048, 1024 bytes)...\n", .{});
    timer.reset();
    _ = try pf.source.readAt(2048, &buf);
    elapsed = timer.read();
    std.debug.print("Read 3 took: {d:.4}s\n", .{@as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0});
}

fn printUsage(exe_name: []const u8) void {
    std.debug.print(
        \\Usage: {s} <command> [options]
        \\
        \\Commands:
        \\  schema <file>       Show the schema tree
        \\  meta   <file>       Show file and row group metadata
        \\  cat    <file>       Dump row data (JSON-like)
        \\  scan   <file>       Benchmark scan speed (no output)
        \\  pages  <file>       Inspect page headers and encodings (deep dive)
        \\
        \\Options:
        \\  --tls-verify        Enable TLS certificate verification for S3/HTTPS
        \\  --async             Force async I/O path
        \\
        \\Environment Variables:
        \\  S3_ENDPOINT         Custom S3 endpoint (e.g. http://localhost:9000 for MinIO)
        \\
    , .{exe_name});
}

fn openFile(
    allocator: std.mem.Allocator,
    path: []const u8,
    force_async: bool,
    loop: *xev.Loop,
    thread_pool: *xev.ThreadPool,
    resolver: Resolver,
    verify_tls: bool,
) !zpq.file.ParquetFile {
    return zpq.s3.factory.openFileWithOptions(
        allocator,
        path,
        .{
            .force_async = force_async,
            .loop = loop,
            .thread_pool = thread_pool,
            .resolver = resolver,
            .verify_tls = verify_tls,
        },
    );
}

fn cmdScan(allocator: std.mem.Allocator, path: []const u8, is_async: bool, loop: *xev.Loop, thread_pool: *xev.ThreadPool, resolver: Resolver, verify_tls: bool) !void {
    var timer = try std.time.Timer.start();

    var pf = try openFile(allocator, path, is_async, loop, thread_pool, resolver, verify_tls);
    defer pf.deinit();
    try pf.readFooter();

    var total_values: u64 = 0;

    if (pf.metadata) |meta| {
        for (meta.row_groups.items, 0..) |rg_meta, rg_idx| {
            var rg = try pf.rowGroup(rg_idx);
            defer rg.deinit();

            // Trigger Massive Parallel Prefetch!
            try rg.prefetch(null); // Prefetch all columns

            for (rg_meta.columns.items, 0..) |col, col_idx| {
                if (col.meta_data) |md| {
                    const levels = meta.getColumnLevels(md.path_in_schema.items);
                    const schema_elem = meta.getColumnSchema(md.path_in_schema.items);
                    const type_length = if (schema_elem) |se| se.type_length else null;
                    const reader = try rg.columnReader(col_idx);

                    // Type dispatch for full-materialization scan
                    const n = switch (md.type) {
                        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => try scanColumnBatch(allocator, []const u8, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length),
                        .INT32 => try scanColumnBatch(allocator, i32, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length),
                        .INT64 => try scanColumnBatch(allocator, i64, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length),
                        .INT96 => try scanColumnBatch(allocator, [12]u8, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length),
                        .FLOAT => try scanColumnBatch(allocator, f32, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length),
                        .DOUBLE => try scanColumnBatch(allocator, f64, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length),
                        else => blk: {
                            // Fallback for unsupported types (just count pages)
                            var reader_inner = try rg.columnReader(col_idx);
                            var count: u64 = 0;
                            while (try reader_inner.next(allocator)) |page| {
                                var p = page;
                                defer p.deinit(allocator);
                                if (p.header.data_page_header) |dph| count += @intCast(dph.num_values);
                            }
                            break :blk count;
                        },
                    };
                    total_values += n;
                }
            }
        }
    }

    const elapsed_ns = timer.read();
    const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0;
    const mvals_per_s = if (elapsed_s > 0) @as(f64, @floatFromInt(total_values)) / elapsed_s / 1_000_000.0 else 0.0;

    std.debug.print("Scanned {d} values in {d:.2}ms ({d:.2} MVal/s)\n", .{ total_values, @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0, mvals_per_s });
}

fn scanColumnBatch(allocator: std.mem.Allocator, comptime T: type, reader: zpq.column.ColumnReader, col_type: zpq.schema.Type, max_def: u16, max_rep: u16, type_length: ?i32) !u64 {
    var batch_reader = zpq.core.batch_reader.BatchReader(T).init(allocator, reader, col_type, max_def, max_rep, type_length);
    defer batch_reader.deinit();

    var total: u64 = 0;
    var buffer: [1024]?T = undefined;

    while (true) {
        const n = try batch_reader.nextBatch(&buffer);
        if (n == 0) break;
        total += n;
    }
    return total;
}

fn cmdSchema(allocator: std.mem.Allocator, path: []const u8, is_async: bool, loop: *xev.Loop, thread_pool: *xev.ThreadPool, resolver: Resolver, verify_tls: bool) !void {
    var pf = try openFile(allocator, path, is_async, loop, thread_pool, resolver, verify_tls);
    defer pf.deinit();
    try pf.readFooter();

    if (pf.metadata) |meta| {
        std.debug.print("Schema for {s}:\n", .{path});
        for (meta.schema.items, 0..) |elem, i| {
            const indent = if (elem.num_children == null) "  " else "";
            const rt = elem.repetition_type orelse .REQUIRED;
            std.debug.print("{s}[{d}] {s} ({any}/{d})", .{ indent, i, elem.name, rt, @intFromEnum(rt) });
            if (elem.type) |t| {
                std.debug.print(" type={any}", .{t});
            }
            if (elem.type_length) |tl| {
                std.debug.print(" len={d}", .{tl});
            }
            if (elem.field_id) |fid| {
                std.debug.print(" id={d}", .{fid});
            }
            std.debug.print("\n", .{});
        }
    }
}

fn cmdMeta(allocator: std.mem.Allocator, path: []const u8, is_async: bool, loop: *xev.Loop, thread_pool: *xev.ThreadPool, resolver: Resolver, verify_tls: bool) !void {
    var pf = try openFile(allocator, path, is_async, loop, thread_pool, resolver, verify_tls);
    defer pf.deinit();
    try pf.readFooter();

    if (pf.metadata) |meta| {
        std.debug.print("File: {s}\n", .{path});
        std.debug.print("Version: {d}\n", .{meta.version});
        std.debug.print("Rows: {d}\n", .{meta.num_rows});
        std.debug.print("Created By: {s}\n", .{meta.created_by orelse "unknown"});
        std.debug.print("Row Groups: {d}\n", .{meta.row_groups.items.len});

        for (meta.row_groups.items, 0..) |rg, i| {
            std.debug.print("\nRow Group {d}:\n", .{i});
            std.debug.print("  Rows: {d}\n", .{rg.num_rows});
            std.debug.print("  Total Bytes: {d}\n", .{rg.total_byte_size});

            std.debug.print("  Columns:\n", .{});
            for (rg.columns.items, 0..) |col, j| {
                if (col.meta_data) |md| {
                    const ratio = if (md.total_compressed_size > 0)
                        @as(f64, @floatFromInt(md.total_uncompressed_size)) / @as(f64, @floatFromInt(md.total_compressed_size))
                    else
                        0.0;

                    std.debug.print("    [{d}] {any} ({any}) ratio={d:.2}x\n", .{ j, md.type, md.codec, ratio });
                    std.debug.print("          Values: {d}, Enc: ", .{md.num_values});
                    for (md.encodings.items) |enc| {
                        std.debug.print("{any} ", .{enc});
                    }
                    std.debug.print("\n", .{});
                }
            }
        }
    }
}

fn cmdCat(allocator: std.mem.Allocator, path: []const u8, limit: usize, is_async: bool, loop: *xev.Loop, thread_pool: *xev.ThreadPool, resolver: Resolver, verify_tls: bool) !void {
    var pf = try openFile(allocator, path, is_async, loop, thread_pool, resolver, verify_tls);
    defer pf.deinit();
    try pf.readFooter();

    // Simple Columnar Dump
    if (pf.metadata) |meta| {
        for (meta.row_groups.items, 0..) |rg, rg_idx| {
            var rg_reader = try pf.rowGroup(rg_idx);
            defer rg_reader.deinit();

            for (rg.columns.items, 0..) |col, col_idx| {
                if (col.meta_data) |md| {
                    std.debug.print("Column {d} (", .{col_idx});
                    for (md.path_in_schema.items, 0..) |part, k| {
                        if (k > 0) std.debug.print(".", .{});
                        std.debug.print("{s}", .{part});
                    }
                    std.debug.print("):\n", .{});

                    const levels = meta.getColumnLevels(md.path_in_schema.items);
                    const schema_elem = meta.getColumnSchema(md.path_in_schema.items);
                    const type_length = if (schema_elem) |se| se.type_length else null;
                    const reader = try rg_reader.columnReader(col_idx);

                    // Type dispatch for BatchReader
                    switch (md.type) {
                        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => try dumpColumnBatch(allocator, []const u8, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, limit),
                        .INT32 => try dumpColumnBatch(allocator, i32, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, limit),
                        .INT64 => try dumpColumnBatch(allocator, i64, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, limit),
                        .INT96 => try dumpColumnBatch(allocator, [12]u8, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, limit),
                        .FLOAT => try dumpColumnBatch(allocator, f32, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, limit),
                        .DOUBLE => try dumpColumnBatch(allocator, f64, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, limit),
                        else => std.debug.print("        (Type {any} not yet supported by BatchReader)\n", .{md.type}),
                    }
                }
            }
        }
    }
}

fn formatInt96(val: [12]u8) !void {
    const nanos = std.mem.readInt(u64, val[0..8], .little);
    const days = std.mem.readInt(u32, val[8..12], .little);
    
    // Julian Day 2440588 is 1970-01-01
    const julian_epoch = 2440588;
    const unix_seconds = (@as(i64, days) - julian_epoch) * 86400 + @as(i64, @intCast(nanos / 1_000_000_000));
    
    std.debug.print("{d} (JD={d}, NS={d})", .{ unix_seconds, days, nanos });
}

fn dumpColumnBatch(allocator: std.mem.Allocator, comptime T: type, reader: zpq.column.ColumnReader, col_type: zpq.schema.Type, max_def: u16, max_rep: u16, type_length: ?i32, limit: usize) !void {
    var batch_reader = zpq.core.batch_reader.BatchReader(T).init(allocator, reader, col_type, max_def, max_rep, type_length);
    defer batch_reader.deinit();

    var values_printed: usize = 0;
    var buffer: [1024]?T = undefined;

    while (values_printed < limit) {
        const batch_size = @min(buffer.len, limit - values_printed);
        const n = try batch_reader.nextBatch(buffer[0..batch_size]);
        if (n == 0) break;

        for (buffer[0..n]) |maybe_val| {
            if (maybe_val) |val| {
                if (T == []const u8) {
                    std.debug.print("  {s}\n", .{val});
                } else if (T == [12]u8) {
                    std.debug.print("  ", .{});
                    try formatInt96(val);
                    std.debug.print("\n", .{});
                } else {
                    std.debug.print("  {any}\n", .{val});
                }
            } else {
                std.debug.print("  null\n", .{});
            }
            values_printed += 1;
        }
    }
}

// The detailed deep-dive inspection (formerly 'inspect')
fn cmdPages(allocator: std.mem.Allocator, path: []const u8, is_async: bool, loop: *xev.Loop, thread_pool: *xev.ThreadPool, resolver: Resolver, verify_tls: bool) !void {
    var pf = try openFile(allocator, path, is_async, loop, thread_pool, resolver, verify_tls);
    defer pf.deinit();

    try pf.readFooter();

    if (pf.metadata) |meta| {
        // We skip printing metadata summary here as that is for 'meta' command

        for (meta.row_groups.items, 0..) |rg, i| {
            std.debug.print("Row Group {d}:\n", .{i});

            for (rg.columns.items, 0..) |col, j| {
                std.debug.print("    Column {d}:\n", .{j});

                // Store dictionary values for this column
                var dict_strings = std.ArrayListUnmanaged([]const u8){};
                defer dict_strings.deinit(allocator);
                var dict_int32 = std.ArrayListUnmanaged(i32){};
                defer dict_int32.deinit(allocator);
                var dict_int64 = std.ArrayListUnmanaged(i64){};
                defer dict_int64.deinit(allocator);
                var dict_double = std.ArrayListUnmanaged(f64){};
                defer dict_double.deinit(allocator);
                var dict_float = std.ArrayListUnmanaged(f32){};
                defer dict_float.deinit(allocator);

                if (col.meta_data) |md| {
                    std.debug.print("      Type: {any}, Codec: {any}\n", .{ md.type, md.codec });

                    const levels = meta.getColumnLevels(md.path_in_schema.items);
                    std.debug.print("      Levels: MaxDef={d}, MaxRep={d}\n", .{ levels.max_def, levels.max_rep });

                    var reader = try zpq.column.ColumnReader.init(pf.source, col);
                    var page_idx: usize = 0;
                    while (try reader.next(allocator)) |page| {
                        var p = page;
                        defer p.deinit(allocator);
                        std.debug.print("      Page {d}: {any} Size={d} (Comp={d})\n", .{ page_idx, p.header.type, p.header.uncompressed_page_size, p.header.compressed_page_size });

                        if (p.header.type == .DICTIONARY_PAGE) {
                            var decoder = zpq.decoder.Decoder.init(p.data);
                            std.debug.print("        Dictionary Values ({d} bytes):\n", .{p.data.len});
                            if (md.type == .BYTE_ARRAY) {
                                while (decoder.hasMore()) {
                                    const val = try decoder.readByteArray();
                                    // Store copy of string because p.data will be freed
                                    const val_copy = try allocator.dupe(u8, val);
                                    try dict_strings.append(allocator, val_copy);
                                    // std.debug.print("          [{d}] {s}\n", .{dict_strings.items.len - 1, val});
                                }
                            } else if (md.type == .INT32) {
                                while (decoder.hasMore()) {
                                    const val = try decoder.readInt32();
                                    try dict_int32.append(allocator, val);
                                }
                            } else if (md.type == .INT64) {
                                while (decoder.hasMore()) {
                                    const val = try decoder.readInt64();
                                    try dict_int64.append(allocator, val);
                                }
                            } else if (md.type == .DOUBLE) {
                                while (decoder.hasMore()) {
                                    const val = try decoder.readDouble();
                                    try dict_double.append(allocator, val);
                                }
                            } else if (md.type == .FLOAT) {
                                while (decoder.hasMore()) {
                                    const val = try decoder.readFloat();
                                    try dict_float.append(allocator, val);
                                }
                            }
                        } else if (p.header.type == .DATA_PAGE) {
                            if (p.header.data_page_header) |dph| {
                                std.debug.print("        Encoding: {any}, Values: {d}\n", .{ dph.encoding, dph.num_values });
                                if (dph.encoding == .RLE_DICTIONARY or dph.encoding == .PLAIN_DICTIONARY) {
                                    var data_slice = p.data;

                                    // Skip Repetition Levels
                                    if (levels.max_rep > 0) {
                                        if (data_slice.len < 4) {
                                            std.debug.print("        Error: Not enough data for Repetition Levels length\n", .{});
                                            continue;
                                        }
                                        const len = std.mem.readInt(u32, data_slice[0..4], .little);
                                        // std.debug.print("        Skipping Repetition Levels: {d} bytes\n", .{len});
                                        if (data_slice.len < 4 + len) {
                                            std.debug.print("        Error: Not enough data for Repetition Levels\n", .{});
                                            continue;
                                        }
                                        data_slice = data_slice[4 + len ..];
                                    }

                                    // Decode Definition Levels
                                    var def_levels = std.ArrayListUnmanaged(i32){};
                                    defer def_levels.deinit(allocator);

                                    if (levels.max_def > 0) {
                                        if (data_slice.len < 4) {
                                            std.debug.print("        Error: Not enough data for Definition Levels length\n", .{});
                                            continue;
                                        }
                                        const len = std.mem.readInt(u32, data_slice[0..4], .little);
                                        // std.debug.print("        Definition Levels: {d} bytes\n", .{len});
                                        if (data_slice.len < 4 + len) {
                                            std.debug.print("        Error: Not enough data for Definition Levels\n", .{});
                                            continue;
                                        }

                                        const def_level_data = data_slice[4 .. 4 + len];
                                        data_slice = data_slice[4 + len ..];

                                        const max_val = @as(u32, @intCast(levels.max_def)) + 1;
                                        const next_pow2 = try std.math.ceilPowerOfTwo(u32, max_val);
                                        const bit_width = std.math.log2_int(u32, next_pow2);

                                        var rle_dec = zpq.rle.RleDecoder.init(def_level_data, @intCast(bit_width));

                                        var count: usize = 0;
                                        while (count < dph.num_values) : (count += 1) {
                                            const res = rle_dec.next();
                                            if (res) |maybe_val| {
                                                if (maybe_val) |val| {
                                                    try def_levels.append(allocator, @intCast(val));
                                                } else {
                                                    break;
                                                }
                                            } else |err| {
                                                std.debug.print("Error decoding def level: {any}\n", .{err});
                                                break;
                                            }
                                        }
                                    }

                                    if (data_slice.len > 0) {
                                        const bit_width = data_slice[0];
                                        std.debug.print("        Indices BitWidth: {d}\n", .{bit_width});
                                        var rle_dec = zpq.rle.RleDecoder.init(data_slice[1..], bit_width);

                                        var print_count: usize = 0;

                                        std.debug.print("        Data Sample:\n", .{});

                                        // If max_def > 0, we iterate def_levels
                                        if (levels.max_def > 0) {
                                            for (def_levels.items) |dl| {
                                                if (dl == levels.max_def) {
                                                    // Value present, read index
                                                    if (try rle_dec.next()) |idx| {
                                                        if (print_count < 10) {
                                                            if (md.type == .BYTE_ARRAY) {
                                                                if (idx < dict_strings.items.len) {
                                                                    std.debug.print("          - {s}\n", .{dict_strings.items[idx]});
                                                                } else {
                                                                    std.debug.print("          - <idx {d} out of bounds>\n", .{idx});
                                                                }
                                                            } else if (md.type == .INT32) {
                                                                if (idx < dict_int32.items.len) {
                                                                    std.debug.print("          - {d}\n", .{dict_int32.items[idx]});
                                                                } else {
                                                                    std.debug.print("          - <idx {d} out of bounds>\n", .{idx});
                                                                }
                                                            } else if (md.type == .INT64) {
                                                                if (idx < dict_int64.items.len) {
                                                                    std.debug.print("          - {d}\n", .{dict_int64.items[idx]});
                                                                } else {
                                                                    std.debug.print("          - <idx {d} out of bounds>\n", .{idx});
                                                                }
                                                            } else if (md.type == .DOUBLE) {
                                                                if (idx < dict_double.items.len) {
                                                                    std.debug.print("          - {d}\n", .{dict_double.items[idx]});
                                                                } else {
                                                                    std.debug.print("          - <idx {d} out of bounds>\n", .{idx});
                                                                }
                                                            } else if (md.type == .FLOAT) {
                                                                if (idx < dict_float.items.len) {
                                                                    std.debug.print("          - {d}\n", .{dict_float.items[idx]});
                                                                } else {
                                                                    std.debug.print("          - <idx {d} out of bounds>\n", .{idx});
                                                                }
                                                            } else {
                                                                std.debug.print("          - <idx {d}>\n", .{idx});
                                                            }
                                                            print_count += 1;
                                                        } else if (print_count == 10) {
                                                            std.debug.print("          ... \n", .{});
                                                            print_count += 1;
                                                        }
                                                    }
                                                } else {
                                                    // NULL
                                                    if (print_count < 10) {
                                                        std.debug.print("          - NULL\n", .{});
                                                        print_count += 1;
                                                    }
                                                }
                                            }
                                        } else {
                                            // No definition levels, all values present
                                            var k: i32 = 0;
                                            while (k < dph.num_values) : (k += 1) {
                                                if (try rle_dec.next()) |idx| {
                                                    if (print_count < 10) {
                                                        if (md.type == .BYTE_ARRAY) {
                                                            if (idx < dict_strings.items.len) {
                                                                std.debug.print("          - {s}\n", .{dict_strings.items[idx]});
                                                            } else {
                                                                std.debug.print("          - <idx {d} out of bounds>\n", .{idx});
                                                            }
                                                        } else if (md.type == .INT64) {
                                                            if (idx < dict_int64.items.len) {
                                                                std.debug.print("          - {d}\n", .{dict_int64.items[idx]});
                                                            } else {
                                                                std.debug.print("          - <idx {d} out of bounds>\n", .{idx});
                                                            }
                                                        } else {
                                                            std.debug.print("          - <idx {d}>\n", .{idx});
                                                        }
                                                        print_count += 1;
                                                    } else if (print_count == 10) {
                                                        std.debug.print("          ... \n", .{});
                                                        print_count += 1;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        page_idx += 1;
                    }
                }

                // Cleanup dictionary strings
                for (dict_strings.items) |s| {
                    allocator.free(s);
                }
            }
        }
    }
}
