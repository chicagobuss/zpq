const std = @import("std");
const zpq = @import("zpq");

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

    if (std.mem.eql(u8, command, "schema")) {
        if (args.len < 3) {
            std.debug.print("Usage: {s} schema <parquet_file>\n", .{args[0]});
            return;
        }
        try cmdSchema(allocator, args[2]);
    } else if (std.mem.eql(u8, command, "meta")) {
        if (args.len < 3) {
            std.debug.print("Usage: {s} meta <parquet_file>\n", .{args[0]});
            return;
        }
        try cmdMeta(allocator, args[2]);
    } else if (std.mem.eql(u8, command, "cat")) {
        if (args.len < 3) {
            std.debug.print("Usage: {s} cat <parquet_file> [limit]\n", .{args[0]});
            return;
        }
        const limit = if (args.len > 3) try std.fmt.parseInt(usize, args[3], 10) else 10;
        try cmdCat(allocator, args[2], limit);
    } else if (std.mem.eql(u8, command, "scan")) {
        if (args.len < 3) {
            std.debug.print("Usage: {s} scan <parquet_file>\n", .{args[0]});
            return;
        }
        try cmdScan(allocator, args[2]);
    } else if (std.mem.eql(u8, command, "pages")) {
        if (args.len < 3) {
            std.debug.print("Usage: {s} pages <parquet_file>\n", .{args[0]});
            return;
        }
        try cmdPages(allocator, args[2]);
    } else if (std.mem.eql(u8, command, "inspect")) {
        // Legacy support
        if (args.len < 3) {
            std.debug.print("Usage: {s} inspect <parquet_file>\n", .{args[0]});
            return;
        }
        try cmdPages(allocator, args[2]);
    } else if (std.mem.eql(u8, command, "debug-s3")) {
        if (args.len < 3) {
            std.debug.print("Usage: {s} debug-s3 <parquet_file>\n", .{args[0]});
            return;
        }
        try cmdDebugS3(allocator, args[2]);
    } else {
        printUsage(args[0]);
    }
}

fn cmdDebugS3(allocator: std.mem.Allocator, path: []const u8) !void {
    std.debug.print("Opening file: {s}\n", .{path});
    var pf = try openFile(allocator, path);
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
        \\Environment Variables:
        \\  S3_ENDPOINT         Custom S3 endpoint (e.g. http://localhost:9000 for MinIO)
        \\
    , .{exe_name});
}

fn cleanupS3(ctx: *anyopaque, allocator: std.mem.Allocator) void {
    const s: *zpq.s3.S3Source = @ptrCast(@alignCast(ctx));
    s.deinit();
    allocator.destroy(s);
}

const AsyncS3Context = struct {
    pool: zpq.s3.ConnectionPool,
    source: zpq.s3.AsyncS3Source,
    allocator: std.mem.Allocator,
    host_owned: ?[]const u8 = null,
    
    pub fn deinit(self: *AsyncS3Context) void {
        self.source.deinit();
        self.pool.deinit();
        if (self.host_owned) |h| self.allocator.free(h);
    }
};

fn cleanupAsyncS3(ctx: *anyopaque, allocator: std.mem.Allocator) void {
    const s: *AsyncS3Context = @ptrCast(@alignCast(ctx));
    s.deinit();
    allocator.destroy(s);
}

fn getEnvOrNull(allocator: std.mem.Allocator, key: []const u8) !?[]u8 {
    return std.process.getEnvVarOwned(allocator, key) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => null,
        else => err,
    };
}

fn openFile(allocator: std.mem.Allocator, path: []const u8) !zpq.file.ParquetFile {
    if (std.mem.startsWith(u8, path, "s3://")) {
        var bucket: []const u8 = undefined;
        var key: []const u8 = undefined;
        
        const no_scheme = path[5..];
        if (std.mem.indexOf(u8, no_scheme, "/")) |idx| {
             bucket = no_scheme[0..idx];
             key = no_scheme[idx+1..];
        } else {
             return error.InvalidS3Path;
        }
        
        // Check if we want to use Async (default for now for benchmark)
        // We'll use AsyncS3Source.
        
        const endpoint_env = try getEnvOrNull(allocator, "S3_ENDPOINT");
        defer if (endpoint_env) |ep| allocator.free(ep);
        
        // Parse Endpoint
        var host: []const u8 = "s3.amazonaws.com"; // Default
        var port: u16 = 443;
        var use_tls: bool = true;
        
        if (endpoint_env) |ep| {
            const uri = try std.Uri.parse(ep);
            if (uri.host) |h| {
                switch (h) {
                    .raw => |s| host = s,
                    .percent_encoded => |s| host = s,
                }
            } else return error.InvalidEndpoint;
            
            port = uri.port orelse (if (std.mem.eql(u8, uri.scheme, "https")) 443 else 80);
            use_tls = std.mem.eql(u8, uri.scheme, "https");
        } else {
            // TODO: Region support for host resolution
            // For now assume standard or use env
            if (try getEnvOrNull(allocator, "AWS_REGION")) |region| {
                defer allocator.free(region);
                // Construct host: s3.{region}.amazonaws.com
                // We need to allocate this host string if it's dynamic
                // For this quick hack, let's just stick to default or S3_ENDPOINT
                // If region is us-east-1, it's s3.amazonaws.com
                if (!std.mem.eql(u8, region, "us-east-1")) {
                     // Alloc host string
                     // host = try std.fmt.allocPrint(allocator, "s3.{s}.amazonaws.com", .{region});
                     // But we can't easily free it down the line without tracking it.
                     // Let's assume S3_ENDPOINT is used for now or we use default.
                }
            }
        }

        const ctx = try allocator.create(AsyncS3Context);
        errdefer allocator.destroy(ctx);
        
        ctx.allocator = allocator;
        ctx.pool = zpq.s3.ConnectionPool.init(allocator);
        errdefer ctx.pool.deinit();
        
        // Initialize Source
        // Note: host is either slice of env var or string literal. 
        // AsyncS3Source.init copies path, but stores host reference?
        // Let's check AsyncS3Source.init.
        // It stores `host: []const u8`. It does NOT copy host.
        // So we must ensure host stays alive. 
        // If it came from `endpoint_env`, it's freed at end of scope.
        // We should duplicate it into `ctx`.
        
        const host_copy = try allocator.dupe(u8, host);
        errdefer allocator.free(host_copy);
        
        ctx.host_owned = host_copy;

        const ca_cert_env = try getEnvOrNull(allocator, "S3_CA_CERT");
        defer if (ca_cert_env) |c| allocator.free(c);

        var trusted_cert: ?[]const u8 = null;
        if (ca_cert_env) |path_val| {
            std.debug.print("[Main] Loading CA cert from {s}\n", .{path_val});
            const max_size = 1024 * 1024;
            const content = try std.fs.cwd().readFileAlloc(path_val, allocator, @enumFromInt(max_size));
            trusted_cert = content;
            std.debug.print("[Main] Loaded cert: {d} bytes\n", .{content.len});
        }
        defer if (trusted_cert) |c| allocator.free(c);
        
        ctx.source = try zpq.s3.AsyncS3Source.init(
            allocator, 
            &ctx.pool, 
            host_copy, 
            port, 
            bucket, 
            key, 
            use_tls, 
            trusted_cert
        );
        
        return zpq.file.ParquetFile.initOwned(allocator, ctx.source.source(), ctx, cleanupAsyncS3);
    }
    
    return zpq.file.ParquetFile.open(allocator, path);
}

fn cmdScan(allocator: std.mem.Allocator, path: []const u8) !void {
    var timer = try std.time.Timer.start();
    
    var pf = try openFile(allocator, path);
    defer pf.deinit();
    try pf.readFooter();
    
    var total_values: u64 = 0;
    var total_bytes: u64 = 0;

    if (pf.metadata) |meta| {
        for (meta.row_groups.items) |rg| {
            for (rg.columns.items) |col| {
                if (col.meta_data) |md| {
                    _ = md; // unused
                    var reader = try zpq.column.ColumnReader.init(pf.source, allocator, col);
                    while (try reader.next()) |page| {
                        var p = page;
                        defer p.deinit();
                        total_bytes += p.data.len;
                        if (p.header.data_page_header) |dph| {
                             total_values += @intCast(dph.num_values);
                             // Force decompression by accessing data
                             // (already done by next() if snappy)
                        }
                    }
                }
            }
        }
    }
    
    const elapsed_ns = timer.read();
    const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0;
    const mb = @as(f64, @floatFromInt(total_bytes)) / (1024.0 * 1024.0);
    
    std.debug.print("Scanned {d} values ({d:.2} MB uncompressed page data) in {d:.4}s\n", .{total_values, mb, elapsed_s});
    std.debug.print("Throughput: {d:.2} MB/s (pages), {d:.2} MVal/s\n", .{mb / elapsed_s, @as(f64, @floatFromInt(total_values)) / elapsed_s / 1_000_000.0});
}

fn cmdSchema(allocator: std.mem.Allocator, path: []const u8) !void {
    var pf = try openFile(allocator, path);
    defer pf.deinit();
    try pf.readFooter();

    if (pf.metadata) |meta| {
        std.debug.print("Schema for {s}:\n", .{path});
        for (meta.schema.items, 0..) |elem, i| {
            const indent = if (elem.num_children == null) "  " else "";
            std.debug.print("{s}[{d}] {s} ({any})", .{indent, i, elem.name, elem.repetition_type orelse .REQUIRED});
            if (elem.type) |t| {
                std.debug.print(" type={any}", .{t});
            }
            if (elem.field_id) |fid| {
                std.debug.print(" id={d}", .{fid});
            }
            std.debug.print("\n", .{});
        }
    }
}

fn cmdMeta(allocator: std.mem.Allocator, path: []const u8) !void {
    var pf = try openFile(allocator, path);
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
                        else 0.0;
                    
                    std.debug.print("    [{d}] {any} ({any}) ratio={d:.2}x\n", .{j, md.type, md.codec, ratio});
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

fn cmdCat(allocator: std.mem.Allocator, path: []const u8, limit: usize) !void {
    var pf = try openFile(allocator, path);
    defer pf.deinit();
    try pf.readFooter();
    
    // Simple Columnar Dump
    if (pf.metadata) |meta| {
        for (meta.row_groups.items) |rg| {
            for (rg.columns.items, 0..) |col, col_idx| {
                if (col.meta_data) |md| {
                    std.debug.print("Column {d} (", .{col_idx});
                    for (md.path_in_schema.items, 0..) |part, k| {
                        if (k > 0) std.debug.print(".", .{});
                        std.debug.print("{s}", .{part});
                    }
                    std.debug.print("):\n", .{});
                    
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

                    const levels = meta.getColumnLevels(md.path_in_schema.items);
                    var reader = try zpq.column.ColumnReader.init(pf.source, allocator, col);
                    
                    var values_printed: usize = 0;
                    
                    while (try reader.next()) |page| {
                        if (values_printed >= limit) break;
                        
                        var p = page;
                        defer p.deinit();
                        
                        if (p.header.type == .DICTIONARY_PAGE) {
                             var decoder = zpq.decoder.Decoder.init(p.data);
                             if (md.type == .BYTE_ARRAY) {
                                 while (decoder.hasMore()) {
                                     const val = try decoder.readByteArray();
                                     const val_copy = try allocator.dupe(u8, val);
                                     try dict_strings.append(allocator, val_copy);
                                 }
                             } else if (md.type == .INT32) {
                                 while (decoder.hasMore()) try dict_int32.append(allocator, try decoder.readInt32());
                             } else if (md.type == .INT64) {
                                 while (decoder.hasMore()) try dict_int64.append(allocator, try decoder.readInt64());
                             } else if (md.type == .DOUBLE) {
                                 while (decoder.hasMore()) try dict_double.append(allocator, try decoder.readDouble());
                             } else if (md.type == .FLOAT) {
                                 while (decoder.hasMore()) try dict_float.append(allocator, try decoder.readFloat());
                             }
                        } else if (p.header.type == .DATA_PAGE) {
                            if (p.header.data_page_header) |dph| {
                                if (dph.encoding == .RLE_DICTIONARY or dph.encoding == .PLAIN_DICTIONARY) {
                                    var data_slice = p.data;
                                    
                                    // Skip Repetition Levels
                                    if (levels.max_rep > 0) {
                                        if (data_slice.len < 4) break;
                                        const len = std.mem.readInt(u32, data_slice[0..4], .little);
                                        if (data_slice.len < 4 + len) break;
                                        data_slice = data_slice[4 + len ..];
                                    }

                                    // Decode Definition Levels
                                    var def_levels = std.ArrayListUnmanaged(i32){};
                                    defer def_levels.deinit(allocator);
                                    
                                    if (levels.max_def > 0) {
                                        if (data_slice.len < 4) break;
                                        const len = std.mem.readInt(u32, data_slice[0..4], .little);
                                        if (data_slice.len < 4 + len) break;
                                        const def_level_data = data_slice[4 .. 4 + len];
                                        data_slice = data_slice[4 + len ..];
                                        
                                        const max_val = @as(u32, @intCast(levels.max_def)) + 1;
                                        const next_pow2 = try std.math.ceilPowerOfTwo(u32, max_val);
                                        const bit_width = std.math.log2_int(u32, next_pow2);
                                        var rle_dec = zpq.rle.RleDecoder.init(def_level_data, @intCast(bit_width));
                                        
                                        var count: usize = 0;
                                        while (count < dph.num_values) : (count += 1) {
                                            if (rle_dec.next()) |res| {
                                                if (res) |val| try def_levels.append(allocator, @intCast(val));
                                            } else |_| break;
                                        }
                                    }

                                    if (data_slice.len > 0) {
                                        const bit_width = data_slice[0];
                                        var rle_dec = zpq.rle.RleDecoder.init(data_slice[1..], bit_width);
                                        
                                        if (levels.max_def > 0) {
                                            for (def_levels.items) |dl| {
                                                if (values_printed >= limit) break;
                                                
                                                if (dl == levels.max_def) {
                                                    if (try rle_dec.next()) |idx| {
                                                        if (md.type == .BYTE_ARRAY) {
                                                            if (idx < dict_strings.items.len) std.debug.print("  {s}\n", .{dict_strings.items[idx]});
                                                        } else if (md.type == .INT64) {
                                                            if (idx < dict_int64.items.len) std.debug.print("  {d}\n", .{dict_int64.items[idx]});
                                                        } else if (md.type == .INT32) {
                                                            if (idx < dict_int32.items.len) std.debug.print("  {d}\n", .{dict_int32.items[idx]});
                                                        } else if (md.type == .DOUBLE) {
                                                            if (idx < dict_double.items.len) std.debug.print("  {d}\n", .{dict_double.items[idx]});
                                                        } else {
                                                            std.debug.print("  <val>\n", .{});
                                                        }
                                                        values_printed += 1;
                                                    }
                                                } else {
                                                    std.debug.print("  null\n", .{});
                                                    values_printed += 1;
                                                }
                                            }
                                        } else {
                                             var k: i32 = 0;
                                             while (k < dph.num_values) : (k += 1) {
                                                if (values_printed >= limit) break;
                                                if (try rle_dec.next()) |idx| {
                                                     if (md.type == .BYTE_ARRAY) {
                                                        if (idx < dict_strings.items.len) std.debug.print("  {s}\n", .{dict_strings.items[idx]});
                                                    } else if (md.type == .INT64) {
                                                        if (idx < dict_int64.items.len) std.debug.print("  {d}\n", .{dict_int64.items[idx]});
                                                    } else {
                                                        std.debug.print("  <val>\n", .{});
                                                    }
                                                    values_printed += 1;
                                                }
                                             }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    for (dict_strings.items) |s| allocator.free(s);
                }
            }
        }
    }
}


// The detailed deep-dive inspection (formerly 'inspect')
fn cmdPages(allocator: std.mem.Allocator, path: []const u8) !void {
     var pf = try openFile(allocator, path);
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
                    std.debug.print("      Type: {any}, Codec: {any}\n", .{md.type, md.codec});
                    
                    const levels = meta.getColumnLevels(md.path_in_schema.items);
                    std.debug.print("      Levels: MaxDef={d}, MaxRep={d}\n", .{levels.max_def, levels.max_rep});

                    var reader = try zpq.column.ColumnReader.init(pf.source, allocator, col);
                    var page_idx: usize = 0;
                    while (try reader.next()) |page| {
                        var p = page;
                        defer p.deinit();
                        std.debug.print("      Page {d}: {any} Size={d} (Comp={d})\n", 
                            .{page_idx, p.header.type, p.header.uncompressed_page_size, p.header.compressed_page_size});
                            
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
                                std.debug.print("        Encoding: {any}, Values: {d}\n", .{dph.encoding, dph.num_values});
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

