const std = @import("std");
const zpq = @import("zpq");
const xev = @import("xev");

var global_async_logger: ?*zpq.log.AsyncLogger = null;

pub const std_options: std.Options = .{
    .log_level = .debug,
    .logFn = logFn,
};

fn logFn(
    comptime level: std.log.Level,
    comptime scope: @TypeOf(.EnumLiteral),
    comptime format: []const u8,
    args: anytype,
) void {
    _ = scope;
    if (global_async_logger) |logger| {
        const zpq_level: zpq.log.Level = switch (level) {
            .err => .err,
            .warn => .warn,
            .info => .info,
            .debug => .debug,
        };
        logger.log(zpq_level, format, args);
    } else {
        // Fallback for logs before logger is initialized
        const prefix = "[" ++ @tagName(level) ++ "] ";
        std.debug.print(prefix ++ format ++ "\n", args);
    }
}

pub const BenchmarkRow = struct {
    int8: i32,
    int16: i32,
    int32_sorted: i32,
    int32_random: i32,
    int64_sorted: i64,
    int64_random: i64,
    uint8: i32,
    uint16: i32,
    uint32: i32,
    uint64: i64,
    float32: f32,
    float64: f64,
    float64_sorted: f64,
    bool: bool,
    bool_sparse: bool,
    string_random: []const u8,
    string_dict_low: []const u8,
    string_dict_high: []const u8,
    string_sorted: []const u8,
    binary: []const u8,
    timestamp: i64,
    timestamp_sorted: i64,
    date: i32,
    // Note: Currently skip nullable for absolute hot path simplicity if needed,
    // but the engine supports them.
    // int32_nullable: ?i32,
    // float64_nullable: ?f64,
    // string_nullable: ?[]const u8,
    // int32_sparse: ?i32,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next(); // skip exe name
    var input_path: ?[]const u8 = null;
    var output_path: ?[]const u8 = null;
    var filter_str: ?[]const u8 = null;
    var select_str: ?[]const u8 = null;
    var log_level: zpq.log.Level = .info;
    var is_benchmark = false;

    // First pass: extract all flags and positional arguments
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--filter") or std.mem.eql(u8, arg, "-f")) {
            filter_str = args.next();
        } else if (std.mem.eql(u8, arg, "--select") or std.mem.eql(u8, arg, "-s")) {
            select_str = args.next();
        } else if (std.mem.eql(u8, arg, "--benchmark")) {
            is_benchmark = true;
        } else if (std.mem.eql(u8, arg, "--log-level")) {
            const level_str = args.next() orelse "info";
            if (std.mem.eql(u8, level_str, "trace")) {
                log_level = .trace;
            } else if (std.mem.eql(u8, level_str, "debug")) {
                log_level = .debug;
            } else if (std.mem.eql(u8, level_str, "info")) {
                log_level = .info;
            } else if (std.mem.eql(u8, level_str, "warn")) {
                log_level = .warn;
            } else if (std.mem.eql(u8, level_str, "err")) {
                log_level = .err;
            }
        } else if (std.mem.eql(u8, arg, "--cid")) {
            if (args.next()) |cid_str| {
                zpq.log.correlation_id = std.fmt.parseInt(u64, cid_str, 0) catch 0;
            }
        } else if (std.mem.startsWith(u8, arg, "-")) {
            // Unknown flag, ignore or warn?
            std.debug.print("Warning: Unknown flag '{s}'\n", .{arg});
        } else {
            // Positional argument
            if (input_path == null) {
                input_path = arg;
            } else if (output_path == null) {
                output_path = arg;
            }
        }
    }

    if (input_path == null or output_path == null) {
        printUsage();
        return;
    }

    if (!is_benchmark and std.mem.indexOf(u8, input_path.?, "benchmark") != null) {
        is_benchmark = true;
    }

    // Initialize xev loop and thread pool
    if (@hasDecl(xev.Dynamic, "detect")) {
        try xev.Dynamic.detect();
    }
    var loop = try xev.Dynamic.Loop.init(.{});
    defer loop.deinit();

    // Initialize Logger
    const logger = try zpq.log.AsyncLogger.init(allocator, log_level);
    defer logger.deinit();
    global_async_logger = logger;
    defer global_async_logger = null;
    try logger.start(&loop);

    var thread_pool = xev.ThreadPool.init(.{ .max_threads = 4 });
    defer {
        thread_pool.shutdown();
        thread_pool.deinit();
    }

    // Open Source
    const source = try zpq.io.factory.openSource(allocator, input_path.?, .{
        .loop = &loop,
        .thread_pool = &thread_pool,
    });
    defer source.close();

    var pfile = zpq.core.file.ParquetFile.init(allocator, source);
    try pfile.readFooter();
    defer pfile.deinit();

    if (is_benchmark) {
        try runQuery(BenchmarkRow, allocator, &pfile, filter_str, select_str, output_path.?, &loop, &thread_pool);
    } else {
        std.debug.print("Full CLI mode (GenericRow) not yet implemented. Use --benchmark for performance testing on known schema.\n", .{});
        return error.UnsupportedMode;
    }
}

fn runQuery(comptime T: type, allocator: std.mem.Allocator, pfile: *zpq.core.file.ParquetFile, filter_str: ?[]const u8, select_str: ?[]const u8, output_path: []const u8, loop: *xev.Dynamic.Loop, thread_pool: *xev.ThreadPool) !void {
    var reader = try zpq.core.reader.ParquetReader(T).init(allocator, pfile);
    defer reader.deinit();

    if (select_str) |s| {
        var mask = [_]bool{false} ** @typeInfo(T).@"struct".fields.len;
        var it = std.mem.tokenizeScalar(u8, s, ',');
        while (it.next()) |col_name_raw| {
            const col_name = std.mem.trim(u8, col_name_raw, " ");
            inline for (@typeInfo(T).@"struct".fields, 0..) |field, i| {
                if (std.mem.eql(u8, field.name, col_name)) {
                    mask[i] = true;
                }
            }
        }
        reader.setProjection(mask);
    }

    var filters = std.ArrayListUnmanaged(zpq.core.filter.Filter){};
    defer filters.deinit(allocator);

    if (filter_str) |f| {
        const parsed = try parseFilter(T, f, pfile);
        try filters.append(allocator, parsed);
    }

    const batch_size = 8192;
    const batch = try allocator.alloc(T, batch_size);
    defer allocator.free(batch);
    @memset(std.mem.sliceAsBytes(batch), 0);

    var total_active: usize = 0;
    var total_scanned: usize = 0;
    const total_rows_in_file: usize = @intCast(pfile.metadata.num_rows);
    var timer = try std.time.Timer.start();

    const output_to_stdout = std.mem.eql(u8, output_path, "--");
    const output_to_null = std.mem.eql(u8, output_path, "/dev/null");

    var writer: ?*zpq.core.writer.ParquetWriter = null;
    if (!output_to_stdout and !output_to_null) {
        const out_sink = try zpq.io.factory.openSink(allocator, output_path, .{
            .loop = loop,
            .thread_pool = thread_pool,
        });
        writer = try zpq.core.writer.ParquetWriter.init(allocator, out_sink, pfile.metadata.schema.items);
    }
    defer {
        if (writer) |w| {
            w.close() catch {};
            w.deinit();
        }
    }

    while (total_scanned < total_rows_in_file) {
        const to_scan = @min(batch.len, total_rows_in_file - total_scanned);
        const n = try reader.nextBatch(batch[0..to_scan], filters.items);

        if (writer) |w| {
            for (batch[0..n]) |row| {
                try w.appendRow(row);
            }
        }

        total_active += n;
        total_scanned += to_scan;

        if (output_to_stdout and total_active < 100) {
            // Just print a few rows to verify
            // std.debug.print("Row {d}: {any}\n", .{ total_active, batch[0] });
        }
    }

    if (writer) |w| {
        try w.flushRowGroup();
    }

    const elapsed = timer.read();
    const elapsed_ms = @as(f64, @floatFromInt(elapsed)) / 1_000_000.0;
    std.debug.print("Scanned {d} rows ({d} matched) in {d:.2}ms ({d:.2} Mrows/sec)\n", .{ total_scanned, total_active, elapsed_ms, @as(f64, @floatFromInt(total_scanned)) / (elapsed_ms * 1000.0) });
}

fn parseFilter(comptime T: type, filter_str: []const u8, pfile: *zpq.core.file.ParquetFile) !zpq.core.filter.Filter {
    // Simple parser for "col=val"
    const eq_idx = std.mem.indexOfScalar(u8, filter_str, '=') orelse return error.InvalidFilter;
    const col_name = filter_str[0..eq_idx];
    const val_str = filter_str[eq_idx + 1 ..];

    const col_idx = try findColumnIndex(col_name, pfile);
    const field_type = try getFieldType(T, col_name);

    return switch (field_type) {
        .ByteArray => return error.InvalidFilter, // Not supported yet
        .Int32 => .{ .int32 = .{ .col_idx = col_idx, .op = .Eq, .value = try std.fmt.parseInt(i32, val_str, 10) } },
        .Int64 => .{ .int64 = .{ .col_idx = col_idx, .op = .Eq, .value = try std.fmt.parseInt(i64, val_str, 10) } },
        .Float => .{ .float = .{ .col_idx = col_idx, .op = .Eq, .value = try std.fmt.parseFloat(f32, val_str) } },
        .Double => .{ .double = .{ .col_idx = col_idx, .op = .Eq, .value = try std.fmt.parseFloat(f64, val_str) } },
        // .Bool => .{ .bool = ... } // Filter doesn't have bool yet? 
        .Bool => return error.InvalidFilter,
    };
}

fn findColumnIndex(name: []const u8, pfile: *zpq.core.file.ParquetFile) !usize {
    // Parquet schema [0] is root
    for (pfile.metadata.schema.items, 0..) |elem, i| {
        if (i == 0) continue;
        if (std.mem.eql(u8, elem.name, name)) return i - 1;
    }
    return error.ColumnNotFound;
}

const FieldType = enum { Int32, Int64, Float, Double, Bool, ByteArray };

fn getFieldType(comptime T: type, name: []const u8) !FieldType {
    const fields = @typeInfo(T).@"struct".fields;
    inline for (fields) |field| {
        if (std.mem.eql(u8, field.name, name)) {
            if (field.type == i32) return .Int32;
            if (field.type == i64) return .Int64;
            if (field.type == f32) return .Float;
            if (field.type == f64) return .Double;
            if (field.type == bool) return .Bool;
            if (field.type == []const u8) return .ByteArray;
        }
    }
    return error.FieldNotFound;
}

fn printUsage() void {
    std.debug.print("Usage: zpq <input> <output> [--filter \"col=val\"] [--select \"col1,col2\"]\n", .{});
}
