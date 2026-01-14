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

    // Check for Lambda environment first (before CLI parsing)
    if (std.posix.getenv("AWS_LAMBDA_RUNTIME_API")) |runtime_api| {
        const lambda = @import("lambda.zig");
        try lambda.run(allocator, runtime_api);
        return;
    }

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next(); // skip exe name
    var input_path: ?[]const u8 = null;
    var output_path: ?[]const u8 = null;
    var filter_str: ?[]const u8 = null;
    var select_str: ?[]const u8 = null;
    var log_level: zpq.log.Level = .info;
    var is_benchmark = false;
    var num_threads: usize = 4;
    var repeat_count: usize = 1;
    var command: enum { query, schema, meta, cat, pages } = .query;

    // First pass: extract command and flags
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "schema")) {
            command = .schema;
        } else if (std.mem.eql(u8, arg, "meta")) {
            command = .meta;
        } else if (std.mem.eql(u8, arg, "cat")) {
            command = .cat;
        } else if (std.mem.eql(u8, arg, "pages")) {
            command = .pages;
        } else if (std.mem.eql(u8, arg, "--filter") or std.mem.eql(u8, arg, "-f")) {
            filter_str = args.next();
        } else if (std.mem.eql(u8, arg, "--select") or std.mem.eql(u8, arg, "-s")) {
            select_str = args.next();
        } else if (std.mem.eql(u8, arg, "--benchmark")) {
            is_benchmark = true;
        } else if (std.mem.eql(u8, arg, "--threads") or std.mem.eql(u8, arg, "-t")) {
            if (args.next()) |t_str| {
                num_threads = try std.fmt.parseInt(usize, t_str, 10);
            }
        } else if (std.mem.eql(u8, arg, "--repeat")) {
            if (args.next()) |r_str| {
                repeat_count = try std.fmt.parseInt(usize, r_str, 10);
            }
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
            std.debug.print("Warning: Unknown flag '{s}'\n", .{arg});
        } else {
            if (input_path == null) {
                input_path = arg;
            } else if (output_path == null) {
                output_path = arg;
            }
        }
    }

    if (input_path == null) {
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

    var thread_pool = xev.ThreadPool.init(.{ .max_threads = @intCast(num_threads) });
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
        try runQuery(BenchmarkRow, allocator, &pfile, filter_str, select_str, output_path orelse "/dev/null", &loop, &thread_pool, repeat_count);
    } else {
        switch (command) {
            .query => try runQuery(BenchmarkRow, allocator, &pfile, filter_str, select_str, output_path orelse "/dev/null", &loop, &thread_pool, repeat_count),
            .schema => {
                std.debug.print("Schema for {s}:\n", .{input_path.?});
                for (pfile.metadata.schema.items, 0..) |elem, i| {
                    const type_str = if (elem.type) |t| @tagName(t) else "n/a";
                    const rep_str = if (elem.repetition_type) |rt| @tagName(rt) else "n/a";
                    std.debug.print("  [{d}] {s}: {s} ({s})\n", .{ i, elem.name, type_str, rep_str });
                }
            },
            .meta => {
                std.debug.print("Metadata for {s}:\n", .{input_path.?});
                std.debug.print("  File Size: {d} bytes\n", .{pfile.source.size()});
                std.debug.print("  Rows: {d}\n", .{pfile.metadata.num_rows});
                std.debug.print("  Row Groups: {d}\n", .{pfile.metadata.row_groups.items.len});
            },
            else => {
                std.debug.print("Command {s} not yet fully implemented in refactor.\n", .{@tagName(command)});
            },
        }
    }
}

fn runQuery(comptime T: type, allocator: std.mem.Allocator, pfile: *zpq.core.file.ParquetFile, filter_str: ?[]const u8, select_str: ?[]const u8, output_path: []const u8, loop: *xev.Dynamic.Loop, thread_pool: *xev.ThreadPool, repeat: usize) !void {
    // 1. Setup Execution Plan
    var plan = zpq.core.planner.ExecutionPlan.init(allocator);
    plan.loop = @ptrCast(loop);
    defer plan.deinit();

    // Map columns
    // We need to know indices for generic T
    // This is a bit hacked for T, ideally T shouldn't dictate it if we are generic,
    // but for the benchmark we use strict indices.
    
    // For T=BenchmarkRow, we know the indices match the file schema indices usually.
    // Let's assume schema based scan.
    
    var req_cols_list = std.ArrayListUnmanaged(usize){};
    defer req_cols_list.deinit(allocator);

    if (select_str) |s| {
        var it = std.mem.tokenizeScalar(u8, s, ',');
        while (it.next()) |col_name_raw| {
            const col_name = std.mem.trim(u8, col_name_raw, " ");
            const idx = try findColumnIndex(col_name, pfile);
            try req_cols_list.append(allocator, idx);
        }
    } else {
        // Select all
        // skip 0 (root)
        for (0..pfile.metadata.schema.items.len - 1) |i| {
            if (pfile.metadata.schema.items[i + 1].type != null) {
                try req_cols_list.append(allocator, i);
            }
        }
    }
    
    plan.required_columns = try req_cols_list.toOwnedSlice(allocator);
    plan.output_columns = try allocator.dupe(usize, plan.required_columns);

    // 2. Setup Filter
    if (filter_str) |f| {
        plan.filter = try parseFilter(T, f, pfile);
        
        // Collect all filter columns (handles composite filters recursively)
        var filter_cols_list = std.ArrayListUnmanaged(usize){};
        try collectFilterColumns(plan.filter.?, &filter_cols_list, allocator);
        plan.filter_columns = try filter_cols_list.toOwnedSlice(allocator);
        
        // Ensure filter columns are in required columns?
        // RowGroupPipeline logic: it reads filter_cols separately. 
        // If filter col is ALSO in output, it reads it again? 
        // Current implementation: yes, naive read.

    }

    // 3. Setup Writer
    const output_to_stdout = std.mem.eql(u8, output_path, "--");
    const output_to_null = std.mem.eql(u8, output_path, "/dev/null");

    var writer: ?*zpq.core.writer.ParquetWriter = null;
    if (!output_to_stdout and !output_to_null) {
        const out_sink = try zpq.io.factory.openSink(allocator, output_path, .{
            .loop = loop,
            .thread_pool = thread_pool,
        });
        
        // Construct writing schema from output columns
        var out_schema = std.ArrayListUnmanaged(zpq.core.schema.SchemaElement){};
        defer out_schema.deinit(allocator);
        
        try out_schema.append(allocator, pfile.metadata.schema.items[0]); // Root
        for (plan.output_columns) |idx| {
            // idx is logical column index (0-based)
            // schema.items[0] is Root, so we need idx + 1
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

    // 4. Parallel Execution
    var timer = try std.time.Timer.start();
    
    // Check for fast path optimizations
    if (pfile.metadata.row_groups.items.len > 0) {
        const total_cols = pfile.metadata.row_groups.items[0].columns.items.len;
        plan.detectOptimization(total_cols);
        if (plan.is_zero_copy) {
            std.debug.print("⚡ Zero-Copy Fast Path Detected! (Pass-through)\n", .{});
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
    for (0..repeat) |_| {
        try executor.execute();
    }
    
    // Stats from executor
    const total_active = executor.rows_matched.load(.monotonic);
    const total_scanned = executor.rows_scanned.load(.monotonic);

    const elapsed = timer.read();
    const elapsed_ms = @as(f64, @floatFromInt(elapsed)) / 1_000_000.0;
    std.debug.print("Scanned {d} rows ({d} matched) in {d:.2}ms ({d:.2} Mrows/sec)\n", .{ total_scanned, total_active, elapsed_ms, @as(f64, @floatFromInt(total_scanned)) / (elapsed_ms * 1000.0) });
}

fn collectFilterColumns(filter: zpq.core.filter.Filter, list: *std.ArrayListUnmanaged(usize), allocator: std.mem.Allocator) !void {
    switch (filter) {
        .int32 => |f| try list.append(allocator, f.col_idx),
        .int64 => |f| try list.append(allocator, f.col_idx),
        .float => |f| try list.append(allocator, f.col_idx),
        .double => |f| try list.append(allocator, f.col_idx),
        .string => |f| try list.append(allocator, f.col_idx),
        .boolean => |f| try list.append(allocator, f.col_idx),
        .and_filter => |f| {
            try collectFilterColumns(f.left.*, list, allocator);
            try collectFilterColumns(f.right.*, list, allocator);
        },
        .or_filter => |f| {
            try collectFilterColumns(f.left.*, list, allocator);
            try collectFilterColumns(f.right.*, list, allocator);
        },
    }
}

fn parseFilter(comptime T: type, filter_str: []const u8, pfile: *zpq.core.file.ParquetFile) !zpq.core.filter.Filter {
    return parseFilterWithAllocator(T, filter_str, pfile, pfile.allocator);
}


fn parseFilterWithAllocator(comptime T: type, filter_str: []const u8, pfile: *zpq.core.file.ParquetFile, allocator: std.mem.Allocator) !zpq.core.filter.Filter {
    // Check for OR first (lower precedence)
    if (std.mem.indexOf(u8, filter_str, " OR ")) |or_idx| {
        const left_str = std.mem.trim(u8, filter_str[0..or_idx], " ");
        const right_str = std.mem.trim(u8, filter_str[or_idx + 4 ..], " ");
        
        const left = try allocator.create(zpq.core.filter.Filter);
        const right = try allocator.create(zpq.core.filter.Filter);
        left.* = try parseFilterWithAllocator(T, left_str, pfile, allocator);
        right.* = try parseFilterWithAllocator(T, right_str, pfile, allocator);
        
        return .{ .or_filter = .{ .left = left, .right = right } };
    }
    
    // Check for AND (higher precedence)
    if (std.mem.indexOf(u8, filter_str, " AND ")) |and_idx| {
        const left_str = std.mem.trim(u8, filter_str[0..and_idx], " ");
        const right_str = std.mem.trim(u8, filter_str[and_idx + 5 ..], " ");
        
        const left = try allocator.create(zpq.core.filter.Filter);
        const right = try allocator.create(zpq.core.filter.Filter);
        left.* = try parseFilterWithAllocator(T, left_str, pfile, allocator);
        right.* = try parseFilterWithAllocator(T, right_str, pfile, allocator);
        
        return .{ .and_filter = .{ .left = left, .right = right } };
    }
    
    // Leaf filter: "colOPval" where OP is =, !=, <, >, <=, >=
    const operators = [_][]const u8{ "!=", "<=", ">=", "=", "<", ">" };
    var op_str: []const u8 = "";
    var op_type: zpq.core.filter.Operator = .Eq;
    var op_idx: usize = 0;

    for (operators) |op| {
        if (std.mem.indexOf(u8, filter_str, op)) |idx| {
            op_idx = idx;
            op_str = op;
            op_type = switch (op[0]) {
                '=' => .Eq,
                '!' => .NotEq,
                '<' => if (op.len > 1) .LtEq else .Lt,
                '>' => if (op.len > 1) .GtEq else .Gt,
                else => unreachable,
            };
            break;
        }
    }

    if (op_str.len == 0) return error.InvalidFilter;

    const col_name = std.mem.trim(u8, filter_str[0..op_idx], " ");
    const val_str = std.mem.trim(u8, filter_str[op_idx + op_str.len ..], " ");

    const col_idx = try findColumnIndex(col_name, pfile);
    const field_type = try getFieldType(T, col_name);

    return switch (field_type) {
        .ByteArray => .{ .string = .{ .col_idx = col_idx, .op = op_type, .value = val_str } },
        .Int32 => .{ .int32 = .{ .col_idx = col_idx, .op = op_type, .value = try std.fmt.parseInt(i32, val_str, 10) } },
        .Int64 => .{ .int64 = .{ .col_idx = col_idx, .op = op_type, .value = try std.fmt.parseInt(i64, val_str, 10) } },
        .Float => .{ .float = .{ .col_idx = col_idx, .op = op_type, .value = try std.fmt.parseFloat(f32, val_str) } },
        .Double => .{ .double = .{ .col_idx = col_idx, .op = op_type, .value = try std.fmt.parseFloat(f64, val_str) } },
        .Bool => blk: {
            const bool_val = std.mem.eql(u8, val_str, "true") or std.mem.eql(u8, val_str, "1");
            break :blk .{ .boolean = .{ .col_idx = col_idx, .op = op_type, .value = bool_val } };
        },
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
