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


    if (is_benchmark) {
        const stats = try zpq.core.engine.runQuery(
            BenchmarkRow,
            allocator,
            input_path.?,
            output_path orelse "/dev/null",
            filter_str,
            select_str,
            &loop,
            &thread_pool,
            repeat_count,
        );
        std.debug.print("Scanned {d} rows ({d} matched) in {d:.2}ms\n", .{ stats.scanned, stats.matched, stats.elapsed_ms });
    } else {
        switch (command) {
            .query => {
                const stats = try zpq.core.engine.runQuery(
                    BenchmarkRow,
                    allocator,
                    input_path.?,
                    output_path orelse "/dev/null",
                    filter_str,
                    select_str,
                    &loop,
                    &thread_pool,
                    repeat_count,
                );
                std.debug.print("Scanned {d} rows ({d} matched) in {d:.2}ms\n", .{ stats.scanned, stats.matched, stats.elapsed_ms });
            },
            .schema => {
                try zpq.core.engine.printSchema(allocator, input_path.?, &loop, &thread_pool);
            },
            .meta => {
                try zpq.core.engine.printMetadata(allocator, input_path.?, &loop, &thread_pool);
            },
            else => {
                std.debug.print("Command {s} not yet fully implemented in refactor.\n", .{@tagName(command)});
            },
        }
    }
}


fn printUsage() void {
    std.debug.print("Usage: zpq <input> <output> [--filter \"col=val\"] [--select \"col1,col2\"]\n", .{});
}
