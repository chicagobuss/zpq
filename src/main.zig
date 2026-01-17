const std = @import("std");
const zpq = @import("zpq");
const xev = @import("xev");
const posix = std.posix;

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
        try runLambda(allocator, runtime_api);
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


    switch (command) {
        .schema => {
            try zpq.core.engine.printSchema(allocator, input_path.?, &loop, &thread_pool);
        },
        .meta => {
            try zpq.core.engine.printMetadata(allocator, input_path.?, &loop, &thread_pool);
        },
        .query => {
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
            }
        },
        else => {
            std.debug.print("Command {s} not yet fully implemented in refactor.\n", .{@tagName(command)});
        },
    }
}


fn printUsage() void {
    std.debug.print("Usage: zpq <input> <output> [--filter \"col=val\"] [--select \"col1,col2\"]\n", .{});
}

// =============================================================================
// Lambda Runtime (Self-Contained)
// =============================================================================

fn runLambda(allocator: std.mem.Allocator, runtime_api: []const u8) !void {
    const colon = std.mem.indexOfScalar(u8, runtime_api, ':') orelse runtime_api.len;
    const host = runtime_api[0..colon];
    const port = if (colon < runtime_api.len)
        std.fmt.parseInt(u16, runtime_api[colon + 1 ..], 10) catch 80
    else
        80;

    // Force Epoll for Lambda
    if (@hasDecl(xev.Dynamic, "prefer")) _ = xev.Dynamic.prefer(.epoll);
    if (@hasDecl(xev.Dynamic, "detect")) try xev.Dynamic.detect();

    var loop = try xev.Dynamic.Loop.init(.{});
    defer loop.deinit();

    // Initialize Global Async Logger
    const logger = try zpq.log.AsyncLogger.init(allocator, .info);
    defer logger.deinit();
    global_async_logger = logger;
    defer global_async_logger = null;
    try logger.start(&loop);

    var pool = xev.ThreadPool.init(.{ .max_threads = 4 });
    defer {
        pool.shutdown();
        pool.deinit();
    }

    std.log.info("ZPQ Lambda Runtime started (epoll)", .{});

    while (true) {
        const event = getLambdaInvocation(allocator, host, port) catch continue;
        defer allocator.free(event.body);
        defer allocator.free(event.request_id);

        const response = processLambdaEvent(allocator, &loop, &pool, event.body) catch |err| {
            const msg = std.fmt.allocPrint(allocator, "{{\"error\":\"{s}\"}}", .{@errorName(err)}) catch continue;
            defer allocator.free(msg);
            postLambdaResult(allocator, host, port, event.request_id, msg, true) catch {};
            continue;
        };
        defer allocator.free(response);

        postLambdaResult(allocator, host, port, event.request_id, response, false) catch {};
    }
}

const LambdaPayload = struct {
    file: []const u8,
    output: ?[]const u8 = null,
    filter: ?[]const u8 = null,
    select: ?[]const u8 = null,
    threads: ?usize = null,
    log_level: ?[]const u8 = null,
};

fn processLambdaEvent(allocator: std.mem.Allocator, loop: *xev.Dynamic.Loop, pool: *xev.ThreadPool, body: []const u8) ![]u8 {
    const parsed = try std.json.parseFromSlice(LambdaPayload, allocator, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const p = parsed.value;

    if (global_async_logger) |logger| {
        if (p.log_level) |lvl| {
            if (std.ascii.eqlIgnoreCase(lvl, "debug")) logger.active_level.store(.debug, .release);
            if (std.ascii.eqlIgnoreCase(lvl, "info")) logger.active_level.store(.info, .release);
        }
    }

    std.log.info("Query: {s} -> {s} (filter: {s})", .{ p.file, p.output orelse "/dev/null", p.filter orelse "none" });

    const stats = try zpq.core.engine.runQuery(
        BenchmarkRow,
        allocator,
        p.file,
        p.output,
        p.filter,
        p.select,
        loop,
        pool,
        1
    );

    return std.fmt.allocPrint(allocator, "{{\"rows\":{d}, \"matched\":{d}, \"ms\":{d:.2}}}", .{ stats.scanned, stats.matched, stats.elapsed_ms });
}

fn getLambdaInvocation(allocator: std.mem.Allocator, host: []const u8, port: u16) !struct { body: []u8, request_id: []u8 } {
    const sock = try connectLambda(host, port);
    defer posix.close(sock);

    _ = try posix.write(sock, "GET /2018-06-01/runtime/invocation/next HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");

    var buf: [65536]u8 = undefined;
    var total: usize = 0;
    while (total < buf.len) {
        const n = posix.read(sock, buf[total..]) catch break;
        if (n == 0) break;
        total += n;
        if (std.mem.indexOf(u8, buf[0..total], "\r\n\r\n") != null) break;
    }

    const sep = std.mem.indexOf(u8, buf[0..total], "\r\n\r\n") orelse return error.Malformed;
    const headers = buf[0..sep];
    const body = buf[sep + 4 .. total];

    var req_id: []const u8 = "unknown";
    var lines = std.mem.splitSequence(u8, headers, "\r\n");
    while (lines.next()) |line| {
        if (std.ascii.startsWithIgnoreCase(line, "lambda-runtime-aws-request-id:")) {
            req_id = std.mem.trim(u8, line["lambda-runtime-aws-request-id:".len..], " \t");
            break;
        }
    }

    return .{
        .body = try allocator.dupe(u8, body),
        .request_id = try allocator.dupe(u8, req_id),
    };
}

fn postLambdaResult(allocator: std.mem.Allocator, host: []const u8, port: u16, id: []const u8, body: []const u8, is_error: bool) !void {
    const sock = try connectLambda(host, port);
    defer posix.close(sock);

    const suffix = if (is_error) "/error" else "/response";
    const req = try std.fmt.allocPrint(allocator,
        "POST /2018-06-01/runtime/invocation/{s}{s} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}",
        .{ id, suffix, body.len, body },
    );
    defer allocator.free(req);
    _ = try posix.write(sock, req);
    var buf: [1024]u8 = undefined;
    _ = posix.read(sock, &buf) catch {};
}

fn connectLambda(host: []const u8, port: u16) !posix.socket_t {
    const sock = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    errdefer posix.close(sock);

    var parts: [4]u8 = undefined;
    var i: usize = 0;
    var iter = std.mem.splitScalar(u8, host, '.');
    while (iter.next()) |p| : (i += 1) {
        if (i >= 4) return error.InvalidIP;
        parts[i] = std.fmt.parseInt(u8, p, 10) catch return error.InvalidIP;
    }

    var addr = std.mem.zeroes(posix.sockaddr.in);
    addr.family = posix.AF.INET;
    addr.port = std.mem.nativeToBig(u16, port);
    addr.addr = std.mem.nativeToBig(u32, @as(u32, parts[0]) << 24 | @as(u32, parts[1]) << 16 | @as(u32, parts[2]) << 8 | parts[3]);

    try posix.connect(sock, @ptrCast(&addr), @sizeOf(posix.sockaddr.in));
    return sock;
}
