const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");
const posix = std.posix;

/// Lambda event payload.
const Payload = struct {
    file: []const u8,
    output: ?[]const u8 = null,
    filter: ?[]const u8 = null,
    select: ?[]const u8 = null,
    threads: ?usize = null,
    log_level: ?[]const u8 = null, // "debug", "info", "warn", "error"
};

/// Lambda Runtime API handler.
/// Uses raw POSIX sockets for control plane, libxev for data plane.
pub fn run(allocator: std.mem.Allocator, runtime_api: []const u8) !void {
    // Parse API endpoint
    const colon = std.mem.indexOfScalar(u8, runtime_api, ':') orelse runtime_api.len;
    const host = runtime_api[0..colon];
    const port = if (colon < runtime_api.len)
        std.fmt.parseInt(u16, runtime_api[colon + 1 ..], 10) catch 80
    else
        80;

    // Setup data plane (xev + thread pool) - reused across invocations
    if (@hasDecl(xev.Dynamic, "detect")) try xev.Dynamic.detect();
    var loop = try xev.Dynamic.Loop.init(.{});
    defer loop.deinit();

    var pool = xev.ThreadPool.init(.{ .max_threads = 4 });
    defer {
        pool.shutdown();
        pool.deinit();
    }

    // Event loop
    while (true) {
        const event = getInvocation(allocator, host, port) catch continue;
        defer allocator.free(event.body);
        defer allocator.free(event.request_id);

        const result = processEvent(allocator, &loop, &pool, event.body);
        const response = result catch |err| {
            const msg = std.fmt.allocPrint(allocator, "{{\"error\":\"{s}\"}}", .{@errorName(err)}) catch continue;
            postResult(allocator, host, port, event.request_id, msg, true) catch {};
            allocator.free(msg);
            continue;
        };
        defer allocator.free(response);

        postResult(allocator, host, port, event.request_id, response, false) catch {};
    }
}

fn parseLogLevel(level_str: ?[]const u8) zpq.log.Level {
    const s = level_str orelse return .info;
    if (std.ascii.eqlIgnoreCase(s, "debug")) return .debug;
    if (std.ascii.eqlIgnoreCase(s, "warn")) return .warn;
    if (std.ascii.eqlIgnoreCase(s, "error")) return .err;
    return .info;
}

fn processEvent(allocator: std.mem.Allocator, loop: *xev.Dynamic.Loop, pool: *xev.ThreadPool, body: []const u8) ![]u8 {
    const parsed = try std.json.parseFromSlice(Payload, allocator, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const p = parsed.value;

    // Setup logger based on payload
    const log_level = parseLogLevel(p.log_level);
    const logger = try zpq.log.AsyncLogger.init(allocator, log_level);
    defer logger.deinit();
    try logger.start(loop);

    std.debug.print("[lambda] Starting query: file={s} log_level={s}\n", .{ p.file, p.log_level orelse "info" });

    // Open source
    const source = try zpq.io.factory.openSource(allocator, p.file, .{ .loop = loop, .thread_pool = pool });
    defer source.close();

    var pfile = zpq.core.file.ParquetFile.init(allocator, source);
    try pfile.readFooter();
    defer pfile.deinit();

    // Build execution plan
    var plan = zpq.core.planner.ExecutionPlan.init(allocator);
    plan.loop = @ptrCast(loop);
    defer plan.deinit();

    // Select all columns
    var cols = std.ArrayListUnmanaged(usize){};
    defer cols.deinit(allocator);
    for (0..pfile.metadata.schema.items.len - 1) |i| {
        if (pfile.metadata.schema.items[i + 1].type != null)
            try cols.append(allocator, i);
    }
    plan.required_columns = try cols.toOwnedSlice(allocator);
    plan.output_columns = try allocator.dupe(usize, plan.required_columns);

    // Execute
    var executor = zpq.core.executor.Executor.init(allocator, &plan, &pfile.metadata, source, pool, null);
    try executor.execute();

    return std.fmt.allocPrint(allocator, "{{\"rows\":{d}}}", .{executor.rows_scanned.load(.acquire)});
}

// --- Socket Helpers ---

const Event = struct { body: []u8, request_id: []u8 };

fn getInvocation(allocator: std.mem.Allocator, host: []const u8, port: u16) !Event {
    const sock = try connect(host, port);
    defer posix.close(sock);

    _ = try posix.write(sock, "GET /2018-06-01/runtime/invocation/next HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");

    var buf: [65536]u8 = undefined;
    var len: usize = 0;
    while (len < buf.len) {
        const n = posix.read(sock, buf[len..]) catch break;
        if (n == 0) break;
        len += n;
        if (isComplete(buf[0..len])) break;
    }

    const sep = std.mem.indexOf(u8, buf[0..len], "\r\n\r\n") orelse return error.Malformed;
    const headers = buf[0..sep];
    const body = buf[sep + 4 .. len];

    var request_id: []const u8 = "unknown";
    var lines = std.mem.splitSequence(u8, headers, "\r\n");
    while (lines.next()) |line| {
        if (std.ascii.startsWithIgnoreCase(line, "lambda-runtime-aws-request-id:")) {
            request_id = std.mem.trim(u8, line["lambda-runtime-aws-request-id:".len..], " \t");
            break;
        }
    }

    return .{
        .body = try allocator.dupe(u8, body),
        .request_id = try allocator.dupe(u8, request_id),
    };
}

fn postResult(allocator: std.mem.Allocator, host: []const u8, port: u16, id: []const u8, body: []const u8, is_error: bool) !void {
    const sock = try connect(host, port);
    defer posix.close(sock);

    const suffix = if (is_error) "/error" else "/response";
    const req = try std.fmt.allocPrint(allocator,
        "POST /2018-06-01/runtime/invocation/{s}{s} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}",
        .{ id, suffix, body.len, body },
    );
    defer allocator.free(req);
    _ = try posix.write(sock, req);

    var buf: [256]u8 = undefined;
    _ = posix.read(sock, &buf) catch {};
}

fn connect(host: []const u8, port: u16) !posix.socket_t {
    const sock = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    errdefer posix.close(sock);

    var parts: [4]u8 = undefined;
    var i: usize = 0;
    var iter = std.mem.splitScalar(u8, host, '.');
    while (iter.next()) |p| : (i += 1) {
        if (i >= 4) return error.InvalidIP;
        parts[i] = std.fmt.parseInt(u8, p, 10) catch return error.InvalidIP;
    }
    if (i != 4) return error.InvalidIP;

    var addr = std.mem.zeroes(posix.sockaddr.in);
    addr.family = posix.AF.INET;
    addr.port = std.mem.nativeToBig(u16, port);
    addr.addr = std.mem.nativeToBig(u32, @as(u32, parts[0]) << 24 | @as(u32, parts[1]) << 16 | @as(u32, parts[2]) << 8 | parts[3]);

    try posix.connect(sock, @ptrCast(&addr), @sizeOf(posix.sockaddr.in));
    return sock;
}

fn isComplete(data: []const u8) bool {
    const sep = std.mem.indexOf(u8, data, "\r\n\r\n") orelse return false;
    var lines = std.mem.splitSequence(u8, data[0..sep], "\r\n");
    while (lines.next()) |line| {
        if (std.ascii.startsWithIgnoreCase(line, "content-length:")) {
            const cl = std.fmt.parseInt(usize, std.mem.trim(u8, line["content-length:".len..], " \t"), 10) catch return true;
            return data.len >= sep + 4 + cl;
        }
    }
    return true;
}
