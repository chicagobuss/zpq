//! libzpq: zpq's JSON request handler (the Lambda's request and response schema) behind a C ABI. See include/zpq.h.
//!
//! A context holds what outlives one request: the allocator, the `std.Io` executor, and the S3 connection pool and
//! footer cache. Each `zpq_run` reads the process environment at call time, as the CLI does at start, for S3
//! credentials, endpoint and region. Process-wide state outside any context is limited to caches that are set once and
//! then only read: the TLS client contexts (CA stores), and probes of the kernel's io_uring and of the gzip backend.

const std = @import("std");
const zpq = @import("zpq");
const service = @import("service");

/// malloc and free, so a host's leak checker (ASan, valgrind) sees every allocation.
const gpa = std.heap.c_allocator;

pub const Ctx = struct {
    threaded: std.Io.Threaded,
    pool: service.PersistentPool,
};

pub const ZPQ_OK: c_int = 0;
pub const ZPQ_ERROR_RESPONSE: c_int = 1;
pub const ZPQ_EINVAL: c_int = -1;
pub const ZPQ_ENOMEM: c_int = -2;

/// The options `zpq_open` takes. Unknown fields are an error: a misspelt option should not be silently ignored.
const Options = struct {
    /// Each query's memory budget when its request names none: bytes, or a size string ("2GB"). Absent, it derives
    /// from the memory available, as the Lambda's does.
    max_memory: ?std.json.Value = null,
    /// Open connections to the S3 endpoint as the context opens (`ZPQ_S3_PRECONNECT` of them, default 4), as the
    /// Lambda does during init, so the first queries find them ready. Off by default.
    preconnect: bool = false,
};

/// `Options` as a context uses them.
const Parsed = struct {
    query_memory: ?usize = null,
    preconnect: bool = false,
};

fn parseOptions(text: []const u8) !Parsed {
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    if (trimmed.len == 0) return .{};
    const parsed = try std.json.parseFromSlice(Options, gpa, trimmed, .{});
    defer parsed.deinit();
    const query_memory: ?usize = if (parsed.value.max_memory) |v| switch (v) {
        .null => null,
        .integer => |n| if (n > 0) @intCast(n) else return error.BadMaxMemory,
        .string => |str| try zpq.core.system.parseSizeString(str),
        else => return error.BadMaxMemory,
    } else null;
    return .{ .query_memory = query_memory, .preconnect = parsed.value.preconnect };
}

export fn zpq_open(options_json: ?[*:0]const u8) ?*Ctx {
    const opts = parseOptions(if (options_json) |o| std.mem.span(o) else "") catch return null;
    const ctx = gpa.create(Ctx) catch return null;
    ctx.* = .{
        .threaded = std.Io.Threaded.init(gpa, .{}),
        .pool = .sized(zpq.core.system.discoverAvailableMemory(processEnviron())),
    };
    ctx.pool.per_call_stats = false;
    ctx.pool.query_memory = opts.query_memory;
    // Ready before any call: every query, the first ones included, gets the footer cache.
    ctx.pool.warm(gpa) catch {
        ctx.threaded.deinit();
        gpa.destroy(ctx);
        return null;
    };
    // Only when asked: a host that never touches S3 should not open connections to it. zpq_close joins them.
    if (opts.preconnect) service.preconnectS3(ctx.threaded.io(), gpa, processEnviron(), &ctx.pool);
    return ctx;
}

export fn zpq_run(
    ctx: ?*Ctx,
    request_json: ?[*]const u8,
    len: usize,
    out_json: ?*?[*:0]u8,
    out_len: ?*usize,
) c_int {
    // Every output the caller gave is cleared first, so no return leaves a stale one behind.
    if (out_json) |o| o.* = null;
    if (out_len) |l| l.* = 0;
    const c = ctx orelse return ZPQ_EINVAL;
    const out = out_json orelse return ZPQ_EINVAL;
    const body: []const u8 = if (len == 0) "" else (request_json orelse return ZPQ_EINVAL)[0..len];

    const response = service.handle(c.threaded.io(), gpa, processEnviron(), body, &c.pool, null) catch
        return ZPQ_ENOMEM;
    defer gpa.free(response);
    const buf: [*]u8 = @ptrCast(std.c.malloc(response.len + 1) orelse return ZPQ_ENOMEM);
    @memcpy(buf[0..response.len], response);
    buf[response.len] = 0;
    out.* = buf[0..response.len :0].ptr;
    if (out_len) |l| l.* = response.len;
    return if (std.mem.startsWith(u8, response, "{\"error\"")) ZPQ_ERROR_RESPONSE else ZPQ_OK;
}

export fn zpq_free(json: ?[*:0]u8) void {
    std.c.free(json);
}

export fn zpq_close(ctx: ?*Ctx) void {
    const c = ctx orelse return;
    c.pool.deinit();
    c.threaded.deinit();
    gpa.destroy(c);
}

/// libzpq's ABI version: bumped when a signature or the meaning of a return code changes.
export fn zpq_abi_version() c_int {
    return 1;
}

/// The process environment as it is now (libc's `environ`).
fn processEnviron() std.process.Environ {
    const live: [*:null]const ?[*:0]const u8 = @ptrCast(std.c.environ);
    return .{ .block = .{ .slice = std.mem.span(live) } };
}

test "options: a byte count, a size string, nothing, or an error" {
    try std.testing.expectEqual(Parsed{}, try parseOptions(""));
    try std.testing.expectEqual(Parsed{}, try parseOptions("{}"));
    try std.testing.expectEqual(@as(?usize, 1 << 20), (try parseOptions("{\"max_memory\": 1048576}")).query_memory);
    try std.testing.expect((try parseOptions("{\"max_memory\": \"2GB\"}")).query_memory.? > 1 << 30);
    try std.testing.expectError(error.UnknownField, parseOptions("{\"max_mem\": 1}"));
    try std.testing.expectError(error.BadMaxMemory, parseOptions("{\"max_memory\": -1}"));
}

test "options: pre-connect is off unless asked, and only a boolean" {
    try std.testing.expect(!(try parseOptions("{\"max_memory\": 1048576}")).preconnect);
    try std.testing.expect((try parseOptions("{\"preconnect\": true}")).preconnect);
    try std.testing.expect(!(try parseOptions("{\"preconnect\": false}")).preconnect);
    try std.testing.expectError(error.UnexpectedToken, parseOptions("{\"preconnect\": 4}"));
}

extern "c" fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
extern "c" fn unsetenv(name: [*:0]const u8) c_int;

test "a context opens connections to the S3 endpoint only when asked to pre-connect" {
    const posix = std.posix;
    // A listener for the S3 endpoint: the kernel completes connections to it without an accept.
    const rc = posix.system.socket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
    if (posix.errno(rc) != .SUCCESS) return error.SkipZigTest;
    const lfd: posix.fd_t = @intCast(rc);
    defer _ = posix.system.close(lfd);
    var addr: posix.sockaddr.in = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f000001) };
    var len: posix.socklen_t = @sizeOf(posix.sockaddr.in);
    if (posix.errno(posix.system.bind(lfd, @ptrCast(&addr), len)) != .SUCCESS) return error.SkipZigTest;
    if (posix.errno(posix.system.listen(lfd, 16)) != .SUCCESS) return error.SkipZigTest;
    if (posix.errno(posix.system.getsockname(lfd, @ptrCast(&addr), &len)) != .SUCCESS) return error.SkipZigTest;

    var endpoint: [64]u8 = undefined;
    const port = std.mem.bigToNative(u16, addr.port);
    const url = try std.fmt.bufPrintSentinel(&endpoint, "http://127.0.0.1:{d}", .{port}, 0);
    const vars = [_][2][*:0]const u8{
        .{ "S3_ENDPOINT_URL", url.ptr }, .{ "AWS_ACCESS_KEY_ID", "ak" }, .{ "AWS_SECRET_ACCESS_KEY", "sk" },
        .{ "AWS_REGION", "us-east-1" },  .{ "ZPQ_S3_PRECONNECT", "2" },
    };
    for (vars) |v| _ = setenv(v[0], v[1], 1);
    defer for (vars) |v| {
        _ = unsetenv(v[0]);
    };

    const plain = zpq_open(null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(?u64, null), plain.pool.preconnect_ns);
    zpq_close(plain);

    const warm = zpq_open("{\"preconnect\": true}") orelse return error.TestUnexpectedResult;
    defer zpq_close(warm);
    try std.testing.expect(warm.pool.preconnect_ns != null);
    // Both connections are parked within the pre-connect's own deadline.
    var waited_ms: u32 = 0;
    while (warm.pool.inner.preconnect_parked.load(.monotonic) < 2 and waited_ms < 2_000) : (waited_ms += 10) {
        const ts: posix.timespec = .{ .sec = 0, .nsec = 10 * std.time.ns_per_ms };
        _ = posix.system.nanosleep(&ts, null);
    }
    try std.testing.expectEqual(@as(u64, 2), warm.pool.inner.preconnect_parked.load(.monotonic));
}
