//! libzpq: zpq's JSON request handler (the Lambda's request and response schema) behind a C ABI. See include/zpq.h.
//!
//! A context holds what outlives one request: the allocator, the `std.Io` executor, and the S3 connection pool and
//! footer cache. Each `zpq_run` reads the process environment at call time, as the
//! CLI does at start, for S3 credentials, endpoint and region. Process-wide state outside any context is limited to
//! caches that are set once and then only read: the TLS client contexts (CA stores), and probes of the kernel's
//! io_uring and of the gzip backend.

const std = @import("std");
const zpq = @import("zpq");
const service = @import("service");
const c = std.c;

/// malloc and free, so a host's leak checker (ASan, valgrind) sees every allocation.
const gpa = std.heap.c_allocator;

/// The status codes of include/zpq.h. Stable: a value is never reused for another meaning.
pub const Status = enum(c_int) {
    ok = 0,
    einval = -1,
    enomem = -2,
    eclosed = -3,
    einternal = -4,
};

pub const Ctx = struct {
    threaded: std.Io.Threaded,
    pool: service.PersistentPool,
    /// Guards `inflight` and `closing`.
    lock: c.pthread_mutex_t = .{},
    cond: c.pthread_cond_t = .{},
    /// `zpq_run` calls between their admission and their return.
    inflight: usize = 0,
    /// Set by `zpq_close`: new calls are refused with ZPQ_ECLOSED.
    closing: bool = false,

    /// Count a call in, unless the context is closing.
    fn enter(self: *Ctx) bool {
        _ = c.pthread_mutex_lock(&self.lock);
        defer _ = c.pthread_mutex_unlock(&self.lock);
        if (self.closing) return false;
        self.inflight += 1;
        return true;
    }

    fn leave(self: *Ctx) void {
        _ = c.pthread_mutex_lock(&self.lock);
        defer _ = c.pthread_mutex_unlock(&self.lock);
        self.inflight -= 1;
        if (self.inflight == 0) _ = c.pthread_cond_broadcast(&self.cond);
    }
};

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

export fn zpq_open(options_json: ?[*:0]const u8, out_ctx: ?*?*Ctx, out_error: ?*?[*:0]u8) c_int {
    if (out_error) |e| e.* = null;
    if (out_ctx) |o| o.* = null;
    const out = out_ctx orelse return fail(out_error, .einval, "NullOutContext");
    const opts = parseOptions(if (options_json) |o| std.mem.span(o) else "") catch |err| return switch (err) {
        error.OutOfMemory => fail(out_error, .enomem, @errorName(err)),
        else => fail(out_error, .einval, @errorName(err)),
    };
    const env = processEnviron();
    const ctx = gpa.create(Ctx) catch return fail(out_error, .enomem, "OutOfMemory");
    ctx.* = .{
        .threaded = std.Io.Threaded.init(gpa, .{}),
        .pool = .sized(zpq.core.system.discoverAvailableMemory(env)),
    };
    ctx.pool.per_call_stats = false;
    ctx.pool.query_memory = opts.query_memory;
    // Ready before any call: every query, the first ones included, gets the footer cache.
    ctx.pool.warm(gpa) catch |err| {
        ctx.threaded.deinit();
        gpa.destroy(ctx);
        const status: Status = if (err == error.OutOfMemory) .enomem else .einternal;
        return fail(out_error, status, @errorName(err));
    };
    // Only when asked: a host that never touches S3 should not open connections to it. zpq_close joins them.
    if (opts.preconnect) service.preconnectS3(ctx.threaded.io(), gpa, env, &ctx.pool);
    out.* = ctx;
    return @backingInt(Status.ok);
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
    const self = ctx orelse return fail(out_json, .einval, "NullContext");
    if (out_json == null) return @backingInt(Status.einval);
    if (len > std.math.maxInt(isize)) return fail(out_json, .einval, "BadLength");
    const request = request_json orelse if (len == 0) "" else return fail(out_json, .einval, "NullRequest");
    const body: []const u8 = request[0..len];
    if (!self.enter()) return fail(out_json, .eclosed, "Closed");
    defer self.leave();

    const response = service.handle(self.threaded.io(), gpa, processEnviron(), body, &self.pool, null) catch |err|
        return switch (err) {
            error.OutOfMemory => @backingInt(Status.enomem),
            error.ContextClosed => fail(out_json, .eclosed, "Closed"),
            else => fail(out_json, .einternal, @errorName(err)),
        };
    defer gpa.free(response);
    const buf: [*]u8 = @ptrCast(c.malloc(response.len + 1) orelse return @backingInt(Status.enomem));
    @memcpy(buf[0..response.len], response);
    buf[response.len] = 0;
    out_json.?.* = buf[0..response.len :0].ptr;
    if (out_len) |l| l.* = response.len;
    return @backingInt(Status.ok);
}

/// Return `status`, with `{"error":"<kind>","reason":"<reason>"}` in `*out` when the caller gave one and it can be
/// allocated. ENOMEM carries none: there may be no memory to describe it with.
fn fail(out: ?*?[*:0]u8, status: Status, reason: []const u8) c_int {
    const o = out orelse return @backingInt(status);
    const kind = switch (status) {
        .einval => "invalid_argument",
        .eclosed => "closed",
        .einternal => "internal",
        .ok, .enomem => return @backingInt(status),
    };
    const text = std.fmt.allocPrintSentinel(gpa, "{{\"error\":\"{s}\",\"reason\":\"{s}\"}}", .{ kind, reason }, 0) catch
        return @backingInt(status);
    o.* = text.ptr;
    return @backingInt(status);
}

export fn zpq_free(json: ?[*:0]u8) void {
    c.free(json);
}

export fn zpq_close(ctx: ?*Ctx) void {
    const self = ctx orelse return;
    _ = c.pthread_mutex_lock(&self.lock);
    self.closing = true;
    _ = c.pthread_mutex_unlock(&self.lock);
    _ = c.pthread_mutex_lock(&self.lock);
    while (self.inflight > 0) _ = c.pthread_cond_wait(&self.cond, &self.lock);
    _ = c.pthread_mutex_unlock(&self.lock);
    self.pool.deinit();
    self.threaded.deinit();
    gpa.destroy(self);
}

/// libzpq's ABI version: bumped when a signature or the meaning of a status code changes.
export fn zpq_abi_version() c_int {
    return 1;
}

export fn zpq_strerror(status: c_int) [*:0]const u8 {
    const s = std.enums.fromInt(Status, status) orelse return "unknown status";
    return switch (s) {
        .ok => "ok",
        .einval => "invalid argument",
        .enomem => "out of memory",
        .eclosed => "context is closed",
        .einternal => "internal failure",
    };
}

/// The process environment as it is now (libc's `environ`). After `clearenv()` glibc leaves `environ` NULL, which
/// std declares as never null: read it through an optional instead, and treat NULL as an empty environment.
fn processEnviron() std.process.Environ {
    const environ_ptr = @extern(*const ?[*:null]const ?[*:0]const u8, .{ .name = "environ" });
    const live = environ_ptr.* orelse return .{ .block = .{ .slice = &no_environ } };
    return .{ .block = .{ .slice = std.mem.span(live) } };
}

const no_environ = [_:null]?[*:0]const u8{};

const testing = std.testing;

test "options: a byte count, a size string, nothing, or an error" {
    try testing.expectEqual(Parsed{}, try parseOptions(""));
    try testing.expectEqual(Parsed{}, try parseOptions("{}"));
    try testing.expectEqual(@as(?usize, 1 << 20), (try parseOptions("{\"max_memory\": 1048576}")).query_memory);
    try testing.expect((try parseOptions("{\"max_memory\": \"2GB\"}")).query_memory.? > 1 << 30);
    try testing.expectError(error.UnknownField, parseOptions("{\"max_mem\": 1}"));
    try testing.expectError(error.BadMaxMemory, parseOptions("{\"max_memory\": -1}"));
}

test "options: pre-connect is off unless asked, and only a boolean" {
    try testing.expect(!(try parseOptions("{\"max_memory\": 1048576}")).preconnect);
    try testing.expect((try parseOptions("{\"preconnect\": true}")).preconnect);
    try testing.expect(!(try parseOptions("{\"preconnect\": false}")).preconnect);
    try testing.expectError(error.UnexpectedToken, parseOptions("{\"preconnect\": 4}"));
}

test "every status has a fixed message, and an unknown one says so" {
    try testing.expectEqualStrings("ok", std.mem.span(zpq_strerror(0)));
    try testing.expectEqualStrings("context is closed", std.mem.span(zpq_strerror(-3)));
    try testing.expectEqualStrings("internal failure", std.mem.span(zpq_strerror(-4)));
    try testing.expectEqualStrings("unknown status", std.mem.span(zpq_strerror(7)));
}

test "zpq_open refuses bad options with a reason, and a NULL out_ctx" {
    var ctx: ?*Ctx = undefined;
    var err: ?[*:0]u8 = undefined;
    try testing.expectEqual(@as(c_int, -1), zpq_open("{\"max_mem\": 1}", &ctx, &err));
    try testing.expectEqual(@as(?*Ctx, null), ctx);
    defer zpq_free(err);
    try testing.expectEqualStrings("{\"error\":\"invalid_argument\",\"reason\":\"UnknownField\"}", std.mem.span(err.?));
    // No error wanted: the status alone.
    try testing.expectEqual(@as(c_int, -1), zpq_open("{\"max_memory\": 0}", &ctx, null));
    try testing.expectEqual(@as(c_int, -1), zpq_open(null, null, null));
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

    var plain: ?*Ctx = null;
    try testing.expectEqual(@as(c_int, 0), zpq_open(null, &plain, null));
    try testing.expectEqual(@as(?u64, null), plain.?.pool.preconnect_ns);
    try testing.expectEqual(@as(u64, 0), plain.?.pool.inner.preconnect_parked.load(.monotonic));
    // Nothing reached the listener: no connection waits to be accepted.
    var pfd = [_]posix.pollfd{.{ .fd = lfd, .events = posix.POLL.IN, .revents = 0 }};
    try testing.expect(posix.system.poll(&pfd, 1, 50) == 0);
    zpq_close(plain);

    var warm: ?*Ctx = null;
    try testing.expectEqual(@as(c_int, 0), zpq_open("{\"preconnect\": true}", &warm, null));
    defer zpq_close(warm);
    try testing.expect(warm.?.pool.preconnect_ns != null);
    // Both connections are parked within the pre-connect's own deadline.
    var waited_ms: u32 = 0;
    while (warm.?.pool.inner.preconnect_parked.load(.monotonic) < 2 and waited_ms < 2_000) : (waited_ms += 10) {
        const ts: posix.timespec = .{ .sec = 0, .nsec = 10 * std.time.ns_per_ms };
        _ = posix.system.nanosleep(&ts, null);
    }
    try testing.expectEqual(@as(u64, 2), warm.?.pool.inner.preconnect_parked.load(.monotonic));
}

test "a context opens and answers after the host clears its environment" {
    const environ_ptr = @extern(*?[*:null]const ?[*:0]const u8, .{ .name = "environ" });
    const saved = environ_ptr.*;
    defer environ_ptr.* = saved;
    environ_ptr.* = null; // what clearenv() leaves
    try testing.expectEqual(@as(usize, 0), processEnviron().block.slice.len);
    var ctx: ?*Ctx = null;
    try testing.expectEqual(@as(c_int, 0), zpq_open(null, &ctx, null));
    defer zpq_close(ctx);
    const req = "{\"inputs\":[\"no/such.parquet\"],\"aggregate\":\"count(*) AS n\"}";
    var out: ?[*:0]u8 = null;
    try testing.expectEqual(@as(c_int, 0), zpq_run(ctx, req, req.len, &out, null));
    defer zpq_free(out);
    try testing.expect(std.mem.startsWith(u8, std.mem.span(out.?), "{\"error\""));
}
