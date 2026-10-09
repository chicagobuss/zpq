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
};

fn parseOptions(text: []const u8) !?usize {
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    if (trimmed.len == 0) return null;
    const parsed = try std.json.parseFromSlice(Options, gpa, trimmed, .{});
    defer parsed.deinit();
    const v = parsed.value.max_memory orelse return null;
    return switch (v) {
        .null => null,
        .integer => |n| if (n > 0) @intCast(n) else error.BadMaxMemory,
        .string => |str| try zpq.core.system.parseSizeString(str),
        else => error.BadMaxMemory,
    };
}

export fn zpq_open(options_json: ?[*:0]const u8) ?*Ctx {
    const query_memory = parseOptions(if (options_json) |o| std.mem.span(o) else "") catch return null;
    const ctx = gpa.create(Ctx) catch return null;
    ctx.* = .{
        .threaded = std.Io.Threaded.init(gpa, .{}),
        .pool = .sized(zpq.core.system.discoverAvailableMemory(processEnviron())),
    };
    ctx.pool.per_call_stats = false;
    ctx.pool.query_memory = query_memory;
    // Ready before any call: every query, the first ones included, gets the footer cache.
    ctx.pool.warm(gpa) catch {
        ctx.threaded.deinit();
        gpa.destroy(ctx);
        return null;
    };
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
    try std.testing.expectEqual(@as(?usize, null), try parseOptions(""));
    try std.testing.expectEqual(@as(?usize, null), try parseOptions("{}"));
    try std.testing.expectEqual(@as(?usize, 1 << 20), try parseOptions("{\"max_memory\": 1048576}"));
    try std.testing.expect((try parseOptions("{\"max_memory\": \"2GB\"}")).? > 1 << 30);
    try std.testing.expectError(error.UnknownField, parseOptions("{\"max_mem\": 1}"));
    try std.testing.expectError(error.BadMaxMemory, parseOptions("{\"max_memory\": -1}"));
}
