const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");

const ResponseParser = zpq.io.response_parser.ResponseParser;

pub const std_options = std.Options{
    .log_level = .info,
    .log_scope_levels = &[_]std.log.ScopeLevel{
        .{ .scope = .tls, .level = .debug },
    },
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const url = std.process.getEnvVarOwned(allocator, "ZPQ_S3_PRESIGNED_URL") catch |err| switch (err) {
        error.EnvironmentVariableNotFound => {
            std.debug.print("SKIP: set ZPQ_S3_PRESIGNED_URL to run S3 range GET test (presigned URL)\n", .{});
            return;
        },
        else => return err,
    };
    defer allocator.free(url);

    // Range selection (defaults)
    const start = parseEnvUsize(allocator, "ZPQ_S3_RANGE_START") orelse 0;
    const len = parseEnvUsize(allocator, "ZPQ_S3_RANGE_LEN") orelse 256;
    if (len == 0) return error.BadRange;

    const expect_sha256_hex = std.process.getEnvVarOwned(allocator, "ZPQ_S3_EXPECT_SHA256_HEX") catch |err| switch (err) {
        error.EnvironmentVariableNotFound => null,
        else => return err,
    };
    defer if (expect_sha256_hex) |s| allocator.free(s);

    const parsed = try parsePresignedUrl(allocator, url);
    defer parsed.deinit(allocator);

    if (!std.mem.eql(u8, parsed.scheme, "https")) {
        std.debug.print("Only https:// URLs are supported (got {s})\n", .{parsed.scheme});
        return error.UnsupportedScheme;
    }

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var thread_pool = xev.ThreadPool.init(.{ .max_threads = 4 });
    defer {
        thread_pool.shutdown();
        thread_pool.deinit();
    }

    var tp_resolver = zpq.s3.dns.ThreadPoolResolverGen(xev).init(&thread_pool, allocator);
    var dns_completion = zpq.s3.dns.ResolverGen(xev).Completion.init();
    defer dns_completion.deinit(allocator);

    var ctx = try allocator.create(Ctx);
    defer allocator.destroy(ctx);
    defer cleanup(allocator, ctx);
    ctx.* = .{
        .allocator = allocator,
        .result = .{},
        .parser = .{},
        .dst_written = 0,
        .want_len = len,
        .req = null,
        .conn = null,
        .loop = &loop,
        .resolved = false,
        .resolve_err = null,
        .addr = null,
        .expect_sha256_hex = expect_sha256_hex,
        .sha256 = std.crypto.hash.sha2.Sha256.init(.{}),
    };

    std.debug.print("Testing S3 range GET over TLS: host={s} path={s} range={}..{}\n", .{
        parsed.host,
        parsed.request_target,
        start,
        start + len - 1,
    });

    // Resolve hostname asynchronously using our existing resolver (getaddrinfo via threadpool).
    tp_resolver.resolver().resolve(&loop, parsed.host, parsed.port, &dns_completion, onResolved, ctx);
    try loop.run(.until_done);

    if (ctx.resolve_err) |e| return e;
    const addr = ctx.addr orelse return error.DnsResolutionFailed;

    // Now perform the TLS+HTTP range fetch.
    try fetchRange(&loop, allocator, parsed.host, addr, parsed.request_target, start, len, ctx);
    try loop.run(.until_done);

    if (ctx.result.err) |err| {
        if (!(ctx.result.got_any_data and (err == error.EOF or err == error.TlsConnectionClosed))) {
            return err;
        }
    }

    if (ctx.http_status != 206) {
        std.debug.print("Unexpected HTTP status: {d}\n", .{ctx.http_status});
        return error.BadStatus;
    }
    if (ctx.dst_written != len) {
        std.debug.print("Short read: wrote {} expected {}\n", .{ ctx.dst_written, len });
        return error.ShortRead;
    }

    // If provided, validate body SHA256.
    if (ctx.expect_sha256_hex) |hex| {
        var digest: [32]u8 = undefined;
        ctx.sha256.final(&digest);
        const got_hex = std.fmt.bytesToHex(digest, .lower);
        if (!std.mem.eql(u8, got_hex[0..], hex)) {
            std.debug.print("SHA256 mismatch.\n  got: {s}\n  exp: {s}\n", .{ got_hex[0..], hex });
            return error.HashMismatch;
        }
        std.debug.print("S3 range GET OK ({} bytes, sha256 matches)\n", .{len});
    } else {
        std.debug.print("S3 range GET OK ({} bytes)\n", .{len});
    }
}

const ParsedUrl = struct {
    scheme: []const u8,
    host: []const u8,
    port: u16,
    request_target: []const u8, // path + optional ?query

    pub fn deinit(self: ParsedUrl, allocator: std.mem.Allocator) void {
        allocator.free(self.scheme);
        allocator.free(self.host);
        allocator.free(self.request_target);
    }
};

fn parsePresignedUrl(allocator: std.mem.Allocator, url: []const u8) !ParsedUrl {
    // Minimal parser tailored for https://host[:port]/path?query
    // Avoids relying on std.Uri shape changes across Zig nightlies.
    const scheme_end = std.mem.indexOf(u8, url, "://") orelse return error.BadUrl;
    const scheme = url[0..scheme_end];
    var rest = url[scheme_end + 3 ..];

    const path_start = std.mem.indexOfScalar(u8, rest, '/') orelse return error.BadUrl;
    const authority = rest[0..path_start];
    const target = rest[path_start..];

    var host: []const u8 = authority;
    var port: u16 = 443;
    if (std.mem.indexOfScalar(u8, authority, ':')) |colon| {
        host = authority[0..colon];
        const port_str = authority[colon + 1 ..];
        port = try std.fmt.parseInt(u16, port_str, 10);
    }

    return .{
        .scheme = try allocator.dupe(u8, scheme),
        .host = try allocator.dupe(u8, host),
        .port = port,
        .request_target = try allocator.dupe(u8, target),
    };
}

fn parseEnvUsize(allocator: std.mem.Allocator, name: []const u8) ?usize {
    const v = std.process.getEnvVarOwned(allocator, name) catch return null;
    defer allocator.free(v);
    return std.fmt.parseInt(usize, v, 10) catch null;
}

const Connection = zpq.io.tls.Connection;

const FetchResult = struct {
    got_any_data: bool = false,
    bytes: usize = 0,
    err: ?anyerror = null,
};

const Ctx = struct {
    allocator: std.mem.Allocator,
    result: FetchResult,
    parser: ResponseParser,
    http_status: u16 = 0,
    dst_written: usize,
    want_len: usize,
    req: ?*ReqCtx,
    conn: ?*zpq.io.tls.ConnectionGen(xev),
    loop: *xev.Loop,

    // DNS resolution result
    resolved: bool,
    resolve_err: ?anyerror,
    addr: ?xev.shim_net.Address,

    // Optional hash checking
    expect_sha256_hex: ?[]const u8,
    sha256: std.crypto.hash.sha2.Sha256,
};

fn onResolved(ud: ?*anyopaque, results: []const xev.shim_net.Address, err: anyerror!void) void {
    const ctx: *Ctx = @ptrCast(@alignCast(ud));
    if (err) |_| {} else |e| {
        ctx.resolve_err = e;
        ctx.loop.stop();
        return;
    }

    // Prefer IPv4.
    for (results) |a| {
        if (a.any.family == std.posix.AF.INET) {
            ctx.addr = a;
            ctx.resolved = true;
            ctx.loop.stop();
            return;
        }
    }
    if (results.len > 0) {
        ctx.addr = results[0];
        ctx.resolved = true;
        ctx.loop.stop();
        return;
    }
    ctx.resolve_err = error.DnsResolutionFailed;
    ctx.loop.stop();
}

fn fetchRange(
    loop: *xev.Loop,
    allocator: std.mem.Allocator,
    host: []const u8,
    addr: xev.shim_net.Address,
    request_target: []const u8,
    start: usize,
    len: usize,
    ctx: *Ctx,
) !void {
    const ConnectionGen = zpq.io.tls.ConnectionGen(xev);
    const conn = try allocator.create(ConnectionGen);
    conn.* = try ConnectionGen.init(loop, allocator, host);
    ctx.conn = conn;

    const req = try allocator.create(ReqCtx);
    req.* = .{
        .conn = conn,
        .ctx = ctx,
        .host = host,
        .request_target = request_target,
        .start = start,
        .len = len,
    };
    ctx.req = req;

    conn.user_ctx = req;
    conn.on_connect = onConnect;
    conn.on_data = onData;
    conn.on_error = onError;

    try conn.connect(addr);
}

const ReqCtx = struct {
    conn: *Connection,
    ctx: *Ctx,
    host: []const u8,
    request_target: []const u8,
    start: usize,
    len: usize,
};

fn onConnect(ctx_void: ?*anyopaque) void {
    const r: *ReqCtx = @ptrCast(@alignCast(ctx_void));
    const end_inclusive = r.start + r.len - 1;
    const req_fmt =
        "GET {s} HTTP/1.1\r\n" ++
        "Host: {s}\r\n" ++
        "User-Agent: zpq-s3-range\r\n" ++
        "Range: bytes={d}-{d}\r\n" ++
        "Connection: close\r\n" ++
        "\r\n";

    const req = std.fmt.allocPrint(r.ctx.allocator, req_fmt, .{
        r.request_target,
        r.host,
        r.start,
        end_inclusive,
    }) catch return;
    defer r.ctx.allocator.free(req);

    r.conn.write(req) catch |err| {
        r.ctx.result.err = err;
        r.conn.loop.stop();
    };
}

fn onData(ctx_void: ?*anyopaque, data: []const u8) void {
    const r: *ReqCtx = @ptrCast(@alignCast(ctx_void));
    r.ctx.result.got_any_data = true;
    r.ctx.result.bytes += data.len;

    r.ctx.parser.feed(data, r, onBody) catch |err| {
        r.ctx.result.err = err;
        r.conn.loop.stop();
    };
}

fn onBody(body_ctx: *anyopaque, chunk: []const u8) void {
    const r: *ReqCtx = @ptrCast(@alignCast(body_ctx));
    const c = r.ctx;

    if (c.parser.status_code != 0) c.http_status = c.parser.status_code;

    const remaining = c.want_len - c.dst_written;
    const take = @min(remaining, chunk.len);
    if (take > 0) {
        c.sha256.update(chunk[0..take]);
        c.dst_written += take;
    }

    if (c.dst_written == c.want_len and c.parser.state == .done) {
        r.conn.loop.stop();
    }
}

fn onError(ctx_void: ?*anyopaque, err: anyerror) void {
    const r: *ReqCtx = @ptrCast(@alignCast(ctx_void));
    r.ctx.result.err = err;
    r.conn.loop.stop();
}

fn cleanup(allocator: std.mem.Allocator, ctx: *Ctx) void {
    if (ctx.req) |req| {
        allocator.destroy(req);
        ctx.req = null;
    }
    if (ctx.conn) |conn| {
        conn.deinit();
        allocator.destroy(conn);
        ctx.conn = null;
    }
}
