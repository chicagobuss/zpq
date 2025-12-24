const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");

const ResponseParser = @import("response_parser").ResponseParser;
const fixtures = @import("minio_fixtures");

pub const std_options = std.Options{
    .log_level = .info,
    .log_scope_levels = &[_]std.log.ScopeLevel{
        .{ .scope = .tls, .level = .debug },
    },
};

const payload = fixtures.range_payload;

pub fn main() !void {
    // Skip if ZPQ_TEST_MINIO not set (requires local minio docker)
    if (std.posix.getenv("ZPQ_TEST_MINIO") == null) {
        std.debug.print("SKIP: set ZPQ_TEST_MINIO=1 to run (requires local minio)\n", .{});
        return;
    }

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    // Ensure MinIO fixture exists:
    //   cd tools/minio_tls && ./setup_fixture.sh
    const host = "localhost";
    const ip = "127.0.0.1";
    const port: u16 = 9000;
    const path = "/zpq-ci/range_payload.bin";

    // Choose a range well within the payload.
    const start: usize = 123;
    const len: usize = 321;
    const expected = payload[start .. start + len];

    var ctx = try allocator.create(Ctx);
    defer allocator.destroy(ctx);
    ctx.* = .{
        .allocator = allocator,
        .result = .{},
        .parser = .{},
        .dst = try allocator.alloc(u8, len),
        .dst_written = 0,
        .req = null,
        .conn = null,
    };
    defer allocator.free(ctx.dst);

    var client = zpq.io.http.Client.init(&loop, allocator);
    defer cleanup(&client, ctx);

    std.debug.print("Testing MinIO range GET over TLS: {s} bytes={}..{}\n", .{ path, start, start + len - 1 });

    try fetchRange(&client, host, ip, port, path, start, len, ctx);
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
    if (!std.mem.eql(u8, ctx.dst[0..len], expected)) {
        return error.RangeMismatch;
    }

    std.debug.print("MinIO range GET OK ({} bytes)\n", .{len});
}

const Ctx = struct {
    allocator: std.mem.Allocator,
    result: zpq.io.http.Client.FetchResult,
    parser: ResponseParser,
    http_status: u16 = 0,
    dst: []u8,
    dst_written: usize,
    req: ?*ReqCtx,
    conn: ?*zpq.io.tls.Connection,
};

fn fetchRange(
    client: *zpq.io.http.Client,
    host: []const u8,
    ip: []const u8,
    port: u16,
    path: []const u8,
    start: usize,
    len: usize,
    ctx: *Ctx,
) !void {
    // We reuse the underlying Client, but override callbacks to parse response/body.
    // We'll create our own Connection through fetchWithResult and then intercept onData.
    // Easiest: create a one-off connection ourselves, because current Client hardcodes
    // onConnect/onData behaviour (prints).

    const conn = try client.allocator.create(zpq.io.tls.Connection);
    conn.* = try zpq.io.tls.Connection.init(client.loop, client.allocator, host);
    ctx.conn = conn;

    // Create a request context compatible with Connection callbacks
    const req = try client.allocator.create(ReqCtx);
    req.* = .{
        .conn = conn,
        .client = client,
        .ctx = ctx,
        .host = host,
        .path = path,
        .start = start,
        .len = len,
    };
    ctx.req = req;

    conn.user_ctx = req;
    conn.on_connect = onConnect;
    conn.on_data = onData;
    conn.on_error = onError;

    // Parse IP
    const addr = try xev.shim_net.Address.parseIp4(ip, port);
    try conn.connect(addr);
}

const ReqCtx = struct {
    conn: *zpq.io.tls.Connection,
    client: *zpq.io.http.Client,
    ctx: *Ctx,
    host: []const u8,
    path: []const u8,
    start: usize,
    len: usize,
};

fn onConnect(ctx_void: ?*anyopaque) void {
    const r: *ReqCtx = @ptrCast(@alignCast(ctx_void));

    const end_inclusive = r.start + r.len - 1;
    const req_fmt =
        "GET {s} HTTP/1.1\r\n" ++
        "Host: {s}\r\n" ++
        "User-Agent: zpq-minio-range\r\n" ++
        "Range: bytes={d}-{d}\r\n" ++
        "Connection: close\r\n" ++
        "\r\n";

    const req = std.fmt.allocPrint(r.ctx.allocator, req_fmt, .{ r.path, r.host, r.start, end_inclusive }) catch return;
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
    const remaining = c.dst.len - c.dst_written;
    const take = @min(remaining, chunk.len);
    if (take > 0) {
        @memcpy(c.dst[c.dst_written .. c.dst_written + take], chunk[0..take]);
        c.dst_written += take;
    }
    if (c.dst_written == c.dst.len and c.parser.state == .done) {
        r.conn.loop.stop();
    }
}

fn onError(ctx_void: ?*anyopaque, err: anyerror) void {
    const r: *ReqCtx = @ptrCast(@alignCast(ctx_void));
    r.ctx.result.err = err;
    r.conn.loop.stop();
}

fn cleanup(client: *zpq.io.http.Client, ctx: *Ctx) void {
    // Best-effort cleanup of heap allocations after loop stops.
    if (ctx.req) |req| {
        client.allocator.destroy(req);
        ctx.req = null;
    }
    if (ctx.conn) |conn| {
        conn.deinit();
        client.allocator.destroy(conn);
        ctx.conn = null;
    }
}
