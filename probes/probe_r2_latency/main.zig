//! probe_r2_latency: measure workstation→R2 (or any S3-compat) latency.
//!
//! Architectural input. Whether to plumb S3 reads through the CLI vs. fan
//! out per-file Lambdas depends on whether per-file footer fetch is
//! dominated by setup cost (DNS, TLS, RTT) or transfer size.
//!
//! Probes (each timed and reported):
//!   1. resolveIpv4 (cold getaddrinfo)
//!   2. TLS handshake (TCP + ClientHello/Finished)
//!   3. single Range GET (last 64 KB) on a fresh connection
//!   4. 8 sequential Range GETs reusing the same connection
//!   5. <count> sequential Range GETs through a pool of 8 connections
//!
//! Output: JSON to stdout.
//!
//! Args:
//!   probe_r2_latency <s3://bucket/key> [<count>]
//!
//! Credentials: reads `S3_*` env vars (preferred) falling back to `AWS_*`.

const std = @import("std");
const linux = std.os.linux;

const zpq = @import("zpq");
const s3 = zpq.io.s3;
const tls = zpq.io.tls;
const http = zpq.io.http;
const sigv4 = zpq.io.sigv4;

fn nowMonoNs() i64 {
    var ts: linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, ts.sec) * std.time.ns_per_s + @as(i64, ts.nsec);
}

fn elapsedMs(start_ns: i64) f64 {
    const dt = nowMonoNs() - start_ns;
    return @as(f64, @floatFromInt(dt)) / 1_000_000.0;
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const env = init.minimal.environ;
    const io = init.io;

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = iter.next(); // skip program name
    const url_arg = iter.next() orelse {
        std.debug.print("usage: probe_r2_latency <s3://bucket/key> [<count>]\n", .{});
        return error.BadArgs;
    };
    const count: usize = if (iter.next()) |s|
        std.fmt.parseInt(usize, s, 10) catch 24
    else
        24;

    const url = s3.Url.parse(url_arg) catch {
        std.debug.print("bad s3 url: {s}\n", .{url_arg});
        return error.BadArgs;
    };

    const creds = s3.Credentials.fromEnv(env) catch |err| {
        std.debug.print("creds: {s} (need S3_* or AWS_* env)\n", .{@errorName(err)});
        return err;
    };

    var stdout: StdoutWriter = .{};
    const w = &stdout;
    defer w.flush();
    try w.print("{{\n", .{});
    try w.print("  \"schema_version\": 1,\n", .{});
    try w.print("  \"bucket\": \"{s}\",\n", .{url.bucket});
    try w.print("  \"endpoint\": \"{s}\",\n", .{creds.endpoint orelse "(default AWS)"});
    try w.print("  \"region\": \"{s}\",\n", .{creds.region});

    // ---- 1. DNS resolution ----
    const host = try s3.hostFor(arena, creds, url.bucket);
    try w.print("  \"host\": \"{s}\",\n", .{host});

    const t_dns = nowMonoNs();
    const addr_v4 = s3.resolveIpv4(arena, host) catch |err| {
        try w.print("  \"dns_error\": \"{s}\"\n}}\n", .{@errorName(err)});
        return;
    };
    const dns_ms = elapsedMs(t_dns);
    try w.print("  \"resolved_ipv4\": \"{s}\",\n", .{addr_v4});
    try w.print("  \"dns_ms\": {d:.2},\n", .{dns_ms});

    // ---- 2. TLS handshake ----
    const t_tls = nowMonoNs();
    var conn = tls.Connection.connect(arena, addr_v4, 443, host) catch |err| {
        try w.print("  \"tls_error\": \"{s}\"\n}}\n", .{@errorName(err)});
        return;
    };
    const tls_ms = elapsedMs(t_tls);
    try w.print("  \"tls_handshake_ms\": {d:.2},\n", .{tls_ms});

    // ---- 3. First Range GET on the fresh connection ----
    const tail_size: u64 = 64 * 1024;
    const path = try s3.pathFor(arena, creds, url.bucket, url.key);
    const t_first = nowMonoNs();
    const first_resp = sendRangeGet(arena, &conn, creds, host, path, s3.Range.suffix(tail_size)) catch |err| {
        try w.print("  \"first_get_error\": \"{s}\"\n}}\n", .{@errorName(err)});
        conn.deinit();
        return;
    };
    const first_ms = elapsedMs(t_first);
    try w.print("  \"first_get\": {{\"status\": {d}, \"body_bytes\": {d}, \"ms\": {d:.2}}},\n", .{
        first_resp.status, first_resp.body.len, first_ms,
    });

    // ---- 4. Reuse connection: 8 sequential range GETs ----
    const reuse_count: usize = 8;
    var samples: [reuse_count]f64 = undefined;
    var i: usize = 0;
    while (i < reuse_count) : (i += 1) {
        const t_g = nowMonoNs();
        _ = sendRangeGet(arena, &conn, creds, host, path, s3.Range.suffix(tail_size)) catch |err| {
            try w.print("  \"reuse_get_error\": \"{s}\",\n", .{@errorName(err)});
            break;
        };
        samples[i] = elapsedMs(t_g);
    }
    conn.deinit();
    var total: f64 = 0;
    var min_ms: f64 = std.math.inf(f64);
    var max_ms: f64 = 0;
    for (samples[0..i]) |s| {
        total += s;
        if (s < min_ms) min_ms = s;
        if (s > max_ms) max_ms = s;
    }
    try w.print("  \"reuse_get\": {{\"count\": {d}, \"total_ms\": {d:.2}, \"min_ms\": {d:.2}, \"max_ms\": {d:.2}, \"avg_ms\": {d:.2}}},\n", .{
        i, total, min_ms, max_ms, total / @as(f64, @floatFromInt(@max(i, 1))),
    });

    // ---- 5. Pool fan-out: <count> tail GETs through s3.Pool(8) ----
    // Sequential through the pool — measures steady-state per-request
    // cost when all 8 connections are warm (TLS sessions reused).
    var pool_state: s3.Pool(8) = undefined;
    try pool_state.init(gpa, host, addr_v4, 443);
    defer pool_state.deinit();

    const t_fanout = nowMonoNs();
    var fanout_ok: usize = 0;
    var fanout_err: usize = 0;
    for (0..count) |_| {
        _ = s3.getViaPool(io, &pool_state, arena, creds, url, s3.Range.suffix(tail_size)) catch {
            fanout_err += 1;
            continue;
        };
        fanout_ok += 1;
    }
    const fanout_ms = elapsedMs(t_fanout);
    try w.print("  \"pool_serial_fanout\": {{\"requested\": {d}, \"ok\": {d}, \"err\": {d}, \"total_ms\": {d:.2}, \"avg_ms\": {d:.4}}}\n", .{
        count, fanout_ok, fanout_err, fanout_ms, fanout_ms / @as(f64, @floatFromInt(@max(fanout_ok, 1))),
    });

    try w.print("}}\n", .{});
}

// Self-contained writer — same shape used by cli/main.zig so probes
// don't have to deal with the std.Io vtable.
const StdoutWriter = struct {
    fd: linux.fd_t = 1,
    buf: [4096]u8 = undefined,
    pos: usize = 0,

    pub fn writeAll(self: *StdoutWriter, bytes: []const u8) !void {
        var i: usize = 0;
        while (i < bytes.len) {
            const space = self.buf.len - self.pos;
            const n = @min(bytes.len - i, space);
            @memcpy(self.buf[self.pos..][0..n], bytes[i..][0..n]);
            self.pos += n;
            i += n;
            if (self.pos == self.buf.len) self.flush();
        }
    }

    pub fn print(self: *StdoutWriter, comptime fmt: []const u8, args: anytype) !void {
        var tmp: [512]u8 = undefined;
        const out = try std.fmt.bufPrint(&tmp, fmt, args);
        try self.writeAll(out);
    }

    pub fn flush(self: *StdoutWriter) void {
        if (self.pos == 0) return;
        var written: usize = 0;
        while (written < self.pos) {
            const r = linux.write(self.fd, self.buf[written..].ptr, self.pos - written);
            const n: isize = @bitCast(r);
            if (n <= 0) break;
            written += @intCast(n);
        }
        self.pos = 0;
    }
};

fn sendRangeGet(
    arena: std.mem.Allocator,
    conn: *tls.Connection,
    creds: s3.Credentials,
    host: []const u8,
    path: []const u8,
    range: s3.Range,
) !http.Response {
    var range_buf: [64]u8 = undefined;
    // s3.Range.writeHeader is private; inline the formatting we need
    // (suffix-style: bytes=-N).
    const range_value = if (range.start == std.math.maxInt(u64))
        try std.fmt.bufPrint(&range_buf, "bytes=-{d}", .{range.end})
    else
        try std.fmt.bufPrint(&range_buf, "bytes={d}-{d}", .{ range.start, range.end });

    const signer: sigv4.SigV4 = .{
        .region = creds.region,
        .access_key = creds.access_key,
        .secret_key = creds.secret_key,
        .session_token = creds.session_token,
    };
    const signed = try signer.sign(
        arena,
        "GET",
        host,
        path,
        null,
        &.{.{ .name = "Range", .value = range_value }},
        "",
        .{ .use_unsigned_payload = true },
    );

    var req_headers: std.ArrayList(http.Header) = .empty;
    defer req_headers.deinit(arena);
    for (signed) |h| try req_headers.append(arena, .{ .name = h.name, .value = h.value });

    return http.sendRequest(arena, conn, .{
        .method = .GET,
        .host = host,
        .path = path,
        .headers = req_headers.items,
    });
}
