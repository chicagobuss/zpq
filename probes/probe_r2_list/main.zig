//! probe_r2_list: confirm ListObjectsV2 against R2 (or AWS S3).
//!
//! Verifies:
//!   - the request shape ZPQ would issue is accepted
//!   - the XML response carries `<Contents><Key>...</Key><Size>...</Size>...`
//!   - timing (single-page latency)
//!   - pagination via NextContinuationToken (if applicable)
//!
//! Args:
//!   probe_r2_list <s3://bucket/prefix> [<max-keys>]
//!
//! Credentials: `S3_*` (preferred) or `AWS_*` env vars.

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
    return @as(f64, @floatFromInt(nowMonoNs() - start_ns)) / 1_000_000.0;
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const env = init.minimal.environ;

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = iter.next(); // skip program name
    const url_arg = iter.next() orelse {
        std.debug.print("usage: probe_r2_list <s3://bucket/prefix> [<max-keys>]\n", .{});
        return error.BadArgs;
    };
    const max_keys: u32 = if (iter.next()) |s|
        std.fmt.parseInt(u32, s, 10) catch 100
    else
        100;

    const url = s3.Url.parse(url_arg) catch {
        std.debug.print("bad s3 url: {s}\n", .{url_arg});
        return error.BadArgs;
    };

    const creds = s3.Credentials.fromEnv(env) catch |err| {
        std.debug.print("creds: {s}\n", .{@errorName(err)});
        return err;
    };

    var w: StdoutWriter = .{};
    defer w.flush();

    try w.print("{{\n", .{});
    try w.print("  \"schema_version\": 1,\n", .{});
    try w.print("  \"bucket\": \"{s}\",\n", .{url.bucket});
    try w.print("  \"prefix\": \"{s}\",\n", .{url.key});
    try w.print("  \"max_keys\": {d},\n", .{max_keys});

    const host = try s3.hostFor(arena, creds, url.bucket);
    const addr_v4 = try s3.resolveIpv4(arena, host);
    var conn = try tls.Connection.connect(arena, addr_v4, 443, host);
    defer conn.deinit();

    // Path: AWS uses "/" with virtual-hosted; path-style uses "/<bucket>".
    // Same logic as s3.pathFor for an empty key.
    const list_path: []const u8 = if (creds.endpoint == null)
        "/"
    else
        try std.fmt.allocPrint(arena, "/{s}", .{url.bucket});

    // Build the canonical query string. Params must be URL-encoded
    // (`%2F` etc.) and sorted by key. For our use we hand-build it
    // — only 3 params, none with special chars in keys.
    const encoded_prefix = try urlEncode(arena, url.key);
    const max_keys_str = try std.fmt.allocPrint(arena, "{d}", .{max_keys});
    const query = try std.fmt.allocPrint(arena, "list-type=2&max-keys={s}&prefix={s}", .{ max_keys_str, encoded_prefix });

    const total_pages_max: usize = 5;
    var page: usize = 0;
    var continuation: ?[]const u8 = null;
    var total_keys: usize = 0;

    try w.print("  \"pages\": [\n", .{});
    while (page < total_pages_max) : (page += 1) {
        const this_query = if (continuation) |c| blk: {
            const enc = try urlEncode(arena, c);
            // Continuation-token must be sorted into the query string by
            // SigV4. With three+ params, manual sort: the canonical
            // ordering is lex by key:
            //   continuation-token, list-type, max-keys, prefix
            break :blk try std.fmt.allocPrint(arena, "continuation-token={s}&list-type=2&max-keys={s}&prefix={s}", .{ enc, max_keys_str, encoded_prefix });
        } else query;

        const t = nowMonoNs();
        const resp = try sendListGet(arena, &conn, creds, host, list_path, this_query);
        const ms = elapsedMs(t);

        if (resp.status != 200) {
            try w.print("    {{\"page\": {d}, \"status\": {d}, \"ms\": {d:.2}, \"body_bytes\": {d}}}\n", .{ page, resp.status, ms, resp.body.len });
            break;
        }

        // Parse Contents and NextContinuationToken from XML.
        const keys_in_page = countTag(resp.body, "<Key>");
        const next_token = extractTag(resp.body, "<NextContinuationToken>", "</NextContinuationToken>");
        const is_truncated = std.mem.indexOf(u8, resp.body, "<IsTruncated>true</IsTruncated>") != null;
        const sample_key = extractTag(resp.body, "<Key>", "</Key>") orelse "";
        const sample_size = extractTag(resp.body, "<Size>", "</Size>") orelse "";

        try w.print("    {{\"page\": {d}, \"status\": 200, \"ms\": {d:.2}, \"body_bytes\": {d}, \"keys_in_page\": {d}, \"is_truncated\": {s}, \"sample_key\": \"{s}\", \"sample_size\": {s}}}{s}\n", .{
            page, ms, resp.body.len, keys_in_page, if (is_truncated) "true" else "false", sample_key, sample_size,
            if (is_truncated and page + 1 < total_pages_max) "," else "",
        });
        total_keys += keys_in_page;

        if (next_token) |t2| {
            continuation = try arena.dupe(u8, t2);
        } else {
            break;
        }
    }
    try w.print("  ],\n", .{});
    try w.print("  \"total_keys_seen\": {d},\n", .{total_keys});
    try w.print("  \"pages_fetched\": {d}\n", .{page + 1});
    try w.print("}}\n", .{});
}

fn sendListGet(
    arena: std.mem.Allocator,
    conn: *tls.Connection,
    creds: s3.Credentials,
    host: []const u8,
    path: []const u8,
    query: []const u8,
) !http.Response {
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
        query,
        &.{},
        "",
        .{ .use_unsigned_payload = true },
    );

    var req_headers: std.ArrayList(http.Header) = .empty;
    defer req_headers.deinit(arena);
    for (signed) |h| try req_headers.append(arena, .{ .name = h.name, .value = h.value });

    const path_with_query = try std.fmt.allocPrint(arena, "{s}?{s}", .{ path, query });

    return http.sendRequest(arena, conn, .{
        .method = .GET,
        .host = host,
        .path = path_with_query,
        .headers = req_headers.items,
    });
}

/// URL-encode for SigV4 canonical query string. Only safe chars
/// (RFC 3986 unreserved) pass through; everything else becomes %HH.
fn urlEncode(arena: std.mem.Allocator, s: []const u8) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    for (s) |c| {
        const is_unreserved = (c >= 'A' and c <= 'Z') or
            (c >= 'a' and c <= 'z') or
            (c >= '0' and c <= '9') or
            c == '-' or c == '_' or c == '.' or c == '~';
        if (is_unreserved) {
            try out.append(arena, c);
        } else {
            try out.print(arena, "%{X:0>2}", .{c});
        }
    }
    return out.items;
}

fn countTag(haystack: []const u8, needle: []const u8) usize {
    var n: usize = 0;
    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, haystack, pos, needle)) |i| {
        n += 1;
        pos = i + needle.len;
    }
    return n;
}

fn extractTag(haystack: []const u8, open_tag: []const u8, close_tag: []const u8) ?[]const u8 {
    const start = std.mem.indexOf(u8, haystack, open_tag) orelse return null;
    const after_open = start + open_tag.len;
    const end = std.mem.indexOfPos(u8, haystack, after_open, close_tag) orelse return null;
    return haystack[after_open..end];
}

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
        var tmp: [2048]u8 = undefined;
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
