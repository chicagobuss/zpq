//! Lambda Invoke over zpq's pooled HTTP/TLS and SigV4, or unsigned local Invoke endpoints.
const std = @import("std");
const fanout = @import("../engine/fanout.zig");
const pool_mod = @import("pool.zig");
const http = @import("http.zig");
const s3 = @import("s3.zig");
const tls = @import("tls.zig");
const SigV4 = @import("sigv4.zig").SigV4;
const envValue = @import("../core/system.zig").winEnvValue;
const A = std.mem.Allocator;
const invoke_ms = 15 * 60_000;
const retry = @import("retry.zig");
const max_attempts = 3; // AWS standard retry mode: the initial call plus two retries.
const RetryKind = enum { connect, handshake, transport, throttle, server };
pub const retry_names = [_][]const u8{ "connect_retries", "handshake_retries", "transport_retries", "throttle_retries", "server_retries" };

const HttpFailure = struct {
    index: usize,
    status: u16,
    attempts: usize,
    aws_type: [96]u8 = undefined,
    len: usize = 0,

    fn read(a: A, response: http.Response, index: usize, attempts: usize) HttpFailure {
        var out: HttpFailure = .{ .index = index, .status = response.status, .attempts = attempts };
        var name = response.header("x-amzn-ErrorType") orelse "";
        if (name.len == 0) {
            const body = fanout.parse(a, response.body[0..@min(response.body.len, 4096)]) catch return out;
            if (body == .object) for ([_][]const u8{ "__type", "code", "errorType", "Type" }) |key| {
                if (body.object.get(key)) |v| if (v == .string) {
                    name = v.string;
                    break;
                };
            };
        }
        if (std.mem.lastIndexOfScalar(u8, name, '#')) |i| name = name[i + 1 ..];
        // Only the type identifier is diagnostic: never the AWS message, payload, or signed headers.
        for (name) |ch| {
            if (out.len == out.aws_type.len or !(std.ascii.isAlphanumeric(ch) or ch == '_' or ch == '-' or ch == '.')) break;
            out.aws_type[out.len] = ch;
            out.len += 1;
        }
        return out;
    }
};

pub const Pool = struct {
    connections: pool_mod.Pool(256) = undefined,
    capability_key: [32]u8 = @splat(0),
    prepared_v1: bool = false,
    aligned_v1: bool = false,
    retry_counts: [retry_names.len]std.atomic.Value(u64) = @splat(.init(0)),
    failure_mutex: std.Io.Mutex = .init,
    http_failure: ?HttpFailure = null,
    retry_start: u64 = 0,
    pub fn init(self: *Pool, a: A) !void {
        self.* = .{};
        try self.connections.init(a);
    }
    pub fn deinit(self: *Pool) void {
        self.connections.deinit();
    }
    pub fn snapshotRetries(self: *Pool) [retry_names.len]u64 {
        var out: [retry_names.len]u64 = undefined;
        for (&out, &self.retry_counts) |*n, *counter| n.* = counter.load(.monotonic);
        return out;
    }
    pub fn beginRequest(self: *Pool, io: std.Io) void {
        self.failure_mutex.lockUncancelable(io);
        defer self.failure_mutex.unlock(io);
        self.http_failure = null;
        self.retry_start = 0;
        for (self.snapshotRetries()) |n| self.retry_start += n;
    }
    fn recordFailure(self: *Pool, io: std.Io, failure: HttpFailure) void {
        self.failure_mutex.lockUncancelable(io);
        defer self.failure_mutex.unlock(io);
        if (self.http_failure == null or failure.index < self.http_failure.?.index) self.http_failure = failure;
    }
    pub fn errorMessage(self: *Pool, io: std.Io, err: anyerror, buffer: []u8) []const u8 {
        self.failure_mutex.lockUncancelable(io);
        defer self.failure_mutex.unlock(io);
        var total: u64 = 0;
        for (self.snapshotRetries()) |n| total += n;
        const retried = total -| self.retry_start;
        if (err == error.InvokeHttpFailed) if (self.http_failure) |f| {
            return std.fmt.bufPrint(buffer, "InvokeHttpFailed: HTTP {d}, AWS {s}, invoke {d}, attempts {d}, retries {d}", .{
                f.status, if (f.len == 0) "Unknown" else f.aws_type[0..f.len], f.index, f.attempts, retried,
            }) catch @errorName(err);
        };
        if (retried == 0) return @errorName(err);
        return std.fmt.bufPrint(buffer, "{s}: retries {d}", .{ @errorName(err), retried }) catch @errorName(err);
    }
};

pub const Target = struct {
    region: []const u8,
    path: []const u8,
    pub fn init(a: A, function: []const u8, configured_region: ?[]const u8) !Target {
        if (function.len == 0) return error.InvalidFunction;
        var name = function;
        var qualifier: ?[]const u8 = null;
        var region = configured_region;
        if (std.mem.startsWith(u8, function, "arn:")) {
            var parts = std.mem.splitScalar(u8, function, ':');
            _ = parts.next();
            _ = parts.next();
            if (!std.mem.eql(u8, parts.next() orelse "", "lambda")) return error.InvalidFunction;
            const arn_region = parts.next() orelse return error.InvalidFunction;
            if (arn_region.len > 0) region = arn_region;
            _ = parts.next() orelse return error.InvalidFunction;
            if (!std.mem.eql(u8, parts.next() orelse "", "function")) return error.InvalidFunction;
            const fname = parts.next() orelse return error.InvalidFunction;
            if (fname.len == 0) return error.InvalidFunction;
            if (parts.next()) |q| {
                qualifier = q;
                name = function[0 .. function.len - q.len - 1];
            }
            if (parts.next() != null) return error.InvalidFunction;
        } else if (std.mem.indexOf(u8, function, ":function:")) |marker| {
            const rest = function[marker + ":function:".len ..];
            if (std.mem.indexOfScalar(u8, rest, ':')) |colon| {
                const end = marker + ":function:".len + colon;
                name = function[0..end];
                qualifier = function[end + 1 ..];
            }
        } else if (std.mem.indexOfScalar(u8, function, ':')) |colon| {
            name = function[0..colon];
            qualifier = function[colon + 1 ..];
        }
        const r = region orelse return error.MissingRegion;
        if (r.len == 0) return error.MissingRegion;
        var path = try std.fmt.allocPrint(a, "/2015-03-31/functions/{s}/invocations", .{try encode(a, name, false)});
        if (qualifier) |q| {
            if (q.len == 0) return error.InvalidFunction;
            path = try std.fmt.allocPrint(a, "{s}?Qualifier={s}", .{ path, try encode(a, q, false) });
        }
        return .{ .region = r, .path = path };
    }
};
fn encode(a: A, raw: []const u8, preserve_slashes: bool) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    for (raw) |ch| {
        if (std.ascii.isAlphanumeric(ch) or std.mem.indexOfScalar(u8, "-_.~", ch) != null or (preserve_slashes and ch == '/'))
            try out.append(a, ch)
        else {
            const hex = "0123456789ABCDEF";
            try out.appendSlice(a, &.{ '%', hex[ch >> 4], hex[ch & 15] });
        }
    }
    return out.toOwnedSlice(a);
}

pub const Endpoint = struct {
    authority: []const u8,
    host: []const u8,
    port: u16,
    use_tls: bool,
    path: []const u8,
    addrs: []const tls.Ipv4,
    pub fn init(a: A, url: []const u8, default_path: []const u8) !Endpoint {
        const secure = std.mem.startsWith(u8, url, "https://");
        if (!secure and !std.mem.startsWith(u8, url, "http://")) return error.InvalidInvokeEndpoint;
        const rest = url[if (secure) 8 else 7..];
        const slash = std.mem.indexOfScalar(u8, rest, '/') orelse rest.len;
        const authority = rest[0..slash];
        if (authority.len == 0 or std.mem.indexOfAny(u8, authority, "@\r\n?#") != null)
            return error.InvalidInvokeEndpoint;
        const colon = std.mem.lastIndexOfScalar(u8, authority, ':');
        const host = if (colon) |i| authority[0..i] else authority;
        const port: u16 = if (colon) |i| try std.fmt.parseInt(u16, authority[i + 1 ..], 10) else if (secure) 443 else 80;
        const path = if (slash == rest.len or std.mem.eql(u8, rest[slash..], "/")) default_path else rest[slash..];
        if (std.mem.indexOfAny(u8, path, "\r\n#") != null) return error.InvalidInvokeEndpoint;
        return .{ .authority = authority, .host = host, .port = port, .use_tls = secure, .path = path, .addrs = try s3.resolveIpv4(a, host) };
    }
};

pub const Client = struct {
    io: std.Io,
    env: std.process.Environ,
    pool: *Pool,
    endpoints: []const Endpoint,
    /// Non-null only for AWS; local endpoints never inspect credentials.
    region: ?[]const u8 = null,
    pub fn transport(self: *Client) fanout.Transport {
        var known: ?*bool = null;
        if (self.region) |region| if (self.endpoints.len == 1) {
            const path = self.endpoints[0].path;
            if (std.mem.indexOf(u8, path, "?Qualifier=")) |q| {
                const version = path[q + "?Qualifier=".len ..];
                var immutable = version.len > 0;
                for (version) |ch| if (!std.ascii.isDigit(ch)) {
                    immutable = false;
                };
                if (immutable) {
                    var hash = std.crypto.hash.sha2.Sha256.init(.{});
                    hash.update(region);
                    hash.update(self.endpoints[0].authority);
                    hash.update(path);
                    const key = hash.finalResult();
                    if (!std.mem.eql(u8, &key, &self.pool.capability_key)) {
                        self.pool.capability_key = key;
                        self.pool.prepared_v1 = false;
                        self.pool.aligned_v1 = false;
                    }
                    known = &self.pool.prepared_v1;
                }
            }
        };
        return .{ .ctx = self, .invoke = invoke, .prepared_v1 = known, .aligned_v1 = if (known != null) &self.pool.aligned_v1 else null };
    }
    fn invoke(ctx: *anyopaque, a: A, index: usize, body: []const u8) ![]const u8 {
        const self: *Client = @ptrCast(@alignCast(ctx));
        if (self.endpoints.len == 0) return error.NoInvokeEndpoints;
        if (body.len > 6 << 20) return error.InvokeRequestTooLarge;
        const endpoint = self.endpoints[index % self.endpoints.len];
        const invocation_deadline = tls.invocation_deadline_ms.load(.monotonic);
        const deadline = @min(tls.deadlineFromNow(invoke_ms), if (invocation_deadline == 0) std.math.maxInt(u64) else invocation_deadline);
        var fresh = false;
        for (0..max_attempts) |attempt| {
            if (remaining(deadline) == 0) return error.RequestTimeout;
            var failure: Failure = .{};
            return self.once(a, endpoint, index, body, fresh, deadline, attempt + 1, &failure) catch |err| {
                if (failure.kind) |kind| if (try self.retryAfter(kind, attempt, deadline)) {
                    if (failure.stale) self.pool.connections.noteRetry(self.io, .stale);
                    fresh = kind == .connect or kind == .handshake or kind == .transport;
                    continue;
                };
                if (failure.http) |f| self.pool.recordFailure(self.io, f);
                return err;
            };
        }
        unreachable;
    }
    const Failure = struct { kind: ?RetryKind = null, http: ?HttpFailure = null, stale: bool = false };
    fn remaining(deadline: u64) u32 {
        return @intCast(@min(deadline -| tls.deadlineFromNow(0), std.math.maxInt(u32)));
    }
    fn retryAfter(self: *Client, kind: RetryKind, attempt: usize, deadline: u64) !bool {
        if (attempt + 1 == max_attempts or remaining(deadline) == 0) return false;
        // AWS's standard full-jitter bases: 50 ms for transient errors, 1000 ms for throttling.
        const policy: retry.Policy = .{ .base_ms = if (kind == .throttle) 1000 else 50, .cap_ms = 20_000 };
        const delay = @min(retry.backoffMs(policy, @intCast(attempt)), remaining(deadline));
        if (delay != 0) try self.io.sleep(.fromMilliseconds(@intCast(delay)), .awake);
        if (remaining(deadline) == 0) return false;
        _ = self.pool.retry_counts[@backingInt(kind)].fetchAdd(1, .monotonic);
        return true;
    }
    fn once(self: *Client, a: A, endpoint: Endpoint, index: usize, body: []const u8, fresh: bool, deadline: u64, attempt: usize, failure: *Failure) ![]const u8 {
        const h = self.pool.connections.acquire(self.io, .{
            .host = endpoint.host,
            .addrs = endpoint.addrs,
            .port = endpoint.port,
            .use_tls = endpoint.use_tls,
            // Synchronous Invoke may be silent until the function finishes, even after response headers.
            .timeouts = .{ .first_byte_ms = invoke_ms, .idle_ms = invoke_ms },
            .deadline_ms = deadline,
        }, .{ .fresh = fresh }) catch |err| {
            if (retry.classifyConnect(err) != .fatal or err == error.ConnectionReset or err == error.UncleanClose) failure.kind = switch (err) {
                error.HandshakeFailed, error.HandshakeTimeout => .handshake,
                else => .connect,
            };
            return err;
        };
        var retained = false;
        defer if (!retained) self.pool.connections.discard(self.io, h);
        const signed_headers = try self.headers(a, endpoint, body);
        const left = remaining(deadline);
        if (left == 0) return error.RequestTimeout;
        const response = http.sendRequest(a, h.conn, .{
            .method = .POST,
            .host = endpoint.authority,
            .path = endpoint.path,
            .headers = signed_headers,
            .body = body,
            .total_ms = left,
            .reject_response_header = "X-Amz-Function-Error",
        }) catch |err| {
            if (err == error.RejectedResponseHeader) return error.WorkerFunctionFailed;
            const classification = retry.classify(err, .{ .idempotent = true, .reused = h.reused });
            if (classification != .fatal or err == error.ConnectionReset or err == error.UncleanClose) failure.kind = .transport;
            failure.stale = classification == .stale;
            return err;
        };
        const close = if (response.header("Connection")) |v| std.ascii.eqlIgnoreCase(v, "close") else false;
        if (!close) {
            self.pool.connections.release(self.io, h);
            retained = true;
        }
        if (response.status != 200) {
            failure.http = HttpFailure.read(a, response, index, attempt);
            const f = failure.http.?;
            const throttle = std.mem.eql(u8, f.aws_type[0..f.len], "TooManyRequestsException");
            failure.kind = if (response.status == 429) .throttle else if (response.status >= 500 and response.status <= 599)
                (if (throttle) .throttle else .server)
            else
                null;
            return error.InvokeHttpFailed;
        }
        if (response.body.len > 6 << 20) return error.InvokeResponseTooLarge;
        return response.body;
    }
    fn headers(self: *Client, a: A, endpoint: Endpoint, body: []const u8) ![]const http.Header {
        if (self.region) |region| {
            const access = envValue(self.env, "AWS_ACCESS_KEY_ID") orelse return error.MissingCredentials;
            const secret = envValue(self.env, "AWS_SECRET_ACCESS_KEY") orelse return error.MissingCredentials;
            if (access.len == 0 or secret.len == 0) return error.MissingCredentials;
            const signer: SigV4 = .{ .region = region, .service = "lambda", .access_key = access, .secret_key = secret, .session_token = envValue(self.env, "AWS_SESSION_TOKEN") };
            const q = std.mem.indexOfScalar(u8, endpoint.path, '?') orelse endpoint.path.len;
            // The shared S3 signer takes a canonical URI verbatim; Lambda escapes the wire path again.
            const canonical_path = try encode(a, endpoint.path[0..q], true);
            const signed = try signer.sign(a, "POST", endpoint.authority, canonical_path, if (q < endpoint.path.len) endpoint.path[q + 1 ..] else null, &.{.{ .name = "Content-Type", .value = "application/json" }}, body, .{});
            const out = try a.alloc(http.Header, signed.len);
            for (signed, out) |s, *d| d.* = .{ .name = s.name, .value = s.value };
            return out;
        }
        return &.{.{ .name = "Content-Type", .value = "application/json" }};
    }
};

test "fanout invoke: preserve account region and qualifier" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const target = try Target.init(arena.allocator(), "arn:aws:lambda:eu-west-1:222222222222:function:worker:7", "us-west-2");
    try std.testing.expectEqualStrings("eu-west-1", target.region);
    try std.testing.expectEqualStrings("/2015-03-31/functions/arn%3Aaws%3Alambda%3Aeu-west-1%3A222222222222%3Afunction%3Aworker/invocations?Qualifier=7", target.path);
    const partial = try Target.init(arena.allocator(), "222222222222:function:worker:7", "eu-west-1");
    try std.testing.expectEqualStrings("/2015-03-31/functions/222222222222%3Afunction%3Aworker/invocations?Qualifier=7", partial.path);
}

test "fanout invoke: Lambda canonical ARN URI is escaped again, wire URI stays single encoded" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const target = try Target.init(a, "123456789012:function:worker:7", "us-west-2");
    const q = std.mem.indexOfScalar(u8, target.path, '?').?;
    try std.testing.expectEqualStrings("/2015-03-31/functions/123456789012%253Afunction%253Aworker/invocations", try encode(a, target.path[0..q], true));
    try std.testing.expectEqualStrings("/2015-03-31/functions/123456789012%3Afunction%3Aworker/invocations?Qualifier=7", target.path);
    try std.testing.expectEqualStrings("/plain/path", try encode(a, "/plain/path", true));
}

test "fanout invoke: retry backoff cannot start an attempt after its deadline or budget" {
    var pool: Pool = undefined;
    try pool.init(std.testing.allocator);
    defer pool.deinit();
    var client: Client = .{ .io = std.testing.io, .env = .empty, .pool = &pool, .endpoints = &.{} };
    try std.testing.expect(!try client.retryAfter(.handshake, 0, tls.deadlineFromNow(0)));
    try std.testing.expect(!try client.retryAfter(.server, max_attempts - 1, tls.deadlineFromNow(1000)));
    try std.testing.expectEqualDeep(@as([retry_names.len]u64, @splat(0)), pool.snapshotRetries());
}
