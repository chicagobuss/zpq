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
pub const Pool = struct {
    connections: pool_mod.Pool(256) = undefined,
    capability_key: [32]u8 = @splat(0),
    prepared_v1: bool = false,
    pub fn init(self: *Pool, a: A) !void {
        self.* = .{};
        try self.connections.init(a);
    }
    pub fn deinit(self: *Pool) void {
        self.connections.deinit();
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
        var path = try std.fmt.allocPrint(a, "/2015-03-31/functions/{s}/invocations", .{try encode(a, name)});
        if (qualifier) |q| {
            if (q.len == 0) return error.InvalidFunction;
            path = try std.fmt.allocPrint(a, "{s}?Qualifier={s}", .{ path, try encode(a, q) });
        }
        return .{ .region = r, .path = path };
    }
};
fn encode(a: A, raw: []const u8) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    for (raw) |ch| {
        if (std.ascii.isAlphanumeric(ch) or std.mem.indexOfScalar(u8, "-_.~", ch) != null)
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
                    }
                    known = &self.pool.prepared_v1;
                }
            }
        };
        return .{ .ctx = self, .invoke = invoke, .prepared_v1 = known };
    }
    fn invoke(ctx: *anyopaque, a: A, index: usize, body: []const u8) ![]const u8 {
        const self: *Client = @ptrCast(@alignCast(ctx));
        if (self.endpoints.len == 0) return error.NoInvokeEndpoints;
        if (body.len > 6 << 20) return error.InvokeRequestTooLarge;
        const endpoint = self.endpoints[index % self.endpoints.len];
        // Retry only a stale keep-alive socket, once. Never retry a function or protocol error.
        for (0..2) |attempt| {
            const h = try self.pool.connections.acquire(self.io, .{
                .host = endpoint.host,
                .addrs = endpoint.addrs,
                .port = endpoint.port,
                .use_tls = endpoint.use_tls,
            }, .{ .fresh = attempt != 0 });
            var retained = false;
            defer if (!retained) self.pool.connections.discard(self.io, h);
            const signed_headers = try self.headers(a, endpoint, body);
            const response = http.sendRequest(a, h.conn, .{
                .method = .POST,
                .host = endpoint.authority,
                .path = endpoint.path,
                .headers = signed_headers,
                .body = body,
                .total_ms = 900_000,
            }) catch |err| {
                if (attempt == 0 and h.reused and (err == error.ClosedBeforeResponse or err == error.SendFailed))
                    continue;
                return err;
            };
            const close = if (response.header("Connection")) |v| std.ascii.eqlIgnoreCase(v, "close") else false;
            if (!close) {
                self.pool.connections.release(self.io, h);
                retained = true;
            }
            if (response.status != 200) return error.InvokeHttpFailed;
            if (response.header("X-Amz-Function-Error") != null) return error.WorkerFunctionFailed;
            if (response.body.len > 6 << 20) return error.InvokeResponseTooLarge;
            return response.body;
        }
        return error.InvokeHttpFailed;
    }
    fn headers(self: *Client, a: A, endpoint: Endpoint, body: []const u8) ![]const http.Header {
        if (self.region) |region| {
            const access = envValue(self.env, "AWS_ACCESS_KEY_ID") orelse return error.MissingCredentials;
            const secret = envValue(self.env, "AWS_SECRET_ACCESS_KEY") orelse return error.MissingCredentials;
            if (access.len == 0 or secret.len == 0) return error.MissingCredentials;
            const signer: SigV4 = .{ .region = region, .service = "lambda", .access_key = access, .secret_key = secret, .session_token = envValue(self.env, "AWS_SESSION_TOKEN") };
            const q = std.mem.indexOfScalar(u8, endpoint.path, '?') orelse endpoint.path.len;
            const signed = try signer.sign(a, "POST", endpoint.authority, endpoint.path[0..q], if (q < endpoint.path.len) endpoint.path[q + 1 ..] else null, &.{.{ .name = "Content-Type", .value = "application/json" }}, body, .{});
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
