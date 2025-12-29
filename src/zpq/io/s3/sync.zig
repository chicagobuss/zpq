const std = @import("std");
const io = @import("../interface.zig");
const sigv4 = @import("sigv4.zig");
const types = @import("types.zig");
const log = @import("../../../zpq.zig").log.s3;

// Export shared types
pub const S3Config = types.S3Config;
pub const Credentials = types.Credentials;

// Export Async Components (Legacy wrappers)

pub const S3Source = struct {
    allocator: std.mem.Allocator,

    // Own the IO runtime
    threaded: *std.Io.Threaded,
    // Client must be a pointer because it contains a Mutex
    client: *std.http.Client,

    url: []u8,
    uri: std.Uri,
    object_size: u64,

    // Auth state
    config: S3Config,

    pub fn init(allocator: std.mem.Allocator, bucket: []const u8, key: []const u8, config: S3Config) !S3Source {
        const threaded = try allocator.create(std.Io.Threaded);
        threaded.* = std.Io.Threaded.init(allocator);
        errdefer {
            threaded.deinit();
            allocator.destroy(threaded);
        }

        const client_ptr = try allocator.create(std.http.Client);
        errdefer allocator.destroy(client_ptr);

        client_ptr.* = std.http.Client{
            .allocator = allocator,
            .io = threaded.io(),
        };
        errdefer client_ptr.deinit();

        // Unify config storage - we now store the config directly as passed (which might borrow)
        // or we can dupe it if we want ownership. For the sync S3Source, we'll borror/dupe selectively.
        // Let's dupe key strings to be safe since this struct might live longer than CLI args.

        var owned_config = config;
        if (config.credentials) |creds| {
            owned_config.credentials = .{
                .access_key = try allocator.dupe(u8, creds.access_key),
                .secret_key = try allocator.dupe(u8, creds.secret_key),
                .session_token = if (creds.session_token) |st| try allocator.dupe(u8, st) else null,
            };
        }
        owned_config.region = try allocator.dupe(u8, config.region);
        owned_config.endpoint = if (config.endpoint) |ep| try allocator.dupe(u8, ep) else null;

        errdefer {
            if (owned_config.credentials) |creds| {
                allocator.free(creds.access_key);
                allocator.free(creds.secret_key);
                if (creds.session_token) |st| allocator.free(st);
            }
            allocator.free(owned_config.region);
            if (owned_config.endpoint) |ep| allocator.free(ep);
        }

        const encoded_key = try s3Encode(allocator, key);
        defer allocator.free(encoded_key);

        const url = if (owned_config.endpoint) |ep| u: {
            const clean_ep = std.mem.trimEnd(u8, ep, "/");
            break :u try std.fmt.allocPrint(allocator, "{s}/{s}/{s}", .{ clean_ep, bucket, encoded_key });
        } else u: {
            if (std.mem.eql(u8, owned_config.region, "us-east-1")) {
                break :u try std.fmt.allocPrint(allocator, "https://{s}.s3.amazonaws.com/{s}", .{ bucket, encoded_key });
            } else {
                break :u try std.fmt.allocPrint(allocator, "https://{s}.s3.{s}.amazonaws.com/{s}", .{ bucket, owned_config.region, encoded_key });
            }
        };

        errdefer allocator.free(url);

        const uri = try std.Uri.parse(url);

        var self = S3Source{
            .allocator = allocator,
            .threaded = threaded,
            .client = client_ptr,
            .url = url,
            .uri = uri,
            .object_size = 0,
            .config = owned_config,
        };

        self.object_size = try self.fetchSize();
        return self;
    }

    fn s3Encode(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
        var list = std.ArrayList(u8){};
        defer list.deinit(allocator);
        const hex = "0123456789ABCDEF";
        for (input) |c| {
            if (std.ascii.isAlphanumeric(c) or c == '-' or c == '_' or c == '.' or c == '~' or c == '/') {
                try list.append(allocator, c);
            } else {
                try list.append(allocator, '%');
                try list.append(allocator, hex[c >> 4]);
                try list.append(allocator, hex[c & 15]);
            }
        }
        return list.toOwnedSlice(allocator);
    }

    pub fn deinit(self: *S3Source) void {
        self.client.deinit();
        self.allocator.destroy(self.client);
        self.threaded.deinit();
        self.allocator.destroy(self.threaded);
        self.allocator.free(self.url);

        if (self.config.credentials) |creds| {
            self.allocator.free(creds.access_key);
            self.allocator.free(creds.secret_key);
            if (creds.session_token) |s| self.allocator.free(s);
        }
        self.allocator.free(self.config.region);
        if (self.config.endpoint) |ep| self.allocator.free(ep);
    }

    fn fetchSize(self: *S3Source) !u64 {
        var size: u64 = 0;
        const Context = struct { size: *u64 };
        const callback = struct {
            fn call(ctx: Context, res: *std.http.Client.Response) !void {
                if (res.head.content_length) |len| {
                    ctx.size.* = len;
                } else {
                    return error.MissingContentLength;
                }
            }
        }.call;

        try self.performRequest(.HEAD, null, Context{ .size = &size }, callback);
        return size;
    }

    fn readAtImpl(ptr: *anyopaque, offset: u64, buf: []u8) !usize {
        const self: *S3Source = @ptrCast(@alignCast(ptr));

        const Context = struct { buf: []u8, n: *usize };
        var n: usize = 0;

        const callback = struct {
            fn call(ctx: Context, res: *std.http.Client.Response) !void {
                var transfer_buf: [4096]u8 = undefined;
                var reader = res.reader(&transfer_buf);
                ctx.n.* = try reader.readSliceShort(ctx.buf);
            }
        }.call;

        try self.performRequest(.GET, .{ .start = offset, .end = offset + buf.len - 1 }, Context{ .buf = buf, .n = &n }, callback);
        return n;
    }

    fn performRequest(
        self: *S3Source,
        method: std.http.Method,
        range: ?struct { start: u64, end: u64 },
        context: anytype,
        comptime callback: fn (@TypeOf(context), *std.http.Client.Response) anyerror!void,
    ) !void {
        var fallback = std.heap.stackFallback(4096, self.allocator);
        var arena = std.heap.ArenaAllocator.init(fallback.get());
        defer arena.deinit();
        const aa = arena.allocator();

        var headers = std.ArrayList(std.http.Header){};

        if (range) |r| {
            const range_val = try std.fmt.allocPrint(aa, "bytes={d}-{d}", .{ r.start, r.end });
            try headers.append(aa, .{ .name = "Range", .value = range_val });
        }

        if (self.config.credentials) |creds| {
            var auth = sigv4.SigV4{
                .region = self.config.region,
                .access_key = creds.access_key,
                .secret_key = creds.secret_key,
                .session_token = creds.session_token,
            };
            try auth.sign(aa, @tagName(method), self.uri, &headers, "");

            // Remove Host header
            var i: usize = 0;
            while (i < headers.items.len) {
                if (std.ascii.eqlIgnoreCase(headers.items[i].name, "Host")) {
                    _ = headers.orderedRemove(i);
                } else {
                    i += 1;
                }
            }
        }

        var req = try self.client.request(method, self.uri, .{
            .extra_headers = headers.items,
        });
        defer req.deinit();

        try req.sendBodiless();

        var redirect_buf: [1024]u8 = undefined;
        var response = try req.receiveHead(&redirect_buf);

        if (!response.head.keep_alive) {
            log.debug("connection will close (keep_alive=false)", .{});
        }

        if (response.head.status != .ok and response.head.status != .partial_content) {
            log.err("{s} failed, status: {}", .{ @tagName(method), response.head.status });
            var transfer_buf: [4096]u8 = undefined;
            var reader = response.reader(&transfer_buf);
            var body_buf: [4096]u8 = undefined;
            const n = reader.readSliceShort(&body_buf) catch 0;
            log.debug("error body: {s}", .{body_buf[0..n]});
            return if (method == .HEAD) error.S3HeadFailed else error.S3ReadFailed;
        }

        try callback(context, &response);
    }

    fn sizeImpl(ptr: *anyopaque) u64 {
        const self: *S3Source = @ptrCast(@alignCast(ptr));
        return self.object_size;
    }

    fn closeImpl(ptr: *anyopaque) void {
        const self: *S3Source = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    pub fn source(self: *S3Source) io.RandomAccessSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .readAt = readAtImpl,
                .size = sizeImpl,
                .close = closeImpl,
            },
        };
    }
};

test "S3Source init" {
    _ = S3Source;
}
