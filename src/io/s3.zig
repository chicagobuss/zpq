const std = @import("std");
const io = @import("interface.zig");
const transport = @import("transport.zig");
const protocol_s3 = @import("../protocol/s3.zig");
const protocol_http = @import("../protocol/http.zig");
const sigv4 = @import("../protocol/sigv4.zig");
const xev_mod = @import("xev");

pub fn AsyncS3SourceGen(comptime Xev: type) type {
    const Loop = *Xev.Loop;
    const Conn = transport.ConnectionGen(Xev);

    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        loop: Loop,
        s3: protocol_s3.S3,
        bucket: []const u8,
        key: []const u8,
        host: []const u8,

        conn: ?*Conn = null,
        resolver: transport.Resolver,

        /// Size of the object, fetched on init.
        content_length: u64,

        pub fn init(allocator: std.mem.Allocator, loop: Loop, thread_pool: *xev_mod.ThreadPool, s3_config: protocol_s3.S3, bucket: []const u8, key: []const u8) !Self {
            const host = try std.fmt.allocPrint(allocator, "{s}.s3.{s}.amazonaws.com", .{ bucket, s3_config.region });

            var self = Self{
                .allocator = allocator,
                .loop = loop,
                .s3 = s3_config,
                .bucket = bucket,
                .key = key,
                .host = host,
                .resolver = transport.Resolver.init(allocator, thread_pool),
                .content_length = 0,
            };

            // Fetch size via HEAD
            try self.fetchSize();
            return self;
        }

        pub fn deinit(self: *Self) void {
            if (self.conn) |c| c.deinit();
            self.allocator.free(self.host);
        }

        pub fn randomAccessSource(self: *Self) io.RandomAccessSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .readAt = readAt,
                    .size = size,
                    .close = close,
                },
            };
        }

        fn fetchSize(self: *Self) !void {
            var ctx = RequestContext(Xev){
                .source = self,
                .method = .HEAD,
                .allocator = self.allocator,
            };
            try ctx.run();
            if (ctx.status_code != 200) return error.S3HeadFailed;
            self.content_length = ctx.content_length;
        }

        fn readAt(ptr: *anyopaque, offset: u64, buf: []u8) anyerror!usize {
            var self: *Self = @ptrCast(@alignCast(ptr));
            const range_end = offset + buf.len - 1;

            var ctx = RequestContext(Xev){
                .source = self,
                .method = .GET,
                .allocator = self.allocator,
                .range_start = offset,
                .range_end = range_end,
                .output_buf = buf,
            };
            try ctx.run();

            if (ctx.status_code != 200 and ctx.status_code != 206) return error.S3GetFailed;
            return ctx.bytes_read;
        }

        fn size(ptr: *anyopaque) u64 {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return self.content_length;
        }

        fn close(ptr: *anyopaque) void {
            var self: *Self = @ptrCast(@alignCast(ptr));
            self.deinit();
            self.allocator.destroy(self);
        }
    };
}

fn RequestContext(comptime Xev: type) type {
    const Conn = transport.ConnectionGen(Xev);
    const S3Source = AsyncS3SourceGen(Xev);

    return struct {
        const Self = @This();

        source: *S3Source,
        method: enum { HEAD, GET },
        allocator: std.mem.Allocator,
        range_start: u64 = 0,
        range_end: u64 = 0,
        output_buf: []u8 = &[_]u8{},

        // Result state
        done: bool = false,
        status_code: u16 = 0,
        content_length: u64 = 0,
        bytes_read: usize = 0,
        err: ?anyerror = null,

        // Internal parser state
        parser: protocol_http.ResponseParser = .{},

        fn run(self: *Self) !void {
            self.parser = .{ .is_head = (self.method == .HEAD) };
            try self.source.resolver.resolve(self.source.loop, self.source.host, 443, self, @ptrCast(&onResolved));

            while (!self.done) {
                try self.source.loop.run(.once);
            }
            if (self.err) |e| return e;
        }

        fn onResolved(ptr: ?*anyopaque, addr: ?transport.Address) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const address = addr orelse {
                self.err = error.ResolutionFailed;
                self.done = true;
                return;
            };

            if (self.source.conn) |c| c.deinit();
            self.source.conn = Conn.init(self.allocator, self.source.loop, true, self.source.host) catch |e| {
                self.err = e;
                self.done = true;
                return;
            };
            const conn = self.source.conn.?;
            conn.callback_ctx = self;
            conn.on_data = onData;
            conn.on_error = onError;
            conn.on_handshake = onHandshake;

            conn.connect(address) catch |e| {
                self.err = e;
                self.done = true;
            };
        }

        fn onHandshake(ptr: ?*anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            self.sendRequest() catch |e| {
                self.err = e;
                self.done = true;
            };
        }

        fn sendRequest(self: *Self) !void {
            var req_buf = std.ArrayListUnmanaged(u8){};
            defer req_buf.deinit(self.allocator);

            // Construct path
            const path = try std.fmt.allocPrint(self.allocator, "/{s}", .{self.source.key});
            defer self.allocator.free(path);

            const signed_headers = switch (self.method) {
                .HEAD => try self.source.s3.formatHeadRequest(self.allocator, self.source.key, .{}),
                .GET => try self.source.s3.formatGetRequest(self.allocator, self.source.key, .{ .start = self.range_start, .end = self.range_end + 1 }, .{}),
            };
            defer {
                for (signed_headers) |h| {
                    self.allocator.free(h.name);
                    self.allocator.free(h.value);
                }
                self.allocator.free(signed_headers);
            }

            const method_str = switch (self.method) {
                .HEAD => "HEAD",
                .GET => "GET",
            };
            try req_buf.appendSlice(self.allocator, method_str);
            try req_buf.appendSlice(self.allocator, " ");
            try req_buf.appendSlice(self.allocator, path);
            try req_buf.appendSlice(self.allocator, " HTTP/1.1\r\n");
            for (signed_headers) |h| {
                try req_buf.appendSlice(self.allocator, h.name);
                try req_buf.appendSlice(self.allocator, ": ");
                try req_buf.appendSlice(self.allocator, h.value);
                try req_buf.appendSlice(self.allocator, "\r\n");
            }
            try req_buf.appendSlice(self.allocator, "Connection: keep-alive\r\n\r\n");

            if (self.source.conn) |conn| {
                try conn.write(req_buf.items);
            }
        }

        fn onData(ptr: ?*anyopaque, data: []const u8) anyerror!void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const BodyCtx = struct {
                ctx: *Self,
                fn onBody(ctx_ptr: *anyopaque, chunk: []const u8) anyerror!void {
                    const bctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
                    const me = bctx.ctx;

                    if (me.method == .HEAD) return;

                    if (me.bytes_read + chunk.len > me.output_buf.len) {
                        return error.BufferOverflow;
                    }
                    @memcpy(me.output_buf[me.bytes_read .. me.bytes_read + chunk.len], chunk);
                    me.bytes_read += chunk.len;
                }
            };
            var bctx = BodyCtx{ .ctx = self };

            try self.parser.feed(data, &bctx, BodyCtx.onBody);

            if (self.parser.state == .done) {
                self.status_code = self.parser.status_code;
                if (self.parser.content_length) |cl| {
                    self.content_length = @intCast(cl);
                }
                // Stop the connection from scheduling more reads
                if (self.source.conn) |conn| {
                    conn.stopped = true;
                }
                self.done = true;
            }
        }

        fn onError(ptr: ?*anyopaque, err: anyerror) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            // If we already have a complete response, ignore connection close errors
            if (self.done or self.parser.state == .done) return;
            self.err = err;
            self.done = true;
        }
    };
}
