const std = @import("std");
const transport = @import("../transport.zig");
const protocol_s3 = @import("../../protocol/s3.zig");
const protocol_http = @import("../../protocol/http.zig");
const sigv4 = @import("../../protocol/sigv4.zig");
const xev_mod = @import("xev");

pub fn AsyncS3SinkWriterGen(comptime Xev: type) type {
    const Loop = *Xev.Loop;
    const Conn = transport.ConnectionGen(Xev);

    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        loop: Loop,
        host: []const u8,
        s3: protocol_s3.S3,
        bucket: []const u8,
        key: []const u8,
        pool: *transport.ConnectionPoolGen(Xev),

        pub fn init(allocator: std.mem.Allocator, loop: Loop, resolver: transport.Resolver, s3_config: protocol_s3.S3, bucket: []const u8, key: []const u8) !Self {
            const host = try std.fmt.allocPrint(allocator, "{s}.s3.{s}.amazonaws.com", .{ bucket, s3_config.region });
            const pool = try transport.ConnectionPoolGen(Xev).init(allocator, loop, resolver, host, 443, true);
            return .{
                .allocator = allocator,
                .loop = loop,
                .s3 = s3_config,
                .bucket = bucket,
                .key = key,
                .host = host,
                .pool = pool,
            };
        }

        pub fn deinit(self: *Self) void {
            self.pool.deinit();
            self.allocator.free(self.host);
        }

        pub fn setAddress(self: *Self, addr: transport.Address) void {
            _ = self;
            _ = addr; // No-op, managed by pool now
        }

        pub fn initiateMultipartUpload(self: *Self) ![]const u8 {
            var ctx = RequestContext{
                .writer = self,
                .method = .POST,
                .query = "uploads",
                .body = .{},
            };
            defer ctx.deinit();
            
            try ctx.start("", .{});
            try self.drive(&ctx);

            if (ctx.status_code != 200) {
                std.debug.print("initiateMultipartUpload failed: status={d} body={s}\n", .{ ctx.status_code, ctx.body.items });
                return error.S3InitiateFailed;
            }
            return protocol_s3.S3.parseUploadId(self.allocator, ctx.body.items);
        }

        pub fn uploadPartAsync(self: *Self, upload_id: []const u8, part_number: u32, data: []const u8) !*RequestContext {
            const owned_data = try self.allocator.dupe(u8, data);
            errdefer self.allocator.free(owned_data);

            var ctx = try self.allocator.create(RequestContext);
            ctx.* = .{
                .writer = self,
                .method = .PUT,
                // Query string for uploadPart: partNumber=X&uploadId=Y
                .upload_id = upload_id,
                .part_number = part_number,
                .owned_payload = owned_data,
                .body = .{},
            };
            
            try ctx.start("", .{});
            return ctx;
        }

        pub fn uploadPart(self: *Self, upload_id: []const u8, part_number: u32, data: []const u8) ![]const u8 {
            var ctx = RequestContext{
                .writer = self,
                .method = .PUT,
                // Query string for uploadPart: partNumber=X&uploadId=Y
                .upload_id = upload_id,
                .part_number = part_number,
                .body = .{},
            };
            defer ctx.deinit();

            try ctx.start(data, .{});
            try self.drive(&ctx);

            if (ctx.status_code != 200) return error.S3UploadPartFailed;
            return try self.allocator.dupe(u8, ctx.etag orelse return error.MissingETag);
        }

        pub fn putObject(self: *Self, data: []const u8) !void {
            var ctx = RequestContext{
                .writer = self,
                .method = .PUT,
                .body = .{},
            };
            defer ctx.deinit();

            try ctx.start(data, .{});
            try self.drive(&ctx);

            if (ctx.status_code != 200) return error.S3PutFailed;
        }

        pub fn completeMultipartUpload(self: *Self, upload_id: []const u8, parts: []const protocol_s3.S3.Part) !void {
            var ctx = RequestContext{
                .writer = self,
                .method = .POST,
                .upload_id = upload_id,
                .is_complete = true,
                .body = .{},
                .parts = parts,
            };
            defer ctx.deinit();

            try ctx.start("", .{});
            try self.drive(&ctx);

            if (ctx.status_code != 200) return error.S3CompleteFailed;
        }

        fn drive(self: *Self, ctx: *RequestContext) !void {
            while (!ctx.done) {
                try self.loop.run(.once);
            }
            if (ctx.err) |e| return e;
        }

        pub const RequestContext = struct {
            writer: *Self,
            method: enum { POST, PUT },
            query: ?[]const u8 = null,
            upload_id: ?[]const u8 = null,
            part_number: u32 = 0,
            is_complete: bool = false,
            parts: []const protocol_s3.S3.Part = &.{},
    
            status_code: u16 = 0,
            etag: ?[]const u8 = null,
            body: std.ArrayListUnmanaged(u8),
            done: bool = false,
            err: ?anyerror = null,
            parser: protocol_http.ResponseParser = .{},
            
            conn: ?*Conn = null,
            final_payload: []const u8 = "",
            owned_payload: ?[]const u8 = null,
            signed_headers: []sigv4.SigV4.Header = &.{},
    
            fn start(self: *RequestContext, payload: []const u8, options: protocol_s3.S3.Options) !void {
                // If we have an owned_payload, use it. Otherwise use the passed payload.
                const effective_payload = self.owned_payload orelse payload;

                // Sign headers
                const signed = try self.sign(effective_payload, options);
                self.signed_headers = signed.headers;
                self.final_payload = signed.body orelse effective_payload;

                try self.writer.pool.dispatch(.{
                    .ptr = self,
                    .on_data = onData,
                    .on_error = onError,
                    .on_handshake = onHandshake,
                    .start_fn = startFn,
                });
            }

            fn startFn(ptr: *anyopaque, conn: *transport.ConnectionGen(Xev)) anyerror!void {
                const self: *RequestContext = @ptrCast(@alignCast(ptr));
                self.conn = conn;
                try self.sendRequest();
            }

            pub fn deinit(self: *RequestContext) void {
                // Pool manages connection, we don't deinit it here.
                
                // Clean up signed headers
                for (self.signed_headers) |h| {
                    self.writer.allocator.free(h.name);
                    self.writer.allocator.free(h.value);
                }
                self.writer.allocator.free(self.signed_headers);
                if (self.is_complete) {
                     self.writer.allocator.free(self.final_payload);
                }

                if (self.owned_payload) |p| self.writer.allocator.free(p);
                self.body.deinit(self.writer.allocator);
                if (self.etag) |e| self.writer.allocator.free(e);
            }

            fn sign(self: *RequestContext, payload: []const u8, options: protocol_s3.S3.Options) !struct { headers: []sigv4.SigV4.Header, body: ?[]const u8 } {
                if (self.method == .POST) {
                    if (self.is_complete) {
                        const res = try self.writer.s3.formatCompleteMultipartRequest(self.writer.allocator, self.writer.key, self.upload_id.?, self.parts, options);
                        return .{ .headers = res.headers, .body = res.body };
                    } else if (std.mem.eql(u8, self.query orelse "", "uploads")) {
                        const h = try self.writer.s3.formatInitiateMultipartRequest(self.writer.allocator, self.writer.key, options);
                        return .{ .headers = h, .body = null };
                    }
                } else if (self.method == .PUT) {
                    if (self.upload_id) |uid| {
                        const h = try self.writer.s3.formatUploadPartRequest(self.writer.allocator, self.writer.key, uid, self.part_number, payload, options);
                        return .{ .headers = h, .body = null };
                    } else {
                        const h = try self.writer.s3.formatPutRequest(self.writer.allocator, self.writer.key, payload, options);
                        return .{ .headers = h, .body = null };
                    }
                }
                return error.UnsupportedMethod;
            }

            fn onHandshake(ptr: ?*anyopaque) void {
                const self: *RequestContext = @ptrCast(@alignCast(ptr));
                // std.debug.print("[S3Writer] Handshake/Connect done (Part {d})\n", .{self.part_number});
                self.sendRequest() catch |e| {
                    self.err = e;
                    self.done = true;
                };
            }

            fn sendRequest(self: *RequestContext) !void {
                var header_buf = std.ArrayListUnmanaged(u8){};
                defer header_buf.deinit(self.writer.allocator);

                const m_str = @tagName(self.method);
                try header_buf.appendSlice(self.writer.allocator, m_str);
                try header_buf.appendSlice(self.writer.allocator, " /");
                try header_buf.appendSlice(self.writer.allocator, self.writer.key);
                
                if (self.method == .POST) {
                    if (self.is_complete) {
                        var buf: [512]u8 = undefined;
                        const s = try std.fmt.bufPrint(&buf, "?uploadId={s}", .{self.upload_id.?});
                        try header_buf.appendSlice(self.writer.allocator, s);
                    } else if (std.mem.eql(u8, self.query orelse "", "uploads")) {
                         try header_buf.appendSlice(self.writer.allocator, "?uploads");
                    }
                } else if (self.method == .PUT and self.upload_id != null) {
                    var buf: [512]u8 = undefined;
                    const s = try std.fmt.bufPrint(&buf, "?partNumber={d}&uploadId={s}", .{ self.part_number, self.upload_id.? });
                    try header_buf.appendSlice(self.writer.allocator, s);
                }

                try header_buf.appendSlice(self.writer.allocator, " HTTP/1.1\r\n");
                for (self.signed_headers) |h| {
                    try header_buf.appendSlice(self.writer.allocator, h.name);
                    try header_buf.appendSlice(self.writer.allocator, ": ");
                    try header_buf.appendSlice(self.writer.allocator, h.value);
                    try header_buf.appendSlice(self.writer.allocator, "\r\n");
                }
                
                var len_buf: [64]u8 = undefined;
                const len_s = try std.fmt.bufPrint(&len_buf, "Content-Length: {d}\r\n\r\n", .{self.final_payload.len});
                try header_buf.appendSlice(self.writer.allocator, len_s);

                const conn = self.conn.?;
                try conn.write(header_buf.items);
                if (self.final_payload.len > 0) {
                    try conn.writeNoCopy(self.final_payload);
                }
            }

            fn onData(ptr: ?*anyopaque, data: []const u8) anyerror!void {
                const self: *RequestContext = @ptrCast(@alignCast(ptr));
                // std.debug.print("[S3Writer] Received {d} bytes (Part {d})\n", .{data.len, self.part_number});
                
                const BodyCtx = struct {
                    ctx: *RequestContext,
                    fn onBody(ctx_ptr: *anyopaque, chunk: []const u8) anyerror!void {
                        const bctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
                        try bctx.ctx.body.appendSlice(bctx.ctx.writer.allocator, chunk);
                    }
                };
                var bctx = BodyCtx{ .ctx = self };
                try self.parser.feed(data, &bctx, BodyCtx.onBody);

                if (self.parser.state == .done) {
                    if (self.conn) |c| c.stop();
                    self.status_code = self.parser.status_code;
                    if (self.parser.etag) |e| {
                        self.etag = try self.writer.allocator.dupe(u8, e);
                    }
                    
                    // Mark connection as idle for pool reuse
                    if (self.conn) |c| {
                        const pc: *transport.ConnectionPoolGen(Xev).PooledConnection = @ptrCast(@alignCast(c.callback_ctx));
                        pc.markIdle();
                    }
                    
                    if (self.method == .PUT and self.status_code == 200) {
                        self.writer.pool.reportProgress(self.final_payload.len);
                    }

                    self.done = true;
                    if (self.status_code >= 400) {
                         std.debug.print("[S3Writer] Request failed: status={d} body={s}\n", .{self.status_code, self.body.items});
                    }
                }
            }

            fn onError(ptr: ?*anyopaque, err: anyerror) void {
                const self: *RequestContext = @ptrCast(@alignCast(ptr));
                if (self.done) return;
                self.err = err;
                self.done = true;
            }
        };
    };
}
