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
        s3: protocol_s3.S3,
        bucket: []const u8,
        key: []const u8,
        host: []const u8,
        resolved_addr: ?transport.Address = null,
        conn: ?*Conn = null,

        pub fn init(allocator: std.mem.Allocator, loop: Loop, s3_config: protocol_s3.S3, bucket: []const u8, key: []const u8) !Self {
            const host = try std.fmt.allocPrint(allocator, "{s}.s3.{s}.amazonaws.com", .{ bucket, s3_config.region });
            return .{
                .allocator = allocator,
                .loop = loop,
                .s3 = s3_config,
                .bucket = bucket,
                .key = key,
                .host = host,
            };
        }

        pub fn deinit(self: *Self) void {
            if (self.conn) |c| c.deinit();
            self.allocator.free(self.host);
        }

        pub fn setAddress(self: *Self, addr: transport.Address) void {
            self.resolved_addr = addr;
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
                if (ctx.conn) |c| {
                    if (c.closed) return error.ConnectionClosedUnexpectedly;
                }
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
            final_payload: []const u8 = undefined,
            owned_payload: ?[]const u8 = null,
            signed_headers: []sigv4.SigV4.Header = undefined,
    
            fn start(self: *RequestContext, payload: []const u8, options: protocol_s3.S3.Options) !void {
                const addr = self.writer.resolved_addr orelse return error.AddressNotResolved;
    
                // If we have an owned_payload, use it. Otherwise use the passed payload.
                const effective_payload = self.owned_payload orelse payload;

                // Each request gets its own connection for true parallelism
                self.conn = try Conn.init(self.writer.allocator, self.writer.loop, true, self.writer.host);
                const conn = self.conn.?;
                conn.callback_ctx = self;
                conn.on_data = onData;
                conn.on_error = onError;
                conn.on_handshake = onHandshake;

                // Sign headers
                const signed = try self.sign(effective_payload, options);
                // Note: signed headers and body memory must stay alive!
                // We'll attach them to the context.
                self.signed_headers = signed.headers;
                self.final_payload = signed.body orelse effective_payload;

                try conn.connect(addr);
            }

            pub fn deinit(self: *RequestContext) void {
                if (self.conn) |c| c.deinit();
                
                // Clean up signed headers
                for (self.signed_headers) |h| {
                    self.writer.allocator.free(h.name);
                    self.writer.allocator.free(h.value);
                }
                self.writer.allocator.free(self.signed_headers);
                // Note: final_payload might be the original buffer, 
                // but if it's signed.body, we should free it IF it was allocated.
                // In CompleteMultipart case, formatCompleteMultipartRequest returns an owned body.
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
                std.debug.print("[S3Writer] Handshake/Connect done (Part {d})\n", .{self.part_number});
                self.sendRequest() catch |e| {
                    self.err = e;
                    self.done = true;
                };
            }

            fn sendRequest(self: *RequestContext) !void {
                var req = std.ArrayListUnmanaged(u8){};
                defer req.deinit(self.writer.allocator);

                const m_str = @tagName(self.method);
                try req.appendSlice(self.writer.allocator, m_str);
                try req.appendSlice(self.writer.allocator, " /");
                try req.appendSlice(self.writer.allocator, self.writer.key);
                
                if (self.method == .POST) {
                    if (self.is_complete) {
                        var buf: [128]u8 = undefined;
                        const s = try std.fmt.bufPrint(&buf, "?uploadId={s}", .{self.upload_id.?});
                        try req.appendSlice(self.writer.allocator, s);
                    } else if (std.mem.eql(u8, self.query orelse "", "uploads")) {
                         try req.appendSlice(self.writer.allocator, "?uploads");
                    }
                } else if (self.method == .PUT and self.upload_id != null) {
                    var buf: [256]u8 = undefined;
                    const s = try std.fmt.bufPrint(&buf, "?partNumber={d}&uploadId={s}", .{ self.part_number, self.upload_id.? });
                    try req.appendSlice(self.writer.allocator, s);
                }

                try req.appendSlice(self.writer.allocator, " HTTP/1.1\r\n");
                for (self.signed_headers) |h| {
                    try req.appendSlice(self.writer.allocator, h.name);
                    try req.appendSlice(self.writer.allocator, ": ");
                    try req.appendSlice(self.writer.allocator, h.value);
                    try req.appendSlice(self.writer.allocator, "\r\n");
                }
                
                var len_buf: [64]u8 = undefined;
                const len_s = try std.fmt.bufPrint(&len_buf, "Content-Length: {d}\r\n\r\n", .{self.final_payload.len});
                try req.appendSlice(self.writer.allocator, len_s);
                try req.appendSlice(self.writer.allocator, self.final_payload);

                std.debug.print("[S3Writer] Sending request ({d} bytes, Part {d})\n", .{req.items.len, self.part_number});
                try self.conn.?.write(req.items);
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
                    // Extract ETag if present
                    if (self.parser.etag) |e| {
                        self.etag = try self.writer.allocator.dupe(u8, e);
                    }
                    self.done = true;
                    if (self.status_code >= 400) {
                         std.debug.print("[S3Writer] Request failed: status={d} body={s}\n", .{self.status_code, self.body.items});
                    } else {
                         std.debug.print("[S3Writer] Request completed: status={d}\n", .{self.status_code});
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
