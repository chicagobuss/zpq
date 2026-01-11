const std = @import("std");

/// Minimal incremental HTTP/1.1 response parser.
/// Designed for Sans-I/O: it does not perform any syscalls.
/// It works purely on memory buffers provided by the transport layer.
pub const ResponseParser = struct {
    const Self = @This();

    pub const State = enum {
        reading_headers,
        reading_body,
        reading_chunk_size,
        reading_chunk_data,
        reading_chunk_trailer,
        done,
        parse_error,
    };

    state: State = .reading_headers,
    is_head: bool = false,

    // Header buffer (bounded to prevent memory exhaustion)
    header_buf: [16 * 1024]u8 = undefined,
    header_len: usize = 0,

    status_code: u16 = 0,
    content_length: ?usize = null,
    is_chunked: bool = false,
    etag: ?[]const u8 = null,

    body_read: usize = 0,

    // Chunked encoding state
    current_chunk_size: usize = 0,
    chunk_bytes_read: usize = 0,
    chunk_size_buf: [32]u8 = undefined,
    chunk_size_len: usize = 0,

    pub fn reset(self: *Self) void {
        const is_head = self.is_head;
        self.* = .{};
        self.is_head = is_head;
    }

    /// Feed a chunk of raw bytes from the network into the parser.
    /// body_ctx and on_body are used to "lend" decrypted body chunks to the consumer (e.g. Parquet decoder).
    pub fn feed(
        self: *Self,
        bytes: []const u8,
        body_ctx: *anyopaque,
        on_body: *const fn (ctx: *anyopaque, chunk: []const u8) anyerror!void,
    ) !void {
        var i: usize = 0;
        while (i < bytes.len) {
            switch (self.state) {
                .reading_headers => {
                    const remaining = self.header_buf.len - self.header_len;
                    if (remaining == 0) return error.HttpHeadersTooLarge;
                    const to_copy = @min(remaining, bytes.len - i);
                    @memcpy(self.header_buf[self.header_len .. self.header_len + to_copy], bytes[i .. i + to_copy]);
                    self.header_len += to_copy;
                    i += to_copy;

                    if (std.mem.indexOf(u8, self.header_buf[0..self.header_len], "\r\n\r\n")) |pos| {
                        const headers_slice = self.header_buf[0 .. pos + 4];
                        try self.parseHeaders(headers_slice);

                        if (self.is_head) {
                            self.state = .done;
                            return;
                        }

                        if (self.is_chunked) {
                            self.state = .reading_chunk_size;
                        } else {
                            self.state = .reading_body;
                            if (self.content_length) |len| {
                                if (len == 0) {
                                    self.state = .done;
                                    return;
                                }
                            }
                        }

                        const body_start = pos + 4;
                        if (body_start < self.header_len) {
                            const extra = self.header_buf[body_start..self.header_len];
                            if (self.is_chunked) {
                                _ = try self.consumeChunked(extra, body_ctx, on_body);
                            } else {
                                try self.consumeBody(extra, body_ctx, on_body);
                            }
                        }
                    }
                },
                .reading_body => {
                    try self.consumeBody(bytes[i..], body_ctx, on_body);
                    i = bytes.len;
                },
                .reading_chunk_size, .reading_chunk_data, .reading_chunk_trailer => {
                    const consumed = try self.consumeChunked(bytes[i..], body_ctx, on_body);
                    i += consumed;
                },
                .done => return,
                .parse_error => return error.HttpParseFailed,
            }
        }
    }

    fn consumeBody(
        self: *Self,
        bytes: []const u8,
        body_ctx: *anyopaque,
        on_body: *const fn (ctx: *anyopaque, chunk: []const u8) anyerror!void,
    ) !void {
        const want = self.content_length orelse return error.MissingContentLength;
        if (self.body_read >= want) {
            self.state = .done;
            return;
        }

        const remaining = want - self.body_read;
        const take = @min(remaining, bytes.len);
        if (take > 0) {
            try on_body(body_ctx, bytes[0..take]);
            self.body_read += take;
        }
        if (self.body_read == want) self.state = .done;
    }

    fn consumeChunked(
        self: *Self,
        bytes: []const u8,
        body_ctx: *anyopaque,
        on_body: *const fn (ctx: *anyopaque, chunk: []const u8) anyerror!void,
    ) !usize {
        var i: usize = 0;
        while (i < bytes.len and self.state != .done) {
            switch (self.state) {
                .reading_chunk_size => {
                    while (i < bytes.len) {
                        const b = bytes[i];
                        i += 1;
                        if (b == '\r') continue;
                        if (b == '\n') {
                            const size_str = self.chunk_size_buf[0..self.chunk_size_len];
                            const hex_end = std.mem.indexOfScalar(u8, size_str, ';') orelse size_str.len;
                            const hex_str = std.mem.trim(u8, size_str[0..hex_end], " \t");
                            if (hex_str.len == 0) return error.InvalidChunkSize;
                            self.current_chunk_size = try std.fmt.parseInt(usize, hex_str, 16);
                            self.chunk_bytes_read = 0;
                            self.chunk_size_len = 0;
                            self.state = if (self.current_chunk_size == 0) .reading_chunk_trailer else .reading_chunk_data;
                            break;
                        } else {
                            if (self.chunk_size_len >= self.chunk_size_buf.len) return error.ChunkSizeTooLarge;
                            self.chunk_size_buf[self.chunk_size_len] = b;
                            self.chunk_size_len += 1;
                        }
                    }
                },
                .reading_chunk_data => {
                    const remaining = self.current_chunk_size - self.chunk_bytes_read;
                    const available = bytes.len - i;
                    const take = @min(remaining, available);
                    if (take > 0) {
                        try on_body(body_ctx, bytes[i .. i + take]);
                        self.body_read += take;
                        self.chunk_bytes_read += take;
                        i += take;
                    }
                    if (self.chunk_bytes_read == self.current_chunk_size) self.state = .reading_chunk_trailer;
                },
                .reading_chunk_trailer => {
                    while (i < bytes.len) {
                        if (bytes[i] == '\n') {
                            i += 1;
                            self.state = if (self.current_chunk_size == 0) .done else .reading_chunk_size;
                            break;
                        }
                        i += 1;
                    }
                },
                else => break,
            }
        }
        return i;
    }

    fn parseHeaders(self: *Self, headers: []const u8) !void {
        const first_crlf = std.mem.indexOf(u8, headers, "\r\n") orelse return error.BadStatusLine;
        const status_line = headers[0..first_crlf];
        const sp1 = std.mem.indexOfScalar(u8, status_line, ' ') orelse return error.BadStatusLine;
        const rest = status_line[sp1 + 1 ..];
        const sp2 = std.mem.indexOfScalar(u8, rest, ' ') orelse rest.len;
        self.status_code = try std.fmt.parseInt(u16, rest[0..sp2], 10);

        var it = std.mem.splitSequence(u8, headers[first_crlf + 2 ..], "\r\n");
        while (it.next()) |line| {
            if (line.len == 0) break;
            if (std.ascii.startsWithIgnoreCase(line, "content-length:")) {
                const v = std.mem.trim(u8, line["content-length:".len..], " \t");
                self.content_length = try std.fmt.parseInt(usize, v, 10);
            }
            if (std.ascii.startsWithIgnoreCase(line, "transfer-encoding:")) {
                const v = std.mem.trim(u8, line["transfer-encoding:".len..], " \t");
                if (std.ascii.eqlIgnoreCase(v, "chunked")) self.is_chunked = true;
            }
            if (std.ascii.startsWithIgnoreCase(line, "etag:")) {
                self.etag = std.mem.trim(u8, line["etag:".len..], " \t\"");
            }
        }
    }
};

/// Formats an HTTP/1.1 request into a writer.
/// This is a generic formatter that works with any headers, including signed ones.
pub fn formatRequest(
    writer: anytype,
    method: []const u8,
    path: []const u8,
    headers: []const sigv4.SigV4.Header,
) !void {
    try writer.print("{s} {s} HTTP/1.1\r\n", .{ method, path });
    for (headers) |h| {
        try writer.print("{s}: {s}\r\n", .{ h.name, h.value });
    }
    try writer.writeAll("\r\n");
}

const sigv4 = @import("sigv4.zig");

test "ResponseParser - basic content-length" {
    const testing = std.testing;
    var parser = ResponseParser{};

    const raw_response = "HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\nHello World";

    var body_accum = std.ArrayList(u8).init(testing.allocator);
    defer body_accum.deinit();

    const BodyCtx = struct {
        accum: *std.ArrayList(u8),
        fn onBody(ctx: *anyopaque, chunk: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try self.accum.appendSlice(chunk);
        }
    };
    var ctx = BodyCtx{ .accum = &body_accum };

    try parser.feed(raw_response, &ctx, BodyCtx.onBody);

    try testing.expectEqual(@as(u16, 200), parser.status_code);
    try testing.expectEqual(@as(usize, 11), parser.content_length.?);
    try testing.expectEqualStrings("Hello World", body_accum.items);
    try testing.expect(parser.state == .done);
}

test "ResponseParser - chunked encoding" {
    const testing = std.testing;
    var parser = ResponseParser{};

    const chunked_data =
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" ++
        "5\r\nHello\r\n" ++
        "6\r\n World\r\n" ++
        "0\r\n\r\n";

    var body_accum = std.ArrayList(u8).init(testing.allocator);
    defer body_accum.deinit();

    const BodyCtx = struct {
        accum: *std.ArrayList(u8),
        fn onBody(ctx: *anyopaque, chunk: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try self.accum.appendSlice(chunk);
        }
    };
    var ctx = BodyCtx{ .accum = &body_accum };

    // Feed it in tiny pieces to test incremental parsing
    for (chunked_data) |c| {
        try parser.feed(&[_]u8{c}, &ctx, BodyCtx.onBody);
    }

    try testing.expectEqual(@as(u16, 200), parser.status_code);
    try testing.expect(parser.is_chunked);
    try testing.expectEqualStrings("Hello World", body_accum.items);
    try testing.expect(parser.state == .done);
}
