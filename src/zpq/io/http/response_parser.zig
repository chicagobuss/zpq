const std = @import("std");

/// Minimal incremental HTTP/1.1 response parser.
/// - Parses status line + headers until CRLFCRLF
/// - Extracts status code and Content-Length
/// - Then streams exactly Content-Length bytes as body
/// - Does NOT support chunked encoding yet (fine for MinIO range tests)
pub const ResponseParser = struct {
    const Self = @This();

    pub const State = enum {
        reading_headers,
        reading_body,
        done,
        parse_error,
    };

    state: State = .reading_headers,

    // Header buffer (grows until header_end, bounded)
    header_buf: [16 * 1024]u8 = undefined,
    header_len: usize = 0,

    status_code: u16 = 0,
    content_length: ?usize = null,

    body_read: usize = 0,

    pub fn headersComplete(self: Self) bool {
        return self.state != .reading_headers;
    }

    pub fn reset(self: *Self) void {
        self.* = .{};
    }

    pub fn feed(
        self: *Self,
        bytes: []const u8,
        body_ctx: *anyopaque,
        on_body: *const fn (ctx: *anyopaque, chunk: []const u8) void,
    ) !void {
        var i: usize = 0;
        while (i < bytes.len) {
            switch (self.state) {
                .reading_headers => {
                    // Append into header buffer
                    const remaining = self.header_buf.len - self.header_len;
                    if (remaining == 0) return error.HttpHeadersTooLarge;
                    const to_copy = @min(remaining, bytes.len - i);
                    @memcpy(self.header_buf[self.header_len .. self.header_len + to_copy], bytes[i .. i + to_copy]);
                    self.header_len += to_copy;
                    i += to_copy;

                    if (std.mem.indexOf(u8, self.header_buf[0..self.header_len], "\r\n\r\n")) |pos| {
                        const headers = self.header_buf[0 .. pos + 4];
                        try self.parseHeaders(headers);
                        self.state = .reading_body;

                        // Check for immediate completion (Content-Length: 0)
                        if (self.content_length) |len| {
                            if (len == 0) {
                                self.state = .done;
                                return;
                            }
                        }

                        // Any extra bytes after the header terminator belong to body.
                        const body_start = pos + 4;
                        if (body_start < self.header_len) {
                            const extra = self.header_buf[body_start..self.header_len];
                            try self.consumeBody(extra, body_ctx, on_body);
                        }
                    }
                },
                .reading_body => {
                    const remaining = bytes[i..];
                    try self.consumeBody(remaining, body_ctx, on_body);
                    i = bytes.len;
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
        on_body: *const fn (ctx: *anyopaque, chunk: []const u8) void,
    ) !void {
        const want = self.content_length orelse return error.MissingContentLength;
        if (self.body_read >= want) {
            self.state = .done;
            return;
        }

        const remaining = want - self.body_read;
        const take = @min(remaining, bytes.len);
        if (take > 0) {
            on_body(body_ctx, bytes[0..take]);
            self.body_read += take;
        }
        if (self.body_read == want) {
            self.state = .done;
        }
    }

    fn parseHeaders(self: *Self, headers: []const u8) !void {
        // Status line: HTTP/1.1 200 OK
        const first_crlf = std.mem.indexOf(u8, headers, "\r\n") orelse return error.BadStatusLine;
        const status_line = headers[0..first_crlf];

        // Find first space, then parse status code token
        const sp1 = std.mem.indexOfScalar(u8, status_line, ' ') orelse return error.BadStatusLine;
        var rest = status_line[sp1 + 1 ..];
        const sp2 = std.mem.indexOfScalar(u8, rest, ' ') orelse rest.len;
        const code_str = rest[0..sp2];
        self.status_code = try std.fmt.parseInt(u16, code_str, 10);

        // Headers: find Content-Length (case-insensitive)
        var it = std.mem.splitSequence(u8, headers[first_crlf + 2 ..], "\r\n");
        while (it.next()) |line| {
            if (line.len == 0) break;
            if (std.ascii.startsWithIgnoreCase(line, "Content-Length:")) {
                const v = std.mem.trim(u8, line["Content-Length:".len..], " \t");
                self.content_length = try std.fmt.parseInt(usize, v, 10);
            }
        }
    }
};
