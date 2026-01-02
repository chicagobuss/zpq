const std = @import("std");

/// Minimal incremental HTTP/1.1 response parser.
/// - Parses status line + headers until CRLFCRLF
/// - Extracts status code and Content-Length
/// - Supports both Content-Length and chunked Transfer-Encoding
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

    // Header buffer (grows until header_end, bounded)
    header_buf: [16 * 1024]u8 = undefined,
    header_len: usize = 0,

    status_code: u16 = 0,
    content_length: ?usize = null,
    is_chunked: bool = false,

    body_read: usize = 0,

    // Chunked encoding state
    current_chunk_size: usize = 0,
    chunk_bytes_read: usize = 0,
    chunk_size_buf: [32]u8 = undefined,
    chunk_size_len: usize = 0,

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

                        if (self.is_chunked) {
                            self.state = .reading_chunk_size;
                        } else {
                            self.state = .reading_body;
                            // Check for immediate completion (Content-Length: 0)
                            if (self.content_length) |len| {
                                if (len == 0) {
                                    self.state = .done;
                                    return;
                                }
                            }
                        }

                        // Any extra bytes after the header terminator belong to body.
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
                    const remaining = bytes[i..];
                    try self.consumeBody(remaining, body_ctx, on_body);
                    i = bytes.len;
                },
                .reading_chunk_size, .reading_chunk_data, .reading_chunk_trailer => {
                    const remaining = bytes[i..];
                    const consumed = try self.consumeChunked(remaining, body_ctx, on_body);
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
            std.log.scoped(.http_parser).debug("consumeBody: reached content-length {d}, state -> done", .{want});
            self.state = .done;
        }
    }

    /// Consume chunked transfer-encoded data. Returns number of bytes consumed.
    fn consumeChunked(
        self: *Self,
        bytes: []const u8,
        body_ctx: *anyopaque,
        on_body: *const fn (ctx: *anyopaque, chunk: []const u8) void,
    ) !usize {
        var i: usize = 0;

        while (i < bytes.len and self.state != .done) {
            switch (self.state) {
                .reading_chunk_size => {
                    // Read chunk size line (hex number followed by CRLF)
                    while (i < bytes.len) {
                        const b = bytes[i];
                        i += 1;

                        if (b == '\r') {
                            // Expect LF next
                            continue;
                        } else if (b == '\n') {
                            // Parse the hex size
                            const size_str = self.chunk_size_buf[0..self.chunk_size_len];
                            // Strip any chunk extensions (after semicolon)
                            const hex_end = std.mem.indexOfScalar(u8, size_str, ';') orelse size_str.len;
                            const hex_str = std.mem.trim(u8, size_str[0..hex_end], " \t");

                            if (hex_str.len == 0) {
                                return error.InvalidChunkSize;
                            }

                            self.current_chunk_size = std.fmt.parseInt(usize, hex_str, 16) catch {
                                return error.InvalidChunkSize;
                            };
                            self.chunk_bytes_read = 0;
                            self.chunk_size_len = 0;

                            if (self.current_chunk_size == 0) {
                                // Final chunk - read trailing CRLF
                                self.state = .reading_chunk_trailer;
                            } else {
                                self.state = .reading_chunk_data;
                            }
                            break;
                        } else {
                            // Accumulate hex digit
                            if (self.chunk_size_len >= self.chunk_size_buf.len) {
                                return error.ChunkSizeTooLarge;
                            }
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
                        on_body(body_ctx, bytes[i .. i + take]);
                        self.body_read += take;
                        self.chunk_bytes_read += take;
                        i += take;
                    }

                    if (self.chunk_bytes_read == self.current_chunk_size) {
                        // Chunk data complete, expect CRLF then next chunk size
                        self.state = .reading_chunk_trailer;
                    }
                },
                .reading_chunk_trailer => {
                    // Skip trailing CRLF after chunk data (or final chunk)
                    while (i < bytes.len) {
                        const b = bytes[i];
                        i += 1;

                        if (b == '\n') {
                            if (self.current_chunk_size == 0) {
                                // Was the final chunk
                                self.state = .done;
                            } else {
                                // More chunks to come
                                self.state = .reading_chunk_size;
                            }
                            break;
                        }
                        // Skip \r
                    }
                },
                else => break,
            }
        }

        return i;
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

        // Headers: find Content-Length and Transfer-Encoding (case-insensitive)
        var it = std.mem.splitSequence(u8, headers[first_crlf + 2 ..], "\r\n");
        while (it.next()) |line| {
            if (line.len == 0) break;

            // Check for Content-Length (case-insensitive)
            if (std.ascii.startsWithIgnoreCase(line, "content-length:")) {
                const v = std.mem.trim(u8, line["content-length:".len..], " \t");
                self.content_length = try std.fmt.parseInt(usize, v, 10);
            }

            // Check for Transfer-Encoding: chunked (case-insensitive)
            if (std.ascii.startsWithIgnoreCase(line, "transfer-encoding:")) {
                const v = std.mem.trim(u8, line["transfer-encoding:".len..], " \t");
                if (std.ascii.eqlIgnoreCase(v, "chunked")) {
                    self.is_chunked = true;
                }
            }
        }
    }
};
