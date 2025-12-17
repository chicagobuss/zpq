const std = @import("std");
const TlsAdapter = @import("tls_adapter.zig").TlsAdapter;

/// States for the HTTP Request Lifecycle
pub const State = enum {
    Idle,
    Connecting,
    SendingRequest,
    ReadingHeaders,
    ReadingBody,
    Finished,
    Error,
};

/// A single Async HTTP Request State Machine.
/// It does NOT own the socket or the loop; it just manages the logic.
pub const AsyncRequest = struct {
    pub const Segment = struct {
        buffer: ?[]u8, // If null, it's a GAP (discard data)
        len: usize,
    };

    // VTable for EventLoop (Must be first)
    onReadReady: *const fn(ptr: *anyopaque, fd: std.posix.fd_t) void,
    onWriteReady: *const fn(ptr: *anyopaque, fd: std.posix.fd_t) void,

    allocator: std.mem.Allocator,
    state: State,
    
    // Request Data
    method: enum { GET, HEAD },
    host: []const u8,
    port: u16,
    path: []const u8,
    range_start: u64,
    range_end: u64, // Exclusive
    tls: ?*TlsAdapter, // TLS Adapter (optional)
    
    // Internal Buffers
    write_buf: std.ArrayListUnmanaged(u8),
    write_pos: usize,
    
    read_buf: [4096]u8,
    read_pos: usize, // Valid data in buffer
    read_cursor: usize, // Parsed position
    
    content_length: u64,
    body_read_total: u64, // Total bytes read from body (including gaps)

    // Target Segments (Scatter/Gather + Gaps)
    segments: std.ArrayListUnmanaged(Segment),
    current_seg_idx: usize,
    current_seg_read: usize,

    pub fn init(allocator: std.mem.Allocator) AsyncRequest {
        return .{
            .onReadReady = onReadReadyCallback,
            .onWriteReady = onWriteReadyCallback,
            .allocator = allocator,
            .state = .Idle,
            .method = .GET,
            .host = "",
            .port = 0,
            .path = "",
            .range_start = 0,
            .range_end = 0,
            .tls = null,
            .write_buf = .{},
            .write_pos = 0,
            .read_buf = undefined,
            .read_pos = 0,
            .read_cursor = 0,
            .content_length = 0,
            .body_read_total = 0,
            .segments = .{},
            .current_seg_idx = 0,
            .current_seg_read = 0,
        };
    }

    pub fn deinit(self: *AsyncRequest) void {
        self.write_buf.deinit(self.allocator);
        self.segments.deinit(self.allocator);
    }

    pub fn reset(self: *AsyncRequest) void {
        self.state = .Idle;
        self.write_buf.clearRetainingCapacity();
        self.segments.clearRetainingCapacity();
        self.write_pos = 0;
        self.read_pos = 0;
        self.read_cursor = 0;
        self.content_length = 0;
        self.body_read_total = 0;
        self.current_seg_idx = 0;
        self.current_seg_read = 0;
        self.tls = null;
    }

    /// Add a segment to receive data.
    /// buffer=null means "discard len bytes" (Gap).
    pub fn addSegment(self: *AsyncRequest, buffer: ?[]u8, len: usize) !void {
        try self.segments.append(self.allocator, .{ .buffer = buffer, .len = len });
    }

    /// Prepare the request for sending.
    pub fn prepare(self: *AsyncRequest, host: []const u8, port: u16, path: []const u8, start: u64, end: u64, tls: ?*TlsAdapter) !void {
        self.host = host;
        self.port = port;
        self.path = path;
        self.range_start = start;
        self.range_end = end;
        self.tls = tls;
        self.method = .GET;

        // Format Request
        const req_str = try std.fmt.allocPrint(self.allocator, 
            "GET {s} HTTP/1.1\r\n" ++
            "Host: {s}:{d}\r\n" ++
            "Range: bytes={d}-{d}\r\n" ++
            "Connection: keep-alive\r\n" ++
            "\r\n",
            .{path, host, port, start, end - 1}
        );
        defer self.allocator.free(req_str);
        
        try self.write_buf.appendSlice(self.allocator, req_str);
        self.state = .SendingRequest;
    }

    /// Prepare a HEAD request.
    pub fn prepareHead(self: *AsyncRequest, host: []const u8, port: u16, path: []const u8, tls: ?*TlsAdapter) !void {
        self.host = host;
        self.port = port;
        self.path = path;
        self.range_start = 0;
        self.range_end = 0;
        self.tls = tls;
        self.method = .HEAD;

        // Format Request
        const req_str = try std.fmt.allocPrint(self.allocator, 
            "HEAD {s} HTTP/1.1\r\n" ++
            "Host: {s}:{d}\r\n" ++
            "Connection: keep-alive\r\n" ++
            "\r\n",
            .{path, host, port}
        );
        defer self.allocator.free(req_str);
        
        try self.write_buf.appendSlice(self.allocator, req_str);
        self.state = .SendingRequest;
    }

    /// Step: Write Request to Socket
    pub fn stepWrite(self: *AsyncRequest, fd: std.posix.fd_t) !bool {
        if (self.state != .SendingRequest) return error.InvalidState;

        while (self.write_pos < self.write_buf.items.len) {
            const chunk = self.write_buf.items[self.write_pos..];
            var n: usize = 0;
            
            if (self.tls) |tls| {
                n = tls.write(chunk) catch |err| {
                    if (@as(anyerror, err) == error.WouldBlock) return false;
                    return err;
                };
            } else {
                n = std.posix.write(fd, chunk) catch |err| switch (err) {
                    error.WouldBlock => return false,
                    else => return err,
                };
            }
            
            if (n == 0) return error.WriteZero;
            self.write_pos += n;
        }

        self.state = .ReadingHeaders;
        return true;
    }

    /// Step: Read Headers from Socket
    pub fn stepReadHeaders(self: *AsyncRequest, fd: std.posix.fd_t) !bool {
        if (self.state != .ReadingHeaders) return error.InvalidState;

        var n: usize = 0;
        if (self.tls) |tls| {
            n = tls.read(self.read_buf[self.read_pos..]) catch |err| {
                if (@as(anyerror, err) == error.WouldBlock) return false;
                return err;
            };
        } else {
            n = std.posix.read(fd, self.read_buf[self.read_pos..]) catch |err| switch (err) {
                error.WouldBlock => return false,
                else => return err,
            };
        }

        if (n == 0) return error.EndOfStream;
        self.read_pos += n;

        const window = self.read_buf[0..self.read_pos];
        if (std.mem.indexOf(u8, window, "\r\n\r\n")) |end_of_headers| {
            if (std.mem.indexOf(u8, window, "Content-Length:")) |cl_idx| {
                const rest = window[cl_idx + "Content-Length:".len ..];
                if (std.mem.indexOf(u8, rest, "\r\n")) |eol| {
                    const val_str = std.mem.trim(u8, rest[0..eol], " ");
                    self.content_length = try std.fmt.parseInt(u64, val_str, 10);
                }
            }

            self.read_cursor = end_of_headers + 4;
            
            if (self.method == .HEAD) {
                self.state = .Finished;
                return true;
            }

            // Move excess bytes to body
            const body_bytes_in_buf = self.read_pos - self.read_cursor;
            self.state = .ReadingBody;
            
            if (body_bytes_in_buf > 0) {
                const excess = self.read_buf[self.read_cursor .. self.read_pos];
                try self.consumeBodyBytes(excess);
            }
            
            if (self.isBodyComplete()) {
                self.state = .Finished;
                return true;
            }
            return true;
        }
        return false;
    }

    fn isBodyComplete(self: *AsyncRequest) bool {
        // Check if we exhausted all segments OR read content_length
        if (self.current_seg_idx >= self.segments.items.len) return true;
        if (self.content_length > 0 and self.body_read_total >= self.content_length) return true;
        return false;
    }

    fn consumeBodyBytes(self: *AsyncRequest, data: []const u8) !void {
        var data_offset: usize = 0;
        while (data_offset < data.len) {
            if (self.current_seg_idx >= self.segments.items.len) return; // Excess data, ignore or error? Ignore.

            const seg = self.segments.items[self.current_seg_idx];
            const needed = seg.len - self.current_seg_read;
            const available = data.len - data_offset;
            const to_copy = @min(needed, available);

            if (seg.buffer) |buf| {
                @memcpy(buf[self.current_seg_read .. self.current_seg_read + to_copy], data[data_offset .. data_offset + to_copy]);
            }
            // If seg.buffer is null, we just skip (Zero-Alloc Gap!)

            self.current_seg_read += to_copy;
            data_offset += to_copy;
            self.body_read_total += to_copy;

            if (self.current_seg_read >= seg.len) {
                self.current_seg_idx += 1;
                self.current_seg_read = 0;
            }
        }
    }

    /// Step: Read Body from Socket
    pub fn stepReadBody(self: *AsyncRequest, fd: std.posix.fd_t) !bool {
        if (self.state != .ReadingBody) return error.InvalidState;

        // Optimization: Read directly into current segment if possible
        if (self.current_seg_idx < self.segments.items.len) {
            const seg = self.segments.items[self.current_seg_idx];
            const needed = seg.len - self.current_seg_read;
            
            var n: usize = 0;
            if (seg.buffer) |buf| {
                // Direct read
                const dest = buf[self.current_seg_read..];
                if (self.tls) |tls| {
                    n = tls.read(dest) catch |err| {
                        if (@as(anyerror, err) == error.WouldBlock) return false;
                        return err;
                    };
                } else {
                    n = std.posix.read(fd, dest) catch |err| switch (err) {
                        error.WouldBlock => return false,
                        else => return err,
                    };
                }
            } else {
                // GAP: Read into scratch buffer to discard
                var scratch: [4096]u8 = undefined;
                const to_read = @min(needed, scratch.len);
                if (self.tls) |tls| {
                    n = tls.read(scratch[0..to_read]) catch |err| {
                        if (@as(anyerror, err) == error.WouldBlock) return false;
                        return err;
                    };
                } else {
                    n = std.posix.read(fd, scratch[0..to_read]) catch |err| switch (err) {
                        error.WouldBlock => return false,
                        else => return err,
                    };
                }
            }
            
            if (n == 0) return error.EndOfStream;
            
            self.current_seg_read += n;
            self.body_read_total += n;
            if (self.current_seg_read >= seg.len) {
                self.current_seg_idx += 1;
                self.current_seg_read = 0;
            }
        } else {
            self.state = .Finished;
            return true;
        }

        if (self.isBodyComplete()) {
            self.state = .Finished;
            return true;
        }
        return false;
    }

    fn onReadReadyCallback(ptr: *anyopaque, fd: std.posix.fd_t) void {
        const self: *AsyncRequest = @ptrCast(@alignCast(ptr));
        switch (self.state) {
            .ReadingHeaders => {
                const done = self.stepReadHeaders(fd) catch |err| {
                    std.debug.print("Read Headers Error: {}\n", .{err});
                    self.state = .Error;
                    return;
                };
                if (done) {
                    if (self.state == .ReadingBody) {
                        _ = self.stepReadBody(fd) catch {};
                    }
                }
            },
            .ReadingBody => {
                // Loop to drain socket as much as possible? 
                // For now, one read per event.
                _ = self.stepReadBody(fd) catch |err| {
                    std.debug.print("Read Body Error: {}\n", .{err});
                    self.state = .Error;
                };
            },
            else => {},
        }
    }

    fn onWriteReadyCallback(ptr: *anyopaque, fd: std.posix.fd_t) void {
        const self: *AsyncRequest = @ptrCast(@alignCast(ptr));
        if (self.state == .SendingRequest) {
            _ = self.stepWrite(fd) catch |err| {
                std.debug.print("Write Error: {}\n", .{err});
                self.state = .Error;
            };
        }
    }
};

test "AsyncRequest - Gapped Read" {
    const allocator = std.testing.allocator;
    var req = AsyncRequest.init(allocator);
    defer req.deinit();

    try req.reset(); // Clear segments
    
    var buf1: [5]u8 = undefined;
    var buf2: [5]u8 = undefined;
    
    try req.addSegment(&buf1, 5);
    try req.addSegment(null, 10);
    try req.addSegment(&buf2, 5);
    
    try req.prepare("localhost", 9000, "/test", 0, 20, null);
    
    try std.testing.expectEqual(AsyncRequest.State.SendingRequest, req.state);
}
