const std = @import("std");
const TlsAdapter = @import("tls_adapter.zig").TlsAdapter;
const sigv4 = @import("sigv4.zig");
const SigV4 = sigv4.SigV4;
const types = @import("types.zig");

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

    // VTable for EventLoop (Must be first) - REMOVED
    // onReadReady: *const fn(ptr: *anyopaque, fd: std.posix.fd_t) void,
    // onWriteReady: *const fn(ptr: *anyopaque, fd: std.posix.fd_t) void,

    state: State,
    
    // Request Data
    method: enum { GET, HEAD },
    host: []const u8,
    port: u16,
    path: []const u8,
    range_start: u64,
    range_end: u64, // Exclusive
    tls: ?*TlsAdapter, // TLS Adapter (optional) - REMOVING, IO logic moved out
    
    // Auth Data
    config: ?types.S3Config,

    // Internal Buffers
    write_buf: std.ArrayListUnmanaged(u8),
    write_pos: usize,
    
    read_buf: std.ArrayListUnmanaged(u8), // Dynamic buffer for push parser
    read_pos: usize, // Valid data in buffer (always read_buf.items.len)
    read_cursor: usize, // Parsed position
    
    content_length: u64,
    body_read_total: u64, // Total bytes read from body (including gaps)

    // Target Segments (Scatter/Gather + Gaps)
    segments: std.ArrayListUnmanaged(Segment),
    current_seg_idx: usize,
    current_seg_read: usize,

    pub fn init() AsyncRequest {
        return .{
            .state = .Idle,
            .method = .GET,
            .host = "",
            .port = 0,
            .path = "",
            .range_start = 0,
            .range_end = 0,
            .tls = null,
            .config = null,
            .write_buf = .{},
            .write_pos = 0,
            .read_buf = .{},
            .read_pos = 0,
            .read_cursor = 0,
            .content_length = 0,
            .body_read_total = 0,
            .segments = .{},
            .current_seg_idx = 0,
            .current_seg_read = 0,
        };
    }

    pub fn deinit(self: *AsyncRequest, allocator: std.mem.Allocator) void {
        self.write_buf.deinit(allocator);
        self.read_buf.deinit(allocator);
        self.segments.deinit(allocator);
    }

    pub fn reset(self: *AsyncRequest) void {
        self.state = .Idle;
        self.write_buf.clearRetainingCapacity();
        self.read_buf.clearRetainingCapacity();
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
    pub fn addSegment(self: *AsyncRequest, allocator: std.mem.Allocator, buffer: ?[]u8, len: usize) !void {
        try self.segments.append(allocator, .{ .buffer = buffer, .len = len });
    }

    /// Prepare the request for sending.
    pub fn prepare(self: *AsyncRequest, allocator: std.mem.Allocator, host: []const u8, port: u16, path: []const u8, start: u64, end: u64, tls: ?*TlsAdapter, config: ?types.S3Config) !void {
        self.host = host;
        self.port = port;
        self.path = path;
        self.range_start = start;
        self.range_end = end;
        self.tls = tls;
        self.method = .GET;
        self.config = config;

        // Use arena for signing headers
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const aa = arena.allocator();

        // 1. Build Base Headers
        var headers = std.ArrayList(std.http.Header).empty;
        
        // Host Header Value
        const host_header_val = if (port == 80 or port == 443) 
            try std.fmt.allocPrint(aa, "{s}", .{host})
        else
            try std.fmt.allocPrint(aa, "{s}:{d}", .{host, port});
            
        // Range Header
        const range_val = try std.fmt.allocPrint(aa, "bytes={d}-{d}", .{start, end - 1});
        try headers.append(aa, .{ .name = "Range", .value = range_val });
        
        try headers.append(aa, .{ .name = "Connection", .value = "keep-alive" });

        // 2. Sign or Add Host
        if (self.config) |conf| {
            if (conf.credentials) |creds| {
                var auth = SigV4{
                    .access_key = creds.access_key,
                    .secret_key = creds.secret_key,
                    .session_token = creds.session_token,
                    .region = conf.region,
                };
                
                const scheme = if (tls != null) "https" else "http";
                const url = try std.fmt.allocPrint(aa, "{s}://{s}{s}", .{scheme, host_header_val, path});
                const uri = try std.Uri.parse(url);

                // sigv4.sign adds the Host header based on the URI
                try auth.sign(aa, "GET", uri, &headers, "");
            } else {
                try headers.append(aa, .{ .name = "Host", .value = host_header_val });
            }
        } else {
            // Anonymous request: must add Host manually
            try headers.append(aa, .{ .name = "Host", .value = host_header_val });
        }

        // 3. Serialize Request
        // Request Line
        try self.write_buf.appendSlice(allocator, "GET ");
        try self.write_buf.appendSlice(allocator, path);
        try self.write_buf.appendSlice(allocator, " HTTP/1.1\r\n");
        
        // Headers
        for (headers.items) |h| {
            try self.write_buf.appendSlice(allocator, h.name);
            try self.write_buf.appendSlice(allocator, ": ");
            try self.write_buf.appendSlice(allocator, h.value);
            try self.write_buf.appendSlice(allocator, "\r\n");
        }
        try self.write_buf.appendSlice(allocator, "\r\n");
        
        self.state = .SendingRequest;
    }

    /// Prepare a HEAD request.
    pub fn prepareHead(self: *AsyncRequest, allocator: std.mem.Allocator, host: []const u8, port: u16, path: []const u8, tls: ?*TlsAdapter, config: ?types.S3Config) !void {
        self.host = host;
        self.port = port;
        self.path = path;
        self.range_start = 0;
        self.range_end = 0;
        self.tls = tls;
        self.method = .HEAD;
        self.config = config;

        // Use arena for signing headers
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const aa = arena.allocator();

        // 1. Build Base Headers
        var headers = std.ArrayList(std.http.Header).empty;
        
        const host_header_val = if (port == 80 or port == 443) 
            try std.fmt.allocPrint(aa, "{s}", .{host})
        else
            try std.fmt.allocPrint(aa, "{s}:{d}", .{host, port});
            
        try headers.append(aa, .{ .name = "Connection", .value = "keep-alive" });

        // 2. Sign or Add Host
        if (self.config) |conf| {
            if (conf.credentials) |creds| {
                var auth = SigV4{
                    .access_key = creds.access_key,
                    .secret_key = creds.secret_key,
                    .session_token = creds.session_token,
                    .region = conf.region,
                };
                
                const scheme = if (tls != null) "https" else "http";
                const url = try std.fmt.allocPrint(aa, "{s}://{s}{s}", .{scheme, host_header_val, path});
                const uri = try std.Uri.parse(url);

                // sigv4.sign adds the Host header based on the URI
                try auth.sign(aa, "HEAD", uri, &headers, "");
            } else {
                try headers.append(aa, .{ .name = "Host", .value = host_header_val });
            }
        } else {
            try headers.append(aa, .{ .name = "Host", .value = host_header_val });
        }

        // 3. Serialize Request
        try self.write_buf.appendSlice(allocator, "HEAD ");
        try self.write_buf.appendSlice(allocator, path);
        try self.write_buf.appendSlice(allocator, " HTTP/1.1\r\n");
        
        for (headers.items) |h| {
            try self.write_buf.appendSlice(allocator, h.name);
            try self.write_buf.appendSlice(allocator, ": ");
            try self.write_buf.appendSlice(allocator, h.value);
            try self.write_buf.appendSlice(allocator, "\r\n");
        }
        try self.write_buf.appendSlice(allocator, "\r\n");
        
        self.state = .SendingRequest;
    }

    /// Feed data into the request state machine.
    /// Should be called whenever new data arrives from the socket/TLS.
    pub fn feed(self: *AsyncRequest, allocator: std.mem.Allocator, data: []const u8) !void {
        if (self.state == .Finished or self.state == .Error) return;
        if (data.len == 0) return;

        // If we are sending request, we shouldn't be receiving data yet usually,
        // but if we do, we transition to ReadingHeaders.
        if (self.state == .SendingRequest) {
            self.state = .ReadingHeaders;
        }

        if (self.state == .ReadingHeaders) {
            try self.read_buf.appendSlice(allocator, data);
            _ = try self.stepReadHeaders(0); // Dummy FD
        } else if (self.state == .ReadingBody) {
            // Optimization: If read_buf is empty, consume directly from data
            // to avoid copy.
            if (self.read_buf.items.len == 0) {
                try self.consumeBodyBytes(data);
            } else {
                // We have some leftover bytes in buffer, append new data and consume
                try self.read_buf.appendSlice(allocator, data);
                // Consume as much as possible
                const bytes_in_buf = self.read_buf.items.len - self.read_cursor;
                if (bytes_in_buf > 0) {
                    const chunk = self.read_buf.items[self.read_cursor..];
                    try self.consumeBodyBytes(chunk);
                    // Reset buffer if fully consumed
                    if (self.read_cursor == self.read_buf.items.len) {
                        self.read_buf.clearRetainingCapacity();
                        self.read_cursor = 0;
                    }
                }
            }
            
            if (self.isBodyComplete()) {
                self.state = .Finished;
            }
        }
    }

    /// Step: Write request to socket
    pub fn stepWrite(self: *AsyncRequest, fd: std.posix.fd_t) !bool {
        if (self.state != .SendingRequest) return true;

        const remaining = self.write_buf.items[self.write_pos..];
        var n: usize = 0;
        
        if (self.tls) |tls| {
            n = tls.write(remaining) catch |err| {
                if (@as(anyerror, err) == error.WouldBlock) return false;
                return err;
            };
        } else {
            n = std.posix.write(fd, remaining) catch |err| switch (err) {
                error.WouldBlock => return false,
                else => return err,
            };
        }
        
        self.write_pos += n;
        if (self.write_pos >= self.write_buf.items.len) {
            self.state = .ReadingHeaders;
            return true;
        }
        return false;
    }

    /// Step: Process Headers from internal buffer
    pub fn stepReadHeaders(self: *AsyncRequest, fd: std.posix.fd_t) !bool {
        _ = fd;
        if (self.state != .ReadingHeaders) return false;

        // We scan read_buf for double-CRLF
        const window = self.read_buf.items[self.read_cursor..];
        if (std.mem.indexOf(u8, window, "\r\n\r\n")) |idx| {
            const end_of_headers = self.read_cursor + idx;
            
            // Parse Content-Length
            // We search from start of headers (read_cursor=0 usually)
            // But we need to search the whole header block
            const header_block = self.read_buf.items[0 .. end_of_headers + 4]; // Include CRLFCRLF
            
            if (findHeader(header_block, "Content-Length")) |val| {
                self.content_length = try std.fmt.parseInt(u64, val, 10);
            }

            self.read_cursor = end_of_headers + 4;
            
            if (self.method == .HEAD) {
                self.state = .Finished;
                return true;
            }

            self.state = .ReadingBody;
            
            // Consume any body bytes already in buffer
            const body_bytes_in_buf = self.read_buf.items.len - self.read_cursor;
            if (body_bytes_in_buf > 0) {
                const excess = self.read_buf.items[self.read_cursor..];
                try self.consumeBodyBytes(excess);
                self.read_buf.clearRetainingCapacity();
                self.read_cursor = 0;
            } else {
                self.read_buf.clearRetainingCapacity();
                self.read_cursor = 0;
            }
            
            if (self.isBodyComplete()) {
                self.state = .Finished;
            }
            return true;
        }
        return false;
    }

    fn findHeader(block: []const u8, name: []const u8) ?[]const u8 {
        var it = std.mem.splitSequence(u8, block, "\r\n");
        while (it.next()) |line| {
            if (std.mem.indexOf(u8, line, ":")) |colon_idx| {
                const key = std.mem.trim(u8, line[0..colon_idx], " ");
                if (std.ascii.eqlIgnoreCase(key, name)) {
                    return std.mem.trim(u8, line[colon_idx+1..], " ");
                }
            }
        }
        return null;
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
    var req = AsyncRequest.init();
    defer req.deinit(allocator);

    try req.reset(); // Clear segments
    
    var buf1: [5]u8 = undefined;
    var buf2: [5]u8 = undefined;
    
    try req.addSegment(allocator, &buf1, 5);
    try req.addSegment(allocator, null, 10);
    try req.addSegment(allocator, &buf2, 5);
    
    try req.prepare(allocator, "localhost", 9000, "/test", 0, 20, null, null);
    
    try std.testing.expectEqual(AsyncRequest.State.SendingRequest, req.state);
}
