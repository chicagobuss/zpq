const std = @import("std");
const types = @import("types.zig");
const Connection = @import("connection.zig").Connection;
const sigv4 = @import("sigv4.zig");
const SigV4 = sigv4.SigV4;

/// States for the HTTP Request Lifecycle
pub const State = enum {
    Idle,
    Connecting,
    RequestSent,
    ReadingHeaders,
    ReadingBody,
    Finished,
    Error,
};

/// A single Async HTTP Request State Machine.
/// It uses a Connection (Plain or TLS) to perform I/O.
pub const AsyncRequest = struct {
    pub const Segment = struct {
        buffer: ?[]u8, // If null, it's a GAP (discard data)
        len: usize,
    };

    allocator: std.mem.Allocator,
    state: State,
    
    // Request Data
    method: enum { GET, HEAD },
    host: []const u8,
    port: u16,
    path: []const u8,
    range_start: u64,
    range_end: u64, // Exclusive
    
    // Auth Data
    config: ?types.S3Config,

    // Internal Buffers
    write_buf: std.ArrayListUnmanaged(u8),
    read_buf: std.ArrayListUnmanaged(u8), // Dynamic buffer for push parser
    read_cursor: usize, // Parsed position
    
    content_length: u64,
    body_read_total: u64, // Total bytes read from body (including gaps)

    // Target Segments (Scatter/Gather + Gaps)
    segments: std.ArrayListUnmanaged(Segment),
    current_seg_idx: usize,
    current_seg_read: usize,

    // Active Connection
    connection: ?*Connection = null,

    // Callback for completion
    done_ctx: ?*anyopaque = null,
    on_done: ?*const fn (ctx: ?*anyopaque, req: *AsyncRequest) void = null,

    pub fn init(allocator: std.mem.Allocator) AsyncRequest {
        return .{
            .allocator = allocator,
            .state = .Idle,
            .method = .GET,
            .host = "",
            .port = 0,
            .path = "",
            .range_start = 0,
            .range_end = 0,
            .config = null,
            .write_buf = .{},
            .read_buf = .{},
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
        self.read_buf.deinit(self.allocator);
        self.segments.deinit(self.allocator);
    }

    pub fn reset(self: *AsyncRequest) void {
        self.state = .Idle;
        self.write_buf.clearRetainingCapacity();
        self.read_buf.clearRetainingCapacity();
        self.segments.clearRetainingCapacity();
        self.read_cursor = 0;
        self.content_length = 0;
        self.body_read_total = 0;
        self.current_seg_idx = 0;
        self.current_seg_read = 0;
    }

    /// Add a segment to receive data.
    /// buffer=null means "discard len bytes" (Gap).
    pub fn addSegment(self: *AsyncRequest, buffer: ?[]u8, len: usize) !void {
        try self.segments.append(self.allocator, .{ .buffer = buffer, .len = len });
    }

    /// Prepare the request for sending.
    pub fn prepare(self: *AsyncRequest, host: []const u8, port: u16, path: []const u8, start: u64, end: u64, use_tls: bool, config: ?types.S3Config) !void {
        self.host = host;
        self.port = port;
        self.path = path;
        self.range_start = start;
        self.range_end = end;
        self.method = .GET;
        self.config = config;

        // Use arena for signing headers
        var arena = std.heap.ArenaAllocator.init(self.allocator);
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
                
                const scheme = if (use_tls) "https" else "http";
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
        try self.write_buf.appendSlice(self.allocator, "GET ");
        try self.write_buf.appendSlice(self.allocator, path);
        try self.write_buf.appendSlice(self.allocator, " HTTP/1.1\r\n");
        
        // Headers
        for (headers.items) |h| {
            try self.write_buf.appendSlice(self.allocator, h.name);
            try self.write_buf.appendSlice(self.allocator, ": ");
            try self.write_buf.appendSlice(self.allocator, h.value);
            try self.write_buf.appendSlice(self.allocator, "\r\n");
        }
        try self.write_buf.appendSlice(self.allocator, "\r\n");
    }

    /// Prepare a HEAD request.
    pub fn prepareHead(self: *AsyncRequest, host: []const u8, port: u16, path: []const u8, use_tls: bool, config: ?types.S3Config) !void {
        self.host = host;
        self.port = port;
        self.path = path;
        self.range_start = 0;
        self.range_end = 0;
        self.method = .HEAD;
        self.config = config;

        // Use arena for signing headers
        var arena = std.heap.ArenaAllocator.init(self.allocator);
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
                
                const scheme = if (use_tls) "https" else "http";
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
        try self.write_buf.appendSlice(self.allocator, "HEAD ");
        try self.write_buf.appendSlice(self.allocator, path);
        try self.write_buf.appendSlice(self.allocator, " HTTP/1.1\r\n");
        
        for (headers.items) |h| {
            try self.write_buf.appendSlice(self.allocator, h.name);
            try self.write_buf.appendSlice(self.allocator, ": ");
            try self.write_buf.appendSlice(self.allocator, h.value);
            try self.write_buf.appendSlice(self.allocator, "\r\n");
        }
        try self.write_buf.appendSlice(self.allocator, "\r\n");
    }

    /// Attach this request to a connection and start the lifecycle.
    pub fn execute(self: *AsyncRequest, conn: *Connection) !void {
        self.state = .Connecting;
        self.connection = conn;
        conn.user_ctx = self;
        conn.on_connect = onConnect;
        conn.on_data = onData;
        conn.on_error = onError;
        
        // If connection is already connected and handshake done (keep-alive reuse),
        // we can start immediately.
        if (conn.handshake_complete) {
            onConnect(conn, self);
        }
    }

    // --- Callbacks for Connection ---

    fn onConnect(conn: *Connection, ctx: ?*anyopaque) void {
        const self: *AsyncRequest = @ptrCast(@alignCast(ctx));
        if (self.state != .Connecting) return;

        self.state = .RequestSent;
        conn.write(self.write_buf.items) catch {
            self.state = .Error;
            if (self.on_done) |cb| cb(self.done_ctx, self);
        };
    }

    fn onData(conn: *Connection, ctx: ?*anyopaque, data: []const u8) void {
        _ = conn;
        const self: *AsyncRequest = @ptrCast(@alignCast(ctx));
        
        self.feed(data) catch |err| {
            std.debug.print("AsyncRequest Feed Error: {}\n", .{err});
            self.state = .Error;
            if (self.on_done) |cb| cb(self.done_ctx, self);
        };
    }

    fn onError(conn: *Connection, ctx: ?*anyopaque, err: anyerror) void {
        _ = conn;
        const self: *AsyncRequest = @ptrCast(@alignCast(ctx));
        if (err == error.EOF) {
            // EOF might be expected if Finished, but if not Finished, it's an error.
            if (self.state != .Finished) {
                self.state = .Error;
            }
        } else {
            self.state = .Error;
        }
        if (self.on_done) |cb| cb(self.done_ctx, self);
    }

    pub fn feed(self: *AsyncRequest, data: []const u8) !void {
        if (self.state == .Finished or self.state == .Error) return;
        if (data.len == 0) return;

        // If we are waiting for response, we transition to ReadingHeaders.
        if (self.state == .RequestSent) {
            self.state = .ReadingHeaders;
        }

        if (self.state == .ReadingHeaders) {
            try self.read_buf.appendSlice(self.allocator, data);
            
            // Scan for double-CRLF
            const window = self.read_buf.items[self.read_cursor..];
            if (std.mem.indexOf(u8, window, "\r\n\r\n")) |idx| {
                const end_of_headers = self.read_cursor + idx;
                const header_block = self.read_buf.items[0 .. end_of_headers + 4];
                
                if (findHeader(header_block, "Content-Length")) |val| {
                    self.content_length = try std.fmt.parseInt(u64, val, 10);
                }

                self.read_cursor = end_of_headers + 4;
                
                if (self.method == .HEAD) {
                    self.state = .Finished;
                    if (self.on_done) |cb| cb(self.done_ctx, self);
                    return;
                }

                self.state = .ReadingBody;
                
                // Consume any body bytes already in buffer
                const body_bytes_in_buf = self.read_buf.items.len - self.read_cursor;
                if (body_bytes_in_buf > 0) {
                    const excess = self.read_buf.items[self.read_cursor..];
                    try self.consumeBodyBytes(excess);
                }
                
                // We can clear read_buf now as we only use it for headers.
                self.read_buf.clearRetainingCapacity();
                self.read_cursor = 0;

                if (self.isBodyComplete()) {
                    self.state = .Finished;
                    if (self.on_done) |cb| cb(self.done_ctx, self);
                }
            }
        } else if (self.state == .ReadingBody) {
            try self.consumeBodyBytes(data);
            if (self.isBodyComplete()) {
                self.state = .Finished;
                if (self.on_done) |cb| cb(self.done_ctx, self);
            }
        }
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
        if (self.current_seg_idx >= self.segments.items.len) return true;
        if (self.content_length > 0 and self.body_read_total >= self.content_length) return true;
        return false;
    }

    fn consumeBodyBytes(self: *AsyncRequest, data: []const u8) !void {
        var data_offset: usize = 0;
        while (data_offset < data.len) {
            if (self.current_seg_idx >= self.segments.items.len) return;

            const seg = self.segments.items[self.current_seg_idx];
            const needed = seg.len - self.current_seg_read;
            const available = data.len - data_offset;
            const to_copy = @min(needed, available);

            if (seg.buffer) |buf| {
                @memcpy(buf[self.current_seg_read .. self.current_seg_read + to_copy], data[data_offset .. data_offset + to_copy]);
            }

            self.current_seg_read += to_copy;
            data_offset += to_copy;
            self.body_read_total += to_copy;

            if (self.current_seg_read >= seg.len) {
                self.current_seg_idx += 1;
                self.current_seg_read = 0;
            }
        }
    }
};
