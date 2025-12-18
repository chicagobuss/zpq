const std = @import("std");
const xev = @import("xev");
const tls = @import("../tls/connection.zig");

pub const Client = struct {
    loop: *xev.Loop,
    allocator: std.mem.Allocator,
    
    pub fn init(loop: *xev.Loop, allocator: std.mem.Allocator) Client {
        return .{ .loop = loop, .allocator = allocator };
    }

    pub const FetchResult = struct {
        /// Set to true once we've received any decrypted bytes.
        got_any_data: bool = false,
        /// The last error observed (if any).
        err: ?anyerror = null,
        /// Total number of decrypted bytes delivered to `on_data`.
        bytes: usize = 0,
        /// Opaque pointer to internal request context, for cleanup after the loop finishes.
        /// (We cannot safely free inside xev callbacks.)
        _ctx: ?*anyopaque = null,
    };

    /// Best-effort cleanup for the heap allocations created by `fetchWithResult`.
    /// Safe to call only after the loop has stopped / `Loop.run()` returned.
    pub fn cleanupFetchResult(self: *Client, result: *FetchResult) void {
        const ctx_void = result._ctx orelse return;
        result._ctx = null;

        const ctx: *ReqContext = @ptrCast(@alignCast(ctx_void));
        ctx.conn.deinit();
        self.allocator.destroy(ctx.conn);
        self.allocator.destroy(ctx);
    }
    
    // Simplest fetch: Connects, sends request, prints response (for verification)
    pub fn fetch(self: *Client, host: []const u8, ip: []const u8, port: u16, path: []const u8) !void {
        return self.fetchWithResult(host, ip, port, path, null);
    }

    pub fn fetchWithResult(
        self: *Client,
        host: []const u8,
        ip: []const u8,
        port: u16,
        path: []const u8,
        result: ?*FetchResult,
    ) !void {
        // Create connection
        // We need to allocate the connection on heap so pointers remain valid
        const conn = try self.allocator.create(tls.Connection);
        conn.* = try tls.Connection.init(self.loop, self.allocator, host);
        
        // Context for callbacks
        const ctx = try self.allocator.create(ReqContext);
        ctx.* = .{
            .conn = conn,
            .allocator = self.allocator,
            .host = host,
            .path = path,
            .result = result,
            .finished = false,
        };
        if (result) |r| r._ctx = ctx;
        
        conn.user_ctx = ctx;
        conn.on_connect = onConnect;
        conn.on_data = onData;
        conn.on_error = onError;
        
        // Parse IP (blocking for now, or assume IP string)
        const addr = try xev.shim_net.Address.parseIp4(ip, port);
        
        try conn.connect(addr);
    }
};

const ReqContext = struct {
    conn: *tls.Connection,
    allocator: std.mem.Allocator,
    host: []const u8,
    path: []const u8,
    result: ?*Client.FetchResult,
    finished: bool,
};

fn onConnect(ctx_void: ?*anyopaque) void {
    const ctx: *ReqContext = @ptrCast(@alignCast(ctx_void));
    std.debug.print("HTTP Client: Connected! Sending Request...\n", .{});
    
    // Send HEAD Request
    const req_fmt = "HEAD {s} HTTP/1.1\r\nHost: {s}\r\nUser-Agent: zpq-client\r\nConnection: close\r\n\r\n";
    const req = std.fmt.allocPrint(ctx.allocator, req_fmt, .{ctx.path, ctx.host}) catch return;
    defer ctx.allocator.free(req);
    
    ctx.conn.write(req) catch |err| {
        std.debug.print("Write error: {}\n", .{err});
    };
}

fn onData(ctx_void: ?*anyopaque, data: []const u8) void {
    const ctx: *ReqContext = @ptrCast(@alignCast(ctx_void));
    if (ctx.result) |r| {
        r.got_any_data = true;
        r.bytes += data.len;
    }
    std.debug.print("HTTP Client: Received {} bytes\n{s}\n", .{data.len, data});
}

fn onError(ctx_void: ?*anyopaque, err: anyerror) void {
    const ctx: *ReqContext = @ptrCast(@alignCast(ctx_void));
    // Guard: we can get multiple callbacks during shutdown.
    if (!ctx.finished) {
        ctx.finished = true;
        if (ctx.result) |r| {
            r.err = err;
        }
        std.debug.print("HTTP Client: Error: {}\n", .{err});
        // Stop the loop so the caller can decide how to handle the error.
        ctx.conn.loop.stop();
        return;
    }

    // If already finished, still stop the loop (idempotent).
    ctx.conn.loop.stop();
}

