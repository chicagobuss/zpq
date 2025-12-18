const std = @import("std");
const xev = @import("xev");
const tls = @import("../tls/connection.zig");

pub const Client = struct {
    loop: *xev.Loop,
    allocator: std.mem.Allocator,
    
    pub fn init(loop: *xev.Loop, allocator: std.mem.Allocator) Client {
        return .{ .loop = loop, .allocator = allocator };
    }
    
    // Simplest fetch: Connects, sends request, prints response (for verification)
    pub fn fetch(self: *Client, host: []const u8, ip: []const u8, port: u16, path: []const u8) !void {
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
        };
        
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
    _ = ctx;
    std.debug.print("HTTP Client: Received {} bytes\n{s}\n", .{data.len, data});
}

fn onError(ctx_void: ?*anyopaque, err: anyerror) void {
    const ctx: *ReqContext = @ptrCast(@alignCast(ctx_void));
    std.debug.print("HTTP Client: Error: {}\n", .{err});
    ctx.conn.loop.stop();
}

