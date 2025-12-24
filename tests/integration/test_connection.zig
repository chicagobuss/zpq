const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");
const Connection = zpq.s3.Connection;

const Context = struct {
    done: bool = false,
    got_data: bool = false,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    const host = "google.com";
    const port = 443;
    const ip = "142.250.190.46"; // Google IP

    var conn = try Connection.init(&loop, allocator, host, true);
    defer conn.deinit();

    var ctx = Context{};
    conn.user_ctx = &ctx;
    conn.on_connect = onConnect;
    conn.on_data = onData;
    conn.on_error = onError;

    const addr = try xev.shim_net.Address.parseIp4(ip, port);
    try conn.connect(addr);

    while (!ctx.done) {
        try loop.run(.once);
    }

    if (!ctx.got_data) {
        std.debug.print("Failed to get data from google.com\n", .{});
        std.process.exit(1);
    }
    std.debug.print("test_connection passed!\n", .{});
}

fn onConnect(conn: *Connection, ctx_void: ?*anyopaque) void {
    _ = ctx_void;
    std.debug.print("Connected to google.com! Sending request...\n", .{});
    const req = "GET / HTTP/1.1\r\nHost: google.com\r\nConnection: close\r\n\r\n";
    conn.write(req) catch |err| {
        std.debug.print("Write error: {}\n", .{err});
    };
}

fn onData(conn: *Connection, ctx_void: ?*anyopaque, data: []const u8) void {
    _ = conn;
    const ctx: *Context = @ptrCast(@alignCast(ctx_void));
    std.debug.print("Received {} bytes data.\n", .{data.len});
    if (std.mem.indexOf(u8, data, "HTTP/1.1") != null) {
        ctx.got_data = true;
    }
}

fn onError(conn: *Connection, ctx_void: ?*anyopaque, err: anyerror) void {
    _ = conn;
    const ctx: *Context = @ptrCast(@alignCast(ctx_void));
    if (err == error.EOF) {
        std.debug.print("Connection closed (EOF).\n", .{});
    } else {
        std.debug.print("Error: {}\n", .{err});
    }
    ctx.done = true;
}
