const std = @import("std");
const xev = @import("xev");
const shim = xev.shim_net;

const Context = struct {
    loop: *xev.Loop,
    client: xev.TCP,
    allocator: std.mem.Allocator,
    c_connect: xev.Completion = .{},
    c_write: xev.Completion = .{},
    c_read: xev.Completion = .{},
    write_buf: []u8 = &[_]u8{},
    read_buf: [4096]u8 = undefined,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // 1. Init Loop
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    // 2. Init Socket
    // 1.1.1.1:80 (Cloudflare)
    const addr = try shim.Address.parseIp4("1.1.1.1", 80);
    // Note: libxev TCP.init takes address but doesn't bind unless bind() called?
    // checking tcp.zig: init(addr) creates socket with family.
    var client = try xev.TCP.init(addr);

    var ctx = Context{
        .loop = &loop,
        .client = client,
        .allocator = allocator,
    };

    std.debug.print("Connecting to 1.1.1.1:80...\n", .{});
    
    // 3. Connect
    client.connect(&loop, &ctx.c_connect, addr, Context, &ctx, onConnect);

    try loop.run(.until_done);
}

fn onConnect(
    ctx: ?*Context,
    loop: *xev.Loop,
    c: *xev.Completion,
    s: xev.TCP, // connect passes self back
    r: xev.ConnectError!void,
) xev.CallbackAction {
    _ = c;
    _ = s;
    const context = ctx.?;
    
    r catch |err| {
        std.debug.print("Connect failed: {}\n", .{err});
        return .disarm;
    };
    std.debug.print("Connected! Sending request...\n", .{});

    // Prepare Request
    const req_str = "GET / HTTP/1.0\r\nHost: 1.1.1.1\r\n\r\n";
    context.write_buf = context.allocator.dupe(u8, req_str) catch unreachable;

    // Write
    context.client.write(
        loop,
        &context.c_write,
        .{ .slice = context.write_buf },
        Context,
        context,
        onWrite
    );

    return .disarm;
}

fn onWrite(
    ctx: ?*Context,
    loop: *xev.Loop,
    c: *xev.Completion,
    s: xev.TCP,
    buf: xev.WriteBuffer,
    r: xev.WriteError!usize,
) xev.CallbackAction {
    _ = c;
    _ = s;
    _ = buf;
    const context = ctx.?;

    const bytes_written = r catch |err| {
        std.debug.print("Write failed: {}\n", .{err});
        return .disarm;
    };
    std.debug.print("Written {} bytes. Reading response...\n", .{bytes_written});

    // Read
    context.client.read(
        loop,
        &context.c_read,
        .{ .slice = &context.read_buf },
        Context,
        context,
        onRead
    );

    return .disarm;
}

fn onRead(
    ctx: ?*Context,
    loop: *xev.Loop,
    c: *xev.Completion,
    s: xev.TCP,
    buf: xev.ReadBuffer,
    r: xev.ReadError!usize,
) xev.CallbackAction {
    _ = c;
    _ = buf;
    _ = s;
    const context = ctx.?;

    const bytes_read = r catch |err| {
        if (err == error.EOF) {
            std.debug.print("Connection closed by peer (EOF).\n", .{});
            
            // Cleanup
            context.allocator.free(context.write_buf);
            
            // Close socket? 
            // We need to close gracefully.
            // Using shutdown or close.
            // But we can just disarm/exit loop by having no more completions.
            // But we should close the fd.
            // s.close(...)
            
            // For this simple test, we can just return disarm.
            // But wait, loop.run(.until_done) waits for completions.
            // If we return disarm, this completion is removed.
            // If no other completions, loop exits.
            return .disarm;
        }
        std.debug.print("Read failed: {}\n", .{err});
        return .disarm;
    };

    if (bytes_read == 0) {
        // EOF handled above usually, but some backends might return 0?
        std.debug.print("Read 0 bytes (EOF).\n", .{});
        context.allocator.free(context.write_buf);
        return .disarm;
    }

    std.debug.print("Read {} bytes:\n{s}\n", .{bytes_read, context.read_buf[0..bytes_read]});

    // Read again? HTTP/1.0 might close after response.
    // Let's try to read again until EOF.
    context.client.read(
        loop,
        &context.c_read,
        .{ .slice = &context.read_buf },
        Context,
        context,
        onRead
    );

    return .disarm;
}

