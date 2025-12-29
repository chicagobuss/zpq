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
    done: bool = false,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    // Try resolving google.com:80 to test real network
    const addr = try shim.Address.parseIp4("8.8.8.8", 53); // DNS port just to see if we can connect
    // Actually, let's use a simple HTTP check if possible, but 8.8.8.8:53 is fine for a connect test.

    var client = try xev.TCP.init(addr);

    var ctx = Context{
        .loop = &loop,
        .client = client,
        .allocator = allocator,
    };

    std.debug.print("Connecting to 8.8.8.8:53...\n", .{});

    client.connect(&loop, &ctx.c_connect, addr, Context, &ctx, onConnect);

    // Run the loop. In ZPQ main, we might use .once in a loop, but here .until_done is fine.
    while (!ctx.done) {
        try loop.run(.once);
    }

    std.debug.print("Probe finished successfully.\n", .{});
}

fn onConnect(
    ctx: ?*Context,
    loop: *xev.Loop,
    c: *xev.Completion,
    s: xev.TCP,
    r: xev.ConnectError!void,
) xev.CallbackAction {
    _ = c;
    _ = s;
    const context = ctx.?;

    r catch |err| {
        std.debug.print("Connect failed: {}\n", .{err});
        context.done = true;
        return .disarm;
    };
    std.debug.print("Connected!\n", .{});

    // Close immediately for this probe
    context.client.close(loop, &context.c_read, Context, context, onClose);
    return .disarm;
}

fn onClose(
    ctx: ?*Context,
    loop: *xev.Loop,
    c: *xev.Completion,
    s: xev.TCP,
    r: xev.CloseError!void,
) xev.CallbackAction {
    _ = loop;
    _ = c;
    _ = s;
    const context = ctx.?;
    r catch |err| {
        std.debug.print("Close failed: {}\n", .{err});
    };
    std.debug.print("Closed.\n", .{});
    context.done = true;
    return .disarm;
}
