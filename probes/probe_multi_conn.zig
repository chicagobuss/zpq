const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");
const tls = zpq.io.tls;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    const count = 100;
    var conns = try allocator.alloc(*tls.Connection, count);
    defer allocator.free(conns);

    const addr = try xev.shim_net.Address.parseIp4("127.0.0.1", 9000);

    std.debug.print("Starting {d} concurrent connections to localhost:9000\n", .{count});

    for (0..count) |i| {
        conns[i] = try allocator.create(tls.Connection);
        conns[i].* = try tls.Connection.init(&loop, allocator, "localhost");
        conns[i].on_error = onError;
        try conns[i].connect(addr);
    }

    std.debug.print("Running loop...\n", .{});
    try loop.run(.until_done);
    std.debug.print("Loop finished.\n", .{});

    for (0..count) |i| {
        conns[i].deinit();
        allocator.destroy(conns[i]);
    }
}

fn onError(ctx: ?*anyopaque, err: anyerror) void {
    _ = ctx;
    if (err != error.EOF) {
        std.debug.print("Error: {any}\n", .{err});
    }
}
