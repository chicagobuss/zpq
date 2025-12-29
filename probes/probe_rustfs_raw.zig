const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");
const ResponseParser = zpq.io.response_parser.ResponseParser;

/// Proves raw TCP connectivity to RustFS without TLS.
/// Verifies that RustFS is responsive to plain HTTP/1.1.
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    // Use xev's own IP parser
    const addr = try xev.shim_net.Address.parseIp4("127.0.0.1", 9999);

    var tcp = try xev.TCP.init(addr);

    var ctx = struct {
        loop: *xev.Loop,
        tcp: *xev.TCP,
        c_connect: xev.Completion = .{},
        c_write: xev.Completion = .{},
        c_read: xev.Completion = .{},
        read_buf: [4096]u8 = undefined,
        finished: bool = false,
        parser: ResponseParser = .{},

        fn onConnect(p: ?*anyopaque, l: *xev.Loop, c: *xev.Completion, _: xev.TCP, r: xev.ConnectError!void) xev.CallbackAction {
            _ = c;
            const self: *@This() = @ptrCast(@alignCast(p.?));
            r catch |err| {
                std.debug.print("Connect error: {}\n", .{err});
                return .disarm;
            };

            const req = "GET /test-bucket/test.parquet HTTP/1.1\r\nHost: localhost:9999\r\nConnection: close\r\n\r\n";
            self.tcp.write(l, &self.c_write, .{ .slice = req }, @This(), self, onWrite);
            return .disarm;
        }

        fn onWrite(p: ?*anyopaque, l: *xev.Loop, c: *xev.Completion, _: xev.TCP, _: xev.WriteBuffer, r: xev.WriteError!usize) xev.CallbackAction {
            _ = c;
            const self: *@This() = @ptrCast(@alignCast(p.?));
            _ = r catch |err| {
                std.debug.print("Write error: {}\n", .{err});
                return .disarm;
            };

            self.tcp.read(l, &self.c_read, .{ .slice = &self.read_buf }, @This(), self, onRead);
            return .disarm;
        }

        fn onRead(p: ?*anyopaque, l: *xev.Loop, c: *xev.Completion, _: xev.TCP, _: xev.ReadBuffer, r: xev.ReadError!usize) xev.CallbackAction {
            _ = c;
            const self: *@This() = @ptrCast(@alignCast(p.?));
            const n = r catch |err| {
                if (err == error.EOF) {
                    self.finished = true;
                    return .disarm;
                }
                std.debug.print("Read error: {}\n", .{err});
                return .disarm;
            };

            const data = self.read_buf[0..n];
            std.debug.print("Received {d} bytes\n", .{n});
            
            _ = self.parser.feed(data, self, onBody) catch |err| {
                std.debug.print("Parse error: {}\n", .{err});
                return .disarm;
            };

            self.tcp.read(l, &self.c_read, .{ .slice = &self.read_buf }, @This(), self, onRead);
            return .disarm;
        }

        fn onBody(_: ?*anyopaque, chunk: []const u8) void {
            std.debug.print("Body chunk: {s}\n", .{chunk});
        }
    }{
        .loop = &loop,
        .tcp = &tcp,
    };

    tcp.connect(&loop, &ctx.c_connect, addr, @TypeOf(ctx), &ctx, @TypeOf(ctx).onConnect);
    try loop.run(.until_done);
}

