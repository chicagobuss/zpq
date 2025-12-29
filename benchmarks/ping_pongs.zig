const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const Instant = std.time.Instant;
const xev = @import("xev");
const build_options = @import("build_options");
const shim = xev.shim_net;

pub const std_options: std.Options = .{
    .log_level = .info,
};

/// Build hash for reproducibility
const git_hash: []const u8 = build_options.git_hash;

pub fn main() !void {
    var thread_pool = xev.ThreadPool.init(.{});
    defer thread_pool.deinit();
    defer thread_pool.shutdown();

    if (xev.dynamic) try xev.detect();
    var loop = try xev.Loop.init(.{
        .entries = std.math.pow(u13, 2, 12),
        .thread_pool = &thread_pool,
    });
    defer loop.deinit();

    const GPA = std.heap.GeneralPurposeAllocator(.{});
    var gpa: GPA = .{};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var server_loop = try xev.Loop.init(.{
        .entries = std.math.pow(u13, 2, 12),
        .thread_pool = &thread_pool,
    });
    defer server_loop.deinit();

    var server = try Server.init(alloc, &server_loop);
    defer server.deinit();
    try server.start();

    const server_thr = try std.Thread.spawn(.{}, Server.threadMain, .{&server});

    var client_loop = try xev.Loop.init(.{
        .entries = std.math.pow(u13, 2, 12),
        .thread_pool = &thread_pool,
    });
    defer client_loop.deinit();

    var client = try Client.init(alloc, &client_loop);
    defer client.deinit();
    try client.start();

    const start_time = try Instant.now();
    try client_loop.run(.until_done);
    server_thr.join();
    const end_time = try Instant.now();

    const elapsed = @as(f64, @floatFromInt(end_time.since(start_time)));
    std.log.info("Ping-Pong Benchmark [{s}]", .{git_hash});
    std.log.info("{d:.2} roundtrips/s", .{@as(f64, @floatFromInt(client.pongs)) / (elapsed / 1e9)});
    std.log.info("{d:.2} seconds total", .{elapsed / 1e9});
}

const BufferPool = std.heap.MemoryPool([4096]u8);
const CompletionPool = std.heap.MemoryPool(xev.Completion);
const TCPPool = std.heap.MemoryPool(xev.TCP);

const Client = struct {
    loop: *xev.Loop,
    allocator: Allocator,
    completion_pool: CompletionPool,
    read_buf: [1024]u8,
    pongs: u64,
    state: usize = 0,
    stop: bool = false,

    pub const PING = "PING\n";

    pub fn init(alloc: Allocator, loop: *xev.Loop) !Client {
        return .{
            .loop = loop,
            .allocator = alloc,
            .completion_pool = CompletionPool.empty,
            .read_buf = undefined,
            .pongs = 0,
            .state = 0,
            .stop = false,
        };
    }

    pub fn deinit(self: *Client) void {
        self.completion_pool.deinit(self.allocator);
    }

    pub fn start(self: *Client) !void {
        const addr = try shim.Address.parseIp4("127.0.0.1", 3131);
        const socket = try xev.TCP.init(addr);

        const c = try self.completion_pool.create(self.allocator);
        socket.connect(self.loop, c, addr, Client, self, connectCallback);
    }

    fn connectCallback(
        self_: ?*Client,
        l: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        r: xev.ConnectError!void,
    ) xev.CallbackAction {
        _ = r catch unreachable;

        const self = self_.?;

        socket.write(l, c, .{ .slice = PING[0..PING.len] }, Client, self, writeCallback);

        const c_read = self.completion_pool.create(self.allocator) catch unreachable;
        socket.read(l, c_read, .{ .slice = &self.read_buf }, Client, self, readCallback);
        return .disarm;
    }

    fn writeCallback(
        self_: ?*Client,
        l: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        b: xev.WriteBuffer,
        r: xev.WriteError!usize,
    ) xev.CallbackAction {
        _ = r catch unreachable;
        _ = l;
        _ = s;
        _ = b;

        self_.?.completion_pool.destroy(c);
        return .disarm;
    }

    fn readCallback(
        self_: ?*Client,
        l: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        buf: xev.ReadBuffer,
        r: xev.ReadError!usize,
    ) xev.CallbackAction {
        const self = self_.?;
        const n = r catch unreachable;
        const data = buf.slice[0..n];

        var i: usize = 0;
        while (i < n) : (i += 1) {
            assert(data[i] == PING[self.state]);
            self.state = (self.state + 1) % (PING.len);
            if (self.state == 0) {
                self.pongs += 1;

                if (self.pongs > 500_000) {
                    socket.shutdown(l, c, Client, self, shutdownCallback);
                    return .disarm;
                }

                const c_ping = self.completion_pool.create(self.allocator) catch unreachable;
                socket.write(l, c_ping, .{ .slice = PING[0..PING.len] }, Client, self, writeCallback);
            }
        }

        return .rearm;
    }

    fn shutdownCallback(
        self_: ?*Client,
        l: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        r: xev.ShutdownError!void,
    ) xev.CallbackAction {
        _ = r catch {};

        const self = self_.?;
        socket.close(l, c, Client, self, closeCallback);
        return .disarm;
    }

    fn closeCallback(
        self_: ?*Client,
        l: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        r: xev.CloseError!void,
    ) xev.CallbackAction {
        _ = l;
        _ = socket;
        _ = r catch unreachable;

        const self = self_.?;
        self.stop = true;
        self.completion_pool.destroy(c);
        return .disarm;
    }
};

const Server = struct {
    loop: *xev.Loop,
    allocator: Allocator,
    buffer_pool: BufferPool,
    completion_pool: CompletionPool,
    socket_pool: TCPPool,
    stop: bool,

    pub fn init(alloc: Allocator, loop: *xev.Loop) !Server {
        return .{
            .loop = loop,
            .allocator = alloc,
            .buffer_pool = BufferPool.empty,
            .completion_pool = CompletionPool.empty,
            .socket_pool = TCPPool.empty,
            .stop = false,
        };
    }

    pub fn deinit(self: *Server) void {
        self.buffer_pool.deinit(self.allocator);
        self.completion_pool.deinit(self.allocator);
        self.socket_pool.deinit(self.allocator);
    }

    pub fn start(self: *Server) !void {
        const addr = try shim.Address.parseIp4("127.0.0.1", 3131);
        var socket = try xev.TCP.init(addr);

        const c = try self.completion_pool.create(self.allocator);
        try socket.bind(addr);
        try socket.listen(128);
        socket.accept(self.loop, c, Server, self, acceptCallback);
    }

    pub fn threadMain(self: *Server) !void {
        try self.loop.run(.until_done);
    }

    fn destroyBuf(self: *Server, buf: []const u8) void {
        self.buffer_pool.destroy(@alignCast(
            @as(*[4096]u8, @ptrFromInt(@intFromPtr(buf.ptr))),
        ));
    }

    fn acceptCallback(
        self_: ?*Server,
        l: *xev.Loop,
        c: *xev.Completion,
        r: xev.AcceptError!xev.TCP,
    ) xev.CallbackAction {
        const self = self_.?;

        const socket = self.socket_pool.create(self.allocator) catch unreachable;
        socket.* = r catch unreachable;

        const buf = self.buffer_pool.create(self.allocator) catch unreachable;
        socket.read(l, c, .{ .slice = buf }, Server, self, readCallback);
        return .disarm;
    }

    fn readCallback(
        self_: ?*Server,
        loop: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        buf: xev.ReadBuffer,
        r: xev.ReadError!usize,
    ) xev.CallbackAction {
        const self = self_.?;
        const n = r catch |err| switch (err) {
            error.EOF => {
                self.destroyBuf(buf.slice);
                socket.shutdown(loop, c, Server, self, shutdownCallback);
                return .disarm;
            },

            else => {
                self.destroyBuf(buf.slice);
                self.completion_pool.destroy(c);
                std.log.warn("server read unexpected err={}", .{err});
                return .disarm;
            },
        };

        const c_echo = self.completion_pool.create(self.allocator) catch unreachable;
        const buf_write = self.buffer_pool.create(self.allocator) catch unreachable;
        @memcpy(buf_write, buf.slice[0..n]);
        socket.write(loop, c_echo, .{ .slice = buf_write[0..n] }, Server, self, writeCallback);

        return .rearm;
    }

    fn writeCallback(
        self_: ?*Server,
        l: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        buf: xev.WriteBuffer,
        r: xev.WriteError!usize,
    ) xev.CallbackAction {
        _ = l;
        _ = s;
        _ = r catch unreachable;

        const self = self_.?;
        self.completion_pool.destroy(c);
        self.destroyBuf(buf.slice);
        return .disarm;
    }

    fn shutdownCallback(
        self_: ?*Server,
        l: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        r: xev.ShutdownError!void,
    ) xev.CallbackAction {
        _ = r catch {};

        const self = self_.?;
        s.close(l, c, Server, self, closeCallback);
        return .disarm;
    }

    fn closeCallback(
        self_: ?*Server,
        l: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        r: xev.CloseError!void,
    ) xev.CallbackAction {
        _ = l;
        _ = r catch unreachable;
        _ = socket;

        const self = self_.?;
        self.stop = true;
        self.completion_pool.destroy(c);
        return .disarm;
    }
};
