const std = @import("std");
const zpq = @import("zpq");
const xev_lib = @import("xev");
// Force Epoll backend for Lambda compatibility (io_uring often blocked/unavailable)
const xev = xev_lib.Epoll;
const shim = xev_lib.shim_net;

// Standard AWS Lambda Runtime API environment variable
const ENV_RUNTIME_API = "AWS_LAMBDA_RUNTIME_API";

// --- Benchmark Logic (Ported from tests/bench/ping_pongs.zig) ---
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const Instant = std.time.Instant;

const BufferPool = std.heap.MemoryPool([4096]u8);
const CompletionPool = std.heap.MemoryPool(xev.Completion);
const TCPPool = std.heap.MemoryPool(xev.TCP);
const PING = "PING\n";

const Client = struct {
    loop: *xev.Loop,
    allocator: Allocator,
    completion_pool: CompletionPool,
    read_buf: [1024]u8,
    pongs: u64,
    state: usize = 0,
    stop: bool = false,

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
        std.debug.print("Client starting connection...\n", .{});
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
        if (r) |_| {
            std.debug.print("Client connected!\n", .{});
        } else |err| {
            std.debug.print("Client connect failed: {any}\n", .{err});
            return .disarm;
        }
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
        _ = l; _ = s; _ = b;
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
        _ = buf;
        const self = self_.?;
        const n = r catch unreachable;
        // const data = buf.slice[0..n];

        var i: usize = 0;
        while (i < n) : (i += 1) {
            // assert(data[i] == PING[self.state]);
            self.state = (self.state + 1) % (PING.len);
            if (self.state == 0) {
                self.pongs += 1;
                // Run for 10k pongs for lambda bench
                if (self.pongs > 10_000) {
                    // std.debug.print("Benchmark finished!\n", .{});
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
        _ = l; _ = socket; _ = r catch unreachable;
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
        std.debug.print("Server starting listen...\n", .{});
        const addr = try shim.Address.parseIp4("127.0.0.1", 3131);
        var socket = try xev.TCP.init(addr);

        const c = try self.completion_pool.create(self.allocator);
        try socket.bind(addr);
        try socket.listen(128); 
        std.debug.print("Server listening on port 3131\n", .{});
        socket.accept(self.loop, c, Server, self, acceptCallback);
    }

    pub fn threadMain(self: *Server) !void {
        try self.loop.run(.until_done);
    }

    fn destroyBuf(self: *Server, buf: []const u8) void {
        self.buffer_pool.destroy(
            @alignCast(
                @as(*[4096]u8, @ptrFromInt(@intFromPtr(buf.ptr))),
            )
        );
    }

    fn acceptCallback(
        self_: ?*Server,
        l: *xev.Loop,
        c: *xev.Completion,
        r: xev.AcceptError!xev.TCP,
    ) xev.CallbackAction {
        if (r) |_| {
             std.debug.print("Server accepted connection!\n", .{});
        } else |err| {
             std.debug.print("Server accept failed: {any}\n", .{err});
             // Rearm accept if it failed? Or die?
             // usually recursive accept
             // But for this bench we just want one connection.
        }
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
        _ = l; _ = s; _ = r catch unreachable;
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
        _ = l; _ = r catch unreachable; _ = socket;
        const self = self_.?;
        self.stop = true;
        self.completion_pool.destroy(c);
        return .disarm;
    }
};

fn run_ping_pongs(allocator: std.mem.Allocator) ![]const u8 {
    // ThreadPool is on xev_lib, not the specific backend
    var thread_pool = xev_lib.ThreadPool.init(.{});
    defer thread_pool.deinit();
    defer thread_pool.shutdown();

    var loop = try xev.Loop.init(.{
        .entries = std.math.pow(u13, 2, 12),
        .thread_pool = &thread_pool,
    });
    defer loop.deinit();

    var server_loop = try xev.Loop.init(.{
        .entries = std.math.pow(u13, 2, 12),
        .thread_pool = &thread_pool,
    });
    defer server_loop.deinit();

    var server = try Server.init(allocator, &server_loop);
    defer server.deinit();
    try server.start();

    const server_thr = try std.Thread.spawn(.{}, Server.threadMain, .{&server});

    var client_loop = try xev.Loop.init(.{
        .entries = std.math.pow(u13, 2, 12),
        .thread_pool = &thread_pool,
    });
    defer client_loop.deinit();

    var client = try Client.init(allocator, &client_loop);
    defer client.deinit();
    try client.start();

    const start_time = try Instant.now();
    try client_loop.run(.until_done);
    // server_thr.join(); // In lambda we might need to be careful with threads joining if not stopping
    // The client loop ends when pongs > N, then it closes socket. Server gets EOF and closes.
    // So server loop should finish.
    server_thr.join();
    
    const end_time = try Instant.now();
    const elapsed = @as(f64, @floatFromInt(end_time.since(start_time)));
    const msg = try std.fmt.allocPrint(allocator, "{d:.2} roundtrips/s ({d} pongs)", .{@as(f64, @floatFromInt(client.pongs)) / (elapsed / 1e9), client.pongs});
    return msg;
}

// --- Main Bootstrap Logic ---

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const runtime_api = std.process.getEnvVarOwned(allocator, ENV_RUNTIME_API) catch |err| {
        std.debug.print("Error getting {s}: {any}\n", .{ENV_RUNTIME_API, err});
        return err;
    };
    defer allocator.free(runtime_api);

    // Create HTTP Client
    var threaded = std.Io.Threaded.init(allocator);
    defer threaded.deinit();

    var client = std.http.Client{ .allocator = allocator, .io = threaded.io() };
    defer client.deinit();

    const next_url = try std.fmt.allocPrint(allocator, "http://{s}/2018-06-01/runtime/invocation/next", .{runtime_api});
    defer allocator.free(next_url);

    while (true) {
        // 1. Get next invocation (blocking)
        var server_header_buffer: [4096]u8 = undefined;
        var req = try client.request(.GET, try std.Uri.parse(next_url), .{});
        defer req.deinit();
        
        try req.sendBodiless();
        
        var res = try req.receiveHead(&server_header_buffer);
        
        if (res.head.status != .ok) {
             std.debug.print("Runtime API Error: {d}\n", .{res.head.status});
             return error.RuntimeApiError;
        }

        var request_id: []u8 = undefined;
        var it = res.head.iterateHeaders();
        while (it.next()) |header| {
            if (std.ascii.eqlIgnoreCase(header.name, "Lambda-Runtime-Aws-Request-Id")) {
                 request_id = try allocator.dupe(u8, header.value);
                 break;
            }
        } else {
             request_id = try allocator.dupe(u8, "unknown");
        }
        defer allocator.free(request_id);

        var read_buf: [4096]u8 = undefined;
        var rdr = res.reader(&read_buf);
        const ByteList = std.ArrayList(u8);
        var body_list = try ByteList.initCapacity(allocator, 4096);
        defer body_list.deinit(allocator);
        
        while (true) {
            var temp_buf: [1024]u8 = undefined;
            const n = try rdr.readSliceShort(&temp_buf);
            if (n == 0) break;
            try body_list.appendSlice(allocator, temp_buf[0..n]);
        }
        const body = try body_list.toOwnedSlice(allocator);
        defer allocator.free(body);
        
        std.debug.print("Received invocation: {s}\n", .{request_id});

        var result_msg: []const u8 = "Benchmark completed";
        
        // Always run ping benchmark for this special bootstrap
        std.debug.print("Running internal ping-pong benchmark (libxev/epoll)...\n", .{});
        if (run_ping_pongs(allocator)) |msg| {
            result_msg = msg;
            std.debug.print("Ping-pong result: {s}\n", .{msg});
        } else |err| {
            std.debug.print("Ping-pong failed: {any}\n", .{err});
            result_msg = "Ping-pong failed";
        }

        // 3. Post Response
        const resp_url = try std.fmt.allocPrint(allocator, "http://{s}/2018-06-01/runtime/invocation/{s}/response", .{runtime_api, request_id});
        defer allocator.free(resp_url);
        
        var resp_req = try client.request(.POST, try std.Uri.parse(resp_url), .{});
        defer resp_req.deinit();
        
        resp_req.transfer_encoding = .chunked;
        var write_buf: [4096]u8 = undefined;
        var body_writer = try resp_req.sendBody(&write_buf);
        try body_writer.writer.writeAll(result_msg);
        try body_writer.end();
        // try resp_req.finish();
        
        var redirect_buf2: [1024]u8 = undefined;
        _ = try resp_req.receiveHead(&redirect_buf2);
    }
}
