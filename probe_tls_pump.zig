const std = @import("std");
const xev = @import("xev");
const shim = xev.shim_net;
const boring = @import("boring_tls");

const REMOTE_IP = "142.250.190.46"; // Google
const REMOTE_PORT = 443;
const HOSTNAME = "google.com";

const Context = struct {
    loop: *xev.Loop,
    tcp: xev.TCP,
    tls: boring.tls_client.TlsClient,
    allocator: std.mem.Allocator,
    
    c_connect: xev.Completion = .{},
    c_read: xev.Completion = .{},
    c_write: xev.Completion = .{},
    
    read_buf: [4096]u8 = undefined,
    request_sent: bool = false,
    done: bool = false,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    const addr = try shim.Address.parseIp4(REMOTE_IP, REMOTE_PORT);
    const tcp = try xev.TCP.init(addr);

    // No certificate verification for the probe to keep it simple
    var tls_client = try boring.tls_client.TlsClient.init(HOSTNAME, .{ .verify_certificate = false });
    defer tls_client.deinit();

    var ctx = Context{
        .loop = &loop,
        .tcp = tcp,
        .tls = tls_client,
        .allocator = allocator,
    };

    std.debug.print("Connecting to {s}:{d} (TLS)...\n", .{REMOTE_IP, REMOTE_PORT});
    
    ctx.tcp.connect(&loop, &ctx.c_connect, addr, Context, &ctx, onConnect);

    while (!ctx.done) {
        try loop.run(.once);
    }
}

fn onConnect(
    ctx: ?*Context,
    loop: *xev.Loop,
    c: *xev.Completion,
    s: xev.TCP,
    r: xev.ConnectError!void,
) xev.CallbackAction {
    _ = c; _ = s; _ = loop;
    const self = ctx.?;
    r catch |err| {
        std.debug.print("TCP Connect failed: {}\n", .{err});
        self.done = true;
        return .disarm;
    };
    std.debug.print("TCP Connected. Starting TLS Handshake...\n", .{});

    const out_slice = self.tls.startHandshake() catch |err| {
        std.debug.print("Start Handshake failed: {}\n", .{err});
        self.done = true;
        return .disarm;
    };
    
    if (out_slice) |data| {
        const buf = self.allocator.dupe(u8, data) catch unreachable;
        self.tcp.write(self.loop, &self.c_write, .{ .slice = buf }, Context, self, onTcpWrite);
    } else {
        pump(self);
    }

    return .disarm;
}

fn pump(self: *Context) void {
    const out_slice = self.tls.processOutgoing(null) catch |err| {
        std.debug.print("TLS processOutgoing failed: {}\n", .{err});
        self.done = true;
        return;
    };

    if (out_slice) |data| {
        const buf = self.allocator.dupe(u8, data) catch unreachable;
        self.tcp.write(self.loop, &self.c_write, .{ .slice = buf }, Context, self, onTcpWrite);
        return;
    }

    if (self.tls.handshake_complete) {
        if (!self.request_sent) {
            std.debug.print("Handshake Complete! Sending HTTP Request...\n", .{});
            const req = "GET / HTTP/1.1\r\nHost: google.com\r\nConnection: close\r\n\r\n";
            const enc_data = self.tls.processOutgoing(req) catch |err| {
                std.debug.print("Encrypt failed: {}\n", .{err});
                self.done = true;
                return;
            };
            if (enc_data) |data| {
                const buf = self.allocator.dupe(u8, data) catch unreachable;
                self.request_sent = true;
                self.tcp.write(self.loop, &self.c_write, .{ .slice = buf }, Context, self, onTcpWrite);
                return;
            }
        }
    }

    // Need more data from net
    self.tcp.read(self.loop, &self.c_read, .{ .slice = &self.read_buf }, Context, self, onTcpRead);
}

fn onTcpWrite(
    ctx: ?*Context,
    loop: *xev.Loop,
    c: *xev.Completion,
    s: xev.TCP,
    buf: xev.WriteBuffer,
    r: xev.WriteError!usize,
) xev.CallbackAction {
    _ = loop; _ = c; _ = s;
    const self = ctx.?;
    self.allocator.free(buf.slice);
    _ = r catch |err| {
        std.debug.print("TCP Write failed: {}\n", .{err});
        self.done = true;
        return .disarm;
    };
    pump(self);
    return .disarm;
}

fn onTcpRead(
    ctx: ?*Context,
    loop: *xev.Loop,
    c: *xev.Completion,
    s: xev.TCP,
    buf: xev.ReadBuffer,
    r: xev.ReadError!usize,
) xev.CallbackAction {
    _ = loop; _ = c; _ = s; _ = buf;
    const self = ctx.?;
    const n = r catch |err| {
        std.debug.print("TCP Read failed: {}\n", .{err});
        self.done = true;
        return .disarm;
    };
    if (n == 0) {
        std.debug.print("TCP EOF.\n", .{});
        self.done = true;
        return .disarm;
    }

    const decrypted = self.tls.processIncoming(self.read_buf[0..n]) catch |err| {
        std.debug.print("TLS processIncoming failed: {}\n", .{err});
        self.done = true;
        return .disarm;
    };

    if (decrypted) |pt| {
        if (std.mem.indexOf(u8, pt, "HTTP/1.1") != null) {
            std.debug.print("SUCCESS: Received HTTP Response.\n", .{});
            self.done = true;
            return .disarm;
        }
    }

    pump(self);
    return .disarm;
}

