const std = @import("std");
const xev = @import("xev");
const shim = xev.shim_net;
const boring = @import("boring_tls");

// 142.250.190.46 (Google)
const REMOTE_IP = "142.250.190.46"; 
const REMOTE_PORT = 443;
const HOSTNAME = "google.com";

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // 1. Setup Loop
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    // 2. Setup TCP
    const addr = try shim.Address.parseIp4(REMOTE_IP, REMOTE_PORT);
    const tcp = try xev.TCP.init(addr);

    // 3. Setup TLS
    // Use verify_certificate = false to avoid CA bundle complexity in microtest for now.
    // Ideally we load system certs, but that's platform specific logic we want to avoid in this specific test if possible.
    var tls_client = try boring.tls_client.TlsClient.init(HOSTNAME, .{ .verify_certificate = false }); 
    defer tls_client.deinit();

    var ctx = Context{
        .loop = &loop,
        .tcp = tcp,
        .tls = &tls_client,
        .allocator = allocator,
    };

    std.debug.print("Connecting to {s}:{d}...\n", .{REMOTE_IP, REMOTE_PORT});

    // 4. Connect TCP
    ctx.tcp.connect(&loop, &ctx.c_connect, addr, Context, &ctx, onConnect);

    try loop.run(.until_done);
}

const Context = struct {
    loop: *xev.Loop,
    tcp: xev.TCP,
    tls: *boring.tls_client.TlsClient,
    allocator: std.mem.Allocator,
    
    // Completions
    c_connect: xev.Completion = .{},
    c_read: xev.Completion = .{},
    c_write: xev.Completion = .{},

    // Buffers
    read_buf: [4096]u8 = undefined,
    
    // State
    handshake_done: bool = false,
    request_sent: bool = false,
};

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
        return .disarm;
    };
    std.debug.print("TCP Connected. Starting TLS Handshake...\n", .{});

    // Start Handshake
    // We initiate by asking TLS if it has anything to say.
    // tls_client.startHandshake() calls processOutgoing(null) internally.
    const out_slice = self.tls.startHandshake() catch |err| {
        std.debug.print("Start Handshake failed: {}\n", .{err});
        return .disarm;
    };
    
    // If startHandshake produced bytes (ClientHello), send them.
    if (out_slice) |data| {
        std.debug.print("TLS ClientHello ({} bytes)\n", .{data.len});
        const buf = self.allocator.dupe(u8, data) catch unreachable;
        self.tcp.write(self.loop, &self.c_write, .{ .slice = buf }, Context, self, onTcpWrite);
        return .disarm;
    }

    // If no bytes generated immediately (unlikely for ClientHello), pump.
    return pumpTls(self);
}

fn pumpTls(self: *Context) xev.CallbackAction {
    // 1. Process Outgoing (Check if TLS has bytes to send to net)
    const out_slice = self.tls.processOutgoing(null) catch |err| {
        std.debug.print("TLS processOutgoing failed: {}\n", .{err});
        return .disarm;
    };

    if (out_slice) |data| {
        // We have data to write to TCP
        std.debug.print("TLS wants to write {} bytes to TCP\n", .{data.len});
        const buf = self.allocator.dupe(u8, data) catch unreachable;
        self.tcp.write(self.loop, &self.c_write, .{ .slice = buf }, Context, self, onTcpWrite);
        return .disarm;
    }

    // 2. If no output, do we need input?
    if (self.tls.handshake_complete) {
        if (!self.request_sent) {
            std.debug.print("Handshake Complete! Sending HTTP Request...\n", .{});
            const req = "GET / HTTP/1.1\r\nHost: google.com\r\nConnection: close\r\n\r\n";
            // Encrypt request
            const enc_data = self.tls.processOutgoing(req) catch |err| {
                 std.debug.print("Encrypt failed: {}\n", .{err});
                 return .disarm;
            };
            if (enc_data) |data| {
                const buf = self.allocator.dupe(u8, data) catch unreachable;
                self.request_sent = true;
                self.tcp.write(self.loop, &self.c_write, .{ .slice = buf }, Context, self, onTcpWrite);
                return .disarm;
            }
        }
        
        // If request sent, we are waiting for response.
        std.debug.print("Waiting for HTTP Response...\n", .{});
        self.tcp.read(self.loop, &self.c_read, .{ .slice = &self.read_buf }, Context, self, onTcpRead);
        return .disarm;
    }

    // Handshake not complete, and no output -> Must need input.
    std.debug.print("TLS Handshake needs input. Reading from TCP...\n", .{});
    self.tcp.read(self.loop, &self.c_read, .{ .slice = &self.read_buf }, Context, self, onTcpRead);
    return .disarm;
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
    // Free the dupe
    self.allocator.free(buf.slice);

    _ = r catch |err| {
        std.debug.print("TCP Write failed: {}\n", .{err});
        return .disarm;
    };
    
    // Pump again
    return pumpTls(self);
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
        return .disarm;
    };
    if (n == 0) {
        std.debug.print("TCP EOF.\n", .{});
        return .disarm;
    }
    std.debug.print("TCP Read {} bytes.\n", .{n});

    // Feed to TLS
    const decrypted = self.tls.processIncoming(self.read_buf[0..n]) catch |err| {
        std.debug.print("TLS processIncoming failed: {}\n", .{err});
        return .disarm;
    };

    if (decrypted) |pt| {
        std.debug.print("Decrypted {} bytes:\n{s}\n", .{pt.len, pt});
        if (std.mem.indexOf(u8, pt, "HTTP/1.1") != null) {
            std.debug.print("SUCCESS: Received HTTP Response.\n", .{});
            return .disarm;
        }
    }

    // Pump again (might have output to send, or need more input)
    return pumpTls(self);
}

