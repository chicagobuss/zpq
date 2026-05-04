//! Synchronous TLS-over-TCP for HTTPS.
//!
//! Combines a raw TCP socket with the BoringSSL memory-BIO client
//! from `vendor/boring_tls`. Phase 3 sync surface: connect, do
//! handshake, send/recv plaintext, close. No event loop yet — bytes
//! shuttle through the BIOs synchronously, with blocking socket I/O.
//!
//! Per Tier-2 grimoire on the BIO_s_mem pattern:
//!   Read path:  Socket → BIO_write → SSL_read → application
//!   Write path: Application → SSL_write → BIO_read → socket
//!
//! Future Phase 3.B: drive the same TlsClient from the epoll Loop.
//! The memory-BIO design means the TLS state machine doesn't change.

const std = @import("std");
const linux = std.os.linux;
const boring = @import("boring_tls");

pub const Error = error{
    DnsFailed,
    SocketFailed,
    ConnectFailed,
    HandshakeFailed,
    SendFailed,
    RecvFailed,
    Closed,
} || std.mem.Allocator.Error;

pub const Connection = struct {
    fd: linux.fd_t,
    tls: boring.tls_client.TlsClient,
    allocator: std.mem.Allocator,

    /// Open a TCP connection, perform a TLS handshake, return the
    /// connected client. Caller eventually calls deinit().
    ///
    /// `addr_v4` is a dotted-quad like "52.219.0.1" — the caller does
    /// DNS resolution. We use raw IPv4 sockets; v6 is a future
    /// follow-up (S3 endpoints are reachable via v4 from Lambda
    /// without exception).
    pub fn connect(
        allocator: std.mem.Allocator,
        addr_v4: []const u8,
        port: u16,
        sni_host: []const u8,
    ) Error!Connection {
        const fd = try openTcp(addr_v4, port);
        errdefer _ = linux.close(fd);

        var tls = boring.tls_client.TlsClient.init(allocator, sni_host, .{ .verify_certificate = true }) catch return error.HandshakeFailed;
        errdefer tls.deinit();

        // Drive the handshake to completion. Each iteration:
        //   1. ask the TLS client for outgoing bytes (ClientHello / Finished / ...)
        //   2. write them to the socket
        //   3. read from the socket, feed back into TLS
        // Stop when the client reports handshake complete.
        const initial = tls.startHandshake() catch return error.HandshakeFailed;
        if (initial.encrypted) |bytes| try writeAll(fd, bytes);

        var rx_buf: [16 * 1024]u8 = undefined;
        while (!tls.isHandshakeComplete()) {
            const n = readSome(fd, &rx_buf) catch return error.HandshakeFailed;
            if (n == 0) return error.HandshakeFailed;
            _ = tls.processIncoming(rx_buf[0..n], null) catch return error.HandshakeFailed;
            // After feeding bytes in, drain any outgoing handshake data.
            const out = tls.processOutgoing(null) catch return error.HandshakeFailed;
            if (out.encrypted) |bytes| try writeAll(fd, bytes);
        }

        return .{ .fd = fd, .tls = tls, .allocator = allocator };
    }

    pub fn deinit(self: *Connection) void {
        self.tls.deinit();
        _ = linux.close(self.fd);
        self.* = undefined;
    }

    /// Encrypt and send plaintext. Loops until all bytes are committed
    /// to the socket.
    pub fn send(self: *Connection, plaintext: []const u8) Error!void {
        var off: usize = 0;
        while (off < plaintext.len) {
            const out = self.tls.processOutgoing(plaintext[off..]) catch return error.SendFailed;
            if (out.encrypted) |bytes| try writeAll(self.fd, bytes);
            if (out.consumed == 0) return error.SendFailed;
            off += out.consumed;
        }
    }

    /// Read up to `dest.len` plaintext bytes. Returns 0 on clean EOF.
    pub fn recv(self: *Connection, dest: []u8) Error!usize {
        var rx_buf: [16 * 1024]u8 = undefined;
        // Try draining any plaintext already buffered in TLS first.
        if (self.tls.processIncoming(&[_]u8{}, dest) catch null) |plain| {
            if (plain.len > 0) return copyOut(dest, plain);
        }
        // Otherwise pull from the socket and feed.
        while (true) {
            const n = readSome(self.fd, &rx_buf) catch return error.RecvFailed;
            if (n == 0) return 0; // socket closed
            if (self.tls.processIncoming(rx_buf[0..n], dest) catch null) |plain| {
                if (plain.len > 0) return copyOut(dest, plain);
            }
            // No plaintext yet (mid-record); loop and read more.
        }
    }
};

fn copyOut(dest: []u8, src: []const u8) usize {
    const n = @min(dest.len, src.len);
    @memcpy(dest[0..n], src[0..n]);
    return n;
}

// ============================================================
// Raw socket helpers
// ============================================================

fn openTcp(addr_v4: []const u8, port: u16) Error!linux.fd_t {
    const r = linux.socket(linux.AF.INET, linux.SOCK.STREAM | linux.SOCK.CLOEXEC, linux.IPPROTO.TCP);
    if (errIs(r)) return error.SocketFailed;
    const fd: linux.fd_t = @intCast(@as(isize, @bitCast(r)));
    errdefer _ = linux.close(fd);

    var parts: [4]u8 = undefined;
    var i: usize = 0;
    var iter = std.mem.splitScalar(u8, addr_v4, '.');
    while (iter.next()) |p| : (i += 1) {
        if (i >= 4) return error.DnsFailed;
        parts[i] = std.fmt.parseInt(u8, p, 10) catch return error.DnsFailed;
    }
    if (i != 4) return error.DnsFailed;

    var addr = std.mem.zeroes(linux.sockaddr.in);
    addr.family = linux.AF.INET;
    addr.port = std.mem.nativeToBig(u16, port);
    const ip: u32 = (@as(u32, parts[0]) << 24) | (@as(u32, parts[1]) << 16) | (@as(u32, parts[2]) << 8) | parts[3];
    addr.addr = std.mem.nativeToBig(u32, ip);

    const cr = linux.connect(fd, @ptrCast(&addr), @sizeOf(linux.sockaddr.in));
    if (errIs(cr)) return error.ConnectFailed;
    return fd;
}

fn writeAll(fd: linux.fd_t, buf: []const u8) Error!void {
    var off: usize = 0;
    while (off < buf.len) {
        const r = linux.write(fd, buf[off..].ptr, buf.len - off);
        if (errIs(r)) return error.SendFailed;
        const n: usize = @intCast(r);
        if (n == 0) return error.SendFailed;
        off += n;
    }
}

fn readSome(fd: linux.fd_t, buf: []u8) error{ReadFailed}!usize {
    const r = linux.read(fd, buf.ptr, buf.len);
    if (errIs(r)) return error.ReadFailed;
    return @intCast(r);
}

fn errIs(r: usize) bool {
    const signed: isize = @bitCast(r);
    return signed >= -4095 and signed < 0;
}
