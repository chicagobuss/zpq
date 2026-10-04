//! Synchronous TLS-over-TCP for HTTPS.
//!
//! Combines a raw TCP socket with the BoringSSL memory-BIO client
//! from `vendor/boring_tls`: connect, handshake, send/recv plaintext,
//! close. Bytes shuttle through the BIOs synchronously, with blocking
//! socket I/O.
//!
//! Memory-BIO pattern:
//!   Read path:  Socket → BIO_write → SSL_read → application
//!   Write path: Application → SSL_write → BIO_read → socket
//!
//! Keeping TLS as a memory-BIO state machine lets the I/O scheduler
//! change without changing the TLS logic.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const boring = @import("boring_tls");

pub const Error = error{
    DnsFailed,
    SocketFailed,
    ConnectFailed,
    HandshakeFailed,
    SendFailed,
    RecvFailed,
    /// The peer reset or aborted the connection (ECONNRESET, ECONNABORTED, ETIMEDOUT on a read).
    ConnectionReset,
    Closed,
} || std.mem.Allocator.Error;

pub const Connection = struct {
    fd: posix.fd_t,
    tls: boring.tls_client.TlsClient,
    allocator: std.mem.Allocator,
    plain: bool = false,
    /// Plaintext bytes that the TLS layer decrypted but the caller's
    /// recv() buffer wasn't big enough to consume. Drained first on
    /// the next recv() call. processIncoming() clears its internal
    /// buffer on each invocation, so we must own this carry-over.
    pending: std.ArrayList(u8) = .empty,

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
        errdefer closeFd(fd);

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

    pub fn connectPlain(
        allocator: std.mem.Allocator,
        addr_v4: []const u8,
        port: u16,
    ) Error!Connection {
        const fd = try openTcp(addr_v4, port);
        return .{ .fd = fd, .tls = undefined, .allocator = allocator, .plain = true };
    }

    pub fn deinit(self: *Connection) void {
        self.pending.deinit(self.allocator);
        if (!self.plain) self.tls.deinit();
        closeFd(self.fd);
        self.* = undefined;
    }

    /// Encrypt and send plaintext. Loops until all bytes are committed
    /// to the socket.
    ///
    /// SSL_write can return 0 with WANT_WRITE when the underlying mem BIO
    /// hits internal back-pressure on a large plaintext (~MBs). In that
    /// case `consumed == 0` doesn't mean failure — it means "drain the
    /// encrypted bytes to the socket, then I can encrypt more." Only fail
    /// when neither side makes progress.
    pub fn send(self: *Connection, plaintext: []const u8) Error!void {
        if (self.plain) return writeAll(self.fd, plaintext);

        var off: usize = 0;
        while (off < plaintext.len) {
            const out = self.tls.processOutgoing(plaintext[off..]) catch return error.SendFailed;
            const drained = if (out.encrypted) |bytes| bytes.len else 0;
            if (drained > 0) try writeAll(self.fd, out.encrypted.?);
            if (out.consumed == 0 and drained == 0) return error.SendFailed;
            off += out.consumed;
        }
    }

    /// Read up to `dest.len` plaintext bytes. Returns 0 on clean EOF.
    pub fn recv(self: *Connection, dest: []u8) Error!usize {
        if (self.plain) return readSome(self.fd, dest) catch |err| return recvError(err);

        // Drain anything we previously buffered.
        if (self.pending.items.len > 0) {
            return self.consumePending(dest);
        }
        // Otherwise pull from the socket and decrypt.
        var rx_buf: [16 * 1024]u8 = undefined;
        while (true) {
            const n = readSome(self.fd, &rx_buf) catch |err| return recvError(err);
            if (n == 0) return 0; // socket closed
            if (self.tls.processIncoming(rx_buf[0..n], null) catch null) |plain| {
                if (plain.len > 0) {
                    try self.pending.appendSlice(self.allocator, plain);
                    return self.consumePending(dest);
                }
            }
            // No plaintext yet (mid-record); loop and read more.
        }
    }

    fn consumePending(self: *Connection, dest: []u8) usize {
        const n = @min(dest.len, self.pending.items.len);
        @memcpy(dest[0..n], self.pending.items[0..n]);
        // Shift the rest down. Cheap for our typical small dest sizes.
        const remaining = self.pending.items.len - n;
        if (remaining > 0) {
            std.mem.copyForwards(u8, self.pending.items[0..remaining], self.pending.items[n..]);
        }
        self.pending.shrinkRetainingCapacity(remaining);
        return n;
    }
};

// ============================================================
// Raw socket helpers
// ============================================================

fn openTcp(addr_v4: []const u8, port: u16) Error!posix.fd_t {
    // Darwin has no SOCK_CLOEXEC; set FD_CLOEXEC right after instead.
    const cloexec_flag = if (builtin.os.tag == .linux) posix.SOCK.CLOEXEC else 0;
    const r = posix.system.socket(posix.AF.INET, posix.SOCK.STREAM | cloexec_flag, posix.IPPROTO.TCP);
    if (posix.errno(r) != .SUCCESS) return error.SocketFailed;
    const fd: posix.fd_t = @intCast(r);
    errdefer closeFd(fd);
    if (builtin.os.tag != .linux) _ = posix.system.fcntl(fd, posix.F.SETFD, @as(c_int, posix.FD_CLOEXEC));

    var parts: [4]u8 = undefined;
    var i: usize = 0;
    var iter = std.mem.splitScalar(u8, addr_v4, '.');
    while (iter.next()) |p| : (i += 1) {
        if (i >= 4) return error.DnsFailed;
        parts[i] = std.fmt.parseInt(u8, p, 10) catch return error.DnsFailed;
    }
    if (i != 4) return error.DnsFailed;

    // `posix.sockaddr.in` carries Darwin's leading `len` byte and defaults `family`.
    const ip: u32 = (@as(u32, parts[0]) << 24) | (@as(u32, parts[1]) << 16) | (@as(u32, parts[2]) << 8) | parts[3];
    const addr: posix.sockaddr.in = .{
        .port = std.mem.nativeToBig(u16, port),
        .addr = std.mem.nativeToBig(u32, ip),
    };

    const cr = posix.system.connect(fd, @ptrCast(&addr), @sizeOf(posix.sockaddr.in));
    if (posix.errno(cr) != .SUCCESS) return error.ConnectFailed;
    return fd;
}

fn closeFd(fd: posix.fd_t) void {
    _ = posix.system.close(fd);
}

fn writeAll(fd: posix.fd_t, buf: []const u8) Error!void {
    var off: usize = 0;
    while (off < buf.len) {
        const r = posix.system.write(fd, buf[off..].ptr, buf.len - off);
        if (posix.errno(r) != .SUCCESS) return error.SendFailed;
        const n: usize = @intCast(r);
        if (n == 0) return error.SendFailed;
        off += n;
    }
}

fn readSome(fd: posix.fd_t, buf: []u8) error{ ReadFailed, ConnectionReset }!usize {
    const r = posix.system.read(fd, buf.ptr, buf.len);
    return switch (posix.errno(r)) {
        .SUCCESS => @intCast(r),
        .CONNRESET, .CONNABORTED, .TIMEDOUT => error.ConnectionReset,
        else => error.ReadFailed,
    };
}

fn recvError(err: error{ ReadFailed, ConnectionReset }) Error {
    return switch (err) {
        error.ConnectionReset => error.ConnectionReset,
        error.ReadFailed => error.RecvFailed,
    };
}
