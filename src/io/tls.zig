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
    /// The server's certificate does not chain to a trusted root or does not name the host. Not retried: another
    /// attempt meets the same certificate.
    CertificateRejected,
    SendFailed,
    RecvFailed,
    /// The peer reset or aborted the connection (ECONNRESET, ECONNABORTED, ETIMEDOUT on a read).
    ConnectionReset,
    /// The TCP stream ended without a TLS close_notify, so the data before it may have been cut short. Whether it
    /// was is for the protocol above to judge.
    UncleanClose,
    Closed,
} || std.mem.Allocator.Error;

pub const ClientContext = boring.tls_client.ClientContext;

/// An IPv4 address, in network byte order.
pub const Ipv4 = [4]u8;

/// Where to connect: a host's IPv4 addresses, tried from `first` on (wrapping) until one accepts the TCP connection.
/// A refused or unreachable address only costs a failed connect, which is the whole of the fallback: there is no
/// racing and no timeout of our own.
pub const Peer = struct {
    addrs: []const Ipv4,
    first: usize = 0,
    port: u16,

    pub fn loopback(port: u16) Peer {
        return .{ .addrs = &.{.{ 127, 0, 0, 1 }}, .port = port };
    }
};

/// A client context built on first use and shared by every connection on every thread from then on. Building one
/// parses its trust store, which for the system bundle cost ~25 ms of CPU and ~2.6 MB per connection on Lambda when
/// each connection built its own. Never freed: it lives as long as the process.
const LazyContext = struct {
    options: boring.tls_client.TlsClientOptions,
    context: ClientContext = undefined,
    ready: std.atomic.Value(bool) = .init(false),
    /// Blocking, not a spin: threads racing on a cold start sleep through the first one's build instead of burning
    /// CPU in yield loops. libc is linked anyway (getaddrinfo).
    lock: std.c.pthread_mutex_t = .{},

    fn get(self: *LazyContext) Error!*const ClientContext {
        if (self.ready.load(.acquire)) return &self.context;
        _ = std.c.pthread_mutex_lock(&self.lock);
        defer _ = std.c.pthread_mutex_unlock(&self.lock);
        if (!self.ready.load(.monotonic)) {
            self.context = ClientContext.init(self.options) catch return error.HandshakeFailed;
            self.ready.store(true, .release);
        }
        return &self.context;
    }

    fn deinitForTest(self: *LazyContext) void {
        if (self.ready.load(.monotonic)) self.context.deinit();
    }
};

var system_context: LazyContext = .{ .options = .{} };

/// The process-wide client context verifying against the system CA bundle.
pub fn sharedContext() Error!*const ClientContext {
    return system_context.get();
}

/// Amazon's five root CAs (see the file for provenance and fingerprints). AWS endpoints chain to them, and a store of
/// five loads in ~0.1 ms where the system bundle takes 20-40 ms of a Lambda cold start.
const amazon_roots = @embedFile("amazon_roots.pem");

var amazon_preference: Preference = .{ .lazy = .{ .options = .{ .ca_pem = amazon_roots } } };

/// A context to try before the system one, for the hosts it is meant for.
const Preference = struct {
    lazy: LazyContext,
    /// Set once a certificate failed against the preferred context and passed against the system one: from then on
    /// this process goes straight to the system context rather than paying two handshakes per connection.
    abandoned: std.atomic.Value(bool) = .init(false),
};

/// Whether `name` (a server name, no port) is an AWS endpoint, which the Amazon roots should verify.
fn isAwsEndpoint(name: []const u8) bool {
    const suffix = ".amazonaws.com";
    return name.len > suffix.len and std.ascii.endsWithIgnoreCase(name, suffix);
}

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
    /// The caller does DNS resolution. We use raw IPv4 sockets; v6 is a
    /// future follow-up (S3 endpoints are reachable via v4 from Lambda
    /// without exception). `host` names the server: a host name or IP
    /// literal, optionally with a `:port` (an HTTP authority).
    pub fn connect(
        allocator: std.mem.Allocator,
        peer: Peer,
        host: []const u8,
    ) Error!Connection {
        const preferred: ?*Preference = if (isAwsEndpoint(serverName(host))) &amazon_preference else null;
        return connectPreferring(allocator, preferred, &system_context, peer, host);
    }

    /// `connect` under a given client context instead of the process-wide one.
    pub fn connectWithContext(
        allocator: std.mem.Allocator,
        context: *const ClientContext,
        peer: Peer,
        host: []const u8,
    ) Error!Connection {
        return connectWith(allocator, context, peer, host, true);
    }

    /// Verify against `preferred`'s context first; a certificate it rejects gets a new connection verified against
    /// `fallback`, which a certificate both reject fails as usual. `fallback` is only built if it is needed.
    fn connectPreferring(
        allocator: std.mem.Allocator,
        preferred: ?*Preference,
        fallback: *LazyContext,
        peer: Peer,
        host: []const u8,
    ) Error!Connection {
        const pref = preferred orelse return connectWith(allocator, try fallback.get(), peer, host, true);
        if (pref.abandoned.load(.monotonic))
            return connectWith(allocator, try fallback.get(), peer, host, true);
        if (connectWith(allocator, try pref.lazy.get(), peer, host, false)) |conn| {
            return conn;
        } else |err| if (err != error.CertificateRejected) return err;
        const conn = try connectWith(allocator, try fallback.get(), peer, host, true);
        if (!pref.abandoned.swap(true, .monotonic)) {
            std.log.warn("TLS: {s} does not chain to the built-in Amazon roots; using the system CA bundle", .{
                serverName(host),
            });
        }
        return conn;
    }

    fn connectWith(
        allocator: std.mem.Allocator,
        context: *const ClientContext,
        peer: Peer,
        host: []const u8,
        log_rejection: bool,
    ) Error!Connection {
        const fd = try openTcpAny(peer);
        errdefer closeFd(fd);

        var tls = boring.tls_client.TlsClient.init(allocator, context, serverName(host)) catch |err|
            return handshakeError(err);
        errdefer tls.deinit();
        handshake(fd, &tls) catch |err| {
            if (log_rejection and err == error.CertificateRejected) {
                const reason = boring.tls.c.X509_verify_cert_error_string(boring.tls.c.SSL_get_verify_result(tls.ssl));
                std.log.warn("TLS: certificate of {s} rejected: {s}", .{ serverName(host), std.mem.span(reason) });
            }
            return err;
        };
        return .{ .fd = fd, .tls = tls, .allocator = allocator };
    }

    /// Drive the handshake to completion. Each iteration:
    ///   1. ask the TLS client for outgoing bytes (ClientHello / Finished / ...)
    ///   2. write them to the socket
    ///   3. read from the socket, feed back into TLS
    /// Stop when the client reports handshake complete.
    fn handshake(fd: posix.fd_t, tls: *boring.tls_client.TlsClient) Error!void {
        const initial = tls.startHandshake() catch |err| return handshakeError(err);
        if (initial.encrypted) |bytes| try writeAll(fd, bytes);

        var rx_buf: [16 * 1024]u8 = undefined;
        while (!tls.isHandshakeComplete()) {
            const n = readSome(fd, &rx_buf) catch return error.HandshakeFailed;
            if (n == 0) return error.HandshakeFailed;
            _ = tls.processIncoming(rx_buf[0..n], null) catch |err| return handshakeError(err);
            // After feeding bytes in, drain any outgoing handshake data.
            const out = tls.processOutgoing(null) catch |err| return handshakeError(err);
            if (out.encrypted) |bytes| try writeAll(fd, bytes);
        }
    }

    pub fn connectPlain(allocator: std.mem.Allocator, peer: Peer) Error!Connection {
        const fd = try openTcpAny(peer);
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

    /// Read up to `dest.len` plaintext bytes. Returns 0 at the end of the stream: over TLS only once the peer has sent
    /// close_notify; the TCP stream ending without one is `error.UncleanClose`, and data that does not decrypt is
    /// `error.RecvFailed`.
    pub fn recv(self: *Connection, dest: []u8) Error!usize {
        if (self.plain) return readSome(self.fd, dest) catch |err| return recvError(err);

        // Drain anything we previously buffered.
        if (self.pending.items.len > 0) {
            return self.consumePending(dest);
        }
        // Otherwise pull from the socket and decrypt.
        var rx_buf: [16 * 1024]u8 = undefined;
        // The first pass feeds TLS nothing and reports what it already holds. A socket read that brought plaintext
        // followed by a corrupt record or a close_notify left that outcome inside the TLS state (BoringSSL keeps a
        // read error), where reading the socket again would miss it, or hang if the peer held the connection open.
        var input: []const u8 = &.{};
        while (true) {
            const decrypted = self.tls.processIncoming(input, null) catch |err| switch (err) {
                error.TlsConnectionClosed => return 0,
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.RecvFailed,
            };
            if (decrypted) |plain| {
                if (plain.len > 0) {
                    try self.pending.appendSlice(self.allocator, plain);
                    return self.consumePending(dest);
                }
            }
            // No plaintext yet (mid-record); read more.
            const n = readSome(self.fd, &rx_buf) catch |err| return recvError(err);
            if (n == 0) return error.UncleanClose;
            input = rx_buf[0..n];
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

fn handshakeError(err: anyerror) Error {
    return switch (err) {
        error.CertificateVerificationFailed => error.CertificateRejected,
        error.OutOfMemory => error.OutOfMemory,
        else => error.HandshakeFailed,
    };
}

// ============================================================
// Raw socket helpers
// ============================================================

fn openTcpAny(peer: Peer) Error!posix.fd_t {
    if (peer.addrs.len == 0) return error.DnsFailed;
    var last: Error = error.ConnectFailed;
    for (0..peer.addrs.len) |k| {
        const addr = peer.addrs[(peer.first +% k) % peer.addrs.len];
        return openTcp(addr, peer.port) catch |err| switch (err) {
            error.ConnectFailed => {
                last = err;
                continue;
            },
            else => return err,
        };
    }
    return last;
}

fn openTcp(ip: Ipv4, port: u16) Error!posix.fd_t {
    // Darwin has no SOCK_CLOEXEC; set FD_CLOEXEC right after instead.
    const cloexec_flag = if (builtin.os.tag == .linux) posix.SOCK.CLOEXEC else 0;
    const r = posix.system.socket(posix.AF.INET, posix.SOCK.STREAM | cloexec_flag, posix.IPPROTO.TCP);
    if (posix.errno(r) != .SUCCESS) return error.SocketFailed;
    const fd: posix.fd_t = @intCast(r);
    errdefer closeFd(fd);
    if (builtin.os.tag != .linux) _ = posix.system.fcntl(fd, posix.F.SETFD, @as(c_int, posix.FD_CLOEXEC));

    // `posix.sockaddr.in` carries Darwin's leading `len` byte and defaults `family`.
    const addr: posix.sockaddr.in = .{
        .port = std.mem.nativeToBig(u16, port),
        .addr = @bitCast(ip),
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

/// The TLS server name in `host`, a host name or IP literal optionally written as an HTTP authority: `:port`, IPv6
/// brackets and a trailing dot are dropped (`minio.local:9000` -> `minio.local`, `[::1]:9000` -> `::1`).
pub fn serverName(host: []const u8) []const u8 {
    var h = host;
    if (h.len > 0 and h[0] == '[') {
        const close = std.mem.indexOfScalar(u8, h, ']') orelse return h;
        return h[1..close];
    }
    // More than one colon is a bare IPv6 literal, not a host and port.
    if (std.mem.indexOfScalar(u8, h, ':')) |colon| {
        if (std.mem.indexOfScalarPos(u8, h, colon + 1, ':') == null) h = h[0..colon];
    }
    if (h.len > 1 and h[h.len - 1] == '.') h = h[0 .. h.len - 1];
    return h;
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;
const c = boring.tls.c;
const test_server = @import("tls_test_server.zig");
const retry = @import("retry.zig");

test "serverName drops the port, IPv6 brackets and a trailing dot" {
    try testing.expectEqualStrings("s3.us-east-1.amazonaws.com", serverName("s3.us-east-1.amazonaws.com"));
    try testing.expectEqualStrings("minio.local", serverName("minio.local:9000"));
    try testing.expectEqualStrings("minio.local", serverName("minio.local.:9000"));
    try testing.expectEqualStrings("10.0.0.5", serverName("10.0.0.5:9000"));
    try testing.expectEqualStrings("::1", serverName("[::1]:9000"));
    try testing.expectEqualStrings("::1", serverName("[::1]"));
    try testing.expectEqualStrings("fe80::1", serverName("fe80::1"));
}

test "SNI carries the bare host name, and nothing for an IP literal" {
    var pki = try test_server.Pki.init();
    defer pki.deinit();
    var leaf = try pki.issue("sni.test", "DNS:sni.test,IP:127.0.0.1");
    defer leaf.deinit();

    // SNI alone is under test here; the certificate checks have their own test.
    var context = try ClientContext.init(.{ .verify_certificate = false });
    defer context.deinit();

    const Case = struct { host: []const u8, sni: ?[]const u8 };
    for ([_]Case{
        .{ .host = "sni.test", .sni = "sni.test" },
        .{ .host = "sni.test:9000", .sni = "sni.test" },
        .{ .host = "127.0.0.1", .sni = null },
        .{ .host = "127.0.0.1:9000", .sni = null },
        .{ .host = "[::1]:9000", .sni = null },
    }) |case| {
        var server: test_server.Server = undefined;
        try server.start(&leaf, 1);
        var conn = Connection.connectWithContext(testing.allocator, &context, .loopback(server.port), case.host);
        if (conn) |*cn| cn.deinit() else |_| {}
        try server.finish();
        _ = try conn;
        try testing.expect(server.handshake_ok);
        if (case.sni) |want| {
            try testing.expectEqualStrings(want, server.sni() orelse "(none)");
        } else if (server.sni()) |got| {
            std.debug.print("host {s}: unexpected SNI {s}\n", .{ case.host, got });
            return error.TestUnexpectedResult;
        }
    }
}

const Verdict = enum { accepted, rejected };

/// Connect to a server presenting `leaf` under the name `host`, trusting `ca`, and report whether the client took it.
fn handshakeVerdict(ca: *boring.tls.c.X509, leaf: *const test_server.Leaf, host: []const u8) !Verdict {
    var server: test_server.Server = undefined;
    try server.start(leaf, 1);
    var context = try ClientContext.init(.{ .ca_cert = ca });
    defer context.deinit();
    var conn = Connection.connectWithContext(testing.allocator, &context, .loopback(server.port), host);
    if (conn) |*cn| cn.deinit() else |_| {}
    try server.finish();
    _ = conn catch |err| switch (err) {
        error.CertificateRejected => return .rejected,
        else => return err,
    };
    try testing.expect(server.handshake_ok);
    return .accepted;
}

test "the server certificate must name the host it was reached by" {
    var pki = try test_server.Pki.init();
    defer pki.deinit();
    const Case = struct { cn: [:0]const u8 = "leaf", san: ?[:0]const u8, host: []const u8, want: Verdict };
    for ([_]Case{
        .{ .san = "DNS:good.test", .host = "good.test", .want = .accepted },
        .{ .san = "DNS:good.test", .host = "good.test:9000", .want = .accepted },
        .{ .san = "DNS:good.test", .host = "GOOD.test.", .want = .accepted },
        // A valid certificate for some other name: the man in the middle.
        .{ .san = "DNS:other.test", .host = "good.test", .want = .rejected },
        .{ .san = "DNS:other.test", .host = "good.test:9000", .want = .rejected },
        // The subject CN is not a name: only subjectAltNames count.
        .{ .cn = "good.test", .san = "DNS:other.test", .host = "good.test", .want = .rejected },
        .{ .cn = "good.test", .san = null, .host = "good.test", .want = .rejected },
        // A wildcard covers exactly one label, which is why dotted S3 buckets go path-style.
        .{ .san = "DNS:*.s3.test", .host = "bucket.s3.test", .want = .accepted },
        .{ .san = "DNS:*.s3.test", .host = "my.bucket.s3.test", .want = .rejected },
        // An IP endpoint matches an IP subjectAltName only, never a DNS name spelling the address.
        .{ .san = "IP:127.0.0.1", .host = "127.0.0.1", .want = .accepted },
        .{ .san = "DNS:good.test,IP:127.0.0.1", .host = "127.0.0.1:9000", .want = .accepted },
        .{ .san = "DNS:127.0.0.1", .host = "127.0.0.1", .want = .rejected },
        .{ .san = "IP:127.0.0.2", .host = "127.0.0.1", .want = .rejected },
        .{ .san = "IP:127.0.0.1", .host = "good.test", .want = .rejected },
    }) |case| {
        var leaf = try pki.issue(case.cn, case.san);
        defer leaf.deinit();
        const got = try handshakeVerdict(pki.ca, &leaf, case.host);
        if (got != case.want) {
            const fmt = "cn={s} san={s} host={s}: {t}, want {t}\n";
            std.debug.print(fmt, .{ case.cn, case.san orelse "-", case.host, got, case.want });
            return error.TestUnexpectedResult;
        }
    }
}

test "a certificate from an untrusted CA is rejected whatever it names" {
    var pki = try test_server.Pki.init();
    defer pki.deinit();
    var stranger = try test_server.Pki.init();
    defer stranger.deinit();
    var leaf = try stranger.issue("good.test", "DNS:good.test");
    defer leaf.deinit();
    try testing.expectEqual(Verdict.rejected, try handshakeVerdict(pki.ca, &leaf, "good.test"));
}

test "every connection on every thread shares one client context" {
    const Worker = struct {
        fn run(out: *?*c.SSL_CTX) void {
            const context = sharedContext() catch return;
            var client = boring.tls_client.TlsClient.init(testing.allocator, context, "s3.test") catch return;
            defer client.deinit();
            out.* = c.SSL_get_SSL_CTX(client.ssl);
        }
    };
    // Eight threads at once, as a cold pool fill opens its connections.
    var got: [8]?*c.SSL_CTX = @splat(null);
    var threads: [8]std.Thread = undefined;
    for (&threads, &got) |*t, *g| t.* = try std.Thread.spawn(.{}, Worker.run, .{g});
    for (threads) |t| t.join();
    const want = (try sharedContext()).ctx;
    for (got) |g| try testing.expectEqual(@as(?*c.SSL_CTX, want), g);
    try testing.expectEqual(want, (try sharedContext()).ctx);
}

test "the embedded Amazon roots are the five pinned certificates" {
    const want = [_][]const u8{
        "8ECDE6884F3D87B1125BA31AC3FCB13D7016DE7F57CC904FE1CB97C6AE98196E", // Amazon Root CA 1
        "1BA5B2AA8C65401A82960118F80BEC4F62304D83CEC4713A19C39C011EA46DB4", // Amazon Root CA 2
        "18CE6CFE7BF14E60B2E347B8DFE868CB31D02EBB3ADA271569F50343B46DB3A4", // Amazon Root CA 3
        "E35D28419ED02025CFA69038CD623962458DA5C695FBDEA3C22B0BFB25897092", // Amazon Root CA 4
        "568D6905A2C88708A4B3025190EDCFEDB1974A606A13C6E5290FCB2AE63EDAB5", // Starfield Services Root CA - G2
    };
    const bio = c.BIO_new_mem_buf(amazon_roots.ptr, @intCast(amazon_roots.len)).?;
    defer _ = c.BIO_free(bio);
    var n: usize = 0;
    while (c.PEM_read_bio_X509(bio, null, null, null)) |x| : (n += 1) {
        defer c.X509_free(x);
        var md: [32]u8 = undefined;
        var md_len: c_uint = 0;
        try testing.expect(c.X509_digest(x, c.EVP_sha256(), &md, &md_len) == 1);
        try testing.expect(n < want.len);
        var hex: [64]u8 = undefined;
        _ = try std.fmt.bufPrint(&hex, "{X}", .{&md});
        try testing.expectEqualStrings(want[n], &hex);
    }
    c.ERR_clear_error();
    try testing.expectEqual(want.len, n);
    var context = try ClientContext.init(.{ .ca_pem = amazon_roots });
    context.deinit();
}

test "only AWS endpoints prefer the Amazon roots" {
    for ([_][]const u8{ "bkt.s3.us-west-2.amazonaws.com", "s3.us-west-2.amazonaws.com", "S3.AMAZONAWS.COM" }) |h|
        try testing.expect(isAwsEndpoint(h));
    for ([_][]const u8{
        "acct.r2.cloudflarestorage.com",
        "amazonaws.com",
        ".amazonaws.com",
        "evil-amazonaws.com",
        "s3.amazonaws.com.evil.test",
        "s3.cn-north-1.amazonaws.com.cn",
        "127.0.0.1",
        "localhost",
    }) |h| try testing.expect(!isAwsEndpoint(h));
}

test "a certificate the preferred roots reject is verified again against the system ones" {
    var pki = try test_server.Pki.init();
    defer pki.deinit();
    var other = try test_server.Pki.init();
    defer other.deinit();
    var fallback: LazyContext = .{ .options = .{ .ca_cert = pki.ca } };
    defer fallback.deinitForTest();
    const host = "bkt.s3.us-west-2.amazonaws.com";

    // Preferred roots that verify it: one connection, and the fallback is never built.
    {
        var leaf = try pki.issue("leaf", "DNS:" ++ host);
        defer leaf.deinit();
        var pref: Preference = .{ .lazy = .{ .options = .{ .ca_cert = pki.ca } } };
        defer pref.lazy.deinitForTest();
        var server: test_server.Server = undefined;
        try server.start(&leaf, 1);
        var conn = Connection.connectPreferring(testing.allocator, &pref, &fallback, .loopback(server.port), host);
        if (conn) |*cn| cn.deinit() else |_| {}
        try server.finish();
        _ = try conn;
        try testing.expect(!pref.abandoned.load(.monotonic));
        try testing.expect(!fallback.ready.load(.monotonic));
    }
    // Preferred roots that do not: a second connection under the fallback, which later connections go to directly.
    {
        var leaf = try pki.issue("leaf", "DNS:" ++ host);
        defer leaf.deinit();
        var pref: Preference = .{ .lazy = .{ .options = .{ .ca_cert = other.ca } } };
        defer pref.lazy.deinitForTest();
        var server: test_server.Server = undefined;
        try server.start(&leaf, 3);
        for (0..2) |_| {
            const port = server.port;
            var conn = try Connection.connectPreferring(testing.allocator, &pref, &fallback, .loopback(port), host);
            conn.deinit();
            try testing.expect(pref.abandoned.load(.monotonic));
        }
        try server.finish();
        try testing.expect(server.handshake_ok);
    }
    // A certificate neither trusts, or one naming another host, still fails.
    {
        var stranger = try test_server.Pki.init();
        defer stranger.deinit();
        var leaf = try stranger.issue("leaf", "DNS:" ++ host);
        defer leaf.deinit();
        var pref: Preference = .{ .lazy = .{ .options = .{ .ca_cert = other.ca } } };
        defer pref.lazy.deinitForTest();
        var server: test_server.Server = undefined;
        try server.start(&leaf, 2);
        const conn = Connection.connectPreferring(testing.allocator, &pref, &fallback, .loopback(server.port), host);
        try server.finish();
        try testing.expectError(error.CertificateRejected, conn);
    }
}

test "a handshake that fails before any certificate is a retryable failure, not a rejected certificate" {
    var pki = try test_server.Pki.init();
    defer pki.deinit();
    var leaf = try pki.issue("leaf", "DNS:bkt.s3.us-west-2.amazonaws.com");
    defer leaf.deinit();
    var fallback: LazyContext = .{ .options = .{ .ca_cert = pki.ca } };
    defer fallback.deinitForTest();
    var pref: Preference = .{ .lazy = .{ .options = .{ .ca_cert = pki.ca } } };
    defer pref.lazy.deinitForTest();

    // The server answers the ClientHello with a handshake_failure alert.
    var server: test_server.Server = undefined;
    // Two accepts: a mistaken fallback to the system roots gets its second connection refused the same way.
    try server.startWith(&leaf, 2, .alert);
    const conn = Connection.connectPreferring(
        testing.allocator,
        &pref,
        &fallback,
        .loopback(server.port),
        "bkt.s3.us-west-2.amazonaws.com",
    );
    try server.finish();
    try testing.expectError(error.HandshakeFailed, conn);
    try testing.expectEqual(@as(usize, 1), server.accepted);
    try testing.expectEqual(retry.Failure.transient, retry.classifyConnect(error.HandshakeFailed));
    // Nothing was learnt about the Amazon roots.
    try testing.expect(!pref.abandoned.load(.monotonic));
    try testing.expect(!fallback.ready.load(.monotonic));
}
