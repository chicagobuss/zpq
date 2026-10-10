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
//!
//! Every network wait has a deadline (`Timeouts`): connecting and the TLS handshake each as a whole, the first byte
//! of a response after the request went out, and silence while reading or sending. The last two are idle timeouts,
//! reset by every byte that moves, so a slow transfer that keeps moving is never cut off. A stalled peer that keeps
//! its connection open would otherwise block a read forever (and with it the whole query).

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const boring = @import("boring_tls");
const clock = @import("../clock.zig");

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
    /// No TCP connection within `Timeouts.connect_ms`.
    ConnectTimeout,
    /// The TLS handshake did not complete within `Timeouts.handshake_ms`.
    HandshakeTimeout,
    /// No byte of a response within `Timeouts.first_byte_ms` of sending the request.
    FirstByteTimeout,
    /// No further byte of a response for `Timeouts.idle_ms`.
    ReadTimeout,
    /// The peer took no byte of what was being sent for `Timeouts.idle_ms`.
    SendTimeout,
} || std.mem.Allocator.Error;

/// How long a connection waits on the network. The defaults, and the environment variables that override them (in
/// milliseconds, read by `fromEnv`):
///
///   connect_ms     5 000   ZPQ_S3_CONNECT_TIMEOUT_MS      TCP connect, per address tried
///   handshake_ms   5 000   ZPQ_S3_HANDSHAKE_TIMEOUT_MS    the whole TLS handshake
///   first_byte_ms 10 000   ZPQ_S3_FIRST_BYTE_TIMEOUT_MS   from the request sent to the first byte of its response
///   idle_ms       10 000   ZPQ_S3_IDLE_TIMEOUT_MS         silence while reading a response or sending a request
///
/// S3 answers its first byte in tens of milliseconds and moves 8 MiB in about 0.1 s even under Lambda's network
/// policer, so ten seconds without a byte is a stalled connection, not a slow one; each extra second waited is billed
/// on Lambda. A timeout fails the attempt; the request layer retries it on a new connection (`retry.classify`).
pub const Timeouts = struct {
    connect_ms: u32 = 5_000,
    handshake_ms: u32 = 5_000,
    first_byte_ms: u32 = 10_000,
    idle_ms: u32 = 10_000,
    /// Test-only. When set, read and write deadlines use this millisecond clock instead of wall time, and waits
    /// poll so a test can expire a stalled attempt by advancing the clock. Production leaves it null.
    clock_ms: ?*const fn () u64 = null,

    /// The defaults, each overridden by its environment variable when that holds a positive integer.
    pub fn fromEnv(env: std.process.Environ) Timeouts {
        var t: Timeouts = .{};
        inline for (.{
            .{ "connect_ms", "ZPQ_S3_CONNECT_TIMEOUT_MS" },
            .{ "handshake_ms", "ZPQ_S3_HANDSHAKE_TIMEOUT_MS" },
            .{ "first_byte_ms", "ZPQ_S3_FIRST_BYTE_TIMEOUT_MS" },
            .{ "idle_ms", "ZPQ_S3_IDLE_TIMEOUT_MS" },
        }) |f| {
            if (env.getPosix(f[1])) |v| {
                const n = std.fmt.parseInt(u32, std.mem.trim(u8, v, " "), 10) catch 0;
                if (n > 0) @field(t, f[0]) = n;
            }
        }
        return t;
    }
};

pub const ClientContext = boring.tls_client.ClientContext;

/// An IPv4 address, in network byte order.
pub const Ipv4 = [4]u8;

/// Where to connect: a host's IPv4 addresses, tried from `first` on (wrapping) until one accepts the TCP connection.
/// A refused, unreachable or silent address (`Timeouts.connect_ms`) only costs a failed connect, which is the whole
/// of the fallback: there is no racing. `deadline_ms`, when set, is one absolute bound over every address and over
/// both TLS contexts; each stage then waits only `min(its cap, the time left)`.
pub const Peer = struct {
    addrs: []const Ipv4,
    first: usize = 0,
    port: u16,
    /// The connection's deadlines, from connecting on.
    timeouts: Timeouts = .{},
    /// Absolute millisecond deadline (`monoMs`) for this whole connect. Null: each stage uses only its own cap.
    deadline_ms: ?u64 = null,

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
    timeouts: Timeouts = .{},
    /// A request has gone out and no byte of its response has come back: a read waits `first_byte_ms`, not `idle_ms`.
    awaiting_response: bool = false,
    /// The socket's SO_RCVTIMEO and SO_SNDTIMEO as last set, so each is only set again when it changes.
    sock: SockTimeouts = .{},

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
    /// `fallback`, which a certificate both reject fails as usual. `fallback` is only built if it is needed. Both
    /// attempts share `peer.deadline_ms`, so a fallback does not start its own second.
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
        var opts: SockTimeouts = .{};
        const budget = stageBudget(peer.timeouts.handshake_ms, peer.deadline_ms);
        if (peer.deadline_ms != null and budget == 0) return error.HandshakeTimeout;
        handshake(fd, &tls, budget, &opts) catch |err| {
            if (log_rejection and err == error.CertificateRejected) {
                const reason = boring.tls.c.X509_verify_cert_error_string(boring.tls.c.SSL_get_verify_result(tls.ssl));
                std.log.warn("TLS: certificate of {s} rejected: {s}", .{ serverName(host), std.mem.span(reason) });
            }
            return err;
        };
        return .{
            .fd = fd,
            .tls = tls,
            .allocator = allocator,
            .timeouts = peer.timeouts,
            .sock = opts,
        };
    }

    /// Drive the handshake to completion, within `budget_ms` in all. Each iteration:
    ///   1. ask the TLS client for outgoing bytes (ClientHello / Finished / ...)
    ///   2. write them to the socket
    ///   3. read from the socket, feed back into TLS
    /// Stop when the client reports handshake complete.
    fn handshake(fd: posix.fd_t, tls: *boring.tls_client.TlsClient, budget_ms: u32, opts: *SockTimeouts) Error!void {
        // One deadline for the whole stage: every read and write gets only what is left of it.
        const deadline = monoMs() + budget_ms;
        const initial = tls.startHandshake() catch |err| return handshakeError(err);
        if (initial.encrypted) |bytes|
            writeAllBy(fd, bytes, budget_ms, deadline, opts) catch |err| return handshakeIo(err);

        var rx_buf: [16 * 1024]u8 = undefined;
        while (!tls.isHandshakeComplete()) {
            const n = readBy(fd, &rx_buf, deadline, opts) catch |err| return if (err == error.WouldBlock)
                error.HandshakeTimeout
            else
                error.HandshakeFailed;
            if (n == 0) return error.HandshakeFailed;
            _ = tls.processIncoming(rx_buf[0..n], null) catch |err| return handshakeError(err);
            // After feeding bytes in, drain any outgoing handshake data.
            const out = tls.processOutgoing(null) catch |err| return handshakeError(err);
            if (out.encrypted) |bytes|
                writeAllBy(fd, bytes, budget_ms, deadline, opts) catch |err| return handshakeIo(err);
        }
    }

    fn handshakeIo(err: WriteError) Error {
        return if (err == error.WouldBlock) error.HandshakeTimeout else error.SendFailed;
    }

    pub fn connectPlain(allocator: std.mem.Allocator, peer: Peer) Error!Connection {
        const fd = try openTcpAny(peer);
        return .{ .fd = fd, .tls = undefined, .allocator = allocator, .plain = true, .timeouts = peer.timeouts };
    }

    /// Use `t` from the next request on: a pooled connection takes the timeouts of the query that reuses it. The
    /// socket options follow at the next read or write.
    pub fn setTimeouts(self: *Connection, t: Timeouts) void {
        self.timeouts = t;
    }

    /// Read from the socket, waiting at most `first_byte_ms` for a response's first byte, else `idle_ms`.
    fn readSocket(self: *Connection, buf: []u8) Error!usize {
        self.sock.clock_ms = self.timeouts.clock_ms;
        const wait = if (self.awaiting_response) self.timeouts.first_byte_ms else self.timeouts.idle_ms;
        const n = readBy(self.fd, buf, nowMs(self.sock.clock_ms) + wait, &self.sock) catch |err| return self.recvError(err);
        if (n > 0) self.awaiting_response = false;
        return n;
    }

    /// Write all of `buf`, failing with `error.SendTimeout` once `idle_ms` pass with no byte taken.
    fn writeSocket(self: *Connection, buf: []const u8) Error!void {
        self.sock.clock_ms = self.timeouts.clock_ms;
        writeAllBy(self.fd, buf, self.timeouts.idle_ms, null, &self.sock) catch |err| return sendError(err);
    }

    /// A socket read that timed out, named for what it was waiting for.
    fn readTimeout(self: *const Connection) Error {
        return if (self.awaiting_response) error.FirstByteTimeout else error.ReadTimeout;
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
    ///
    /// Sending is what a request does, so afterwards the next read waits for the first byte of its response.
    pub fn send(self: *Connection, plaintext: []const u8) Error!void {
        self.awaiting_response = true;
        if (self.plain) return self.writeSocket(plaintext);

        var off: usize = 0;
        while (off < plaintext.len) {
            const out = self.tls.processOutgoing(plaintext[off..]) catch return error.SendFailed;
            const drained = if (out.encrypted) |bytes| bytes.len else 0;
            if (drained > 0) try self.writeSocket(out.encrypted.?);
            if (out.consumed == 0 and drained == 0) return error.SendFailed;
            off += out.consumed;
        }
    }

    /// Read up to `dest.len` plaintext bytes. Returns 0 at the end of the stream: over TLS only once the peer has sent
    /// close_notify; the TCP stream ending without one is `error.UncleanClose`, and data that does not decrypt is
    /// `error.RecvFailed`.
    pub fn recv(self: *Connection, dest: []u8) Error!usize {
        if (self.plain) return self.readSocket(dest);

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
            // No plaintext yet (mid-record); read more. Any byte is progress: the idle timeout applies from here.
            const n = try self.readSocket(&rx_buf);
            if (n == 0) return error.UncleanClose;
            input = rx_buf[0..n];
        }
    }

    fn recvError(self: *const Connection, err: ReadError) Error {
        return switch (err) {
            error.ConnectionReset => error.ConnectionReset,
            error.ReadFailed => error.RecvFailed,
            error.WouldBlock => self.readTimeout(),
        };
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
        const wait = stageBudget(peer.timeouts.connect_ms, peer.deadline_ms);
        if (peer.deadline_ms != null and wait == 0) return if (last == error.ConnectFailed) error.ConnectTimeout else last;
        const addr = peer.addrs[(peer.first +% k) % peer.addrs.len];
        return openTcp(addr, peer.port, wait) catch |err| switch (err) {
            error.ConnectFailed, error.ConnectTimeout => {
                last = err;
                continue;
            },
            else => return err,
        };
    }
    return last;
}

/// A connected TCP socket to `ip:port`, in blocking mode, or `error.ConnectTimeout` when the connection is not
/// established within `timeout_ms`. The connect itself runs non-blocking under `poll`, which bounds it the same way
/// on Linux and Darwin (Darwin's connect ignores SO_SNDTIMEO).
fn openTcp(ip: Ipv4, port: u16, timeout_ms: u32) Error!posix.fd_t {
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

    const flags = posix.system.fcntl(fd, posix.F.GETFL, @as(c_int, 0));
    if (posix.errno(flags) != .SUCCESS) return error.SocketFailed;
    const nonblock: c_int = @intCast(@as(u32, @bitCast(posix.O{ .NONBLOCK = true })));
    const blocking: c_int = @intCast(flags);
    if (posix.errno(posix.system.fcntl(fd, posix.F.SETFL, blocking | nonblock)) != .SUCCESS) return error.SocketFailed;

    const cr = posix.system.connect(fd, @ptrCast(&addr), @sizeOf(posix.sockaddr.in));
    switch (posix.errno(cr)) {
        .SUCCESS => {},
        .INPROGRESS, .INTR => {
            var pfd = [_]posix.pollfd{.{ .fd = fd, .events = posix.POLL.OUT, .revents = 0 }};
            const deadline = monoMs() + timeout_ms;
            while (true) {
                const now = monoMs();
                if (now >= deadline) return error.ConnectTimeout;
                const pr = posix.system.poll(&pfd, 1, @intCast(@min(deadline - now, std.math.maxInt(i32))));
                switch (posix.errno(pr)) {
                    .SUCCESS => if (pr > 0) break else return error.ConnectTimeout,
                    .INTR => continue,
                    else => return error.ConnectFailed,
                }
            }
            var so_err: c_int = 0;
            var len: posix.socklen_t = @sizeOf(c_int);
            const gr = posix.system.getsockopt(fd, posix.SOL.SOCKET, posix.SO.ERROR, @ptrCast(&so_err), &len);
            if (posix.errno(gr) != .SUCCESS or so_err != 0) return error.ConnectFailed;
        },
        else => return error.ConnectFailed,
    }
    if (posix.errno(posix.system.fcntl(fd, posix.F.SETFL, blocking)) != .SUCCESS) return error.SocketFailed;
    return fd;
}

/// A socket's SO_RCVTIMEO and SO_SNDTIMEO as last set, ms (0: never).
pub const SockTimeouts = struct {
    rcv_ms: u32 = 0,
    snd_ms: u32 = 0,
    /// Copied from `Timeouts.clock_ms` for the wait in progress. Null uses wall time and the socket timeout.
    clock_ms: ?*const fn () u64 = null,
};

const test_clock = struct {
    var on: std.atomic.Value(bool) = .init(false);
    var ms: std.atomic.Value(u64) = .init(0);
    /// One test arms the clock at a time. A second waits, so two UploadPart tests cannot move each other's deadlines.
    var held: std.atomic.Value(bool) = .init(false);
};

fn waitTestClock() void {
    while (test_clock.held.cmpxchgWeak(false, true, .acq_rel, .acquire) != null) {
        var ts: posix.timespec = .{ .sec = 0, .nsec = 1_000_000 };
        _ = posix.system.nanosleep(&ts, &ts);
    }
}

/// Freeze deadline time at the current wall clock. `testClockMs` then stands still until `advanceTestClock`.
pub fn armTestClock() void {
    waitTestClock();
    test_clock.ms.store(monoMs(), .release);
    test_clock.on.store(true, .release);
}

pub fn disarmTestClock() void {
    test_clock.on.store(false, .release);
    test_clock.held.store(false, .release);
}

/// Move the frozen clock forward. A no-op unless `armTestClock` is in effect, so a stall in some other test cannot
/// expire a deadline that is still on the wall clock.
pub fn advanceTestClock(by_ms: u64) void {
    if (test_clock.on.load(.acquire)) _ = test_clock.ms.fetchAdd(by_ms, .release);
}

pub fn testClockMs() u64 {
    return test_clock.ms.load(.acquire);
}

/// Set SO_RCVTIMEO or SO_SNDTIMEO to `ms` unless it already holds it. A blocking read or write that waits that long
/// without moving a byte then fails with EAGAIN.
fn armSockTimeout(fd: posix.fd_t, opt: u32, ms: u32, current: *u32) bool {
    if (current.* == ms) return true;
    const tv: posix.timeval = .{ .sec = @intCast(ms / 1000), .usec = @intCast(@as(u32, ms % 1000) * 1000) };
    const r = posix.system.setsockopt(fd, posix.SOL.SOCKET, opt, @ptrCast(&tv), @sizeOf(posix.timeval));
    if (posix.errno(r) != .SUCCESS) return false;
    current.* = ms;
    return true;
}

/// Milliseconds left until `deadline`, at least 1; null once it has passed.
fn msLeft(deadline: u64, clock_ms: ?*const fn () u64) ?u32 {
    const now = nowMs(clock_ms);
    if (now >= deadline) return null;
    return @intCast(@min(deadline - now, std.math.maxInt(u32)));
}

fn monoMs() u64 {
    return @intCast(@divTrunc(clock.monoNs(), std.time.ns_per_ms));
}

/// Absolute millisecond deadline `ms` from now, for `Peer.deadline_ms`.
pub fn deadlineFromNow(ms: u32) u64 {
    return monoMs() + ms;
}

/// `cap`, or what is left until `deadline_ms` when that is sooner. Zero means the deadline has already passed.
fn stageBudget(cap: u32, deadline_ms: ?u64) u32 {
    const deadline = deadline_ms orelse return cap;
    const now = monoMs();
    if (now >= deadline) return 0;
    return @intCast(@min(@as(u64, cap), deadline - now));
}

fn nowMs(clock_ms: ?*const fn () u64) u64 {
    if (clock_ms) |f| return f();
    return monoMs();
}

fn closeFd(fd: posix.fd_t) void {
    _ = posix.system.close(fd);
}

const WriteError = error{ SendFailed, WouldBlock };
const ReadError = error{ ReadFailed, ConnectionReset, WouldBlock };

/// Write all of `buf`. `error.WouldBlock`: `idle_ms` passed with no byte taken, or `stage` (an absolute deadline,
/// for the handshake) passed. Every byte taken restarts the idle wait; a signal interrupting a wait does not, since
/// the socket timeout is set to the time left before each write.
fn writeAllBy(fd: posix.fd_t, buf: []const u8, idle_ms: u32, stage: ?u64, opts: *SockTimeouts) WriteError!void {
    const clock_ms = opts.clock_ms;
    var deadline = nowMs(clock_ms) + idle_ms;
    if (stage) |d| deadline = @min(deadline, d);
    // A test clock must not arm a short socket timeout: that still fires on wall time while the machine is loaded.
    // Poll and recheck the virtual deadline instead. The socket timeout is only a backstop.
    if (clock_ms != null) {
        if (!armSockTimeout(fd, posix.SO.SNDTIMEO, 30_000, &opts.snd_ms)) return error.SendFailed;
        const real_start = monoMs();
        var off: usize = 0;
        while (off < buf.len) {
            if (nowMs(clock_ms) >= deadline) return error.WouldBlock;
            // Wall-clock backstop only, so a stuck socket cannot hang the test. The stall itself expires on the
            // virtual clock, which a loaded machine does not move.
            if (monoMs() - real_start > 120_000) return error.WouldBlock;
            var pfd = [_]posix.pollfd{.{ .fd = fd, .events = posix.POLL.OUT, .revents = 0 }};
            const pr = posix.system.poll(&pfd, 1, 20);
            switch (posix.errno(pr)) {
                .SUCCESS => {},
                .INTR => continue,
                else => return error.SendFailed,
            }
            if (pr == 0) continue;
            const r = posix.system.write(fd, buf[off..].ptr, buf.len - off);
            switch (posix.errno(r)) {
                .SUCCESS => {},
                .INTR, .AGAIN => continue,
                else => return error.SendFailed,
            }
            const n: usize = @intCast(r);
            if (n == 0) return error.SendFailed;
            off += n;
            deadline = nowMs(clock_ms) + idle_ms;
            if (stage) |d| deadline = @min(deadline, d);
        }
        return;
    }
    var off: usize = 0;
    while (off < buf.len) {
        const left = msLeft(deadline, null) orelse return error.WouldBlock;
        if (!armSockTimeout(fd, posix.SO.SNDTIMEO, left, &opts.snd_ms)) return error.SendFailed;
        const r = posix.system.write(fd, buf[off..].ptr, buf.len - off);
        switch (posix.errno(r)) {
            .SUCCESS => {},
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            else => return error.SendFailed,
        }
        const n: usize = @intCast(r);
        if (n == 0) return error.SendFailed;
        off += n;
        deadline = monoMs() + idle_ms;
        if (stage) |d| deadline = @min(deadline, d);
    }
}

fn sendError(err: WriteError) Error {
    return switch (err) {
        error.WouldBlock => error.SendTimeout,
        error.SendFailed => error.SendFailed,
    };
}

/// One read, by `deadline` (absolute, `monoMs`). `error.WouldBlock`: the deadline passed without a byte arriving.
/// A signal interrupting the wait does not extend it: the socket timeout is set to the time left before each try.
fn readBy(fd: posix.fd_t, buf: []u8, deadline: u64, opts: *SockTimeouts) ReadError!usize {
    if (opts.clock_ms) |clock_ms| {
        if (!armSockTimeout(fd, posix.SO.RCVTIMEO, 30_000, &opts.rcv_ms)) return error.ReadFailed;
        const real_start = monoMs();
        while (true) {
            if (clock_ms() >= deadline) return error.WouldBlock;
            // Wall-clock backstop only, so a stuck socket cannot hang the test. The stall itself expires on the
            // virtual clock, which a loaded machine does not move.
            if (monoMs() - real_start > 120_000) return error.WouldBlock;
            var pfd = [_]posix.pollfd{.{ .fd = fd, .events = posix.POLL.IN, .revents = 0 }};
            const pr = posix.system.poll(&pfd, 1, 20);
            switch (posix.errno(pr)) {
                .SUCCESS => {},
                .INTR => continue,
                else => return error.ReadFailed,
            }
            if (pr == 0) continue;
            const r = posix.system.read(fd, buf.ptr, buf.len);
            switch (posix.errno(r)) {
                .SUCCESS => return @intCast(r),
                .INTR, .AGAIN => continue,
                .CONNRESET, .CONNABORTED, .TIMEDOUT => return error.ConnectionReset,
                else => return error.ReadFailed,
            }
        }
    }
    while (true) {
        const left = msLeft(deadline, null) orelse return error.WouldBlock;
        if (!armSockTimeout(fd, posix.SO.RCVTIMEO, left, &opts.rcv_ms)) return error.ReadFailed;
        const r = posix.system.read(fd, buf.ptr, buf.len);
        return switch (posix.errno(r)) {
            .SUCCESS => @intCast(r),
            .INTR => continue,
            .AGAIN => error.WouldBlock,
            .CONNRESET, .CONNABORTED, .TIMEDOUT => error.ConnectionReset,
            else => error.ReadFailed,
        };
    }
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

test "Timeouts.fromEnv takes positive overrides and keeps the defaults otherwise" {
    const block = [_:null]?[*:0]const u8{
        "ZPQ_S3_CONNECT_TIMEOUT_MS=250",
        "ZPQ_S3_HANDSHAKE_TIMEOUT_MS=0",
        "ZPQ_S3_FIRST_BYTE_TIMEOUT_MS=abc",
        "ZPQ_S3_IDLE_TIMEOUT_MS= 30000",
    };
    const t = Timeouts.fromEnv(.{ .block = .{ .slice = &block } });
    try testing.expectEqual(@as(u32, 250), t.connect_ms);
    try testing.expectEqual(@as(u32, 5_000), t.handshake_ms);
    try testing.expectEqual(@as(u32, 10_000), t.first_byte_ms);
    try testing.expectEqual(@as(u32, 30_000), t.idle_ms);
    try testing.expectEqual(Timeouts{}, Timeouts.fromEnv(.empty));
}

/// Milliseconds since `t0` (from `monoMs`).
fn sinceMs(t0: u64) u64 {
    return monoMs() - t0;
}

const short: Timeouts = .{ .connect_ms = 200, .handshake_ms = 200, .first_byte_ms = 200, .idle_ms = 200 };

test "a TLS handshake the server never answers fails within its timeout, as a retryable connect failure" {
    var pki = try test_server.Pki.init();
    defer pki.deinit();
    var leaf = try pki.issue("mute.test", "DNS:mute.test");
    defer leaf.deinit();
    var server: test_server.Server = undefined;
    try server.startWith(&leaf, 1, .mute);
    defer server.finish() catch {};
    var context = try ClientContext.init(.{ .verify_certificate = false });
    defer context.deinit();

    var peer: Peer = .loopback(server.port);
    peer.timeouts = short;
    const t0 = monoMs();
    const got = Connection.connectWithContext(testing.allocator, &context, peer, "mute.test");
    try testing.expectError(error.HandshakeTimeout, got);
    try testing.expect(sinceMs(t0) < 2_000);
    try testing.expectEqual(retry.Failure.transient, retry.classifyConnect(error.HandshakeTimeout));
}

test "a TLS response that never starts fails with FirstByteTimeout, and a request on it may not be retried at once" {
    var pki = try test_server.Pki.init();
    defer pki.deinit();
    var leaf = try pki.issue("hold.test", "DNS:hold.test");
    defer leaf.deinit();
    var server: test_server.Server = undefined;
    try server.startWith(&leaf, 1, .hold);
    defer server.finish() catch {};
    var context = try ClientContext.init(.{ .verify_certificate = false });
    defer context.deinit();

    var peer: Peer = .loopback(server.port);
    peer.timeouts = short;
    var conn = try Connection.connectWithContext(testing.allocator, &context, peer, "hold.test");
    defer conn.deinit();
    try conn.send("GET /k HTTP/1.1\r\nHost: hold.test\r\n\r\n");
    var buf: [64]u8 = undefined;
    const t0 = monoMs();
    try testing.expectError(error.FirstByteTimeout, conn.recv(&buf));
    try testing.expect(sinceMs(t0) >= 150 and sinceMs(t0) < 2_000);
    // Charged and backed off on a new connection, whether or not this one came from the pool.
    const fbt = error.FirstByteTimeout;
    try testing.expectEqual(retry.Failure.stalled, retry.classify(fbt, .{ .idempotent = true, .reused = true }));
    try testing.expectEqual(retry.Failure.fatal, retry.classify(fbt, .{ .idempotent = false, .reused = false }));
}

test "a connect the listener never completes fails within its timeout" {
    // A listener that never accepts, with the shortest backlog: once its queue is full the kernel drops further SYNs,
    // so a connect waits on retransmissions with no answer, as an unreachable front end looks.
    const lfd_r = posix.system.socket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
    if (posix.errno(lfd_r) != .SUCCESS) return error.SkipZigTest;
    const lfd: posix.fd_t = @intCast(lfd_r);
    defer closeFd(lfd);
    var addr: posix.sockaddr.in = .{ .port = 0, .addr = @bitCast([4]u8{ 127, 0, 0, 1 }) };
    var len: posix.socklen_t = @sizeOf(posix.sockaddr.in);
    if (posix.errno(posix.system.bind(lfd, @ptrCast(&addr), len)) != .SUCCESS) return error.SkipZigTest;
    if (posix.errno(posix.system.listen(lfd, 0)) != .SUCCESS) return error.SkipZigTest;
    if (posix.errno(posix.system.getsockname(lfd, @ptrCast(&addr), &len)) != .SUCCESS) return error.SkipZigTest;

    var peer: Peer = .loopback(std.mem.bigToNative(u16, addr.port));
    peer.timeouts = short;
    var opened: [8]?Connection = @splat(null);
    defer for (&opened) |*o| if (o.*) |*conn| conn.deinit();
    for (&opened) |*o| {
        const t0 = monoMs();
        o.* = Connection.connectPlain(testing.allocator, peer) catch |err| {
            try testing.expectEqual(error.ConnectTimeout, err);
            try testing.expect(sinceMs(t0) >= 150 and sinceMs(t0) < 2_000);
            try testing.expectEqual(retry.Failure.transient, retry.classifyConnect(err));
            return;
        };
    }
    return error.SkipZigTest; // this kernel queued every connection: nothing to time out here
}

/// Interrupts one thread with SIGUSR1 every 20 ms, through a handler installed without SA_RESTART, so each of its
/// blocking reads and writes keeps returning EINTR, as a profiler's or runtime's signals do.
const Interrupter = struct {
    target: std.c.pthread_t,
    stop: std.atomic.Value(bool) = .init(false),
    sent: std.atomic.Value(u32) = .init(0),
    old: posix.Sigaction = undefined,
    thread: std.Thread = undefined,

    fn ignore(_: posix.SIG) callconv(.c) void {}

    fn start(self: *Interrupter) !void {
        const act: posix.Sigaction = .{ .handler = .{ .handler = ignore }, .mask = posix.sigemptyset(), .flags = 0 };
        posix.sigaction(.USR1, &act, &self.old);
        errdefer posix.sigaction(.USR1, &self.old, null);
        self.thread = try std.Thread.spawn(.{}, run, .{self});
    }

    fn run(self: *Interrupter) void {
        while (!self.stop.load(.acquire)) {
            _ = std.c.pthread_kill(self.target, .USR1);
            _ = self.sent.fetchAdd(1, .monotonic);
            const ts: posix.timespec = .{ .sec = 0, .nsec = 20 * std.time.ns_per_ms };
            _ = posix.system.nanosleep(&ts, null);
        }
    }

    fn finish(self: *Interrupter) void {
        self.stop.store(true, .release);
        self.thread.join();
        posix.sigaction(.USR1, &self.old, null);
    }
};

test "signals interrupting a wait do not extend the handshake, first-byte, idle read or send deadlines" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var irq: Interrupter = .{ .target = std.c.pthread_self() };
    try irq.start();
    defer irq.finish();

    {
        var pki = try test_server.Pki.init();
        defer pki.deinit();
        var leaf = try pki.issue("mute.test", "DNS:mute.test");
        defer leaf.deinit();
        var server: test_server.Server = undefined;
        try server.startWith(&leaf, 1, .mute);
        defer server.finish() catch {};
        var context = try ClientContext.init(.{ .verify_certificate = false });
        defer context.deinit();
        var peer: Peer = .loopback(server.port);
        peer.timeouts = short;
        const t0 = monoMs();
        const got = Connection.connectWithContext(testing.allocator, &context, peer, "mute.test");
        try testing.expectError(error.HandshakeTimeout, got);
        try testing.expect(sinceMs(t0) >= 150 and sinceMs(t0) < 600);
    }
    {
        var pki = try test_server.Pki.init();
        defer pki.deinit();
        var leaf = try pki.issue("hold.test", "DNS:hold.test");
        defer leaf.deinit();
        var server: test_server.Server = undefined;
        try server.startWith(&leaf, 1, .hold);
        defer server.finish() catch {};
        var context = try ClientContext.init(.{ .verify_certificate = false });
        defer context.deinit();
        var peer: Peer = .loopback(server.port);
        peer.timeouts = short;
        var conn = try Connection.connectWithContext(testing.allocator, &context, peer, "hold.test");
        defer conn.deinit();
        try conn.send("GET /k HTTP/1.1\r\nHost: hold.test\r\n\r\n");
        var buf: [64]u8 = undefined;
        const t0 = monoMs();
        try testing.expectError(error.FirstByteTimeout, conn.recv(&buf));
        try testing.expect(sinceMs(t0) >= 150 and sinceMs(t0) < 600);
    }
    {
        // A plain connection over a socket pair whose far end neither writes nor reads.
        var fds: [2]i32 = undefined;
        if (posix.errno(posix.system.socketpair(posix.AF.UNIX, posix.SOCK.STREAM, 0, &fds)) != .SUCCESS)
            return error.SkipZigTest;
        defer closeFd(fds[1]);
        var conn: Connection = .{ .fd = fds[0], .tls = undefined, .allocator = testing.allocator, .plain = true };
        defer conn.deinit();
        conn.setTimeouts(short);
        var buf: [64]u8 = undefined;
        var t0 = monoMs();
        try testing.expectError(error.ReadTimeout, conn.recv(&buf));
        try testing.expect(sinceMs(t0) >= 150 and sinceMs(t0) < 600);

        const big = try testing.allocator.alloc(u8, 8 << 20);
        defer testing.allocator.free(big);
        @memset(big, 'x');
        t0 = monoMs();
        try testing.expectError(error.SendTimeout, conn.send(big));
        try testing.expect(sinceMs(t0) >= 150 and sinceMs(t0) < 600);
    }
    // At 20 ms apart, each 200 ms wait above was interrupted several times.
    try testing.expect(irq.sent.load(.monotonic) >= 20);
}
