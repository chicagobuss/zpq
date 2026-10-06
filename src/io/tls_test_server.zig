//! In-process TLS server and throwaway PKI for tests of the TLS client. The CA and the leaf certificates are generated
//! at test time, so the tree holds no key material and nothing expires. A `Server` serves one connection from a thread
//! and records what the client sent in its ClientHello.
//!
//! Test-only: imported from tests, never from the engine.

const std = @import("std");
const posix = std.posix;
const c = @import("boring_tls").tls.c;

pub const Error = error{TestPki};

pub const Pki = struct {
    ca_key: *c.EVP_PKEY,
    ca: *c.X509,

    pub fn init() Error!Pki {
        const key = try newKey();
        errdefer c.EVP_PKEY_free(key);
        const ca = try newCert(key, key, null, "zpq test CA", null, true);
        return .{ .ca_key = key, .ca = ca };
    }

    pub fn deinit(self: *Pki) void {
        c.X509_free(self.ca);
        c.EVP_PKEY_free(self.ca_key);
    }

    /// A leaf certificate issued by the CA. `san` is a subjectAltName in OpenSSL's syntax (`DNS:a.test,IP:127.0.0.1`);
    /// `cn` goes in the subject's common name, which a client must not fall back to.
    pub fn issue(self: *const Pki, cn: [:0]const u8, san: ?[:0]const u8) Error!Leaf {
        const key = try newKey();
        errdefer c.EVP_PKEY_free(key);
        const cert = try newCert(key, self.ca_key, self.ca, cn, san, false);
        return .{ .key = key, .cert = cert };
    }
};

pub const Leaf = struct {
    key: *c.EVP_PKEY,
    cert: *c.X509,

    pub fn deinit(self: *Leaf) void {
        c.X509_free(self.cert);
        c.EVP_PKEY_free(self.key);
    }
};

var next_serial: std.atomic.Value(c_long) = .init(1);

fn newKey() Error!*c.EVP_PKEY {
    const ec = c.EC_KEY_new_by_curve_name(c.NID_X9_62_prime256v1) orelse return error.TestPki;
    if (c.EC_KEY_generate_key(ec) != 1) {
        c.EC_KEY_free(ec);
        return error.TestPki;
    }
    const pkey = c.EVP_PKEY_new() orelse {
        c.EC_KEY_free(ec);
        return error.TestPki;
    };
    if (c.EVP_PKEY_assign_EC_KEY(pkey, ec) != 1) {
        c.EC_KEY_free(ec);
        c.EVP_PKEY_free(pkey);
        return error.TestPki;
    }
    return pkey;
}

fn newCert(
    key: *c.EVP_PKEY,
    issuer_key: *c.EVP_PKEY,
    issuer: ?*c.X509,
    cn: [:0]const u8,
    san: ?[:0]const u8,
    is_ca: bool,
) Error!*c.X509 {
    const x = c.X509_new() orelse return error.TestPki;
    errdefer c.X509_free(x);
    if (c.X509_set_version(x, c.X509_VERSION_3) != 1) return error.TestPki;
    if (c.ASN1_INTEGER_set(c.X509_get_serialNumber(x), next_serial.fetchAdd(1, .monotonic)) != 1) return error.TestPki;
    if (c.X509_gmtime_adj(c.X509_getm_notBefore(x), -3600) == null) return error.TestPki;
    if (c.X509_gmtime_adj(c.X509_getm_notAfter(x), 24 * 3600) == null) return error.TestPki;
    const name = c.X509_get_subject_name(x);
    if (c.X509_NAME_add_entry_by_txt(name, "CN", c.MBSTRING_ASC, cn.ptr, -1, -1, 0) != 1) return error.TestPki;
    const issuer_name = if (issuer) |i| c.X509_get_subject_name(i) else name;
    if (c.X509_set_issuer_name(x, issuer_name) != 1) return error.TestPki;
    if (c.X509_set_pubkey(x, key) != 1) return error.TestPki;

    var v3: c.X509V3_CTX = undefined;
    c.X509V3_set_ctx(&v3, issuer orelse x, x, null, null, 0);
    if (is_ca) try addExt(x, &v3, c.NID_basic_constraints, "critical,CA:TRUE");
    if (san) |s| try addExt(x, &v3, c.NID_subject_alt_name, s);

    if (c.X509_sign(x, issuer_key, c.EVP_sha256()) <= 0) return error.TestPki;
    return x;
}

fn addExt(x: *c.X509, v3: *c.X509V3_CTX, nid: c_int, value: [:0]const u8) Error!void {
    const ext = c.X509V3_EXT_nconf_nid(null, v3, nid, value.ptr) orelse return error.TestPki;
    defer c.X509_EXTENSION_free(ext);
    if (c.X509_add_ext(x, ext, -1) != 1) return error.TestPki;
}

/// What the server does with each connection.
pub const Behavior = union(enum) {
    /// Complete the handshake and hold the connection until the client closes it.
    hold,
    /// Answer the ClientHello with a fatal handshake_failure alert, before any certificate is sent.
    alert,
    /// Complete the handshake, read one HTTP request head, send `response`, then end the stream as `end` says.
    respond: struct { response: []const u8, end: End },

    pub const End = enum {
        /// close_notify, then close.
        close_notify,
        /// Close the TCP connection with no close_notify.
        tcp_close,
        /// Send the response's last record and bytes that are not a TLS record in one write, so the client reads
        /// them together, then hold the connection (up to 2 s) until the client closes it.
        garbage,
    };
};

/// How long `finish` waits for the server thread, and the longest one blocking read or write on a connection may take.
/// Neither is reached by a test that works: they bound how long a broken one takes to fail.
const finish_timeout_ms = 20_000;
const io_timeout_s = 10;

/// A TLS server on 127.0.0.1 presenting `leaf` to the connections it accepts, one at a time.
pub const Server = struct {
    port: u16,
    thread: std.Thread,
    /// Everything the server thread touches lives here, off the caller's stack, so a `finish` that gives up on the
    /// thread can leave it running without it writing into a returned frame.
    state: *State,

    /// Read after `finish`. The number of connections accepted, and how the last one went.
    accepted: usize = 0,
    handshake_ok: bool = false,
    /// The ClientHello's server_name, if it carried one.
    sni_buf: [256]u8 = undefined,
    sni_len: ?usize = null,

    /// Serves `accepts` connections, then stops.
    pub fn start(self: *Server, leaf: *const Leaf, accepts: usize) !void {
        return self.startWith(leaf, accepts, .hold);
    }

    pub fn startWith(self: *Server, leaf: *const Leaf, accepts: usize, behavior: Behavior) !void {
        const gpa = std.heap.page_allocator;
        const st = try gpa.create(State);
        errdefer gpa.destroy(st);
        st.* = .{ .accepts = accepts, .behavior = behavior };
        if (behavior == .respond) st.behavior.respond.response = try gpa.dupe(u8, behavior.respond.response);
        errdefer if (st.behavior == .respond) gpa.free(st.behavior.respond.response);

        st.ctx = c.SSL_CTX_new(c.TLS_server_method()) orelse return error.TestPki;
        errdefer c.SSL_CTX_free(st.ctx);
        if (c.SSL_CTX_use_certificate(st.ctx, leaf.cert) != 1) return error.TestPki;
        if (c.SSL_CTX_use_PrivateKey(st.ctx, leaf.key) != 1) return error.TestPki;
        // No session tickets: a TLS 1.3 server writes them after the handshake, possibly into a socket the client has
        // already closed.
        _ = c.SSL_CTX_set_num_tickets(st.ctx, 0);

        if (posix.errno(posix.system.socketpair(posix.AF.UNIX, posix.SOCK.STREAM, 0, &st.stop)) != .SUCCESS)
            return error.SkipZigTest;
        errdefer for (st.stop) |fd| {
            _ = posix.system.close(fd);
        };
        const rc = posix.system.socket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
        if (posix.errno(rc) != .SUCCESS) return error.SkipZigTest;
        st.listen_fd = @intCast(rc);
        errdefer _ = posix.system.close(st.listen_fd);
        var addr: posix.sockaddr.in = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f000001) };
        var len: posix.socklen_t = @sizeOf(posix.sockaddr.in);
        if (posix.errno(posix.system.bind(st.listen_fd, @ptrCast(&addr), len)) != .SUCCESS) return error.SkipZigTest;
        if (posix.errno(posix.system.listen(st.listen_fd, 4)) != .SUCCESS) return error.SkipZigTest;
        if (posix.errno(posix.system.getsockname(st.listen_fd, @ptrCast(&addr), &len)) != .SUCCESS) return error.SkipZigTest;
        self.* = .{
            .port = std.mem.bigToNative(u16, addr.port),
            .state = st,
            .thread = try std.Thread.spawn(.{}, State.serve, .{st}),
        };
    }

    /// Stop the server once the client has finished with its connections (or failed to make them), and collect what
    /// it saw. A server still waiting for a connection that never came stops at once. One that has not stopped after
    /// `finish_timeout_ms` is abandoned, and the test fails with `error.TestServerStuck`.
    ///
    /// The stop is a byte on a socketpair the server thread polls alongside its sockets. Shutting the listening socket
    /// down is not enough: on Linux it wakes a thread blocked in accept, but macOS leaves that thread blocked.
    pub fn finish(self: *Server) error{TestServerStuck}!void {
        const st = self.state;
        _ = posix.system.write(st.stop[0], "s", 1);
        if (!waitReadable(st.stop[0], finish_timeout_ms)) {
            std.log.err("tls_test_server on port {d}: the server thread has not stopped {d} ms after finish(); " ++
                "abandoning it ({d} of {d} connections accepted)", .{ self.port, finish_timeout_ms, @atomicLoad(usize, &st.accepted, .monotonic), st.accepts });
            self.thread.detach();
            return error.TestServerStuck;
        }
        self.thread.join();
        self.accepted = st.accepted;
        self.handshake_ok = st.handshake_ok;
        self.sni_len = st.sni_len;
        if (st.sni_len) |n| @memcpy(self.sni_buf[0..n], st.sni_buf[0..n]);
        st.deinit();
    }

    pub fn sni(self: *const Server) ?[]const u8 {
        return if (self.sni_len) |n| self.sni_buf[0..n] else null;
    }
};

const State = struct {
    ctx: *c.SSL_CTX = undefined,
    listen_fd: posix.fd_t = -1,
    /// `finish` writes to `stop[0]`; the server thread polls `stop[1]`, and writes to it once it has stopped.
    stop: [2]posix.fd_t = .{ -1, -1 },
    accepts: usize,
    /// `respond.response` is a copy the state owns.
    behavior: Behavior,

    accepted: usize = 0,
    handshake_ok: bool = false,
    sni_buf: [256]u8 = undefined,
    sni_len: ?usize = null,

    fn deinit(st: *State) void {
        _ = posix.system.close(st.listen_fd);
        for (st.stop) |fd| _ = posix.system.close(fd);
        c.SSL_CTX_free(st.ctx);
        if (st.behavior == .respond) std.heap.page_allocator.free(st.behavior.respond.response);
        std.heap.page_allocator.destroy(st);
    }

    fn serve(st: *State) void {
        defer _ = posix.system.write(st.stop[1], "d", 1);
        for (0..st.accepts) |_| {
            const fd = st.acceptNext() orelse return;
            @atomicStore(usize, &st.accepted, st.accepted + 1, .monotonic);
            defer _ = posix.system.close(fd);
            // Bounds SSL_accept and SSL_write, which block without looking at `stop`.
            const tv: posix.timeval = .{ .sec = io_timeout_s, .usec = 0 };
            _ = posix.system.setsockopt(fd, posix.SOL.SOCKET, posix.SO.RCVTIMEO, @ptrCast(&tv), @sizeOf(posix.timeval));
            _ = posix.system.setsockopt(fd, posix.SOL.SOCKET, posix.SO.SNDTIMEO, @ptrCast(&tv), @sizeOf(posix.timeval));
            st.serveOne(fd);
        }
    }

    /// The next connection, or null once `finish` has asked the thread to stop. A connection already queued is
    /// accepted even then: the client may have finished with it before the thread got to it.
    fn acceptNext(st: *State) ?posix.fd_t {
        while (true) {
            var pfd = [_]posix.pollfd{
                .{ .fd = st.listen_fd, .events = posix.POLL.IN, .revents = 0 },
                .{ .fd = st.stop[1], .events = posix.POLL.IN, .revents = 0 },
            };
            const n = posix.system.poll(&pfd, pfd.len, -1);
            switch (posix.errno(n)) {
                .SUCCESS => {},
                .INTR => continue,
                else => return null,
            }
            if (pfd[0].revents & posix.POLL.IN == 0) return null;
            const rc = posix.system.accept(st.listen_fd, null, null);
            switch (posix.errno(rc)) {
                .SUCCESS => return @intCast(rc),
                // The client gave up on a queued connection.
                .INTR, .CONNABORTED, .AGAIN => continue,
                else => return null,
            }
        }
    }

    /// Wait for `fd` to have something to read (or its end), up to `io_timeout_s`. False on a timeout, or when
    /// `finish` stops the server first.
    fn readable(st: *const State, fd: posix.fd_t) bool {
        return st.readableWithin(fd, io_timeout_s * 1000);
    }

    fn readableWithin(st: *const State, fd: posix.fd_t, timeout_ms: i32) bool {
        while (true) {
            var pfd = [_]posix.pollfd{
                .{ .fd = fd, .events = posix.POLL.IN, .revents = 0 },
                .{ .fd = st.stop[1], .events = posix.POLL.IN, .revents = 0 },
            };
            const n = posix.system.poll(&pfd, pfd.len, timeout_ms);
            switch (posix.errno(n)) {
                .SUCCESS => {},
                .INTR => continue,
                else => return false,
            }
            return pfd[0].revents != 0;
        }
    }

    fn serveOne(st: *State, fd: posix.fd_t) void {
        if (st.behavior == .alert) {
            var hello: [4096]u8 = undefined;
            if (!st.readable(fd)) return;
            _ = posix.system.read(fd, &hello, hello.len);
            const alert = [_]u8{ 0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28 }; // fatal handshake_failure
            _ = posix.system.write(fd, &alert, alert.len);
            return;
        }
        const ssl = c.SSL_new(st.ctx) orelse return;
        defer c.SSL_free(ssl);
        if (c.SSL_set_fd(ssl, fd) != 1) return;
        const ok = c.SSL_accept(ssl) == 1;
        if (c.SSL_get_servername(ssl, c.TLSEXT_NAMETYPE_host_name)) |name| {
            const s = std.mem.span(name);
            const n = @min(s.len, st.sni_buf.len);
            @memcpy(st.sni_buf[0..n], s[0..n]);
            st.sni_len = n;
        } else st.sni_len = null;
        c.ERR_clear_error();
        st.handshake_ok = ok;
        if (!ok) return;
        defer c.ERR_clear_error();
        var buf: [4096]u8 = undefined;
        switch (st.behavior) {
            .alert => unreachable,
            // Hold the connection until the client closes it.
            .hold => while (c.SSL_pending(ssl) > 0 or st.readable(fd)) {
                if (c.SSL_read(ssl, &buf, buf.len) <= 0) break;
            },
            .respond => |r| {
                var head: std.ArrayList(u8) = .empty;
                defer head.deinit(std.heap.page_allocator);
                while (std.mem.indexOf(u8, head.items, "\r\n\r\n") == null) {
                    if (c.SSL_pending(ssl) == 0 and !st.readable(fd)) return;
                    const n = c.SSL_read(ssl, &buf, buf.len);
                    if (n <= 0) return;
                    head.appendSlice(std.heap.page_allocator, buf[0..@intCast(n)]) catch return;
                }
                if (r.end == .garbage) {
                    // Encrypt into memory, then send record and garbage with a single write.
                    const mem = c.BIO_new(c.BIO_s_mem()) orelse return;
                    c.SSL_set0_wbio(ssl, mem);
                    if (c.SSL_write(ssl, r.response.ptr, @intCast(r.response.len)) != r.response.len) return;
                    var out: [8192]u8 = undefined;
                    const n: usize = @intCast(@max(0, c.BIO_read(mem, &out, out.len - 16)));
                    @memcpy(out[n..][0..16], "not a TLS record");
                    _ = posix.system.write(fd, &out, n + 16);
                    _ = st.readableWithin(fd, 2000);
                    return;
                }
                if (c.SSL_write(ssl, r.response.ptr, @intCast(r.response.len)) != r.response.len) return;
                if (r.end == .close_notify) _ = c.SSL_shutdown(ssl);
            },
        }
    }
};

/// Whether `fd` becomes readable within `timeout_ms`.
fn waitReadable(fd: posix.fd_t, timeout_ms: i32) bool {
    while (true) {
        var pfd = [_]posix.pollfd{.{ .fd = fd, .events = posix.POLL.IN, .revents = 0 }};
        const n = posix.system.poll(&pfd, pfd.len, timeout_ms);
        switch (posix.errno(n)) {
            .SUCCESS => return n > 0,
            .INTR => continue,
            else => return false,
        }
    }
}
