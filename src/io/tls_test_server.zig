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

/// A TLS server on 127.0.0.1 presenting `leaf` to the connections it accepts, one at a time.
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

pub const Server = struct {
    ctx: *c.SSL_CTX,
    listen_fd: posix.fd_t,
    port: u16,
    thread: std.Thread = undefined,
    accepts: usize,
    behavior: Behavior = .hold,

    /// Set by the server thread; read after `finish`. Each describes the last connection.
    handshake_ok: bool = false,
    /// The ClientHello's server_name, if it carried one.
    sni_buf: [256]u8 = undefined,
    sni_len: ?usize = null,

    /// Pointer-init: the server thread holds `self`. Serves `accepts` connections, then stops.
    pub fn start(self: *Server, leaf: *const Leaf, accepts: usize) !void {
        return self.startWith(leaf, accepts, .hold);
    }

    pub fn startWith(self: *Server, leaf: *const Leaf, accepts: usize, behavior: Behavior) !void {
        const ctx = c.SSL_CTX_new(c.TLS_server_method()) orelse return error.TestPki;
        errdefer c.SSL_CTX_free(ctx);
        if (c.SSL_CTX_use_certificate(ctx, leaf.cert) != 1) return error.TestPki;
        if (c.SSL_CTX_use_PrivateKey(ctx, leaf.key) != 1) return error.TestPki;
        // No session tickets: a TLS 1.3 server writes them after the handshake, possibly into a socket the client has
        // already closed.
        _ = c.SSL_CTX_set_num_tickets(ctx, 0);

        const rc = posix.system.socket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
        if (posix.errno(rc) != .SUCCESS) return error.SkipZigTest;
        const fd: posix.fd_t = @intCast(rc);
        errdefer _ = posix.system.close(fd);
        var addr: posix.sockaddr.in = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f000001) };
        var len: posix.socklen_t = @sizeOf(posix.sockaddr.in);
        if (posix.errno(posix.system.bind(fd, @ptrCast(&addr), len)) != .SUCCESS) return error.SkipZigTest;
        if (posix.errno(posix.system.listen(fd, 4)) != .SUCCESS) return error.SkipZigTest;
        if (posix.errno(posix.system.getsockname(fd, @ptrCast(&addr), &len)) != .SUCCESS) return error.SkipZigTest;
        self.* = .{
            .ctx = ctx,
            .listen_fd = fd,
            .port = std.mem.bigToNative(u16, addr.port),
            .accepts = accepts,
            .behavior = behavior,
        };
        self.thread = try std.Thread.spawn(.{}, serve, .{self});
    }

    /// Wait for the server thread, after the client has finished with its connection (or failed to make one).
    pub fn finish(self: *Server) void {
        // Unblocks an `accept` still waiting for a client that never came.
        _ = posix.system.shutdown(self.listen_fd, posix.SHUT.RDWR);
        self.thread.join();
        _ = posix.system.close(self.listen_fd);
        c.SSL_CTX_free(self.ctx);
    }

    pub fn sni(self: *const Server) ?[]const u8 {
        return if (self.sni_len) |n| self.sni_buf[0..n] else null;
    }

    fn serve(self: *Server) void {
        for (0..self.accepts) |_| if (!self.serveOne()) return;
    }

    /// False once the listening socket is shut down.
    fn serveOne(self: *Server) bool {
        const rc = posix.system.accept(self.listen_fd, null, null);
        if (posix.errno(rc) != .SUCCESS) return false;
        const fd: posix.fd_t = @intCast(rc);
        defer _ = posix.system.close(fd);
        if (self.behavior == .alert) {
            var hello: [4096]u8 = undefined;
            _ = posix.system.read(fd, &hello, hello.len);
            const alert = [_]u8{ 0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28 }; // fatal handshake_failure
            _ = posix.system.write(fd, &alert, alert.len);
            return true;
        }
        const ssl = c.SSL_new(self.ctx) orelse return false;
        defer c.SSL_free(ssl);
        if (c.SSL_set_fd(ssl, fd) != 1) return false;
        const ok = c.SSL_accept(ssl) == 1;
        if (c.SSL_get_servername(ssl, c.TLSEXT_NAMETYPE_host_name)) |name| {
            const s = std.mem.span(name);
            const n = @min(s.len, self.sni_buf.len);
            @memcpy(self.sni_buf[0..n], s[0..n]);
            self.sni_len = n;
        } else self.sni_len = null;
        c.ERR_clear_error();
        self.handshake_ok = ok;
        if (!ok) return true;
        defer c.ERR_clear_error();
        var buf: [4096]u8 = undefined;
        switch (self.behavior) {
            .alert => unreachable,
            // Hold the connection until the client closes it.
            .hold => while (c.SSL_read(ssl, &buf, buf.len) > 0) {},
            .respond => |r| {
                var head: std.ArrayList(u8) = .empty;
                defer head.deinit(std.heap.page_allocator);
                while (std.mem.indexOf(u8, head.items, "\r\n\r\n") == null) {
                    const n = c.SSL_read(ssl, &buf, buf.len);
                    if (n <= 0) return true;
                    head.appendSlice(std.heap.page_allocator, buf[0..@intCast(n)]) catch return true;
                }
                if (r.end == .garbage) {
                    // Encrypt into memory, then send record and garbage with a single write.
                    const mem = c.BIO_new(c.BIO_s_mem()) orelse return true;
                    c.SSL_set0_wbio(ssl, mem);
                    if (c.SSL_write(ssl, r.response.ptr, @intCast(r.response.len)) != r.response.len) return true;
                    var out: [8192]u8 = undefined;
                    const n: usize = @intCast(@max(0, c.BIO_read(mem, &out, out.len - 16)));
                    @memcpy(out[n..][0..16], "not a TLS record");
                    _ = posix.system.write(fd, &out, n + 16);
                    var pfd = [_]posix.pollfd{.{ .fd = fd, .events = posix.POLL.IN, .revents = 0 }};
                    _ = posix.system.poll(&pfd, 1, 2000);
                    return true;
                }
                if (c.SSL_write(ssl, r.response.ptr, @intCast(r.response.len)) != r.response.len) return true;
                if (r.end == .close_notify) _ = c.SSL_shutdown(ssl);
            },
        }
        return true;
    }
};
