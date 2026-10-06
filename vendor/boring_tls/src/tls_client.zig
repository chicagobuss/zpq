const std = @import("std");
const tls = @import("tls.zig");

const c = tls.c;
pub const TlsError = tls.TlsError;

const BUFFER_SIZE = tls.BUFFER_SIZE;

pub const TlsClientOptions = struct {
    /// Verify the server's certificate chain and that the certificate names the host given to `init`.
    verify_certificate: bool = true,
    /// Trust this CA instead of the system bundle.
    ca_cert: ?*c.X509 = null,
    /// Trust exactly the certificates in this PEM text instead of the system bundle.
    ca_pem: ?[]const u8 = null,
};

/// Whether `name` is an IPv4 or IPv6 address literal (without brackets or port) rather than a DNS name.
pub fn isIpLiteral(name: []const u8) bool {
    _ = std.Io.net.IpAddress.parse(name, 0) catch return false;
    return true;
}

/// A configured client SSL_CTX: trust store, verification mode and options. Building one parses the CA bundle (tens
/// of milliseconds and megabytes), so build it once and share it: BoringSSL allows an SSL_CTX to be used by
/// connections on many threads at once, provided nothing reconfigures it after it is shared. Each `TlsClient` holds
/// its own reference through its SSL, so `deinit` may run while clients are still alive.
pub const ClientContext = struct {
    ctx: *c.SSL_CTX,
    verify_certificate: bool,

    pub fn init(options: TlsClientOptions) !ClientContext {
        tls.initOpenSsl();
        return .{ .ctx = try createSslContext(options), .verify_certificate = options.verify_certificate };
    }

    pub fn deinit(self: *ClientContext) void {
        c.SSL_CTX_free(self.ctx);
        self.* = undefined;
    }

    fn createSslContext(options: TlsClientOptions) !*c.SSL_CTX {
        const method = c.TLS_client_method();
        const ctx = c.SSL_CTX_new(method) orelse {
            std.log.err("Failed to create SSL context", .{});
            return TlsError.TlsContextFailed;
        };

        if (options.ca_cert) |ca| {
            c.SSL_CTX_set_verify(ctx, c.SSL_VERIFY_PEER, null);
            if (c.X509_STORE_add_cert(c.SSL_CTX_get_cert_store(ctx), ca) != 1) {
                c.SSL_CTX_free(ctx);
                return TlsError.CertificateLoadFailed;
            }
        } else if (options.ca_pem) |pem| {
            c.SSL_CTX_set_verify(ctx, c.SSL_VERIFY_PEER, null);
            loadPem(c.SSL_CTX_get_cert_store(ctx).?, pem) catch |err| {
                c.SSL_CTX_free(ctx);
                return err;
            };
        } else if (options.verify_certificate) {
            c.SSL_CTX_set_verify(ctx, c.SSL_VERIFY_PEER, null);

            // Try explicit CA bundle first (macOS), then fall back to default paths
            const ca_paths = [_][*:0]const u8{
                "/etc/ssl/cert.pem",
                "/opt/homebrew/etc/ca-certificates/cert.pem",
                "/usr/local/etc/ca-certificates/cert.pem",
            };

            var loaded = false;
            for (ca_paths) |ca_path| {
                if (c.SSL_CTX_load_verify_locations(ctx, ca_path, null) == 1) {
                    loaded = true;
                    break;
                }
            }

            if (!loaded) {
                // Fall back to default paths (works on Linux)
                if (c.SSL_CTX_set_default_verify_paths(ctx) != 1) {
                    std.log.warn("Failed to load CA certificates", .{});
                }
            }
            // Failed probes of absent paths leave errors queued on this thread, where a later SSL_get_error would
            // find them.
            c.ERR_clear_error();
        } else {
            c.SSL_CTX_set_verify(ctx, c.SSL_VERIFY_NONE, null);
        }

        _ = c.SSL_CTX_set_options(ctx, c.SSL_OP_NO_SSLv2 | c.SSL_OP_NO_SSLv3 | c.SSL_OP_NO_COMPRESSION);
        // Allow SSL_write to make partial progress on large plaintexts —
        // returns the bytes-actually-written rather than 0/WANT_WRITE when
        // the BIO needs draining. Required for ~MB-sized PUT bodies.
        _ = c.SSL_CTX_set_mode(ctx, c.SSL_MODE_ENABLE_PARTIAL_WRITE | c.SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
        return ctx;
    }

    /// Add every certificate in `pem` to `store`; text between certificates is ignored. No certificate is an error.
    fn loadPem(store: *c.X509_STORE, pem: []const u8) !void {
        const bio = c.BIO_new_mem_buf(pem.ptr, @intCast(pem.len)) orelse return TlsError.CertificateLoadFailed;
        defer _ = c.BIO_free(bio);
        var added: usize = 0;
        while (c.PEM_read_bio_X509(bio, null, null, null)) |x| {
            defer c.X509_free(x);
            if (c.X509_STORE_add_cert(store, x) != 1) return TlsError.CertificateLoadFailed;
            added += 1;
        }
        // The read that found no further certificate queued an error.
        c.ERR_clear_error();
        if (added == 0) return TlsError.CertificateLoadFailed;
    }
};

pub const TlsClient = struct {
    allocator: std.mem.Allocator,
    ssl: *c.SSL,
    bio_read: *c.BIO,
    bio_write: *c.BIO,
    handshake_complete: bool = false,
    hostname: []const u8,
    buffers: tls.TlsBuffers = .{},
    verify_certificate: bool = true,

    const Self = @This();

    /// A connection to `hostname` under `context`, which only has to outlive this call.
    pub fn init(allocator: std.mem.Allocator, context: *const ClientContext, hostname: []const u8) !Self {
        const ssl = try createSslConnection(context.ctx);
        errdefer c.SSL_free(ssl);

        try setSniHostname(ssl, hostname);
        if (context.verify_certificate) try setExpectedPeer(ssl, hostname);

        const bio_read = try tls.createBio();
        const bio_write = tls.createBio() catch |err| {
            _ = c.BIO_free(bio_read);
            return err;
        };
        c.SSL_set_bio(ssl, bio_read, bio_write);

        return .{
            .allocator = allocator,
            .ssl = ssl,
            .bio_read = bio_read,
            .bio_write = bio_write,
            .hostname = hostname,
            .verify_certificate = context.verify_certificate,
        };
    }

    pub fn deinit(self: *Self) void {
        self.buffers.deinit(self.allocator);
        c.SSL_free(self.ssl);
    }

    pub fn startHandshake(self: *Self) !tls.ProcessResult {
        c.SSL_set_connect_state(self.ssl);
        const handshake_result = c.SSL_do_handshake(self.ssl);

        if (handshake_result == 1) {
            self.handshake_complete = true;
        } else {
            const ssl_error = c.SSL_get_error(self.ssl, handshake_result);
            if (ssl_error != c.SSL_ERROR_WANT_READ and ssl_error != c.SSL_ERROR_WANT_WRITE) {
                std.log.err("TLS handshake start failed with error: {}", .{ssl_error});
                return TlsError.TlsHandshakeFailed;
            }
        }

        return try self.processOutgoing(null);
    }

    pub fn processIncoming(self: *Self, encrypted_data: []const u8, user_buf: ?[]u8) !?[]const u8 {
        if (encrypted_data.len > 0) {
            try tls.writeToBio(self.bio_read, encrypted_data);
        }
        if (!self.handshake_complete) {
            try self.performHandshake();
        }
        if (!self.handshake_complete) return null;
        return try self.readDecryptedData(user_buf);
    }

    pub fn processOutgoing(self: *Self, plaintext: ?[]const u8) !tls.ProcessResult {
        var consumed: usize = 0;
        if (plaintext) |data| {
            consumed = try self.writeEncryptedData(data);
        }

        try self.readFromWriteBio();
        return .{
            .encrypted = if (self.buffers.encrypted_out.items.len > 0) self.buffers.getEncryptedSlice() else null,
            .consumed = consumed,
        };
    }

    pub fn isHandshakeComplete(self: *Self) bool {
        return self.handshake_complete;
    }

    fn createSslConnection(ctx: *c.SSL_CTX) !*c.SSL {
        return tls.createSslInstance(ctx);
    }

    /// RFC 6066 §3: server_name carries a DNS host name, never an IP literal, so an IP gets no SNI.
    fn setSniHostname(ssl: *c.SSL, hostname: []const u8) !void {
        if (hostname.len == 0 or isIpLiteral(hostname)) return;
        var hostname_buf: [256]u8 = undefined;
        if (hostname.len >= hostname_buf.len) {
            std.log.warn("Hostname too long for SNI", .{});
            return;
        }

        @memcpy(hostname_buf[0..hostname.len], hostname);
        hostname_buf[hostname.len] = 0;

        if (c.SSL_set_tlsext_host_name(ssl, hostname_buf[0..hostname.len :0].ptr) != 1) {
            std.log.warn("Failed to set SNI hostname", .{});
        }
    }

    /// Require the peer certificate to name `hostname`: a DNS name is matched against the certificate's DNS
    /// subjectAltNames, an IP literal against its IP subjectAltNames. The subject CN is never consulted (BoringSSL
    /// still falls back to it by default), and without a name to check a valid chain proves nothing, so an empty
    /// name is an error rather than an unchecked connection.
    fn setExpectedPeer(ssl: *c.SSL, hostname: []const u8) !void {
        if (hostname.len == 0) return TlsError.CertificateVerificationFailed;
        const param = c.SSL_get0_param(ssl) orelse return TlsError.TlsContextFailed;
        c.X509_VERIFY_PARAM_set_hostflags(param, c.X509_CHECK_FLAG_NEVER_CHECK_SUBJECT);
        const ok = if (std.Io.net.IpAddress.parse(hostname, 0)) |ip| switch (ip) {
            .ip4 => |a| c.X509_VERIFY_PARAM_set1_ip(param, &a.bytes, a.bytes.len),
            .ip6 => |a| c.X509_VERIFY_PARAM_set1_ip(param, &a.bytes, a.bytes.len),
        } else |_| c.X509_VERIFY_PARAM_set1_host(param, hostname.ptr, hostname.len);
        if (ok != 1) return TlsError.CertificateVerificationFailed;
    }

    fn performHandshake(self: *Self) !void {
        const handshake_result = c.SSL_do_handshake(self.ssl);

        if (handshake_result == 1) {
            self.handshake_complete = true;
            try self.verifyCertificate();
            return;
        }

        if (!tls.sslWantsMoreData(self.ssl, handshake_result)) {
            const ssl_error = c.SSL_get_error(self.ssl, handshake_result);
            const verify_result = c.SSL_get_verify_result(self.ssl);
            c.ERR_clear_error();
            // Only a verification that ran and failed is a rejected certificate. Until one has run the result is
            // X509_V_ERR_INVALID_CALL, so an alert, a decrypt error or a middlebox cutting the handshake short stays
            // an ordinary (retryable) handshake failure. The caller decides whether a rejection is worth reporting.
            const rejected = verify_result != c.X509_V_OK and verify_result != c.X509_V_ERR_INVALID_CALL;
            if (self.verify_certificate and rejected) return TlsError.CertificateVerificationFailed;
            std.log.warn("TLS handshake with {s} failed with error: {}", .{ self.hostname, ssl_error });
            return TlsError.TlsHandshakeFailed;
        }
    }

    /// SSL_VERIFY_PEER already fails the handshake on a bad certificate; this guards against a context configured
    /// without it.
    fn verifyCertificate(self: *Self) !void {
        if (!self.verify_certificate) return;
        if (c.SSL_get_verify_result(self.ssl) != c.X509_V_OK) return TlsError.CertificateVerificationFailed;
    }

    fn readDecryptedData(self: *Self, _: ?[]u8) !?[]const u8 {
        self.buffers.decrypted_out.clearRetainingCapacity();

        var temp_buf: [BUFFER_SIZE]u8 = undefined;
        while (true) {
            const bytes_read = c.SSL_read(self.ssl, &temp_buf, temp_buf.len);
            if (bytes_read > 0) {
                try self.buffers.decrypted_out.appendSlice(self.allocator, temp_buf[0..@intCast(bytes_read)]);
            } else {
                if (self.buffers.decrypted_out.items.len > 0) return self.buffers.getDecryptedSlice();
                return try tls.handleSslReadError(self.ssl, bytes_read);
            }
        }
    }

    fn writeEncryptedData(self: *Self, data: []const u8) !usize {
        if (!self.handshake_complete) {
            std.log.warn("Attempt to encrypt data before handshake complete", .{});
            return TlsError.TlsNotReady;
        }

        const bytes_written = c.SSL_write(self.ssl, data.ptr, @intCast(data.len));
        if (bytes_written > 0) return @as(usize, @intCast(bytes_written));

        if (tls.sslWantsMoreData(self.ssl, bytes_written)) return 0;

        std.log.err("SSL_write failed with error: {}", .{c.SSL_get_error(self.ssl, bytes_written)});
        return TlsError.TlsWriteFailed;
    }

    fn readFromWriteBio(self: *Self) !void {
        self.buffers.encrypted_out.clearRetainingCapacity();
        var temp_buf: [BUFFER_SIZE]u8 = undefined;
        while (true) {
            const encrypted_len = try tls.readFromBio(self.bio_write, &temp_buf);
            if (encrypted_len == 0) break;
            try self.buffers.encrypted_out.appendSlice(self.allocator, temp_buf[0..encrypted_len]);
        }
    }
};
