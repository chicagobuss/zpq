const std = @import("std");
const xev = @import("xev");
const tls = @import("tls.zig");

const c = tls.c;
pub const TlsError = tls.TlsError;

const BUFFER_SIZE = tls.BUFFER_SIZE;

const TlsClientOptions = struct {
    verify_certificate: bool = true,
};

pub const TlsClient = struct {
    ssl_ctx: *c.SSL_CTX,
    ssl: *c.SSL,
    bio_read: *c.BIO,
    bio_write: *c.BIO,
    handshake_complete: bool = false,
    hostname: []const u8,
    buffers: tls.TlsBuffers = .{},
    verify_certificate: bool = true,

    const Self = @This();

    pub fn init(hostname: []const u8, options: TlsClientOptions) !Self {
        tls.initOpenSsl();

        const ctx = try createSslContext(options.verify_certificate);
        const ssl = createSslConnection(ctx) catch |err| {
            c.SSL_CTX_free(ctx);
            return err;
        };

        setSniHostname(ssl, hostname) catch |err| {
            c.SSL_free(ssl);
            c.SSL_CTX_free(ctx);
            return err;
        };

        const bio_read = tls.createBio() catch |err| {
            c.SSL_free(ssl);
            c.SSL_CTX_free(ctx);
            return err;
        };

        const bio_write = tls.createBio() catch |err| {
            _ = c.BIO_free(bio_read);
            c.SSL_free(ssl);
            c.SSL_CTX_free(ctx);
            return err;
        };

        c.SSL_set_bio(ssl, bio_read, bio_write);

        return .{
            .ssl_ctx = ctx,
            .ssl = ssl,
            .bio_read = bio_read,
            .bio_write = bio_write,
            .hostname = hostname,
            .verify_certificate = options.verify_certificate,
        };
    }

    pub fn deinit(self: *Self) void {
        c.SSL_free(self.ssl);
        c.SSL_CTX_free(self.ssl_ctx);
    }

    pub fn startHandshake(self: *Self) !?[]const u8 {
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
        const t0 = std.time.Instant.now() catch unreachable;

        if (encrypted_data.len > 0) {
            try tls.writeToBio(self.bio_read, encrypted_data);
        }

        const t1 = std.time.Instant.now() catch unreachable;

        if (!self.handshake_complete) {
            try self.performHandshake();
        }

        const t2 = std.time.Instant.now() catch unreachable;

        if (!self.handshake_complete) return null;

        const result = try self.readDecryptedData(user_buf);

        const t3 = std.time.Instant.now() catch unreachable;

        const bio_ns = t1.since(t0);
        const hs_ns = t2.since(t1);
        const read_ns = t3.since(t2);
        const total_ns = t3.since(t0);

        // Only log if total > 1ms
        if (total_ns > 1_000_000) {
            std.log.info("processIncoming breakdown: bio={d}us hs={d}us read={d}us total={d}us", .{ bio_ns / 1000, hs_ns / 1000, read_ns / 1000, total_ns / 1000 });
        }

        return result;
    }

    pub fn processOutgoing(self: *Self, plaintext: ?[]const u8) !?[]const u8 {
        if (plaintext) |data| {
            try self.writeEncryptedData(data);
        }

        return try self.readFromWriteBio();
    }

    pub fn isHandshakeComplete(self: *Self) bool {
        return self.handshake_complete;
    }

    fn createSslContext(verify_certificate: bool) !*c.SSL_CTX {
        const method = c.TLS_client_method();
        const ctx = c.SSL_CTX_new(method) orelse {
            std.log.err("Failed to create SSL context", .{});
            return TlsError.TlsContextFailed;
        };

        if (verify_certificate) {
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
        } else {
            c.SSL_CTX_set_verify(ctx, c.SSL_VERIFY_NONE, null);
        }

        _ = c.SSL_CTX_set_options(ctx, c.SSL_OP_NO_SSLv2 | c.SSL_OP_NO_SSLv3 | c.SSL_OP_NO_COMPRESSION);
        return ctx;
    }

    fn createSslConnection(ctx: *c.SSL_CTX) !*c.SSL {
        return tls.createSslInstance(ctx);
    }

    fn setSniHostname(ssl: *c.SSL, hostname: []const u8) !void {
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

    fn performHandshake(self: *Self) !void {
        const handshake_result = c.SSL_do_handshake(self.ssl);

        if (handshake_result == 1) {
            self.handshake_complete = true;
            self.verifyCertificate();
            return;
        }

        if (!tls.sslWantsMoreData(self.ssl, handshake_result)) {
            std.log.err("TLS handshake failed with error: {}", .{c.SSL_get_error(self.ssl, handshake_result)});
            return TlsError.TlsHandshakeFailed;
        }
    }

    fn verifyCertificate(self: *Self) void {
        if (!self.verify_certificate) return;

        const verify_result = c.SSL_get_verify_result(self.ssl);
        if (verify_result != c.X509_V_OK) {
            std.log.warn("Certificate verification failed: {}", .{verify_result});
        }
    }

    fn readDecryptedData(self: *Self, user_buf: ?[]u8) !?[]const u8 {
        self.buffers.resetDecrypted();

        if (user_buf) |buf| {
            const bytes_read = c.SSL_read(self.ssl, buf.ptr, @intCast(buf.len));
            if (bytes_read > 0) {
                return buf[0..@as(usize, @intCast(bytes_read))];
            }
            return try tls.handleSslReadError(self.ssl, bytes_read);
        }

        var temp_buf: [BUFFER_SIZE]u8 = undefined;
        const bytes_read = c.SSL_read(self.ssl, &temp_buf, temp_buf.len);
        if (bytes_read > 0) {
            const read_size = @as(usize, @intCast(bytes_read));
            if (read_size > self.buffers.decrypted_buffer.len) {
                return TlsError.TlsReadFailed;
            }
            @memcpy(self.buffers.decrypted_buffer[0..read_size], temp_buf[0..read_size]);
            self.buffers.decrypted_len = read_size;
            return self.buffers.getDecryptedSlice();
        }

        return try tls.handleSslReadError(self.ssl, bytes_read);
    }

    fn writeEncryptedData(self: *Self, data: []const u8) !void {
        if (!self.handshake_complete) {
            std.log.warn("Attempt to encrypt data before handshake complete", .{});
            return TlsError.TlsNotReady;
        }

        const bytes_written = c.SSL_write(self.ssl, data.ptr, @intCast(data.len));
        if (bytes_written > 0) return;

        if (tls.sslWantsMoreData(self.ssl, bytes_written)) return;

        std.log.err("SSL_write failed with error: {}", .{c.SSL_get_error(self.ssl, bytes_written)});
        return TlsError.TlsWriteFailed;
    }

    fn readFromWriteBio(self: *Self) !?[]const u8 {
        self.buffers.resetEncrypted();
        const encrypted_len = try tls.readFromBio(self.bio_write, &self.buffers.encrypted_buffer);
        self.buffers.encrypted_len = encrypted_len;

        return if (self.buffers.encrypted_len > 0) self.buffers.getEncryptedSlice() else null;
    }
};
