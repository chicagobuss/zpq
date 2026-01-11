const std = @import("std");

// Force update
pub const c = @cImport({
    @cDefine("_FORTIFY_SOURCE", "0");
    @cInclude("openssl/ssl.h");
    @cInclude("openssl/err.h");
    @cInclude("openssl/bio.h");
    @cInclude("openssl/x509v3.h");
});

pub const TlsError = error{
    TlsContextFailed,
    TlsConnectionFailed,
    BioFailed,
    TlsHandshakeFailed,
    TlsReadFailed,
    TlsWriteFailed,
    TlsNotReady,
    CertificateLoadFailed,
    PrivateKeyLoadFailed,
    CertificateVerificationFailed,
    TlsConnectionClosed,
};

pub const BUFFER_SIZE = 4096;

pub fn initOpenSsl() void {
    _ = c.OPENSSL_init_ssl(c.OPENSSL_INIT_LOAD_SSL_STRINGS | c.OPENSSL_INIT_LOAD_CRYPTO_STRINGS, null);
}

pub fn createBio() !*c.BIO {
    return c.BIO_new(c.BIO_s_mem()) orelse {
        std.log.err("Failed to create BIO", .{});
        return TlsError.BioFailed;
    };
}

pub fn createSslInstance(ctx: *c.SSL_CTX) !*c.SSL {
    return c.SSL_new(ctx) orelse {
        std.log.err("Failed to create SSL instance", .{});
        return TlsError.TlsConnectionFailed;
    };
}

pub fn writeToBio(bio: *c.BIO, data: []const u8) !void {
    const written = c.BIO_write(bio, data.ptr, @intCast(data.len));
    if (written <= 0) {
        std.log.err("Failed to write to BIO", .{});
        return TlsError.TlsReadFailed;
    }
}

pub fn readFromBio(bio: *c.BIO, buffer: []u8) !usize {
    var total_read: usize = 0;

    while (total_read < buffer.len) {
        const remaining = buffer.len - total_read;
        const bytes_read = c.BIO_read(bio, buffer.ptr + total_read, @intCast(remaining));
        if (bytes_read <= 0) break;

        total_read += @as(usize, @intCast(bytes_read));
    }

    return total_read;
}

pub fn handleSslReadError(ssl: *c.SSL, bytes_read: c_int) !?[]const u8 {
    const ssl_error = c.SSL_get_error(ssl, bytes_read);

    return switch (ssl_error) {
        c.SSL_ERROR_WANT_READ, c.SSL_ERROR_WANT_WRITE => null,
        c.SSL_ERROR_ZERO_RETURN => TlsError.TlsConnectionClosed,
        else => {
            std.log.err("SSL_read failed with error: {}", .{ssl_error});
            logOpenSslError();
            return TlsError.TlsReadFailed;
        },
    };
}

pub fn sslWantsMoreData(ssl: *c.SSL, result: c_int) bool {
    const ssl_error = c.SSL_get_error(ssl, result);
    return ssl_error == c.SSL_ERROR_WANT_READ or ssl_error == c.SSL_ERROR_WANT_WRITE;
}

pub fn logOpenSslError() void {
    const err_code = c.ERR_get_error();
    if (err_code == 0) return;

    var err_buf: [256]u8 = undefined;
    _ = c.ERR_error_string_n(err_code, &err_buf, err_buf.len);
    std.log.err("OpenSSL error: {s}", .{std.mem.sliceTo(&err_buf, 0)});
}

pub fn logDetailedSslError(ssl: *c.SSL) void {
    std.log.err("=== SSL Error Details ===", .{});

    var error_count: u32 = 0;
    while (true) {
        const err_code = c.ERR_get_error();
        if (err_code == 0) break;

        var err_buf: [256]u8 = undefined;
        _ = c.ERR_error_string_n(err_code, &err_buf, err_buf.len);
        std.log.err("Error {}: {s}", .{ error_count, std.mem.sliceTo(&err_buf, 0) });
        error_count += 1;
    }

    if (error_count == 0) {
        std.log.err("No errors in OpenSSL queue", .{});
    }

    const state = c.SSL_get_state(ssl);
    const state_string = c.SSL_state_string_long(ssl);
    std.log.err("SSL State: {} ({s})", .{ state, @as([*:0]const u8, @ptrCast(state_string)) });

    std.log.err("=== End SSL Error Details ===", .{});
}

pub const TlsBuffers = struct {
    encrypted_out: std.ArrayListUnmanaged(u8) = .{},
    decrypted_out: std.ArrayListUnmanaged(u8) = .{},

    pub fn deinit(self: *TlsBuffers, allocator: std.mem.Allocator) void {
        self.encrypted_out.deinit(allocator);
        self.decrypted_out.deinit(allocator);
    }

    pub fn getEncryptedSlice(self: *TlsBuffers) []const u8 {
        return self.encrypted_out.items;
    }

    pub fn getDecryptedSlice(self: *TlsBuffers) []const u8 {
        return self.decrypted_out.items;
    }
};

pub const ProcessResult = struct {
    encrypted: ?[]const u8,
    consumed: usize,
};
