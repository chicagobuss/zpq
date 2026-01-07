const std = @import("std");
const boring = @import("boring_tls");

/// A Sans-I/O wrapper around BoringSSL's TLS client.
/// This manages the cryptographic state machine but does not perform any I/O.
/// It is designed to be "pumped" by a transport layer (like libxev or std.Io.Evented).
///
/// Pattern:
/// 1. Call `encrypt(null)` to get initial handshake data (ClientHello).
/// 2. Write that to the socket.
/// 3. When data arrives on socket, call `decrypt(data)`.
/// 4. If `decrypt` returns plaintext, pass it to the application.
/// 5. Check if `encrypt(null)` has more data to send (handshake responses).
pub const Client = struct {
    inner: boring.tls_client.TlsClient,

    pub const Options = struct {
        verify_certificate: bool = true,
    };

    /// Initialize a new TLS client for the given host.
    pub fn init(host: []const u8, options: Options) !Client {
        return .{
            .inner = try boring.tls_client.TlsClient.init(host, .{
                .verify_certificate = options.verify_certificate,
            }),
        };
    }

    pub fn deinit(self: *Client) void {
        self.inner.deinit();
    }

    /// Returns the initial handshake data (ClientHello) to be sent to the server.
    pub fn startHandshake(self: *Client) !?[]const u8 {
        return self.inner.startHandshake();
    }

    /// Decrypts data received from the wire.
    /// Returns a slice of plaintext data if available. The slice is valid until
    /// the next call to any decryption/encryption method on this client.
    pub fn decrypt(self: *Client, encrypted: []const u8) !?[]const u8 {
        return self.inner.processIncoming(encrypted, null);
    }

    /// Encrypts plaintext data for transmission over the wire.
    /// If `plaintext` is null, it continues any pending handshake or administrative operations.
    /// Returns a slice of encrypted data to be written to the socket.
    /// The slice is valid until the next call to any decryption/encryption method.
    pub fn encrypt(self: *Client, plaintext: ?[]const u8) !?[]const u8 {
        return self.inner.processOutgoing(plaintext);
    }

    /// Returns true if the TLS handshake has successfully completed.
    pub fn isHandshakeComplete(self: *const Client) bool {
        return self.inner.handshake_complete;
    }
};
