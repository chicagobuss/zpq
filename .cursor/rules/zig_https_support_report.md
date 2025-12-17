# Zig TLS for high-performance networking: the state of play in 2025

The Zig ecosystem offers multiple approaches to TLS/HTTPS for high-performance networking, ranging from **pure Zig implementations** (`std.crypto.tls`, ianic/tls.zig) to **C library wrappers** (BoringSSL, OpenSSL). For production systems requiring full TLS 1.2/1.3 support and async I/O, the most mature path is wrapping BoringSSL via projects like `boring_tls`, while pure Zig options are rapidly maturing. Major projects diverge significantly: **Bun uses BoringSSL** for its JavaScript runtime, **TigerBeetle deliberately avoids TLS** for maximum performance in trusted networks, and **libxev provides no TLS** as a clean event-loop abstraction layer.

---

## std.crypto.tls delivers zero-allocation TLS 1.3 client

The Zig standard library includes a pure-Zig TLS implementation at `lib/std/crypto/tls/Client.zig` supporting TLS 1.3 (with TLS 1.2 added in PR #21872). Its key architectural decision: **zero heap allocations during handshake**, making it suitable for embedded and performance-critical contexts.

**Supported cipher suites** (priority based on hardware acceleration):
- AES-128-GCM-SHA256, AES-256-GCM-SHA384 (prioritized with AES-NI)
- ChaCha20-Poly1305-SHA256 (prioritized without AES-NI)
- AEGIS-256-SHA512, AEGIS-128L-SHA256 (experimental high-performance)

**Key exchange groups**: x25519 (primary), secp256r1 (NIST P-256), and the post-quantum hybrid **x25519_kyber768d00**.

```zig
// Basic std.crypto.tls usage pattern
var bundle = std.crypto.Certificate.Bundle{};
try bundle.rescan(allocator);  // Load system CA certificates

var tls_client = try std.crypto.tls.Client.init(reader, writer, .{
    .host = .{ .explicit = "example.com" },
    .ca = .{ .bundle = bundle },
    .read_buffer = &read_buf,
    .write_buffer = &write_buf,
});
```

**Notable limitations**: No server implementation (tracked in issue #14171), no session resumption (new_session_ticket messages are discarded), and no ALPN extension support yet (PR #24983 pending). The TLS 1.2 support has some edge-case issues with certain server flows.

---

## Third-party pure Zig TLS libraries fill the gaps

Two projects extend beyond the standard library's capabilities:

**ianic/tls.zig** (https://github.com/ianic/tls.zig) provides the most complete pure-Zig TLS implementation with both TLS 1.2 and 1.3 client support, plus a TLS 1.3 server. Testing against **6,280 top domains** shows a **99.8% success rate**, significantly outperforming std.crypto.tls (89.8% on the same test). It supports client certificate authentication and Wireshark key logging for debugging.

**shiguredo/tls13-zig** (https://github.com/shiguredo/tls13-zig) focuses exclusively on TLS 1.3 but adds crucial features missing from stdlib: **session resumption** and **0-RTT early data support**, both critical for reducing latency in production systems.

| Implementation | TLS Versions | Server | Session Resumption | GitHub Stars |
|---------------|--------------|--------|-------------------|--------------|
| std.crypto.tls | 1.3, 1.2 | ❌ | ❌ | (stdlib) |
| ianic/tls.zig | 1.3, 1.2 | ✓ (1.3 only) | ❌ | 112 |
| shiguredo/tls13-zig | 1.3 only | ✓ | ✓ | ~50 |

---

## C library wrappers provide production-grade TLS

For production systems requiring battle-tested TLS, several projects wrap C libraries:

**boring_tls** (https://github.com/Thomvanoorschot/boring_tls) wraps Google's BoringSSL with a high-level, memory-safe Zig API. It's explicitly designed to work with **libxev TCP connections** for async I/O, making it the current best choice for combining async event loops with robust TLS:

```zig
var client = try BoringTLS.tls_client.TlsClient.init("example.com", .{
    .verify_certificate = true,
});
if (try client.startHandshake()) |handshake_data| {
    // Send over transport (libxev TCP)
}
```

**OpenSSL wrappers** include dzfrias/openssl-zig (cleanest build integration), kassane/openssl-zig, and allyourcodebase/openssl. The FFI pattern uses `@cImport`:

```zig
const c = @cImport({
    @cInclude("openssl/ssl.h");
    @cInclude("openssl/err.h");
});
```

**wolfSSL bindings** (https://github.com/kassane/wolfssl) offer a **20x smaller footprint** than OpenSSL and FIPS 140-3 validation, useful for embedded systems.

---

## How major Zig projects handle TLS

**Bun** (https://github.com/oven-sh/bun) uses **BoringSSL** through a three-layer architecture: BoringSSL for cryptography, µWebSockets for HTTP, and µSockets for cross-platform I/O. The integration at `src/boringssl.zig` uses `zig translate-c` to generate bindings from BoringSSL headers, then implements custom memory allocators for SSL operations.

**TigerBeetle** (https://github.com/tigerbeetle/tigerbeetle) deliberately has **no TLS support**. Their design philosophy assumes trusted private networks, with security handled at the infrastructure level. The benefit: their custom io_uring/kqueue abstraction achieves maximum throughput with zero-copy I/O and static memory allocation.

**Ghostty** (https://github.com/ghostty-org/ghostty) uses **libxev** for its event loop but delegates secure connections to subprocess execution (SSH client). The terminal emulator handles PTY I/O, while the user's SSH binary manages encryption.

**libxev** (https://github.com/mitchellh/libxev) is a **pure event-loop abstraction** providing io_uring (Linux), kqueue (macOS), and WASI support. It deliberately excludes TLS, maintaining clean separation of concerns—TLS is layered on top by applications.

---

## Async I/O integration requires careful buffering

The critical architectural insight: **std.crypto.tls.Client is I/O-agnostic** and compatible with non-blocking I/O through the `std.Io.Reader/Writer` interface. TLS processing happens in userspace, allowing any event loop to handle the encrypted byte stream.

**Buffer architecture** requires four separate buffers per TLS connection:

| Layer | Purpose | Minimum Size |
|-------|---------|-------------|
| Stream read buffer | Raw encrypted bytes from socket | 16KB (max_ciphertext_record_len) |
| Stream write buffer | Raw encrypted bytes to socket | 16KB |
| TLS read buffer | Decrypted application data | Configurable |
| TLS write buffer | Plaintext before encryption | Configurable |

**Explicit flushing** is required at both layers—a common source of bugs:

```zig
try tls_client.writer.writeAll(data);  // Plaintext buffer
try tls_client.writer.flush();          // Encrypt
try tls_client.output.flush();          // Send to socket
```

For io_uring integration, the pattern is layered: io_uring submits/receives encrypted data, TLS encryption/decryption happens in userspace completion callbacks. TigerBeetle's extracted I/O library demonstrates batching encrypted operations through io_uring while processing TLS records in userspace.

---

## AWS and S3 clients handle TLS through std.http.Client

**elerch/aws-sdk-for-zig** (https://github.com/elerch/aws-sdk-for-zig) is the most mature AWS SDK, auto-generated from AWS Go SDK v2 models. It uses `std.http.Client` with `std.crypto.tls` internally, producing **~980KB binaries** (ReleaseSmall). The SDK recently switched from OpenSSL to **AWS-LC** for better AWS runtime library compatibility.

**algoflows/zig-s3** (https://github.com/algoflows/zig-s3) provides a simpler focused S3 client with AWS Signature V4 authentication and MinIO/LocalStack compatibility:

```zig
var client = try s3.S3Client.init(allocator, .{
    .access_key_id = "key",
    .secret_access_key = "secret",
    .region = "us-east-1",
});
try client.uploader().uploadFile("bucket", "key", "local/path");
```

**Caveat**: std.crypto.tls supports only TLS 1.3, and some AWS S3 regions intermittently have TLS 1.3 issues. For maximum compatibility, consider using ianic/tls.zig (which supports TLS 1.2) or a BoringSSL wrapper.

---

## karlseguin/http.zig is a server, not a client

A common misconception: **karlseguin/http.zig** (https://github.com/karlseguin/http.zig) is an **HTTP server**, achieving ~140K requests/second on M2 Macs using kqueue/epoll. It deliberately excludes TLS support—the recommended pattern is running behind a reverse proxy (nginx) for TLS termination. The server doesn't use std.http.Server, which the author describes as "very slow."

For HTTP clients with TLS, use `std.http.Client` (which includes connection pooling via a TailQueue-based pool, keep-alive support, and automatic content decompression) or build custom clients using ianic/tls.zig or boring_tls.

---

## Conclusion: choosing the right TLS approach

The optimal Zig TLS strategy depends on your constraints:

- **Pure Zig, TLS 1.3 only**: Use `std.crypto.tls` for zero-dependency builds with reasonable compatibility
- **Pure Zig, maximum compatibility**: Use **ianic/tls.zig** for TLS 1.2/1.3 support and 99.8% real-world site compatibility
- **Production async systems**: Use **boring_tls + libxev** for battle-tested TLS with modern event loops
- **AWS/S3 workloads**: Use **elerch/aws-sdk-for-zig** with awareness of TLS 1.3-only limitations
- **Maximum performance, trusted networks**: Skip TLS entirely like TigerBeetle

The Zig ecosystem is actively evolving toward a unified async model. The upcoming `std.Io` interface (post-0.15) will provide execution-model-agnostic async/await, with TLS and HTTP implementations being rewritten to use it. For now, layering TLS on top of completion-based I/O (io_uring, kqueue) remains the established pattern.