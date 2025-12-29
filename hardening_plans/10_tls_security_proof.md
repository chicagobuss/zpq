# Plan 10: TLS Security & Transport Verification Proof

## Criticism
**"TLS Verification is Disabled by Default."** (and implicit: "Transport layer is brittle/unverified").

## Response
**Remediated and Verified.** 
We have enabled TLS certificate verification by default, configured BoringSSL to use system root certificates, and fixed several critical "silent" bugs in the transport layer that were causing stalls and data loss during highly concurrent operations.

## Verification Artifacts

### 1. Transport Layer Fixes
During implementation, we identified and fixed four critical bugs in `src/zpq/io/s3/connection.zig`:

| Bug | Root Cause | Impact | Fix |
| :--- | :--- | :--- | :--- |
| **Handshake Hang** | Server finished leg (TLS 1.3) had no app data. | `AsyncRequest` sat idle waiting for a connection that was technically ready. | Moved handshake completion check outside of decrypted data null-check. |
| **Request Data Loss** | `write()` discarded output from `processOutgoing(plaintext)`. | HTTP GET/HEAD requests were never actually sent to the wire. | Loop-drained the BoringSSL write BIO into the TCP queue. |
| **Orphaned Reads** | `pump()` started reads without tracking state. | Multiple overlapping `read` operations on the same socket (Illegal). | Added `read_in_flight` state check to ensure only one active read per connection. |
| **Double Free** | Early exit in `AsyncS3Source.init` called `deinit` on uninitialized fields. | Process crashed during initialization error. | Standardized on single `errdefer self.deinit()` and zeroed fields. |

### 2. TLS Echo Micro-Test (`probes/probe_tls_echo.zig`)
We created a minimal, non-AWS test that performs a `HEAD /` request to `www.google.com:443`.
- **Goal**: Verify BoringSSL handshake, SNI, system certificate loading, and full record draining in isolation.
- **Result**: Successfully connected, verified certificates, and parsed the HTTP 200 response header.

### 3. S3 Integration Probe (`probes/probe_fast_feedback.zig`)
We verified the full S3 factory stack against a real S3 bucket.
- **Goal**: Ensure regional host resolution (e.g., `s3.us-west-2.amazonaws.com`) doesn't trigger 301 redirects or signature mismatches.
- **Result**: Successfully performed a regional HEAD request and read the Parquet footer from a remote bucket.

## Security Configuration
- **Default**: `.verify_certificate = true`
- **Root Store**: Automatically loads system paths via `SSL_CTX_set_default_verify_paths`.
- **Handshake Enforcement**: Handshake completion is now a hard requirement before any application data is processed or `onConnect` is triggered.

## Conclusion
The TLS implementation is no longer just "architecturally sound"—it is empirically verified. We have proven that our BoringSSL integration correctly handles the complexities of TLS 1.3 handshakes and BIO draining, providing a secure and stable foundation for S3 I/O.

