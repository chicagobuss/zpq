# S3 Read Design (Phase 3)

The contract for `src/io/{tls,http,sigv4,s3}.zig`. Read this before
writing or modifying S3 I/O code.

## Provenance

The pre-rewrite tree had an `src/protocol/{sigv4,http,s3}.zig` stack
(~800 LoC total) plus a `vendor/boring_tls/src/tls_client.zig`
(~263 LoC) wrapping BoringSSL via memory BIOs. Most of it survives
the v2 rewrite intact except for:
- 0.16-stdlib removals: `std.posix.clock_gettime`,
  `std.time.Instant.now()`, `ArrayListUnmanaged{}` empty literal.
- Stale `const xev = @import("xev")` imports (unused since vendored
  libxev was removed).

Strategy: **port what's well-shaped, rewrite what was coupled to
the old transport layer.**

| Component | Strategy | Why |
|---|---|---|
| `vendor/boring_tls/*` | port (drop unused xev imports + 0.16 stdlib fixes) | The BIO_s_mem pattern is correct per Tier-2 grimoire; rewriting wins us nothing. |
| `src/io/sigv4.zig` | port from `src/protocol/sigv4.zig` | Self-contained sans-IO module; AWS spec is fiddly so we keep the proven implementation. |
| `src/io/tls.zig` | thin new wrapper around boring_tls | The old wrapper had profiling instrumentation we don't need; rewrite as a smaller surface. |
| `src/io/http.zig` | rewrite | The old code was coupled to the deleted transport layer. Cleaner to start fresh. |
| `src/io/s3.zig` | rewrite | Same. |

## Decisions

### 1. Synchronous first, async later

Phase 3 ships a synchronous client: open TCP, TLS handshake, send
request, drain response, close. One request per connection.

Async S3 (parallel range fetches via the epoll Loop) is Phase 3.B —
real perf wins but real complexity. We need the sync version to
exist first so the design + tests are well-shaped.

### 2. TLS via BoringSSL + memory BIOs

The Tier-2 grimoire is explicit:
> Use `BIO_s_mem` to decouple crypto from I/O.
> *Read Path*: Socket → Ring Buffer → `BIO_write` → `SSL_read` → Application.
> *Write Path*: Application → `SSL_write` → `BIO_read` → Ring Buffer → Socket.

Rationale: TLS state machine is independent of I/O strategy. Memory
BIOs let us pump bytes through TLS however we want — sync sockets
in Phase 3, the epoll Loop in Phase 3.B.

Cert verification: load from `/etc/ssl/cert.pem` (Lambda) /
`/etc/pki/tls/cert.pem` (AL2023) / system default. Always verify
in production; flag to disable for tests against self-signed
endpoints.

### 3. HTTP/1.1 client shape

```zig
pub const Request = struct {
    method: enum { GET, HEAD, PUT, POST },
    host: []const u8,
    path: []const u8,
    headers: []const Header,
    body: []const u8,
};

pub const Response = struct {
    status: u16,
    headers: []const Header,
    body: []const u8, // arena-allocated, lifetime = arena
};

pub fn send(arena, host, port, request) !Response;
```

Caller-allocated arena. One request per call. No connection pool
yet. Keep-alive + pool live in Phase 3.B.

Body length: trust `Content-Length`. `chunked` not supported in
Phase 3 — S3 GETs return `Content-Length`. Add chunked when a
real fixture demands it.

### 4. SigV4 signing at request build time

Sign the canonical request before sending. No streaming-payload
SigV4 (we don't need it for GETs; PUTs come in Phase 4).

Use `UNSIGNED-PAYLOAD` for GETs — saves the SHA-256 hash over the
empty body. Permitted by AWS for HTTPS connections (which we
always use to S3).

### 5. S3 URL parsing

```
s3://<bucket>/<key>           # input format
↓
host = <bucket>.s3.<region>.amazonaws.com
path = /<key>                 # url-encoded segments
```

Virtual-hosted style. Modern. Region from `AWS_REGION` env or
explicit override. No legacy path-style support — bucket names
with dots can't use vhost style, but those are rare in modern
practice and we error out clearly.

### 6. Credentials sourcing

Read from environment, in order:
1. `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (+ optional
   `AWS_SESSION_TOKEN`).
2. (Future) IMDSv2 for non-Lambda EC2 metadata.

Lambda always populates the env vars; no IMDS fallback needed
for our deploy target. The CLI binary picks them up from the
shell or `.env`.

Region: `AWS_REGION` env var. No fallback to `us-east-1` — fail
explicitly if missing.

### 7. Footer-fetch then column-fetch pattern

```
1. HEAD bucket/key                   → Content-Length = file size
   (or skip — see below)
2. GET bucket/key bytes=last 64KB    → footer + maybe more
   - Parse trailing PAR1 magic + length
   - If footer is bigger than what we fetched, GET again with
     correct range
3. Plan: which row groups + columns do we actually need?
4. For each needed column chunk:
   GET bucket/key bytes=start-end    → column chunk bytes
5. Decode in-memory
```

Skipping the HEAD: if the file is small enough to fetch in one
range request, we can do GET bytes=-65536 (the last 64 KB) and
also receive the file size in the `Content-Range` response
header. One round-trip instead of two for the metadata phase.
Implement after we have the basic GET working.

### 8. Lifetime contract

All bytes returned (response body, decoded values) are
arena-allocated. The handler-level arena resets per invocation.
Per-row-group sub-arenas can reset within an invocation if we
need finer memory control.

The TLS Client + HTTP Client + S3 Client themselves are each
caller-stack-allocated for the duration of one fetch.

## What this design rejects

- **Async runtime / event loop integration in Phase 3.** Adds two
  layers of state machinery and we don't need it for correctness.
  Phase 3.B is when we layer the epoll Loop on top.
- **Connection pool.** Cold S3 connect is ~5 ms (per the probe);
  for one-shot Lambda invocations this is acceptable. Pool when
  we have a workload that does N>1 fetches per invocation.
- **HTTP chunked transfer encoding.** S3 GETs always send
  Content-Length. PUTs we'll handle when we ship them.
- **AWS SDK.** Same reason as everywhere else — 50 MB of binary
  bloat for one HTTP call.
- **`std.http.Client`.** It's there in 0.16 stdlib but it doesn't
  speak SigV4 and rolling SigV4 against the std.http abstractions
  is more work than rolling our own HTTP client.
- **Path-style URL support.** Modern S3 has been vhost-style only
  for years. Bucket names with dots that need path-style aren't
  worth the API surface.

## What this design defers

- Phase 3.B: connection pooling + keep-alive.
- Phase 3.C: parallel range fetches via the epoll Loop.
- Phase 3.D: PUT / multipart upload (the S3 sink).
- Future: IMDSv2 for non-Lambda EC2 credential discovery.
- Future: HEAD optimization (skip the round-trip; use
  `Content-Range` from the suffix-byte GET).

## Test plan

Unit tests:
- SigV4: AWS spec example vectors (the canonical-request,
  string-to-sign, derived-key chain has well-published test
  cases).
- HTTP/1.1 parser: valid + malformed status lines, header parse.
- S3 URL parser: well-formed and pathological inputs.

Integration tests (drive against the real S3, gated on `.env`):
- Range GET: fetch the last 64 KB of `data/benchmark_100mb.parquet`
  uploaded to our test bucket.
- Open + decode end-to-end: pull the file from S3, decode int8,
  assert the same row count as the local fixture.

Lambda smoke test: invoke with `{"s3_url": "s3://..."}`, the
handler fetches and decodes, returns aggregate stats. Confirm in
production that the SigV4 + HTTPS + range GET path works against
real S3 from inside Lambda.
