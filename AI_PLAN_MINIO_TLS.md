# Plan: MinIO TLS Verification & Integration

**Goal**: Verify `zpq`'s new `libxev` + `boring_tls` stack against a local, TLS-enabled S3 compatible server (MinIO). This ensures full protocol compliance (encryption, HTTP/1.1 chunking, S3 signatures) before hitting AWS.

## 1. Infrastructure Setup (`tools/minio_tls/`)
We need a reproducible environment.
*   [x] **Directory**: `tools/minio_tls`
*   [x] **Certs**: Script to generate `public.crt` and `private.key` (or let MinIO auto-gen and extract them).
*   [x] **Docker Compose**: `docker-compose.yml` to run MinIO with:
    *   TLS enabled (mounting certs).
    *   Console exposed.
    *   Static credentials (`minioadmin` / `minioadmin`).
*   [x] **Trace Helper**: `trace.sh` wrapper around `mc admin trace` to see server-side request details.

## 2. Client Debugging Enhancements
We need visibility into the "Black Box" of TLS.
*   [x] **Toggleable Logging**: Modify `src/zpq/io/tls/connection.zig` to support a compile-time or runtime flag for verbose logging (don't spam by default).
*   [x] **State Transitions**: Log `Handshake Start` -> `Handshake Complete` -> `App Data`.
*   [x] **Error Mapping**: Ensure BoringSSL error codes are printed clearly (not just `error.Unexpected`).

## 3. Integration Test (`test_minio_https.zig`)
A dedicated test for this environment.
*   [x] **Clone**: Base on `test_http_client.zig`.
*   [x] **Config**: Connect to `localhost:9000` (or mapped port).
*   [x] **Trust**:
    *   *Phase A*: `verify_certificate = false` (Verify encryption/protocol). **DONE**
    *   *Phase B*: Load MinIO's `public.crt` as CA (Verify chain of trust).
*   [x] **Scenario**:
    *   `HEAD /bucket` (404 Not Found is success - proves connectivity).
    *   `GET /bucket/object` (403 Forbidden is success - proves S3 checks signature).

## 4. Execution Workflow
1.  `cd tools/minio_tls && ./gen_certs.sh && docker-compose up -d`
2.  `./trace.sh` (in separate term)
3.  `zig build --build-file micro_build.zig test-minio-https`
4.  Verify client logs match server trace.

## 5. Next Steps (Post-Verification)
*   **`AsyncS3Source`**: Wiring this validated client into the main Parquet reader.
