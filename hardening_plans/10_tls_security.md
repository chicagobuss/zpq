# Plan 10: TLS Security (Critical)

## Criticism
**"TLS Certificate Verification is Disabled."**
`verify_certificate = false` is insecure and unacceptable for production code.

## Response
**CRITICAL.** This was a shortcut for development (testing against self-signed MinIO) that was left as default.

## Action Plan

### 1. Secure Defaults
Change `Connection.init`:
- Default `verify_certificate = true`.
- Default `trust_store = SystemDefault`.

### 2. Root CA Loading
We need a way to load root CAs.
- **Linux/macOS**: `boring_tls` might handle this if configured correctly, or we need to probe common paths (`/etc/ssl/certs/ca-certificates.crt`).
- **Zig Integration**: Use `std.crypto.Certificate.Bundle` (if available) or `zig-cert` to embed a CA bundle if system loading is unreliable in static binaries (common issue in Lambda/Alpine).
- **Decision**: For now, **embed the Mozilla CA bundle** (via `@embedFile`) as a fallback if system roots aren't found. This ensures the static binary "just works" securely.

### 3. Insecure Flag
Add explicit `S3Config.allow_insecure` boolean.
- Exposed in CLI as `--insecure`.
- Only when this is `true` do we set `verify_certificate = false`.

### 4. Verification
Test against `https://badssl.com/` (or similar) to prove that:
- `expired.badssl.com` fails.
- `self-signed.badssl.com` fails.
- `google.com` passes.

