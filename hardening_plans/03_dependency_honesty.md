# Plan 03: Dependency Honesty

## Criticism
**"'Zero-Dependency' is Misleading."**
We claim "Zero-Dependency" while linking `libc`, `openssl` (via boring), and using `libxev`.

## Response
**Valid.** In the Zig community, "Zero Dependency" usually implies "Pure Zig, no C libs, no system deps". We are not that. We are "No Runtime SDK Dependency" (i.e., we don't need the AWS SDK or `libcurl`).

## Action Plan

### 1. Reframe Documentation
Update `README.md` and `STATUS.md`.
- **Change**: "Zero-Dependency" -> "**No External SDK Dependencies**".
- **Clarify**: Explicitly list build-time and link-time dependencies:
    - `libxev` (Zig package, static).
    - `boring_tls` (C library, statically linked).
    - `libc` (System library, required for DNS/OpenSSL).

### 2. Architecture Diagram Update
If we have diagrams, show `zpq` statically encompassing its dependencies, contrasting with a dynamic link to `libcurl` or Python's `boto3`.

### 3. "Pure Zig" Aspiration
Add a roadmap item (Milestone X) to investigate `zig-tls` (pure Zig TLS) and `zig-dns`, which would allow us to drop `boring_tls` and `libc`, truly achieving "Zero Dependency".

