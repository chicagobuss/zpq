---
trigger: always_on
---

# Tier 1: The Soul of ZPQ

**Mission**: Build the fastest, leanest serverless Parquet engine in existence.

## The Core Philosophy: "Laziness is Performance"
ZPQ is architected around doing the absolute minimum amount of work required to satisfy a query.
1.  **Late Materialization**: Data stays encoded (RLE/BitPacked) until the last possible nanosecond.
2.  **Zero-Copy**: Never copy what you can borrow. Use the kernel's page cache. Pass pointers, not buffers.
3.  **Zero-Alloc**: Allocations are failures of prediction. Use arenas. Use pools. Predict memory usage upfront.
4.  **Sans-IO**: Logic is pure. I/O is a side effect. The CLI binary and the Lambda binary share `core/` and pick different `io/` strategies — the core never knows which.

## The Stack (The "Holy Trinity")
We trade "safety abstractions" for raw, hand-tuned architectural control.
*   **Language**: **Zig 0.16.0** (release, not master). Pinned in `.zig-version` and `build.zig.zon`. Read the standard library before guessing — 0.16 reorganized large parts of `std.posix`, `std.fs`, `std.net`, `std.time`, and the `Io` interface.
*   **Event Loop**: **libxev**, with the backend chosen per target. **io_uring on native Linux**, **kqueue on macOS**, **epoll on Lambda** (mandatory — see Tier 2). Hot-path code reasons in epoll-readiness terms because that's the lowest common denominator.
*   **Crypto**: **BoringSSL**. We do not use Go/Rust wrappers. We bind directly to the C library using memory BIOs for pure "stream encryption" without blocking sockets. Vendored prebuilt artifacts (`vendor/boring_tls/prebuilt/<triple>/`) — never source-built.
*   **Storage**: **AWS S3 (Native)**. We do not use the AWS SDK. We wrote our own SigV4 signer, HTTP/1.1 client, and XML parser to save 50 MB of binary bloat.

## The Two Binaries
ZPQ ships as two top-level entry points sharing the same `src/zpq.zig` core:
*   `zpq` — CLI binary (`src/cli/main.zig`). Workstation use. Imports libxev with whichever backend the target has.
*   `zpq-lambda` — Lambda bootstrap binary (`src/lambda/main.zig`). **Excludes io_uring code at compile time** via the `build_options.lambda` flag. Single static binary, packaged as `bootstrap` for `provided.al2023`.

The split is enforced at the build-system level, not at runtime. A Lambda binary that contains io_uring code is a bug.

## The Mindset: "Grumpy Elitism"
*   **Skepticism**: "Libraries are usually bloated and broken." We verify everything. We probe Lambda's actual seccomp policy with `probe_lambda_caps` instead of trusting AWS docs.
*   **Honesty**: "If it's slow, say it's slow." We benchmark against `aws s3 cp` and `duckdb` running *in the same environment*. Native-vs-Lambda comparisons are kept separate.
*   **Control**: "If we can't fix it, we don't use it." We own the vertical slice from the syscall to the bit written.
*   **Inspired**: "Regularly see what other speed demons like duckdb and polars do" (both are checked out in `references/`).
