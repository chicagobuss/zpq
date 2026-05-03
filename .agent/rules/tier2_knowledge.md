---
trigger: always_on
---

# Tier 2: The Grimoire of Knowledge

**Hard-earned lessons. Read before assuming.**

## Zig 0.16.0 (release baseline)
We're on the released Zig 0.16.0, not master. The release reorganized large parts of `std`. Custom code now justifies itself on perf or binary-size grounds, not on routing-around-instability grounds.

**API churn cheat sheet** (things that broke in the 0.15→0.16 jump):
*   **`std.posix`** lost most networking and many file ops: `socket`, `connect`, `write`, `close`, `accept`, `bind`, `listen`, `readlink`, `clock_gettime` are all gone. The constants (`AF`, `SOCK`, `IPPROTO`, `SOL`, `SO`, `MAP`, `PROT`, `MADV`) are still there. `read` and `getrlimit` survived.
*   **`std.fs`** is gutted. `std.fs.cwd()`, `openFileAbsolute`, `createFile`, `deleteFile` are gone. Networking and file I/O moved to `std.Io.Net` and `std.Io.Dir` which require an `Io` vtable parameter.
*   **`std.net`** is gone — replaced by `std.Io.Net`.
*   **`std.time`** lost `timestamp()`, `nanoTimestamp()`, `milliTimestamp()`, `microTimestamp()`. The `ns_per_*` constants survived. Use `std.os.linux.clock_gettime` directly for raw access.
*   **`std.Thread.sleep`** is gone — moved to `std.Io.sleep` (needs `io`). Use `std.os.linux.nanosleep` for raw.
*   **`std.heap.GeneralPurposeAllocator`** renamed to `std.heap.DebugAllocator`.
*   **`std.ArrayListUnmanaged(T){}`** is now `std.ArrayList(T).empty` (the unified `ArrayList` is unmanaged by default; the previous "managed" form is gone). Init pattern: `var list: std.ArrayList(T) = .empty;`.
*   **`std.process.Child{...}.spawnAndWait()`** is gone — use top-level `std.process.run(gpa, io, options)` or `std.process.spawn(io, options)`.
*   **`pub fn main()`** can now take `init: std.process.Init.Minimal` to receive `args` and `environ` cleanly. Use `init.environ.getPosix("KEY")` to read env vars.
*   **`std.posix.PROT.READ | PROT.WRITE`** is now a packed struct: `linux.PROT{ .READ = true, .WRITE = true }`.

For a worked-out example of these, read `probes/probe_lambda_caps/main.zig` — it uses raw `std.os.linux` syscalls throughout because the high-level `Io` interface needs a vtable we don't want in a standalone probe.

**Allocator discipline**: pass `std.mem.Allocator` explicitly. Use `ArenaAllocator` for per-request or per-row-group lifecycles to avoid fragmentation. Trust caller-provided allocators; don't validate.

**Comptime**: use it to generate specialized decoders (e.g., bit-unpacking). Avoid runtime `if` switches in hot loops.

## The Async Stack (libxev + BoringSSL)

### Backend selection per target
Lambda's seccomp filter blocks `io_uring_setup` (returns ENOSYS), so we have **three backends with one abstraction**:

| Target           | Backend  | Notes                                              |
| ---------------- | -------- | -------------------------------------------------- |
| Linux CLI        | io_uring | preferred; needed for SQE batching, fixed buffers |
| Linux Lambda     | epoll    | mandatory, kernel-policy enforced                  |
| macOS CLI        | kqueue   | the dev environment                                |

Hot-path designs reason in **epoll-readiness terms**, not io_uring-completion terms — that's the lowest common denominator. io_uring-only optimizations (`IORING_REGISTER_BUFFERS`, `IOSQE_IO_LINK`, multishot recv) live in `src/io/iouring.zig` and are excluded from the Lambda binary at compile time via `build_options.lambda`.

The previous Tier-2 framing — "We control the event loop explicitly (Submission/Completion Queue)" — is misleading because CQE/SQE semantics are an io_uring-only concept. Don't carry that mental model into Lambda code.

### Lambda capability ground truth
From `docs/lambda_capabilities.md` (probed empirically, 2026-05-03 in `provided.al2023`):
*   **Kernel is AL2 5.10**, not AL2023 6.x — even though the runtime tag says al2023. Several syscalls (`epoll_pwait2` in particular, added in 5.11) are missing for kernel-age reasons, separate from seccomp.
*   **`io_uring_setup` returns ENOSYS** — AWS hides the syscall via seccomp; both arm64 and x86_64.
*   **Allowed syscalls**: `eventfd2`, `timerfd_create`, `signalfd4`, `epoll_create1`, `mlock`, `userfaultfd` (yes, surprisingly).
*   **Allowed setsockopt**: `SO_REUSEPORT`, `SO_REUSEADDR`, `TCP_NODELAY`, `TCP_FASTOPEN`, `SO_INCOMING_CPU`, `SO_ZEROCOPY`. The last two are useful — `MSG_ZEROCOPY` sends are usable for S3 multipart upload payloads.
*   **Capabilities**: zero (`CapEff = 0x0`). `NoNewPrivs = 1`. Seccomp mode 2 (filter).
*   **CPUs**: `sched_getaffinity` reports 2 CPUs at every memory tier from 256 MB onwards, scaling to 6 at 10240 MB. Lambda meters CPU *time*, not affinity — multi-threaded code runs at any size.
*   **`/tmp` throughput** peaks ~587 MB/s at 3008 MB and declines slightly past that. Local spilling viable for sort/agg, but the dominant strategy is "stream from S3 to S3 without touching `/tmp`."
*   **Cold start init**: 4.13 ms for the 165 KB probe binary. Static binaries pay off here.

Re-run `just probe-lambda` any time AWS announces runtime changes — seccomp policy is not API contract.

### BoringSSL traps
*   **Implicit State Trap**: BoringSSL requires explicit state initialization (`SSL_set_connect_state`). Do not rely on `SSL_read/write` to trigger handshakes.
*   **Memory BIOs**: Use `BIO_s_mem` to decouple crypto from I/O.
    *   *Read Path*: Socket → Ring Buffer → `BIO_write` → `SSL_read` → Application.
    *   *Write Path*: Application → `SSL_write` → `BIO_read` → Ring Buffer → Socket.
*   **Partial Writes**: `xev.write` may write fewer bytes than requested. **ALWAYS** loop until the entire buffer is drained.
*   **Backpressure**: Implement a bounded queue for outgoing writes. Don't blindly `write()` faster than the network can transmit.

### Vendor reality
*   `vendor/boring_tls` ships **prebuilt-only** — `tools/r2-fetch-artifacts.sh` populates `vendor/boring_tls/prebuilt/<triple>/`. The vendor `build.zig` no longer carries source-build paths.
*   `vendor/libxev` is a fork. As of 0.16.0 release the io_uring backend uses removed `posix.clock_gettime` — when wiring the CLI hot path back in, either patch the fork or bump to a newer libxev commit. The Lambda binary doesn't need this since it never imports the io_uring backend.

## Workflow & Benchmarking
*   **Fair comparisons**: a benchmark must be apples-to-apples. If ZPQ reads from local + writes to S3, the comparator must do the exact same task. Native-vs-Lambda numbers are reported separately, never blended.
*   **Always source `.env`** before running benchmarks (AWS creds + bench file paths).
*   **`just` is the source of truth** — only ship recipes that work today.
*   **Reproducibility**: Zig version is pinned in `.zig-version` and enforced by `build.zig.zon` `minimum_zig_version`. Standard test file: `data/benchmark/benchmark_100mb.parquet` (also in S3 at `s3://${AWS_S3_BUCKET}/zpq_test_data/benchmark/`).

## DNS & Networking
The 3-tier resolver stack we want to maintain (when we re-introduce it):
1.  **Fast path**: in-memory LRU cache.
2.  **Deduplication**: SingleFlight — coalesce concurrent requests for the same host.
3.  **Transport**: `xev`-based async resolution (never block the loop).

In Lambda, `/etc/resolv.conf` points at AWS's link-local resolver (`169.254.x.x`). Cold-resolve latency to a regional S3 endpoint is single-digit milliseconds — an LRU cache earns its keep more from request volume than from absolute miss cost.

## Observability & Logging
*   No fancy debug-mode compilation. ReleaseFast everywhere; gate verbosity at runtime with a CLI arg / env var driving log level.
*   For deeper observability use eBPF on Linux (`bpftrace`, `perf`) or Instruments on macOS.
*   In Lambda, CloudWatch is the only observability surface — anything written to stdout/stderr is captured. Don't reach for fancier tools there.
