# Lambda Capabilities: Empirical Probe Results

## Cold starts are the design point

zpq-lambda is meant to be started cold. On 2026-10-05 in us-west-2 at 3008 MB, zpq 0.4.0 (`provided.al2023` zips of
9.5 MB on arm64 and 10.0 MB on x86_64) had a median Lambda `Init Duration` of 18–22 ms on arm64 and 26–35 ms on x86_64
across eleven read, aggregate and S3-to-S3 write scenarios, three forced cold starts each. DuckDB 1.5.6 and Polars
1.44.2, run as Python 3.12 container images in the same region at the same memory size, had medians of 448–832 ms and
478–806 ms. Cold Init + Duration was lower for zpq than for DuckDB in every scenario. Tables, inputs and method:
[`measurements/lambda_cold_start_2026-10-05.md`](measurements/lambda_cold_start_2026-10-05.md).

With initialisation that small, the intended way to use zpq on Lambda is cold, bursty and wide:

- one invocation per file, partition or compaction unit, fanned out in parallel and fired when the work arrives;
- a fresh sandbox per invocation is fine: each invocation opens its own S3 connections and reads the footers it needs,
  and nothing carried over from a previous invocation is required for a correct answer;
- concurrency, not reuse, is the scaling knob.

Keep-warm pings, provisioned concurrency bought to hide initialisation, and designs that count on a sandbox's
connections or caches surviving between invocations are not the intended use. They pay for idle sandboxes to avoid a
cost of tens of milliseconds, and a reused sandbox brings its own behaviour (below). Lambda will still reuse sandboxes
on its own; zpq copes with that, it just does not depend on it.

## Probe results

**Run date**: 2026-06-13
**Probe binary**: `probes/probe_lambda_caps/main.zig` (165 KB static musl)
**Runtime**: `provided.al2023`
**Region**: `us-west-2`
**Account**: sandbox staging (id redacted)
**Schema**: v1 — see top of `probes/probe_lambda_caps/main.zig`

## TL;DR

1. **`io_uring` is unavailable in Lambda.** `io_uring_setup` returns `ENOSYS`, both arm64 and x86_64, both cold and
   warm. AWS makes it look like the syscall doesn't exist. Build the Lambda binary against epoll only.
2. **The kernel is Amazon Linux 2 (5.10), not AL2023 (6.1+)** — even when the runtime tag is `provided.al2023`. Several
   syscalls we *would* expect on a 5.x kernel are absent because they were added in 5.11+ (`epoll_pwait2`).
3. **Seccomp is in filter mode** with `NoNewPrivs=1` and zero effective capabilities — the standard hardening posture.
4. **Networking is permissive.** TCP egress works (~5 ms to 1.1.1.1:443), and every `setsockopt` we tested is allowed
   including `TCP_FASTOPEN`, `SO_INCOMING_CPU`, and `SO_ZEROCOPY`.
5. **CPU affinity reports 2+ cores at every memory tier**, scaling to 6 at 10240 MB. Lambda's "1 vCPU per 1769 MB" rule
   is about CPU *time*, not the affinity mask.
6. **`/tmp` writes peak around 587 MB/s at 3008 MB and plateau** — adequate for spilling, not for hot-path I/O.
7. **Cold start init is 4.13 ms** for the 165 KB probe — confirms Zig's binary-size advantage on Lambda is real.

## Methodology

Single Zig file does both modes. CLI mode prints JSON to stdout; Lambda mode polls `/runtime/invocation/next` and
returns the same JSON as the response body. Re-runs probes per invocation so cold and warm both exercise the actual
collection code, not just a cached result. All probes capture errno on failure; no probe aborts the run.

Three deployments compared:
- **Workstation** (Ubuntu 24.04 / Linux 6.8.0-107) — control values, no seccomp.
- **Lambda arm64** (`zpq-probe-caps-arm64`, `provided.al2023`).
- **Lambda x86_64** (`zpq-probe-caps-x86_64`, `provided.al2023`).

Lambda was invoked at 1024 MB cold + warm, then memory-swept for CPU/throughput scaling. A docker AL2023 baseline was
considered but skipped — the host kernel determines syscall availability and Lambda is running 5.10 (an older AL2-class
kernel), so AL2023 docker on a 6.x host wouldn't be apples-to-apples.

The raw JSON outputs of that run were written to `output/probe/`, which is gitignored and not committed; re-run the
probe (below) to regenerate them.

## Syscall reachability

| Syscall | Workstation (6.8, no seccomp) | Lambda (5.10, seccomp) | Cause |
|---|---|---|---|
| `io_uring_setup` | OK | **ENOSYS** | **Seccomp** (5.10 supports it; AWS hides it) |
| `userfaultfd` | EPERM (no CAP_SYS_PTRACE) | **OK** | Surprise — Lambda allows it |
| `eventfd2` | OK | OK | — |
| `timerfd_create` | OK | OK | — |
| `signalfd4` | OK | OK | — |
| `pidfd_open` | OK | **ENOSYS** | **Seccomp** (5.10 has it since 5.3) |
| `epoll_create1` | OK | OK | — |
| `epoll_pwait2` | EINVAL (reachable) | **ENOSYS** | Kernel age (5.11+) — not seccomp |
| `clone3` | EINVAL (reachable) | **ENOSYS** | **Seccomp** (5.10 has it since 5.3) |
| `perf_event_open` | OK | EPERM | Seccomp (or capability) |
| `bpf` | EINVAL (reachable) | **EPERM** | Seccomp (or capability) |
| `ptrace_traceme` | OK | EPERM | Seccomp (or capability) |
| `mlock_4k` | OK | **OK** | We can pin pages! |

**Interpreting the errno:**
- `ENOSYS` = "no such system call." Either the syscall is genuinely missing from this kernel, or seccomp is rewriting
  the failure to *look* like it's missing. The latter is sneaky but common — it forces well-behaved code to fall through
  to alternatives instead of branching on EPERM.
- `EPERM` = "operation not permitted." Seccomp returned the standard denial.
- `EINVAL` = "invalid argument." Means the syscall reached kernel logic and rejected the args we sent — it's allowed.

The `io_uring_setup → ENOSYS` result is the load-bearing finding. Linux 5.10 has io_uring (it was added in 5.1). AWS is
clearly applying a seccomp filter that returns ENOSYS for the io_uring family.

## Networking

| Probe | Result |
|---|---|
| TCP connect to 1.1.1.1:443 | OK, 5 ms |
| `SO_REUSEPORT` | OK |
| `SO_REUSEADDR` | OK |
| `TCP_NODELAY` | OK |
| `TCP_FASTOPEN` | OK |
| `SO_INCOMING_CPU` | OK |
| `SO_ZEROCOPY` | OK |
| IPv6 socket creation | OK |
| `/etc/resolv.conf` | populated (169.254.x.x AWS resolvers) |

`SO_ZEROCOPY` being allowed is a small surprise — that means `MSG_ZEROCOPY` sends are usable on Lambda for large
outbound writes (e.g. S3 multipart upload payloads).

`TCP_FASTOPEN` allowed at the setsockopt level doesn't mean the *kernel* will actually do TFO — that depends on
`/proc/sys/net/ipv4/tcp_fastopen` which we didn't probe. But the option sets without error.

### If a sandbox is reused

Not the intended pattern (see above), but Lambda reuses sandboxes on its own, so these are the facts zpq handles.
Measured 2026-10-03, us-west-2, 3008 MB, x86_64 and arm64.

- **Pooled S3 connections do not survive a freeze.** After about 5 s between invocations, every pooled keep-alive
  connection failed on its next request. `src/io/pool.zig` therefore closes connections idle for more than 4 s instead
  of reusing them, and a request that still finds a reused connection dead before any response byte is retried at once
  on a new one (`retry.Failure.stale`). Idle age is measured on CLOCK_BOOTTIME: the pool's `max_idle_ms` matched the
  caller-side gap (13.5 s median for 13.0 s gaps), so it keeps counting while the sandbox is frozen.
- **Read throughput depends on how long the sandbox rested.** A 174.8 MB full scan read in about 1.95 s back-to-back
  (~90 MB/s), 0.93 s after 1 s idle, and 0.34–0.44 s after 2–3 s idle (~450 MB/s), on both architectures, on reused and
  fresh connections alike, with decode time unchanged. Polars in the same configuration slows the same way (2.25 s
  back-to-back, 0.53–0.61 s after 2–6 s idle), so this is a per-sandbox network allowance, not engine state.
  Back-to-back full scans measure that allowance, not the engine.

## Memory

| Probe | Result |
|---|---|
| `mmap_anon_4k` | OK |
| `mmap_anon_2m_hugetlb` | **ENOMEM** (no huge pages reserved for us) |
| `mmap_populate` | OK |
| `madvise(MADV_HUGEPAGE)` | OK |
| `madvise(MADV_DONTNEED)` | OK |

`mlock` works (per syscall table above). Combined with `madvise`, that gives us enough for buffer-pool tuning. Explicit
hugetlb is unavailable but transparent huge pages are not blocked.

## CPU and `/tmp` scaling (arm64)

| Memory (MB) | `sched_getaffinity` count | `/tmp` write (MB/s) |
|---:|---:|---:|
| 256 | 2 | 118 |
| 1024 | 2 | 285 |
| 1769 | 2 | 435 |
| 3008 | 2 | 587 |
| 5120 | 3 | 581 |
| 10240 | 6 | 507 |

Two observations:
- The affinity mask reports 2 CPUs even at the smallest memory tier. This means **multi-threaded code can run on Lambda
  at any tier** — it just won't be faster than single-threaded below 1769 MB, because Lambda meters CPU *time*
  proportional to memory.
- `/tmp` throughput peaks around 3008 MB and *declines* slightly after — likely the I/O bandwidth ceiling kicks in, and
  there's no benefit to bigger tiers for /tmp-heavy workloads.

## Cold-start cost

CloudWatch `REPORT` line for an ARM64 cold invoke at 1024 MB:

```
Duration: 214.14 ms  Billed Duration: 219 ms  Memory Size: 1024 MB
Max Memory Used: 78 MB  Init Duration: 4.13 ms
```

`Init Duration: 4.13 ms` is the kernel-to-`main` boot cost. The 165 KB static binary is the reason — there's almost
nothing to load. The full `zpq-lambda` binary, at about 10 MB zipped, measured 18–35 ms (median per scenario) on
2026-10-05; see [Cold starts are the design point](#cold-starts-are-the-design-point).

## Implications for ZPQ v2 architecture

These findings should drive the current architecture, not be appended to it:

1. **Compile-time backend selection is mandatory.** The Lambda build must not link the io_uring code path at all.
   Smaller binary, faster cold start, no risk of accidental ENOSYS panic if a runtime auto-detect ever guesses wrong.
   When the io_uring backend lands (it doesn't exist yet), a per-binary build option, as `enable_sql` is, must exclude
   it from the Lambda binary's import graph.
2. **Treat io_uring as a workstation-only optimization.** Hot-path performance reasoning should stay grounded in
   epoll-readiness terms because that's the lowest common denominator across our targets.
3. **Own the event loop.** We don't depend on libxev (or any other event-loop library). Lambda needs an epoll wrapper
   regardless — at ~150 LoC, that's the entire baseline. io_uring (CLI hot path) and kqueue (macOS dev) live in separate
   files and ship on demand. Reasoning: Tier-1 "if we can't fix it, we don't use it" + binary-size discipline + the
   S3-streaming workload is narrow enough to want a purpose-built loop.
4. **Use raw socket helpers for the Lambda runtime API loop.** Low-frequency, plain HTTP/1.1, stays in epoll-land —
   `probes/probe_lambda_caps/main.zig` already has the pattern (~30 LoC of socket code). Don't reach for `std.http` or
   `std.Io.Net` here; the runtime API is a poor fit for vtable-based abstractions.
5. **Consider `MSG_ZEROCOPY` for S3 upload payloads.** `SO_ZEROCOPY` allowed means we can zero-copy *outbound* sends,
   which is exactly the S3 multipart upload story. This is independent of io_uring availability.
6. **Threading is fine at every memory tier, sized to the tier.** The affinity mask shows 2 CPUs at every tier up to
   3008 MB, so it does not say how much CPU time a sandbox gets. `src/core/system.zig` caps parallelism at the memory
   tier's vCPUs, rounded up (2 at 3008 MB), or at a cgroup CPU quota when one is visible.
7. **`/tmp` is unsuitable for hot-path I/O.** 587 MB/s peak is half what we get on bare metal, and at 1024 MB Lambda
   configs (the common case for small jobs) you get ~285 MB/s. Local spilling is viable for sort/agg, but the dominant
   strategy remains "stream from S3 to S3 without touching /tmp."
8. **Two benchmark tracks, formally.** Stop reporting one S3 throughput number. Native and Lambda environments differ
   (and will diverge further when the CLI gains io_uring); the right comparison is `aws s3 cp` *running on the same
   Lambda*, not on a workstation.

## Reproducing

```bash
# Build
zig build-exe probes/probe_lambda_caps/main.zig -O ReleaseSmall \
  -target aarch64-linux-musl \
  -femit-bin=zig-out/probe_lambda_caps_arm64

# Local CLI run (builds the native probe first)
just probe-local

# Lambda deploy + invoke (functions exist in sandbox account)
source .env
aws lambda invoke \
  --function-name zpq-probe-caps-arm64 \
  --cli-binary-format raw-in-base64-out \
  --payload '{}' \
  --region us-west-2 \
  out.json
jq < out.json
```

The probe is idempotent and free to run repeatedly. Re-run if AWS announces runtime changes — the seccomp filter is
policy, not API contract, so it can shift without warning.
