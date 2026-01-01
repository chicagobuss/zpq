# Plan 04: DNS Stack Justification & Verification

## Criticism
**"DNS Stack is Over-Engineered Without Proven Benefit."**
Three tiers of resolvers for a simple CLI tool seems excessive compared to `getaddrinfo`.

## Response
**Partially Valid / Defended.**
- **Defense**: We are not just a CLI tool; we are a "Lambda-First" library. In Lambda, fetching 50 column chunks in parallel from S3 (which often resolves to different IPs) benefits from "Happy Eyeballs" (IPv4/v6 racing) and non-blocking resolution. A single blocking `getaddrinfo` call pauses the *entire* event loop, stalling all active transfers.
- **Concession**: We have not proven this benefit with numbers.

## Action Plan

### 1. Benchmark: Blocking vs. Async
Create `bench_dns.zig`:
- **Scenario**: Resolve 50 unique subdomains (simulate sharded S3 buckets).
- **Compare**:
    - Serial `std.c.getaddrinfo`.
    - `ThreadPoolResolver` (Async).
- **Metric**: Total time to resolution + impact on a parallel "dummy workload" (simulating active downloads).

### 2. Document "Why"
If the benchmark shows >10ms improvement or prevents head-of-line blocking, document this in `docs/architecture.md`. "We prevent Head-of-Line blocking on the single-threaded event loop."

### 3. Simplify if Failed
If the benchmark shows negligible gain (<1ms), we will **deprecate** `SpeculativeResolver` and `SingleFlightResolver`, keeping only the `ThreadPoolResolver` for correctness (non-blocking).

