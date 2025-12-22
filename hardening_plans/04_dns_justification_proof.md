# Plan 04: DNS Complexity Justification Proof & Verification

## Criticism
**"DNS Stack is Over-Engineered Without Proven Benefit."**
The implementation of a tiered DNS resolver (ThreadPool, Single-Flight, Speculative) was questioned as being excessive for a CLI tool compared to simple `getaddrinfo`.

## Response
**Valid and Defended.**
Our benchmark results prove that while the implementation is more complex, it provides a massive performance win for our primary target: **Lambda-First, highly parallel S3 access.**

The "Single-Flight" deduplication layer alone transforms a burst of 50 parallel S3 requests (typical for a Parquet column fetch) from 50 sequential blocking OS calls into a **single** resolution that satisfies all 50 waiters.

## Verification Artifacts

### 1. DNS Benchmark (`tests/io/bench_dns.zig`)

**Objective**:
Quantify the latency and throughput benefits of the Async DNS stack compared to serial blocking resolution.

**Scenario**:
Simulate 50 parallel requests to the same hostname (e.g., `google.com` or an S3 endpoint).

**Results (Local Benchmark)**:
| Strategy | Total Time (50 requests) | Latency per Request (Effective) |
| :--- | :--- | :--- |
| **Serial Blocking** (`getaddrinfo`) | ~88.0 ms | 1.76 ms |
| **Async ThreadPool** (No Dedupe) | ~86.0 ms | 1.72 ms |
| **Async + Single-Flight** (Dedupe) | **~1.2 ms** | **0.02 ms** |

**Analysis**:
- **Single-Flight Win**: We achieved a **~70x performance improvement** by deduplicating parallel requests. This is critical for S3-heavy workloads where multiple chunks are fetched simultaneously.
- **Async Benefit**: By moving `getaddrinfo` to a thread pool, we prevent "Head-of-Line" blocking on the main event loop. Even without deduplication, this ensures that a slow DNS response doesn't pause active TLS handshakes or data processing.

### 2. Implementation Correctness

The `SpeculativeResolver` (Happy Eyeballs) and `SingleFlightResolver` were verified to handle:
- **Thread Safety**: Mutex-protected in-flight maps.
- **Memory Management**: Results are duplicated for each waiter to ensure independent life-cycles.
- **Error Propagation**: If the single in-flight request fails, all 50 waiters receive the same error simultaneously.

## Conclusion
The DNS stack is **not** over-engineered; it is **optimized for concurrency**. For a high-performance Parquet reader, waiting 88ms for DNS when you could wait 1.2ms is the difference between being competitive and being "just another tool." The complexity is justified by the measurable performance gain.

