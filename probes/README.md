# Probes

Probes are **point-in-time learning experiments** - one-off code written to understand Zig, libraries, or debug specific issues. They are historical artifacts, not maintained tests.

## Quick Start (Isolated Build)

Probes have their own build system that doesn't rebuild the entire zpq project:

```bash
cd probes

# List available probes
zig build --help

# Build a specific probe
zig build probe_conn_reuse -Doptimize=ReleaseFast

# Build and run
zig build run-probe_conn_reuse -Doptimize=ReleaseFast

# With environment variables (for S3 probes)
S3_HOST=s3.us-west-2.amazonaws.com \
S3_BUCKET=my-bucket \
S3_KEY=path/to/file.parquet \
AWS_ACCESS_KEY_ID=... \
AWS_SECRET_ACCESS_KEY=... \
zig build run-probe_conn_reuse -Doptimize=ReleaseFast
```

This builds only the probe and its dependencies, not the entire zpq CLI.

## Philosophy

- Probes capture "how did we figure this out?" moments
- They may not compile with current Zig versions (that's okay)
- They are NOT run in CI
- When a probe's insight becomes production code, the probe stays as documentation

## Adding a New Probe

1. Name it `probe_<descriptive_name>.zig`
2. Add a doc comment at the top explaining what you're investigating
3. Register it in `probes/build.zig`:
   ```zig
   addProbe(b, target, optimize, zpq_mod, xev_mod, "probe_your_name");
   ```
4. Update this README with the probe's purpose and date

## Current Probes

| Probe | Purpose | Created |
|-------|---------|---------|
| `probe_async_dns` | Understanding xev ThreadPool + async DNS resolution pattern | Dec 2024 |
| `probe_batch_reuse` | Testing batch connection reuse patterns | Dec 2024 |
| `probe_conn_cost` | Measure TLS handshake vs transfer time tradeoffs | Dec 2024 |
| `probe_conn_reuse` | Verify connection pool reuse within XevS3Source lifetime | Dec 2024 |
| `probe_dns_xev` | xev-based DNS resolution testing | Dec 2024 |
| `probe_fast_feedback` | Quick zpq DNS integration sanity check | Dec 2024 |
| `probe_multi_conn` | Debugging S3 connection concurrency - isolating hang in connection vs dispatch | Dec 2024 |
| `probe_parallel_upload` | Testing parallel PUT uploads for S3 multipart | Dec 2024 |
| `probe_parallel_write` | Understanding parallel TLS writes with xev | Dec 2024 |
| `probe_pool_cleanup` | Test XevConnectionPool cleanup sequence | Dec 2024 |
| `probe_s3_writer` | S3Writer integration with local rustfs | Dec 2024 |
| `probe_sf_crash` | Investigating SingleFlightResolver crash scenarios | Dec 2024 |
| `probe_tls_pump` | Low-level boring_tls integration without zpq abstractions | Dec 2024 |
| `probe_traced_upload` | Heavily instrumented TLS upload tracing | Dec 2024 |
| `probe_write_roundtrip` | Write → read roundtrip verification | Dec 2024 |
| `probe_xev_s3_head` | XevS3Source HEAD request verification | Dec 2024 |
| `probe_xev_tcp_lifecycle` | Learning xev TCP socket lifecycle and completion handling | Dec 2024 |
| `bench_skip_breakdown` | Time breakdown for filtered scans (outputs JSON trace) | Dec 2024 |
| `shootout_tls_throughput` | Micro-shootout comparing TLS decryption strategies | Dec 2024 |
| `verify_minish_context` | Verifying minish fuzzer context/allocator patterns | Dec 2024 |

## RIE (Runtime Interface Emulator)

The `rie/` subdirectory contains a minimal Lambda Runtime Interface Emulator for local testing:

```bash
cd probes/rie
zig build
./zig-out/bin/rie
```

## Python Probes

Rapid prototyping probes for testing theories before implementing in Zig:

| Probe | Purpose | Created |
|-------|---------|---------|
| `python/test_slot_padding.py` | Proves padding between row groups is tolerated by Parquet readers | Dec 2024 |
| `python/test_footer_reconstruction.py` | Tests footer offset requirements and manipulation | Dec 2024 |
| `python/test_full_slot_assembly.py` | Full proof of slot-based parallel write approach | Dec 2024 |
| `python/test_slot_writer_e2e.py` | End-to-end SlotWriter verification with pyarrow | Dec 2024 |

Key findings from Python probes:
- **Padding tolerance CONFIRMED**: PyArrow reads files with zeros between row groups
- **Footer offsets MUST be accurate**: Can't guess, but CAN pre-compute with slots
- **pwrite() parallelism works**: File integrity maintained with concurrent slot writes
- **S3 ETags are predictable**: `MD5(MD5(part1)||MD5(part2)||...)-N` formula confirmed

## eBPF Probes

For kernel-level tracing (requires root):

| Probe | Purpose |
|-------|---------|
| `remote_latency_shootout.bt` | Diagnose kernel-to-user scheduling lag on ARM64 |
| `trace_io_uring.bt` | io_uring submission/completion tracing |
| `trace_recv_gaps.bt` | TCP receive gap analysis |
| `trace_tcp_latency.bt` | TCP syscall latency measurement |

Run with: `sudo bpftrace probes/trace_tcp_latency.bt`
