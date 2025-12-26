# Probes

Probes are **point-in-time learning experiments** - one-off code written to understand Zig, libraries, or debug specific issues. They are historical artifacts, not maintained tests.

## Philosophy

- Probes capture "how did we figure this out?" moments
- They may not compile with current Zig versions (that's okay)
- They are NOT run in CI
- When a probe's insight becomes production code, the probe stays as documentation

## Current Probes

| Probe | Purpose | Created |
|-------|---------|---------|
| `probe_async_dns.zig` | Understanding xev ThreadPool + async DNS resolution pattern | Dec 2024 |
| `probe_fast_feedback.zig` | Quick zpq DNS integration sanity check | Dec 2024 |
| `probe_multi_conn.zig` | Debugging S3 connection concurrency - isolating hang in connection vs dispatch | Dec 2024 |
| `probe_s3_head_hang.zig` | Diagnosing async S3 HEAD request hangs on large files | Dec 2024 |
| `probe_sf_crash.zig` | Investigating SingleFlightResolver crash scenarios | Dec 2024 |
| `probe_tls_echo.zig` | Understanding zpq Connection + TLS handshake pattern | Dec 2024 |
| `probe_tls_pump.zig` | Low-level boring_tls integration without zpq abstractions | Dec 2024 |
| `probe_xev_tcp_lifecycle.zig` | Learning xev TCP socket lifecycle and completion handling | Dec 2024 |
| `verify_minish_context.zig` | Verifying minish fuzzer context/allocator patterns | Dec 2024 |
| `probe_encoding.zig` | Verifying S3 path percent-encoding with special characters | Dec 2024 |
| `probe_dns_xev.zig` | Standalone verification of the xev-based ThreadPool DNS resolver | Dec 2025 |
| `probe_timespec_fields.zig` | Investigating `posix.timespec` field naming (Darwin vs Linux) | Dec 2025 |
| `probe_arraylist_unmanaged.zig` | Documenting the move to `ArrayListUnmanaged` in Zig 0.16.x | Dec 2025 |
| `probe_rustfs_raw.zig` | Proving raw TCP connectivity to RustFS without TLS | Dec 2025 |
| `shootout_tls_throughput.zig` | Micro-shootout comparing TLS decryption strategies (Baseline vs Direct vs Monster) | Dec 2025 |
| `remote_latency_shootout.bt` | eBPF script to diagnose 100Mbps bottleneck by tracking kernel-to-user scheduling lag and app processing gaps on Linux ARM64 | Dec 2025 |

## Running a Probe

Probes with build steps can be run via:
```bash
zig build probe-<name>   # if defined in build.zig
# or
zig run probes/probe_foo.zig  # standalone
```

## Adding a New Probe

1. Name it `probe_<descriptive_name>.zig`
2. Add a doc comment at the top explaining what you're investigating
3. Update this README with the probe's purpose and date
4. Optionally add a build step in `build_tests.zig` if you need zpq imports
