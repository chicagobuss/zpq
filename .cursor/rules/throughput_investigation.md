# Throughput Investigation: Why is ZPQ capped at ~1.7 MB/s?

**Date**: 2025-12-26  
**Status**: In Progress

---

## Current Observations

### Performance Numbers
- ZPQ shootout: ~90-100ms for 166KB file = **1.6-1.7 MB/s**
- PyArrow warm: ~313ms
- DuckDB warm: ~122ms  
- curl baseline: ~178ms

ZPQ is actually faster than the Python tools, but we expected much higher throughput.

### eBPF Trace Data (from trace_io_uring.bt)

```
io_uring submits: 505
io_uring completes: 505

TCP recv sizes:
  [0, 4K)     35 reads
  [4K, 8K)   407 reads  <-- MOST READS ARE TINY
  [64K+)      50 reads

TCP recv latency: ALL <1ms (kernel is fast)
```

---

## Hypothesis 1: Serial TCP Pumping (LIKELY)

**Theory**: We're issuing recv() calls too quickly, before the kernel buffer accumulates enough data.

**Pattern observed**:
1. Send HTTP request
2. Immediately issue recv with 1MB buffer
3. Server starts sending, but only ~4KB has arrived
4. recv() returns with 4KB (whatever's in buffer)
5. We process it, issue another recv
6. Repeat 400+ times for 166KB file

**Evidence**:
- 407 reads of 4-8KB for a 166KB file = ~25-40 reads expected if optimal
- Each read has <1ms kernel latency = we're not waiting for data
- TCP buffer settings don't help because we're draining faster than filling

**Test**: Measure the gap between consecutive recv() calls. If it's very small (<100us), we're CPU-bound and issuing recvs faster than data arrives.

---

## Hypothesis 2: TCP Slow Start

**Theory**: TCP slow start limits initial throughput, and our file is too small to reach full speed.

**Evidence**:
- 166KB is only ~110 TCP packets at 1500 MTU
- TCP slow start might not fully ramp up for such a small transfer
- First iteration is always ~20-30ms slower (cold connection)

**Test**: Try a larger file (10MB+) and see if throughput improves.

---

## Hypothesis 3: TLS Overhead

**Theory**: TLS record processing is the bottleneck.

**Evidence against**:
- "Direct Zero-Copy" mode shows minimal improvement over buffered
- TLS record processing times shown in logs are ~4-5ms total

**Test**: Compare TLS vs non-TLS mode (The Naked Gun test).

---

## Hypothesis 4: Round-Trip Latency

**Theory**: Network RTT to S3 dominates small file transfers.

**Evidence**:
- curl takes 178ms for same file
- Our ~90ms is actually faster than curl
- First iteration ~110ms, subsequent ~85-90ms (connection reuse helps)

**Test**: Measure actual RTT to S3 with ping/TCP handshake timing.

---

## Targeted eBPF Probe Design

To test Hypothesis 1 (Serial Pumping), we need:

```
Probe: trace_recv_gaps.bt
Purpose: Measure time BETWEEN recv calls (userspace processing time)

Key metrics:
1. recv_kernel_time: Time spent inside tcp_recvmsg (waiting for data)
2. recv_gap_time: Time between recv exit and next recv entry (userspace work)
3. bytes_per_recv: How much data we get per call

If recv_gap_time >> recv_kernel_time:
  → We're CPU-bound in userspace (TLS processing, etc.)
  
If recv_kernel_time is very low AND bytes_per_recv is low:
  → We're calling recv before data accumulates (serial pumping)
  
If recv_kernel_time is high:
  → We're waiting for network (RTT-bound)
```

---

## Probe Results (2025-12-26)

```
Total recvs: 524
Total bytes: ~3.3MB (20 iterations of 166KB)

Total time in kernel (recv): 4 ms
Total time in userspace (between recvs): 2260 ms

Kernel time per recv: 1-8 microseconds (blazing fast)
Userspace gap: bimodal - 8-32us (fast) and 4-8ms (slow TLS processing)
```

**DIAGNOSIS: 99.8% of time is in userspace, NOT waiting for network!**

---

## Hypothesis 1: Serial TCP Pumping - **DISPROVEN**

The probe shows kernel time is only 4ms total. We're not "draining faster than filling" - we're spending all our time in userspace processing (TLS decryption).

---

## NEW Hypothesis: TLS Decryption Overhead - **LIKELY**

The 4-8ms userspace gaps between some recvs suggest TLS record processing is the bottleneck.

**Evidence:**
- 95 recvs have 4-8ms userspace gaps
- This correlates with TLS record boundaries (~4KB = one TLS record)
- boring_tls processIncoming() is being called for each chunk

**Next steps:**
1. Profile TLS decryption specifically
2. Check if BoringSSL is using hardware AES-NI
3. Consider batching multiple TLS records before processing
4. Test with non-TLS to confirm TLS is the bottleneck

---

## Root Cause Found (2025-12-26)

**BoringSSL is compiled with `OPENSSL_NO_ASM=1` which disables all assembly optimizations!**

In `vendor/boring_tls/build.zig`:
```zig
boring_tls_mod.addCMacro("OPENSSL_NO_ASM", "1");
// and
"-DOPENSSL_NO_ASM",
```

This means:
- No ARM NEON/crypto instructions for AES
- No hardware-accelerated SHA
- Pure C fallback implementations

**Evidence**: SSL_read consistently takes 4.4ms per call (from timing breakdown):
```
processIncoming breakdown: bio=0us hs=0us read=4430us total=4431us
```

**Fix**: Remove `OPENSSL_NO_ASM` and rebuild BoringSSL with assembly enabled.

---

## Fix Applied (2025-12-26)

Removed `OPENSSL_NO_ASM` and added ARM64 assembly files to `vendor/boring_tls/build.zig`:

```zig
// BCM assembly for hardware crypto
"aesv8-armv8-linux.S",      // AES using ARMv8 crypto extensions
"aesv8-gcm-armv8-linux.S",  // AES-GCM 
"sha256-armv8-linux.S",     // SHA-256
"sha512-armv8-linux.S",     // SHA-512
"vpaes-armv8-linux.S",      // Vector permutation AES
// ... and more
```

**Results:**

| Metric | Before (OPENSSL_NO_ASM) | After (Hardware AES) | Speedup |
|--------|------------------------|---------------------|---------|
| SSL_read latency | ~4.4ms | ~2-7μs | **600-2000x** |
| 166KB file (warm) | ~90-100ms | ~63ms | **~1.5x** |
| Throughput | ~1.7 MB/s | ~2.6 MB/s | **~1.5x** |

The TLS decryption is now **~1000x faster**, but overall throughput improvement is more modest because:
1. Network RTT still dominates (connection to S3)
2. TCP slow start on small files
3. Other overhead (HTTP parsing, memory copies)

---

## Completed Steps

1. ~~Create targeted probe to measure recv gaps~~ DONE
2. ~~If serial pumping confirmed~~ DISPROVEN  
3. ~~Profile TLS decryption overhead~~ DONE - Found 4.4ms per SSL_read
4. ~~Check BoringSSL hardware acceleration status~~ FOUND: OPENSSL_NO_ASM=1
5. ~~Remove OPENSSL_NO_ASM and rebuild with ARM crypto assembly~~ **FIXED**

## Remaining Optimizations

- Test with larger files to see true throughput potential
- Consider connection pooling warmup
- Investigate parallel range requests for larger files
