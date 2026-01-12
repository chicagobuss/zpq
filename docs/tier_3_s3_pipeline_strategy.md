# Tier 3: S3→S3 Pipeline Strategy

> The comprehensive guide to how ZPQ should orchestrate an optimal S3-to-S3 Parquet transformation.

---

## Core Principle: The Conductor Model

There is **one Conductor** (the main event loop) that owns all I/O. Workers never call `loop.run()`. They consume data from queues and produce output to channels.

```
┌─────────────────────────────────────────────────────────────────┐
│                        CONDUCTOR (Main Thread)                  │
│  - Drives xev event loop                                        │
│  - Dispatches S3 reads/writes                                   │
│  - Never does CPU-heavy work                                    │
└─────────────────────────────────────────────────────────────────┘
        │                    │                    │
        ▼                    ▼                    ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   Worker 1   │    │   Worker 2   │    │   Worker N   │
│ - Decompress │    │ - Decompress │    │ - Decompress │
│ - Decode     │    │ - Decode     │    │ - Decode     │
│ - Filter     │    │ - Filter     │    │ - Filter     │
│ - Encode     │    │ - Encode     │    │ - Encode     │
└──────────────┘    └──────────────┘    └──────────────┘
        │                    │                    │
        ▼                    ▼                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                     OUTPUT CHANNEL (thread-safe)                │
│  - Workers push compressed row groups here                      │
│  - Sink consumes and uploads parts                              │
└─────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Bootstrap (Footer + Planning)

### Step 1: Discover File Size
```
HEAD s3://bucket/input.parquet
→ Content-Length: 104857600 (100MB)
```

### Step 2: Read Footer
```
GET s3://bucket/input.parquet
Range: bytes=-8              → Magic + footer length
Range: bytes=104850000-      → Actual footer blob
```

**What we learn:**
- Schema (column names, types, encodings)
- Row group metadata (count, row counts, byte offsets)
- Column chunk locations (file_offset, compressed_size, uncompressed_size)
- Statistics (min/max per column per row group)

### Step 3: Query Planning

Given the user's query, build an I/O plan:

| Query Type | I/O Strategy |
|------------|--------------|
| `SELECT *` | Fetch all columns, maximize bandwidth |
| `SELECT cols` | Fetch only requested columns |
| `WHERE col = val` | Fetch filter column first, then data columns |
| `WHERE col > val` (with stats) | Skip row groups where max < val |

**Output:** List of (row_group_id, column_id, byte_range) tuples, ordered for optimal fetch.

---

## Phase 2: Connection Pooling

### How Many Connections?

| Scenario | Recommended Connections |
|----------|------------------------|
| Lambda (1769MB, same AZ) | 8-12 |
| EC2 (same region) | 12-16 |
| Cross-region | 4-8 (latency dominates) |
| Small file (<10MB) | 2-4 |
| Large file (>1GB) | 16-24 |

**Heuristic:**
```
connections = clamp(num_row_groups * 2, 4, 16)
```

### Connection Lifecycle
1. **Pre-warm during footer read** - Open N connections while parsing
2. **Keep-alive reuse** - Reuse connections across range requests
3. **Graceful close** - After all reads complete

---

## Phase 3: Fetching Strategy

### Filter Columns First

For `SELECT a, b WHERE c = 5`:

```
Phase A: Fetch filter columns
  RG0: column c (bytes 50000-55000)
  RG1: column c (bytes 150000-155000)
  RG2: column c (bytes 250000-255000)

Phase B: Evaluate filters
  RG0: 1000 rows match
  RG1: 0 rows match → SKIP data columns!
  RG2: 500 rows match

Phase C: Fetch data columns (only for non-empty RGs)
  RG0: column a, column b
  RG2: column a, column b
```

### Range Coalescing

If column A ends at offset 50000 and column B starts at 50001, fetch bytes 0-100000 as one request instead of two. Threshold: **64KB gap**.

```
# Before coalescing: 3 requests
GET bytes=0-50000
GET bytes=50001-80000
GET bytes=80001-100000

# After coalescing: 1 request
GET bytes=0-100000
```

### Prefetch Strategy

While processing RG[i], prefetch RG[i+1] and RG[i+2].

```
T=0:   Start fetching RG0
T=5:   Start fetching RG1 (prefetch)
T=20:  RG0 complete → Worker starts processing
T=25:  Start fetching RG2 (prefetch)
T=40:  RG1 complete, RG0 processed → Worker gets RG1
...
```

---

## Phase 4: Processing Pipeline

### Worker Contract

Workers receive:
- **Input:** Compressed column chunks (bytes)
- **Task:** Decompress, decode, filter, re-encode

Workers produce:
- **Output:** Compressed row group (bytes + metadata)

Workers **never**:
- Call `loop.run()`
- Block on network I/O
- Access shared mutable state (except atomic counters)

### Memory Flow

```
S3 Read Buffer     → Per-RG Column Buffers  → ColumnBatch (decoded)
(managed by           (owned by worker)        (temporary, recycled)
 Conductor)                    ↓
                        SelectionVector (filter result)
                               ↓
                        Output MemorySink (compressed pages)
                               ↓
                        Output Channel → S3 Upload
```

---

## Phase 5: Output Strategy

### When to Use Multipart

| Output Size | Strategy |
|-------------|----------|
| < 5MB | Simple PUT |
| 5MB - 5GB | Multipart (parts = ceil(size / 8MB)) |
| > 5GB | Multipart with larger parts |

**Decision point:** We don't know final size until processing is done. So:

1. Buffer output in memory
2. If buffer exceeds 5MB threshold → Initiate multipart, upload Part 1
3. Continue buffering Part 2 while Part 1 uploads
4. If processing finishes with <5MB → Abort multipart (if started), simple PUT

### Part Upload Parallelism

While Parquet doesn't support true streaming (footer at end), we can still parallelize:

```
Worker finishes RG0 → Push to output channel
Worker finishes RG1 → Push to output channel
Sink sees 8MB → Upload Part 1
Worker finishes RG2 → Push to output channel
Sink sees 8MB → Upload Part 2 (parallel with Part 1)
...
All workers done → Finalize footer → Upload last part → CompleteMultipartUpload
```

---

## Phase 6: Timeline (Ideal Execution)

For a 100MB input, 95MB output, 4 row groups, Lambda 1769MB:

```
T=0ms:      HEAD request sent
T=5ms:      HEAD complete, GET footer sent
T=15ms:     Footer parsed, I/O plan built, open 8 connections
T=20ms:     Start fetching RG0 + RG1 (prefetch)
T=60ms:     RG0 arrived (25MB), Worker 1 starts
T=80ms:     RG1 arrived, Worker 2 starts
T=100ms:    RG0 processed (8MB output), buffer building
T=110ms:    Start fetching RG2 + RG3 (prefetch)
T=120ms:    RG1 processed, buffer = 16MB
T=130ms:    Buffer > 8MB → Initiate multipart, upload Part 1
T=150ms:    RG2 arrived, Worker 3 starts
T=160ms:    Part 1 uploaded, upload Part 2
T=170ms:    RG3 arrived, Worker 4 starts
T=180ms:    RG2+RG3 processed
T=200ms:    All parts uploaded, finalize footer
T=210ms:    CompleteMultipartUpload
T=220ms:    Done. Total: 220ms for 100MB → 95MB.
```

**Key metrics:**
- Network: ~135ms (bounded by S3 latency + bandwidth)
- CPU: ~80ms (decompress + filter + recompress)
- Overlap: ~50ms saved by prefetch + parallel upload

---

## Verification Checklist

Use this to validate the implementation is correct:

### Bootstrap
- [ ] Footer is read in ≤2 S3 requests
- [ ] Column offsets are extracted from metadata
- [ ] Statistics are available for predicate pruning

### I/O Scheduling
- [ ] Only requested columns are fetched (projection pushdown)
- [ ] Filter columns fetched before data columns
- [ ] Row groups are skipped when stats allow
- [ ] Ranges are coalesced within 64KB gaps
- [ ] Prefetching stays 1-2 row groups ahead

### Concurrency
- [ ] Conductor is sole driver of event loop
- [ ] Workers block on semaphores, not loop.run()
- [ ] No data races on shared state
- [ ] Connection pool respects concurrency limit

### Output
- [ ] Multipart initiated only when buffer > 5MB
- [ ] Parts uploaded in parallel with processing
- [ ] Footer written last
- [ ] CompleteMultipartUpload called exactly once

### Performance
- [ ] Wall time within 2x of raw `aws s3 cp` for full copies
- [ ] CPU utilization > 70% across cores
- [ ] Memory stays bounded (no unbounded buffering)

---

## Anti-Patterns to Avoid

1. **Workers calling loop.run()** → NestedRunsNotAllowed
2. **Serial S3 requests** → Latency stacking
3. **Buffering entire file before writing** → OOM on large files
4. **Single connection for all reads** → Underutilizing bandwidth
5. **Ignoring column statistics** → Wasting bandwidth on prunable RGs
6. **Starting multipart unconditionally** → Extra API calls for small outputs
