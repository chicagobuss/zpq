# Decode-path microbenchmark

Times the parquet decode path in isolation. Strips out glob expansion,
mmap, aggregator, and output formatting so per-(encoding × type × bit-
width × null-rate) costs are directly visible.

## Run

```bash
just microbench
```

Or against any parquet file:

```bash
zig build microbench -Doptimize=ReleaseFast
benchmarks/microbench/run.sh path/to/file.parquet 7 2  # runs, warmup
```

Output is JSON (one column per line). Pipe through `jq` to format:

```bash
benchmarks/microbench/run.sh | jq -r '"\(.name)\t\(.type)\t\(.ns_per_value)\t\(.rows_per_sec)\t\(.decoded_mb_per_sec)"'
```

## What's measured

For each column:
- mmap the file once, find the column's chunks across all RGs.
- Per iteration: per-RG `consumer.decodeColumnT` with a fresh arena
  (so allocator state doesn't pollute across runs).
- Report `min_ns` over N samples (default 7, after 2 warmup runs).
- `ns_per_value`, `rows_per_sec`, `decoded_mb_per_sec` derived.

`min` not `median` because the test exists to find peak performance —
median includes contention from other cores, kernel preemption, etc.,
which are real but not part of "what does the decoder cost when
nothing else is in the way."

## Findings (2026-05-07, baseline)

Run against `data/benchmark_100mb.parquet` on i7-13700HX:

| Type | ns/value | rows/sec | dec MB/s | notes |
|---|---:|---:|---:|---|
| bool | 0.86 | 1.17 B | 146 | RLE-encoded bools — the bit-pack fast path is doing its job |
| string_dict_low | 1.32 | 750 M | 381 | tiny dict, bw=4 — gather is L1-resident, hot |
| string_dict_high | 1.83 | 550 M | 757 | larger dict, still hot |
| int8 / uint8 | 3.3 | 300 M | 305 | 1-byte writes |
| int16 / uint16 | 4.5 | 220 M | 830 | 2-byte writes |
| int32 / float32 | 6.0 | 165 M | 1010 | 4-byte writes |
| int64 / float64 | 8.3 | 120 M | 1220 | 8-byte writes |
| timestamp_sorted | 15.0 | 67 M | 670 | high bit-width dict |
| string_random | 20.0 | 50 M | 2960 | byte-throughput-bound |
| float64_nullable | 9.2 | 110 M | 1010 | only 11% slower than non-nullable |

Two big takeaways:

**The numeric decode path is memory-write-bandwidth-bound, not
compute-bound.** ns/value scales linearly with output byte size
(3 ns for i8, 6 ns for i32, 8 ns for i64). Decoded throughput sits at
~1.2 GB/s for f64/i64 — roughly 4% of the 30 GB/s sequential write
bandwidth a modern CPU can hit. The dict-gather + index-unpack +
write-to-output chain has dependent ops; the CPU isn't pipelining
the way it could. Likely lever: vectorize the gather + write so 4
or 8 lanes commit at once.

**Nullable variants are only ~10-15% slower than non-nullable.** The
`decodePageSlice` fast path that skips the scatter when
`num_present == values.len` is paying off. Real OPTIONAL columns
with no actual nulls (like NYC Taxi's f64 amounts) hit the fast
path constantly.

## Caveats

- All columns in this fixture are SNAPPY-compressed. ZSTD numbers
  may be different (libzstd's decompression path differs).
- All columns have `max_def ≤ 1` (at most one level of nullability);
  no nested types or list/map columns. To measure nested decode,
  generate a fixture with those shapes.
- The harness uses one fixture (`data/benchmark_100mb.parquet`),
  so it only measures shapes that fixture produces. Synthetic
  fixtures with controlled bit_widths / null rates / dict
  cardinalities are a follow-up.
