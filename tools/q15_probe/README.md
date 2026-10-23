# Composite COUNT kernel probes

Standalone measurements for `GROUP BY k,f ORDER BY count(*) DESC LIMIT 10`. These tools do not change query
execution. The source dataset is decoded once into owned contiguous key bytes and compact eight-byte rows;
Parquet decode, extraction, input validation and prefaulting are outside kernel timing. Allocation, worker startup,
identity, partition writes, grouping and top-ten selection are inside it. Treat the timings as kernel costs.

Use the repository's pinned Zig toolchain and a Python environment with PyArrow, NumPy and DuckDB:

```sh
python tools/q15_probe/extract.py input.parquet /tmp/composite-probe
zig build-exe tools/q15_probe/main.zig -O ReleaseFast -lc -femit-bin=/tmp/msd-probe
zig build-exe -O ReleaseFast -lc --dep bytes -Mroot=tools/q15_probe/intern.zig \
  -Mbytes=src/core/bytes.zig -femit-bin=/tmp/intern-probe
zig build-exe tools/q15_probe/sketch.zig -O ReleaseFast -lc -femit-bin=/tmp/sketch-probe
/tmp/msd-probe /tmp/composite-probe 12 32 0 11
/tmp/intern-probe /tmp/composite-probe 12 64
/tmp/sketch-probe /tmp/composite-probe 12 3 8
```

Run one measured kernel at a time, with identical CPU affinity and an external memory limit. Results are JSON on
stderr. `meta.json` contains the exact top ten from DuckDB, including minimum input ordinals, and the group count.
The probes accept non-NULL byte keys of at most 255 bytes, fewer than 2^25 rows, a byte image smaller than 2^32,
and integer `f` in `[0,1000)`. These are checked probe domains, not SQL type restrictions. Empty keys and embedded
zero bytes are valid. Extraction fully reads both columns. No metadata value answers the query.

`main.zig` arguments are directory, workers, insertion cutoff, prefix bytes to verify, maximum radix bits,
and optional mode (`prune`, `full`, `tail`, `tailfull`). Worker count is 1–64; radix bits are 1–11. Prefix 0 is
general. A nonzero prefix checks that many bytes of `https://example.org/` against **every row**, declining on a
mismatch. The full prefix is 20 bytes; the initial sweep deliberately retained one slash with prefix 19.
`tail` caches the last eight bytes first, then visits every needed head word. `full` modes disable bound pruning
and return exact group/row totals. The returned `first` ordinal identifies the output key in the raw rows.

The MSD record is 16 bytes: a big-endian cached word, u32 byte offset, and packed ordinal/length. The high length
bit is carried by the initial `f*2+high_length_bit` bucket. Within each completed bucket, split on the remaining
length bits and recurse on cached digits, skipping equal eight-byte words. Only word reloads touch original keys.
Small buckets finish by insertion on cached u64 words. Each completed exact leaf supplies count and minimum
ordinal to a worker heap. The maximum of worker tenth counts is a proven global lower bound: buckets strictly
smaller than it cannot win. Equal-count buckets remain. No sketch or hash identity is involved.

The interner uses full-byte equality and one owned copy per distinct key, sharded batch locking, then sparse
16-byte `(internal_id,f,count,first)` records in buffered owner partitions. Internal IDs are representative
ordinals and never decide output order. Complete partition tables feed the same top-ten comparison.

The CMS probe deliberately receives an **oracle threshold** as its third argument, followed by counter width
8 or 16. It measures two full-byte hash passes: constructing a shared 32 MiB, two-row, batched-shard sketch,
then counting rows whose bound reaches the supplied threshold. Saturated counters represent infinity.
It does **not** build a witness heap or exactly verify survivors. Therefore its time is an optimistic lower
bound for that proposed sketch design, and its output is not a query answer. Collisions only enlarge survivors.
In a real scan, counting witnesses in pass two establishes the threshold only at the end; filtering with that
final threshold needs retained decoded data or a third pass. Never present this probe as a two-pass exact engine.

These are bounded experimental executables: allocation and thread failures terminate them. They have no query
ledger or fallback machinery. An engine port must prepay ownership, preserve decoder error precedence and permit
replay before consuming input. In particular, the 2.4 GB owned decoded byte image is not free: current sliced
readers reset page storage, so retaining or copying those bytes is a separate design and bandwidth cost.

After building the three executables in one directory, run
`python tools/q15_probe/check.py /tmp /tmp/composite-fixtures`. It checks ties, empty and embedded-zero keys,
length boundaries, a late discriminating suffix and a flat count distribution at workers 1,2,4,8,16. Full-mode
checks include every group and row, while bounded modes compare the ordered top ten to exact byte tuples.
