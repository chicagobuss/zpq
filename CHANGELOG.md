# Changelog

## 0.5.0 (unreleased)

### Behaviour changes

- DECIMAL is exact. `sum` / `min` / `max` / `avg` over DECIMAL columns and DECIMAL arithmetic (`+`, `-`, `*`, e.g.
  `price * (1 - discount) * (1 + tax)`) compute in scaled 128-bit integers with the result types DuckDB and Trino use,
  where they answered in f64 before (a TPC-H Q1 sum was a cent off at SF30). Numeric literals with a decimal point are
  DECIMAL, as in SQL. The JSON prints exact text, `-o` writes DECIMAL columns (a DECIMAL `GROUP BY` key and min/max
  were DOUBLE), and a result past its type's 38 digits is an error instead of a rounded value. `avg` keeps the
  argument's type, its exact mean rounded half away from zero (Trino's rule), and its JSON gains `"avg"`. DECIMAL `/`
  is DOUBLE. See the README for the result-type rules and where DuckDB's differ.
- `GROUP BY` lists groups in the order they first appear in the input (files in the order given, then row groups, then
  rows), at every `-j`; before, groups came in the byte order of their serialized keys. Floating-point sums and
  averages come out with the same bits at every `-j` and on every run. `--unordered` (the Lambda's `"ordered": false`)
  gives both up for speed.
- Float `min` / `max` order NaN above every number, as DuckDB, Postgres and Spark do: `max` is NaN if any value is,
  `min` skips NaN unless every value is NaN, and an all-NaN input answers NaN (it answered `Infinity` / `-Infinity`
  without `GROUP BY`). Of two equal zeros, `-0` is the lesser. Every path agrees, whatever the order rows and partial
  results meet in: plain and grouped, dictionary-coded or not, and answers from statistics, where a `max` is taken only
  from a chunk known to hold no NaN.

### Added

- Split-block Bloom filters. `=` and `IN` filters prune row groups by a chunk's Bloom filter, or by its dictionary when
  the chunk is fully dictionary-encoded and has no filter. On S3 a cost model decides whether fetching the filters is
  worth it, and the Lambda keeps them warm between invocations. `--bloom-filter` (the Lambda's `bloom_filters`) writes
  them, and byte copies carry them through. See [docs/bloom_filters.md](docs/bloom_filters.md).
- Query profiling. `--profile` writes a per-thread trace that opens in Perfetto, and `--profile-summary` prints time per
  phase, I/O and memory. The Lambda takes `"profile": true` and `"profile_url"`. See
  [docs/profiling.md](docs/profiling.md).
- `--unordered` (the Lambda's `"ordered": false`), which trades the order guarantees above for speed.
- Explicit local reads on Linux. An aggregate over files mostly not in the page cache reads only the column chunks it
  needs, through io_uring with `O_DIRECT` or `pread`, ahead of the workers and within a bounded window.
  `ZPQ_IO_PROFILE`, `ZPQ_IO_WINDOW_MIB` and `ZPQ_IO_THREADS` tune it. See [docs/local_io.md](docs/local_io.md).
- The query summary reports `total_us` beside `total_ms`.

### Performance

- `IN` and `NOT IN` lists bind once per query to a set suited to their values (vector compare, bitmap, hash table or
  sorted array). Over dictionary-coded columns each dictionary entry is tested once.
- Filter leaves compare 64 rows at a time into a selection word. Null masks and null scatter run without a branch per
  row.
- Aggregate scans work on dictionary codes where they can, and evaluate computed aggregate arguments only over the
  selected rows. String `min` / `max` compare a 16-byte prefix before the full bytes.
- Scan workers start as a spawn tree, and each one backs its decode memory with a single huge-page region.
- `--compress-priority size|speed` (the Lambda's `"compress_priority"`). `size`, the default everywhere, compresses
  zstd pages at level 3 as before. `speed`, an opt-in, stores each page in the form that costs least (raw blocks,
  literals only, level 1 or level 3), estimated from a sample and checked against the whole page, falling back to
  level 3 when a check fails. It uses less CPU but has no size bound relative to level 3: a page compressible only
  through matches the checks miss is stored raw (see the README). The output is the same at every `-j`.
- zstd's Huffman coding of literals runs on AVX-512 (F, BW, VL, VBMI and VBMI2) where the CPU has it, writing the same
  bytes as zstd's own encoder, which runs everywhere else. `ZPQ_ZSTD_SCALAR=1` turns it off. The change to the vendored
  zstd is one patch, described in [vendor/README.md](vendor/README.md).
- Re-encoded string columns read faster everywhere and come out a little smaller: a dictionary while a quarter or
  fewer of the values are distinct and it fits 1 MiB (was: a tenth), DELTA_BYTE_ARRAY only where values share long
  prefixes with the previous one, and DELTA_LENGTH_BYTE_ARRAY otherwise, which every reader we measured decodes
  fastest.
- A copy that changes codec recompresses a row group's pages across the CPU budget: a file of one or a few row groups
  is no longer recompressed on one core, a column at a time. The output is the same bytes.
- GZIP pages are read with libdeflate (vendored, MIT), about three times as fast as the Zig stdlib's inflater it
  replaces. `ZPQ_GZIP_STD=1` selects the stdlib inflater, to compare the two or to fall back.
- LZ4_RAW pages are written with liblz4's reference encoder (vendored, BSD-2-Clause) in place of zpq's own, which cost
  more than zstd level 3: several times as fast, its output a few percent larger or smaller by page. Reading is
  unchanged.
- Column encoding writes the same bytes in fewer instructions: bit packing and run detection work a word at a time,
  string statistics compare a 16-byte prefix first, and dense columns copy and pack without per-value branches.
- Writes filter first and then decode and project one column at a time. They re-encode dictionary columns from their
  codes, keep working memory across row groups, and recompress byte-copied row groups in parallel.

### Fixes

- An S3 response that stalls no longer hangs the query. A read had no deadline, so a connection the server or the
  network left open but silent blocked its request forever, and with it the whole query: about one in ten Lambda
  rewrites of an 868 MB object stalled until the function timed out. Connecting, the TLS handshake, the first byte of
  a response and every gap mid-transfer now have deadlines (5 s, 5 s, 10 s and 10 s by default; the
  `ZPQ_S3_*_TIMEOUT_MS` variables override them). A timed-out GET or part upload is retried on a new connection
  within the usual retry budget. 0.4.0 had the same gap.
- A CreateMultipartUpload that could not connect (DNS, TCP or the TLS handshake failed before anything was sent) is
  tried again on a new connection, charged to the retry budget, instead of failing the write at once. One sent and
  then unanswered, or answered with an error, still fails the write and is never sent again. A failed Create,
  Complete or Abort now reports which request failed, at what point, and its error or HTTP status and S3 error code:
  on stderr, and as `"s3"` in the Lambda's error JSON. The README recommends an `AbortIncompleteMultipartUpload`
  lifecycle rule as the backstop for uploads left open.
- `--max-memory` bounds a query's working memory on every path, over S3 and locally, where an S3 query held every
  byte it read at once (a 6.7 GB full scan ran out of memory on a 3008 MB Lambda) and a large row group was decoded
  whole. S3 inputs past a quarter of it (64 MiB to 1 GiB; `ZPQ_S3_WINDOW_KIB` overrides it) are fetched ahead of the
  readers and released behind them (Linux; elsewhere still up front), and a print with `--limit` stops fetching once
  it has its rows. Half of it is for decoded row groups, shared by the workers: a row group too big for one is
  aggregated, printed or written a slice of rows at a time, a page per column held beside the slice, and one with no
  sliced form (a nested column) is refused with `RowGroupTooLarge` before it decodes, never killed. A write emits a
  row group per slice: contents are identical, but row-group boundaries of filtered or computed writes may depend on
  `--max-memory` and `-j` (and are the same for the same values). A
  write holds at most a quarter of it encoded: a row group whose rewrite could take more (an expanding codec, a sliced
  row group) streams to the output a page or a slice at a time, and S3 uploads keep at most an eighth in flight. The
  default also honours a cgroup v2 memory limit, as it already did a Lambda's memory size.
- Corrupted GZIP pages are rejected instead of returning wrong values. Each gzip member's CRC-32 and length are now
  checked; before, decoding stopped once a page's bytes were out and never read the member's trailer.
- A `--trust-stats` float `min` / `max` of zero decodes the chunk rather than taking its sign from bounds, since writers
  record `-0` / `+0` bounds for any chunk that holds a zero.
- UUID literals match the 16 bytes a UUID column stores.
- Long `IN` lists nest log-deep, so they no longer overflow the stack.
- S3: the fetched file tail is freed when every row group prunes, and the connection pool is freed when opening an
  input fails.
- `--profile` checks its path before the query runs, and replaces the file only once the trace is complete.
