# Changelog

## 0.5.0 (unreleased)

### Behaviour changes

- CLI queries now emit answer-only NDJSON rows by default for every query kind. `jsonl` remains an alias;
  `--format json` emits an array, and `parquet` supports binary stdout (refusing a terminal). Named `-o` defaults to
  Parquet regardless of extension and leaves stdout empty; `-o -` defaults to NDJSON. Success summaries are opt-in
  with `--summary=json`, which also types stderr errors and JSON profiles. `--limit` and SQL LIMIT cap final rows
  across formats and destinations; LIMIT 0 retains schema without decoding pages. Local outputs publish through a
  sibling temporary and atomic rename, preserving old targets on failure and rejecting input/output aliases.
  Checked flushes distinguish closed pipes (141) from disk-full failures (1); invalid or unsupported queries exit 2.
  Overwrites preserve existing file modes; a destination symlink is replaced itself, leaving its referent untouched.
  Scripts must replace `.agg` extraction with row parsing.
  CSV is reserved but deferred. The libzpq, service and Lambda response envelopes are unchanged.
- Byte values print losslessly in JSON and NDJSON (rows, GROUP BY keys, min/max, libzpq SQL NDJSON and the Lambda's
  `agg`). Bytes that were not UTF-8 printed as text with U+FFFD for each such byte, so distinct values (`ff`, `fe`)
  printed alike. A BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY without an annotation still prints as a JSON string when its
  bytes are UTF-8; **when they are not, it prints as `{"base64":"..."}`** (standard alphabet, padded), where it
  printed a string. BSON, GEOMETRY and GEOGRAPHY values always print as `{"base64":"..."}`. A UUID prints as
  lowercase `8-4-4-4-12` and a legacy INTERVAL as `{"months":m,"days":d,"millis":ms}`, where both printed their raw
  bytes as text. A STRING, ENUM or JSON value that is not UTF-8 is an error naming the column (exit 1), where it
  printed U+FFFD; the file is invalid. See `docs/binary-output.md`.

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
- An answer returned inline (libzpq and Lambda: SQL rows, GROUP BY rows) is capped by the request's
  `max_result_bytes`: by default the larger of the query's budget and 1 GiB, and just under the 6 MB a Lambda
  response may be on Lambda. Past it the response is a `result_too_large` error naming the cap, where an answer past
  6 MB was posted to the Lambda Runtime API and refused without a reason. Raise `max_result_bytes`, or write the answer
  to `output_url`. The answer is built once, where it was held three times over.

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

- Inputs written by different writers copy and re-encode together when their columns mean the same thing: a legacy
  ConvertedType (`UTF8`, `TIMESTAMP_MICROS`, `INT_8`...) matches its logical type, and logical types compare by
  value, not by how their Thrift fields were ordered or encoded. Such inputs failed with `schema mismatch across
  inputs`. Genuinely different types, and annotations zpq does not interpret that differ at all, are still refused.
- Encrypted Parquet is refused on every path, naming the file (exit 1). A file with a plaintext footer over
  encrypted columns was copied by `-o` with exit 0 into a file no key could decrypt: the new footer dropped the
  columns' crypto metadata. An encrypted footer (`PARE`) was reported as an invalid file. zpq does not decrypt.
- A legacy REPEATED field with no LIST or MAP annotation (a repeated primitive, or a field under a repeated group)
  printed one row per stored value, so `[[0,1,2,3],[],[4],[5,6,7,8]]` came out as four scalar rows; it is now refused
  in row output, as LIST and MAP columns are, naming the column (exit 2). A filter naming any repeated column is
  refused when the query is parsed (exit 2, where LIST/MAP filters exited 1 at the decode), so a predicate that
  statistics could answer no longer answers for a column the decode would refuse. Copies are unchanged.
- INT96 timestamps read exactly in any year. Values outside 1677-2262, which 64-bit nanoseconds cannot hold, wrapped
  into another instant: Spark's 0001-01-01 and 9999-12-31 sentinels read as 1754 and as some other date. They now
  read as the exact count of nanoseconds since the epoch, printed as that integer, and compare, sort, group and
  aggregate exactly. Values inside the range print as before. Written as a timestamp (`-o` of a GROUP BY key or a
  min/max), one past the range is refused.
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
- Rewrites write non-decimal FIXED_LEN_BYTE_ARRAY columns (UUID, fixed binary) correctly. A dictionary-encoded one
  carried a length before each dictionary entry, which FLBA's PLAIN form does not have, so other readers returned
  incorrect values without an error; 0.4.1 did the same. One without a dictionary could be written as
  DELTA_LENGTH_BYTE_ARRAY, which the format allows for BYTE_ARRAY only, and strict readers refused the file. FLBA now
  writes bare values, as DELTA_BYTE_ARRAY where shared prefixes pay and as PLAIN otherwise.
- Written float statistics follow the format's rules: NaN never enters min or max (a chunk whose first value was NaN
  wrote NaN as both, so no reader could prune it), an all-NaN chunk writes no bounds, and a zero bound is written as
  `-0.0` (min) or `+0.0` (max). A row group's `total_byte_size` is its uncompressed column bytes, as the format
  defines it, where it was the compressed size. 0.4.1 had both. Byte-copied chunks keep their source's statistics.
