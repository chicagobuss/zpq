# Changelog

## 0.5.0 (unreleased)

### Behaviour changes

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
- Column encoding writes the same bytes in fewer instructions: bit packing and run detection work a word at a time,
  string statistics compare a 16-byte prefix first, and dense columns copy and pack without per-value branches.
- Writes filter first and then decode and project one column at a time. They re-encode dictionary columns from their
  codes, keep working memory across row groups, and recompress byte-copied row groups in parallel.

### Fixes

- A `--trust-stats` float `min` / `max` of zero decodes the chunk rather than taking its sign from bounds, since writers
  record `-0` / `+0` bounds for any chunk that holds a zero.
- UUID literals match the 16 bytes a UUID column stores.
- Long `IN` lists nest log-deep, so they no longer overflow the stack.
- S3: the fetched file tail is freed when every row group prunes, and the connection pool is freed when opening an
  input fails.
- `--profile` checks its path before the query runs, and replaces the file only once the trace is complete.
