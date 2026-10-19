# Changelog

## 0.5.0 (unreleased)

**Read "What to do" first if you script zpq or rely on its answers.** 0.5.0 changes what `zpq query` prints, fixes
answers that 0.4.1 got incorrect without an error, and turns `--max-memory` into a real ceiling. It adds a C library
(libzpq) with a Python binding, `zpq inspect`, a much larger SQL surface, Bloom filters, profiling, and Homebrew,
shell-completion and man-page packaging.

### What to do

1. **Scripts that parse `zpq query` output must change.** Every query now prints its answer as NDJSON rows on stdout,
   and nothing else. The `{"ok":true,...,"agg":...}` summary that 0.4.1 printed is gone from stdout; take the rows
   instead, or add `--summary=json` to get a typed summary line on **stderr**.
2. **`--format csv` no longer works.** 0.4.1 printed CSV; 0.5.0 exits 2 with `csv output is not supported yet`. Use
   NDJSON (`--format ndjson`, or `jsonl`, the default), `--format json`, or Parquet (`-o FILE`).
3. **Re-run the queries listed under "Answers that changed"** if you relied on their 0.4.1 results: DECIMAL sums and
   filters, UUID filters, `NOT IN` lists containing `NULL`, `LIKE` with `_`, `coalesce`, integer arithmetic near the
   64-bit limit, INT96 timestamps, float min/max with NaN, and any Parquet file 0.4.1 **rewrote** from a fixed-width
   binary (UUID) or BYTE_ARRAY DECIMAL column.
4. **Check `--max-memory` values you pass.** It used to bound GROUP BY entries only; it now bounds everything an
   aggregate or a write holds. A query that answered at a given value can now refuse instead, with
   `the query needs more memory than --max-memory allows` (exit 1). Raise the value, or leave it unset.
5. **Expect GROUP BY rows in a different order.** Groups now come in the order they first appear in the input, at
   every `-j`; 0.4.1 sorted them by their serialized key bytes. Add `ORDER BY` if you need an order.
6. **Invalid queries exit 2, not 1.** Read, decode and write failures stay 1; a closed stdout pipe is 141.

### Highlights

- **Answers on stdout, ready for Unix tools.** NDJSON rows by default for every query kind, `--format json` for an
  array, Parquet to a file or to stdout. Local outputs are published atomically.
- **Exact by default.** DECIMAL computes in scaled 128-bit integers, integer arithmetic fails instead of wrapping, and
  every aggregate (float sums included) gives the same bits at every `-j` and on every run.
- **A memory ceiling that holds.** `--max-memory` is one budget per query that tables, decoding, read-ahead, the
  merge, the answer and write buffers all draw from; a query that does not fit is retried on narrower paths and
  otherwise refused, never killed.
- **libzpq**: the engine as a C library with a Python binding (`python/zpq.py`) and an HTTP service. SQL through it is
  EXPERIMENTAL.
- **SQL**: `ORDER BY`, `LIMIT`/`OFFSET`, `HAVING`, `count(DISTINCT ...)`, `CASE`, `strlen`, GROUP BY by expression,
  position or alias, and parenthesized filters with `NOT`.
- **`zpq inspect`** reports footer metadata without decoding data pages.
- **Lossless JSON** for binary, UUID, INTERVAL, BSON and GEOMETRY values.
- **Bloom filters** read, written and used to prune `=` and `IN`.
- **Profiling** with `--profile` (a Perfetto trace) and `--profile-summary`.

### Performance

{{PERF: clickbench vs 0.4.1}}

{{PERF: cold matrix (v7) vs 0.4.1}}

{{PERF: GROUP BY / count(DISTINCT) / Top-N study cells vs 0.4.1}}

{{PERF: writes and rewrites vs 0.4.1}}

{{PERF: Lambda cold start and S3-to-S3 vs 0.4.1}}

{{PERF: peak memory at the default budget vs 0.4.1}}

{{PERF: comparison with other engines, if any}}

What changed underneath:

- Aggregates run over dictionary codes where they can; `IN` lists bind once to a set suited to their values; filters
  compare 64 rows at a time.
- A rule-based planner picks among grouping strategies (plain tables, shared partitions, exact count cuts for
  `ORDER BY count(*) DESC LIMIT n`, a dense fold over a dictionary domain) from footer and dictionary facts only;
  `--explain` shows the facts, the candidates and the rule that decided. Every strategy is exact.
- `ORDER BY ... LIMIT n` over plain rows ranks by its keys in parallel and reads only the winners' rows, skipping row
  groups that cannot win.
- GROUP BY state is stored at the width each reduction needs, and group lookups cache their hashes.
- Local reads on Linux can bypass the page cache (io_uring with `O_DIRECT`, or `pread`) for cold files, reading only
  the column chunks a query needs.
- GZIP pages inflate with libdeflate; LZ4_RAW pages are written with liblz4; zstd's literal Huffman coding uses
  AVX-512 where the CPU has it, writing the same bytes.
- Writes filter first, then decode and encode one column at a time, re-encode dictionary columns from their codes, and
  recompress pages in parallel.

### Answers that changed: incorrect in 0.4.1, fixed in 0.5.0

Each of these returned an incorrect answer in 0.4.1 with exit 0 and no error. Where the "run" note says so, the 0.4.1
answer below was reproduced with the 0.4.1 binary.

| What was incorrect in 0.4.1 | Queries affected | What to do |
|---|---|---|
| DECIMAL `sum`/`avg`/`min`/`max` and DECIMAL arithmetic were computed in 64-bit floating point, so large sums could be off in the last digits (a TPC-H Q1 sum was a cent off at SF30). | Aggregates or `--select` arithmetic over DECIMAL columns | Re-run them. Results now print every digit of the scale (`3.00`, where 0.4.1 printed `3`). |
| A DECIMAL filter compared through a double, so values past about 15 significant digits matched their neighbours: `d = 9007199254740993` also matched `...992`. | Filters on DECIMAL columns with more than 15 digits | Re-run them. |
| A UUID literal never matched a UUID column: `u = '00010203-...'` returned no rows. | Filters on UUID columns | Re-run them. |
| `s NOT IN ('a', NULL)` in `--filter` compared with the text `'NULL'` and kept rows that SQL keeps none of. | `--filter` with `NULL` in an `IN` / `NOT IN` list (0.4.1's SQL refused it) | Re-run them. |
| A `--filter` string comparison whose right side was a column (`s = t`) compared with the text `t`. It is now refused (exit 2). | `--filter` comparing two columns | Compare columns in SQL `--select`/`CASE`, or filter on literals. |
| `LIKE` `_` matched one byte, not one character, so `'_'` missed `'é'`; and `%` facing a literal `%` in the value stopped matching (`'a%b' LIKE 'a%_b'` was false). | `LIKE` patterns with `_` over non-ASCII text, or over values containing `%` | Re-run them. |
| `coalesce(a, b)` gave 0 (or `""`) where both were NULL, so `count`, `min`, `avg` and GROUP BY saw a phantom value; and a UINT32 of 2^31 or more came out negative. | `coalesce` over nullable columns or UINT32 columns | Re-run them. |
| Integer `+ - * /` wrapped silently on overflow: `sum(a * 4)` with `a = 2^62` answered 4. It now fails with `integer overflow in *` (exit 1). | Integer arithmetic near the 64-bit limit | Cast to DECIMAL, or check the inputs' range. |
| Float `min`/`max` over a column of only NaN answered `Infinity`/`-Infinity`. | Float min/max | See also the NaN ordering change under "Behaviour changes". |
| `--trust-stats` float `min`/`max` of a chunk holding zero took its sign from the statistics: `min` of `[0.0, 5.0]` answered `-0`. | `--trust-stats` with float columns | Re-run them. |
| INT96 timestamps outside 1677-2262 wrapped into another instant: the 9999-12-31 sentinel read as a negative number. They now print the exact count of nanoseconds since the epoch. | INT96 columns with far-past or far-future values | Re-run them. |
| A legacy REPEATED field without a LIST or MAP annotation printed one row per stored value. Row output and filters now refuse it, naming the column (exit 2). | Row output from files with legacy repeated fields | Copies (`-o` without a filter) still keep them. |
| Binary values that are not UTF-8 printed with U+FFFD for each such byte, so distinct values printed alike (two different GROUP BY keys looked the same); UUIDs printed their raw bytes. See "JSON output". | Rows, GROUP BY keys and min/max of binary and UUID columns | Read the new forms. |
| **Rewritten files:** a fixed-width binary column (UUID, `fixed_size_binary`) re-encoded with a dictionary got a 4-byte length before each entry, so other readers returned incorrect values with no error. | Files 0.4.1 wrote with a filter, `--select` or `--codec` from inputs with such columns | **Rewrite those files with 0.5.0** from their sources. |
| **Rewritten files:** a BYTE_ARRAY DECIMAL column was re-encoded as DOUBLE, losing exactness and changing the schema. It is now written as a DECIMAL (FIXED_LEN_BYTE_ARRAY). | Files 0.4.1 rewrote from BYTE_ARRAY DECIMAL inputs | Rewrite them with 0.5.0. |
| **Rewritten files:** float statistics wrote NaN as min and max when a chunk's first value was NaN, so readers could never prune the chunk; `total_byte_size` held the compressed size. | Files 0.4.1 wrote from float columns with NaN | Answers were not affected; rewrite to restore pruning. |
| **Encrypted Parquet** with a plaintext footer was copied with exit 0 into a file no key could decrypt. Every path now refuses encrypted input, naming the file (exit 1). | Copies of encrypted files | zpq does not decrypt. |
| Corrupted GZIP pages could return incorrect values: the gzip member's CRC-32 and length were not checked. | GZIP-compressed inputs | None; a corrupted page is now an error. |
| Two inputs whose legacy DECIMAL scales differed were read as one schema, the second at the first's scale (a hundred times off). | Multi-file queries over files from different writers | Re-run them. |
| A DECIMAL carrying only the logical annotation printed in rows without its scale (`1.25` as `125`). | Row output of such columns | Re-run them. |
| When the first input of a multi-file write had no row groups, a LIST column could be re-encoded without its repetition levels, writing an unreadable file with exit 0. | Multi-file writes whose first file is empty | Rewrite those files. |

Fixed during the 0.5.0 cycle and never released, so not listed: `c IS NOT NULL AND (c = 2 OR b = 1)` dropping rows
(0.4.1 refused that shape), and several grouped-aggregate paths over dictionary codes that are new in 0.5.0.

### Behaviour changes

**Output**
- Every query prints only its answer: NDJSON rows on stdout, one object per line, scalar aggregates as one row. The
  summary is opt-in with `--summary=json`, on stderr, which also types errors (`{"type":"error",...}`) and JSON
  profiles. `jsonl` remains an alias of `ndjson`; `--format json` prints an array; `--format parquet` writes Parquet
  to stdout and refuses a terminal.
- `--format csv` exits 2. 0.4.1 supported it.
- A query with no `-o` and no aggregate prints rows; 0.4.1 required `--output`.
- A named `-o FILE` writes Parquet whatever its extension and leaves stdout empty; `-o -` writes stdout (NDJSON by
  default). Local outputs are written to a sibling temporary file and renamed into place, so a failure leaves the old
  file; an output that is also an input (hard links included) is refused. Overwrites keep the file's mode; a symlink
  at the destination is replaced itself.
- `--limit` and SQL `LIMIT` cap the final rows for every format and destination; giving both is an error. `LIMIT 0`
  checks the query and schema without decoding pages.
- Exit codes: 0 success, 1 read/decode/write failure, 2 invalid or unsupported query or option (0.4.1: 1), 141 when
  the reader of stdout closes it. A disk-full flush is 1.

**Answers**
- GROUP BY lists groups in first-appearance order (files as given, then row groups, then rows) at every `-j`; 0.4.1
  used the byte order of their serialized keys. `--unordered` (Lambda: `"ordered": false`) gives up the order, and
  bit-identical float sums, for speed.
- Float sums and averages are summed in one fixed order, so they are the same at every `-j`, budget and decoder; their
  last bits can differ from 0.4.1's.
- Float `min`/`max` order NaN above every number: `max` is NaN if any value is NaN (0.4.1 skipped NaN: `max` of
  `[1, NaN, 2]` was 2, now NaN), `min` skips NaN unless every value is NaN. `-0` is less than `+0`.
- DECIMAL: results are DECIMAL with fixed result-type rules (`+`/`-` take the larger scale and one more digit, `*`
  adds scales, `sum` is DECIMAL(38, s), `min`/`max`/`avg` keep the column's type, `avg` rounds half away from zero);
  numeric literals with a decimal point are DECIMAL; `/` is DOUBLE; a result past 38 digits is an error. A DECIMAL
  `avg` adds `"avg"` beside `{"sum","count"}`; `-o` writes DECIMAL columns where 0.4.1 wrote DOUBLE.
- Integer overflow in `+ - * /` fails the query (`integer overflow in *`, exit 1) instead of wrapping, but only on a
  row the query consumes.
- A count past 2^63 across files is an error (`RowCountTooLarge`) rather than saturating.

**Refusals that were answers** (each names the column or file)
- Row output and filters refuse repeated columns (LIST, MAP, legacy REPEATED), exit 2; filters refuse them when the
  query is parsed. Copies keep them.
- A `--filter` string comparison must have a quoted literal (or a bare word that names no column) on its right.
- A STRING, ENUM or JSON value that is not UTF-8 is an error (exit 1); the file is invalid.
- Encrypted Parquet, on every path (exit 1).
- A FIFO or a directory given as an input.
- Files that break the format: an RLE run value wider than its bit width, a FIXED_LEN_BYTE_ARRAY of no length, a
  DELTA_BYTE_ARRAY value of another length than its FLBA column, an INT32 past its 8- or 16-bit integer type, a level
  past its column's maximum, a level run claiming bytes past its stream, a second dictionary page in a chunk.
- A local input truncated while a query reads it is an error (`InputChanged`), not a crash.

**Accepted now**
- Inputs from different writers whose column annotations mean the same thing (a legacy `UTF8` beside a STRING
  annotation, Thrift fields in another order) copy and re-encode together; 0.4.1 refused them with
  `schema mismatch across inputs`. Different types are still refused.
- INT96 values in any year.
- A page whose V2 header miscounts its nulls is read by its levels, as other readers do.

**Writes**
- With a `--max-memory` that binds, a filtered or computed write may emit one output row group per slice: contents
  are identical, row-group boundaries depend on `--max-memory` and `-j`.
- Written float statistics leave NaN out of min/max and write zero bounds as `-0.0`/`+0.0`; `total_byte_size` is the
  uncompressed size. Byte-copied chunks keep their source's statistics.
- Re-encoded strings choose dictionary (a quarter or fewer distinct and within 1 MiB), DELTA_BYTE_ARRAY (long shared
  prefixes) or DELTA_LENGTH_BYTE_ARRAY; FIXED_LEN_BYTE_ARRAY falls back to PLAIN.
- Annotations zpq does not interpret (GEOMETRY, GEOGRAPHY, VARIANT, unknown) are kept through copies, where they were
  rewritten as NullType.
- `--compress-priority size|speed`; `size` (level 3) is the default everywhere.

**Lambda and S3**
- Every S3 wait has a deadline (connect 5 s, TLS handshake 5 s, first byte 10 s, idle 10 s, a request in all 30 s,
  CompleteMultipartUpload 15 min; `ZPQ_S3_*_TIMEOUT_MS` override them); a stalled request is retried on a new
  connection. 0.4.0 and 0.4.1 could hang until the Lambda timed out.
- A CreateMultipartUpload or CompleteMultipartUpload that could not connect is retried; control-plane failures name
  the request, the point it failed and the S3 error code (`"s3"` in the Lambda's error JSON).
- An inline answer (SQL rows, GROUP BY rows) is capped by the request's `max_result_bytes`: by default the larger of the
  query's budget and 1 GiB, and just under the 6 MB Lambda limit on Lambda. Past it the response is a
  `result_too_large` error naming the cap.
- The Lambda stops S3 work a second before its invocation deadline and reports through `/error` instead of being
  killed.
- The Lambda pre-connects to the regional S3 endpoint during init and fetches a footer with one suffix GET.
- Request strictness: a NUL byte in a path, header-breaking bytes in an S3 URL, an unknown `output_codec`, a size of
  `0MB` and a `max_memory` of 0 are refused.

### Memory ceiling: `--max-memory`

`--max-memory` (the Lambda's and libzpq's `max_memory`) is one budget per query. Everything an aggregate or a write
holds at once draws from it **before** allocating: GROUP BY tables, each worker's decoded row groups, a local reader's
read-ahead, the S3 footer, the merge, the answer and its encoding, scalar string `min`/`max` winners, and a write's
encoded output in flight. It does not depend on `-j`. In 0.4.1 it bounded GROUP BY entries only.

**Outside the budget** is a fixed slack: the binary, thread stacks, the mapped input, allocator bookkeeping, each scan
worker's first 2 MiB of decode memory, 16 MiB for a GROUP BY's merge and answer past its tables, and 32 MiB for one
decode (the last retry below, or a write's row group sliced on its writer).

**Default:** 80% of the memory Linux reports available, but no more than three quarters of a cgroup v2 `memory.max`;
three quarters of a Lambda's memory size; half the physical memory on macOS.

**When a query does not fit**, a GROUP BY or aggregate scan is run again from the start, each time on a narrower path,
and the answer is the same whichever path answers:
1. the plan the planner chose;
2. without each fast path the refused scan used (dense fold, spill, count(DISTINCT) pairs, cost-chosen shared
   partitions); a plain grouping chosen in place of an exact count cut gives way to that cut;
3. where several workers' tables were refused and shared partitions can fit: forced shared partitions, folding a
   partition's groups at a time;
4. on one worker, where one worker may fit, or where decode or merge memory was refused;
5. on one worker, with decoding on its own 32 MiB budget (row groups past half of it are decoded in slices), where
   decode memory was refused;
6. otherwise the query fails, exit 1, with:

   ```
   zpq query: the query needs more memory than --max-memory allows
   ```

   (`--summary=json`: `{"type":"error","ok":false,"code":"ExceededMemoryBudget",...}`). At most six scans run.

Row groups too big for a worker's share are decoded, filtered and aggregated, printed or written a slice of rows at a
time; one that cannot be sliced (a nested column) is refused with `RowGroupTooLarge` before it decodes. A write whose
row group does not fit beside its producers hands it to the writer, which slices it. S3 inputs past a quarter of the
budget (64 MiB to 1 GiB; `ZPQ_S3_WINDOW_KIB`) are fetched just ahead of the readers and released behind them (Linux).

A query close to its budget can now take noticeably longer: it pays for a refused parallel scan before the one-worker
rerun. {{PERF: edge-budget reruns, if the lead wants a figure}}

### JSON output for binary, UUID, INTERVAL, BSON and GEOMETRY

Every value printed as JSON round-trips to its stored bytes, and no two distinct values print alike:

| Column | JSON value |
|---|---|
| STRING / UTF8, ENUM, JSON | a string; a value that is not UTF-8 is an error naming the column (exit 1) |
| no annotation (BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY) | a string when the bytes are UTF-8, else `{"base64":"..."}` |
| BSON, GEOMETRY, GEOGRAPHY | always `{"base64":"..."}` |
| UUID | lowercase `8-4-4-4-12` text |
| legacy INTERVAL | `{"months":m,"days":d,"millis":ms}` |

Base64 is the standard alphabet, padded. A string never equals an object, so `{"base64":"/w=="}` (the byte `ff`) and
the text `"/w=="` stay distinct. This applies to rows, GROUP BY keys, `min`/`max`, libzpq SQL NDJSON and the Lambda's
`agg`. Filters, ordering and `min`/`max` compare the stored bytes, unsigned. A computed string (`CASE`, `||`) is text.
DECIMAL and FLOAT16 over FIXED_LEN_BYTE_ARRAY are numbers. `-o` Parquet output is unaffected.

### New SQL surface

- `ORDER BY` over aggregate results, keys, and terms the SELECT list lacks; `LIMIT` and `OFFSET`.
- `ORDER BY` over plain rows, with or without `LIMIT` (ties by input position).
- `HAVING`, comparing aggregates or keys with literals.
- `count(DISTINCT x)`, exact.
- GROUP BY keys as expressions (bound by value), SELECT positions or aliases.
- Searched `CASE WHEN ... THEN ... [ELSE ...] END` (a branch is evaluated only on rows that take it) and
  `strlen(s)` (bytes; NULL for NULL).
- Filters (SQL `WHERE` and `--filter`): grouping parentheses, `NOT` over a group (three-valued), `NOT BETWEEN`, `<>`,
  `NULL` in `IN` lists as SQL's NULL, quoted text containing keywords, `''` for a quote. Nesting is limited to 32.
- DECIMAL literals and exact DECIMAL arithmetic (see Behaviour changes).
- `--explain` prints the aggregate plan as JSON without reading data pages.

Not supported: the simple `CASE x WHEN` form, `%`, joins.

### libzpq and Python (SQL is EXPERIMENTAL)

- `libzpq` exposes the Lambda's JSON request handler through seven C functions (`include/zpq.h`, ABI version 1):
  local paths and `s3://` URLs, aggregates and writes, shared S3 connections and footers per context, and a context
  memory ceiling shared by its queries in arrival order. Build with `zig build libzpq -Doptimize=ReleaseFast`.
- `python/zpq.py` binds it with ctypes (no compiled extension); `python/zpq_service.py` is a small HTTP service.
- **SQL requests are EXPERIMENTAL in 0.5.0** and their request and response shapes may change:
  `{"sql": "...", "parallelism": n}`, or `Zpq.query()` (rows as dicts) and `Zpq.query_raw()` (NDJSON identical to the
  CLI's stdout). The binding has no Arrow export, and a SQL request cannot write a file (`output_url` is excluded); use
  a structured request with `output_url` to write Parquet.
- Also experimental: the response's `pool` and `profile` members, the Python API and the HTTP service.
- libzpq never raises or resets SIGPIPE in its host and runs each request on a stack of its own when the caller's is
  short. A local input truncated while libzpq reads it raises SIGBUS in the host; write a new file and rename it.

### `zpq inspect`, completions, man page

- `zpq inspect FILE... [--format json] [--detail]` reports each file's footer, schema, row groups and column chunks
  (codecs, encodings, sizes, statistics with their exactness, index and Bloom filter presence) without decoding data.
  JSON has `schema_version: 1`; values are strings so 64-bit and DECIMAL bounds do not round. `--detail` reads bounded
  page, index and Bloom metadata (8 MiB). A file that fails is reported in place and the command exits 1.
- Bash, zsh and fish completions and a man page are in `packaging/`, generated from the CLI's help text.
- `zpq --version` prints the version and build revision.

### Homebrew

A formula is staged in `packaging/homebrew/zpq.rb` for macOS on Apple silicon and Linux (arm64 and x86_64, static
musl). It installs the binary, completions, man page and a test fixture. It is not a published tap yet.
{{BREW: tap name and install command, once published}}

### Also new

- Split-block Bloom filters: `=` and `IN` prune row groups by a chunk's Bloom filter, or by its dictionary when the
  chunk is fully dictionary-encoded; `--bloom-filter COL[:FPP]` (Lambda: `bloom_filters`) writes them; byte copies
  carry them.
- `--profile TRACE.json` (Perfetto) and `--profile-summary[=text|json]`; the Lambda takes `"profile"` and
  `"profile_url"`.
- `--unordered`.
- `--compress-priority size|speed`.
- Explicit local reads on Linux (`ZPQ_IO_PROFILE`, `ZPQ_IO_WINDOW_MIB`, `ZPQ_IO_THREADS`).
- The summary reports `total_us` beside `total_ms`.

### Platforms

- CLI release builds: Linux x86_64 and arm64 (static musl), macOS arm64 and x86_64 (macOS 15 or later).
- Lambda: `zpq-lambda` for `provided.al2023`, arm64 and x86_64. The Lambda build has no SQL; a SQL request answers
  `SqlNotCompiledIn`.
- libzpq: Linux x86_64 and arm64 (glibc 2.28 or later, or musl) and macOS arm64 and x86_64; the macOS libraries are
  cross-built and not run. Windows is not built.
- snappy's NEON paths are enabled on arm64.
- Building from source requires Zig 0.17.0 exactly.

### Known limits

- `--max-memory` does not yet cover: grouped string `min`/`max` winners inside group tables, first-keys/census/sketch
  structures, the S3 fetch window (its own quarter of the budget), the row-printing and Top-N paths, the row-to-column
  copy of an ordered GROUP BY answer written to Parquet, and the recompressing byte copy. libzpq leaves its host's C
  heap settings alone, so freed blocks can stay resident in a libzpq host.
- Arithmetic over a column that holds NULLs is refused (`nullable values are not supported`).
- Windowed filter and re-encode writes omit page indexes; readers fall back to row-group pruning.
- A plain Parquet write with `--limit` refuses nested leaves and INT96.

## 0.4.1

**Fixes incorrect results from 0.4.0. Upgrade if you use `zpq query --query` with `NOT`.**

zpq 0.4.0 could return incorrect results, with no error, for a SQL `WHERE` clause (or an aggregate `FILTER (WHERE ...)`)
that applies `NOT` to a parenthesized `AND`/`OR`. For example, `WHERE NOT (x > 5 AND y < 3)` was evaluated as
`WHERE (NOT x > 5) AND y < 3`. The `--filter` flag was not affected, and neither was SQL without `NOT` over a group.

What to do: upgrade to 0.4.1, and re-run any 0.4.0 `--query` whose `WHERE` or `FILTER` applies `NOT` to a parenthesized
group. Until you can upgrade, rewrite such a predicate without the outer `NOT` (`NOT (a AND b)` is `NOT a OR NOT b`;
`NOT (a OR b)` is `NOT a AND NOT b`).

In 0.4.1, `NOT` is pushed down to the comparisons, keeping SQL's NULL semantics. Since the filter grammar is a flat OR
of ANDs, a predicate that still needs grouping after that (e.g. `NOT ((a AND b) OR c)`) is refused with an error
instead of evaluated differently.

0.4.1 also refuses SQL conditions that 0.4.0 misread and could answer incorrectly. Each condition must now be
`column op literal`, `LIKE` (without `ESCAPE`), `IS [NOT] NULL`, or `IN`/`BETWEEN` over literals. Refused are: `NULL`
as a value (0.4.0 compared against the string `'NULL'`, so `s NOT IN ('a', NULL)` returned rows where SQL returns
none), another column, an expression, `CAST` or `COLLATE` as a value (0.4.0 compared against their text), and a string
literal or column name containing a quote, ` AND `, ` OR ` or ` BETWEEN ` (or a comma, inside an `IN` list).
`x NOT BETWEEN lo AND hi` now works in SQL, and `a OR (b OR c)` is accepted instead of refused.
