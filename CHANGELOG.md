# Changelog

## 0.5.0 (unreleased)

**Read "What to do" first if you script zpq or rely on its answers.** 0.5.0 changes what `zpq query` prints, fixes
answers that 0.4.1 got incorrect without an error, and turns `--max-memory` into a real ceiling. It adds a C library
(libzpq) with a Python binding, `zpq inspect`, a much larger SQL surface, Bloom filters, profiling, and Homebrew,
shell-completion and man-page packaging.

### What to do

1. **Scripts that parse `zpq query` output must change.** Every query now prints its answer as NDJSON rows on stdout,
   and nothing else. The `{"ok":true,...,"agg":...}` summary that 0.4.1 printed is gone from stdout; take the rows
   instead, or add `--summary=json` to get a typed summary line on **stderr**. `docs/output-format.md` (shipped in
   the release archive) is the contract for the shapes and for how each type prints.
2. **BREAKING: `avg(x)` is now a plain number, in every format.** 0.4.1 answered it as a `{"sum":..,"count":..}`
   object in JSON (a DECIMAL one with an `"avg"` field too) and as `<name>__sum` / `<name>__count` columns in CSV and
   Parquet; the Lambda, libzpq and Python answers carried the same object. It is now one value, NULL over no values:
   - a DOUBLE for integer and floating `x`, the sum divided once by the count;
   - for DECIMAL(p, s), DECIMAL(q, s + q - p) with q = min(38, p + 4): the exact mean rounded half away from zero,
     with up to four more fractional digits and never fewer integer digits (DECIMAL(15,2) gives DECIMAL(19,6),
     DECIMAL(36,35) gives DECIMAL(38,37), and p = 38 keeps the input type, as Trino does). It never overflows.

   If you merge averages across files or Lambda fan-out, ask for **`avg_state(x)`** instead: it answers exactly what
   `avg(x)` answered in 0.4.1 (minus the DECIMAL `"avg"` field), and `sum(<name>__sum) / sum(<name>__count)` over its
   per-file results is the exact overall average. A query that keeps `avg(x)` and parses `__sum` / `__count` or the
   object breaks.
3. **If you read zpq's CSV, check dates and empty strings.** `--format csv` works as in 0.4.1, but DATE, TIME and
   TIMESTAMP now print as ISO text (0.4.1 printed integers), and an empty string prints as `""` so it differs from
   NULL (0.4.1 printed both as an empty field). Aggregate and GROUP BY answers can now be CSV too.
4. **Re-run the queries listed under "Answers that changed"** if you relied on their 0.4.1 results: DECIMAL sums and
   filters, UUID filters, `NOT IN` lists containing `NULL`, `LIKE NULL` and `LIKE` with `_`, `coalesce`, integer
   arithmetic near the 64-bit limit, INT96 timestamps, float filters and min/max over columns that hold NaN, BOOLEAN
   GROUP BY keys, and any Parquet
   file 0.4.1 **rewrote** from a fixed-width binary (UUID), FLOAT16 or BYTE_ARRAY DECIMAL column, or from inputs with
   different DECIMAL scales.
5. **Check `--max-memory` values you pass.** It used to bound GROUP BY entries only; it now bounds everything an
   aggregate or a write holds. A query that answered at a given value can now refuse instead, with
   `the query needs more memory than --max-memory allows` (exit 1). Raise the value, or leave it unset.
6. **Expect GROUP BY rows in a different order.** Groups now come in the order they first appear in the input, at
   every `-j`; 0.4.1 sorted them by their serialized key bytes. Add `ORDER BY` if you need an order.
7. **Invalid queries exit 2, not 1.** Read, decode and write failures stay 1; a closed stdout pipe is 141. An
   aggregate or a GROUP BY key over a list's elements (a repeated leaf) is now refused while planning, exit 2, with a
   sentence naming the column (`aggregate argument column `l.list.element` is repeated`), as a filter on one already
   was; rc1 decoded the column first and exited 1 with a bare `UnsupportedAggType` or a GROUP BY message.
8. **BREAKING for summary readers: `rows_kept` is gone.** It meant rows in the row groups read for an aggregate, but
   rows written for a write, so a filtered aggregate reported every row of the row groups it read as kept. The
   `--summary=json` line, and the Lambda's and libzpq's responses, now carry `rows_in` (every input row),
   `rows_scanned` (rows in the row groups read) and `rows_matched` (rows the filter kept: what an aggregate folds, a
   write writes, a row output prints before ORDER BY and LIMIT), beside `rows` (rows in the answer). The Lambda write
   response's `rows_kept` is `rows_matched`. A row output's `rows_in` now counts every input row, not just the row
   groups reached before a LIMIT.
9. **BREAKING for summary readers: `phase` is now `wall_ms` and `worker_ms`.** `wall_ms` holds wall-clock phases
   (`open`, `parse`, `scan`, `footer`; a Lambda write adds `mp_await`, `mp_complete`), which run one after another.
   `worker_ms` holds each scan step summed over every worker (`decode`, `filter`, `fold` for aggregates, `encode` for
   writes, `sink`), so with N workers one can reach N times `wall_ms.scan`: a `decode_ms` many times `total_ms` in
   rc1 was many workers decoding at once, not a slow decode. `wall_ms.scan` is now measured for writes and row output
   too. The field names lost their `_ms` suffix inside the objects. `docs/output-format.md` defines every field.

### Highlights

- **Answers on stdout, ready for Unix tools.** NDJSON rows by default for every query kind, `--format json` for an
  array, CSV, Parquet to a file or to stdout. Local outputs are published atomically.
- **Exact by default.** DECIMAL computes in scaled 128-bit integers, integer arithmetic fails instead of wrapping, and
  every aggregate (float sums included) gives the same bits at every `-j` and on every run. Floats compare in one
  total order on every path, filters and pruning included, so NaN rows are never dropped. From Python, DECIMAL values
  arrive as `decimal.Decimal`.
- **A memory ceiling that holds.** `--max-memory` is one budget per query that tables, decoding, read-ahead, Top-N, the
  merge, the answer and write buffers all draw from; a query that does not fit is retried on narrower paths and
  otherwise refused, never killed.
- **Faster GROUP BY**, from compact group state, cached group lookups and private partition runs for
  high-cardinality keys.
- **libzpq**: the engine as a C library with a Python binding (`python/zpq.py`) and an HTTP service. SQL through it is
  EXPERIMENTAL.
- **SQL**: `ORDER BY`, `LIMIT`/`OFFSET`, `HAVING`, `count(DISTINCT ...)`, `CASE`, `strlen`, GROUP BY by expression,
  position or alias, BOOLEAN columns as predicates, and parenthesized filters with `NOT`.
- **`zpq inspect`** reports footer metadata without decoding data pages.
- **Lossless JSON** for binary, UUID, INTERVAL, BSON and GEOMETRY values, and typed BOOLEAN answers.
- **Bloom filters** read, written and used to prune `=` and `IN`.
- **Profiling** with `--profile` (a Perfetto trace) and `--profile-summary`.

### Performance

**How these were measured.** Cold, as a user meets a file the first time. Every query starts a fresh process of each
engine. Before each of 3 repetitions, the input's pages are evicted from the page cache and the eviction is verified.
A case's time is the median of its repetitions whose answer matched pinned truth and whose environment check passed.
Times include process start and the cold reads.

| | |
|---|---|
| Run | `scorecard-v7-073dbf2`, zpq 0.5.0 build 073dbf2 |
| Machine | [redacted], Linux 7.0; every engine pinned to the same 8 cores (16 threads) and capped at 32 GiB |
| Engines | DuckDB 2.0.0-alpha (v2.0.0-alpha46057, b15358c371; CLI), DuckDB 1.5.5 (Python), Polars 2.0.0, DataFusion CLI 55.2.0 |
| Coverage | 6,799 of 6,960 cells finished; 92 have no valid timing because the environment check rejected them, not because an engine failed |

The run has no zpq 0.4.1 lane. The comparison with 0.4.1 further down is from an earlier scorecard.

- **ClickBench, 100M rows (one 14.8 GB file).**
  - zpq runs 34 of the 43 queries. It refuses the other 9 before reading data: date literals against hits'
    unannotated day column, `extract`/`DATE_TRUNC`, and `REGEXP_REPLACE` are not 0.5.0 SQL.
  - Of the 34, zpq is the fastest of the five engines on 24, at a geometric mean of 0.72× the fastest other engine's
    time.
  - All 34 are within 3× of the fastest other engine, and one is over 2× (Q35: 938 ms against 439 ms for DuckDB 2.0).
  - Against DuckDB 2.0 alone, zpq is faster on 27 of 34, at a geometric mean of 0.52× its time.

- **26 synthetic datasets × 15 query shapes, 390 cases.**
  - zpq is fastest on 248 of 390, at a geometric mean of 0.88× the fastest other engine.
  - None is over 5× behind (the worst is 3.82×); 35 are over 2× and 10 over 3×.
  - The cases that trail most are full GROUP BY and top-by-sum over near-unique keys. For example, a full GROUP BY on
    sstr10m takes 2.95 s against 771 ms for DuckDB 2.0.
  - Against the previous cold scorecard of the same method (v5, build 94ed895): over 3× behind went from 15 to 10,
    over 2× from 42 to 35, fastest from 237 to 248, and the geomean from 0.92 to 0.88.

- **Local reads** (TPC-H SF1 lineitem in snappy and zstd, and a 2.1M-row synthetic table): fastest on 14 of 14, at a
  geometric mean of 0.47× the fastest other engine.
- **Writes** (outputs to tmpfs for every engine): fastest on 8 of 8, at a geometric mean of 0.47×. For example, a
  full zstd rewrite of the 2.1M-row table takes 164 ms, against 337 ms for Polars 2.0.
- **DuckDB's votes test** (228M rows, local file): 134 ms, against 219 ms for DuckDB 2.0.
- **PDS-H Q1 and Q6** (SF1 to SF30): fastest on 4 of 8, at a geometric mean of 0.92×. Q6 leads; Q1 trails by at
  most 1.4×.

**Against zpq 0.4.1: writes and peak memory.** These come from the earlier scorecard `scorecard-v2-a88915d3b494`:
0.5.0 build a88915d (from before the memory work) against the released 0.4.1, on the same machine in the same session.
Each run was a fresh process, median of 3, with zstd level 3 output.
- Across eight rewrite jobs, 0.5.0 takes 0.33× 0.4.1's time (geometric mean). The range is 0.89× for a full zstd
  rewrite of the 2.1M-row table to 0.13× for a gzip-to-zstd rewrite of NYC taxi.
- Output sizes differ by at most 1.1%.
- At the default budget, peak memory is higher on 6 of the 8 jobs, because 0.5.0 encodes several row groups at once
  in its window (for example 176 → 571 MiB for that full rewrite). It is lower on 2 (1631 → 1155 MiB and
  1054 → 728 MiB).

**AWS Lambda, cold starts.**

How these were measured:
- **Builds:** zpq's final stripped Lambda builds against native builds of the other engines, in the same session on
  the same architecture.
- **Setup:** us-west-2, 3008 MB, default network quota.
- **Engines:** DataFusion 55.2.0 (Rust; 32 partitions, page index and pushdown off), Polars 2.0.0 (native) and DuckDB
  1.5.6 (native container image; 16 threads, file cache on).
- **Scenarios:** the same 11 as earlier runs, with answers checked against reference results. Six read a 174.8 MB
  synthetic file, four read TPC-H SF1 lineitem (207.1 MB), and one filters the Overture places file and writes the
  result to S3. The Overture outputs were read back with PyArrow and DuckDB.
- **Metric:** each cell is 5 forced cold starts (the function's configuration is changed before each round). The
  figure compared is the median Init + median Duration per call.

Results:
- **arm64 (Graviton):** zpq is faster than DataFusion in 11 of 11 scenarios, Polars in 10 of 11 and DuckDB in 11 of
  11. The loss is the full scan of the 174.8 MB file (B1): 777 ms against Polars' 729 ms.
- **x86_64:** zpq is faster than all three in 11 of 11 scenarios.
- **Against an earlier 0.5.0 build** (d2f4fa1, 2026-10-09, same session): zpq is at or below it on 9 of 11 scenarios
  on each architecture.
- **Init:** median 30.8 ms on arm64 and 40.3 ms on x86_64, up from 23.0 and 26.0 ms for d2f4fa1. Most of the
  difference is the S3 pre-connect, which now runs during Init. DataFusion's Init is 149.8 ms, Polars' 74.5 ms and
  DuckDB's 218.7 ms on arm64.
- **B1, the trade-off:** zpq now holds much less memory there. On arm64 it takes 777 ms at a 114 MB peak, against
  670 ms at 269 MB for d2f4fa1. On x86_64, 771 ms at 129 MB against 722 ms at 271 MB.

From an earlier build (de39261, AWS run B): Bloom-filter point lookups were 2.2–4.6× faster than zpq 0.4.0.

What changed underneath:

- Aggregates run over dictionary codes where they can; `IN` lists bind once to a set suited to their values; filters
  compare 64 rows at a time.
- A rule-based planner picks among grouping strategies from footer and dictionary facts only, and `--explain` shows
  the facts, the candidates and the rule that decided. The strategies are plain tables, shared partitions, exact count
  cuts for `ORDER BY count(*) DESC LIMIT n`, and a dense fold over a dictionary domain. Every strategy is exact.
- GROUP BY state is stored at the width each reduction needs, and group lookups cache their hashes. A count over
  long, near-unique composite keys can move each worker's groups to private partition runs, funded from the query's
  memory budget, instead of one shared table.
- `ORDER BY ... LIMIT n` over plain rows ranks by its keys in parallel and reads only the winners' rows, skipping row
  groups that cannot win.
- Local reads on Linux can bypass the page cache (io_uring with `O_DIRECT`, or `pread`) for cold files, reading only
  the column chunks a query needs.
- GZIP pages inflate with libdeflate; LZ4_RAW pages are written with liblz4; zstd's literal Huffman coding uses
  AVX-512 where the CPU has it, writing the same bytes.
- Writes filter first, then decode and encode one column at a time. They re-encode dictionary columns from their codes
  and recompress pages in parallel.

### Answers that changed: incorrect in 0.4.1, fixed in 0.5.0

Each of these returned an incorrect answer in 0.4.1 with exit 0 and no error. Where the "run" note says so, the 0.4.1
answer below was reproduced with the 0.4.1 binary.

| What was incorrect in 0.4.1 | Queries affected | What to do |
|---|---|---|
| DECIMAL `sum`/`avg`/`min`/`max` and DECIMAL arithmetic were computed in 64-bit floating point, so large sums could be off in the last digits (a TPC-H Q1 sum was a cent off at SF30). | Aggregates or `--select` arithmetic over DECIMAL columns | Re-run them. Results now print every digit of the scale (`3.00`, where 0.4.1 printed `3`). |
| A DECIMAL filter compared through a double, so values past about 15 significant digits matched their neighbours: `d = 9007199254740993` also matched `...992`. | Filters on DECIMAL columns with more than 15 digits | Re-run them. |
| A UUID literal never matched a UUID column: `u = '00010203-...'` returned no rows. | Filters on UUID columns | Re-run them. |
| `s NOT IN ('a', NULL)` in `--filter` compared with the text `'NULL'` and kept rows that SQL keeps none of. | `--filter` with `NULL` in an `IN` / `NOT IN` list (0.4.1's SQL refused it) | Re-run them. |
| `s LIKE NULL` in `--filter` matched rows holding the text `'NULL'`, and `s NOT LIKE NULL` every other non-NULL row; both now keep no row, as in SQL. | `--filter` with `LIKE NULL` | Re-run them. |
| A `--filter` string comparison whose right side was a column (`s = t`) compared with the text `t`. It is now refused (exit 2). | `--filter` comparing two columns | Compare columns in SQL `--select`/`CASE`, or filter on literals. |
| `LIKE` `_` matched one byte, not one character, so `'_'` missed `'é'`; and `%` facing a literal `%` in the value stopped matching (`'a%b' LIKE 'a%_b'` was false). | `LIKE` patterns with `_` over non-ASCII text, or over values containing `%` | Re-run them. |
| `coalesce(a, b)` gave 0 (or `""`) where both were NULL, so `count`, `min`, `avg` and GROUP BY saw a phantom value; and a UINT32 of 2^31 or more came out negative. | `coalesce` over nullable columns or UINT32 columns | Re-run them. |
| Integer `+ - * /` wrapped silently on overflow: `sum(a * 4)` with `a = 2^62` answered 4. It now fails with `integer overflow in *` (exit 1). | Integer arithmetic near the 64-bit limit | Cast to DECIMAL, or check the inputs' range. |
| Float `min`/`max` over a column of only NaN answered `Infinity`/`-Infinity`. | Float min/max | See also the NaN ordering change under "Behaviour changes". |
| `--trust-stats` float `min`/`max` of a chunk holding zero took its sign from the statistics: `min` of `[0.0, 5.0]` answered `-0`. | `--trust-stats` with float columns | Re-run them. |
| Float filters dropped NaN rows. `x > 5` and `x >= 5` left NaN out, though ORDER BY and `max` already put NaN above every number. `NOT (x < 5)` was rewritten to `x >= 5`, and so dropped NaN rows under either reading. `x = nan` matched nothing, and `IN (nan, ...)` ignored its NaN member. On files with no NaN counts, `x >= c` also skipped row groups whose max was below `c`, along with the NaN rows they held. | Filters and SQL `WHERE`/`FILTER`/`CASE WHEN` on float columns that hold NaN | Re-run them; see "Floats in filters" below. |
| A FLOAT literal past the 32-bit float range rounded to infinity: `f > 3.5e38` missed a stored `+inf`, and `f = 3.5e38` matched it. | Filters on FLOAT (32-bit) columns with very large literals | Re-run them. |
| A BOOLEAN GROUP BY key, and `min`/`max` of a BOOLEAN, printed `0`/`1` where rows print `false`/`true`. | GROUP BY or min/max over BOOLEAN columns, read as JSON | Compare with `true`/`false`. Parquet output was already BOOLEAN. |
| INT96 timestamps outside 1677-2262 wrapped into another instant: the 9999-12-31 sentinel read as a negative number. They now print the exact count of nanoseconds since the epoch. | INT96 columns with far-past or far-future values | Re-run them. |
| A legacy REPEATED field without a LIST or MAP annotation printed one row per stored value. Row output and filters now refuse it, naming the column (exit 2). | Row output from files with legacy repeated fields | Copies (`-o` without a filter) still keep them. |
| Binary values that are not UTF-8 printed with U+FFFD for each such byte, so distinct values printed alike (two different GROUP BY keys looked the same); UUIDs printed their raw bytes. See "JSON output". | Rows, GROUP BY keys and min/max of binary and UUID columns | Read the new forms. |
| Two inputs whose DECIMAL scales differed were read as one schema: rows and copies took the second file at the first file's scale (`1.00` became `0.0100`). Such inputs are now refused as a schema mismatch. | Multi-file row output and copies over files with different DECIMAL scales | Rewrite those copies; query such files separately. |
| A DECIMAL carrying only the logical annotation printed in rows without its scale (`1.25` as `125`). | Row output of such columns | Re-run them. |
| `count(col)` answered from statistics trusted a negative `null_count` in a corrupt footer and counted more rows than exist. | `count(col)` over files with corrupt statistics | None; such a count now decodes the column. |
| **Rewritten files:** a fixed-width binary column (UUID, `fixed_size_binary`) re-encoded with a dictionary got a 4-byte length before each entry, so other readers returned incorrect values with no error. | Files 0.4.1 wrote with a filter, `--select` or `--codec` from inputs with such columns | **Rewrite those files with 0.5.0** from their sources. |
| **Rewritten files:** a BYTE_ARRAY DECIMAL column was re-encoded as DOUBLE, losing exactness and changing the schema. It is now written as a DECIMAL (FIXED_LEN_BYTE_ARRAY). | Files 0.4.1 rewrote from BYTE_ARRAY DECIMAL inputs | Rewrite them with 0.5.0. |
| **Rewritten files:** when the first input of a multi-file filtered write had no row groups, a LIST column was re-encoded without its repetition levels, and a FLOAT16 column was copied under a DOUBLE footer, both with exit 0. Other readers cannot read those files. | Multi-file writes whose first file is empty | Rewrite those files. |
| **Rewritten files:** float statistics wrote NaN as min and max when a chunk's first value was NaN, so readers could never prune the chunk; `total_byte_size` held the compressed size. | Files 0.4.1 wrote from float columns with NaN | Answers were not affected; rewrite to restore pruning. |
| **Rewritten files:** GEOMETRY, GEOGRAPHY and other annotations 0.4.1 did not interpret were written as a Null logical type, even in a plain copy. They are now kept. | Copies of geospatial or other annotated files | Rewrite them with 0.5.0. |
| **Encrypted Parquet** with a plaintext footer was copied with exit 0 into a file no key could decrypt. Every path now refuses encrypted input, naming the file (exit 1). | Copies of encrypted files | zpq does not decrypt. |
| A corrupted GZIP page whose deflate stream still decoded to the right length was accepted: the gzip member's CRC-32 and length were never checked. | GZIP-compressed inputs | None; a corrupted page is now an error. |

Fixed during the 0.5.0 cycle and never released, so not listed: `c IS NOT NULL AND (c = 2 OR b = 1)` dropping rows
(0.4.1 refused that shape), several grouped-aggregate paths over dictionary codes that are new in 0.5.0, a UINT64
min/max lost to a compiler miscompile in optimized builds, aliased columns re-typed on write (0.4.1 refused those
writes), and a float `sum` over no rows answering 0 instead of NULL on the dictionary-code path.

### Behaviour changes

**Errors**
- A page or column chunk that fails to decode names where, on every query kind and at every `-j`:
  `cannot decode input file data/b.parquet` / `row group 0, column `z`, page 1:` / `the page's encoded data is
  invalid (UnexpectedPage)`. rc1 printed only the error's name (`UnexpectedPage`, `DictionaryMissing`,
  `ShortDecode`, ...), so over a glob the corrupt file could not be told. The error's name stays last and is still the
  `code` of `--summary=json`'s error line. Pages count from 0 in the column chunk, a dictionary page included.

**Output**
- Every query prints only its answer: NDJSON rows on stdout, one object per line, scalar aggregates as one row.
  - The summary is opt-in with `--summary=json`, on stderr. That flag also types errors (`{"type":"error",...}`) and
    JSON profiles. Row output and writes report `row_groups_pruned` there too, as aggregates do.
  - `jsonl` remains an alias of `ndjson`.
  - `--format json` prints an array.
  - `--format parquet` writes Parquet to stdout and refuses a terminal.
- `--format csv` is RFC 4180 with a header row, now for aggregate and GROUP BY answers too (0.4.1 refused `--format`
  with `--aggregate`).
  - NULL is an empty unquoted field and the empty string is `""`.
  - BOOLEAN prints `true`/`false`, and DECIMAL every digit of its scale.
  - DATE, TIME and TIMESTAMP (INT96 included) print as ISO text with the column's own fractional digits, plus `Z`
    when adjusted to UTC.
  - Bytes follow the JSON rules, and an object (non-UTF-8 bytes, INTERVAL) is its JSON text in a quoted field.
  - An average is `<name>__sum` and `<name>__count`.
- A query with no `-o` and no aggregate prints rows; 0.4.1 required `--output`.
- A named `-o FILE` writes Parquet whatever its extension and leaves stdout empty; `-o -` writes stdout (NDJSON by
  default).
  - Local outputs go to a sibling temporary file that is renamed into place, so a failure leaves the old file.
  - An output that is also an input (hard links included) is refused.
  - Overwrites keep the file's mode, and a symlink at the destination is replaced itself.
- `--limit` and SQL `LIMIT` cap the final rows for every format and destination; giving both is an error. `LIMIT 0`
  checks the query and schema without decoding pages. A CSV header row does not count.
- An output column is named by its alias's text: `AS "n m"` names it `n m` and `AS "a""b"` names it `a"b`, with
  surrounding spaces kept.
- Exit codes: 0 success, 1 read/decode/write failure, 2 invalid or unsupported query or option (0.4.1: 1), 141 when
  the reader of stdout closes it. A disk-full flush is 1.

**Answers**
- GROUP BY lists groups in first-appearance order (files as given, then row groups, then rows) at every `-j`; 0.4.1
  used the byte order of their serialized keys. `--unordered` (Lambda: `"ordered": false`) gives up the order, and
  bit-identical float sums, for speed.
- Float sums and averages are summed in one fixed order, so they are the same at every `-j`, budget and decoder; their
  last bits can differ from 0.4.1's.
- Float `min`/`max` order NaN above every number: `max` is NaN if any value is NaN (0.4.1 skipped NaN: `max` of
  `[1, NaN, 2]` was 2, now NaN), `min` skips NaN unless every value is NaN. Of `-0` and `+0`, which compare equal,
  `min` returns `-0` and `max` `+0`.
- DECIMAL results are DECIMAL, with fixed result-type rules:
  - `+`/`-` take the larger scale and one more digit, and `*` adds scales;
  - `sum` is DECIMAL(38, s), `min`/`max` keep the column's type, and `avg` widens it by up to four fractional digits
    (see "What to do"), rounding half away from zero;
  - numeric literals with a decimal point are DECIMAL, and `/` is DOUBLE;
  - a result past 38 digits is an error.

  `-o` writes DECIMAL columns where 0.4.1 wrote DOUBLE, `avg_state`'s `__sum` included.
- BOOLEAN answers are BOOLEAN everywhere (`true`/`false` in JSON and CSV, BOOLEAN in Parquet). That covers keys,
  min/max, `SELECT b AS k` and `CASE ... THEN b END`.
- `SELECT c AS k` writes the same column, renamed: same type, same nullability.
- Integer overflow in `+ - * /` fails the query (`integer overflow in *`, exit 1) instead of wrapping, but only on a
  row the query consumes.
- Arithmetic over a NULL is NULL, as in SQL. `x * 2` over a nullable column gives NULL where `x` is NULL, and the
  output column is nullable. 0.4.1 refused such queries with `nullable values are not supported` (exit 1), and could
  do so after rows had already streamed. NULL rows never raise an overflow or a division by zero. Arithmetic over
  nested columns, and over UINT64, stays refused.
- A count past 2^63 across files is an error (`RowCountTooLarge`), where 0.4.1's unchecked add wrapped.

**Floats in filters**

Every path compares floats in one total order. NaN equals NaN and sorts above every number, `+inf` included, and `-0`
equals `+0`. So `x > c` and `x >= c` keep NaN rows, `x < c` does not, `NOT` is the exact complement, and `x = nan`
and `IN (nan, ...)` match NaN rows. ORDER BY, `min`/`max`, GROUP BY and `count(DISTINCT)` already used this order.

Parquet's float min and max leave NaN out. A row group whose max is below `c` can therefore still hold rows with
`x > c`, and only the chunk's NaN count can rule them out. zpq is exact by default: it skips a float row group or page
for `>`/`>=` only when it is proven free of NaN, by one of these:
- a NaN count of zero, in the chunk's statistics or in the page index (a file written with IEEE_754_TOTAL_ORDER always
  has one);
- a dictionary that holds no NaN;
- zpq's NaN probe, which reads the chunk's values first. It runs for row output, writes and S3 inputs, and for
  uncompressed chunks in aggregates.

On files with no NaN counts (most PyArrow- and DuckDB-written files), such filters can therefore skip fewer row
groups than in 0.4.1. Files zpq writes carry the counts and prune as before. `--trust-stats` now also reads a missing
NaN count as zero, which restores 0.4.1's pruning, and its dropped NaN rows, on request.

The NaN page ({{URL: published nan.html}}) shows the problem on a real file and why zpq stays exact by default. On
the same file, DuckDB 1.5.5 and DataFusion drop the NaN rows when they prune.

A FLOAT literal outside the 32-bit range compares exactly against a FLOAT column (`f > 3.5e38` matches `+inf`). A
literal inside the range is first rounded to 32 bits, as a cast would be, so `f = 0.1` matches the stored 32-bit 0.1.

**Python (libzpq), for anyone on a 0.5.0 preview build.** `Zpq.query()` rows and `Zpq.run()`'s `agg` give DECIMAL
columns as `decimal.Decimal`, every digit and the scale kept, a scale-0 DECIMAL included. Other numbers are as JSON
reads them. Pass `decimal=False` to get floats. SQL and structured-aggregate responses carry `"decimal": [names]`, so a
client of the C ABI can do the same.

**Refusals that were answers** (each names the column or file)
- Row output and filters refuse repeated columns (LIST, MAP, legacy REPEATED), exit 2; filters refuse them when the
  query is parsed. Copies keep them.
- A `--filter` string comparison must have a quoted literal (or a bare word that names no column) on its right.
- A STRING, ENUM or JSON value that is not UTF-8 is an error (exit 1); the file is invalid.
- Encrypted Parquet, on every path (exit 1).
- Inputs whose DECIMAL scales differ.
- A FIFO or a directory given as an input.
- Files that break the format:
  - an RLE run value wider than its bit width;
  - a FIXED_LEN_BYTE_ARRAY of no length, or a DELTA_BYTE_ARRAY value of another length than its FLBA column;
  - an INT32 past its 8- or 16-bit integer type;
  - a level past its column's maximum, or a level run claiming bytes past its stream;
  - a second dictionary page in a chunk.
- A local input truncated while a query reads it is an error (`InputChanged`), not a crash.

**Accepted now**
- Inputs from different writers whose column annotations mean the same thing (a legacy `UTF8` beside a STRING
  annotation, Thrift fields in another order) copy and re-encode together; 0.4.1 refused them with
  `schema mismatch across inputs`. Different types are still refused.
- INT96 values in any year.
- Float bounds under the IEEE_754_TOTAL_ORDER column order are read and used for pruning. 0.4.1, like 0.4.0, turned
  pruning off for a column order it did not know.
- `-o` of `min`/`max` over a UINT64 column holding 2^63 or more writes UINT64; 0.4.1 refused it with `AggIntTooWide`.
- A page whose V2 header miscounts its nulls is read by its levels, as other readers do.

**Writes**
- With a `--max-memory` that binds, a filtered or computed write may emit one output row group per slice: contents
  are identical, row-group boundaries depend on `--max-memory` and `-j`.
- Written float statistics leave NaN out of min/max and write zero bounds as `-0.0`/`+0.0`. Every FLOAT and DOUBLE
  chunk zpq encodes gets a `nan_count`, and its page index gets per-page `nan_counts`, so readers can prune it
  exactly. `total_byte_size` is the uncompressed size. Byte-copied chunks keep their source's statistics, and no NaN
  count is invented for them.
- A file whose column data zpq encodes (any filter, `--select`, computed columns, row output or an aggregate written as
  Parquet) is `created_by` `zpq version 0.5.0`. 0.4.1 kept the first input's string, as plain copies and codec-only
  recompressions still do. In such a file, a byte-copied chunk drops a deprecated min/max pair that disagrees with its
  `min_value`/`max_value`, because readers key workarounds for that pair on `created_by`.
- Re-encoded strings choose dictionary (a quarter or fewer distinct and within 1 MiB), DELTA_BYTE_ARRAY (long shared
  prefixes) or DELTA_LENGTH_BYTE_ARRAY; FIXED_LEN_BYTE_ARRAY falls back to PLAIN.
- `--compress-priority size|speed`; `size` (level 3) is the default everywhere.

**Lambda and S3**
- Every S3 wait has a deadline, and a stalled request is retried on a new connection. 0.4.0 and 0.4.1 could hang
  until the Lambda timed out. The defaults are below; the `ZPQ_S3_*_TIMEOUT_MS` variables override them.
  - connect 5 s, TLS handshake 5 s, first byte 10 s, idle 10 s;
  - a request in all 30 s;
  - CompleteMultipartUpload 15 min.
- A CreateMultipartUpload or CompleteMultipartUpload that could not connect is retried. A control-plane failure names
  the request, the point it failed, the attempts and the S3 error code (`"s3"` in the Lambda's error JSON).
- An inline answer (SQL rows, GROUP BY rows) is capped by the request's `max_result_bytes`: by default the larger of the
  query's budget and 1 GiB, and just under the 6 MB Lambda limit on Lambda. Past it the response is a
  `result_too_large` error naming the cap.
- The Lambda stops S3 work a second before its invocation deadline and reports through `/error` instead of being
  killed.
- The Lambda pre-connects to the regional S3 endpoint during init and fetches a footer with one suffix GET.
- Request strictness: a NUL byte in a path, header-breaking bytes in an S3 URL, an unknown `output_codec`, a size of
  `0MB` and a `max_memory` of 0 are refused.
- On Lambda, 8 S3 data requests are kept in flight on arm64 (Graviton) and 16 on x86_64. Each connection's TLS
  stream costs CPU, and a 3008 MB Graviton sandbox has two vCPUs: there, 16 at once finished cold scans later. Only
  that memory size was measured, so the default goes by architecture, not by vCPU count; `ZPQ_S3_IN_FLIGHT`
  overrides it.
- S3 fetch workers waiting for a slot sleep until one frees instead of polling, and an aggregate whose input is
  fetched through the read-ahead window takes full batches from its first fetch. Only a read that may stop early (a
  print with `--limit`) still starts small.
- On macOS, a pooled S3 connection the server has reset is retried at once on a fresh connection, without backoff or
  charging the retry budget, as on Linux. Before, macOS counted it as a failed read.

### Memory ceiling: `--max-memory`

`--max-memory` (the Lambda's and libzpq's `max_memory`) is one budget per query, and it does not depend on `-j`.
Give it as bytes, or as a number (fractions allowed) followed by `K`, `M`, `G` or `T`, with or without a `B`, in any
case, in powers of 1024: `--max-memory 2GB`, `1.5g`, `1073741824`. A space, an exponent or a `MiB` spelling is
refused (exit 2). It is now in the help's usage synopsis, with its syntax, default and an example.
Everything an aggregate, a Top-N or a write holds at once draws from it **before** allocating:
- GROUP BY tables and `count(DISTINCT)` sets, which draw in proportion to their size;
- each worker's decoded row groups, sliced string dictionaries, and a local reader's read-ahead;
- the S3 footer;
- a Top-N's heaps, kept rows, gather and ranking decode;
- the merge, the answer and its encoding;
- scalar string `min`/`max` winners;
- a write's encoded output in flight.

In 0.4.1 it bounded GROUP BY entries only.

**Outside the budget** is a fixed slack: the binary, thread stacks, the mapped input, allocator bookkeeping, each scan
worker's first 2 MiB of decode memory, 16 MiB for a GROUP BY's merge and answer past its tables, and 32 MiB for one
decode (the last retry below, or a write's row group sliced on its writer).

**Default:**
- On Linux, 80% of the memory reported available, but no more than three quarters of a cgroup v2 `memory.max`.
- On Lambda, three quarters of the function's memory size.
- On macOS, half of physical memory.
- 512 MB where none of these can be read.

Under a cgroup limit, the default answers the queries it answered before 0.5.0's ceiling, and some that the cgroup
used to kill.

**When a query does not fit**, a GROUP BY or aggregate scan is run again from the start, each time on a narrower path,
and the answer is the same whichever path answers:
1. the plan the planner chose;
2. without private partition runs, if the refused scan used them;
3. without each fast path the refused scan used (dense fold, spill, count(DISTINCT) pairs, cost-chosen shared
   partitions); a plain grouping chosen in place of an exact count cut gives way to that cut;
4. where several workers' tables were refused and shared partitions can fit: forced shared partitions, folding a
   partition's groups at a time;
5. on one worker, where one worker may fit, or where decode or merge memory was refused;
6. on one worker, with decoding on its own 32 MiB budget (row groups past half of it are decoded in slices), where
   decode memory was refused;
7. otherwise the query fails, exit 1, with:

   ```
   zpq query: the query needs more memory than --max-memory allows
   ```

   (`--summary=json`: `{"type":"error","ok":false,"code":"ExceededMemoryBudget",...}`). At most seven scans run.

A parallel Top-N that does not fit runs on the serial path, which holds its kept rows and decode in the same budget.

**Slicing large row groups.** A row group too big for a worker's share is decoded, filtered and aggregated, printed or
written a slice of rows at a time. One that cannot be sliced (a nested column) is refused with `RowGroupTooLarge`
before it decodes. A write whose row group does not fit beside its producers hands it to the writer, which slices it.

**S3 read-ahead.** S3 inputs are fetched just ahead of the readers and released behind them (Linux), in a window of a
quarter of the budget, clamped to 64 MiB–1 GiB (at most 128 MiB on Lambda; `ZPQ_S3_WINDOW_KIB` overrides it).

`--profile-summary` reports the budget's peak.

A query close to its budget can now take noticeably longer: it pays for a refused parallel scan before the narrower
rerun.

### JSON output for binary, UUID, INTERVAL, BSON, GEOMETRY and BOOLEAN

Every value printed as JSON round-trips to its stored bytes, and no two distinct values print alike:

| Column | JSON value |
|---|---|
| STRING / UTF8, ENUM, JSON | a string; a value that is not UTF-8 is an error naming the column (exit 1) |
| no annotation (BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY) | a string when the bytes are UTF-8, else `{"base64":"..."}` |
| BSON, GEOMETRY, GEOGRAPHY | always `{"base64":"..."}` |
| UUID | lowercase `8-4-4-4-12` text |
| legacy INTERVAL | `{"months":m,"days":d,"millis":ms}` |
| BOOLEAN | `true` / `false`, as a row, a GROUP BY key or a min/max |

Base64 is the standard alphabet, padded. A string never equals an object, so `{"base64":"/w=="}` (the byte `ff`) and
the text `"/w=="` stay distinct. This applies to rows, GROUP BY keys, `min`/`max`, libzpq SQL NDJSON and the Lambda's
`agg`. Filters, ordering and `min`/`max` compare the stored bytes, unsigned. A computed string (`CASE`, `||`) is text.
DECIMAL and FLOAT16 over FIXED_LEN_BYTE_ARRAY are numbers. `-o` Parquet output is unaffected.

### New SQL surface

- `ORDER BY` over aggregate results, keys, and terms the SELECT list lacks; `LIMIT` and `OFFSET`.
- `ORDER BY` over plain rows, with or without `LIMIT` (ties by input position).
- `HAVING`, comparing aggregates or keys with literals.
- `count(DISTINCT x)`, exact.
- `avg_state(x)` (also in `--aggregate`), the mergeable `{sum, count}` form of `avg(x)` for fan-out.
- GROUP BY keys as expressions (bound by value), SELECT positions or aliases; ORDER BY and HAVING on a quoted alias.
- Searched `CASE WHEN ... THEN ... [ELSE ...] END` (a branch is evaluated only on rows that take it) and
  `strlen(s)` (bytes; NULL for NULL).
- `coalesce` takes any number of arguments of any kind: columns, literals or expressions (`coalesce(x * 2, y, 0)`).
  Each argument is evaluated only on the rows every earlier one left NULL, so a fault in it counts only where it is
  the answer.
- Filters (SQL `WHERE`, `FILTER (WHERE ...)` and `--filter`):
  - grouping parentheses, and `NOT` over a group (three-valued), nested at most 32 deep;
  - `NOT BETWEEN`, `<>`, and quoted text containing keywords, with `''` for a quote;
  - a BOOLEAN column alone as a predicate (`WHERE b`, `WHERE NOT b`, `CASE WHEN b`), read as `b = true`;
  - `NULL` in an `IN` list, as a `LIKE` pattern or as a `BETWEEN` bound, read as SQL's NULL:
    `x NOT BETWEEN NULL AND 5` keeps the rows past 5.
- STRUCT and LIST columns by name:
  - in rows, `SELECT st` is refused (exit 2), naming a leaf to select instead (`st.a`);
  - with `-o`, a SELECT of bare columns in the file's order keeps a STRUCT, LIST or MAP whole, as `--columns` does.
- DECIMAL literals and exact DECIMAL arithmetic (see Behaviour changes).
- `--explain` prints the aggregate plan as JSON without reading data pages.

Not supported: `%` (refused as `operator % is not supported`), the simple `CASE x WHEN` form, joins.

### libzpq and Python (SQL is EXPERIMENTAL)

- `libzpq` exposes the Lambda's JSON request handler through seven C functions (`include/zpq.h`, ABI version 1):
  - local paths and `s3://` URLs;
  - aggregates and writes;
  - shared S3 connections and footers per context;
  - a context memory ceiling, shared by its queries in arrival order.

  Build it with `zig build libzpq -Doptimize=ReleaseFast`.
- `python/zpq.py` binds it with ctypes (no compiled extension); `python/zpq_service.py` is a small HTTP service.
- **SQL requests are EXPERIMENTAL in 0.5.0**, and their request and response shapes may change.
  - Send `{"sql": "...", "parallelism": n}`, or call `Zpq.query()` (rows as dicts) or `Zpq.query_raw()` (NDJSON
    identical to the CLI's stdout).
  - The binding has no Arrow export.
  - A SQL request cannot write a file (`output_url` is excluded); use a structured request with `output_url` to write
    Parquet.
- DECIMAL values come back from `query()` and `run()` as `decimal.Decimal` by default (`decimal=False` for floats),
  using the response's `"decimal"` list of column names.
- Also experimental: the response's `pool` and `profile` members, the Python API and the HTTP service.
- libzpq never raises or resets SIGPIPE in its host, and runs each request on a stack of its own when the caller's
  stack is short. A local input truncated while libzpq reads it raises SIGBUS in the host; write a new file and
  rename it instead.

### `zpq inspect`, completions, man page

- `zpq inspect FILE... [--format json] [--detail]` reports each file without decoding data.
  - It covers the footer, the schema, the row groups and each column chunk: codecs, encodings, sizes, statistics with
    their exactness, each float chunk's NaN count, and whether a page index or Bloom filter is present.
  - The JSON has `schema_version: 1`. Values are strings, so 64-bit and DECIMAL bounds do not round.
  - `--detail` reads bounded page, index and Bloom metadata (8 MiB).
  - A file that fails is reported in place, and the command exits 1.
- Bash, zsh and fish completions and a man page are in `packaging/`, generated from the CLI's help text. The man page
  is regenerated for the release, including `--trust-stats` and missing NaN counts.
- `zpq --version` prints the version and build revision. A release candidate carries its tag:
  `zpq 0.5.0-rc2 (<rev>)`, and the same version is in the `created_by` of files it writes, its archive names and
  `release.json`. 0.5.0-rc1 said `0.5.0` in each. `tools/package_release.py --tag v0.5.0-rc2` (or
  `zig build -Dprerelease=rc2`) builds one, and refuses a tag that does not name `build.zig.zon`'s version.
- Release archives are self-contained. Beside the binary, man page, completions and `share/zpq/tiny.parquet`, they
  ship `share/doc/zpq/`: `getting-started.md` (a filtered read, a GROUP BY with `avg`, and a computed Parquet write,
  each with its exact output against `tiny.parquet`), `output-format.md` and `binary-output.md`. Help names these
  guides, and no longer points at the README, which the archive does not carry. Packaging fails if a shipped doc
  names a file the archive lacks. The Homebrew formula installs the guides.

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
- Release binaries are stripped, which makes them much smaller, and they carry no paths from the machine that built
  them. A build is reproducible: the same commit and flags give the same bytes, whatever directory they were built
  in. The Lambda zip is written the same way every time.
  - The x86_64 Linux CLI is 8.1 MB.
  - The Lambda bootstraps are 6.0 MB (arm64) and 7.3 MB (x86_64), against 44.8 and 47.2 MB unstripped.
  - Local builds keep their symbols unless built with `-Dstrip=true`.
- macOS: the macOS build carries no Linux-only system calls, and its fetch workers wait through libSystem.
- Building from source requires Zig 0.17.0 exactly.

### Known limits

- **SQL shapes still refused:**
  - `coalesce` over BOOLEAN columns (`type mismatch in expression`, exit 1, after a CSV header);
  - `TRUE`/`FALSE` literals inside expressions, and comparisons as values (`SELECT x > 5`);
  - a quoted alias used as a GROUP BY key;
  - a fractional literal against an integer column (`x > 1.5`).
- A DOUBLE that holds an integral value prints without a fraction (`1`), so a JSON reader may take it as an integer.
- JSON prints DATE, TIME and TIMESTAMP (INT96 included) as their stored integers (days, or the column's unit since
  midnight or the epoch; INT96 as nanoseconds), where CSV prints ISO text.
- **`--max-memory` does not yet cover** the following:
  - grouped string `min`/`max` winners inside group tables;
  - first-keys, census and sketch structures;
  - the S3 fetch window, which has its own share of the budget;
  - the plain row-printing path;
  - the row-to-column copy of an ordered GROUP BY answer written to Parquet;
  - the recompressing byte copy.

  Fixed-width group tables are charged a little below what they hold at their peak, so a few large GROUP BYs answer
  slightly over the budget. libzpq leaves its host's C heap settings alone, so freed blocks can stay resident in a
  libzpq host.
- A high-cardinality GROUP BY just inside a cgroup's default budget can take many times longer than with headroom.
  It retries narrower paths before the one that fits.
- Windowed filter and re-encode writes omit page indexes, so readers fall back to row-group pruning.
- A filtered or limited Parquet write refuses nested leaves and INT96 (`INT96 re-encoding is not supported yet`); a
  plain copy keeps both.

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
