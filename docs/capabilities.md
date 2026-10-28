# What zpq can do

Moved from the README for 0.5.0: what each part of zpq reads, filters, computes, aggregates and writes.

**Read** standard Parquet — V1 + V2 data pages; PLAIN / RLE / RLE_DICTIONARY / DELTA_BINARY_PACKED /
DELTA_LENGTH_BYTE_ARRAY / DELTA_BYTE_ARRAY / BYTE_STREAM_SPLIT encodings; SNAPPY / ZSTD / GZIP / LZ4_RAW / UNCOMPRESSED
codecs, plus the deprecated LZ4 codec in each container writers used for it (parquet-mr's Hadoop framing, a bare LZ4
block, an LZ4 frame); all physical types including FIXED_LEN_BYTE_ARRAY; flat, struct, and LIST/MAP nested schemas.
Row output and filters refuse repeated columns (LIST, MAP, legacy REPEATED); copies keep them. Bytes print losslessly:
text when UTF-8, else `{"base64":...}`; UUIDs and INTERVALs in their own forms
([docs/binary-output.md](binary-output.md)).
Encrypted Parquet is refused: zpq does not decrypt.

**Filter** with `= != <> < <= > >=`, `IN` / `NOT IN`, `[NOT] BETWEEN x AND y`, `[NOT] LIKE`, `IS [NOT] NULL`, `AND` /
`OR` / `NOT` and grouping parentheses (nested at most 32 deep), plus row-group stat pruning and Hive-partition pruning
before any bytes are fetched. Keywords and parentheses inside a quoted literal are the literal's; a quote inside one is
doubled (`'O''Reilly'`), and an unterminated quote is an error. NOT is three-valued: a row whose operand is NULL
passes neither `p` nor `NOT p`. DATE / TIME / TIMESTAMP columns take quoted literals
(`'2024-01-01'`, `'2024-01-01 12:00:00.25'`, `'12:00:00'`) compared exactly: a literal finer than the column's unit is
not rounded to it.

**Project** by column name, nested-aware: `events` resolves to every `events.list.element.*` leaf.

**Compute** new columns via `--select`: arithmetic on numerics with promotion (`i32 + f64 → f64`), string concat (`||`),
`coalesce(a, b, ...)` over any expressions, `strlen(s)` (bytes; NULL for NULL), searched `CASE WHEN cond THEN v ...
[ELSE v] END`, parens, unary minus. A CASE evaluates a branch only on the rows that take it, and coalesce an argument
only on the rows every earlier one left NULL, so a fault where no row needs the value is no error; the simple form
(`CASE x WHEN ...`) is not supported. DECIMAL arithmetic is exact (see below).

**Aggregate** with `sum / count / min / max / avg / avg_state`, conditional `agg(...) FILTER (WHERE ...)`, and
`GROUP BY`. In SQL, GROUP BY keys may be expressions (bound by value, not spelling), SELECT positions or aliases (a
column of the name wins, as in DuckDB); `HAVING` compares aggregates or keys with literals (only TRUE keeps a group;
without GROUP BY the one group may be dropped); ORDER BY may rank by aggregates or keys the SELECT list lacks. Plain
rows take `ORDER BY` columns with a `LIMIT`, keeping the first rows by those keys, ties by input position.
As in SQL, `sum / min / max / avg` over no non-null values are NULL (JSON `null`, a null cell in `-o` parquet;
avg_state's pair becomes `{"sum": null, "count": 0}`) while `count` is 0.
`avg(x)` is the mean, one value in every format: DOUBLE for integer and floating `x` (the sum, its integers summed
exactly, divided once by the count), DECIMAL for a DECIMAL `x` (below). `avg_state(x)` is the same average as mergeable
state for fan-out: `{"sum": .., "count": ..}` in JSON and NDJSON, `<name>__sum` and `<name>__count` columns in CSV and
Parquet (a DOUBLE or exact DECIMAL(38, s) sum, a non-null INT64 count), so per-file answers merge exactly with
`sum(<name>__sum) / sum(<name>__count)`; you cannot average means. A double avg is the avg_state sum over its count,
bit for bit.
`-o out.parquet` writes the result as parquet: one row, or one row per group with the same columns and values as the
JSON (avg_state as `<name>__sum` / `<name>__count`).
`count(*)` is answered straight from row-group metadata; `min / max / sum` are computed by decoding the data unless you
opt into trusting file statistics — see [Statistics: trust is opt-in](statistics-trust.md). `--max-memory`
is a ceiling on what an aggregate or a write holds at once: one budget (`src/core/memory.zig`) that the GROUP BY tables,
decoded row groups, a local reader's read-ahead, the merge, the answer and its encoding, and a write's encoded output in
flight all draw from before they allocate, whatever `-j` is. A fixed slack sits outside it: the binary, thread stacks,
the mapped input, each worker's first 2 MiB of decode memory, each worker's codec state (about 0.1 MiB reading; writing,
0.2 MiB more for snappy, 0.1 MiB for gzip, 1.3 MiB for zstd, or 3.5 MiB for zstd with `--compress-priority speed`),
16 MiB for a GROUP BY's merge and answer past its tables, and a 32 MiB budget of its own for one decode, a refused
query's last retry or a write's row group sliced on its writer. A query the budget refuses runs again on narrower paths
(fast paths off, partitions, one worker, then small slices) and, if none fits, fails with an error rather than taking
more memory. It also bounds a query's working memory on every path (aggregate, print, write, byte copy), over S3 and
locally alike, so a scan larger than memory completes:
- **Fetched bytes:** S3 inputs past a quarter of it (between 64 MiB and 1 GiB; `ZPQ_S3_WINDOW_KIB` sets it) are
  fetched just ahead of the readers and released behind them (Linux; elsewhere still up front). A print with
  `--limit` fetches row group by row group and stops once it has its rows.
- **Decoded rows:** half of it is for decoded row groups, shared by the workers. A row group too big for a worker's
  share is decoded, filtered and aggregated, printed or written a slice of rows at a time, holding one page per
  column beside the slice; a slice the budget refuses is decoded again at half its rows. Slicing never changes what a
  write writes (see "Output layout" below). A row group that cannot be sliced (a nested column) and does not fit is
  refused (`RowGroupTooLarge`) before anything is decoded, rather than run out of memory.
- **Output:** a write holds at most a quarter of it encoded and waiting. A row group whose rewrite could take more (a
  codec that expands, such as `--codec uncompressed`, or one written in slices) goes straight to the output in its
  turn, a page or a slice at a time; S3 uploads keep at most an eighth of it in flight.

It is a working budget, not a hard cap: the binary, page buffers and output in flight come on top. In 0.5.0,
hash-table growth and buffer replacement in a high-cardinality GROUP BY or `count(DISTINCT)` can also go past it, by up
to about 2x near the limit; the answer stays exact, or the query refuses cleanly. For a hard ceiling, run zpq under a
cgroup `memory.max` or `ulimit -v`. Scans, filters, printed rows, writes, S3 streaming and low- to
medium-cardinality aggregation stay within the budget. Its default
(`src/core/system.zig`) is three quarters of the memory size on Lambda; on Linux, 80% of available memory, but no more
than three quarters of a cgroup v2 `memory.max`; on macOS, half of physical memory; and 512 MiB where none of these
can be read. Give it as bytes, or as a number (fractions allowed) followed by `K`, `M`, `G` or `T`, with or without a
`B`, in any case, all powers of 1024: `--max-memory 2GB`, `--max-memory 1.5g`, `--max-memory 1073741824`. A space,
an exponent (`1e9`) or a `MiB` spelling is refused (exit 2).

Diagnostic path and cache layout overrides are documented in [execution overrides](execution_knobs.md).

Add `--explain` to an aggregate query to print its plan as one JSON object and exit before scanning. It accepts SQL
`--query` and the `--aggregate` / `--group-by` flags, and reports the chosen grouping strategy, the reasons for it,
the column passes, conditional fallbacks and upper bounds on the number of distinct keys, without reading data
pages. Locally it may read dictionary pages to tighten those bounds; over S3 it reads only footers. Every strategy
is exact, and memory estimates stay estimates: execution enforces the budget. `ZPQ_NDV_UNION=0` turns off the
optional dictionary-union probe, and `ZPQ_PARTAGG=1` forces an eligible plain GROUP BY through shared tables.

**DECIMAL is exact.** DECIMAL columns, and numeric literals with a decimal point (`0.05` is DECIMAL(3,2), as in SQL),
compute in scaled 128-bit integers, never through a double. Result types follow Trino's (Athena's) rules: `a + b` /
`a - b` take the larger scale and one more integer digit, both sides first brought to their common type (the larger
scale, every integer digit either side has, at most 38 digits; a value that does not fit it is an error, as the cast
is there), `a * b` adds scales and precisions (capped at 38 digits), an integer operand counts as DECIMAL(10,0) or
DECIMAL(19,0), and a DOUBLE operand gives DOUBLE. Two places follow DuckDB instead: `/` gives DOUBLE, and an `avg`'s
sum may pass 38 digits. DuckDB agrees on every scale but keeps some results at 18 digits where Trino and zpq widen
(the product of two DECIMAL(15,2) is DECIMAL(18,4) in DuckDB, DECIMAL(30,4) here); values are the same.
`sum` is DECIMAL(38, s); `min` / `max` keep the column's type. `avg` over DECIMAL(p, s) is DECIMAL(q, s + q - p) with
q = min(38, p + 4): up to four more fractional digits and never fewer integer digits, so DataFusion's (p + 4, s + 4)
for p <= 34, the input's own type at p = 38 as in Trino, and DECIMAL(38, 37) for DECIMAL(36, 35). The mean is exact,
divided from the exact sum and rounded half away from zero once, so it always fits (it is never larger than the
largest value). The JSON prints every digit of the scale (`3344516955134.66`) and `-o` writes DECIMAL columns.
avg_state's sum is exact at any width in JSON, but `-o`'s `__sum` column is DECIMAL(38, s), so an avg_state whose sum
passes 38 digits answers in JSON and errors with `-o`; avg itself never does. A value that needs more than its type's
digits stops the query with an error rather than wrapping or rounding, but only in rows the filter keeps. Filters
compare a DECIMAL column with a literal exactly, at the column's scale. Arithmetic over a NULL is NULL, for every type,
as in SQL: `x * 2` is NULL where `x` is, aggregates skip it, and a NULL row raises no overflow or division by zero.

**Parquet version.** `--parquet-version v1|v2` chooses a value-encoding profile; v2 is the default.
v2 writes a version 2 footer, V1 data pages, PLAIN dictionary pages and RLE_DICTIONARY dictionary data pages;
it uses DELTA encodings where selected. v1 writes a version 1 footer and V1 data pages, with PLAIN or
PLAIN_DICTIONARY values and no DELTA or RLE_DICTIONARY. v1 is DuckDB-V1-like, not a strict Parquet 1.0 file:
codecs, logical annotations and optional metadata are independent. Untouched copied chunks keep their input
pages and encodings, regardless of the requested footer version. JSON requests use `"parquet_version": "v1"` or `"v2"`.

**Write** with SNAPPY / ZSTD / GZIP / LZ4_RAW / UNCOMPRESSED output (`--codec lz4` means LZ4_RAW; zpq never compresses
to the deprecated LZ4 codec, though a plain copy keeps an LZ4 chunk as it is); RLE_DICTIONARY for low-cardinality byte
arrays; DELTA_BINARY_PACKED for INT32 / INT64; for other strings DELTA_BYTE_ARRAY where values share long prefixes and
DELTA_LENGTH_BYTE_ARRAY otherwise (PLAIN for FIXED_LEN_BYTE_ARRAY, which DELTA_LENGTH_BYTE_ARRAY does not allow).
DECIMAL columns re-encode losslessly: INT32 / INT64 / FIXED_LEN_BYTE_ARRAY backings carry the unscaled integer, never
a DOUBLE. Direct projection copies preserve page indexes; re-encoded chunks get their own.

**Output layout.** A write that re-encodes (a filter, `--select` or a computed column) cuts its row groups from the
rows it writes, in order, not from the input's row groups: one closes once it holds `--row-group-size` rows (default
131,072) or its values take `--row-group-bytes` (default 64 MiB) at their PLAIN size, a fixed-width value its
width and a byte array 4 bytes plus its length. Each column chunk is split into data pages of at most 1 MiB of
values or 20,000 rows, and gets a page index (ColumnIndex and OffsetIndex, written after the row groups, column by
column). The bytes written depend only on the inputs and the options: the same file at every `-j`, on every machine.
An explicit `--max-memory` caps row groups at a sixteenth of it, so there the file can depend on that value (never
on `-j`). Holding a row group's decoded rows until it is encoded is the write's largest fixed cost: with the default
budget a machine needs a few times `--row-group-bytes` of memory, or an explicit `--max-memory`. Plain copies and row
groups a filter's statistics prove fully matching are copied as they are, keeping their source's row groups. Computed
columns are evaluated only for the rows the filter keeps.

`--codec` sets the output codec. Without it, re-encoded output is SNAPPY and a plain copy keeps each column chunk's own
codec; with it, a plain copy recompresses any chunk stored in another codec. The `codec` field of the write summary names
the codec every written chunk uses, or is `"MIXED"` when a plain copy without `--codec` keeps chunks in more than one
(for example, inputs written with different codecs).

`--compress-priority` (the Lambda's `"compress_priority"`) sets how hard zstd works. `size`, the default, compresses
every page at level 3. `speed` is an opt-in that trades size for CPU: each page may instead be stored as raw blocks
(for pages nothing compresses, such as bit-packed dictionary indices), as entropy-coded literals with no match search,
or at level 1, each a standard zstd frame every reader decodes. The form is the one that costs least when an output
byte is worth 100 cycles, its size estimated by compressing four slices spread over the page, and it is kept only if
the whole page passes three checks: the frame is within 10% plus 1 KiB of the size the sample predicted, a raw or
literals-only page has no more than that to gain from a fast full-page match search, and a raw page no more than that
from entropy coding. A page that fails is compressed at level 3 too, and the smaller frame kept. Every step depends on
the page's bytes alone, so the output is the same at every `-j` and on every run. Pages under 16 KiB are compressed at
level 3.

`speed` has no bound relative to level 3. The checks search less than level 3 does (a 512 KiB window and matches of 6
bytes or more, where level 3 looks back 1 to 2 MiB for matches of 5), so a page that compresses only through matches
they miss is stored as raw blocks: its own size plus about 0.01%. An 8 MiB page repeating 600,000 random bytes, for
example, is written in 8,388,809 bytes under `speed` and 610,006 at level 3, 13.75 times smaller.
`--profile-summary` reports the priority and, under `speed`, how many pages took each form and how many failed their
checks.

**Order** is the input's: every output that lists rows (print, `--format`, `-o` writes and byte copies, several files,
S3 and Lambda) keeps files in the order given, then row groups, then rows, at any `-j`. `GROUP BY` lists groups in the
order they first appear in the input (before 0.5.0, in the byte order of their serialized keys), and every aggregate,
floating-point sums and averages included, comes out with the same bits at every `-j` and on every run: for float
results each fixed block of row groups folds into its own table and the blocks merge in input order, which can take up
to about twice the GROUP BY memory of `--unordered` (`--max-memory` counts it). `--unordered` (the Lambda's
`"ordered": false`) gives both up for speed: groups in no set order, float aggregates free to differ in their last
bits.

**Read local files** through the page cache or explicitly: on Linux, an aggregate over files mostly not in the page
cache reads just the column chunks it needs, row group by row group, with io_uring (`O_DIRECT`) or `pread`, in a
bounded window; see [Local reads](local_io.md).

**Stream** S3-to-S3 with in-flight memory bounded independently of file size: multipart upload as the encoder
produces bytes, a sliding row-group window for re-encode backpressure, and parallel fetch across files.

```bash
zpq query data.parquet -o out.parquet \
  --filter "int8 BETWEEN -10 AND 10" \
  --select "name, qty * price AS revenue, coalesce(notes, '') AS notes"
```
