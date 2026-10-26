# ZPQ

An agentically-engineered Apache Parquet engine in Zig, optimized for serverless compute against object storage.  The
core philosophy is to do only the minimum work necessary to get the requested task finished to the desired accuracy
level.  Vendored prebuilt crypto, and native S3/SigV4 for further minimization of lambda execution time.

Workloads it's intended for:

* Iceberg / Delta table maintenance — one Lambda per file, filter and rewrite in place.
* Billing / cost roll-ups — answer from row-group statistics when possible, decode bytes only when necessary.
* Log summarization — Parquet queries without the Athena round-trip; one-line CLI output for ad-hoc work.

Two binaries from one sans-IO core: a workstation CLI (`zpq`) and an AWS Lambda bootstrap (`zpq-lambda`). Native SigV4,
vendored prebuilt BoringSSL, in-tree event loop. No AWS SDK, no system OpenSSL.

## First five minutes

Download the CLI for your OS/architecture from a published release, or build it with
`zig build cli -Doptimize=ReleaseFast` using Zig 0.17.0. `zpq --version` prints the version and build revision.
The Homebrew formula is staged in `packaging/homebrew/`; it is not a published tap yet.

New to zpq? [docs/getting-started.md](docs/getting-started.md) walks through a filtered read, a GROUP BY with `avg`
and a computed Parquet write, each with its exact output; [docs/output-format.md](docs/output-format.md) is the
contract for JSON, NDJSON and CSV answers and how each type prints.

```sh
# Answers are NDJSON rows, ready for Unix tools (jq is a separate install).
zpq query trips.parquet --columns VendorID,trip_distance --limit 5 | jq .

# Filter and project into a Parquet file; named -o leaves stdout empty.
zpq query trips.parquet --filter 'trip_distance > 10' \
  --columns VendorID,trip_distance -o selected.parquet

# Inspect footer metadata without decoding the data pages.
zpq inspect trips.parquet
zpq inspect 'daily/*.parquet' --format json | jq '.files[].row_groups'
zpq inspect trips.parquet --detail --format json

# The same paths work with configured S3 credentials and region.
zpq schema s3://my-bucket/trips.parquet
zpq inspect s3://my-bucket/trips.parquet
zpq query s3://my-bucket/trips.parquet --aggregate 'count(*) AS rows'
```

Quote a glob so zpq expands it: **one `*` in the basename**, with lexically sorted matches. Directory wildcards,
`**`, `?` and character classes are not supported by zpq. Query and inspect accept multiple explicit paths or these
globs (including S3 prefix listings); schema takes exactly one local path or S3 URL. An empty match is an error.
Multiple inspect inputs produce one `files` array in JSON, or one text report per file. A file error is retained
in that array and makes the command exit 1. See [the inspection schema](docs/inspect.md).

Python can hand filtered Parquet to DuckDB without parsing a console table:

```python
import subprocess
import duckdb

subprocess.run(["zpq", "query", "trips.parquet", "--filter", "trip_distance > 10",
                "--columns", "VendorID,trip_distance", "-o", "selected.parquet"], check=True)
rows = duckdb.sql("SELECT VendorID, sum(trip_distance) FROM 'selected.parquet' GROUP BY VendorID").fetchall()
```

This stages a file; its write/read costs are part of the hand-off. For the separately built in-process binding,
see [libzpq](docs/libzpq.md). `--format json` emits an array, and `--summary=json` opts into a typed stderr summary.
CSV is reserved. Set `pipefail` when downstream consumers must detect partial output or a closed pipe (exit 141).
Completions and a manual are in `packaging/`: source `packaging/completions/zpq.bash`, put `_zpq` on zsh's `fpath`
before `compinit`, or copy `zpq.fish` into fish's completions directory. For example, `zpq --<TAB>` lists options.
`man -l packaging/man/zpq.1` opens the manual. Regenerate these files with `python3 tools/generate_cli_docs.py`;
`--check` rejects drift from the embedded help source.

## On AWS Lambda: built for cold starts

`zpq-lambda` is a static binary for the `provided.al2023` runtime, and its cold start is the point. On 2026-10-05 in
us-west-2, with every function at 3008 MB, zpq 0.4.0's median Lambda `Init Duration` was 18–22 ms on arm64 and
26–35 ms on x86_64 across eleven scenarios (three forced cold starts each). DuckDB 1.5.6 in a Python 3.12 container
image, same region and memory, measured 448–832 ms, and its cold Init + Duration was 2.4× to 5.6× zpq's (x86_64 against
x86_64) in every scenario. Polars 1.44.2, packaged the same way, measured 478–806 ms. The full tables, inputs and
method are in [`docs/measurements/lambda_cold_start_2026-10-05.md`](docs/measurements/lambda_cold_start_2026-10-05.md).

So the intended pattern is cold, bursty and wide: one invocation per file, partition or compaction unit, fanned out
in parallel when the work arrives, with each sandbox free to be thrown away afterwards. Nothing from an earlier
invocation is needed for a correct answer, and concurrency, not sandbox reuse, is how it scales.

Keeping sandboxes warm is not the intended use: keep-warm pings, provisioned concurrency bought to hide
initialisation, or designs that count on a sandbox's connections and caches surviving between calls pay for idle
capacity to save tens of milliseconds. When Lambda reuses a sandbox on its own, zpq handles it: connections idle for
more than 4 s are closed rather than reused (they do not survive a frozen sandbox), and a reused connection found dead
is retried at once on a new one. See [`docs/lambda_capabilities.md`](docs/lambda_capabilities.md) and the request and
response format in [`docs/lambda_requests.md`](docs/lambda_requests.md).

S3 access: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` / `AWS_REGION` (on Lambda, the execution
role's). `S3_`-prefixed variables of the same names, plus `S3_ENDPOINT_URL`, take precedence and target S3-compatible
stores such as R2 or MinIO. `S3_NO_SIGN_REQUEST=1` sends requests unsigned, for buckets that allow public reads; like
signed requests, these need `AWS_REGION` or `S3_REGION`, and zpq reports a missing region as such.

Every S3 connection has deadlines, in milliseconds, each overridable from the environment:

| wait | default | variable |
|---|---:|---|
| TCP connect (per address) | 5 000 | `ZPQ_S3_CONNECT_TIMEOUT_MS` |
| TLS handshake | 5 000 | `ZPQ_S3_HANDSHAKE_TIMEOUT_MS` |
| first byte of a response, after the request is sent | 10 000 | `ZPQ_S3_FIRST_BYTE_TIMEOUT_MS` |
| no byte received or sent mid-transfer | 10 000 | `ZPQ_S3_IDLE_TIMEOUT_MS` |
| a request and its response in all, at least | 30 000 | `ZPQ_S3_REQUEST_TIMEOUT_MS` |
| the slowest transfer let run, in KiB/s (0: none is bounded by size) | 64 | `ZPQ_S3_MIN_RATE_KIBS` |
| CompleteMultipartUpload in all | 900 000 | `ZPQ_S3_COMPLETE_TIMEOUT_MS` |

The idle timeout restarts at every byte, so a transfer that keeps moving is cut off only by the whole-request
bound: the longer of 30 s and its bytes at the minimum rate, which a server trickling a byte at a time cannot outlast.
On a link slower than 64 KiB/s, lower the rate. CompleteMultipartUpload has its own bound instead: S3 can take
minutes over a large object, sending whitespace meanwhile, which keeps the idle timeout from firing. A request that
times out is sent again on a new connection when that is safe (GETs, HEADs, object and part PUTs). The retries are
charged to its usual retry budget, so a server that keeps stalling fails the query with the timeout instead of
hanging it. Creating and completing a multipart upload are never sent twice; one that could not connect (DNS, TCP
or the TLS handshake failed, so nothing was sent) is tried again on a new connection within the same budget. When
one of them fails, the error names the request, whether it failed while connecting or after it was sent, and the HTTP
status and S3 error code it was answered with: on stderr from the CLI, and in the Lambda's error JSON as `"s3"`.

An upload that fails part-way is aborted, but an abort can fail too, and a Create whose answer was lost may have
started an upload zpq never hears of. Give the output bucket a lifecycle rule with `AbortIncompleteMultipartUpload`
(a day or so after initiation) so such uploads, and the storage their parts hold, are cleaned up whatever happened.

## Performance

Numbers and trade-offs are dated and contextualised as they're captured in [`docs/measurements/`](docs/measurements/);
the latest cold-start figures are in
[`docs/measurements/lambda_cold_start_2026-10-05.md`](docs/measurements/lambda_cold_start_2026-10-05.md).

<!-- 0.5.0 release figures go here once the release benchmark run is reviewed. -->

## What it can do

**Read** standard Parquet — V1 + V2 data pages; PLAIN / RLE / RLE_DICTIONARY / DELTA_BINARY_PACKED /
DELTA_LENGTH_BYTE_ARRAY / DELTA_BYTE_ARRAY / BYTE_STREAM_SPLIT encodings; SNAPPY / ZSTD / GZIP / LZ4_RAW / UNCOMPRESSED
codecs, plus the deprecated LZ4 codec in each container writers used for it (parquet-mr's Hadoop framing, a bare LZ4
block, an LZ4 frame); all physical types including FIXED_LEN_BYTE_ARRAY; flat, struct, and LIST/MAP nested schemas.
Row output and filters refuse repeated columns (LIST, MAP, legacy REPEATED); copies keep them. Bytes print losslessly:
text when UTF-8, else `{"base64":...}`; UUIDs and INTERVALs in their own forms
([docs/binary-output.md](docs/binary-output.md)).
Encrypted Parquet is refused: zpq does not decrypt.

**Filter** with `= != <> < <= > >=`, `IN` / `NOT IN`, `[NOT] BETWEEN x AND y`, `[NOT] LIKE`, `IS [NOT] NULL`, `AND` /
`OR` / `NOT` and grouping parentheses (nested at most 32 deep), plus row-group stat pruning and Hive-partition pruning
before any bytes are fetched. Keywords and parentheses inside a quoted literal are the literal's; a quote inside one is
doubled (`'O''Reilly'`), and an unterminated quote is an error. NOT is three-valued: a row whose operand is NULL
passes neither `p` nor `NOT p`. DATE / TIME / TIMESTAMP columns take quoted literals
(`'2024-01-01'`, `'2024-01-01 12:00:00.25'`, `'12:00:00'`) compared exactly: a literal finer than the column's unit is
not rounded to it.

**Project** by column name (nested-aware — `events` resolves to every `events.list.element.*` leaf).

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
avg_state's pair becomes `{"sum": null, "count": 0}`) while `count` is 0. Earlier versions reported 0 or `""`.
`avg(x)` is the mean, one value in every format: DOUBLE for integer and floating `x` (the sum, its integers summed
exactly, divided once by the count), DECIMAL for a DECIMAL `x` (below). `avg_state(x)` is the same average as mergeable
state for fan-out: `{"sum": .., "count": ..}` in JSON and NDJSON, `<name>__sum` and `<name>__count` columns in CSV and
Parquet (a DOUBLE or exact DECIMAL(38, s) sum, a non-null INT64 count), so per-file answers merge exactly with
`sum(<name>__sum) / sum(<name>__count)`; you cannot average means. A double avg is the avg_state sum over its count,
bit for bit.
`-o out.parquet` writes the result as parquet: one row, or one row per group with the same columns and values as the
JSON (avg_state as `<name>__sum` / `<name>__count`).
`count(*)` is answered straight from row-group metadata; `min / max / sum` are computed by decoding the data unless you
opt into trusting file statistics — see [Statistics: trust is opt-in](#statistics-trust-is-opt-in). `--max-memory`
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

It is a working budget, not a hard cap: the binary, page buffers and output in flight come on top. Its default
(`src/core/system.zig`) is three quarters of the memory size on Lambda; on Linux, 80% of available memory, but no more
than three quarters of a cgroup v2 `memory.max`; on macOS, half of physical memory; and 512 MiB where none of these
can be read. Give it as bytes, or as a number (fractions allowed) followed by `K`, `M`, `G` or `T`, with or without a
`B`, in any case, all powers of 1024: `--max-memory 2GB`, `--max-memory 1.5g`, `--max-memory 1073741824`. A space,
an exponent (`1e9`) or a `MiB` spelling is refused (exit 2).

Diagnostic path and cache layout overrides are documented in [execution overrides](docs/execution_knobs.md).

Add `--explain` to an aggregate query to print one JSON plan and exit before allocating scan workers or GROUP BY tables.
It accepts SQL `--query` and the `--aggregate` / `--group-by` flags. It reports the strategy, reasons, column passes,
conditional fallbacks and NDV upper bounds without reading data pages. Local planning can read dictionary headers and,
when footer coverage and byte budgets allow it, dictionary pages to tighten those bounds. S3 explain reads only footers.
`ZPQ_NDV_UNION=0` disables the optional union probe. `ZPQ_PARTAGG=1` forces eligible plain GROUP BY through shared tables;
the planner also chooses them for large string keys or worker-copy pressure supported by a lower bound or a
single-key dictionary-union cost estimate. Composite upper bounds only prove smallness. Prefix LIMIT plans retain
`first_keys` precedence and skip the union probe. Memory estimates remain estimates;
execution enforces the GROUP BY budget. COUNT cuts may permit an unfused census after a sketch bail for fixed-width
keys. Tiny bounds and cheap clustered/skewed R1 scans keep plain grouping. Unknown or high NDV also needs a
cell-dispersion cost hint: sparse repeats, or moderate effective cardinality for a single key. These hints never prove
uniqueness; census admission remains certified, with an exact plain fallback. Short string composites with only COUNT
may also try this gate; single-string and payload cuts keep their existing plans. Explain reports the conditional gate.

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

**Write** with SNAPPY / ZSTD / GZIP / LZ4_RAW / UNCOMPRESSED output (`--codec lz4` means LZ4_RAW; zpq never compresses
to the deprecated LZ4 codec, though a plain copy keeps an LZ4 chunk as it is); RLE_DICTIONARY for low-cardinality byte
arrays; DELTA_BINARY_PACKED for INT32 / INT64; for other strings DELTA_BYTE_ARRAY where values share long prefixes and
DELTA_LENGTH_BYTE_ARRAY otherwise (PLAIN for FIXED_LEN_BYTE_ARRAY, which DELTA_LENGTH_BYTE_ARRAY does not allow).
DECIMAL columns re-encode losslessly (INT32 / INT64 / FIXED_LEN_BYTE_ARRAY backings carry the unscaled integer — never
a lossy detour through DOUBLE). Direct projection copies preserve page indexes; re-encoded chunks get their own.

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
bounded window; see [Local reads](docs/local_io.md).

**Stream** S3-to-S3 with byte-bounded in-flight memory independent of total file size — multipart upload as the encoder
produces bytes, a sliding row-group window for re-encode backpressure, and parallel fetch across files.

```bash
zpq query data.parquet -o out.parquet \
  --filter "int8 BETWEEN -10 AND 10" \
  --select "name, qty * price AS revenue, coalesce(notes, '') AS notes"
```

## CLI output contract

Every query emits answer rows as NDJSON by default, including scans, projections, scalar aggregates and grouped
aggregates. Each line is a row object; scalar aggregates produce one row. `jsonl` remains an alias for `ndjson`.
`--format json` emits an array of row objects, `--format csv` RFC 4180 CSV, and `--format parquet` Parquet. These
flags apply to SQL and flag queries alike.

CSV has a header row, then one row per answer row, for scans and aggregates alike. A NULL is an empty, unquoted field
and an empty string `""`; a field is quoted when it holds a comma, a quote or a line break. BOOLEAN prints `true` /
`false`, DECIMAL every digit of its scale, DATE `YYYY-MM-DD`, TIME `HH:MM:SS.fff` and TIMESTAMP (INT96 included)
`YYYY-MM-DD HH:MM:SS.fff`, with the column's own number of fractional digits and a trailing `Z` when it is adjusted to
UTC. Bytes follow the JSON rules ([docs/binary-output.md](docs/binary-output.md)): text as text, a UUID as
8-4-4-4-12, and what JSON prints as an object (bytes that are not UTF-8, BSON and GEOMETRY, INTERVAL) as that object's
JSON text in a quoted field. An average is one column; an `avg_state` is two, `<name>__sum` and `<name>__count`, as a
Parquet write holds it. `--limit` and SQL `LIMIT` / `OFFSET` count rows, never the header.

```sh
zpq query input.parquet --limit 10                          # NDJSON rows
zpq query input.parquet --aggregate 'count(*) AS n'          # {"n":...}
zpq query --query "SELECT k, count(*) AS n FROM 'input.parquet' GROUP BY k" --format json
zpq query input.parquet -o answer.parquet                    # file only; stdout empty
zpq query input.parquet -o answer.json --format json         # JSON file
zpq query input.parquet --format csv > rows.csv                # CSV, header row first
zpq query input.parquet -o - --format parquet > answer.parquet
zpq query input.parquet --summary=json > rows.ndjson 2> diagnostics.ndjson
```

A named `-o FILE` defaults to Parquet regardless of its extension and leaves stdout empty. Explicit `--format`
overrides that default. `-o -` selects stdout and defaults to NDJSON; explicit Parquet also streams to stdout without
`-o`. Binary stdout refuses a terminal. Limited Parquet row output rejects nested leaves and INT96; unlimited
passthrough preserves both. A limited Parquet write of FLOAT16 widens to DOUBLE, as filtered writes do.
Parquet aggregate output rejects integer sums outside int64; text output retains exact i128 sums.
Existing S3 write support is unchanged; text formats, aggregate writes and limited Parquet S3 writes
are unsupported. `--explain` still prints one aggregate plan JSON object without executing or writing an answer,
even when `-o` is supplied.

`--limit N` caps final result rows for every format and destination, after grouping and ordering. SQL LIMIT has the
same semantics; combining it with `--limit` is an error. LIMIT 0 validates the query and schema without decoding data
pages. Empty NDJSON is zero bytes, empty JSON is `[]` followed by a newline, and empty Parquet retains its schema.
A scalar aggregate over empty input still emits one row unless the final limit or offset excludes it. Column order,
input/group order, exact numeric digits and nulls are the same in every format: avg is one value, avg_state its
`{sum,count}` object in JSON and its split columns in CSV and Parquet. Grouped results still materialize.

Successful queries are silent on stderr unless diagnostics were requested. `--summary=json` emits one typed
`{"type":"summary","ok":true,"rows":N,"output":...}` line after the answer is flushed and a named file is published.
It counts rows four ways: `rows` in the answer, `rows_in` in the inputs, `rows_scanned` in the row groups read, and
`rows_matched` kept by the filter. Its times are wall clock (`total_ms`, `output_us`, `wall_ms`) or, in `worker_ms`,
summed over the workers, so a `worker_ms.decode` above `total_ms` means several workers decoded at once.
[docs/output-format.md](docs/output-format.md) defines every field. `total_us` includes formatting, flushing and
publication and excludes writing the summary. In this mode failures emit a typed `error` line and JSON profile
summaries a typed `profile` line; text profile summaries are rejected. The libzpq, service and Lambda responses carry
the same row counters and time objects.

Exit status is 0 for success, 1 for read/decode/write failure, 2 for invalid or unsupported options/queries, and 141
when a consumer closes stdout. Disk-full errors remain failures. Named local outputs use an exclusive sibling
temporary file and atomic rename: failures remove the temporary and preserve the old target. Input/output aliases,
including hard links, are rejected. This guarantees atomic visibility, not crash durability. S3 success follows PUT
or multipart completion; network errors can leave completion ambiguous and do not promise POSIX rename semantics.
Stdout and shell redirection cannot roll back: a failed NDJSON stream may end mid-record, JSON may lack its closing
bracket, and Parquet may lack its footer. Check exit status, including `set -o pipefail` in shell pipelines.

## Statistics: trust is opt-in

Parquet files carry per-column statistics (min / max / null counts). The spec says they must be accurate — but
real-world writers ship inaccurate ones (we have a `parquet-mr 1.8.2` file in the test corpus whose recorded `min` is
`2.00` when the column actually contains `1.00`). A reader that trusts those stats returns a **silently wrong answer**.

By default, `min / max / sum` decode their values instead of taking the answer from statistics. Filters can still
prune data using corroborated statistics. The metadata shortcuts have separate trust boundaries:

- **`count(*)`** is answered from the row group's `num_rows` (structural, not a statistic).
- **Row-group pruning** uses footer/page statistics that pass type/order and consistency checks. Internally
  consistent but false bounds can still skip matching rows and change the result; `--scan-all` ignores statistics.
  See the [validation contract](docs/output-format.md#validation-and-skipped-data).

Floats compare in total order, as they sort: NaN equals NaN and lies above every number, so `x > 5` and `NOT (x < 5)`
keep NaN rows. Float bounds leave NaN out, so a chunk whose max is below 5 can still hold rows passing `x > 5`. ZPQ
prunes such a chunk only when NaN is proven absent: by a `nan_count` of zero in the chunk's statistics or its page
index (a file declaring IEEE_754_TOTAL_ORDER must write one), or by the NaN probe, which reads the float chunk alone and
tests its values (its dictionary, when every page is coded) before any row group is planned. A missing count means NaN
may be present. ZPQ writes `nan_count` for every float chunk it encodes; a chunk it byte-copies keeps its source's
statistics, count or no count. The probe runs for row output, writes and S3 reads; aggregates over local files use it
only for uncompressed chunks, where it costs less than the scan it replaces.

If you know your writer's statistics are trustworthy, opt into the stats fast-path — `min / max / sum` answered from
metadata in microseconds, without touching a data page:

```bash
zpq query data.parquet --aggregate "min(price), max(price)" --trust-stats
```

`--trust-stats` also reads a float chunk's missing `nan_count` as zero, so `>` and `>=` prune on its bounds as if it
held no NaN; a NaN row in such a chunk is then lost.

`--trust-stats` is a scalpel: it trades correctness-on-bad-files for speed, and it's your call per query. `--scan-all`
is the opposite extreme — no statistics at all, so row-group pruning is off too and every filter is evaluated on decoded
values (`count(*)` still comes from the row counts).

For supported flat OPTIONAL primitive columns that contain no nulls, `--fast-levels` can skip materializing definition
levels. The check reads the encoded level stream rather than trusting writer statistics, and falls back to the normal
decoder unless one RLE run proves that the entire page is present. Aggregate scans use this proof automatically;
`ZPQ_LEVEL_RUNS=0` selects their legacy plain-value level handling.
Projection/write paths still require `--fast-levels`.

## Bloom filters and dictionaries

Statistics cannot prune an `=` or `IN` on a column whose values span its whole range in every row group, such as an
ID, a UUID or a trace key. Two structures can: a column chunk's split-block Bloom filter, and the dictionary of a chunk
whose every data page is dictionary-encoded. ZPQ reads both, for the row groups statistics keep and the literals they
leave open, and skips a row group that neither could contain. Neither has false negatives, so a skipped row group never
held a match; `--scan-all` turns both off along with statistics.

On S3 the filters are fetched while the read is planned, in as few requests as a cost model over the connection pool
finds fastest, as whole filters or as just the 32-byte blocks the literals select. When the first eight row groups'
filters prune too few to pay for the rest, the rest are left unread. A warm Lambda keeps whole filters, keyed by the
object's ETag, so repeated lookups read none.

Writing them is opt-in, per column:

```bash
zpq query events.parquet -o compacted.parquet --bloom-filter trace_id --bloom-filter user_id:0.001
```

Each filter is sized from the chunk's values at the false-positive probability given (default 0.01) and placed after
the row groups, as the spec lays them out. A byte copy keeps the filters its source already has, for every column it
copies; a column dropped or re-encoded loses its filter unless `--bloom-filter` asks for one. See
[docs/bloom_filters.md](docs/bloom_filters.md).

## Test coverage

CI ([`.github/workflows/verify.yml`](.github/workflows/verify.yml)) runs unit tests, Lambda integration tests, CLI
cross-implementation smoke, and the `apache/parquet-testing` conformance corpus on every code change. The smoke path
validates ZPQ-written Parquet with pyarrow; the workflow also identity-checks the Lambda deploy artifact to catch the
"packaged the wrong binary" class of bug.

## Build

Requires **Zig 0.17.0** (release, not master) — pinned in `build.zig.zon`. On a fresh machine, `just bootstrap` installs
the pinned toolchain to `~/.zvm/0.17.0` and warms the build. The first build fetches prebuilt BoringSSL artifacts from
R2 and checks them against the sha256 digests in `vendor/boring_tls/prebuilt.sha256`; `just fetch-deps` stores them in
`vendor/boring_tls/prebuilt/` for offline builds. No source builds. No system OpenSSL.

```bash
just build           # ReleaseFast — both binaries
just test            # unit tests (fixture-dependent ones report as
                     # skipped unless the parquet-testing corpus is present)
just test-integration  # Lambda integration tests
just lambda-build    # static musl Lambda zips (ReleaseFast), both archs
just lambda-deploy zpq-filter-s3 x86_64   # push to AWS
```

`zpq --version` reports the release the binary was built from.

The CLI is `zig-out/bin/zpq`; the Lambda bootstrap is `zig-out/bin/zpq-lambda`. Tagged releases ship the CLI for Linux
and macOS, each x86_64 and arm64, as `zpq-<version>-<revision>-<target>.tar.gz` (plus a `.sha256` and `release.json`)
on the GitHub Release and at `https://pub-4d2e7e2925bb43dc9d3c0323d6d61a84.r2.dev/releases/latest/<asset>` (or
`releases/v<version>/`). An archive holds the binary, the man page, completions, `tiny.parquet` and the guides under
`share/doc/zpq/`. `python3 tools/package_release.py --tag v0.5.0-rc2` builds a release candidate: the tag's suffix
goes into `zpq --version`, the binary's `created_by`, the archive names and `release.json` (`zig build
-Dprerelease=rc2` does the same for a local build). A final tag (`v0.5.0`) builds exactly as without `--tag`.

For a private local Homebrew rehearsal, pass the release manifest as the generator's positional argument.
Keep the tarballs beside that manifest and use their directory as the local URL prefix:

```bash
python3 tools/homebrew_formula.py /absolute/path/to/releases/release.json \
  --base-url file:///absolute/path/to/releases --out local-tap/Formula/zpq.rb
```

## Use as a library

The engine is consumable as a Zig module — the same sans-IO core the binaries use, minus the SQL frontend and its C
parser (libzstd is still built from C, and BoringSSL is linked from the pinned prebuilts):

```zig
// build.zig.zon
.zpq = .{ .url = "...", .hash = "..." },

// build.zig
const zpq = b.dependency("zpq", .{ .target = target, .optimize = optimize })
    .module("zpq");
exe.root_module.addImport("zpq", zpq);
```

`@import("zpq")` exposes `core.*` (schema, thrift, scan, filter, expr, writer) and `io.*` (S3, SigV4, TLS, epoll loop).

## Profiling

`--profile` writes a per-thread trace of a query that opens in [Perfetto](https://ui.perfetto.dev): spans for every
phase (footer fetch and parse, planning and pruning, each S3 request with its time to first byte, each column chunk's
decode and each page's decompression, filter, aggregation, encode and write), counters for requests and bytes in flight
and RSS, and the run's metadata. `--profile-summary` prints the digest to stderr: time per phase, each phase's share of
the wall time where phases overlap, the costliest column chunks, I/O and pruning counts.

```bash
zpq query data/lineitem.parquet --aggregate 'sum(l_extendedprice * l_discount) AS revenue' \
  --filter "l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'" \
  --profile trace.json --profile-summary
```

The Lambda takes `"profile": true` (summary in the response) and `"profile_url": "s3://..."` (full trace uploaded).
Off, profiling costs a branch per span site; see [docs/profiling.md](docs/profiling.md) for what each span means,
how the wall shares are computed, the measured cost, and a walkthrough.

Frame pointers are on by default (cost measured in the noise), which means flamegraphs are a one-liner.

```bash
just flamegraph query data/benchmark_100mb.parquet \
  --aggregate 'sum(int64_sorted) AS s, count(*) AS n'
# wrote prof/flame-<ts>.svg
```

Sub-100 ms runs are too short to sample meaningfully — wrap in a 50× shell loop or use a multi-file glob to get hundreds
of samples. Open the SVG in a browser to drill into hotspots.

`just flamegraph-bpf <secs>` attaches via `profile-bpfcc` for long-running processes (eBPF-based, lower overhead, needs
sudo).

## Layout

```
src/
  zpq.zig                Public surface — exports core.* + io.*
  cli/main.zig           zpq binary entry (workstation)
  lambda/main.zig        zpq-lambda binary entry (AWS Lambda bootstrap)
  engine.zig             runQuery: input dispatch, parallel fetch, scan, output
  profile.zig            Query profiling: per-thread spans, counters, trace export
  profile_summary.zig    Profile summary: phases, wall shares, top chunks
  core/
    schema.zig           Parquet thrift types
    thrift.zig           In-tree thrift reader/writer
    consumer.zig         Per-RG scan→consumer protocol (decode+filter+encode)
    expr/                Typed expression AST + parser + evaluator
    filter/              Filter AST, parser, prune, partition, eval
    parquet/             Page/column readers, codecs, encodings
    writer/              Encoder, fastpath, streaming sink writer
  io/
    s3.zig               SigV4 + HTTP/1.1, transport-agnostic
    multipart_sink.zig   Streaming S3 multipart upload coordinator
    epoll.zig            In-tree event loop (Lambda-mandatory backend)
    tls.zig              BoringSSL via memory BIOs
    http.zig             HTTP/1.1
    pool.zig             Connection pool
    coalescer.zig        Range coalescer for byte-range fetches
    local_reader.zig     Gated local reads: pread / io_uring into a windowed buffer
    sigv4.zig            SigV4 signer

tools/serverless/aws.sh     Lambda lifecycle (build, deploy, invoke)
docs/                       Demos, capability findings, measurements, validation notes
vendor/boring_tls/          Vendored prebuilt-only BoringSSL bindings
```

## Lambda capabilities

Findings driving the architecture are in [`docs/lambda_capabilities.md`](docs/lambda_capabilities.md).

## License

MIT. See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for third-party dependency licenses.
