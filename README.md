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

## Performance

Numbers and trade-offs are dated and contextualised as they're captured. Harnesses:

* `benchmarks/storage_compare.py` — workstation-direct comparison of zpq / duckdb / polars across local files, S3, and
  R2.
* `benchmarks/run_selectivity.sh` — Lambda S3-to-S3 sweep across selectivity levels (zpq vs Python lambda running polars
  + duckdb).
* `benchmarks/run_coldstart.sh` — cold-start comparison; the latest cold-start figures are in
  [`docs/measurements/lambda_cold_start_2026-10-05.md`](docs/measurements/lambda_cold_start_2026-10-05.md).
* `benchmarks/regression/runner.py` — golden-output regression net.

## What it can do

**Read** standard Parquet — V1 + V2 data pages; PLAIN / RLE / RLE_DICTIONARY / DELTA_BINARY_PACKED /
DELTA_LENGTH_BYTE_ARRAY / DELTA_BYTE_ARRAY / BYTE_STREAM_SPLIT encodings; SNAPPY / ZSTD / GZIP / LZ4_RAW / UNCOMPRESSED
codecs, plus the deprecated LZ4 codec in each container writers used for it (parquet-mr's Hadoop framing, a bare LZ4
block, an LZ4 frame); all physical types including FIXED_LEN_BYTE_ARRAY; flat, struct, and LIST/MAP nested schemas.

**Filter** with `= != < <= > >=`, `AND` / `OR`, `BETWEEN x AND y`, plus row-group stat pruning and Hive-partition
pruning before any bytes are fetched. DATE / TIME / TIMESTAMP columns take quoted literals (`'2024-01-01'`,
`'2024-01-01 12:00:00.25'`, `'12:00:00'`) compared exactly: a literal finer than the column's unit is not rounded to it.

**Project** by column name (nested-aware — `events` resolves to every `events.list.element.*` leaf).

**Compute** new columns via `--select`: arithmetic on numerics with promotion (`i32 + 1.0 → f64`), string concat (`||`),
`coalesce(col, default)`, parens, unary minus.

**Aggregate** with `sum / count / min / max / avg`, conditional `agg(...) FILTER (WHERE ...)`, and `GROUP BY`.
As in SQL, `sum / min / max / avg` over no non-null values are NULL (JSON `null`, a null cell in `-o` parquet; avg's
`{"sum", "count"}` pair becomes `{"sum": null, "count": 0}`) while `count` is 0. Earlier versions reported 0 or `""`.
`-o out.parquet` writes the result as parquet: one row, or one row per group with the same columns and values as the
JSON (avg as `<name>__sum` / `<name>__count`).
`count(*)` is answered straight from row-group metadata; `min / max / sum` are computed by decoding the data unless you
opt into trusting file statistics — see [Statistics: trust is opt-in](#statistics-trust-is-opt-in). `--max-memory`
limits accounted GROUP BY entries across worker tables; allocator overhead and materialized result rows are outside the
entry budget, so leave headroom.

**Write** with SNAPPY / ZSTD / GZIP / LZ4_RAW / UNCOMPRESSED output (`--codec lz4` means LZ4_RAW; zpq never compresses
to the deprecated LZ4 codec, though a plain copy keeps an LZ4 chunk as it is); RLE_DICTIONARY for low-cardinality byte
arrays; DELTA_BINARY_PACKED for INT32 / INT64; DELTA_BYTE_ARRAY for high-cardinality strings. DECIMAL columns re-encode
losslessly (INT32 / INT64 / FIXED_LEN_BYTE_ARRAY backings carry the unscaled integer — never a lossy detour through
DOUBLE). Direct projection copies preserve page indexes; windowed filter/re-encode writes currently omit them and
readers fall back to row-group pruning.

`--codec` sets the output codec. Without it, re-encoded output is SNAPPY and a plain copy keeps each column chunk's own
codec; with it, a plain copy recompresses any chunk stored in another codec. The `codec` field of the write summary names
the codec every written chunk uses, or is `"MIXED"` when a plain copy without `--codec` keeps chunks in more than one
(for example, inputs written with different codecs).

**Order** is the input's: every output that lists rows (print, `--format`, `-o` writes and byte copies, several files,
S3 and Lambda) keeps files in the order given, then row groups, then rows, at any `-j`. `GROUP BY` lists groups in the
order they first appear in the input (before 0.5.0, in the byte order of their serialized keys), and every aggregate,
floating-point sums and averages included, comes out with the same bits at every `-j` and on every run: each fixed
block of row groups folds into its own table and the blocks merge in input order. `--unordered` (the Lambda's
`"ordered": false`) gives both up for speed: groups in no set order, float aggregates free to differ in their last
bits.

**Stream** S3-to-S3 with byte-bounded in-flight memory independent of total file size — multipart upload as the encoder
produces bytes, a sliding row-group window for re-encode backpressure, and parallel fetch across files.

```bash
zpq query data.parquet -o out.parquet \
  --filter "int8 BETWEEN -10 AND 10" \
  --select "name, qty * price AS revenue, coalesce(notes, '') AS notes"
```

## Statistics: trust is opt-in

Parquet files carry per-column statistics (min / max / null counts). The spec says they must be accurate — but
real-world writers ship inaccurate ones (we have a `parquet-mr 1.8.2` file in the test corpus whose recorded `min` is
`2.00` when the column actually contains `1.00`). A reader that trusts those stats returns a **silently wrong answer**.

ZPQ's default is **correctness**: `min / max / sum` are computed by decoding the data, so the answer is right regardless
of what the file claims. Two things are still always fast and always safe, because they can't produce a wrong value:

- **`count(*)`** is answered from the row group's `num_rows` (structural, not a statistic).
- **Row-group pruning** uses stats only to *skip* groups that provably can't match a filter — it never invents a value,
  so a bad stat can at worst cost a little extra decoding, never a wrong result.

If you know your writer's statistics are trustworthy, opt into the stats fast-path — `min / max / sum` answered from
metadata in microseconds, without touching a data page:

```bash
zpq query data.parquet --aggregate "min(price), max(price)" --trust-stats
```

`--trust-stats` is a scalpel: it trades correctness-on-bad-files for speed, and it's your call per query. `--scan-all`
is the opposite extreme — no statistics at all, so row-group pruning is off too and every filter is evaluated on decoded
values (`count(*)` still comes from the row counts).

For supported flat OPTIONAL primitive columns that contain no nulls, `--fast-levels` can skip materializing definition
levels. The check reads the encoded level stream rather than trusting writer statistics, and falls back to the normal
decoder unless one RLE run proves that the entire page is present. It is off by default while the path is new.

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
and macOS, each x86_64 and arm64, as `zpq-<linux|macos>-<x86_64|arm64>.tar.gz` (plus a `.sha256`) on the GitHub Release
and at `https://pub-4d2e7e2925bb43dc9d3c0323d6d61a84.r2.dev/releases/latest/<asset>` (or `releases/v<version>/`).

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
    sigv4.zig            SigV4 signer

probes/probe_lambda_caps/   Empirical Lambda capability prober
benchmarks/                 Selectivity / cold-start / partition sweeps
tools/serverless/aws.sh     Lambda lifecycle (build, deploy, invoke)
docs/                       Demos, capability findings, measurements, validation notes
vendor/boring_tls/          Vendored prebuilt-only BoringSSL bindings
```

## Lambda capability probe

`probe_lambda_caps` enumerates what AWS Lambda actually allows (seccomp filter, kernel version, allowed setsockopt, CPU
affinity at each memory tier, `/tmp` throughput). Re-run any time AWS announces a runtime change — seccomp is not API
contract.

```bash
just probe-local         # JSON to stdout from your workstation
just probe-lambda        # deploy + invoke in AWS, prints JSON
```

Findings driving the architecture are in [`docs/lambda_capabilities.md`](docs/lambda_capabilities.md).

## License

MIT. See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for third-party dependency licenses.
