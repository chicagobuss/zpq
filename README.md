# ZPQ

An agentically-engineered Apache Parquet engine in Zig, optimized for serverless compute against object storage.  The
core philosophy is to do only the minimum work necessary to get the requested task finished to the desired accuracy
level.  Vendored prebuilt crypto, and native S3/SigV4 for further minimization of lambda execution time.

Workloads it's intended for:

* Iceberg / Delta table maintenance — one Lambda per file, filter and rewrite in place.
* Billing / cost roll-ups — answer from row-group statistics when possible, decode bytes only when necessary.
* Log summarization — Parquet queries without the Athena round- trip; one-line CLI output for ad-hoc work.

Two binaries from one sans-IO core: a workstation CLI (`zpq`) and an AWS Lambda bootstrap (`zpq-lambda`). Native SigV4,
vendored prebuilt BoringSSL, in-tree event loop. No AWS SDK, no system OpenSSL.

## Performance

Numbers and trade-offs are dated and contextualised as they're captured. Harnesses:

* `benchmarks/storage_compare.py` — workstation-direct comparison of zpq / duckdb / polars across local files, S3, and
  R2.
* `benchmarks/run_selectivity.sh` — Lambda S3-to-S3 sweep across selectivity levels (zpq vs Python lambda running polars
  + duckdb).
* `benchmarks/run_coldstart.sh` — cold-start comparison.
* `benchmarks/regression/runner.py` — golden-output regression net.

## What it can do

**Read** standard Parquet — V1 + V2 data pages; PLAIN / RLE / RLE_DICTIONARY / DELTA_BINARY_PACKED /
DELTA_LENGTH_BYTE_ARRAY / DELTA_BYTE_ARRAY / BYTE_STREAM_SPLIT encodings; SNAPPY / ZSTD / GZIP / LZ4_RAW / UNCOMPRESSED
codecs; all physical types including FIXED_LEN_BYTE_ARRAY; flat, struct, and LIST/MAP nested schemas.

**Filter** with `= != < <= > >=`, `AND` / `OR`, `BETWEEN x AND y`, plus row-group stat pruning and Hive-partition
pruning before any bytes are fetched.

**Project** by column name (nested-aware — `events` resolves to every `events.list.element.*` leaf).

**Compute** new columns via `--select`: arithmetic on numerics with promotion (`i32 + 1.0 → f64`), string concat (`||`),
`coalesce(col, default)`, parens, unary minus.

**Aggregate** with `sum / count / min / max / avg`, conditional `agg(...) FILTER (WHERE ...)`, and `GROUP BY`.
`count(*)` is answered straight from row-group metadata; `min / max / sum` are computed by decoding the data unless you
opt into trusting file statistics — see [Statistics: trust is opt-in](#statistics-trust-is-opt-in). `--max-memory`
limits accounted GROUP BY entries across worker tables; allocator overhead and materialized result rows are outside the
entry budget, so leave headroom.

**Write** with SNAPPY / ZSTD / GZIP / LZ4_RAW / UNCOMPRESSED output; RLE_DICTIONARY for low-cardinality byte arrays;
DELTA_BINARY_PACKED for INT32 / INT64; DELTA_BYTE_ARRAY for high-cardinality strings. DECIMAL columns re-encode
losslessly (INT32 / INT64 / FIXED_LEN_BYTE_ARRAY backings carry the unscaled integer — never a lossy detour through
DOUBLE). Direct projection copies preserve page indexes; windowed filter/re-encode writes currently omit them and
readers fall back to row-group pruning.

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
is the opposite extreme — decode everything, disable pruning too, for when you don't trust even the row counts.

For supported flat OPTIONAL primitive columns that contain no nulls, `--fast-levels` can skip materializing definition
levels. The check reads the encoded level stream rather than trusting writer statistics, and falls back to the normal
decoder unless one RLE run proves that the entire page is present. It is off by default while the path is new.

## Test coverage

CI ([`.github/workflows/verify.yml`](.github/workflows/verify.yml)) runs unit tests, Lambda integration tests, CLI
cross-implementation smoke, and the `apache/parquet-testing` conformance corpus on every code change. The smoke path
validates ZPQ-written Parquet with pyarrow; the workflow also identity-checks the Lambda deploy artifact to catch the
"packaged the wrong binary" class of bug.

## Build

Requires **Zig 0.16.0** (release, not master) — pinned in `build.zig.zon`. On a fresh machine, `just bootstrap` installs
the pinned toolchain to `~/.zvm/0.16.0` and warms the build. The first build fetches prebuilt BoringSSL artifacts from
R2. No source builds. No system OpenSSL.

```bash
just build           # ReleaseFast — both binaries
just test            # unit tests (fixture-dependent ones report as
                     # skipped unless the parquet-testing corpus is present)
just test-integration  # Lambda integration tests
just lambda-build    # static musl Lambda binary, both archs
just lambda-deploy zpq-filter-s3 x86_64   # push to AWS
```

`zpq --version` reports the release the binary was built from.

The CLI is `zig-out/bin/zpq`; the Lambda bootstrap is `zig-out/bin/zpq-lambda`. Tagged releases (Linux x86_64 + aarch64;
macOS lands with the kqueue backend) ship to `https://pub-4d2e7e2925bb43dc9d3c0323d6d61a84.r2.dev/releases/latest/` and
a matching GitHub Release.

## Use as a library

The engine is consumable as a Zig module — the same sans-IO core the binaries use, minus the SQL frontend (no C sources
for consumers):

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

Frame pointers are on by default (cost measured in the noise), which means flamegraphs are a one-liner.

```bash
just flamegraph query data/benchmark_100mb.parquet \
  --aggregate 'sum(int64_sorted) AS s, count(*) AS n'
# wrote prof/flame-<ts>.svg
```

Sub-100 ms runs are too short to sample meaningfully — wrap in a 50× shell loop or use a multi-file glob to get hundreds
of samples. Open the SVG in a browser to drill into hotspots.

`just flamegraph-bpf <secs>` attaches via `profile-bpfcc` for long- running processes (eBPF-based, lower overhead, needs
sudo).

## Layout

```
src/
  zpq.zig                Public surface — exports core.* + io.*
  cli/main.zig           zpq binary entry (workstation)
  lambda/main.zig        zpq-lambda binary entry (AWS Lambda bootstrap)
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
