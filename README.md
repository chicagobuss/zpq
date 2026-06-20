# ZPQ

An agentically-engineered Apache Parquet engine in Zig, optimized for 
serverless compute against object storage.


Workloads it's intended for:

* Iceberg / Delta table maintenance — one Lambda per file, filter
  and rewrite in place.
* Billing / cost roll-ups — answer from row-group statistics when
  possible, decode bytes only when necessary.
* Log summarization — Parquet queries without the Athena round-
  trip; one-line CLI output for ad-hoc work.

Two binaries from one sans-IO core: a workstation CLI (`zpq`) and an
AWS Lambda bootstrap (`zpq-lambda`). Native SigV4, vendored prebuilt
BoringSSL, in-tree event loop. No AWS SDK, no system OpenSSL.

## Performance

Numbers and trade-offs are dated and contextualised as they're
captured. Harnesses:

* `benchmarks/storage_compare.py` — workstation-direct comparison of
  zpq / duckdb / polars across local files, S3, and R2.
* `benchmarks/run_selectivity.sh` — Lambda S3-to-S3 sweep across
  selectivity levels (zpq vs Python lambda running polars + duckdb).
* `benchmarks/run_coldstart.sh` — cold-start comparison.
* `benchmarks/regression/runner.py` — golden-output regression net.

## What it can do

**Read** standard Parquet — V1 + V2 data pages; PLAIN / RLE /
RLE_DICTIONARY / DELTA_BINARY_PACKED / DELTA_BYTE_ARRAY encodings;
SNAPPY / ZSTD / GZIP / LZ4_RAW / UNCOMPRESSED codecs; flat, struct,
and LIST/MAP nested schemas.

**Filter** with `= != < <= > >=`, `AND` / `OR`, `BETWEEN x AND y`,
plus row-group stat pruning and Hive-partition pruning before any
bytes are fetched.

**Project** by column name (nested-aware — `events` resolves to every
`events.list.element.*` leaf).

**Compute** new columns via `--select`: arithmetic on numerics with
promotion (`i32 + 1.0 → f64`), string concat (`||`),
`coalesce(col, default)`, parens, unary minus.

**Aggregate** with `sum / count / min / max / avg`, including
conditional `agg(...) FILTER (WHERE ...)`. Stat-only short-circuit
for `count(*)` / `count(col)` / `min(col)` / `max(col)` when the row
group's metadata has the answer.

**Write** with SNAPPY / ZSTD / UNCOMPRESSED output; RLE_DICTIONARY for
low-cardinality byte arrays; DELTA_BINARY_PACKED for INT32 / INT64.

**Stream** S3-to-S3 with `O(one-row-group)` memory regardless of total
file size — multipart upload as the encoder produces bytes, parallel
fetcher across files, single decoder per file.

```bash
zpq query data.parquet -o out.parquet \
  --filter "int8 BETWEEN -10 AND 10" \
  --select "name, qty * price AS revenue, coalesce(notes, '') AS notes"
```

## Test coverage

CI ([`.github/workflows/verify.yml`](.github/workflows/verify.yml))
runs unit tests, Lambda integration tests, CLI cross-implementation
smoke, and the `apache/parquet-testing` conformance corpus on every
code change. The smoke path validates ZPQ-written Parquet with pyarrow;
the workflow also identity-checks the Lambda deploy artifact to catch
the "packaged the wrong binary" class of bug.

## Build

Requires **Zig 0.16.0** (release, not master) — pinned in
`build.zig.zon`. On a fresh machine, `just bootstrap` installs the
pinned toolchain to `~/.zvm/0.16.0` and warms the build. The first
build fetches prebuilt BoringSSL artifacts from R2. No source builds.
No system OpenSSL.

```bash
just build           # ReleaseFast — both binaries
just test            # 224 unit tests
just test-integration  # Lambda integration tests
just lambda-build    # static musl Lambda binary, both archs
just lambda-deploy zpq-filter-s3 x86_64   # push to AWS
```

The CLI is `zig-out/bin/zpq`; the Lambda bootstrap is
`zig-out/bin/zpq-lambda`. Tagged releases (Linux x86_64 + aarch64;
macOS lands with the kqueue backend) ship to
`https://pub-4d2e7e2925bb43dc9d3c0323d6d61a84.r2.dev/releases/latest/`
and a matching GitHub Release.

## Profiling

Frame pointers are on by default (cost measured in the noise),
which means flamegraphs are a one-liner.

```bash
just flamegraph query data/benchmark_100mb.parquet \
  --aggregate 'sum(int64_sorted) AS s, count(*) AS n'
# wrote prof/flame-<ts>.svg
```

Sub-100 ms runs are too short to sample meaningfully — wrap in a
50× shell loop or use a multi-file glob to get hundreds of
samples. Open the SVG in a browser to drill into hotspots.

`just flamegraph-bpf <secs>` attaches via `profile-bpfcc` for long-
running processes (eBPF-based, lower overhead, needs sudo).

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

`probe_lambda_caps` enumerates what AWS Lambda actually allows
(seccomp filter, kernel version, allowed setsockopt, CPU affinity at
each memory tier, `/tmp` throughput). Re-run any time AWS announces a
runtime change — seccomp is not API contract.

```bash
just probe-local         # JSON to stdout from your workstation
just probe-lambda        # deploy + invoke in AWS, prints JSON
```

Findings driving the architecture are in
[`docs/lambda_capabilities.md`](docs/lambda_capabilities.md).

## Philosophy

Do the absolute minimum work the query requires: sans-IO core, two
binaries (workstation CLI and Lambda bootstrap) sharing one engine,
vendored prebuilt crypto, and native S3/SigV4 for a small static binary.
The capabilities above are what's shipped.

## License

MIT
