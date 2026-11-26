# zpq

zpq is a small Unix tool for Parquet. It queries, filters, aggregates and rewrites Parquet files on your disk or
straight from S3. Output is NDJSON, CSV or Parquet, ready for the next tool in the pipe.

zpq does the least work a query needs for an exact answer. It checks footers and statistics first, then reads only the
pages that matter. DECIMAL values stay in exact integers. Queries return the same rows in the same order at any thread
count.

Visit the project site at **[zpq.chicagobuss.dev](https://zpq.chicagobuss.dev)** for every benchmark and example.

## Install

**Homebrew** (Apple-silicon macOS and Linux):

```bash
brew install chicagobuss/zpq/zpq
```

**Static binaries** are on the [latest release](https://github.com/chicagobuss/zpq/releases/latest) page. Each archive
contains `bin/`, `share/` and `MANIFEST.json` directly, without a top-level folder, so extract it into a new directory:

```bash
curl -sSL https://github.com/chicagobuss/zpq/releases/latest/download/zpq-x86_64-linux-musl.tar.gz \
  | (mkdir zpq && tar xz -C zpq)
export PATH="$PWD/zpq/bin:$PATH"
```

| Platform | Archive |
|---|---|
| Linux x86_64 (needs AVX2) | `zpq-x86_64-linux-musl.tar.gz` |
| Linux x86_64, older CPUs without AVX2 | `zpq-x86_64-linux-musl-compat.tar.gz` |
| Linux arm64 | `zpq-aarch64-linux-musl.tar.gz` |
| macOS, Apple silicon | `zpq-aarch64-macos.tar.gz` |
| macOS, Intel | `zpq-x86_64-macos.tar.gz` |
| Windows x86_64 | `zpq-x86_64-windows-gnu.zip` |

**Debian, Ubuntu, Fedora and RHEL:** The release also includes `zpq_0.6.1_amd64.deb`, `zpq_0.6.1_arm64.deb`,
`zpq-0.6.1-1.x86_64.rpm` and `zpq-0.6.1-1.aarch64.rpm`.

```bash
sudo apt install ./zpq_0.6.1_amd64.deb      # or: sudo dnf install ./zpq-0.6.1-1.x86_64.rpm
```

The default Linux x86_64 build needs AVX2. On older CPUs, use the `-compat` build.

**Build it yourself.** All you need is a Zig 0.17.x release. Zig's bundled clang builds the C codecs, and the first
build fetches checksum-pinned BoringSSL prebuilts. Cross-compiling takes one flag.

```bash
git clone https://github.com/chicagobuss/zpq && cd zpq
zig build cli -Doptimize=ReleaseFast                                  # zig-out/bin/zpq
zig build cli -Doptimize=ReleaseFast -Dtarget=aarch64-linux-musl      # a static arm64 Linux binary, from anywhere
```

## First commands

These examples use ClickBench's public
[`hits_0.parquet`](https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet) (122 MB, one
million rows of web analytics). Each snippet shows real output. You can find more examples with their output on the
[Examples](https://zpq.chicagobuss.dev/examples.html) page.

<!-- examples:begin -->
Look inside a file without reading its data pages:

```console
$ zpq inspect hits_0.parquet | head -12
hits_0.parquet: Parquet v1, 1000000 rows, 122446530 bytes, 2 row groups
  created_by: parquet-cpp version 1.5.1-SNAPSHOT; sorting_columns: unsupported
  row group 0: 450560 rows, 188923477 uncompressed bytes
    WatchID INT64 SNAPPY: 3883794/3883596 compressed/uncompressed bytes
      min=4611687214012840539 (unknown) max=9223369186199968220 (unknown); nulls=0
      index=absent/absent, Bloom=absent, dictionary fallback=present
      hint: page pruning unavailable: column index absent
      hint: compression saves no bytes: compressed >= uncompressed
      hint: dictionary fallback: mixed data encodings in encoding_stats
    JavaEnable INT32 SNAPPY: 20529/24207 compressed/uncompressed bytes
      min=0 (unknown) max=1 (unknown); nulls=0
      index=absent/absent, Bloom=absent, dictionary fallback=absent
```

Query it with SQL and pipe the rows to `jq`:

```console
$ zpq query --query "SELECT SearchPhrase, count(*) AS c FROM 'hits_0.parquet' \
  WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 3" | jq -c .
{"SearchPhrase":"ведомосквы вместу","c":4943}
{"SearchPhrase":"ведомосквы вы из","c":2471}
{"SearchPhrase":"ведомосквиталия страции","c":2026}
```

Filter and aggregate with flags instead of SQL:

```console
$ zpq query hits_0.parquet --filter 'IsMobile = 1' \
  --aggregate 'count(*) AS n, count(DISTINCT UserID) AS users, avg(ResolutionWidth) AS width'
{"n":35239,"users":5146,"width":846.5504696501035}
```

Write Parquet with `-o`, then read it back as CSV:

```console
$ zpq query hits_0.parquet --filter "SearchPhrase <> ''" --select 'UserID, RegionID, SearchPhrase' -o phrases.parquet
$ zpq query phrases.parquet --limit 3 --format csv
UserID,RegionID,SearchPhrase
-9214751021948998350,2,авомосква веб каменисный
-9214751021948998350,2,авомосква веб каменисный
-9213728704863893851,103,чагин выпуска на волна 2 сезон 24 резюме онлайн
```

Read straight from S3. A glob reads all 100 files; `count(*)` comes from their footers alone:

```console
$ S3_NO_SIGN_REQUEST=1 AWS_REGION=eu-central-1 zpq query \
  's3://clickhouse-public-datasets/hits_compatible/athena_partitioned/hits_*.parquet' --aggregate 'count(*) AS n'
{"n":99997497}
```
<!-- examples:end -->

By default, zpq writes Parquet files with version 2 encodings (`--parquet-version v2`). That uses DELTA where it fits,
RLE_DICTIONARY, and a version 2 footer. For readers that only understand version 1 files, pass `--parquet-version v1`
to write PLAIN / PLAIN_DICTIONARY encodings with a version 1 footer.

## Three ways in

- **The `zpq` CLI** is shown above. `zpq --help` and `man zpq` list every option. See
  [docs/getting-started.md](docs/getting-started.md) for a walkthrough and
  [docs/output-format.md](docs/output-format.md) for the output contract.
- **`zpq-lambda`** is a static AWS Lambda bootstrap for `provided.al2023`. It starts cold in tens of milliseconds and
  reads and writes S3 natively, with no AWS SDK. `tools/serverless/fanout.py` fans one query out across many workers by
  row group ([docs/lambda-and-s3.md](docs/lambda-and-s3.md)).
- **libzpq** is a C library ([docs/libzpq.md](docs/libzpq.md)) with a Python binding, `python/zpq.py`, that needs no
  compiled extension. SQL through libzpq is experimental. libzpq can also return answers as Arrow C Data streams
  (experimental). `python/zpq_arrow.py` reads those streams into PyArrow. A Spark DataSource V2,
  [`spark.read.format("zpq")`](integrations/spark/README.md), runs Parquet scans and partial aggregates in zpq and
  takes the rows back as Arrow batches.

## What zpq isn't

zpq isn't a general SQL database or a dataframe library. Its joins cover star and snowflake queries over local or S3
Parquet. A star query joins one large table of facts, such as sales or page views, to smaller lookup tables, such as
products, customers or dates, and each fact row matches at most one row in each lookup table (an N:1 join). A snowflake
query is the same shape with lookup tables that join further lookup tables, such as a product to its category. Each
lookup (build) table must have unique keys and fit in `--max-memory`. zpq refuses other join shapes by name.
For everything else, pair zpq with DuckDB, Polars or Spark, and let zpq do the Parquet part.

## How fast

Across 464 benchmark queries, zpq 0.6.0 is faster than DuckDB 2.0.0 on 457 of 464 and faster than Polars 2.1.0 on 459
of the 461 where Polars' answer validated. The suite covers ClickBench, PDS-H and 390 synthetic aggregation cases. Each
query ran hot in a fresh process on a Ryzen 9 9950X.

See the [Benchmarks](https://zpq.chicagobuss.dev/numbers.html) page for every query, the cold runs, the method and
where zpq is still behind.

## Limits

- `--max-memory` is a working budget, not a hard cap. It counts aggregation, DISTINCT and string memory, but not the
  binary, thread stacks or mapped input pages. Because of that, a query can go somewhat past the budget (a 256 MiB
  budget peaked at about 284 MiB in testing). The answer stays exact, or the query refuses cleanly. For a hard ceiling,
  run zpq under a cgroup `memory.max` or `ulimit -v`.
- Window functions work (see docs/capabilities.md). zpq refuses some other SQL with a message and exit 2, or exit 1 when the query had already started to run (`zpq --help`, exit codes).
- JSON prints DATE and TIMESTAMP as their stored integers; CSV prints ISO text.
- On Windows, only the x86_64 CLI is built. libzpq is not built for Windows (see docs/libzpq.md).

The full list is under "Known limits" in [CHANGELOG.md](CHANGELOG.md).

## Learn more

- [How it works](https://zpq.chicagobuss.dev/how.html): every optimization, Parquet itself, testing, the write path,
  Lambda fan-out, the JVM and NaN.
- [Examples](https://zpq.chicagobuss.dev/examples.html): a guide with real output for each command.
- [CHANGELOG.md](CHANGELOG.md): release history and upgrade notes.

## Reference

- [What zpq can do](docs/capabilities.md): reading, filtering, computing, aggregating, writing, ordering, local reads
  and S3 streaming.
- [CLI output contract](docs/cli-output-contract.md) and the full [output format](docs/output-format.md).
- [Statistics: trust is opt-in](docs/statistics-trust.md).
- [Bloom filters and dictionaries](docs/bloom-filters-and-dictionaries.md).
- [zpq on AWS Lambda, and reading S3](docs/lambda-and-s3.md), with the
  [Lambda request format](docs/lambda_requests.md).
- [libzpq](docs/libzpq.md), with its Arrow C stream results, and [using zpq as a Zig module](docs/zig-module.md).
- [The Spark DataSource V2](integrations/spark/README.md).
- [Profiling a query](docs/profiling-quickstart.md).
- [Source layout](docs/source-layout.md).

## Build and contribute

```bash
just build             # ReleaseFast: the CLI and the Lambda bootstrap
just check             # the commit gate: fixtures, unit tests, every case against DuckDB
just full              # the full gate (tools/check/README.md)
just test-integration  # Lambda integration tests
```

Contributors and coding agents should start with [AGENTS.md](AGENTS.md). For more detail, see
[tools/check/README.md](tools/check/README.md) for the check tiers, [docs/test-coverage.md](docs/test-coverage.md) for
test coverage, and [docs/building.md](docs/building.md) for the toolchain, release archives and packaging.

## License

MIT. See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for third-party dependency licenses.
