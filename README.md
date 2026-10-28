# zpq

zpq is a small Unix tool for Parquet. It queries, filters, aggregates and rewrites Parquet files, on your disk or
straight from S3, and prints NDJSON, CSV or Parquet for the next tool in the pipe. It does the least work a query
needs for an exact answer: footers and statistics first, and only the pages that matter. DECIMAL stays in exact
integers, and the same rows come back in the same order at any thread count.

The project site, with every benchmark and example: **[zpq.chicagobuss.dev](https://zpq.chicagobuss.dev)**.

## Install

**Homebrew** (Apple-silicon macOS and Linux):

```bash
brew install chicagobuss/zpq/zpq
```

**A static binary** from the [latest release](https://github.com/chicagobuss/zpq/releases/latest). Each archive holds
`bin/`, `share/` and `MANIFEST.json` with no top folder, so give it one:

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

**Debian, Ubuntu, Fedora, RHEL:** the release also carries `zpq_0.5.0_amd64.deb`, `zpq_0.5.0_arm64.deb`,
`zpq-0.5.0-1.x86_64.rpm` and `zpq-0.5.0-1.aarch64.rpm`.

```bash
sudo apt install ./zpq_0.5.0_amd64.deb      # or: sudo dnf install ./zpq-0.5.0-1.x86_64.rpm
```

The default Linux x86_64 build needs AVX2; on older CPUs use the `-compat` build.

**Build it yourself.** All you need is Zig 0.17.0: Zig's bundled clang builds the C codecs, and the first build
fetches checksum-pinned BoringSSL prebuilts. Cross-compiling is one flag.

```bash
git clone https://github.com/chicagobuss/zpq && cd zpq
zig build cli -Doptimize=ReleaseFast                                  # zig-out/bin/zpq
zig build cli -Doptimize=ReleaseFast -Dtarget=aarch64-linux-musl      # a static arm64 Linux binary, from anywhere
```

## First commands

These run on ClickBench's public
[`hits_0.parquet`](https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet) (122 MB, one
million rows of web analytics). The output shown is the real output; more examples, each with its output, are on the
site's [Examples](https://zpq.chicagobuss.dev/examples.html) page.

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

## Three ways in

- **The `zpq` CLI**, shown above. `zpq --help` and `man zpq` list every option;
  [docs/getting-started.md](docs/getting-started.md) walks through more, and
  [docs/output-format.md](docs/output-format.md) is the output contract.
- **`zpq-lambda`**, a static AWS Lambda bootstrap for `provided.al2023` that starts cold in tens of milliseconds and
  reads and writes S3 natively, with no AWS SDK. `tools/serverless/fanout.py` fans one query out across many workers
  by row group ([docs/lambda-and-s3.md](docs/lambda-and-s3.md)).
- **libzpq**, a C library ([docs/libzpq.md](docs/libzpq.md)), with a Python binding, `python/zpq.py`, that needs no
  compiled extension. SQL through libzpq is experimental.

## What zpq isn't

zpq works on one table at a time: there are no joins and no dataframe API yet. Use DuckDB for those, and let zpq do
the Parquet part: filter and project with zpq, write Parquet or NDJSON, and read that into DuckDB.

## How fast

On 464 benchmark queries (ClickBench, PDS-H and 390 synthetic aggregation cases), run cold with a fresh process for
every query, zpq is the fastest of zpq, DuckDB 2.0 and Polars 2.0 on 438, and never more than 1.5× behind; its geometric
mean is 0.56× the faster rival's time cold and 0.48× hot. Every query, the method, and the few places zpq is still
behind are on the site's [Benchmarks](https://zpq.chicagobuss.dev/numbers.html) page.

## Limits

- `--max-memory` is a working budget, not a hard cap: a high-cardinality GROUP BY or `count(DISTINCT)` can go past it
  by up to about 2× near the limit. The answer stays exact, or the query refuses cleanly. For a hard ceiling, run zpq
  under a cgroup `memory.max` or `ulimit -v`.
- No joins and no window functions; some other SQL is refused with exit 2 and a message.
- JSON prints DATE and TIMESTAMP as their stored integers; CSV prints ISO text.
- SQL through libzpq is experimental, and Windows is not built.

The full list is under "Known limits" in [CHANGELOG.md](CHANGELOG.md).

## Learn more

- [How it works](https://zpq.chicagobuss.dev/how.html): every optimization, Parquet itself, testing, the write path,
  Lambda fan-out, the JVM and NaN.
- [Examples](https://zpq.chicagobuss.dev/examples.html): a guide with real output for each command.
- [CHANGELOG.md](CHANGELOG.md): everything new in 0.5.0, and what to do when upgrading from 0.4.x.

## Reference

- [What zpq can do](docs/capabilities.md): reading, filtering, computing, aggregating, writing, ordering, local reads
  and S3 streaming.
- [CLI output contract](docs/cli-output-contract.md) and the full [output format](docs/output-format.md).
- [Statistics: trust is opt-in](docs/statistics-trust.md).
- [Bloom filters and dictionaries](docs/bloom-filters-and-dictionaries.md).
- [zpq on AWS Lambda, and reading S3](docs/lambda-and-s3.md), with the
  [Lambda request format](docs/lambda_requests.md).
- [libzpq](docs/libzpq.md), and [using zpq as a Zig module](docs/zig-module.md).
- [Profiling a query](docs/profiling-quickstart.md).
- [Source layout](docs/source-layout.md).

## Build and contribute

```bash
just build             # ReleaseFast: the CLI and the Lambda bootstrap
just check             # the commit gate: fixtures, unit tests, every case against DuckDB
just full              # the full gate (tools/check/README.md)
just test-integration  # Lambda integration tests
```

The check tiers are described in [tools/check/README.md](tools/check/README.md), test coverage in
[docs/test-coverage.md](docs/test-coverage.md), and toolchain, release archives and packaging in
[docs/building.md](docs/building.md). Contributors and coding agents start with [AGENTS.md](AGENTS.md).

## License

MIT. See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for third-party dependency licenses.
