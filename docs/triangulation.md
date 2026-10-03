# Triangulation Correctness Harness

The triangulation correctness harness (`tools/triangulate.py`) acts as an automated 3-engine referee between:
1. **ZPQ** (The engine under test)
2. **Hardwood** (The strict-reader Java oracle, [hardwood-hq/hardwood](https://github.com/hardwood-hq/hardwood))
3. **DuckDB** (The C++ analytics referee)

It is invoked via `just triangulate` and serves as a Tier 3 (gauntlet) test. The banner names the Hardwood binary and
version in use; a missing Hardwood is a loud warning, not a silent 2-engine run.

## Getting Hardwood

```bash
just fetch-hardwood                              # pinned prebuilt native CLI + fixtures; no JDK needed
tools/fetch_hardwood.sh --from-source ~/hardwood # JVM launcher built from a checkout (JDK 25+)
```

Both populate the gitignored `tools/hardwood/`: `bin/hardwood` (found by the harness automatically) and `fixtures`
(Hardwood's `core/src/test/resources`).

- **Prebuilt (default).** Downloads `hardwood-cli-early-access-<os>-<arch>.tar.gz` from Hardwood's `1.0-early-access`
  GitHub release and verifies it against the SHA-256 pinned in `tools/fetch_hardwood.sh`, then sparse-checks-out the
  fixtures at the commit that build came from. That release asset is rebuilt on every push to Hardwood's main, so the
  digest is the pin: once upstream refreshes it the fetch fails instead of silently changing the oracle. To bump, update
  the digests and the commit (the one `hardwood --version` prints) together.
- **From source.** `--from-source DIR` runs `./mvnw -DskipTests package` for the `cli` module in a Hardwood checkout
  and writes a `java -cp …` launcher; `fixtures` links to DIR's own resources, so CLI and fixtures stay in step with
  whatever DIR has checked out. A native binary needs GraalVM 25 (`./mvnw -Dnative package -pl cli -am`, result at
  `cli/target/hardwood-cli`); point `$HARDWOOD` at it.

Lookup order for the CLI: `$HARDWOOD` (a wrong path is an error), `tools/hardwood/bin/hardwood`, then `$PATH`. Fixtures:
`--hardwood-fixtures DIR`, `$HARDWOOD_FIXTURES`, then `tools/hardwood/fixtures`; DIR may be a Hardwood checkout root or
its `core/src/test/resources`. Prefer fixtures from the same commit as the CLI: newer fixtures can exercise features an
older CLI rejects.

## Design

The harness tests two independent directions:

### ZPQ write correctness
We generate a small input matrix of Parquet files using PyArrow: integers, floats (with NaN/inf), booleans, strings,
decimals, a single-distinct dictionary column, dates and timestamps, and an INT96 timestamp file.

Each file goes through ZPQ twice: an unfiltered `SELECT *` (the byte-copy fastpath), and a `--select` of every column,
which always re-encodes and so is the only way to exercise each output codec (`snappy`, `zstd`, `gzip`, `lz4_raw`,
`uncompressed`). INT96 only takes the byte-copy path; ZPQ does not re-encode it. The output must be structurally
consistent (footer columns = chunks per row group), use the requested codec, and read back through both DuckDB and
Hardwood with the source's values.

**Rule:** A file rejected by an external reader, or whose values mismatch the source, is an immediate `FAIL`. When
Hardwood reads ZPQ's output identically to how it reads the source but its text rendering still differs from DuckDB's
typed values, that is a normalizer gap and reported `INFO`.

The write-mode section adds projection variants, nested passthrough, clean rejection of nested re-encode, and that an
unfiltered `SELECT * --codec X` does not report a codec it did not apply.

### ZPQ read correctness
Corpus: `apache/parquet-testing` (`data/parquet-testing/data`), ZPQ's own `ci/fixtures/parquet`, and Hardwood's test
fixtures. Each file is labelled by source (`parquet-testing:…`, `zpq-ci:…`, `hardwood:<path>`).

Per file, the harness runs these probes:
- **agg** — `sum`/`min`/`max` of every flat numeric column, `min`/`max` of every flat string column, and `count(*)`.
- **filtered counts** — `count(*)` under `col > literal` for the first integer and first string column, with the
  literal at the median distinct value so statistics-based row-group and page pruning actually decides something.
  (PyArrow only picks the literal; it does not vote.)

ZPQ and DuckDB run the SQL directly. Hardwood has no query engine, so the harness exports the needed columns with
`hardwood convert --format json -c …` and evaluates the probe in Python with SQL semantics: NULLs ignored, and `sum`,
`min`, `max` over no values are NULL.

Float `min`/`max` are judged NaN-free: ZPQ skips NaN (as Arrow compute and Polars do), DuckDB orders NaN above every
number. The DuckDB query adds `FILTER (WHERE NOT isnan(col))` and the Hardwood evaluation drops NaN, so that convention
difference does not mask real decode errors.

**Referee Rule** (applied per result column):
- If DuckDB and Hardwood agree with each other and ZPQ differs, `FAIL`.
- With only one oracle answering (Hardwood absent or erroring, or DuckDB erroring), ZPQ must match it, else `FAIL`; a
  match with one oracle rejecting the file is `INFO`.
- If Hardwood and DuckDB disagree with each other, it is reported as `INFO` with all three answers.
- ZPQ rejecting a file another engine reads is a `FAIL`; if all engines reject it, it is intentionally malformed and
  passes.

### Referee overrides
Majority voting accepts a wrong answer when ZPQ and DuckDB share a bug — DuckDB is lenient where the spec is strict
(for example, it reads files whose column chunks contradict the schema), and it can share a statistics-handling bug
with ZPQ. `REFEREE_OVERRIDES` at the top of `tools/triangulate.py` takes such a file out of the vote, keyed by its
label:

```python
REFEREE_OVERRIDES = {
    "hardwood:compat_chunk_path_swapped.parquet": {
        "authority": "hardwood",   # Hardwood alone decides, including whether the file must be rejected
        "why": "chunk path_in_schema contradicts the schema's leaf order; a reader must reject, not guess",
    },
    "parquet-testing:example.parquet": {
        "expected": {"total_rows": 10, "x_gt_count": 4},  # hand-verified answers; beat every engine
        "why": "deprecated min/max stats are signed-ordered; DuckDB prunes on them",
    },
}
```

- `"authority": "hardwood"` — for strict-spec cases. DuckDB still runs and its answer is shown, but it gets no vote.
- `"expected"` — per result column (the probe's alias, e.g. `x_sum`, `total_rows`, `x_gt_count`); use when no engine
  can be trusted or Hardwood may be absent. `None` means SQL NULL.
- `"why"` is required and printed with any non-OK result. Keep entries narrow: an override switches a referee off.

## Value Equality
Hardwood 1.1's JSON export is typed for numbers and booleans and renders the rest as text; when one side of a pair is
text, it is coerced toward the other side's type before comparing:
- **Decimals:** ZPQ decodes DECIMALS to `f64`. Hardwood prints plain decimal strings; DuckDB returns `Decimal`. All
  compare as floats.
- **Floats:** Compared using a relative tolerance ($1e-4$) to ignore precision jitter. `NaN` and `±Infinity` (strings
  in Hardwood's JSON) compare by token.
- **Temporals:** ISO-8601 text with a `T` separator, trailing fractional zeros trimmed, UTC offset dropped. Hardwood
  renders INT96 and UTC-adjusted timestamps as instants with a trailing `Z`; DuckDB runs with `TimeZone='UTC'`.
- **Binary:** rendered the way Hardwood does (strict UTF-8 without control characters is text, otherwise `0x` hex).
- **Nested:** compared structurally; Hardwood emits native JSON lists and objects.
- **Nulls:** JSON `null`. Hardwood's CSV export writes an empty field for null (`--null-string` overrides), which CSV
  itself cannot tell apart from an empty string, so the harness reads JSON, where null has its own spelling.

## Soft Hardwood Dependency
Hardwood is treated as a soft dependency to maintain project philosophy ("if we can't fix it, we don't use it"). If the
CLI is not found, the script warns and degrades to a 2-engine (ZPQ vs DuckDB) verification without failing the run.
