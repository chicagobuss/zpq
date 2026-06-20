# Triangulation Correctness Harness

The triangulation correctness harness (`tools/triangulate.py`) acts as an automated 3-engine referee between:
1. **ZPQ** (The engine under test)
2. **Hardwood** (The strict-reader Java oracle)
3. **DuckDB** (The C++ analytics referee)

It is invoked via `just triangulate` and serves as a Tier 3 (gauntlet) test.

## Design

The harness tests two independent directions:

### ZPQ write correctness
We generate a broad input matrix of Parquet files using PyArrow. This matrix covers all ZPQ-supported types (integers, floats, decimals, strings, timestamps, booleans), nullable permutations, and edge values (nulls, empty strings, single-distinct values). 

For each file, ZPQ performs a read-then-write roundtrip across all output codecs (`snappy`, `zstd`, `uncompressed`). Both Hardwood and DuckDB then attempt to read ZPQ's output. 

**Rule:** A file rejected by an external reader, or where values mismatch the generator's ground truth, is an immediate `FAIL`. (This catches corrupted dictionary encodings where ZPQ's own reader might accept its broken writes).

### ZPQ read correctness
We read from the `apache/parquet-testing` corpus and Hardwood's own test fixtures (`references/hardwood/core/src/test/resources/`).

For each file, we compute aggregate values (sums, counts, min/max depending on column type) using all three engines.

**Referee Rule:** 
- A value is "correct" when $\ge 2$ of the 3 engines agree. 
- If ZPQ disagrees with the majority, it is reported as a `FAIL`.
- If Hardwood and DuckDB disagree but ZPQ matches one, it is reported as `INFO` (not a failure).
- If all three engines fail to parse a file, it is considered intentionally malformed (error-path fixture) and passes.

## Value Equality
Value equality is complex due to engine-specific design choices:
- **Decimals:** ZPQ decodes DECIMALS to `f64`. We normalize DuckDB/Hardwood decimals to `f64` for comparison.
- **Floats:** Compared using a relative tolerance ($1e-4$) to ignore precision jitter. `NaN`s are treated as explicitly equal.
- **Timestamps:** Converted to ISO strings to avoid timezone shift false-positives.

## Soft Hardwood Dependency
Hardwood is treated as a soft dependency to maintain project philosophy ("if we can't fix it, we don't use it"). If the prebuilt Hardwood CLI is not found in `$HARDWOOD`, `tools/hardwood/`, or `$PATH`, the script gracefully degrades to a 2-engine (ZPQ vs DuckDB) verification without failing the run.
