# Writer-version statistics policy

Known writer defects make the affected statistics absent, including null counts.
Neither pruning nor `--trust-stats` answers may use them. Decoded values and
structural row counts keep their existing semantics. Chunk statistics and page
indexes use the same policy; one affected page drops its index as a whole.
Copy and recompression paths remove affected footer, page-header and index
statistics before an output can advertise another writer, including mixed inputs and a fully matching
filter copied beside encoded data. Raw input metadata remains available for inspection.

| Writer | Statistics declined |
| --- | --- |
| `parquet-cpp` before 1.3.0 | Non-signed-order columns, unless both encoded extrema are equal |
| `parquet-mr` before 1.10.0 | The same non-signed-order rule |
| `parquet-mr` before semantic version 1.8.0 | BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY, even with equal extrema or a CDH backport banner |

Unlike parquet-mr's CDH exception for `[1.5.0-cdh5.5.0, 1.5.0)`, zpq conservatively
rejects all pre-1.8 binary statistics, as Arrow does. Numeric version boundaries
for the ordering rule ignore suffixes, as Arrow does. The binary rule orders
prereleases and parquet-mr's extra suffix (`rc2`, for example) as parquet-mr does.
DECIMAL has signed numeric logical order; binary DECIMAL still faces the binary
corruption rule. Existing checks still refuse deprecated signed-byte extrema
where they do not express the type's order, even if equal. This policy only
removes statistics; it never makes previously unreadable bounds usable.

`created_by` parsing is allocation-free and case-sensitive. Exact application names,
ASCII whitespace, optional `(build ...)`, numeric major/minor/patch (omitted minor
or patch becomes zero), and rc/prerelease/build suffixes are recognized. Parsing
must succeed completely before a version is classified. A recognized application
(`parquet-mr` or `parquet-cpp`) with a missing or malformed version is **0.0.0**,
so the corresponding old-writer rules apply. Missing/empty `created_by` or an unknown
application means **modern**: no writer-specific suppression. This follows Arrow's
permissive unknown-application choice and conservative known-application fallback;
it deliberately differs from parquet-mr's conservative unknown-binary-writer rule.
It is not evidence that unknown writers' statistics are true. Other applications,
including `parquet-cpp-arrow`, zpq, DuckDB, Polars and Spark, get no special rule;
Spark files identifying as `parquet-mr` are checked by that advertised version.

Sources: Arrow's [ApplicationVersion parser and HasCorrectStatistics](https://github.com/apache/arrow/blob/main/cpp/src/parquet/metadata.cc),
[logical sort orders](https://github.com/apache/arrow/blob/main/cpp/src/parquet/types.cc),
and parquet-mr's [CorruptStatistics](https://github.com/apache/parquet-java/blob/master/parquet-column/src/main/java/org/apache/parquet/CorruptStatistics.java),
[SemanticVersion](https://github.com/apache/parquet-java/blob/master/parquet-common/src/main/java/org/apache/parquet/SemanticVersion.java)
and [CDH regression tests](https://github.com/apache/parquet-java/blob/master/parquet-column/src/test/java/org/apache/parquet/CorruptStatisticsTest.java).
These cover the PARQUET-686/PARQUET-1025 ordering era and PARQUET-251 binary corruption.

Tests construct synthetic statistics across boundary versions, malformed strings,
CDH variants, unsigned integers, binary and decimal annotations, equal/missing
extrema, and both trust modes. `ci/fixtures/parquet/writer_stats.parquet` contains
real PyArrow-encoded nullable STRING/FLBA DECIMAL/UINT32 pages with deliberately
false chunk and page statistics and a rewritten old-writer footer. It is synthetic,
not an artifact produced by an historical writer. The generator is
`write_writer_stats()` in `tools/gen_stats_order_fixtures.py`; PyArrow's writer does
not expose a `created_by` override, so the Compact Thrift footer is rewritten.
Core tests rewrite its writer strings, check pruning/count/min fallback and the
CDH/equal-extrema cases, and compare against `--scan-all` under tight budgets.
Modern-writer parity is also checked on valid generated data by
`tools/writer_statistics_check.py CANDIDATE BASE [POLARS_PYTHON]`. It tests actual
Arrow, DuckDB and zpq outputs, optional Polars output, and the checked-in Spark
INT96 fixture, in addition to rewritten writer strings. Every generated output
is read strictly with PyArrow; query answers compare with base bytes and DuckDB.

Copied pages keep their encoded bodies and CRCs. Affected header statistics are
removed with bounded stack scratch, without decompressing or buffering the page.
Chunk offsets and compressed/uncompressed totals follow changed header lengths;
optional offset/column indexes are dropped if the page layout changes. Recompressed
pages use the same statistics policy and drop the old payload CRC. The source
policy is runtime provenance, retained through parallel recompression, and is never
serialized as a new footer field. Modern-writer copies retain their existing path.
`writer_headers_v1.parquet` and `writer_headers_v2.parquet` pin both data-page
versions, dictionary offsets, checksums, and false header/footer bounds. Their
bodies are PyArrow encoded; `write_writer_headers()` rewrites only metadata.
