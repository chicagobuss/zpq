# zpq Spark DataSource V2 (`spark.read.format("zpq")`)

A `libzpq`-backed Apache Spark DataSource V2 (`org.apache.spark.sql.connector.catalog.TableProvider` + `DataSourceRegister`) that uses Java 22+ Foreign Function & Memory (`java.lang.foreign`) to run Parquet scans, filters, projections, and partial aggregations natively inside `libzpq`.

## Architecture

- **`ZpqDataSource` / `ZpqTable` (`TableProvider`, `SupportsRead`)**
  - Registers short name `"zpq"` via `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`.
  - Infers the Spark `StructType` from the first input Parquet file's footer using `ParquetFileReader` and `ParquetToSparkSchemaConverter` (respecting `spark.sql.parquet.binaryAsString`), caching file footers and row-group counts per JVM.
- **`ZpqNative` (One `zpq_ctx` per executor JVM)**
  - Lazily opens a process-wide `zpq_ctx` singleton via `zpq_open` using `java.lang.foreign.Linker.nativeLinker()` and registers a JVM shutdown hook that performs the two-phase `zpq_shutdown` + `zpq_close` sequence once in-flight calls drain.
  - Because `libzpq` reserves the full context memory ceiling when a request omits `"max_memory"`, each Spark task passes `"max_memory": "1GB"` (configurable via `.option("zpq.task.max_memory", ...)`) against a `"16GB"` context ceiling (`.option("zpq.context.max_memory", ...)`), allowing all 16 `local[16]` Spark tasks to execute concurrently on one shared `zpq_ctx`.
- **`ZpqScanBuilder` & `ZpqFilterTranslator` (Pushdown)**
  - `SupportsPushDownRequiredColumns`: prunes unreferenced columns before building the `libzpq` request.
  - `SupportsPushDownFilters`: translates Spark `Filter` trees (`EqualTo`, `EqualNullSafe`, `<`, `<=`, `>`, `>=`, `In`, `IsNull`, `IsNotNull`, `StringStartsWith`, `StringEndsWith`, `StringContains`, `And`, `Or`, `Not`) into `libzpq` filter syntax.
  - `SupportsPushDownAggregates`: pushes `COUNT(*)`, `COUNT(col)`, `SUM(col)`, `MIN(col)`, `MAX(col)`, `AVG(col)` (via Spark's partial `SUM` + `COUNT` rewrite on non-overflowing types), and `GROUP BY` columns into `libzpq` structured requests (`"group_by"`, `"aggregate"`). Spark merges the per-split partial aggregates.
- **`ZpqScan` & `ZpqPartitionReader` (Row-Group Splitting & Handoff)**
  - Structured aggregate and empty-schema count scans split input files into row-group slices using `ZpqPaths.splitRowGroups` (matching `tools/serverless/fanout.py`) and pass `"row_groups": [[startRg, endRg]]` to `libzpq`.
  - Row-returning scans execute `{"sql": "SELECT ... FROM 'file' WHERE ..."}` per file (or, with `.option("rowMode", "parquet")`, write filtered/projected Parquet slices via `"output_url"`).
  - `ZpqJson` parses `libzpq`'s JSON/NDJSON responses directly into Spark Catalyst `InternalRow`s, keeping exact `BigDecimal` precision/scale, `DateType` epoch days, `TimestampType` microseconds, and decoding `{"base64":"..."}` binary payloads losslessly.

## Requirements

- `libzpq.so` built from the repository root:
  ```sh
  zig build libzpq -Doptimize=ReleaseFast
  ```
- OpenJDK 22+ (tested with OpenJDK 25.0.5) for `java.lang.foreign` FFM.
- Apache Spark 4.x (tested with Spark 4.2.1).

## Building

```sh
./integrations/spark/build.sh
```

By default `build.sh` reads `ZPQ_SPARK_OUT_DIR` (default `zig-out/spark`), compiles against `$ZPQ_SPARK_HOME/jars/*`, and writes `$ZPQ_SPARK_OUT_DIR/zpq-spark.jar`.

## Usage

```java
Dataset<Row> df = spark.read()
    .format("zpq")
    .option("zpq.lib", "/path/to/zig-out/lib/libzpq.so")
    .load("/path/to/hits_*.parquet");

df.createOrReplaceTempView("hits");
spark.sql("SELECT AdvEngineID, COUNT(*) AS c FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY c DESC")
    .show();
```

Run `spark-submit` with `--enable-native-access=ALL-UNNAMED`:

```sh
spark-submit \
  --master "local[16]" \
  --conf "spark.driver.extraJavaOptions=--enable-native-access=ALL-UNNAMED" \
  --conf "spark.executor.extraJavaOptions=--enable-native-access=ALL-UNNAMED" \
  --jars /path/to/zpq-spark.jar \
  ...
```

## Correctness Suite

`integrations/spark/check_correctness.sh` builds the JAR and runs `dev.zpq.spark.ZpqCorrectnessSuite`, executing 15 queries (13 ClickBench queries + 2 exact `DATE` and `DECIMAL` queries) through both `spark.read.parquet` and `spark.read.format("zpq")`. It sorts both result sets canonically, compares every cell (including exact `BigDecimal` value + scale and exact `Date` equality), and exits non-zero on any mismatch:

```sh
./integrations/spark/check_correctness.sh
```

## Benchmark Suite

`dev.zpq.spark.ZpqBenchSuite` runs the Phase 4 informal timings (`local[16]`, median of 3) and the NDJSON-vs-`output_url` handoff measurement:

```sh
# Warm in-session timings + NDJSON-vs-columnar breakdown:
spark-submit --master "local[16]" \
  --conf "spark.driver.extraJavaOptions=--enable-native-access=ALL-UNNAMED" \
  --class dev.zpq.spark.ZpqBenchSuite \
  zig-out/spark/zpq-spark.jar warm

# Single cold query execution (engine = parquet | zpq):
spark-submit --master "local[16]" \
  --conf "spark.driver.extraJavaOptions=--enable-native-access=ALL-UNNAMED" \
  --class dev.zpq.spark.ZpqBenchSuite \
  zig-out/spark/zpq-spark.jar single zpq CB_Q20
```
