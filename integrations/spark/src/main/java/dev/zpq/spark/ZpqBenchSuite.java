package dev.zpq.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Phase 4 Informal Benchmark Runner:
 * - Mode "warm": runs warm in-session median-of-3 timings on local[16] for Parquet vs zpq,
 *   recording libzpq native execution time, JVM JSON parse time, and response bytes,
 *   plus the NDJSON-vs-Parquet-handoff row scan comparison.
 * - Mode "single <parquet|zpq> <queryId>": runs a single query once for fresh spark-submit timing.
 */
public final class ZpqBenchSuite {

    public record BenchQuery(String id, String sqlTemplate, String cliSqlTemplate) {}

    public static final Map<String, BenchQuery> QUERIES = new LinkedHashMap<>();
    static {
        add(new BenchQuery(
            "CB_Q00",
            "SELECT COUNT(*) AS c FROM {t}",
            "SELECT COUNT(*) AS c FROM '{glob}'"
        ));
        add(new BenchQuery(
            "CB_Q01",
            "SELECT COUNT(*) AS c FROM {t} WHERE AdvEngineID <> 0",
            "SELECT COUNT(*) AS c FROM '{glob}' WHERE AdvEngineID <> 0"
        ));
        add(new BenchQuery(
            "CB_Q02",
            "SELECT SUM(AdvEngineID) AS s, COUNT(*) AS c, AVG(ResolutionWidth) AS a FROM {t}",
            "SELECT SUM(AdvEngineID) AS s, COUNT(*) AS c, AVG(ResolutionWidth) AS a FROM '{glob}'"
        ));
        add(new BenchQuery(
            "CB_Q06",
            "SELECT MIN(EventDate) AS min_d, MAX(EventDate) AS max_d FROM {t}",
            "SELECT MIN(EventDate) AS min_d, MAX(EventDate) AS max_d FROM '{glob}'"
        ));
        add(new BenchQuery(
            "CB_Q07",
            "SELECT AdvEngineID, COUNT(*) AS c FROM {t} WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY c DESC, AdvEngineID ASC",
            "SELECT AdvEngineID, COUNT(*) AS c FROM '{glob}' WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY c DESC, AdvEngineID ASC"
        ));
        add(new BenchQuery(
            "CB_Q10_pushed",
            "SELECT RegionID, SUM(AdvEngineID) AS s, COUNT(*) AS c, AVG(ResolutionWidth) AS a, MIN(ResolutionWidth) AS mn, MAX(ResolutionWidth) AS mx FROM {t} GROUP BY RegionID ORDER BY c DESC, RegionID ASC LIMIT 10",
            "SELECT RegionID, SUM(AdvEngineID) AS s, COUNT(*) AS c, AVG(ResolutionWidth) AS a, MIN(ResolutionWidth) AS mn, MAX(ResolutionWidth) AS mx FROM '{glob}' GROUP BY RegionID ORDER BY c DESC, RegionID ASC LIMIT 10"
        ));
        add(new BenchQuery(
            "CB_Q20",
            "SELECT COUNT(*) AS c FROM {t} WHERE URL LIKE '%google%'",
            "SELECT COUNT(*) AS c FROM '{glob}' WHERE URL LIKE '%google%'"
        ));
        add(new BenchQuery(
            "CB_Q21",
            "SELECT SearchPhrase, MIN(URL) AS min_url, COUNT(*) AS c FROM {t} WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC, SearchPhrase ASC LIMIT 10",
            "SELECT SearchPhrase, MIN(URL) AS min_url, COUNT(*) AS c FROM '{glob}' WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC, SearchPhrase ASC LIMIT 10"
        ));
        add(new BenchQuery(
            "CB_Q28",
            "SELECT CounterID, AVG(ResolutionWidth) AS a, COUNT(*) AS c FROM {t} WHERE ResolutionWidth > 0 GROUP BY CounterID HAVING c > 10000 ORDER BY c DESC, CounterID ASC LIMIT 10",
            "SELECT CounterID, AVG(ResolutionWidth) AS a, COUNT(*) AS c FROM '{glob}' WHERE ResolutionWidth > 0 GROUP BY CounterID HAVING c > 10000 ORDER BY c DESC, CounterID ASC LIMIT 10"
        ));
        add(new BenchQuery(
            "CB_Q36_rows",
            "SELECT WatchID, ClientIP, EventTime, URL, ResolutionWidth FROM {t} WHERE UserID = 1508127196834704092 ORDER BY EventTime ASC, WatchID ASC",
            "SELECT WatchID, ClientIP, EventTime, URL, ResolutionWidth FROM '{glob}' WHERE UserID = 1508127196834704092 ORDER BY EventTime ASC, WatchID ASC"
        ));
    }

    private static void add(BenchQuery q) {
        QUERIES.put(q.id(), q);
    }

    public static void main(String[] args) {
        String mode = args.length > 0 ? args[0] : "warm";
        // The ClickBench inputs, under the same output directory build.sh uses.
        String outDir = System.getenv().getOrDefault("ZPQ_SPARK_OUT_DIR", "zig-out/spark");
        String dataPath = outDir + "/data/clickbench";

        SparkSession spark = SparkSession.builder()
            .master("local[16]")
            .appName("ZpqBenchSuite-" + mode)
            .config("spark.ui.enabled", "false")
            .config("spark.sql.parquet.binaryAsString", "true")
            .config("spark.sql.shuffle.partitions", "16")
            .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        try {
            if ("single".equals(mode)) {
                String engine = args[1];
                String qid = args[2];
                BenchQuery q = QUERIES.get(qid);
                if (q == null) {
                    throw new IllegalArgumentException("Unknown queryId: " + qid);
                }
                long t0 = System.nanoTime();
                if ("parquet".equals(engine)) {
                    spark.read().parquet(dataPath).createOrReplaceTempView("t");
                } else if ("zpq".equals(engine)) {
                    spark.read().format("zpq").load(dataPath).createOrReplaceTempView("t");
                } else {
                    throw new IllegalArgumentException("Unknown engine: " + engine);
                }
                List<Row> rows = spark.sql(q.sqlTemplate().replace("{t}", "t")).collectAsList();
                long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
                System.out.printf("SINGLE_RESULT|%s|%s|rows=%d|in_jvm_ms=%d%n", engine, qid, rows.size(), elapsedMs);
                return;
            }

            runWarmSuite(spark, dataPath);
        } finally {
            spark.stop();
        }
    }

    private static void runWarmSuite(SparkSession spark, String dataPath) {
        Dataset<Row> pqDf = spark.read().parquet(dataPath);
        pqDf.createOrReplaceTempView("hits_pq");

        Dataset<Row> zpqDf = spark.read().format("zpq").load(dataPath);
        zpqDf.createOrReplaceTempView("hits_zpq");

        Dataset<Row> zpqPqHandoffDf = spark.read().format("zpq").option("rowMode", "parquet").load(dataPath);
        zpqPqHandoffDf.createOrReplaceTempView("hits_zpq_pq");

        // Warm up both engines and the libzpq context
        spark.sql("SELECT COUNT(*) FROM hits_pq").collectAsList();
        spark.sql("SELECT COUNT(*) FROM hits_zpq").collectAsList();

        System.out.println("=== WARM IN-SESSION TIMINGS (local[16], median of 3) ===");
        System.out.printf("%-14s | %10s | %10s | %10s | %10s | %12s | %8s%n",
            "Query", "Parquet ms", "zpq ms", "Native ms", "Parse ms", "JSON Bytes", "Speedup");
        System.out.println("--------------------------------------------------------------------------------------------");

        for (BenchQuery q : QUERIES.values()) {
            String sqlPq = q.sqlTemplate().replace("{t}", "hits_pq");
            String sqlZpq = q.sqlTemplate().replace("{t}", "hits_zpq");

            // 1 warmup each
            spark.sql(sqlPq).collectAsList();
            spark.sql(sqlZpq).collectAsList();

            long[] pqTimes = new long[3];
            for (int i = 0; i < 3; i++) {
                long t0 = System.nanoTime();
                spark.sql(sqlPq).collectAsList();
                pqTimes[i] = (System.nanoTime() - t0) / 1_000_000L;
            }
            Arrays.sort(pqTimes);
            long pqMed = pqTimes[1];

            long[] zpqTimes = new long[3];
            long[] nativeTimes = new long[3];
            long[] parseTimes = new long[3];
            long respBytes = 0;
            for (int i = 0; i < 3; i++) {
                ZpqNative.resetTelemetry();
                long t0 = System.nanoTime();
                spark.sql(sqlZpq).collectAsList();
                zpqTimes[i] = (System.nanoTime() - t0) / 1_000_000L;
                nativeTimes[i] = ZpqNative.getTotalNativeNs() / 1_000_000L;
                parseTimes[i] = ZpqNative.getTotalParseNs() / 1_000_000L;
                respBytes = ZpqNative.getTotalResponseBytes();
            }
            Arrays.sort(zpqTimes);
            Arrays.sort(nativeTimes);
            Arrays.sort(parseTimes);
            long zpqMed = zpqTimes[1];
            double speedup = (double) pqMed / Math.max(1.0, (double) zpqMed);

            System.out.printf("%-14s | %10d | %10d | %10d | %10d | %12d | %7.2fx%n",
                q.id(), pqMed, zpqMed, nativeTimes[1], parseTimes[1], respBytes, speedup);
        }

        System.out.println();
        System.out.println("=== NDJSON vs COLUMNAR/PARQUET HANDOFF MEASUREMENT (local[16], median of 3) ===");
        String[][] handoffQueries = new String[][] {
            {
                "ROW_1.3K (Q36: 1,303 rows, 5 cols)",
                "SELECT WatchID, ClientIP, EventTime, URL, ResolutionWidth FROM {t} WHERE UserID = 1508127196834704092 ORDER BY EventTime ASC, WatchID ASC"
            },
            {
                "ROW_169K (AdvEngineID<>0: 168,733 rows, 3 cols, summed in Spark)",
                "SELECT COUNT(DISTINCT WatchID) FROM (SELECT WatchID, EventTime, ResolutionWidth FROM {t} WHERE AdvEngineID <> 0)"
            },
            {
                "ROW_4M (Q08: 4,000,000 rows, 2 cols, COUNT DISTINCT in Spark)",
                "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM {t} GROUP BY RegionID ORDER BY u DESC, RegionID ASC LIMIT 10"
            }
        };

        for (String[] hq : handoffQueries) {
            String label = hq[0];
            String tmpl = hq[1];
            String sPq = tmpl.replace("{t}", "hits_pq");
            String sNdjson = tmpl.replace("{t}", "hits_zpq");
            String sPqHandoff = tmpl.replace("{t}", "hits_zpq_pq");

            spark.sql(sPq).collectAsList();
            spark.sql(sNdjson).collectAsList();
            spark.sql(sPqHandoff).collectAsList();

            long[] tPq = new long[3];
            for (int i = 0; i < 3; i++) {
                long t0 = System.nanoTime();
                spark.sql(sPq).collectAsList();
                tPq[i] = (System.nanoTime() - t0) / 1_000_000L;
            }
            Arrays.sort(tPq);

            long[] tNdjson = new long[3];
            long ndNativeSumMs = 0;
            long ndParseSumMs = 0;
            long ndBytes = 0;
            for (int i = 0; i < 3; i++) {
                ZpqNative.resetTelemetry();
                long t0 = System.nanoTime();
                spark.sql(sNdjson).collectAsList();
                tNdjson[i] = (System.nanoTime() - t0) / 1_000_000L;
                ndNativeSumMs = ZpqNative.getTotalNativeNs() / 1_000_000L;
                ndParseSumMs = ZpqNative.getTotalParseNs() / 1_000_000L;
                ndBytes = ZpqNative.getTotalResponseBytes();
            }
            Arrays.sort(tNdjson);

            long[] tPqHandoff = new long[3];
            long pqhNativeSumMs = 0;
            for (int i = 0; i < 3; i++) {
                ZpqNative.resetTelemetry();
                long t0 = System.nanoTime();
                spark.sql(sPqHandoff).collectAsList();
                tPqHandoff[i] = (System.nanoTime() - t0) / 1_000_000L;
                pqhNativeSumMs = ZpqNative.getTotalNativeNs() / 1_000_000L;
            }
            Arrays.sort(tPqHandoff);

            System.out.printf("%s%n  Spark Parquet : %d ms%n  zpq NDJSON    : %d ms (task-sum native=%d ms, task-sum parse=%d ms, JSON=%d bytes)%n  zpq output_url: %d ms (task-sum native=%d ms)%n",
                label, tPq[1], tNdjson[1], ndNativeSumMs, ndParseSumMs, ndBytes, tPqHandoff[1], pqhNativeSumMs);
        }
    }
}
