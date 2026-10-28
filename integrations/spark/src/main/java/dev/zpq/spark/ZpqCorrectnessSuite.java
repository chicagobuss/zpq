package dev.zpq.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Phase 3 Correctness Runner: runs ClickBench queries plus exact DECIMAL and DATE queries
 * through both Spark's built-in Parquet reader and the libzpq-backed DataSource V2 ("zpq"),
 * asserting identical results after sorting (with exact BigDecimal value+scale and Date checks).
 * Exits non-zero on any mismatch.
 */
public final class ZpqCorrectnessSuite {

    private record QuerySpec(String id, String description, String sqlTemplate) {}

    public static void main(String[] args) {
        String clickbenchPath = args.length > 0
            ? args[0]
            : "zig-out/spark/data/clickbench";
        String decDatePath = args.length > 1
            ? args[1]
            : "zig-out/spark/data/decimal_dates";

        SparkSession spark = SparkSession.builder()
            .master("local[16]")
            .appName("ZpqCorrectnessSuite")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.parquet.binaryAsString", "true")
            .config("spark.sql.shuffle.partitions", "16")
            .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        try {
            ensureDecimalDateFixture(spark, clickbenchPath, decDatePath);

            Dataset<Row> pqHits = spark.read().parquet(clickbenchPath);
            pqHits.createOrReplaceTempView("hits_pq");

            Dataset<Row> zpqHits = spark.read().format("zpq").load(clickbenchPath);
            zpqHits.createOrReplaceTempView("hits_zpq");

            Dataset<Row> pqDec = spark.read().parquet(decDatePath);
            pqDec.createOrReplaceTempView("dec_pq");

            Dataset<Row> zpqDec = spark.read().format("zpq").load(decDatePath);
            zpqDec.createOrReplaceTempView("dec_zpq");

            List<QuerySpec> queries = List.of(
                new QuerySpec(
                    "CB_Q00",
                    "ClickBench Q0: Unfiltered COUNT(*)",
                    "SELECT COUNT(*) AS c FROM {hits}"
                ),
                new QuerySpec(
                    "CB_Q01",
                    "ClickBench Q1: Filtered COUNT(*) (AdvEngineID <> 0)",
                    "SELECT COUNT(*) AS c FROM {hits} WHERE AdvEngineID <> 0"
                ),
                new QuerySpec(
                    "CB_Q02",
                    "ClickBench Q2: Scalar SUM, COUNT(*), AVG",
                    "SELECT SUM(AdvEngineID) AS s, COUNT(*) AS c, AVG(ResolutionWidth) AS a FROM {hits}"
                ),
                new QuerySpec(
                    "CB_Q06",
                    "ClickBench Q6: Scalar MIN and MAX on EventDate",
                    "SELECT MIN(EventDate) AS min_d, MAX(EventDate) AS max_d FROM {hits}"
                ),
                new QuerySpec(
                    "CB_Q07",
                    "ClickBench Q7: Filtered GROUP BY AdvEngineID with ORDER BY",
                    "SELECT AdvEngineID, COUNT(*) AS c FROM {hits} WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY c DESC, AdvEngineID ASC"
                ),
                new QuerySpec(
                    "CB_Q08_top10",
                    "ClickBench Q8: RegionID GROUP BY with COUNT(DISTINCT UserID) (column pruning + Spark distinct agg)",
                    "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM {hits} GROUP BY RegionID ORDER BY u DESC, RegionID ASC LIMIT 10"
                ),
                new QuerySpec(
                    "CB_Q10_pushed",
                    "ClickBench Q10 (non-distinct): RegionID GROUP BY with SUM, COUNT, AVG, MIN, MAX",
                    "SELECT RegionID, SUM(AdvEngineID) AS s, COUNT(*) AS c, AVG(ResolutionWidth) AS a, MIN(ResolutionWidth) AS mn, MAX(ResolutionWidth) AS mx FROM {hits} GROUP BY RegionID ORDER BY c DESC, RegionID ASC LIMIT 10"
                ),
                new QuerySpec(
                    "CB_Q20",
                    "ClickBench Q20: String LIKE filter pushdown + COUNT(*)",
                    "SELECT COUNT(*) AS c FROM {hits} WHERE URL LIKE '%google%'"
                ),
                new QuerySpec(
                    "CB_Q21",
                    "ClickBench Q21: Compound LIKE + inequality filter + GROUP BY SearchPhrase + MIN(URL)",
                    "SELECT SearchPhrase, MIN(URL) AS min_url, COUNT(*) AS c FROM {hits} WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC, SearchPhrase ASC LIMIT 10"
                ),
                new QuerySpec(
                    "CB_Q24",
                    "ClickBench Q24: Filtered SearchPhrase GROUP BY + COUNT(*) top 10",
                    "SELECT SearchPhrase, COUNT(*) AS c FROM {hits} WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC, SearchPhrase ASC LIMIT 10"
                ),
                new QuerySpec(
                    "CB_Q28",
                    "ClickBench Q28: Filtered CounterID GROUP BY with AVG, COUNT(*), and HAVING",
                    "SELECT CounterID, AVG(ResolutionWidth) AS a, COUNT(*) AS c FROM {hits} WHERE ResolutionWidth > 0 GROUP BY CounterID HAVING c > 10000 ORDER BY c DESC, CounterID ASC LIMIT 10"
                ),
                new QuerySpec(
                    "CB_Q33",
                    "ClickBench Q33: Multi-column GROUP BY (WatchID, ClientIP) with COUNT, SUM, AVG",
                    "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh) AS s, AVG(ResolutionWidth) AS a FROM {hits} WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC, WatchID ASC, ClientIP ASC LIMIT 10"
                ),
                new QuerySpec(
                    "CB_Q36_rows",
                    "ClickBench selective row scan: projection + UserID equality filter (1,303 rows)",
                    "SELECT WatchID, ClientIP, EventTime, URL, ResolutionWidth FROM {hits} WHERE UserID = 1508127196834704092 ORDER BY EventTime ASC, WatchID ASC"
                ),
                new QuerySpec(
                    "DEC_DATE_Q1_agg",
                    "Exact DATE & DECIMAL aggregate pushdown: GROUP BY DATE with COUNT, SUM(DECIMAL), MIN(DECIMAL), MAX(DECIMAL), AVG(DECIMAL)",
                    "SELECT EventDate_date, COUNT(*) AS c, SUM(PriceDec) AS sum_p, MIN(PriceDec) AS min_p, MAX(PriceDec) AS max_p, AVG(HighPrecDec) AS avg_hp FROM {dec} WHERE EventDate_date >= DATE '2013-07-15' AND EventDate_date <= DATE '2013-07-25' GROUP BY EventDate_date ORDER BY EventDate_date ASC"
                ),
                new QuerySpec(
                    "DEC_DATE_Q2_rows",
                    "Exact DATE & DECIMAL row scan with DATE and DECIMAL filter pushdown",
                    "SELECT WatchID, EventDate_date, PriceDec, HighPrecDec, RegionID FROM {dec} WHERE EventDate_date = DATE '2013-07-15' AND PriceDec > 15.00 ORDER BY WatchID ASC LIMIT 50"
                )
            );

            int passed = 0;
            int failed = 0;

            System.out.println("================================================================================");
            System.out.println("zpq Spark DataSource V2 Correctness Suite (" + queries.size() + " queries)");
            System.out.println("ClickBench path : " + clickbenchPath);
            System.out.println("Decimal/Date path: " + decDatePath);
            System.out.println("================================================================================");

            for (QuerySpec q : queries) {
                String sqlPq = q.sqlTemplate()
                    .replace("{hits}", "hits_pq")
                    .replace("{dec}", "dec_pq");
                String sqlZpq = q.sqlTemplate()
                    .replace("{hits}", "hits_zpq")
                    .replace("{dec}", "dec_zpq");

                long t0 = System.nanoTime();
                Dataset<Row> dfPq = spark.sql(sqlPq);
                List<Row> rowsPq = new ArrayList<>(dfPq.collectAsList());
                long pqMs = (System.nanoTime() - t0) / 1_000_000L;

                long t1 = System.nanoTime();
                Dataset<Row> dfZpq = spark.sql(sqlZpq);
                List<Row> rowsZpq = new ArrayList<>(dfZpq.collectAsList());
                long zpqMs = (System.nanoTime() - t1) / 1_000_000L;

                rowsPq.sort(ROW_COMPARATOR);
                rowsZpq.sort(ROW_COMPARATOR);

                String mismatch = compareRowLists(rowsPq, rowsZpq);
                if (mismatch == null) {
                    passed++;
                    System.out.printf("[PASS] %-16s | rows=%-5d | pq=%5d ms | zpq=%5d ms | %s%n",
                        q.id(), rowsZpq.size(), pqMs, zpqMs, q.description());
                } else {
                    failed++;
                    System.out.printf("[FAIL] %-16s | %s%n  Reason: %s%n",
                        q.id(), q.description(), mismatch);
                }
            }

            System.out.println("================================================================================");
            System.out.printf("Summary: %d/%d passed, %d failed%n", passed, queries.size(), failed);
            System.out.println("================================================================================");

            if (failed > 0) {
                System.exit(1);
            }
        } finally {
            spark.stop();
        }
    }

    private static void ensureDecimalDateFixture(SparkSession spark, String clickbenchPath, String decDatePath) {
        File dir = new File(decDatePath);
        File[] existing = dir.listFiles((d, name) -> name.endsWith(".parquet"));
        if (existing != null && existing.length > 0) {
            return;
        }
        String srcFile = clickbenchPath + "/hits_0.parquet";
        Dataset<Row> src = spark.read().parquet(srcFile);
        src.createOrReplaceTempView("hits_0_src");
        Dataset<Row> derived = spark.sql(
            "SELECT " +
            "  WatchID, " +
            "  RegionID, " +
            "  AdvEngineID, " +
            "  DATE_ADD(DATE '1970-01-01', CAST(EventDate AS INT)) AS EventDate_date, " +
            "  CAST(CAST(ParamPrice AS DECIMAL(12, 2)) + CAST(ResolutionWidth AS DECIMAL(12, 2)) / 100.0 AS DECIMAL(12, 2)) AS PriceDec, " +
            "  CAST(CAST(ResolutionWidth AS DECIMAL(18, 4)) / CAST(GREATEST(ResolutionHeight, 1) AS DECIMAL(18, 4)) AS DECIMAL(18, 4)) AS HighPrecDec " +
            "FROM hits_0_src " +
            "WHERE WatchID IS NOT NULL " +
            "LIMIT 200000"
        );
        derived.repartition(2).write().mode(SaveMode.Overwrite).parquet(decDatePath);
    }

    private static String compareRowLists(List<Row> expected, List<Row> actual) {
        if (expected.size() != actual.size()) {
            return "Row count mismatch: parquet=" + expected.size() + ", zpq=" + actual.size();
        }
        for (int r = 0; r < expected.size(); r++) {
            Row expRow = expected.get(r);
            Row actRow = actual.get(r);
            if (expRow.size() != actRow.size()) {
                return "Column count mismatch at row " + r + ": parquet=" + expRow.size() + ", zpq=" + actRow.size();
            }
            for (int c = 0; c < expRow.size(); c++) {
                Object e = expRow.get(c);
                Object a = actRow.get(c);
                String cellDiff = compareCell(e, a);
                if (cellDiff != null) {
                    return "Mismatch at row " + r + ", col " + c + ": " + cellDiff
                        + " (parquet row=" + expRow + ", zpq row=" + actRow + ")";
                }
            }
        }
        return null;
    }

    private static String compareCell(Object e, Object a) {
        if (e == null && a == null) {
            return null;
        }
        if (e == null || a == null) {
            return "null mismatch (parquet=" + e + ", zpq=" + a + ")";
        }
        if (e instanceof BigDecimal bdE && a instanceof BigDecimal bdA) {
            // Exact comparison including scale!
            if (!bdE.equals(bdA)) {
                return "BigDecimal mismatch (parquet=" + bdE.toPlainString() + " [scale=" + bdE.scale() + "], zpq="
                    + bdA.toPlainString() + " [scale=" + bdA.scale() + "])";
            }
            return null;
        }
        if (e instanceof Date dE && a instanceof Date dA) {
            if (!dE.equals(dA) || !dE.toString().equals(dA.toString())) {
                return "Date mismatch (parquet=" + dE + ", zpq=" + dA + ")";
            }
            return null;
        }
        if (e instanceof LocalDate ldE && a instanceof LocalDate ldA) {
            if (!ldE.equals(ldA)) {
                return "LocalDate mismatch (parquet=" + ldE + ", zpq=" + ldA + ")";
            }
            return null;
        }
        if (e instanceof byte[] bE && a instanceof byte[] bA) {
            if (!Arrays.equals(bE, bA)) {
                return "byte[] mismatch";
            }
            return null;
        }
        if (!Objects.equals(e, a)) {
            return "Value mismatch (parquet=" + e + " [" + e.getClass().getSimpleName()
                + "], zpq=" + a + " [" + a.getClass().getSimpleName() + "])";
        }
        return null;
    }

    private static final Comparator<Row> ROW_COMPARATOR = (r1, r2) -> {
        int n = Math.min(r1.size(), r2.size());
        for (int i = 0; i < n; i++) {
            int cmp = compareValues(r1.get(i), r2.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(r1.size(), r2.size());
    };

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compareValues(Object a, Object b) {
        if (a == b) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        if (a instanceof BigDecimal bdA && b instanceof BigDecimal bdB) {
            int c = bdA.compareTo(bdB);
            return c != 0 ? c : Integer.compare(bdA.scale(), bdB.scale());
        }
        if (a instanceof byte[] ba && b instanceof byte[] bb) {
            return Arrays.compare(ba, bb);
        }
        if (a instanceof Timestamp tsA && b instanceof Timestamp tsB) {
            return tsA.compareTo(tsB);
        }
        if (a instanceof Instant iA && b instanceof Instant iB) {
            return iA.compareTo(iB);
        }
        if (a instanceof LocalDateTime ldtA && b instanceof LocalDateTime ldtB) {
            return ldtA.compareTo(ldtB);
        }
        if (a instanceof Comparable ca && a.getClass().isInstance(b)) {
            return ca.compareTo(b);
        }
        return a.toString().compareTo(b.toString());
    }
}
