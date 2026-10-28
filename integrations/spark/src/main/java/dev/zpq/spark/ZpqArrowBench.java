package dev.zpq.spark;

import org.apache.spark.sql.SparkSession;
import java.util.Arrays;
import java.util.LinkedHashMap;

/** Same warm local[16] queries as the Spark probe, including its unpushed 4M-row scan. */
public final class ZpqArrowBench {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[16]").appName("ZpqArrowBench")
            .config("spark.ui.enabled", "false").config("spark.sql.parquet.binaryAsString", "true")
            .config("spark.sql.shuffle.partitions", "16").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        try {
            spark.read().parquet(args[0]).createOrReplaceTempView("pq");
            spark.read().format("zpq").option("rowMode", "ndjson").load(args[0]).createOrReplaceTempView("ndjson");
            spark.read().format("zpq").option("rowMode", "arrow").load(args[0]).createOrReplaceTempView("arrow");
            var queries = new LinkedHashMap<String, String>();
            for (var q : ZpqBenchSuite.QUERIES.values()) queries.put(q.id(), q.sqlTemplate());
            queries.put("ROW_169K", "SELECT COUNT(DISTINCT WatchID) FROM (SELECT WatchID, EventTime, ResolutionWidth FROM {t} WHERE AdvEngineID <> 0)");
            queries.put("ROW_4M", "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM {t} GROUP BY RegionID ORDER BY u DESC, RegionID ASC LIMIT 10");
            System.out.println("QUERY|PARQUET_MS|NDJSON_MS|ARROW_MS|NDJSON_OVER_PARQUET|ARROW_OVER_PARQUET");
            for (var q : queries.entrySet()) {
                String[] engines = {"pq", "ndjson", "arrow"};
                for (String engine : engines) spark.sql(q.getValue().replace("{t}", engine)).collectAsList();
                long[][] times = new long[3][3];
                // Alternate engines within each repetition to reduce drift during an informal desktop run.
                for (int rep = 0; rep < 3; rep++) for (int e = 0; e < engines.length; e++) {
                    long start = System.nanoTime();
                    spark.sql(q.getValue().replace("{t}", engines[e])).collectAsList();
                    times[e][rep] = (System.nanoTime() - start) / 1_000_000;
                }
                for (var t : times) Arrays.sort(t);
                System.out.printf("%s|%d|%d|%d|%.3f|%.3f%n", q.getKey(), times[0][1], times[1][1], times[2][1],
                    (double)times[1][1]/times[0][1], (double)times[2][1]/times[0][1]);
            }
        } finally { spark.stop(); }
    }
}
