package dev.zpq.spark;

import org.apache.spark.sql.connector.read.InputPartition;

/**
 * Serializable Spark InputPartition representing either one row-group range [startRg, endRg)
 * of a Parquet file (for structured requests) or a whole file (startRg == -1, for SQL requests).
 */
public record ZpqInputPartition(
    String filePath,
    int startRg,
    int endRg,
    int parallelism
) implements InputPartition {}
