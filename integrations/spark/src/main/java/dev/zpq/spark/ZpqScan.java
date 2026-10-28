package dev.zpq.spark;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Spark DataSource V2 Scan and Batch planning row-group or file splits for libzpq.
 */
public final class ZpqScan implements Scan, Batch {
    private final List<String> files;
    private final StructType readSchema;
    private final Map<String, String> options;
    private final String zpqFilter;
    private final boolean hasPushedAggregation;
    private final String zpqGroupBy;
    private final String zpqAggregate;
    private final int numGroups;
    private final int numAggs;

    public ZpqScan(
        List<String> files,
        StructType readSchema,
        Map<String, String> options,
        String zpqFilter,
        boolean hasPushedAggregation,
        String zpqGroupBy,
        String zpqAggregate,
        int numGroups,
        int numAggs
    ) {
        this.files = files;
        this.readSchema = readSchema;
        this.options = options;
        this.zpqFilter = zpqFilter;
        this.hasPushedAggregation = hasPushedAggregation;
        this.zpqGroupBy = zpqGroupBy;
        this.zpqAggregate = zpqAggregate;
        this.numGroups = numGroups;
        this.numAggs = numAggs;
    }

    @Override
    public StructType readSchema() {
        return readSchema;
    }

    @Override
    public String description() {
        return "ZpqScan[files=" + files.size()
            + ", pushedFilter=" + (zpqFilter != null ? zpqFilter : "NONE")
            + ", pushedGroupBy=" + (zpqGroupBy != null ? zpqGroupBy : "NONE")
            + ", pushedAgg=" + (zpqAggregate != null ? zpqAggregate : "NONE")
            + ", readSchema=" + readSchema.simpleString() + "]";
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        int maxSplits = Integer.parseInt(options.getOrDefault("maxSplits", "16"));
        String rowMode = options.getOrDefault("rowMode", "arrow");

        // Structured requests (aggregates, empty-schema count scans, or parquet output_url scans)
        // split across row-group ranges using the row_groups request field (as in fanout.py).
        if (hasPushedAggregation || readSchema.isEmpty() || "parquet".equalsIgnoreCase(rowMode) || "arrow".equalsIgnoreCase(rowMode)) {
            List<ZpqPaths.RowGroupSlice> slices = ZpqPaths.splitRowGroups(files, maxSplits);
            int perSplitParallelism = Math.max(1, 16 / Math.max(1, slices.size()));
            InputPartition[] parts = new InputPartition[slices.size()];
            for (int i = 0; i < slices.size(); i++) {
                ZpqPaths.RowGroupSlice s = slices.get(i);
                parts[i] = new ZpqInputPartition(
                    s.filePath(),
                    s.startRg(),
                    s.endRg(),
                    perSplitParallelism
                );
            }
            return parts;
        }

        // SQL NDJSON row scans run one partition per file with internal parallelism scaled to use 16 threads.
        int perFileParallelism = Math.max(1, 16 / Math.max(1, files.size()));
        List<InputPartition> parts = new ArrayList<>(files.size());
        for (String f : files) {
            parts.add(new ZpqInputPartition(f, -1, -1, perFileParallelism));
        }
        return parts.toArray(new InputPartition[0]);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new ZpqPartitionReaderFactory(
            readSchema,
            options,
            zpqFilter,
            hasPushedAggregation,
            zpqGroupBy,
            zpqAggregate,
            numGroups,
            numAggs
        );
    }
}
