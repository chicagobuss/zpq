package dev.zpq.spark;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

/**
 * Factory creating {@link ZpqPartitionReader} instances on Spark executors.
 */
public final class ZpqPartitionReaderFactory implements PartitionReaderFactory {
    private final StructType readSchema;
    private final Map<String, String> options;
    private final String zpqFilter;
    private final boolean hasPushedAggregation;
    private final String zpqGroupBy;
    private final String zpqAggregate;
    private final int numGroups;
    private final int numAggs;

    public ZpqPartitionReaderFactory(
        StructType readSchema,
        Map<String, String> options,
        String zpqFilter,
        boolean hasPushedAggregation,
        String zpqGroupBy,
        String zpqAggregate,
        int numGroups,
        int numAggs
    ) {
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
    public boolean supportColumnarReads(InputPartition partition) {
        return !readSchema.isEmpty() && "arrow".equalsIgnoreCase(options.getOrDefault("rowMode", "arrow"));
    }

    @Override
    public PartitionReader<org.apache.spark.sql.vectorized.ColumnarBatch> createColumnarReader(InputPartition partition) {
        return new ZpqArrowReader((ZpqInputPartition) partition, readSchema, options,
            zpqFilter, hasPushedAggregation, zpqGroupBy, zpqAggregate);
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return new ZpqPartitionReader(
            (ZpqInputPartition) partition,
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
