package dev.zpq.spark;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnVector;

import java.io.IOException;
import java.util.Map;

/** Arrow Java C Data imports, borrowed by Spark until next()/close(). No row materialization. */
final class ZpqArrowReader implements PartitionReader<ColumnarBatch> {
    private final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    private final StructType schema;
    private final String request;
    private final ZpqNative nativeCtx;
    private ZpqNative.ArrowCall call;
    private ColumnarBatch batch;

    ZpqArrowReader(ZpqInputPartition part, StructType schema, Map<String, String> options,
                   String filter, boolean aggregation, String groups, String aggregate) {
        this.schema = schema;
        nativeCtx = ZpqNative.getInstance(options.get("zpq.lib"), options.get("zpq.context.max_memory"));
        StringBuilder b = new StringBuilder("{\"inputs\":[");
        ZpqJson.appendQuoted(part.filePath(), b);
        b.append("]");
        if (part.startRg() >= 0) b.append(",\"row_groups\":[[").append(part.startRg()).append(',').append(part.endRg()).append("]]");
        b.append(",\"parallelism\":").append(Math.max(1, part.parallelism()));
        b.append(",\"max_memory\":");
        ZpqJson.appendQuoted(options.getOrDefault("zpq.task.max_memory", "1GB"), b);
        b.append(",\"cache\":").append(Boolean.parseBoolean(options.getOrDefault("zpq.cache", "true")));
        if (filter != null && !filter.isEmpty()) { b.append(",\"filter\":"); ZpqJson.appendQuoted(filter, b); }
        if (aggregation) {
            if (groups != null && !groups.isEmpty()) { b.append(",\"group_by\":"); ZpqJson.appendQuoted(groups, b); }
            if (aggregate != null && !aggregate.isEmpty()) { b.append(",\"aggregate\":"); ZpqJson.appendQuoted(aggregate, b); }
        } else {
            b.append(",\"columns\":[");
            for (int i = 0; i < schema.length(); i++) {
                if (i > 0) b.append(',');
                ZpqJson.appendQuoted(schema.fields()[i].name(), b);
            }
            b.append(']');
        }
        request = b.append('}').toString();
    }

    @Override public boolean next() throws IOException {
        try {
            if (call == null) call = nativeCtx.openArrow(request, allocator);
            if (!call.reader.loadNextBatch()) { batch = null; return false; }
            VectorSchemaRoot root = call.reader.getVectorSchemaRoot();
            if (root.getFieldVectors().size() != schema.length()) throw new IOException("Arrow result column count differs from Spark scan schema");
            ColumnVector[] vectors = new ColumnVector[schema.length()];
            for (int i = 0; i < vectors.length; i++) {
                final var v = root.getVector(i);
                final var expected = schema.fields()[i].dataType();
                vectors[i] = new ZpqArrowColumnVector(v, expected);
            }
            batch = new ColumnarBatch(vectors, root.getRowCount());
            return true;
        } catch (Throwable t) {
            try { close(); } catch (Throwable cleanup) { t.addSuppressed(cleanup); }
            if (t instanceof IOException e) throw e;
            throw new IOException("zpq Arrow scan failed", t);
        }
    }

    @Override public ColumnarBatch get() { return batch; }

    @Override public void close() throws IOException {
        try {
            if (call != null) { var previous = call; call = null; previous.close(); }
        } finally { batch = null; allocator.close(); }
    }
}
