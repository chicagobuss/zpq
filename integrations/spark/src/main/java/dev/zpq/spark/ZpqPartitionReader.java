package dev.zpq.spark;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * PartitionReader that executes a libzpq request for one file / row-group slice
 * and yields Spark Catalyst {@link InternalRow}s.
 */
public final class ZpqPartitionReader implements PartitionReader<InternalRow> {
    private static final InternalRow EMPTY_ROW = new GenericInternalRow(new Object[0]);

    private final ZpqInputPartition part;
    private final StructType readSchema;
    private final Map<String, String> options;
    private final String zpqFilter;
    private final boolean hasPushedAggregation;
    private final String zpqGroupBy;
    private final String zpqAggregate;
    private final int numGroups;
    private final int numAggs;

    private boolean initialized = false;
    private Iterator<InternalRow> rowIterator;
    private InternalRow currentRow;
    private long remainingEmptyRows = -1;

    public ZpqPartitionReader(
        ZpqInputPartition part,
        StructType readSchema,
        Map<String, String> options,
        String zpqFilter,
        boolean hasPushedAggregation,
        String zpqGroupBy,
        String zpqAggregate,
        int numGroups,
        int numAggs
    ) {
        this.part = part;
        this.readSchema = readSchema;
        this.options = options;
        this.zpqFilter = zpqFilter;
        this.hasPushedAggregation = hasPushedAggregation;
        this.zpqGroupBy = zpqGroupBy;
        this.zpqAggregate = zpqAggregate;
        this.numGroups = numGroups;
        this.numAggs = numAggs;
    }

    private void ensureInitialized() {
        if (initialized) {
            return;
        }
        initialized = true;

        ZpqNative nativeCtx = ZpqNative.getInstance(
            options.get("zpq.lib"),
            options.get("zpq.context.max_memory")
        );
        String taskMaxMem = options.getOrDefault("zpq.task.max_memory", "1GB");
        boolean cache = Boolean.parseBoolean(options.getOrDefault("zpq.cache", "true"));

        if (hasPushedAggregation) {
            String req = buildStructuredAggRequest(taskMaxMem, cache, zpqGroupBy, zpqAggregate);
            String resp = nativeCtx.runJson(req);
            List<InternalRow> rows = ZpqJson.parseAggregateRows(resp, readSchema, numGroups, numAggs);
            this.rowIterator = rows.iterator();
            return;
        }

        if (readSchema.isEmpty()) {
            String req = buildStructuredAggRequest(taskMaxMem, cache, null, "count(*) AS _a0");
            String resp = nativeCtx.runJson(req);
            Map<String, Object> env = ZpqJson.parseObject(resp);
            if (env.containsKey("error")) {
                throw new RuntimeException("libzpq error response: " + resp);
            }
            Map<?, ?> agg = (Map<?, ?>) env.get("agg");
            Object cObj = agg.get("_a0");
            long count = (cObj instanceof ZpqJson.RawNumber rn)
                ? Long.parseLong(rn.text())
                : Long.parseLong(cObj.toString());
            this.remainingEmptyRows = count;
            return;
        }

        String rowMode = options.getOrDefault("rowMode", "ndjson");
        if ("parquet".equalsIgnoreCase(rowMode)) {
            List<InternalRow> rows = runParquetOutputUrlScan(nativeCtx, taskMaxMem, cache);
            this.rowIterator = rows.iterator();
            return;
        }

        String req = buildSqlScanRequest(taskMaxMem, cache);
        String resp = nativeCtx.runJson(req);
        List<InternalRow> rows = ZpqJson.parseSqlNdjsonRows(resp, readSchema);
        this.rowIterator = rows.iterator();
    }

    private String buildStructuredAggRequest(String taskMaxMem, boolean cache, String groupBy, String aggregate) {
        StringBuilder sb = new StringBuilder(256);
        sb.append("{\"inputs\":[");
        ZpqJson.appendQuoted(part.filePath(), sb);
        sb.append("]");
        if (part.startRg() >= 0 && part.endRg() >= 0) {
            sb.append(",\"row_groups\":[[").append(part.startRg()).append(",").append(part.endRg()).append("]]");
        }
        sb.append(",\"parallelism\":").append(Math.max(1, part.parallelism()));
        sb.append(",\"max_memory\":");
        ZpqJson.appendQuoted(taskMaxMem, sb);
        sb.append(",\"cache\":").append(cache ? "true" : "false");
        if (zpqFilter != null && !zpqFilter.isEmpty()) {
            sb.append(",\"filter\":");
            ZpqJson.appendQuoted(zpqFilter, sb);
        }
        if (groupBy != null && !groupBy.isEmpty()) {
            sb.append(",\"group_by\":");
            ZpqJson.appendQuoted(groupBy, sb);
        }
        if (aggregate != null && !aggregate.isEmpty()) {
            sb.append(",\"aggregate\":");
            ZpqJson.appendQuoted(aggregate, sb);
        }
        sb.append("}");
        return sb.toString();
    }

    private String buildSqlScanRequest(String taskMaxMem, boolean cache) {
        String sql = rowScanSql(part.filePath(), zpqFilter);

        StringBuilder req = new StringBuilder(sql.length() + 128);
        req.append("{\"sql\":");
        ZpqJson.appendQuoted(sql, req);
        req.append(",\"parallelism\":").append(Math.max(1, part.parallelism()));
        req.append(",\"max_memory\":");
        ZpqJson.appendQuoted(taskMaxMem, req);
        req.append(",\"cache\":").append(cache ? "true" : "false");
        req.append("}");
        return req.toString();
    }

    private String rowScanSql(String path, String filter) {
        StringBuilder sql = new StringBuilder("SELECT ");
        StructField[] fields = readSchema.fields();
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) sql.append(", ");
            sql.append(sqlQuoted(fields[i].name(), '"')).append(" AS _c").append(i);
        }
        sql.append(" FROM ").append(sqlQuoted(path, '\''));
        if (filter != null && !filter.isEmpty()) sql.append(" WHERE ").append(filter);
        return sql.toString();
    }

    private static String sqlQuoted(String text, char quote) {
        if (text.indexOf('\0') >= 0) throw new IllegalArgumentException("SQL names and paths cannot contain NUL");
        // liteparser and zpq's expression lexer escape quotes by doubling; backslashes are literal bytes.
        String delimiter = Character.toString(quote);
        return delimiter + text.replace(delimiter, delimiter + delimiter) + delimiter;
    }

    private List<InternalRow> runParquetOutputUrlScan(ZpqNative nativeCtx, String taskMaxMem, boolean cache) {
        String tmpDir = System.getProperty("java.io.tmpdir", "zig-out/spark/tmp");
        Path tmpFile = Path.of(tmpDir, "zpq-split-" + UUID.randomUUID() + ".parquet");
        try {
            StringBuilder sb = new StringBuilder(256);
            sb.append("{\"inputs\":[");
            ZpqJson.appendQuoted(part.filePath(), sb);
            sb.append("]");
            if (part.startRg() >= 0 && part.endRg() >= 0) {
                sb.append(",\"row_groups\":[[").append(part.startRg()).append(",").append(part.endRg()).append("]]");
            }
            sb.append(",\"columns\":[");
            StructField[] fields = readSchema.fields();
            for (int i = 0; i < fields.length; i++) {
                if (i > 0) sb.append(",");
                ZpqJson.appendQuoted(fields[i].name(), sb);
            }
            sb.append("]");
            if (zpqFilter != null && !zpqFilter.isEmpty()) {
                sb.append(",\"filter\":");
                ZpqJson.appendQuoted(zpqFilter, sb);
            }
            sb.append(",\"output_url\":");
            ZpqJson.appendQuoted(tmpFile.toAbsolutePath().toString(), sb);
            sb.append(",\"output_codec\":\"uncompressed\"");
            sb.append(",\"fsync\":false");
            sb.append(",\"parallelism\":").append(Math.max(1, part.parallelism()));
            sb.append(",\"max_memory\":");
            ZpqJson.appendQuoted(taskMaxMem, sb);
            sb.append(",\"cache\":").append(cache ? "true" : "false");
            sb.append("}");

            String resp = nativeCtx.runJson(sb.toString());
            Map<String, Object> env = ZpqJson.parseObject(resp);
            if (env.containsKey("error")) {
                throw new RuntimeException("libzpq output_url error: " + resp);
            }
            Object matchedObj = env.get("rows_matched");
            long matched = (matchedObj instanceof ZpqJson.RawNumber rn)
                ? Long.parseLong(rn.text())
                : Long.parseLong(matchedObj.toString());
            if (matched == 0L) {
                return List.of();
            }

            // Read back the staged uncompressed Parquet via zpq SQL on the small filtered output
            String sql = rowScanSql(tmpFile.toAbsolutePath().toString(), null);
            String readReq = "{\"sql\":" + ZpqJson.quoteString(sql)
                + ",\"parallelism\":1,\"max_memory\":\"256MB\",\"cache\":true}";
            String readResp = nativeCtx.runJson(readReq);
            return ZpqJson.parseSqlNdjsonRows(readResp, readSchema);
        } finally {
            try {
                Files.deleteIfExists(tmpFile);
            } catch (IOException ignored) {
            }
        }
    }

    @Override
    public boolean next() {
        ensureInitialized();
        if (remainingEmptyRows >= 0) {
            if (remainingEmptyRows > 0) {
                remainingEmptyRows--;
                currentRow = EMPTY_ROW;
                return true;
            }
            return false;
        }
        if (rowIterator != null && rowIterator.hasNext()) {
            currentRow = rowIterator.next();
            return true;
        }
        return false;
    }

    @Override
    public InternalRow get() {
        return currentRow;
    }

    @Override
    public void close() {
        rowIterator = null;
        currentRow = null;
    }
}
