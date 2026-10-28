package dev.zpq.spark;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Spark DataSource V2 Table backed by Parquet files queried through libzpq.
 */
public final class ZpqTable implements SupportsRead {
    private final StructType schema;
    private final List<String> files;
    private final Map<String, String> properties;

    public ZpqTable(StructType schema, List<String> files, Map<String, String> properties) {
        this.schema = schema;
        this.files = files;
        this.properties = properties;
    }

    @Override
    public String name() {
        return "zpq(" + files.size() + " files)";
    }

    @Override
    @SuppressWarnings("deprecation")
    public StructType schema() {
        return schema;
    }

    @Override
    public Map<String, String> properties() {
        return properties;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return EnumSet.of(TableCapability.BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new ZpqScanBuilder(schema, files, options);
    }
}
