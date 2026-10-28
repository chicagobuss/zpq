package dev.zpq.spark;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.List;
import java.util.Map;

/**
 * Spark DataSource V2 provider for libzpq: {@code spark.read.format("zpq").load(path)}.
 */
public final class ZpqDataSource implements TableProvider, DataSourceRegister {
    public ZpqDataSource() {}

    @Override
    public String shortName() {
        return "zpq";
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return ZpqPaths.inferSchema(options);
    }

    @Override
    public Table getTable(
        StructType schema,
        Transform[] partitioning,
        Map<String, String> properties
    ) {
        List<String> files = ZpqPaths.resolveFiles(properties);
        return new ZpqTable(schema, files, properties);
    }
}
