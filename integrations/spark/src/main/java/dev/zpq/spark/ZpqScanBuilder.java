package dev.zpq.spark;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.aggregate.*;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ScanBuilder implementing column pruning, filter pushdown, and partial aggregate pushdown
 * into libzpq across row-group splits.
 */
public final class ZpqScanBuilder implements
    ScanBuilder,
    SupportsPushDownRequiredColumns,
    SupportsPushDownFilters,
    SupportsPushDownAggregates {

    private final StructType tableSchema;
    private final List<String> files;
    private final Map<String, String> options;

    private StructType requiredSchema;
    private Filter[] pushedFilters = new Filter[0];
    private String zpqFilter = null;

    private boolean hasPushedAggregation = false;
    private StructType pushedAggSchema = null;
    private String zpqGroupBy = null;
    private String zpqAggregate = null;
    private int numGroups = 0;
    private int numAggs = 0;
    private boolean sawAvgOnLong = false;

    public ZpqScanBuilder(StructType tableSchema, List<String> files, CaseInsensitiveStringMap options) {
        this.tableSchema = tableSchema;
        this.files = files;
        this.options = new HashMap<>(options.asCaseSensitiveMap());
        this.requiredSchema = tableSchema;
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        this.requiredSchema = requiredSchema;
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        if ("false".equalsIgnoreCase(options.get("pushFilters"))) {
            this.pushedFilters = new Filter[0];
            this.zpqFilter = null;
            return filters;
        }
        List<Filter> pushed = new ArrayList<>();
        List<Filter> unpushed = new ArrayList<>();
        List<String> zpqParts = new ArrayList<>();

        for (Filter f : filters) {
            String expr = ZpqFilterTranslator.translate(f, tableSchema);
            if (expr != null) {
                pushed.add(f);
                zpqParts.add(expr);
            } else {
                unpushed.add(f);
            }
        }
        this.pushedFilters = pushed.toArray(new Filter[0]);
        this.zpqFilter = zpqParts.isEmpty() ? null : String.join(" AND ", zpqParts);
        return unpushed.toArray(new Filter[0]);
    }

    @Override
    public Filter[] pushedFilters() {
        return pushedFilters;
    }

    @Override
    public boolean supportCompletePushDown(Aggregation aggregation) {
        for (AggregateFunc fn : aggregation.aggregateExpressions()) {
            if (fn instanceof Avg avg) {
                String col = extractTopLevelColumn(avg.column());
                StructField sf = (col != null) ? findField(tableSchema, col) : null;
                if (sf != null && sf.dataType() instanceof LongType) {
                    this.sawAvgOnLong = true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean pushAggregation(Aggregation aggregation) {
        if ("false".equalsIgnoreCase(options.get("pushAggregates")) || sawAvgOnLong) {
            return false;
        }
        Expression[] groupExprs = aggregation.groupByExpressions();
        AggregateFunc[] aggFuncs = aggregation.aggregateExpressions();

        List<StructField> outFields = new ArrayList<>();
        List<String> groupParts = new ArrayList<>();
        for (int i = 0; i < groupExprs.length; i++) {
            String colName = extractTopLevelColumn(groupExprs[i]);
            if (colName == null) {
                return false;
            }
            StructField sf = findField(tableSchema, colName);
            if (sf == null || !isSupportedGroupType(sf.dataType())) {
                return false;
            }
            String quoted = ZpqFilterTranslator.quoteIdent(colName);
            if (quoted == null) {
                return false;
            }
            groupParts.add(quoted + " AS _g" + i);
            outFields.add(new StructField("_g" + i, sf.dataType(), sf.nullable(), Metadata.empty()));
        }

        List<String> aggParts = new ArrayList<>();
        for (int i = 0; i < aggFuncs.length; i++) {
            AggregateFunc fn = aggFuncs[i];
            String alias = "_a" + i;
            if (fn instanceof CountStar) {
                aggParts.add("count(*) AS " + alias);
                outFields.add(new StructField(alias, DataTypes.LongType, false, Metadata.empty()));
            } else if (fn instanceof Count c) {
                if (c.isDistinct()) return false;
                String col = extractTopLevelColumn(c.column());
                if (col == null || findField(tableSchema, col) == null) return false;
                String quoted = ZpqFilterTranslator.quoteIdent(col);
                if (quoted == null) return false;
                aggParts.add("count(" + quoted + ") AS " + alias);
                outFields.add(new StructField(alias, DataTypes.LongType, false, Metadata.empty()));
            } else if (fn instanceof Sum s) {
                if (s.isDistinct()) return false;
                String col = extractTopLevelColumn(s.column());
                StructField sf = (col != null) ? findField(tableSchema, col) : null;
                if (sf == null) return false;
                DataType sumType = sumOutputType(sf.dataType());
                if (sumType == null) return false;
                String quoted = ZpqFilterTranslator.quoteIdent(col);
                if (quoted == null) return false;
                aggParts.add("sum(" + quoted + ") AS " + alias);
                outFields.add(new StructField(alias, sumType, true, Metadata.empty()));
            } else if (fn instanceof Min m) {
                String col = extractTopLevelColumn(m.column());
                StructField sf = (col != null) ? findField(tableSchema, col) : null;
                if (sf == null || !isSupportedMinMaxType(sf.dataType())) return false;
                String quoted = ZpqFilterTranslator.quoteIdent(col);
                if (quoted == null) return false;
                aggParts.add("min(" + quoted + ") AS " + alias);
                outFields.add(new StructField(alias, sf.dataType(), true, Metadata.empty()));
            } else if (fn instanceof Max m) {
                String col = extractTopLevelColumn(m.column());
                StructField sf = (col != null) ? findField(tableSchema, col) : null;
                if (sf == null || !isSupportedMinMaxType(sf.dataType())) return false;
                String quoted = ZpqFilterTranslator.quoteIdent(col);
                if (quoted == null) return false;
                aggParts.add("max(" + quoted + ") AS " + alias);
                outFields.add(new StructField(alias, sf.dataType(), true, Metadata.empty()));
            } else {
                return false;
            }
        }

        if (groupParts.isEmpty() && aggParts.isEmpty()) {
            return false;
        }

        this.hasPushedAggregation = true;
        this.numGroups = groupParts.size();
        this.numAggs = aggParts.size();
        this.zpqGroupBy = groupParts.isEmpty() ? null : String.join(", ", groupParts);
        this.zpqAggregate = aggParts.isEmpty() ? "count(*) AS _dummy" : String.join(", ", aggParts);
        this.pushedAggSchema = new StructType(outFields.toArray(new StructField[0]));
        return true;
    }

    private static String extractTopLevelColumn(Expression expr) {
        if (expr instanceof NamedReference ref) {
            String[] names = ref.fieldNames();
            if (names != null && names.length == 1) {
                return names[0];
            }
        }
        return null;
    }

    private static StructField findField(StructType schema, String name) {
        for (StructField sf : schema.fields()) {
            if (sf.name().equals(name)) {
                return sf;
            }
        }
        return null;
    }

    private static boolean isSupportedGroupType(DataType dt) {
        return dt instanceof BooleanType
            || dt instanceof ByteType
            || dt instanceof ShortType
            || dt instanceof IntegerType
            || dt instanceof LongType
            || dt instanceof FloatType
            || dt instanceof DoubleType
            || dt instanceof DecimalType
            || dt instanceof StringType
            || dt instanceof BinaryType
            || dt instanceof DateType
            || dt instanceof TimestampType
            || dt instanceof TimestampNTZType;
    }

    private static boolean isSupportedMinMaxType(DataType dt) {
        return isSupportedGroupType(dt);
    }

    private static DataType sumOutputType(DataType in) {
        if (in instanceof ByteType || in instanceof ShortType || in instanceof IntegerType || in instanceof LongType) {
            return DataTypes.LongType;
        }
        if (in instanceof FloatType || in instanceof DoubleType) {
            return DataTypes.DoubleType;
        }
        if (in instanceof DecimalType dt) {
            return DataTypes.createDecimalType(Math.min(38, dt.precision() + 10), dt.scale());
        }
        return null;
    }

    @Override
    public Scan build() {
        StructType outSchema = hasPushedAggregation ? pushedAggSchema : requiredSchema;
        return new ZpqScan(
            files,
            outSchema,
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
