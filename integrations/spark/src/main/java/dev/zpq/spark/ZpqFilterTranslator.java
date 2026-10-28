package dev.zpq.spark;

import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Translates Spark {@link Filter} predicates into zpq filter expressions.
 */
public final class ZpqFilterTranslator {
    private ZpqFilterTranslator() {}

    public static String quoteIdent(String col) {
        if (col == null || col.isEmpty() || col.indexOf('"') >= 0 || col.indexOf('\0') >= 0) {
            return null;
        }
        return "\"" + col + "\"";
    }

    public static String translate(Filter filter, StructType schema) {
        if (filter instanceof EqualTo f) {
            if (!hasColumn(schema, f.attribute()) || f.value() == null) return null;
            String col = quoteIdent(f.attribute());
            String lit = formatLiteral(f.value());
            return (col != null && lit != null) ? col + " = " + lit : null;
        }
        if (filter instanceof GreaterThan f) {
            if (!hasColumn(schema, f.attribute()) || f.value() == null) return null;
            String col = quoteIdent(f.attribute());
            String lit = formatLiteral(f.value());
            return (col != null && lit != null) ? col + " > " + lit : null;
        }
        if (filter instanceof GreaterThanOrEqual f) {
            if (!hasColumn(schema, f.attribute()) || f.value() == null) return null;
            String col = quoteIdent(f.attribute());
            String lit = formatLiteral(f.value());
            return (col != null && lit != null) ? col + " >= " + lit : null;
        }
        if (filter instanceof LessThan f) {
            if (!hasColumn(schema, f.attribute()) || f.value() == null) return null;
            String col = quoteIdent(f.attribute());
            String lit = formatLiteral(f.value());
            return (col != null && lit != null) ? col + " < " + lit : null;
        }
        if (filter instanceof LessThanOrEqual f) {
            if (!hasColumn(schema, f.attribute()) || f.value() == null) return null;
            String col = quoteIdent(f.attribute());
            String lit = formatLiteral(f.value());
            return (col != null && lit != null) ? col + " <= " + lit : null;
        }
        if (filter instanceof IsNull f) {
            if (!hasColumn(schema, f.attribute())) return null;
            String col = quoteIdent(f.attribute());
            return col != null ? col + " IS NULL" : null;
        }
        if (filter instanceof IsNotNull f) {
            if (!hasColumn(schema, f.attribute())) return null;
            String col = quoteIdent(f.attribute());
            return col != null ? col + " IS NOT NULL" : null;
        }
        if (filter instanceof In f) {
            if (!hasColumn(schema, f.attribute())) return null;
            Object[] vals = f.values();
            if (vals == null || vals.length == 0) return null;
            String col = quoteIdent(f.attribute());
            if (col == null) return null;
            List<String> lits = new ArrayList<>(vals.length);
            for (Object v : vals) {
                if (v == null) return null;
                String lit = formatLiteral(v);
                if (lit == null) return null;
                lits.add(lit);
            }
            return col + " IN (" + String.join(", ", lits) + ")";
        }
        if (filter instanceof StringContains f) {
            if (!hasColumn(schema, f.attribute()) || !isSafeLikePattern(f.value())) return null;
            String col = quoteIdent(f.attribute());
            String lit = formatStringLiteral("%" + f.value() + "%");
            return (col != null && lit != null) ? col + " LIKE " + lit : null;
        }
        if (filter instanceof StringStartsWith f) {
            if (!hasColumn(schema, f.attribute()) || !isSafeLikePattern(f.value())) return null;
            String col = quoteIdent(f.attribute());
            String lit = formatStringLiteral(f.value() + "%");
            return (col != null && lit != null) ? col + " LIKE " + lit : null;
        }
        if (filter instanceof StringEndsWith f) {
            if (!hasColumn(schema, f.attribute()) || !isSafeLikePattern(f.value())) return null;
            String col = quoteIdent(f.attribute());
            String lit = formatStringLiteral("%" + f.value());
            return (col != null && lit != null) ? col + " LIKE " + lit : null;
        }
        if (filter instanceof Not f) {
            Filter child = f.child();
            if (child instanceof EqualTo eq && hasColumn(schema, eq.attribute()) && eq.value() != null) {
                String col = quoteIdent(eq.attribute());
                String lit = formatLiteral(eq.value());
                if (col != null && lit != null) {
                    return col + " != " + lit;
                }
            }
            if (child instanceof StringContains sc && hasColumn(schema, sc.attribute()) && isSafeLikePattern(sc.value())) {
                String col = quoteIdent(sc.attribute());
                String lit = formatStringLiteral("%" + sc.value() + "%");
                if (col != null && lit != null) {
                    return col + " NOT LIKE " + lit;
                }
            }
            if (child instanceof StringStartsWith sw && hasColumn(schema, sw.attribute()) && isSafeLikePattern(sw.value())) {
                String col = quoteIdent(sw.attribute());
                String lit = formatStringLiteral(sw.value() + "%");
                if (col != null && lit != null) {
                    return col + " NOT LIKE " + lit;
                }
            }
            if (child instanceof StringEndsWith ew && hasColumn(schema, ew.attribute()) && isSafeLikePattern(ew.value())) {
                String col = quoteIdent(ew.attribute());
                String lit = formatStringLiteral("%" + ew.value());
                if (col != null && lit != null) {
                    return col + " NOT LIKE " + lit;
                }
            }
            String inner = translate(child, schema);
            return inner != null ? "NOT (" + inner + ")" : null;
        }
        if (filter instanceof And f) {
            String l = translate(f.left(), schema);
            String r = translate(f.right(), schema);
            return (l != null && r != null) ? "(" + l + " AND " + r + ")" : null;
        }
        if (filter instanceof Or f) {
            String l = translate(f.left(), schema);
            String r = translate(f.right(), schema);
            return (l != null && r != null) ? "(" + l + " OR " + r + ")" : null;
        }
        return null;
    }

    private static boolean hasColumn(StructType schema, String name) {
        if (schema == null || name == null || name.indexOf('.') >= 0) {
            return false;
        }
        for (StructField sf : schema.fields()) {
            if (sf.name().equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isSafeLikePattern(String s) {
        return s != null && s.indexOf('%') < 0 && s.indexOf('_') < 0 && s.indexOf('\\') < 0;
    }

    public static String formatLiteral(Object v) {
        if (v instanceof Byte || v instanceof Short || v instanceof Integer || v instanceof Long) {
            return v.toString();
        }
        if (v instanceof BigDecimal bd) {
            return bd.toPlainString();
        }
        if (v instanceof Float f) {
            return Float.isFinite(f) ? f.toString() : null;
        }
        if (v instanceof Double d) {
            return Double.isFinite(d) ? d.toString() : null;
        }
        if (v instanceof Boolean b) {
            return b ? "true" : "false";
        }
        if (v instanceof String s) {
            return formatStringLiteral(s);
        }
        if (v instanceof byte[] bytes) {
            return formatStringLiteral(new String(bytes, StandardCharsets.UTF_8));
        }
        if (v instanceof Date d) {
            return formatStringLiteral(d.toLocalDate().toString());
        }
        if (v instanceof LocalDate ld) {
            return formatStringLiteral(ld.toString());
        }
        return null;
    }

    public static String formatStringLiteral(String s) {
        if (s == null) return null;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < 0x20 || c == '\'' || c == '\\') {
                return null;
            }
        }
        return "'" + s + "'";
    }
}
