package dev.zpq.spark;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

/**
 * Lightweight, zero-dependency JSON builder and parser for libzpq requests and responses.
 * Preserves exact DECIMAL digits (no double round-trip), handles {"base64":"..."} binary
 * values losslessly, and converts directly into Spark Catalyst {@link InternalRow}s.
 */
public final class ZpqJson {
    private ZpqJson() {}

    /** Wrapper for raw JSON numeric tokens so BigDecimal / Long / Double preserve all digits. */
    public record RawNumber(String text) {}

    /** Wrapper for {"base64":"..."} byte values emitted by libzpq on non-UTF8 binary columns. */
    public record RawBytes(byte[] bytes) {}

    public static String quoteString(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 8);
        appendQuoted(s, sb);
        return sb.toString();
    }

    public static void appendQuoted(String s, StringBuilder sb) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseObject(String json) {
        Parser p = new Parser(json);
        Object val = p.parseValue();
        if (!(val instanceof Map<?, ?> m)) {
            throw new IllegalArgumentException("Expected JSON object, got: " + json);
        }
        return (Map<String, Object>) m;
    }

    public static List<InternalRow> parseAggregateRows(
        String responseJson,
        StructType readSchema,
        int numGroups,
        int numAggs
    ) {
        long t0 = System.nanoTime();
        try {
            Map<String, Object> env = parseObject(responseJson);
            if (env.containsKey("error")) {
                throw new RuntimeException("libzpq error response: " + responseJson);
            }
            Object aggObj = env.get("agg");
            StructField[] fields = readSchema.fields();
            List<InternalRow> out = new ArrayList<>();

            if (numGroups == 0) {
                if (!(aggObj instanceof Map<?, ?> rowMap)) {
                    throw new RuntimeException("Expected agg object in response: " + responseJson);
                }
                Object[] values = new Object[fields.length];
                for (int i = 0; i < numAggs; i++) {
                    Object raw = rowMap.get("_a" + i);
                    values[i] = convertValue(raw, fields[i].dataType());
                }
                out.add(new GenericInternalRow(values));
            } else {
                if (!(aggObj instanceof List<?> list)) {
                    throw new RuntimeException("Expected agg array in grouped response: " + responseJson);
                }
                for (Object item : list) {
                    Map<?, ?> rowMap = (Map<?, ?>) item;
                    Object[] values = new Object[fields.length];
                    for (int g = 0; g < numGroups; g++) {
                        Object raw = rowMap.get("_g" + g);
                        values[g] = convertValue(raw, fields[g].dataType());
                    }
                    for (int a = 0; a < numAggs; a++) {
                        Object raw = rowMap.get("_a" + a);
                        values[numGroups + a] = convertValue(raw, fields[numGroups + a].dataType());
                    }
                    out.add(new GenericInternalRow(values));
                }
            }
            return out;
        } finally {
            ZpqNative.addParseNs(System.nanoTime() - t0);
        }
    }

    public static List<InternalRow> parseSqlNdjsonRows(String responseJson, StructType readSchema) {
        long t0 = System.nanoTime();
        try {
            Map<String, Object> env = parseObject(responseJson);
            if (env.containsKey("error")) {
                throw new RuntimeException("libzpq SQL error response: " + responseJson);
            }
            Object ndjsonObj = env.get("ndjson");
            if (!(ndjsonObj instanceof String ndjson) || ndjson.isEmpty()) {
                return Collections.emptyList();
            }
            StructField[] fields = readSchema.fields();
            String[] colKeys = new String[fields.length];
            for (int i = 0; i < fields.length; i++) {
                colKeys[i] = "_c" + i;
            }
            List<InternalRow> rows = new ArrayList<>();
            int lineStart = 0;
            int len = ndjson.length();
            while (lineStart < len) {
                int nl = ndjson.indexOf('\n', lineStart);
                int lineEnd = (nl < 0) ? len : nl;
                if (lineEnd > lineStart) {
                    Parser p = new Parser(ndjson, lineStart, lineEnd);
                    Map<?, ?> rowMap = (Map<?, ?>) p.parseValue();
                    Object[] values = new Object[fields.length];
                    for (int i = 0; i < fields.length; i++) {
                        Object raw = rowMap.get(colKeys[i]);
                        if (raw == null && !rowMap.containsKey(colKeys[i])) {
                            raw = rowMap.get(fields[i].name());
                        }
                        values[i] = convertValue(raw, fields[i].dataType());
                    }
                    rows.add(new GenericInternalRow(values));
                }
                lineStart = lineEnd + 1;
            }
            return rows;
        } finally {
            ZpqNative.addParseNs(System.nanoTime() - t0);
        }
    }

    public static Object convertValue(Object raw, DataType dataType) {
        if (raw == null) {
            return null;
        }
        if (dataType instanceof BooleanType) {
            if (raw instanceof Boolean b) return b;
            if (raw instanceof RawNumber n) return Long.parseLong(n.text()) != 0L;
            return Boolean.parseBoolean(raw.toString());
        }
        if (dataType instanceof ByteType) {
            return (byte) parseLong(raw);
        }
        if (dataType instanceof ShortType) {
            return (short) parseLong(raw);
        }
        if (dataType instanceof IntegerType) {
            return (int) parseLong(raw);
        }
        if (dataType instanceof LongType) {
            return parseLong(raw);
        }
        if (dataType instanceof FloatType) {
            return (float) parseDouble(raw);
        }
        if (dataType instanceof DoubleType) {
            return parseDouble(raw);
        }
        if (dataType instanceof DecimalType dt) {
            BigDecimal bd = (raw instanceof RawNumber n)
                ? new BigDecimal(n.text())
                : new BigDecimal(raw.toString());
            return Decimal.apply(bd, dt.precision(), dt.scale());
        }
        if (dataType instanceof StringType) {
            if (raw instanceof RawBytes rb) {
                return UTF8String.fromBytes(rb.bytes());
            }
            return UTF8String.fromString(raw.toString());
        }
        if (dataType instanceof BinaryType) {
            if (raw instanceof RawBytes rb) {
                return rb.bytes();
            }
            return raw.toString().getBytes(StandardCharsets.UTF_8);
        }
        if (dataType instanceof DateType) {
            if (raw instanceof RawNumber n) {
                return Integer.parseInt(n.text());
            }
            String s = raw.toString();
            try {
                return Integer.parseInt(s);
            } catch (NumberFormatException e) {
                return (int) LocalDate.parse(s).toEpochDay();
            }
        }
        if (dataType instanceof TimestampType || dataType instanceof TimestampNTZType) {
            if (raw instanceof RawNumber n) {
                return Long.parseLong(n.text());
            }
            String s = raw.toString();
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException e) {
                String iso = s.replace(' ', 'T');
                if (iso.endsWith("Z")) {
                    Instant inst = Instant.parse(iso);
                    return inst.getEpochSecond() * 1_000_000L + inst.getNano() / 1_000L;
                } else {
                    LocalDateTime ldt = LocalDateTime.parse(iso);
                    Instant inst = ldt.toInstant(ZoneOffset.UTC);
                    return inst.getEpochSecond() * 1_000_000L + inst.getNano() / 1_000L;
                }
            }
        }
        return UTF8String.fromString(raw.toString());
    }

    private static long parseLong(Object raw) {
        String s = (raw instanceof RawNumber n) ? n.text() : raw.toString();
        return Long.parseLong(s);
    }

    private static double parseDouble(Object raw) {
        String s = (raw instanceof RawNumber n) ? n.text() : raw.toString();
        return Double.parseDouble(s);
    }

    private static final class Parser {
        private final String s;
        private int pos;
        private final int end;

        Parser(String s) {
            this(s, 0, s.length());
        }

        Parser(String s, int start, int end) {
            this.s = s;
            this.pos = start;
            this.end = end;
        }

        private void skipWhitespace() {
            while (pos < end) {
                char c = s.charAt(pos);
                if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
                    pos++;
                } else {
                    break;
                }
            }
        }

        Object parseValue() {
            skipWhitespace();
            if (pos >= end) {
                throw new IllegalArgumentException("Unexpected end of JSON");
            }
            char c = s.charAt(pos);
            return switch (c) {
                case '{' -> parseObjectInternal();
                case '[' -> parseArrayInternal();
                case '"' -> parseStringInternal();
                case 't', 'f' -> parseBooleanInternal();
                case 'n' -> parseNullInternal();
                default -> parseNumberInternal();
            };
        }

        private Object parseObjectInternal() {
            pos++; // '{'
            skipWhitespace();
            if (pos < end && s.charAt(pos) == '}') {
                pos++;
                return Collections.emptyMap();
            }
            Map<String, Object> map = new LinkedHashMap<>();
            while (pos < end) {
                skipWhitespace();
                String key = parseStringInternal();
                skipWhitespace();
                if (pos >= end || s.charAt(pos) != ':') {
                    throw new IllegalArgumentException("Expected ':' after key " + key + " at pos " + pos);
                }
                pos++; // ':'
                Object val = parseValue();
                map.put(key, val);
                skipWhitespace();
                if (pos < end && s.charAt(pos) == ',') {
                    pos++;
                } else if (pos < end && s.charAt(pos) == '}') {
                    pos++;
                    break;
                } else {
                    throw new IllegalArgumentException("Expected ',' or '}' at pos " + pos);
                }
            }
            if (map.size() == 1 && map.get("base64") instanceof String b64) {
                return new RawBytes(Base64.getDecoder().decode(b64));
            }
            return map;
        }

        private List<Object> parseArrayInternal() {
            pos++; // '['
            skipWhitespace();
            if (pos < end && s.charAt(pos) == ']') {
                pos++;
                return Collections.emptyList();
            }
            List<Object> list = new ArrayList<>();
            while (pos < end) {
                list.add(parseValue());
                skipWhitespace();
                if (pos < end && s.charAt(pos) == ',') {
                    pos++;
                } else if (pos < end && s.charAt(pos) == ']') {
                    pos++;
                    break;
                } else {
                    throw new IllegalArgumentException("Expected ',' or ']' at pos " + pos);
                }
            }
            return list;
        }

        private String parseStringInternal() {
            if (pos >= end || s.charAt(pos) != '"') {
                throw new IllegalArgumentException("Expected '\"' at pos " + pos);
            }
            pos++; // '"'
            int start = pos;
            // Fast path for strings with no escapes
            while (pos < end) {
                char c = s.charAt(pos);
                if (c == '"') {
                    String res = s.substring(start, pos);
                    pos++;
                    return res;
                }
                if (c == '\\') {
                    break;
                }
                pos++;
            }
            StringBuilder sb = new StringBuilder(pos - start + 16);
            sb.append(s, start, pos);
            while (pos < end) {
                char c = s.charAt(pos++);
                if (c == '"') {
                    return sb.toString();
                }
                if (c == '\\') {
                    if (pos >= end) throw new IllegalArgumentException("Unterminated escape");
                    char esc = s.charAt(pos++);
                    switch (esc) {
                        case '"' -> sb.append('"');
                        case '\\' -> sb.append('\\');
                        case '/' -> sb.append('/');
                        case 'b' -> sb.append('\b');
                        case 'f' -> sb.append('\f');
                        case 'n' -> sb.append('\n');
                        case 'r' -> sb.append('\r');
                        case 't' -> sb.append('\t');
                        case 'u' -> {
                            int code = Integer.parseInt(s.substring(pos, pos + 4), 16);
                            sb.append((char) code);
                            pos += 4;
                        }
                        default -> sb.append(esc);
                    }
                } else {
                    sb.append(c);
                }
            }
            throw new IllegalArgumentException("Unterminated string literal");
        }

        private Boolean parseBooleanInternal() {
            if (s.startsWith("true", pos)) {
                pos += 4;
                return Boolean.TRUE;
            }
            if (s.startsWith("false", pos)) {
                pos += 5;
                return Boolean.FALSE;
            }
            throw new IllegalArgumentException("Invalid boolean token at pos " + pos);
        }

        private Object parseNullInternal() {
            if (s.startsWith("null", pos)) {
                pos += 4;
                return null;
            }
            throw new IllegalArgumentException("Invalid null token at pos " + pos);
        }

        private RawNumber parseNumberInternal() {
            int start = pos;
            while (pos < end) {
                char c = s.charAt(pos);
                if ((c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E') {
                    pos++;
                } else {
                    break;
                }
            }
            if (pos == start) {
                throw new IllegalArgumentException("Unexpected character '" + s.charAt(pos) + "' at pos " + pos);
            }
            return new RawNumber(s.substring(start, pos));
        }
    }
}
