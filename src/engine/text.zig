const engine_types = @import("types.zig");
const PrintFormat = engine_types.PrintFormat;
const AggregateExplanation = engine_types.AggregateExplanation;
const agg_plan = @import("../core/agg_plan.zig");
const Diag = @import("../core/diag.zig").Diag;
const core_memory = @import("../core/memory.zig");
const json_string = @import("json_string.zig");
const std = @import("std");
const schema = @import("../core/schema.zig");
const scan = @import("../core/scan.zig");
const consumer = @import("../core/consumer.zig");
const decimal_mod = @import("../core/parquet/decimal.zig");
const membership = @import("../core/filter/membership.zig");

/// One answer value as CSV cells: NULL an empty field, a BOOLEAN true/false, a DATE/TIME/TIMESTAMP its ISO text,
/// a DECIMAL every digit of its scale, bytes as `writeBytesCsv`, an avg_state its sum and count.
pub fn writeAggValueCsv(writer: anytype, v: scan.AggValue, kind: TextKind, ty: AnswerType) !void {
    switch (v) {
        .null_val => if (ty.avg) try writer.writeByte(','),
        .i => |x| if (kind == .boolean)
            try writer.writeAll(if (x != 0) "true" else "false")
        else if (ty.temporal) |t|
            try t.write(writer, x)
        else
            try writer.print("{d}", .{x}),
        .f => |x| try writeFloat(writer, x, false),
        .s => |x| try writeBytesCsv(writer, x, kind),
        .dec => |d| if (ty.temporal) |t| try t.write(writer, d.v) else try writeDecimal(writer, d.v, d.spec.scale),
        .avg => |p| {
            if (p.sum) |x| try writeFloat(writer, x, false);
            try writer.print(",{d}", .{p.count});
        },
        .avg_dec => |p| {
            if (p.sum) |x| try writeDecimal(writer, x, p.spec.scale);
            try writer.print(",{d}", .{p.count});
        },
    }
}


/// Formats into a stack buffer and hands the bytes to `w.writeAll`. Text longer than the buffer, such as a long string
/// statistic on a `zpq inspect` line, is formatted on the heap instead of failing with NoSpaceLeft.
pub fn printThrough(w: anytype, comptime fmt: []const u8, args: anytype) !void {
    var tmp: [512]u8 = undefined;
    const out = std.fmt.bufPrint(&tmp, fmt, args) catch |err| switch (err) {
        error.NoSpaceLeft => {
            const long = try std.fmt.allocPrint(std.heap.page_allocator, fmt, args);
            defer std.heap.page_allocator.free(long);
            return w.writeAll(long);
        },
    };
    try w.writeAll(out);
}


fn writeFloat(writer: anytype, f: anytype, is_jsonl: bool) !void {
    if (std.math.isFinite(f)) {
        try writer.print("{d}", .{f});
    } else if (std.math.isNan(f)) {
        if (is_jsonl) {
            try writer.writeAll("\"NaN\"");
        } else {
            try writer.writeAll("NaN");
        }
    } else if (f > 0) {
        if (is_jsonl) {
            try writer.writeAll("\"Infinity\"");
        } else {
            try writer.writeAll("Infinity");
        }
    } else {
        if (is_jsonl) {
            try writer.writeAll("\"-Infinity\"");
        } else {
            try writer.writeAll("-Infinity");
        }
    }
}

/// `unscaled` (any signed integer width, an avg's 192-bit sum included) as decimal text with `scale` digits after the
/// point, every one printed: `-0.05`, `300.00`.
fn writeDecimal(writer: anytype, unscaled: anytype, scale: i32) !void {
    if (scale <= 0) {
        try writer.print("{d}", .{unscaled});
        return;
    }
    var buf: [64]u8 = undefined;
    const digits = try std.fmt.bufPrint(&buf, "{d}", .{@abs(unscaled)});
    const s: usize = @intCast(scale);
    if (unscaled < 0) try writer.writeByte('-');
    if (digits.len > s) {
        try writer.print("{s}.{s}", .{ digits[0 .. digits.len - s], digits[digits.len - s ..] });
    } else {
        try writer.writeAll("0.");
        for (0..s - digits.len) |_| try writer.writeByte('0');
        try writer.writeAll(digits);
    }
}

/// `s` as an RFC 4180 field: quoted when it holds a comma, quote or line break, and when empty, which an unquoted
/// empty field (NULL) would not tell apart.
pub fn writeCsvString(writer: anytype, s: []const u8) !void {
    return writeCsvParts(writer, &.{s});
}

/// The concatenation of `parts` as one CSV field (`writeCsvString`).
pub fn writeCsvParts(writer: anytype, parts: []const []const u8) !void {
    var len: usize = 0;
    var needs_quotes = false;
    for (parts) |part| {
        len += part.len;
        for (part) |c| if (c == ',' or c == '"' or c == '\n' or c == '\r') {
            needs_quotes = true;
        };
    }
    if (len == 0) needs_quotes = true;
    if (!needs_quotes) {
        for (parts) |part| try writer.writeAll(part);
        return;
    }
    try writer.writeByte('"');
    const quoting: CsvQuoting(@TypeOf(writer)) = .{ .inner = writer };
    for (parts) |part| try quoting.writeAll(part);
    try writer.writeByte('"');
}


/// A JSON number for `f`. Aggregates can be non-finite (a NaN or ±inf input), and JSON has no literal for that: a bare
/// `nan` is invalid and `null` would read as "no value". Write the strings "NaN", "Infinity" and "-Infinity", as
/// `--format jsonl` does and DuckDB/pyarrow surface them.
pub fn writeJsonFloat(writer: anytype, f: f64) !void {
    try writeFloat(writer, f, true);
}

/// One aggregate result as a JSON value. The CLI and Lambda responses both go through here so they escape strings and
/// spell NULL and NaN the same way.
/// `"membership":{...}`: what Bloom filter and dictionary probing read, and the row groups each ruled out beyond
/// statistics (already counted in `row_groups_pruned`).
pub fn writeMembershipJson(writer: anytype, s: membership.Stats) !void {
    try writer.print(
        "\"membership\":{{\"bloom_pruned\":{d},\"dict_pruned\":{d},\"filters\":{d},\"dictionaries\":{d},\"requests\":{d},\"bytes\":{d},\"suffix_bytes\":{d},\"bloom_stopped\":{},\"dict_stopped\":{},\"bloom_skipped_oversize\":{d},\"request_kb\":{d},\"us\":{d}}}",
        .{
            s.row_groups_bloom, s.row_groups_dict,        s.filters,              s.dictionaries,
            s.requests,         s.bytes,                  s.suffix_bytes,         s.bloom_stopped,
            s.dict_stopped,     s.bloom_skipped_oversize, s.request_bytes / 1024, s.ns / std.time.ns_per_us,
        },
    );
}

/// Whether `s` is UTF-8. Most unannotated text is ASCII: 16 bytes at a time until a byte with the high bit, which
/// starts a sequence because every byte before it is ASCII; the rest goes to the full check.
fn isUtf8(s: []const u8) bool {
    const V = @Vector(16, u8);
    var i: usize = 0;
    while (i + 16 <= s.len) : (i += 16) {
        const v: V = s[i..][0..16].*;
        if (@reduce(.Or, v) & 0x80 != 0) break;
    }
    while (i < s.len and s[i] < 0x80) i += 1;
    return std.unicode.utf8ValidateSlice(s[i..]);
}

/// `{"base64":"..."}`, standard alphabet, padded.
fn writeBase64Json(writer: anytype, value: []const u8) !void {
    try writer.writeAll("{\"base64\":\"");
    var buf: [4096]u8 = undefined;
    var rest = value;
    while (rest.len > 0) {
        // A whole number of 3-byte groups a chunk, so only the last one pads.
        const n = @min(rest.len, buf.len / 4 * 3);
        try writer.writeAll(std.base64.standard.Encoder.encode(&buf, rest[0..n]));
        rest = rest[n..];
    }
    try writer.writeAll("\"}");
}

/// Column `ci`'s kind; a computed string's, when none is known.
pub fn kindAt(kinds: []const TextKind, ci: usize) TextKind {
    return if (ci < kinds.len) kinds[ci] else .utf8;
}

/// `v` as `writeAggValueJson` writes it, its bytes in the form `kind` gives them and a BOOLEAN's 0/1 as false/true,
/// as its rows print.
pub fn writeTypedAggValueJson(writer: anytype, v: scan.AggValue, kind: TextKind) !void {
    if (v == .s) return writeBytesJson(writer, v.s, kind);
    if (kind == .boolean and v == .i) return writer.writeAll(if (v.i != 0) "true" else "false");
    return writeAggValueJson(writer, v);
}

pub fn writeAggValueJson(writer: anytype, v: scan.AggValue) !void {
    switch (v) {
        .i => |x| try writer.print("{d}", .{x}),
        .f => |x| try writeJsonFloat(writer, x),
        .s => |x| try writeJsonQuoted(writer, x),
        .avg => |pair| {
            try writer.writeAll("{\"sum\":");
            if (pair.sum) |sum| try writeJsonFloat(writer, sum) else try writer.writeAll("null");
            try writer.print(",\"count\":{d}}}", .{pair.count});
        },
        // Exact text: a JSON number with every digit of the scale, so a reader parsing numbers as decimals gets the
        // value bit for bit (one parsing them as doubles gets the nearest double, as with any JSON number).
        .dec => |d| try writeDecimal(writer, d.v, d.spec.scale),
        .avg_dec => |p| {
            try writer.writeAll("{\"sum\":");
            if (p.sum) |sum| try writeDecimal(writer, sum, p.spec.scale) else try writer.writeAll("null");
            try writer.print(",\"count\":{d}}}", .{p.count});
        },
        .null_val => try writer.writeAll("null"),
    }
}

/// `s` as a quoted, escaped JSON string.
pub fn writeJsonQuoted(writer: anytype, s: []const u8) !void {
    try writer.writeByte('"');
    try writeJsonString(writer, s);
    try writer.writeByte('"');
}

pub const writeJsonString = json_string.writeJsonString;
pub const writeJsonText = json_string.writeJsonText;
const escapeJson = json_string.escapeJson;

/// How a byte value is written as JSON text, from its column's annotation (`docs/binary-output.md`). Every form is
/// lossless and no two values print alike: a string is never equal to an object. An answer column also marks a
/// BOOLEAN source, whose lane holds 0/1.
pub const TextKind = enum {
    /// STRING, ENUM or JSON: the text. A value that is not UTF-8 is an invalid file, not a value to print.
    utf8,
    /// No annotation: text when the bytes are UTF-8, else `{"base64":...}`.
    bytes,
    /// Bytes that are never text (BSON, GEOMETRY, GEOGRAPHY): always `{"base64":...}`.
    base64,
    /// Canonical lowercase 8-4-4-4-12.
    uuid,
    /// The legacy FLBA(12) INTERVAL as `{"months":m,"days":d,"millis":ms}`.
    interval,
    /// A BOOLEAN answer column (a key, min or max): `true` / `false`. It holds no bytes.
    boolean,

    /// An annotation this reader does not interpret falls back to the legacy one beside it, as the format allows.
    pub fn of(elem: schema.SchemaElement) TextKind {
        if (elem.logical_type) |lt| switch (lt) {
            .STRING, .ENUM, .JSON => return .utf8,
            .UUID => return .uuid,
            .BSON => return .base64,
            .UNKNOWN => if (elem.rawLogicalType()) |raw| switch (raw) {
                .GEOMETRY, .GEOGRAPHY => return .base64,
                else => {},
            },
            else => {},
        };
        if (elem.converted_type) |ct| switch (ct) {
            .UTF8, .ENUM, .JSON => return .utf8,
            .BSON => return .base64,
            .INTERVAL => return .interval,
            else => {},
        };
        return .bytes;
    }

    /// A value whose source column is known; a computed string (no source) is text.
    pub fn ofSource(source: ?schema.SchemaElement) TextKind {
        const src = source orelse return .utf8;
        return if (src.type == .BOOLEAN) .boolean else of(src);
    }
};

/// How a CSV cell prints an integer column that SQL types DATE, TIME or TIMESTAMP: as ISO text, not its stored
/// count of days or units. INT96 is a TIMESTAMP in nanoseconds; a UTC-adjusted TIME or TIMESTAMP ends in `Z`.
pub const Temporal = struct {
    kind: enum { date, time, timestamp },
    unit: enum { ms, us, ns } = .us,
    utc: bool = false,

    pub fn of(elem: schema.SchemaElement) ?Temporal {
        if (elem.type == .INT96) return .{ .kind = .timestamp, .unit = .ns };
        if (elem.type != .INT32 and elem.type != .INT64) return null;
        const lt = elem.effectiveLogicalType() orelse return null;
        return switch (lt) {
            .DATE => .{ .kind = .date },
            .TIME => |t| .{ .kind = .time, .unit = unitOf(t.unit), .utc = t.isAdjustedToUTC },
            .TIMESTAMP => |t| .{ .kind = .timestamp, .unit = unitOf(t.unit), .utc = t.isAdjustedToUTC },
            else => null,
        };
    }

    fn unitOf(u: schema.TimeUnit) @FieldType(Temporal, "unit") {
        return switch (u) {
            .MILLIS => .ms,
            .MICROS => .us,
            .NANOS => .ns,
        };
    }

    fn perSecond(self: Temporal) i128 {
        return switch (self.unit) {
            .ms => 1_000,
            .us => 1_000_000,
            .ns => 1_000_000_000,
        };
    }

    fn digits(self: Temporal) usize {
        return switch (self.unit) {
            .ms => 3,
            .us => 6,
            .ns => 9,
        };
    }

    /// `v` (days, or units since midnight or the epoch) as `YYYY-MM-DD`, `HH:MM:SS.fff` or `YYYY-MM-DD HH:MM:SS.fff`,
    /// every fractional digit of the unit printed.
    pub fn write(self: Temporal, writer: anytype, v: i128) !void {
        if (self.kind == .date) return writeCivilDate(writer, v);
        const per = self.perSecond();
        const day = per * 86_400;
        const days = @divFloor(v, day);
        const in_day = @mod(v, day);
        if (self.kind == .timestamp) {
            try writeCivilDate(writer, days);
            try writer.writeByte(' ');
        }
        // A TIME past a day is no time of day; it prints its hours whole.
        const t = if (self.kind == .time) v else in_day;
        if (t < 0) return error.InvalidBinaryValue;
        const secs = @divFloor(t, per);
        try writer.print("{d:0>2}:{d:0>2}:{d:0>2}.", .{ @as(u128, @intCast(@divFloor(secs, 3600))), @as(u128, @intCast(@mod(@divFloor(secs, 60), 60))), @as(u128, @intCast(@mod(secs, 60))) });
        var frac_buf: [9]u8 = undefined;
        const frac = std.fmt.bufPrint(&frac_buf, "{d}", .{@as(u128, @intCast(@mod(t, per)))}) catch unreachable;
        for (0..self.digits() - frac.len) |_| try writer.writeByte('0');
        try writer.writeAll(frac);
        if (self.utc) try writer.writeByte('Z');
    }
};

/// Day `days` since 1970-01-01 as a proleptic Gregorian `YYYY-MM-DD` (H. Hinnant's civil_from_days); a year before
/// 1 prints signed, as ISO 8601 writes it.
fn writeCivilDate(writer: anytype, days: i128) !void {
    const z = days + 719_468;
    const era = @divFloor(z, 146_097);
    const doe = z - era * 146_097;
    const yoe = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36_524) - @divFloor(doe, 146_096), 365);
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const d = doy - @divFloor(153 * mp + 2, 5) + 1;
    const m = if (mp < 10) mp + 3 else mp - 9;
    const y = yoe + era * 400 + @intFromBool(m <= 2);
    if (y < 0) try writer.writeByte('-');
    try writer.print("{d:0>4}-{d:0>2}-{d:0>2}", .{ @as(u128, @intCast(@abs(y))), @as(u128, @intCast(m)), @as(u128, @intCast(d)) });
}

/// Writes through to `inner`, doubling each `"`: the body of a quoted CSV field.
fn CsvQuoting(comptime W: type) type {
    return struct {
        inner: W,

        pub fn writeAll(self: @This(), bytes: []const u8) !void {
            var start: usize = 0;
            for (bytes, 0..) |c, i| if (c == '"') {
                try self.inner.writeAll(bytes[start .. i + 1]);
                try self.inner.writeByte('"');
                start = i + 1;
            };
            try self.inner.writeAll(bytes[start..]);
        }

        pub fn writeByte(self: @This(), c: u8) !void {
            try self.writeAll(&.{c});
        }

        pub fn print(self: @This(), comptime fmt: []const u8, args: anytype) !void {
            try printThrough(self, fmt, args);
        }
    };
}

/// `value` as a CSV field under the same lossless rules as JSON (`writeBytesJson`): text is the field, quoted as RFC
/// 4180 needs; a UUID its canonical text; anything JSON prints as an object (bytes that are not UTF-8, BSON and the
/// like, an INTERVAL) that object's JSON text, quoted.
pub fn writeBytesCsv(writer: anytype, value: []const u8, kind: TextKind) !void {
    switch (kind) {
        .utf8 => {
            if (!isUtf8(value)) return error.InvalidUtf8;
            try writeCsvString(writer, value);
        },
        .bytes => if (isUtf8(value)) try writeCsvString(writer, value) else try writeJsonAsCsv(writer, value, kind),
        .uuid => {
            var buf: [40]u8 = undefined;
            var w: std.Io.Writer = .fixed(&buf);
            writeBytesJson(&w, value, .uuid) catch |err| switch (err) {
                error.WriteFailed => unreachable,
                else => |e| return e,
            };
            const text = w.buffered();
            try writer.writeAll(text[1 .. text.len - 1]);
        },
        .base64, .interval => try writeJsonAsCsv(writer, value, kind),
        .boolean => return error.InvalidBinaryValue,
    }
}

fn writeJsonAsCsv(writer: anytype, value: []const u8, kind: TextKind) !void {
    try writer.writeByte('"');
    try writeBytesJson(CsvQuoting(@TypeOf(writer)){ .inner = writer }, value, kind);
    try writer.writeByte('"');
}

/// What an answer column needs besides its `TextKind` to print as CSV: DATE/TIME/TIMESTAMP text, and an avg_state
/// split into `<name>__sum` and `<name>__count` cells, as `-o` writes it; and whether it is DECIMAL (an exact sum,
/// min/max or average of a DECIMAL, or a DECIMAL key), whose text a reader may want to parse exactly.
pub const AnswerType = struct {
    temporal: ?Temporal = null,
    avg: bool = false,
    decimal: bool = false,
};

/// Names of DECIMAL output columns, as `QueryArgs.decimal_columns` collects them.
pub const DecimalColumns = struct {
    gpa: std.mem.Allocator,
    names: std.ArrayList([]const u8) = .empty,

    pub fn deinit(self: *DecimalColumns) void {
        for (self.names.items) |n| self.gpa.free(n);
        self.names.deinit(self.gpa);
    }
};

/// `value` as a JSON value of `kind`. A UUID or INTERVAL of the wrong width is not one: `InvalidBinaryValue`.
pub fn writeBytesJson(writer: anytype, value: []const u8, kind: TextKind) !void {
    switch (kind) {
        .utf8 => {
            try writer.writeByte('"');
            try writeJsonText(writer, value);
            try writer.writeByte('"');
        },
        .bytes => {
            // One pass for most values: escaped strictly into `buf` (a byte escapes to at most 6), kept if all UTF-8.
            const short = 1024;
            var buf: [6 * short]u8 = undefined;
            if (value.len <= short) {
                var text: std.Io.Writer = .fixed(&buf);
                escapeJson(&text, value, true) catch |err| switch (err) {
                    error.InvalidUtf8 => return writeBase64Json(writer, value),
                    error.WriteFailed => unreachable,
                };
                try writer.writeByte('"');
                try writer.writeAll(text.buffered());
                try writer.writeByte('"');
            } else if (isUtf8(value)) {
                try writer.writeByte('"');
                try writeJsonString(writer, value);
                try writer.writeByte('"');
            } else try writeBase64Json(writer, value);
        },
        .base64 => try writeBase64Json(writer, value),
        // A BOOLEAN column holds no bytes.
        .boolean => return error.InvalidBinaryValue,
        .uuid => {
            if (value.len != 16) return error.InvalidBinaryValue;
            const hex = "0123456789abcdef";
            var text: [38]u8 = undefined;
            text[0] = '"';
            var at: usize = 1;
            for (value, 0..) |b, i| {
                if (i == 4 or i == 6 or i == 8 or i == 10) {
                    text[at] = '-';
                    at += 1;
                }
                text[at] = hex[b >> 4];
                text[at + 1] = hex[b & 15];
                at += 2;
            }
            text[at] = '"';
            try writer.writeAll(&text);
        },
        .interval => {
            if (value.len != 12) return error.InvalidBinaryValue;
            try writer.print("{{\"months\":{d},\"days\":{d},\"millis\":{d}}}", .{
                std.mem.readInt(u32, value[0..4], .little),
                std.mem.readInt(u32, value[4..8], .little),
                std.mem.readInt(u32, value[8..12], .little),
            });
        },
    }
}

pub fn printCell(
    writer: anytype,
    col: consumer.OutputAggregator.ColumnBuf,
    row_idx: usize,
    elem: schema.SchemaElement,
    is_jsonl: bool,
) !void {
    // The scale as resolved: a leaf may carry only the logical type, whose scale the legacy field then lacks.
    const scale = if (decimal_mod.kindFromSchema(&elem)) |k| k.scale else 0;
    // An unsigned column's lane holds its bits in a signed integer; print the stored value, not the sign bit.
    const unsigned = schema.isUnsignedInt(elem);
    switch (col) {
        .i32 => |tb| {
            if (tb.max_def > 0 and tb.def_levels.items[row_idx] < tb.max_def) {
                try writer.writeAll(if (is_jsonl) "null" else "");
            } else {
                const v = tb.values.items[row_idx];
                if (!is_jsonl) if (Temporal.of(elem)) |t| return t.write(writer, v);
                if (unsigned) try writer.print("{d}", .{@as(u32, @bitCast(v))}) else try writer.print("{d}", .{v});
            }
        },
        .i64 => |tb| {
            if (tb.max_def > 0 and tb.def_levels.items[row_idx] < tb.max_def) {
                try writer.writeAll(if (is_jsonl) "null" else "");
            } else {
                const v = tb.values.items[row_idx];
                if (!is_jsonl) if (Temporal.of(elem)) |t| return t.write(writer, v);
                if (unsigned) try writer.print("{d}", .{@as(u64, @bitCast(v))}) else try writer.print("{d}", .{v});
            }
        },
        .f32 => |tb| {
            if (tb.max_def > 0 and tb.def_levels.items[row_idx] < tb.max_def) {
                try writer.writeAll(if (is_jsonl) "null" else "");
            } else {
                try writeFloat(writer, tb.values.items[row_idx], is_jsonl);
            }
        },
        .f64 => |tb| {
            if (tb.max_def > 0 and tb.def_levels.items[row_idx] < tb.max_def) {
                try writer.writeAll(if (is_jsonl) "null" else "");
            } else {
                try writeFloat(writer, tb.values.items[row_idx], is_jsonl);
            }
        },
        .string => |tb| {
            if (tb.max_def > 0 and tb.def_levels.items[row_idx] < tb.max_def) {
                try writer.writeAll(if (is_jsonl) "null" else "");
            } else {
                const val = tb.values.items[row_idx];
                if (is_jsonl) {
                    try writeBytesJson(writer, val, TextKind.of(elem));
                } else {
                    try writeBytesCsv(writer, val, TextKind.of(elem));
                }
            }
        },
        .boolean => |tb| {
            if (tb.max_def > 0 and tb.def_levels.items[row_idx] < tb.max_def) {
                try writer.writeAll(if (is_jsonl) "null" else "");
            } else {
                try writer.writeAll(if (tb.values.items[row_idx]) "true" else "false");
            }
        },
        .i128 => |tb| {
            if (tb.max_def > 0 and tb.def_levels.items[row_idx] < tb.max_def) {
                try writer.writeAll(if (is_jsonl) "null" else "");
            } else {
                // INT96 rides this lane as exact nanoseconds.
                if (!is_jsonl) if (Temporal.of(elem)) |t| return t.write(writer, tb.values.items[row_idx]);
                try writeDecimal(writer, tb.values.items[row_idx], scale);
            }
        },
    }
}


test "printCell prints an unsigned column's stored value, not its sign bit" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var c32: consumer.OutputAggregator.TypedBuf(i32) = .{};
    try c32.values.append(a, @bitCast(@as(u32, 3_000_000_000)));
    var c64: consumer.OutputAggregator.TypedBuf(i64) = .{};
    try c64.values.append(a, @bitCast(@as(u64, 18_000_000_000_000_000_000)));

    const plain: schema.SchemaElement = .{
        .type = .INT32,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "c",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    var u32_elem = plain;
    u32_elem.converted_type = .UINT_32;
    var u64_elem = plain;
    u64_elem.type = .INT64;
    u64_elem.logical_type = .{ .INTEGER = .{ .bitWidth = 64, .isSigned = false } };

    var buf: [128]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try printCell(&w, .{ .i32 = c32 }, 0, u32_elem, true);
    try w.writeByte(' ');
    try printCell(&w, .{ .i64 = c64 }, 0, u64_elem, false);
    try w.writeByte(' ');
    try printCell(&w, .{ .i32 = c32 }, 0, plain, true);
    try testing.expectEqualStrings("3000000000 18000000000000000000 -1294967296", w.buffered());
}

test "regression: printCell scales a DECIMAL whose leaf carries only the logical type" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var c: consumer.OutputAggregator.TypedBuf(i128) = .{};
    try c.values.append(a, 125);
    try c.values.append(a, -350);
    const elem: schema.SchemaElement = .{
        .type = .INT32,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "d",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
        .logical_type = .{ .DECIMAL = .{ .scale = 2, .precision = 5 } },
    };
    var buf: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try printCell(&w, .{ .i128 = c }, 0, elem, true);
    try w.writeByte(' ');
    try printCell(&w, .{ .i128 = c }, 1, elem, true);
    try testing.expectEqualStrings("1.25 -3.50", w.buffered());
}

test "writeBytesJson: lossless bytes, UUID and INTERVAL text; a STRING value that is not UTF-8 fails" {
    const a = std.testing.allocator;
    const cases = [_]struct { in: []const u8, kind: TextKind, want: []const u8 }{
        .{ .in = "\xff", .kind = .bytes, .want = "{\"base64\":\"/w==\"}" },
        .{ .in = "\xfe", .kind = .bytes, .want = "{\"base64\":\"/g==\"}" },
        // Text that looks like base64 stays a string, so it never meets the object of the bytes it spells.
        .{ .in = "/w==", .kind = .bytes, .want = "\"/w==\"" },
        .{ .in = "a\"\x00\xe2\x80\xa8", .kind = .bytes, .want = "\"a\\\"\\u0000\\u2028\"" },
        .{ .in = "", .kind = .bytes, .want = "\"\"" },
        .{ .in = "", .kind = .base64, .want = "{\"base64\":\"\"}" },
        .{ .in = "ab", .kind = .base64, .want = "{\"base64\":\"YWI=\"}" },
        .{ .in = "abc", .kind = .base64, .want = "{\"base64\":\"YWJj\"}" },
        .{ .in = "caf\xc3\xa9\n", .kind = .utf8, .want = "\"caf\xc3\xa9\\n\"" },
        .{
            .in = "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\xfe\xff",
            .kind = .uuid,
            .want = "\"00010203-0405-0607-0809-0a0b0c0dfeff\"",
        },
        .{
            .in = "\x01\x00\x00\x00\xff\xff\xff\xff\x80\xee\x36\x00",
            .kind = .interval,
            .want = "{\"months\":1,\"days\":4294967295,\"millis\":3600000}",
        },
    };
    for (cases) |c| {
        var aw: std.Io.Writer.Allocating = .init(a);
        defer aw.deinit();
        try writeBytesJson(&aw.writer, c.in, c.kind);
        try std.testing.expectEqualStrings(c.want, aw.written());
    }
    // Past one 3072-byte chunk: padding only at the very end, and the text decodes back to the bytes.
    var long: [3074]u8 = undefined;
    for (&long, 0..) |*b, i| b.* = @truncate(i *% 7);
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();
    try writeBytesJson(&aw.writer, &long, .base64);
    const text = aw.written()["{\"base64\":\"".len .. aw.written().len - "\"}".len];
    try std.testing.expectEqual(@as(?usize, text.len - 1), std.mem.indexOfScalar(u8, text, '='));
    var back: [3074]u8 = undefined;
    try std.base64.standard.Decoder.decode(&back, text);
    try std.testing.expectEqualSlices(u8, &long, &back);

    // The ASCII fast path hands a sequence that starts past 16 bytes, or splits across a 16-byte block, to the check.
    for ([_][]const u8{ "0123456789abcdef\xff", "0123456789abcde\xc3\xa9x", "0123456789abcdefg\xc3" }, [_]bool{ false, true, false }) |in, want| {
        try std.testing.expectEqual(want, isUtf8(in));
    }
    // Past the one-pass size: text stays text, and one bad byte at the end makes the whole value base64.
    var wide: [1500]u8 = undefined;
    @memset(&wide, 'a');
    for ([_]bool{ true, false }) |valid| {
        if (!valid) wide[wide.len - 1] = 0xff;
        var out: std.Io.Writer.Allocating = .init(a);
        defer out.deinit();
        try writeBytesJson(&out.writer, &wide, .bytes);
        try std.testing.expectEqual(valid, out.written()[0] == '"');
    }
    var sink: std.Io.Writer.Discarding = .init(&.{});
    try std.testing.expectError(error.InvalidUtf8, writeBytesJson(&sink.writer, "a\xff", .utf8));
    try std.testing.expectError(error.InvalidBinaryValue, writeBytesJson(&sink.writer, "short", .uuid));
    try std.testing.expectError(error.InvalidBinaryValue, writeBytesJson(&sink.writer, "short", .interval));
}

test "TextKind: text annotations print as text, UUID and INTERVAL by type, blobs as base64, other bytes as found" {
    var elem: schema.SchemaElement = .{
        .type = .BYTE_ARRAY,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "v",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    try std.testing.expectEqual(TextKind.bytes, TextKind.of(elem));
    // GEOMETRY (union arm 17, a CRS) and GEOGRAPHY (18), which this reader keeps only as raw thrift.
    elem.logical_type = .{ .UNKNOWN = .{} };
    elem.logical_type_raw = &.{ 0x0c, 0x22, 0x18, 0x03, 'c', 'r', 's', 0x00, 0x00 };
    try std.testing.expectEqual(TextKind.base64, TextKind.of(elem));
    elem.logical_type_raw = &.{ 0x0c, 0x24, 0x00, 0x00 };
    try std.testing.expectEqual(TextKind.base64, TextKind.of(elem));
    // A future arm with no legacy annotation is plain bytes.
    elem.logical_type_raw = &.{ 0x0c, 0xc8, 0x01, 0x00, 0x00 };
    try std.testing.expectEqual(TextKind.bytes, TextKind.of(elem));
    elem.logical_type = null;
    elem.logical_type_raw = &.{};
    elem.converted_type = .UTF8;
    try std.testing.expectEqual(TextKind.utf8, TextKind.of(elem));
    // An annotation this reader does not interpret falls back to the legacy one.
    elem.logical_type = .{ .UNKNOWN = .{} };
    try std.testing.expectEqual(TextKind.utf8, TextKind.of(elem));
    elem.converted_type = .BSON;
    elem.logical_type = .{ .BSON = .{} };
    try std.testing.expectEqual(TextKind.base64, TextKind.of(elem));
    elem.logical_type = .{ .JSON = .{} };
    try std.testing.expectEqual(TextKind.utf8, TextKind.of(elem));
    elem.logical_type = null;
    elem.converted_type = .INTERVAL;
    try std.testing.expectEqual(TextKind.interval, TextKind.of(elem));
    elem.converted_type = null;
    elem.logical_type = .{ .UUID = .{} };
    try std.testing.expectEqual(TextKind.uuid, TextKind.of(elem));
    try std.testing.expectEqual(TextKind.utf8, TextKind.ofSource(null));
}


pub fn releaseStreamAnswer(answer: *scan.AggOutput) void {
    if (answer.integer_stream) |stream| {
        const rows = stream.state.group_count;
        stream.state.deinit();
        answer.integer_stream = null;
        answer.typed = .{ .rows = rows, .columns = &.{} };
    }
    if (answer.byte_stream) |stream| {
        const rows = stream.state.group_count;
        stream.state.deinit();
        answer.byte_stream = null;
        answer.typed = .{ .rows = rows, .columns = &.{} };
    }
}

/// The `AnswerType` of each answer column: a DATE/TIME/TIMESTAMP from the column its values pass through, and
/// whether it is an avg_state pair.
pub fn answerTypes(gpa: std.mem.Allocator, out: scan.AggOutput, state: scan.ScanState) ![]AnswerType {
    if (out.typed != null) return &.{};
    const n = if (out.group_cols) |cols| cols.len else out.aggs.len;
    const types = try gpa.alloc(AnswerType, n);
    for (types, 0..) |*ty, ci| {
        ty.* = .{};
        if (out.group_cols != null) {
            const all = state.group_col_types orelse continue;
            if (ci >= all.len) continue;
            ty.avg = all[ci].lane == .avg or all[ci].lane == .avg_dec;
            if (all[ci].source) |src| ty.temporal = Temporal.of(src);
            ty.decimal = (all[ci].lane == .dec or all[ci].lane == .avg_dec) and ty.temporal == null;
        } else {
            ty.avg = out.aggs[ci].value == .avg or out.aggs[ci].value == .avg_dec;
            if (ci < state.agg_sources.len) if (state.agg_sources[ci]) |src| {
                ty.temporal = Temporal.of(src);
            };
            if (ci < state.agg_calls.len) {
                const result = state.agg_calls[ci].result;
                ty.decimal = (result == .dec or result == .avg_dec) and ty.temporal == null;
            }
        }
    }
    return types;
}

/// The `TextKind` of each answer column, from the column its values pass through. With `check`, a text answer: its
/// STRING values are checked to be UTF-8 before any is printed, the column named on one that is not.
pub fn answerTextKinds(
    gpa: std.mem.Allocator,
    out: scan.AggOutput,
    state: scan.ScanState,
    check: bool,
    diag: ?*Diag,
) ![]TextKind {
    if (out.typed != null) return &.{};
    const grouped = out.group_cols != null;
    const n = if (out.group_cols) |cols| cols.len else out.aggs.len;
    const kinds = try gpa.alloc(TextKind, n);
    errdefer gpa.free(kinds);
    for (kinds, 0..) |*kind, ci| {
        const source: ?schema.SchemaElement = if (grouped)
            (if (state.group_col_types) |types| (if (ci < types.len) types[ci].source else null) else null)
        else if (ci < state.agg_sources.len) state.agg_sources[ci] else null;
        kind.* = TextKind.ofSource(source);
        if (!check or kind.* != .utf8) continue;
        const name = if (source) |src| src.name else if (out.group_cols) |cols| cols[ci] else out.aggs[ci].alias;
        if (out.group_rows) |rows| {
            for (rows) |row| try checkUtf8(row[ci], name, diag);
        } else if (out.byte_stream) |stream| {
            var it = stream.state.orderedCursor(0);
            while (it.next()) |id| try checkUtf8(ownerTextValue(stream, id, stream.outputs[ci]), name, diag);
        } else if (!grouped) try checkUtf8(out.aggs[ci].value, name, diag);
    }
    return kinds;
}

fn checkUtf8(v: scan.AggValue, name: []const u8, diag: ?*Diag) !void {
    if (v == .s and !std.unicode.utf8ValidateSlice(v.s)) {
        Diag.setColumn(diag, name);
        return error.InvalidUtf8;
    }
}


/// Shared row framing and value formatting for materialized and owner answers, on every text target.
pub fn writeAggregateText(
    writer: anytype,
    answer: scan.AggOutput,
    kinds: []const TextKind,
    types: []const AnswerType,
    rows: usize,
    format: PrintFormat,
) !void {
    return writeAggregateTextValue(writer, answer, kinds, types, rows, format, true);
}

/// A grouped JSON array inside a service envelope, without changing its wire whitespace.
pub fn writeAggregateJsonValue(
    writer: anytype,
    answer: scan.AggOutput,
    kinds: []const TextKind,
    rows: usize,
) !void {
    return writeAggregateTextValue(writer, answer, kinds, &.{}, rows, .json, false);
}

fn writeAggregateTextValue(
    writer: anytype,
    answer: scan.AggOutput,
    kinds: []const TextKind,
    types: []const AnswerType,
    rows: usize,
    format: PrintFormat,
    trailing_newline: bool,
) !void {
    var emit = AggregateTextEmitter(@TypeOf(writer)){
        .writer = writer,
        .kinds = kinds,
        .types = types,
        .format = format,
        .trailing_newline = trailing_newline,
    };
    try emit.start(answer);
    if (answer.group_rows) |groups| {
        for (groups) |values| {
            try emit.beginRow();
            for (answer.group_cols.?, values, 0..) |name, value, ci| try emit.field(name, value, ci);
            try emit.endRow();
        }
    } else if (answer.integer_stream) |stream| {
        try emitOwnerText(&emit, answer.group_cols.?, stream);
    } else if (answer.byte_stream) |stream| {
        try emitOwnerText(&emit, answer.group_cols.?, stream);
    } else if (rows > 0 and answer.group_cols == null) {
        try emit.beginRow();
        for (answer.aggs, 0..) |item, ci| try emit.field(item.alias, item.value, ci);
        try emit.endRow();
    }
    try emit.finish();
}

fn AggregateTextEmitter(comptime Writer: type) type {
    return struct {
        writer: Writer,
        kinds: []const TextKind,
        types: []const AnswerType,
        format: PrintFormat,
        emitted: usize = 0,
        trailing_newline: bool,

        fn start(e: *@This(), answer: scan.AggOutput) !void {
            if (e.format == .json) try e.writer.writeByte('[');
            if (e.format != .csv) return;
            const n = if (answer.group_cols) |cols| cols.len else answer.aggs.len;
            for (0..n) |ci| {
                if (ci > 0) try e.writer.writeByte(',');
                const name = if (answer.group_cols) |cols| cols[ci] else answer.aggs[ci].alias;
                if (typeAt(e.types, ci).avg) {
                    try writeCsvParts(e.writer, &.{ name, "__sum" });
                    try e.writer.writeByte(',');
                    try writeCsvParts(e.writer, &.{ name, "__count" });
                } else try writeCsvString(e.writer, name);
            }
            try e.writer.writeByte('\n');
        }

        fn beginRow(e: *@This()) !void {
            if (e.format == .csv) return;
            if (e.format == .json and e.emitted > 0) try e.writer.writeByte(',');
            try e.writer.writeByte('{');
        }

        fn field(e: *@This(), name: []const u8, value: scan.AggValue, ci: usize) !void {
            if (ci > 0) try e.writer.writeByte(',');
            if (e.format == .csv) return writeAggValueCsv(e.writer, value, kindAt(e.kinds, ci), typeAt(e.types, ci));
            try e.writer.writeByte('"');
            try writeJsonString(e.writer, name);
            try e.writer.writeAll("\":");
            try writeTypedAggValueJson(e.writer, value, kindAt(e.kinds, ci));
        }

        fn endRow(e: *@This()) !void {
            try e.writer.writeAll(switch (e.format) {
                .csv => "\n",
                .json => "}",
                else => "}\n",
            });
            e.emitted += 1;
        }

        fn finish(e: *@This()) !void {
            if (e.format == .json) try e.writer.writeAll(if (e.trailing_newline) "]\n" else "]");
        }
    };
}

fn ownerTextValue(stream: anytype, id: u32, out: anytype) scan.AggValue {
    const group = &stream.state.groups[id];
    return switch (out.source) {
        .key => if (@TypeOf(stream) == scan.IntegerStream)
            (if (group.keyValue()) |v| .{ .i = v } else .null_val)
        else
            (if (group.keyBytes()) |v| .{ .s = v } else .null_val),
        .agg => |ai| if (ai == stream.count_call)
            .{ .i = group.countValue() }
        else if (group.sumValue()) |v| .{ .i = v } else .null_val,
    };
}

fn emitOwnerText(emit: anytype, names: []const []const u8, stream: anytype) !void {
    var it = stream.state.orderedCursor(0);
    while (it.next()) |id| {
        try emit.beginRow();
        for (names, stream.outputs, 0..) |name, out, ci| try emit.field(name, ownerTextValue(stream, id, out), ci);
        try emit.endRow();
    }
}

test "owner text matches materialized rows for wide sums, NULLs and every text format" {
    const owner = @import("../core/owner_groups.zig");
    const a = std.testing.allocator;
    const names = [_][]const u8{ "s", "key", "c" };
    const wide: i128 = @as(i128, std.math.maxInt(i64)) * 2;
    inline for (.{ false, true }) |bytes_key| {
        const O = if (bytes_key) owner.Full else owner.IntFull;
        var budget: core_memory.SharedBudget = .{ .limit = 256 << 20 };
        defer std.debug.assert(budget.used.load(.monotonic) == 0);
        const state = (try O.State.init(a, &budget, 6, 2, 0, 1, {}, owner.full_parts, 0)).?;
        var live = true;
        defer if (live) state.deinit();
        state.wide_sums = true;
        for (0..6) |i| {
            const k = i % 3;
            const v: ?i64 = switch (i) {
                0, 3 => std.math.maxInt(i64),
                2 => 3,
                4 => -4,
                else => null,
            };
            if (bytes_key) {
                try state.locals[i % 2].append(switch (k) {
                    0 => "z",
                    1 => null,
                    else => "a,\n\"",
                }, v, @intCast(i));
            } else {
                try state.locals[i % 2].append(
                    if (k == 1) null else @as(?i64, if (k == 0) 17 else -5),
                    v,
                    @intCast(i),
                );
            }
        }
        try state.finish();
        const Stream = if (bytes_key) scan.ByteStream else scan.IntegerStream;
        const stream: Stream = .{
            .state = state,
            .count_call = 0,
            .outputs = &.{
                .{ .source = .{ .agg = 1 }, .name = names[0] },
                .{ .source = .{ .key = 0 }, .name = names[1] },
                .{ .source = .{ .agg = 0 }, .name = names[2] },
            },
        };
        const r0 = [_]scan.AggValue{ .{ .i = wide }, if (bytes_key) .{ .s = "z" } else .{ .i = 17 }, .{ .i = 2 } };
        const r1 = [_]scan.AggValue{ .{ .i = -4 }, .null_val, .{ .i = 2 } };
        const r2 = [_]scan.AggValue{ .{ .i = 3 }, if (bytes_key) .{ .s = "a,\n\"" } else .{ .i = -5 }, .{ .i = 2 } };
        const rows = [_][]const scan.AggValue{ &r0, &r1, &r2 };
        const kinds = [_]TextKind{ .bytes, .utf8, .bytes };
        const answer: scan.AggOutput = if (bytes_key)
            .{ .group_cols = &names, .byte_stream = stream }
        else
            .{ .group_cols = &names, .integer_stream = stream };
        try std.testing.expectEqual(@as(usize, 3), answer.rowCount());
        for ([_]PrintFormat{ .json, .jsonl, .csv }) |format| {
            var expected: std.Io.Writer.Allocating = .init(a);
            defer expected.deinit();
            var got: std.Io.Writer.Allocating = .init(a);
            defer got.deinit();
            try writeAggregateText(
                &expected.writer,
                .{ .group_cols = &names, .group_rows = &rows },
                &kinds,
                &.{},
                3,
                format,
            );
            try writeAggregateText(&got.writer, answer, &kinds, &.{}, 3, format);
            try std.testing.expectEqualStrings(expected.written(), got.written());
        }
        var envelope: std.Io.Writer.Allocating = .init(a);
        defer envelope.deinit();
        try envelope.writer.writeAll("{\"agg\":");
        try writeAggregateJsonValue(&envelope.writer, answer, &kinds, 3);
        try envelope.writer.writeByte('}');
        var parsed = try std.json.parseFromSlice(std.json.Value, a, envelope.written(), .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(usize, 3), parsed.value.object.get("agg").?.array.items.len);
        try std.testing.expect(std.mem.endsWith(u8, envelope.written(), "]}"));
        var tiny: [4]u8 = undefined;
        var failing: std.Io.Writer = .fixed(&tiny);
        try std.testing.expectError(error.WriteFailed, writeAggregateText(&failing, answer, &kinds, &.{}, 3, .json));
        var cursor = state.orderedCursor(1);
        const ids = try state.window(a, 1, 3);
        defer a.free(ids);
        for (ids) |id| try std.testing.expectEqual(id, cursor.next().?);
        try std.testing.expect(cursor.next() == null);
        var drained = answer;
        releaseStreamAnswer(&drained);
        live = false;
        try std.testing.expect(drained.integer_stream == null and drained.byte_stream == null);
        try std.testing.expectEqual(@as(usize, 3), drained.rowCount());
        try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    }
}


test "owner text checks every UTF8 key before emission, including the last ordinal" {
    const owner = @import("../core/owner_groups.zig");
    const a = std.testing.allocator;
    const state = (try owner.Full.State.init(a, null, 3, 2, 0, 1, {}, owner.full_parts, 0)).?;
    defer state.deinit();
    for ([_][]const u8{ "first", "second", "last\xff" }, 0..) |key, i|
        try state.locals[i % 2].append(key, 1, @intCast(i));
    try state.finish();
    const names = [_][]const u8{ "alias", "c", "s" };
    const stream: scan.ByteStream = .{
        .state = state,
        .count_call = 0,
        .outputs = &.{
            .{ .source = .{ .key = 0 }, .name = names[0] },
            .{ .source = .{ .agg = 0 }, .name = names[1] },
            .{ .source = .{ .agg = 1 }, .name = names[2] },
        },
    };
    const source: schema.SchemaElement = .{
        .type = .BYTE_ARRAY,
        .name = "original",
        .converted_type = .UTF8,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    const columns = [_]scan.GroupColType{
        .{ .lane = .string, .nullable = true, .source = source },
        .{ .lane = .i64, .nullable = false },
        .{ .lane = .i64, .nullable = true },
    };
    const st: scan.ScanState = .{ .accumulators = &.{}, .agg_calls = &.{}, .group_col_types = &columns };
    const answer: scan.AggOutput = .{ .group_cols = &names, .byte_stream = stream };
    var diag: Diag = .{};
    try std.testing.expectError(error.InvalidUtf8, answerTextKinds(a, answer, st, true, &diag));
    try std.testing.expectEqualStrings("original", diag.column.get());
    const kinds = try answerTextKinds(a, answer, st, false, null);
    defer a.free(kinds);
    try std.testing.expectEqual(TextKind.utf8, kinds[0]);
}

fn typeAt(types: []const AnswerType, ci: usize) AnswerType {
    return if (ci < types.len) types[ci] else .{};
}


/// The same single-object JSON convention as query results. Passes describe permitted work, not a runtime trace.
/// The facts a plan was made from, each with how far it can be trusted (`exact`, `bound`, `estimate`), the strategies
/// whose exactness preconditions hold, and the one the cost rules chose (its `reasons` say why).
fn writeExplainFacts(writer: anytype, explanation: scan.AggregateExplanation) !void {
    const plan = explanation.plan;
    const f = plan.facts;
    try writer.print(",\"facts\":{{\"rows\":{{\"value\":{d},\"provenance\":\"exact\"}},\"row_groups\":{{\"value\":{d},\"provenance\":\"exact\"}}", .{ f.rows, f.row_groups });
    if (plan.ndv_stat) |stat| {
        const prov = if (stat.confidence == .upper_bound) "bound" else "unknown";
        try writer.print(",\"groups\":{{\"value\":{d},\"provenance\":\"{s}\",\"source\":\"{s}\"}}", .{ stat.hi, prov, @tagName(stat.source) });
        if (stat.estimate) |e| try writer.print(",\"groups_estimate\":{{\"value\":{d},\"provenance\":\"estimate\"}}", .{e});
        if (stat.entries) |e| try writer.print(",\"dictionary_entries\":{{\"value\":{d},\"provenance\":\"exact\"}}", .{e});
    }
    if (f.pass_share) |p| try writer.print(",\"filter_pass\":{{\"value\":{d:.4},\"provenance\":\"estimate\"}}", .{p});
    if (f.sampled_pass) |p|
        try writer.print(",\"filter_pass_sampled\":{{\"value\":{d:.4},\"provenance\":\"first_row_group\"}}", .{p});
    if (f.planning) |pl| try writer.print(",\"planning\":{{\"source\":\"{s}\",\"why\":\"{s}\"}}", .{
        @tagName(pl.source), @tagName(pl.why),
    });
    try writer.print(",\"readers\":{},\"probe\":{{\"ranges\":{d},\"bytes\":{d},\"capped\":{}}}}}", .{ f.readers, f.probe_ranges, f.probe_bytes, f.probe_capped });
    try writer.writeAll(",\"candidates\":[");
    var sep: []const u8 = "";
    for (std.enums.values(@FieldType(scan.AggregateDispatch, "strategy"))) |kind| {
        try writer.print("{s}{{\"strategy\":\"{s}\",\"admissible\":{}}}", .{ sep, @tagName(kind), f.admissible.contains(kind) });
        sep = ",";
    }
    try writer.print("{s}{{\"strategy\":\"dense\",\"admissible\":{}}}", .{ sep, explanation.dense });
    try writer.print("],\"winner\":\"{s}\",\"rule\":\"{s}\"", .{ explanation.strategyName(), @tagName(scan.decidingRule(plan)) });
}

pub fn writeAggregateExplanationJson(writer: anytype, explanation: AggregateExplanation) !void {
    const a = explanation.aggregate;
    const d = a.plan.dispatch;
    const merge = if (a.dense) "dense_ids" else if (d.strategy == .partitioned_hash)
        "shared_partitions"
    else if (d.use_blocks) "ordered_blocks" else if (d.strategy == .scalar) "scalar" else "partitions";
    try writer.print(
        "{{\"ok\":true,\"explain\":true,\"strategy\":\"{s}\",\"merge\":\"{s}\",\"io\":\"{s}\"," ++
            "\"io_reason\":\"{s}\"," ++
            "\"workers\":{d},\"tables\":{d},\"work_items\":{d},\"max_memory\":{d},\"encoded\":{},\"reasons\":[",
        .{
            a.strategyName(),
            merge,
            @tagName(explanation.io_mode),
            explanation.io_reason.phrase(),
            a.workers,
            d.tables,
            a.work_items,
            explanation.max_memory,
            explanation.encoded,
        },
    );
    var sep: []const u8 = "";
    var reasons = a.plan.reasons.iterator();
    while (reasons.next()) |reason| {
        try writer.print("{s}\"{s}\"", .{ sep, @tagName(reason) });
        sep = ",";
    }
    try writer.writeAll("],\"record_path\":");
    if (a.record_path) |r| {
        try writer.print("{{\"layout\":\"{s}\",\"reservation\":{d},\"partitions\":{d},\"conditional\":true}}", .{
            @tagName(r.layout), r.reservation, r.partitions,
        });
    } else try writer.writeAll("null");
    try writer.writeAll(",\"ndv\":");
    if (a.plan.ndv_stat) |stat| {
        try writer.print("{{\"source\":\"{s}\",\"lo\":{d},\"hi\":{d},\"rows\":{d},\"covered_rows\":{d},\"encoding_stats_rows\":{d},\"confidence\":\"{s}\",\"dictionary_bytes\":{d},\"column_bytes\":{d},\"key_bytes_estimate\":{d},\"union_attempted\":{},\"union_complete\":{}", .{
            @tagName(stat.source),
            stat.lo,
            stat.hi,
            stat.rows,
            stat.covered_rows,
            stat.encoding_stats_rows,
            @tagName(stat.confidence),
            stat.dictionary_bytes,
            stat.column_bytes,
            stat.key_bytes,
            stat.union_attempted,
            stat.union_complete,
        });
        // Separate a cost estimate from a proven upper bound, especially for composite keys.
        try writer.writeAll(",\"estimate\":");
        if (stat.estimate) |n| try writer.print("{d}", .{n}) else try writer.writeAll("null");
        try writer.writeAll("}");
    } else try writer.writeAll("null");
    try writer.writeAll(",\"conjuncts\":[");
    for (explanation.conjuncts, 0..) |c, i| {
        if (i > 0) try writer.writeByte(',');
        try writer.writeAll("{\"column\":\"");
        try writeJsonString(writer, c.column);
        try writer.print("\",\"cost\":\"{s}\"}}", .{@tagName(c.cost)});
    }
    try writer.writeByte(']');
    try writeExplainFacts(writer, a);
    try writer.writeAll(",\"plain_bytes_estimate\":");
    if (a.plan.plain_bytes) |n| try writer.print("{d}", .{n}) else try writer.writeAll("null");
    try writer.writeAll(",\"group_bytes_estimate\":");
    if (a.plan.group_bytes) |n| try writer.print("{d}", .{n}) else try writer.writeAll("null");
    try writer.print(",\"shared_tables\":{d},\"count_agg\":", .{
        @as(usize, if (d.strategy == .partitioned_hash) 32 else 0),
    });
    if (d.count_agg) |idx| try writer.print("{d}", .{idx}) else try writer.writeAll("null");
    try writer.writeAll(",\"sketch\":");
    if (explanation.sketch) |cfg| {
        try writer.print("{{\"cell_bits\":{d},\"admit_per_row\":{d},\"certify\":{},\"census_allowed\":{},\"census_fused\":{},\"census_cell_repeat_gate\":{},\"census_fp_bits\":{d},\"census_check_rows\":{d}}}", .{
            cfg.cell_bits,             cfg.admit_per_row,  cfg.certify,           d.census_allowed, d.census_fused,
            d.census_cell_repeat_gate, cfg.census_fp_bits, cfg.census_check_rows,
        });
    } else try writer.writeAll("null");
    try writer.writeAll(",\"passes\":[");
    switch (d.strategy) {
        .scalar, .plain_hash, .partitioned_hash => try writeExplainPass(writer, "aggregate", null, a.column_names, a.columns),
        .first_keys => {
            try writeExplainPass(writer, "discover_prefix", null, a.column_names, a.columns);
            try writer.writeAll(",");
            try writeExplainPass(writer, "admit", "discovery_did_not_read_all", a.column_names, a.columns);
        },
        .sketch => if (d.distinct_census) {
            // The census rides the count pass; its admit pass reads the keys it admits, unless it gives up first.
            try writeExplainPass(writer, "census_count", null, a.column_names, a.count_columns.?);
            try writer.writeAll(",");
            try writeExplainPass(writer, "admit", "census_admits_few_rows", a.column_names, a.columns);
            try writer.writeAll(",");
            try writeExplainPass(
                writer,
                "aggregate",
                "census_abandoned_or_certificate_failed",
                a.column_names,
                a.columns,
            );
        } else {
            try writeExplainPass(writer, "count", null, a.column_names, a.count_columns.?);
            if (d.census_allowed) {
                try writer.writeAll(",");
                try writeExplainPass(writer, "census_count", if (d.census_fused)
                    "sketch_bails_and_fused_census_dropped"
                else if (d.census_cell_repeat_gate)
                    "sketch_bails_and_cells_favor_census"
                else
                    "sketch_bails", a.column_names, a.count_columns.?);
            }
            try writer.writeAll(",");
            try writeExplainPass(writer, "admit", "sketch_or_census_cut_viable", a.column_names, a.columns);
            try writer.writeAll(",");
            try writeExplainPass(
                writer,
                "aggregate",
                "cut_not_viable_or_certificate_failed",
                a.column_names,
                a.columns,
            );
        },
    }
    // The stage every strategy feeds (`ResultProgram`): what it keeps, ranks and drops after the groups are complete.
    const r = explanation.result;
    try writer.print("],\"result_program\":{{\"having\":{},\"hidden_columns\":{d},\"order_keys\":{d},\"cut_limit\":\"{s}\"}}", .{
        r.having, r.hidden, r.order_keys, @tagName(r.cut_limit),
    });
    try writer.writeAll(",\"runtime\":{\"batch_encoding\":true,\"slicing\":true,\"bloom_probing\":\"execution_only\"}}\n");
}

fn writeExplainPass(
    writer: anytype,
    kind: []const u8,
    condition: ?[]const u8,
    names: []const []const u8,
    columns: agg_plan.Columns,
) !void {
    try writer.print("{{\"kind\":\"{s}\",\"condition\":", .{kind});
    if (condition) |c| try writeJsonQuoted(writer, c) else try writer.writeAll("null");
    try writer.writeAll(",\"columns\":");
    try writeExplainColumns(writer, names, columns.read);
    try writer.writeAll(",\"full_match_columns\":");
    try writeExplainColumns(writer, names, columns.consumed);
    try writer.writeAll("}");
}

fn writeExplainColumns(writer: anytype, names: []const []const u8, mask: []const bool) !void {
    try writer.writeAll("[");
    var sep: []const u8 = "";
    for (names, mask) |name, reads| if (reads) {
        try writer.writeAll(sep);
        try writeJsonQuoted(writer, name);
        sep = ",";
    };
    try writer.writeAll("]");
}

