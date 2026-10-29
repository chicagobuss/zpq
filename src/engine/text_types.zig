//! Shared answer annotations, below query contracts and formatting.
const std = @import("std");
const schema = @import("../core/schema.zig");

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
