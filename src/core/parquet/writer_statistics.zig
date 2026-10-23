//! Known writer defects only. Unknown/malformed created_by is treated as modern, never as a decode error.
//! This policy can remove statistics, including counts, but cannot make otherwise unreadable bounds usable.
//! Sources and compatibility choices: docs/writer-statistics.md.

const std = @import("std");
const schema = @import("../schema.zig");

const spaces = " \t\r\n\x0b\x0c";
const Writer = struct { app: enum { cpp, mr }, version: Version };

const Version = struct {
    number: [3]u31 = .{ 0, 0, 0 },
    unknown: bool = false,
    pre: ?[]const u8 = null,

    fn numericOrder(v: Version, other: [3]u31) std.math.Order {
        return std.mem.order(u31, &v.number, &other);
    }

    // parquet-mr SemanticVersion places its extra suffix (e.g. rc2) before any ordinary prerelease.
    fn order(v: Version, other: Version) std.math.Order {
        const n = v.numericOrder(other.number);
        if (n != .eq) return n;
        if (v.unknown != other.unknown) return if (v.unknown) .lt else .gt;
        const a: std.SemanticVersion = .{
            .major = v.number[0],
            .minor = v.number[1],
            .patch = v.number[2],
            .pre = v.pre,
        };
        const b: std.SemanticVersion = .{
            .major = other.number[0],
            .minor = other.number[1],
            .patch = other.number[2],
            .pre = other.pre,
        };
        return a.order(b);
    }
};

/// Arrow's signed-order/equal-extrema guard, plus parquet-mr's binary-corruption/CDH rule. A refusal applies to
/// the entire statistics record, not only its extrema. --trust-stats cannot override a known writer defect.
pub fn usable(
    created_by: ?[]const u8,
    physical: schema.Type,
    elem: ?schema.SchemaElement,
    min: ?[]const u8,
    max: ?[]const u8,
) bool {
    const writer = parse(created_by) orelse return true;
    const fixed: [3]u31 = if (writer.app == .cpp) .{ 1, 3, 0 } else .{ 1, 10, 0 };
    if (writer.version.numericOrder(fixed) == .lt and !signedOrder(physical, elem)) {
        const equal = min != null and max != null and std.mem.eql(u8, min.?, max.?);
        if (!equal) return false;
    }
    if (writer.app != .mr or (physical != .BYTE_ARRAY and physical != .FIXED_LEN_BYTE_ARRAY)) return true;
    const binary_fixed: Version = .{ .number = .{ 1, 8, 0 } };
    if (writer.version.order(binary_fixed) != .lt) return true;
    const cdh_start: Version = .{ .number = .{ 1, 5, 0 }, .pre = "cdh5.5.0" };
    const cdh_end: Version = .{ .number = .{ 1, 5, 0 } };
    return writer.version.order(cdh_start) != .lt and writer.version.order(cdh_end) == .lt;
}

fn signedOrder(physical: schema.Type, elem: ?schema.SchemaElement) bool {
    if (elem) |e| {
        if (e.logical_type) |lt| return switch (lt) {
            .INTEGER => |it| it.isSigned,
            .DECIMAL, .DATE, .TIME, .TIMESTAMP, .FLOAT16 => true,
            else => false,
        };
        if (e.converted_type) |ct| return switch (ct) {
            .INT_8,
            .INT_16,
            .INT_32,
            .INT_64,
            .DATE,
            .TIME_MICROS,
            .TIME_MILLIS,
            .TIMESTAMP_MICROS,
            .TIMESTAMP_MILLIS,
            .DECIMAL,
            => true,
            else => false,
        };
    }
    return switch (physical) {
        .BOOLEAN, .INT32, .INT64, .FLOAT, .DOUBLE => true,
        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY, .INT96 => false,
    };
}

/// Allocation-free, bounded by created_by length. Recognize exact application names, whitespace, optional build
/// wrappers, numeric Arrow versions (missing minor/patch = 0) and parquet-mr's rc/prerelease/build suffixes.
/// Parse all of a known version before applying rules: a malformed prefix never becomes an old partial version.
fn parse(created_by: ?[]const u8) ?Writer {
    const text = std.mem.trim(u8, created_by orelse return null, spaces);
    var words = std.mem.tokenizeAny(u8, text, spaces);
    const app = words.next() orelse return null;
    const kind: @FieldType(Writer, "app") = kind: {
        if (std.mem.eql(u8, app, "parquet-mr")) break :kind .mr;
        if (std.mem.eql(u8, app, "parquet-cpp")) break :kind .cpp;
        return null;
    };
    if (!std.mem.eql(u8, words.next() orelse return null, "version")) return null;
    const token = words.next() orelse return null;
    const version = parseVersion(token) orelse return null;
    const rest = std.mem.trim(u8, words.rest(), spaces);
    if (rest.len != 0) {
        if (rest.len < 2 or rest[0] != '(' or rest[rest.len - 1] != ')') return null;
        const inner = std.mem.trim(u8, rest[1 .. rest.len - 1], spaces);
        var build = std.mem.tokenizeAny(u8, inner, spaces);
        if (!std.mem.eql(u8, build.next() orelse return null, "build")) return null;
        if (std.mem.findAny(u8, build.rest(), "()") != null) return null;
    }
    return .{ .app = kind, .version = version };
}

fn parseVersion(text: []const u8) ?Version {
    var v: Version = .{};
    var at: usize = 0;
    for (&v.number, 0..) |*part, i| {
        const start = at;
        while (at < text.len and std.ascii.isDigit(text[at])) : (at += 1) {}
        if (at == start) return null;
        part.* = std.fmt.parseUnsigned(u31, text[start..at], 10) catch return null;
        if (at == text.len) return v;
        if (i != 2) {
            if (text[at] != '.') return null;
            at += 1;
        }
    }
    const unknown = at;
    while (at < text.len and text[at] != '-' and text[at] != '+') : (at += 1) {
        const c = text[at];
        if (!std.ascii.isAlphanumeric(c) and c != '.' and c != '_') return null;
    }
    v.unknown = at != unknown;
    if (at < text.len and text[at] == '-') {
        const start = at + 1;
        at = std.mem.findScalarPos(u8, text, start, '+') orelse text.len;
        const pre = text[start..at];
        if (!identifiers(pre, true)) return null;
        v.pre = pre;
    }
    if (at < text.len) {
        if (text[at] != '+' or !identifiers(text[at + 1 ..], false)) return null;
    }
    return v;
}

fn identifiers(text: []const u8, pre: bool) bool {
    var parts = std.mem.splitScalar(u8, text, '.');
    while (parts.next()) |id| {
        if (id.len == 0) return false;
        var digits = true;
        for (id) |c| {
            if (!std.ascii.isAlphanumeric(c) and c != '-') return false;
            digits = digits and std.ascii.isDigit(c);
        }
        // std.SemanticVersion.order parses numeric identifiers; validate their range before it can do so.
        if (pre and digits) _ = std.fmt.parseUnsigned(u31, id, 10) catch return false;
    }
    return true;
}

const testing = std.testing;

test "writer statistics: numeric boundaries and equal extrema are independent of binary corruption" {
    const Case = struct { writer: []const u8, signed: bool = true, bytes: bool, equal_bytes: bool };
    const spaced = " \tparquet-mr \n version \x0b 1.7.0rc99-SNAPSHOT \r ( build hash ) ";
    const cases = [_]Case{
        .{ .writer = "parquet-cpp version 1.2.9", .bytes = false, .equal_bytes = true },
        .{ .writer = "parquet-cpp version 1.3.0", .bytes = true, .equal_bytes = true },
        .{ .writer = "parquet-cpp version 1.3.0-SNAPSHOT", .bytes = true, .equal_bytes = true },
        .{ .writer = "parquet-cpp version 1.10.0", .bytes = true, .equal_bytes = true },
        .{ .writer = "parquet-mr version 1.7.999", .bytes = false, .equal_bytes = false },
        .{ .writer = "parquet-mr version 1.8.0-SNAPSHOT", .bytes = false, .equal_bytes = false },
        .{ .writer = "parquet-mr version 1.8.0rc2-SNAPSHOT", .bytes = false, .equal_bytes = false },
        .{ .writer = "parquet-mr version 1.8.0", .bytes = false, .equal_bytes = true },
        .{ .writer = "parquet-mr version 1.9.999", .bytes = false, .equal_bytes = true },
        .{ .writer = "parquet-mr version 1.10.0", .bytes = true, .equal_bytes = true },
        .{ .writer = "parquet-mr version 1.10.0-SNAPSHOT", .bytes = true, .equal_bytes = true },
        .{ .writer = "parquet-mr version 2.0.0", .bytes = true, .equal_bytes = true },
        .{ .writer = "parquet-mr version 1.5.0-cdh5.4.999", .bytes = false, .equal_bytes = false },
        .{ .writer = "parquet-mr version 1.5.0-cdh5.5.0", .bytes = false, .equal_bytes = true },
        .{ .writer = "parquet-mr version 1.5.0-cdh5.5.0-SNAPSHOT (build hash)", .bytes = false, .equal_bytes = true },
        .{ .writer = "parquet-mr version 1.5.0-cdh5.5.1+build", .bytes = false, .equal_bytes = true },
        .{ .writer = "parquet-mr version 1.5.0-cdh5.6.0", .bytes = false, .equal_bytes = true },
        .{ .writer = "parquet-mr version 1.5.0ab-cdh5.5.0+cd", .bytes = false, .equal_bytes = false },
        .{ .writer = "parquet-mr version 1.5.0", .bytes = false, .equal_bytes = false },
        .{ .writer = "parquet-mr version 1.5.1-cdh5.5.0", .bytes = false, .equal_bytes = false },
        .{ .writer = spaced, .bytes = false, .equal_bytes = false },
    };
    for (cases) |c| {
        try testing.expectEqual(c.signed, usable(c.writer, .INT64, null, "a", "b"));
        for ([_]schema.Type{ .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY }) |t| {
            try testing.expectEqual(c.bytes, usable(c.writer, t, null, "a", "b"));
            try testing.expectEqual(c.equal_bytes, usable(c.writer, t, null, "a", "a"));
        }
        try testing.expectEqual(c.bytes, usable(c.writer, .BYTE_ARRAY, null, null, null));
    }
}

test "writer statistics: unknown malformed and modern applications retain existing statistics" {
    const modern = [_]?[]const u8{
        null,
        "",
        "unknown",
        "parquet-mr",
        "parquet-mr version",
        "parquet-mr version nope",
        "parquet-mr version 1.",
        "parquet-mr version 1.7.",
        "parquet-mr version 1.7.x",
        "parquet-mr version 2147483648.0.0",
        "parquet-mr version -1.7.0",
        "parquet-mr version 1.7.0-",
        "parquet-mr version 1.7.0+",
        "parquet-mr version 1.7.0+a+b",
        "parquet-mr version 1.7.0 garbage",
        "parquet-mr version 1.7.0 (build missing",
        "parquet-mr version 1.7.0-cdh5.5.999999999999999999999999",
        "parquet-mr version 1.7.0 (bad hash)",
        "parquet-mr version 1.7.0 (build hash) trailing",
        "parquet-mr-other version 1.0.0",
        "zpq version 0.5.0",
        "parquet-cpp-arrow version 24.0.0",
        "Arrow version 1.0.0",
        "DuckDB version v2.0.0",
        "Polars",
        "Spark version 4.0.0",
        "parquet-mr version 1.15.2 (build spark)",
    };
    for (modern) |s| for ([_]schema.Type{ .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY, .INT32, .DOUBLE }) |t| {
        try testing.expect(usable(s, t, null, "a", "b"));
        try testing.expect(usable(s, t, null, null, null));
    };
    for ([_][]const u8{ "parquet-mr version 1", "parquet-mr version 1.7", "parquet-cpp version 1.2" }) |s|
        try testing.expect(!usable(s, .BYTE_ARRAY, null, "a", "b"));
}

test "writer statistics: signed logical order is distinct from the physical byte order" {
    var e = @import("test_fixtures.zig").statsLeaf(.INT32);
    e.logical_type = .{ .INTEGER = .{ .bitWidth = 32, .isSigned = false } };
    const old = "parquet-cpp version 1.2.0";
    try testing.expect(!usable(old, .INT32, e, "a", "b"));
    try testing.expect(usable(old, .INT32, e, "a", "a"));
    e.logical_type = null;
    e.converted_type = .UINT_32;
    try testing.expect(!usable(old, .INT32, e, "a", "b"));
    e.converted_type = .DATE;
    try testing.expect(usable(old, .INT32, e, "a", "b"));
    e.type = .FIXED_LEN_BYTE_ARRAY;
    e.converted_type = .DECIMAL;
    try testing.expect(usable(old, .FIXED_LEN_BYTE_ARRAY, e, "a", "b"));
    try testing.expect(!usable("parquet-mr version 1.7.0", .FIXED_LEN_BYTE_ARRAY, e, "a", "a"));
    e.logical_type = .{ .DECIMAL = .{ .precision = 9, .scale = 2 } };
    try testing.expect(usable("parquet-mr version 1.8.0", .FIXED_LEN_BYTE_ARRAY, e, "a", "b"));
}
