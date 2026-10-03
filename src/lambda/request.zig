//! The JSON invocation event, decoded with std.json: string escapes (`\"`, `\uXXXX` and surrogate pairs) decode,
//! and a malformed event (bad syntax or UTF-8, a lone surrogate, a duplicate key, a field of the wrong type) is an
//! error rather than a field cut short at its first escaped quote. Unknown fields are ignored: callers send extras
//! such as `mode`. They are skipped by the scanner, never built, so a large one costs no memory.

const std = @import("std");

pub const Error = error{
    /// A known field holds the wrong JSON type; `Request.parse` names it.
    BadFieldType,
    /// Neither `inputs` nor `s3_url`.
    MissingField,
} || std.json.ParseError(std.json.Scanner) || std.mem.Allocator.Error;

/// The fields the handler reads, each kept as a JSON value until checked so a wrong type can be named. JSON null
/// parses as absent.
const Wire = struct {
    inputs: ?std.json.Value = null,
    s3_url: ?std.json.Value = null,
    columns: ?std.json.Value = null,
    filter: ?std.json.Value = null,
    output_url: ?std.json.Value = null,
    output_codec: ?std.json.Value = null,
    select: ?std.json.Value = null,
    aggregate: ?std.json.Value = null,
    group_by: ?std.json.Value = null,
    column_order: ?std.json.Value = null,
    scan_all: ?std.json.Value = null,
    trust_stats: ?std.json.Value = null,
    max_memory: ?std.json.Value = null,
};

pub const Request = struct {
    parsed: std.json.Parsed(Wire),
    /// `inputs`, or the single `s3_url`.
    inputs: []const []const u8,
    /// `columns`, names trimmed and empty ones dropped; null when absent or empty.
    columns: ?[]const []const u8 = null,
    filter: ?[]const u8 = null,
    output_url: ?[]const u8 = null,
    output_codec: ?[]const u8 = null,
    select: ?[]const u8 = null,
    aggregate: ?[]const u8 = null,
    group_by: ?[]const u8 = null,
    column_order: ?[]const u8 = null,
    scan_all: bool = false,
    trust_stats: bool = false,
    /// `max_memory`: a byte count, or a size string the caller parses (`"512MB"`).
    max_memory: ?MaxMemory = null,

    pub const MaxMemory = union(enum) { bytes: usize, text: []const u8 };

    /// Decode `body`. Strings borrow the returned request; `deinit` frees them. On `BadFieldType`, `bad_field` is
    /// the field's name.
    pub fn parse(gpa: std.mem.Allocator, body: []const u8, bad_field: *[]const u8) Error!Request {
        const parsed = try std.json.parseFromSlice(Wire, gpa, body, .{ .ignore_unknown_fields = true });
        errdefer parsed.deinit();
        const arena = parsed.arena.allocator();
        const f: Fields = .{ .wire = &parsed.value, .bad_field = bad_field };

        var req: Request = .{ .parsed = parsed, .inputs = &.{} };
        if (try f.strings(arena, "inputs", false)) |inputs| {
            req.inputs = inputs;
        } else if (try f.string("s3_url")) |url| {
            const one = try arena.alloc([]const u8, 1);
            one[0] = url;
            req.inputs = one;
        } else return error.MissingField;
        if (try f.strings(arena, "columns", true)) |cols| {
            if (cols.len > 0) req.columns = cols;
        }
        req.filter = try f.string("filter");
        req.output_url = try f.string("output_url");
        req.output_codec = try f.string("output_codec");
        req.select = try f.string("select");
        req.aggregate = try f.string("aggregate");
        req.group_by = try f.string("group_by");
        req.column_order = try f.string("column_order");
        req.scan_all = try f.boolean("scan_all");
        req.trust_stats = try f.boolean("trust_stats");
        if (parsed.value.max_memory) |v| req.max_memory = switch (v) {
            .null => null,
            .integer => |n| if (n >= 0) .{ .bytes = @intCast(n) } else return f.bad("max_memory"),
            .float => |x| if (x >= 0 and x < 0x1p64) .{ .bytes = @intFromFloat(x) } else return f.bad("max_memory"),
            .string => |s| .{ .text = s },
            else => return f.bad("max_memory"),
        };
        return req;
    }

    pub fn deinit(self: *Request) void {
        self.parsed.deinit();
    }
};

const Fields = struct {
    wire: *const Wire,
    bad_field: *[]const u8,

    fn bad(self: Fields, name: []const u8) Error {
        self.bad_field.* = name;
        return error.BadFieldType;
    }

    fn string(self: Fields, comptime name: []const u8) Error!?[]const u8 {
        const v = @field(self.wire, name) orelse return null;
        return switch (v) {
            .string => |s| s,
            .null => null,
            else => self.bad(name),
        };
    }

    fn boolean(self: Fields, comptime name: []const u8) Error!bool {
        const v = @field(self.wire, name) orelse return false;
        return switch (v) {
            .bool => |b| b,
            .null => false,
            else => self.bad(name),
        };
    }

    /// An array of strings, each trimmed of spaces and tabs when `trim` (empty ones then dropped).
    fn strings(
        self: Fields,
        arena: std.mem.Allocator,
        comptime name: []const u8,
        trim: bool,
    ) Error!?[]const []const u8 {
        const v = @field(self.wire, name) orelse return null;
        const items = switch (v) {
            .array => |a| a.items,
            .null => return null,
            else => return self.bad(name),
        };
        var out: std.ArrayList([]const u8) = .empty;
        for (items) |item| {
            const s = switch (item) {
                .string => |s| s,
                else => return self.bad(name),
            };
            const t = if (trim) std.mem.trim(u8, s, " \t") else s;
            if (trim and t.len == 0) continue;
            try out.append(arena, t);
        }
        return out.items;
    }
};

const testing = std.testing;

test "request strings decode every JSON escape" {
    var bad: []const u8 = "";
    var req = try Request.parse(testing.allocator,
        \\{"inputs":["s3://b/k\"1"],"filter":"s = 'a \"q\" \\ \/ \b\f\n\r\t \u0041 \u00e9 \ud83d\ude00'",
        \\ "group_by":"\"a\".\"b\"","columns":[" \"a.b\" ", "", "x,y"],"scan_all":true,"mode":"ignored",
        \\ "max_memory":"512MB"}
    , &bad);
    defer req.deinit();
    try testing.expectEqual(@as(usize, 1), req.inputs.len);
    try testing.expectEqualStrings("s3://b/k\"1", req.inputs[0]);
    try testing.expectEqualStrings("s = 'a \"q\" \\ / \x08\x0c\n\r\t A \u{e9} \u{1F600}'", req.filter.?);
    try testing.expectEqualStrings("\"a\".\"b\"", req.group_by.?);
    try testing.expectEqual(@as(usize, 2), req.columns.?.len);
    try testing.expectEqualStrings("\"a.b\"", req.columns.?[0]);
    try testing.expectEqualStrings("x,y", req.columns.?[1]);
    try testing.expect(req.scan_all and !req.trust_stats);
    try testing.expectEqualStrings("512MB", req.max_memory.?.text);
    try testing.expectEqual(@as(?[]const u8, null), req.select);
}

test "request: s3_url shorthand, numeric max_memory, nulls as absent" {
    var bad: []const u8 = "";
    var req = try Request.parse(testing.allocator,
        \\{"s3_url":"s3://b/k","max_memory":1048576,"filter":null,"columns":[],"trust_stats":false}
    , &bad);
    defer req.deinit();
    try testing.expectEqualStrings("s3://b/k", req.inputs[0]);
    try testing.expectEqual(@as(usize, 1 << 20), req.max_memory.?.bytes);
    try testing.expectEqual(@as(?[]const u8, null), req.filter);
    try testing.expectEqual(@as(?[]const []const u8, null), req.columns);

    var nulls = try Request.parse(testing.allocator,
        \\{"inputs":["s3://b/k"],"max_memory":null,"scan_all":null,"columns":null,"select":null}
    , &bad);
    defer nulls.deinit();
    try testing.expect(nulls.max_memory == null and !nulls.scan_all and nulls.columns == null and nulls.select == null);
}

test "malformed requests are errors, wrong-typed fields named" {
    const Case = struct { body: []const u8, err: anyerror, field: []const u8 = "" };
    const cases = [_]Case{
        .{ .body = "{\"inputs\":[", .err = error.UnexpectedEndOfInput },
        .{ .body = "{\"inputs\":[\"a\"],\"filter\":\"\\q\"}", .err = error.SyntaxError },
        .{ .body = "{\"inputs\":[\"a\"],\"filter\":\"\\ud800\"}", .err = error.SyntaxError },
        .{ .body = "{\"inputs\":[\"a\"],\"filter\":\"\\ude00\\ud83d\"}", .err = error.SyntaxError },
        .{ .body = "{\"inputs\":[\"a\"],\"filter\":\"\xff\"}", .err = error.SyntaxError },
        .{ .body = "{\"inputs\":[\"a\"],\"filter\":\"a\nb\"}", .err = error.SyntaxError },
        .{ .body = "{\"inputs\":[\"a\"]} x", .err = error.SyntaxError },
        .{ .body = "{\"inputs\":[\"a\"],\"filter\":\"a\",\"filter\":\"b\"}", .err = error.DuplicateField },
        .{ .body = "[\"a\"]", .err = error.UnexpectedToken },
        .{ .body = "{\"filter\":\"a\"}", .err = error.MissingField },
        .{ .body = "{\"inputs\":\"a\"}", .err = error.BadFieldType, .field = "inputs" },
        .{ .body = "{\"inputs\":[\"a\",1]}", .err = error.BadFieldType, .field = "inputs" },
        .{ .body = "{\"inputs\":[\"a\"],\"filter\":5}", .err = error.BadFieldType, .field = "filter" },
        .{ .body = "{\"inputs\":[\"a\"],\"columns\":\"x\"}", .err = error.BadFieldType, .field = "columns" },
        .{ .body = "{\"inputs\":[\"a\"],\"scan_all\":1}", .err = error.BadFieldType, .field = "scan_all" },
        .{ .body = "{\"inputs\":[\"a\"],\"max_memory\":-1}", .err = error.BadFieldType, .field = "max_memory" },
        .{ .body = "{\"inputs\":[\"a\"],\"max_memory\":[]}", .err = error.BadFieldType, .field = "max_memory" },
    };
    for (cases) |c| {
        var bad: []const u8 = "";
        try testing.expectError(c.err, Request.parse(testing.allocator, c.body, &bad));
        try testing.expectEqualStrings(c.field, bad);
    }
}

/// Tracks the peak of live bytes allocated through it.
const PeakAllocator = struct {
    child: std.mem.Allocator,
    live: usize = 0,
    peak: usize = 0,

    fn allocator(self: *PeakAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn grew(self: *PeakAllocator, old: usize, new: usize) void {
        self.live = self.live - old + new;
        self.peak = @max(self.peak, self.live);
    }

    fn alloc(ctx: *anyopaque, len: usize, a: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *PeakAllocator = @ptrCast(@alignCast(ctx));
        const p = self.child.rawAlloc(len, a, ra) orelse return null;
        self.grew(0, len);
        return p;
    }

    fn resize(ctx: *anyopaque, memory: []u8, a: std.mem.Alignment, new_len: usize, ra: usize) bool {
        const self: *PeakAllocator = @ptrCast(@alignCast(ctx));
        if (!self.child.rawResize(memory, a, new_len, ra)) return false;
        self.grew(memory.len, new_len);
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, a: std.mem.Alignment, new_len: usize, ra: usize) ?[*]u8 {
        const self: *PeakAllocator = @ptrCast(@alignCast(ctx));
        const p = self.child.rawRemap(memory, a, new_len, ra) orelse return null;
        self.grew(memory.len, new_len);
        return p;
    }

    fn free(ctx: *anyopaque, memory: []u8, a: std.mem.Alignment, ra: usize) void {
        const self: *PeakAllocator = @ptrCast(@alignCast(ctx));
        self.child.rawFree(memory, a, ra);
        self.live -= memory.len;
    }
};

test "an unknown field is skipped, not built: a large one costs no allocation" {
    // About 2 MB of nested objects, arrays and strings under a field the handler does not read.
    const note: [40]u8 = @splat('x');
    var body: std.ArrayList(u8) = .empty;
    defer body.deinit(testing.allocator);
    try body.appendSlice(testing.allocator, "{\"inputs\":[\"s3://b/k\"],\"trace\":{\"spans\":[");
    for (0..20_000) |i| {
        if (i > 0) try body.append(testing.allocator, ',');
        const span = "{{\"id\":{d},\"tags\":[\"a\\nb\",\"c\",{{\"k\":[1,2.5,true,null]}}],\"note\":\"{s}\"}}";
        try body.print(testing.allocator, span, .{ i, &note });
    }
    try body.appendSlice(testing.allocator, "]},\"filter\":\"x = 1\"}");
    try testing.expect(body.items.len > 1_500_000);

    var peak: PeakAllocator = .{ .child = testing.allocator };
    var bad: []const u8 = "";
    var req = try Request.parse(peak.allocator(), body.items, &bad);
    defer req.deinit();
    try testing.expectEqualStrings("x = 1", req.filter.?);
    try testing.expectEqualStrings("s3://b/k", req.inputs[0]);
    testing.expect(peak.peak < 64 * 1024) catch |err| {
        std.debug.print("peak {d} bytes for a {d}-byte body\n", .{ peak.peak, body.items.len });
        return err;
    };
}
