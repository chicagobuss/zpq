//! Oracle verdicts: each compare mode checks one zpq output against DuckDB, or against PyArrow's strict read of a
//! file (tools/check/readback.py). Every mode returns null on a match and a one-line reason otherwise.

const std = @import("std");
const compare = @import("compare.zig");
const oracle_mod = @import("oracle.zig");

const Val = compare.Val;
const Lane = compare.Lane;
const Allocator = std.mem.Allocator;

pub const Spec = struct {
    compare: []const u8,
    sql: ?[]const u8 = null,
    where: ?[]const u8 = null,
    mean: ?Mean = null,
    epoch: bool = false,
    pred: ?Pred = null,
    py_agg: ?[]const u8 = null,
    names: ?[]const []const u8 = null,
    types: ?[]const []const u8 = null,
    orders: ?[]const u8 = null,
    row_count: ?usize = null,
    schema: ?[]const []const u8 = null,
    duck_select: ?[]const u8 = null,
    /// `pinned`: the expected rows, and why no rival reader is the oracle for them.
    rows: ?[]const std.json.Value = null,
    why: ?[]const u8 = null,
};

pub const Mean = struct { scale: u16, sum: ?[]const u8 = null, count: ?[]const u8 = null, alias: ?[]const u8 = null };
pub const Pred = struct { col: []const u8, op: []const u8, value: []const u8 };

pub const Inputs = struct {
    arena: Allocator,
    io: std.Io,
    gpa: Allocator,
    oracle: *oracle_mod.Oracle,
    python: []const u8,
    fixture: []const u8,
    /// zpq's output: JSON or JSONL text, or a written Parquet file.
    text: []const u8 = "",
    path: []const u8 = "",
    /// Columns the case projects (copy cases).
    columns: ?[]const []const u8 = null,
    select: bool = false,
};

const Reason = ?[]const u8;

fn fail(in: Inputs, comptime fmt: []const u8, args: anytype) Reason {
    const text = std.fmt.allocPrint(in.arena, fmt, args) catch "out of memory";
    return text[0..@min(text.len, 400)];
}

pub fn run(in: Inputs, spec: Spec) !Reason {
    const m = spec.compare;
    const modes = .{
        .{ "agg", modeAgg },             .{ "group", modeGroup },         .{ "ordered", modeOrdered },
        .{ "usame", modeUsame },         .{ "dense", modeDense },         .{ "values", modeValues },
        .{ "except", modeExcept },       .{ "parquet", modeParquet },     .{ "product", modeProduct },
        .{ "stats_rows", modeStatsRows }, .{ "stats_agg", modeStatsAgg }, .{ "stats_select", modeStatsSelect },
        .{ "orders", modeOrders },       .{ "wire", modeWire },           .{ "names", modeNames },
        .{ "order", modeOrder },         .{ "select_order", modeSelectOrder }, .{ "avg_exact", modeAvgExact },
        .{ "levels", modeLevels },       .{ "typed", modeTyped },         .{ "decimal", modeTyped },         .{ "sliced_write", modeSlicedWrite },
        .{ "sliced_print", modeSlicedPrint }, .{ "row_count", modeRowCount }, .{ "corpus", modeCorpus },
        .{ "corpus_arrow", modeCorpusArrow },
        .{ "pinned", modePinned },
    };
    inline for (modes) |entry| {
        if (std.mem.eql(u8, m, entry[0])) return entry[1](in, spec);
    }
    if (std.mem.startsWith(u8, m, "wire:")) return modeWire(in, spec);
    return error.BadCompareMode;
}

// ---- inputs

pub const Table = struct {
    names: []const []const u8,
    types: []const []const u8,
    lanes: []Lane,
    rows: [][]Val,
};

fn fill(in: Inputs, sql: []const u8) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    var rest = sql;
    while (std.mem.indexOf(u8, rest, "{path}")) |at| {
        try out.appendSlice(in.arena, rest[0..at]);
        try out.appendSlice(in.arena, in.fixture);
        rest = rest[at + "{path}".len ..];
    }
    try out.appendSlice(in.arena, rest);
    return out.items;
}

/// The oracle's rows for `sql`, coerced by the declared types. A refusal is an error the caller reports.
fn duck(in: Inputs, sql: []const u8) !Table {
    const ans = try in.oracle.ask(in.arena, sql);
    if (ans.refused) |why| {
        last_refusal = why;
        return error.OracleRefused;
    }
    const lanes = try in.arena.alloc(Lane, ans.types.len);
    for (ans.types, 0..) |t, i| lanes[i] = compare.laneOf(t);
    const rows = try in.arena.alloc([]Val, ans.rows.len);
    for (ans.rows, 0..) |row, r| {
        rows[r] = try in.arena.alloc(Val, ans.names.len);
        for (ans.names, ans.types, 0..) |name, t, c| rows[r][c] = try compare.fromOracle(row.get(name) orelse .null, t);
    }
    return .{ .names = ans.names, .types = ans.types, .lanes = lanes, .rows = rows };
}

pub threadlocal var last_refusal: []const u8 = "";

fn oracleSql(in: Inputs, spec: Spec) ![]const u8 {
    return fill(in, spec.sql orelse return error.MissingOracleSql);
}

/// zpq's JSON as a list of objects (a lone object is one row), or JSONL lines.
fn zpqRows(in: Inputs) ![]std.json.ObjectMap {
    const text = std.mem.trim(u8, in.text, " \t\r\n");
    var out: std.ArrayList(std.json.ObjectMap) = .empty;
    if (text.len == 0) return out.items;
    if (text[0] == '[' or (text[0] == '{' and std.mem.indexOfScalar(u8, text, '\n') == null)) {
        const v = try compare.parse(in.arena, text);
        switch (v) {
            .array => |xs| for (xs.items) |x| try out.append(in.arena, x.object),
            .object => |o| try out.append(in.arena, o),
            else => return error.BadZpqJson,
        }
        return out.items;
    }
    var it = std.mem.splitScalar(u8, text, '\n');
    while (it.next()) |line| {
        if (std.mem.trim(u8, line, " \t\r").len == 0) continue;
        try out.append(in.arena, (try compare.parse(in.arena, line)).object);
    }
    return out.items;
}

/// A row with each avg_state {"sum","count"} pair split into name__sum and name__count, in order.
fn splitState(arena: Allocator, row: std.json.ObjectMap) !std.json.ObjectMap {
    var out: std.json.ObjectMap = .empty;
    var it = row.iterator();
    while (it.next()) |e| {
        const v = e.value_ptr.*;
        if (v == .object and v.object.count() == 2 and v.object.contains("sum") and v.object.contains("count")) {
            try out.put(arena, try std.fmt.allocPrint(arena, "{s}__sum", .{e.key_ptr.*}), v.object.get("sum").?);
            try out.put(arena, try std.fmt.allocPrint(arena, "{s}__count", .{e.key_ptr.*}), v.object.get("count").?);
        } else try out.put(arena, e.key_ptr.*, v);
    }
    return out;
}

fn laneAt(lanes: []const Lane, i: usize) Lane {
    return if (i < lanes.len) lanes[i] else .plain;
}

fn zpqValues(in: Inputs, row: std.json.ObjectMap, lanes: []const Lane) ![]Val {
    const out = try in.arena.alloc(Val, row.count());
    for (row.values(), 0..) |v, i| out[i] = try compare.fromZpq(v, laneAt(lanes, i));
    return out;
}

fn zpqByName(in: Inputs, row: std.json.ObjectMap, names: []const []const u8, lanes: []const Lane) !?[]Val {
    const out = try in.arena.alloc(Val, names.len);
    for (names, 0..) |n, i| out[i] = try compare.fromZpq(row.get(n) orelse return null, laneAt(lanes, i));
    return out;
}

/// PyArrow's strict read of a file, with `filters` pushed down for counts.
pub const Readback = struct {
    names: []const []const u8,
    types: []const []const u8,
    rows: []const std.json.Value,
    row_groups: []const std.json.Value,
    column_orders: std.json.Value,
    pushed: []const std.json.Value,
};

pub fn readback(in: Inputs, path: []const u8, filters: []const []const u8) !Readback {
    var argv: std.ArrayList([]const u8) = .empty;
    try argv.appendSlice(in.arena, &.{ in.python, "tools/check/readback.py", path });
    for (filters) |f| try argv.append(in.arena, f);
    const out = try oracle_mod.capture(in.gpa, in.io, argv.items);
    defer in.gpa.free(out.stderr);
    if (out.code != 0) {
        in.gpa.free(out.stdout);
        last_refusal = try in.arena.dupe(u8, std.mem.trim(u8, out.stderr, " \t\r\n"));
        return error.PyarrowRejected;
    }
    const text = try in.arena.dupe(u8, out.stdout);
    in.gpa.free(out.stdout);
    const v = (try compare.parse(in.arena, text)).object;
    return .{
        .names = try strings(in.arena, v.get("names").?),
        .types = try strings(in.arena, v.get("types").?),
        .rows = v.get("rows").?.array.items,
        .row_groups = v.get("row_groups").?.array.items,
        .column_orders = v.get("column_orders").?,
        .pushed = v.get("pushed").?.array.items,
    };
}

fn strings(arena: Allocator, v: std.json.Value) ![]const []const u8 {
    const out = try arena.alloc([]const u8, v.array.items.len);
    for (v.array.items, 0..) |x, i| out[i] = x.string;
    return out;
}

/// A PyArrow cell as the policy sees it. Tagged types stay distinct from the oracle's text unless `plain` says how
/// a mode prints them (hex bytes, ISO temporals).
fn fromArrow(arena: Allocator, v: std.json.Value, plain: bool) !Val {
    switch (v) {
        .object => |o| if (o.count() == 1) {
            var it = o.iterator();
            const e = it.next().?;
            const k = e.key_ptr.*;
            if (k.len > 0 and k[0] == '$') {
                const s = e.value_ptr.string;
                if (std.mem.eql(u8, k, "$dec")) return compare.exactNumber(s);
                if (plain) return .{ .text = s };
                return .{ .text = try std.fmt.allocPrint(arena, "\x00{s}:{s}", .{ k, s }) };
            }
        },
        else => {},
    }
    return switch (v) {
        .null => .null,
        .bool => |b| .{ .boolean = b },
        .number_string => |s| if (std.mem.indexOfAny(u8, s, ".eE") != null)
            compare.fromFloat(try std.fmt.parseFloat(f64, s))
        else
            .{ .int = try std.fmt.parseInt(compare.Big, s, 10) },
        .string => |s| if (std.mem.eql(u8, s, "NaN") or std.mem.eql(u8, s, "Infinity") or std.mem.eql(u8, s, "-Infinity"))
            .{ .text = s }
        else
            .{ .text = s },
        else => .{ .nested = v },
    };
}

/// Binary cells as the text their bytes spell, other cells as `fromArrow` reads them.
fn utf8Arrow(arena: Allocator, v: std.json.Value) !Val {
    if (v == .object and v.object.count() == 1) if (v.object.get("$bytes")) |hex| {
        const bytes = try arena.alloc(u8, hex.string.len / 2);
        _ = try std.fmt.hexToBytes(bytes, hex.string);
        return .{ .text = bytes };
    };
    return fromArrow(arena, v, false);
}

fn arrowRows(in: Inputs, rb: Readback, names: []const []const u8, plain: bool) !?[][]Val {
    var index = try in.arena.alloc(usize, names.len);
    for (names, 0..) |n, i| index[i] = indexOf(rb.names, n) orelse return null;
    const rows = try in.arena.alloc([]Val, rb.rows.len);
    for (rb.rows, 0..) |row, r| {
        rows[r] = try in.arena.alloc(Val, names.len);
        for (index, 0..) |c, i| rows[r][i] = try fromArrow(in.arena, row.array.items[c], plain);
    }
    return rows;
}

fn indexOf(names: []const []const u8, name: []const u8) ?usize {
    for (names, 0..) |n, i| if (std.mem.eql(u8, n, name)) return i;
    return null;
}

// ---- answer modes

fn applyMean(in: Inputs, t: *Table, mean: ?Mean) !void {
    const m = mean orelse return;
    if (m.alias) |alias| {
        const si = indexOf(t.names, m.sum.?) orelse return error.MissingMeanColumn;
        const ci = indexOf(t.names, m.count.?) orelse return error.MissingMeanColumn;
        const mi = indexOf(t.names, alias) orelse return error.MissingMeanColumn;
        t.lanes[mi] = .exact;
        for (t.rows) |row| row[mi] = try compare.decimalMean(row[si], row[ci], m.scale);
        return;
    }
    // Scalar form: the select starts with the sum and the count the mean is taken from.
    for (t.rows, 0..) |row, r| {
        const out = try in.arena.alloc(Val, row.len - 1);
        out[0] = try compare.decimalMean(row[0], row[1], m.scale);
        @memcpy(out[1..], row[2..]);
        t.rows[r] = out;
    }
    t.names = t.names[1..];
    const lanes = try in.arena.alloc(Lane, t.lanes.len - 1);
    lanes[0] = .exact;
    @memcpy(lanes[1..], t.lanes[2..]);
    t.lanes = lanes;
}

fn oracleTable(in: Inputs, spec: Spec) !Table {
    var t = try duck(in, try oracleSql(in, spec));
    try applyMean(in, &t, spec.mean);
    return t;
}

fn modeAgg(in: Inputs, spec: Spec) !Reason {
    const z = try zpqRows(in);
    if (z.len != 1) return fail(in, "zpq rows {d}", .{z.len});
    const t = try oracleTable(in, spec);
    if (t.rows.len != 1) return fail(in, "duck rows {d}", .{t.rows.len});
    const got = try zpqValues(in, try splitState(in.arena, z[0]), t.lanes);
    const want = t.rows[0];
    if (got.len != want.len) return fail(in, "zpq {d} values, duck {d}", .{ got.len, want.len });
    for (got, want) |g, w| if (!compare.same(g, w)) return fail(in, "zpq={s} duck={s}", .{ compare.show(in.arena, g), compare.show(in.arena, w) });
    return null;
}

fn modeGroup(in: Inputs, spec: Spec) !Reason {
    const z = try zpqRows(in);
    const t = try oracleTable(in, spec);
    const got = try in.arena.alloc([]const Val, z.len);
    for (z, 0..) |row, i| got[i] = (try zpqByName(in, try splitState(in.arena, row), t.names, t.lanes)) orelse
        return fail(in, "zpq row lacks a column of {s}", .{t.names[0]});
    return compare.sameMultiset(in.arena, got, t.rows);
}

fn modeOrdered(in: Inputs, spec: Spec) !Reason {
    const t = try oracleTable(in, spec);
    var got: []const []const Val = undefined;
    if (in.path.len != 0) {
        var written = try duck(in, try std.fmt.allocPrint(in.arena, "SELECT * FROM '{s}'", .{in.path}));
        got = written.rows;
        _ = &written;
    } else {
        const z = try zpqRows(in);
        const rows = try in.arena.alloc([]const Val, z.len);
        for (z, 0..) |row, i| rows[i] = try zpqValues(in, row, t.lanes);
        got = rows;
    }
    if (spec.epoch) for (t.rows) |row| for (row, 0..) |*v, i| {
        v.* = try toEpoch(v.*, t.types[i]);
    };
    return compare.sameOrdered(in.arena, got, t.rows);
}

/// JSONL prints a DATE as days and a TIMESTAMP as milliseconds, both since 1970-01-01.
fn toEpoch(v: Val, type_name: []const u8) !Val {
    if (v != .text) return v;
    const s = v.text;
    if (std.ascii.startsWithIgnoreCase(type_name, "TIMESTAMP")) {
        const days = try epochDays(s[0..10]);
        const h = try std.fmt.parseInt(i64, s[11..13], 10);
        const mi = try std.fmt.parseInt(i64, s[14..16], 10);
        const sec = try std.fmt.parseInt(i64, s[17..19], 10);
        var micros: i64 = 0;
        if (s.len > 20 and s[19] == '.') {
            var frac: [6]u8 = @splat('0');
            const digits = s[20..@min(s.len, 26)];
            @memcpy(frac[0..digits.len], digits);
            micros = try std.fmt.parseInt(i64, &frac, 10);
        }
        const total_us = ((days * 86400 + h * 3600 + mi * 60 + sec) * 1_000_000) + micros;
        return .{ .int = @divFloor(total_us, 1000) };
    }
    if (std.ascii.startsWithIgnoreCase(type_name, "DATE")) return .{ .int = try epochDays(s[0..10]) };
    return v;
}

fn epochDays(s: []const u8) !i64 {
    const y = try std.fmt.parseInt(i64, s[0..4], 10);
    const m = try std.fmt.parseInt(i64, s[5..7], 10);
    const d = try std.fmt.parseInt(i64, s[8..10], 10);
    // days_from_civil (Howard Hinnant)
    const yy = if (m <= 2) y - 1 else y;
    const era = @divFloor(yy, 400);
    const yoe = yy - era * 400;
    const mp = @mod(m + 9, 12);
    const doy = @divFloor(153 * mp + 2, 5) + d - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}

fn canonVals(in: Inputs, row: []const Val) ![]Val {
    const out = try in.arena.alloc(Val, row.len);
    for (row, 0..) |v, i| out[i] = if (v == .float) compare.roundFloat(v.float) else v;
    return out;
}

fn sortRows(in: Inputs, rows: []const []const Val) ![][]const Val {
    const Keyed = struct { key: []const u8, row: []const Val };
    const keyed = try in.arena.alloc(Keyed, rows.len);
    for (rows, 0..) |r, i| keyed[i] = .{ .key = try compare.rowKey(in.arena, r), .row = r };
    std.mem.sort(Keyed, keyed, {}, struct {
        fn lt(_: void, a: Keyed, b: Keyed) bool {
            return std.mem.order(u8, a.key, b.key) == .lt;
        }
    }.lt);
    const out = try in.arena.alloc([]const Val, rows.len);
    for (keyed, 0..) |k, i| out[i] = k.row;
    return out;
}

fn modeUsame(in: Inputs, spec: Spec) !Reason {
    const z = try zpqRows(in);
    const t = try oracleTable(in, spec);
    const got = try in.arena.alloc([]const Val, z.len);
    for (z, 0..) |row, i| got[i] = try canonVals(in, (try zpqByName(in, row, t.names, t.lanes)) orelse
        return fail(in, "zpq row lacks an oracle column", .{}));
    const want = try in.arena.alloc([]const Val, t.rows.len);
    for (t.rows, 0..) |row, i| want[i] = try canonVals(in, row);
    if (got.len != want.len) return fail(in, "zpq={d} rows duck={d}", .{ got.len, want.len });
    const a = try sortRows(in, got);
    const b = try sortRows(in, want);
    for (a, b) |gr, wr| for (gr, wr) |g, w| if (!compare.same(g, w))
        return fail(in, "zpq={s} duck={s}", .{ compare.show(in.arena, g), compare.show(in.arena, w) });
    return null;
}

/// Python-style value equality for modes that compare raw values: numbers by exact value across int, decimal and
/// float (a float by its shortest decimal), everything else by type and value.
fn exactKey(arena: Allocator, v: Val) ![]const u8 {
    return switch (v) {
        .float => |f| blk: {
            var buf: [64]u8 = undefined;
            const text = try std.fmt.bufPrint(&buf, "{d}", .{f});
            break :blk compare.canon(arena, try compare.exactNumber(text));
        },
        else => compare.canon(arena, v),
    };
}

fn exactRowKey(arena: Allocator, row: []const Val) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    for (row) |v| {
        const k = try exactKey(arena, v);
        try out.print(arena, "{d}:{s}|", .{ k.len, k });
    }
    return out.items;
}

fn exactMultiset(in: Inputs, got: []const []const Val, want: []const []const Val, ordered: bool) !Reason {
    if (got.len != want.len) return fail(in, "zpq={d} rows truth={d}", .{ got.len, want.len });
    const a = try in.arena.alloc([]const u8, got.len);
    const b = try in.arena.alloc([]const u8, want.len);
    for (got, 0..) |r, i| a[i] = try exactRowKey(in.arena, r);
    for (want, 0..) |r, i| b[i] = try exactRowKey(in.arena, r);
    if (!ordered) {
        std.mem.sort([]const u8, a, {}, compare.lessText);
        std.mem.sort([]const u8, b, {}, compare.lessText);
    }
    for (a, b) |x, y| if (!std.mem.eql(u8, x, y)) return fail(in, "row zpq={s} truth={s}", .{ x, y });
    return null;
}

fn rawZpq(_: Inputs, v: std.json.Value) !Val {
    return compare.fromZpq(v, .exact);
}

fn modeDense(in: Inputs, spec: Spec) !Reason {
    const z = try zpqRows(in);
    const t = try duck(in, try oracleSql(in, spec));
    const got = try in.arena.alloc([]const Val, z.len);
    for (z, 0..) |row, i| {
        const vals = try in.arena.alloc(Val, t.names.len);
        for (t.names, 0..) |n, c| vals[c] = try rawZpq(in, row.get(n) orelse return fail(in, "zpq lacks {s}", .{n}));
        got[i] = vals;
    }
    return exactMultiset(in, got, t.rows, false);
}

fn modeValues(in: Inputs, spec: Spec) !Reason {
    const sql = try oracleSql(in, spec);
    const wrapped = try std.fmt.allocPrint(in.arena,
        \\SELECT c::VARCHAR AS c, CASE WHEN s IS NULL THEN 'NULL' ELSE s::VARCHAR END AS s FROM ({s}) q(c, s)
    , .{sql});
    const ans = try in.oracle.ask(in.arena, wrapped);
    if (ans.refused != null) return fail(in, "zpq answered {s}; duckdb refuses", .{in.text});
    if (ans.rows.len != 1) return fail(in, "duck rows {d}", .{ans.rows.len});
    const want = try std.fmt.allocPrint(in.arena, "{s},{s}", .{ ans.rows[0].get("c").?.string, ans.rows[0].get("s").?.string });
    if (!std.mem.eql(u8, in.text, want)) return fail(in, "zpq {s} duck {s}", .{ in.text, want });
    return null;
}

fn modeExcept(in: Inputs, spec: Spec) !Reason {
    const rb = readback(in, in.path, &.{}) catch |err| return refusedReason(in, err);
    var want_names: std.ArrayList(u8) = .empty;
    if (in.select) try want_names.appendSlice(in.arena, "x") else for (spec.schema orelse &.{}) |name| {
        if (in.columns) |cols| if (indexOf(cols, name) == null) continue;
        if (want_names.items.len != 0) try want_names.append(in.arena, ',');
        try want_names.appendSlice(in.arena, name);
    }
    const got_names = try std.mem.join(in.arena, ",", rb.names);
    if (!std.mem.eql(u8, got_names, want_names.items)) return fail(in, "column order got {s} want {s}", .{ got_names, want_names.items });
    var cols: std.ArrayList(u8) = .empty;
    if (in.select) try cols.appendSlice(in.arena, "x") else if (in.columns) |wanted| {
        for (wanted, 0..) |name, i| {
            if (i != 0) try cols.appendSlice(in.arena, ", ");
            try cols.print(in.arena, "\"{s}\"", .{name});
        }
    } else try cols.appendSlice(in.arena, "*");
    const where = if (spec.where) |w| try std.fmt.allocPrint(in.arena, " WHERE {s}", .{w}) else "";
    const oracle = try std.fmt.allocPrint(in.arena, "SELECT {s} FROM '{s}'{s}", .{ spec.duck_select orelse cols.items, in.fixture, where });
    const written = try std.fmt.allocPrint(in.arena, "SELECT {s} FROM '{s}'", .{ cols.items, in.path });
    const diff = try std.fmt.allocPrint(in.arena, "SELECT count(*) AS n FROM (({s} EXCEPT ALL {s}) UNION ALL ({s} EXCEPT ALL {s}))", .{ written, oracle, oracle, written });
    const t = duck(in, diff) catch |err| return refusedReason(in, err);
    if (t.rows.len != 1 or t.rows[0][0] != .int or t.rows[0][0].int != 0) return fail(in, "row mismatch count {s}", .{compare.show(in.arena, t.rows[0][0])});
    return null;
}

fn refusedReason(in: Inputs, err: anyerror) !Reason {
    if (err == error.OracleRefused or err == error.PyarrowRejected) return fail(in, "{s}", .{last_refusal});
    return err;
}

fn modeParquet(in: Inputs, _: Spec) !Reason {
    const z = try zpqRows(in);
    const rb = readback(in, in.path, &.{}) catch |err| return refusedReason(in, err);
    var exact = try in.arena.alloc(Lane, rb.types.len);
    for (rb.types, 0..) |t, i| exact[i] = if (std.mem.startsWith(u8, t, "decimal")) .exact else .plain;
    if (z.len != 0) {
        const want_cols = (try splitState(in.arena, z[0])).keys();
        if (!std.mem.eql(u8, try std.mem.join(in.arena, ",", rb.names), try std.mem.join(in.arena, ",", want_cols)))
            return fail(in, "columns {s} want {s}", .{ try std.mem.join(in.arena, ",", rb.names), try std.mem.join(in.arena, ",", want_cols) });
    }
    const want = try in.arena.alloc([]const Val, z.len);
    for (z, 0..) |row, i| want[i] = try canonVals(in, try zpqValues(in, try splitState(in.arena, row), exact));
    const got = try in.arena.alloc([]const Val, rb.rows.len);
    for (rb.rows, 0..) |row, r| {
        const vals = try in.arena.alloc(Val, row.array.items.len);
        for (row.array.items, 0..) |v, c| vals[c] = try utf8Arrow(in.arena, v);
        got[r] = try canonVals(in, vals);
    }
    return compare.sameOrdered(in.arena, got, want);
}

fn modeProduct(in: Inputs, _: Spec) !Reason {
    const truth = try zpqRows(in);
    const rb = readback(in, in.path, &.{}) catch |err| return refusedReason(in, err);
    const names: []const []const u8 = if (truth.len != 0) truth[0].keys() else rb.names;
    const got_names = try std.mem.join(in.arena, ",", rb.names);
    if (!std.mem.eql(u8, got_names, try std.mem.join(in.arena, ",", names))) return fail(in, "product parquet columns={s}", .{got_names});
    const want = try in.arena.alloc([]const Val, truth.len);
    for (truth, 0..) |row, i| {
        const vals = try in.arena.alloc(Val, names.len);
        for (names, 0..) |n, c| vals[c] = try rawZpq(in, row.get(n).?);
        want[i] = vals;
    }
    const got = (try arrowRows(in, rb, names, false)).?;
    if (try exactMultiset(in, got, want, true)) |why| return fail(in, "product parquet: {s}", .{why});
    const back = duck(in, try std.fmt.allocPrint(in.arena, "SELECT * FROM '{s}'", .{in.path})) catch |err| return refusedReason(in, err);
    if (try exactMultiset(in, back.rows, want, true)) |why| return fail(in, "product readback: {s}", .{why});
    return null;
}

// ---- statistics modes (synthetic files whose statistics lie; decoded rows are the truth there)

fn predHolds(in: Inputs, v: Val, p: Pred) !bool {
    if (std.mem.eql(u8, p.op, "lt_bytes")) {
        if (v != .text) return false;
        return std.mem.order(u8, v.text, p.value) == .lt;
    }
    const eq = switch (v) {
        .null => false,
        .dec => |d| blk: {
            const want = try compare.exactNumber(p.value);
            break :blk want == .dec and want.dec.unscaled == d.unscaled and want.dec.scale == d.scale;
        },
        .boolean => |b| b == std.mem.eql(u8, p.value, "true"),
        .int => |n| blk: {
            const want = compare.exactNumber(p.value) catch break :blk false;
            break :blk want == .int and want.int == n;
        },
        .float => |f| f == try std.fmt.parseFloat(f64, p.value),
        .text => |s| std.mem.eql(u8, s, p.value),
        .nested => false,
    };
    _ = in;
    if (std.mem.eql(u8, p.op, "eq")) return eq;
    if (std.mem.eql(u8, p.op, "ne")) return !eq;
    return error.BadPredicate;
}

/// The fixture's rows PyArrow decodes, without statistics, kept by the case's predicate.
fn truthRows(in: Inputs, spec: Spec) !struct { names: []const []const u8, rows: []const []const Val, lanes: []const Lane } {
    if (spec.pred) |p| {
        const rb = try readback(in, in.fixture, &.{});
        const all = (try arrowRows(in, rb, rb.names, false)).?;
        const col = indexOf(rb.names, p.col) orelse return error.MissingPredColumn;
        var kept: std.ArrayList([]const Val) = .empty;
        for (all) |r| if (try predHolds(in, r[col], p)) try kept.append(in.arena, r);
        const t = try duck(in, try std.fmt.allocPrint(in.arena, "SELECT * FROM '{s}'", .{in.fixture}));
        return .{ .names = rb.names, .rows = kept.items, .lanes = t.lanes };
    }
    const where = if (spec.where) |w| try std.fmt.allocPrint(in.arena, " WHERE {s}", .{w}) else "";
    const t = try duck(in, try std.fmt.allocPrint(in.arena, "SELECT * FROM '{s}'{s}", .{ in.fixture, where }));
    return .{ .names = t.names, .rows = t.rows, .lanes = t.lanes };
}

fn modeStatsRows(in: Inputs, spec: Spec) !Reason {
    const truth = truthRows(in, spec) catch |err| return refusedReason(in, err);
    var got: []const []const Val = undefined;
    if (in.path.len != 0) {
        const rb = readback(in, in.path, &.{}) catch |err| return refusedReason(in, err);
        got = (try arrowRows(in, rb, truth.names, false)) orelse return fail(in, "written file lacks a column", .{});
    } else {
        const z = try zpqRows(in);
        const rows = try in.arena.alloc([]const Val, z.len);
        for (z, 0..) |row, i| rows[i] = (try zpqByName(in, row, truth.names, truth.lanes)) orelse return fail(in, "zpq row lacks a column", .{});
        got = rows;
    }
    return compare.sameMultiset(in.arena, got, truth.rows);
}

fn modeStatsAgg(in: Inputs, spec: Spec) !Reason {
    const select = spec.sql orelse return error.MissingOracleSql;
    const described = duck(in, try std.fmt.allocPrint(in.arena, "SELECT {s} FROM '{s}' LIMIT 0", .{ select, in.fixture })) catch |err| return refusedReason(in, err);
    var lanes = described.lanes;
    var want: []const Val = undefined;
    if (spec.py_agg) |name| {
        const truth = try truthRows(in, spec);
        want = try pyAggregate(in, name, truth.names, truth.rows);
    } else {
        const where = if (spec.where) |w| try std.fmt.allocPrint(in.arena, " WHERE {s}", .{w}) else "";
        const t = duck(in, try std.fmt.allocPrint(in.arena, "SELECT {s} FROM '{s}'{s}", .{ select, in.fixture, where })) catch |err| return refusedReason(in, err);
        if (t.rows.len != 1) return fail(in, "duck rows {d}", .{t.rows.len});
        want = t.rows[0];
        lanes = t.lanes;
    }
    const z = try zpqRows(in);
    if (z.len != 1) return fail(in, "zpq rows {d}", .{z.len});
    const got = try zpqValues(in, try splitState(in.arena, z[0]), lanes);
    if (got.len != want.len) return fail(in, "zpq {d} values, truth {d}", .{ got.len, want.len });
    for (got, want) |g, w| if (!compare.same(g, w)) return fail(in, "zpq={s} truth={s}", .{ compare.show(in.arena, g), compare.show(in.arena, w) });
    return null;
}

fn pyAggregate(in: Inputs, name: []const u8, names: []const []const u8, rows: []const []const Val) ![]const Val {
    const col = struct {
        fn of(ns: []const []const u8, n: []const u8) !usize {
            return indexOf(ns, n) orelse error.MissingColumn;
        }
    }.of;
    const count: Val = .{ .int = @intCast(rows.len) };
    var sum_i: Val = .null;
    if (rows.len != 0) {
        const i = try col(names, "i");
        var s: compare.Big = 0;
        for (rows) |r| s += r[i].int;
        sum_i = .{ .int = s };
    }
    if (std.mem.eql(u8, name, "nan")) return try in.arena.dupe(Val, &.{ count, sum_i });
    if (std.mem.eql(u8, name, "deprecated")) {
        const dec = try col(names, "dec");
        const u = try col(names, "u");
        return try in.arena.dupe(Val, &.{ count, sum_i, try extreme(rows, dec, .lt), try extreme(rows, dec, .gt), try extreme(rows, u, .gt) });
    }
    if (std.mem.eql(u8, name, "column_order")) {
        return try in.arena.dupe(Val, &.{ count, sum_i, try extreme(rows, try col(names, "s"), .lt) });
    }
    return error.BadPyAgg;
}

fn extreme(rows: []const []const Val, c: usize, want: std.math.Order) !Val {
    var best: Val = .null;
    for (rows) |r| {
        const v = r[c];
        if (v == .null) return error.NullInExtreme;
        if (best == .null or order(v, best) == want) best = v;
    }
    return best;
}

fn order(a: Val, b: Val) std.math.Order {
    if (a == .text and b == .text) return std.mem.order(u8, a.text, b.text);
    const x = toExact(a);
    const y = toExact(b);
    const s = @max(x.scale, y.scale);
    return std.math.order(x.unscaled * pow10(s - x.scale), y.unscaled * pow10(s - y.scale));
}

fn toExact(v: Val) compare.Dec {
    return switch (v) {
        .int => |n| .{ .unscaled = n, .scale = 0 },
        .dec => |d| d,
        else => .{ .unscaled = 0, .scale = 0 },
    };
}

fn pow10(n: u16) compare.Big {
    var x: compare.Big = 1;
    for (0..n) |_| x *= 10;
    return x;
}

fn modeStatsSelect(in: Inputs, spec: Spec) !Reason {
    const where = if (spec.where) |w| try std.fmt.allocPrint(in.arena, " WHERE {s}", .{w}) else "";
    const t = duck(in, try std.fmt.allocPrint(in.arena, "SELECT {s} FROM '{s}'{s}", .{ spec.sql.?, in.fixture, where })) catch |err| return refusedReason(in, err);
    var got: []const []const Val = undefined;
    if (in.path.len != 0) {
        const rb = readback(in, in.path, &.{}) catch |err| return refusedReason(in, err);
        got = (try arrowRows(in, rb, t.names, false)) orelse return fail(in, "written file lacks a column", .{});
    } else {
        const z = try zpqRows(in);
        const rows = try in.arena.alloc([]const Val, z.len);
        for (z, 0..) |row, i| {
            const vals = try in.arena.alloc(Val, t.names.len);
            for (t.names, 0..) |n, c| vals[c] = try rawZpq(in, row.get(n) orelse return fail(in, "zpq lacks {s}", .{n}));
            rows[i] = vals;
        }
        got = rows;
    }
    return exactMultiset(in, got, t.rows, false);
}

fn modeOrders(in: Inputs, spec: Spec) !Reason {
    const how = spec.orders orelse "";
    if (std.mem.eql(u8, how, "mixed")) {
        const rb = readback(in, in.path, &.{ "--filter", "s", "==", "B" }) catch |err| return refusedReason(in, err);
        const s = indexOf(rb.names, "s").?;
        var truth: usize = 0;
        for (rb.rows) |r| if (r.array.items[s] == .string and std.mem.eql(u8, r.array.items[s].string, "B")) {
            truth += 1;
        };
        const pushed: usize = @intCast(try num(rb.pushed[0]));
        var has_zero = false;
        if (rb.column_orders == .array) for (rb.column_orders.array.items) |o| {
            has_zero = has_zero or try num(o) == 0;
        };
        if (has_zero or pushed != 2 or truth != 2) return fail(in, "column_orders zero={} s='B' -> {d} truth={d}", .{ has_zero, pushed, truth });
        return null;
    }
    if (std.mem.eql(u8, how, "string")) {
        const rb = readback(in, in.path, &.{ "--filter", "s", "==", "a" }) catch |err| return refusedReason(in, err);
        var usable = true;
        for (rb.row_groups) |g| usable = usable and g.object.get("columns").?.array.items[0].object.get("has_min_max").?.bool;
        const pushed = try num(rb.pushed[0]);
        if (!usable or pushed != 1) return fail(in, "has_min_max={} s='a' -> {d}", .{ usable, pushed });
        return null;
    }
    const rb = readback(in, in.path, &.{ "--filter", "u64", ">=", "$pow2:63", "--filter", "u32", "<", "$pow2:31" }) catch |err| return refusedReason(in, err);
    var usable = true;
    for (rb.row_groups) |g| for (g.object.get("columns").?.array.items) |c| {
        const p = c.object.get("path").?.string;
        if (std.mem.eql(u8, p, "u32") or std.mem.eql(u8, p, "u64")) usable = usable and c.object.get("has_min_max").?.bool;
    };
    const u64c = indexOf(rb.names, "u64").?;
    const u32c = indexOf(rb.names, "u32").?;
    var truth = [2]i64{ 0, 0 };
    for (rb.rows) |r| {
        if (try num(r.array.items[u64c]) >= (1 << 63)) truth[0] += 1;
        if (try num(r.array.items[u32c]) < (1 << 31)) truth[1] += 1;
    }
    const pushed = [2]i128{ try num(rb.pushed[0]), try num(rb.pushed[1]) };
    if (!usable or pushed[0] != truth[0] or pushed[1] != truth[1])
        return fail(in, "has_min_max={} pushdown={d},{d} truth={d},{d}", .{ usable, pushed[0], pushed[1], truth[0], truth[1] });
    return null;
}

fn num(v: std.json.Value) !i128 {
    return switch (v) {
        .number_string => |s| std.fmt.parseInt(i128, s, 10),
        .integer => |n| n,
        else => error.NotANumber,
    };
}

// ---- wire modes (written files: values via both readers, sizes, encodings, float bounds)

const WireCell = union(enum) { val: Val, float: f64, nan };

fn wireKey(arena: Allocator, c: WireCell) ![]const u8 {
    return switch (c) {
        .nan => "nan",
        .float => |f| std.fmt.allocPrint(arena, "w{x}", .{@as(u64, @bitCast(f))}),
        .val => |v| exactKey(arena, v),
    };
}

fn wireFloat(f: f64) WireCell {
    return if (std.math.isNan(f)) .nan else .{ .float = f };
}

fn floatOfJson(v: std.json.Value) !f64 {
    return switch (v) {
        .number_string => |s| std.fmt.parseFloat(f64, s),
        .string => |s| std.fmt.parseFloat(f64, if (std.mem.eql(u8, s, "Infinity")) "inf" else if (std.mem.eql(u8, s, "-Infinity")) "-inf" else s),
        else => error.NotAFloat,
    };
}

fn modeWire(in: Inputs, spec: Spec) !Reason {
    const check = if (std.mem.startsWith(u8, spec.compare, "wire:")) spec.compare[5..] else "pyarrow";
    if (std.mem.eql(u8, check, "bytes") or std.mem.eql(u8, check, "encodings") or std.mem.eql(u8, check, "bounds")) {
        const rb = readback(in, in.path, &.{}) catch |err| return refusedReason(in, err);
        if (std.mem.eql(u8, check, "bytes")) {
            for (rb.row_groups) |g| {
                var total: i128 = 0;
                for (g.object.get("columns").?.array.items) |c| total += try num(c.object.get("uncompressed").?);
                if (total != try num(g.object.get("total_byte_size").?)) return fail(in, "total_byte_size", .{});
            }
            return null;
        }
        if (std.mem.eql(u8, check, "encodings")) {
            for (rb.row_groups, 0..) |g, gi| for (g.object.get("columns").?.array.items) |c| {
                if (!std.mem.eql(u8, c.object.get("physical").?.string, "FIXED_LEN_BYTE_ARRAY")) continue;
                for (c.object.get("encodings").?.array.items) |e| if (std.mem.eql(u8, e.string, "DELTA_LENGTH_BYTE_ARRAY"))
                    return fail(in, "FLBA rg{d}.{s}", .{ gi, c.object.get("path").?.string });
            };
            return null;
        }
        for (rb.row_groups[0].object.get("columns").?.array.items) |c| {
            const phys = c.object.get("physical").?.string;
            if (!std.mem.eql(u8, phys, "FLOAT") and !std.mem.eql(u8, phys, "DOUBLE")) continue;
            const path = c.object.get("path").?.string;
            const ci = indexOf(rb.names, path).?;
            var lo: ?f64 = null;
            var hi: ?f64 = null;
            for (rb.rows) |r| {
                const v = r.array.items[ci];
                if (v == .null) continue;
                const f = try floatOfJson(v);
                if (std.math.isNan(f)) continue;
                lo = if (lo) |x| @min(x, f) else f;
                hi = if (hi) |x| @max(x, f) else f;
            }
            const bounded = c.object.get("has_min_max").?.bool;
            if (lo == null) {
                if (bounded) return fail(in, "{s}: bounds on an all-NaN chunk", .{path});
                continue;
            }
            const want_lo: f64 = if (lo.? == 0) -0.0 else lo.?;
            const want_hi: f64 = if (hi.? == 0) 0.0 else hi.?;
            if (!bounded) return fail(in, "{s}: no bounds", .{path});
            const smin = try floatOfJson(c.object.get("min").?);
            const smax = try floatOfJson(c.object.get("max").?);
            if (@as(u64, @bitCast(smin)) != @as(u64, @bitCast(want_lo)) or @as(u64, @bitCast(smax)) != @as(u64, @bitCast(want_hi)))
                return fail(in, "{s}: [{d}, {d}] want [{d}, {d}]", .{ path, smin, smax, want_lo, want_hi });
        }
        return null;
    }
    const rb = readback(in, in.path, &.{}) catch |err| return refusedReason(in, err);
    const described = duck(in, try std.fmt.allocPrint(in.arena, "SELECT * FROM '{s}' LIMIT 0", .{in.path})) catch |err| return refusedReason(in, err);
    var select: std.ArrayList(u8) = .empty;
    const types = try in.arena.alloc([]const u8, rb.names.len);
    for (rb.names, 0..) |name, i| {
        types[i] = described.types[indexOf(described.names, name).?];
        if (i != 0) try select.appendSlice(in.arena, ", ");
        const quoted = try std.fmt.allocPrint(in.arena, "\"{s}\"", .{name});
        if (std.ascii.startsWithIgnoreCase(types[i], "BLOB")) {
            try select.print(in.arena, "lower(hex({s})) AS {s}", .{ quoted, quoted });
        } else if (std.ascii.startsWithIgnoreCase(types[i], "UUID")) {
            try select.print(in.arena, "lower({s}::VARCHAR) AS {s}", .{ quoted, quoted });
        } else try select.appendSlice(in.arena, quoted);
    }
    const where = if (spec.where) |w| try std.fmt.allocPrint(in.arena, " WHERE {s}", .{w}) else "";
    const want = try wireDuck(in, try std.fmt.allocPrint(in.arena, "SELECT {s} FROM '{s}'{s}", .{ select.items, in.fixture, where }), rb.names, types);
    var got: [][]const u8 = undefined;
    if (std.mem.eql(u8, check, "duck")) {
        got = try wireDuck(in, try std.fmt.allocPrint(in.arena, "SELECT {s} FROM '{s}'", .{ select.items, in.path }), rb.names, types);
    } else {
        got = try in.arena.alloc([]const u8, rb.rows.len);
        for (rb.rows, 0..) |r, i| {
            var key: std.ArrayList(u8) = .empty;
            for (r.array.items) |v| {
                const cell: WireCell = switch (v) {
                    .number_string => |s| if (std.mem.indexOfAny(u8, s, ".eE") != null) wireFloat(try std.fmt.parseFloat(f64, s)) else .{ .val = try fromArrow(in.arena, v, true) },
                    .string => |s| if (std.mem.eql(u8, s, "NaN")) .nan else if (std.mem.eql(u8, s, "Infinity") or std.mem.eql(u8, s, "-Infinity")) wireFloat(try floatOfJson(v)) else .{ .val = .{ .text = s } },
                    else => .{ .val = try fromArrow(in.arena, v, true) },
                };
                const k = try wireKey(in.arena, cell);
                try key.print(in.arena, "{d}:{s}|", .{ k.len, k });
            }
            got[i] = key.items;
        }
    }
    if (got.len != want.len) return fail(in, "zpq={d} rows truth={d}", .{ got.len, want.len });
    std.mem.sort([]const u8, got, {}, compare.lessText);
    std.mem.sort([]const u8, want, {}, compare.lessText);
    for (got, want) |a, b| if (!std.mem.eql(u8, a, b)) return fail(in, "row zpq={s} truth={s}", .{ a, b });
    return null;
}

fn wireDuck(in: Inputs, sql: []const u8, names: []const []const u8, types: []const []const u8) ![][]const u8 {
    const ans = try in.oracle.ask(in.arena, sql);
    if (ans.refused) |why| {
        last_refusal = why;
        return error.OracleRefused;
    }
    const out = try in.arena.alloc([]const u8, ans.rows.len);
    for (ans.rows, 0..) |row, r| {
        var key: std.ArrayList(u8) = .empty;
        for (names, types) |n, t| {
            const v = row.get(n) orelse .null;
            const cell: WireCell = if (v == .null)
                .{ .val = .null }
            else if (std.ascii.startsWithIgnoreCase(t, "DOUBLE") or std.ascii.startsWithIgnoreCase(t, "FLOAT") or std.ascii.startsWithIgnoreCase(t, "REAL"))
                wireFloat(try floatOfJson(v))
            else if (std.ascii.startsWithIgnoreCase(t, "BLOB") or std.ascii.startsWithIgnoreCase(t, "UUID"))
                .{ .val = .{ .text = try std.ascii.allocLowerString(in.arena, v.string) } }
            else switch (v) {
                .number_string => |s| .{ .val = if (std.mem.indexOfAny(u8, s, ".eE") != null) try compare.exactNumber(s) else .{ .int = try std.fmt.parseInt(compare.Big, s, 10) } },
                .string => |s| .{ .val = .{ .text = s } },
                .bool => |b| .{ .val = .{ .boolean = b } },
                else => .{ .val = .{ .nested = v } },
            };
            const k = try wireKey(in.arena, cell);
            try key.print(in.arena, "{d}:{s}|", .{ k.len, k });
        }
        out[r] = key.items;
    }
    return out;
}

// ---- tail modes

fn modeNames(in: Inputs, spec: Spec) !Reason {
    const want = spec.names orelse return error.MissingNames;
    for (try zpqRows(in)) |row| {
        const got = row.keys();
        if (got.len != want.len) return fail(in, "names {s} want {s}", .{ try std.mem.join(in.arena, ",", got), try std.mem.join(in.arena, ",", want) });
        for (got, want) |g, w| if (!std.mem.eql(u8, g, w)) return fail(in, "names {s} want {s}", .{ try std.mem.join(in.arena, ",", got), try std.mem.join(in.arena, ",", want) });
    }
    return null;
}

fn modeOrder(in: Inputs, _: Spec) !Reason {
    const out = readback(in, in.path, &.{}) catch |err| return refusedReason(in, err);
    const src = readback(in, in.fixture, &.{}) catch |err| return refusedReason(in, err);
    const oi = indexOf(out.names, "id") orelse return fail(in, "no id column", .{});
    const si = indexOf(src.names, "id").?;
    var want: std.ArrayList(i128) = .empty;
    for (src.rows) |r| {
        const v = r.array.items[si];
        if (v != .null and try num(v) > 0) try want.append(in.arena, try num(v));
    }
    if (out.rows.len != want.items.len) return fail(in, "order zpq={d} input={d}", .{ out.rows.len, want.items.len });
    for (out.rows, want.items) |r, w| {
        const v = r.array.items[oi];
        if (v == .null or try num(v) != w) return fail(in, "order zpq={d} input={d}", .{ out.rows.len, want.items.len });
    }
    return null;
}

fn modeSelectOrder(in: Inputs, _: Spec) !Reason {
    const out = readback(in, in.path, &.{}) catch |err| return refusedReason(in, err);
    const got = try std.mem.join(in.arena, ",", out.names);
    if (!std.mem.eql(u8, got, "name,id")) return fail(in, "columns {s}", .{got});
    return null;
}

fn modeAvgExact(in: Inputs, spec: Spec) !Reason {
    const z = try zpqRows(in);
    const ans = try in.oracle.ask(in.arena, try oracleSql(in, spec));
    if (ans.refused) |why| return fail(in, "{s}", .{why});
    if (z.len != ans.rows.len) return fail(in, "avg group count", .{});
    for (ans.rows) |d| {
        const g = d.get("g").?;
        const row = for (z) |row| {
            if (compare.jsonEqual(row.get("g").?, g)) break row;
        } else return fail(in, "avg g missing", .{});
        // The exact sum is wider than f64. The check is the float of that sum, which is what zpq prints.
        const sv: f64 = @floatFromInt(try std.fmt.parseInt(compare.Big, try jsonText(d.get("sv").?), 10));
        const nv = try std.fmt.parseInt(i64, try jsonText(d.get("nv").?), 10);
        const sw: f64 = @floatFromInt(try std.fmt.parseInt(compare.Big, try jsonText(d.get("sw").?), 10));
        const nw = try std.fmt.parseInt(i64, try jsonText(d.get("nw").?), 10);
        const a = try floatOfJson(row.get("a").?);
        const b = try floatOfJson(row.get("b").?);
        if (a != sv / @as(f64, @floatFromInt(nv)) or b != sw / @as(f64, @floatFromInt(nw))) return fail(in, "avg zpq={d},{d}", .{ a, b });
        const st = row.get("st").?.object;
        if (try floatOfJson(st.get("sum").?) != sv or try num(st.get("count").?) != nv) return fail(in, "avg_state", .{});
    }
    return null;
}

fn jsonText(v: std.json.Value) ![]const u8 {
    return switch (v) {
        .number_string => |s| s,
        .string => |s| s,
        else => error.NotText,
    };
}

fn modeLevels(in: Inputs, spec: Spec) !Reason {
    const z = try zpqRows(in);
    const ans = try in.oracle.ask(in.arena, try oracleSql(in, spec));
    if (ans.refused) |why| return fail(in, "{s}", .{why});
    const got = try num(z[0].get("n").?);
    const want = try std.fmt.parseInt(i128, try jsonText(ans.rows[0].get("n").?), 10);
    if (got != want) return fail(in, "count {d} want {d}", .{ got, want });
    return null;
}

fn modeTyped(in: Inputs, spec: Spec) !Reason {
    const rb = readback(in, in.path, &.{}) catch |err| return refusedReason(in, err);
    const want_types = spec.types orelse return error.MissingTypes;
    const got_types = try std.mem.join(in.arena, "|", rb.types);
    if (!std.mem.eql(u8, got_types, try std.mem.join(in.arena, "|", want_types))) return fail(in, "types {s}", .{got_types});
    const zrows = (try arrowRows(in, rb, rb.names, true)).?;
    const t = try duck(in, try oracleSql(in, spec));
    if (spec.orders) |scales| {
        var parts = std.mem.splitScalar(u8, scales, '|');
        while (parts.next()) |part| {
            const colon = std.mem.indexOfScalar(u8, part, ':').?;
            const name = part[0..colon];
            const scale = try std.fmt.parseInt(u16, part[colon + 1 ..], 10);
            const si = indexOf(t.names, try std.fmt.allocPrint(in.arena, "{s}__sum", .{name})).?;
            const ci = indexOf(t.names, try std.fmt.allocPrint(in.arena, "{s}__count", .{name})).?;
            const mi = indexOf(t.names, name).?;
            for (t.rows) |row| row[mi] = try compare.decimalMean(row[si], row[ci], scale);
        }
        for (t.rows) |*row| row.* = row.*[0..want_types.len];
    }
    if (try compare.sameMultiset(in.arena, zrows, t.rows)) |why| return why;
    if (std.mem.eql(u8, spec.compare, "decimal")) {
        const back = try duck(in, try std.fmt.allocPrint(in.arena, "SELECT * FROM '{s}'", .{in.path}));
        if (try compare.sameMultiset(in.arena, back.rows, zrows)) |_| return fail(in, "DuckDB reads other values back", .{});
    }
    return null;
}

fn modeSlicedWrite(in: Inputs, spec: Spec) !Reason {
    const rb = readback(in, in.path, &.{}) catch |err| return refusedReason(in, err);
    if (rb.row_groups.len < 2) return fail(in, "one row group out: the write was not sliced", .{});
    const t = try duck(in, try oracleSql(in, spec));
    const z = (try arrowRows(in, rb, t.names, false)) orelse return fail(in, "written file lacks a column", .{});
    if (try compare.sameMultiset(in.arena, z, t.rows)) |_| return fail(in, "sliced write zpq={d} duck={d}", .{ z.len, t.rows.len });
    return null;
}

fn modeSlicedPrint(in: Inputs, spec: Spec) !Reason {
    var t = try duck(in, try oracleSql(in, spec));
    if (spec.row_count) |n| t.rows = t.rows[0..@min(n, t.rows.len)];
    const z = try zpqRows(in);
    const got = try in.arena.alloc([]const Val, z.len);
    for (z, 0..) |row, i| got[i] = (try zpqByName(in, row, t.names, t.lanes)) orelse return fail(in, "zpq row lacks a column", .{});
    if (try compare.sameMultiset(in.arena, got, t.rows)) |_| return fail(in, "sliced print zpq={d} duck={d}", .{ got.len, t.rows.len });
    return null;
}

fn modeRowCount(in: Inputs, spec: Spec) !Reason {
    var lines: usize = 0;
    var it = std.mem.splitScalar(u8, in.text, '\n');
    while (it.next()) |line| if (line.len != 0) {
        lines += 1;
    };
    if (spec.row_count) |n| if (lines != n) return fail(in, "rows {d} want {d}", .{ lines, n });
    return null;
}

// ---- corpus: every row of a file, as zpq's NDJSON prints it (docs/output-format.md)

/// The stored unit of a temporal column, from its Parquet schema: INT96 is nanoseconds.
fn temporalUnit(physical: []const u8, converted: []const u8, logical: []const u8, duck_type: []const u8) []const u8 {
    if (std.mem.eql(u8, physical, "INT96")) return "ns";
    if (std.mem.indexOf(u8, converted, "MILLIS") != null or std.mem.indexOf(u8, logical, "MILLIS") != null) return "ms";
    if (std.mem.indexOf(u8, converted, "MICROS") != null or std.mem.indexOf(u8, logical, "MICROS") != null) return "us";
    if (std.mem.indexOf(u8, logical, "NANOS") != null) return "ns";
    if (std.ascii.startsWithIgnoreCase(duck_type, "TIMESTAMP_MS")) return "ms";
    if (std.ascii.startsWithIgnoreCase(duck_type, "TIMESTAMP_NS")) return "ns";
    return "us";
}

fn modeCorpus(in: Inputs, _: Spec) !Reason {
    const file = in.fixture;
    const shape = duck(in, try std.fmt.allocPrint(in.arena, "SELECT * FROM '{s}' LIMIT 0", .{file})) catch |err| return refusedReason(in, err);
    const schema = try in.oracle.ask(in.arena, try std.fmt.allocPrint(in.arena,
        "SELECT name, coalesce(type, '') AS physical, coalesce(converted_type, '') AS converted, " ++
            "coalesce(logical_type, '') AS logical FROM parquet_schema('{s}')", .{file}));
    if (schema.refused) |why| return fail(in, "oracle cannot read the schema: {s}", .{why});
    var select: std.ArrayList(u8) = .empty;
    for (shape.names, shape.types, 0..) |name, t, i| {
        if (i != 0) try select.appendSlice(in.arena, ", ");
        const q = try std.fmt.allocPrint(in.arena, "\"{s}\"", .{try std.mem.replaceOwned(u8, in.arena, name, "\"", "\"\"")});
        var physical: []const u8 = "";
        var converted: []const u8 = "";
        var logical: []const u8 = "";
        for (schema.rows) |row| if (std.mem.eql(u8, row.get("name").?.string, name)) {
            physical = row.get("physical").?.string;
            converted = row.get("converted").?.string;
            logical = row.get("logical").?.string;
        };
        if (std.ascii.eqlIgnoreCase(t, "DATE")) {
            try select.print(in.arena, "({s} - DATE '1970-01-01') AS {s}", .{ q, q });
        } else if (std.ascii.startsWithIgnoreCase(t, "TIMESTAMP") or std.ascii.startsWithIgnoreCase(t, "TIME")) {
            const unit = temporalUnit(physical, converted, logical, t);
            // INT96 reads at microseconds and can pass epoch_ns's range: scale the microseconds instead.
            if (std.mem.eql(u8, physical, "INT96")) {
                try select.print(in.arena, "epoch_us({s})::HUGEINT * 1000 AS {s}", .{ q, q });
            } else try select.print(in.arena, "epoch_{s}({s}) AS {s}", .{ unit, q, q });
        } else if (std.ascii.eqlIgnoreCase(t, "BLOB") or std.ascii.startsWithIgnoreCase(t, "GEOMETRY")) {
            const wkb = std.ascii.startsWithIgnoreCase(t, "GEOMETRY") or std.mem.indexOf(u8, converted, "BSON") != null or
                std.mem.indexOf(u8, logical, "Bson") != null or std.mem.indexOf(u8, logical, "Geo") != null;
            if (wkb) {
                try select.print(in.arena, "CASE WHEN {s} IS NULL THEN NULL ELSE {{'base64': to_base64({s}::BLOB)}} END AS {s}", .{ q, q, q });
            } else try select.print(in.arena, "CASE WHEN {s} IS NULL THEN NULL WHEN TRY(decode({s})) IS NOT NULL THEN to_json(decode({s})) " ++
                "ELSE to_json({{'base64': to_base64({s})}}) END AS {s}", .{ q, q, q, q, q });
        } else if (std.ascii.eqlIgnoreCase(t, "UUID") or std.ascii.eqlIgnoreCase(t, "JSON")) {
            try select.print(in.arena, "{s}::VARCHAR AS {s}", .{ q, q });
        } else try select.appendSlice(in.arena, q);
    }
    const t = duck(in, try std.fmt.allocPrint(in.arena, "SELECT {s} FROM '{s}'", .{ select.items, file })) catch |err| return refusedReason(in, err);
    const z = try zpqRows(in);
    const got = try in.arena.alloc([]const Val, z.len);
    for (z, 0..) |row, i| got[i] = (try zpqByName(in, row, t.names, t.lanes)) orelse return fail(in, "zpq row lacks an oracle column", .{});
    return compare.sameOrdered(in.arena, got, t.rows);
}

/// A PyArrow cell as zpq prints it: unannotated bytes as text when they are UTF-8, else {"base64": ...}.
fn printedArrow(arena: Allocator, v: std.json.Value, always_base64: bool) !Val {
    if (v == .object and v.object.count() == 1) if (v.object.get("$bytes")) |hex| {
        const bytes = try arena.alloc(u8, hex.string.len / 2);
        _ = try std.fmt.hexToBytes(bytes, hex.string);
        if (!always_base64 and std.unicode.utf8ValidateSlice(bytes)) return .{ .text = bytes };
        const enc = std.base64.standard.Encoder;
        const b64 = try arena.alloc(u8, enc.calcSize(bytes.len));
        _ = enc.encode(b64, bytes);
        var obj: std.json.ObjectMap = .empty;
        try obj.put(arena, "base64", .{ .string = b64 });
        return .{ .nested = .{ .object = obj } };
    };
    return fromArrow(arena, v, true);
}

/// Every row against PyArrow's stored-value read, where DuckDB reads a column as a type zpq does not (VARIANT).
fn modeCorpusArrow(in: Inputs, _: Spec) !Reason {
    var argv: std.ArrayList([]const u8) = .empty;
    try argv.appendSlice(in.arena, &.{ in.python, "tools/check/readback.py", in.fixture, "--stored" });
    const out = try oracle_mod.capture(in.gpa, in.io, argv.items);
    defer in.gpa.free(out.stderr);
    defer in.gpa.free(out.stdout);
    if (out.code != 0) return fail(in, "{s}", .{std.mem.trim(u8, out.stderr, " \t\r\n")});
    const v = (try compare.parse(in.arena, try in.arena.dupe(u8, out.stdout))).object;
    const names = try strings(in.arena, v.get("names").?);
    const types = try strings(in.arena, v.get("types").?);
    const lanes = try in.arena.alloc(Lane, types.len);
    for (types, 0..) |t, i| lanes[i] = if (std.mem.startsWith(u8, t, "decimal")) .exact else .plain;
    // BSON and the WKB types print as base64 whatever their bytes hold (docs/binary-output.md).
    const notes = try strings(in.arena, v.get("annotations").?);
    const wkb = try in.arena.alloc(bool, names.len);
    for (notes, 0..) |n, c| wkb[c] = std.mem.indexOf(u8, n, "BSON") != null or std.mem.indexOf(u8, n, "Geometry") != null or
        std.mem.indexOf(u8, n, "Geography") != null;
    const rows = v.get("rows").?.array.items;
    const want = try in.arena.alloc([]const Val, rows.len);
    for (rows, 0..) |row, r| {
        const vals = try in.arena.alloc(Val, names.len);
        for (row.array.items, 0..) |cell, c| vals[c] = try printedArrow(in.arena, cell, wkb[c]);
        want[r] = vals;
    }
    const z = try zpqRows(in);
    const got = try in.arena.alloc([]const Val, z.len);
    for (z, 0..) |row, i| got[i] = (try zpqByName(in, row, names, lanes)) orelse return fail(in, "zpq row lacks a PyArrow column", .{});
    return compare.sameOrdered(in.arena, got, want);
}

/// Rows pinned in the case, where the rival readers are known to be wrong (`why` says how).
fn modePinned(in: Inputs, spec: Spec) !Reason {
    if (spec.why == null) return error.PinnedRowsNeedAReason;
    const want_rows = spec.rows orelse return error.MissingRows;
    const z = try zpqRows(in);
    if (z.len != want_rows.len) return fail(in, "zpq={d} rows pinned={d}", .{ z.len, want_rows.len });
    for (z, want_rows, 0..) |row, w, i| {
        const names = w.object.keys();
        if (row.count() != names.len) return fail(in, "row {d}: {d} columns, pinned {d}", .{ i, row.count(), names.len });
        for (names) |n| {
            const g = try compare.fromZpq(row.get(n) orelse return fail(in, "row {d} lacks {s}", .{ i, n }), .exact);
            const e = try compare.fromZpq(w.object.get(n).?, .exact);
            if (!compare.same(g, e)) return fail(in, "row {d} {s}: zpq={s} pinned={s}", .{ i, n, compare.show(in.arena, g), compare.show(in.arena, e) });
        }
    }
    return null;
}
