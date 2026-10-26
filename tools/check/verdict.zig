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
    /// `golden`, `golden_rows`: the file under tools/check/goldens holding the reviewed output.
    golden: ?[]const u8 = null,
    /// `summary`: the Parquet codec the summary must report (null for text answers).
    codec: ?[]const u8 = null,
    /// `summary`: membership filters (Bloom or dictionary) must have pruned a row group.
    pruned: bool = false,
    /// `bloom`: which chunks carry a filter. "dictionary": exactly the chunks with a dictionary page, plus `names`;
    /// "names": exactly `names`. Null: at least `names`, and some filter.
    bloom_on: ?[]const u8 = null,
    /// `plan`, `footer`: what the `--explain` JSON, or readback.py's view of the written file, must hold.
    holds: ?std.json.Value = null,
    /// `golden_rows`: columns whose values fold in any order (float sums and their means), compared at the row
    /// policy's precision; every other value compares exactly.
    inexact: ?[]const []const u8 = null,
    /// Fragments a CLI point's stderr must contain (traces), whatever the mode.
    stderr_has: ?[]const []const u8 = null,
    /// The `--profile` trace the case writes (its `{out...}` file) records from 1 to this many scans (plan events).
    max_scans: ?usize = null,
    /// Statements DuckDB runs first (a view over the input), as part of the question.
    setup: ?[]const u8 = null,
    /// `pinned`: the expected rows, and why no rival reader is the oracle for them.
    rows: ?[]const std.json.Value = null,
    why: ?[]const u8 = null,
    /// `tied_rows`: the positions of the ORDER BY columns in the answer. A tie at the cut may pick other rows, never
    /// other ranks: when rows differ by position these columns must still match row by row.
    ranks: ?[]const usize = null,
    /// The answer comes from an engine that prints wide numbers as JSON strings (DuckDB's HUGEINT): a string in a
    /// numeric oracle column is read as that number. Never set for zpq, whose number output is pinned.
    text_numbers: bool = false,
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
    setup: []const u8 = "",
    /// A CLI point's stderr (summaries, profiles).
    stderr: []const u8 = "",
    /// Rewrite goldens instead of comparing with them (`--bless`); `blessed` counts them.
    bless: bool = false,
    blessed: ?*usize = null,
};

const Reason = ?[]const u8;

fn fail(in: Inputs, comptime fmt: []const u8, args: anytype) Reason {
    const text = std.fmt.allocPrint(in.arena, fmt, args) catch "out of memory";
    return text[0..@min(text.len, 400)];
}

pub fn run(in_raw: Inputs, spec: Spec) !Reason {
    var in = in_raw;
    if (spec.setup) |setup| in.setup = setup;
    if (spec.stderr_has) |fragments| for (fragments) |f| {
        if (std.mem.indexOf(u8, in.stderr, f) == null) return fail(in, "stderr lacks {s}", .{f});
    };
    if (spec.max_scans) |most| {
        const trace = std.Io.Dir.cwd().readFileAlloc(in.io, in.path, in.arena, .limited(64 << 20)) catch
            return fail(in, "no profile trace", .{});
        const events = (try compare.parse(in.arena, trace)).object.get("traceEvents").?.array.items;
        var scans: usize = 0;
        for (events) |e| if (e.object.get("name")) |n| {
            scans += @intFromBool(n == .string and std.mem.eql(u8, n.string, "plan"));
        };
        if (scans == 0 or scans > most) return fail(in, "{d} scans, want 1 to {d}", .{ scans, most });
    }
    const m = spec.compare;
    const modes = .{
        .{ "agg", modeAgg },
        .{ "group", modeGroup },
        .{ "ordered", modeOrdered },
        .{ "usame", modeUsame },
        .{ "dense", modeDense },
        .{ "values", modeValues },
        .{ "except", modeExcept },
        .{ "parquet", modeParquet },
        .{ "product", modeProduct },
        .{ "stats_rows", modeStatsRows },
        .{ "stats_agg", modeStatsAgg },
        .{ "stats_select", modeStatsSelect },
        .{ "orders", modeOrders },
        .{ "wire", modeWire },
        .{ "names", modeNames },
        .{ "order", modeOrder },
        .{ "select_order", modeSelectOrder },
        .{ "avg_exact", modeAvgExact },
        .{ "levels", modeLevels },
        .{ "typed", modeTyped },
        .{ "decimal", modeTyped },
        .{ "sliced_write", modeSlicedWrite },
        .{ "sliced_print", modeSlicedPrint },
        .{ "row_count", modeRowCount },
        .{ "corpus", modeCorpus },
        .{ "corpus_arrow", modeCorpusArrow },
        .{ "pinned", modePinned },
        .{ "tied_rows", modeTiedRows },
        .{ "golden", modeGolden },
        .{ "golden_rows", modeGoldenRows },
        .{ "readers_agree", modeReadersAgree },
        .{ "written_as", modeWrittenAs },
        .{ "json_parses", modeJsonParses },
        .{ "summary", modeSummary },
        .{ "stderr_types", modeStderrTypes },
        .{ "explain_only", modeExplainOnly },
        .{ "bloom", modeBloom },
        .{ "exact_ordered", modeDense },
        .{ "plan", modePlan },
        .{ "written_sql", modeWrittenSql },
        .{ "footer", modeFooter },
        .{ "page_index", modePageIndex },
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
    const ans = try in.oracle.askAfter(in.arena, in.setup, sql);
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

/// zpq's row values under the oracle's columns. zpq prints a non-finite float as the string "NaN", "Infinity" or
/// "-Infinity" (docs/output-format.md): in a float column (`types`) that string is the value, elsewhere it is text.
fn zpqByName(
    in: Inputs,
    row: std.json.ObjectMap,
    names: []const []const u8,
    lanes: []const Lane,
    types: ?[]const []const u8,
) !?[]Val {
    const out = try in.arena.alloc(Val, names.len);
    for (names, 0..) |n, i| {
        const v = row.get(n) orelse return null;
        const float_column = if (types) |ts| compare.isFloatType(ts[i]) else false;
        out[i] = if (float_column and v == .string) try compare.fromOracle(v, types.?[i]) else try compare.fromZpq(
            v,
            laneAt(lanes, i),
        );
    }
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
                if (std.mem.eql(u8, k, "$float")) return compare.fromFloat(try nonFinite(s));
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
        .string => |s| .{ .text = s },
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
    for (got, want) |g, w| if (!compare.same(g, w)) return fail(in, "zpq={s} duck={s}", .{
        compare.show(in.arena, g),
        compare.show(in.arena, w),
    });
    return null;
}

fn modeGroup(in: Inputs, spec: Spec) !Reason {
    const z = try zpqRows(in);
    const t = try oracleTable(in, spec);
    const got = try in.arena.alloc([]const Val, z.len);
    for (z, 0..) |row, i| got[i] = (try zpqByName(in, try splitState(in.arena, row), t.names, t.lanes, t.types)) orelse
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
    for (z, 0..) |row, i| got[i] = try canonVals(in, (try zpqByName(in, row, t.names, t.lanes, t.types)) orelse
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

fn exactMultiset(in: Inputs, got: []const []const Val, want: []const []const Val, ordered: bool) !Reason {
    return compare.exactRows(in.arena, got, want, ordered);
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
        // zpq's JSON read by the oracle's column types: a DOUBLE is its binary value, a DECIMAL its digits.
        for (t.names, t.types, 0..) |n, ty, c| {
            const v = row.get(n) orelse return fail(in, "zpq lacks {s}", .{n});
            vals[c] = compare.fromOracle(v, ty) catch return fail(in, "zpq {s} is not a {s}", .{ n, ty });
        }
        got[i] = vals;
    }
    return exactMultiset(in, got, t.rows, std.mem.eql(u8, spec.compare, "exact_ordered"));
}

fn modeValues(in: Inputs, spec: Spec) !Reason {
    const sql = try oracleSql(in, spec);
    const wrapped = try std.fmt.allocPrint(in.arena,
        \\SELECT c::VARCHAR AS c, CASE WHEN s IS NULL THEN 'NULL' ELSE s::VARCHAR END AS s FROM ({s}) q(c, s)
    , .{sql});
    const ans = try in.oracle.ask(in.arena, wrapped);
    if (ans.refused != null) return fail(in, "zpq answered {s}; duckdb refuses", .{in.text});
    if (ans.rows.len != 1) return fail(in, "duck rows {d}", .{ans.rows.len});
    const want =
        try std.fmt.allocPrint(in.arena, "{s},{s}", .{ ans.rows[0].get("c").?.string, ans.rows[0].get("s").?.string });
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
    if (!std.mem.eql(u8, got_names, want_names.items)) return fail(in, "column order got {s} want {s}", .{
        got_names,
        want_names.items,
    });
    var cols: std.ArrayList(u8) = .empty;
    if (in.select) try cols.appendSlice(in.arena, "x") else if (in.columns) |wanted| {
        for (wanted, 0..) |name, i| {
            if (i != 0) try cols.appendSlice(in.arena, ", ");
            try cols.print(in.arena, "\"{s}\"", .{name});
        }
    } else try cols.appendSlice(in.arena, "*");
    const where = if (spec.where) |w| try std.fmt.allocPrint(in.arena, " WHERE {s}", .{w}) else "";
    const oracle = try std.fmt.allocPrint(in.arena, "SELECT {s} FROM '{s}'{s}", .{ spec.duck_select orelse
        cols.items, in.fixture, where });
    const written = try std.fmt.allocPrint(in.arena, "SELECT {s} FROM '{s}'", .{ cols.items, in.path });
    const diff =
        try std.fmt.allocPrint(
            in.arena,
            "SELECT count(*) AS n FROM (({s} EXCEPT ALL {s}) UNION ALL ({s} EXCEPT ALL {s}))",
            .{ written, oracle, oracle, written },
        );
    const t = duck(in, diff) catch |err| return refusedReason(in, err);
    if (t.rows.len != 1 or t.rows[0][0] != .int or
        t.rows[0][0].int != 0) return fail(in, "row mismatch count {s}", .{compare.show(in.arena, t.rows[0][0])});
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
            return fail(in, "columns {s} want {s}", .{ try std.mem.join(in.arena, ",", rb.names), try std.mem.join(
                in.arena,
                ",",
                want_cols,
            ) });
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
    if (!std.mem.eql(u8, got_names, try std.mem.join(in.arena, ",", names))) return fail(
        in,
        "product parquet columns={s}",
        .{got_names},
    );
    const want = try in.arena.alloc([]const Val, truth.len);
    for (truth, 0..) |row, i| {
        const vals = try in.arena.alloc(Val, names.len);
        for (names, 0..) |n, c| vals[c] = try rawZpq(in, row.get(n).?);
        want[i] = vals;
    }
    const got = (try arrowRows(in, rb, names, false)).?;
    if (try exactMultiset(in, got, want, true)) |why| return fail(in, "product parquet: {s}", .{why});
    const back = duck(in, try std.fmt.allocPrint(in.arena, "SELECT * FROM '{s}'", .{in.path})) catch |err|
        return refusedReason(in, err);
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
        .nested, .special => false,
    };
    _ = in;
    if (std.mem.eql(u8, p.op, "eq")) return eq;
    if (std.mem.eql(u8, p.op, "ne")) return !eq;
    return error.BadPredicate;
}

/// The fixture's rows PyArrow decodes, without statistics, kept by the case's predicate.
const Truth = struct { names: []const []const u8, rows: []const []const Val, lanes: []const Lane };

fn truthRows(in: Inputs, spec: Spec) !Truth {
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
        for (z, 0..) |row, i| rows[i] = (try zpqByName(in, row, truth.names, truth.lanes, null)) orelse
            return fail(in, "zpq row lacks a column", .{});
        got = rows;
    }
    return compare.sameMultiset(in.arena, got, truth.rows);
}

fn modeStatsAgg(in: Inputs, spec: Spec) !Reason {
    const select = spec.sql orelse return error.MissingOracleSql;
    const described =
        duck(in, try std.fmt.allocPrint(in.arena, "SELECT {s} FROM '{s}' LIMIT 0", .{ select, in.fixture })) catch |err|
            return refusedReason(in, err);
    var lanes = described.lanes;
    var want: []const Val = undefined;
    if (spec.py_agg) |name| {
        const truth = try truthRows(in, spec);
        want = try pyAggregate(in, name, truth.names, truth.rows);
    } else {
        const where = if (spec.where) |w| try std.fmt.allocPrint(in.arena, " WHERE {s}", .{w}) else "";
        const t =
            duck(
                in,
                try std.fmt.allocPrint(in.arena, "SELECT {s} FROM '{s}'{s}", .{ select, in.fixture, where }),
            ) catch |err|
                return refusedReason(in, err);
        if (t.rows.len != 1) return fail(in, "duck rows {d}", .{t.rows.len});
        want = t.rows[0];
        lanes = t.lanes;
    }
    const z = try zpqRows(in);
    if (z.len != 1) return fail(in, "zpq rows {d}", .{z.len});
    const got = try zpqValues(in, try splitState(in.arena, z[0]), lanes);
    if (got.len != want.len) return fail(in, "zpq {d} values, truth {d}", .{ got.len, want.len });
    for (got, want) |g, w| if (!compare.same(g, w)) return fail(in, "zpq={s} truth={s}", .{
        compare.show(in.arena, g),
        compare.show(in.arena, w),
    });
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
        return try in.arena.dupe(Val, &.{
            count,
            sum_i,
            try extreme(rows, dec, .lt),
            try extreme(rows, dec, .gt),
            try extreme(rows, u, .gt),
        });
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
    const t =
        duck(
            in,
            try std.fmt.allocPrint(in.arena, "SELECT {s} FROM '{s}'{s}", .{ spec.sql.?, in.fixture, where }),
        ) catch |err|
            return refusedReason(in, err);
    var got: []const []const Val = undefined;
    if (in.path.len != 0) {
        const rb = readback(in, in.path, &.{}) catch |err| return refusedReason(in, err);
        got = (try arrowRows(in, rb, t.names, false)) orelse return fail(in, "written file lacks a column", .{});
    } else {
        const z = try zpqRows(in);
        const rows = try in.arena.alloc([]const Val, z.len);
        for (z, 0..) |row, i| {
            const vals = try in.arena.alloc(Val, t.names.len);
            // zpq's JSON read by the oracle's column types: a DOUBLE is its binary value, a DECIMAL its digits.
            for (t.names, t.types, 0..) |n, ty, c| {
                const v = row.get(n) orelse return fail(in, "zpq lacks {s}", .{n});
                vals[c] = compare.fromOracle(v, ty) catch return fail(in, "zpq {s} is not a {s}", .{ n, ty });
            }
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
        if (has_zero or pushed != 2 or
            truth != 2) return fail(in, "column_orders zero={} s='B' -> {d} truth={d}", .{ has_zero, pushed, truth });
        return null;
    }
    if (std.mem.eql(u8, how, "string")) {
        const rb = readback(in, in.path, &.{ "--filter", "s", "==", "a" }) catch |err| return refusedReason(in, err);
        var usable = true;
        for (rb.row_groups) |g| usable = usable and
            g.object.get("columns").?.array.items[0].object.get("has_min_max").?.bool;
        const pushed = try num(rb.pushed[0]);
        if (!usable or pushed != 1) return fail(in, "has_min_max={} s='a' -> {d}", .{ usable, pushed });
        return null;
    }
    const rb =
        readback(in, in.path, &.{ "--filter", "u64", ">=", "$pow2:63", "--filter", "u32", "<", "$pow2:31" }) catch |err|
            return refusedReason(in, err);
    var usable = true;
    for (rb.row_groups) |g| for (g.object.get("columns").?.array.items) |c| {
        const p = c.object.get("path").?.string;
        if (std.mem.eql(u8, p, "u32") or std.mem.eql(u8, p, "u64")) usable = usable and
            c.object.get("has_min_max").?.bool;
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
        return fail(in, "has_min_max={} pushdown={d},{d} truth={d},{d}", .{
            usable,
            pushed[0],
            pushed[1],
            truth[0],
            truth[1],
        });
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
        .val => |v| blk: {
            var nan: usize = 0;
            break :blk compare.exactKey(arena, v, &nan);
        },
    };
}

fn wireFloat(f: f64) WireCell {
    return if (std.math.isNan(f)) .nan else .{ .float = f };
}

/// A JSON float: a number, a bare non-finite token (as `compare.parse` keeps it), or a PyArrow {"$float"} tag.
fn floatOfJson(v: std.json.Value) !f64 {
    return switch (v) {
        .number_string => |s| std.fmt.parseFloat(f64, s),
        .string => |s| nonFinite(if (s.len > 0 and s[0] == 0) s[1..] else s),
        .object => |o| nonFinite((o.get("$float") orelse return error.NotAFloat).string),
        else => error.NotAFloat,
    };
}

fn nonFinite(token: []const u8) !f64 {
    if (std.mem.eql(u8, token, "NaN")) return std.math.nan(f64);
    if (std.mem.eql(u8, token, "Infinity")) return std.math.inf(f64);
    if (std.mem.eql(u8, token, "-Infinity")) return -std.math.inf(f64);
    return error.NotAFloat;
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
                for (c.object.get("encodings").?.array.items) |e| if (std.mem.eql(
                    u8,
                    e.string,
                    "DELTA_LENGTH_BYTE_ARRAY",
                ))
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
            if (@as(u64, @bitCast(smin)) != @as(u64, @bitCast(want_lo)) or
                @as(u64, @bitCast(smax)) != @as(u64, @bitCast(want_hi)))
                return fail(in, "{s}: [{d}, {d}] want [{d}, {d}]", .{ path, smin, smax, want_lo, want_hi });
        }
        return null;
    }
    const rb = readback(in, in.path, &.{}) catch |err| return refusedReason(in, err);
    const described = duck(in, try std.fmt.allocPrint(in.arena, "SELECT * FROM '{s}' LIMIT 0", .{in.path})) catch |err|
        return refusedReason(in, err);
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
    const want =
        try wireDuck(in, try std.fmt.allocPrint(in.arena, "SELECT {s} FROM '{s}'{s}", .{
            select.items,
            in.fixture,
            where,
        }), rb.names, types);
    var got: [][]const u8 = undefined;
    if (std.mem.eql(u8, check, "duck")) {
        got = try wireDuck(
            in,
            try std.fmt.allocPrint(in.arena, "SELECT {s} FROM '{s}'", .{ select.items, in.path }),
            rb.names,
            types,
        );
    } else {
        got = try in.arena.alloc([]const u8, rb.rows.len);
        for (rb.rows, 0..) |r, i| {
            var key: std.ArrayList(u8) = .empty;
            for (r.array.items) |v| {
                const cell: WireCell = switch (v) {
                    .number_string => |s| if (std.mem.indexOfAny(u8, s, ".eE") != null)
                        wireFloat(try std.fmt.parseFloat(f64, s))
                    else
                        .{ .val = try fromArrow(in.arena, v, true) },
                    .string => |s| .{ .val = .{ .text = s } },
                    .object => |o| if (o.contains("$float")) wireFloat(try floatOfJson(v)) else .{ .val = try fromArrow(
                        in.arena,
                        v,
                        true,
                    ) },
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
    const ans = try in.oracle.askAfter(in.arena, in.setup, sql);
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
            else if (std.ascii.startsWithIgnoreCase(t, "DOUBLE") or std.ascii.startsWithIgnoreCase(t, "FLOAT") or
                std.ascii.startsWithIgnoreCase(t, "REAL"))
                wireFloat(try floatOfJson(v))
            else if (std.ascii.startsWithIgnoreCase(t, "BLOB") or std.ascii.startsWithIgnoreCase(t, "UUID"))
                .{ .val = .{ .text = try std.ascii.allocLowerString(in.arena, v.string) } }
            else switch (v) {
                .number_string => |s| .{ .val = if (std.mem.indexOfAny(u8, s, ".eE") != null)
                    try compare.exactNumber(s)
                else
                    .{ .int = try std.fmt.parseInt(compare.Big, s, 10) } },
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
        if (got.len != want.len) return fail(in, "names {s} want {s}", .{
            try std.mem.join(in.arena, ",", got),
            try std.mem.join(in.arena, ",", want),
        });
        for (got, want) |g, w| if (!std.mem.eql(u8, g, w)) return fail(in, "names {s} want {s}", .{ try std.mem.join(
            in.arena,
            ",",
            got,
        ), try std.mem.join(in.arena, ",", want) });
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
        if (v == .null or
            try num(v) != w) return fail(in, "order zpq={d} input={d}", .{ out.rows.len, want.items.len });
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
    const ans = try in.oracle.askAfter(in.arena, in.setup, try oracleSql(in, spec));
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
        if (a != sv / @as(f64, @floatFromInt(nv)) or
            b != sw / @as(f64, @floatFromInt(nw))) return fail(in, "avg zpq={d},{d}", .{ a, b });
        const st = row.get("st").?.object;
        if (try floatOfJson(st.get("sum").?) != sv or
            try num(st.get("count").?) != nv) return fail(in, "avg_state", .{});
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
    const ans = try in.oracle.askAfter(in.arena, in.setup, try oracleSql(in, spec));
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
    if (!std.mem.eql(u8, got_types, try std.mem.join(in.arena, "|", want_types))) return fail(
        in,
        "types {s}",
        .{got_types},
    );
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
        if (try compare.sameMultiset(in.arena, back.rows, zrows)) |_| return fail(
            in,
            "DuckDB reads other values back",
            .{},
        );
    }
    return null;
}

fn modeSlicedWrite(in: Inputs, spec: Spec) !Reason {
    const rb = readback(in, in.path, &.{}) catch |err| return refusedReason(in, err);
    if (rb.row_groups.len < 2) return fail(in, "one row group out: the write was not sliced", .{});
    const t = try duck(in, try oracleSql(in, spec));
    const z = (try arrowRows(in, rb, t.names, false)) orelse return fail(in, "written file lacks a column", .{});
    if (try compare.sameMultiset(in.arena, z, t.rows)) |_| return fail(in, "sliced write zpq={d} duck={d}", .{
        z.len,
        t.rows.len,
    });
    return null;
}

fn modeSlicedPrint(in: Inputs, spec: Spec) !Reason {
    var t = try duck(in, try oracleSql(in, spec));
    if (spec.row_count) |n| t.rows = t.rows[0..@min(n, t.rows.len)];
    const z = try zpqRows(in);
    const got = try in.arena.alloc([]const Val, z.len);
    for (z, 0..) |row, i| got[i] = (try zpqByName(in, row, t.names, t.lanes, t.types)) orelse
        return fail(in, "zpq row lacks a column", .{});
    if (try compare.sameMultiset(in.arena, got, t.rows)) |_| return fail(in, "sliced print zpq={d} duck={d}", .{
        got.len,
        t.rows.len,
    });
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
    const shape = duck(in, try std.fmt.allocPrint(in.arena, "SELECT * FROM '{s}' LIMIT 0", .{file})) catch |err|
        return refusedReason(in, err);
    const schema =
        try in.oracle.ask(in.arena, try std.fmt.allocPrint(in.arena, "SELECT name, coalesce(type, '') AS " ++
            "physical, coalesce(converted_type, '') AS converted, " ++
            "coalesce(logical_type, '') AS logical FROM parquet_schema('{s}')", .{file}));
    if (schema.refused) |why| return fail(in, "oracle cannot read the schema: {s}", .{why});
    var select: std.ArrayList(u8) = .empty;
    const widths = try in.arena.alloc(u8, shape.names.len);
    for (shape.names, shape.types, 0..) |name, t, i| {
        if (i != 0) try select.appendSlice(in.arena, ", ");
        const q =
            try std.fmt.allocPrint(in.arena, "\"{s}\"", .{try std.mem.replaceOwned(u8, in.arena, name, "\"", "\"\"")});
        var physical: []const u8 = "";
        var converted: []const u8 = "";
        var logical: []const u8 = "";
        for (schema.rows) |row| if (std.mem.eql(u8, row.get("name").?.string, name)) {
            physical = row.get("physical").?.string;
            converted = row.get("converted").?.string;
            logical = row.get("logical").?.string;
        };
        widths[i] = widthOf(t, logical);
        if (std.ascii.eqlIgnoreCase(t, "DATE")) {
            try select.print(in.arena, "({s} - DATE '1970-01-01') AS {s}", .{ q, q });
        } else if (std.ascii.startsWithIgnoreCase(t, "TIMESTAMP") or std.ascii.startsWithIgnoreCase(t, "TIME")) {
            const unit = temporalUnit(physical, converted, logical, t);
            // INT96 reads at microseconds and can pass epoch_ns's range: scale the microseconds instead.
            if (std.mem.eql(u8, physical, "INT96")) {
                try select.print(in.arena, "epoch_us({s})::HUGEINT * 1000 AS {s}", .{ q, q });
            } else try select.print(in.arena, "epoch_{s}({s}) AS {s}", .{ unit, q, q });
        } else if (std.ascii.eqlIgnoreCase(t, "BLOB") or std.ascii.startsWithIgnoreCase(t, "GEOMETRY")) {
            const wkb = std.ascii.startsWithIgnoreCase(t, "GEOMETRY") or
                std.mem.indexOf(u8, converted, "BSON") != null or
                std.mem.indexOf(u8, logical, "Bson") != null or std.mem.indexOf(u8, logical, "Geo") != null;
            if (wkb) {
                try select.print(
                    in.arena,
                    "CASE WHEN {s} IS NULL THEN NULL ELSE {{'base64': to_base64({s}::BLOB)}} END AS {s}",
                    .{ q, q, q },
                );
            } else try select.print(in.arena, "CASE WHEN {s} IS NULL THEN NULL WHEN TRY(decode({s})) IS NOT NULL " ++
                "THEN to_json(decode({s})) " ++
                "ELSE to_json({{'base64': to_base64({s})}}) END AS {s}", .{ q, q, q, q, q });
        } else if (std.ascii.eqlIgnoreCase(t, "UUID") or std.ascii.eqlIgnoreCase(t, "JSON")) {
            try select.print(in.arena, "{s}::VARCHAR AS {s}", .{ q, q });
        } else try select.appendSlice(in.arena, q);
    }
    const t = duck(in, try std.fmt.allocPrint(in.arena, "SELECT {s} FROM '{s}'", .{ select.items, file })) catch |err|
        return refusedReason(in, err);
    const z = try zpqRows(in);
    const got = try in.arena.alloc([]const Val, z.len);
    for (z, 0..) |row, i| got[i] = (try zpqByName(in, row, t.names, t.lanes, t.types)) orelse
        return fail(in, "zpq row lacks an oracle column", .{});
    return corpusRows(in, got, t.rows, widths);
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
    for (notes, 0..) |n, c| wkb[c] = std.mem.indexOf(u8, n, "BSON") != null or
        std.mem.indexOf(u8, n, "Geometry") != null or
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
    for (z, 0..) |row, i| got[i] = (try zpqByName(in, row, names, lanes, null)) orelse
        return fail(in, "zpq row lacks a PyArrow column", .{});
    const widths = try in.arena.alloc(u8, names.len);
    for (types, notes, 0..) |t, n, c| widths[c] = widthOf(t, n);
    return corpusRows(in, got, want, widths);
}

/// Rows pinned in the case, where the rival readers are known to be wrong (`why` says how).
fn modePinned(in: Inputs, spec: Spec) !Reason {
    if (spec.why == null) return error.PinnedRowsNeedAReason;
    const want_rows = spec.rows orelse return error.MissingRows;
    const z = try zpqRows(in);
    if (z.len != want_rows.len) return fail(in, "zpq={d} rows pinned={d}", .{ z.len, want_rows.len });
    for (z, want_rows, 0..) |row, w, i| {
        const names = w.object.keys();
        if (row.count() != names.len) return fail(
            in,
            "row {d}: {d} columns, pinned {d}",
            .{ i, row.count(), names.len },
        );
        for (names) |n| {
            const g = try compare.fromZpq(row.get(n) orelse return fail(in, "row {d} lacks {s}", .{ i, n }), .exact);
            const e = try compare.fromZpq(w.object.get(n).?, .exact);
            if (!compare.same(g, e)) return fail(in, "row {d} {s}: zpq={s} pinned={s}", .{ i, n, compare.show(
                in.arena,
                g,
            ), compare.show(in.arena, e) });
        }
    }
    return null;
}

/// Corpus rows compare exactly: a float column's values at its own width (f16, f32, f64), so the shortest decimal
/// zpq prints and the wider double the oracle prints of the same stored value agree, and nothing else does.
fn corpusRows(in: Inputs, got: []const []const Val, want: []const []const Val, widths: []const u8) !Reason {
    if (got.len != want.len) return fail(in, "zpq={d} rows oracle={d}", .{ got.len, want.len });
    var nan: usize = 0;
    for (got, want, 0..) |g, w, r| for (g, w, widths, 0..) |a, b, width, c| {
        const equal = if (a == .special or b == .special)
            a == .special and b == .special and a.special == b.special
        else if (width != 64 and isNumber(a) and isNumber(b))
            atWidth(a, width) == atWidth(b, width)
        else
            std.mem.eql(u8, try compare.exactKey(in.arena, a, &nan), try compare.exactKey(in.arena, b, &nan));
        if (!equal) return fail(in, "row {d} column {d}: zpq={s} oracle={s}", .{
            r,
            c,
            compare.show(in.arena, a),
            compare.show(in.arena, b),
        });
    };
    return null;
}

fn isNumber(v: Val) bool {
    return v == .int or v == .dec or v == .float;
}

fn atWidth(v: Val, width: u8) u64 {
    const f: f64 = switch (v) {
        .int => |n| @floatFromInt(n),
        .dec => |d| blk: {
            var buf: [200]u8 = undefined;
            break :blk std.fmt.parseFloat(f64, compare.decText(&buf, d)) catch unreachable;
        },
        .float => |x| x,
        else => unreachable,
    };
    if (width == 16) return @as(u16, @bitCast(@as(f16, @floatCast(f))));
    return @as(u32, @bitCast(@as(f32, @floatCast(f))));
}

fn widthOf(type_name: []const u8, annotation: []const u8) u8 {
    if (std.mem.indexOf(u8, annotation, "Float16") != null or std.mem.eql(u8, type_name, "halffloat")) return 16;
    if (std.ascii.eqlIgnoreCase(type_name, "FLOAT") or std.mem.eql(u8, type_name, "float")) return 32;
    return 64;
}

/// ClickBench's rule: rows equal position by position (within the scalar tolerance); without LIMIT, the same
/// multiset; with LIMIT, a cut through tied sort keys may pick any tied rows, so every zpq row must be a row of the
/// oracle's whole answer (exactly). A DATE the oracle's view makes of stored days is compared as those days.
fn modeTiedRows(in: Inputs, spec: Spec) !Reason {
    const t = duck(in, try oracleSql(in, spec)) catch |err| return refusedReason(in, err);
    const z = try zpqRows(in);
    const got = try in.arena.alloc([]Val, z.len);
    for (z, 0..) |row, i| {
        got[i] = try zpqValues(in, row, t.lanes);
        if (spec.text_numbers) for (row.values(), 0..) |v, c| if (v == .string and c < t.types.len and
            (isIntegerType(t.types[c]) or compare.isFloatType(t.types[c]) or compare.laneOf(t.types[c]) == .exact))
        {
            got[i][c] = compare.fromOracle(v, t.types[c]) catch got[i][c];
        };
    }
    if (got.len != t.rows.len) return fail(in, "zpq={d} rows duck={d}", .{ got.len, t.rows.len });
    for (got, t.rows) |g, w| if (g.len != w.len) return fail(in, "shape differs", .{});
    // zpq prints a DATE as its day number, other engines as ISO text: both compare as days.
    for (t.types, 0..) |ty, c| if (std.ascii.eqlIgnoreCase(ty, "DATE")) {
        for (t.rows) |row| row[c] = try toEpoch(row[c], ty);
        for (got) |row| row[c] = try toEpoch(row[c], ty);
    };
    const positional = for (got, t.rows) |g, w| {
        const equal = for (g, w) |a, b| {
            if (!compare.same(a, b)) break false;
        } else true;
        if (!equal) break false;
    } else true;
    if (positional) return null;
    for (spec.ranks orelse &.{}) |c| {
        if (c >= t.names.len) return fail(in, "rank column {d} not in the answer", .{c});
        for (got, t.rows, 0..) |g, w, r| if (!compare.same(g[c], w[c])) return fail(in, "rank {s} differs at row " ++
            "{d}: {s} want {s}", .{ t.names[c], r, compare.show(in.arena, g[c]), compare.show(in.arena, w[c]) });
    }
    const sql = try oracleSql(in, spec);
    const upper = try std.ascii.allocUpperString(in.arena, sql);
    const cut = std.mem.lastIndexOf(u8, upper, " LIMIT ") orelse return compare.sameMultiset(in.arena, got, t.rows);
    // Membership in DuckDB, as the old check did: an unlimited answer can be every row of the input (Q32's 16M
    // groups), which zpq-check must never hold. Each distinct zpq row needs at least as many exact matches in the
    // whole answer as zpq printed copies of it.
    var q: std.ArrayList(u8) = .empty;
    try q.print(in.arena, "WITH whole AS ({s}), g AS (SELECT * FROM (VALUES ", .{sql[0..cut]});
    for (got, 0..) |row, r| {
        try q.appendSlice(in.arena, if (r == 0) "(" else ", (");
        for (row, t.types, 0..) |v, ty, c| {
            if (c != 0) try q.appendSlice(in.arena, ", ");
            try sqlLiteral(in.arena, &q, v, ty);
        }
        try q.append(in.arena, ')');
    }
    try q.appendSlice(in.arena, ") AS v(");
    for (t.names, 0..) |_, c| try q.print(in.arena, "{s}g{d}", .{ if (c == 0) "" else ", ", c });
    try q.appendSlice(in.arena, ")) SELECT count(*) AS missing FROM (SELECT *, count(*) AS n FROM g GROUP BY ALL) " ++
        "AS d WHERE d.n > (SELECT count(*) FROM whole w WHERE ");
    for (t.names, 0..) |name, c| {
        try q.print(in.arena, "{s}w.\"{s}\" IS NOT DISTINCT FROM d.g{d}", .{ if (c == 0) "" else " AND ", name, c });
    }
    try q.append(in.arena, ')');
    const m = duck(in, q.items) catch |err| return refusedReason(in, err);
    const missing = m.rows[0][0];
    if (missing != .int or missing.int != 0)
        return fail(in, "{s} zpq rows not in DuckDB's whole answer", .{compare.show(in.arena, missing)});
    return null;
}

/// A written Parquet file holds exactly the rows of `sql`: the same column names in order, and the same multiset
/// of rows under each column's oracle type. An integer column may be written at another integer width (a sum
/// kept in INT64, where the oracle widens to HUGEINT): its values compare exactly after the cast, and a wrapped
/// value would differ. A DOUBLE where the oracle has an integer (a writer that widened) passes only if every value
/// is integral and in range, so no rounding hides behind the cast; an exact DECIMAL never degrades to floating
/// point.
fn modeWrittenSql(in: Inputs, spec: Spec) !Reason {
    const sql = try oracleSql(in, spec);
    const want = duck(in, try std.fmt.allocPrint(in.arena, "SELECT * FROM ({s}) LIMIT 0", .{sql})) catch |err|
        return refusedReason(in, err);
    const file = try std.fmt.allocPrint(in.arena, "read_parquet('{s}')", .{in.path});
    const got = duck(in, try std.fmt.allocPrint(in.arena, "SELECT * FROM {s} LIMIT 0", .{file})) catch |err|
        return refusedReason(in, err);
    const got_names = try std.mem.join(in.arena, ",", got.names);
    const want_names = try std.mem.join(in.arena, ",", want.names);
    if (!std.mem.eql(u8, got_names, want_names)) return fail(in, "columns {s} want {s}", .{ got_names, want_names });
    var cols: std.ArrayList(u8) = .empty;
    var integral: std.ArrayList(u8) = .empty;
    for (want.names, want.types, got.types, 0..) |name, wt, gt, i| {
        if (i != 0) try cols.appendSlice(in.arena, ", ");
        if (std.mem.eql(u8, wt, gt)) {
            try cols.print(in.arena, "\"{s}\"", .{name});
            continue;
        }
        const float_for_int = compare.isFloatType(gt) and isIntegerType(wt);
        if (!float_for_int and !(isIntegerType(gt) and isIntegerType(wt)))
            return fail(in, "column {s} is {s}, want {s}", .{ name, gt, wt });
        try cols.print(in.arena, "CAST(\"{s}\" AS {s}) AS \"{s}\"", .{ name, wt, name });
        if (!float_for_int) continue;
        try integral.print(in.arena, "{s}(\"{s}\" IS NOT NULL AND (NOT isfinite(\"{s}\") OR \"{s}\" != " ++
            "trunc(\"{s}\") OR TRY_CAST(\"{s}\" AS {s}) IS NULL))", .{
            if (integral.items.len == 0) "" else " OR ", name, name, name, name, name, wt,
        });
    }
    if (integral.items.len != 0) {
        const bad = duck(in, try std.fmt.allocPrint(in.arena, "SELECT count(*) AS n FROM {s} WHERE {s}", .{
            file,
            integral.items,
        })) catch |err| return refusedReason(in, err);
        if (bad.rows[0][0] != .int or bad.rows[0][0].int != 0) return fail(in, "{s} widened values are not exact " ++
            "integers", .{compare.show(in.arena, bad.rows[0][0])});
    }
    const written = try std.fmt.allocPrint(in.arena, "SELECT {s} FROM {s}", .{ cols.items, file });
    const diff = try std.fmt.allocPrint(
        in.arena,
        "SELECT count(*) AS n FROM ((({s}) EXCEPT ALL ({s})) UNION ALL (({s}) EXCEPT ALL ({s})))",
        .{ written, sql, sql, written },
    );
    const t = duck(in, diff) catch |err| return refusedReason(in, err);
    if (t.rows[0][0] != .int or t.rows[0][0].int != 0)
        return fail(in, "{s} rows differ from the oracle's", .{compare.show(in.arena, t.rows[0][0])});
    return null;
}

fn isIntegerType(t: []const u8) bool {
    const names = [_][]const u8{
        "TINYINT",  "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
        "UHUGEINT",
    };
    for (names) |n| if (std.ascii.eqlIgnoreCase(t, n)) return true;
    return false;
}

/// A zpq value as a DuckDB literal of the oracle's column type. DATEs arrive as days since 1970-01-01.
fn sqlLiteral(arena: Allocator, q: *std.ArrayList(u8), v: Val, ty: []const u8) !void {
    var buf: [200]u8 = undefined;
    switch (v) {
        .null => return q.print(arena, "CAST(NULL AS {s})", .{ty}),
        .int => |n| if (std.ascii.eqlIgnoreCase(ty, "DATE"))
            return q.print(arena, "(DATE '1970-01-01' + {d})", .{n})
        else
            return q.print(arena, "CAST({d} AS {s})", .{ n, ty }),
        .dec => |d| return q.print(arena, "CAST('{s}' AS {s})", .{ compare.decText(&buf, d), ty }),
        .float => |f| return q.print(arena, "CAST('{e}' AS {s})", .{ f, ty }),
        .special => |sp| return q.print(arena, "CAST('{s}' AS {s})", .{ sp.token(), ty }),
        .boolean => |b| return q.print(arena, "CAST({} AS {s})", .{ b, ty }),
        .text => |text| {
            try q.appendSlice(arena, "CAST('");
            for (text) |ch| {
                if (ch == '\'') try q.append(arena, '\'');
                try q.append(arena, ch);
            }
            return q.print(arena, "' AS {s})", .{ty});
        },
        .nested => return error.NestedInTiedCut,
    }
}

// ---- goldens: reviewed output, pinned byte for byte (stable text, written files as PyArrow reads them)

fn goldenPath(in: Inputs, spec: Spec) ![]const u8 {
    const name = spec.golden orelse return error.MissingGolden;
    return std.fmt.allocPrint(in.arena, "tools/check/goldens/{s}", .{name});
}

/// What a golden pins: text as printed, or PyArrow's read of Parquet (on stdout or written): its bytes carry the
/// writer's version.
/// Text with the run's own paths (the fixture's, the output's) as `{path}` and `{out}`: a golden holds what zpq
/// said, not where this run's work directory is.
pub fn unpathed(arena: Allocator, text: []const u8, fixture: []const u8, out: []const u8) ![]const u8 {
    var t = text;
    if (out.len != 0) t = try std.mem.replaceOwned(u8, arena, t, out, "{out}");
    if (fixture.len != 0) t = try std.mem.replaceOwned(u8, arena, t, fixture, "{path}");
    return t;
}

fn produced(in: Inputs) ![]const u8 {
    last_refusal = "PyArrow cannot read the output";
    var file = in.path;
    if (file.len != 0) {
        const head = std.Io.Dir.cwd().readFileAlloc(in.io, file, in.arena, .limited(64 << 20)) catch
            return error.PyarrowRejected;
        if (!std.mem.startsWith(u8, head, "PAR1")) return head;
    } else {
        if (!std.mem.startsWith(u8, in.text, "PAR1")) return in.text;
        file = try std.fmt.allocPrint(in.arena, "zig-out/check-stdout-{d}.parquet", .{std.c.getpid()});
        try std.Io.Dir.cwd().writeFile(in.io, .{ .sub_path = file, .data = in.text });
    }
    var argv: std.ArrayList([]const u8) = .empty;
    try argv.appendSlice(in.arena, &.{ in.python, "tools/check/readback.py", file });
    const out = try oracle_mod.capture(in.gpa, in.io, argv.items);
    defer in.gpa.free(out.stderr);
    defer in.gpa.free(out.stdout);
    if (out.code != 0) {
        last_refusal = try in.arena.dupe(u8, std.mem.trim(u8, out.stderr, " \t\r\n"));
        return error.PyarrowRejected;
    }
    return in.arena.dupe(u8, out.stdout);
}

fn modeGolden(in: Inputs, spec: Spec) !Reason {
    const path = try goldenPath(in, spec);
    const got = try unpathed(in.arena, produced(in) catch |err| return refusedReason(in, err), in.fixture, in.path);
    if (in.bless) {
        try std.Io.Dir.cwd().createDirPath(in.io, std.fs.path.dirname(path).?);
        try std.Io.Dir.cwd().writeFile(in.io, .{ .sub_path = path, .data = got });
        in.blessed.?.* += 1;
        return null;
    }
    const want = std.Io.Dir.cwd().readFileAlloc(in.io, path, in.arena, .limited(64 << 20)) catch
        return fail(in, "no golden {s}: review the output and run --bless", .{path});
    if (!std.mem.eql(u8, got, want)) {
        const at = std.mem.indexOfDiff(u8, got, want) orelse 0;
        return fail(in, "differs from {s} at byte {d}: {s}", .{ path, at, got[at..@min(got.len, at + 80)] });
    }
    return null;
}

/// An unordered answer: the golden's rows as a multiset, under the row policy.
fn modeGoldenRows(in: Inputs, spec: Spec) !Reason {
    const path = try goldenPath(in, spec);
    // It reads a golden another case blesses; the run after blessing checks it.
    if (in.bless) return null;
    const want_text = std.Io.Dir.cwd().readFileAlloc(in.io, path, in.arena, .limited(64 << 20)) catch
        return fail(in, "no golden {s}", .{path});
    const head = std.mem.trimStart(u8, want_text, " \n");
    if (head.len != 0 and head[0] != '[' and head[0] != '{') return sameLines(in, spec, in.text, want_text);
    var golden = in;
    golden.text = want_text;
    const want = try zpqRows(golden);
    const got = try zpqRows(in);
    // Both sides are zpq's own text: read every number exactly, so a DECIMAL mean is never rounded through f64.
    const a = try in.arena.alloc([]const Val, got.len);
    for (got, 0..) |row, i| a[i] = try goldenRow(in, spec, row);
    const b = try in.arena.alloc([]const Val, want.len);
    for (want, 0..) |row, i| b[i] = try goldenRow(in, spec, row);
    return compare.sameMultiset(in.arena, a, b);
}

fn goldenRow(in: Inputs, spec: Spec, raw: std.json.ObjectMap) ![]const Val {
    const row = try splitState(in.arena, raw);
    const out = try in.arena.alloc(Val, row.count());
    for (row.keys(), row.values(), 0..) |k, v, i| out[i] = if (indexOf(spec.inexact orelse &.{}, k) != null)
        try inexactVal(try compare.fromZpq(v, .plain))
    else
        try compare.fromZpq(v, .exact);
    return out;
}

fn inexactVal(v: Val) !Val {
    return switch (v) {
        .float => |f| compare.roundFloat(f),
        .int => |n| compare.roundFloat(@floatFromInt(n)),
        else => v,
    };
}

/// CSV and other line answers in any row order: the same header line and the same multiset of lines, `inexact`
/// columns' fields read as floats at the row policy's precision.
fn sameLines(in: Inputs, spec: Spec, got: []const u8, want: []const u8) !Reason {
    var a = try splitLines(in.arena, got);
    var b = try splitLines(in.arena, want);
    if (a.len == 0 or b.len == 0 or !std.mem.eql(u8, a[0], b[0])) return fail(in, "header differs", .{});
    if (a.len != b.len) return fail(in, "zpq={d} lines golden={d}", .{ a.len, b.len });
    var loose: std.ArrayList(usize) = .empty;
    var names = std.mem.splitScalar(u8, a[0], ',');
    var col: usize = 0;
    while (names.next()) |n| : (col += 1) if (indexOf(spec.inexact orelse
        &.{}, n) != null) try loose.append(in.arena, col);
    a = a[1..];
    b = b[1..];
    for ([_][][]const u8{ a, b }) |lines| for (lines) |*line| if (loose.items.len != 0) {
        var fields: std.ArrayList([]const u8) = .empty;
        var it = std.mem.splitScalar(u8, line.*, ',');
        while (it.next()) |f| try fields.append(in.arena, f);
        for (loose.items) |c| if (c < fields.items.len) if (std.fmt.parseFloat(f64, fields.items[c])) |f| {
            fields.items[c] = try std.fmt.allocPrint(in.arena, "{e:.5}", .{f});
        } else |_| {};
        line.* = try std.mem.join(in.arena, ",", fields.items);
    };
    std.mem.sort([]const u8, a, {}, compare.lessText);
    std.mem.sort([]const u8, b, {}, compare.lessText);
    for (a, b) |x, y| if (!std.mem.eql(u8, x, y)) return fail(in, "line {s} vs golden {s}", .{ x, y });
    return null;
}

fn splitLines(arena: Allocator, text: []const u8) ![][]const u8 {
    var out: std.ArrayList([]const u8) = .empty;
    var it = std.mem.splitScalar(u8, std.mem.trimEnd(u8, text, "\n"), '\n');
    while (it.next()) |line| try out.append(arena, line);
    return out.items;
}

/// A written file reads the same through PyArrow and DuckDB, value for value.
fn modeReadersAgree(in: Inputs, _: Spec) !Reason {
    const rb = readback(in, in.path, &.{}) catch |err| return refusedReason(in, err);
    const t = duck(in, try std.fmt.allocPrint(in.arena, "SELECT * FROM '{s}'", .{in.path})) catch |err|
        return refusedReason(in, err);
    const arrow = (try arrowRows(in, rb, t.names, false)) orelse return fail(in, "readers name different columns", .{});
    // PyArrow tags what JSON lacks; DuckDB prints the same values as text (bytes in its BLOB escape form).
    for (arrow) |row| for (row, t.types) |*v, ty| {
        if (v.* != .text or !std.mem.startsWith(u8, v.text, "\x00$")) continue;
        const colon = std.mem.indexOfScalar(u8, v.text, ':') orelse continue;
        const value = v.text[colon + 1 ..];
        v.* = .{ .text = if (std.mem.eql(u8, v.text[0..colon], "\x00$bytes")) blk: {
            if (!std.mem.eql(u8, ty, "BLOB")) continue;
            break :blk try duckBlob(in.arena, value);
        } else value };
    };
    const widths = try in.arena.alloc(u8, t.names.len);
    @memset(widths, 64);
    return corpusRows(in, arrow, t.rows, widths);
}

/// Bytes (as hex) the way DuckDB prints a BLOB: printable ASCII as is, other bytes and \\ ' " as \\xHH.
fn duckBlob(arena: Allocator, hex: []const u8) ![]const u8 {
    const bytes = try arena.alloc(u8, hex.len / 2);
    _ = try std.fmt.hexToBytes(bytes, hex);
    var out: std.ArrayList(u8) = .empty;
    for (bytes) |b| if (b >= 0x20 and b <= 0x7E and b != '\\' and b != '\'' and b != '"')
        try out.append(arena, b)
    else
        try out.print(arena, "\\x{X:0>2}", .{b});
    return out.items;
}

test "BLOB bytes print as DuckDB prints them" {
    var state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer state.deinit();
    try std.testing.expectEqualStrings("\\x5C ~\\x7F\\x1F\\x22\\x27A", try duckBlob(
        state.allocator(),
        "5c207e7f1f222741",
    ));
}

/// DuckDB reads the written file exactly as it reads `sql` over the inputs (value for value, in order), and
/// PyArrow reads it with `types` when they are given.
fn modeWrittenAs(in: Inputs, spec: Spec) !Reason {
    if (spec.types) |want| {
        const rb = readback(in, in.path, &.{}) catch |err| return refusedReason(in, err);
        const got = try std.mem.join(in.arena, "|", rb.types);
        if (!std.mem.eql(u8, got, try std.mem.join(in.arena, "|", want))) return fail(in, "PyArrow types {s}", .{got});
    }
    const written = duck(in, try std.fmt.allocPrint(in.arena, "SELECT * FROM '{s}'", .{in.path})) catch |err|
        return refusedReason(in, err);
    const want = duck(in, try oracleSql(in, spec)) catch |err| return refusedReason(in, err);
    const widths = try in.arena.alloc(u8, written.names.len);
    @memset(widths, 64);
    return corpusRows(in, written.rows, want.rows, widths);
}

/// `--explain` JSON holding `plan`: a planner choice the case's answers depend on, pinned so they test that path.
fn modePlan(in: Inputs, spec: Spec) !Reason {
    if (std.mem.indexOfScalar(u8, std.mem.trim(u8, in.text, "\n"), '\n') != null) return fail(
        in,
        "explain printed several lines",
        .{},
    );
    if (in.path.len != 0) if (std.Io.Dir.cwd().access(in.io, in.path, .{})) |_| return fail(
        in,
        "explain wrote {s}",
        .{in.path},
    ) else |_| {};
    const got = compare.parse(in.arena, in.text) catch return fail(in, "explain is not JSON", .{});
    if (try holds(in.arena, got, spec.holds orelse
        return error.MissingHolds)) |why| return fail(in, "plan {s}", .{why});
    return null;
}

/// Every chunk's pages, OffsetIndex and ColumnIndex agree with the values PyArrow decodes (tools/check/page_index.py).
fn modePageIndex(in: Inputs, _: Spec) !Reason {
    var argv: std.ArrayList([]const u8) = .empty;
    try argv.appendSlice(in.arena, &.{ in.python, "tools/check/page_index.py", in.path, "--require-index", "--quiet" });
    const out = try oracle_mod.capture(in.gpa, in.io, argv.items);
    defer in.gpa.free(out.stderr);
    defer in.gpa.free(out.stdout);
    if (out.code != 0) return fail(in, "{s}", .{std.mem.trim(u8, try std.mem.concat(in.arena, u8, &.{
        out.stdout,
        out.stderr,
    }), " \t\r\n")});
    return null;
}

/// The written file's footer as PyArrow and a raw walk read it (readback.py's `row_groups`) holds `holds`.
fn modeFooter(in: Inputs, spec: Spec) !Reason {
    var argv: std.ArrayList([]const u8) = .empty;
    try argv.appendSlice(in.arena, &.{ in.python, "tools/check/readback.py", in.path });
    const out = try oracle_mod.capture(in.gpa, in.io, argv.items);
    defer in.gpa.free(out.stderr);
    defer in.gpa.free(out.stdout);
    if (out.code != 0) return fail(in, "{s}", .{std.mem.trim(u8, out.stderr, " \t\r\n")});
    const got = try compare.parse(in.arena, try in.arena.dupe(u8, out.stdout));
    if (try holds(in.arena, got, spec.holds orelse
        return error.MissingHolds)) |why| return fail(in, "footer {s}", .{why});
    return null;
}

/// An object holds each key's value; a list holds each listed element somewhere; {"$len": N} is a list's length;
/// {"$list": [...]} is a list element by element; {"$all": X} is a list whose every element holds X; scalars are
/// equal. Returns where `got` falls short.
fn holds(arena: Allocator, got: std.json.Value, want: std.json.Value) !?[]const u8 {
    switch (want) {
        .object => |o| {
            if (o.get("$len")) |n| return if (got == .array and got.array.items.len == try num(n)) null else "length";
            if (o.get("$all")) |each| {
                if (got != .array) return "not a list";
                for (got.array.items, 0..) |g, i| if (try holds(arena, g, each)) |why|
                    return try std.fmt.allocPrint(arena, "[{d}]: {s}", .{ i, why });
                return null;
            }
            if (o.get("$list")) |xs| {
                if (got != .array or got.array.items.len != xs.array.items.len) return "length";
                for (got.array.items, xs.array.items, 0..) |g, x, i| if (try holds(arena, g, x)) |why|
                    return try std.fmt.allocPrint(arena, "[{d}]: {s}", .{ i, why });
                return null;
            }
            if (got != .object) return "not an object";
            var it = o.iterator();
            while (it.next()) |e| {
                const g = got.object.get(e.key_ptr.*) orelse
                    return try std.fmt.allocPrint(arena, "{s} missing", .{e.key_ptr.*});
                if (try holds(arena, g, e.value_ptr.*)) |why| return try std.fmt.allocPrint(arena, "{s}: {s}", .{
                    e.key_ptr.*,
                    why,
                });
            }
            return null;
        },
        .array => |xs| {
            if (got != .array) return "not a list";
            for (xs.items) |x| {
                const found = for (got.array.items) |g| {
                    if (try holds(arena, g, x) == null) break true;
                } else false;
                if (!found) return try std.fmt.allocPrint(arena, "lacks {f}", .{std.json.fmt(x, .{})});
            }
            return null;
        },
        else => return if (compare.jsonEqual(got, want)) null else try std.fmt.allocPrint(
            arena,
            "is {f}",
            .{std.json.fmt(got, .{})},
        ),
    }
}

test "a plan holds only what it names, with the values it names" {
    var state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer state.deinit();
    const a = state.allocator();
    const got =
        try compare.parse(a, try a.dupe(
            u8,
            "{\"strategy\": \"dense\", \"reasons\": [\"small_ndv\", \"x\"], \"ndv\": {\"hi\": 100}, \"passes\": " ++
                "[1], \"count_agg\": null}",
        ));
    const ok =
        [_][]const u8{
            "{\"strategy\": \"dense\", \"reasons\": [\"small_ndv\"], \"ndv\": {\"hi\": 100}, " ++
                "\"passes\": {\"$len\": 1}, \"count_agg\": null}",
            "{}",
            "{\"reasons\": {\"$list\": [\"small_ndv\", \"x\"]}}",
            "{\"passes\": {\"$all\": 1}}",
        };
    for (ok) |w| try std.testing.expect(try holds(a, got, try compare.parse(a, try a.dupe(u8, w))) == null);
    const bad = [_][]const u8{
        "{\"strategy\": \"sketch\"}",
        "{\"reasons\": [\"few\"]}",
        "{\"ndv\": {\"hi\": 99}}",
        "{\"passes\": {\"$len\": 2}}",
        "{\"count_agg\": 0}",
        "{\"winner\": null}",
        "{\"reasons\": {\"$list\": [\"x\", \"small_ndv\"]}}",
        "{\"reasons\": {\"$list\": [\"small_ndv\"]}}",
        "{\"reasons\": {\"$all\": \"x\"}}",
    };
    for (bad) |w| try std.testing.expect(try holds(a, got, try compare.parse(a, try a.dupe(u8, w))) != null);
}

/// The output is JSON (an explanation, whose contents may change with the plan).
fn modeJsonParses(in: Inputs, _: Spec) !Reason {
    _ = compare.parse(in.arena, in.text) catch |err| return fail(in, "not JSON: {s}", .{@errorName(err)});
    return null;
}

/// A `--summary=json` record: of type summary, `rows` as declared, `output_us` within `total_us`, `bytes_out` the
/// bytes printed when the answer went to stdout, and the declared Parquet codec (or none for text).
fn modeSummary(in: Inputs, spec: Spec) !Reason {
    const line = std.mem.trim(u8, in.stderr, " \n");
    const v = (compare.parse(in.arena, line) catch
        return fail(in, "summary is not JSON: {s}", .{line[0..@min(line.len, 120)]})).object;
    if (!std.mem.eql(u8, v.get("type").?.string, "summary")) return fail(in, "not a summary", .{});
    if (spec.row_count) |n| if (try num(v.get("rows").?) != n) return fail(in, "summary rows {d}, want {d}", .{
        try num(v.get("rows").?),
        n,
    });
    const out = try num(v.get("output_us").?);
    if (out < 0 or out > try num(v.get("total_us").?)) return fail(in, "output_us outside total_us", .{});
    if (in.path.len == 0 and
        try num(v.get("bytes_out").?) != in.text.len) return fail(in, "bytes_out differs from stdout", .{});
    if (spec.pruned) {
        const m = (v.get("membership") orelse return fail(in, "summary has no membership", .{})).object;
        if (try num(m.get("bloom_pruned").?) + try num(m.get("dict_pruned").?) == 0) return fail(
            in,
            "no row group pruned by membership",
            .{},
        );
    }
    if (v.get("codec")) |codec| {
        const got: ?[]const u8 = if (codec == .string) codec.string else null;
        const want = spec.codec;
        if ((got == null) != (want == null) or (got != null and
            !std.mem.eql(u8, got.?, want.?))) return fail(in, "summary codec", .{});
    }
    return null;
}

/// stderr is NDJSON records whose types are exactly `names`.
fn modeStderrTypes(in: Inputs, spec: Spec) !Reason {
    const want = spec.names orelse return error.MissingNames;
    var it = std.mem.splitScalar(u8, std.mem.trim(u8, in.stderr, " \n"), '\n');
    var seen: std.ArrayList([]const u8) = .empty;
    while (it.next()) |line| {
        const v = compare.parse(in.arena, line) catch return fail(in, "stderr line is not JSON", .{});
        const t = v.object.get("type").?.string;
        if (indexOf(seen.items, t) == null) try seen.append(in.arena, t);
    }
    if (seen.items.len != want.len) return fail(in, "record types {s}", .{try std.mem.join(in.arena, ",", seen.items)});
    for (want) |w| if (indexOf(seen.items, w) == null) return fail(in, "no {s} record", .{w});
    return null;
}

/// `--explain` prints its JSON and writes no answer, even when `-o` names a file.
fn modeExplainOnly(in: Inputs, _: Spec) !Reason {
    const v = compare.parse(in.arena, in.text) catch return fail(in, "explanation is not JSON", .{});
    if (v != .object or v.object.get("explain") == null) return fail(in, "no explain object", .{});
    if (in.path.len != 0) if (std.Io.Dir.cwd().access(in.io, in.path, .{})) |_| return fail(
        in,
        "an explanation wrote {s}",
        .{in.path},
    ) else |_| {};
    return null;
}

// ---- Bloom filters, checked from parquet-format's BloomFilter.md alone (never through zpq's own reader)

const salt = [8]u32{ 0x47B6137B, 0x44974D91, 0x8824AD5B, 0xA2B7289D, 0x705495C7, 0x2DF1424B, 0x9EFC4947, 0x5C6BFB31 };

/// The spec's block_check.
fn blockCheck(bits: []const u8, h: u64) bool {
    const blocks: u64 = bits.len / 32;
    const i: usize = @intCast(((h >> 32) * blocks) >> 32);
    const x: u32 = @truncate(h);
    for (salt, 0..) |s, k| {
        const w = std.mem.readInt(u32, bits[i * 32 + 4 * k ..][0..4], .little);
        if ((w >> @intCast((x *% s) >> 27)) & 1 == 0) return false;
    }
    return true;
}

/// The PLAIN bytes a writer hashes for a stored value (a BYTE_ARRAY without its length prefix).
fn plainBytes(arena: Allocator, physical: []const u8, v: std.json.Value) ![]const u8 {
    if (v == .object) {
        const hex = v.object.get("$bytes").?.string;
        const out = try arena.alloc(u8, hex.len / 2);
        _ = try std.fmt.hexToBytes(out, hex);
        return out;
    }
    const out = try arena.alloc(u8, 8);
    if (std.mem.eql(u8, physical, "INT32")) {
        std.mem.writeInt(u32, out[0..4], @truncate(@as(u128, @bitCast(@as(i128, try num(v))))), .little);
        return out[0..4];
    }
    if (std.mem.eql(u8, physical, "INT64")) {
        std.mem.writeInt(u64, out[0..8], @truncate(@as(u128, @bitCast(@as(i128, try num(v))))), .little);
        return out[0..8];
    }
    const f = try floatOfJson(v);
    if (std.mem.eql(u8, physical, "FLOAT")) {
        std.mem.writeInt(u32, out[0..4], @bitCast(@as(f32, @floatCast(f))), .little);
        return out[0..4];
    }
    if (std.mem.eql(u8, physical, "DOUBLE")) {
        std.mem.writeInt(u64, out[0..8], @bitCast(f), .little);
        return out[0..8];
    }
    return error.BadPhysicalType;
}

/// Every stored value of every chunk with a filter tests present in it; no filter admits everything; and the
/// chunks `names` and `bloom_on` select carry a filter in every row group. `types` are the chunks' physical types.
fn modeBloom(in: Inputs, spec: Spec) !Reason {
    const file = if (in.path.len != 0) in.path else in.fixture;
    var argv: std.ArrayList([]const u8) = .empty;
    try argv.appendSlice(in.arena, &.{ in.python, "tools/check/readback.py", file, "--bloom" });
    const out = try oracle_mod.capture(in.gpa, in.io, argv.items);
    defer in.gpa.free(out.stderr);
    defer in.gpa.free(out.stdout);
    if (out.code != 0) return fail(in, "{s}", .{std.mem.trim(u8, out.stderr, " \t\r\n")});
    const groups = try compare.parse(in.arena, try in.arena.dupe(u8, out.stdout));
    var filters: usize = 0;
    var prng = std.Random.DefaultPrng.init(1);
    for (groups.array.items, 0..) |g, ri| for (g.array.items, 0..) |chunk, ci| {
        const o = chunk.object;
        const path = o.get("path").?.string;
        if (spec.types) |want| if (!std.mem.eql(u8, o.get("physical").?.string, want[ci]))
            return fail(in, "rg {d} {s}: physical type {s}", .{ ri, path, o.get("physical").?.string });
        const named = if (spec.names) |names| indexOf(names, path) != null else false;
        const by_rule = if (spec.bloom_on) |rule| std.mem.eql(u8, rule, "dictionary") and
            o.get("dictionary").?.bool else false;
        if (o.get("bits").? == .null) {
            if (named or by_rule) return fail(in, "rg {d} {s}: no filter", .{ ri, path });
            continue;
        }
        if (spec.bloom_on != null and !named and
            !by_rule) return fail(in, "rg {d} {s}: an unwanted filter", .{ ri, path });
        filters += 1;
        const hex = o.get("bits").?.string;
        const bits = try in.arena.alloc(u8, hex.len / 2);
        _ = try std.fmt.hexToBytes(bits, hex);
        if (bits.len == 0 or
            bits.len % 32 != 0) return fail(in, "rg {d} {s}: {d}-byte bitset", .{ ri, path, bits.len });
        const physical = o.get("physical").?.string;
        for (o.get("values").?.array.items) |v| {
            const h = std.hash.XxHash64.hash(0, try plainBytes(in.arena, physical, v));
            if (!blockCheck(bits, h)) return fail(in, "rg {d} {s}: a stored value tests absent", .{ ri, path });
        }
        // A filter that says "maybe" to everything would pass the loop above.
        var hits: usize = 0;
        for (0..200) |_| hits += @intFromBool(blockCheck(bits, prng.random().int(u64)));
        if (hits == 200) return fail(in, "rg {d} {s}: the filter admits everything", .{ ri, path });
    };
    if (filters == 0 and spec.bloom_on == null) return fail(in, "no Bloom filters written", .{});
    return null;
}

test "the Bloom block check finds what the spec's insert put there" {
    var bits: [64]u8 = @splat(0);
    const h = std.hash.XxHash64.hash(0, "parquet");
    // block_insert, from the spec
    const i: usize = @intCast(((h >> 32) * 2) >> 32);
    const x: u32 = @truncate(h);
    for (salt, 0..) |s, k| {
        const at = bits[i * 32 + 4 * k ..][0..4];
        std.mem.writeInt(
            u32,
            at,
            std.mem.readInt(u32, at, .little) | (@as(u32, 1) << @intCast((x *% s) >> 27)),
            .little,
        );
    }
    try std.testing.expect(blockCheck(&bits, h));
    try std.testing.expect(!blockCheck(&bits, std.hash.XxHash64.hash(0, "absent")));
    try std.testing.expectEqual(@as(u64, 0x3C9D29275C52E429), std.hash.XxHash64.hash(0, "parquet"));
}

test "zpq's \"NaN\" is the float in a float column and text in a text column" {
    var state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer state.deinit();
    var in: Inputs = undefined;
    in.arena = state.allocator();
    const row = (try compare.parse(in.arena, try in.arena.dupe(u8, "{\"x\": \"NaN\", \"s\": \"NaN\"}"))).object;
    const got = (try zpqByName(in, row, &.{ "x", "s" }, &.{ .plain, .plain }, &.{ "DOUBLE", "VARCHAR" })).?;
    try std.testing.expect(got[0] == .special and got[0].special == .nan);
    try std.testing.expect(got[1] == .text);
}
