//! `SELECT ... UNION [ALL] | INTERSECT | EXCEPT SELECT ...` for the CLI. Each branch runs as an ordinary query into a
//! Parquet file in a scratch directory, a pass over it names its columns `c0`, `c1`, ... and gives them the types the
//! two sides of the operator share, and the operator is an ordinary query over those files: UNION ALL reads them
//! together, UNION is a DISTINCT, INTERSECT and EXCEPT group the rows of both sides by every column and keep the groups
//! that have rows on the sides the operator asks for. The query's own ORDER BY / LIMIT / OFFSET, output format and
//! flags belong to the last query, which also gives the columns back the first branch's names.
//!
//! The branches' types are unified as DuckDB does for the combinations zpq knows (integers of any width are BIGINT,
//! integers with a DOUBLE are DOUBLE); any other difference between two branches' column types is refused by name.
const std = @import("std");
const zpq = @import("zpq");
const sql_setops = zpq.core.expr.sql_setops;
const metadata = zpq.core.parquet.metadata;
const schema = zpq.core.schema;
const schema_tree = zpq.core.parquet.schema_tree;
const local_fs = zpq.local_fs;

/// How the combination runs one query. The CLI supplies both; this file knows nothing of its options.
pub const Stager = struct {
    ptr: *anyopaque,
    /// Run `sql` and write its rows to the Parquet file `out`. `leaf` is true for a branch of the user's query, whose
    /// inputs and CSV options are theirs, and false for a query over the scratch files.
    stage: *const fn (ptr: *anyopaque, sql: []const u8, out: []const u8, leaf: bool) anyerror!void,
    /// Run `sql` as the user's query: their output, format and flags.
    finish: *const fn (ptr: *anyopaque, sql: []const u8) anyerror!void,
    /// A diagnostic for the user, as the SQL frontend prints its own.
    say: *const fn (message: []const u8) void,
};

pub const Error = error{
    /// Two branches name a different number of columns.
    SetOperationColumns,
    /// Two branches' columns have types zpq has no common-type rule for.
    SetOperationTypes,
    /// Two branches' columns of a type zpq cannot rewrite are nullable in only one of them.
    SetOperationNullability,
    /// The scratch directory's path cannot be written into a query.
    BadArgs,
};

const Kind = union(enum) {
    int,
    double,
    varchar,
    date,
    /// TIMESTAMP in microseconds, not UTC-adjusted: the one timestamp zpq casts to itself.
    timestamp,
    decimal: struct { precision: i32, scale: i32 },
    /// A column zpq does not rewrite (BOOLEAN, FLOAT, other timestamps, ...): carried as it was written, so two
    /// branches must agree on its signature and on its nullability.
    plain: []const u8,

    fn eql(a: Kind, b: Kind) bool {
        if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
        return switch (a) {
            .decimal => |x| x.precision == b.decimal.precision and x.scale == b.decimal.scale,
            .plain => |x| std.mem.eql(u8, x, b.plain),
            else => true,
        };
    }
};

const Col = struct { name: []const u8, kind: Kind, optional: bool };

/// Rows in one or more Parquet files, all with the columns `cols`.
const Dataset = struct {
    files: []const []const u8,
    cols: []const Col,
    /// The files already hold `c0`, `c1`, ... in their final kinds, castable ones nullable.
    prepared: bool,
};

const Run = struct {
    arena: std.mem.Allocator,
    dir: []const u8,
    stager: Stager,
    leaves: []const []const u8,
    next_file: usize = 0,
    /// The first branch's own column names: what the combination's result is called.
    names: []const []const u8 = &.{},

    fn path(self: *Run, comptime stem: []const u8) ![]const u8 {
        self.next_file += 1;
        return std.fmt.allocPrint(self.arena, "{s}/" ++ stem ++ "{d}.parquet", .{ self.dir, self.next_file });
    }

    fn evalNode(self: *Run, node: *const sql_setops.Node) anyerror!Dataset {
        switch (node.*) {
            .leaf => |i| {
                const out = try self.path("raw");
                try self.stager.stage(self.stager.ptr, self.leaves[i], out, true);
                const cols = try readColumns(self.arena, out);
                if (i == 0) {
                    const names = try self.arena.alloc([]const u8, cols.len);
                    for (cols, names) |c, *n| n.* = c.name;
                    self.names = names;
                }
                const files = try self.arena.alloc([]const u8, 1);
                files[0] = out;
                return .{ .files = files, .cols = cols, .prepared = false };
            },
            .bin => |b| {
                const left = try self.evalNode(b.left);
                const right = try self.evalNode(b.right);
                if (left.cols.len != right.cols.len) {
                    self.say("sql: the two sides of a set operation have {d} and {d} columns\n", .{ left.cols.len, right.cols.len });
                    return error.SetOperationColumns;
                }
                const kinds = try self.unify(left.cols, right.cols);
                switch (b.op) {
                    .union_all => {
                        const l = try self.prepare(left, kinds, null);
                        const r = try self.prepare(right, kinds, null);
                        const files = try std.mem.concat(self.arena, []const u8, &.{ l.files, r.files });
                        return .{ .files = files, .cols = l.cols, .prepared = true };
                    },
                    .union_distinct => {
                        const l = try self.prepare(left, kinds, null);
                        const r = try self.prepare(right, kinds, null);
                        const source = try self.glob(try std.mem.concat(self.arena, []const u8, &.{ l.files, r.files }));
                        const out = try self.path("u");
                        try self.stager.stage(self.stager.ptr, try self.format("SELECT DISTINCT {s} FROM '{s}'", .{ try self.columnList(kinds.len), source }), out, false);
                        return self.single(out, l.cols);
                    },
                    .intersect, .except => {
                        const l = try self.prepare(left, kinds, 0);
                        const r = try self.prepare(right, kinds, 1);
                        const source = try self.glob(try std.mem.concat(self.arena, []const u8, &.{ l.files, r.files }));
                        const keep = if (b.op == .intersect) "min(zpq_side) = 0 AND max(zpq_side) = 1" else "max(zpq_side) = 0";
                        const list = try self.columnList(kinds.len);
                        const out = try self.path("s");
                        try self.stager.stage(self.stager.ptr, try self.format("SELECT {s} FROM '{s}' GROUP BY {s} HAVING {s}", .{ list, source, list, keep }), out, false);
                        return self.single(out, l.cols);
                    },
                }
            },
        }
    }

    /// The result of a combining query: one file named `c0`, `c1`, ..., whose nullability is the data's.
    fn single(self: *Run, out: []const u8, cols: []const Col) !Dataset {
        const files = try self.arena.alloc([]const u8, 1);
        files[0] = out;
        const written = try readColumns(self.arena, out);
        // The kinds are the operands' (a writer may say an INT64 where a cast said BIGINT, and the reverse).
        const merged = try self.arena.alloc(Col, cols.len);
        for (merged, cols, written) |*m, c, w| m.* = .{ .name = w.name, .kind = c.kind, .optional = w.optional };
        return .{ .files = files, .cols = merged, .prepared = false };
    }

    fn say(self: *Run, comptime fmt: []const u8, args: anytype) void {
        var buf: [512]u8 = undefined;
        const text = std.fmt.bufPrint(&buf, fmt, args) catch return;
        self.stager.say(text);
    }

    fn format(self: *Run, comptime fmt: []const u8, args: anytype) ![]const u8 {
        return std.fmt.allocPrint(self.arena, fmt, args);
    }

    /// `"c0", "c1", ...`
    fn columnList(self: *Run, n: usize) ![]const u8 {
        var out: std.ArrayList(u8) = .empty;
        for (0..n) |i| {
            if (i > 0) try out.appendSlice(self.arena, ", ");
            try out.print(self.arena, "c{d}", .{i});
        }
        return out.items;
    }

    /// The kinds the two sides share, or the refusal.
    fn unify(self: *Run, a: []const Col, b: []const Col) ![]const Kind {
        const out = try self.arena.alloc(Kind, a.len);
        for (a, b, out, 0..) |x, y, *k, i| {
            if (x.kind.eql(y.kind)) {
                k.* = x.kind;
                if (x.kind == .plain and x.optional != y.optional) {
                    self.say("sql: column {d} of the set operation is {s} on one side and {s} on the other; zpq cannot rewrite a {s} column to match\n", .{ i + 1, nullability(x.optional), nullability(y.optional), x.kind.plain });
                    return error.SetOperationNullability;
                }
            } else if ((x.kind == .int and y.kind == .double) or (x.kind == .double and y.kind == .int)) {
                k.* = .double;
            } else {
                self.say("sql: column {d} of the set operation has different types on its two sides (zpq combines equal types, integers of any width, and an integer with a DOUBLE)\n", .{i + 1});
                return error.SetOperationTypes;
            }
        }
        return out;
    }

    /// `ds` with every column `c{j}` of kind `kinds[j]`, castable ones nullable (the files of one scan must agree on
    /// it), and `zpq_side` after them when `tag` is given. A pass per file, unless the files already are that.
    fn prepare(self: *Run, ds: Dataset, kinds: []const Kind, tag: ?i64) !Dataset {
        var same = ds.prepared and tag == null;
        if (same) for (ds.cols, kinds) |c, k| if (!c.kind.eql(k)) {
            same = false;
        };
        if (same) return ds;
        var select: std.ArrayList(u8) = .empty;
        for (ds.cols, kinds, 0..) |c, k, j| {
            if (j > 0) try select.appendSlice(self.arena, ", ");
            const source = if (ds.prepared) try self.format("c{d}", .{j}) else try quoted(self.arena, c.name);
            switch (k) {
                .int => try select.print(self.arena, "TRY_CAST({s} AS BIGINT)", .{source}),
                .double => try select.print(self.arena, "TRY_CAST({s} AS DOUBLE)", .{source}),
                .varchar => try select.print(self.arena, "TRY_CAST({s} AS VARCHAR)", .{source}),
                .date => try select.print(self.arena, "TRY_CAST({s} AS DATE)", .{source}),
                .timestamp => try select.print(self.arena, "TRY_CAST({s} AS TIMESTAMP)", .{source}),
                .decimal => |d| try select.print(self.arena, "TRY_CAST({s} AS DECIMAL({d},{d}))", .{ source, d.precision, d.scale }),
                .plain => try select.appendSlice(self.arena, source),
            }
            try select.print(self.arena, " AS c{d}", .{j});
        }
        if (tag) |t| try select.print(self.arena, ", {d} AS zpq_side", .{t});
        const cols = try self.arena.alloc(Col, ds.cols.len);
        for (cols, ds.cols, kinds, 0..) |*c, was, k, j| c.* = .{
            .name = try self.format("c{d}", .{j}),
            .kind = k,
            .optional = switch (k) {
                .plain => was.optional,
                else => true,
            },
        };
        const files = try self.arena.alloc([]const u8, ds.files.len);
        for (ds.files, files) |f, *out| {
            out.* = try self.path("p");
            try self.stager.stage(self.stager.ptr, try self.format("SELECT {s} FROM '{s}'", .{ select.items, f }), out.*, false);
        }
        return .{ .files = files, .cols = cols, .prepared = true };
    }

    /// One path a query can name for `files`: the file itself, or a pattern over the files renamed to share a prefix.
    fn glob(self: *Run, files: []const []const u8) ![]const u8 {
        if (files.len == 1) return files[0];
        self.next_file += 1;
        const id = self.next_file;
        for (files, 0..) |f, i| {
            const to = try self.format("{s}/g{d}_{d}.parquet", .{ self.dir, id, i });
            try local_fs.rename(f, to);
            @constCast(files)[i] = to;
        }
        return self.format("{s}/g{d}_*.parquet", .{ self.dir, id });
    }
};

fn nullability(optional: bool) []const u8 {
    return if (optional) "nullable" else "NOT NULL";
}

/// `name` as a double-quoted identifier.
fn quoted(arena: std.mem.Allocator, name: []const u8) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    try out.append(arena, '"');
    for (name) |ch| {
        if (ch == '"') try out.append(arena, '"');
        try out.append(arena, ch);
    }
    try out.append(arena, '"');
    return out.items;
}

/// The columns of the Parquet file at `path`.
fn readColumns(arena: std.mem.Allocator, path: []const u8) ![]const Col {
    const m = try local_fs.mapFile(path);
    defer m.unmap();
    const meta = try metadata.open(arena, m.bytes);
    const tree = try schema_tree.SchemaTree.build(arena, meta.schema.items);
    const cols = try arena.alloc(Col, tree.leaves.len);
    for (tree.leaves, cols) |leaf, *c| c.* = .{
        .name = try arena.dupe(u8, leaf.path[leaf.path.len - 1]),
        .kind = try classify(arena, leaf),
        .optional = leaf.repetition != .REQUIRED,
    };
    return cols;
}

/// A column zpq does not rewrite, by its physical type and whatever annotates it, so two that differ in either are two kinds.
fn signature(arena: std.mem.Allocator, leaf: schema_tree.PrimitiveNode) ![]const u8 {
    const logical: []const u8 = if (leaf.logical_type) |lt| @tagName(lt) else if (leaf.converted_type) |ct| @tagName(ct) else "";
    return std.fmt.allocPrint(arena, "{s}/{s}", .{ @tagName(leaf.type), logical });
}

fn classify(arena: std.mem.Allocator, leaf: schema_tree.PrimitiveNode) !Kind {
    if (leaf.path.len != 1 or leaf.max_rep > 0) return .{ .plain = "nested" };
    if (leaf.logical_type) |lt| switch (lt) {
        .STRING => if (leaf.type == .BYTE_ARRAY) return .varchar,
        .DATE => return .date,
        .DECIMAL => |d| return .{ .decimal = .{ .precision = d.precision, .scale = d.scale } },
        .INTEGER => |i| if (i.isSigned) return .int else return .{ .plain = try std.fmt.allocPrint(arena, "UINT{d}", .{i.bitWidth}) },
        .TIMESTAMP => |t| {
            const unit = @tagName(t.unit);
            if (!t.isAdjustedToUTC and std.mem.eql(u8, unit, "MICROS")) return .timestamp;
            return .{ .plain = try std.fmt.allocPrint(arena, "TIMESTAMP({s},{})", .{ unit, t.isAdjustedToUTC }) };
        },
        else => {},
    } else if (leaf.converted_type) |ct| switch (ct) {
        .UTF8 => return .varchar,
        .DATE => return .date,
        .DECIMAL => return .{ .decimal = .{ .precision = leaf.precision orelse 0, .scale = leaf.scale orelse 0 } },
        .INT_8, .INT_16, .INT_32, .INT_64 => return .int,
        .UINT_8, .UINT_16, .UINT_32, .UINT_64 => return .{ .plain = try std.fmt.allocPrint(arena, "{s}", .{@tagName(ct)}) },
        else => {},
    };
    return switch (leaf.type) {
        .INT32, .INT64 => if (leaf.logical_type == null and leaf.converted_type == null) .int else .{ .plain = try signature(arena, leaf) },
        .DOUBLE => .double,
        else => .{ .plain = try signature(arena, leaf) },
    };
}

/// Where the scratch directory goes: in `dir_hint` (the output's directory, whose filesystem has room for the result) or
/// the system's temporary one, but never in a directory a branch reads from: its files would sit among the inputs.
fn scratchParent(leaves: []const []const u8, dir_hint: ?[]const u8) []const u8 {
    const preferred = dir_hint orelse "/tmp";
    if (!readsFrom(leaves, preferred)) return preferred;
    return if (!readsFrom(leaves, "/tmp")) "/tmp" else preferred;
}

/// Whether a quoted path in the branches (a table, a glob, a join's other table) is in the directory `dir`.
fn readsFrom(leaves: []const []const u8, dir: []const u8) bool {
    for (leaves) |sql| {
        var i: usize = 0;
        while (i < sql.len) {
            if (sql[i] != '\'') {
                i += 1;
                continue;
            }
            const end = sql_setops.quotedEnd(sql, i);
            const text = sql[i + 1 .. @max(i + 1, end -| 1)];
            i = end;
            if (std.mem.indexOfScalar(u8, text, '/') == null and !std.mem.endsWith(u8, text, ".parquet") and
                !std.mem.endsWith(u8, text, ".csv") and std.mem.indexOfScalar(u8, text, '*') == null) continue;
            const in_dir = std.fs.path.dirname(text) orelse ".";
            if (local_fs.sameFile(dir, in_dir) catch false) return true;
        }
    }
    return false;
}

/// Run the combination `compound` writes and give the result to `stager.finish`. `dir_hint` is a directory on the
/// filesystem the result goes to (or null for the system's temporary one), where the scratch directory is made.
pub fn run(
    gpa: std.mem.Allocator,
    io: std.Io,
    compound: sql_setops.Compound,
    dir_hint: ?[]const u8,
    stager: Stager,
) !void {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const parent = scratchParent(compound.leaves, dir_hint);
    const pid: u32 = @bitCast(std.posix.system.getpid());
    const dir = try std.fmt.allocPrint(arena, "{s}/.zpq-setop-{d}", .{ std.mem.trimEnd(u8, parent, "/"), pid });
    // The directory's name goes into queries as a quoted path with a wildcard.
    if (std.mem.indexOfAny(u8, dir, "'*\"") != null) {
        stager.say("sql: set operations keep their scratch files beside the output; its directory name may not contain a quote or a *\n");
        return error.BadArgs;
    }
    try std.Io.Dir.cwd().createDirPath(io, dir);
    defer std.Io.Dir.cwd().deleteTree(io, dir) catch {};

    var state: Run = .{ .arena = arena, .dir = dir, .stager = stager, .leaves = compound.leaves };
    var result = try state.evalNode(compound.root);
    // A single branch (one grouping set) has not been through the pass that names its columns `c0`, `c1`, ...
    if (!result.prepared) {
        const kinds = try arena.alloc(Kind, result.cols.len);
        for (result.cols, kinds) |c, *k| k.* = c.kind;
        result = try state.prepare(result, kinds, null);
    }
    const source = try state.glob(result.files);

    var select: std.ArrayList(u8) = .empty;
    for (state.names, 0..) |n, i| {
        if (i > 0) try select.appendSlice(arena, ", ");
        try select.print(arena, "c{d} AS {s}", .{ i, try quoted(arena, n) });
    }
    const sql = try std.fmt.allocPrint(arena, "SELECT {s} FROM '{s}' {s}", .{ select.items, source, compound.tail });
    try stager.finish(stager.ptr, sql);
}

fn silent(_: []const u8) void {}

test "two branches' column types unify as DuckDB does, or are refused by name" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    var state: Run = .{
        .arena = arena_state.allocator(),
        .dir = "",
        .stager = .{ .ptr = undefined, .stage = undefined, .finish = undefined, .say = silent },
        .leaves = &.{},
    };
    const int_col = [_]Col{.{ .name = "a", .kind = .int, .optional = true }};
    const double_col = [_]Col{.{ .name = "b", .kind = .double, .optional = false }};
    const text_col = [_]Col{.{ .name = "c", .kind = .varchar, .optional = true }};
    try std.testing.expect((try state.unify(&int_col, &int_col))[0] == .int);
    try std.testing.expect((try state.unify(&int_col, &double_col))[0] == .double);
    try std.testing.expect((try state.unify(&double_col, &int_col))[0] == .double);
    try std.testing.expectError(error.SetOperationTypes, state.unify(&int_col, &text_col));
    const dec_a = [_]Col{.{ .name = "d", .kind = .{ .decimal = .{ .precision = 9, .scale = 2 } }, .optional = true }};
    const dec_b = [_]Col{.{ .name = "d", .kind = .{ .decimal = .{ .precision = 18, .scale = 6 } }, .optional = true }};
    try std.testing.expect((try state.unify(&dec_a, &dec_a))[0] == .decimal);
    try std.testing.expectError(error.SetOperationTypes, state.unify(&dec_a, &dec_b));
    // A column zpq cannot rewrite must be nullable on both sides or neither.
    const flag_null = [_]Col{.{ .name = "f", .kind = .{ .plain = "BOOLEAN" }, .optional = true }};
    const flag_req = [_]Col{.{ .name = "f", .kind = .{ .plain = "BOOLEAN" }, .optional = false }};
    try std.testing.expect((try state.unify(&flag_null, &flag_null))[0] == .plain);
    try std.testing.expectError(error.SetOperationNullability, state.unify(&flag_null, &flag_req));
}

test "a column name is quoted as an identifier" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    try std.testing.expectEqualStrings("\"count(*)\"", try quoted(a, "count(*)"));
    try std.testing.expectEqualStrings("\"say \"\"hi\"\"\"", try quoted(a, "say \"hi\""));
}

test "scratch files do not go in a directory a branch reads from" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [256]u8 = undefined;
    const here = try std.fmt.bufPrint(&buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    var q1: [320]u8 = undefined;
    const reads_here = try std.fmt.bufPrint(&q1, "SELECT x FROM '{s}/in.parquet' WHERE y > 1", .{here});
    try std.testing.expectEqualStrings("/tmp", scratchParent(&.{reads_here}, here));
    var q2: [320]u8 = undefined;
    const reads_glob = try std.fmt.bufPrint(&q2, "SELECT x FROM '{s}/*.parquet'", .{here});
    try std.testing.expectEqualStrings("/tmp", scratchParent(&.{ "SELECT 1 FROM 'elsewhere/a.parquet'", reads_glob }, here));
    // Another directory, or a string that is no path, leaves it where the output is.
    try std.testing.expectEqualStrings(here, scratchParent(&.{"SELECT x FROM 'elsewhere/in.parquet' WHERE s = 'a'"}, here));
    try std.testing.expectEqualStrings("/tmp", scratchParent(&.{"SELECT x FROM 'a.parquet'"}, null));
}
