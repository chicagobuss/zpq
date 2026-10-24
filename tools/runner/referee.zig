//! Differential referee. Cases and known divergences are JSON. zpq runs in-process.
//! DuckDB is the CLI. Pyarrow is the strict reader for files zpq writes.
//!
//!   referee --cases tools/runner/cases/filter_projection.json \
//!           --cases tools/runner/cases/sql_pred.json \
//!           --divergences tests/known_divergences.json \
//!           [--duckdb PATH] [--pyarrow PATH] [--limit N]

const std = @import("std");
const zpq = @import("zpq");

const engine = zpq.engine;

const usage =
    \\referee --cases CASES.json --divergences DIVERGENCES.json [--duckdb PATH] [--pyarrow PATH] [--work DIR] [--limit N]
    \\
;

const Case = struct {
    id: []const u8,
    kind: []const u8,
    fixture_name: []const u8,
    filter: ?[]const u8 = null,
    where: ?[]const u8 = null,
    columns: ?[]const []const u8 = null,
    select_z: ?[]const u8 = null,
    select_d: ?[]const u8 = null,
    sql: ?[]const u8 = null,
    jobs: []const usize = &.{},
    aggregate: ?[]const u8 = null,
    group_by: ?[]const u8 = null,
    column_order: ?[]const u8 = null,
    duck: ?[]const u8 = null,
    parquet: bool = false,
    mean_scale: ?i64 = null,
    mean_sum: ?[]const u8 = null,
    mean_count: ?[]const u8 = null,
    mean_alias: ?[]const u8 = null,
    spill_groups: ?[]const u8 = null,
    distinct_pairs: ?[]const u8 = null,
    format: ?[]const u8 = null,
    expect: ?[]const u8 = null,
    epoch: bool = false,
    scan_all: bool = false,
};

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var case_files: std.ArrayList([]const u8) = .empty;
    var divergences_path: ?[]const u8 = null;
    var duckdb: []const u8 = "";
    var pyarrow: []const u8 = ".venv/bin/python";
    var work: []const u8 = "zig-out/referee-work";
    var limit: ?usize = null;

    var iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = iter.next();
    while (iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            std.debug.print("{s}", .{usage});
            return;
        } else if (std.mem.eql(u8, arg, "--cases")) {
            try case_files.append(arena, iter.next() orelse return error.BadArgs);
        } else if (std.mem.eql(u8, arg, "--divergences")) {
            divergences_path = iter.next() orelse return error.BadArgs;
        } else if (std.mem.eql(u8, arg, "--duckdb")) {
            duckdb = iter.next() orelse return error.BadArgs;
        } else if (std.mem.eql(u8, arg, "--pyarrow")) {
            pyarrow = iter.next() orelse return error.BadArgs;
        } else if (std.mem.eql(u8, arg, "--work")) {
            work = iter.next() orelse return error.BadArgs;
        } else if (std.mem.eql(u8, arg, "--limit")) {
            const text = iter.next() orelse return error.BadArgs;
            limit = try std.fmt.parseInt(usize, text, 10);
        } else return error.BadArgs;
    }
    if (case_files.items.len == 0) return error.BadArgs;
    const divergences_file = divergences_path orelse return error.BadArgs;
    if (duckdb.len == 0) {
        const home = std.mem.span(std.c.getenv("HOME") orelse return error.BadArgs);
        duckdb = try std.fmt.allocPrint(arena, "{s}/.cache/zpq-duckdb2/bin-2.0.0/duckdb", .{home});
    }

    const div_text = try std.Io.Dir.cwd().readFileAlloc(io, divergences_file, arena, .limited(1 << 20));
    var div_parsed = try std.json.parseFromSlice(std.json.Value, arena, div_text, .{});
    defer div_parsed.deinit();

    var known = std.StringHashMap(void).init(arena);
    for (div_parsed.value.object.get("records").?.array.items) |rec| {
        try known.put(rec.object.get("id").?.string, {});
    }

    try std.Io.Dir.cwd().createDirPath(io, work);
    const ctx: engine.Context = .{ .gpa = gpa, .env = init.minimal.environ, .io = io };
    zpq.core.expr.sql_parser.diagnostic_fn = swallowDiagnostic;
    defer zpq.core.expr.sql_parser.diagnostic_fn = null;
    var matched: usize = 0;
    var expected: usize = 0;
    var failed: usize = 0;
    var refused: usize = 0;
    var total: usize = 0;
    for (case_files.items, 0..) |cases_file, file_i| {
        const outcome = try runFile(gpa, arena, io, ctx, duckdb, pyarrow, work, file_i, cases_file, &known, limit, &total);
        matched += outcome.matched;
        expected += outcome.expected;
        failed += outcome.failed;
        refused += outcome.refused;
        if (limit) |n| if (total >= n) break;
    }

    std.debug.print("referee: {d}/{d} checks match\n", .{ matched, total });
    std.debug.print("referee: expected {d}\n", .{expected});
    std.debug.print("referee: refusals {d}\n", .{refused});
    std.debug.print("referee: divergences {d}\n", .{known.count()});
    if (failed != 0) std.process.exit(1);
}

fn swallowDiagnostic(_: []const u8) void {}

const Counts = struct { matched: usize = 0, expected: usize = 0, failed: usize = 0, refused: usize = 0 };

fn runFile(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    io: std.Io,
    ctx: engine.Context,
    duckdb: []const u8,
    pyarrow: []const u8,
    work: []const u8,
    file_i: usize,
    cases_file: []const u8,
    known: *std.StringHashMap(void),
    limit: ?usize,
    total: *usize,
) !Counts {
    const cases_text = try std.Io.Dir.cwd().readFileAlloc(io, cases_file, arena, .limited(32 << 20));
    var cases_parsed = try std.json.parseFromSlice(std.json.Value, arena, cases_text, .{});
    defer cases_parsed.deinit();
    const root = cases_parsed.value.object;

    var fixtures = std.StringHashMap([]const u8).init(arena);
    if (root.get("fixture_copy")) |copy| {
        const path = try std.fmt.allocPrint(arena, "{s}/f{d}-main.parquet", .{ work, file_i });
        var buf: std.ArrayList(u8) = .empty;
        try substitute(arena, &buf, copy.string, "{path}", path);
        try duckExec(gpa, io, duckdb, buf.items);
        try fixtures.put("main", path);
    }
    if (root.get("fixtures")) |listed| {
        for (listed.array.items) |item| {
            const name = item.object.get("name").?.string;
            const path = try std.fmt.allocPrint(arena, "{s}/f{d}-{s}.parquet", .{ work, file_i, name });
            var buf: std.ArrayList(u8) = .empty;
            try substitute(arena, &buf, item.object.get("copy").?.string, "{path}", path);
            try duckExec(gpa, io, duckdb, buf.items);
            try fixtures.put(name, path);
        }
    }
    const schema_order: []const []const u8 = if (root.get("schema_order")) |v| try stringList(arena, v) else &.{};
    const out_path = try std.fmt.allocPrint(arena, "{s}/f{d}-out.parquet", .{ work, file_i });
    const diff_path = try std.fmt.allocPrint(arena, "{s}/f{d}-diff.csv", .{ work, file_i });
    const ans_path = try std.fmt.allocPrint(arena, "{s}/f{d}-ans.csv", .{ work, file_i });

    var counts: Counts = .{};
    for (root.get("cases").?.array.items) |raw| {
        if (limit) |n| if (total.* >= n) break;
        const case = try decodeCase(arena, raw);
        const many = std.mem.eql(u8, case.kind, "sql") or std.mem.eql(u8, case.kind, "sql_ordered") or std.mem.eql(u8, case.kind, "sql_rows");
        const checks: usize = if (many) @max(case.jobs.len, 1) else 1;
        total.* += checks;
        const fixture = fixtures.get(case.fixture_name) orelse "";
        if (fixture.len == 0 and !std.mem.eql(u8, case.kind, "sql_ordered") and !std.mem.eql(u8, case.kind, "sql_rows")) return error.MissingFixture;
        const failure = if (std.mem.eql(u8, case.kind, "sql"))
            runSql(gpa, io, ctx, duckdb, fixture, ans_path, case, &counts.refused)
        else if (std.mem.eql(u8, case.kind, "sql_ordered"))
            runSqlOrdered(gpa, io, ctx, duckdb, pyarrow, fixture, ans_path, case)
        else if (std.mem.eql(u8, case.kind, "sql_rows"))
            runSqlRows(gpa, io, ctx, duckdb, pyarrow, fixture, out_path, ans_path, case)
        else if (std.mem.eql(u8, case.kind, "agg") or std.mem.eql(u8, case.kind, "groupby"))
            runAnswer(gpa, io, ctx, duckdb, pyarrow, fixture, out_path, ans_path, case)
        else
            runCase(gpa, io, ctx, duckdb, pyarrow, fixture, out_path, diff_path, schema_order, case);
        const why = failure catch |err| try std.fmt.allocPrint(arena, "{s}", .{@errorName(err)});
        if (why) |text| {
            if (known.contains(case.id)) {
                counts.expected += checks;
                std.debug.print("EXPECTED {s}  {s}\n", .{ case.id, text });
            } else {
                counts.failed += checks;
                std.debug.print("FAIL {s}  {s}\n", .{ case.id, text });
            }
        } else counts.matched += checks;
    }
    return counts;
}

fn decodeCase(arena: std.mem.Allocator, raw: std.json.Value) !Case {
    const obj = raw.object;
    return .{
        .id = obj.get("id").?.string,
        .kind = optionalString(obj, "kind") orelse "write",
        .fixture_name = optionalString(obj, "fixture") orelse "main",
        .filter = optionalString(obj, "filter"),
        .where = optionalString(obj, "where"),
        .columns = if (obj.get("columns")) |v| if (v == .null) null else try stringList(arena, v) else null,
        .select_z = optionalString(obj, "select"),
        .select_d = optionalString(obj, "duckdb_select"),
        .sql = optionalString(obj, "sql"),
        .jobs = if (obj.get("jobs")) |v| try jobList(arena, v) else &.{},
        .aggregate = optionalString(obj, "aggregate"),
        .group_by = optionalString(obj, "group_by"),
        .column_order = optionalString(obj, "column_order"),
        .duck = optionalString(obj, "duck"),
        .parquet = if (obj.get("parquet")) |v| switch (v) {
            .bool => |b| b,
            else => false,
        } else false,
        .mean_scale = if (obj.get("mean_scale")) |v| switch (v) {
            .integer => v.integer,
            else => null,
        } else null,
        .mean_sum = optionalString(obj, "mean_sum"),
        .mean_count = optionalString(obj, "mean_count"),
        .mean_alias = optionalString(obj, "mean_alias"),
        .spill_groups = optionalString(obj, "spill_groups"),
        .distinct_pairs = optionalString(obj, "distinct_pairs"),
        .format = optionalString(obj, "format"),
        .expect = optionalString(obj, "expect"),
        .epoch = if (obj.get("epoch")) |v| switch (v) {
            .bool => |b| b,
            else => false,
        } else false,
        .scan_all = if (obj.get("scan_all")) |v| switch (v) {
            .bool => |b| b,
            else => false,
        } else false,
    };
}

const MemWriter = struct {
    gpa: std.mem.Allocator,
    buf: std.ArrayList(u8) = .empty,

    fn deinit(self: *@This()) void {
        self.buf.deinit(self.gpa);
    }

    pub fn writeAll(self: *@This(), bytes: []const u8) !void {
        try self.buf.appendSlice(self.gpa, bytes);
    }

    pub fn writeByte(self: *@This(), c: u8) !void {
        try self.buf.append(self.gpa, c);
    }

    pub fn print(self: *@This(), comptime fmt: []const u8, args: anytype) !void {
        const text = try std.fmt.allocPrint(self.gpa, fmt, args);
        defer self.gpa.free(text);
        try self.buf.appendSlice(self.gpa, text);
    }
};

fn runAnswer(
    gpa: std.mem.Allocator,
    io: std.Io,
    ctx: engine.Context,
    duckdb: []const u8,
    pyarrow: []const u8,
    fixture: []const u8,
    out_path: []const u8,
    ans_path: []const u8,
    case: Case,
) !?[]const u8 {
    const zpq_path = try std.fmt.allocPrint(gpa, "{s}.json", .{ans_path});
    defer gpa.free(zpq_path);
    const json_text = try aggregateJson(gpa, ctx, fixture, case, null);
    defer gpa.free(json_text);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = zpq_path, .data = json_text });

    if (case.parquet) {
        std.Io.Dir.cwd().deleteFile(io, out_path) catch {};
        const ignored = aggregateJson(gpa, ctx, fixture, case, out_path) catch |err| {
            return try std.fmt.allocPrint(gpa, "zpq {s}", .{@errorName(err)});
        };
        gpa.free(ignored);
        return compareAnswer(gpa, io, pyarrow, duckdb, zpq_path, null, out_path, "parquet", case);
    }

    const sql = try answerSql(gpa, fixture, case);
    defer gpa.free(sql);
    const mode: []const u8 = if (std.mem.eql(u8, case.kind, "groupby")) "group" else "agg";
    return compareAnswer(gpa, io, pyarrow, duckdb, zpq_path, sql, null, mode, case);
}

fn aggregateJson(
    gpa: std.mem.Allocator,
    ctx: engine.Context,
    fixture: []const u8,
    case: Case,
    output: ?[]const u8,
) ![]u8 {
    var result = try engine.runQuery(ctx, .{
        .inputs = &.{fixture},
        .output = output,
        .typed_answer = output != null,
        .filter = case.filter,
        .aggregate = case.aggregate,
        .group_by = case.group_by,
        .column_order = case.column_order,
    });
    defer result.deinit(gpa);
    const answer = switch (result) {
        .aggregate => |*ar| ar,
        .write => return error.NotAggregate,
    };
    const n: usize = if (answer.output.group_rows) |groups|
        groups.len
    else if (answer.output.no_row)
        0
    else
        1;
    var w: MemWriter = .{ .gpa = gpa };
    errdefer w.deinit();
    try engine.writeAggregateText(&w, answer.output, answer.text_kinds, answer.answer_types, n, .json);
    return w.buf.toOwnedSlice(gpa);
}

fn answerSql(gpa: std.mem.Allocator, fixture: []const u8, case: Case) ![]u8 {
    const duck = case.duck orelse return error.MissingSql;
    if (std.mem.eql(u8, case.kind, "groupby")) {
        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(gpa);
        try substitute(gpa, &buf, duck, "{path}", fixture);
        if (case.where) |w| {
            const marker = " GROUP BY ";
            const at = std.mem.indexOf(u8, buf.items, marker) orelse return error.BadDuckSql;
            var out: std.ArrayList(u8) = .empty;
            errdefer out.deinit(gpa);
            try out.appendSlice(gpa, buf.items[0..at]);
            try out.appendSlice(gpa, " WHERE ");
            try out.appendSlice(gpa, w);
            try out.appendSlice(gpa, buf.items[at..]);
            buf.deinit(gpa);
            return out.toOwnedSlice(gpa);
        }
        return buf.toOwnedSlice(gpa);
    }
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(gpa);
    try buf.appendSlice(gpa, "SELECT ");
    try buf.appendSlice(gpa, duck);
    try buf.appendSlice(gpa, " FROM '");
    try buf.appendSlice(gpa, fixture);
    try buf.appendSlice(gpa, "'");
    if (case.where) |w| {
        try buf.appendSlice(gpa, " WHERE ");
        try buf.appendSlice(gpa, w);
    }
    return buf.toOwnedSlice(gpa);
}

fn compareAnswer(
    gpa: std.mem.Allocator,
    io: std.Io,
    pyarrow: []const u8,
    duckdb: []const u8,
    zpq_path: []const u8,
    sql: ?[]const u8,
    parquet: ?[]const u8,
    mode: []const u8,
    case: Case,
) !?[]const u8 {
    var argv: [20][]const u8 = undefined;
    var n: usize = 0;
    argv[n] = pyarrow;
    n += 1;
    argv[n] = "tools/runner/answer_cmp.py";
    n += 1;
    argv[n] = mode;
    n += 1;
    argv[n] = "--zpq";
    n += 1;
    argv[n] = zpq_path;
    n += 1;
    if (sql) |text| {
        argv[n] = "--duckdb";
        n += 1;
        argv[n] = duckdb;
        n += 1;
        argv[n] = "--sql";
        n += 1;
        argv[n] = text;
        n += 1;
    }
    if (parquet) |path| {
        argv[n] = "--parquet";
        n += 1;
        argv[n] = path;
        n += 1;
    }
    var scale_buf: [32]u8 = undefined;
    if (case.mean_scale) |scale| {
        const text = std.fmt.bufPrint(&scale_buf, "{d}", .{scale}) catch return error.Format;
        argv[n] = "--mean-scale";
        n += 1;
        argv[n] = text;
        n += 1;
    }
    if (case.mean_sum) |name| {
        argv[n] = "--mean-sum";
        n += 1;
        argv[n] = name;
        n += 1;
    }
    if (case.mean_count) |name| {
        argv[n] = "--mean-count";
        n += 1;
        argv[n] = name;
        n += 1;
    }
    if (case.mean_alias) |name| {
        argv[n] = "--mean-alias";
        n += 1;
        argv[n] = name;
        n += 1;
    }
    if (case.epoch) {
        argv[n] = "--epoch";
        n += 1;
    }
    const out = try capture(gpa, io, argv[0..n]);
    defer gpa.free(out.stdout);
    defer gpa.free(out.stderr);
    if (out.code != 0) {
        const msg = if (out.stderr.len != 0) out.stderr else out.stdout;
        const line = std.mem.trim(u8, msg, " \t\r\n");
        const start = if (std.mem.lastIndexOfScalar(u8, line, '\n')) |at| at + 1 else 0;
        return try gpa.dupe(u8, line[start..]);
    }
    return null;
}

fn jobList(arena: std.mem.Allocator, v: std.json.Value) ![]const usize {
    const items = v.array.items;
    const out = try arena.alloc(usize, items.len);
    for (items, 0..) |item, i| {
        out[i] = switch (item) {
            .integer => |n| @intCast(n),
            else => return error.BadJobs,
        };
    }
    return out;
}

threadlocal var diag_buf: [8192]u8 = undefined;
threadlocal var diag_len: usize = 0;

fn captureDiag(msg: []const u8) void {
    const room = diag_buf.len -| diag_len;
    const n = @min(msg.len, room);
    if (n == 0) return;
    @memcpy(diag_buf[diag_len..][0..n], msg[0..n]);
    diag_len += n;
}

fn lastDiagLine(gpa: std.mem.Allocator) ![]u8 {
    const text = std.mem.trim(u8, diag_buf[0..diag_len], " \t\r\n");
    const line = if (std.mem.lastIndexOfScalar(u8, text, '\n')) |at|
        std.mem.trim(u8, text[at + 1 ..], " \t\r\n")
    else
        text;
    return gpa.dupe(u8, line);
}

fn runSql(
    gpa: std.mem.Allocator,
    io: std.Io,
    ctx: engine.Context,
    duckdb: []const u8,
    fixture: []const u8,
    ans_path: []const u8,
    case: Case,
    refusals: *usize,
) !?[]const u8 {
    const sql_src = case.sql orelse return error.MissingSql;
    var sql_buf: std.ArrayList(u8) = .empty;
    defer sql_buf.deinit(gpa);
    try substitute(gpa, &sql_buf, sql_src, "{path}", fixture);
    const wrapped = try std.fmt.allocPrint(gpa,
        \\COPY (SELECT c::VARCHAR, CASE WHEN s IS NULL THEN 'NULL' ELSE s::VARCHAR END FROM ({s}) q(c, s)) TO '{s}' (HEADER false)
    , .{ sql_buf.items, ans_path });
    defer gpa.free(wrapped);
    const duck = try capture(gpa, io, &.{ duckdb, "-no-agent", "-c", wrapped });
    defer gpa.free(duck.stdout);
    defer gpa.free(duck.stderr);
    const duck_refused = duck.code != 0;
    const want: []u8 = if (duck_refused) "" else blk: {
        const text = try std.Io.Dir.cwd().readFileAlloc(io, ans_path, gpa, .limited(1 << 20));
        defer gpa.free(text);
        break :blk try stripQuotes(gpa, std.mem.trim(u8, text, " \t\r\n"));
    };
    defer if (want.len != 0 or !duck_refused) gpa.free(want);

    const prev_sql = zpq.core.expr.sql_parser.diagnostic_fn;
    const prev_front = zpq.frontend.diagnostic_fn;
    zpq.core.expr.sql_parser.diagnostic_fn = captureDiag;
    zpq.frontend.diagnostic_fn = captureDiag;
    defer zpq.core.expr.sql_parser.diagnostic_fn = prev_sql;
    defer zpq.frontend.diagnostic_fn = prev_front;

    const jobs: []const usize = if (case.jobs.len == 0) &.{0} else case.jobs;
    var first_refusal: ?[]u8 = null;
    defer if (first_refusal) |msg| gpa.free(msg);
    // A refusal with one stable message is a pass, including when DuckDB can
    // answer. That is the sql_pred gate: zpq may refuse more, and an answer
    // must equal DuckDB. An answer where DuckDB refuses is a failure.
    var answered = false;
    for (jobs) |j| {
        diag_len = 0;
        var diag: zpq.core.diag.Diag = .{};
        const got = queryAggregate(gpa, ctx, sql_buf.items, j, &diag) catch |err| {
            if (answered) return try std.fmt.allocPrint(gpa, "differs across -j", .{});
            if (diag_len == 0) zpq.frontend.queryError(err, &diag);
            const msg = try lastDiagLine(gpa);
            defer gpa.free(msg);
            if (msg.len == 0) return try std.fmt.allocPrint(gpa, "refused without a message ({s})", .{@errorName(err)});
            if (first_refusal) |prev| {
                if (!std.mem.eql(u8, prev, msg)) return try std.fmt.allocPrint(gpa, "differs across -j: {s} vs {s}", .{ prev, msg });
            } else first_refusal = try gpa.dupe(u8, msg);
            continue;
        };
        defer gpa.free(got);
        if (first_refusal != null) return try std.fmt.allocPrint(gpa, "differs across -j", .{});
        answered = true;
        if (duck_refused) return try std.fmt.allocPrint(gpa, "zpq answered {s}; duckdb refuses", .{got});
        if (!std.mem.eql(u8, got, want)) return try std.fmt.allocPrint(gpa, "zpq {s} duck {s}", .{ got, want });
    }
    if (!answered) refusals.* += jobs.len;
    return null;
}

fn stripQuotes(gpa: std.mem.Allocator, text: []const u8) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(gpa);
    for (text) |c| if (c != '"') try buf.append(gpa, c);
    return buf.toOwnedSlice(gpa);
}

fn queryAggregate(gpa: std.mem.Allocator, ctx: engine.Context, sql: []const u8, jobs: usize, diag: *zpq.core.diag.Diag) ![]u8 {
    const pq = try zpq.core.expr.sql_parser.parseSqlQuery(gpa, sql);
    defer pq.deinit(gpa);
    var result = try engine.runQuery(ctx, .{
        .inputs = &.{pq.table_name},
        .aggregate = pq.aggregate_str,
        .filter = pq.filter_str,
        .group_by = pq.group_by_str,
        .select = pq.select_str,
        .select_cols = pq.select_cols,
        .result_order = pq.result_order,
        .parallelism = jobs,
        .diag = diag,
    });
    defer result.deinit(gpa);
    const aggs = switch (result) {
        .aggregate => |*answer| answer.output.aggs,
        .write => return error.NotAggregate,
    };
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(gpa);
    for (aggs, 0..) |item, i| {
        if (i != 0) try buf.append(gpa, ',');
        switch (item.value) {
            .i => |n| {
                var tmp: [64]u8 = undefined;
                const text = std.fmt.bufPrint(&tmp, "{d}", .{n}) catch return error.Format;
                try buf.appendSlice(gpa, text);
            },
            .null_val => try buf.appendSlice(gpa, "NULL"),
            else => return error.UnexpectedAgg,
        }
    }
    return buf.toOwnedSlice(gpa);
}

fn optionalString(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    const v = obj.get(key) orelse return null;
    return switch (v) {
        .string => |s| s,
        else => null,
    };
}

fn stringList(arena: std.mem.Allocator, v: std.json.Value) ![]const []const u8 {
    const items = v.array.items;
    const out = try arena.alloc([]const u8, items.len);
    for (items, 0..) |item, i| out[i] = item.string;
    return out;
}

fn runCase(
    gpa: std.mem.Allocator,
    io: std.Io,
    ctx: engine.Context,
    duckdb: []const u8,
    pyarrow: []const u8,
    fixture: []const u8,
    out_path: []const u8,
    diff_path: []const u8,
    schema_order: []const []const u8,
    case: Case,
) !?[]const u8 {
    std.Io.Dir.cwd().deleteFile(io, out_path) catch {};
    var result = engine.runQuery(ctx, .{
        .inputs = &.{fixture},
        .output = out_path,
        .filter = case.filter,
        .columns = case.columns,
        .select = case.select_z,
    }) catch |err| {
        return try std.fmt.allocPrint(gpa, "zpq {s}", .{@errorName(err)});
    };
    defer result.deinit(gpa);

    const names = try physicalNames(gpa, io, pyarrow, out_path);
    defer gpa.free(names);
    const want_names = try expectedNames(gpa, schema_order, case);
    defer gpa.free(want_names);
    if (!std.mem.eql(u8, names, want_names)) {
        return try std.fmt.allocPrint(gpa, "column order got {s} want {s}", .{ names, want_names });
    }

    const sql = try diffSql(gpa, fixture, out_path, diff_path, case);
    defer gpa.free(sql);
    try duckExec(gpa, io, duckdb, sql);
    const diff = try std.Io.Dir.cwd().readFileAlloc(io, diff_path, gpa, .limited(1 << 20));
    defer gpa.free(diff);
    const trimmed = std.mem.trim(u8, diff, " \t\r\n");
    if (!std.mem.eql(u8, trimmed, "0")) {
        return try std.fmt.allocPrint(gpa, "row mismatch count {s}", .{trimmed});
    }
    return null;
}

fn expectedNames(gpa: std.mem.Allocator, schema_order: []const []const u8, case: Case) ![]u8 {
    if (case.select_z != null) return gpa.dupe(u8, "x");
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(gpa);
    const cols = case.columns;
    for (schema_order) |name| {
        if (cols) |wanted| {
            var keep = false;
            for (wanted) |w| keep = keep or std.mem.eql(u8, w, name);
            if (!keep) continue;
        }
        if (buf.items.len != 0) try buf.append(gpa, ',');
        try buf.appendSlice(gpa, name);
    }
    return buf.toOwnedSlice(gpa);
}

fn physicalNames(gpa: std.mem.Allocator, io: std.Io, pyarrow: []const u8, path: []const u8) ![]u8 {
    const code =
        \\import pyarrow.parquet as pq, sys
        \\print(",".join(pq.read_table(sys.argv[1]).column_names), end="")
    ;
    const out = try capture(gpa, io, &.{ pyarrow, "-c", code, path });
    defer gpa.free(out.stderr);
    if (out.code != 0) {
        defer gpa.free(out.stdout);
        return error.PyarrowRejected;
    }
    return out.stdout;
}

fn diffSql(gpa: std.mem.Allocator, fixture: []const u8, out_path: []const u8, diff_path: []const u8, case: Case) ![]u8 {
    var cols: std.ArrayList(u8) = .empty;
    defer cols.deinit(gpa);
    if (case.select_z != null) {
        try cols.appendSlice(gpa, "x");
    } else if (case.columns) |wanted| {
        for (wanted, 0..) |name, i| {
            if (i != 0) try cols.appendSlice(gpa, ", ");
            try cols.append(gpa, '"');
            try cols.appendSlice(gpa, name);
            try cols.append(gpa, '"');
        }
    } else try cols.appendSlice(gpa, "*");
    var where_buf: std.ArrayList(u8) = .empty;
    defer where_buf.deinit(gpa);
    if (case.where) |w| {
        try where_buf.appendSlice(gpa, " WHERE ");
        try where_buf.appendSlice(gpa, w);
    }
    const projection = if (case.select_d) |expr| expr else cols.items;
    const oracle = try std.fmt.allocPrint(gpa, "SELECT {s} FROM '{s}'{s}", .{
        projection,
        fixture,
        where_buf.items,
    });
    defer gpa.free(oracle);
    const written = try std.fmt.allocPrint(gpa, "SELECT {s} FROM '{s}'", .{ cols.items, out_path });
    defer gpa.free(written);
    return std.fmt.allocPrint(gpa,
        \\COPY (SELECT count(*) FROM ({s} EXCEPT ALL {s} UNION ALL {s} EXCEPT ALL {s})) TO '{s}' (HEADER false)
    , .{ written, oracle, oracle, written, diff_path });
}

fn substOpt(arena: std.mem.Allocator, buf: *std.ArrayList(u8), sql: []const u8, key: []const u8, value: []const u8) !void {
    if (std.mem.indexOf(u8, sql, key) == null) {
        try buf.appendSlice(arena, sql);
        return;
    }
    try substitute(arena, buf, sql, key, value);
}

fn posixEnv(name: [*:0]const u8) ?[]const u8 {
    const v = std.c.getenv(name) orelse return null;
    return std.mem.span(v);
}

extern "c" fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
extern "c" fn unsetenv(name: [*:0]const u8) c_int;

fn setPosixEnv(gpa: std.mem.Allocator, name: [*:0]const u8, value: ?[]const u8) !void {
    if (value) |v| {
        const z = try gpa.allocSentinel(u8, v.len, 0);
        defer gpa.free(z);
        @memcpy(z[0..v.len], v);
        if (setenv(name, z, 1) != 0) return error.SetEnv;
    } else _ = unsetenv(name);
}

fn runSqlOrdered(
    gpa: std.mem.Allocator,
    io: std.Io,
    ctx: engine.Context,
    duckdb: []const u8,
    pyarrow: []const u8,
    fixture: []const u8,
    ans_path: []const u8,
    case: Case,
) !?[]const u8 {
    const spill_prev: ?[]u8 = if (case.spill_groups != null) blk: {
        const cur = posixEnv("ZPQ_SPILL_GROUPS");
        break :blk if (cur) |c| try gpa.dupe(u8, c) else null;
    } else null;
    defer if (spill_prev) |p| gpa.free(p);
    const distinct_prev: ?[]u8 = if (case.distinct_pairs != null) blk: {
        const cur = posixEnv("ZPQ_DISTINCT_PAIRS");
        break :blk if (cur) |c| try gpa.dupe(u8, c) else null;
    } else null;
    defer if (distinct_prev) |p| gpa.free(p);
    if (case.spill_groups) |v| try setPosixEnv(gpa, "ZPQ_SPILL_GROUPS", v);
    defer if (case.spill_groups != null) setPosixEnv(gpa, "ZPQ_SPILL_GROUPS", spill_prev) catch {};
    if (case.distinct_pairs) |v| try setPosixEnv(gpa, "ZPQ_DISTINCT_PAIRS", v);
    defer if (case.distinct_pairs != null) setPosixEnv(gpa, "ZPQ_DISTINCT_PAIRS", distinct_prev) catch {};

    const sql_src = case.sql orelse return error.MissingSql;
    var sql_buf: std.ArrayList(u8) = .empty;
    defer sql_buf.deinit(gpa);
    try substOpt(gpa, &sql_buf, sql_src, "{path}", fixture);
    var duck_buf: std.ArrayList(u8) = .empty;
    defer duck_buf.deinit(gpa);
    try substOpt(gpa, &duck_buf, case.duck orelse sql_src, "{path}", fixture);

    const prev_sql = zpq.core.expr.sql_parser.diagnostic_fn;
    const prev_front = zpq.frontend.diagnostic_fn;
    zpq.core.expr.sql_parser.diagnostic_fn = captureDiag;
    zpq.frontend.diagnostic_fn = captureDiag;
    defer zpq.core.expr.sql_parser.diagnostic_fn = prev_sql;
    defer zpq.frontend.diagnostic_fn = prev_front;

    const zpq_path = try std.fmt.allocPrint(gpa, "{s}.json", .{ans_path});
    defer gpa.free(zpq_path);
    const jobs: []const usize = if (case.jobs.len == 0) &.{0} else case.jobs;
    for (jobs) |j| {
        diag_len = 0;
        const json_text = querySqlJson(gpa, ctx, sql_buf.items, j) catch |err| {
            if (diag_len == 0) {
                var diag: zpq.core.diag.Diag = .{};
                zpq.frontend.queryError(err, &diag);
            }
            const msg = try lastDiagLine(gpa);
            defer gpa.free(msg);
            if (msg.len == 0) return try std.fmt.allocPrint(gpa, "{s}", .{@errorName(err)});
            return try std.fmt.allocPrint(gpa, "{s}", .{msg});
        };
        defer gpa.free(json_text);
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = zpq_path, .data = json_text });
        if (try compareAnswer(gpa, io, pyarrow, duckdb, zpq_path, duck_buf.items, null, "ordered", case)) |why| return why;
    }
    return null;
}

fn runSqlRows(
    gpa: std.mem.Allocator,
    io: std.Io,
    ctx: engine.Context,
    duckdb: []const u8,
    pyarrow: []const u8,
    fixture: []const u8,
    out_path: []const u8,
    ans_path: []const u8,
    case: Case,
) !?[]const u8 {
    const sql_src = case.sql orelse return error.MissingSql;
    var sql_buf: std.ArrayList(u8) = .empty;
    defer sql_buf.deinit(gpa);
    try substOpt(gpa, &sql_buf, sql_src, "{path}", fixture);
    if (std.mem.indexOf(u8, sql_buf.items, "{glob}")) |_| {
        const glob = try rowsGlob(gpa, fixture);
        defer gpa.free(glob);
        var next: std.ArrayList(u8) = .empty;
        defer next.deinit(gpa);
        try substitute(gpa, &next, sql_buf.items, "{glob}", glob);
        sql_buf.clearAndFree(gpa);
        try sql_buf.appendSlice(gpa, next.items);
    }
    var duck_buf: std.ArrayList(u8) = .empty;
    defer duck_buf.deinit(gpa);
    try substOpt(gpa, &duck_buf, case.duck orelse sql_src, "{path}", fixture);
    if (std.mem.indexOf(u8, duck_buf.items, "{glob}")) |_| {
        const glob = try rowsGlob(gpa, fixture);
        defer gpa.free(glob);
        var next: std.ArrayList(u8) = .empty;
        defer next.deinit(gpa);
        try substitute(gpa, &next, duck_buf.items, "{glob}", glob);
        duck_buf.clearAndFree(gpa);
        try duck_buf.appendSlice(gpa, next.items);
    }

    const prev_sql = zpq.core.expr.sql_parser.diagnostic_fn;
    const prev_front = zpq.frontend.diagnostic_fn;
    zpq.core.expr.sql_parser.diagnostic_fn = captureDiag;
    zpq.frontend.diagnostic_fn = captureDiag;
    defer zpq.core.expr.sql_parser.diagnostic_fn = prev_sql;
    defer zpq.frontend.diagnostic_fn = prev_front;

    const parquet = case.format != null and std.mem.eql(u8, case.format.?, "parquet");
    const out = if (parquet) out_path else try std.fmt.allocPrint(gpa, "{s}.jsonl", .{ans_path});
    defer if (!parquet) gpa.free(out);
    const jobs: []const usize = if (case.jobs.len == 0) &.{0} else case.jobs;
    for (jobs) |j| {
        diag_len = 0;
        zpq.local_fs.removeFile(out);
        const wrote = printSqlRows(gpa, ctx, sql_buf.items, j, out, parquet, case.scan_all) catch |err| {
            if (case.expect) |needle| {
                if (diag_len == 0) {
                    var diag: zpq.core.diag.Diag = .{};
                    zpq.frontend.queryError(err, &diag);
                }
                const msg = try lastDiagLine(gpa);
                defer gpa.free(msg);
                if (std.mem.indexOf(u8, msg, needle) != null) return null;
                return try std.fmt.allocPrint(gpa, "refused ({s}), want {s}", .{ msg, needle });
            }
            if (diag_len == 0) {
                var diag: zpq.core.diag.Diag = .{};
                zpq.frontend.queryError(err, &diag);
            }
            const msg = try lastDiagLine(gpa);
            defer gpa.free(msg);
            if (msg.len == 0) return try std.fmt.allocPrint(gpa, "{s}", .{@errorName(err)});
            return try std.fmt.allocPrint(gpa, "{s}", .{msg});
        };
        _ = wrote;
        if (case.expect) |needle| return try std.fmt.allocPrint(gpa, "zpq answered, want refusal {s}", .{needle});
        const why = try compareAnswer(gpa, io, pyarrow, duckdb, out, duck_buf.items, if (parquet) out else null, "rows", case);
        if (why) |text| return text;
    }
    return null;
}

fn ownedInputs(gpa: std.mem.Allocator, ctx: engine.Context, pattern: []const u8) ![]const []const u8 {
    var scratch = std.heap.ArenaAllocator.init(gpa);
    defer scratch.deinit();
    const raw = try zpq.frontend.expandGlob(scratch.allocator(), ctx.io, pattern, ctx.env);
    const inputs = try gpa.alloc([]const u8, raw.len);
    errdefer gpa.free(inputs);
    for (raw, 0..) |p, i| inputs[i] = try gpa.dupe(u8, p);
    return inputs;
}

fn freeInputs(gpa: std.mem.Allocator, inputs: []const []const u8) void {
    for (inputs) |p| gpa.free(p);
    gpa.free(inputs);
}

fn rowsGlob(gpa: std.mem.Allocator, fixture: []const u8) ![]u8 {
    const suffix = "tie_a.parquet";
    if (!std.mem.endsWith(u8, fixture, suffix)) return error.BadGlob;
    return std.fmt.allocPrint(gpa, "{s}tie_*.parquet", .{fixture[0 .. fixture.len - suffix.len]});
}

fn printSqlRows(
    gpa: std.mem.Allocator,
    ctx: engine.Context,
    sql: []const u8,
    jobs: usize,
    out_path: []const u8,
    parquet: bool,
    scan_all: bool,
) !void {
    const pq = try zpq.core.expr.sql_parser.parseSqlQuery(gpa, sql);
    defer pq.deinit(gpa);
    const inputs = try ownedInputs(gpa, ctx, pq.table_name);
    defer freeInputs(gpa, inputs);
    const fd = try zpq.local_fs.createFile(out_path);
    defer zpq.local_fs.close(fd);
    var diag: zpq.core.diag.Diag = .{};
    _ = try engine.runPrintTo(ctx, .{
        .inputs = inputs,
        .filter = pq.filter_str,
        .select = pq.select_str,
        .aggregate = pq.aggregate_str,
        .group_by = pq.group_by_str,
        .select_cols = pq.select_cols,
        .result_order = pq.result_order,
        .parallelism = jobs,
        .scan_all = scan_all,
        .diag = &diag,
    }, if (parquet) .parquet else .jsonl, if (pq.result_order.limit) |n| @as(usize, @intCast(n)) else null, fd, @intCast(pq.result_order.offset));
}

fn querySqlJson(gpa: std.mem.Allocator, ctx: engine.Context, sql: []const u8, jobs: usize) ![]u8 {
    const pq = try zpq.core.expr.sql_parser.parseSqlQuery(gpa, sql);
    defer pq.deinit(gpa);
    const inputs = try ownedInputs(gpa, ctx, pq.table_name);
    defer freeInputs(gpa, inputs);
    var diag: zpq.core.diag.Diag = .{};
    var result = try engine.runQuery(ctx, .{
        .inputs = inputs,
        .aggregate = pq.aggregate_str,
        .filter = pq.filter_str,
        .group_by = pq.group_by_str,
        .select = pq.select_str,
        .select_cols = pq.select_cols,
        .result_order = pq.result_order,
        .parallelism = jobs,
        .diag = &diag,
    });
    defer result.deinit(gpa);
    const answer = switch (result) {
        .aggregate => |*ar| ar,
        .write => return error.NotAggregate,
    };
    const n: usize = if (answer.output.group_rows) |groups|
        groups.len
    else if (answer.output.no_row)
        0
    else
        1;
    var w: MemWriter = .{ .gpa = gpa };
    errdefer w.deinit();
    try engine.writeAggregateText(&w, answer.output, answer.text_kinds, answer.answer_types, n, .json);
    return w.buf.toOwnedSlice(gpa);
}

fn substitute(arena: std.mem.Allocator, buf: *std.ArrayList(u8), sql: []const u8, key: []const u8, value: []const u8) !void {
    const at = std.mem.indexOf(u8, sql, key) orelse return error.BadFixtureSql;
    try buf.appendSlice(arena, sql[0..at]);
    try buf.appendSlice(arena, value);
    try buf.appendSlice(arena, sql[at + key.len ..]);
}

fn duckExec(gpa: std.mem.Allocator, io: std.Io, duckdb: []const u8, sql: []const u8) !void {
    const out = try capture(gpa, io, &.{ duckdb, "-no-agent", "-c", sql });
    defer gpa.free(out.stdout);
    defer gpa.free(out.stderr);
    if (out.code != 0) {
        std.debug.print("duckdb: {s}\n", .{out.stderr});
        return error.DuckDb;
    }
}

const Captured = struct { code: u8, stdout: []u8, stderr: []u8 };

fn capture(gpa: std.mem.Allocator, io: std.Io, argv: []const []const u8) !Captured {
    var child = try std.process.spawn(io, .{
        .argv = argv,
        .stdin = .ignore,
        .stdout = .pipe,
        .stderr = .pipe,
    });
    var multi_reader_buffer: std.Io.File.MultiReader.Buffer(2) = undefined;
    var multi_reader: std.Io.File.MultiReader = undefined;
    multi_reader.init(gpa, io, multi_reader_buffer.toStreams(), &.{ child.stdout.?, child.stderr.? });
    defer multi_reader.deinit();
    while (multi_reader.fill(4096, .none)) |_| {} else |_| {}
    const term = try child.wait(io);
    const code: u8 = switch (term) {
        .exited => |c| c,
        else => 255,
    };
    return .{
        .code = code,
        .stdout = try multi_reader.toOwnedSlice(0),
        .stderr = try multi_reader.toOwnedSlice(1),
    };
}
