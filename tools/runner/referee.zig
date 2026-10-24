//! Differential referee. Cases and known divergences are JSON. zpq runs in-process.
//! DuckDB is the CLI. Pyarrow is the strict reader for files zpq writes.
//!
//!   referee --cases tools/runner/cases/filter_projection.json \
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
    filter: ?[]const u8 = null,
    where: ?[]const u8 = null,
    columns: ?[]const []const u8 = null,
    select_z: ?[]const u8 = null,
    select_d: ?[]const u8 = null,
};

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var cases_path: ?[]const u8 = null;
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
            cases_path = iter.next() orelse return error.BadArgs;
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
    const cases_file = cases_path orelse return error.BadArgs;
    const divergences_file = divergences_path orelse return error.BadArgs;
    if (duckdb.len == 0) {
        const home = std.mem.span(std.c.getenv("HOME") orelse return error.BadArgs);
        duckdb = try std.fmt.allocPrint(arena, "{s}/.cache/zpq-duckdb2/bin-2.0.0/duckdb", .{home});
    }

    const cases_text = try std.Io.Dir.cwd().readFileAlloc(io, cases_file, arena, .limited(16 << 20));
    const div_text = try std.Io.Dir.cwd().readFileAlloc(io, divergences_file, arena, .limited(1 << 20));
    var cases_parsed = try std.json.parseFromSlice(std.json.Value, arena, cases_text, .{});
    defer cases_parsed.deinit();
    var div_parsed = try std.json.parseFromSlice(std.json.Value, arena, div_text, .{});
    defer div_parsed.deinit();

    var known = std.StringHashMap(void).init(arena);
    for (div_parsed.value.object.get("records").?.array.items) |rec| {
        try known.put(rec.object.get("id").?.string, {});
    }

    try std.Io.Dir.cwd().createDirPath(io, work);
    const fixture = try std.fmt.allocPrint(arena, "{s}/fixture.parquet", .{work});
    const out_path = try std.fmt.allocPrint(arena, "{s}/out.parquet", .{work});
    const diff_path = try std.fmt.allocPrint(arena, "{s}/diff.csv", .{work});
    const copy_sql = cases_parsed.value.object.get("fixture_copy").?.string;
    var copy_buf: std.ArrayList(u8) = .empty;
    try substitute(arena, &copy_buf, copy_sql, "{path}", fixture);
    try duckExec(gpa, io, duckdb, copy_buf.items);

    const schema_order = try stringList(arena, cases_parsed.value.object.get("schema_order").?);
    const case_values = cases_parsed.value.object.get("cases").?.array.items;
    const ctx: engine.Context = .{ .gpa = gpa, .env = init.minimal.environ, .io = io };
    var matched: usize = 0;
    var expected: usize = 0;
    var failed: usize = 0;
    var total: usize = 0;

    for (case_values) |raw| {
        if (limit) |n| if (total >= n) break;
        total += 1;
        const case = try decodeCase(arena, raw);
        const failure = runCase(gpa, io, ctx, duckdb, pyarrow, fixture, out_path, diff_path, schema_order, case) catch |err| blk: {
            break :blk try std.fmt.allocPrint(arena, "{s}", .{@errorName(err)});
        };
        if (failure) |why| {
            if (known.contains(case.id)) {
                expected += 1;
                std.debug.print("EXPECTED {s}  {s}\n", .{ case.id, why });
            } else {
                failed += 1;
                std.debug.print("FAIL {s}  {s}\n", .{ case.id, why });
            }
        } else matched += 1;
    }

    std.debug.print("referee: {d}/{d} checks match\n", .{ matched, total });
    std.debug.print("referee: expected {d}\n", .{expected});
    std.debug.print("referee: divergences {d}\n", .{known.count()});
    if (failed != 0) std.process.exit(1);
}

fn decodeCase(arena: std.mem.Allocator, raw: std.json.Value) !Case {
    const obj = raw.object;
    return .{
        .id = obj.get("id").?.string,
        .filter = optionalString(obj, "filter"),
        .where = optionalString(obj, "where"),
        .columns = if (obj.get("columns")) |v| if (v == .null) null else try stringList(arena, v) else null,
        .select_z = optionalString(obj, "select"),
        .select_d = optionalString(obj, "duckdb_select"),
    };
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
