//! zpq-check: the correctness runner. zpq runs in-process; DuckDB 2.0 is the oracle (tools/check/oracle.zig);
//! PyArrow is the strict reader of files zpq writes (tools/check/readback.py).
//!
//!   zpq-check --cases tools/check/cases/agg.json [--cases ...] [--duckdb PATH] [--python PATH] [--work DIR]
//!             [--cache DIR | --no-cache] [--only ID] [--limit N]
//!
//! A case runs at each of its axis points, which must give byte-identical output (or the same refusal); its
//! expectation is then checked once, at the first point. Case files are documented in tools/check/README.md.

const std = @import("std");
const zpq = @import("zpq");
const engine = zpq.engine;
const forcing = @import("forcing.zig");
const compare = @import("compare.zig");
const oracle_mod = @import("oracle.zig");
const verdict = @import("verdict.zig");

const usage =
    \\zpq-check --cases FILE [--cases FILE ...] [--duckdb PATH] [--python PATH] [--zpq PATH] [--data DIR] [--work DIR]
    \\          [--cache DIR | --no-cache] [--only ID] [--limit N] [--parity-case ID --force K=V ...]
    \\          [--bless]     rewrite goldens from this run's output (never exits 0)
    \\zpq-check --self-test      the runner fails on the answers and refusals it must reject
    \\
;

const Query = struct {
    aggregate: ?[]const u8 = null,
    filter: ?[]const u8 = null,
    group_by: ?[]const u8 = null,
    column_order: ?[]const u8 = null,
    columns: ?[]const []const u8 = null,
    select: ?[]const u8 = null,
    codec: ?[]const u8 = null,
};

const Emit = enum { answer, answer_parquet, rows, rows_parquet, copy, values, mixed_orders };

const Point = struct {
    jobs: ?usize = null,
    /// "" clears the case's budget at this point.
    max_memory: ?[]const u8 = null,
    force: ?std.json.ArrayHashMap([]const u8) = null,
    scan_all: ?bool = null,
};

const Refuse = struct {
    @"error": []const u8,
    diag: ?[]const u8 = null,
    /// A second substring the diagnostic must hold.
    also: ?[]const u8 = null,
    /// The refusal printed nothing to stdout first and left no `{out}` file (CLI cases).
    no_output: bool = false,
    /// What the refusal prints to stdout must hold this (`zpq inspect` reports its errors there).
    stdout_has: ?[]const u8 = null,
    /// The whole diagnostic, pinned in tools/check/goldens (`--bless` writes it).
    golden: ?[]const u8 = null,
    /// The CLI's exit status (a named refusal's kind does not carry it).
    exit: ?u8 = null,
    /// DuckDB must refuse too, with a Binder Error, when asked this SQL (`{path}` and `{fixture:NAME}` apply).
    binder_sql: ?[]const u8 = null,
};

const Case = struct {
    id: []const u8,
    fixture: []const u8 = "main",
    /// A file path used instead of a named fixture.
    input: ?[]const u8 = null,
    /// Run the zpq CLI with these arguments (`{path}` is the input) instead of the engine in-process: one process
    /// per point, bounded by `timeout_s`, so a crash or hang on hostile input fails this case alone.
    cli: ?[]const []const u8 = null,
    timeout_s: u32 = 60,
    sql: ?[]const u8 = null,
    query: Query = .{},
    emit: Emit = .answer,
    jobs: ?usize = null,
    max_memory: ?[]const u8 = null,
    trust_stats: bool = false,
    scan_all: bool = false,
    force: ?std.json.ArrayHashMap([]const u8) = null,
    /// Explicit points, or the name of a preset in the file's `axes`.
    axes: ?std.json.Value = null,
    oracle: ?verdict.Spec = null,
    refuse: ?Refuse = null,
    /// Byte parity only, with the reason there is no independent oracle.
    same: ?[]const u8 = null,
    /// Refusals of these kinds are acceptable answers (identical at every point).
    safe: ?[]const []const u8 = null,
    /// Also write the typed answer at the first point (at `jobs` if given, with no point forcing) and check it with
    /// `compare` against the JSON answer: `product` (and DuckDB's read of it) or `parquet` (PyArrow's read).
    typed: ?struct { compare: []const u8, jobs: ?usize = null } = null,
    /// CLI arguments of a second command that must give the same outcome byte for byte at the first point (two
    /// front doors, or a copy path against the path it must equal); it stands beside any expectation.
    same_as: ?[]const []const u8 = null,
};

const Fixture = struct {
    name: ?[]const u8 = null,
    copy: ?[]const u8 = null,
    path: ?[]const u8 = null,
    glob: ?[]const u8 = null,
    recipe: ?[]const u8 = null,
    script: ?[]const u8 = null,
    files: ?[]const []const u8 = null,
    /// A file zpq itself writes: CLI arguments, `{path}` the file and `{fixture:NAME}` an earlier fixture.
    zpq: ?[]const []const u8 = null,
};

const File = struct {
    fixtures: []const Fixture = &.{},
    schema: ?[]const []const u8 = null,
    /// Default jobs axis for cases that declare none.
    jobs: ?[]const usize = null,
    axes: ?std.json.ArrayHashMap([]const Point) = null,
    /// Parametric cases, expanded into `cases` at load (see `expand`).
    matrices: []const std.json.Value = &.{},
    /// Declared refusals by case id, for cases a matrix generates.
    refuse: ?std.json.ArrayHashMap(Refuse) = null,
    cases: []const Case = &.{},
};

const Run = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    ctx: *engine.Context,
    oracle: *oracle_mod.Oracle,
    python: []const u8,
    work: []const u8,
    zpq: []const u8 = "zig-out/bin/zpq",
    bless: bool = false,
    blessed: usize = 0,
    /// Root of external data (`{data}` in cases): ClickBench and other files too large for the tree.
    data: ?[]const u8 = null,
    only: ?[]const u8 = null,
    limit: ?usize = null,
    total: usize = 0,
    matched: usize = 0,
    failed: usize = 0,
    expected: usize = 0,
    refusals: usize = 0,
    declared_refusals: usize = 0,
};

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var case_files: std.ArrayList([]const u8) = .empty;
    var duckdb: []const u8 = "";
    var python: []const u8 = ".venv/bin/python";
    var work: []const u8 = "zig-out/check-work";
    var zpq_bin: []const u8 = "zig-out/bin/zpq";
    var data_root: ?[]const u8 = null;
    var bless = false;
    var cache: ?[]const u8 = null;
    var no_cache = false;
    var only: ?[]const u8 = null;
    var limit: ?usize = null;
    var parity_case: ?[]const u8 = null;
    var forced: std.ArrayList([]const u8) = .empty;
    var self_test = false;

    var iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = iter.next();
    while (iter.next()) |arg| {
        const eq = std.mem.eql;
        if (eq(u8, arg, "--help") or eq(u8, arg, "-h")) {
            std.debug.print("{s}", .{usage});
            return;
        } else if (eq(u8, arg, "--cases")) {
            try case_files.append(arena, iter.next() orelse return error.BadArgs);
        } else if (eq(u8, arg, "--duckdb")) {
            duckdb = iter.next() orelse return error.BadArgs;
        } else if (eq(u8, arg, "--python") or eq(u8, arg, "--pyarrow")) {
            python = iter.next() orelse return error.BadArgs;
        } else if (eq(u8, arg, "--bless")) {
            bless = true;
        } else if (eq(u8, arg, "--data")) {
            data_root = iter.next() orelse return error.BadArgs;
        } else if (eq(u8, arg, "--zpq")) {
            zpq_bin = iter.next() orelse return error.BadArgs;
        } else if (eq(u8, arg, "--work")) {
            work = iter.next() orelse return error.BadArgs;
        } else if (eq(u8, arg, "--cache")) {
            cache = iter.next() orelse return error.BadArgs;
        } else if (eq(u8, arg, "--self-test")) {
            self_test = true;
        } else if (eq(u8, arg, "--no-cache")) {
            no_cache = true;
        } else if (eq(u8, arg, "--only")) {
            only = iter.next() orelse return error.BadArgs;
        } else if (eq(u8, arg, "--parity-case")) {
            parity_case = iter.next() orelse return error.BadArgs;
        } else if (eq(u8, arg, "--force")) {
            try forced.append(arena, iter.next() orelse return error.BadArgs);
        } else if (eq(u8, arg, "--limit")) {
            limit = try std.fmt.parseInt(usize, iter.next() orelse return error.BadArgs, 10);
        } else return error.BadArgs;
    }
    if (case_files.items.len == 0 and !self_test) return error.BadArgs;
    const home = std.mem.span(std.c.getenv("HOME") orelse return error.BadArgs);
    if (duckdb.len == 0) duckdb = try std.fmt.allocPrint(arena, "{s}/.cache/zpq-duckdb2/bin-2.0.0/duckdb", .{home});
    if (no_cache) cache = null else if (cache == null) cache =
        try std.fmt.allocPrint(arena, "{s}/.cache/zpq-check/oracle", .{home});
    const tz = if (std.c.getenv("TZ")) |t| std.mem.span(t) else "";

    try std.Io.Dir.cwd().createDirPath(io, work);
    var ctx: engine.Context = .{ .gpa = gpa, .env = init.minimal.environ, .io = io };
    forcing.cli_overrides = forced.items;
    forcing.observe = parity_case != null;
    defer forcing.deinit(gpa);
    var bindings: std.ArrayList(forcing.Binding) = .empty;
    defer while (bindings.pop()) |binding| {
        var b = binding;
        b.deinit();
    };
    for (forced.items) |entry| {
        const at = std.mem.indexOfScalar(u8, entry, '=') orelse return error.BadArgs;
        const name = try arena.dupeSentinel(u8, entry[0..at], 0);
        try bindings.append(arena, try forcing.bind(gpa, &ctx, name, entry[at + 1 ..]));
    }
    zpq.core.expr.sql_parser.diagnostic_fn = captureDiag;
    zpq.frontend.diagnostic_fn = captureDiag;
    defer zpq.core.expr.sql_parser.diagnostic_fn = null;
    defer zpq.frontend.diagnostic_fn = null;

    var oracle = try oracle_mod.Oracle.init(gpa, io, duckdb, cache, tz);
    var run: Run = .{
        .gpa = gpa,
        .io = io,
        .ctx = &ctx,
        .oracle = &oracle,
        .python = python,
        .work = work,
        .zpq = zpq_bin,
        .data = data_root,
        .bless = bless,
        .only = only,
        .limit = limit,
    };

    if (self_test) return selfTest(&run, arena);

    var ids = std.StringHashMap(void).init(arena);
    for (case_files.items, 0..) |path, file_i| {
        const text = try std.Io.Dir.cwd().readFileAlloc(io, path, arena, .limited(32 << 20));
        const file = loadFile(arena, text) catch |err| {
            std.debug.print("{s}: {s}\n", .{ path, @errorName(err) });
            return err;
        };
        for (file.cases) |c| {
            if ((try ids.getOrPut(c.id)).found_existing) {
                std.debug.print("duplicate case id {s}\n", .{c.id});
                return error.DuplicateCase;
            }
            if (c.refuse != null) run.declared_refusals += 1;
        }
        if (parity_case) |want| {
            for (file.cases) |c| if (std.mem.eql(u8, c.id, want)) {
                const fixtures = try prepareFixtures(&run, arena, file, file_i);
                try runParity(&run, arena, fixtures, fixtures.get(c.fixture) orelse return error.MissingFixture, c);
                std.debug.print("check: path-parity case {s}\n", .{c.id});
                return;
            };
            continue;
        }
        if (only) |want| {
            var any = false;
            for (file.cases) |c| any = any or std.mem.eql(u8, c.id, want);
            if (!any) continue;
        }
        const fixtures = try prepareFixtures(&run, arena, file, file_i);
        for (file.cases) |c| {
            if (only) |want| if (!std.mem.eql(u8, c.id, want)) continue;
            if (run.limit) |n| if (run.total >= n) break;
            var case_arena = std.heap.ArenaAllocator.init(gpa);
            defer case_arena.deinit();
            try runCase(&run, case_arena.allocator(), file, file_i, fixtures, c);
        }
    }
    if (parity_case) |want| {
        std.debug.print("check: no case {s}\n", .{want});
        return error.MissingCase;
    }

    std.debug.print("check: {d}/{d} checks match\n", .{ run.matched, run.total });
    std.debug.print("check: expected {d}\n", .{run.expected});
    std.debug.print("check: refusals {d}\n", .{run.refusals});
    std.debug.print("check: refusal declarations {d}\n", .{run.declared_refusals});
    std.debug.print("check: oracle cache {d} hits, {d} misses{s}\n", .{
        oracle.hits,
        oracle.misses,
        if (cache == null) " (uncached)" else "",
    });
    if (run.failed != 0) std.process.exit(1);
    if (bless) {
        // A bless run is never a verification, even when nothing else failed.
        std.debug.print("check: blessed {d} goldens; review the diff, then run without --bless\n", .{run.blessed});
        std.process.exit(2);
    }
    if (run.total == 0) return error.NoChecks;
    if (limit != null or only != null) {
        std.debug.print("check: limited run; full coverage not established\n", .{});
        std.process.exit(2);
    }
}

// ---- loading: matrices expand into ordinary cases

fn loadFile(arena: std.mem.Allocator, text: []const u8) !File {
    var file = try std.json.parseFromSliceLeaky(File, arena, text, .{});
    var cases: std.ArrayList(Case) = .empty;
    for (file.matrices) |m| try expand(arena, m, &cases);
    try cases.appendSlice(arena, file.cases);
    if (file.refuse) |declared| {
        var it = declared.map.iterator();
        while (it.next()) |e| {
            const c = for (cases.items) |*c| {
                if (std.mem.eql(u8, c.id, e.key_ptr.*)) break c;
            } else return error.RefusalForNoCase;
            // The declared refusal is this cell's expectation; the matrix's oracle no longer applies to it.
            c.refuse = e.value_ptr.*;
            c.oracle = null;
        }
    }
    if (cases.items.len == 0) return error.EmptyCaseFile;
    for (cases.items) |c| try checkExpectation(c);
    file.cases = cases.items;
    return file;
}

/// Exactly one expectation, and none that is empty: a second declaration could otherwise waive the first's check.
fn checkExpectation(c: Case) !void {
    const value = c.oracle != null or c.typed != null;
    const n = @as(u8, @intFromBool(value)) + @intFromBool(c.refuse != null) + @intFromBool(c.same != null) +
        @intFromBool(c.safe != null);
    if (n != 1) {
        std.debug.print("case {s}: declare exactly one of oracle/typed, refuse, same or safe ({d} given)\n", .{
            c.id,
            n,
        });
        return if (n == 0) error.NoExpectation else error.ConflictingExpectations;
    }
    if (c.same) |note| if (std.mem.trim(u8, note, " ").len == 0) return error.EmptyExpectation;
    if (c.safe) |kinds| if (kinds.len == 0) return error.EmptyExpectation;
}

/// A matrix is {"vars": {DIM: {NAME: {VAR: VALUE}}}, "cells": {NAME: [NAME]}?, "defaults": {VAR: VALUE}?,
/// "case": TEMPLATE}. Every combination
/// of one entry per dimension (or, with `cells`, the listed pairs of the first two) instantiates TEMPLATE: a string
/// that is exactly "${VAR}" becomes VAR's value (null when the entry lacks it), "${DIM}" is the entry's name, and
/// other strings interpolate; a list element that is exactly "${VAR}" splices VAR's list. Values may themselves
/// refer to other variables.
fn expand(arena: std.mem.Allocator, m: std.json.Value, out: *std.ArrayList(Case)) !void {
    const vars = m.object.get("vars").?.object;
    const dims = vars.keys();
    var pick = try arena.alloc(usize, dims.len);
    @memset(pick, 0);
    const cells = m.object.get("cells");
    while (true) {
        var env: std.StringArrayHashMapUnmanaged(std.json.Value) = .empty;
        if (m.object.get("defaults")) |defaults| {
            var it = defaults.object.iterator();
            while (it.next()) |e| try env.put(arena, e.key_ptr.*, e.value_ptr.*);
        }
        var keep = true;
        for (dims, 0..) |dim, d| {
            const entries = vars.get(dim).?.object;
            const name = entries.keys()[pick[d]];
            try env.put(arena, dim, .{ .string = name });
            var it = entries.values()[pick[d]].object.iterator();
            while (it.next()) |e| try env.put(arena, e.key_ptr.*, e.value_ptr.*);
        }
        if (cells) |listed| {
            const first = env.get(dims[0]).?.string;
            const second = env.get(dims[1]).?.string;
            keep = false;
            if (listed.object.get(first)) |names| for (names.array.items) |n| {
                keep = keep or std.mem.eql(u8, n.string, second);
            };
        }
        if (keep) {
            const instance = try instantiate(arena, m.object.get("case").?, &env);
            try out.append(arena, try std.json.parseFromValueLeaky(Case, arena, instance, .{}));
        }
        var d = dims.len;
        while (d > 0) {
            d -= 1;
            pick[d] += 1;
            if (pick[d] < vars.get(dims[d]).?.object.count()) break;
            pick[d] = 0;
        } else return;
    }
}

fn instantiate(
    arena: std.mem.Allocator,
    t: std.json.Value,
    env: *std.StringArrayHashMapUnmanaged(std.json.Value),
) !std.json.Value {
    switch (t) {
        .string => |s| {
            if (std.mem.startsWith(u8, s, "${") and std.mem.endsWith(u8, s, "}") and
                std.mem.indexOfScalarPos(u8, s, 2, '$') == null)
            {
                const v = env.get(s[2 .. s.len - 1]) orelse return .null;
                return instantiate(arena, v, env);
            }
            if (std.mem.indexOf(u8, s, "${") == null) return t;
            var text: []const u8 = s;
            for (0..4) |_| {
                var outbuf: std.ArrayList(u8) = .empty;
                var rest = text;
                while (std.mem.indexOf(u8, rest, "${")) |at| {
                    const end = std.mem.indexOfScalarPos(u8, rest, at, '}') orelse return error.BadTemplate;
                    try outbuf.appendSlice(arena, rest[0..at]);
                    const v = env.get(rest[at + 2 .. end]) orelse std.json.Value.null;
                    switch (v) {
                        .string => |x| try outbuf.appendSlice(arena, x),
                        .null => {},
                        else => return error.BadTemplate,
                    }
                    rest = rest[end + 1 ..];
                }
                try outbuf.appendSlice(arena, rest);
                text = outbuf.items;
                if (std.mem.indexOf(u8, text, "${") == null) break;
            }
            return .{ .string = text };
        },
        .array => |xs| {
            var out = std.json.Array.init(arena);
            for (xs.items) |x| switch (try instantiate(arena, x, env)) {
                // An element that is exactly "${VAR}" naming a list splices it (flag lists in `cli`).
                .array => |inner| if (x == .string)
                    try out.appendSlice(inner.items)
                else
                    try out.append(.{ .array = inner }),
                else => |v| try out.append(v),
            };
            return .{ .array = out };
        },
        .object => |o| {
            var out: std.json.ObjectMap = .empty;
            var it = o.iterator();
            while (it.next()) |e| {
                // A field whose variable is unset disappears; a literal null stays.
                const v = try instantiate(arena, e.value_ptr.*, env);
                if (v != .null or e.value_ptr.* == .null) try out.put(arena, e.key_ptr.*, v);
            }
            return .{ .object = out };
        },
        else => return t,
    }
}

// ---- fixtures

fn prepareFixtures(run: *Run, arena: std.mem.Allocator, file: File, file_i: usize) !std.StringHashMap([]const u8) {
    var out = std.StringHashMap([]const u8).init(arena);
    for (file.fixtures) |f| {
        if (f.copy) |sql| {
            const path = try std.fmt.allocPrint(arena, "{s}/f{d}-{s}.parquet", .{ run.work, file_i, f.name.? });
            try run.oracle.exec(try replace(arena, sql, "{path}", path));
            try out.put(f.name.?, path);
        } else if (f.path) |path| {
            try out.put(f.name.?, path);
        } else if (f.zpq) |args| {
            const path = try std.fmt.allocPrint(arena, "{s}/f{d}-{s}.parquet", .{ run.work, file_i, f.name.? });
            std.Io.Dir.cwd().deleteFile(run.io, path) catch {};
            var argv: std.ArrayList([]const u8) = .empty;
            try argv.append(arena, run.zpq);
            for (args) |a| try argv.append(arena, try substitute(arena, a, path, out));
            const made = try oracle_mod.capture(run.gpa, run.io, argv.items);
            defer run.gpa.free(made.stdout);
            defer run.gpa.free(made.stderr);
            if (made.code != 0) {
                std.debug.print("fixture {s}: {s}\n", .{ f.name.?, made.stderr });
                return error.Fixture;
            }
            try out.put(f.name.?, path);
        } else if (f.glob) |pattern| {
            try out.put(f.name.?, try std.fmt.allocPrint(arena, "{s}/f{d}-{s}", .{ run.work, file_i, pattern }));
        } else {
            var argv: std.ArrayList([]const u8) = .empty;
            try argv.append(arena, run.python);
            const names = f.files orelse return error.BadFixture;
            if (f.recipe) |recipe| {
                const dir = try std.fmt.allocPrint(arena, "{s}/f{d}-recipe", .{ run.work, file_i });
                try argv.appendSlice(arena, &.{ "tools/zpq-gen", recipe, "--out", dir });
                for (names) |n| try out.put(n, try std.fmt.allocPrint(arena, "{s}/{s}.parquet", .{ dir, n }));
            } else {
                try argv.append(arena, f.script orelse return error.BadFixture);
                for (names) |n| {
                    const path = try std.fmt.allocPrint(arena, "{s}/f{d}-{s}.parquet", .{ run.work, file_i, n });
                    try argv.append(arena, path);
                    try out.put(n, path);
                }
            }
            const made = try oracle_mod.capture(run.gpa, run.io, argv.items);
            defer run.gpa.free(made.stdout);
            defer run.gpa.free(made.stderr);
            if (made.code != 0) {
                std.debug.print("fixture: {s}\n", .{made.stderr});
                return error.Fixture;
            }
        }
    }
    return out;
}

fn replace(arena: std.mem.Allocator, text: []const u8, key: []const u8, value: []const u8) ![]const u8 {
    return std.mem.replaceOwned(u8, arena, text, key, value);
}

// ---- running a case

threadlocal var diag_buf: [8192]u8 = undefined;
threadlocal var diag_len: usize = 0;

fn captureDiag(msg: []const u8) void {
    const n = @min(msg.len, diag_buf.len -| diag_len);
    @memcpy(diag_buf[diag_len..][0..n], msg[0..n]);
    diag_len += n;
}

fn diagText() []const u8 {
    return std.mem.trim(u8, diag_buf[0..diag_len], " \t\r\n");
}

fn lastDiagLine() []const u8 {
    const t = diagText();
    return if (std.mem.lastIndexOfScalar(u8, t, '\n')) |at| std.mem.trim(u8, t[at + 1 ..], " \t\r\n") else t;
}

const Outcome = union(enum) {
    /// Output bytes (text, or the written file's contents).
    ok: []const u8,
    /// A CLI point's stderr beside its answer: summaries carry timings, so it is not part of the bytes compared.
    ok_cli: struct { bytes: []const u8, stdout: []const u8, stderr: []const u8, path: []const u8 },
    refused: struct { kind: []const u8, line: []const u8, diag: []const u8, stdout: []const u8 = "", wrote: bool =
        false,
    code: u8 = 0 },
};

fn pointsOf(arena: std.mem.Allocator, file: File, c: Case) ![]const Point {
    if (c.axes) |axes| switch (axes) {
        .string => |name| return (file.axes orelse return error.UnknownAxes).map.get(name) orelse
            return error.UnknownAxes,
        .array => {
            const parsed = try std.json.parseFromValueLeaky([]const Point, arena, axes, .{});
            if (parsed.len == 0) return error.EmptyAxes;
            return parsed;
        },
        else => return error.BadAxes,
    };
    const jobs = file.jobs orelse return &.{.{}};
    const out = try arena.alloc(Point, jobs.len);
    for (jobs, 0..) |j, i| out[i] = .{ .jobs = j };
    return out;
}

fn runCase(
    run: *Run,
    arena: std.mem.Allocator,
    file: File,
    file_i: usize,
    fixtures: std.StringHashMap([]const u8),
    c: Case,
) !void {
    forcing.refresh(run.ctx);
    const input = if (c.input) |raw| try dataPath(run, arena, raw) else null;
    const fixture = input orelse fixtures.get(c.fixture) orelse {
        std.debug.print("FAIL {s}  no fixture {s}\n", .{ c.id, c.fixture });
        return error.MissingFixture;
    };
    const points = try pointsOf(arena, file, c);
    const checks = points.len + @intFromBool(c.typed != null);
    run.total += checks;
    const why = checkCase(run, arena, file, file_i, fixtures, fixture, c, points) catch |err|
        try std.fmt.allocPrint(arena, "{s}", .{@errorName(err)});
    if (why) |text| {
        run.failed += checks;
        std.debug.print("FAIL {s}  {s}\n", .{ c.id, text });
        return;
    }
    run.matched += checks;
    if (c.refuse != null) run.expected += checks;
    if (c.same) |note| std.debug.print("BYTE-PARITY {s}: {s}\n", .{ c.id, note });
}

fn checkCase(
    run: *Run,
    arena: std.mem.Allocator,
    file: File,
    file_i: usize,
    fixtures: std.StringHashMap([]const u8),
    fixture: []const u8,
    raw: Case,
    points: []const Point,
) !?[]const u8 {
    var c = raw;
    if (c.sql) |sql| c.sql = try substitute(arena, sql, fixture, fixtures);
    if (c.cli) |args| {
        const filled = try arena.alloc([]const u8, args.len);
        for (args, 0..) |a, i| filled[i] = try substitute(arena, a, fixture, fixtures);
        c.cli = filled;
    }
    if (c.oracle) |o| {
        if (o.sql) |sql| c.oracle.?.sql = try substitute(arena, sql, fixture, fixtures);
        if (o.setup) |sql| c.oracle.?.setup = try substitute(arena, sql, fixture, fixtures);
    }
    if (c.refuse) |r| if (r.binder_sql) |sql| {
        c.refuse.?.binder_sql = try substitute(arena, sql, fixture, fixtures);
    };
    const writes = writesOut(c);
    const extension = if (writes) "parquet" else switch (c.emit) {
        .answer, .values => "json",
        .rows => "jsonl",
        else => "parquet",
    };
    const out_path = try std.fmt.allocPrint(arena, "{s}/f{d}-out.{s}", .{ run.work, file_i, extension });
    var first: ?Outcome = null;
    for (points, 0..) |point, i| {
        const got = try executeAt(run, arena, fixture, c, point, out_path);
        if (first) |base| {
            if (!sameOutcome(base, got)) return try std.fmt.allocPrint(arena, "point {d} differs from point 0 ({s})", .{
                i,
                describe(got),
            });
        } else first = got;
    }
    const base = first.?;
    if (base == .refused and (std.mem.eql(u8, base.refused.kind, "signal") or
        std.mem.eql(u8, base.refused.kind, "timeout")))
        return try std.fmt.allocPrint(arena, "zpq {s}: {s}", .{ base.refused.kind, base.refused.line });
    if (c.same_as) |args| {
        var other = c;
        const filled = try arena.alloc([]const u8, args.len);
        for (args, 0..) |a, i| filled[i] = try substitute(arena, a, fixture, fixtures);
        other.cli = filled;
        const got = try executeAt(run, arena, fixture, other, points[0], out_path);
        if (!sameOutcome(base, got)) return try std.fmt.allocPrint(arena, "same_as differs ({s})", .{describe(got)});
    }
    if (c.refuse) |r| {
        const refused = switch (base) {
            .refused => |x| x,
            .ok, .ok_cli => return try std.fmt.allocPrint(arena, "zpq answered, expected refusal {s}", .{r.@"error"}),
        };
        if (!std.mem.eql(u8, refused.kind, r.@"error")) return try std.fmt.allocPrint(
            arena,
            "refused {s} (exit {d}), expected {s}",
            .{ refused.kind, refused.code, r.@"error" },
        );
        if (r.exit) |code| if (refused.code != code) return try std.fmt.allocPrint(
            arena,
            "refused with exit {d}, expected {d}",
            .{ refused.code, code },
        );
        if (refused.line.len == 0) return try std.fmt.allocPrint(
            arena,
            "refused without a message ({s})",
            .{refused.kind},
        );
        for ([_]?[]const u8{ r.diag, r.also }) |maybe| if (maybe) |needle| {
            if (std.mem.indexOf(u8, refused.diag, needle) == null)
                return try std.fmt.allocPrint(arena, "refused ({s}), want {s}", .{ refused.line, needle });
        };
        if (r.no_output and refused.stdout.len != 0)
            return try std.fmt.allocPrint(arena, "refused after printing {d} bytes", .{refused.stdout.len});
        if (r.no_output and refused.wrote) return try arena.dupe(u8, "refused but left an output file");
        if (r.stdout_has) |needle| if (std.mem.indexOf(u8, refused.stdout, needle) == null)
            return try std.fmt.allocPrint(arena, "refusal's stdout lacks {s}", .{needle});
        if (r.golden) |name| {
            const path = try std.fmt.allocPrint(arena, "tools/check/goldens/{s}", .{name});
            const diag = try verdict.unpathed(arena, refused.diag, fixture, out_path);
            if (run.bless) {
                try std.Io.Dir.cwd().createDirPath(run.io, std.fs.path.dirname(path).?);
                try std.Io.Dir.cwd().writeFile(run.io, .{ .sub_path = path, .data = diag });
                run.blessed += 1;
            } else {
                const want = std.Io.Dir.cwd().readFileAlloc(run.io, path, arena, .limited(1 << 20)) catch
                    return try std.fmt.allocPrint(
                        arena,
                        "no golden {s}: review the diagnostic and run --bless",
                        .{path},
                    );
                if (!std.mem.eql(u8, want, diag)) return try std.fmt.allocPrint(
                    arena,
                    "diagnostic differs from {s}",
                    .{path},
                );
            }
        }
        if (r.binder_sql) |sql| {
            const ans = try run.oracle.ask(arena, sql);
            const said = ans.refused orelse return try arena.dupe(u8, "duck answered, zpq refused");
            if (!std.mem.startsWith(u8, said, "Binder Error:")) return try std.fmt.allocPrint(
                arena,
                "duck refused otherwise: {s}",
                .{said},
            );
        }
        run.refusals += points.len;
        std.debug.print("EXPECTED {s} {s} ({d} points)\n", .{ c.id, r.@"error", points.len });
        return null;
    }
    switch (base) {
        .refused => |x| {
            if (c.safe) |kinds| for (kinds) |k| if (std.mem.eql(u8, k, x.kind)) return null;
            return try std.fmt.allocPrint(
                arena,
                "{s}",
                .{if (x.diag.len != 0) x.diag[0..@min(x.diag.len, 180)] else x.kind},
            );
        },
        .ok, .ok_cli => {},
    }
    if (c.same != null or c.safe != null) return null;
    const file_out = writes or switch (c.emit) {
        .answer_parquet, .rows_parquet, .copy, .mixed_orders => true,
        else => false,
    };
    // The first point's output is the one judged; re-run it if a later point overwrote the file.
    if (file_out and points.len > 1) _ = try executeAt(run, arena, fixture, c, points[0], out_path);
    const in: verdict.Inputs = .{
        .arena = arena,
        .io = run.io,
        .gpa = run.gpa,
        .oracle = run.oracle,
        .python = run.python,
        .fixture = fixture,
        .text = switch (base) {
            .ok => |bytes| if (file_out) "" else bytes,
            .ok_cli => |x| x.stdout,
            .refused => unreachable,
        },
        .path = switch (base) {
            .ok_cli => |x| if (writes) x.path else "",
            else => if (file_out) out_path else "",
        },
        .stderr = if (base == .ok_cli) base.ok_cli.stderr else "",
        .columns = c.query.columns,
        .select = c.query.select != null,
        .bless = run.bless,
        .blessed = &run.blessed,
    };
    if (c.oracle) |spec| {
        var with_schema = spec;
        if (with_schema.schema == null) with_schema.schema = file.schema;
        if (try verdict.run(in, with_schema)) |why| return why;
    }
    if (c.typed) |typed| {
        const path = try std.fmt.allocPrint(arena, "{s}/f{d}-typed.parquet", .{ run.work, file_i });
        var as_parquet = c;
        as_parquet.emit = .answer_parquet;
        as_parquet.force = null;
        const at: Point = .{ .jobs = typed.jobs orelse points[0].jobs, .max_memory = points[0].max_memory };
        switch (try executeAt(run, arena, fixture, as_parquet, at, path)) {
            .refused => |x| return try std.fmt.allocPrint(arena, "typed write refused: {s}", .{x.line}),
            .ok, .ok_cli => {},
        }
        var typed_in = in;
        typed_in.path = path;
        if (try verdict.run(typed_in, .{ .compare = typed.compare })) |why| return why;
    }
    return null;
}

/// A CLI case that writes `{out}`: its verdict reads that file.
fn writesOut(c: Case) bool {
    const args = c.cli orelse return false;
    // `{out.answer}` and the like are written files too: the verdict reads what they name.
    for (args) |a| if (std.mem.indexOf(u8, a, "{out") != null) return true;
    return false;
}

fn sameOutcome(a: Outcome, b: Outcome) bool {
    return switch (a) {
        .ok => |x| b == .ok and std.mem.eql(u8, x, b.ok),
        .ok_cli => |x| b == .ok_cli and std.mem.eql(u8, x.bytes, b.ok_cli.bytes),
        // The whole diagnostic, not only its last line: error order must not depend on the point.
        .refused => |x| b == .refused and std.mem.eql(u8, x.kind, b.refused.kind) and
            std.mem.eql(u8, x.diag, b.refused.diag) and
            std.mem.eql(u8, x.stdout, b.refused.stdout) and x.code == b.refused.code,
    };
}

fn describe(o: Outcome) []const u8 {
    return switch (o) {
        .ok, .ok_cli => "an answer",
        .refused => |x| x.kind,
    };
}

fn executeAt(
    run: *Run,
    arena: std.mem.Allocator,
    fixture: []const u8,
    c: Case,
    point: Point,
    out_path: []const u8,
) !Outcome {
    var bound: std.ArrayList(forcing.Binding) = .empty;
    defer while (bound.pop()) |binding| {
        var b = binding;
        b.deinit();
    };
    for ([_]?std.json.ArrayHashMap([]const u8){ c.force, point.force }) |maybe| if (maybe) |env| {
        var it = env.map.iterator();
        while (it.next()) |e| {
            const name = try arena.dupeSentinel(u8, e.key_ptr.*, 0);
            if (forcing.isForced(name)) continue;
            try bound.append(arena, try forcing.bind(run.gpa, run.ctx, name, e.value_ptr.*));
        }
    };
    var at = c;
    if (point.jobs) |j| at.jobs = j;
    if (point.max_memory) |m| at.max_memory = if (m.len == 0) null else m;
    if (point.scan_all) |v| at.scan_all = v;
    if (c.cli) |args| return runCli(run, arena, fixture, at, point, args, out_path);
    diag_len = 0;
    var diag: zpq.core.diag.Diag = .{};
    const result = execute(run, arena, fixture, at, out_path, &diag);
    if (result) |bytes| return .{ .ok = bytes } else |err| {
        if (diag_len == 0) zpq.frontend.queryError(err, &diag);
        return .{ .refused = .{
            .kind = @errorName(err),
            .line = try arena.dupe(u8, lastDiagLine()),
            .diag = try arena.dupe(u8, diagText()),
        } };
    }
}

/// The CLI's outcome: stdout on exit 0, else a refusal whose kind is the `(ErrorName)` its message ends with.
fn runCli(
    run: *Run,
    arena: std.mem.Allocator,
    fixture: []const u8,
    c: Case,
    point: Point,
    args: []const []const u8,
    out_path: []const u8,
) !Outcome {
    // `{out}` is the written file; `{out.json}` the same with a suffix, for cases that name the extension.
    var target = out_path;
    for (args) |a| if (std.mem.indexOf(u8, a, "{out")) |at| {
        const end = std.mem.indexOfScalarPos(u8, a, at, '}') orelse return error.BadSubstitution;
        target = try std.mem.concat(arena, u8, &.{ out_path, a[at + 4 .. end] });
    };
    std.Io.Dir.cwd().deleteFile(run.io, target) catch {};
    var argv: std.ArrayList([]const u8) = .empty;
    const timeout = try std.fmt.allocPrint(arena, "{d}", .{c.timeout_s});
    try argv.appendSlice(arena, &.{ "timeout", "--kill-after=5", timeout, run.zpq });
    for (args) |a| {
        var arg = try replace(arena, a, "{path}", fixture);
        if (std.mem.indexOf(u8, arg, "{out")) |at| {
            const end = std.mem.indexOfScalarPos(u8, arg, at, '}').?;
            arg = try std.mem.concat(arena, u8, &.{ arg[0..at], target, arg[end + 1 ..] });
        }
        try argv.append(arena, arg);
    }
    if (c.jobs) |j| try argv.appendSlice(arena, &.{ "-j", try std.fmt.allocPrint(arena, "{d}", .{j}) });
    if (c.max_memory) |m| try argv.appendSlice(arena, &.{ "--max-memory", m });
    // The child reads overrides from its environment, not from this process's bindings; `--force` wins.
    var env = try run.ctx.env.createMap(arena);
    for ([_]?std.json.ArrayHashMap([]const u8){ c.force, point.force }) |maybe| if (maybe) |forced| {
        var it = forced.map.iterator();
        while (it.next()) |e| try env.put(e.key_ptr.*, e.value_ptr.*);
    };
    for (forcing.cli_overrides) |entry| {
        const at = std.mem.indexOfScalar(u8, entry, '=').?;
        try env.put(entry[0..at], entry[at + 1 ..]);
    }
    const out = try oracle_mod.captureEnv(run.gpa, run.io, argv.items, &env);
    defer run.gpa.free(out.stdout);
    defer run.gpa.free(out.stderr);
    if (out.code == 0 and !out.signaled) {
        // A written file is part of the answer: every point must write the same bytes.
        const written = std.Io.Dir.cwd().readFileAlloc(run.io, target, arena, .limited(256 << 20)) catch "";
        return .{ .ok_cli = .{
            .bytes = try std.mem.concat(arena, u8, &.{ out.stdout, written }),
            .stdout = try arena.dupe(u8, out.stdout),
            .stderr = try arena.dupe(u8, out.stderr),
            .path = target,
        } };
    }
    const diag = try arena.dupe(u8, std.mem.trim(u8, out.stderr, " \t\r\n"));
    const line = if (std.mem.lastIndexOfScalar(u8, diag, '\n')) |at| diag[at + 1 ..] else diag;
    const kind: []const u8 = if (out.signaled or out.code >= 128)
        "signal"
    else if (out.code == 124)
        "timeout"
    else if (jsonCode(arena, diag)) |code| code else named: {
        const fallback = try std.fmt.allocPrint(arena, "exit {d}", .{out.code});
        const open = std.mem.lastIndexOfScalar(u8, diag, '(') orelse break :named fallback;
        const close = std.mem.indexOfScalarPos(u8, diag, open, ')') orelse break :named fallback;
        const name = diag[open + 1 .. close];
        if (name.len == 0 or !std.ascii.isUpper(name[0])) break :named fallback;
        for (name) |ch| if (!std.ascii.isAlphanumeric(ch)) break :named fallback;
        break :named name;
    };
    const wrote = if (std.Io.Dir.cwd().access(run.io, target, .{})) |_| true else |_| false;
    return .{ .refused = .{
        .kind = kind,
        .line = line,
        .diag = diag,
        .stdout = try arena.dupe(u8, out.stdout),
        .wrote = wrote,
        .code = out.code,
    } };
}

/// The `code` of a `--summary=json` error record on stderr, when there is one.
fn jsonCode(arena: std.mem.Allocator, diag: []const u8) ?[]const u8 {
    const line = if (std.mem.lastIndexOfScalar(u8, diag, '\n')) |at| diag[at + 1 ..] else diag;
    if (line.len == 0 or line[0] != '{') return null;
    const v = std.json.parseFromSliceLeaky(std.json.Value, arena, line, .{}) catch return null;
    if (v != .object) return null;
    const code = v.object.get("code") orelse return null;
    return if (code == .string) code.string else null;
}

fn memoryOf(c: Case) !?usize {
    const text = c.max_memory orelse return null;
    return try zpq.core.system.parseSizeString(text);
}

fn execute(
    run: *Run,
    arena: std.mem.Allocator,
    fixture: []const u8,
    c: Case,
    out_path: []const u8,
    diag: *zpq.core.diag.Diag,
) ![]const u8 {
    const gpa = run.gpa;
    const ctx = run.ctx.*;
    const jobs = c.jobs orelse 0;
    const mem = try memoryOf(c);
    std.Io.Dir.cwd().deleteFile(run.io, out_path) catch {};
    if (c.sql) |sql| {
        const pq = try zpq.core.expr.sql_parser.parseSqlQuery(gpa, sql);
        defer pq.deinit(gpa);
        if (c.emit == .values) return valuesOf(gpa, arena, ctx, pq, jobs, diag);
        const inputs = try expandInputs(arena, ctx, pq.table_name);
        var args: engine.QueryArgs = .{
            .inputs = inputs,
            .aggregate = pq.aggregate_str,
            .filter = pq.filter_str,
            .group_by = pq.group_by_str,
            .select = pq.select_str,
            .select_cols = pq.select_cols,
            .result_order = pq.result_order,
            .parallelism = jobs,
            .trust_stats = c.trust_stats,
            .scan_all = c.scan_all,
            .diag = diag,
        };
        if (mem) |n| {
            args.max_memory = n;
            args.max_memory_explicit = n;
        }
        switch (c.emit) {
            .answer => return answerText(gpa, arena, ctx, args),
            .answer_parquet => {
                args.output = out_path;
                args.typed_answer = true;
                var result = try forcing.runQuery(ctx, args);
                result.deinit(gpa);
                return readOut(run, arena, out_path);
            },
            .rows, .rows_parquet => {
                if (mem == null) args.max_memory = 512 * 1024 * 1024;
                const fd = try zpq.local_fs.createFile(out_path);
                defer zpq.local_fs.close(fd);
                const limit: ?usize = if (pq.result_order.limit) |n| @intCast(n) else null;
                _ = try forcing.runPrintTo(
                    ctx,
                    args,
                    if (c.emit == .rows) .jsonl else .parquet,
                    limit,
                    fd,
                    @intCast(pq.result_order.offset),
                );
                return readOut(run, arena, out_path);
            },
            else => return error.BadEmit,
        }
    }
    const q = c.query;
    var args: engine.QueryArgs = .{
        .inputs = try arena.dupe([]const u8, &.{fixture}),
        .aggregate = q.aggregate,
        .filter = q.filter,
        .group_by = q.group_by,
        .column_order = q.column_order,
        .columns = q.columns,
        .select = q.select,
        .codec = if (q.codec) |name| try parseCodec(name) else null,
        .parallelism = jobs,
        .trust_stats = c.trust_stats,
        .scan_all = c.scan_all,
        .diag = diag,
    };
    if (mem) |n| {
        args.max_memory = n;
        args.max_memory_explicit = n;
    }
    switch (c.emit) {
        .answer => return aggregateText(gpa, arena, ctx, args),
        .answer_parquet, .copy => {
            args.output = out_path;
            args.typed_answer = c.emit == .answer_parquet;
            var result = try forcing.runQuery(ctx, args);
            result.deinit(gpa);
            return readOut(run, arena, out_path);
        },
        .rows => {
            const fd = try zpq.local_fs.createFile(out_path);
            defer zpq.local_fs.close(fd);
            _ = try forcing.runPrintTo(ctx, args, .jsonl, null, fd, 0);
            return readOut(run, arena, out_path);
        },
        .mixed_orders => {
            // A file declaring no column order, re-encoded beside one that does: the output must declare it.
            const norm = try std.fmt.allocPrint(arena, "{s}.norm.parquet", .{out_path});
            var first: engine.QueryArgs = args;
            first.filter = "i > -1";
            first.output = norm;
            var r1 = try forcing.runQuery(ctx, first);
            r1.deinit(gpa);
            var both = args;
            both.inputs = try arena.dupe([]const u8, &.{ fixture, norm });
            both.output = out_path;
            var r2 = try forcing.runQuery(ctx, both);
            r2.deinit(gpa);
            return readOut(run, arena, out_path);
        },
        else => return error.BadEmit,
    }
}

fn readOut(run: *Run, arena: std.mem.Allocator, path: []const u8) ![]const u8 {
    return std.Io.Dir.cwd().readFileAlloc(run.io, path, arena, .limited(256 << 20));
}

fn answerText(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    ctx: engine.Context,
    base: engine.QueryArgs,
) ![]const u8 {
    var text: std.Io.Writer.Allocating = .init(gpa);
    defer text.deinit();
    var args = base;
    args.text_output = .{ .format = .json, .sink = &text.writer };
    var result = forcing.runQuery(ctx, args) catch |err|
        return if (err == error.WriteFailed) error.OutOfMemory else err;
    defer result.deinit(gpa);
    if (result != .aggregate) return error.NotAggregate;
    return arena.dupe(u8, text.written());
}

/// The JSON text of a query-form aggregate, as the CLI prints it.
fn aggregateText(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    ctx: engine.Context,
    args: engine.QueryArgs,
) ![]const u8 {
    var result = try forcing.runQuery(ctx, args);
    defer result.deinit(gpa);
    const answer = switch (result) {
        .aggregate => |*ar| ar,
        .write => return error.NotAggregate,
    };
    const n: usize = if (answer.output.group_rows) |groups| groups.len else if (answer.output.no_row) 0 else 1;
    var text: std.Io.Writer.Allocating = .init(arena);
    try engine.writeAggregateText(&text.writer, answer.output, answer.text_kinds, answer.answer_types, n, .json);
    return text.written();
}

/// `count, sum`-style integer answers as "c,s" text, NULL as the word.
fn valuesOf(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    ctx: engine.Context,
    pq: anytype,
    jobs: usize,
    diag: *zpq.core.diag.Diag,
) ![]const u8 {
    var result = try forcing.runQuery(ctx, .{
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
    for (aggs, 0..) |item, i| {
        if (i != 0) try buf.append(arena, ',');
        switch (item.value) {
            .i => |n| try buf.print(arena, "{d}", .{n}),
            .null_val => try buf.appendSlice(arena, "NULL"),
            else => return error.UnexpectedAgg,
        }
    }
    return buf.items;
}

fn expandInputs(arena: std.mem.Allocator, ctx: engine.Context, pattern: []const u8) ![]const []const u8 {
    return zpq.frontend.expandGlob(arena, ctx.io, pattern, ctx.env);
}

/// `{data}` is the external data root; without `--data` a case that needs it fails rather than skips.
fn dataPath(run: *Run, arena: std.mem.Allocator, raw: []const u8) ![]const u8 {
    if (std.mem.indexOf(u8, raw, "{data}") == null) return raw;
    return replace(arena, raw, "{data}", run.data orelse return error.NoDataRoot);
}

/// `{path}` is the case's fixture and `{fixture:NAME}` any fixture of the file.
fn substitute(
    arena: std.mem.Allocator,
    text: []const u8,
    fixture: []const u8,
    fixtures: std.StringHashMap([]const u8),
) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    var rest = try replace(arena, text, "{path}", fixture);
    while (std.mem.indexOf(u8, rest, "{fixture:")) |at| {
        const end = std.mem.indexOfScalarPos(u8, rest, at, '}') orelse return error.BadSubstitution;
        try out.appendSlice(arena, rest[0..at]);
        try out.appendSlice(arena, fixtures.get(rest[at + 9 .. end]) orelse return error.MissingFixture);
        rest = rest[end + 1 ..];
    }
    try out.appendSlice(arena, rest);
    return out.items;
}

fn parseCodec(name: []const u8) !zpq.core.schema.CompressionCodec {
    const eq = std.ascii.eqlIgnoreCase;
    if (eq(name, "zstd")) return .ZSTD;
    if (eq(name, "snappy")) return .SNAPPY;
    if (eq(name, "gzip")) return .GZIP;
    if (eq(name, "uncompressed")) return .UNCOMPRESSED;
    if (eq(name, "lz4") or eq(name, "lz4_raw")) return .LZ4_RAW;
    return error.BadCodec;
}

/// Path-parity mode: write the case's JSON and Parquet answers and their execution proofs into the work directory.
fn runParity(
    run: *Run,
    arena: std.mem.Allocator,
    fixtures: std.StringHashMap([]const u8),
    fixture: []const u8,
    raw: Case,
) !void {
    var c = raw;
    c.sql = try substitute(arena, c.sql orelse return error.MissingSql, fixture, fixtures);
    const json_path = try std.fmt.allocPrint(arena, "{s}/parity.json", .{run.work});
    const parquet_path = try std.fmt.allocPrint(arena, "{s}/parity.parquet", .{run.work});
    var at = c;
    at.jobs = c.jobs orelse 16;
    if (c.axes != null) {
        const points = try pointsOf(arena, .{ .cases = &.{} }, c);
        if (points[0].jobs) |j| at.jobs = j;
    }
    const parsed = try zpq.core.expr.sql_parser.parseSqlQuery(run.gpa, c.sql.?);
    const aggregate = parsed.aggregate_str != null or parsed.group_by_str != null;
    parsed.deinit(run.gpa);
    var bound: std.ArrayList(forcing.Binding) = .empty;
    defer while (bound.pop()) |binding| {
        var b = binding;
        b.deinit();
    };
    if (c.force) |env| {
        var it = env.map.iterator();
        while (it.next()) |e| {
            const name = try arena.dupeSentinel(u8, e.key_ptr.*, 0);
            if (!forcing.isForced(name)) try bound.append(
                arena,
                try forcing.bind(run.gpa, run.ctx, name, e.value_ptr.*),
            );
        }
    }
    at.emit = if (aggregate) .answer else .rows;
    var diag: zpq.core.diag.Diag = .{};
    const bytes = try execute(run, arena, fixture, at, json_path, &diag);
    if (aggregate) try std.Io.Dir.cwd().writeFile(run.io, .{ .sub_path = json_path, .data = bytes });
    const json_proof = try std.json.Stringify.valueAlloc(arena, forcing.last_proof, .{});
    try std.Io.Dir.cwd().writeFile(
        run.io,
        .{ .sub_path = try std.fmt.allocPrint(
            arena,
            "{s}/parity-json-proof.json",
            .{run.work},
        ), .data = json_proof },
    );
    at.emit = if (aggregate) .answer_parquet else .rows_parquet;
    _ = try execute(run, arena, fixture, at, parquet_path, &diag);
    const proof = try std.json.Stringify.valueAlloc(arena, .{ .case_id = c.id, .parquet = forcing.last_proof }, .{});
    try std.Io.Dir.cwd().writeFile(
        run.io,
        .{ .sub_path = try std.fmt.allocPrint(
            arena,
            "{s}/parity-proof.json",
            .{run.work},
        ), .data = proof },
    );
}

// ---- self-test: a runner that passes wrong answers is the worst bug, so each guard is exercised here

const self_fixture =
    \\"fixtures": [{"name": "main",
    \\ "copy": "COPY (SELECT * FROM (VALUES (1), (2), (2), (NULL)) t(id)) TO '{path}' (FORMAT PARQUET)"}],
    \\ "schema": ["id"]
;

fn selfCase(
    run: *Run,
    arena: std.mem.Allocator,
    extra: []const u8,
    cases: []const u8,
) !struct { failed: usize, matched: usize, refusals: usize } {
    const text = try std.fmt.allocPrint(arena, "{{{s}{s}, \"cases\": [{s}]}}", .{ self_fixture, extra, cases });
    const file = try loadFile(arena, text);
    const before = run.*;
    const fixtures = try prepareFixtures(run, arena, file, 99);
    for (file.cases) |c| try runCase(run, arena, file, 99, fixtures, c);
    return .{
        .failed = run.failed - before.failed,
        .matched = run.matched - before.matched,
        .refusals = run.refusals - before.refusals,
    };
}

fn selfTest(run: *Run, arena: std.mem.Allocator) !void {
    var problems: usize = 0;
    const expect = struct {
        fn ok(cond: bool, what: []const u8, n: *usize) void {
            if (!cond) {
                std.debug.print("SELF-TEST FAIL: {s}\n", .{what});
                n.* += 1;
            }
        }
    }.ok;
    // Extra, missing, changed, duplicate and NULL rows fail; equal rows pass.
    for ([_][]const u8{ "id = 1", "id IS NOT NULL", "id <> 2", "id IS NULL" }) |where| {
        const c =
            try std.fmt.allocPrint(
                arena,
                "{{\"id\": \"extra\", \"emit\": \"copy\", \"oracle\": {{\"compare\": \"except\", \"where\": " ++
                    "\"{s}\"}}}}",
                .{where},
            );
        expect(
            (try selfCase(run, arena, "", c)).failed == 1,
            "an oracle with fewer rows than zpq's copy must fail",
            &problems,
        );
    }
    expect((try selfCase(
        run,
        arena,
        "",
        "{\"id\": \"missing\", \"emit\": \"copy\", \"query\": {\"filter\": \"id = 1\"}, \"oracle\": {\"compare\": " ++
            "\"except\"}}",
    )).failed == 1, "missing rows must fail", &problems);
    expect((try selfCase(
        run,
        arena,
        "",
        "{\"id\": \"equal\", \"emit\": \"copy\", \"oracle\": {\"compare\": \"except\"}}",
    )).matched == 1, "equal rows must pass", &problems);
    // A wrong aggregate value fails under every answer mode.
    const wrong = [_][]const u8{
        "{\"id\": \"agg\", \"query\": {\"aggregate\": \"sum(id) AS s\"}, \"oracle\": {\"compare\": \"agg\", " ++
            "\"sql\": \"SELECT sum(id) + 1 FROM '{path}'\"}}",
        "{\"id\": \"null\", \"query\": {\"aggregate\": \"sum(id) AS s\", \"filter\": \"id > 5\"}, \"oracle\": " ++
            "{\"compare\": \"agg\", \"sql\": \"SELECT 0\"}}",
        "{\"id\": \"group\", \"sql\": \"SELECT id, count(*) AS c FROM '{path}' GROUP BY id\", \"oracle\": " ++
            "{\"compare\": \"group\", \"sql\": \"SELECT id, count(*) + (id = 2)::INT AS c FROM '{path}' GROUP BY " ++
            "id\"}}",
        "{\"id\": \"ordered\", \"sql\": \"SELECT id FROM '{path}' WHERE id > 0 ORDER BY id\", \"emit\": \"rows\", " ++
            "\"oracle\": {\"compare\": \"ordered\", \"sql\": \"SELECT id FROM '{path}' WHERE id > 0 ORDER BY id " ++
            "DESC\"}}",
        "{\"id\": \"values\", \"sql\": \"SELECT count(*) AS c, sum(id) AS s FROM '{path}'\", \"emit\": " ++
            "\"values\", \"oracle\": {\"compare\": \"values\", \"sql\": \"SELECT count(*), sum(id) - 1 FROM " ++
            "'{path}'\"}}",
    };
    for (wrong) |c| expect((try selfCase(run, arena, "", c)).failed == 1, "a wrong value must fail", &problems);
    // A refusal passes only as declared, with its exact kind.
    const sql = "SELECT count(*) AS c, sum(id) AS s FROM '{path}' WHERE id = id";
    const declared =
        try std.fmt.allocPrint(
            arena,
            "{{\"id\": \"refuse\", \"sql\": \"{s}\", \"emit\": \"values\", \"axes\": [{{\"jobs\": 1}}, " ++
                "{{\"jobs\": 4}}], \"refuse\": {{\"error\": \"BadValue\"}}}}",
            .{sql},
        );
    const r = try selfCase(run, arena, "", declared);
    expect(r.failed == 0 and r.refusals == 2, "a declared refusal passes and counts at each point", &problems);
    const undeclared =
        try std.fmt.allocPrint(
            arena,
            "{{\"id\": \"refuse\", \"sql\": \"{s}\", \"emit\": \"values\", \"oracle\": {{\"compare\": \"values\", " ++
                "\"sql\": \"{s}\"}}}}",
            .{ sql, sql },
        );
    expect((try selfCase(run, arena, "", undeclared)).failed == 1, "an undeclared refusal fails", &problems);
    const other =
        try std.fmt.allocPrint(
            arena,
            "{{\"id\": \"refuse\", \"sql\": \"{s}\", \"emit\": \"values\", \"refuse\": {{\"error\": " ++
                "\"BadOperator\"}}}}",
            .{sql},
        );
    expect((try selfCase(run, arena, "", other)).failed == 1, "a refusal of another kind fails", &problems);
    expect((try selfCase(
        run,
        arena,
        "",
        "{\"id\": \"answered\", \"sql\": \"SELECT count(*) AS c, sum(id) AS s FROM '{path}'\", \"emit\": " ++
            "\"values\", \"refuse\": {\"error\": \"BadValue\"}}",
    )).failed == 1, "an answer where a refusal is declared fails", &problems);
    // Points that disagree fail, whichever of them is right.
    expect((try selfCase(
        run,
        arena,
        "",
        "{\"id\": \"points\", \"query\": {\"aggregate\": \"sum(id) AS s\"}, \"axes\": [{}, {\"max_memory\": " ++
            "\"not-a-size\"}], \"oracle\": {\"compare\": \"agg\", \"sql\": \"SELECT sum(id) FROM '{path}'\"}}",
    )).failed == 2, "points with different outcomes fail", &problems);
    // A second front door must give the same bytes.
    const twin =
        "{\"id\": \"same-as\", \"cli\": [\"query\", \"{path}\", \"--aggregate\", \"count(*) AS n\"], \"same\": " ++
        "\"x\", \"same_as\": [\"query\", \"{path}\", \"--aggregate\", \"";
    expect(
        (try selfCase(run, arena, "", try std.mem.concat(arena, u8, &.{ twin, "count(*) AS n\"]}" }))).failed == 0,
        "an identical second command passes",
        &problems,
    );
    expect(
        (try selfCase(run, arena, "", try std.mem.concat(arena, u8, &.{ twin, "sum(id) AS n\"]}" }))).failed == 1,
        "a second command with other bytes fails",
        &problems,
    );
    // A case with no expectation, two of them, an empty one, an empty file, a misspelled field, or a misspelled
    // mode is never a pass.
    const rejected = [_][]const u8{
        "{\"id\": \"nothing\", \"query\": {\"aggregate\": \"sum(id) AS s\"}}",
        "{\"id\": \"safe+oracle\", \"query\": {\"aggregate\": \"sum(id) AS s\"}, \"safe\": " ++
            "[\"ExceededMemoryBudget\"], \"oracle\": {\"compare\": \"agg\", \"sql\": \"SELECT 6\"}}",
        "{\"id\": \"same+oracle\", \"query\": {\"aggregate\": \"sum(id) AS s\"}, \"same\": \"x\", \"oracle\": " ++
            "{\"compare\": \"agg\", \"sql\": \"SELECT 6\"}}",
        "{\"id\": \"empty-same\", \"query\": {\"aggregate\": \"sum(id) AS s\"}, \"same\": \"\"}",
        "{\"id\": \"empty-safe\", \"query\": {\"aggregate\": \"sum(id) AS s\"}, \"safe\": []}",
        "",
    };
    for (rejected) |c| expect(
        if (selfCase(run, arena, "", c)) |_| false else |_| true,
        "an ambiguous, empty or missing expectation is rejected at load",
        &problems,
    );
    expect(
        if (selfCase(run, arena, "", "{\"id\": \"typo\", \"orcale\": {}}")) |_| false else |_| true,
        "an unknown field is rejected",
        &problems,
    );
    expect((try selfCase(
        run,
        arena,
        "",
        "{\"id\": \"mode\", \"query\": {\"aggregate\": \"sum(id) AS s\"}, \"oracle\": {\"compare\": \"agregate\"}}",
    )).failed == 1, "an unknown compare mode fails", &problems);
    if (problems != 0) {
        std.debug.print("check: self-test found {d} guards that let a wrong result through\n", .{problems});
        std.process.exit(1);
    }
    std.debug.print("check: self-test passed ({d} cases ran, {d} expected failures)\n", .{ run.total, run.failed });
}

test {
    _ = compare;
    _ = oracle_mod;
    _ = verdict;
}

test "a list variable splices into a template list and nests elsewhere" {
    var state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();
    var env: std.StringArrayHashMapUnmanaged(std.json.Value) = .empty;
    const flags = try std.json.parseFromSliceLeaky(std.json.Value, arena, "[\"-j\", \"3\"]", .{});
    try env.put(arena, "flags", flags);
    try env.put(arena, "none", try std.json.parseFromSliceLeaky(std.json.Value, arena, "[]", .{}));
    const t = try std.json.parseFromSliceLeaky(std.json.Value, arena,
        \\{"cli": ["query", "${flags}", "${none}"], "names": "${flags}", "gone": "${unset}"}
    , .{});
    const v = (try instantiate(arena, t, &env)).object;
    const cli = v.get("cli").?.array.items;
    try std.testing.expectEqual(@as(usize, 3), cli.len);
    try std.testing.expectEqualStrings("3", cli[2].string);
    try std.testing.expectEqual(@as(usize, 2), v.get("names").?.array.items.len);
    try std.testing.expect(v.get("gone") == null);
}
