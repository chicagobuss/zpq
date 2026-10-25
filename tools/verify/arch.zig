//! Portable CLI mini-matrix. Exact zpq hashes precede budgeted rival measurements.
const std = @import("std");
const builtin = @import("builtin");
const A = std.mem.Allocator;
const Case = struct {
    id: []const u8,
    file: []const u8,
    sql: []const u8,
    kind: []const u8,
    output: []const u8,
    codec: []const u8 = "snappy",
    tolerance: f64 = 1e-9,
    zpq_args: ?[]const []const u8 = null,
    blob_keys: bool = false,
};
const Sample = struct { engine: []const u8, mode: []const u8, rep: usize, ms: ?f64, sha256: []const u8 };
const EngineResult = struct {
    engine: []const u8,
    reps: usize,
    median_ms: ?f64,
    stop_reason: []const u8,
    first_pass_reps: usize,
};
const Query = struct {
    id: []const u8,
    sql: []const u8,
    hashes: std.ArrayList([]const u8) = .empty,
    samples: std.ArrayList(Sample) = .empty,
    timings: std.ArrayList(EngineResult) = .empty,
    correctness: []const u8 = "pending",
    error_text: ?[]const u8 = null,
    polars: []const u8 = "pending",
    zpq_over_best_rival: ?f64 = null,

    pub fn jsonStringify(self: Query, writer: anytype) !void {
        try writer.write(.{
            .id = self.id,
            .sql = self.sql,
            .hashes = self.hashes.items,
            .samples = self.samples.items,
            .timings = self.timings.items,
            .correctness = self.correctness,
            .error_text = self.error_text,
            .polars = self.polars,
            .zpq_over_best_rival = self.zpq_over_best_rival,
        });
    }
};
const Book = struct {
    schema: usize = 2,
    tier: []const u8 = "arch",
    sha: []const u8,
    manifest_sha256: []const u8,
    binary_sha256: []const u8,
    duckdb_sha256: []const u8,
    target: []const u8,
    cpu: []const u8 = "baseline",
    optimize: []const u8 = "ReleaseFast",
    strip: bool = true,
    runner_zig: []const u8 = "0.17.0",
    threads: ?usize,
    thread_policy: []const u8,
    metric: []const u8 = "fresh process through full capture/reap; imports included for Polars",
    cache_policy: []const u8 = "first pass has uncontrolled file cache; hot samples read input first",
    host: struct {
        hostname: []const u8,
        uname: []const u8,
        cpu: []const u8,
        load_before: []const u8,
        top_before: []const u8,
        load_after: []const u8 = "",
        resources: []const u8,
    },
    queries: []Query,
    hash_agreement: []const u8 = "pending",
    budget_seconds: usize,
    wall_seconds: f64 = 0,
    stop_reason: []const u8 = "running",
    exit: ?u8 = null,
};
const Opt = struct {
    data: []const u8 = "data",
    binary: []const u8 = "./zpq",
    duckdb: []const u8 = "./duckdb",
    duck_sha: []const u8 = "",
    polars: ?[]const u8 = null,
    polars_script: []const u8 = "arch_polars.py",
    revision: []const u8 = "",
    target: []const u8 = "",
    out: []const u8 = "",
    reference: ?[]const u8 = null,
    threads: ?usize = null,
    reps: usize = 3,
    budget: usize = 90,
    hash_only: bool = false,
    tiny: bool = false,
    resources: []const u8 = "external scope",
};
const Runner = struct {
    a: A,
    io: std.Io,
    opt: Opt,
    started: std.Io.Timestamp,
    deadline: std.Io.Clock.Timestamp,
    book: Book,
    raw_cases: []std.json.Value,
    cases: []Case,
    reference: ?std.json.Value,

    fn text(self: *Runner, comptime fmt: []const u8, args: anytype) ![]const u8 {
        return std.fmt.allocPrint(self.a, fmt, args);
    }
    fn capture(self: *Runner, argv: []const []const u8) !std.process.RunResult {
        return std.process.run(self.a, self.io, .{
            .argv = argv,
            .timeout = .{ .deadline = self.deadline },
            .stdout_limit = .limited(64 << 20),
            .stderr_limit = .limited(1 << 20),
        });
    }
    fn command(self: *Runner, argv: []const []const u8) ![]const u8 {
        const result = try self.capture(argv);
        if (!good(result.term)) {
            std.debug.print("command failed {s}: {s}\n", .{ argv[0], result.stderr });
            return error.CommandFailed;
        }
        return std.mem.trim(u8, result.stdout, " \r\n\t");
    }
    fn hash(self: *Runner, path: []const u8) ![]const u8 {
        const value = if (builtin.os.tag == .macos)
            try self.command(&.{ "shasum", "-a", "256", path })
        else
            try self.command(&.{ "sha256sum", path });
        if (value.len < 64) return error.BadHash;
        return value[0..64];
    }
    fn read(self: *Runner, path: []const u8) ![]const u8 {
        return std.Io.Dir.cwd().readFileAlloc(self.io, path, self.a, .limited(64 << 20));
    }
    fn write(self: *Runner, path: []const u8, bytes: []const u8) !void {
        try std.Io.Dir.cwd().writeFile(self.io, .{ .sub_path = path, .data = bytes });
    }
    fn save(self: *Runner) !void {
        self.book.wall_seconds = elapsed(self.started, self.io) / 1000;
        const json = try std.json.Stringify.valueAlloc(self.a, self.book, .{ .whitespace = .indent_2 });
        const path = try self.text("{s}/summary.json", .{self.opt.out});
        const temp = try self.text("{s}.tmp", .{path});
        try self.write(temp, json);
        try std.Io.Dir.cwd().rename(temp, std.Io.Dir.cwd(), path, self.io);
    }
    fn casePath(self: *Runner, c: Case) ![]const u8 {
        return self.text("{s}/{s}", .{ self.opt.data, c.file });
    }
    fn caseSql(self: *Runner, c: Case) ![]const u8 {
        const source = try self.text("'{s}'", .{try escape(self.a, try self.casePath(c))});
        return replace(self.a, c.sql, "{T}", source);
    }
    fn duck(self: *Runner, sql: []const u8) ![]const u8 {
        const prefix = if (self.opt.threads) |n| try self.text("SET threads={d}; ", .{n}) else "";
        return self.command(&.{
            self.opt.duckdb,                           "-no-agent", "-init", "/dev/null", "-json", "-c",
            try self.text("{s}{s}", .{ prefix, sql }),
        });
    }
    fn budgetLeft(self: *Runner) bool {
        return elapsed(self.started, self.io) + 1000 < @as(f64, @floatFromInt(self.opt.budget * 1000));
    }
    fn answer(self: *Runner, c: Case, engine: []const u8, mode: []const u8, rep: usize) ![]const u8 {
        const suffix: []const u8 = if (eql(c.output, "parquet")) "parquet" else "json";
        return self.text("{s}/{s}-{s}-{s}-{d}.{s}", .{ self.opt.out, c.id, engine, mode, rep, suffix });
    }
    fn execute(self: *Runner, i: usize, engine: []const u8, mode: []const u8, rep: usize) ![]const u8 {
        const c = self.cases[i];
        const output = try self.answer(c, engine, mode, rep);
        if (eql(mode, "hot")) {
            const source = try self.casePath(c);
            const allocator = std.heap.page_allocator;
            const bytes = try std.Io.Dir.cwd().readFileAlloc(self.io, source, allocator, .limited(64 << 20));
            allocator.free(bytes);
        }
        var argv: std.ArrayList([]const u8) = .empty;
        if (eql(engine, "zpq")) {
            if (c.zpq_args) |args| {
                try argv.appendSlice(self.a, &.{ self.opt.binary, "query", try self.casePath(c) });
                try argv.appendSlice(self.a, args);
            } else try argv.appendSlice(self.a, &.{ self.opt.binary, "query", "--query", try self.caseSql(c) });
            if (self.opt.threads) |n| try argv.appendSlice(self.a, &.{ "-j", try self.text("{d}", .{n}) });
            if (eql(c.output, "parquet")) try argv.appendSlice(self.a, &.{ "-o", output, "--codec", c.codec });
        } else if (eql(engine, "duckdb")) {
            var sql = try self.caseSql(c);
            if (eql(c.output, "parquet")) {
                const level: []const u8 = if (eql(c.codec, "zstd")) ", COMPRESSION_LEVEL 3" else "";
                sql = try self.text("COPY ({s}) TO '{s}' (FORMAT PARQUET, COMPRESSION {s}{s})", .{
                    sql, try escape(self.a, output), c.codec, level,
                });
            }
            const prefix = if (self.opt.threads) |n| try self.text("SET threads={d}; ", .{n}) else "";
            try argv.appendSlice(self.a, &.{
                self.opt.duckdb,                           "-no-agent", "-init", "/dev/null", "-json", "-c",
                try self.text("{s}{s}", .{ prefix, sql }),
            });
        } else {
            const spec = try self.text("{s}/{s}-polars-spec.json", .{ self.opt.out, c.id });
            const body = .{ .case = self.raw_cases[i], .input = try self.casePath(c) };
            try self.write(spec, try std.json.Stringify.valueAlloc(self.a, body, .{}));
            try argv.appendSlice(self.a, &.{ self.opt.polars.?, self.opt.polars_script, spec, output });
        }
        const start = std.Io.Clock.awake.now(self.io);
        const result = try self.capture(argv.items);
        if (!good(result.term)) {
            self.book.queries[i].error_text = result.stderr;
            return error.QueryFailed;
        }
        if (!eql(c.output, "parquet") and !eql(engine, "polars")) try self.write(output, result.stdout);
        if (eql(c.output, "parquet") and !eql(engine, "zpq")) {
            const file = try std.Io.Dir.cwd().openFile(self.io, output, .{});
            defer file.close(self.io);
            try file.sync(self.io);
            var dir = try std.Io.Dir.cwd().openDir(self.io, self.opt.out, .{ .iterate = true });
            defer dir.close(self.io);
            try (std.Io.File{ .handle = dir.handle, .flags = .{ .nonblocking = false } }).sync(self.io);
        }
        const ms = elapsed(start, self.io);
        const digest = try self.hash(output);
        if (eql(engine, "zpq")) {
            var q = &self.book.queries[i];
            var expected: ?[]const u8 = if (q.hashes.items.len > 0) q.hashes.items[0] else null;
            if (self.reference) |ref| {
                for (ref.object.get("queries").?.array.items) |other| {
                    if (eql(other.object.get("id").?.string, c.id)) {
                        const hashes = other.object.get("hashes").?.array.items;
                        if (hashes.len == 0) return error.BadReference;
                        expected = hashes[0].string;
                        break;
                    }
                }
                if (expected == null) return error.BadReference;
            }
            try q.hashes.append(self.a, digest);
            if (expected) |want| if (!eql(want, digest)) {
                self.book.hash_agreement = "MISMATCH";
                q.correctness = "HASH_MISMATCH";
                q.error_text = try self.text("expected {s}, got {s}; exact output {s}", .{ want, digest, output });
                try self.save();
                std.debug.print("HASH_MISMATCH {s} {s}\n", .{ c.id, q.error_text.? });
                return error.HashMismatch;
            };
        }
        try self.book.queries[i].samples.append(self.a, .{
            .engine = engine,
            .mode = mode,
            .rep = rep,
            .ms = if (self.opt.hash_only) null else ms,
            .sha256 = digest,
        });
        return output;
    }
    fn validate(self: *Runner, i: usize, engine: []const u8, output: []const u8) !bool {
        const c = self.cases[i];
        const zpath = try self.answer(c, "zpq", "first-pass", 0);
        if (eql(c.output, "parquet")) {
            const source = try self.caseSql(c);
            const qpath = try escape(self.a, output);
            const sql = try self.text("SELECT EXISTS ((SELECT * FROM '{s}' EXCEPT ALL ({s})) UNION ALL " ++
                "(({s}) EXCEPT ALL SELECT * FROM '{s}')) AS bad", .{ qpath, source, source, qpath });
            const parsed = try std.json.parseFromSlice(std.json.Value, self.a, try self.duck(sql), .{});
            return !parsed.value.array.items[0].object.get("bad").?.bool;
        }
        const describe_sql = try self.text("DESCRIBE {s}", .{try self.caseSql(c)});
        const description = try std.json.parseFromSlice(std.json.Value, self.a, try self.duck(describe_sql), .{});
        const types = description.value.array.items;
        const zrows = try parseRows(self.a, try self.read(zpath), true);
        const rrows = try parseRows(self.a, try self.read(output), false);
        try typeRows(zrows, types);
        try typeRows(rrows, types);
        if (c.blob_keys and eql(engine, "duckdb")) try decodeBlobRows(self.a, rrows);
        if (zrows.len != rrows.len) return false;
        if (eql(c.kind, "top")) {
            const original = try self.caseSql(c);
            const order = std.mem.indexOf(u8, original, " ORDER BY ") orelse return error.BadTopQuery;
            const truth = try parseRows(self.a, try self.duck(original[0..order]), false);
            try typeRows(truth, types);
            if (c.blob_keys) try decodeBlobRows(self.a, truth);
            for ([_][]const []const std.json.Value{ zrows, rrows }) |answer_rows| {
                for (answer_rows, 0..) |row, row_i| {
                    for (answer_rows[0..row_i]) |prior| {
                        if (sameRow(prior[0 .. prior.len - 1], row[0 .. row.len - 1], 0)) return false;
                    }
                    var found = false;
                    for (truth) |full| if (sameRow(full, row, c.tolerance)) {
                        found = true;
                        break;
                    };
                    if (!found) return false;
                }
            }
            // DuckDB supplies the ranked counts; tie membership may differ between engines.
            for (zrows, rrows) |z, r| if (!same(z[z.len - 1], r[r.len - 1], c.tolerance)) return false;
            return true;
        }
        const used = try self.a.alloc(bool, rrows.len);
        @memset(used, false);
        for (zrows) |z| {
            var found = false;
            for (rrows, 0..) |r, j| if (!used[j] and sameRow(z, r, c.tolerance)) {
                used[j] = true;
                found = true;
                break;
            };
            if (!found) return false;
        }
        return true;
    }
};

pub fn main(init: std.process.Init) void {
    run(init) catch |err| {
        std.debug.print("arch blocked: {s}\n", .{@errorName(err)});
        std.process.exit(2);
    };
}

fn run(init: std.process.Init) !void {
    var arena = std.heap.ArenaAllocator.init(init.gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const io = init.io;
    const started = std.Io.Clock.awake.now(io);
    var opt: Opt = .{};
    var it = std.process.Args.Iterator.init(init.minimal.args);
    _ = it.next();
    while (it.next()) |arg| {
        if (eql(arg, "--hash-only")) {
            opt.hash_only = true;
            continue;
        }
        if (eql(arg, "--tiny-perf")) {
            opt.tiny = true;
            continue;
        }
        const val = it.next() orelse return error.BadArgs;
        if (eql(arg, "--data")) {
            opt.data = val;
        } else if (eql(arg, "--binary")) {
            opt.binary = val;
        } else if (eql(arg, "--duckdb")) {
            opt.duckdb = val;
        } else if (eql(arg, "--duckdb-sha256")) {
            opt.duck_sha = val;
        } else if (eql(arg, "--polars-python")) {
            opt.polars = val;
        } else if (eql(arg, "--polars-script")) {
            opt.polars_script = val;
        } else if (eql(arg, "--revision")) {
            opt.revision = val;
        } else if (eql(arg, "--target")) {
            opt.target = val;
        } else if (eql(arg, "--out")) {
            opt.out = val;
        } else if (eql(arg, "--reference")) {
            opt.reference = val;
        } else if (eql(arg, "--resources")) {
            opt.resources = val;
        } else if (eql(arg, "--threads")) {
            opt.threads = try std.fmt.parseInt(usize, val, 10);
        } else if (eql(arg, "--reps")) {
            opt.reps = try std.fmt.parseInt(usize, val, 10);
        } else if (eql(arg, "--budget-seconds")) {
            opt.budget = try std.fmt.parseInt(usize, val, 10);
        } else return error.BadArgs;
    }
    if (opt.out.len == 0 or opt.revision.len == 0 or opt.duck_sha.len != 64 or opt.budget > 120 or opt.budget == 0)
        return error.BadArgs;
    if (opt.threads != null and opt.threads.? == 0 or opt.reps == 0) return error.BadArgs;
    var runner: Runner = undefined;
    runner.a = a;
    runner.io = io;
    runner.opt = opt;
    runner.started = started;
    runner.deadline = started.withClock(.awake).addDuration(.{
        .clock = .awake,
        .raw = .fromSeconds(@intCast(opt.budget)),
    });
    const summary_path = try runner.text("{s}/summary.json", .{opt.out});
    const existing = std.Io.Dir.cwd().openFile(io, summary_path, .{});
    if (existing) |file| {
        file.close(io);
        return error.SummaryExists;
    } else |_| {}
    try std.Io.Dir.cwd().createDirPath(io, opt.out);
    const manifest_path = try runner.text("{s}/manifest.json", .{opt.data});
    const manifest = try std.json.parseFromSlice(std.json.Value, a, try runner.read(manifest_path), .{});
    const parsed_cases = try std.json.parseFromValue([]Case, a, manifest.value.object.get("cases").?, .{
        .ignore_unknown_fields = true,
    });
    runner.cases = parsed_cases.value;
    runner.raw_cases = manifest.value.object.get("cases").?.array.items;
    runner.reference = if (opt.reference) |path|
        (try std.json.parseFromSlice(std.json.Value, a, try runner.read(path), .{})).value
    else
        null;
    const manifest_sha = try runner.hash(manifest_path);
    if (runner.reference) |ref| {
        if (!eql(ref.object.get("sha").?.string, opt.revision) or
            !eql(ref.object.get("manifest_sha256").?.string, manifest_sha)) return error.BadReference;
    }
    var file_it = manifest.value.object.get("files").?.object.iterator();
    while (file_it.next()) |entry| {
        const path = try runner.text("{s}/{s}", .{ opt.data, entry.key_ptr.* });
        if (!eql(try runner.hash(path), entry.value_ptr.object.get("sha256").?.string)) return error.InputHashMismatch;
    }
    if (!eql(try runner.hash(opt.duckdb), opt.duck_sha)) return error.DuckDbHashMismatch;
    const version = try runner.command(&.{ opt.binary, "--version" });
    if (std.mem.indexOf(u8, version, opt.revision) == null) return error.WrongEngineRevision;
    const queries = try a.alloc(Query, runner.cases.len);
    for (queries, runner.cases) |*q, c| q.* = .{ .id = c.id, .sql = c.sql };
    const cpu_probe = "if command -v lscpu >/dev/null; then lscpu; " ++
        "else sysctl -a 2>/dev/null | head -80; fi";
    const top_probe = "if [ \"$(uname -s)\" = Darwin ]; then top -l 1 -n 5; " ++
        "else top -b -n 1 -w 180 | head -14; fi";
    runner.book = .{
        .sha = opt.revision,
        .manifest_sha256 = manifest_sha,
        .binary_sha256 = try runner.hash(opt.binary),
        .duckdb_sha256 = opt.duck_sha,
        .target = opt.target,
        .threads = opt.threads,
        .thread_policy = if (opt.threads == null) "engine defaults" else "explicit host cap",
        .host = .{
            .hostname = try runner.command(&.{"hostname"}),
            .uname = try runner.command(&.{ "uname", "-a" }),
            .cpu = try runner.command(&.{ "sh", "-c", cpu_probe }),
            .load_before = try runner.command(&.{"uptime"}),
            .top_before = try runner.command(&.{ "sh", "-c", top_probe }),
            .resources = opt.resources,
        },
        .queries = queries,
        .budget_seconds = opt.budget,
    };
    try runner.save();
    const code = runMatrix(&runner) catch |err| blk: {
        runner.book.stop_reason = @errorName(err);
        break :blk @as(u8, if (err == error.HashMismatch) 3 else if (err == error.Timeout) 2 else 1);
    };
    runner.book.exit = code;
    try finalizeTimings(&runner);
    runner.book.host.load_after = runner.command(&.{"uptime"}) catch "budget exhausted";
    try runner.save();
    std.debug.print("summary {s} exit={d} wall={d:.2}s stop={s}\n", .{
        summary_path, code, runner.book.wall_seconds, runner.book.stop_reason,
    });
    std.process.exit(code);
}

fn runMatrix(r: *Runner) !u8 {
    // The hash pass is complete before any rival process or perf decision.
    for (r.cases, 0..) |c, i| {
        _ = try r.execute(i, "zpq", "first-pass", 0);
        std.debug.print("HASH {s} {s}\n", .{ c.id, r.book.queries[i].hashes.items[0] });
        try r.save();
    }
    r.book.hash_agreement = if (r.reference == null) "reference established" else "all match reference";
    try r.save();
    if (r.opt.hash_only) {
        for (r.book.queries) |*q| {
            q.correctness = "hash-only";
            q.polars = "not requested";
        }
        r.book.stop_reason = "hash-only host";
        return 0;
    }
    var failed = false;
    for (r.cases, 0..) |c, i| {
        const output = try r.execute(i, "duckdb", "first-pass", 0);
        const zoutput = try r.answer(c, "zpq", "first-pass", 0);
        const valid_z = if (eql(c.output, "parquet")) try r.validate(i, "zpq", zoutput) else true;
        const valid_d = try r.validate(i, "duckdb", output);
        r.book.queries[i].correctness = if (valid_z and valid_d) "pass" else "FAIL";
        failed = failed or !valid_z or !valid_d;
        if (r.opt.polars != null) {
            const poutput = r.execute(i, "polars", "first-pass", 0) catch |err| {
                if (err == error.Timeout) return err;
                r.book.queries[i].polars = "skipped: unavailable query/runtime";
                try r.save();
                continue;
            };
            const valid_p = try r.validate(i, "polars", poutput);
            r.book.queries[i].polars = if (valid_p) "pass" else "FAIL";
            failed = failed or !valid_p;
        } else r.book.queries[i].polars = "skipped: Polars unavailable";
        std.debug.print("CHECK {s} {s} polars={s}\n", .{
            c.id, r.book.queries[i].correctness, r.book.queries[i].polars,
        });
        try r.save();
    }
    var budget_stop = false;
    rounds: for (0..r.opt.reps) |rep| {
        for (r.cases, 0..) |c, i| {
            if (r.opt.tiny and !eql(c.id, "card100k.q13_point") and !eql(c.id, "pdsh.q6")) continue;
            for (0..3) |offset| {
                const engine = ([_][]const u8{ "zpq", "duckdb", "polars" })[(rep + offset) % 3];
                if (eql(engine, "polars") and !eql(r.book.queries[i].polars, "pass")) continue;
                if (!r.budgetLeft()) {
                    budget_stop = true;
                    break :rounds;
                }
                const output = r.execute(i, engine, "hot", rep) catch |err| {
                    if (err == error.Timeout) {
                        budget_stop = true;
                        break :rounds;
                    }
                    return err;
                };
                if (eql(c.output, "parquet") and !eql(engine, "zpq")) {
                    if (!try r.validate(i, engine, output)) {
                        failed = true;
                        r.book.queries[i].correctness = "FAIL: repeated rival Parquet output";
                    }
                }
            }
        }
        try r.save();
    }
    r.book.stop_reason = if (budget_stop)
        "wall time budget (independent of measured ratios)"
    else
        "fixed rep target reached";
    return if (failed) 1 else 0;
}

fn finalizeTimings(r: *Runner) !void {
    for (r.book.queries) |*q| {
        var zpq_ms: ?f64 = null;
        var best: f64 = std.math.inf(f64);
        for ([_][]const u8{ "zpq", "duckdb", "polars" }) |engine| {
            var values: std.ArrayList(f64) = .empty;
            var first_pass_reps: usize = 0;
            for (q.samples.items) |sample| {
                if (eql(sample.engine, engine) and eql(sample.mode, "first-pass")) first_pass_reps += 1;
            }
            for (q.samples.items) |sample| if (eql(sample.engine, engine) and eql(sample.mode, "hot"))
                try values.append(r.a, sample.ms orelse continue);
            std.mem.sort(f64, values.items, {}, std.sort.asc(f64));
            const med = median(values.items);
            try q.timings.append(r.a, .{
                .engine = engine,
                .reps = values.items.len,
                .first_pass_reps = first_pass_reps,
                .median_ms = med,
                .stop_reason = reason: {
                    if (values.items.len == r.opt.reps) break :reason "fixed rep target reached";
                    if (r.opt.hash_only) break :reason "hash-only host";
                    if (eql(engine, "polars") and std.mem.startsWith(u8, q.polars, "skipped:"))
                        break :reason "Polars skipped";
                    if (r.opt.tiny) break :reason "host tiny perf policy";
                    break :reason r.book.stop_reason;
                },
            });
            if (med) |ms| {
                if (eql(engine, "zpq")) zpq_ms = ms else best = @min(best, ms);
            }
        }
        if (zpq_ms != null and !std.math.isInf(best)) q.zpq_over_best_rival = zpq_ms.? / best;
    }
}

fn elapsed(start: std.Io.Timestamp, io: std.Io) f64 {
    return @as(f64, @floatFromInt(start.untilNow(io, .awake).nanoseconds)) / 1e6;
}
fn good(term: std.process.Child.Term) bool {
    return switch (term) {
        .exited => |c| c == 0,
        else => false,
    };
}
fn eql(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}
fn replace(a: A, source: []const u8, needle: []const u8, with: []const u8) ![]const u8 {
    var list: std.ArrayList(u8) = .empty;
    var split = std.mem.splitSequence(u8, source, needle);
    try list.appendSlice(a, split.next().?);
    while (split.next()) |part| {
        try list.appendSlice(a, with);
        try list.appendSlice(a, part);
    }
    return list.toOwnedSlice(a);
}
fn escape(a: A, value: []const u8) ![]const u8 {
    return replace(a, value, "'", "''");
}
fn parseRows(a: A, text: []const u8, ndjson: bool) ![]const []const std.json.Value {
    var rows: std.ArrayList([]const std.json.Value) = .empty;
    if (ndjson) {
        var lines = std.mem.tokenizeScalar(u8, text, '\n');
        while (lines.next()) |line| {
            const v = (try std.json.parseFromSlice(std.json.Value, a, line, .{ .parse_numbers = false })).value;
            try rows.append(a, try a.dupe(std.json.Value, v.object.values()));
        }
    } else {
        const v = (try std.json.parseFromSlice(std.json.Value, a, text, .{ .parse_numbers = false })).value;
        for (v.array.items) |row| try rows.append(a, if (row == .object)
            try a.dupe(std.json.Value, row.object.values())
        else
            row.array.items);
    }
    for (rows.items) |row| for (@constCast(row)) |*v| {
        if (v.* == .number_string) {
            if (std.fmt.parseInt(i64, v.number_string, 10)) |n| v.* = .{ .integer = n } else |_| {}
        }
    };
    return rows.toOwnedSlice(a);
}
fn typeRows(rows: []const []const std.json.Value, types: []const std.json.Value) !void {
    for (rows) |row| {
        if (row.len != types.len) return error.BadRowWidth;
        for (@constCast(row), types) |*v, column| {
            if (v.* == .null) continue;
            const t = column.object.get("column_type").?.string;
            const floating = eql(t, "DOUBLE") or eql(t, "FLOAT") or eql(t, "REAL");
            const decimal = std.mem.startsWith(u8, t, "DECIMAL");
            const integer = std.mem.endsWith(u8, t, "INT") or eql(t, "INTEGER") or eql(t, "UINTEGER");
            const value = v.*;
            if (floating) {
                v.* = .{ .float = switch (value) {
                    .integer => |n| @floatFromInt(n),
                    .number_string, .string => |n| try std.fmt.parseFloat(f64, n),
                    .float => |n| n,
                    else => return error.BadNumericType,
                } };
            } else if ((decimal or integer) and v.* == .string) {
                v.* = .{ .number_string = value.string };
            }
        }
    }
}
fn decodeBlobRows(a: A, rows: []const []const std.json.Value) !void {
    for (rows) |row| {
        for (@constCast(row)) |*v| {
            if (v.* != .string) continue;
            const s = v.string;
            var bytes: std.ArrayList(u8) = .empty;
            var i: usize = 0;
            while (i < s.len) {
                if (i + 3 < s.len and s[i] == '\\' and s[i + 1] == 'x') {
                    try bytes.append(a, std.fmt.parseInt(u8, s[i + 2 .. i + 4], 16) catch return error.BadBlobEscape);
                    i += 4;
                } else {
                    try bytes.append(a, s[i]);
                    i += 1;
                }
            }
            v.* = .{ .string = try bytes.toOwnedSlice(a) };
        }
    }
}
const ExactNumber = struct {
    coefficient: i256,
    exponent: i32,

    fn normalized(n: i256, e: i32) ExactNumber {
        var coefficient = n;
        var exponent = e;
        if (coefficient == 0) return .{ .coefficient = 0, .exponent = 0 };
        while (@rem(coefficient, 10) == 0) {
            coefficient = @divTrunc(coefficient, 10);
            exponent = std.math.add(i32, exponent, 1) catch return .{ .coefficient = n, .exponent = e };
        }
        return .{ .coefficient = coefficient, .exponent = exponent };
    }
    fn parse(v: std.json.Value) ?ExactNumber {
        if (v == .integer) return normalized(v.integer, 0);
        if (v != .number_string) return null;
        const s = v.number_string;
        const end = std.mem.indexOfAny(u8, s, "eE") orelse s.len;
        var exponent: i32 = if (end < s.len) std.fmt.parseInt(i32, s[end + 1 ..], 10) catch return null else 0;
        var coefficient: i256 = 0;
        var fraction = false;
        var digits: usize = 0;
        for (s[0..end], 0..) |c, i| {
            if (i == 0 and (c == '-' or c == '+')) continue;
            if (c == '.' and !fraction) {
                fraction = true;
                continue;
            }
            if (c < '0' or c > '9') return null;
            coefficient = std.math.mul(i256, coefficient, 10) catch return null;
            coefficient = std.math.add(i256, coefficient, c - '0') catch return null;
            if (fraction) exponent = std.math.sub(i32, exponent, 1) catch return null;
            digits += 1;
        }
        if (digits == 0) return null;
        if (s[0] == '-') coefficient = -coefficient;
        return normalized(coefficient, exponent);
    }
};
fn same(a: std.json.Value, b: std.json.Value, tolerance: f64) bool {
    if (a == .null or b == .null) return a == .null and b == .null;
    if (a == .string and b == .string) return eql(a.string, b.string);
    if (a == .bool and b == .bool) return a.bool == b.bool;
    if (a == .float and b == .float) {
        const x = a.float;
        const y = b.float;
        return x == y or (std.math.isNan(x) and std.math.isNan(y)) or
            (std.math.isFinite(x) and std.math.isFinite(y) and @abs(x - y) <= tolerance * @max(1, @abs(x), @abs(y)));
    }
    if (ExactNumber.parse(a)) |x| if (ExactNumber.parse(b)) |y|
        return x.coefficient == y.coefficient and x.exponent == y.exponent;
    return false;
}
fn sameRow(a: []const std.json.Value, b: []const std.json.Value, tolerance: f64) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| if (!same(x, y, tolerance)) return false;
    return true;
}
fn median(values: []const f64) ?f64 {
    if (values.len == 0) return null;
    if (values.len % 2 == 1) return values[values.len / 2];
    return (values[values.len / 2 - 1] + values[values.len / 2]) / 2;
}
test "raw JSON order is preserved and rival rows compare by values" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const z = try parseRows(a, "{\"k\":8,\"c\":2}\n{\"k\":1,\"c\":1}\n", true);
    try std.testing.expectEqual(@as(i64, 8), z[0][0].integer);
    const d = try parseRows(a, "[{\"other\":8,\"n\":2}]", false);
    try std.testing.expect(sameRow(z[0], d[0], 0));
    try std.testing.expect(!sameRow(z[1], d[0], 0));
}
test "numeric tolerance and medians" {
    try std.testing.expect(same(.{ .float = 1.00001 }, .{ .float = 1.00002 }, 1e-4));
    try std.testing.expect(!same(.{ .integer = 1 }, .{ .integer = 2 }, 1e-9));
    try std.testing.expectEqual(@as(?f64, 2), median(&.{ 1, 2, 3 }));
    try std.testing.expectEqual(@as(?f64, 2.5), median(&.{ 1, 2, 3, 4 }));
}
test "blob decoding is confined to rival comparisons" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const z = try parseRows(a, "{\"k\":\"вед\",\"c\":2}\n", true);
    const d = try parseRows(a, "[[\"\\\\xD0\\\\xB2\\\\xD0\\\\xB5\\\\xD0\\\\xB4\",2]]", false);
    try std.testing.expect(!sameRow(z[0], d[0], 0));
    try decodeBlobRows(a, d);
    try std.testing.expect(sameRow(z[0], d[0], 0));
}
test "budget stop reports actual reps and null for missing samples" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var q = [_]Query{.{ .id = "q", .sql = "SELECT count(*)", .polars = "pass" }};
    try q[0].samples.append(a, .{
        .engine = "zpq",
        .mode = "hot",
        .rep = 0,
        .ms = 0.01,
        .sha256 = "hash",
    });
    var r: Runner = undefined;
    r.a = a;
    r.opt = .{};
    r.book.queries = &q;
    r.book.stop_reason = "wall time budget";
    try finalizeTimings(&r);
    try std.testing.expectEqual(@as(usize, 1), q[0].timings.items[0].reps);
    try std.testing.expectEqualStrings("wall time budget", q[0].timings.items[0].stop_reason);
    try std.testing.expectEqual(@as(?f64, null), q[0].timings.items[1].median_ms);
    try std.testing.expectEqual(@as(?f64, null), q[0].zpq_over_best_rival);
}

test "integer precision and text bytes cannot use numeric tolerance" {
    try std.testing.expect(!same(.{ .integer = 9007199254740993 }, .{ .integer = 9007199254740992 }, 1e-4));
    try std.testing.expect(!same(.{ .string = "01001" }, .{ .string = "1001" }, 1e-4));
    try std.testing.expect(!same(
        .{ .number_string = "12345678901234567890123456789.01" },
        .{ .number_string = "12345678901234567890123456789.02" },
        1e-4,
    ));
    try std.testing.expect(same(.{ .number_string = "1.10" }, .{ .number_string = "1.1" }, 0));
}

test "schema preserves decimal tokens and restricts float tolerance to float columns" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const schema = "[{\"column_type\":\"DECIMAL(38,2)\"}," ++
        "{\"column_type\":\"VARCHAR\"},{\"column_type\":\"DOUBLE\"}]";
    const types = (try std.json.parseFromSlice(std.json.Value, a, schema, .{})).value.array.items;
    const z = try parseRows(a, "{\"d\":12345678901234567890123456789.01,\"s\":\"01001\",\"f\":1.00001}\n", true);
    const d = try parseRows(a, "[[\"12345678901234567890123456789.01\",\"01001\",1.00002]]", false);
    try typeRows(z, types);
    try typeRows(d, types);
    try std.testing.expect(sameRow(z[0], d[0], 1e-4));
    @constCast(d[0])[0] = .{ .number_string = "12345678901234567890123456789.02" };
    try std.testing.expect(!sameRow(z[0], d[0], 1e-4));
    @constCast(d[0])[0] = z[0][0];
    @constCast(d[0])[1] = .{ .string = "1001" };
    try std.testing.expect(!sameRow(z[0], d[0], 1e-4));
}
