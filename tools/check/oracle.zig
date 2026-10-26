//! The one oracle: the DuckDB 2.0 CLI, one process per question, with an optional answer cache.
//!
//! DuckDB runs with no init file (`-no-init`: ~/.duckdbrc could change any default) and only TZ, LC_ALL=C and PATH
//! in its environment. A cache key is that configuration, the DuckDB binary's sha256, the SQL verbatim, and the
//! sha256 of every file a quoted literal in it names (a glob by its sorted matches). A named file that cannot be
//! hashed makes the question uncacheable. An entry is used only when its stored key matches exactly, its answer
//! matches its stored digest, and the answer decodes; anything else is a miss that asks DuckDB again. Commit runs
//! use the cache; full runs pass no cache directory.

const std = @import("std");
const compare = @import("compare.zig");

pub const Answer = struct {
    names: []const []const u8 = &.{},
    types: []const []const u8 = &.{},
    rows: []const std.json.ObjectMap = &.{},
    /// DuckDB refused: its stderr.
    refused: ?[]const u8 = null,
};

pub const Oracle = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    duckdb: []const u8,
    duckdb_sha: [64]u8,
    tz: []const u8,
    cache_dir: ?[]const u8,
    hits: usize = 0,
    misses: usize = 0,

    /// `tz` is the zone DuckDB runs in: UTC when the caller has none, never the host's unstated default.
    pub fn init(
        gpa: std.mem.Allocator,
        io: std.Io,
        duckdb: []const u8,
        cache_dir: ?[]const u8,
        tz: []const u8,
    ) !Oracle {
        return .{
            .gpa = gpa,
            .io = io,
            .duckdb = duckdb,
            .duckdb_sha = try fileSha(gpa, io, duckdb),
            .tz = if (tz.len == 0) "UTC" else tz,
            .cache_dir = cache_dir,
        };
    }

    fn run(self: *Oracle, args: []const []const u8) !Captured {
        var env = std.process.Environ.Map.init(self.gpa);
        defer env.deinit();
        try env.put("TZ", self.tz);
        try env.put("LC_ALL", "C");
        if (std.c.getenv("PATH")) |path| try env.put("PATH", std.mem.span(path));
        var argv: std.ArrayList([]const u8) = .empty;
        defer argv.deinit(self.gpa);
        try argv.appendSlice(self.gpa, &.{ self.duckdb, "-no-agent", "-no-init" });
        try argv.appendSlice(self.gpa, args);
        return captureEnv(self.gpa, self.io, argv.items, &env);
    }

    /// Run `sql`, a single SELECT, and its DESCRIBE. Results live in `arena`.
    pub fn ask(self: *Oracle, arena: std.mem.Allocator, sql: []const u8) !Answer {
        return self.askAfter(arena, "", sql);
    }

    /// `ask` after `setup` statements (views, macros, settings) that print nothing.
    pub fn askAfter(self: *Oracle, arena: std.mem.Allocator, setup: []const u8, sql: []const u8) !Answer {
        const raw = try self.rawAnswer(arena, setup, sql);
        return decode(arena, raw);
    }

    /// Execute statements for their side effects (fixture COPY). Never cached.
    pub fn exec(self: *Oracle, sql: []const u8) !void {
        const out = try self.run(&.{ "-c", sql });
        defer self.gpa.free(out.stdout);
        defer self.gpa.free(out.stderr);
        if (out.code != 0) {
            std.debug.print("duckdb: {s}\n", .{out.stderr});
            return error.DuckDb;
        }
    }

    fn rawAnswer(self: *Oracle, arena: std.mem.Allocator, setup: []const u8, sql: []const u8) ![]const u8 {
        // Hashing every named file is the cost of a key: an uncached run never pays it.
        const material = if (self.cache_dir == null) null else try self.keyMaterialAfter(arena, setup, sql);
        var path: ?[]const u8 = null;
        if (self.cache_dir) |dir| if (material) |m| {
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(m, &digest, .{});
            path = try std.fmt.allocPrint(arena, "{s}/{s}.json", .{ dir, &std.fmt.bytesToHex(digest, .lower) });
            if (self.lookup(arena, path.?, m)) |hit| {
                self.hits += 1;
                return hit;
            }
        };
        self.misses += 1;
        const full = try std.fmt.allocPrint(arena, "SET threads=1; {s}\nDESCRIBE {s}; {s}", .{ setup, sql, sql });
        const out = try self.run(&.{ "-json", "-c", full });
        defer self.gpa.free(out.stdout);
        defer self.gpa.free(out.stderr);
        const raw = if (out.code == 0)
            try std.fmt.allocPrint(arena, "O{s}", .{out.stdout})
        else
            try std.fmt.allocPrint(arena, "E{s}", .{std.mem.trim(u8, out.stderr, " \t\r\n")});
        if (path) |p| self.store(arena, p, material.?, raw) catch |err|
            std.debug.print("oracle cache: {s}\n", .{@errorName(err)});
        return raw;
    }

    fn lookup(self: *Oracle, arena: std.mem.Allocator, path: []const u8, material: []const u8) ?[]const u8 {
        const text = std.Io.Dir.cwd().readFileAlloc(self.io, path, arena, .limited(256 << 20)) catch return null;
        const Entry = struct { material: []const u8, answer: []const u8, answer_sha256: []const u8 };
        const entry = std.json.parseFromSliceLeaky(Entry, arena, text, .{}) catch return null;
        if (!std.mem.eql(u8, entry.material, material)) return null;
        if (!std.mem.eql(u8, entry.answer_sha256, &sha(entry.answer))) return null;
        if (entry.answer.len == 0 or (entry.answer[0] != 'O' and entry.answer[0] != 'E')) return null;
        _ = decode(arena, entry.answer) catch return null;
        return entry.answer;
    }

    fn store(self: *Oracle, arena: std.mem.Allocator, path: []const u8, material: []const u8, raw: []const u8) !void {
        try std.Io.Dir.cwd().createDirPath(self.io, self.cache_dir.?);
        const text = try std.json.Stringify.valueAlloc(
            arena,
            .{
                .material = material,
                .answer = raw,
                .answer_sha256 = &sha(raw),
            },
            .{},
        );
        const tmp = try std.fmt.allocPrint(arena, "{s}.{d}.tmp", .{ path, std.c.getpid() });
        try std.Io.Dir.cwd().writeFile(self.io, .{ .sub_path = tmp, .data = text });
        try std.Io.Dir.cwd().rename(tmp, std.Io.Dir.cwd(), path, self.io);
    }

    /// The bytes that decide an answer, or null when a file the SQL names cannot be hashed (no cache then).
    pub fn keyMaterial(self: *Oracle, arena: std.mem.Allocator, sql: []const u8) !?[]const u8 {
        return self.keyMaterialAfter(arena, "", sql);
    }

    fn keyMaterialAfter(self: *Oracle, arena: std.mem.Allocator, setup: []const u8, sql: []const u8) !?[]const u8 {
        var out: std.ArrayList(u8) = .empty;
        try out.print(arena, "zpq-oracle v3\nduckdb {s} -no-agent -no-init threads=1\nTZ={s} LC_ALL=C\nsetup " ++
            "{d}:{s}\nsql {d}:{s}\n", .{
            &self.duckdb_sha, self.tz, setup.len, setup, sql.len, sql,
        });
        const both = try std.mem.concat(arena, u8, &.{ setup, "\n", sql });
        var i: usize = 0;
        while (std.mem.indexOfScalarPos(u8, both, i, '\'')) |start| {
            const end = std.mem.indexOfScalarPos(u8, both, start + 1, '\'') orelse break;
            const literal = both[start + 1 .. end];
            i = end + 1;
            switch (try self.contentOf(arena, literal)) {
                .not_a_file => {},
                .unhashable => return null,
                .hashed => |h| try out.print(arena, "file {d}:{s} {s}\n", .{ literal.len, literal, h }),
            }
        }
        return out.items;
    }

    const Content = union(enum) { not_a_file, unhashable, hashed: []const u8 };

    fn contentOf(self: *Oracle, arena: std.mem.Allocator, literal: []const u8) !Content {
        if (literal.len == 0 or std.mem.indexOfScalar(u8, literal, '\n') != null) return .not_a_file;
        if (std.mem.indexOfScalar(u8, literal, '*') == null) {
            const h = fileSha(self.gpa, self.io, literal) catch |err| return switch (err) {
                error.FileNotFound, error.IsDir, error.NotDir, error.NameTooLong, error.BadPathName => .not_a_file,
                else => .unhashable,
            };
            return .{ .hashed = try arena.dupe(u8, &h) };
        }
        const matches = globFiles(arena, self.io, literal) catch return .unhashable;
        if (matches.len == 0) return .not_a_file;
        var out: std.ArrayList(u8) = .empty;
        for (matches) |m| {
            const h = fileSha(self.gpa, self.io, m) catch return .unhashable;
            try out.print(arena, "{s}={s} ", .{ m, &h });
        }
        return .{ .hashed = out.items };
    }
};

fn sha(bytes: []const u8) [64]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return std.fmt.bytesToHex(digest, .lower);
}

/// Files matching a pattern whose `*` wildcards are in the last path component, sorted.
pub fn globFiles(arena: std.mem.Allocator, io: std.Io, pattern: []const u8) ![]const []const u8 {
    const slash = std.mem.lastIndexOfScalar(u8, pattern, '/');
    const dir_path = if (slash) |s| pattern[0..s] else ".";
    const base = if (slash) |s| pattern[s + 1 ..] else pattern;
    if (std.mem.indexOfScalar(u8, dir_path, '*') != null) return error.UnsupportedGlob;
    var dir = std.Io.Dir.cwd().openDir(io, dir_path, .{ .iterate = true }) catch return &.{};
    defer dir.close(io);
    var names: std.ArrayList([]const u8) = .empty;
    var it = dir.iterate();
    while (try it.next(io)) |entry| {
        if (entry.kind != .file) continue;
        if (!wildMatch(base, entry.name)) continue;
        try names.append(arena, try std.fmt.allocPrint(arena, "{s}/{s}", .{ dir_path, entry.name }));
    }
    std.mem.sort([]const u8, names.items, {}, compare.lessText);
    return names.items;
}

fn wildMatch(pattern: []const u8, name: []const u8) bool {
    const star = std.mem.indexOfScalar(u8, pattern, '*') orelse return std.mem.eql(u8, pattern, name);
    const head = pattern[0..star];
    if (!std.mem.startsWith(u8, name, head)) return false;
    const rest = pattern[star + 1 ..];
    var i: usize = head.len;
    while (i <= name.len) : (i += 1) if (wildMatch(rest, name[i..])) return true;
    return false;
}

/// sha256 of a file's bytes, streamed: no size limit.
pub fn fileSha(gpa: std.mem.Allocator, io: std.Io, path: []const u8) ![64]u8 {
    _ = gpa;
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    if ((try file.stat(io)).kind == .directory) return error.IsDir;
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var buf: [1 << 16]u8 = undefined;
    var reader = file.reader(io, &buf);
    var chunk: [1 << 16]u8 = undefined;
    while (true) {
        const n = try reader.interface.readSliceShort(&chunk);
        if (n == 0) break;
        hasher.update(chunk[0..n]);
    }
    var digest: [32]u8 = undefined;
    hasher.final(&digest);
    return std.fmt.bytesToHex(digest, .lower);
}

/// Split DuckDB's -json output (one top-level array per statement) and decode DESCRIBE + rows.
fn decode(arena: std.mem.Allocator, raw: []const u8) !Answer {
    if (raw[0] == 'E') return .{ .refused = raw[1..] };
    const arrays = try topLevelArrays(arena, raw[1..]);
    if (arrays.len < 2) return error.BadOracleOutput;
    const describe = try compare.parse(arena, arrays[arrays.len - 2]);
    const rows = try compare.parse(arena, arrays[arrays.len - 1]);
    const n = describe.array.items.len;
    const names = try arena.alloc([]const u8, n);
    const types = try arena.alloc([]const u8, n);
    for (describe.array.items, 0..) |row, i| {
        names[i] = row.object.get("column_name").?.string;
        types[i] = row.object.get("column_type").?.string;
    }
    const out = try arena.alloc(std.json.ObjectMap, rows.array.items.len);
    for (rows.array.items, 0..) |row, i| out[i] = row.object;
    return .{ .names = names, .types = types, .rows = out };
}

fn topLevelArrays(arena: std.mem.Allocator, text: []const u8) ![]const []const u8 {
    var out: std.ArrayList([]const u8) = .empty;
    var depth: usize = 0;
    var start: usize = 0;
    var in_string = false;
    var i: usize = 0;
    while (i < text.len) : (i += 1) {
        const c = text[i];
        if (in_string) {
            if (c == '\\') i += 1 else if (c == '"') in_string = false;
            continue;
        }
        switch (c) {
            '"' => in_string = true,
            '[' => {
                if (depth == 0) start = i;
                depth += 1;
            },
            ']' => {
                if (depth == 0) return error.BadOracleOutput;
                depth -= 1;
                if (depth == 0) try out.append(arena, text[start .. i + 1]);
            },
            else => {},
        }
    }
    if (depth != 0) return error.BadOracleOutput;
    return out.items;
}

pub const Captured = struct {
    code: u8,
    stdout: []u8,
    stderr: []u8,
    /// Killed by a signal: never an answer and never an acceptable refusal.
    signaled: bool = false,
};

pub fn capture(gpa: std.mem.Allocator, io: std.Io, argv: []const []const u8) !Captured {
    return captureEnv(gpa, io, argv, null);
}

/// Run `argv` to completion; `env` replaces the child's environment when given.
pub fn captureEnv(
    gpa: std.mem.Allocator,
    io: std.Io,
    argv: []const []const u8,
    env: ?*const std.process.Environ.Map,
) !Captured {
    var child = try std.process.spawn(io, .{
        .argv = argv,
        .stdin = .ignore,
        .stdout = .pipe,
        .stderr = .pipe,
        .environ_map = env,
    });
    var multi_reader_buffer: std.Io.File.MultiReader.Buffer(2) = undefined;
    var multi_reader: std.Io.File.MultiReader = undefined;
    multi_reader.init(gpa, io, multi_reader_buffer.toStreams(), &.{ child.stdout.?, child.stderr.? });
    defer multi_reader.deinit();
    while (multi_reader.fill(4096, .none)) |_| {} else |err| {
        if (err != error.EndOfStream) return err;
    }
    const term = try child.wait(io);
    const code: u8 = switch (term) {
        .exited => |c| c,
        else => 255,
    };
    return .{
        .code = code,
        .signaled = term != .exited,
        .stdout = try multi_reader.toOwnedSlice(0),
        .stderr = try multi_reader.toOwnedSlice(1),
    };
}

const testing = std.testing;

/// A stand-in DuckDB that answers one row (`value`), and logs its arguments and environment to `calls`.
fn fakeDuck(io: std.Io, dir: []const u8, value: []const u8) ![]const u8 {
    const path = try std.fmt.allocPrint(testing.allocator, "{s}/duckdb", .{dir});
    const script = try std.fmt.allocPrint(testing.allocator,
        \\#!/bin/sh
        \\echo "call $* TZ=$TZ LC_ALL=$LC_ALL HOME=$HOME" >> {s}/calls
        \\echo '[{{"column_name":"a","column_type":"INTEGER"}}]'
        \\echo '[{{"a":{s}}}]'
        \\
    , .{ dir, value });
    defer testing.allocator.free(script);
    try std.Io.Dir.cwd().writeFile(
        io,
        .{
            .sub_path = path,
            .data = script,
            .flags = .{ .permissions = .fromMode(0o755) },
        },
    );
    return path;
}

fn callLog(io: std.Io, arena: std.mem.Allocator, dir: []const u8) ![]const u8 {
    const p = try std.fmt.allocPrint(arena, "{s}/calls", .{dir});
    return std.Io.Dir.cwd().readFileAlloc(io, p, arena, .limited(1 << 20)) catch "";
}

fn calls(io: std.Io, arena: std.mem.Allocator, dir: []const u8) !usize {
    // The question spans lines (setup, then the SQL and its DESCRIBE): count the calls, not the lines.
    return std.mem.count(u8, try callLog(io, arena, dir), "call -no-agent");
}

const Sandbox = struct {
    tmp: std.testing.TmpDir,
    arena_state: std.heap.ArenaAllocator,
    root: []const u8 = "",

    fn init() !Sandbox {
        var b: Sandbox = .{ .tmp = testing.tmpDir(.{}), .arena_state = .init(testing.allocator) };
        b.root = try std.fmt.allocPrint(b.arena_state.allocator(), ".zig-cache/tmp/{s}", .{&b.tmp.sub_path});
        return b;
    }
    fn deinit(b: *Sandbox) void {
        b.arena_state.deinit();
        b.tmp.cleanup();
    }
    fn file(b: *Sandbox, name: []const u8, data: []const u8) ![]const u8 {
        const path = try std.fmt.allocPrint(b.arena_state.allocator(), "{s}/{s}", .{ b.root, name });
        try std.Io.Dir.cwd().writeFile(testing.io, .{ .sub_path = path, .data = data });
        return path;
    }
};

fn entryPath(o: *Oracle, a: std.mem.Allocator, sql: []const u8) ![]const u8 {
    const m = (try o.keyMaterial(a, sql)).?;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(m, &digest, .{});
    return std.fmt.allocPrint(a, "{s}/{s}.json", .{ o.cache_dir.?, &std.fmt.bytesToHex(digest, .lower) });
}

test "the cache never answers for other inputs: changed files, binaries or zones miss" {
    const io = testing.io;
    var box = try Sandbox.init();
    defer box.deinit();
    const a = box.arena_state.allocator();
    const duck = try fakeDuck(io, box.root, "1");
    defer testing.allocator.free(duck);
    const cache = try std.fmt.allocPrint(a, "{s}/cache", .{box.root});
    const fixture = try box.file("f.parquet", "one");
    const sql = try std.fmt.allocPrint(a, "SELECT a FROM '{s}'", .{fixture});

    var o = try Oracle.init(testing.allocator, io, duck, cache, "");
    _ = try o.ask(a, sql);
    _ = try o.ask(a, sql);
    try testing.expectEqual(@as(usize, 1), try calls(io, a, box.root));
    try testing.expectEqual(@as(usize, 1), o.hits);
    // The same SQL over different bytes is a different question.
    _ = try box.file("f.parquet", "two");
    _ = try o.ask(a, sql);
    try testing.expectEqual(@as(usize, 2), try calls(io, a, box.root));
    // Another DuckDB binary never reuses this one's answers.
    var box2 = try Sandbox.init();
    defer box2.deinit();
    const duck2 = try fakeDuck(io, box2.root, "2");
    defer testing.allocator.free(duck2);
    var o2 = try Oracle.init(testing.allocator, io, duck2, cache, "");
    const other = try o2.ask(a, sql);
    try testing.expectEqualStrings("2", other.rows[0].get("a").?.number_string);
    // Nor does another time zone.
    var o3 = try Oracle.init(testing.allocator, io, duck, cache, "America/Chicago");
    const before = try calls(io, a, box.root);
    _ = try o3.ask(a, sql);
    try testing.expectEqual(before + 1, try calls(io, a, box.root));
}

test "a truncated, foreign, corrupted or undecodable entry is a miss that asks DuckDB again" {
    const io = testing.io;
    var box = try Sandbox.init();
    defer box.deinit();
    const a = box.arena_state.allocator();
    const duck = try fakeDuck(io, box.root, "42");
    defer testing.allocator.free(duck);
    var o = try Oracle.init(testing.allocator, io, duck, try std.fmt.allocPrint(a, "{s}/cache", .{box.root}), "");
    const sql = try std.fmt.allocPrint(a, "SELECT a FROM '{s}'", .{try box.file("f.parquet", "x")});
    _ = try o.ask(a, sql);
    const entry = try entryPath(&o, a, sql);
    const good = try std.Io.Dir.cwd().readFileAlloc(io, entry, a, .limited(1 << 20));
    const material = try std.json.Stringify.valueAlloc(a, (try o.keyMaterial(a, sql)).?, .{});
    const bad = [_][]const u8{
        "",
        "{\"material\":",
        "{\"material\":\"other\",\"answer\":\"O[][]\",\"answer_sha256\":\"x\"}",
        // The answer changed under an intact key: its digest no longer matches.
        try std.mem.replaceOwned(u8, a, good, "\"a\\\":42", "\"a\\\":43"),
        // A malformed answer whose digest matches: it must not decode into a hit.
        try std.fmt.allocPrint(a, "{{\"material\":{s},\"answer\":\"O[\",\"answer_sha256\":\"{s}\"}}", .{
            material,
            &sha("O["),
        }),
    };
    try testing.expect(!std.mem.eql(u8, bad[3], good));
    for (bad) |text| {
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = entry, .data = text });
        const before = try calls(io, a, box.root);
        const got = try o.ask(a, sql);
        try testing.expectEqual(before + 1, try calls(io, a, box.root));
        try testing.expectEqualStrings("42", got.rows[0].get("a").?.number_string);
    }
}

test "paths stay in the key: the same bytes under two names are two questions" {
    const io = testing.io;
    var box = try Sandbox.init();
    defer box.deinit();
    const a = box.arena_state.allocator();
    const duck = try fakeDuck(io, box.root, "1");
    defer testing.allocator.free(duck);
    var o = try Oracle.init(testing.allocator, io, duck, null, "");
    const one = try box.file("one.parquet", "same");
    const two = try box.file("two.parquet", "same");
    const k1 = (try o.keyMaterial(a, try std.fmt.allocPrint(a, "SELECT '{s}' AS p", .{one}))).?;
    const k2 = (try o.keyMaterial(a, try std.fmt.allocPrint(a, "SELECT '{s}' AS p", .{two}))).?;
    try testing.expect(!std.mem.eql(u8, k1, k2));
    try testing.expect(std.mem.indexOf(u8, k1, one) != null);
    // A glob is keyed by every file it matches.
    const glob = try std.fmt.allocPrint(a, "SELECT * FROM '{s}/*.parquet'", .{box.root});
    const g1 = (try o.keyMaterial(a, glob)).?;
    _ = try box.file("two.parquet", "changed");
    try testing.expect(!std.mem.eql(u8, g1, (try o.keyMaterial(a, glob)).?));
}

test "a named file that cannot be hashed makes the question uncacheable" {
    const io = testing.io;
    var box = try Sandbox.init();
    defer box.deinit();
    const a = box.arena_state.allocator();
    const duck = try fakeDuck(io, box.root, "1");
    defer testing.allocator.free(duck);
    var o = try Oracle.init(testing.allocator, io, duck, try std.fmt.allocPrint(a, "{s}/cache", .{box.root}), "");
    const locked = try box.file("locked.parquet", "secret");
    try std.Io.Dir.cwd().setFilePermissions(io, locked, .fromMode(0o000), .{});
    defer std.Io.Dir.cwd().setFilePermissions(io, locked, .fromMode(0o644), .{}) catch {};
    const sql = try std.fmt.allocPrint(a, "SELECT a FROM '{s}'", .{locked});
    if (std.c.getuid() == 0) return error.SkipZigTest; // root reads any file
    try testing.expect((try o.keyMaterial(a, sql)) == null);
    _ = try o.ask(a, sql);
    _ = try o.ask(a, sql);
    try testing.expectEqual(@as(usize, 2), try calls(io, a, box.root));
    try testing.expectEqual(@as(usize, 0), o.hits);
}

test "DuckDB runs without an init file, in a pinned environment that the key names" {
    const io = testing.io;
    var box = try Sandbox.init();
    defer box.deinit();
    const a = box.arena_state.allocator();
    const duck = try fakeDuck(io, box.root, "1");
    defer testing.allocator.free(duck);
    var o = try Oracle.init(testing.allocator, io, duck, null, "");
    _ = try o.ask(a, "SELECT 1 AS a");
    const log = try callLog(io, a, box.root);
    try testing.expect(std.mem.indexOf(u8, log, "-no-init") != null);
    try testing.expect(std.mem.indexOf(u8, log, "TZ=UTC LC_ALL=C HOME=\n") != null);
    const k = (try o.keyMaterial(a, "SELECT 1 AS a")).?;
    try testing.expect(std.mem.indexOf(u8, k, "-no-init") != null and std.mem.indexOf(u8, k, "TZ=UTC") != null);
}
