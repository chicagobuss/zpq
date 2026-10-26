//! The one oracle: the DuckDB 2.0 CLI, one process per question, with an optional answer cache.
//!
//! A cache entry is keyed by the DuckDB binary's sha256, TZ (TIMESTAMPTZ prints in local time), and the SQL with
//! every quoted path it names replaced by the sha256 of that file's bytes (a glob by its sorted matches). The entry
//! stores the key material and is used only when it matches exactly; anything else (missing, unreadable, truncated,
//! foreign) is a miss that asks DuckDB again. Commit runs use the cache; full runs pass no cache directory.

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

    pub fn init(gpa: std.mem.Allocator, io: std.Io, duckdb: []const u8, cache_dir: ?[]const u8, tz: []const u8) !Oracle {
        return .{
            .gpa = gpa,
            .io = io,
            .duckdb = duckdb,
            .duckdb_sha = try fileSha(gpa, io, duckdb),
            .tz = tz,
            .cache_dir = cache_dir,
        };
    }

    /// Run `sql`, a single SELECT, and its DESCRIBE. Results live in `arena`.
    pub fn ask(self: *Oracle, arena: std.mem.Allocator, sql: []const u8) !Answer {
        const raw = try self.rawAnswer(arena, sql);
        return decode(arena, raw);
    }

    /// Execute statements for their side effects (fixture COPY). Never cached.
    pub fn exec(self: *Oracle, sql: []const u8) !void {
        const out = try capture(self.gpa, self.io, &.{ self.duckdb, "-no-agent", "-c", sql });
        defer self.gpa.free(out.stdout);
        defer self.gpa.free(out.stderr);
        if (out.code != 0) {
            std.debug.print("duckdb: {s}\n", .{out.stderr});
            return error.DuckDb;
        }
    }

    fn rawAnswer(self: *Oracle, arena: std.mem.Allocator, sql: []const u8) ![]const u8 {
        const material = try self.keyMaterial(arena, sql);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(material, &digest, .{});
        const hex = std.fmt.bytesToHex(digest, .lower);
        var path: ?[]const u8 = null;
        if (self.cache_dir) |dir| {
            path = try std.fmt.allocPrint(arena, "{s}/{s}.json", .{ dir, hex });
            if (self.lookup(arena, path.?, material)) |hit| {
                self.hits += 1;
                return hit;
            }
        }
        self.misses += 1;
        const full = try std.fmt.allocPrint(arena, "SET threads=1; DESCRIBE {s}; {s}", .{ sql, sql });
        const out = try capture(self.gpa, self.io, &.{ self.duckdb, "-no-agent", "-json", "-c", full });
        defer self.gpa.free(out.stdout);
        defer self.gpa.free(out.stderr);
        const raw = if (out.code == 0)
            try std.fmt.allocPrint(arena, "O{s}", .{out.stdout})
        else
            try std.fmt.allocPrint(arena, "E{s}", .{std.mem.trim(u8, out.stderr, " \t\r\n")});
        if (path) |p| self.store(arena, p, material, raw) catch |err| std.debug.print("oracle cache: {s}\n", .{@errorName(err)});
        return raw;
    }

    fn lookup(self: *Oracle, arena: std.mem.Allocator, path: []const u8, material: []const u8) ?[]const u8 {
        const text = std.Io.Dir.cwd().readFileAlloc(self.io, path, arena, .limited(256 << 20)) catch return null;
        const entry = std.json.parseFromSliceLeaky(struct { material: []const u8, answer: []const u8 }, arena, text, .{}) catch return null;
        if (!std.mem.eql(u8, entry.material, material)) return null;
        if (entry.answer.len == 0 or (entry.answer[0] != 'O' and entry.answer[0] != 'E')) return null;
        return entry.answer;
    }

    fn store(self: *Oracle, arena: std.mem.Allocator, path: []const u8, material: []const u8, raw: []const u8) !void {
        try std.Io.Dir.cwd().createDirPath(self.io, self.cache_dir.?);
        const text = try std.json.Stringify.valueAlloc(arena, .{ .material = material, .answer = raw }, .{});
        const tmp = try std.fmt.allocPrint(arena, "{s}.{d}.tmp", .{ path, std.c.getpid() });
        try std.Io.Dir.cwd().writeFile(self.io, .{ .sub_path = tmp, .data = text });
        try std.Io.Dir.cwd().rename(tmp, std.Io.Dir.cwd(), path, self.io);
    }

    /// The bytes that decide an answer: DuckDB, TZ and the SQL with each named file replaced by its content hash.
    pub fn keyMaterial(self: *Oracle, arena: std.mem.Allocator, sql: []const u8) ![]const u8 {
        var out: std.ArrayList(u8) = .empty;
        try out.print(arena, "zpq-oracle v2\nduckdb {s}\nTZ={s}\n", .{ &self.duckdb_sha, self.tz });
        var i: usize = 0;
        while (i < sql.len) {
            if (sql[i] != '\'') {
                try out.append(arena, sql[i]);
                i += 1;
                continue;
            }
            const end = std.mem.indexOfScalarPos(u8, sql, i + 1, '\'') orelse {
                try out.appendSlice(arena, sql[i..]);
                break;
            };
            const literal = sql[i + 1 .. end];
            if (try self.contentOf(arena, literal)) |hashes| {
                try out.print(arena, "<files {s}>", .{hashes});
            } else {
                try out.appendSlice(arena, sql[i .. end + 1]);
            }
            i = end + 1;
        }
        return out.items;
    }

    fn contentOf(self: *Oracle, arena: std.mem.Allocator, literal: []const u8) !?[]const u8 {
        if (literal.len == 0 or std.mem.indexOfScalar(u8, literal, '\n') != null) return null;
        if (std.mem.indexOfScalar(u8, literal, '*') == null) {
            const sha = fileSha(self.gpa, self.io, literal) catch return null;
            return try arena.dupe(u8, &sha);
        }
        const matches = try globFiles(arena, self.io, literal);
        if (matches.len == 0) return null;
        var out: std.ArrayList(u8) = .empty;
        for (matches) |m| {
            const sha = try fileSha(self.gpa, self.io, m);
            try out.print(arena, "{s} ", .{&sha});
        }
        return out.items;
    }
};

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

pub fn fileSha(gpa: std.mem.Allocator, io: std.Io, path: []const u8) ![64]u8 {
    const data = try std.Io.Dir.cwd().readFileAlloc(io, path, gpa, .limited(4 << 30));
    defer gpa.free(data);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(data, &digest, .{});
    return std.fmt.bytesToHex(digest, .lower);
}

/// Split DuckDB's -json output (one top-level array per statement) and decode DESCRIBE + rows.
fn decode(arena: std.mem.Allocator, raw: []const u8) !Answer {
    if (raw[0] == 'E') return .{ .refused = raw[1..] };
    const arrays = try topLevelArrays(arena, raw[1..]);
    if (arrays.len != 2) return error.BadOracleOutput;
    const describe = try compare.parse(arena, arrays[0]);
    const rows = try compare.parse(arena, arrays[1]);
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

/// A stand-in DuckDB that answers one row and counts its invocations in `calls`.
fn fakeDuck(io: std.Io, dir: []const u8, value: []const u8) ![]const u8 {
    const path = try std.fmt.allocPrint(testing.allocator, "{s}/duckdb", .{dir});
    const script = try std.fmt.allocPrint(testing.allocator,
        \\#!/bin/sh
        \\echo x >> {s}/calls
        \\echo '[{{"column_name":"a","column_type":"INTEGER"}}]'
        \\echo '[{{"a":{s}}}]'
        \\
    , .{ dir, value });
    defer testing.allocator.free(script);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = script, .flags = .{ .permissions = .fromMode(0o755) } });
    return path;
}

fn calls(io: std.Io, dir: []const u8) !usize {
    const p = try std.fmt.allocPrint(testing.allocator, "{s}/calls", .{dir});
    defer testing.allocator.free(p);
    const text = std.Io.Dir.cwd().readFileAlloc(io, p, testing.allocator, .limited(1 << 20)) catch return 0;
    defer testing.allocator.free(text);
    return std.mem.count(u8, text, "\n");
}

test "the cache never answers for other inputs: changed files, binaries, corrupt or foreign entries miss" {
    const io = testing.io;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{&tmp.sub_path});
    const duck = try fakeDuck(io, root, "1");
    defer testing.allocator.free(duck);
    const cache = try std.fmt.allocPrint(a, "{s}/cache", .{root});
    const fixture = try std.fmt.allocPrint(a, "{s}/f.parquet", .{root});
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = fixture, .data = "one" });
    const sql = try std.fmt.allocPrint(a, "SELECT a FROM '{s}'", .{fixture});

    var o = try Oracle.init(testing.allocator, io, duck, cache, "UTC");
    _ = try o.ask(a, sql);
    _ = try o.ask(a, sql);
    try testing.expectEqual(@as(usize, 1), try calls(io, root)); // a hit
    try testing.expectEqual(@as(usize, 1), o.hits);

    // The same SQL over different bytes is a different question.
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = fixture, .data = "two" });
    _ = try o.ask(a, sql);
    try testing.expectEqual(@as(usize, 2), try calls(io, root));

    // A truncated, empty or foreign entry is a miss, never an answer.
    const material = try o.keyMaterial(a, sql);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(material, &digest, .{});
    const entry = try std.fmt.allocPrint(a, "{s}/{s}.json", .{ cache, &std.fmt.bytesToHex(digest, .lower) });
    for ([_][]const u8{ "", "{\"material\":", "{\"material\":\"other\",\"answer\":\"O[][]\"}" }) |bad| {
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = entry, .data = bad });
        const before = try calls(io, root);
        const got = try o.ask(a, sql);
        try testing.expectEqual(before + 1, try calls(io, root));
        try testing.expectEqual(@as(usize, 1), got.rows.len);
    }

    // Another DuckDB binary never reuses this one's answers.
    var tmp2 = testing.tmpDir(.{});
    defer tmp2.cleanup();
    const root2 = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{&tmp2.sub_path});
    const duck2 = try fakeDuck(io, root2, "2");
    defer testing.allocator.free(duck2);
    var o2 = try Oracle.init(testing.allocator, io, duck2, cache, "UTC");
    const other = try o2.ask(a, sql);
    try testing.expectEqual(@as(usize, 1), try calls(io, root2));
    try testing.expectEqualStrings("2", other.rows[0].get("a").?.number_string);

    // Nor does another time zone.
    var o3 = try Oracle.init(testing.allocator, io, duck, cache, "America/Chicago");
    const before = try calls(io, root);
    _ = try o3.ask(a, sql);
    try testing.expectEqual(before + 1, try calls(io, root));
}

test "a glob is keyed by every file it matches" {
    const io = testing.io;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{&tmp.sub_path});
    const duck = try fakeDuck(io, root, "1");
    defer testing.allocator.free(duck);
    for ([_][]const u8{ "t_a.parquet", "t_b.parquet" }) |name| {
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = try std.fmt.allocPrint(a, "{s}/{s}", .{ root, name }), .data = name });
    }
    var o = try Oracle.init(testing.allocator, io, duck, null, "UTC");
    const sql = try std.fmt.allocPrint(a, "SELECT * FROM '{s}/t_*.parquet'", .{root});
    const k1 = try o.keyMaterial(a, sql);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = try std.fmt.allocPrint(a, "{s}/t_b.parquet", .{root}), .data = "changed" });
    const k2 = try o.keyMaterial(a, sql);
    try testing.expect(!std.mem.eql(u8, k1, k2));
    try testing.expect(std.mem.indexOf(u8, k1, root) == null);
}
