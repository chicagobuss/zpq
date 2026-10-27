//! Stage receipts and the tier verdict. `zpq-check stage` runs one stage under a timeout and leaves its log and a
//! receipt naming the bytes it judged (SHA, tree state, artifacts, inputs); `zpq-check verdict` passes a tier only
//! when every expected stage has a receipt for this SHA from a clean tree, its log is the one the receipt hashed, and
//! its counts show it ran something, failed nothing and skipped nothing it may not.

const std = @import("std");
const oracle_mod = @import("oracle.zig");

const Allocator = std.mem.Allocator;

pub const Kind = enum { units, cases, contracts, plain };

pub const Counts = struct {
    total: u64 = 0,
    passed: u64 = 0,
    failed: u64 = 0,
    skipped: u64 = 0,
    /// Unit tests that skipped, by name; any one fails the verdict (fixtures are a tier prerequisite).
    skips: []const []const u8 = &.{},
};

pub const Artifact = struct { name: []const u8, sha256: []const u8 };

/// CPU features a unit test can need to test anything at all.
pub const CpuFeature = enum {
    /// AVX-512 F, BW, VL, VBMI and VBMI2, with the OS saving ZMM state: vendor/zstd/zpq/huf4x_avx512.c's own check.
    avx512vbmi2,
};

/// Unit tests that skip on a CPU without a feature, by name prefix, declared here rather than by the run or the test.
/// The verdict accepts such a skip only when the receipt records its host lacking the feature, and reports it apart
/// from the passes; the same skip on a host with the feature, or one whose features went unrecorded, fails.
pub const hardware_skips = [_]struct { prefix: []const u8, needs: CpuFeature }{
    .{ .prefix = "core.parquet.compression.test.the AVX-512 Huffman encoder writes ", .needs = .avx512vbmi2 },
    .{
        .prefix = "core.parquet.compression.test.zstd frames are the same bytes with the AVX-512 ",
        .needs = .avx512vbmi2,
    },
};

pub const Header = struct {
    sha: []const u8,
    tree_clean: bool,
    host: []const u8,
    kernel: []const u8,
    zig: []const u8,
    duckdb_sha256: []const u8,
    artifacts: []const Artifact,
    cases_sha256: []const u8,
    fixtures: []const u8,
    /// The `CpuFeature`s this host lacks, read at run time (a fleet bundle targets a baseline CPU, so `builtin.cpu`
    /// is not the host's); null when unread, which accepts no hardware skip.
    cpu_lacks: ?[]const CpuFeature = null,
};

pub const Receipt = struct {
    version: u32 = 1,
    header: Header,
    stage: []const u8,
    tier: []const u8,
    kind: Kind,
    argv: []const []const u8,
    rc: u8,
    timed_out: bool,
    seconds: f64,
    log_sha256: []const u8,
    counts: Counts,
    /// Test-name prefixes this stage may skip, by name: tests a platform cannot run (the Linux-only epoll loop on
    /// macOS). Recorded so the verdict judges the skips against what the run declared, never silently.
    allowed_skips: []const []const u8 = &.{},
};

/// What a log says ran. Each kind reads its own runner's last summary line; a log without one ran nothing.
pub fn parseCounts(arena: Allocator, kind: Kind, log: []const u8, rc: u8) !Counts {
    var c: Counts = .{};
    var skips: std.ArrayList([]const u8) = .empty;
    var lines = std.mem.splitScalar(u8, log, '\n');
    while (lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, " \r");
        switch (kind) {
            .units => {
                if (std.mem.endsWith(u8, line, "...SKIP")) {
                    const space = std.mem.indexOfScalar(u8, line, ' ') orelse continue;
                    try skips.append(arena, line[space + 1 .. line.len - "...SKIP".len]);
                } else if (scan(line, "All ", " tests passed.")) |n| {
                    c = .{ .total = n, .passed = n };
                } else if (std.mem.endsWith(u8, line, " failed.") and std.mem.indexOf(u8, line, " skipped; ") != null) {
                    var it = std.mem.tokenizeAny(u8, line, " ;.");
                    const p = try std.fmt.parseInt(u64, it.next() orelse continue, 10);
                    _ = it.next();
                    const s = try std.fmt.parseInt(u64, it.next() orelse continue, 10);
                    _ = it.next();
                    const f = try std.fmt.parseInt(u64, it.next() orelse continue, 10);
                    c = .{ .total = p + s + f, .passed = p, .skipped = s, .failed = f };
                }
            },
            .cases => if (std.mem.startsWith(u8, line, "check: ") and std.mem.endsWith(u8, line, " checks match")) {
                const body = line["check: ".len .. line.len - " checks match".len];
                const slash = std.mem.indexOfScalar(u8, body, '/') orelse continue;
                const m = try std.fmt.parseInt(u64, body[0..slash], 10);
                const t = try std.fmt.parseInt(u64, body[slash + 1 ..], 10);
                c = .{ .total = t, .passed = m, .failed = t -| m };
            },
            .contracts => if (std.mem.startsWith(u8, line, "contracts: ")) {
                var it = std.mem.tokenizeAny(u8, line["contracts: ".len..], " ,");
                const p = try std.fmt.parseInt(u64, it.next() orelse continue, 10);
                _ = it.next();
                const s = try std.fmt.parseInt(u64, it.next() orelse continue, 10);
                for (0..4) |_| _ = it.next();
                const f = try std.fmt.parseInt(u64, it.next() orelse continue, 10);
                // Contracts skip only by a named need (tools/check/contracts.py); those are not unit skips.
                c = .{ .total = p + f, .passed = p, .failed = f };
                _ = s;
            },
            .plain => {},
        }
    }
    if (kind == .plain) c = .{ .total = 1, .passed = @intFromBool(rc == 0), .failed = @intFromBool(rc != 0) };
    c.skips = skips.items;
    return c;
}

fn scan(line: []const u8, prefix: []const u8, suffix: []const u8) ?u64 {
    if (!std.mem.startsWith(u8, line, prefix) or !std.mem.endsWith(u8, line, suffix)) return null;
    return std.fmt.parseInt(u64, line[prefix.len .. line.len - suffix.len], 10) catch null;
}

/// Null when the receipt is evidence of a pass for `sha`, else why not. A receipt is never trusted on its own word:
/// the log must still hash to what it recorded, and the counts are judged again here.
pub fn judge(arena: Allocator, io: std.Io, dir: []const u8, name: []const u8, sha: []const u8) !?[]const u8 {
    return (try assess(arena, io, dir, name, sha)).why;
}

/// `why` as `judge`; `note` names a pass's hardware skips, which a tier reports apart from its passes.
pub const Assessment = struct { why: ?[]const u8 = null, note: ?[]const u8 = null };

pub fn assess(arena: Allocator, io: std.Io, dir: []const u8, name: []const u8, sha: []const u8) !Assessment {
    const path = try std.fmt.allocPrint(arena, "{s}/{s}.json", .{ dir, name });
    const text = std.Io.Dir.cwd().readFileAlloc(io, path, arena, .limited(16 << 20)) catch return .{ .why = "no receipt" };
    const r = std.json.parseFromSliceLeaky(Receipt, arena, text, .{ .ignore_unknown_fields = true }) catch
        return .{ .why = "unreadable receipt" };
    if (r.version != 1) return .{ .why = "unknown receipt version" };
    if (!std.mem.eql(u8, r.stage, name)) return .{ .why = "receipt for another stage" };
    if (!std.mem.eql(u8, r.header.sha, sha)) return .{ .why = try std.fmt.allocPrint(arena, "receipt is for {s}", .{r.header.sha}) };
    if (!r.header.tree_clean) return .{ .why = "unofficial: run from a dirty tree" };
    const log_path = try std.fmt.allocPrint(arena, "{s}/{s}.log", .{ dir, name });
    const log_sha = oracle_mod.fileSha(arena, io, log_path) catch return .{ .why = "no log" };
    if (!std.mem.eql(u8, &log_sha, r.log_sha256)) return .{ .why = "log differs from the one the receipt hashed" };
    if (r.timed_out) return .{ .why = try std.fmt.allocPrint(arena, "timed out after {d:.0}s", .{r.seconds}) };
    if (r.rc != 0) return .{ .why = try std.fmt.allocPrint(arena, "exit {d}", .{r.rc}) };
    const c = r.counts;
    if (c.total == 0) return .{ .why = "ran nothing" };
    if (c.failed != 0 or c.passed + c.skipped != c.total) return .{ .why = try std.fmt.allocPrint(arena, "{d} of {d} failed", .{ c.total - c.passed, c.total }) };
    if (c.skipped != c.skips.len) return .{ .why = try std.fmt.allocPrint(arena, "{d} skipped, {d} by name", .{ c.skipped, c.skips.len }) };
    var by_hardware: usize = 0;
    var lacking = std.EnumSet(CpuFeature).empty;
    for (c.skips) |skip| {
        // Hardware first, so a run's --allow-skip cannot excuse a hardware skip on a host that has the feature.
        if (for (hardware_skips) |h| {
            if (std.mem.startsWith(u8, skip, h.prefix)) break h.needs;
        } else null) |need| {
            const lacks = r.header.cpu_lacks orelse
                return .{ .why = try std.fmt.allocPrint(arena, "{s} skipped; the host's CPU is unrecorded", .{skip}) };
            if (std.mem.indexOfScalar(CpuFeature, lacks, need) == null)
                return .{ .why = try std.fmt.allocPrint(arena, "{s} skipped on a host with {t}", .{ skip, need }) };
            by_hardware += 1;
            lacking.insert(need);
            continue;
        }
        const allowed = for (r.allowed_skips) |prefix| {
            if (std.mem.startsWith(u8, skip, prefix)) break true;
        } else false;
        if (!allowed) return .{ .why = try std.fmt.allocPrint(arena, "{d} skipped (first not allowed: {s})", .{
            c.skipped, skip,
        }) };
    }
    if (by_hardware == 0) return .{};
    var note: std.ArrayList(u8) = .empty;
    try note.print(arena, "{d} skipped on {s}, which lacks", .{ by_hardware, r.header.host });
    var it = lacking.iterator();
    while (it.next()) |f| try note.print(arena, " {t}", .{f});
    return .{ .note = note.items };
}

/// `zpq-check verdict --dir DIR --expect a,b,c [--sha SHA]`: every expected stage judged; exit 1 on any failure.
pub fn verdictMain(arena: Allocator, io: std.Io, args: []const []const u8) !u8 {
    var dir: ?[]const u8 = null;
    var expect: ?[]const u8 = null;
    var sha: ?[]const u8 = null;
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        const a = args[i];
        if (i + 1 >= args.len) return error.BadArgs;
        i += 1;
        if (std.mem.eql(u8, a, "--dir")) dir = args[i] else if (std.mem.eql(u8, a, "--expect")) expect = args[i] else if (std.mem.eql(u8, a, "--sha")) sha = args[i] else return error.BadArgs;
    }
    const want = sha orelse (try gitHead(arena, io)) orelse return error.NoSha;
    var failed: usize = 0;
    var noted: usize = 0;
    var n: usize = 0;
    var it = std.mem.tokenizeScalar(u8, expect orelse return error.BadArgs, ',');
    while (it.next()) |name| : (n += 1) {
        const v = try assess(arena, io, dir orelse return error.BadArgs, name, want);
        if (v.why) |why| {
            failed += 1;
            std.debug.print("FAIL {s}: {s}\n", .{ name, why });
        } else if (v.note) |note| {
            noted += 1;
            std.debug.print("PASS {s} (hardware: {s})\n", .{ name, note });
        } else std.debug.print("PASS {s}\n", .{name});
    }
    const hw = if (noted == 0) "" else try std.fmt.allocPrint(arena, ", {d} with hardware skips", .{noted});
    const result = if (failed == 0 and n != 0) "pass" else "fail";
    std.debug.print("verdict: {s} ({d} stages, {d} failed{s}) for {s}\n", .{ result, n, failed, hw, want });
    return if (failed == 0 and n != 0) 0 else 1;
}

/// `zpq-check stage --name N --dir D --kind K [--tier T] [--timeout S] [--sha S] [--allow-skip PREFIX]...
/// [--artifact NAME=PATH]... -- ARGV...`: runs
/// ARGV in its own process group under `timeout`, writes D/N.log and D/N.json, and exits 0 whatever the stage did
/// (the verdict judges it), so one failing stage never hides the rest of the tier.
pub fn stageMain(arena: Allocator, io: std.Io, gpa: Allocator, args: []const []const u8) !u8 {
    var name: ?[]const u8 = null;
    var dir: ?[]const u8 = null;
    var kind: Kind = .plain;
    var tier: []const u8 = "";
    var timeout_s: u32 = 3600;
    var duckdb: []const u8 = "";
    var vouched: ?[]const u8 = null;
    var allowed: std.ArrayList([]const u8) = .empty;
    var artifacts: std.ArrayList(Artifact) = .empty;
    var i: usize = 0;
    while (i < args.len and !std.mem.eql(u8, args[i], "--")) : (i += 2) {
        if (i + 1 >= args.len) return error.BadArgs;
        const a = args[i];
        const v = args[i + 1];
        if (std.mem.eql(u8, a, "--name")) {
            name = v;
        } else if (std.mem.eql(u8, a, "--dir")) {
            dir = v;
        } else if (std.mem.eql(u8, a, "--kind")) {
            kind = std.meta.stringToEnum(Kind, v) orelse return error.BadArgs;
        } else if (std.mem.eql(u8, a, "--tier")) {
            tier = v;
        } else if (std.mem.eql(u8, a, "--timeout")) {
            timeout_s = try std.fmt.parseInt(u32, v, 10);
        } else if (std.mem.eql(u8, a, "--duckdb")) {
            duckdb = v;
        } else if (std.mem.eql(u8, a, "--sha")) {
            vouched = v;
        } else if (std.mem.eql(u8, a, "--allow-skip")) {
            try allowed.append(arena, v);
        } else if (std.mem.eql(u8, a, "--artifact")) {
            const eq = std.mem.indexOfScalar(u8, v, '=') orelse return error.BadArgs;
            const h = oracle_mod.fileSha(gpa, io, v[eq + 1 ..]) catch return error.MissingArtifact;
            try artifacts.append(arena, .{ .name = v[0..eq], .sha256 = try arena.dupe(u8, &h) });
        } else return error.BadArgs;
    }
    if (i >= args.len) return error.BadArgs;
    const argv = args[i + 1 ..];
    const stage_name = name orelse return error.BadArgs;
    const out_dir = dir orelse return error.BadArgs;
    try std.Io.Dir.cwd().createDirPath(io, out_dir);

    var full: std.ArrayList([]const u8) = .empty;
    try full.appendSlice(arena, &.{ "timeout", "--kill-after=30", try std.fmt.allocPrint(arena, "{d}", .{timeout_s}) });
    try full.appendSlice(arena, argv);
    const started = std.Io.Clock.awake.now(io);
    const out = try oracle_mod.capture(gpa, io, full.items);
    defer gpa.free(out.stdout);
    defer gpa.free(out.stderr);
    const ns: i64 = @intCast(started.untilNow(io, .awake).toNanoseconds());
    const seconds = @as(f64, @floatFromInt(ns)) / 1e9;
    const log = try std.mem.concat(arena, u8, &.{ out.stdout, out.stderr });
    // `timeout` exits 124 when it signalled the stage, 137 when it had to kill it.
    const timed_out = !out.signaled and (out.code == 124 or out.code == 137);
    const log_path = try std.fmt.allocPrint(arena, "{s}/{s}.log", .{ out_dir, stage_name });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = log_path, .data = log });

    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(log, &digest, .{});
    var head = try header(arena, io, gpa, duckdb, artifacts.items);
    // A fleet host runs a bundle built and shipped from a clean tree at this SHA (tools/check/fleet.sh); it has no git.
    if (vouched) |v| {
        head.sha = v;
        head.tree_clean = true;
    }
    const receipt: Receipt = .{
        .header = head,
        .stage = stage_name,
        .tier = tier,
        .kind = kind,
        .argv = argv,
        .rc = if (out.signaled) 255 else out.code,
        .timed_out = timed_out,
        .seconds = seconds,
        .log_sha256 = &std.fmt.bytesToHex(digest, .lower),
        .counts = try parseCounts(arena, kind, log, out.code),
        .allowed_skips = allowed.items,
    };
    const json_path = try std.fmt.allocPrint(arena, "{s}/{s}.json", .{ out_dir, stage_name });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = json_path, .data = try std.json.Stringify.valueAlloc(arena, receipt, .{ .whitespace = .indent_2 }) });
    const v = try assess(arena, io, out_dir, stage_name, receipt.header.sha);
    const verdict = v.why;
    std.debug.print("{s}: {s} ({d:.0}s; {s})\n", .{ stage_name, if (verdict == null) "pass" else "FAIL", seconds, verdict orelse v.note orelse "ok" });
    if (verdict != null) std.debug.print("{s}\n", .{log[log.len -| 3000..]});
    return 0;
}

/// Null when OTHER's answers are REF's, case for case; else what differs. An empty manifest proves nothing.
pub fn manifestDiff(arena: Allocator, ref: []const u8, other: []const u8) !?[]const u8 {
    var want = std.StringHashMap([]const u8).init(arena);
    var lines = std.mem.tokenizeScalar(u8, ref, '\n');
    while (lines.next()) |line| {
        const tab = std.mem.indexOfScalar(u8, line, '\t') orelse return "malformed reference manifest";
        try want.put(line[0..tab], line[tab + 1 ..]);
    }
    if (want.count() == 0) return "empty reference manifest";
    var seen: usize = 0;
    var diffs: std.ArrayList(u8) = .empty;
    lines = std.mem.tokenizeScalar(u8, other, '\n');
    while (lines.next()) |line| {
        const tab = std.mem.indexOfScalar(u8, line, '\t') orelse return "malformed manifest";
        const id = line[0..tab];
        if (want.get(id)) |sha| {
            seen += 1;
            if (!std.mem.eql(u8, sha, line[tab + 1 ..])) try diffs.print(arena, "{s} differs; ", .{id});
        } else try diffs.print(arena, "{s} not in the reference; ", .{id});
    }
    if (seen != want.count()) try diffs.print(arena, "{d} reference cases missing", .{want.count() - seen});
    return if (diffs.items.len == 0) null else diffs.items;
}

pub fn manifestDiffMain(arena: Allocator, io: std.Io, args: []const []const u8) !u8 {
    if (args.len != 2) return error.BadArgs;
    const ref = try std.Io.Dir.cwd().readFileAlloc(io, args[0], arena, .limited(64 << 20));
    const other = try std.Io.Dir.cwd().readFileAlloc(io, args[1], arena, .limited(64 << 20));
    if (try manifestDiff(arena, ref, other)) |why| {
        std.debug.print("manifest-diff: {s}\n", .{why[0..@min(why.len, 4000)]});
        return 1;
    }
    std.debug.print("manifest-diff: identical answers\n", .{});
    return 0;
}

fn run(arena: Allocator, io: std.Io, argv: []const []const u8) ?[]const u8 {
    const out = oracle_mod.capture(arena, io, argv) catch return null;
    if (out.code != 0) return null;
    return std.mem.trim(u8, out.stdout, " \n");
}

fn gitHead(arena: Allocator, io: std.Io) !?[]const u8 {
    return run(arena, io, &.{ "git", "rev-parse", "HEAD" });
}

/// Who ran what: the revision and whether the tree matched it, the host, the compiler, the oracle, every artifact
/// named on the command line, the case files and goldens, and the fixtures (corpus pin, generated files).
fn header(arena: Allocator, io: std.Io, gpa: Allocator, duckdb: []const u8, artifacts: []const Artifact) !Header {
    const uts = std.posix.uname();
    const dirty = run(arena, io, &.{ "git", "status", "--porcelain", "--untracked-files=no" });
    const duck = if (duckdb.len != 0) oracle_mod.fileSha(gpa, io, duckdb) catch null else null;
    return .{
        .sha = (try gitHead(arena, io)) orelse "unknown",
        .tree_clean = if (dirty) |d| d.len == 0 else false,
        .host = try arena.dupe(u8, std.mem.sliceTo(&uts.nodename, 0)),
        .kernel = try arena.dupe(u8, std.mem.sliceTo(&uts.release, 0)),
        .zig = run(arena, io, &.{ "zig", "version" }) orelse "none",
        .duckdb_sha256 = if (duck) |d| try arena.dupe(u8, &d) else "none",
        .artifacts = artifacts,
        .cases_sha256 = run(arena, io, &.{ "git", "rev-parse", "HEAD:tools/check" }) orelse "unknown",
        .fixtures = try std.fmt.allocPrint(arena, "parquet-testing {s}; {s}", .{
            run(arena, io, &.{ "git", "-C", "data/parquet-testing", "rev-parse", "HEAD" }) orelse "absent",
            run(arena, io, &.{ "sh", "-c", "cd data && ls -l --time-style=+ *.parquet 2>/dev/null | awk '{print $6\"=\"$5}' | tr '\\n' ' '" }) orelse "",
        }),
        .cpu_lacks = cpuLacks(arena, io),
    };
}

fn cpuLacks(arena: Allocator, io: std.Io) ?[]const CpuFeature {
    const host = std.zig.system.resolveTargetQuery(io, .{}) catch return null;
    var lacks: std.ArrayList(CpuFeature) = .empty;
    for (std.enums.values(CpuFeature)) |f| if (!cpuHas(host.cpu, f)) lacks.append(arena, f) catch return null;
    return lacks.items;
}

fn cpuHas(cpu: std.Target.Cpu, f: CpuFeature) bool {
    return switch (f) {
        .avx512vbmi2 => cpu.arch == .x86_64 and std.Target.x86.featureSetHasAll(cpu.features, .{
            .avx512f, .avx512bw, .avx512vl, .avx512vbmi, .avx512vbmi2,
        }),
    };
}

// ---- fail-open: missing, stale, wrong-SHA, dirty, altered, empty or skipping evidence never passes

const testing = std.testing;

fn writeReceipt(io: std.Io, a: Allocator, dir: []const u8, r: Receipt, log: []const u8) !void {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(log, &digest, .{});
    var with_log = r;
    with_log.log_sha256 = &std.fmt.bytesToHex(digest, .lower);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = try std.fmt.allocPrint(a, "{s}/{s}.log", .{ dir, r.stage }), .data = log });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = try std.fmt.allocPrint(a, "{s}/{s}.json", .{ dir, r.stage }), .data = try std.json.Stringify.valueAlloc(a, with_log, .{}) });
}

test "a receipt passes only as evidence of a clean, complete, skip-free run of this SHA" {
    const io = testing.io;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var state = std.heap.ArenaAllocator.init(testing.allocator);
    defer state.deinit();
    const a = state.allocator();
    const dir = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{&tmp.sub_path});
    const sha = "1111111111111111111111111111111111111111";
    const good: Receipt = .{
        .header = .{ .sha = sha, .tree_clean = true, .host = "h", .kernel = "k", .zig = "z", .duckdb_sha256 = "d", .artifacts = &.{}, .cases_sha256 = "c", .fixtures = "f" },
        .stage = "s",
        .tier = "commit",
        .kind = .units,
        .argv = &.{"x"},
        .rc = 0,
        .timed_out = false,
        .seconds = 1,
        .log_sha256 = "",
        .counts = .{ .total = 3, .passed = 3 },
    };
    try writeReceipt(io, a, dir, good, "All 3 tests passed.\n");
    try testing.expect(try judge(a, io, dir, "s", sha) == null);
    try testing.expect(try judge(a, io, dir, "missing", sha) != null);
    try testing.expect(try judge(a, io, dir, "s", "2222222222222222222222222222222222222222") != null);

    var bad = [_]Receipt{ good, good, good, good, good, good, good };
    bad[0].header.tree_clean = false;
    bad[1].counts = .{};
    bad[2].counts = .{ .total = 3, .passed = 2, .failed = 1 };
    bad[3].counts = .{ .total = 3, .passed = 2, .skipped = 1 };
    bad[4].timed_out = true;
    bad[5].rc = 1;
    bad[6].counts = .{ .total = 3, .passed = 3, .skips = &.{"needs data/x.parquet"} };
    for (bad) |r| {
        try writeReceipt(io, a, dir, r, "All 3 tests passed.\n");
        try testing.expect(try judge(a, io, dir, "s", sha) != null);
    }
    // A skip passes only when the run declared it by name (a platform's Linux-only tests), never otherwise.
    var platform = good;
    platform.counts = .{ .total = 3, .passed = 2, .skipped = 1, .skips = &.{"io.epoll.test.ready"} };
    platform.allowed_skips = &.{"io.epoll."};
    try writeReceipt(io, a, dir, platform, "All 3 tests passed.\n");
    try testing.expect(try judge(a, io, dir, "s", sha) == null);
    platform.allowed_skips = &.{"io.uring."};
    try writeReceipt(io, a, dir, platform, "All 3 tests passed.\n");
    try testing.expect(try judge(a, io, dir, "s", sha) != null);
    // A hardware skip passes, reported, only where the receipt records the host lacking the feature it needs.
    const huf = hardware_skips[0].prefix ++ "zstd's bytes";
    var hw = good;
    hw.counts = .{ .total = 3, .passed = 2, .skipped = 1, .skips = &.{huf} };
    hw.header.cpu_lacks = &.{.avx512vbmi2};
    try writeReceipt(io, a, dir, hw, "All 3 tests passed.\n");
    const noted = try assess(a, io, dir, "s", sha);
    try testing.expect(noted.why == null);
    try testing.expectEqualStrings("1 skipped on h, which lacks avx512vbmi2", noted.note.?);
    try testing.expect((try assess(a, io, dir, "s", sha)).note != null);
    for ([_]?[]const CpuFeature{ &.{}, null }) |lacks| {
        hw.header.cpu_lacks = lacks;
        hw.allowed_skips = &.{"core.parquet."}; // a run's own allowance does not excuse it on a capable host
        try writeReceipt(io, a, dir, hw, "All 3 tests passed.\n");
        try testing.expect(try judge(a, io, dir, "s", sha) != null);
    }
    hw.header.cpu_lacks = &.{.avx512vbmi2};
    hw.allowed_skips = &.{};
    hw.counts.skips = &.{"core.parquet.compression.test.needs data/x.parquet"};
    try writeReceipt(io, a, dir, hw, "All 3 tests passed.\n");
    try testing.expect(try judge(a, io, dir, "s", sha) != null);
    try testing.expect((try assess(a, io, dir, "s", sha)).note == null);
    // A log altered after its receipt was written, or no log at all, is not the evidence the receipt names.
    try writeReceipt(io, a, dir, good, "All 3 tests passed.\n");
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = try std.fmt.allocPrint(a, "{s}/s.log", .{dir}), .data = "All 4 tests passed.\n" });
    try testing.expect(try judge(a, io, dir, "s", sha) != null);
    try std.Io.Dir.cwd().deleteFile(io, try std.fmt.allocPrint(a, "{s}/s.log", .{dir}));
    try testing.expect(try judge(a, io, dir, "s", sha) != null);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = try std.fmt.allocPrint(a, "{s}/s.json", .{dir}), .data = "{\"version\": 1" });
    try testing.expect(try judge(a, io, dir, "s", sha) != null);
}

test "counts come from each runner's own summary, and a log without one ran nothing" {
    var state = std.heap.ArenaAllocator.init(testing.allocator);
    defer state.deinit();
    const a = state.allocator();
    const units = try parseCounts(a, .units, "1/3 a...OK\n2/3 b...SKIP\n3/3 c...OK\n2 passed; 1 skipped; 0 failed.\n", 0);
    try testing.expectEqual(@as(u64, 3), units.total);
    try testing.expectEqual(@as(u64, 1), units.skipped);
    try testing.expectEqualStrings("b", units.skips[0]);
    try testing.expectEqual(@as(u64, 5), (try parseCounts(a, .units, "All 5 tests passed.\n", 0)).passed);
    const cases = try parseCounts(a, .cases, "check: 41/42 checks match\n", 1);
    try testing.expectEqual(@as(u64, 1), cases.failed);
    const contracts = try parseCounts(a, .contracts, "contracts: 13 passed, 3 skipped by named need, 0 failed\n", 0);
    try testing.expectEqual(@as(u64, 13), contracts.total);
    try testing.expectEqual(@as(u64, 0), (try parseCounts(a, .cases, "zpq-check: crashed\n", 134)).total);
    try testing.expectEqual(@as(u64, 1), (try parseCounts(a, .plain, "", 1)).failed);
}

test "hosts agree only on the same answers to every reference case" {
    var state = std.heap.ArenaAllocator.init(testing.allocator);
    defer state.deinit();
    const a = state.allocator();
    const ref = "a\t11\nb\t22\n";
    try testing.expect(try manifestDiff(a, ref, "b\t22\na\t11\n") == null);
    try testing.expect(try manifestDiff(a, ref, "a\t11\nb\t23\n") != null);
    try testing.expect(try manifestDiff(a, ref, "a\t11\n") != null);
    try testing.expect(try manifestDiff(a, ref, "a\t11\nb\t22\nc\t33\n") != null);
    try testing.expect(try manifestDiff(a, "", "") != null);
    try testing.expect(try manifestDiff(a, ref, "") != null);
}
