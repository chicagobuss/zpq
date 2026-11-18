//! The SQL binder's remote footers (`frontend.JoinSchemas`): one batch for a join's tables, one listing per glob, and
//! the same errors as reading them one by one. A fake S3 counts what the binder sends.
const std = @import("std");
const test_skip = @import("test_skip.zig");
const frontend = @import("frontend.zig");
const fake_s3 = @import("io/fake_s3.zig");
const metadata = @import("core/parquet/metadata.zig");
const s3_fetch = @import("engine/s3_fetch.zig");
const Diag = @import("core/diag.zig").Diag;

const fixtures = "ci/fixtures/parquet/";

/// The environment a client of `fake` runs in, kept in `a`.
fn fakeEnv(a: std.mem.Allocator, fake: *const fake_s3.FakeS3) !std.process.Environ {
    const lines = try a.allocSentinel(?[*:0]const u8, 4, null);
    lines[0] = try std.fmt.allocPrintSentinel(a, "S3_ENDPOINT_URL=http://127.0.0.1:{d}", .{fake.port}, 0);
    lines[1] = "AWS_REGION=us-east-1";
    const keys = [_][*:0]const u8{ "AWS_ACCESS_KEY_ID=ak", "AWS_SECRET_ACCESS_KEY=sk" };
    lines[2] = keys[0];
    lines[3] = keys[1];
    return .{ .block = .{ .slice = lines } };
}

fn schemasFor(a: std.mem.Allocator, env: std.process.Environ, diag: *Diag) frontend.JoinSchemas {
    return .{
        .arena = a,
        .io = std.testing.io,
        .env = env,
        .diag = diag,
        .local = .{ .arena = a, .ctx = .{ .gpa = std.testing.allocator, .io = std.testing.io, .env = env, .diag = diag } },
    };
}

/// Serve `name` from the fixture of that file name, and return its rows and row groups for comparison.
fn serve(fake: *fake_s3.FakeS3, a: std.mem.Allocator, key: []const u8, name: []const u8) !struct { rows: i64, groups: usize } {
    const bytes = try metadata.readFileSlice(try std.fmt.allocPrint(a, "{s}{s}", .{ fixtures, name }), a);
    try fake.serveBytes(key, bytes);
    const meta = try metadata.open(a, bytes);
    return .{ .rows = meta.num_rows, .groups = meta.row_groups.items.len };
}

test "binder: a join's footers come in one GET each, whatever their size, and are read once" {
    if (comptime test_skip.lacks(.posix)) return test_skip.skipNeeding(.posix, "POSIX environment fixture");
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(std.testing.allocator);
    defer fake.deinit();
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    // Smaller than the probe (the whole object is the tail), larger than it with a footer that fits, and a footer
    // larger than the probe, which costs the one extra GET for the part it missed.
    const tiny = try serve(&fake, a, "t/tiny.parquet", "decimal_precision40.parquet");
    const mid = try serve(&fake, a, "t/mid.parquet", "group_order.parquet");
    const wide = try serve(&fake, a, "t/wide.parquet", "many_tiny.parquet");
    const wide_bytes = try metadata.readFileSlice(fixtures ++ "many_tiny.parquet", a);
    try std.testing.expect(std.mem.readInt(u32, wide_bytes[wide_bytes.len - 8 ..][0..4], .little) > s3_fetch.FOOTER_PROBE_MIN);

    var diag: Diag = .{};
    var schemas = schemasFor(a, try fakeEnv(a, &fake), &diag);
    defer schemas.deinit();
    const paths = [_][]const u8{ "s3://bkt/t/tiny.parquet", "s3://bkt/t/mid.parquet", "s3://bkt/t/wide.parquet" };
    frontend.JoinSchemas.prefetch(&schemas, &paths);
    try std.testing.expectEqual(@as(u32, 4), fake.gets);
    const want = [_]@TypeOf(tiny){ tiny, mid, wide };
    for (paths, want) |path, expect| {
        const meta = try frontend.JoinSchemas.footer(&schemas, path);
        try std.testing.expectEqual(expect.rows, meta.num_rows);
        try std.testing.expectEqual(expect.groups, meta.row_groups.items.len);
    }
    try std.testing.expectEqual(@as(u32, 4), fake.gets);
    try std.testing.expect(schemas.local.remote_seen);
}

test "binder: a footer read alone is also one GET, and its objects are not tailed beyond the footer" {
    if (comptime test_skip.lacks(.posix)) return test_skip.skipNeeding(.posix, "POSIX environment fixture");
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(std.testing.allocator);
    defer fake.deinit();
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const mid = try serve(&fake, a, "t/mid.parquet", "group_order.parquet");
    var diag: Diag = .{};
    var schemas = schemasFor(a, try fakeEnv(a, &fake), &diag);
    defer schemas.deinit();
    const meta = try frontend.JoinSchemas.footer(&schemas, "s3://bkt/t/mid.parquet");
    try std.testing.expectEqual(mid.rows, meta.num_rows);
    try std.testing.expectEqual(@as(u32, 1), fake.gets);
    // 64 KiB of an object that is larger than that: the footer and what precedes it, not the whole file.
    try std.testing.expect(fake.get_bytes <= s3_fetch.FOOTER_PROBE_MIN);
}

test "binder: a glob is listed once, a one-file glob works, and an empty one fails as it did" {
    if (comptime test_skip.lacks(.posix)) return test_skip.skipNeeding(.posix, "POSIX environment fixture");
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(std.testing.allocator);
    defer fake.deinit();
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const only = try serve(&fake, a, "g/only.parquet", "group_order.parquet");
    const Capture = struct {
        var bytes: [2048]u8 = undefined;
        var len: usize = 0;
        fn write(message: []const u8) void {
            const n = @min(message.len, bytes.len - len);
            @memcpy(bytes[len..][0..n], message[0..n]);
            len += n;
        }
    };
    const saved = frontend.diagnostic_fn;
    frontend.diagnostic_fn = Capture.write;
    defer frontend.diagnostic_fn = saved;
    Capture.len = 0;

    var diag: Diag = .{};
    var schemas = schemasFor(a, try fakeEnv(a, &fake), &diag);
    defer schemas.deinit();
    const pattern = "s3://bkt/g/*.parquet";
    frontend.JoinSchemas.prefetch(&schemas, &.{pattern});
    const meta = try frontend.JoinSchemas.footer(&schemas, pattern);
    try std.testing.expectEqual(only.rows, meta.num_rows);
    const again = try schemas.listing(pattern);
    try std.testing.expectEqual(@as(usize, 1), again.len);
    // One LIST and one GET; the binder asked for neither twice.
    try std.testing.expectEqual(@as(u32, 2), fake.requests_seen.load(.monotonic));

    const none = "s3://bkt/nothing/*.parquet";
    frontend.JoinSchemas.prefetch(&schemas, &.{none});
    try std.testing.expectError(error.NoMatches, frontend.JoinSchemas.footer(&schemas, none));
    const said = Capture.bytes[0..Capture.len];
    const listed_empty = std.mem.count(u8, said, "matched no objects");
    try std.testing.expectEqual(@as(usize, 1), listed_empty);
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, said, "no files matched FROM table"));
}

test "binder: a missing object in a batch is the same named error, on the same object, as reading one at a time" {
    if (comptime test_skip.lacks(.posix)) return test_skip.skipNeeding(.posix, "POSIX environment fixture");
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(std.testing.allocator);
    defer fake.deinit();
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const ok = try serve(&fake, a, "t/ok.parquet", "group_order.parquet");
    const env = try fakeEnv(a, &fake);
    const good = "s3://bkt/t/ok.parquet";
    const missing = "s3://bkt/t/missing.parquet";

    var plain_diag: Diag = .{};
    var plain = schemasFor(a, env, &plain_diag);
    defer plain.deinit();
    const plain_err = frontend.JoinSchemas.footer(&plain, missing);
    try std.testing.expect(std.meta.isError(plain_err));

    var batch_diag: Diag = .{};
    var batched = schemasFor(a, env, &batch_diag);
    defer batched.deinit();
    frontend.JoinSchemas.prefetch(&batched, &.{ good, missing });
    // The batch failed as a whole; the good table is still read, singly, and the bad one reports itself.
    const meta = try frontend.JoinSchemas.footer(&batched, good);
    try std.testing.expectEqual(ok.rows, meta.num_rows);
    const batch_err = frontend.JoinSchemas.footer(&batched, missing);
    try std.testing.expectEqual(plain_err, batch_err);
    try std.testing.expectEqualStrings(plain_diag.input.?, batch_diag.input.?);
    try std.testing.expectEqualStrings(missing, batch_diag.input.?);
    try std.testing.expectEqual(plain_diag.detail.s3_request.status, batch_diag.detail.s3_request.status);
    try std.testing.expectEqualStrings(plain_diag.detail.s3_request.key.get(), batch_diag.detail.s3_request.key.get());
}

test "binder: a local table in a join reads as it did, and only the S3 table goes to the batch" {
    if (comptime test_skip.lacks(.posix)) return test_skip.skipNeeding(.posix, "POSIX environment fixture");
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(std.testing.allocator);
    defer fake.deinit();
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const remote = try serve(&fake, a, "t/mid.parquet", "group_order.parquet");
    var diag: Diag = .{};
    var schemas = schemasFor(a, try fakeEnv(a, &fake), &diag);
    defer schemas.deinit();
    const local_path = fixtures ++ "decimal_precision40.parquet";
    const local_bytes = try metadata.readFileSlice(local_path, a);
    const local_rows = (try metadata.open(a, local_bytes)).num_rows;

    const local = try frontend.JoinSchemas.footer(&schemas, local_path);
    try std.testing.expectEqual(local_rows, local.num_rows);
    try std.testing.expect(!schemas.local.remote_seen);
    try std.testing.expectEqual(@as(u32, 0), fake.gets);
    try std.testing.expectEqual(@as(usize, 1), schemas.local.seen.count());

    frontend.JoinSchemas.prefetch(&schemas, &.{ local_path, "s3://bkt/t/mid.parquet" });
    try std.testing.expectEqual(@as(u32, 1), fake.gets);
    try std.testing.expectEqual(@as(usize, 1), schemas.local.seen.count());
    try std.testing.expectEqual(@as(usize, 1), schemas.local.remote.count());
    const meta = try frontend.JoinSchemas.footer(&schemas, "s3://bkt/t/mid.parquet");
    try std.testing.expectEqual(remote.rows, meta.num_rows);
    try std.testing.expect(schemas.local.remote_seen);
}

test "binder: the retry's bind reads nothing, lists nothing, and still reports a remote join" {
    if (comptime test_skip.lacks(.posix)) return test_skip.skipNeeding(.posix, "POSIX environment fixture");
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(std.testing.allocator);
    defer fake.deinit();
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const first = try serve(&fake, a, "g/a.parquet", "group_order.parquet");
    const env = try fakeEnv(a, &fake);
    var memo: frontend.JoinMemo = .{};
    {
        var diag: Diag = .{};
        var attempt = schemasFor(a, env, &diag);
        attempt.restore(&memo);
        defer attempt.save(&memo);
        defer attempt.deinit();
        frontend.JoinSchemas.prefetch(&attempt, &.{"s3://bkt/g/*.parquet"});
        _ = try frontend.JoinSchemas.footer(&attempt, "s3://bkt/g/*.parquet");
    }
    const requests = fake.requests_seen.load(.monotonic);
    try std.testing.expectEqual(@as(u32, 2), requests);
    var diag: Diag = .{};
    var retry = schemasFor(a, env, &diag);
    retry.restore(&memo);
    defer retry.deinit();
    frontend.JoinSchemas.prefetch(&retry, &.{"s3://bkt/g/*.parquet"});
    const meta = try frontend.JoinSchemas.footer(&retry, "s3://bkt/g/*.parquet");
    try std.testing.expectEqual(first.rows, meta.num_rows);
    try std.testing.expectEqual(requests, fake.requests_seen.load(.monotonic));
    try std.testing.expect(retry.local.remote_seen);
}

test "binder: a glob of many files keeps no more footer reads in flight than the S3 read concurrency" {
    if (comptime test_skip.lacks(.posix)) return test_skip.skipNeeding(.posix, "POSIX environment fixture");
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(std.testing.allocator);
    defer fake.deinit();
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const files = 3 * s3_fetch.READ_CONCURRENCY;
    const bytes = try metadata.readFileSlice(fixtures ++ "group_order.parquet", a);
    var names: [files][]const u8 = undefined;
    for (&names, 0..) |*name, i| {
        const key = try std.fmt.allocPrint(a, "many/f{d:0>3}.parquet", .{i});
        try fake.serveBytes(key, bytes);
        name.* = try std.fmt.allocPrint(a, "s3://bkt/{s}", .{key});
    }
    // Every GET waits at the server; a watcher counts how many wait at once before it lets them go.
    fake.hold_gets = true;
    const Watch = struct {
        fn run(f: *fake_s3.FakeS3, peak: *u32) void {
            var waited: u32 = 0;
            // Until the cap is reached, then a moment longer to see whether anything goes past it.
            while (waited < 5000 and f.max_waiting.load(.acquire) < s3_fetch.READ_CONCURRENCY) : (waited += 5) {
                std.testing.io.sleep(.fromMilliseconds(5), .awake) catch {};
            }
            std.testing.io.sleep(.fromMilliseconds(300), .awake) catch {};
            peak.* = f.max_waiting.load(.acquire);
            f.release_gets.store(true, .release);
        }
    };
    var peak: u32 = 0;
    const watcher = try std.Thread.spawn(.{}, Watch.run, .{ &fake, &peak });
    var diag: Diag = .{};
    var schemas = schemasFor(a, try fakeEnv(a, &fake), &diag);
    defer schemas.deinit();
    frontend.JoinSchemas.prefetch(&schemas, &names);
    watcher.join();
    try std.testing.expectEqual(@as(u32, s3_fetch.READ_CONCURRENCY), peak);
    try std.testing.expectEqual(@as(u32, files), fake.gets);
    try std.testing.expectEqual(@as(usize, files), schemas.local.remote.count());
}

test "binder: a batch of footer reads holds the footers, not the 64 KiB tails they came from" {
    if (comptime test_skip.lacks(.posix)) return test_skip.skipNeeding(.posix, "POSIX environment fixture");
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(std.testing.allocator);
    defer fake.deinit();
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const files = 4 * s3_fetch.READ_CONCURRENCY;
    const bytes = try metadata.readFileSlice(fixtures ++ "group_order.parquet", a);
    var names: [files][]const u8 = undefined;
    for (&names, 0..) |*name, i| {
        const key = try std.fmt.allocPrint(a, "many/f{d:0>3}.parquet", .{i});
        try fake.serveBytes(key, bytes);
        name.* = try std.fmt.allocPrint(a, "s3://bkt/{s}", .{key});
    }
    var diag: Diag = .{};
    const ctx: @import("engine/types.zig").Context = .{ .gpa = std.testing.allocator, .io = std.testing.io, .env = try fakeEnv(a, &fake), .diag = &diag };
    const window: s3_fetch.FetchWindow = .{ .limit = 1 << 30, .footer_only = true, .footer_probe = s3_fetch.FOOTER_PROBE_MIN };
    var opened = try @import("engine/input.zig").openInputs(ctx, a, .{ .inputs = &names, .diag = &diag }, window);
    defer opened.deinit();
    // In flight at most one probe per read slot, and a retained tail would add up to one more per file.
    try std.testing.expect(opened.footer_resident_peak < files * s3_fetch.FOOTER_PROBE_MIN);
}

/// `names` as paths below `base`, sorted.
fn below(a: std.mem.Allocator, base: []const u8, names: []const []const u8) ![]const []const u8 {
    const out = try a.alloc([]const u8, names.len);
    for (names, out) |name, *o| {
        try std.testing.expect(std.mem.startsWith(u8, name, base));
        o.* = name[base.len..];
    }
    std.sort.pdq([]const u8, out, {}, struct {
        fn lt(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.lessThan(u8, x, y);
        }
    }.lt);
    return out;
}

test "glob: an S3 pattern names the files the same pattern names locally, and none from a subdirectory" {
    if (comptime test_skip.lacks(.posix)) return test_skip.skipNeeding(.posix, "POSIX environment fixture");
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(std.testing.allocator);
    defer fake.deinit();
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var tmp = @import("test_scratch.zig").tmpDir(.{});
    defer tmp.cleanup();

    // The tree, as keys and as files. The keys after the first group have no local twin: a directory marker, a key
    // that ends in `/`, and an empty path component.
    const tree = [_][]const u8{
        "top.parquet",       "dir/p1.parquet",          "dir/p2.parquet",     "dir/q1.parquet",
        "dir/sub/p1.parquet", "dir/sub/deeper/p9.parquet", "dirx/p1.parquet",
    };
    const s3_only = [_][]const u8{ "dir/", "dir/p3.parquet/", "dir//p4.parquet", "dir/sub/" };
    for (tree ++ s3_only) |key| try fake.serveBytes(key, "x");
    try tmp.dir.createDirPath(std.testing.io, "dir/sub/deeper");
    try tmp.dir.createDirPath(std.testing.io, "dirx");
    for (tree) |name| {
        const fd = try @import("local_fs.zig").createFile(try std.fmt.allocPrint(a, "{s}/{s}", .{ tmp.path, name }));
        @import("local_fs.zig").close(fd);
    }

    const env = try fakeEnv(a, &fake);
    const Capture = struct {
        fn write(_: []const u8) void {}
    };
    const saved = frontend.diagnostic_fn;
    frontend.diagnostic_fn = Capture.write;
    defer frontend.diagnostic_fn = saved;
    var diag: Diag = .{};
    const cases = [_]struct { pattern: []const u8, want: []const []const u8 }{
        .{ .pattern = "dir/p*.parquet", .want = &.{ "dir/p1.parquet", "dir/p2.parquet" } },
        // A prefix that is also the start of another directory's name: `dirx/p1` is not under `dir/`.
        .{ .pattern = "dir/p*", .want = &.{ "dir/p1.parquet", "dir/p2.parquet" } },
        .{ .pattern = "dir/*.parquet", .want = &.{ "dir/p1.parquet", "dir/p2.parquet", "dir/q1.parquet" } },
        .{ .pattern = "dir/*", .want = &.{ "dir/p1.parquet", "dir/p2.parquet", "dir/q1.parquet" } },
        .{ .pattern = "dir/sub/p*.parquet", .want = &.{"dir/sub/p1.parquet"} },
        .{ .pattern = "dirx/p*", .want = &.{"dirx/p1.parquet"} },
        .{ .pattern = "*.parquet", .want = &.{"top.parquet"} },
        // Only `*` is a wildcard, once. `?` is a plain character, so a pattern without `*` is one path, as it is
        // locally; a second `*` is a plain character too, and matches no file here or there.
        .{ .pattern = "dir/p?.parquet", .want = &.{"dir/p?.parquet"} },
        .{ .pattern = "dir/p**.parquet", .want = &.{} },
    };
    const local_base = try std.fmt.allocPrint(a, "{s}/", .{tmp.path});
    for (cases) |c| {
        const remote = try frontend.expandGlob(a, std.testing.io, try std.fmt.allocPrint(a, "s3://bkt/{s}", .{c.pattern}), env, &diag);
        const local = try frontend.expandGlob(a, std.testing.io, try std.fmt.allocPrint(a, "{s}{s}", .{ local_base, c.pattern }), env, &diag);
        const remote_names = try below(a, "s3://bkt/", remote);
        const local_names = try below(a, local_base, local);
        try std.testing.expectEqual(c.want.len, remote_names.len);
        for (c.want, remote_names) |w, got| try std.testing.expectEqualStrings(w, got);
        try std.testing.expectEqual(local_names.len, remote_names.len);
        for (local_names, remote_names) |l, r| try std.testing.expectEqualStrings(l, r);
    }
    // A wildcard in a directory is refused in both places.
    for ([_][]const u8{ "d*/p*.parquet", "dir/*/p*.parquet" }) |pattern| {
        try std.testing.expectError(error.BadArgs, frontend.expandGlob(a, std.testing.io, try std.fmt.allocPrint(a, "s3://bkt/{s}", .{pattern}), env, &diag));
        try std.testing.expectError(error.BadArgs, frontend.expandGlob(a, std.testing.io, try std.fmt.allocPrint(a, "{s}{s}", .{ local_base, pattern }), env, &diag));
    }
}
