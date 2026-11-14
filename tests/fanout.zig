//! Native coordinator parity: real worker processes, Runtime API, Invoke HTTP, whole query and DuckDB.
const std = @import("std");
const zpq = @import("zpq");
const fake = @import("lambda_integration.zig");
const opts = @import("integration_opts");
const V = std.json.Value;
const a = std.heap.smp_allocator;
const full = "ci/fixtures/parquet/full_match.parquet";
const grouped = "ci/fixtures/parquet/group_order.parquet";
const io = std.testing.io;

const Worker = struct {
    runtime: fake.FakeServer,
    endpoint: fake.FakeServer,
    child: std.process.Child,
    thread: ?std.Thread = null,
    failure: ?anyerror = null,
    fn start(self: *Worker) !void {
        self.runtime = try fake.FakeServer.start();
        errdefer self.runtime.deinit();
        self.endpoint = try fake.FakeServer.start();
        errdefer self.endpoint.deinit();
        const address = try std.fmt.allocPrint(a, "127.0.0.1:{d}", .{self.runtime.port});
        defer a.free(address);
        var extra: std.ArrayList([2][]const u8) = .empty;
        defer extra.deinit(a);
        for ([_][]const u8{ "S3_ENDPOINT_URL", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY", "S3_REGION" }) |name| {
            if (std.testing.environ.getPosix(name)) |value| try extra.append(a, .{ name, value });
        }
        self.child = try fake.spawnLambdaWithEnv(a, address, extra.items);
        errdefer fake.killChild(&self.child);
        self.thread = try std.Thread.spawn(.{}, serve, .{self});
    }
    fn serve(self: *Worker) void {
        self.loop() catch |err| {
            self.failure = err;
        };
    }
    fn loop(self: *Worker) !void {
        while (true) {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const scratch = arena.allocator();
            var req = self.endpoint.acceptRequest(scratch) catch return;
            const response = try fake.invoke(&self.runtime, scratch, req.body);
            try req.replyAndClose(scratch, 200, "Content-Type: application/json\r\n", response);
        }
    }
    fn stop(self: *Worker) void {
        _ = std.os.linux.shutdown(self.endpoint.listen_fd, std.os.linux.SHUT.RDWR);
        fake.killChild(&self.child);
        _ = std.os.linux.shutdown(self.runtime.listen_fd, std.os.linux.SHUT.RDWR);
        if (self.thread) |t| t.join();
        self.endpoint.deinit();
        self.runtime.deinit();
    }
};
const Fixture = struct {
    workers: [4]Worker = undefined,
    pool: zpq.io.invoke.Pool = undefined,
    endpoints: [4]zpq.io.invoke.Endpoint = undefined,
    arena: std.heap.ArenaAllocator,
    started: usize = 0,
    fn start(self: *Fixture) !void {
        self.* = .{ .arena = .init(a) };
        errdefer self.stopWorkers();
        for (&self.workers, &self.endpoints) |*worker, *endpoint| {
            worker.* = .{ .runtime = undefined, .endpoint = undefined, .child = undefined };
            try worker.start();
            self.started += 1;
            const url = try std.fmt.allocPrint(self.arena.allocator(), "http://127.0.0.1:{d}", .{worker.endpoint.port});
            endpoint.* = try zpq.io.invoke.Endpoint.init(self.arena.allocator(), url, "/2015-03-31/functions/worker/invocations");
        }
        try self.pool.init(a);
    }
    fn stopWorkers(self: *Fixture) void {
        for (self.workers[0..self.started]) |*w| w.stop();
        self.arena.deinit();
    }
    fn stop(self: *Fixture) void {
        self.pool.deinit();
        self.stopWorkers();
    }
    fn client(self: *Fixture) zpq.io.invoke.Client {
        return .{ .io = io, .env = .empty, .pool = &self.pool, .endpoints = &self.endpoints };
    }
};
fn get(v: V, key: []const u8) !V {
    if (v != .object) return error.ExpectedObject;
    return v.object.get(key) orelse error.MissingMember;
}
fn same(l: V, r: V) bool {
    if (l == .null or r == .null) return l == .null and r == .null;
    if (l == .number_string or l == .float or l == .integer) {
        const x = numeric(l) catch return false;
        const y = numeric(r) catch return false;
        return @abs(x - y) <= 1e-9 * @max(1, @max(@abs(x), @abs(y)));
    }
    if (l == .string and r == .string) return std.mem.eql(u8, l.string, r.string);
    if (l == .bool and r == .bool) return l.bool == r.bool;
    if (l == .object and r == .object) {
        if (l.object.count() != r.object.count()) return false;
        for (l.object.keys(), l.object.values()) |key, val| if (!same(val, r.object.get(key) orelse return false)) return false;
        return true;
    }
    if (l == .array and r == .array) {
        if (l.array.items.len != r.array.items.len) return false;
        for (l.array.items) |row| {
            var found = false;
            for (r.array.items) |other| if (same(row, other)) {
                found = true;
                break;
            };
            if (!found) return false;
        }
        return true;
    }
    return false;
}
fn numeric(v: V) !f64 {
    return switch (v) {
        .number_string => try std.fmt.parseFloat(f64, v.number_string),
        .string => try std.fmt.parseFloat(f64, v.string),
        .float => v.float,
        .integer => @floatFromInt(v.integer),
        else => error.ExpectedNumber,
    };
}
const Case = struct { inputs: []const []const u8, aggregate: []const u8, group_by: ?[]const u8 = null, filter: ?[]const u8 = null };
const all = "count(*) AS c, count(n) AS cn, sum(n) AS sn, min(n) AS mn, max(n) AS xn, avg(n) AS an, min(s) AS ms, max(s) AS xs, sum(x) AS sx, avg(x) AS ax, sum(u) AS su";
const cases = [_]Case{
    .{ .inputs = &.{full}, .aggregate = all },
    .{ .inputs = &.{full}, .aggregate = all, .filter = "ts < 300" },
    .{ .inputs = &.{full}, .aggregate = all, .filter = "b < 0" },
    .{ .inputs = &.{ full, full }, .aggregate = all, .filter = "n > 100" },
    .{ .inputs = &.{full}, .aggregate = "count(*) AS c, count(n) AS cn, sum(n) AS sn, min(n) AS mn, max(n) AS xn, avg(n) AS an, avg(x) AS ax", .group_by = "b" },
    .{ .inputs = &.{full}, .aggregate = "count(*) AS c, sum(n) AS sn, avg(n) AS an, min(s) AS ms", .group_by = "b AS bucket", .filter = "ts < 300" },
    .{ .inputs = &.{grouped}, .aggregate = "count(*) AS c, sum(i) AS si, min(x) AS mx, max(i) AS xi, avg(x) AS ax", .group_by = "k" },
    .{ .inputs = &.{ grouped, grouped }, .aggregate = "count(*) AS c, sum(x) AS sx, avg(i) AS ai", .group_by = "k AS key", .filter = "i > 5000" },
};
fn request(scratch: std.mem.Allocator, c: Case) !V {
    return zpq.engine.fanout.parse(scratch, try std.json.Stringify.valueAlloc(scratch, c, .{ .emit_null_optional_fields = false }));
}
fn oracle(scratch: std.mem.Allocator, c: Case) !V {
    return oracleWithOrder(scratch, c, "");
}
fn oracleWithOrder(scratch: std.mem.Allocator, c: Case, suffix: []const u8) !V {
    var names: std.ArrayList(u8) = .empty;
    try names.append(scratch, '[');
    for (c.inputs, 0..) |input, i| {
        if (i > 0) try names.append(scratch, ',');
        try names.print(scratch, "'{s}'", .{input});
    }
    try names.append(scratch, ']');
    // GROUP BY aliases belong in SELECT; DuckDB accepts GROUP BY ALL for these aggregate-only requests.
    const sql = try std.fmt.allocPrint(scratch, "SET memory_limit='1GB'; SET threads=1; SELECT {s}{s}{s} FROM read_parquet({s}) {s}{s} {s} {s};", .{
        if (c.group_by != null) c.group_by.? else "", if (c.group_by != null) "," else "", c.aggregate,                                    names.items,
        if (c.filter != null) "WHERE " else "",       c.filter orelse "",                  if (c.group_by != null) "GROUP BY ALL" else "", suffix,
    });
    const result = try std.process.run(scratch, io, .{ .argv = &.{ opts.duckdb, "-no-init", "-json", "-c", sql } });
    if (result.term != .exited or result.term.exited != 0) {
        std.debug.print("oracle: {s}\n", .{result.stderr});
        return error.OracleFailed;
    }
    return zpq.engine.fanout.parse(scratch, result.stdout);
}

test "fanout parity: eight query shapes, wrong counts, globs, real Invoke and DuckDB" {
    if (@import("builtin").os.tag != .linux) return error.SkipZigTest;
    var fixture: Fixture = undefined;
    try fixture.start();
    defer fixture.stop();
    var client = fixture.client();
    for (cases) |c| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const req = try request(scratch, c);
        const whole = try zpq.engine.fanout.parse(scratch, try client.transport().invoke(client.transport().ctx, scratch, 0, try std.json.Stringify.valueAlloc(scratch, req, .{})));
        const ref = try oracle(scratch, c);
        for ([_]usize{ 1, 2, 4, 16 }) |workers| {
            const counts = try scratch.alloc(u64, c.inputs.len);
            @memset(counts, 3);
            for ([_]?[]const u64{ null, counts }) |hint| {
                const out = try zpq.engine.fanout.run(scratch, a, client.transport(), req, .{ .workers = workers, .concurrency = 4, .counts = hint });
                const agg = try get(out, "agg");
                const whole_agg = try get(whole, "agg");
                const expected = if (c.group_by == null) ref.array.items[0] else ref;
                if (!same(agg, whole_agg) or !same(agg, expected)) {
                    std.debug.print("query {s} workers={d}\ngot={s}\nwhole={s}\nref={s}\n", .{
                        c.aggregate,                                                workers,                                                   try std.json.Stringify.valueAlloc(scratch, agg, .{}),
                        try std.json.Stringify.valueAlloc(scratch, whole_agg, .{}), try std.json.Stringify.valueAlloc(scratch, expected, .{}),
                    });
                    return error.AnswerMismatch;
                }
            }
        }
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const req = try request(scratch, .{ .inputs = &.{"ci/fixtures/parquet/full_matc*.parquet"}, .aggregate = "count(*) AS n" });
    const out = try zpq.engine.fanout.run(scratch, a, client.transport(), req, .{ .workers = 4, .concurrency = 4 });
    try std.testing.expectEqual(@as(f64, 1600), try numeric(try get(try get(out, "agg"), "n")));
}

test "fanout parity: worker refusals, quoted aliases, empty ranges and global order" {
    var fixture: Fixture = undefined;
    try fixture.start();
    defer fixture.stop();
    var client = fixture.client();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const quoted: Case = .{ .inputs = &.{full}, .aggregate = "count(*) FILTER (WHERE s != 'O''Reilly)') AS \"count value\", avg(x) AS \"average value\"", .group_by = "b AS \"group name\"" };
    const req = try request(scratch, quoted);
    const out = try zpq.engine.fanout.run(scratch, a, client.transport(), req, .{ .workers = 3, .concurrency = 4 });
    const quoted_ref = try oracle(scratch, quoted);
    if (!same(try get(out, "agg"), quoted_ref)) {
        std.debug.print("quoted got={s} ref={s}\n", .{ try std.json.Stringify.valueAlloc(scratch, try get(out, "agg"), .{}), try std.json.Stringify.valueAlloc(scratch, quoted_ref, .{}) });
        return error.QuotedMismatch;
    }
    for ([_][]const u8{ "count(DISTINCT s) AS d", "median(x) AS m", "quantile(x, 0.9) AS q", "sum(x) OVER (PARTITION BY b) AS w", "sum(x) / count(*) AS r", "x AS y" }) |agg| {
        const refused = zpq.engine.fanout.run(scratch, a, client.transport(), try request(scratch, .{ .inputs = &.{"missing.parquet"}, .aggregate = agg }), .{ .workers = 4, .concurrency = 4 });
        if (refused) |_| return error.ExpectedRefusal else |_| {}
    }
    const ordered: Case = .{ .inputs = &.{grouped}, .aggregate = "sum(i) AS si, count(*) AS c", .group_by = "k" };
    const ordered_out = try zpq.engine.fanout.run(scratch, a, client.transport(), try request(scratch, ordered), .{ .workers = 8, .concurrency = 4, .order_by = "si DESC, k", .limit = 5 });
    const expected = try oracleWithOrder(scratch, ordered, "ORDER BY si DESC, k LIMIT 5");
    const got = try get(ordered_out, "agg");
    try std.testing.expectEqual(5, got.array.items.len);
    for (got.array.items, expected.array.items) |l, r| if (!same(l, r)) return error.OrderMismatch;
    const ranges = [_][]const u8{
        "{\"operation\":\"partial_aggregate\",\"plan_version\":1,\"partial\":{\"inputs\":[\"" ++ full ++ "\"],\"row_groups\":[[5,5]],\"aggregate\":\"count(*) AS c, sum(n) AS s, avg(n) AS a\"}}",
        "{\"operation\":\"partial_aggregate\",\"plan_version\":1,\"partial\":{\"inputs\":[\"" ++ full ++ "\"],\"row_groups\":[[0,9]],\"aggregate\":\"count(*) AS c\"}}",
    };
    const empty = try zpq.engine.fanout.parse(scratch, try client.transport().invoke(client.transport().ctx, scratch, 0, ranges[0]));
    const empty_agg = try zpq.engine.fanout.merge(scratch, &.{empty});
    try std.testing.expectEqual(@as(f64, 0), try numeric(try get(empty_agg, "c")));
    if ((try get(empty_agg, "s")) != .null) return error.EmptySumMismatch;
    if ((try get(empty_agg, "a")) != .null) return error.EmptyAvgMismatch;
    const past = try zpq.engine.fanout.parse(scratch, try client.transport().invoke(client.transport().ctx, scratch, 0, ranges[1]));
    try std.testing.expectEqualStrings("RowGroupOutOfRange", (try get(past, "reason")).string);
    var argv: std.ArrayList([]const u8) = .empty;
    try argv.appendSlice(scratch, &.{ opts.cli_bin, "fanout", "--workers", "4", "--concurrency", "4", "--input", full, "--aggregate", "count(*) AS n" });
    for (fixture.endpoints) |endpoint| try argv.appendSlice(scratch, &.{ "--endpoint", try std.fmt.allocPrint(scratch, "http://{s}", .{endpoint.authority}) });
    const cli_result = try std.process.run(scratch, io, .{ .argv = argv.items });
    if (cli_result.term != .exited or cli_result.term.exited != 0) {
        std.debug.print("CLI: {s}\n", .{cli_result.stderr});
        return error.CliFailed;
    }
    const cli_answer = try zpq.engine.fanout.parse(scratch, cli_result.stdout);
    try std.testing.expectEqual(@as(f64, 1600), try numeric(try get(try get(cli_answer, "agg"), "n")));
    var coordinator_rt = try fake.FakeServer.start();
    defer coordinator_rt.deinit();
    var coordinator_child = try fake.spawnLambdaWithEnv(a, try std.fmt.allocPrint(scratch, "127.0.0.1:{d}", .{coordinator_rt.port}), &.{});
    defer fake.killChild(&coordinator_child);
    const urls = try scratch.alloc([]const u8, fixture.endpoints.len);
    for (fixture.endpoints, urls) |endpoint, *url| url.* = try std.fmt.allocPrint(scratch, "http://{s}", .{endpoint.authority});
    const event: zpq.fanout.Event = .{ .endpoints = urls, .workers = 4, .concurrency = 4, .request = try request(scratch, .{ .inputs = &.{full}, .aggregate = "count(*) AS n" }) };
    const coordinated = try zpq.engine.fanout.parse(scratch, try fake.invoke(&coordinator_rt, scratch, try std.json.Stringify.valueAlloc(scratch, event, .{})));
    try std.testing.expectEqual(@as(f64, 1600), try numeric(try get(try get(coordinated, "agg"), "n")));
    if (std.testing.environ.getPosix("ZPQ_FANOUT_S3_INPUT")) |input| {
        const remote: Case = .{ .inputs = &.{input}, .aggregate = all };
        const remote_req = try request(scratch, remote);
        const remote_out = try zpq.engine.fanout.run(scratch, a, client.transport(), remote_req, .{ .workers = 4, .concurrency = 4 });
        const local_ref = try oracle(scratch, .{ .inputs = &.{full}, .aggregate = all });
        try std.testing.expect(same(try get(remote_out, "agg"), local_ref.array.items[0]));
        std.debug.print("rustfs fanout: 4 real workers, S3-only input, DuckDB answer matched\n", .{});
    }
}

test "fanout parity: schema disagreement and plan version refusals before I/O" {
    var fixture: Fixture = undefined;
    try fixture.start();
    defer fixture.stop();
    var client = fixture.client();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const dec = "zig-out/fanout-schema-dec.parquet";
    const big = "zig-out/fanout-schema-big.parquet";
    defer std.Io.Dir.cwd().deleteFile(io, dec) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, big) catch {};
    const made = try std.process.run(scratch, io, .{ .argv = &.{ opts.duckdb, "-no-init", "-c", "SET memory_limit='1GB'; SET threads=1; COPY (SELECT 1.00::DECIMAL(15,2) AS x) TO '" ++ dec ++ "' (FORMAT parquet); COPY (SELECT 1::BIGINT AS x) TO '" ++ big ++ "' (FORMAT parquet);" } });
    if (made.term != .exited or made.term.exited != 0) return error.OracleFailed;
    const mixed = try request(scratch, .{ .inputs = &.{ dec, big }, .aggregate = "count(*) AS c", .group_by = "x" });
    for ([_]?[]const u64{ null, &.{ 1, 1 }, &.{ 1, 0 } }) |hint| {
        try std.testing.expectError(error.SchemaMismatch, zpq.engine.fanout.run(scratch, a, client.transport(), mixed, .{ .workers = 2, .concurrency = 4, .counts = hint }));
    }
    const bad = [_]struct { body: []const u8, reason: []const u8 }{
        .{ .body = "{\"inputs\":[\"missing\"],\"plan_version\":2,\"plan\":{}}", .reason = "UnsupportedPlanVersion" },
        .{ .body = "{\"inputs\":[\"missing\"],\"plan\":{}}", .reason = "UnsupportedPlanVersion" },
        .{ .body = "{\"operation\":\"prepare_aggregate\",\"plan_version\":1,\"plan\":{},\"inputs\":[\"missing\"]}", .reason = "ConflictingPlanFields" },
        .{ .body = "{\"operation\":\"partial_aggregate\",\"plan_version\":1,\"inputs\":[\"missing\"],\"partial\":{}}", .reason = "ConflictingPlanFields" },
    };
    for (bad) |item| {
        const response = try zpq.engine.fanout.parse(scratch, try client.transport().invoke(client.transport().ctx, scratch, 0, item.body));
        try std.testing.expectEqualStrings(item.reason, (try get(response, "reason")).string);
    }
    const legacy = try zpq.engine.fanout.parse(scratch, try client.transport().invoke(client.transport().ctx, scratch, 0, "{\"inputs\":[\"" ++ full ++ "\"],\"aggregate\":\"count(*) AS n\"}"));
    const null_fields = try zpq.engine.fanout.parse(scratch, try client.transport().invoke(client.transport().ctx, scratch, 0, "{\"inputs\":[\"" ++ full ++ "\"],\"aggregate\":\"count(*) AS n\",\"operation\":null,\"plan\":null,\"plan_version\":null}"));
    try std.testing.expect(same(try get(legacy, "agg"), try get(null_fields, "agg")));
    var deep: std.ArrayList(u8) = .empty;
    try deep.appendSlice(scratch, "{\"inputs\":[\"missing\"],\"plan_version\":1,\"plan\":");
    try deep.appendNTimes(scratch, '[', 512);
    try deep.appendSlice(scratch, "null");
    try deep.appendNTimes(scratch, ']', 512);
    try deep.append(scratch, '}');
    const response = try zpq.engine.fanout.parse(scratch, try client.transport().invoke(client.transport().ctx, scratch, 0, deep.items));
    try std.testing.expectEqualStrings("PlanTooDeep", (try get(response, "reason")).string);
}
