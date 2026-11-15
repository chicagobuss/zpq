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
    fn startWithEnv(self: *Worker, override: []const [2][]const u8) !void {
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
        try extra.appendSlice(a, override);
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
        return self.startWithEnv(&.{});
    }
    fn startWithEnv(self: *Fixture, override: []const [2][]const u8) !void {
        self.* = .{ .arena = .init(a) };
        errdefer self.stopWorkers();
        for (&self.workers, &self.endpoints) |*worker, *endpoint| {
            worker.* = .{ .runtime = undefined, .endpoint = undefined, .child = undefined };
            try worker.startWithEnv(override);
            self.started += 1;
            const url = try std.fmt.allocPrint(self.arena.allocator(), "http://127.0.0.1:{d}", .{worker.endpoint.port});
            endpoint.* = try zpq.io.invoke.Endpoint.init(self.arena.allocator(), url, "/2015-03-31/functions/worker/invocations");
        }
        try self.pool.init(a);
    }
    /// Workers that read objects from `store` with fake credentials.
    fn startS3(self: *Fixture, scratch: std.mem.Allocator, store: *const fake.FakeS3) !void {
        return self.startWithEnv(&.{
            .{ "S3_ENDPOINT_URL", try std.fmt.allocPrint(scratch, "http://127.0.0.1:{d}", .{store.port}) },
            .{ "S3_ACCESS_KEY_ID", "fake" },
            .{ "S3_SECRET_ACCESS_KEY", "fake" },
            .{ "S3_REGION", "us-east-1" },
            .{ "ZPQ_S3_PRECONNECT", "0" },
        });
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
/// DuckDB's `-json` stdout for `script`, run single-threaded under a 1GB cap; any failure is `OracleFailed`.
fn duckdb(scratch: std.mem.Allocator, script: []const u8) ![]const u8 {
    const full_script = try std.mem.concat(scratch, u8, &.{ "SET memory_limit='1GB'; SET threads=1; ", script });
    const result = try std.process.run(scratch, io, .{ .argv = &.{ opts.duckdb, "-no-init", "-json", "-c", full_script } });
    if (result.term != .exited or result.term.exited != 0) {
        std.debug.print("duckdb: {s}\n", .{result.stderr});
        return error.OracleFailed;
    }
    return result.stdout;
}
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
    const sql = try std.fmt.allocPrint(scratch, "SELECT {s}{s}{s} FROM read_parquet({s}) {s}{s} {s} {s};", .{
        if (c.group_by != null) c.group_by.? else "", if (c.group_by != null) "," else "", c.aggregate,                                    names.items,
        if (c.filter != null) "WHERE " else "",       c.filter orelse "",                  if (c.group_by != null) "GROUP BY ALL" else "", suffix,
    });
    return zpq.engine.fanout.parse(scratch, try duckdb(scratch, sql));
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
                for ((try get(out, "worker_stats")).array.items) |stat| {
                    const pool = try get(stat, "pool");
                    for ([_][]const u8{ "get_requests", "head_requests", "bytes_fetched", "tls_handshake_ms_sum", "ttfb_ms_sum" }) |key|
                        try std.testing.expectEqual(@as(f64, 0), try numeric(try get(pool, key)));
                }
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

test "fanout Invoke retries transient failures exactly once per split and reports terminal HTTP details" {
    const Fault = struct {
        status: u16 = 503,
        close: bool = false,
        truncated: bool = false,
        repeat: bool = false,
        function_error: bool = false,
        header_type: bool = false,
        code: []const u8 = "ServiceException",
        expected: ?anyerror = null,
        calls: usize = 5,
        retries: u64 = 1,
    };
    const Server = struct {
        server: fake.FakeServer,
        fault: Fault,
        calls: std.atomic.Value(usize) = .init(0),
        fn serve(self: *@This()) void {
            while (true) {
                var req = self.server.acceptRequest(a) catch return;
                const n = self.calls.fetchAdd(1, .monotonic);
                if (n == 0 or self.fault.repeat) {
                    if (self.fault.close or self.fault.truncated) {
                        if (self.fault.truncated) {
                            const raw = if (self.fault.function_error)
                                "HTTP/1.1 200 OK\r\nX-Amz-Function-Error: Unhandled\r\nContent-Length: 100\r\n\r\n{"
                            else
                                "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{";
                            _ = std.os.linux.sendto(req.conn, raw.ptr, raw.len, std.os.linux.MSG.NOSIGNAL, null, 0);
                        }
                        req.deinit();
                        continue;
                    }
                    var arena = std.heap.ArenaAllocator.init(a);
                    defer arena.deinit();
                    const scratch = arena.allocator();
                    const body = std.fmt.allocPrint(scratch, "{{\"__type\":\"namespace#{s}\",\"message\":\"PRIVATE-MESSAGE\"}}", .{self.fault.code}) catch return;
                    const headers = if (self.fault.function_error) "X-Amz-Function-Error: Unhandled\r\n" else if (self.fault.header_type)
                        "x-amzn-ErrorType: TooManyRequestsException:extra\r\n"
                    else
                        "";
                    req.replyAndClose(a, self.fault.status, headers, body) catch return;
                } else {
                    req.replyAndClose(a, 200, "",
                        \\{"partial_version":1,"schema_fingerprint":"s","plan_digest":"p","groups":[],"aggregates":[{"function":"count","name":"n"}],"agg":{"_a0":1}}
                    ) catch return;
                }
            }
        }
    };
    for ([_]Fault{
        .{ .close = true },
        .{ .truncated = true },
        .{ .status = 429, .header_type = true, .code = "TooManyRequestsException" },
        .{},
        .{ .code = "TooManyRequestsException" },
        .{ .repeat = true, .expected = error.InvokeHttpFailed, .calls = 3, .retries = 2 },
        .{ .status = 403, .repeat = true, .code = "AccessDeniedException", .expected = error.InvokeHttpFailed, .calls = 1, .retries = 0 },
        .{ .status = 408, .repeat = true, .expected = error.InvokeHttpFailed, .calls = 1, .retries = 0 },
        .{ .status = 400, .repeat = true, .code = "TooManyRequestsException", .expected = error.InvokeHttpFailed, .calls = 1, .retries = 0 },
        .{ .function_error = true, .repeat = true, .expected = error.WorkerFunctionFailed, .calls = 1, .retries = 0 },
        .{ .status = 200, .function_error = true, .repeat = true, .expected = error.WorkerFunctionFailed, .calls = 1, .retries = 0 },
        .{ .truncated = true, .function_error = true, .expected = error.WorkerFunctionFailed, .calls = 1, .retries = 0 },
    }) |fault| {
        var server: Server = .{ .server = try fake.FakeServer.start(), .fault = fault };
        defer server.server.deinit();
        const thread = try std.Thread.spawn(.{}, Server.serve, .{&server});
        defer {
            _ = std.os.linux.shutdown(server.server.listen_fd, std.os.linux.SHUT.RDWR);
            thread.join();
        }
        var pool: zpq.io.invoke.Pool = undefined;
        try pool.init(a);
        defer pool.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const url = try std.fmt.allocPrint(scratch, "http://127.0.0.1:{d}", .{server.server.port});
        const event: zpq.fanout.Event = .{
            .endpoints = &.{url},
            .workers = if (fault.expected == null) 4 else 1,
            .concurrency = 4,
            .request = try zpq.engine.fanout.parse(scratch, "{\"inputs\":[\"unused\"],\"aggregate\":\"count(*) AS n\"}"),
        };
        const body = try std.json.Stringify.valueAlloc(scratch, event, .{});
        const result = zpq.fanout.handle(io, a, .empty, &pool, body);
        if (fault.expected) |err| {
            try std.testing.expectError(err, result);
            if (err == error.InvokeHttpFailed) {
                var buffer: [256]u8 = undefined;
                const detail = pool.errorMessage(io, err, &buffer);
                try std.testing.expect(std.mem.indexOf(u8, detail, try std.fmt.allocPrint(scratch, "HTTP {d}", .{fault.status})) != null);
                try std.testing.expect(std.mem.indexOf(u8, detail, fault.code) != null);
                try std.testing.expect(std.mem.indexOf(u8, detail, "PRIVATE-MESSAGE") == null);
            }
        } else {
            const answer = try result;
            defer a.free(answer);
            const parsed = try zpq.engine.fanout.parse(scratch, answer);
            try std.testing.expectEqual(@as(f64, 4), try numeric(try get(try get(parsed, "agg"), "n")));
            try std.testing.expectEqual(@as(f64, 1), try numeric(try get(try get(parsed, "transport"), "retries")));
            const kind = if (fault.close or fault.truncated) "transport_retries" else if (fault.status == 429 or std.mem.eql(u8, fault.code, "TooManyRequestsException")) "throttle_retries" else "server_retries";
            try std.testing.expectEqual(@as(f64, 1), try numeric(try get(try get(parsed, "transport"), kind)));
        }
        var retries: u64 = 0;
        for (pool.snapshotRetries()) |n| retries += n;
        try std.testing.expectEqual(fault.calls, server.calls.load(.monotonic));
        try std.testing.expectEqual(fault.retries, retries);
        if (fault.repeat and fault.status == 503 and !fault.function_error) {
            var rt = try fake.FakeServer.start();
            defer rt.deinit();
            var child = try fake.spawnLambdaWithEnv(a, try std.fmt.allocPrint(scratch, "127.0.0.1:{d}", .{rt.port}), &.{});
            defer fake.killChild(&child);
            var poll = try rt.acceptRequest(scratch);
            try poll.replyAndClose(scratch, 200, "Lambda-Runtime-Aws-Request-Id: retry-error\r\n", body);
            var response = try rt.acceptRequest(scratch);
            try std.testing.expect(std.mem.endsWith(u8, response.path, "/error"));
            const failed = try zpq.engine.fanout.parse(scratch, response.body);
            try response.replyAndClose(scratch, 202, "", "");
            const detail = (try get(failed, "errorMessage")).string;
            try std.testing.expect(std.mem.indexOf(u8, detail, "HTTP 503, AWS ServiceException") != null);
            try std.testing.expect(std.mem.indexOf(u8, detail, "PRIVATE-MESSAGE") == null);
            try std.testing.expectEqual(@as(usize, 6), server.calls.load(.monotonic));
        }
    }
}

test "fanout Invoke permits function execution beyond S3 first-byte and body idle limits" {
    const Slow = struct {
        fn serve(server: *fake.FakeServer) void {
            var req = server.acceptRequest(a) catch return;
            defer req.deinit();
            zpq.clock.sleepNs(11 * std.time.ns_per_s);
            const header = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n";
            const linux = std.os.linux;
            if (linux.sendto(req.conn, header.ptr, header.len, linux.MSG.NOSIGNAL, null, 0) != header.len) return;
            zpq.clock.sleepNs(11 * std.time.ns_per_s);
            _ = linux.sendto(req.conn, "{}".ptr, 2, linux.MSG.NOSIGNAL, null, 0);
        }
    };
    var server = try fake.FakeServer.start();
    defer server.deinit();
    const thread = try std.Thread.spawn(.{}, Slow.serve, .{&server});
    defer {
        _ = std.os.linux.shutdown(server.listen_fd, std.os.linux.SHUT.RDWR);
        thread.join();
    }
    var pool: zpq.io.invoke.Pool = undefined;
    try pool.init(a);
    defer pool.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const endpoints = [_]zpq.io.invoke.Endpoint{try zpq.io.invoke.Endpoint.init(scratch, try std.fmt.allocPrint(scratch, "http://127.0.0.1:{d}", .{server.port}), "/2015-03-31/functions/worker/invocations")};
    var client: zpq.io.invoke.Client = .{ .io = io, .env = .empty, .pool = &pool, .endpoints = &endpoints };
    const transport = client.transport();
    try std.testing.expectEqualStrings("{}", try transport.invoke(transport.ctx, scratch, 0, "{}"));
}

test "fanout parity: grouped AVG orders ASC and DESC with NULL last against DuckDB" {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    try std.Io.Dir.cwd().createDirPath(io, "zig-out");
    const path = try zpq.test_scratch.filePath(scratch, "zig-out/fanout-avg-order.parquet");
    defer std.Io.Dir.cwd().deleteFile(io, path) catch {};
    _ = try duckdb(scratch, try std.mem.concat(scratch, u8, &.{ "COPY (SELECT i%3 AS k, ", "CASE WHEN i%3=2 THEN NULL ELSE i%2+10*(i%3) END AS x FROM range(6144) t(i)) ", "TO '", path, "' (FORMAT PARQUET, ROW_GROUP_SIZE 2048);" }));
    var fixture: Fixture = undefined;
    try fixture.start();
    defer fixture.stop();
    var client = fixture.client();
    const c: Case = .{ .inputs = &.{path}, .aggregate = "avg(x) AS a", .group_by = "k" };
    for ([_][]const u8{ "a ASC, k", "a DESC, k" }) |ordering| {
        const out = try zpq.engine.fanout.run(scratch, a, client.transport(), try request(scratch, c), .{
            .workers = 4,
            .concurrency = 4,
            .order_by = ordering,
        });
        const expected = try oracleWithOrder(scratch, c, try std.fmt.allocPrint(scratch, "ORDER BY {s}", .{ordering}));
        const rows = (try get(out, "agg")).array.items;
        try std.testing.expectEqual(3, rows.len);
        try std.testing.expectEqual(3, expected.array.items.len);
        try std.testing.expect((try get(rows[0], "a")) == .float and (try get(rows[1], "a")) == .float);
        try std.testing.expect((try get(rows[2], "a")) == .null);
        for (rows, expected.array.items) |l, r| if (!same(l, r)) return error.OrderMismatch;
    }
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
    try std.Io.Dir.cwd().createDirPath(io, "zig-out");
    const dec = try zpq.test_scratch.filePath(scratch, "zig-out/fanout-schema-dec.parquet");
    const big = try zpq.test_scratch.filePath(scratch, "zig-out/fanout-schema-big.parquet");
    defer std.Io.Dir.cwd().deleteFile(io, dec) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, big) catch {};
    _ = try duckdb(scratch, try std.mem.concat(scratch, u8, &.{ "COPY (SELECT 1.00::DECIMAL(15,2) AS x) TO '", dec, "' (FORMAT parquet); COPY (SELECT 1::BIGINT AS x) TO '", big, "' (FORMAT parquet);" }));
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

test "fanout joins: bounded replicated S3 dimensions, inner left semi anti, generation disagreement" {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    try std.Io.Dir.cwd().createDirPath(io, "zig-out");
    const fact = try zpq.test_scratch.filePath(scratch, "zig-out/fanout-join-fact.parquet");
    const dim = try zpq.test_scratch.filePath(scratch, "zig-out/fanout-join-dimension.parquet");
    defer std.Io.Dir.cwd().deleteFile(io, fact) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, dim) catch {};
    _ = try duckdb(scratch, try std.mem.concat(scratch, u8, &.{ "COPY (SELECT i::BIGINT AS id, CASE WHEN i%64=0 THEN NULL ELSE (i%4)::BIGINT END AS fk, i::BIGINT AS amount FROM range(8192) t(i)) TO '", fact, "' (FORMAT parquet, ROW_GROUP_SIZE 2048); COPY (SELECT k::BIGINT AS dim_key, 'r'||k AS region FROM range(3) t(k)) TO '", dim, "' (FORMAT parquet);" }));
    const fact_bytes = try fake.paddedParquet(scratch, fact, 2 << 20);
    const dim_bytes = try std.Io.Dir.cwd().readFileAlloc(io, dim, scratch, .limited(64 << 20));
    var store: fake.FakeS3 = undefined;
    try store.startPair(fact_bytes, dim_bytes, 0);
    defer store.deinit();
    var fixture: Fixture = undefined;
    try fixture.startS3(scratch, &store);
    defer fixture.stop();
    var client = fixture.client();
    for ([_][]const u8{ "inner", "left", "semi", "anti" }) |kind| {
        const simple = std.mem.eql(u8, kind, "semi") or std.mem.eql(u8, kind, "anti");
        const req = try zpq.engine.fanout.parse(scratch, try std.json.Stringify.valueAlloc(scratch, .{
            .inputs = &.{"s3://test/fact.parquet"},
            .join_strategy = "replicated",
            .aggregate = "count(*) AS c, sum(amount) AS s, avg(amount) AS a",
            .group_by = if (simple) "fk" else "region",
            .join = .{ .kind = kind, .fk = "fk", .build = .{
                .path = "s3://test/dimension.parquet",
                .key = "dim_key",
                .columns = if (simple) @as([]const []const u8, &.{}) else @as([]const []const u8, &.{"region"}),
            } },
        }, .{}));
        const parts = try zpq.engine.fanout.split(scratch, &.{4}, 1);
        const probe = try zpq.engine.fanout.parse(scratch, try client.transport().invoke(client.transport().ctx, scratch, 0, try zpq.engine.fanout.partialPayload(scratch, req, parts[0])));
        if (probe.object.get("error") != null) {
            std.debug.print("join {s} worker: {s}\n", .{ kind, try std.json.Stringify.valueAlloc(scratch, probe, .{}) });
            return error.WorkerJoinFailed;
        }
        const out = try zpq.engine.fanout.run(scratch, a, client.transport(), req, .{ .workers = 4, .concurrency = 4 });
        for ((try get(out, "worker_stats")).array.items) |stat| {
            const pool = try get(stat, "pool");
            try std.testing.expect(try numeric(try get(pool, "get_requests")) > 0);
            try std.testing.expect(try numeric(try get(pool, "bytes_fetched")) > 0);
            try std.testing.expectEqual(@as(f64, 0), try numeric(try get(pool, "head_requests")));
            try std.testing.expectEqual(@as(f64, 0), try numeric(try get(pool, "tls_handshake_ms_sum")));
            try std.testing.expect(try numeric(try get(pool, "ttfb_ms_max")) > 0);
            try std.testing.expect(try numeric(try get(pool, "ttfb_ms_sum")) >= try numeric(try get(pool, "ttfb_ms_max")));
        }
        const sql = if (std.mem.eql(u8, kind, "anti"))
            try std.fmt.allocPrint(scratch, "SELECT fk, count(*) AS c, sum(amount) AS s, avg(amount) AS a FROM read_parquet('{s}') WHERE fk NOT IN (SELECT dim_key FROM read_parquet('{s}')) GROUP BY ALL;", .{ fact, dim })
        else
            try std.fmt.allocPrint(scratch, "SELECT {s}, count(*) AS c, sum(amount) AS s, avg(amount) AS a FROM read_parquet('{s}') f {s} JOIN read_parquet('{s}') d ON f.fk=d.dim_key GROUP BY ALL;", .{ if (simple) "fk" else "region", fact, kind, dim });
        try std.testing.expect(same(try get(out, "agg"), try zpq.engine.fanout.parse(scratch, try duckdb(scratch, sql))));
        if (!simple) {
            var filtered = req;
            filtered.object = try req.object.clone(scratch);
            try filtered.object.put(scratch, "filter", .{ .string = "region = 'r1'" });
            const filtered_out = try zpq.engine.fanout.run(scratch, a, client.transport(), filtered, .{ .workers = 4, .concurrency = 4 });
            const filtered_sql = try std.fmt.allocPrint(scratch, "SELECT region,count(*) AS c,sum(amount) AS s,avg(amount) AS a FROM read_parquet('{s}') f {s} JOIN read_parquet('{s}') d ON f.fk=d.dim_key WHERE region='r1' GROUP BY ALL;", .{ fact, kind, dim });
            try std.testing.expect(same(try get(filtered_out, "agg"), try zpq.engine.fanout.parse(scratch, try duckdb(scratch, filtered_sql))));
        }
        if (std.mem.eql(u8, kind, "inner")) {
            var automatic = req;
            automatic.object = try req.object.clone(scratch);
            _ = automatic.object.swapRemove("join_strategy");
            const automatic_out = try zpq.engine.fanout.run(scratch, a, client.transport(), automatic, .{ .workers = 3, .concurrency = 3 });
            try std.testing.expectEqualStrings("partial", (try get(automatic_out, "protocol")).string);
            try std.testing.expect(same(try get(automatic_out, "agg"), try get(out, "agg")));
            try automatic.object.put(scratch, "join_strategy", .{ .string = "aligned" });
            try std.testing.expectError(error.AlignedJoinMissingStats, zpq.engine.fanout.run(scratch, a, client.transport(), automatic, .{ .workers = 3 }));
            var compact = req;
            compact.object = try req.object.clone(scratch);
            try compact.object.put(scratch, "aggregate", .{ .string = "count(*) AS c, sum(amount) AS s" });
            var twice = std.json.Array.init(scratch);
            try twice.append(.{ .string = "s3://test/fact.parquet" });
            try twice.append(.{ .string = "s3://test/fact.parquet" });
            try compact.object.put(scratch, "inputs", .{ .array = twice });
            const compact_out = try zpq.engine.fanout.run(scratch, a, client.transport(), compact, .{ .workers = 3, .concurrency = 3, .counts = &.{ 4, 4 } });
            try std.testing.expectEqual((try get(out, "agg")).array.items.len, (try get(compact_out, "agg")).array.items.len);
            for ((try get(compact_out, "agg")).array.items) |row| {
                const key = try get(row, "region");
                var matched = false;
                for ((try get(out, "agg")).array.items) |original| {
                    if (same(key, try get(original, "region"))) {
                        try std.testing.expectEqual(2 * try numeric(try get(original, "c")), try numeric(try get(row, "c")));
                        try std.testing.expectEqual(2 * try numeric(try get(original, "s")), try numeric(try get(row, "s")));
                        matched = true;
                    }
                }
                try std.testing.expect(matched);
            }
            var star = req;
            star.object = try req.object.clone(scratch);
            var star_join = try get(req, "join");
            star_join.object = try star_join.object.clone(scratch);
            var star_build = try get(star_join, "build");
            star_build.object = try star_build.object.clone(scratch);
            try star_build.object.put(scratch, "columns", .{ .array = std.json.Array.init(scratch) });
            try star_join.object.put(scratch, "build", star_build);
            try star_join.object.put(scratch, "dims", try zpq.engine.fanout.parse(scratch, "[{\"fk\":\"fk\",\"build\":{\"path\":\"s3://test/dimension.parquet\",\"key\":\"dim_key\",\"columns\":[\"region\"]}}]"));
            try star.object.put(scratch, "join", star_join);
            const star_out = try zpq.engine.fanout.run(scratch, a, client.transport(), star, .{ .workers = 4, .concurrency = 4 });
            try std.testing.expect(same(try get(out, "agg"), try get(star_out, "agg")));
            var tiny = req;
            tiny.object = try req.object.clone(scratch);
            try tiny.object.put(scratch, "max_memory", .{ .integer = 32 * 1024 });
            const tiny_probe = try zpq.engine.fanout.parse(scratch, try client.transport().invoke(client.transport().ctx, scratch, 0, try zpq.engine.fanout.partialPayload(scratch, tiny, parts[0])));
            if (!std.mem.eql(u8, (try get(tiny_probe, "reason")).string, "JoinBuildExceedsBudget")) {
                std.debug.print("replica budget refusal: {s}\n", .{try std.json.Stringify.valueAlloc(scratch, tiny_probe, .{})});
                return error.UnexpectedReplicaBudgetRefusal;
            }
            try std.testing.expectError(error.JoinBuildExceedsBudget, zpq.engine.fanout.run(scratch, a, client.transport(), tiny, .{ .workers = 1, .concurrency = 1 }));
            store.changing_dimension.store(true, .release);
            const changed = zpq.engine.fanout.run(scratch, a, client.transport(), req, .{ .workers = 4, .concurrency = 4 });
            store.changing_dimension.store(false, .release);
            try std.testing.expectError(error.SchemaMismatch, changed);
        }
    }
    const dim_null = try zpq.test_scratch.filePath(scratch, "zig-out/fanout-join-dimension-null.parquet");
    defer std.Io.Dir.cwd().deleteFile(io, dim_null) catch {};
    _ = try duckdb(scratch, try std.mem.concat(scratch, u8, &.{ "COPY (SELECT k::BIGINT AS dim_key FROM range(3) t(k) UNION ALL SELECT NULL::BIGINT) TO '", dim_null, "' (FORMAT parquet);" }));
    var null_store: fake.FakeS3 = undefined;
    try null_store.startPair(fact_bytes, try std.Io.Dir.cwd().readFileAlloc(io, dim_null, scratch, .limited(64 << 20)), 0);
    defer null_store.deinit();
    var null_fixture: Fixture = undefined;
    try null_fixture.startS3(scratch, &null_store);
    defer null_fixture.stop();
    var null_client = null_fixture.client();
    const null_req = try zpq.engine.fanout.parse(scratch, "{\"inputs\":[\"s3://test/fact.parquet\"],\"aggregate\":\"count(*) AS n\",\"group_by\":\"fk\",\"join\":{\"kind\":\"anti\",\"fk\":\"fk\",\"build\":{\"path\":\"s3://test/dimension.parquet\",\"key\":\"dim_key\"}}}");
    const null_out = try zpq.engine.fanout.run(scratch, a, null_client.transport(), null_req, .{ .workers = 4, .concurrency = 4 });
    try std.testing.expectEqual(0, (try get(null_out, "agg")).array.items.len);
    const null_reference = try zpq.engine.fanout.parse(scratch, try duckdb(scratch, try std.mem.concat(scratch, u8, &.{ "SELECT count(*) AS n FROM read_parquet('", fact, "') WHERE fk NOT IN (SELECT dim_key FROM read_parquet('", dim_null, "'));" })));
    try std.testing.expectEqual(@as(f64, 0), try numeric(try get(null_reference.array.items[0], "n")));
}

test "fanout aligned joins: boundaries, both sides split, oversized source, small dimensions and snapshot refusal" {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    try std.Io.Dir.cwd().createDirPath(io, "zig-out");
    const fact = try zpq.test_scratch.filePath(scratch, "zig-out/fanout-aligned-fact.parquet");
    const dim = try zpq.test_scratch.filePath(scratch, "zig-out/fanout-aligned-build.parquet");
    defer std.Io.Dir.cwd().deleteFile(io, fact) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, dim) catch {};
    _ = try duckdb(scratch, try std.mem.concat(scratch, u8, &.{ "COPY (SELECT (i//3-4096)::BIGINT AS fk, i::BIGINT AS amount FROM range(24576) t(i)) TO '", fact, "' (FORMAT parquet, ROW_GROUP_SIZE 2048); COPY (SELECT (k-4096)::BIGINT AS dim_key, 'r'||(k%3) AS bucket FROM range(8192) t(k) WHERE k%5<>0) TO '", dim, "' (FORMAT parquet, ROW_GROUP_SIZE 2048);" }));
    const fact_bytes = try fake.paddedParquet(scratch, fact, 2 << 20);
    const dimension = try fake.paddedParquet(scratch, dim, (64 << 20) + 4096);
    const small = try std.Io.Dir.cwd().readFileAlloc(io, dim, scratch, .limited(1 << 20));
    var store: fake.FakeS3 = undefined;
    try store.startTriple(fact_bytes, dimension, small, 0);
    defer store.deinit();
    var fixture: Fixture = undefined;
    try fixture.startS3(scratch, &store);
    defer fixture.stop();
    var client = fixture.client();
    for ([_][]const u8{ "inner", "left", "semi", "anti" }) |kind| {
        const simple = std.mem.eql(u8, kind, "semi") or std.mem.eql(u8, kind, "anti");
        const req = try zpq.engine.fanout.parse(scratch, try std.json.Stringify.valueAlloc(scratch, .{
            .inputs = &.{"s3://test/fact.parquet"},
            .join_strategy = "aligned",
            .aggregate = "count(*) AS c, sum(amount) AS s, avg(amount) AS a",
            .group_by = if (simple) "fk" else "bucket",
            .join = .{ .kind = kind, .fk = "fk", .build = .{ .path = "s3://test/dimension.parquet", .key = "dim_key", .columns = if (simple) @as([]const []const u8, &.{}) else @as([]const []const u8, &.{"bucket"}) } },
        }, .{}));
        const sql = if (std.mem.eql(u8, kind, "anti"))
            try std.fmt.allocPrint(scratch, "SELECT fk,count(*) AS c,sum(amount) AS s,avg(amount) AS a FROM '{s}' WHERE fk NOT IN (SELECT dim_key FROM '{s}') GROUP BY ALL", .{ fact, dim })
        else
            try std.fmt.allocPrint(scratch, "SELECT {s},count(*) AS c,sum(amount) AS s,avg(amount) AS a FROM '{s}' f {s} JOIN '{s}' d ON f.fk=d.dim_key GROUP BY ALL", .{ if (simple) "fk" else "bucket", fact, kind, dim });
        const reference = try duckdb(scratch, sql);
        const expected = try zpq.engine.fanout.parse(scratch, reference);
        for ([_]usize{ 1, 3, 8 }) |workers| {
            const out = try zpq.engine.fanout.run(scratch, a, client.transport(), req, .{ .workers = workers, .concurrency = 4, .counts = &.{1} });
            try std.testing.expectEqualStrings("aligned", (try get(out, "protocol")).string);
            if (!same(try get(out, "agg"), expected)) {
                std.debug.print("aligned {s} N{d}: {s}\nexpected: {s}\n", .{ kind, workers, try std.json.Stringify.valueAlloc(scratch, out, .{}), reference });
                return error.AlignedAnswerMismatch;
            }
        }
        if (std.mem.eql(u8, kind, "inner")) {
            var automatic = req;
            automatic.object = try req.object.clone(scratch);
            _ = automatic.object.swapRemove("join_strategy");
            const auto_out = try zpq.engine.fanout.run(scratch, a, client.transport(), automatic, .{ .workers = 3 });
            try std.testing.expectEqualStrings("aligned", (try get(auto_out, "protocol")).string);
            try std.testing.expect(same(try get(auto_out, "agg"), expected));
            var star = req;
            star.object = try req.object.clone(scratch);
            var joined = try get(req, "join");
            joined.object = try joined.object.clone(scratch);
            try joined.object.put(scratch, "dims", try zpq.engine.fanout.parse(scratch, "[{\"fk\":\"fk\",\"build\":{\"path\":\"s3://test/small.parquet\",\"key\":\"dim_key\",\"columns\":[]}}]"));
            try star.object.put(scratch, "join", joined);
            const star_out = try zpq.engine.fanout.run(scratch, a, client.transport(), star, .{ .workers = 3 });
            try std.testing.expect(same(try get(star_out, "agg"), expected));
            store.change_on_match.store(true, .release);
            const changed = zpq.engine.fanout.run(scratch, a, client.transport(), req, .{ .workers = 3 });
            store.change_on_match.store(false, .release);
            try std.testing.expectError(error.AlignedJoinSnapshotChanged, changed);
            var replica = req;
            replica.object = try req.object.clone(scratch);
            try replica.object.put(scratch, "join_strategy", .{ .string = "replicated" });
            const replica_out = try zpq.engine.fanout.run(scratch, a, client.transport(), replica, .{ .workers = 1 });
            try std.testing.expect(same(try get(replica_out, "agg"), expected));
        }
    }
    try std.testing.expect(store.matched_requests.load(.acquire) > 0);
}
