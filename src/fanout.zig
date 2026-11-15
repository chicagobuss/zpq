//! Shared CLI/Lambda coordinator entry point. Only the Invoke adapter knows Lambda transport.
const std = @import("std");
const coordinator = @import("engine/fanout.zig");
const invoke = @import("io/invoke.zig");
const memory = @import("core/memory.zig");
const clock = @import("clock.zig");
const envValue = @import("core/system.zig").winEnvValue;

pub const Event = struct {
    operation: []const u8 = "fanout",
    function: []const u8 = "zpq-worker",
    endpoints: []const []const u8 = &.{},
    region: ?[]const u8 = null,
    sign_endpoints: bool = false,
    workers: usize = 32,
    concurrency: usize = 128,
    request: std.json.Value,
    counts: ?[]const u64 = null,
    row_group_counts: ?[]const u64 = null,
    order_by: ?[]const u8 = null,
    limit: ?usize = null,
};

pub fn isCoordinator(a: std.mem.Allocator, body: []const u8) bool {
    const parsed = std.json.parseFromSlice(struct { operation: ?[]const u8 = null }, a, body, .{ .ignore_unknown_fields = true }) catch return false;
    defer parsed.deinit();
    return if (parsed.value.operation) |op| std.mem.eql(u8, op, "fanout") else false;
}

pub fn handle(io: std.Io, gpa: std.mem.Allocator, env: std.process.Environ, pool: *invoke.Pool, body: []const u8) ![]u8 {
    var budget: memory.SharedBudget = .{ .limit = 512 << 20 };
    var charged: memory.Charged = .{ .child = gpa, .budget = &budget };
    return handleOver(io, gpa, env, pool, body, charged.allocator()) catch |err| return charged.refusal(err);
}
fn handleOver(io: std.Io, gpa: std.mem.Allocator, env: std.process.Environ, pool: *invoke.Pool, body: []const u8, backing: std.mem.Allocator) ![]u8 {
    pool.beginRequest(io);
    const started = clock.monoNs();
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    const event = try std.json.parseFromSliceLeaky(Event, a, body, .{ .parse_numbers = false });
    const region = event.region orelse envValue(env, "AWS_REGION") orelse envValue(env, "AWS_DEFAULT_REGION");
    // Local endpoints need no region; function path construction still has one uniform contract.
    if (event.counts != null and event.row_group_counts != null) return error.ConflictingCounts;
    const target = try invoke.Target.init(a, event.function, region orelse if (event.endpoints.len > 0) "local" else null);
    const urls = if (event.endpoints.len > 0) event.endpoints else &.{
        try std.fmt.allocPrint(a, "https://lambda.{s}.amazonaws.com", .{target.region}),
    };
    const endpoints = try a.alloc(invoke.Endpoint, urls.len);
    for (urls, endpoints) |url, *endpoint| endpoint.* = try invoke.Endpoint.init(a, url, target.path);
    var client: invoke.Client = .{ .io = io, .env = env, .pool = pool, .endpoints = endpoints, .region = if (event.endpoints.len == 0 or event.sign_endpoints) target.region else null };
    const ready_ns = clock.monoNs();
    const before = pool.connections.snapshotStats();
    const retries_before = pool.snapshotRetries();
    var out = try coordinator.run(a, backing, client.transport(), event.request, .{
        .workers = event.workers,
        .concurrency = event.concurrency,
        .counts = event.counts orelse event.row_group_counts,
        .order_by = event.order_by,
        .limit = event.limit,
    });
    const after = pool.connections.snapshotStats();
    var transport: std.json.Value = .{ .object = .{} };
    const retries_after = pool.snapshotRetries();
    var retries: u64 = 0;
    inline for (invoke.retry_names, 0..) |key, i| {
        const n = retries_after[i] -| retries_before[i];
        retries += n;
        try transport.object.put(a, key, .{ .integer = @intCast(n) });
    }
    try transport.object.put(a, "retries", .{ .integer = @intCast(retries) });
    inline for (.{ "acquires", "opens", "reuses", "stale_retries", "idle_evictions" }) |key| {
        try transport.object.put(a, key, .{ .number_string = try std.fmt.allocPrint(a, "{d}", .{@field(after, key) -| @field(before, key)}) });
    }
    try transport.object.put(a, "acquire_wait_ms", .{ .float = @as(f64, @floatFromInt(after.acquire_wait_ns -| before.acquire_wait_ns)) / 1e6 });
    try transport.object.put(a, "acquire_lock_ms", .{ .float = @as(f64, @floatFromInt(after.acquire_lock_ns -| before.acquire_lock_ns)) / 1e6 });
    try out.object.put(a, "transport", transport);
    try out.object.put(a, "coordinator_setup_ms", .{ .float = @as(f64, @floatFromInt(ready_ns - started)) / 1e6 });
    try out.object.put(a, "e2e_ms", .{ .float = @as(f64, @floatFromInt(clock.monoNs() - started)) / 1e6 });
    return std.json.Stringify.valueAlloc(gpa, out, .{});
}

test "fanout event: legacy count hints decode and conflicting aliases refuse before endpoint I/O" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const event = try std.json.parseFromSliceLeaky(Event, arena.allocator(), "{\"request\":{},\"row_group_counts\":[8]}", .{});
    try std.testing.expectEqual(@as(u64, 8), event.row_group_counts.?[0]);
    var pool: invoke.Pool = undefined;
    try pool.init(std.testing.allocator);
    defer pool.deinit();
    try std.testing.expectError(error.ConflictingCounts, handle(std.testing.io, std.testing.allocator, .empty, &pool, "{\"endpoints\":[\"invalid\"],\"request\":{},\"counts\":[8],\"row_group_counts\":[8]}"));
}
