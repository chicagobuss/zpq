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
    const started = clock.monoNs();
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    const event = try std.json.parseFromSliceLeaky(Event, a, body, .{ .parse_numbers = false });
    const region = event.region orelse envValue(env, "AWS_REGION") orelse envValue(env, "AWS_DEFAULT_REGION");
    // Local endpoints need no region; function path construction still has one uniform contract.
    const target = try invoke.Target.init(a, event.function, region orelse if (event.endpoints.len > 0) "local" else null);
    const urls = if (event.endpoints.len > 0) event.endpoints else &.{
        try std.fmt.allocPrint(a, "https://lambda.{s}.amazonaws.com", .{target.region}),
    };
    const endpoints = try a.alloc(invoke.Endpoint, urls.len);
    for (urls, endpoints) |url, *endpoint| endpoint.* = try invoke.Endpoint.init(a, url, target.path);
    var client: invoke.Client = .{ .io = io, .env = env, .pool = pool, .endpoints = endpoints, .region = if (event.endpoints.len == 0 or event.sign_endpoints) target.region else null };
    var out = try coordinator.run(a, backing, client.transport(), event.request, .{
        .workers = event.workers,
        .concurrency = event.concurrency,
        .counts = event.counts,
        .order_by = event.order_by,
        .limit = event.limit,
    });
    try out.object.put(a, "e2e_ms", .{ .float = @as(f64, @floatFromInt(clock.monoNs() - started)) / 1e6 });
    return std.json.Stringify.valueAlloc(gpa, out, .{});
}
