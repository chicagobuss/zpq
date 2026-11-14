const std = @import("std");
const zpq = @import("zpq");

pub fn run(init: std.process.Init, iter: *std.process.Args.Iterator, gpa: std.mem.Allocator) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var urls: std.ArrayList([]const u8) = .empty;
    var inputs: std.ArrayList([]const u8) = .empty;
    var counts: std.ArrayList(u64) = .empty;
    var function: []const u8 = "zpq-worker";
    var region: ?[]const u8 = null;
    var sign_endpoints = false;
    var aggregate: ?[]const u8 = null;
    var group_by: ?[]const u8 = null;
    var filter: ?[]const u8 = null;
    var order_by: ?[]const u8 = null;
    var limit: ?usize = null;
    var workers: usize = 32;
    var concurrency: usize = 128;
    var raw_request: ?[]const u8 = null;
    while (iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--sign-endpoints")) sign_endpoints = true else if (std.mem.eql(u8, arg, "--endpoint")) try urls.append(a, iter.next() orelse return error.MissingArgument) else if (std.mem.eql(u8, arg, "--input")) try inputs.append(a, iter.next() orelse return error.MissingArgument) else if (std.mem.eql(u8, arg, "--function")) function = iter.next() orelse return error.MissingArgument else if (std.mem.eql(u8, arg, "--region")) region = iter.next() orelse return error.MissingArgument else if (std.mem.eql(u8, arg, "--aggregate")) aggregate = iter.next() orelse return error.MissingArgument else if (std.mem.eql(u8, arg, "--group-by")) group_by = iter.next() orelse return error.MissingArgument else if (std.mem.eql(u8, arg, "--filter")) filter = iter.next() orelse return error.MissingArgument else if (std.mem.eql(u8, arg, "--order-by")) order_by = iter.next() orelse return error.MissingArgument else if (std.mem.eql(u8, arg, "--request")) raw_request = iter.next() orelse return error.MissingArgument else if (std.mem.eql(u8, arg, "--workers")) workers = try std.fmt.parseInt(usize, iter.next() orelse return error.MissingArgument, 10) else if (std.mem.eql(u8, arg, "--concurrency")) concurrency = try std.fmt.parseInt(usize, iter.next() orelse return error.MissingArgument, 10) else if (std.mem.eql(u8, arg, "--limit")) limit = try std.fmt.parseInt(usize, iter.next() orelse return error.MissingArgument, 10) else if (std.mem.eql(u8, arg, "--counts")) {
            var parts = std.mem.splitScalar(u8, iter.next() orelse return error.MissingArgument, ',');
            while (parts.next()) |part| try counts.append(a, try std.fmt.parseInt(u64, part, 10));
        } else return error.UnknownOption;
    }
    const request = try zpq.engine.fanout.parse(a, raw_request orelse try std.json.Stringify.valueAlloc(a, .{
        .inputs = inputs.items,
        .aggregate = aggregate orelse return error.MissingAggregate,
        .group_by = group_by,
        .filter = filter,
    }, .{ .emit_null_optional_fields = false }));
    const body = try std.json.Stringify.valueAlloc(a, zpq.fanout.Event{
        .function = function,
        .region = region,
        .sign_endpoints = sign_endpoints,
        .endpoints = urls.items,
        .workers = workers,
        .concurrency = concurrency,
        .request = request,
        .counts = if (counts.items.len > 0) counts.items else null,
        .order_by = order_by,
        .limit = limit,
    }, .{});
    var pool: zpq.io.invoke.Pool = undefined;
    try pool.init(gpa);
    defer pool.deinit();
    return zpq.fanout.handle(init.io, gpa, init.minimal.environ, &pool, body);
}
