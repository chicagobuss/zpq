const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");
const dns = zpq.s3.dns;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var thread_pool = xev.ThreadPool.init(.{});
    defer thread_pool.deinit();

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var tp_resolver = dns.ThreadPoolResolver.init(&thread_pool, allocator);
    var sf_resolver = dns.SingleFlightResolver.init(allocator, tp_resolver.resolver());
    defer sf_resolver.deinit();
    const resolver = sf_resolver.resolver();

    const Context = struct {
        done: bool = false,
        completion: dns.Resolver.Completion = .{},
        pub fn onResolve(ud: ?*anyopaque, results: []const dns.Address, err: anyerror!void) void {
            const self = @as(*@This(), @ptrCast(@alignCast(ud)));
            _ = results;
            err catch |e| std.debug.print("Error: {}\n", .{e});
            self.done = true;
        }
    };

    var ctx = Context{};
    resolver.resolve(&loop, "google.com", 443, &ctx.completion, Context.onResolve, &ctx);

    while (!ctx.done) {
        try loop.run(.once);
    }
    ctx.completion.deinit(allocator);
    std.debug.print("Single request passed\n", .{});

    var ctx1 = Context{};
    var ctx2 = Context{};
    resolver.resolve(&loop, "google.com", 443, &ctx1.completion, Context.onResolve, &ctx1);
    resolver.resolve(&loop, "google.com", 443, &ctx2.completion, Context.onResolve, &ctx2);

    while (!ctx1.done or !ctx2.done) {
        try loop.run(.once);
    }
    ctx1.completion.deinit(allocator);
    ctx2.completion.deinit(allocator);
    std.debug.print("Parallel deduplicated requests passed\n", .{});
}

