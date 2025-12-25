const std = @import("std");
const xev = @import("xev");
const dns = @import("zpq").s3.dns;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var pool = xev.ThreadPool.init(.{});
    defer {
        pool.shutdown();
        pool.deinit();
    }

    var resolver_inner = dns.ThreadPoolResolver.init(&pool, allocator);
    var resolver = resolver_inner.resolver();

    var comp = dns.Resolver.Completion.init();
    defer comp.deinit(allocator);

    const Context = struct {
        done: bool = false,
        err: ?anyerror = null,
        results: []const dns.Address = &.{},
        
        fn callback(ud: ?*anyopaque, results: []const dns.Address, err: anyerror!void) void {
            const ctx: *@This() = @ptrCast(@alignCast(ud));
            err catch |e| {
                std.debug.print("DNS Error: {}\n", .{e});
                ctx.err = e;
                ctx.done = true;
                return;
            };
            std.debug.print("DNS Success: {d} results\n", .{results.len});
            ctx.results = results;
            ctx.done = true;
        }
    };

    var ctx = Context{};
    std.debug.print("Starting resolution for localhost:9000...\n", .{});
    resolver.resolve(&loop, "localhost", 9000, &comp, Context.callback, &ctx);

    var ticks: usize = 0;
    while (!ctx.done) {
        ticks += 1;
        try loop.run(.once);
        if (ticks > 1000000) {
            std.debug.print("Still waiting after 1M ticks...\n", .{});
            ticks = 0; // Reset to avoid flooding
        }
    }

    std.debug.print("Finished in {d} ticks\n", .{ticks});
}
