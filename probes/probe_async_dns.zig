const std = @import("std");
const xev = @import("xev");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var thread_pool = xev.ThreadPool.init(.{ .max_threads = 4 });
    // We don't deinit the pool because libxev's ThreadPool.deinit() is join() which blocks.

    var notifier = try xev.Async.init();
    defer notifier.deinit();

    const Context = struct {
        hostname: []const u8,
        results: ?[]xev.shim_net.Address = null,
        done: bool = false,
        notifier: *xev.Async,
        allocator: std.mem.Allocator,
        task: xev.ThreadPool.Task = .{ .callback = taskCallback },
        completion: xev.Completion = .{},

        fn taskCallback(task: *xev.ThreadPool.Task) void {
            const self: *@This() = @fieldParentPtr("task", task);
            std.debug.print("Thread: resolving {s}...\n", .{self.hostname});

            const hostname_z = self.allocator.dupeZ(u8, self.hostname) catch unreachable;
            defer self.allocator.free(hostname_z);

            var hints: std.c.addrinfo = std.mem.zeroInit(std.c.addrinfo, .{
                .family = std.c.AF.UNSPEC,
                .socktype = std.c.SOCK.STREAM,
            });
            var res: ?*std.c.addrinfo = null;
            const rc = std.c.getaddrinfo(hostname_z.ptr, "443\x00", &hints, &res);

            if (@intFromEnum(rc) != 0) {
                std.debug.print("Thread: error resolving: {s}\n", .{std.mem.span(std.c.gai_strerror(rc))});
                self.done = true;
                self.notifier.notify() catch {};
                return;
            }
            if (res) |r| {
                defer std.c.freeaddrinfo(r);

                var count: usize = 0;
                var cur: ?*std.c.addrinfo = r;
                while (cur) |info| : (cur = info.next) {
                    count += 1;
                }

                const addrs = self.allocator.alloc(xev.shim_net.Address, count) catch unreachable;
                cur = r;
                var i: usize = 0;
                while (cur) |info| : (cur = info.next) {
                    addrs[i] = xev.shim_net.Address.initPosix(info.addr.?);
                    i += 1;
                }
                self.results = addrs;
                self.done = true;

                std.debug.print("Thread: resolved {s}, notifying loop\n", .{self.hostname});
                self.notifier.notify() catch {};
            } else {
                self.done = true;
                self.notifier.notify() catch {};
            }
        }

        fn asyncCallback(
            ud: ?*anyopaque,
            l: *xev.Loop,
            c: *xev.Completion,
            r: xev.Async.WaitError!void,
        ) xev.CallbackAction {
            _ = l;
            _ = c;
            _ = r catch unreachable;
            const self: *@This() = @ptrCast(@alignCast(ud));
            std.debug.print("Loop: received notification for {s}\n", .{self.hostname});

            if (self.results) |addrs| {
                for (addrs) |addr| {
                    if (addr.any.family == std.posix.AF.INET) {
                        const in = addr.in;
                        std.debug.print("Loop: result IPv4: {}.{}.{}.{}\n", .{
                            @as(u8, @intCast(in.addr >> 0 & 0xff)),
                            @as(u8, @intCast(in.addr >> 8 & 0xff)),
                            @as(u8, @intCast(in.addr >> 16 & 0xff)),
                            @as(u8, @intCast(in.addr >> 24 & 0xff)),
                        });
                    } else if (addr.any.family == std.posix.AF.INET6) {
                        std.debug.print("Loop: result IPv6 detected\n", .{});
                    }
                }
            }

            return .disarm;
        }
    };

    var ctx = Context{
        .hostname = "google.com",
        .notifier = &notifier,
        .allocator = allocator,
    };

    // 1. Register the async waiter on the loop
    notifier.wait(&loop, &ctx.completion, anyopaque, &ctx, Context.asyncCallback);

    // 2. Schedule the work on the thread pool
    thread_pool.schedule(xev.ThreadPool.Batch.from(&ctx.task));

    // 3. Run the loop
    std.debug.print("Main: running loop...\n", .{});
    try loop.run(.until_done);
    std.debug.print("Main: loop done\n", .{});
}
