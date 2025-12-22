const std = @import("std");
const zpq = @import("zpq");
const Connection = zpq.s3.Connection;
const dns = zpq.s3.dns;
const xev = dns.xev;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("--- TLS Echo Micro-Test (Google HEAD) ---\n", .{});

    var thread_pool = xev.ThreadPool.init(.{});
    defer thread_pool.deinit();

    var loop_ptr = try allocator.create(xev.Loop);
    defer allocator.destroy(loop_ptr);
    loop_ptr.* = try xev.Loop.init(.{ .thread_pool = &thread_pool });
    defer loop_ptr.deinit();

    var tp_resolver = dns.ThreadPoolResolver.init(&thread_pool, allocator);
    defer tp_resolver.deinit();
    const resolver = tp_resolver.resolver();

    const host = "www.google.com";
    const port = 443;

    const WaitCtx = struct {
        done: bool = false,
        connected: bool = false,
        data_received: usize = 0,
        err: ?anyerror = null,

        fn onConnect(conn: *Connection, ctx: ?*anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            std.debug.print("[Test] Connected!\n", .{});
            self.connected = true;
            const req = "HEAD / HTTP/1.1\r\nHost: www.google.com\r\nConnection: close\r\n\r\n";
            conn.write(req) catch |err| {
                self.err = err;
                self.done = true;
            };
        }

        fn onData(conn: *Connection, ctx: ?*anyopaque, data: []const u8) void {
            _ = conn;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            std.debug.print("[Test] Received {d} bytes:\n{s}\n", .{ data.len, data });
            self.data_received += data.len;
        }

        fn onError(conn: *Connection, ctx: ?*anyopaque, err: anyerror) void {
            _ = conn;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (err == error.EOF) {
                std.debug.print("[Test] EOF\n", .{});
            } else {
                std.debug.print("[Test] Error: {}\n", .{err});
                self.err = err;
            }
            self.done = true;
        }
    };

    var wait_ctx = WaitCtx{};
    const conn = try Connection.init(loop_ptr, allocator, host, true);
    defer conn.deinit();

    conn.user_ctx = &wait_ctx;
    conn.on_connect = WaitCtx.onConnect;
    conn.on_data = WaitCtx.onData;
    conn.on_error = WaitCtx.onError;

    // Resolve
    var dns_comp = dns.Resolver.Completion.init();
    defer dns_comp.deinit(allocator);

    const DnsWait = struct {
        addr: ?dns.Address = null,
        done: bool = false,
        fn callback(ud: ?*anyopaque, results: []const dns.Address, err: anyerror!void) void {
            const self: *@This() = @ptrCast(@alignCast(ud));
            err catch |e| {
                std.debug.print("DNS failed: {}\n", .{e});
                self.done = true;
                return;
            };
            if (results.len > 0) self.addr = results[0];
            self.done = true;
        }
    };
    var dns_wait = DnsWait{};
    resolver.resolve(loop_ptr, host, port, &dns_comp, DnsWait.callback, &dns_wait);

    while (!dns_wait.done) {
        _ = try loop_ptr.run(.once);
    }

    if (dns_wait.addr == null) return error.DnsFailed;

    std.debug.print("[Test] Connecting to {}...\n", .{dns_wait.addr.?});
    try conn.connect(dns_wait.addr.?);

    while (!wait_ctx.done) {
        _ = try loop_ptr.run(.once);
    }

    if (wait_ctx.err) |err| return err;
    if (wait_ctx.data_received == 0) return error.NoDataReceived;

    std.debug.print("[Test] Success!\n", .{});
    std.process.exit(0);
}

