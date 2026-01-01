// Probe: Test std.http.Client with heap-allocated client (like sync.zig)
// Purpose: Isolate the body reading issue seen in lambda.zig
// Date: 2026-01-01
//
// Build: zig build-exe probes/probe_http_client.zig -ODebug
// Run: ./probe_http_client

const std = @import("std");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const url = "http://httpbin.org/get";

    std.debug.print("=== HTTP Client Probe (heap-allocated, like sync.zig) ===\n", .{});
    std.debug.print("URL: {s}\n\n", .{url});

    // Setup HTTP client - heap allocated like sync.zig
    const threaded = try allocator.create(std.Io.Threaded);
    threaded.* = std.Io.Threaded.init(allocator);
    defer {
        threaded.deinit();
        allocator.destroy(threaded);
    }

    const client = try allocator.create(std.http.Client);
    client.* = std.http.Client{
        .allocator = allocator,
        .io = threaded.io(),
    };
    defer {
        client.deinit();
        allocator.destroy(client);
    }

    const uri = try std.Uri.parse(url);

    std.debug.print("1. Creating request...\n", .{});
    var req = try client.request(.GET, uri, .{});
    defer req.deinit();

    std.debug.print("2. Sending request (bodiless)...\n", .{});
    try req.sendBodiless();

    std.debug.print("3. Receiving head...\n", .{});
    var redirect_buf: [1024]u8 = undefined;
    var response = try req.receiveHead(&redirect_buf);

    std.debug.print("4. Response status: {}\n", .{response.head.status});
    std.debug.print("   Content-Length: {?}\n", .{response.head.content_length});

    // Read body using callback pattern with pointer
    const Context = struct {
        body: *std.ArrayListUnmanaged(u8),
        allocator: std.mem.Allocator,
    };

    var body_list = std.ArrayListUnmanaged(u8){};
    defer body_list.deinit(allocator);

    const callback = struct {
        fn call(ctx: Context, res: *std.http.Client.Response) !void {
            std.debug.print("5. Inside callback, reading body...\n", .{});
            var transfer_buf: [4096]u8 = undefined;
            const reader = res.reader(&transfer_buf);

            while (true) {
                var buf: [1024]u8 = undefined;
                const n = reader.readSliceShort(&buf) catch |err| {
                    std.debug.print("   Read ended: {}\n", .{err});
                    break;
                };
                if (n == 0) break;
                std.debug.print("   Read {d} bytes\n", .{n});
                try ctx.body.appendSlice(ctx.allocator, buf[0..n]);
            }
        }
    }.call;

    try callback(.{ .body = &body_list, .allocator = allocator }, &response);

    std.debug.print("\n6. Total body: {d} bytes\n", .{body_list.items.len});
    if (body_list.items.len > 0 and body_list.items.len < 500) {
        std.debug.print("Body:\n{s}\n", .{body_list.items});
    }

    std.debug.print("\n=== Probe Complete ===\n", .{});
}
