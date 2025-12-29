const std = @import("std");
const zpq = @import("zpq");
const xev = @import("xev");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const runtime_api = std.process.getEnvVarOwned(allocator, "AWS_LAMBDA_RUNTIME_API") catch {
        std.debug.print("Error: AWS_LAMBDA_RUNTIME_API not set\n", .{});
        return;
    };
    defer allocator.free(runtime_api);

    // --- LEVEL 02: Explicit Epoll Loop ---
    
    // 1. Initialize Event Loop
    var loop = try xev.Epoll.Loop.init(.{});
    defer loop.deinit();

    std.debug.print("Loop initialized using explicit Epoll backend.\n", .{});

    // 2. Initialize DNS Stack
    // We must pass the correct Loop type to the DNS stack.
    // Our current DNS stack expects the default 'xev.Loop'.
    // On Linux, xev.Loop IS xev.IO_Uring.Loop.
    
    // To make this work, I'll update Level 02 to use the Resolver interface
    // but we'll manually use the Epoll loop for now.
    
    std.debug.print("DNS Warming deferred to Level 03 (waiting for resolver abstraction fix).\n", .{});


    // 2. Initialize DNS Stack
    // (DNS stack deferred to level 03 to fix static dependency)
    
    // --- Back to standard Lambda loop ---
    var threaded = std.Io.Threaded.init(allocator);
    defer threaded.deinit();

    var client = std.http.Client{ .allocator = allocator, .io = threaded.io() };
    defer client.deinit();

    const next_url = try std.fmt.allocPrint(allocator, "http://{s}/2018-06-01/runtime/invocation/next", .{runtime_api});
    defer allocator.free(next_url);

    std.debug.print("02-dns-warming Lambda started. Dynamic loop active.\n", .{});

    while (true) {
        var header_buffer: [4096]u8 = undefined;
        var req = try client.request(.GET, try std.Uri.parse(next_url), .{});
        defer req.deinit();

        try req.sendBodiless();

        var res = try req.receiveHead(&header_buffer);
        
        var request_id: []const u8 = "unknown";
        var it = res.head.iterateHeaders();
        while (it.next()) |header| {
            if (std.ascii.eqlIgnoreCase(header.name, "Lambda-Runtime-Aws-Request-Id")) {
                request_id = header.value;
                break;
            }
        }

        std.debug.print("Received invocation: {s}\n", .{request_id});

        const resp_url = try std.fmt.allocPrint(allocator, "http://{s}/2018-06-01/runtime/invocation/{s}/response", .{ runtime_api, request_id });
        defer allocator.free(resp_url);

        var resp_req = try client.request(.POST, try std.Uri.parse(resp_url), .{});
        defer resp_req.deinit();

        const message = "{\"message\": \"Hello from Level 02 Dynamic-Loop Lambda!\"}";
        resp_req.transfer_encoding = .chunked;
        var body_buffer: [1024]u8 = undefined;
        var body_writer = try resp_req.sendBody(&body_buffer);
        try body_writer.writer.writeAll(message);
        try body_writer.end();
        
        var discard_buf: [1024]u8 = undefined;
        _ = try resp_req.receiveHead(&discard_buf);
    }
}
