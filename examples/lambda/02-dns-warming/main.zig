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

    // --- LEVEL 02: DNS Warming & Event Loop ---
    // We initialize the infrastructure we'll need for S3
    
    // 1. Initialize Event Loop
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    // 2. Initialize DNS Stack
    // This is our custom Zig DNS stack that works without libc
    var dns_stack = try zpq.s3.dns.Stack.init(allocator, &loop);
    defer dns_stack.deinit(allocator);

    // 3. Warm the DNS
    // Resolve a common endpoint to ensure the stack is functional
    const addr = dns_stack.resolve("google.com", 80) catch |err| {
        std.debug.print("DNS Warming failed: {any}\n", .{err});
        // We continue anyway to see the perf impact
        null;
    };
    if (addr) |a| {
        std.debug.print("DNS Warming success: google.com -> {any}\n", .{a});
    }

    // --- Back to standard Lambda loop ---
    var threaded = std.Io.Threaded.init(allocator);
    defer threaded.deinit();

    var client = std.http.Client{ .allocator = allocator, .io = threaded.io() };
    defer client.deinit();

    const next_url = try std.fmt.allocPrint(allocator, "http://{s}/2018-06-01/runtime/invocation/next", .{runtime_api});
    defer allocator.free(next_url);

    std.debug.print("02-dns-warming Lambda started. Loop and DNS initialized.\n", .{});

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

        const message = "{\"message\": \"Hello from Level 02 DNS-Warming Lambda!\"}";
        resp_req.transfer_encoding = .chunked;
        var body_buffer: [1024]u8 = undefined;
        var body_writer = try resp_req.sendBody(&body_buffer);
        try body_writer.writer.writeAll(message);
        try body_writer.end();
        
        var discard_buf: [1024]u8 = undefined;
        _ = try resp_req.receiveHead(&discard_buf);
    }
}

