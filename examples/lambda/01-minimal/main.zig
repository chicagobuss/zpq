const std = @import("std");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const runtime_api = std.process.getEnvVarOwned(allocator, "AWS_LAMBDA_RUNTIME_API") catch {
        std.debug.print("Error: AWS_LAMBDA_RUNTIME_API not set\n", .{});
        return;
    };
    defer allocator.free(runtime_api);

    // Minimal HTTP Client using standard library
    var threaded = std.Io.Threaded.init(allocator);
    defer threaded.deinit();

    var client = std.http.Client{ .allocator = allocator, .io = threaded.io() };
    defer client.deinit();

    const next_url = try std.fmt.allocPrint(allocator, "http://{s}/2018-06-01/runtime/invocation/next", .{runtime_api});
    defer allocator.free(next_url);

    std.debug.print("01-minimal Lambda started. Listening on {s}\n", .{runtime_api});

    while (true) {
        var header_buffer: [4096]u8 = undefined;
        var req = try client.request(.GET, try std.Uri.parse(next_url), .{});
        defer req.deinit();

        try req.sendBodiless();

        var res = try req.receiveHead(&header_buffer);
        
        // Extract Request ID
        var request_id: []const u8 = "unknown";
        var it = res.head.iterateHeaders();
        while (it.next()) |header| {
            if (std.ascii.eqlIgnoreCase(header.name, "Lambda-Runtime-Aws-Request-Id")) {
                request_id = header.value;
                break;
            }
        }

        std.debug.print("Received invocation: {s}\n", .{request_id});

        // Send Hello World Response
        const resp_url = try std.fmt.allocPrint(allocator, "http://{s}/2018-06-01/runtime/invocation/{s}/response", .{ runtime_api, request_id });
        defer allocator.free(resp_url);

        var resp_req = try client.request(.POST, try std.Uri.parse(resp_url), .{});
        defer resp_req.deinit();

        const message = "{\"message\": \"Hello from Minimal Zig Lambda!\"}";
        resp_req.transfer_encoding = .chunked;
        var body_buffer: [1024]u8 = undefined;
        var body_writer = try resp_req.sendBody(&body_buffer);
        try body_writer.writer.writeAll(message);
        try body_writer.end();
        
        var discard_buf: [1024]u8 = undefined;
        _ = try resp_req.receiveHead(&discard_buf);
    }
}

