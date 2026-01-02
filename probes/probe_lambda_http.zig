//! Lambda HTTP Client Test
//! Tests std.http.Client against the Lambda Runtime API
//! This isolates whether the issue is with std.http.Client or something else

const std = @import("std");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const runtime_api = std.posix.getenv("AWS_LAMBDA_RUNTIME_API") orelse {
        std.debug.print("ERROR: AWS_LAMBDA_RUNTIME_API not set\n", .{});
        return error.NoRuntimeApi;
    };

    std.debug.print("Lambda HTTP test (runtime: {s})\n", .{runtime_api});

    // Setup HTTP client
    var threaded = std.Io.Threaded.init(allocator);
    defer threaded.deinit();

    var client = std.http.Client{
        .allocator = allocator,
        .io = threaded.io(),
    };
    defer client.deinit();

    // Build URL
    const url = try std.fmt.allocPrint(allocator, "http://{s}/2018-06-01/runtime/invocation/next", .{runtime_api});
    defer allocator.free(url);

    std.debug.print("GET {s}\n", .{url});

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

    // Get request ID from headers
    var request_id: ?[]const u8 = null;
    var iter = response.head.iterateHeaders();
    while (iter.next()) |header| {
        std.debug.print("   Header: {s}: {s}\n", .{ header.name, header.value });
        if (std.ascii.eqlIgnoreCase(header.name, "lambda-runtime-aws-request-id")) {
            request_id = header.value;
        }
    }

    if (request_id) |id| {
        std.debug.print("\n5. Request ID: {s}\n", .{id});
    } else {
        std.debug.print("\n5. ERROR: No request ID found!\n", .{});
    }

    // Try to read body
    std.debug.print("\n6. Reading body...\n", .{});
    std.debug.print("   Transfer-Encoding: {any}\n", .{response.head.transfer_encoding});
    std.debug.print("   Content-Length: {?}\n", .{response.head.content_length});
    std.debug.print("   req ptr: {*}\n", .{&req});
    std.debug.print("   response.request ptr: {*}\n", .{response.request});
    std.debug.print("   req.reader ptr: {*}\n", .{&req.reader});
    std.debug.print("   response.request.reader ptr: {*}\n", .{&response.request.reader});
    std.debug.print("   Request reader state: {any}\n", .{req.reader.state});

    var body_list = std.ArrayListUnmanaged(u8){};
    defer body_list.deinit(allocator);

    // Check if there's actually a body to read
    const content_len = response.head.content_length orelse 0;
    if (content_len == 0 and response.head.transfer_encoding == .none) {
        std.debug.print("   No body (content-length=0, no transfer-encoding)\n", .{});
    } else {
        // Try reading directly from the underlying connection reader
        // to bypass http.Reader machinery
        std.debug.print("   Trying direct read from req.reader.in...\n", .{});
        const underlying_reader = req.reader.in;
        std.debug.print("   underlying_reader ptr: {*}\n", .{underlying_reader});

        // The body might already be buffered - check buffered data first
        const buffered = underlying_reader.buffered();
        std.debug.print("   Already buffered: {d} bytes\n", .{buffered.len});
        if (buffered.len > 0) {
            const to_read = @min(buffered.len, content_len);
            try body_list.appendSlice(allocator, buffered[0..to_read]);
            std.debug.print("   Read from buffer: {d} bytes\n", .{to_read});
            underlying_reader.toss(to_read);
        }

        // Read remaining if needed
        const remaining = content_len - body_list.items.len;
        if (remaining > 0) {
            std.debug.print("   Need to read {d} more bytes from stream\n", .{remaining});
            var buf: [1024]u8 = undefined;
            const to_read = @min(buf.len, remaining);
            underlying_reader.readSliceAll(buf[0..to_read]) catch |err| {
                std.debug.print("   Read error: {s}\n", .{@errorName(err)});
            };
            try body_list.appendSlice(allocator, buf[0..to_read]);
        }
    }

    std.debug.print("\n7. Total body: {d} bytes\n", .{body_list.items.len});
    if (body_list.items.len > 0) {
        std.debug.print("Body: {s}\n", .{body_list.items});
    }

    // If we got here, send response
    if (request_id) |id| {
        std.debug.print("\n8. Sending response...\n", .{});

        const response_url = try std.fmt.allocPrint(allocator, "http://{s}/2018-06-01/runtime/invocation/{s}/response", .{ runtime_api, id });
        defer allocator.free(response_url);

        std.debug.print("POST {s}\n", .{response_url});

        const response_uri = try std.Uri.parse(response_url);
        var post_req = try client.request(.POST, response_uri, .{});
        defer post_req.deinit();

        var body_mut = "{\"status\": \"ok\", \"handler\": \"zig-http-client\"}".*;
        try post_req.sendBodyComplete(&body_mut);

        var post_redirect_buf: [1024]u8 = undefined;
        var post_response = try post_req.receiveHead(&post_redirect_buf);
        std.debug.print("   POST response status: {}\n", .{post_response.head.status});
    }

    std.debug.print("\n=== Test Complete ===\n", .{});
}
