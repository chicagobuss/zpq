//! Minimal Lambda Echo Handler
//! Tests that RIE works and we can receive/respond to events
//! Uses shell curl instead of Zig HTTP client to isolate the problem

const std = @import("std");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const runtime_api = std.posix.getenv("AWS_LAMBDA_RUNTIME_API") orelse {
        std.debug.print("ERROR: AWS_LAMBDA_RUNTIME_API not set\n", .{});
        return error.NoRuntimeApi;
    };

    std.debug.print("Lambda echo handler starting (runtime: {s})\n", .{runtime_api});

    // Event loop
    var iteration: usize = 0;
    while (iteration < 10) : (iteration += 1) {
        std.debug.print("\n=== Iteration {d} ===\n", .{iteration});

        // GET next invocation using curl
        const get_url = try std.fmt.allocPrint(allocator, "http://{s}/2018-06-01/runtime/invocation/next", .{runtime_api});
        defer allocator.free(get_url);

        std.debug.print("GET {s}\n", .{get_url});

        const get_result = try std.process.Child.run(.{
            .allocator = allocator,
            .argv = &.{ "curl", "-s", "-i", get_url },
        });
        defer allocator.free(get_result.stdout);
        defer allocator.free(get_result.stderr);

        std.debug.print("Response:\n{s}\n", .{get_result.stdout});

        // Parse request ID from headers
        const request_id = parseRequestId(get_result.stdout) orelse {
            std.debug.print("ERROR: Could not find request ID in response\n", .{});
            continue;
        };
        std.debug.print("Request ID: {s}\n", .{request_id});

        // Parse body (after \r\n\r\n)
        const body = parseBody(get_result.stdout) orelse "{}";
        std.debug.print("Body: {s}\n", .{body});

        // POST response - echo back the body
        const post_url = try std.fmt.allocPrint(allocator, "http://{s}/2018-06-01/runtime/invocation/{s}/response", .{ runtime_api, request_id });
        defer allocator.free(post_url);

        const response_body = try std.fmt.allocPrint(allocator, "{{\"echo\": {s}, \"iteration\": {d}}}", .{ body, iteration });
        defer allocator.free(response_body);

        std.debug.print("POST {s}\n", .{post_url});
        std.debug.print("Body: {s}\n", .{response_body});

        const post_result = try std.process.Child.run(.{
            .allocator = allocator,
            .argv = &.{ "curl", "-s", "-X", "POST", "-d", response_body, post_url },
        });
        defer allocator.free(post_result.stdout);
        defer allocator.free(post_result.stderr);

        std.debug.print("POST result: {s}\n", .{post_result.stdout});
    }

    std.debug.print("\nEcho handler completed {d} iterations\n", .{iteration});
}

fn parseRequestId(response: []const u8) ?[]const u8 {
    // Look for Lambda-Runtime-Aws-Request-Id header
    const needle = "lambda-runtime-aws-request-id:";
    var lower_buf: [4096]u8 = undefined;
    const len = @min(response.len, lower_buf.len);
    for (response[0..len], 0..) |c, i| {
        lower_buf[i] = std.ascii.toLower(c);
    }

    const idx = std.mem.indexOf(u8, lower_buf[0..len], needle) orelse return null;
    const start = idx + needle.len;

    // Skip whitespace
    var actual_start = start;
    while (actual_start < len and (lower_buf[actual_start] == ' ' or lower_buf[actual_start] == '\t')) {
        actual_start += 1;
    }

    // Find end of line
    const end = std.mem.indexOfAny(u8, response[actual_start..], "\r\n") orelse return null;

    return response[actual_start .. actual_start + end];
}

fn parseBody(response: []const u8) ?[]const u8 {
    // Find \r\n\r\n separator
    const separator = "\r\n\r\n";
    const idx = std.mem.indexOf(u8, response, separator) orelse return null;
    const body = response[idx + separator.len ..];
    if (body.len == 0) return null;
    return body;
}
