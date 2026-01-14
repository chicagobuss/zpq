const std = @import("std");
const posix = std.posix;

/// Minimal Zig Lambda Runtime - Hello World
/// Uses raw POSIX sockets to avoid std.http/std.net complexity
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const runtime_api = std.posix.getenv("AWS_LAMBDA_RUNTIME_API") orelse {
        std.debug.print("Not in Lambda environment (AWS_LAMBDA_RUNTIME_API not set)\n", .{});
        std.debug.print("Run with: AWS_LAMBDA_RUNTIME_API=localhost:9001 ./bootstrap\n", .{});
        return;
    };

    std.debug.print("Lambda starting. Runtime API: {s}\n", .{runtime_api});

    // Parse host:port
    const colon_idx = std.mem.indexOfScalar(u8, runtime_api, ':') orelse runtime_api.len;
    const host = runtime_api[0..colon_idx];
    const port_str = if (colon_idx < runtime_api.len) runtime_api[colon_idx + 1 ..] else "80";
    const port = std.fmt.parseInt(u16, port_str, 10) catch 80;

    std.debug.print("Parsed: host={s} port={d}\n", .{ host, port });

    // Main loop
    var iteration: u32 = 0;
    while (true) : (iteration += 1) {
        std.debug.print("[{d}] Polling for invocation...\n", .{iteration});

        // 1. GET /runtime/invocation/next
        const event = getNextInvocation(allocator, host, port) catch |err| {
            std.debug.print("[{d}] Error getting invocation: {any}\n", .{ iteration, err });
            continue;
        };
        defer allocator.free(event.body);
        defer allocator.free(event.request_id);

        std.debug.print("[{d}] Received invocation: {s}\n", .{ iteration, event.request_id });
        std.debug.print("[{d}] Payload ({d} bytes): {s}\n", .{ iteration, event.body.len, event.body });

        // 2. Process (just echo back)
        const response = std.fmt.allocPrint(allocator, 
            \\{{"message": "Hello from Zig!", "received": {s}}}
        , .{event.body}) catch |err| {
            std.debug.print("[{d}] Format error: {any}\n", .{ iteration, err });
            continue;
        };
        defer allocator.free(response);

        // 3. POST /runtime/invocation/{id}/response
        postResponse(allocator, host, port, event.request_id, response) catch |err| {
            std.debug.print("[{d}] Error posting response: {any}\n", .{ iteration, err });
            continue;
        };

        std.debug.print("[{d}] Response sent successfully\n", .{iteration});
    }
}

const InvocationEvent = struct {
    body: []u8,
    request_id: []u8,
};

fn connectToHost(host: []const u8, port: u16) !posix.socket_t {
    std.debug.print("  connectToHost: {s}:{d}\n", .{ host, port });

    // Create socket
    const sock = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    errdefer posix.close(sock);

    // Parse dotted quad IP (Lambda uses 169.254.x.x link-local addresses)
    var parts: [4]u8 = undefined;
    var i: usize = 0;
    var iter = std.mem.splitScalar(u8, host, '.');
    while (iter.next()) |part| {
        if (i >= 4) return error.InvalidAddress;
        parts[i] = std.fmt.parseInt(u8, part, 10) catch return error.InvalidAddress;
        i += 1;
    }
    if (i != 4) return error.InvalidAddress;

    // Build sockaddr_in - use native byte order for address, network order for port
    var addr = std.mem.zeroes(posix.sockaddr.in);
    addr.family = posix.AF.INET;
    addr.port = std.mem.nativeToBig(u16, port);
    // Address needs to be in network byte order (big-endian)
    // parts[0] is MSB, parts[3] is LSB in dotted notation
    addr.addr = @as(u32, parts[0]) << 24 | @as(u32, parts[1]) << 16 | @as(u32, parts[2]) << 8 | @as(u32, parts[3]);
    // Convert to network byte order
    addr.addr = std.mem.nativeToBig(u32, addr.addr);

    std.debug.print("  addr bytes: {d}.{d}.{d}.{d} port={d}\n", .{ parts[0], parts[1], parts[2], parts[3], port });
    std.debug.print("  connecting (addr=0x{x}, port=0x{x})...\n", .{ addr.addr, addr.port });

    try posix.connect(sock, @ptrCast(&addr), @sizeOf(posix.sockaddr.in));
    std.debug.print("  connected!\n", .{});

    return sock;
}

fn getNextInvocation(allocator: std.mem.Allocator, host: []const u8, port: u16) !InvocationEvent {
    const sock = try connectToHost(host, port);
    defer posix.close(sock);

    // Send GET request
    const request = "GET /2018-06-01/runtime/invocation/next HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    std.debug.print("  sending request ({d} bytes)...\n", .{request.len});
    _ = try posix.write(sock, request);
    std.debug.print("  request sent, waiting for response...\n", .{});

    // Read response (this WILL block until Lambda sends an invocation)
    // Lambda Runtime API keeps the connection open until an event arrives
    var buf: [65536]u8 = undefined;
    var total_read: usize = 0;
    while (total_read < buf.len) {
        const n = posix.read(sock, buf[total_read..]) catch |err| {
            std.debug.print("  read error: {any}\n", .{err});
            return err;
        };
        std.debug.print("  read {d} bytes\n", .{n});
        if (n == 0) break;
        total_read += n;

        // Check if we have a complete response (headers + body based on content-length)
        if (std.mem.indexOf(u8, buf[0..total_read], "\r\n\r\n")) |header_end| {
            const headers = buf[0..header_end];
            // Find Content-Length
            var content_length: usize = 0;
            var lines = std.mem.splitSequence(u8, headers, "\r\n");
            while (lines.next()) |line| {
                if (std.ascii.startsWithIgnoreCase(line, "content-length:")) {
                    const val = std.mem.trim(u8, line["content-length:".len..], " \t");
                    content_length = std.fmt.parseInt(usize, val, 10) catch 0;
                    break;
                }
            }
            const body_start = header_end + 4;
            const body_so_far = total_read - body_start;
            if (body_so_far >= content_length) {
                break; // Got full response
            }
        }
    }

    if (total_read == 0) return error.EmptyResponse;

    std.debug.print("  got {d} bytes total\n", .{total_read});
    const response = buf[0..total_read];

    // Parse headers
    const header_end = std.mem.indexOf(u8, response, "\r\n\r\n") orelse return error.MalformedResponse;
    const headers = response[0..header_end];
    const body = response[header_end + 4 ..];

    // Extract request ID from headers
    var request_id: []const u8 = "unknown";
    var lines = std.mem.splitSequence(u8, headers, "\r\n");
    while (lines.next()) |line| {
        if (std.ascii.startsWithIgnoreCase(line, "lambda-runtime-aws-request-id:")) {
            request_id = std.mem.trim(u8, line["lambda-runtime-aws-request-id:".len..], " \t");
            break;
        }
    }

    return .{
        .body = try allocator.dupe(u8, body),
        .request_id = try allocator.dupe(u8, request_id),
    };
}

fn postResponse(allocator: std.mem.Allocator, host: []const u8, port: u16, request_id: []const u8, body: []const u8) !void {
    const sock = try connectToHost(host, port);
    defer posix.close(sock);

    const request = try std.fmt.allocPrint(allocator,
        "POST /2018-06-01/runtime/invocation/{s}/response HTTP/1.1\r\n" ++
        "Host: localhost\r\n" ++
        "Content-Type: application/json\r\n" ++
        "Content-Length: {d}\r\n" ++
        "Connection: close\r\n\r\n" ++
        "{s}",
        .{ request_id, body.len, body },
    );
    defer allocator.free(request);

    _ = try posix.write(sock, request);

    // Read response (just to confirm)
    var buf: [1024]u8 = undefined;
    _ = posix.read(sock, &buf) catch {};
}
