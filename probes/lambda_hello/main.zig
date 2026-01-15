const std = @import("std");
const posix = std.posix;
const xev = @import("xev");

/// Minimal Zig Lambda Runtime - Hello World + Epoll Test
/// 1. Uses raw POSIX sockets for control plane (proven working)
/// 2. Initializes xev.Epoll explicitly to verify backend stability
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const runtime_api = std.posix.getenv("AWS_LAMBDA_RUNTIME_API") orelse {
        std.debug.print("Run with: AWS_LAMBDA_RUNTIME_API=localhost:9001 ./bootstrap\n", .{});
        return;
    };

    std.debug.print("Lambda starting. Runtime API: {s}\n", .{runtime_api});

    // TEST: Initialize Epoll Loop explicitly
    // This will segfault here if Lambda kernel forbids epoll (unlikely) or if xev is broken
    std.debug.print("TEST: Initializing xev.Epoll...\n", .{});
    var loop = try xev.Epoll.Loop.init(.{});
    defer loop.deinit();
    std.debug.print("TEST: xev.Epoll initialized successfully!\n", .{});

    // TEST: Run a trivial timer to prove the loop works
    var timer_c: xev.Epoll.Completion = undefined;
    var timer = try xev.Epoll.Timer.init();
    defer timer.deinit();
    
    const Callback = struct {
        fn call(_: ?*void, _: *xev.Epoll.Loop, _: *xev.Epoll.Completion, _: error{Canceled,Unexpected}!void) xev.CallbackAction {
            return .disarm;
        }
    };
    
    timer.run(&loop, &timer_c, 1, void, null, Callback.call);
    try loop.run(.until_done);
    std.debug.print("TEST: xev.Epoll timer fired successfully!\n", .{});


    // Parse host:port for Control Plane
    const colon_idx = std.mem.indexOfScalar(u8, runtime_api, ':') orelse runtime_api.len;
    const host = runtime_api[0..colon_idx];
    const port_str = if (colon_idx < runtime_api.len) runtime_api[colon_idx + 1 ..] else "80";
    const port = std.fmt.parseInt(u16, port_str, 10) catch 80;

    // Main loop
    while (true) {
        const event = getNextInvocation(allocator, host, port) catch |err| {
            std.debug.print("Polling error: {any}\n", .{err});
            continue;
        };
        defer allocator.free(event.body);
        defer allocator.free(event.request_id);

        std.debug.print("Received: {s}\n", .{event.request_id});

        const response = std.fmt.allocPrint(allocator, 
            \\{{"message": "Epoll Works!", "echo": {s}}}
        , .{event.body}) catch continue;
        defer allocator.free(response);

        postResponse(allocator, host, port, event.request_id, response) catch continue;
    }
}

// --- Minimal Socket Helpers (Copy-Pasted from working probe) ---

const InvocationEvent = struct { body: []u8, request_id: []u8 };

fn getNextInvocation(allocator: std.mem.Allocator, host: []const u8, port: u16) !InvocationEvent {
    const sock = try connectToHost(host, port);
    defer posix.close(sock);
    _ = try posix.write(sock, "GET /2018-06-01/runtime/invocation/next HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    
    var buf: [65536]u8 = undefined;
    var total: usize = 0;
    while (total < buf.len) {
        const n = posix.read(sock, buf[total..]) catch break;
        if (n == 0) break;
        total += n;
        if (std.mem.indexOf(u8, buf[0..total], "\r\n\r\n") != null) break; // Simplification for probe
    }
    const response = buf[0..total];
    const header_end = std.mem.indexOf(u8, response, "\r\n\r\n") orelse return error.Malformed;
    const headers = response[0..header_end];
    const body = response[header_end + 4 ..];
    
    var req_id: []const u8 = "unknown";
    var lines = std.mem.splitSequence(u8, headers, "\r\n");
    while (lines.next()) |line| {
        if (std.ascii.startsWithIgnoreCase(line, "lambda-runtime-aws-request-id:")) {
            req_id = std.mem.trim(u8, line["lambda-runtime-aws-request-id:".len..], " \t");
            break;
        }
    }
    return .{ .body = try allocator.dupe(u8, body), .request_id = try allocator.dupe(u8, req_id) };
}

fn postResponse(allocator: std.mem.Allocator, host: []const u8, port: u16, id: []const u8, body: []const u8) !void {
    const sock = try connectToHost(host, port);
    defer posix.close(sock);
    const req = try std.fmt.allocPrint(allocator, 
        "POST /2018-06-01/runtime/invocation/{s}/response HTTP/1.1\r\nHost: localhost\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}", 
        .{id, body.len, body});
    defer allocator.free(req);
    _ = try posix.write(sock, req);
    var buf: [1024]u8 = undefined;
    _ = posix.read(sock, &buf) catch {};
}

fn connectToHost(host: []const u8, port: u16) !posix.socket_t {
    const sock = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    errdefer posix.close(sock);
    var parts: [4]u8 = undefined;
    var i: usize = 0;
    var iter = std.mem.splitScalar(u8, host, '.');
    while (iter.next()) |p| : (i += 1) parts[i] = std.fmt.parseInt(u8, p, 10) catch 0;
    var addr = std.mem.zeroes(posix.sockaddr.in);
    addr.family = posix.AF.INET;
    addr.port = std.mem.nativeToBig(u16, port);
    addr.addr = std.mem.nativeToBig(u32, @as(u32, parts[0]) << 24 | @as(u32, parts[1]) << 16 | @as(u32, parts[2]) << 8 | parts[3]);
    try posix.connect(sock, @ptrCast(&addr), @sizeOf(posix.sockaddr.in));
    return sock;
}
