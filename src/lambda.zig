const std = @import("std");
const xev = @import("xev");
const query = @import("query.zig");

const QueryParams = query.QueryParams;
const QueryResult = query.QueryResult;

/// Lambda Runtime API handler.
/// Polls for events and executes queries in a loop until the container is killed.
pub fn run(allocator: std.mem.Allocator, runtime_api: [*:0]const u8) !void {
    const api_str = std.mem.span(runtime_api);

    std.debug.print("zpq: Lambda mode (runtime: {s})\n", .{api_str});

    // Setup once (warm container reuse)
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var thread_pool = xev.ThreadPool.init(.{ .max_threads = 4 });
    defer {
        thread_pool.shutdown();
        thread_pool.deinit();
    }

    // Build base URL for Lambda Runtime API
    const base_url = try std.fmt.allocPrint(allocator, "http://{s}/2018-06-01/runtime", .{api_str});
    defer allocator.free(base_url);

    // Setup HTTP client with std.Io.Threaded (Zig 0.16 pattern)
    var threaded = std.Io.Threaded.init(allocator);
    defer threaded.deinit();

    var client = std.http.Client{
        .allocator = allocator,
        .io = threaded.io(),
    };
    defer client.deinit();

    // Event loop (runs until container killed)
    while (true) {
        // GET /runtime/invocation/next (blocks until event arrives)
        const event_result = httpGet(allocator, &client, base_url, "/invocation/next") catch |err| {
            std.debug.print("zpq: error polling for invocation: {s}\n", .{@errorName(err)});
            continue;
        };
        defer allocator.free(event_result.body);
        defer allocator.free(event_result.request_id);

        std.debug.print("zpq: invocation {s}\n", .{event_result.request_id});

        // Parse JSON event → QueryParams
        const params = query.parseQueryJson(allocator, event_result.body) catch |err| {
            // POST error response
            const error_json = try std.fmt.allocPrint(allocator,
                \\{{"errorType":"InvalidRequest","errorMessage":"Failed to parse event: {s}"}}
            , .{@errorName(err)});
            defer allocator.free(error_json);

            httpPost(allocator, &client, base_url, "/invocation/", event_result.request_id, "/error", error_json) catch |post_err| {
                std.debug.print("zpq: Failed to post error: {s}\n", .{@errorName(post_err)});
            };
            continue;
        };
        defer {
            allocator.free(params.input);
            if (params.output) |o| allocator.free(o);
            if (params.filter) |f| allocator.free(f);
            if (params.select) |s| allocator.free(s);
        }

        // Execute query
        const result = query.executeQuery(allocator, &loop, &thread_pool, params) catch |err| {
            // POST error response
            const error_json = try std.fmt.allocPrint(allocator,
                \\{{"errorType":"ExecutionError","errorMessage":"Query failed: {s}"}}
            , .{@errorName(err)});
            defer allocator.free(error_json);

            httpPost(allocator, &client, base_url, "/invocation/", event_result.request_id, "/error", error_json) catch |post_err| {
                std.debug.print("zpq: Failed to post error: {s}\n", .{@errorName(post_err)});
            };
            continue;
        };

        // Format and POST success response
        const response_json = try query.formatResultJson(allocator, result);
        defer allocator.free(response_json);

        httpPost(allocator, &client, base_url, "/invocation/", event_result.request_id, "/response", response_json) catch |err| {
            std.debug.print("zpq: Failed to post response: {s}\n", .{@errorName(err)});
        };

        std.debug.print("zpq: completed {s}\n", .{event_result.request_id});
    }
}

// =============================================================================
// HTTP Client for Lambda Runtime API
// =============================================================================

const EventResult = struct {
    body: []u8,
    request_id: []u8,
};

/// GET request to Lambda Runtime API
fn httpGet(allocator: std.mem.Allocator, client: *std.http.Client, base_url: []const u8, path: []const u8) !EventResult {
    const url = try std.fmt.allocPrint(allocator, "{s}{s}", .{ base_url, path });
    defer allocator.free(url);

    const uri = try std.Uri.parse(url);

    var req = try client.request(.GET, uri, .{});
    defer req.deinit();

    try req.sendBodiless();

    var redirect_buf: [1024]u8 = undefined;
    var response = try req.receiveHead(&redirect_buf);

    // Extract request ID from headers
    var request_id: []u8 = undefined;
    var found_id = false;

    var header_iter = response.head.iterateHeaders();
    while (header_iter.next()) |header| {
        if (std.ascii.eqlIgnoreCase(header.name, "lambda-runtime-aws-request-id")) {
            request_id = try allocator.dupe(u8, header.value);
            found_id = true;
            break;
        }
    }

    if (!found_id) {
        request_id = try allocator.dupe(u8, "unknown");
    }

    // Read body directly from underlying reader
    // NOTE: response.reader() has a bug in Zig 0.16 with @fieldParentPtr,
    // so we read directly from req.reader.in instead
    var body_list = std.ArrayListUnmanaged(u8){};
    errdefer body_list.deinit(allocator);

    const content_len = response.head.content_length orelse 0;
    if (content_len > 0) {
        const underlying_reader = req.reader.in;

        // Check for already-buffered data first
        const buffered = underlying_reader.buffered();
        if (buffered.len > 0) {
            const to_read = @min(buffered.len, content_len);
            try body_list.appendSlice(allocator, buffered[0..to_read]);
            underlying_reader.toss(to_read);
        }

        // Read remaining if needed
        if (body_list.items.len < content_len) {
            var buf: [4096]u8 = undefined;
            while (body_list.items.len < content_len) {
                const to_read = @min(buf.len, content_len - body_list.items.len);
                underlying_reader.readSliceAll(buf[0..to_read]) catch break;
                try body_list.appendSlice(allocator, buf[0..to_read]);
            }
        }
    }

    // Mark reader state as ready so deinit doesn't try to drain body
    // (we already read it directly from the underlying reader)
    req.reader.state = .ready;

    return .{
        .body = try body_list.toOwnedSlice(allocator),
        .request_id = request_id,
    };
}

/// POST request to Lambda Runtime API
fn httpPost(
    allocator: std.mem.Allocator,
    client: *std.http.Client,
    base_url: []const u8,
    path_prefix: []const u8,
    request_id: []const u8,
    path_suffix: []const u8,
    body: []const u8,
) !void {
    const url = try std.fmt.allocPrint(allocator, "{s}{s}{s}{s}", .{
        base_url,
        path_prefix,
        request_id,
        path_suffix,
    });
    defer allocator.free(url);

    const uri = try std.Uri.parse(url);

    var req = try client.request(.POST, uri, .{});
    defer req.deinit();

    // sendBodyComplete requires mutable slice, so dupe it
    const body_mut = try allocator.dupe(u8, body);
    defer allocator.free(body_mut);
    try req.sendBodyComplete(body_mut);

    var redirect_buf: [1024]u8 = undefined;
    _ = try req.receiveHead(&redirect_buf);
}
