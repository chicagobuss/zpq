const std = @import("std");
const zpq = @import("zpq");
const xev = @import("xev");

const Pipeline = zpq.core.pipeline.Pipeline;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const runtime_api = std.process.getEnvVarOwned(allocator, "AWS_LAMBDA_RUNTIME_API") catch |err| {
        std.debug.print("Not running in Lambda (no AWS_LAMBDA_RUNTIME_API): {}\n", .{err});
        return err;
    };
    defer allocator.free(runtime_api);

    // Lambda runtime loop
    var http_client = std.http.Client{ .allocator = allocator };
    defer http_client.deinit();

    const next_url = try std.fmt.allocPrint(allocator, "http://{s}/2018-06-01/runtime/invocation/next", .{runtime_api});
    defer allocator.free(next_url);

    while (true) {
        // Get next invocation
        var req = try http_client.open(.GET, try std.Uri.parse(next_url), .{}, .{});
        defer req.deinit();
        try req.send();
        try req.wait();

        const request_id = req.response.iterateHeaders().first("Lambda-Runtime-Aws-Request-Id") orelse "unknown";
        const body = try req.reader().readAllAlloc(allocator, 1024 * 1024);
        defer allocator.free(body);

        // Process
        const result = processEvent(allocator, body) catch |err| blk: {
            break :blk try std.fmt.allocPrint(allocator, "{{\"error\": \"{s}\"}}", .{@errorName(err)});
        };
        defer allocator.free(result);

        // Send response
        const resp_url = try std.fmt.allocPrint(allocator, "http://{s}/2018-06-01/runtime/invocation/{s}/response", .{ runtime_api, request_id });
        defer allocator.free(resp_url);

        var resp_req = try http_client.open(.POST, try std.Uri.parse(resp_url), .{}, .{});
        defer resp_req.deinit();
        resp_req.transfer_encoding = .{ .content_length = result.len };
        try resp_req.send();
        try resp_req.writeAll(result);
        try resp_req.finish();
        try resp_req.wait();
    }
}

fn processEvent(allocator: std.mem.Allocator, payload: []const u8) ![]const u8 {
    const input_path = extractJsonString(payload, "input") orelse return error.MissingInput;
    const output_path = extractJsonString(payload, "output") orelse return error.MissingOutput;
    const filter_expr = extractJsonString(payload, "filter") orelse return error.MissingFilter;
    const select_expr = extractJsonString(payload, "select"); // optional

    // Initialize xev runtime (use Epoll for Lambda compatibility)
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var thread_pool = xev.ThreadPool.init(.{ .max_threads = 4 });
    defer {
        thread_pool.shutdown();
        thread_pool.deinit();
    }

    // Run pipeline
    var pipeline = Pipeline.init(allocator);
    defer pipeline.deinit();

    pipeline.setInput(input_path);
    pipeline.setOutput("/tmp/output.parquet");
    try pipeline.setFilter(filter_expr);
    if (select_expr) |s| try pipeline.setProjection(s);
    pipeline.setRuntime(&loop, &thread_pool);

    const result = try pipeline.execute(.slot_parallel);

    // Upload to S3
    try uploadToS3(allocator, "/tmp/output.parquet", output_path);

    return std.fmt.allocPrint(allocator,
        \\{{"status":"success","input_rows":{d},"output_rows":{d},"time_ms":{d:.1},"output":"{s}"}}
    , .{ result.input_rows, result.output_rows, result.elapsed_ms, output_path });
}

fn uploadToS3(allocator: std.mem.Allocator, local_path: []const u8, s3_path: []const u8) !void {
    if (!std.mem.startsWith(u8, s3_path, "s3://")) return error.InvalidS3Path;

    const path_after_scheme = s3_path[5..];
    const slash_idx = std.mem.indexOf(u8, path_after_scheme, "/") orelse return error.InvalidS3Path;
    const bucket = path_after_scheme[0..slash_idx];
    const key = path_after_scheme[slash_idx + 1 ..];

    const access_key = try std.process.getEnvVarOwned(allocator, "AWS_ACCESS_KEY_ID");
    defer allocator.free(access_key);
    const secret_key = try std.process.getEnvVarOwned(allocator, "AWS_SECRET_ACCESS_KEY");
    defer allocator.free(secret_key);
    const session_token = std.process.getEnvVarOwned(allocator, "AWS_SESSION_TOKEN") catch null;
    defer if (session_token) |t| allocator.free(t);
    const region = std.process.getEnvVarOwned(allocator, "AWS_REGION") catch try allocator.dupe(u8, "us-east-1");
    defer allocator.free(region);

    // Read local file
    const file = try std.fs.openFileAbsolute(local_path, .{});
    defer file.close();
    const content = try file.readToEndAlloc(allocator, 100 * 1024 * 1024);
    defer allocator.free(content);

    // Upload via S3Writer
    var s3w = try zpq.s3.S3Writer.init(allocator, bucket, key, region);
    defer s3w.deinit();
    s3w.use_unsigned_payload = true;
    try s3w.setCredentials(access_key, secret_key, session_token);
    try s3w.writeAll(content);
    try s3w.finish();
}

fn extractJsonString(json: []const u8, key: []const u8) ?[]const u8 {
    // Simple JSON string extraction (production should use proper parser)
    const search = std.fmt.comptimePrint("\"{s}\"", .{key});
    const key_pos = std.mem.indexOf(u8, json, search) orelse return null;
    const after_key = key_pos + search.len;
    const colon = std.mem.indexOfPos(u8, json, after_key, ":") orelse return null;
    const val_start = std.mem.indexOfPos(u8, json, colon + 1, "\"") orelse return null;
    const val_end = std.mem.indexOfPos(u8, json, val_start + 1, "\"") orelse return null;
    return json[val_start + 1 .. val_end];
}
