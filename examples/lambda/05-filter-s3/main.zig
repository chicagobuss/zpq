const std = @import("std");
const zpq = @import("zpq");
const xev = @import("xev");

const ENV_RUNTIME_API = "AWS_LAMBDA_RUNTIME_API";

pub fn main() !void {
    std.debug.print("ZPQ Filter Lambda starting...\n", .{});
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const runtime_api = std.process.getEnvVarOwned(allocator, ENV_RUNTIME_API) catch |err| {
        std.debug.print("Error getting {s}: {any}\n", .{ ENV_RUNTIME_API, err });
        return err;
    };
    defer allocator.free(runtime_api);

    var threaded = std.Io.Threaded.init(allocator);
    defer threaded.deinit();

    var client = std.http.Client{ .allocator = allocator, .io = threaded.io() };
    defer client.deinit();

    const next_url = try std.fmt.allocPrint(allocator, "http://{s}/2018-06-01/runtime/invocation/next", .{runtime_api});
    defer allocator.free(next_url);

    while (true) {
        std.debug.print("Waiting for next invocation...\n", .{});

        var server_header_buffer: [4096]u8 = undefined;
        var req = try client.request(.GET, try std.Uri.parse(next_url), .{});
        defer req.deinit();

        try req.sendBodiless();
        var res = try req.receiveHead(&server_header_buffer);

        if (res.head.status != .ok) {
            std.debug.print("Runtime API Error: {d}\n", .{res.head.status});
            return error.RuntimeApiError;
        }

        var request_id: []u8 = undefined;
        var it = res.head.iterateHeaders();
        while (it.next()) |header| {
            if (std.ascii.eqlIgnoreCase(header.name, "Lambda-Runtime-Aws-Request-Id")) {
                request_id = try allocator.dupe(u8, header.value);
                break;
            }
        } else {
            request_id = try allocator.dupe(u8, "unknown");
        }
        defer allocator.free(request_id);

        var body: []u8 = &[_]u8{};
        defer if (body.len > 0) allocator.free(body);

        if (res.head.content_length) |cl| {
            body = try allocator.alloc(u8, cl);
            var transfer_buf: [4096]u8 = undefined;
            const rdr = res.reader(&transfer_buf);
            try rdr.*.readSliceAll(body);
        }

        std.debug.print("Received invocation: {s}\n", .{request_id});
        std.debug.print("Payload: {s}\n", .{body});

        // Process event and build response
        const result = processEvent(allocator, body) catch |err| blk: {
            std.debug.print("Process error: {any}\n", .{err});
            break :blk try std.fmt.allocPrint(allocator, "{{\"error\": \"{s}\"}}", .{@errorName(err)});
        };
        defer allocator.free(result);

        // Post response
        const resp_url = try std.fmt.allocPrint(allocator, "http://{s}/2018-06-01/runtime/invocation/{s}/response", .{ runtime_api, request_id });
        defer allocator.free(resp_url);

        var resp_req = try client.request(.POST, try std.Uri.parse(resp_url), .{});
        defer resp_req.deinit();

        resp_req.transfer_encoding = .chunked;
        var write_buf: [4096]u8 = undefined;
        var body_writer = try resp_req.sendBody(&write_buf);
        try body_writer.writer.writeAll(result);
        try body_writer.end();

        var redirect_buf: [1024]u8 = undefined;
        _ = try resp_req.receiveHead(&redirect_buf);
    }
}

fn processEvent(allocator: std.mem.Allocator, payload: []const u8) ![]const u8 {
    // Parse JSON payload
    const input_path = extractJsonString(payload, "input_path") orelse return error.MissingInputPath;
    const output_path = extractJsonString(payload, "output_path") orelse return error.MissingOutputPath;
    const filter_column = extractJsonString(payload, "filter_column") orelse return error.MissingFilterColumn;
    const filter_value = extractJsonString(payload, "filter_value") orelse return error.MissingFilterValue;

    std.debug.print("Input: {s}\n", .{input_path});
    std.debug.print("Output: {s}\n", .{output_path});
    std.debug.print("Filter: {s} = {s}\n", .{ filter_column, filter_value });

    // Read and filter
    const stats = try filterParquet(allocator, input_path, filter_column, filter_value);

    // Upload result to S3
    try uploadToS3(allocator, stats.temp_path, output_path);

    return std.fmt.allocPrint(allocator,
        \\{{"status": "success", "input_rows": {d}, "output_rows": {d}, "output_size_bytes": {d}, "output_path": "{s}"}}
    , .{ stats.input_rows, stats.output_rows, stats.output_size, output_path });
}

const FilterStats = struct {
    input_rows: u64,
    output_rows: u64,
    output_size: u64,
    temp_path: []const u8,
};

fn filterParquet(allocator: std.mem.Allocator, input_path: []const u8, filter_column: []const u8, filter_value: []const u8) !FilterStats {
    var timer = try std.time.Timer.start();

    // Initialize xev loop for async S3 - use Epoll to avoid io_uring (not supported in Lambda)
    var loop = try xev.Epoll.Loop.init(.{});
    defer loop.deinit();

    var thread_pool = xev.ThreadPool.init(.{});
    defer {
        thread_pool.shutdown();
        thread_pool.deinit();
    }

    std.debug.print("Opening S3 file: {s}\n", .{input_path});
    var pf = try zpq.s3.factory.openS3WithLoop(allocator, &loop, &thread_pool, input_path, .{
        .force_async = true,
        .verify_tls = true,
    });
    defer pf.deinit();
    try pf.readFooter();

    const meta = pf.metadata orelse return error.NoMetadata;
    const input_rows: u64 = @intCast(meta.num_rows);
    std.debug.print("Input file has {d} rows, {d} row groups\n", .{ input_rows, meta.row_groups.items.len });

    // Find filter column index
    const filter_col_idx = pf.getColumnIndexByName(filter_column) orelse {
        std.debug.print("Filter column '{s}' not found!\n", .{filter_column});
        return error.FilterColumnNotFound;
    };
    std.debug.print("Filter column '{s}' at index {d}\n", .{ filter_column, filter_col_idx });

    // Collect matching values
    var output_rows: u64 = 0;
    var matching_values: std.ArrayListUnmanaged([]u8) = .{};
    defer {
        for (matching_values.items) |v| allocator.free(v);
        matching_values.deinit(allocator);
    }

    // Scan all row groups
    for (meta.row_groups.items, 0..) |rg_meta, rg_idx| {
        std.debug.print("Processing row group {d}...\n", .{rg_idx});

        // Use projection to only fetch the filter column
        var rg_reader = try pf.rowGroupWithProjection(rg_idx, &[_][]const u8{filter_column});
        defer rg_reader.deinit();

        // Get filter column metadata
        const filter_col = rg_meta.columns.items[filter_col_idx];
        const filter_md = filter_col.meta_data orelse return error.MissingColumnMetaData;
        const filter_levels = meta.getColumnLevels(filter_md.path_in_schema.items);
        const filter_schema = meta.getColumnSchema(filter_md.path_in_schema.items);
        const filter_type_len = if (filter_schema) |se| se.type_length else null;

        const filter_col_reader = try rg_reader.columnReaderByName(filter_column);

        var batch_reader = zpq.core.batch_reader.BatchReader([]const u8).init(
            allocator,
            filter_col_reader,
            filter_md.type,
            @intCast(filter_levels.max_def),
            @intCast(filter_levels.max_rep),
            filter_type_len,
        );
        defer batch_reader.deinit();

        // Read batches and filter
        var buf: [1024]?[]const u8 = undefined;
        while (true) {
            const n = try batch_reader.nextBatch(&buf);
            if (n == 0) break;

            for (buf[0..n]) |maybe_val| {
                if (maybe_val) |val| {
                    if (std.mem.eql(u8, val, filter_value)) {
                        // Match - store a copy
                        const copy = try allocator.dupe(u8, val);
                        try matching_values.append(allocator, copy);
                        output_rows += 1;
                    }
                }
            }
        }
    }

    std.debug.print("Filter matched {d} rows out of {d}\n", .{ output_rows, input_rows });

    // Write filtered data to temp file
    const temp_path = "/tmp/zpq_filtered_output.parquet";
    const output_size = try writeFilteredParquet(allocator, filter_column, matching_values.items, temp_path);

    const elapsed_ns = timer.read();
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    std.debug.print("Filter completed in {d:.2}ms\n", .{elapsed_ms});

    return FilterStats{
        .input_rows = input_rows,
        .output_rows = output_rows,
        .output_size = output_size,
        .temp_path = temp_path,
    };
}

fn writeFilteredParquet(
    allocator: std.mem.Allocator,
    column_name: []const u8,
    values: [][]u8,
    output_path: []const u8,
) !u64 {
    std.debug.print("Writing {d} rows to {s}\n", .{ values.len, output_path });

    var writer = try zpq.core.writer.ParquetWriter.init(allocator, output_path);
    defer writer.deinit();

    // Single column output with the filter column values
    try writer.setColumns(&[_]zpq.core.writer.ColumnDef{
        .{ .name = column_name, .type = .BYTE_ARRAY },
    });

    if (values.len > 0) {
        var rg = try writer.beginRowGroup();

        // Convert [][]u8 to [][]const u8
        var const_values: std.ArrayListUnmanaged([]const u8) = .{};
        defer const_values.deinit(allocator);

        for (values) |v| {
            try const_values.append(allocator, v);
        }

        try rg.writeByteArrayColumn(const_values.items);
        try writer.finishRowGroup(rg, @intCast(values.len));
    }

    try writer.finish();

    // Get file size
    const file = try std.fs.openFileAbsolute(output_path, .{});
    defer file.close();
    const stat = try file.stat();

    std.debug.print("Wrote filtered parquet to {s} ({d} bytes)\n", .{ output_path, stat.size });
    return stat.size;
}

fn uploadToS3(allocator: std.mem.Allocator, local_path: []const u8, s3_path: []const u8) !void {
    std.debug.print("Uploading {s} to {s} using S3Writer\n", .{ local_path, s3_path });

    // Parse S3 path: s3://bucket/key
    if (!std.mem.startsWith(u8, s3_path, "s3://")) return error.InvalidS3Path;
    const path_after_scheme = s3_path[5..];
    const slash_idx = std.mem.indexOf(u8, path_after_scheme, "/") orelse return error.InvalidS3Path;
    const bucket = path_after_scheme[0..slash_idx];
    const key = path_after_scheme[slash_idx + 1 ..];

    // Get AWS credentials from environment (Lambda execution role provides these)
    const access_key = std.process.getEnvVarOwned(allocator, "AWS_ACCESS_KEY_ID") catch |err| {
        std.debug.print("No AWS_ACCESS_KEY_ID: {}\n", .{err});
        return error.NoCredentials;
    };
    defer allocator.free(access_key);

    const secret_key = std.process.getEnvVarOwned(allocator, "AWS_SECRET_ACCESS_KEY") catch |err| {
        std.debug.print("No AWS_SECRET_ACCESS_KEY: {}\n", .{err});
        return error.NoCredentials;
    };
    defer allocator.free(secret_key);

    const session_token = std.process.getEnvVarOwned(allocator, "AWS_SESSION_TOKEN") catch null;
    defer if (session_token) |t| allocator.free(t);

    const region = std.process.getEnvVarOwned(allocator, "AWS_REGION") catch
        std.process.getEnvVarOwned(allocator, "AWS_DEFAULT_REGION") catch {
        std.debug.print("No AWS_REGION, defaulting to us-east-1\n", .{});
        return error.NoRegion;
    };
    defer allocator.free(region);

    // Read the local file
    const file = std.fs.openFileAbsolute(local_path, .{}) catch |err| {
        std.debug.print("Failed to open local file: {}\n", .{err});
        return err;
    };
    defer file.close();

    const stat = try file.stat();
    const file_size = stat.size;
    const content = try allocator.alloc(u8, file_size);
    defer allocator.free(content);
    // Read file in chunks (Zig 0.16 API)
    var total_read: usize = 0;
    while (total_read < file_size) {
        const n = try file.read(content[total_read..]);
        if (n == 0) break;
        total_read += n;
    }
    const bytes_read = total_read;

    std.debug.print("Read {d} bytes from local file\n", .{bytes_read});

    // Use S3Writer with Epoll backend (Lambda compatible)
    const S3Writer = zpq.s3.S3WriterGen(xev.Epoll);
    var s3w = try S3Writer.init(allocator, bucket, key, region);
    defer s3w.deinit();

    // Enable UNSIGNED-PAYLOAD for faster uploads (safe over HTTPS)
    s3w.use_unsigned_payload = true;

    // Set credentials
    try s3w.setCredentials(access_key, secret_key, session_token);

    // Upload timing
    var upload_timer = try std.time.Timer.start();

    // Write all content
    try s3w.writeAll(content[0..bytes_read]);

    // Finish the upload (completes multipart if needed)
    try s3w.finish();

    const elapsed_ns = upload_timer.read();
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    const throughput = (@as(f64, @floatFromInt(bytes_read)) / (1024.0 * 1024.0)) / (elapsed_ms / 1000.0);

    std.debug.print("Upload complete: {d} bytes in {d:.1}ms ({d:.1} MB/s)\n", .{ bytes_read, elapsed_ms, throughput });
}

fn extractJsonString(json: []const u8, key: []const u8) ?[]const u8 {
    var i: usize = 0;
    while (i < json.len) {
        const q1 = std.mem.indexOfPos(u8, json, i, "\"") orelse return null;
        const after_q1 = q1 + 1;
        if (after_q1 >= json.len) return null;

        const q2 = std.mem.indexOfPos(u8, json, after_q1, "\"") orelse return null;
        const found_key = json[after_q1..q2];

        if (std.mem.eql(u8, found_key, key)) {
            const after_key = q2 + 1;
            const colon = std.mem.indexOfPos(u8, json, after_key, ":") orelse return null;
            const after_colon = colon + 1;
            const val_q1 = std.mem.indexOfPos(u8, json, after_colon, "\"") orelse return null;
            const val_start = val_q1 + 1;
            if (val_start >= json.len) return null;
            const val_q2 = std.mem.indexOfPos(u8, json, val_start, "\"") orelse return null;
            return json[val_start..val_q2];
        }

        i = q2 + 1;
    }
    return null;
}
