const std = @import("std");
const zpq = @import("zpq");
const xev = @import("xev");

// Standard AWS Lambda Runtime API environment variable
const ENV_RUNTIME_API = "AWS_LAMBDA_RUNTIME_API";

pub fn main() !void {
    std.debug.print("LEVEL 04: Starting SUV Lambda...\n", .{});
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const runtime_api = std.process.getEnvVarOwned(allocator, ENV_RUNTIME_API) catch |err| {
        std.debug.print("Error getting {s}: {any}\n", .{ ENV_RUNTIME_API, err });
        return err;
    };
    defer allocator.free(runtime_api);

    // 1. Create HTTP Client for Lambda Runtime API
    // We use standard threaded IO for the runtime API as it's sequential/blocking anyway
    var threaded = std.Io.Threaded.init(allocator);
    defer threaded.deinit();

    var client = std.http.Client{ .allocator = allocator, .io = threaded.io() };
    defer client.deinit();

    const next_url = try std.fmt.allocPrint(allocator, "http://{s}/2018-06-01/runtime/invocation/next", .{runtime_api});
    defer allocator.free(next_url);

    while (true) {
        // Get next invocation
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

        // Process Event
        var result_msg: []const u8 = "Done";
        if (std.mem.indexOf(u8, body, "\"file\"")) |idx| {
            if (std.mem.indexOf(u8, body[idx + 6 ..], "\"")) |q1| {
                const start = idx + 6 + q1 + 1;
                if (std.mem.indexOf(u8, body[start..], "\"")) |q2| {
                    const val = body[start .. start + q2];
                    std.debug.print("Processing file: {s}\n", .{val});

                    scan_benchmark(allocator, val) catch |err| {
                        std.debug.print("Scan error: {any}\n", .{err});
                        result_msg = "Error";
                    };
                }
            }
        }

        // Post Response
        const resp_url = try std.fmt.allocPrint(allocator, "http://{s}/2018-06-01/runtime/invocation/{s}/response", .{ runtime_api, request_id });
        defer allocator.free(resp_url);

        var resp_req = try client.request(.POST, try std.Uri.parse(resp_url), .{});
        defer resp_req.deinit();

        resp_req.transfer_encoding = .chunked;
        var write_buf: [4096]u8 = undefined;
        var body_writer = try resp_req.sendBody(&write_buf);
        try body_writer.writer.writeAll(result_msg);
        try body_writer.end();

        var redirect_buf2: [1024]u8 = undefined;
        _ = try resp_req.receiveHead(&redirect_buf2);
    }
}

fn scan_benchmark(allocator: std.mem.Allocator, path: []const u8) !void {
    std.debug.print("LEVEL 04: scan_benchmark started for {s}\n", .{path});
    var timer = try std.time.Timer.start();

    // SUV Lambda: Use Epoll to avoid io_uring blocks in Lambda
    std.debug.print("LEVEL 04: Initializing Epoll loop...\n", .{});
    var loop = try xev.Epoll.Loop.init(.{});
    defer loop.deinit();

    std.debug.print("LEVEL 04: Initializing ThreadPool...\n", .{});
    var thread_pool = xev.ThreadPool.init(.{});
    defer {
        thread_pool.shutdown();
        thread_pool.deinit();
    }

    std.debug.print("LEVEL 04: Opening S3 file with Loop...\n", .{});
    var pf = try zpq.s3.factory.openS3WithLoop(allocator, &loop, &thread_pool, path, .{
        .force_async = true,
        .verify_tls = true,
    });
    defer pf.deinit();

    const row_count = pf.metadata.?.num_rows;
    std.debug.print("LEVEL 04: Successfully opened Parquet file. Rows: {d}\n", .{row_count});
    defer pf.deinit();
    try pf.readFooter();

    var total_values: u64 = 0;
    if (pf.metadata) |meta| {
        for (meta.row_groups.items) |rg| {
            for (rg.columns.items) |col| {
                if (col.meta_data) |md| {
                    _ = md;
                    var reader = try zpq.column.ColumnReader.init(pf.source, col);
                    while (try reader.next(allocator)) |page| {
                        var p = page;
                        defer p.deinit(allocator);
                        if (p.header.data_page_header) |dph| {
                            total_values += @intCast(dph.num_values);
                        }
                    }
                }
            }
        }
    }

    const elapsed_ns = timer.read();
    const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0;
    std.debug.print("Scanned {d} values in {d:.4}s\n", .{ total_values, elapsed_s });
}

