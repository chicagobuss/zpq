const std = @import("std");
const zpq = @import("zpq");

// Standard AWS Lambda Runtime API environment variable
const ENV_RUNTIME_API = "AWS_LAMBDA_RUNTIME_API";

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const runtime_api = std.process.getEnvVarOwned(allocator, ENV_RUNTIME_API) catch |err| {
        std.debug.print("Error getting {s}: {any}\n", .{ENV_RUNTIME_API, err});
        return err;
    };
    defer allocator.free(runtime_api);

    // Create HTTP Client
    // We need threaded IO for the client
    var threaded = std.Io.Threaded.init(allocator);
    defer threaded.deinit();

    var client = std.http.Client{ .allocator = allocator, .io = threaded.io() };
    defer client.deinit();

    const next_url = try std.fmt.allocPrint(allocator, "http://{s}/2018-06-01/runtime/invocation/next", .{runtime_api});
    defer allocator.free(next_url);

    while (true) {
        // 1. Get next invocation (blocking)
        std.debug.print("Waiting for next invocation...\n", .{});
        
        var server_header_buffer: [4096]u8 = undefined;
        var req = try client.request(.GET, try std.Uri.parse(next_url), .{});
        defer req.deinit();
        
        try req.sendBodiless();
        
        var res = try req.receiveHead(&server_header_buffer);
        
        if (res.head.status != .ok) {
             std.debug.print("Runtime API Error: {d}\n", .{res.head.status});
             // In a real retry loop we might backoff, but here we crash to let AWS restart us
             return error.RuntimeApiError;
        }

        var request_id: []u8 = undefined;
        // Use confirmed API: res.head.iterateHeaders()
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

        // Use confirmed API: ArrayList.initCapacity
        var read_buf: [4096]u8 = undefined;
        var rdr = res.reader(&read_buf);
        const ByteList = std.ArrayList(u8);
        var body_list = try ByteList.initCapacity(allocator, 4096);
        defer body_list.deinit(allocator);
        
        while (true) {
            var temp_buf: [1024]u8 = undefined;
            const n = try rdr.readSliceShort(&temp_buf);
            if (n == 0) break;
            try body_list.appendSlice(allocator, temp_buf[0..n]);
        }
        const body = try body_list.toOwnedSlice(allocator);
        defer allocator.free(body);
        
        std.debug.print("Received invocation: {s}\n", .{request_id});

        // 2. Process Event (Echo for now)
        // Parse "file" from JSON body manually or use std.json
        // Simple manual scan for "file": "..."
        var result_msg: []const u8 = "Done";
        
        if (std.mem.indexOf(u8, body, "\"file\"")) |idx| {
             // quick and dirty parse
             if (std.mem.indexOf(u8, body[idx+6..], "\"")) |q1| {
                 const start = idx + 6 + q1 + 1;
                 if (std.mem.indexOf(u8, body[start..], "\"")) |q2| {
                     const val = body[start .. start + q2];
                     std.debug.print("Processing file: {s}\n", .{val});
                     
                     // Run the scan
                     scan_benchmark(allocator, val) catch |err| {
                         std.debug.print("Scan error: {any}\n", .{err});
                         result_msg = "Error";
                     };
                 }
             }
        }

        // 3. Post Response
        const resp_url = try std.fmt.allocPrint(allocator, "http://{s}/2018-06-01/runtime/invocation/{s}/response", .{runtime_api, request_id});
        defer allocator.free(resp_url);
        
        var resp_req = try client.request(.POST, try std.Uri.parse(resp_url), .{});
        defer resp_req.deinit();
        
        resp_req.transfer_encoding = .chunked;
        var write_buf: [4096]u8 = undefined;
        var body_writer = try resp_req.sendBody(&write_buf);
        try body_writer.writer.writeAll(result_msg);
        try body_writer.end();
        // try resp_req.finish();
        
        var redirect_buf2: [1024]u8 = undefined;
        _ = try resp_req.receiveHead(&redirect_buf2);
    }
}

// Adapted from src/main.zig:cmdScan
fn scan_benchmark(allocator: std.mem.Allocator, path: []const u8) !void {
    var timer = try std.time.Timer.start();
    
    // We need logic to open file similar to main.zig openFile
    // But since we can't easily import `main.zig` declarations (it's a binary root),
    // we should really move shared logic to `src/zpq.zig` or `src/lib.zig`.
    // For now, to keep it simple and self-contained as requested, 
    // I will duplicate the S3/File open logic here or reference reusable parts if available.
    // Looking at main.zig, `openFile` does S3 detection.
    
    // REFACTOR OPPORTUNITY: We should move `openFile` to `zpq` struct if possible,
    // but `zpq` is the library. 
    // Let's rely on `zpq.file.ParquetFile.open` for local and implement S3 bridging if needed.
    // Wait, `zpq.file.ParquetFile` is available via `@import("zpq")`.
    
    // For S3 support, we need `AsyncS3Source` logic which is in `main.zig`.
    // The user wants a "Lambda bench". Lambda usually means S3.
    // I will assume for this MVP step we copy the minimal S3 logic or just support local if the VM has the file.
    // BUT the VM is remote, and Lambda is usually S3.
    // Let's implement minimal S3 open here using `zpq.s3`.
    
    var pf = try openFile(allocator, path);
    defer pf.deinit();
    try pf.readFooter();
    
    var total_values: u64 = 0;
    // var total_bytes: u64 = 0;

    if (pf.metadata) |meta| {
        for (meta.row_groups.items) |rg| {
            for (rg.columns.items) |col| {
                if (col.meta_data) |md| {
                    _ = md;
                    var reader = try zpq.column.ColumnReader.init(pf.source, allocator, col);
                    while (try reader.next()) |page| {
                        var p = page;
                        defer p.deinit();
                        // total_bytes += p.data.len;
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
    std.debug.print("Scanned {d} values in {d:.4}s\n", .{total_values, elapsed_s});
}

// Minimal OpenFile adaptation
fn openFile(allocator: std.mem.Allocator, path: []const u8) !zpq.file.ParquetFile {
    if (std.mem.startsWith(u8, path, "s3://")) {
        // S3 support is not yet implemented for the bootstrap
        return error.S3NotImplementedYet; 
    }
    return zpq.file.ParquetFile.open(allocator, path);
}
