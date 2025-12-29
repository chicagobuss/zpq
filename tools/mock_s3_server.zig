const std = @import("std");
const net = std.Io.net;
const Io = std.Io;

// Configuration
const PORT = 9000;
const FILE_SIZE: u64 = 10 * 1024 * 1024; // 10 MB

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    const address = try net.IpAddress.parse("127.0.0.1", PORT);
    var server = try net.IpAddress.listen(address, io, .{ .reuse_address = true });
    defer server.deinit(io);

    std.debug.print("Mock S3 Server listening on 127.0.0.1:{d}\n", .{PORT});

    while (true) {
        const stream = try server.accept(io);
        std.debug.print("[Conn] New connection\n", .{});
        
        const thread = try std.Thread.spawn(.{}, handleConnection, .{ stream, io });
        thread.detach();
    }
}

fn writeAll(stream: net.Stream, io: Io, data: []const u8) !void {
    _ = io;
    var written: usize = 0;
    while (written < data.len) {
        const n = std.posix.write(stream.socket.handle, data[written..]) catch |err| {
            if (err == error.WouldBlock) {
                 std.posix.nanosleep(0, 1 * std.time.ns_per_ms);
                 continue;
            }
            return err;
        };
        if (n == 0) return error.WriteZero;
        written += n;
    }
}

fn handleConnection(stream: net.Stream, io: Io) void {
    defer stream.close(io);
    
    var header_buffer: [4096]u8 = undefined;
    var request_count: usize = 0;
    const MAX_REQUESTS_PER_CONN = 1000; 

    while (request_count < MAX_REQUESTS_PER_CONN) {
        std.debug.print("[Conn] Waiting for request #{d}...\n", .{request_count + 1});
        
        // Read Header
        const n = std.posix.read(stream.socket.handle, &header_buffer) catch |err| {
            std.debug.print("[Err] Read error: {}\n", .{err});
            return;
        };

        if (n == 0) {
             std.debug.print("[Conn] Client closed connection (EOF)\n", .{});
             return;
        }

        request_count += 1;
        std.debug.print("[Req #{d}] Received {d} bytes\n", .{request_count, n});

        const request_data = header_buffer[0..n];
        
        // Parse
        var method: []const u8 = "";
        var path: []const u8 = "";
        var range_start: ?u64 = null;
        var range_end: ?u64 = null;

        var line_it = std.mem.splitSequence(u8, request_data, "\r\n");
        if (line_it.next()) |request_line| {
            var part_it = std.mem.splitScalar(u8, request_line, ' ');
            if (part_it.next()) |m| method = m;
            if (part_it.next()) |p| path = p;
        }

        // Find Range Header
        var header_it = std.mem.splitSequence(u8, request_data, "\r\n");
        while (header_it.next()) |header| {
            if (std.ascii.startsWithIgnoreCase(header, "Range:")) {
                if (std.mem.indexOf(u8, header, "=")) |eq_idx| {
                    const range_val = std.mem.trim(u8, header[eq_idx+1..], " "); 
                    if (std.mem.indexOf(u8, range_val, "-")) |dash_idx| {
                        const start_str = std.mem.trim(u8, range_val[0..dash_idx], " ");
                        const end_str = std.mem.trim(u8, range_val[dash_idx+1..], " ");
                        
                        if (start_str.len == 0) {
                            // Suffix Range: bytes=-N
                            if (end_str.len > 0) {
                                const suffix = std.fmt.parseInt(u64, end_str, 10) catch 0;
                                if (suffix > 0) {
                                    range_start = FILE_SIZE -| suffix;
                                    range_end = FILE_SIZE - 1;
                                }
                            }
                        } else {
                            // Normal Range: bytes=N- or bytes=N-M
                            range_start = std.fmt.parseInt(u64, start_str, 10) catch 0;
                            if (end_str.len > 0) {
                                range_end = std.fmt.parseInt(u64, end_str, 10) catch null;
                            }
                        }
                    }
                }
            }
        }

        std.debug.print("  Method: {s}, Path: {s}, Range: {any}-{any}\n", .{method, path, range_start, range_end});

        // Response
        var status_code: u16 = 200;
        var status_text: []const u8 = "OK";
        var content_length = FILE_SIZE;
        var start: u64 = 0;
        var end: u64 = FILE_SIZE - 1;
        
        if (range_start) |s| {
            status_code = 206;
            status_text = "Partial Content";
            start = s;
            end = range_end orelse (FILE_SIZE - 1);
            content_length = end - start + 1;
        }

        std.debug.print("  Responding: {d} {s}, Content-Length: {d}\n", .{status_code, status_text, content_length});

        var header_res_buf: [1024]u8 = undefined;
        var headers_slice: []u8 = undefined;

        if (status_code == 206) {
             headers_slice = std.fmt.bufPrint(&header_res_buf, 
                "HTTP/1.1 {d} {s}\r\n" ++
                "Content-Type: application/octet-stream\r\n" ++
                "Content-Length: {d}\r\n" ++
                "Content-Range: bytes {d}-{d}/{d}\r\n" ++ 
                "Connection: keep-alive\r\n" ++
                "ETag: \"mock-etag\"\r\n" ++
                "\r\n",
                .{status_code, status_text, content_length, start, end, FILE_SIZE}
            ) catch return;
        } else {
             headers_slice = std.fmt.bufPrint(&header_res_buf, 
                "HTTP/1.1 {d} {s}\r\n" ++
                "Content-Type: application/octet-stream\r\n" ++
                "Content-Length: {d}\r\n" ++
                "Connection: keep-alive\r\n" ++
                "ETag: \"mock-etag\"\r\n" ++
                "\r\n",
                .{status_code, status_text, content_length}
            ) catch return;
        }

        writeAll(stream, io, headers_slice) catch |err| {
             std.debug.print("  [Err] writeAll headers failed: {}\n", .{err});
             return;
        };

        if (std.mem.eql(u8, method, "HEAD")) {
             continue;
        }

        // Write Body
        if (content_length > 0) {
            var written: u64 = 0;
            var data_buf: [8192]u8 = undefined;
            while (written < content_length) {
                const chunk_size_u = @min(data_buf.len, content_length - written);
                const chunk_size = @as(usize, @intCast(chunk_size_u));
                
                for (0..chunk_size) |i| {
                    const offset = start + written + i;
                    data_buf[i] = @intCast(offset % 256);
                }
                
                writeAll(stream, io, data_buf[0..chunk_size]) catch |err| {
                    std.debug.print("  [Err] writeAll body failed: {}\n", .{err});
                    return;
                };
                written += chunk_size;
            }
        }
    }
}
