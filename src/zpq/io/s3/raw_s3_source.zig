const std = @import("std");
const io = @import("../io/interface.zig");
const SigV4 = @import("sigv4.zig");
const ConnectionPool = @import("connection_pool.zig").ConnectionPool;
const ConnectionKey = @import("connection_pool.zig").ConnectionKey;
const Connection = @import("connection_pool.zig").Connection;
const Io = std.Io;
const zpq_log = @import("../../zpq.zig").log;
const log = zpq_log.s3;

pub const RawS3Source = struct {
    allocator: std.mem.Allocator,

    host: []const u8,
    port: u16,
    bucket: []const u8,
    key: []const u8,
    use_tls: bool,

    threaded: Io.Threaded,
    io: Io,

    pool: *ConnectionPool,

    // We no longer own a single socket; we use the pool.
    // socket: ?std.posix.fd_t = null,

    source: io.RandomAccessSource,

    pub fn init(allocator: std.mem.Allocator, uri: std.Uri) !*RawS3Source {
        const self = try allocator.create(RawS3Source);
        errdefer allocator.destroy(self);

        self.allocator = allocator;

        const host_slice = switch (uri.host.?) {
            .raw => |s| s,
            .percent_encoded => |s| s,
        };
        self.host = try allocator.dupe(u8, host_slice);
        self.port = uri.port orelse (if (std.mem.eql(u8, uri.scheme, "https")) 443 else 80);

        const path_slice = switch (uri.path) {
            .raw => |s| s,
            .percent_encoded => |s| s,
        };
        var path_iter = std.mem.splitScalar(u8, path_slice, '/');
        _ = path_iter.next(); // Skip leading slash
        const bucket = path_iter.next() orelse return error.InvalidPath;
        const key = path_iter.rest();
        if (key.len == 0) return error.InvalidPath;

        self.bucket = try allocator.dupe(u8, bucket);
        self.key = try allocator.dupe(u8, key);
        self.use_tls = std.mem.eql(u8, uri.scheme, "https");

        self.threaded = Io.Threaded.init(allocator);
        self.io = self.threaded.io();

        // Initialize pool (heap allocated so it can be shared if needed, though owned by Source for now)
        self.pool = try allocator.create(ConnectionPool);
        self.pool.* = ConnectionPool.init(allocator);

        self.source = .{
            .ptr = self,
            .vtable = &.{
                .readAt = readAtImpl,
                .size = getSizeImpl,
                .close = deinitImpl,
            },
        };

        return self;
    }

    fn deinitImpl(ptr: *anyopaque) void {
        const self: *RawS3Source = @ptrCast(@alignCast(ptr));
        self.pool.deinit();
        self.allocator.destroy(self.pool);

        self.threaded.deinit();
        self.allocator.free(self.host);
        self.allocator.free(self.bucket);
        self.allocator.free(self.key);
        self.allocator.destroy(self);
    }

    fn getSizeImpl(ptr: *anyopaque) u64 {
        const self: *RawS3Source = @ptrCast(@alignCast(ptr));
        _ = self;
        return 0;
    }

    fn readWithTimeout(fd: std.posix.fd_t, buf: []u8, timeout_ms: i32) !usize {
        var fds = [1]std.posix.pollfd{
            .{ .fd = fd, .events = std.posix.POLL.IN, .revents = 0 },
        };
        const count = try std.posix.poll(&fds, timeout_ms);
        if (count == 0) {
            log.debug("read timeout ({d}ms)", .{timeout_ms});
            return error.Timeout;
        }
        if (fds[0].revents & std.posix.POLL.ERR != 0) {
            log.debug("socket POLL.ERR", .{});
            return error.SocketError;
        }
        if (fds[0].revents & std.posix.POLL.HUP != 0) {
            // HUP means closed? Read should return 0.
        }

        return std.posix.read(fd, buf);
    }

    fn readAtImpl(ptr: *anyopaque, offset: u64, buf: []u8) anyerror!usize {
        const self: *RawS3Source = @ptrCast(@alignCast(ptr));
        const key = ConnectionKey{
            .host = self.host,
            .port = self.port,
            .use_tls = self.use_tls,
        };

        var attempt: usize = 0;
        const max_retries = 2; // Try cached, then try fresh

        while (attempt < max_retries) : (attempt += 1) {
            // 1. Get Connection (Pooled or New)
            var conn: Connection = undefined;
            var reused = false;

            if (self.pool.acquire(key)) |c| {
                conn = c;
                reused = true;
                log.debug("reusing connection FD {d}", .{conn.fd});
            } else {
                log.debug("connecting new socket...", .{});
                const fd = try self.connectNew();
                conn = Connection{ .fd = fd };
                reused = false;
            }

            // 2. Perform Request (Wrapped in inner block to catch errors)
            const result = performRequest(self, conn.fd, offset, buf) catch |err| {
                // Handle retryable errors
                if (reused and (err == error.BrokenPipe or err == error.EndOfStream or err == error.ConnectionReset)) {
                    log.debug("stale connection detected (FD {d}), retrying...", .{conn.fd});
                    std.posix.close(conn.fd); // Close bad socket
                    continue; // Retry loop
                }

                // If it wasn't reused, or error is fatal, return error
                if (!reused) {
                    std.posix.close(conn.fd);
                    return err;
                }
                // Should not reach here if reused and error was fatal-ish but not retryable?
                // Let's assume performRequest errors are network errors.
                std.posix.close(conn.fd);
                return err;
            };

            // 3. Success! Return to pool
            try self.pool.release(key, conn);
            return result;
        }

        return error.RetryLimitExceeded;
    }

    fn performRequest(self: *RawS3Source, fd: std.posix.fd_t, offset: u64, buf: []u8) !usize {
        // Construct Request
        var req_buf: [1024]u8 = undefined;
        const req = try std.fmt.bufPrint(&req_buf, "GET /{s}/{s} HTTP/1.1\r\n" ++
            "Host: {s}:{d}\r\n" ++
            "Range: bytes={d}-{d}\r\n" ++
            "Connection: keep-alive\r\n" ++
            "\r\n", .{ self.bucket, self.key, self.host, self.port, offset, offset + buf.len - 1 });

        // Send Request
        // std.debug.print("[RawS3] Sending request...\n", .{});
        var written: usize = 0;
        while (written < req.len) {
            const n = std.posix.write(fd, req[written..]) catch |err| {
                if (err == error.WouldBlock) {
                    std.posix.nanosleep(0, 1 * std.time.ns_per_ms);
                    continue;
                }
                return err;
            };
            if (n == 0) return error.WriteZero;
            written += n;
        }
        // std.debug.print("[RawS3] Request sent.\n", .{});

        // Read Response (Manual buffering)
        var internal_buf: [4096]u8 = undefined;
        var buf_pos: usize = 0;
        var buf_len: usize = 0;

        const refill = struct {
            fn call(fd_in: std.posix.fd_t, b: []u8, pos: *usize, len: *usize) !void {
                if (pos.* < len.*) return;
                pos.* = 0;

                // Poll for data (2s timeout)
                var fds = [1]std.posix.pollfd{
                    .{ .fd = fd_in, .events = std.posix.POLL.IN, .revents = 0 },
                };
                const count = try std.posix.poll(&fds, 2000);
                if (count == 0) {
                    // std.debug.print("[RawS3] Poll timeout\n", .{});
                    return error.Timeout;
                }
                if (fds[0].revents & std.posix.POLL.ERR != 0) {
                    // std.debug.print("[RawS3] Poll Error\n", .{});
                    return error.SocketError;
                }
                if (fds[0].revents & std.posix.POLL.HUP != 0) {
                    // HUP is fine if we read 0 bytes next
                }

                const n = try std.posix.read(fd_in, b);
                if (n == 0) return error.EndOfStream;
                len.* = n;
            }
        }.call;

        // Helper to read byte
        const readByte = struct {
            fn call(fd_in: std.posix.fd_t, b: []u8, pos: *usize, len: *usize) !u8 {
                try refill(fd_in, b, pos, len);
                const byte = b[pos.*];
                pos.* += 1;
                return byte;
            }
        }.call;

        // Parse Headers
        // std.debug.print("[RawS3] Reading headers...\n", .{});
        var content_length: u64 = 0;

        var header_line_buf: [1024]u8 = undefined;
        while (true) {
            // Read line
            var line_len: usize = 0;
            while (line_len < header_line_buf.len) {
                const b = try readByte(fd, &internal_buf, &buf_pos, &buf_len);
                header_line_buf[line_len] = b;
                line_len += 1;
                if (b == '\n') break;
            }
            if (line_len == 0) break; // Should not happen if EOF checked in readByte

            const line = header_line_buf[0..line_len];
            const line_trimmed = std.mem.trimEnd(u8, line, "\r\n");

            if (line_trimmed.len == 0) break; // End of headers

            if (std.ascii.startsWithIgnoreCase(line_trimmed, "Content-Length:")) {
                if (std.mem.indexOf(u8, line_trimmed, ":")) |colon| {
                    const val = std.mem.trim(u8, line_trimmed[colon + 1 ..], " ");
                    content_length = try std.fmt.parseInt(u64, val, 10);
                }
            }
        }
        // std.debug.print("[RawS3] Headers parsed. Content-Length: {d}\n", .{content_length});

        // Read Body
        // std.debug.print("[RawS3] Reading body...\n", .{});
        const want_u64 = @min(content_length, @as(u64, @intCast(buf.len)));
        const want: usize = @intCast(want_u64);
        var body_read: usize = 0;

        // Drain buffered data first
        if (buf_pos < buf_len) {
            const avail = buf_len - buf_pos;
            const to_copy = @min(avail, want);
            @memcpy(buf[0..to_copy], internal_buf[buf_pos .. buf_pos + to_copy]);
            buf_pos += to_copy;
            body_read += to_copy;
        }

        // Read rest directly or via buffer
        while (body_read < want) {
            const dest = buf[body_read..want];

            // Busy loop read
            var n: usize = 0;
            var attempts: usize = 0;
            while (true) {
                n = std.posix.read(fd, dest) catch |err| {
                    if (err == error.WouldBlock) {
                        if (attempts > 5000) return error.Timeout;
                        attempts += 1;
                        std.posix.nanosleep(0, 1 * std.time.ns_per_ms);
                        continue;
                    }
                    return err;
                };
                break;
            }

            if (n == 0) break; // EOF
            body_read += n;
        }

        // std.debug.print("[RawS3] Body read: {d} bytes.\n", .{body_read});

        return body_read;
    }

    fn connectNew(self: *RawS3Source) !std.posix.fd_t {
        // Parse IP directly (assuming IP for now, or use Io.net.IpAddress)
        const addr = try Io.net.IpAddress.parse(self.host, self.port);

        const fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
        errdefer std.posix.close(fd);

        switch (addr) {
            .ip4 => |ip4| {
                const sa = std.posix.sockaddr.in{
                    .family = std.posix.AF.INET,
                    .port = std.mem.nativeToBig(u16, ip4.port),
                    .addr = @as(u32, @bitCast(ip4.bytes)),
                };
                try std.posix.connect(fd, @ptrCast(&sa), @sizeOf(std.posix.sockaddr.in));
            },
            else => return error.UnsupportedAddressFamily,
        }

        // Blocking mode (default)
        return fd;
    }
};
