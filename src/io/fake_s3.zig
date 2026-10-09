//! In-process, plain-HTTP, path-style S3 stand-in for unit tests. Serves CreateMultipartUpload, UploadPart,
//! CompleteMultipartUpload, AbortMultipartUpload, ListMultipartUploads, HEAD and single-object PUT from memory, and
//! ranged GETs of objects a test registers with `serveBytes`, one thread per connection. `drop` makes it lose the
//! response to one matching request, the way a response lost on the network looks to the client: the server applies
//! the request (or, with `apply_dropped = false`, does not) and then resets the connection without sending a byte.
//! `stall` makes it go silent on matching requests while keeping the connection open, the way a stuck server or
//! network path looks: before the response, part-way through its headers or its body, or (for a request with a body)
//! without reading that body, so the client's send blocks.
//!
//! Test-only: imported from tests, never from the engine.

const std = @import("std");
const posix = std.posix;
const Md5 = std.crypto.hash.Md5;

pub const Upload = struct {
    key: []u8,
    id: []u8,
    initiated_s: i64,
    parts: std.ArrayList(Part) = .empty,
};

pub const Part = struct { number: u32, md5: [Md5.digest_length]u8 };

pub const Object = struct {
    key: []u8,
    etag: []u8,
    len: usize,
    /// The bytes a GET reads, borrowed from the test (`serveBytes`); null for objects only written.
    data: ?[]const u8 = null,
};

pub const FakeS3 = struct {
    const MAX_CONNS = 64;

    allocator: std.mem.Allocator,
    listen_fd: posix.fd_t,
    port: u16,
    /// `deinit` writes to `stop[0]`; the accept thread polls `stop[1]`, and writes to it once it has stopped.
    stop: [2]posix.fd_t,
    accept_thread: std.Thread = undefined,
    conn_fds: [MAX_CONNS]posix.fd_t = @splat(-1),
    conn_threads: [MAX_CONNS]?std.Thread = @splat(null),
    conns: usize = 0,

    /// Guards everything below.
    busy: std.atomic.Value(bool) = .init(false),
    uploads: std.ArrayList(Upload) = .empty,
    objects: std.ArrayList(Object) = .empty,
    next_id: u32 = 1,
    /// Substring of "METHOD target" whose next response is lost.
    drop: ?[]const u8 = null,
    apply_dropped: bool = true,
    dropped: u32 = 0,
    /// When set, object GETs past this many are refused (403), as a revoked credential or a deleted object would be.
    gets_allowed: ?u32 = null,
    gets: u32 = 0,
    /// Substring of "METHOD target" on whose next `stalls_left` matches the server goes silent (`stall_at`), holding
    /// the connection open until the client closes it or the server stops.
    stall: ?[]const u8 = null,
    stall_at: StallAt = .mid_body,
    stalls_left: u32 = 1,
    stalled: u32 = 0,
    trickle_ms: u32 = 60,
    /// Set by `deinit`: stalled connection threads stop waiting.
    stopping: std.atomic.Value(bool) = .init(false),

    pub const StallAt = enum {
        /// No byte of the response.
        before_headers,
        /// Part of the status line.
        mid_headers,
        /// The headers and half the body.
        mid_body,
        /// The request's head is read but not its body: a large body (an UploadPart) fills the socket buffers and
        /// the client's send blocks.
        request_body,
        /// Not a stall: the whole response, its body in eight pieces `trickle_ms` apart, slower in all than a
        /// client's idle timeout but never silent for that long.
        trickle,
    };

    pub fn start(self: *FakeS3, allocator: std.mem.Allocator) !void {
        var stop: [2]posix.fd_t = undefined;
        if (posix.errno(posix.system.socketpair(posix.AF.UNIX, posix.SOCK.STREAM, 0, &stop)) != .SUCCESS)
            return error.SkipZigTest;
        errdefer for (stop) |fd| {
            _ = posix.system.close(fd);
        };
        const rc = posix.system.socket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
        if (posix.errno(rc) != .SUCCESS) return error.SkipZigTest;
        const fd: posix.fd_t = @intCast(rc);
        errdefer _ = posix.system.close(fd);
        var addr: posix.sockaddr.in = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f000001) };
        var len: posix.socklen_t = @sizeOf(posix.sockaddr.in);
        if (posix.errno(posix.system.bind(fd, @ptrCast(&addr), len)) != .SUCCESS) return error.SkipZigTest;
        if (posix.errno(posix.system.listen(fd, 64)) != .SUCCESS) return error.SkipZigTest;
        if (posix.errno(posix.system.getsockname(fd, @ptrCast(&addr), &len)) != .SUCCESS) return error.SkipZigTest;
        self.* = .{ .allocator = allocator, .listen_fd = fd, .port = std.mem.bigToNative(u16, addr.port), .stop = stop };
        self.accept_thread = try std.Thread.spawn(.{}, acceptLoop, .{self});
    }

    /// Call once the client side has closed its connections.
    ///
    /// The accept thread is stopped by a byte on a socketpair it polls alongside the listening socket: shutting that
    /// socket down wakes a blocked accept on Linux but not on macOS. Connection threads are woken by shutting their
    /// sockets down, which does work everywhere. A thread that still has not stopped after `stop_timeout_ms` is a
    /// panic naming the server, not a test run that never ends; it cannot be abandoned, since it points into `self`.
    pub fn deinit(self: *FakeS3) void {
        self.stopping.store(true, .release);
        _ = posix.system.write(self.stop[0], "s", 1);
        if (!waitReadable(self.stop[0], stop_timeout_ms))
            std.debug.panic("fake_s3 on port {d}: the accept thread has not stopped {d} ms after deinit()", .{ self.port, stop_timeout_ms });
        self.accept_thread.join();
        for (self.conn_fds[0..self.conns], self.conn_threads[0..self.conns]) |fd, thread| {
            _ = posix.system.shutdown(fd, posix.SHUT.RDWR);
            if (thread) |t| t.join();
            _ = posix.system.close(fd);
        }
        _ = posix.system.close(self.listen_fd);
        for (self.stop) |fd| _ = posix.system.close(fd);
        for (self.uploads.items) |*u| self.freeUpload(u);
        self.uploads.deinit(self.allocator);
        for (self.objects.items) |o| {
            self.allocator.free(o.key);
            self.allocator.free(o.etag);
        }
        self.objects.deinit(self.allocator);
    }

    pub fn lock(self: *FakeS3) void {
        while (self.busy.cmpxchgWeak(false, true, .acquire, .monotonic) != null) std.atomic.spinLoopHint();
    }

    pub fn unlock(self: *FakeS3) void {
        self.busy.store(false, .release);
    }

    /// An open upload for `key` started `age_s` seconds ago, as if by another writer.
    pub fn addUpload(self: *FakeS3, key: []const u8, age_s: i64) !void {
        self.lock();
        defer self.unlock();
        _ = try self.createUpload(key, realtimeS() - age_s);
    }

    pub fn openUploadsFor(self: *FakeS3, key: []const u8) usize {
        self.lock();
        defer self.unlock();
        var n: usize = 0;
        for (self.uploads.items) |u| {
            if (std.mem.eql(u8, u.key, key)) n += 1;
        }
        return n;
    }

    /// Serve `data` (borrowed; it must outlive the server) as object `key` to GETs.
    pub fn serveBytes(self: *FakeS3, key: []const u8, data: []const u8) !void {
        self.lock();
        defer self.unlock();
        try self.putObject(key, try std.fmt.allocPrint(self.allocator, "etag-{d}", .{data.len}), data.len);
        for (self.objects.items) |*o| if (std.mem.eql(u8, o.key, key)) {
            o.data = data;
        };
    }

    pub fn object(self: *FakeS3, key: []const u8) ?Object {
        self.lock();
        defer self.unlock();
        for (self.objects.items) |o| if (std.mem.eql(u8, o.key, key)) return o;
        return null;
    }

    fn freeUpload(self: *FakeS3, u: *Upload) void {
        self.allocator.free(u.key);
        self.allocator.free(u.id);
        u.parts.deinit(self.allocator);
    }

    fn createUpload(self: *FakeS3, key: []const u8, initiated_s: i64) ![]const u8 {
        const id = try std.fmt.allocPrint(self.allocator, "upload-{d}", .{self.next_id});
        self.next_id += 1;
        try self.uploads.append(self.allocator, .{ .key = try self.allocator.dupe(u8, key), .id = id, .initiated_s = initiated_s });
        return id;
    }

    fn acceptLoop(self: *FakeS3) void {
        defer _ = posix.system.write(self.stop[1], "d", 1);
        while (self.conns < MAX_CONNS) {
            var pfd = [_]posix.pollfd{
                .{ .fd = self.listen_fd, .events = posix.POLL.IN, .revents = 0 },
                .{ .fd = self.stop[1], .events = posix.POLL.IN, .revents = 0 },
            };
            switch (posix.errno(posix.system.poll(&pfd, pfd.len, -1))) {
                .SUCCESS => {},
                .INTR => continue,
                else => return,
            }
            // Stopped, and no connection waiting.
            if (pfd[0].revents & posix.POLL.IN == 0) return;
            const rc = posix.system.accept(self.listen_fd, null, null);
            switch (posix.errno(rc)) {
                .SUCCESS => {},
                .INTR, .CONNABORTED, .AGAIN => continue,
                else => return,
            }
            const fd: posix.fd_t = @intCast(rc);
            if (self.stall != null and self.stall_at == .request_body) {
                // A small receive buffer, so a body this server does not read soon blocks the client's send.
                const small: c_int = 16 * 1024;
                _ = posix.system.setsockopt(fd, posix.SOL.SOCKET, posix.SO.RCVBUF, @ptrCast(&small), @sizeOf(c_int));
            }
            // A response goes out in several writes; without this, Nagle holds each later one for the client's delayed
            // ACK, about 40 ms a request, which a test fetching row group by row group pays hundreds of times.
            const one: c_int = 1;
            _ = posix.system.setsockopt(fd, posix.IPPROTO.TCP, posix.TCP.NODELAY, @ptrCast(&one), @sizeOf(c_int));
            self.conn_fds[self.conns] = fd;
            self.conn_threads[self.conns] = std.Thread.spawn(.{}, serveConn, .{ self, fd }) catch null;
            self.conns += 1;
        }
    }

    fn serveConn(self: *FakeS3, fd: posix.fd_t) void {
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(self.allocator);
        while (true) {
            // One request: head, then Content-Length bytes of body.
            const head_end = while (true) {
                if (std.mem.indexOf(u8, buf.items, "\r\n\r\n")) |i| break i;
                if (!readMore(self.allocator, fd, &buf)) return;
            };
            const body_len = contentLength(buf.items[0..head_end]);
            if (self.takeStall(buf.items[0..head_end], .request_body)) {
                self.holdSilent(fd);
                return;
            }
            while (buf.items.len < head_end + 4 + body_len) if (!readMore(self.allocator, fd, &buf)) return;
            // Sliced only now: reading the body may have moved `buf`.
            const request = buf.items[0 .. head_end + 4 + body_len];
            const keep = self.handle(fd, request[0..head_end], request[head_end + 4 ..]) catch false;
            if (!keep) {
                // Reset rather than close: the client sees ECONNRESET or EOF with no response byte.
                const linger: extern struct { on: c_int, secs: c_int } = .{ .on = 1, .secs = 0 };
                _ = posix.system.setsockopt(fd, posix.SOL.SOCKET, posix.SO.LINGER, @ptrCast(&linger), @sizeOf(@TypeOf(linger)));
                _ = posix.system.shutdown(fd, posix.SHUT.RDWR);
                return;
            }
            const used = request.len;
            std.mem.copyForwards(u8, buf.items[0 .. buf.items.len - used], buf.items[used..]);
            buf.shrinkRetainingCapacity(buf.items.len - used);
        }
    }

    /// Whether the request with this head is the next to stall at `at` (and count it).
    fn takeStall(self: *FakeS3, head: []const u8, at: StallAt) bool {
        if (self.stall_at != at) return false;
        const line_end = std.mem.indexOf(u8, head, "\r\n") orelse head.len;
        const sp2 = std.mem.lastIndexOfScalar(u8, head[0..line_end], ' ') orelse return false;
        self.lock();
        defer self.unlock();
        const pat = self.stall orelse return false;
        if (std.mem.indexOf(u8, head[0..sp2], pat) == null) return false;
        self.stalled += 1;
        self.stalls_left -|= 1;
        if (self.stalls_left == 0) self.stall = null;
        return true;
    }

    /// Say nothing more on `fd`, without reading from it, until the server stops; the client gives up and closes its
    /// end in the meantime.
    fn holdSilent(self: *FakeS3, fd: posix.fd_t) void {
        _ = fd;
        while (!self.stopping.load(.acquire)) sleepMs(5);
    }

    /// Apply one request and answer it; false when the connection is to be dropped instead.
    fn handle(self: *FakeS3, fd: posix.fd_t, head: []const u8, body: []const u8) !bool {
        const line_end = std.mem.indexOf(u8, head, "\r\n") orelse head.len;
        const line = head[0..line_end];
        const sp1 = std.mem.indexOfScalar(u8, line, ' ') orelse return false;
        const sp2 = std.mem.lastIndexOfScalar(u8, line, ' ') orelse return false;
        const method = line[0..sp1];
        const target = line[sp1 + 1 .. sp2];
        const desc = line[0..sp2];

        self.lock();
        var locked = true;
        defer if (locked) self.unlock();
        var lose = false;
        stall_reply = null;
        if (self.stall_at != .request_body) if (self.stall) |pat| if (std.mem.indexOf(u8, desc, pat) != null) {
            self.stalled += 1;
            self.stalls_left -|= 1;
            if (self.stalls_left == 0) self.stall = null;
            stall_reply = self.stall_at;
        };
        if (self.drop) |pat| if (std.mem.indexOf(u8, desc, pat) != null) {
            self.drop = null;
            self.dropped += 1;
            lose = true;
            if (!self.apply_dropped) return false;
        };

        var arena_state = std.heap.ArenaAllocator.init(self.allocator);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const q = std.mem.indexOfScalar(u8, target, '?');
        const path = target[0 .. q orelse target.len];
        const query = if (q) |i| target[i + 1 ..] else "";
        // Path-style: /bucket/key
        const after_bucket = std.mem.indexOfScalarPos(u8, path, 1, '/');
        const key = if (after_bucket) |i| try percentDecode(a, path[i + 1 ..]) else "";

        var status: u16 = 200;
        var extra: []const u8 = "";
        var resp_body: []const u8 = "";
        var advertised_len: ?usize = null;
        if (std.mem.eql(u8, method, "POST") and param(query, "uploads") != null) {
            const id = try self.createUpload(key, realtimeS());
            resp_body = try std.fmt.allocPrint(a, "<InitiateMultipartUploadResult><UploadId>{s}</UploadId></InitiateMultipartUploadResult>", .{id});
        } else if (std.mem.eql(u8, method, "PUT") and param(query, "uploadId") != null) {
            const u = self.findUpload(param(query, "uploadId").?) orelse return self.reply(fd, lose, 404, "", "<Error><Code>NoSuchUpload</Code></Error>", null);
            var md5: [Md5.digest_length]u8 = undefined;
            Md5.hash(body, &md5, .{});
            const number = try std.fmt.parseInt(u32, param(query, "partNumber") orelse "0", 10);
            // As S3: a part sent again replaces the one with its number.
            for (u.parts.items, 0..) |p, i| if (p.number == number) {
                _ = u.parts.orderedRemove(i);
                break;
            };
            try u.parts.append(self.allocator, .{ .number = number, .md5 = md5 });
            extra = try std.fmt.allocPrint(a, "ETag: \"{x}\"\r\n", .{md5});
        } else if (std.mem.eql(u8, method, "POST") and param(query, "uploadId") != null) {
            const id = param(query, "uploadId").?;
            const u = self.findUpload(id) orelse return self.reply(fd, lose, 404, "", "<Error><Code>NoSuchUpload</Code></Error>", null);
            std.mem.sort(Part, u.parts.items, {}, struct {
                fn lt(_: void, x: Part, y: Part) bool {
                    return x.number < y.number;
                }
            }.lt);
            var h = Md5.init(.{});
            for (u.parts.items) |p| h.update(&p.md5);
            var digest: [Md5.digest_length]u8 = undefined;
            h.final(&digest);
            const etag = try std.fmt.allocPrint(self.allocator, "{x}-{d}", .{ digest, u.parts.items.len });
            try self.putObject(key, etag, body.len);
            self.removeUpload(id);
            resp_body = try std.fmt.allocPrint(a, "<CompleteMultipartUploadResult><ETag>\"{s}\"</ETag></CompleteMultipartUploadResult>", .{etag});
        } else if (std.mem.eql(u8, method, "DELETE") and param(query, "uploadId") != null) {
            self.removeUpload(param(query, "uploadId").?);
            status = 204;
        } else if (std.mem.eql(u8, method, "GET") and param(query, "uploads") != null) {
            const prefix = try percentDecode(a, param(query, "prefix") orelse "");
            var out: std.ArrayList(u8) = .empty;
            try out.appendSlice(a, "<ListMultipartUploadsResult>");
            for (self.uploads.items) |u| {
                if (!std.mem.startsWith(u8, u.key, prefix)) continue;
                const t = std.time.epoch.EpochSeconds{ .secs = @intCast(u.initiated_s) };
                const yd = t.getEpochDay().calculateYearDay();
                const md = yd.calculateMonthDay();
                const ds = t.getDaySeconds();
                try out.print(a, "<Upload><Key>{s}</Key><UploadId>{s}</UploadId><Initiated>{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.000Z</Initiated></Upload>", .{
                    u.key, u.id, yd.year, md.month.numeric(), md.day_index + 1, ds.getHoursIntoDay(), ds.getMinutesIntoHour(), ds.getSecondsIntoMinute(),
                });
            }
            try out.appendSlice(a, "</ListMultipartUploadsResult>");
            resp_body = out.items;
        } else if (std.mem.eql(u8, method, "GET")) {
            var found: ?Object = null;
            for (self.objects.items) |o| if (std.mem.eql(u8, o.key, key)) {
                found = o;
            };
            const o = found orelse return self.reply(fd, lose, 404, "", "<Error><Code>NoSuchKey</Code></Error>", null);
            self.gets += 1;
            if (self.gets_allowed) |n| if (self.gets > n) {
                return self.reply(fd, lose, 403, "", "<Error><Code>AccessDenied</Code></Error>", null);
            };
            const no_key = "<Error><Code>NoSuchKey</Code></Error>";
            const data = o.data orelse return self.reply(fd, lose, 404, "", no_key, null);
            var lo: usize = 0;
            var hi: usize = data.len; // exclusive
            if (header(head, "Range")) |r| if (std.mem.startsWith(u8, r, "bytes=")) {
                const spec = r["bytes=".len..];
                const dash = std.mem.indexOfScalar(u8, spec, '-') orelse return false;
                if (dash == 0) {
                    const n = try std.fmt.parseInt(usize, spec[1..], 10);
                    lo = data.len -| n;
                } else {
                    lo = try std.fmt.parseInt(usize, spec[0..dash], 10);
                    if (dash + 1 < spec.len) {
                        hi = @min(data.len, try std.fmt.parseInt(usize, spec[dash + 1 ..], 10) + 1);
                    }
                }
                status = 206;
                extra = try std.fmt.allocPrint(a, "ETag: \"{s}\"\r\nContent-Range: bytes {d}-{d}/{d}\r\n", .{
                    o.etag, lo, hi -| 1, data.len,
                });
            };
            if (lo > hi) return self.reply(fd, lose, 416, "", "", null);
            if (status == 200) extra = try std.fmt.allocPrint(a, "ETag: \"{s}\"\r\n", .{o.etag});
            resp_body = data[lo..hi];
        } else if (std.mem.eql(u8, method, "HEAD")) {
            var found: ?Object = null;
            for (self.objects.items) |o| if (std.mem.eql(u8, o.key, key)) {
                found = o;
            };
            const o = found orelse return self.reply(fd, lose, 404, "", "", 0);
            extra = try std.fmt.allocPrint(a, "ETag: \"{s}\"\r\n", .{o.etag});
            advertised_len = o.len;
        } else if (std.mem.eql(u8, method, "PUT")) {
            var md5: [Md5.digest_length]u8 = undefined;
            Md5.hash(body, &md5, .{});
            try self.putObject(key, try std.fmt.allocPrint(self.allocator, "{x}", .{md5}), body.len);
            extra = try std.fmt.allocPrint(a, "ETag: \"{x}\"\r\n", .{md5});
        } else {
            status = 400;
        }
        self.unlock();
        locked = false;
        return self.reply(fd, lose, status, extra, resp_body, advertised_len);
    }

    fn reply(
        self: *FakeS3,
        fd: posix.fd_t,
        lose: bool,
        status: u16,
        extra: []const u8,
        body: []const u8,
        advertised_len: ?usize,
    ) !bool {
        if (lose) return false;
        var head_buf: [512]u8 = undefined;
        const head = try std.fmt.bufPrint(&head_buf, "HTTP/1.1 {d} X\r\nContent-Length: {d}\r\n{s}\r\n", .{ status, advertised_len orelse body.len, extra });
        if (stall_reply) |at| {
            if (at == .trickle) {
                try writeAll(fd, head);
                const step = (body.len + 7) / 8;
                var off: usize = 0;
                while (off < body.len) : (off += step) {
                    sleepMs(self.trickle_ms);
                    try writeAll(fd, body[off..@min(body.len, off + step)]);
                }
                return true;
            }
            switch (at) {
                .before_headers, .request_body, .trickle => {},
                .mid_headers => try writeAll(fd, head[0 .. head.len / 2]),
                .mid_body => {
                    try writeAll(fd, head);
                    try writeAll(fd, body[0 .. body.len / 2]);
                },
            }
            self.holdSilent(fd);
            return false;
        }
        try writeAll(fd, head);
        if (status != 204 and advertised_len == null) try writeAll(fd, body);
        return true;
    }

    fn findUpload(self: *FakeS3, id: []const u8) ?*Upload {
        for (self.uploads.items) |*u| if (std.mem.eql(u8, u.id, id)) return u;
        return null;
    }

    fn removeUpload(self: *FakeS3, id: []const u8) void {
        for (self.uploads.items, 0..) |*u, i| if (std.mem.eql(u8, u.id, id)) {
            self.freeUpload(u);
            _ = self.uploads.orderedRemove(i);
            return;
        };
    }

    /// Takes ownership of `etag`.
    fn putObject(self: *FakeS3, key: []const u8, etag: []u8, len: usize) !void {
        for (self.objects.items) |*o| if (std.mem.eql(u8, o.key, key)) {
            self.allocator.free(o.etag);
            o.etag = etag;
            o.len = len;
            return;
        };
        try self.objects.append(self.allocator, .{ .key = try self.allocator.dupe(u8, key), .etag = etag, .len = len });
    }
};

fn sleepMs(ms: u32) void {
    var ts: posix.timespec = .{
        .sec = @intCast(ms / 1000),
        .nsec = @intCast(@as(u64, ms % 1000) * std.time.ns_per_ms),
    };
    _ = posix.system.nanosleep(&ts, &ts);
}

/// The stall `handle` picked for the request this connection thread is answering, acted on by `reply`.
threadlocal var stall_reply: ?FakeS3.StallAt = null;

/// How long `deinit` waits for the accept thread. A test that works never gets near it.
const stop_timeout_ms = 20_000;

/// Whether `fd` becomes readable within `timeout_ms`.
fn waitReadable(fd: posix.fd_t, timeout_ms: i32) bool {
    while (true) {
        var pfd = [_]posix.pollfd{.{ .fd = fd, .events = posix.POLL.IN, .revents = 0 }};
        const n = posix.system.poll(&pfd, pfd.len, timeout_ms);
        switch (posix.errno(n)) {
            .SUCCESS => return n > 0,
            .INTR => continue,
            else => return false,
        }
    }
}

fn realtimeS() i64 {
    return @import("../clock.zig").realtimeS();
}

fn readMore(allocator: std.mem.Allocator, fd: posix.fd_t, buf: *std.ArrayList(u8)) bool {
    var tmp: [64 * 1024]u8 = undefined;
    const r = posix.system.read(fd, &tmp, tmp.len);
    if (posix.errno(r) != .SUCCESS or r == 0) return false;
    buf.appendSlice(allocator, tmp[0..@intCast(r)]) catch return false;
    return true;
}

fn writeAll(fd: posix.fd_t, bytes: []const u8) !void {
    var off: usize = 0;
    while (off < bytes.len) {
        const r = posix.system.write(fd, bytes[off..].ptr, bytes.len - off);
        if (posix.errno(r) != .SUCCESS or r == 0) return error.WriteFailed;
        off += @intCast(r);
    }
}

/// The value of header `name` (case-insensitive), trimmed.
fn header(head: []const u8, name: []const u8) ?[]const u8 {
    var lines = std.mem.splitSequence(u8, head, "\r\n");
    _ = lines.next();
    while (lines.next()) |l| {
        const colon = std.mem.indexOfScalar(u8, l, ':') orelse continue;
        if (std.ascii.eqlIgnoreCase(l[0..colon], name)) return std.mem.trim(u8, l[colon + 1 ..], " ");
    }
    return null;
}

fn contentLength(head: []const u8) usize {
    var lines = std.mem.splitSequence(u8, head, "\r\n");
    _ = lines.next();
    while (lines.next()) |line| {
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        if (std.ascii.eqlIgnoreCase(line[0..colon], "content-length"))
            return std.fmt.parseInt(usize, std.mem.trim(u8, line[colon + 1 ..], " \t"), 10) catch 0;
    }
    return 0;
}

/// Value of `name` in a query string (empty for a bare `name=`); null when absent.
fn param(query: []const u8, name: []const u8) ?[]const u8 {
    var it = std.mem.splitScalar(u8, query, '&');
    while (it.next()) |kv| {
        const eq = std.mem.indexOfScalar(u8, kv, '=') orelse kv.len;
        if (std.mem.eql(u8, kv[0..eq], name)) return if (eq < kv.len) kv[eq + 1 ..] else "";
    }
    return null;
}

fn percentDecode(a: std.mem.Allocator, s: []const u8) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    var i: usize = 0;
    while (i < s.len) : (i += 1) {
        if (s[i] == '%' and i + 2 < s.len) {
            try out.append(a, try std.fmt.parseInt(u8, s[i + 1 .. i + 3], 16));
            i += 2;
        } else try out.append(a, s[i]);
    }
    return out.items;
}
