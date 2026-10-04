//! In-process, plain-HTTP, path-style S3 stand-in for unit tests of the write path. Serves CreateMultipartUpload,
//! UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListMultipartUploads, HEAD and single-object PUT from
//! memory, one thread per connection. `drop` makes it lose the response to one matching request, the way a response
//! lost on the network looks to the client: the server applies the request (or, with `apply_dropped = false`, does
//! not) and then resets the connection without sending a byte.
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

pub const Object = struct { key: []u8, etag: []u8, len: usize };

pub const FakeS3 = struct {
    const MAX_CONNS = 64;

    allocator: std.mem.Allocator,
    listen_fd: posix.fd_t,
    port: u16,
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

    pub fn start(self: *FakeS3, allocator: std.mem.Allocator) !void {
        const rc = posix.system.socket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
        if (posix.errno(rc) != .SUCCESS) return error.SkipZigTest;
        const fd: posix.fd_t = @intCast(rc);
        errdefer _ = posix.system.close(fd);
        var addr: posix.sockaddr.in = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f000001) };
        var len: posix.socklen_t = @sizeOf(posix.sockaddr.in);
        if (posix.errno(posix.system.bind(fd, @ptrCast(&addr), len)) != .SUCCESS) return error.SkipZigTest;
        if (posix.errno(posix.system.listen(fd, 64)) != .SUCCESS) return error.SkipZigTest;
        if (posix.errno(posix.system.getsockname(fd, @ptrCast(&addr), &len)) != .SUCCESS) return error.SkipZigTest;
        self.* = .{ .allocator = allocator, .listen_fd = fd, .port = std.mem.bigToNative(u16, addr.port) };
        self.accept_thread = try std.Thread.spawn(.{}, acceptLoop, .{self});
    }

    /// Call once the client side has closed its connections.
    pub fn deinit(self: *FakeS3) void {
        _ = posix.system.shutdown(self.listen_fd, posix.SHUT.RDWR);
        self.accept_thread.join();
        for (self.conn_fds[0..self.conns], self.conn_threads[0..self.conns]) |fd, thread| {
            _ = posix.system.shutdown(fd, posix.SHUT.RDWR);
            if (thread) |t| t.join();
            _ = posix.system.close(fd);
        }
        _ = posix.system.close(self.listen_fd);
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
        while (self.conns < MAX_CONNS) {
            const rc = posix.system.accept(self.listen_fd, null, null);
            if (posix.errno(rc) != .SUCCESS) return;
            const fd: posix.fd_t = @intCast(rc);
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
            const head = buf.items[0..head_end];
            const body_len = contentLength(head);
            while (buf.items.len < head_end + 4 + body_len) if (!readMore(self.allocator, fd, &buf)) return;
            const request = buf.items[0 .. head_end + 4 + body_len];
            const keep = self.handle(fd, head, request[head_end + 4 ..]) catch false;
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

    fn reply(_: *FakeS3, fd: posix.fd_t, lose: bool, status: u16, extra: []const u8, body: []const u8, advertised_len: ?usize) !bool {
        if (lose) return false;
        var head_buf: [512]u8 = undefined;
        const head = try std.fmt.bufPrint(&head_buf, "HTTP/1.1 {d} X\r\nContent-Length: {d}\r\n{s}\r\n", .{ status, advertised_len orelse body.len, extra });
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
