//! Streaming S3 multipart upload sink.
//!
//! See `docs/streaming_design.md` for the design rationale. In short:
//! producers `push(bytes)` repeatedly; the sink buffers until
//! `target_part_size`, then schedules an `UploadPart` task on a long-
//! lived `Io.Group`. Concurrent in-flight uploads are bounded by
//! BYTES not part count (Vortex's pattern), via a mutex+condition
//! around `bytes_in_flight`. `close` flushes any remainder, awaits
//! the group, and either calls `CompleteMultipartUpload` or falls
//! back to a single `PutObject` if no part ever flushed.
//!
//! Worker tasks use the existing connection pool from `pool.zig` and
//! the `createMultipart` / `completeMultipart` helpers from `s3.zig`,
//! keeping HTTP/SigV4 details in one place.

const std = @import("std");
const Io = std.Io;
const s3 = @import("s3.zig");
const tls = @import("tls.zig");
const http = @import("http.zig");
const sigv4 = @import("sigv4.zig");
const pool = @import("pool.zig");

pub const POOL_SIZE = 8;
pub const MIN_PART_SIZE: usize = 5 * 1024 * 1024;
pub const TARGET_PART_SIZE: usize = 19 * 1024 * 1024;
pub const MAX_PARTS: u32 = 10_000;
pub const DEFAULT_MAX_BYTES_IN_FLIGHT: usize = 64 * 1024 * 1024;

pub const Error = error{
    PartUploadFailed,
    CreateMultipartFailed,
    CompleteMultipartFailed,
    PartNumberOverflow,
    SinkAlreadyClosed,
    BadResponse,
} || std.mem.Allocator.Error || Io.Cancelable;

pub const Options = struct {
    target_part_size: usize = TARGET_PART_SIZE,
    max_bytes_in_flight: usize = DEFAULT_MAX_BYTES_IN_FLIGHT,
};

const EtagSlot = struct {
    buf: [512]u8 = undefined,
    len: usize = 0,

    fn slice(self: *const EtagSlot) []const u8 {
        return self.buf[0..self.len];
    }
};

const PartTaskCtx = struct {
    sink: *MultipartSink,
    part_number: u32,
    body: []const u8, // owned by the sink's gpa; freed in the worker
    body_len: usize,
    etag_slot: *EtagSlot,
};

pub const MultipartSink = struct {
    // S3 session
    creds: s3.Credentials,
    url: s3.Url,
    pool_ptr: *pool.Pool(POOL_SIZE),
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator, // for ephemeral per-control-message allocs
    io: Io,
    options: Options,

    // Multipart state
    upload_id: ?[]const u8 = null,
    next_part_number: u32 = 1,

    // Producer-side accumulator
    buffer: std.ArrayList(u8) = .empty,

    // Backpressure: byte-aware bound (Vortex pattern)
    bytes_lock: Io.Mutex = .init,
    bytes_cond: Io.Condition = .init,
    bytes_in_flight: usize = 0,

    // Long-lived group: tasks repeatedly added; resources released
    // when each task returns. We `await` once at close to drain.
    group: Io.Group = .init,

    // Per-part etag storage. Heap-allocated so pointers handed to
    // workers are stable across ArrayList growth.
    etags: std.ArrayList(*EtagSlot) = .empty,

    // First worker error wins; checked at close.
    error_lock: Io.Mutex = .init,
    first_error: ?[]const u8 = null,

    closed: bool = false,

    pub fn init(
        io: Io,
        gpa: std.mem.Allocator,
        arena: std.mem.Allocator,
        creds: s3.Credentials,
        url: s3.Url,
        pool_ptr: *pool.Pool(POOL_SIZE),
        options: Options,
    ) MultipartSink {
        return .{
            .creds = creds,
            .url = url,
            .pool_ptr = pool_ptr,
            .gpa = gpa,
            .arena = arena,
            .io = io,
            .options = options,
        };
    }

    /// Append bytes to the producer-side buffer. When it crosses
    /// target_part_size, flush full parts to the multipart upload.
    pub fn push(self: *MultipartSink, bytes: []const u8) Error!void {
        if (self.closed) return error.SinkAlreadyClosed;
        try self.buffer.appendSlice(self.gpa, bytes);
        while (self.buffer.items.len >= self.options.target_part_size) {
            try self.flushOnePart(self.options.target_part_size);
        }
    }

    fn flushOnePart(self: *MultipartSink, n: usize) Error!void {
        if (self.upload_id == null) {
            // Lazy-create on first part. Some outputs are small enough to
            // stay below target_part_size and never flush — they go via
            // single PutObject in close().
            self.upload_id = createMultipartUpload(self.arena, self.creds, self.url) catch
                return error.CreateMultipartFailed;
        }

        if (self.next_part_number > MAX_PARTS) return error.PartNumberOverflow;

        const owned = try self.gpa.dupe(u8, self.buffer.items[0..n]);
        // Compact buffer: shift remainder to front.
        const remainder_len = self.buffer.items.len - n;
        if (remainder_len > 0) {
            std.mem.copyForwards(u8, self.buffer.items[0..remainder_len], self.buffer.items[n..]);
        }
        self.buffer.shrinkRetainingCapacity(remainder_len);

        const part_no = self.next_part_number;
        self.next_part_number += 1;

        const slot = try self.gpa.create(EtagSlot);
        slot.* = .{};
        try self.etags.append(self.gpa, slot);

        // Backpressure: reserve `n` bytes of in-flight budget. The
        // `n > 0 && bytes_in_flight > 0` guard is the pragmatic-
        // degradation case from Vortex — a single large part is
        // allowed to exceed the cap if it's the only one in flight,
        // otherwise a single oversized part would deadlock.
        try self.reserveBytes(n);

        const ctx = try self.gpa.create(PartTaskCtx);
        ctx.* = .{
            .sink = self,
            .part_number = part_no,
            .body = owned,
            .body_len = n,
            .etag_slot = slot,
        };

        try self.group.concurrent(self.io, partWorker, .{ self.io, ctx });
    }

    fn reserveBytes(self: *MultipartSink, n: usize) Error!void {
        try self.bytes_lock.lock(self.io);
        defer self.bytes_lock.unlock(self.io);
        while (self.bytes_in_flight > 0 and self.bytes_in_flight + n > self.options.max_bytes_in_flight) {
            try self.bytes_cond.wait(self.io, &self.bytes_lock);
        }
        self.bytes_in_flight += n;
    }

    fn releaseBytes(self: *MultipartSink, n: usize) void {
        self.bytes_lock.lockUncancelable(self.io);
        defer self.bytes_lock.unlock(self.io);
        self.bytes_in_flight -= n;
        self.bytes_cond.signal(self.io);
    }

    fn recordError(self: *MultipartSink, msg: []const u8) void {
        self.error_lock.lockUncancelable(self.io);
        defer self.error_lock.unlock(self.io);
        if (self.first_error == null) self.first_error = msg;
    }

    /// Flush any remainder, drain in-flight workers, then either
    /// CompleteMultipartUpload or fall back to a single PutObject if
    /// nothing was multiparted.
    pub fn close(self: *MultipartSink) Error!void {
        if (self.closed) return error.SinkAlreadyClosed;
        self.closed = true;

        // Flush whatever's left in the buffer as the last part. The
        // last part is exempt from the 5 MB minimum.
        if (self.buffer.items.len > 0 and self.upload_id != null) {
            try self.flushOnePart(self.buffer.items.len);
        }

        // Drain. Long-lived group awaits all in-flight tasks.
        try self.group.await(self.io);

        if (self.first_error) |msg| {
            // Best-effort abort — we don't expose the abort error
            // separately; the caller already knows there was a
            // failure.
            if (self.upload_id) |id| {
                abortMultipartUpload(self.arena, self.creds, self.url, id) catch {};
            }
            std.log.warn("multipart sink: first worker error: {s}", .{msg});
            return error.PartUploadFailed;
        }

        if (self.upload_id) |id| {
            try completeMultipartUpload(self.arena, self.creds, self.url, id, self.etags.items);
        } else {
            // No part ever flushed → single PutObject for the whole buffer.
            try singlePut(self.arena, self.creds, self.url, self.buffer.items);
        }
    }

    pub fn deinit(self: *MultipartSink) void {
        self.buffer.deinit(self.gpa);
        for (self.etags.items) |slot| self.gpa.destroy(slot);
        self.etags.deinit(self.gpa);
    }
};

/// Worker task. Acquires a connection from the sink's pool, sends
/// the UploadPart, parses the ETag from the response. Releases its
/// in-flight byte reservation on exit (success or failure).
fn partWorker(io: Io, ctx: *PartTaskCtx) Io.Cancelable!void {
    defer ctx.sink.gpa.destroy(ctx);
    defer ctx.sink.gpa.free(ctx.body);
    defer ctx.sink.releaseBytes(ctx.body_len);

    var attempts: u8 = 0;
    while (attempts <= POOL_SIZE) : (attempts += 1) {
        var arena_state = std.heap.ArenaAllocator.init(ctx.sink.gpa);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        const handle = ctx.sink.pool_ptr.acquire(io) catch {
            ctx.sink.recordError("pool_acquire_failed");
            return;
        };

        const ok = doUploadPart(arena, ctx, handle.conn) catch |err| {
            ctx.sink.pool_ptr.discard(io, handle);
            switch (err) {
                error.RecvFailed,
                error.SendFailed,
                error.BodyTruncated,
                error.BadStatusLine,
                => continue, // retry on transient connection errors
                else => {
                    ctx.sink.recordError(@errorName(err));
                    return;
                },
            }
        };
        if (!ok) {
            ctx.sink.pool_ptr.discard(io, handle);
            ctx.sink.recordError("part_upload_bad_status");
            return;
        }
        ctx.sink.pool_ptr.release(io, handle) catch return;
        return;
    }
    ctx.sink.recordError("part_upload_exhausted_retries");
}

fn doUploadPart(arena: std.mem.Allocator, ctx: *PartTaskCtx, conn: *tls.Connection) !bool {
    const url = ctx.sink.url;
    const creds = ctx.sink.creds;
    const upload_id = ctx.sink.upload_id.?;

    const host = try std.fmt.allocPrint(arena, "{s}.s3.{s}.amazonaws.com", .{ url.bucket, creds.region });
    const path = try s3.buildEncodedPath(arena, url.key);
    const query = try std.fmt.allocPrint(arena, "partNumber={d}&uploadId={s}", .{ ctx.part_number, upload_id });
    const path_with_query = try std.fmt.allocPrint(arena, "{s}?{s}", .{ path, query });

    const signer: sigv4.SigV4 = .{
        .region = creds.region,
        .access_key = creds.access_key,
        .secret_key = creds.secret_key,
        .session_token = creds.session_token,
    };
    const signed = try signer.sign(arena, "PUT", host, path, query, &.{}, ctx.body, .{ .use_unsigned_payload = true });

    var headers: std.ArrayList(http.Header) = .empty;
    for (signed) |h| try headers.append(arena, .{ .name = h.name, .value = h.value });

    const resp = try http.sendRequest(arena, conn, .{
        .method = .PUT,
        .host = host,
        .path = path_with_query,
        .headers = headers.items,
        .body = ctx.body,
    });
    if (resp.status != 200) return false;

    var etag = resp.header("ETag") orelse return false;
    if (etag.len >= 2 and etag[0] == '"' and etag[etag.len - 1] == '"') {
        etag = etag[1 .. etag.len - 1];
    }
    if (etag.len > ctx.etag_slot.buf.len) return false;
    @memcpy(ctx.etag_slot.buf[0..etag.len], etag);
    ctx.etag_slot.len = etag.len;
    return true;
}

// ============================================================
// Control-plane operations (one-off connections, not pool-backed)
// ============================================================

fn createMultipartUpload(arena: std.mem.Allocator, creds: s3.Credentials, url: s3.Url) ![]const u8 {
    const host = try std.fmt.allocPrint(arena, "{s}.s3.{s}.amazonaws.com", .{ url.bucket, creds.region });
    const path = try s3.buildEncodedPath(arena, url.key);
    const query = "uploads=";
    const path_with_query = try std.fmt.allocPrint(arena, "{s}?{s}", .{ path, query });

    const addr_v4 = try s3.resolveIpv4(arena, host);
    var conn = try tls.Connection.connect(arena, addr_v4, 443, host);
    defer conn.deinit();

    const signer: sigv4.SigV4 = .{
        .region = creds.region,
        .access_key = creds.access_key,
        .secret_key = creds.secret_key,
        .session_token = creds.session_token,
    };
    const signed = try signer.sign(arena, "POST", host, path, query, &.{}, "", .{});

    var headers: std.ArrayList(http.Header) = .empty;
    for (signed) |h| try headers.append(arena, .{ .name = h.name, .value = h.value });

    const resp = try http.sendRequest(arena, &conn, .{
        .method = .POST,
        .host = host,
        .path = path_with_query,
        .headers = headers.items,
        .body = "",
    });
    if (resp.status != 200) return error.CreateMultipartFailed;

    return try s3.extractXml(arena, resp.body, "UploadId");
}

fn completeMultipartUpload(
    arena: std.mem.Allocator,
    creds: s3.Credentials,
    url: s3.Url,
    upload_id: []const u8,
    etag_slots: []const *EtagSlot,
) !void {
    const host = try std.fmt.allocPrint(arena, "{s}.s3.{s}.amazonaws.com", .{ url.bucket, creds.region });
    const path = try s3.buildEncodedPath(arena, url.key);
    const query = try std.fmt.allocPrint(arena, "uploadId={s}", .{upload_id});
    const path_with_query = try std.fmt.allocPrint(arena, "{s}?{s}", .{ path, query });

    var body: std.ArrayList(u8) = .empty;
    try body.appendSlice(arena, "<CompleteMultipartUpload>");
    for (etag_slots, 1..) |slot, part_no| {
        const piece = try std.fmt.allocPrint(
            arena,
            "<Part><PartNumber>{d}</PartNumber><ETag>{s}</ETag></Part>",
            .{ part_no, slot.slice() },
        );
        try body.appendSlice(arena, piece);
    }
    try body.appendSlice(arena, "</CompleteMultipartUpload>");

    const addr_v4 = try s3.resolveIpv4(arena, host);
    var conn = try tls.Connection.connect(arena, addr_v4, 443, host);
    defer conn.deinit();

    const signer: sigv4.SigV4 = .{
        .region = creds.region,
        .access_key = creds.access_key,
        .secret_key = creds.secret_key,
        .session_token = creds.session_token,
    };
    const signed = try signer.sign(arena, "POST", host, path, query, &.{}, body.items, .{});

    var headers: std.ArrayList(http.Header) = .empty;
    for (signed) |h| try headers.append(arena, .{ .name = h.name, .value = h.value });

    const resp = try http.sendRequest(arena, &conn, .{
        .method = .POST,
        .host = host,
        .path = path_with_query,
        .headers = headers.items,
        .body = body.items,
    });
    if (resp.status != 200) return error.CompleteMultipartFailed;
    if (std.mem.indexOf(u8, resp.body, "<Error>") != null) return error.CompleteMultipartFailed;
}

fn abortMultipartUpload(
    arena: std.mem.Allocator,
    creds: s3.Credentials,
    url: s3.Url,
    upload_id: []const u8,
) !void {
    const host = try std.fmt.allocPrint(arena, "{s}.s3.{s}.amazonaws.com", .{ url.bucket, creds.region });
    const path = try s3.buildEncodedPath(arena, url.key);
    const query = try std.fmt.allocPrint(arena, "uploadId={s}", .{upload_id});
    const path_with_query = try std.fmt.allocPrint(arena, "{s}?{s}", .{ path, query });

    const addr_v4 = try s3.resolveIpv4(arena, host);
    var conn = try tls.Connection.connect(arena, addr_v4, 443, host);
    defer conn.deinit();

    const signer: sigv4.SigV4 = .{
        .region = creds.region,
        .access_key = creds.access_key,
        .secret_key = creds.secret_key,
        .session_token = creds.session_token,
    };
    const signed = try signer.sign(arena, "DELETE", host, path, query, &.{}, "", .{});

    var headers: std.ArrayList(http.Header) = .empty;
    for (signed) |h| try headers.append(arena, .{ .name = h.name, .value = h.value });

    _ = try http.sendRequest(arena, &conn, .{
        .method = .DELETE,
        .host = host,
        .path = path_with_query,
        .headers = headers.items,
        .body = "",
    });
}

fn singlePut(
    arena: std.mem.Allocator,
    creds: s3.Credentials,
    url: s3.Url,
    body: []const u8,
) !void {
    const resp = try s3.put(arena, creds, url, body);
    if (resp.status != 200) return error.PartUploadFailed;
}

// Compile-time reference check: forces type-checking of the public API
// and the worker. Without this, Zig's lazy body-compilation would not
// surface errors in functions that no caller in the tree references yet.
test "multipart_sink: API is well-typed" {
    _ = MultipartSink.init;
    _ = MultipartSink.push;
    _ = MultipartSink.close;
    _ = MultipartSink.deinit;
    _ = partWorker;
    _ = doUploadPart;
    _ = createMultipartUpload;
    _ = completeMultipartUpload;
    _ = abortMultipartUpload;
    _ = singlePut;
}
