//! Streaming S3 multipart upload sink.
//!
//! Producers `push(bytes)` repeatedly; the sink tops up any partial
//! buffer, then schedules full `UploadPart` tasks on a long-lived
//! `Io.Group`. Concurrent in-flight uploads are bounded by
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
const retry = @import("retry.zig");

/// Percent-encode an S3 query-parameter value (RFC 3986: keep the unreserved
/// set A-Za-z0-9-._~, escape everything else as %XX). `sigv4.zig` signs the
/// query string as-is (assumes it's already encoded), and S3 multipart upload
/// IDs routinely contain '+', '/', '=' — so the raw value must be encoded
/// before it goes into the query or both the SigV4 signature and S3's own
/// query parse break. Matches the canonical-query encoding SigV4 expects.
fn uriEncodeQueryValue(arena: std.mem.Allocator, s: []const u8) ![]u8 {
    const hex = "0123456789ABCDEF";
    var out: std.ArrayList(u8) = .empty;
    for (s) |c| {
        if (std.ascii.isAlphanumeric(c) or c == '-' or c == '.' or c == '_' or c == '~') {
            try out.append(arena, c);
        } else {
            try out.appendSlice(arena, &.{ '%', hex[c >> 4], hex[c & 0xf] });
        }
    }
    return out.toOwnedSlice(arena);
}

inline fn nowMonoNs() i64 {
    var ts: std.os.linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, ts.sec) * std.time.ns_per_s + @as(i64, ts.nsec);
}

pub const RETRY_LIMIT = 8;
pub const MIN_PART_SIZE: usize = 5 * 1024 * 1024;
/// Smaller parts with high concurrency. The narrow per-part wall
/// time (8 MB / ~100 MB-per-second effective ≈ 80 ms) lets many
/// parts overlap, and total upload wall is bounded by the slowest
/// part rather than the sum. For 65 MB output: 8-9 parts at ~80 ms
/// each running concurrently lands ~80 ms wall + Complete RT
/// instead of 4 × ~250 ms = 1000 ms serialized via backpressure.
pub const TARGET_PART_SIZE: usize = 8 * 1024 * 1024;
pub const MAX_PARTS: u32 = 10_000;
/// In-flight byte budget. Honest bound: real upload concurrency
/// is capped by the S3 pool's permit count (`s3.MAX_PARTS = 8`),
/// not by this number. Sized to let the producer stage one part
/// ahead per worker — `pool_permits × 2 × target_part_size` —
/// so workers are never idle waiting for buffer fill, but memory
/// doesn't grow without bound. With 8 permits + 8 MB parts that
/// lands at 128 MB. Anything larger just keeps already-encoded
/// part bodies sitting in memory, since there are still only
/// 8 sockets actively uploading.
pub const DEFAULT_MAX_BYTES_IN_FLIGHT: usize = 128 * 1024 * 1024;

pub const Error = error{
    PartUploadFailed,
    CreateMultipartFailed,
    CompleteMultipartFailed,
    PartNumberOverflow,
    SinkAlreadyClosed,
    BadResponse,
} || std.mem.Allocator.Error || Io.Cancelable || Io.ConcurrentError;

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

/// Type-erased pool dispatch. Mirrors the existing `s3.PoolHandle`
/// pattern in s3.zig so callers can hand us any `*Pool(N)` regardless
/// of the comptime size.
const PoolDispatch = struct {
    ptr: *anyopaque,
    acquireFn: *const fn (*anyopaque, Io, s3.PoolCriteria) anyerror!s3.PoolHandle,
    releaseFn: *const fn (*anyopaque, Io, s3.PoolHandle) void,
    discardFn: *const fn (*anyopaque, Io, s3.PoolHandle) void,
};

fn acquireFnFor(comptime P: type) *const fn (*anyopaque, Io, s3.PoolCriteria) anyerror!s3.PoolHandle {
    return struct {
        fn f(p: *anyopaque, io: Io, criteria: s3.PoolCriteria) anyerror!s3.PoolHandle {
            const typed: *P = @ptrCast(@alignCast(p));
            const h = try typed.acquire(io, criteria);
            return .{ .conn = h.conn, .node = @ptrCast(h.node), .permit = h.permit };
        }
    }.f;
}

fn releaseFnFor(comptime P: type) *const fn (*anyopaque, Io, s3.PoolHandle) void {
    return struct {
        fn f(p: *anyopaque, io: Io, h: s3.PoolHandle) void {
            const typed: *P = @ptrCast(@alignCast(p));
            typed.release(io, .{
                .conn = h.conn,
                .node = @ptrCast(@alignCast(h.node)),
                .permit = h.permit,
            });
        }
    }.f;
}

fn discardFnFor(comptime P: type) *const fn (*anyopaque, Io, s3.PoolHandle) void {
    return struct {
        fn f(p: *anyopaque, io: Io, h: s3.PoolHandle) void {
            const typed: *P = @ptrCast(@alignCast(p));
            typed.discard(io, .{
                .conn = h.conn,
                .node = @ptrCast(@alignCast(h.node)),
                .permit = h.permit,
            });
        }
    }.f;
}

pub const MultipartSink = struct {
    // S3 session
    creds: s3.Credentials,
    url: s3.Url,
    pool: PoolDispatch,
    criteria: s3.PoolCriteria,
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

    // Telemetry — populated as the upload progresses, surfaced via
    // `snapshotStats()`. `complete_ns` is the CompleteMultipartUpload
    // round-trip; `await_ns` is how long close() waits for in-flight
    // part workers to drain.
    complete_ns: u64 = 0,
    await_ns: u64 = 0,

    /// `pool_ptr` is `*Pool(N)` for any comptime N — type-erased here
    /// via `PoolDispatch` so the sink struct itself is not generic.
    pub fn init(
        io: Io,
        gpa: std.mem.Allocator,
        arena: std.mem.Allocator,
        creds: s3.Credentials,
        url: s3.Url,
        pool_ptr: anytype,
        criteria: s3.PoolCriteria,
        options: Options,
    ) MultipartSink {
        const P = @TypeOf(pool_ptr.*);
        return .{
            .creds = creds,
            .url = url,
            .pool = .{
                .ptr = @ptrCast(pool_ptr),
                .acquireFn = acquireFnFor(P),
                .releaseFn = releaseFnFor(P),
                .discardFn = discardFnFor(P),
            },
            .criteria = criteria,
            .gpa = gpa,
            .arena = arena,
            .io = io,
            .options = options,
        };
    }

    /// Route incoming bytes to the multipart upload with at most one
    /// memcpy of any byte. Three stages:
    ///   1. If the producer buffer is partially full, top it up to a
    ///      complete part and dispatch it via `flushBufferAsPart`
    ///      (zero-copy steal of the buffer).
    ///   2. While the remaining input is at least one part, schedule
    ///      target-sized slices directly from the caller's slice via
    ///      `flushSliceAsPart` (one gpa.dupe per part).
    ///   3. Stash the final sub-target fragment in the buffer.
    ///
    /// Step 2 must not copy the still-unflushed remainder back into the
    /// staging buffer on every part boundary; that turns a single large
    /// encoder push quadratic. This shape copies each byte once: `dupe`
    /// for full parts plus `appendSlice` for the small tail.
    pub fn push(self: *MultipartSink, bytes: []const u8) !void {
        if (self.closed) return error.SinkAlreadyClosed;
        try self.failIfWorkerError();
        var input = bytes;

        // 1. Top up the producer buffer to a full part if we have
        //    enough new bytes to do so. Otherwise just append and
        //    return.
        if (self.buffer.items.len > 0 and
            self.buffer.items.len + input.len >= self.options.target_part_size)
        {
            const need = self.options.target_part_size - self.buffer.items.len;
            try self.buffer.appendSlice(self.gpa, input[0..need]);
            input = input[need..];
            try self.flushBufferAsPart(self.options.target_part_size);
        }

        // 2. Carve off full target-sized slices directly from input.
        //    One `dupe` per part — no remainder shuffling.
        while (input.len >= self.options.target_part_size) {
            const part = input[0..self.options.target_part_size];
            try self.flushSliceAsPart(part);
            input = input[self.options.target_part_size..];
        }

        // 3. Final fragment goes into the producer buffer for the
        //    next push (or close()).
        if (input.len > 0) {
            try self.buffer.appendSlice(self.gpa, input);
        }
    }

    /// Send the producer buffer's first `n` bytes as one multipart
    /// part. Steals the existing buffer (toOwnedSlice) — zero extra
    /// copy. Used when the buffer has been topped up to exactly a
    /// full part and at close() for the trailing fragment.
    fn flushBufferAsPart(self: *MultipartSink, n: usize) !void {
        std.debug.assert(self.buffer.items.len == n);
        try self.ensureUploadId();

        var stolen = self.buffer;
        self.buffer = .empty;
        const owned = stolen.toOwnedSlice(self.gpa) catch |err| {
            self.buffer = stolen;
            return err;
        };
        try self.dispatchPart(owned, n);
    }

    /// Send a slice of caller-owned bytes as one multipart part.
    /// Allocates a worker-owned copy (one `dupe`) since the worker
    /// outlives this call.
    fn flushSliceAsPart(self: *MultipartSink, slice: []const u8) !void {
        try self.ensureUploadId();

        const owned = try self.gpa.dupe(u8, slice);
        try self.dispatchPart(owned, slice.len);
    }

    fn ensureUploadId(self: *MultipartSink) !void {
        if (self.upload_id != null) return;
        // Lazy-create on first part. Some outputs are small enough to
        // stay below target_part_size and never flush — they go via
        // single PutObject in close().
        self.upload_id = createMultipartUpload(self.arena, self.creds, self.url) catch
            return error.CreateMultipartFailed;
    }

    /// Common machinery: register the etag slot, reserve the byte
    /// budget, spawn the worker.
    fn dispatchPart(self: *MultipartSink, owned: []u8, n: usize) !void {
        errdefer self.gpa.free(owned);
        try self.failIfWorkerError();
        if (self.next_part_number > MAX_PARTS) return error.PartNumberOverflow;

        const part_no = self.next_part_number;

        const slot = try self.gpa.create(EtagSlot);
        errdefer self.gpa.destroy(slot);
        slot.* = .{};
        try self.etags.append(self.gpa, slot);
        errdefer _ = self.etags.pop();

        // Backpressure: reserve `n` bytes of in-flight budget. The
        // `n > 0 && bytes_in_flight > 0` guard is the pragmatic-
        // degradation case from Vortex — a single large part is
        // allowed to exceed the cap if it's the only one in flight,
        // otherwise a single oversized part would deadlock.
        try self.reserveBytes(n);
        errdefer self.releaseBytes(n);

        const ctx = try self.gpa.create(PartTaskCtx);
        errdefer self.gpa.destroy(ctx);
        ctx.* = .{
            .sink = self,
            .part_number = part_no,
            .body = owned,
            .body_len = n,
            .etag_slot = slot,
        };

        try self.group.concurrent(self.io, partWorker, .{ self.io, ctx });
        self.next_part_number += 1;
    }

    fn reserveBytes(self: *MultipartSink, n: usize) !void {
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

    fn failIfWorkerError(self: *MultipartSink) !void {
        self.error_lock.lockUncancelable(self.io);
        defer self.error_lock.unlock(self.io);
        if (self.first_error != null) return error.PartUploadFailed;
    }

    fn abortOpenUpload(self: *MultipartSink) void {
        if (self.upload_id) |id| {
            abortMultipartUpload(self.arena, self.creds, self.url, id) catch {};
            self.upload_id = null;
        }
    }

    /// Flush any remainder, drain in-flight workers, then either
    /// CompleteMultipartUpload or fall back to a single PutObject if
    /// nothing was multiparted.
    pub fn close(self: *MultipartSink) !void {
        if (self.closed) return error.SinkAlreadyClosed;
        self.closed = true;

        // Flush whatever's left in the buffer as the last part. The
        // last part is exempt from the 5 MB minimum.
        if (self.buffer.items.len > 0 and self.upload_id != null) {
            self.flushBufferAsPart(self.buffer.items.len) catch |err| {
                self.group.cancel(self.io);
                self.abortOpenUpload();
                return err;
            };
        }

        // Drain. Long-lived group awaits all in-flight tasks.
        const t_await_start = nowMonoNs();
        self.group.await(self.io) catch |err| {
            self.abortOpenUpload();
            return err;
        };
        self.await_ns +%= @intCast(nowMonoNs() - t_await_start);

        if (self.first_error) |msg| {
            // Best-effort abort — we don't expose the abort error
            // separately; the caller already knows there was a
            // failure.
            self.abortOpenUpload();
            std.log.warn("multipart sink: first worker error: {s}", .{msg});
            return error.PartUploadFailed;
        }

        const t_complete_start = nowMonoNs();
        if (self.upload_id) |id| {
            completeMultipartUpload(self.arena, self.creds, self.url, id, self.etags.items) catch |err| {
                self.abortOpenUpload();
                return err;
            };
            self.upload_id = null;
        } else {
            // No part ever flushed → single PutObject for the whole buffer.
            try singlePut(self.arena, self.creds, self.url, self.buffer.items);
        }
        self.complete_ns +%= @intCast(nowMonoNs() - t_complete_start);
    }

    /// Best-effort cleanup for callers that hit an error after multipart
    /// workers have been scheduled but before `close()` can commit.
    pub fn abort(self: *MultipartSink) void {
        if (self.closed) return;
        self.closed = true;
        self.group.cancel(self.io);
        self.abortOpenUpload();
    }

    pub fn isClosed(self: *const MultipartSink) bool {
        return self.closed;
    }

    pub const Stats = struct {
        complete_ns: u64 = 0,
        await_ns: u64 = 0,
    };

    pub fn snapshotStats(self: *const MultipartSink) Stats {
        return .{ .complete_ns = self.complete_ns, .await_ns = self.await_ns };
    }

    pub fn deinit(self: *MultipartSink) void {
        self.buffer.deinit(self.gpa);
        for (self.etags.items) |slot| self.gpa.destroy(slot);
        self.etags.deinit(self.gpa);
    }
};

/// Adapter matching `core.writer.streaming.Sink.write_fn`'s signature.
/// Defined here to keep `core/` from depending on `io/`: callers in
/// `lambda/` or `cli/` build the bridge as
///   `streaming.Sink{ .ctx = &mp_sink, .write_fn = sinkWriteFn }`.
pub fn sinkWriteFn(ctx: *anyopaque, bytes: []const u8) anyerror!void {
    const self: *MultipartSink = @ptrCast(@alignCast(ctx));
    return self.push(bytes);
}

/// Worker task. Acquires a connection from the sink's pool, sends
/// the UploadPart, parses the ETag from the response. Releases its
/// in-flight byte reservation on exit (success or failure).
fn partWorker(io: Io, ctx: *PartTaskCtx) Io.Cancelable!void {
    defer ctx.sink.gpa.destroy(ctx);
    defer ctx.sink.gpa.free(ctx.body);
    defer ctx.sink.releaseBytes(ctx.body_len);

    var attempts: u8 = 0;
    while (attempts <= RETRY_LIMIT) : (attempts += 1) {
        if (attempts > 0) try retry.sleepBackoff(io, retry.default_policy, attempts - 1);
        var arena_state = std.heap.ArenaAllocator.init(ctx.sink.gpa);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        const dispatch = ctx.sink.pool;
        const handle = dispatch.acquireFn(dispatch.ptr, io, ctx.sink.criteria) catch {
            ctx.sink.recordError("pool_acquire_failed");
            return;
        };

        const ok = doUploadPart(arena, ctx, handle.conn) catch |err| {
            dispatch.discardFn(dispatch.ptr, io, handle);
            switch (err) {
                error.RecvFailed,
                error.SendFailed,
                error.BodyTruncated,
                error.BadStatusLine,
                error.RetryableStatus, // throttle (429/5xx) — paced by sleepBackoff above
                => continue,
                else => {
                    ctx.sink.recordError(@errorName(err));
                    return;
                },
            }
        };
        if (!ok) {
            dispatch.discardFn(dispatch.ptr, io, handle);
            ctx.sink.recordError("part_upload_bad_status");
            return;
        }
        dispatch.releaseFn(dispatch.ptr, io, handle);
        return;
    }
    ctx.sink.recordError("part_upload_exhausted_retries");
}

fn doUploadPart(arena: std.mem.Allocator, ctx: *PartTaskCtx, conn: *tls.Connection) !bool {
    const url = ctx.sink.url;
    const creds = ctx.sink.creds;
    const upload_id = ctx.sink.upload_id.?;

    const host = try s3.hostFor(arena, creds, url.bucket);
    const path = try s3.pathFor(arena, creds, url.bucket, url.key);
    const query = try std.fmt.allocPrint(arena, "partNumber={d}&uploadId={s}", .{ ctx.part_number, try uriEncodeQueryValue(arena, upload_id) });
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
    if (retry.retryableStatus(resp.status)) return error.RetryableStatus;
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
    const host = try s3.hostFor(arena, creds, url.bucket);
    const path = try s3.pathFor(arena, creds, url.bucket, url.key);
    const query = "uploads=";
    const path_with_query = try std.fmt.allocPrint(arena, "{s}?{s}", .{ path, query });

    const connect_host = try s3.connectHostFor(arena, creds, url.bucket);
    const addr_v4 = try s3.resolveIpv4(arena, connect_host);
    var conn = try s3.connect(arena, creds, addr_v4, host);
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
    const host = try s3.hostFor(arena, creds, url.bucket);
    const path = try s3.pathFor(arena, creds, url.bucket, url.key);
    const query = try std.fmt.allocPrint(arena, "uploadId={s}", .{try uriEncodeQueryValue(arena, upload_id)});
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

    const connect_host = try s3.connectHostFor(arena, creds, url.bucket);
    const addr_v4 = try s3.resolveIpv4(arena, connect_host);
    var conn = try s3.connect(arena, creds, addr_v4, host);
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
    const host = try s3.hostFor(arena, creds, url.bucket);
    const path = try s3.pathFor(arena, creds, url.bucket, url.key);
    const query = try std.fmt.allocPrint(arena, "uploadId={s}", .{try uriEncodeQueryValue(arena, upload_id)});
    const path_with_query = try std.fmt.allocPrint(arena, "{s}?{s}", .{ path, query });

    const connect_host = try s3.connectHostFor(arena, creds, url.bucket);
    const addr_v4 = try s3.resolveIpv4(arena, connect_host);
    var conn = try s3.connect(arena, creds, addr_v4, host);
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
