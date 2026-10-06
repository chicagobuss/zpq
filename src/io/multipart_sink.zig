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
const clock = @import("../clock.zig");
const nowMonoNs = clock.monoNs;
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

pub const MultipartSink = struct {
    // S3 session
    creds: s3.Credentials,
    url: s3.Url,
    pool: s3.AnyPool,
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
    /// via `AnyPool` so the sink struct itself is not generic.
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
        return .{
            .creds = creds,
            .url = url,
            .pool = s3.AnyPool.of(pool_ptr),
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
        const started_s = clock.realtimeS();
        self.upload_id = createMultipartUpload(self.arena, self.creds, self.url) catch |err| {
            // The upload may exist even though its id never reached us; nothing else could ever abort it.
            if (outcomeUnknown(err, error.CreateMultipartFailed)) abortUploadsStartedSince(self.arena, self.creds, self.url, started_s);
            return error.CreateMultipartFailed;
        };
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
                // A Complete whose answer was lost may have committed the object: accept it only if the key now holds
                // exactly this upload.
                if (!outcomeUnknown(err, error.CompleteMultipartFailed) or
                    !completedObjectMatches(self.arena, self.creds, self.url, self.etags.items))
                {
                    self.abortOpenUpload();
                    return err;
                }
                std.log.warn("multipart sink: CompleteMultipartUpload answer lost ({s}); the object carries this upload's ETag", .{@errorName(err)});
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

    var arena_state = std.heap.ArenaAllocator.init(ctx.sink.gpa);
    defer arena_state.deinit();
    // UploadPart replaces the whole part, so it is idempotent: a dead pooled connection retries at once; connect
    // failures, throttling (429/5xx) and the rest back off.
    const resp = s3.requestViaPool(io, ctx.sink.pool, arena_state.allocator(), ctx.sink.criteria, .{ .idempotent = true }, ctx, doUploadPart) catch |err| {
        if (err == error.Canceled) return error.Canceled;
        ctx.sink.recordError(@errorName(err));
        return;
    };
    if (!storePartEtag(ctx, resp)) ctx.sink.recordError("part_upload_bad_status");
}

/// Record the part's ETag from a successful UploadPart response; false for any other answer.
fn storePartEtag(ctx: *PartTaskCtx, resp: http.Response) bool {
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

fn doUploadPart(ctx: *PartTaskCtx, arena: std.mem.Allocator, conn: *tls.Connection) !http.Response {
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

    return http.sendRequest(arena, conn, .{
        .method = .PUT,
        .host = host,
        .path = path_with_query,
        .headers = headers.items,
        .body = ctx.body,
    });
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
    const addrs = try s3.resolveIpv4(arena, connect_host);
    var conn = try s3.connect(arena, creds, addrs, host);
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
    const addrs = try s3.resolveIpv4(arena, connect_host);
    var conn = try s3.connect(arena, creds, addrs, host);
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

/// Whether a control-plane request that failed with `err` may still have taken effect on the server. `definite` is
/// the error for an answer that said no; failing to connect means nothing was sent.
fn outcomeUnknown(err: anyerror, definite: anyerror) bool {
    if (err == definite) return false;
    return switch (err) {
        error.DnsFailed,
        error.SocketFailed,
        error.ConnectFailed,
        error.HandshakeFailed,
        error.CertificateRejected,
        => false,
        else => true,
    };
}

/// Best effort, after a CreateMultipartUpload whose answer was lost: abort the in-progress uploads of exactly this key
/// that were initiated no earlier than `since_s` (wall-clock seconds when the Create was sent), which are the ones it
/// could have started. Older uploads, and uploads of keys this one is only a prefix of, are never touched. An upload
/// another writer starts on the same key in the same window cannot be told apart from ours; a bucket lifecycle rule
/// that aborts incomplete multipart uploads is the backstop for anything this misses.
fn abortUploadsStartedSince(arena: std.mem.Allocator, creds: s3.Credentials, url: s3.Url, since_s: i64) void {
    const uploads = listMultipartUploads(arena, creds, url) catch |err| {
        std.log.warn("multipart sink: could not list uploads to clean up after a lost Create: {s}", .{@errorName(err)});
        return;
    };
    for (uploads) |u| {
        if (!std.mem.eql(u8, u.key, url.key)) continue;
        const initiated = parseIso8601Seconds(u.initiated) orelse continue;
        if (initiated < since_s) continue;
        abortMultipartUpload(arena, creds, url, u.upload_id) catch |err| {
            std.log.warn("multipart sink: abort of orphaned upload failed: {s}", .{@errorName(err)});
            continue;
        };
        std.log.warn("multipart sink: aborted an upload orphaned by a lost CreateMultipartUpload", .{});
    }
}

const ListedUpload = struct { key: []const u8, upload_id: []const u8, initiated: []const u8 };

/// ListMultipartUploads with `prefix` = the key; the first page only (up to 1000 uploads under that prefix).
fn listMultipartUploads(arena: std.mem.Allocator, creds: s3.Credentials, url: s3.Url) ![]const ListedUpload {
    const host = try s3.hostFor(arena, creds, url.bucket);
    const list_path: []const u8 = if (s3.virtualHosted(creds, url.bucket))
        "/"
    else
        try std.fmt.allocPrint(arena, "/{s}", .{url.bucket});
    const query = try std.fmt.allocPrint(arena, "prefix={s}&uploads=", .{try uriEncodeQueryValue(arena, url.key)});
    const path_with_query = try std.fmt.allocPrint(arena, "{s}?{s}", .{ list_path, query });

    const connect_host = try s3.connectHostFor(arena, creds, url.bucket);
    const addrs = try s3.resolveIpv4(arena, connect_host);
    var conn = try s3.connect(arena, creds, addrs, host);
    defer conn.deinit();

    const signer: sigv4.SigV4 = .{
        .region = creds.region,
        .access_key = creds.access_key,
        .secret_key = creds.secret_key,
        .session_token = creds.session_token,
    };
    const signed = try signer.sign(arena, "GET", host, list_path, query, &.{}, "", .{ .use_unsigned_payload = true });
    var headers: std.ArrayList(http.Header) = .empty;
    for (signed) |h| try headers.append(arena, .{ .name = h.name, .value = h.value });

    const resp = try http.sendRequest(arena, &conn, .{
        .method = .GET,
        .host = host,
        .path = path_with_query,
        .headers = headers.items,
    });
    if (resp.status != 200) return error.BadResponse;
    return parseListedUploads(arena, resp.body);
}

fn parseListedUploads(arena: std.mem.Allocator, xml: []const u8) ![]const ListedUpload {
    var out: std.ArrayList(ListedUpload) = .empty;
    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, xml, pos, "<Upload>")) |start| {
        const end = std.mem.indexOfPos(u8, xml, start, "</Upload>") orelse break;
        const block = xml[start..end];
        pos = end;
        const key = xmlText(block, "Key") orelse continue;
        const id = xmlText(block, "UploadId") orelse continue;
        const initiated = xmlText(block, "Initiated") orelse continue;
        try out.append(arena, .{ .key = try xmlUnescape(arena, key), .upload_id = try xmlUnescape(arena, id), .initiated = initiated });
    }
    return out.items;
}

fn xmlText(block: []const u8, comptime tag: []const u8) ?[]const u8 {
    const open = "<" ++ tag ++ ">";
    const start = (std.mem.indexOf(u8, block, open) orelse return null) + open.len;
    const end = std.mem.indexOfPos(u8, block, start, "</" ++ tag ++ ">") orelse return null;
    return block[start..end];
}

fn xmlUnescape(arena: std.mem.Allocator, s: []const u8) ![]const u8 {
    if (std.mem.indexOfScalar(u8, s, '&') == null) return s;
    const entities = [_]struct { []const u8, u8 }{ .{ "&amp;", '&' }, .{ "&lt;", '<' }, .{ "&gt;", '>' }, .{ "&quot;", '"' }, .{ "&apos;", '\'' } };
    var out: std.ArrayList(u8) = .empty;
    var i: usize = 0;
    outer: while (i < s.len) {
        if (s[i] == '&') for (entities) |e| if (std.mem.startsWith(u8, s[i..], e[0])) {
            try out.append(arena, e[1]);
            i += e[0].len;
            continue :outer;
        };
        try out.append(arena, s[i]);
        i += 1;
    }
    return out.items;
}

/// Seconds since the epoch of an S3 timestamp such as `2026-10-03T22:21:03.000Z` (fraction ignored); null when it
/// does not parse.
fn parseIso8601Seconds(s: []const u8) ?i64 {
    if (s.len < 20 or s[4] != '-' or s[7] != '-' or s[10] != 'T' or s[13] != ':' or s[16] != ':') return null;
    if (s[s.len - 1] != 'Z') return null;
    const num = struct {
        fn f(t: []const u8) ?i64 {
            return std.fmt.parseInt(i64, t, 10) catch null;
        }
    }.f;
    const y = num(s[0..4]) orelse return null;
    const m = num(s[5..7]) orelse return null;
    const d = num(s[8..10]) orelse return null;
    const hh = num(s[11..13]) orelse return null;
    const mm = num(s[14..16]) orelse return null;
    const ss = num(s[17..19]) orelse return null;
    if (m < 1 or m > 12 or d < 1 or d > 31) return null;
    // Days from 1970-01-01 to y-m-d in the proleptic Gregorian calendar (Howard Hinnant's days_from_civil).
    const yy = if (m <= 2) y - 1 else y;
    const era = @divFloor(yy, 400);
    const yoe = yy - era * 400;
    const mp = @mod(m + 9, 12);
    const doy = @divFloor(153 * mp + 2, 5) + d - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    const days = era * 146097 + doe - 719468;
    return days * 86400 + hh * 3600 + mm * 60 + ss;
}

/// The ETag S3 gives an object completed from `parts`: the MD5 of the parts' binary MD5s, then `-` and the part count.
/// Null when a part ETag is not a plain MD5 (SSE-KMS, for one), since then the object's ETag cannot be predicted.
fn multipartEtag(arena: std.mem.Allocator, parts: []const *EtagSlot) !?[]const u8 {
    const Md5 = std.crypto.hash.Md5;
    var h = Md5.init(.{});
    for (parts) |slot| {
        const hex = slot.slice();
        if (hex.len != 2 * Md5.digest_length) return null;
        var md5: [Md5.digest_length]u8 = undefined;
        _ = std.fmt.hexToBytes(&md5, hex) catch return null;
        h.update(&md5);
    }
    var digest: [Md5.digest_length]u8 = undefined;
    h.final(&digest);
    return try std.fmt.allocPrint(arena, "{x}-{d}", .{ &digest, parts.len });
}

/// After a CompleteMultipartUpload whose answer was lost: whether `url` now holds the object those parts complete to,
/// by HEAD and ETag. Any doubt (a request failing, an unpredictable ETag) answers no.
fn completedObjectMatches(arena: std.mem.Allocator, creds: s3.Credentials, url: s3.Url, parts: []const *EtagSlot) bool {
    const want = (multipartEtag(arena, parts) catch return false) orelse return false;
    const resp = headObject(arena, creds, url) catch return false;
    if (resp.status != 200) return false;
    var got = resp.header("ETag") orelse return false;
    if (got.len >= 2 and got[0] == '"' and got[got.len - 1] == '"') got = got[1 .. got.len - 1];
    return std.ascii.eqlIgnoreCase(got, want);
}

fn headObject(arena: std.mem.Allocator, creds: s3.Credentials, url: s3.Url) !http.Response {
    const host = try s3.hostFor(arena, creds, url.bucket);
    const path = try s3.pathFor(arena, creds, url.bucket, url.key);
    const connect_host = try s3.connectHostFor(arena, creds, url.bucket);
    const addrs = try s3.resolveIpv4(arena, connect_host);
    var conn = try s3.connect(arena, creds, addrs, host);
    defer conn.deinit();

    const signer: sigv4.SigV4 = .{
        .region = creds.region,
        .access_key = creds.access_key,
        .secret_key = creds.secret_key,
        .session_token = creds.session_token,
    };
    const signed = try signer.sign(arena, "HEAD", host, path, null, &.{}, "", .{ .use_unsigned_payload = true });
    var headers: std.ArrayList(http.Header) = .empty;
    for (signed) |h| try headers.append(arena, .{ .name = h.name, .value = h.value });
    return http.sendRequest(arena, &conn, .{ .method = .HEAD, .host = host, .path = path, .headers = headers.items });
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
    const addrs = try s3.resolveIpv4(arena, connect_host);
    var conn = try s3.connect(arena, creds, addrs, host);
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

const testing = std.testing;
const fake_s3 = @import("fake_s3.zig");

/// Push `payload` through a sink with 1 KiB parts to `fake`'s bucket `bkt`, then close it.
fn writeThroughSink(fake: *fake_s3.FakeS3, key: []const u8, payload: []const u8) !void {
    var threaded = std.Io.Threaded.init(testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var endpoint_buf: [64]u8 = undefined;
    const creds: s3.Credentials = .{
        .access_key = "ak",
        .secret_key = "sk",
        .region = "us-east-1",
        .endpoint = try std.fmt.bufPrint(&endpoint_buf, "http://127.0.0.1:{d}", .{fake.port}),
    };
    const url: s3.Url = .{ .bucket = "bkt", .key = key };
    var pool: s3.Pool(4) = undefined;
    try pool.init(testing.allocator);
    defer pool.deinit();
    const criteria = try s3.poolCriteria(arena, creds, url.bucket);

    var sink = MultipartSink.init(io, testing.allocator, arena, creds, url, &pool, criteria, .{ .target_part_size = 1024 });
    defer {
        if (!sink.isClosed()) sink.abort();
        sink.deinit();
    }
    try sink.push(payload);
    try sink.close();
}

const test_payload: [2500]u8 = blk: {
    @setEvalBranchQuota(10_000);
    var b: [2500]u8 = undefined;
    for (&b, 0..) |*c, i| c.* = @truncate(i *% 31 +% 7);
    break :blk b;
};

test "a CreateMultipartUpload whose response is lost leaves no upload of ours behind" {
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(testing.allocator);
    defer fake.deinit();
    // Not ours: another writer's upload of the same key from an hour ago, and a newer one of a key it is a prefix of.
    try fake.addUpload("out.parquet", 3600);
    try fake.addUpload("out.parquet.bak", 0);
    fake.drop = "POST /bkt/out.parquet?uploads";

    try testing.expectError(error.CreateMultipartFailed, writeThroughSink(&fake, "out.parquet", &test_payload));
    try testing.expectEqual(@as(u32, 1), fake.dropped);
    try testing.expectEqual(@as(usize, 1), fake.openUploadsFor("out.parquet"));
    try testing.expectEqual(@as(usize, 1), fake.openUploadsFor("out.parquet.bak"));
}

test "a CompleteMultipartUpload whose response is lost succeeds when the object carries the upload's ETag" {
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(testing.allocator);
    defer fake.deinit();
    fake.drop = "POST /bkt/out.parquet?uploadId";

    try writeThroughSink(&fake, "out.parquet", &test_payload);
    try testing.expectEqual(@as(u32, 1), fake.dropped);
    try testing.expect(fake.object("out.parquet") != null);
    try testing.expectEqual(@as(usize, 0), fake.openUploadsFor("out.parquet"));
}

test "a lost CompleteMultipartUpload that did not take effect still fails and aborts the upload" {
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(testing.allocator);
    defer fake.deinit();
    fake.drop = "POST /bkt/out.parquet?uploadId";
    fake.apply_dropped = false;

    try testing.expectError(error.ClosedBeforeResponse, writeThroughSink(&fake, "out.parquet", &test_payload));
    try testing.expect(fake.object("out.parquet") == null);
    try testing.expectEqual(@as(usize, 0), fake.openUploadsFor("out.parquet"));
}

test "parseIso8601Seconds reads S3 timestamps with and without a fraction" {
    try testing.expectEqual(@as(?i64, 0), parseIso8601Seconds("1970-01-01T00:00:00Z"));
    try testing.expectEqual(@as(?i64, 1_791_066_063), parseIso8601Seconds("2026-10-03T22:21:03.000Z"));
    try testing.expectEqual(@as(?i64, 951_782_400), parseIso8601Seconds("2000-02-29T00:00:00.5Z"));
    try testing.expectEqual(@as(?i64, null), parseIso8601Seconds("2026-10-03 22:21:03"));
    try testing.expectEqual(@as(?i64, null), parseIso8601Seconds("2026-13-03T22:21:03Z"));
}

test "parseListedUploads decodes keys and keeps every upload's id and start time" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const xml = "<ListMultipartUploadsResult><Bucket>b</Bucket>" ++
        "<Upload><Key>a&amp;b.parquet</Key><UploadId>u1</UploadId><Initiated>2026-10-03T22:21:03.000Z</Initiated></Upload>" ++
        "<Upload><Key>a&amp;b.parquet.bak</Key><UploadId>u2</UploadId><Initiated>2026-10-03T22:21:04.000Z</Initiated></Upload>" ++
        "</ListMultipartUploadsResult>";
    const ups = try parseListedUploads(arena.allocator(), xml);
    try testing.expectEqual(@as(usize, 2), ups.len);
    try testing.expectEqualStrings("a&b.parquet", ups[0].key);
    try testing.expectEqualStrings("u2", ups[1].upload_id);
    try testing.expectEqualStrings("2026-10-03T22:21:04.000Z", ups[1].initiated);
}

test "multipartEtag is the MD5 of the part MD5s and the part count, and refuses ETags that are not MD5s" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const Md5 = std.crypto.hash.Md5;
    var a: EtagSlot = .{};
    var b: EtagSlot = .{};
    var ma: [16]u8 = undefined;
    var mb: [16]u8 = undefined;
    Md5.hash("part one", &ma, .{});
    Md5.hash("part two", &mb, .{});
    a.len = (try std.fmt.bufPrint(&a.buf, "{x}", .{&ma})).len;
    b.len = (try std.fmt.bufPrint(&b.buf, "{x}", .{&mb})).len;
    var cat: [32]u8 = undefined;
    @memcpy(cat[0..16], &ma);
    @memcpy(cat[16..], &mb);
    var want: [16]u8 = undefined;
    Md5.hash(&cat, &want, .{});
    const got = (try multipartEtag(arena.allocator(), &.{ &a, &b })).?;
    try testing.expectEqualStrings(try std.fmt.allocPrint(arena.allocator(), "{x}-2", .{&want}), got);

    var kms: EtagSlot = .{};
    kms.len = (try std.fmt.bufPrint(&kms.buf, "not-an-md5", .{})).len;
    try testing.expectEqual(@as(?[]const u8, null), try multipartEtag(arena.allocator(), &.{ &a, &kms }));
}
