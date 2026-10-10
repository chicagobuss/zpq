//! The JSON request handler the Lambda and libzpq share: a request (inputs, filter, aggregate or output, ...) in, a
//! JSON response out, over a `PersistentPool` of warm S3 connections and footers that outlives each request.

const std = @import("std");
const zpq = @import("zpq");
const nowMonoNs = zpq.clock.monoNs;
const request = @import("request.zig");
const Diag = zpq.core.diag.Diag;
const engine = zpq.engine;
const schema = zpq.core.schema;
const s3 = zpq.io.s3;

pub const POOL_SIZE: usize = engine.POOL_SIZE;

/// Persistent state that lives for the lifetime of one Lambda
/// container (across invocations): connection pool + metadata cache.
/// `s3.Pool` keeps a bounded request permit queue plus a host-keyed
/// idle LRU, so changing input/output buckets no longer tears down
/// the whole warm pool. Output uploads take permits of their own over
/// the same connections (`engine.OutputLane`). `meta_cache` caches
/// parquet footers keyed by (bucket, key) and revalidates with
/// `If-None-Match`.
pub const PersistentPool = struct {
    const Inner = s3.Pool(POOL_SIZE);
    /// Capacity tuned for typical Lambda fan-out (up to a few dozen
    /// distinct files in a single invocation, plus carryover across
    /// warm calls). Each entry stores a parquet footer (~hundreds of
    /// bytes to a few KB), so even at the cap the cache is sub-MB.
    const META_CACHE_CAPACITY: usize = 128;

    inner: Inner = undefined,
    cache: zpq.io.meta_cache.MetaCache = undefined,
    /// What the cache may hold of Bloom filters across warm invocations.
    blob_budget: usize = zpq.io.meta_cache.MetaCache.default_blob_budget,
    /// Set, with a release store, once `inner` and `cache` are; read with acquire loads, so a query that finds it set
    /// on another thread's first call also sees what it publishes. Written only under `lock`.
    initialized: std.atomic.Value(bool) = .init(false),
    output: engine.OutputLane = undefined,
    output_initialized: bool = false,
    /// Set when init tried to pre-connect. Null when it did not (no S3 config, or the count is 0).
    preconnect_ns: ?u64 = null,
    /// Guards the lazy set-up below, for callers that run queries on one pool from several threads (libzpq).
    lock: std.c.pthread_mutex_t = .{},
    /// Zero the connection counters at each query, so its `"pool"` member reports that query alone. Off where
    /// queries overlap (libzpq), which then report the pool's totals.
    per_call_stats: bool = true,
    /// A query's `max_memory` when its request names none; null derives it from the memory available (`queryMemory`).
    query_memory: ?usize = null,
    /// Gates each query's budget against a ceiling its pool's queries share (libzpq's context). Null: no gate (the
    /// Lambda runs one query at a time).
    admission: ?Admission = null,

    /// A sixteenth of the function's memory for warm Bloom filters (188 MB at 3008 MB), at most 256 MiB.
    pub fn sized(available: usize) PersistentPool {
        return .{ .blob_budget = @min(256 << 20, available / 16) };
    }

    /// A query's default `max_memory`: what the warm filters, which outlive it, may leave of `available`.
    pub fn queryMemory(self: *const PersistentPool, available: usize) usize {
        return available -| self.blob_budget;
    }

    pub fn ensureForBucket(
        self: *PersistentPool,
        gpa: std.mem.Allocator,
        creds: s3.Credentials,
        bucket: []const u8,
    ) !*Inner {
        _ = creds;
        _ = bucket;
        _ = std.c.pthread_mutex_lock(&self.lock);
        defer _ = std.c.pthread_mutex_unlock(&self.lock);
        return self.ensureLocked(gpa);
    }

    /// Set the pool up now rather than at the first S3 request, so queries that start together all see it ready.
    pub fn warm(self: *PersistentPool, gpa: std.mem.Allocator) !void {
        _ = std.c.pthread_mutex_lock(&self.lock);
        defer _ = std.c.pthread_mutex_unlock(&self.lock);
        _ = try self.ensureLocked(gpa);
    }

    fn ensureLocked(self: *PersistentPool, gpa: std.mem.Allocator) !*Inner {
        if (self.initialized.load(.acquire)) return &self.inner;
        try self.inner.init(gpa);
        self.cache.init(gpa, META_CACHE_CAPACITY);
        self.cache.blob_budget = self.blob_budget;
        self.initialized.store(true, .release);
        return &self.inner;
    }

    pub fn ensureOutput(
        self: *PersistentPool,
        gpa: std.mem.Allocator,
        creds: s3.Credentials,
        bucket: []const u8,
    ) !*engine.OutputLane {
        _ = creds;
        _ = bucket;
        _ = std.c.pthread_mutex_lock(&self.lock);
        defer _ = std.c.pthread_mutex_unlock(&self.lock);
        const inner = try self.ensureLocked(gpa);
        if (!self.output_initialized) {
            self.output.init(inner);
            self.output_initialized = true;
        }
        return &self.output;
    }

    /// Free what the pool set up. Only for an owner that outlives every query on it (libzpq's `zpq_close`); the Lambda
    /// keeps its pool for the life of the process.
    pub fn deinit(self: *PersistentPool) void {
        if (!self.initialized.load(.acquire)) return;
        self.cache.deinit();
        self.inner.deinit();
        self.initialized.store(false, .release);
    }

    pub fn metaCache(self: *PersistentPool) ?*zpq.io.meta_cache.MetaCache {
        return if (self.initialized.load(.acquire)) &self.cache else null;
    }

    /// Zero the connection-pool counters, so `stats` after a query reports that invocation alone.
    pub fn resetStats(self: *PersistentPool) void {
        if (self.initialized.load(.acquire)) self.inner.resetStats();
    }

    pub fn stats(self: *PersistentPool) Inner.Stats {
        return if (self.initialized.load(.acquire)) self.inner.snapshotStats() else .{};
    }
};

/// The diagnostics a request that neither aggregates nor writes falls to: the Lambda's single-file int8 scan, given the
/// first input (null when the body is no JSON object) and the filter.
/// Start the regional endpoint's handshakes and return. Best-effort: a missing configuration, a thread that does not
/// spawn, or a handshake that fails leaves the query to connect on its own. The elapsed time is how long it took to
/// start them, not the handshakes. The Lambda calls this during init; libzpq only when a context asks for it.
pub fn preconnectS3(
    io: std.Io,
    allocator: std.mem.Allocator,
    env: std.process.Environ,
    pool: *PersistentPool,
) void {
    const n = s3.preconnectCount(env);
    if (n == 0) return;
    var creds = s3.Credentials.fromEnv(env) catch return;
    creds.path_style = true;
    const t0 = nowMonoNs();
    pool.preconnect_ns = 0;
    const inner = pool.ensureForBucket(allocator, creds, "preconnect") catch return;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const criteria = s3.poolCriteria(arena_state.allocator(), creds, "preconnect") catch {
        pool.preconnect_ns = @as(u64, @intCast(nowMonoNs() - t0));
        return;
    };
    // Return as soon as the threads are started. The handshakes overlap `/next` and the first query.
    inner.preconnect(io, criteria, n);
    pool.preconnect_ns = @as(u64, @intCast(nowMonoNs() - t0));
}

/// Admits a query's memory budget before it runs and takes it back after: libzpq's context budget.
pub const Admission = struct {
    ctx: *anyopaque,
    /// Reserve `bytes`, waiting at most `wait_ms` (null: the gate's own default).
    acquireFn: *const fn (ctx: *anyopaque, bytes: usize, wait_ms: ?u64) Refusal!void,
    releaseFn: *const fn (ctx: *anyopaque, bytes: usize) void,
    /// The gate's ceiling, for the error response that names it.
    ceiling: usize,

    pub const Refusal = error{ OverCeiling, WaitTimeout, Closed };
};

pub const Legacy = *const fn (
    allocator: std.mem.Allocator,
    env: std.process.Environ,
    input: ?[]const u8,
    filter: ?[]const u8,
    body: []const u8,
) anyerror![]u8;

/// Answer one JSON request with a JSON response, an `{"error":...}` one for a request or query that fails. The same
/// request schema and response shapes for the Lambda and for libzpq; inputs and `output_url` may be local paths as
/// well as `s3://` URLs. Runs on `pool`'s connections and metadata cache, which outlive the request. Errors only when
/// no response can be built (out of memory).
pub fn handle(
    io: std.Io,
    allocator: std.mem.Allocator,
    env: std.process.Environ,
    body: []const u8,
    pool: *PersistentPool,
    legacy: ?Legacy,
) ![]u8 {
    if (body.len == 0) {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"empty_body\"}}", .{});
    }

    const trimmed = std.mem.trim(u8, body, " \r\n\t");

    if (trimmed.len > 0 and trimmed[0] == '{') {
        // Inputs: either {"inputs": ["s3://...", ...]} (multi-file)
        // or {"s3_url": "s3://..."} (single-file shorthand).
        var bad_field: []const u8 = "";
        var req = request.Request.parse(allocator, trimmed, &bad_field) catch |err| {
            if (err == error.OutOfMemory) return err;
            if (err == error.BadFieldType) return rejectResponse(allocator, "bad_json", @errorName(err), "field", bad_field);
            return std.fmt.allocPrint(allocator, "{{\"error\":\"bad_json\",\"reason\":\"{s}\"}}", .{@errorName(err)});
        };
        defer req.deinit();

        // Output codec: "snappy", "zstd", "gzip", "lz4"/"lz4_raw" (both LZ4_RAW;
        // nothing compresses to the deprecated LZ4 codec), or "uncompressed" —
        // parity with the CLI's --codec. Absent, re-encoded output is snappy and
        // byte copies keep the source codec. Anything else is treated as snappy
        // with no error (strict validation can come with the API-versioning work).
        const output_codec: ?schema.CompressionCodec = if (req.output_codec) |s| blk: {
            if (std.ascii.eqlIgnoreCase(s, "zstd")) break :blk .ZSTD;
            if (std.ascii.eqlIgnoreCase(s, "uncompressed")) break :blk .UNCOMPRESSED;
            if (std.ascii.eqlIgnoreCase(s, "gzip")) break :blk .GZIP;
            if (std.ascii.eqlIgnoreCase(s, "lz4") or std.ascii.eqlIgnoreCase(s, "lz4_raw")) break :blk .LZ4_RAW;
            break :blk .SNAPPY;
        } else null;

        // How hard zstd works on the output, as the CLI's --compress-priority. Absent: size (level 3).
        const compress_priority: engine.CompressPriority = if (req.compress_priority) |s|
            std.meta.stringToEnum(engine.CompressPriority, s) orelse
                return rejectResponse(allocator, "bad_json", "BadCompressPriority", "compress_priority", s)
        else
            .size;

        const max_memory: ?usize = if (req.max_memory) |m| switch (m) {
            .bytes => |n| n,
            .text => |t| zpq.core.system.parseSizeString(t) catch return std.fmt.allocPrint(
                allocator,
                "{{\"error\":\"bad_json\",\"reason\":\"BadFieldType\",\"field\":\"max_memory\"}}",
                .{},
            ),
        } else null;

        const bloom_filters = try allocator.alloc(engine.BloomSpec, req.bloom_filters.len);
        defer allocator.free(bloom_filters);
        for (req.bloom_filters, bloom_filters) |text, *spec| spec.* = engine.BloomSpec.parse(text) catch
            return rejectResponse(allocator, "bad_json", "BadBloomFilter", "bloom_filters", text);

        // Single source of query options: the JSON event maps onto the
        // SAME `engine.QueryArgs` the CLI's flags populate (src/cli/main.zig).
        // Convention: CLI `--scan-all` ↔ JSON `"scan_all"` ↔ field `scan_all`.
        // A new option is one field on QueryArgs + one parse line on each
        // surface — the handlers below pass `qa` straight through to the
        // engine, so options like `scan_all` flow without touching them.
        const budget = max_memory orelse pool.query_memory orelse
            pool.queryMemory(zpq.core.system.discoverAvailableMemory(env));
        // The profile's buffers come out of the query's budget, sized to it (`profile.Options.forBudget`).
        const prof_opts = zpq.profile.Options.forBudget(budget);
        // A query that aggregates or writes holds its whole budget while it runs, where a gate is set.
        const runs = req.aggregate != null or req.group_by != null or req.output_url != null;
        const gate: ?Admission = if (runs) pool.admission else null;
        if (gate) |g| g.acquireFn(g.ctx, budget, req.budget_wait_ms) catch |err| switch (err) {
            // The caller's handle is going away: no response, the ABI reports it.
            error.Closed => return error.ContextClosed,
            error.OverCeiling => return std.fmt.allocPrint(
                allocator,
                "{{\"error\":\"budget\",\"reason\":\"OverCeiling\",\"max_memory\":{d},\"ceiling\":{d}}}",
                .{ budget, g.ceiling },
            ),
            error.WaitTimeout => return std.fmt.allocPrint(
                allocator,
                "{{\"error\":\"budget\",\"reason\":\"WaitTimeout\",\"max_memory\":{d},\"ceiling\":{d}}}",
                .{ budget, g.ceiling },
            ),
        };
        defer if (gate) |g| g.releaseFn(g.ctx, budget);

        var profiler: ?*zpq.profile.Profiler = null;
        defer if (profiler) |p| p.destroy();
        if (req.profile and zpq.profile.enabled) {
            var meta = zpq.profile.Meta.discover(env);
            meta.query = zpq.profile.utf8Prefix(trimmed, MAX_PROFILE_QUERY);
            meta.s3_preconnect_ns = pool.preconnect_ns;
            // Read, not stored: requests on one pool may run at once (libzpq).
            if (pool.initialized.load(.acquire))
                meta.s3_preconnect_opened = pool.inner.preconnect_parked.load(.monotonic);
            profiler = try zpq.profile.Profiler.create(allocator, prof_opts, meta);
        }

        var qa: engine.QueryArgs = .{
            .inputs = req.inputs,
            .filter = req.filter,
            .output = req.output_url,
            .columns = req.columns,
            .select = req.select,
            .aggregate = req.aggregate,
            .codec = output_codec,
            .compress_priority = compress_priority,
            .scan_all = req.scan_all,
            .trust_stats = req.trust_stats,
            .unordered = req.unordered,
            .max_memory = if (profiler != null) budget -| prof_opts.reservedBytes() else budget,
            .group_by = req.group_by,
            .column_order = req.column_order,
            .bloom_filters = bloom_filters,
        };
        qa.profiler = profiler;

        // or group-by wins, then write, then the legacy single-file diagnostics.
        if (qa.aggregate != null or qa.group_by != null) {
            const response = try runAggregate(io, allocator, env, pool, qa);
            const p = profiler orelse return response;
            return attachProfile(io, allocator, env, pool, p, req.profile_url, budget, response);
        }
        if (qa.output != null) {
            const response = try runWrite(io, allocator, env, pool, qa);
            const p = profiler orelse return response;
            return attachProfile(io, allocator, env, pool, p, req.profile_url, budget, response);
        }
        const f = legacy orelse return rejectResponse(
            allocator,
            "bad_request",
            "NoOperation",
            "expected",
            "aggregate, group_by or output_url",
        );
        return f(allocator, env, if (req.inputs.len > 0) req.inputs[0] else null, qa.filter, body);
    }

    // Not a JSON object.
    const f = legacy orelse
        return std.fmt.allocPrint(allocator, "{{\"error\":\"bad_json\",\"reason\":\"NotAnObject\"}}", .{});
    return f(allocator, env, null, null, body);
}

/// Bytes of the request echoed into a profile's metadata.
const MAX_PROFILE_QUERY: usize = 4096;

/// `response` (a JSON object) with the profile summary added as `"profile"`, and, given `profile_url`, the trace
/// uploaded there and reported as `"profile_trace"`. The summary is bounded (see `profile_summary.writeJson`), a few KB
/// against Lambda's 6 MB response limit; the trace, which grows with the query, never rides in the response and is
/// capped at `profile.traceBytesFor(budget)`. Profiling never fails the query: whatever goes wrong here is reported
/// in the response, or at worst the answer goes back without its profile. Consumes `response`.
fn attachProfile(
    io: std.Io,
    allocator: std.mem.Allocator,
    env: std.process.Environ,
    pool: *PersistentPool,
    p: *zpq.profile.Profiler,
    profile_url: ?[]const u8,
    budget: usize,
    response: []u8,
) []u8 {
    p.finish();
    const body = std.mem.trimEnd(u8, response, " \r\n\t");
    if (body.len == 0 or body[body.len - 1] != '}') return response;

    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();
    writeProfiled(io, allocator, env, pool, p, profile_url, budget, body, &aw.writer) catch return response;
    const out = aw.toOwnedSlice() catch return response;
    allocator.free(response);
    return out;
}

fn writeProfiled(
    io: std.Io,
    allocator: std.mem.Allocator,
    env: std.process.Environ,
    pool: *PersistentPool,
    p: *zpq.profile.Profiler,
    profile_url: ?[]const u8,
    budget: usize,
    body: []const u8,
    w: *std.Io.Writer,
) !void {
    try w.writeAll(body[0 .. body.len - 1]);
    try w.writeAll(",\"profile\":");
    {
        // Into a buffer of its own, so a failure part way leaves no half-written member behind.
        var sw: std.Io.Writer.Allocating = .init(allocator);
        defer sw.deinit();
        if (summaryJson(allocator, p, &sw.writer)) {
            try w.writeAll(sw.written());
        } else |err| {
            try w.print("{{\"error\":\"{s}\"}}", .{@errorName(err)});
        }
    }
    if (profile_url) |url_str| {
        try w.writeAll(",\"profile_trace\":{\"url\":");
        try engine.writeJsonQuoted(w, url_str);
        if (uploadTrace(io, allocator, env, pool, p, url_str, zpq.profile.traceBytesFor(budget))) |st| {
            try w.print(",\"bytes\":{d},\"omitted_events\":{d}}}", .{ st.bytes, st.omitted });
        } else |err| {
            try w.print(",\"error\":\"{s}\"}}", .{@errorName(err)});
        }
    }
    try w.writeByte('}');
}

fn summaryJson(allocator: std.mem.Allocator, p: *zpq.profile.Profiler, w: *std.Io.Writer) !void {
    const summary = try zpq.profile_summary.compute(allocator, p);
    try zpq.profile_summary.writeJson(summary, p, w);
}

/// PUT the Chrome trace, cut at `max_bytes`, to `url_str` over the warm pool.
fn uploadTrace(
    io: std.Io,
    allocator: std.mem.Allocator,
    env: std.process.Environ,
    pool: *PersistentPool,
    p: *zpq.profile.Profiler,
    url_str: []const u8,
    max_bytes: usize,
) !zpq.profile.TraceStats {
    const url = s3.Url.parse(url_str) catch return error.BadProfileUrl;
    var creds = s3.Credentials.fromEnv(env) catch return error.NoCredentials;
    creds.path_style = true;
    var trace: std.Io.Writer.Allocating = .init(allocator);
    defer trace.deinit();
    const st = try p.writeTraceCapped(allocator, &trace.writer, max_bytes);
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const inner = try pool.ensureForBucket(allocator, creds, url.bucket);
    const resp = try s3.putViaPool(io, inner, arena_state.allocator(), creds, url, trace.written());
    if (resp.status != 200) return error.TraceUploadFailed;
    return st;
}

/// The response for a failed query: the error, and the column or input file it is about when `diag` names one.
fn engineError(allocator: std.mem.Allocator, err: anyerror, diag: *const Diag) ![]u8 {
    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();
    const w = &aw.writer;
    try w.print("{{\"error\":\"engine\",\"reason\":\"{s}\"", .{@errorName(err)});
    if (diag.column.len > 0) {
        try w.writeAll(",\"column\":");
        try engine.writeJsonQuoted(w, diag.column.get());
    }
    if (diag.input) |input| {
        try w.writeAll(",\"input\":");
        try engine.writeJsonQuoted(w, input);
        if (diag.cause) |cause| try w.print(",\"cause\":\"{s}\"", .{@errorName(cause)});
    }
    if (diag.detail == .s3_request) try writeS3Failure(w, diag.detail.s3_request);
    try w.writeByte('}');
    return aw.toOwnedSlice();
}

/// `,"s3":{...}`: the failed S3 request behind an engine error, its phase, and its error or its status and S3 code.
fn writeS3Failure(w: *std.Io.Writer, f: zpq.core.diag.S3Failure) !void {
    try w.print(",\"s3\":{{\"op\":\"{s}\",\"phase\":\"{s}\",\"attempts\":{d}", .{
        @tagName(f.op), @tagName(f.phase), f.attempts,
    });
    if (f.err) |e| try w.print(",\"error\":\"{s}\"", .{@errorName(e)});
    if (f.phase == .response) {
        try w.print(",\"status\":{d},\"code\":", .{f.status});
        try engine.writeJsonQuoted(w, f.code.get());
    }
    try w.writeByte('}');
}

/// Adapter that lets engine.runQuery use a PersistentPool (warm connection reuse) without engine knowing about it.
fn persistentPoolAdapter(pool: *PersistentPool) engine.PoolRegistry {
    const Wrapper = struct {
        fn ensure(
            ctx: *anyopaque,
            gpa: std.mem.Allocator,
            creds: s3.Credentials,
            bucket: []const u8,
        ) anyerror!*s3.Pool(engine.POOL_SIZE) {
            const self: *PersistentPool = @ptrCast(@alignCast(ctx));
            return self.ensureForBucket(gpa, creds, bucket);
        }
        fn ensureOutput(
            ctx: *anyopaque,
            gpa: std.mem.Allocator,
            creds: s3.Credentials,
            bucket: []const u8,
        ) anyerror!*engine.OutputLane {
            const self: *PersistentPool = @ptrCast(@alignCast(ctx));
            return self.ensureOutput(gpa, creds, bucket);
        }
    };
    return .{
        .ctx = @ptrCast(pool),
        .ensure_for_bucket = Wrapper.ensure,
        .ensure_output = Wrapper.ensureOutput,
    };
}

/// The aggregate handler — one shape for single-file and multi-file. Same code path as the CLI's
/// `zpq query 's3://...'`, with a JSON envelope instead of stdout.
fn runAggregate(
    io: std.Io,
    allocator: std.mem.Allocator,
    env: std.process.Environ,
    pool: *PersistentPool,
    qa: engine.QueryArgs,
) ![]u8 {
    // 1-row S3 output not yet supported (Stage 5b); engine.runQuery
    // gives aggregate precedence over output, so qa.output is ignored.
    const t_start = nowMonoNs();

    var registry = persistentPoolAdapter(pool);
    var diag: Diag = .{};
    var args = qa;
    args.diag = &diag;

    if (pool.per_call_stats) pool.resetStats();
    var result = engine.runQuery(.{
        .gpa = allocator,
        .env = env,
        .io = io,
        .pool_registry = &registry,
        .meta_cache = pool.metaCache(),
    }, args) catch |err| return engineError(allocator, err, &diag);
    defer result.deinit(allocator);
    const ar = result.aggregate;
    const answer = ar.output;
    const total_ms = @divTrunc(nowMonoNs() - t_start, std.time.ns_per_ms);

    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();
    const w = &aw.writer;
    if (answer.group_rows) |rows| {
        try w.print("{{\"ok\":true,\"files_in\":{d},\"rows_in\":{d},\"row_groups_in\":{d},\"row_groups_pruned\":{d},\"row_groups_full_match\":{d},\"cols_stat_pruned\":{d},\"bytes_in\":{d},\"agg\":[", .{
            ar.files_in,              ar.rows_in,          ar.row_groups_in, ar.row_groups_pruned,
            ar.row_groups_full_match, ar.cols_stat_pruned, ar.bytes_in,
        });
        for (rows, 0..) |row_vals, row_idx| {
            if (row_idx > 0) try w.writeByte(',');
            try w.writeByte('{');
            for (answer.group_cols.?, 0..) |col_name, col_idx| {
                if (col_idx > 0) try w.writeByte(',');
                try engine.writeJsonQuoted(w, col_name);
                try w.writeByte(':');
                try engine.writeAggValueJson(w, row_vals[col_idx]);
            }
            try w.writeByte('}');
        }
        try w.writeByte(']');
    } else {
        try w.print("{{\"ok\":true,\"files_in\":{d},\"rows_in\":{d},\"row_groups_in\":{d},\"row_groups_pruned\":{d},\"row_groups_full_match\":{d},\"cols_stat_pruned\":{d},\"bytes_in\":{d},\"agg\":{{", .{
            ar.files_in,              ar.rows_in,          ar.row_groups_in, ar.row_groups_pruned,
            ar.row_groups_full_match, ar.cols_stat_pruned, ar.bytes_in,
        });
        for (answer.aggs, 0..) |item, i| {
            if (i > 0) try w.writeByte(',');
            try engine.writeJsonQuoted(w, item.alias);
            try w.writeByte(':');
            try engine.writeAggValueJson(w, item.value);
        }
        try w.writeByte('}');
    }
    try w.print(",\"total_ms\":{d},\"phase\":{{\"read_ms\":{d},\"decode_ms\":{d},\"eval_ms\":{d},\"encode_ms\":{d}}},", .{
        total_ms,
        ar.timings.read_ns / std.time.ns_per_ms,
        ar.timings.core.decode_ns / std.time.ns_per_ms,
        ar.timings.core.eval_ns / std.time.ns_per_ms,
        ar.timings.core.encode_ns / std.time.ns_per_ms,
    });
    try engine.writeMembershipJson(w, ar.membership);
    try w.writeByte(',');
    try writePoolStats(w, pool.stats());
    try w.writeByte('}');
    return aw.toOwnedSlice();
}

/// The write handler — the iceberg-compaction shape (filter + project + write). Same engine path as the CLI's
/// `zpq query 's3://...' -o 's3://...'`, with a JSON envelope.
fn runWrite(
    io: std.Io,
    allocator: std.mem.Allocator,
    env: std.process.Environ,
    pool: *PersistentPool,
    qa: engine.QueryArgs,
) ![]u8 {
    const t_start = nowMonoNs();

    var registry = persistentPoolAdapter(pool);
    var diag: Diag = .{};
    var args = qa;
    args.diag = &diag;

    if (pool.per_call_stats) pool.resetStats();
    const result = engine.runQuery(.{
        .gpa = allocator,
        .env = env,
        .io = io,
        .pool_registry = &registry,
        .meta_cache = pool.metaCache(),
    }, args) catch |err| return engineError(allocator, err, &diag);
    const wr = result.write;
    const output_url_str = qa.output.?; // dispatch only routes here when set
    const total_ms = @divTrunc(nowMonoNs() - t_start, std.time.ns_per_ms);

    const cache_stats: zpq.io.meta_cache.Stats = if (pool.metaCache()) |c| blk: {
        const s = c.snapshotStats();
        // Don't reset — cache stats are cumulative for the warm
        // container. Inserts/evictions are LRU-state, not per-invoke.
        break :blk s;
    } else .{};
    return writeResponse(allocator, output_url_str, wr, total_ms, pool.stats(), cache_stats);
}

/// The success response for a write. `output` is the caller's URL or path, so it is escaped like every other string
/// the response echoes back.
fn writeResponse(
    allocator: std.mem.Allocator,
    output: []const u8,
    wr: engine.WriteResult,
    total_ms: i64,
    stats: s3.Pool(POOL_SIZE).Stats,
    cache_stats: zpq.io.meta_cache.Stats,
) ![]u8 {
    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();
    const w = &aw.writer;
    try w.writeAll("{\"ok\":true,\"output\":");
    try engine.writeJsonQuoted(w, output);
    try w.print(
        ",\"files_in\":{d},\"rows_in\":{d},\"rows_kept\":{d},\"bytes_in\":{d},\"bytes_out\":{d},\"row_groups_in\":{d},\"row_groups_kept\":{d},\"total_ms\":{d},\"phase\":{{\"read_ms\":{d},\"parse_ms\":{d},\"decode_ms\":{d},\"eval_ms\":{d},\"encode_ms\":{d},\"sink_ms\":{d},\"footer_ms\":{d},\"mp_await_ms\":{d},\"mp_complete_ms\":{d}}},\"meta_cache\":{{\"hits\":{d},\"misses\":{d},\"revalidations\":{d},\"invalidations\":{d},\"inserts\":{d},\"evictions\":{d}}},",
        .{
            wr.files_in,
            wr.rows_in,
            wr.rows_kept,
            wr.bytes_in,
            wr.bytes_out,
            wr.row_groups_in,
            wr.row_groups_kept,
            total_ms,
            wr.timings.read_ns / std.time.ns_per_ms,
            wr.timings.parse_ns / std.time.ns_per_ms,
            wr.timings.core.decode_ns / std.time.ns_per_ms,
            wr.timings.core.eval_ns / std.time.ns_per_ms,
            wr.timings.core.encode_ns / std.time.ns_per_ms,
            wr.timings.core.sink_ns / std.time.ns_per_ms,
            wr.timings.footer_ns / std.time.ns_per_ms,
            wr.timings.mp_await_ns / std.time.ns_per_ms,
            wr.timings.mp_complete_ns / std.time.ns_per_ms,
            cache_stats.hits,
            cache_stats.misses,
            cache_stats.revalidations,
            cache_stats.invalidations,
            cache_stats.inserts,
            cache_stats.evictions,
        },
    );
    try engine.writeMembershipJson(w, wr.membership);
    try w.writeByte(',');
    try writePoolStats(w, stats);
    try w.writeByte('}');
    return aw.toOwnedSlice();
}

/// The `"pool"` member: this invocation's connection reuse, idle evictions and retries. Cold TLS handshakes against
/// warm reuse, and stale or backed-off retries, are the first things to read when warm-path latency changes.
fn writePoolStats(w: *std.Io.Writer, stats: s3.Pool(POOL_SIZE).Stats) !void {
    try w.print(
        "\"pool\":{{\"acquires\":{d},\"opens\":{d},\"reuses\":{d},\"discards\":{d},\"idle_evictions\":{d}," ++
            "\"stale_retries\":{d},\"backoff_retries\":{d},\"timeout_retries\":{d},\"max_idle_ms\":{d}," ++
            "\"acquire_wait_ms\":{d},\"acquire_lock_ms\":{d}}}",
        .{
            stats.acquires,
            stats.opens,
            stats.reuses,
            stats.discards,
            stats.idle_evictions,
            stats.stale_retries,
            stats.backoff_retries,
            stats.timeout_retries,
            stats.max_idle_ns / std.time.ns_per_ms,
            stats.acquire_wait_ns / std.time.ns_per_ms,
            stats.acquire_lock_ns / std.time.ns_per_ms,
        },
    );
}

/// `{"error":kind,"reason":reason,"<key>":value}`: an early-exit response naming the field or text it rejects. `kind`
/// and `reason` are fixed names; `value` comes from the request, so it is escaped.
pub fn rejectResponse(
    allocator: std.mem.Allocator,
    kind: []const u8,
    reason: []const u8,
    key: []const u8,
    value: []const u8,
) ![]u8 {
    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();
    const w = &aw.writer;
    try w.print("{{\"error\":\"{s}\",\"reason\":\"{s}\",\"{s}\":", .{ kind, reason, key });
    try engine.writeJsonQuoted(w, value);
    try w.writeByte('}');
    return aw.toOwnedSlice();
}

/// Parse `body` as one JSON object and return its string field `key`, failing unless both hold.
fn expectStringField(body: []const u8, key: []const u8, want: []const u8) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, body, .{});
    defer parsed.deinit();
    const got = parsed.value.object.get(key) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(want, got.string);
}

test "regression: a query's default memory budget leaves room for the warm Bloom filters" {
    for ([_]usize{ 128 << 20, 1024 << 20, 3008 << 20, 10240 << 20 }) |available| {
        const pool: PersistentPool = .sized(available);
        try std.testing.expect(pool.blob_budget > 0 and pool.blob_budget <= 256 << 20);
        try std.testing.expectEqual(available, pool.queryMemory(available) + pool.blob_budget);
    }
}

test "the write response escapes the output name" {
    const a = std.testing.allocator;
    const wr: engine.WriteResult = .{
        .codec = .SNAPPY,
        .files_in = 1,
        .rows_in = 3,
        .rows_kept = 2,
        .bytes_in = 10,
        .bytes_out = 8,
        .row_groups_in = 1,
        .row_groups_kept = 1,
        .timings = .{},
    };
    for ([_][]const u8{ "out.parquet", "s3://b/a \"q\" c:\\d\n.parquet" }) |name| {
        const body = try writeResponse(a, name, wr, 5, .{}, .{});
        defer a.free(body);
        try expectStringField(body, "output", name);
    }
}

test "uploads take permits of their own over the warm pool's connections" {
    const io = std.testing.io;
    var pool: PersistentPool = .{};
    var registry = persistentPoolAdapter(&pool);
    const creds: s3.Credentials = .{ .access_key = "ak", .secret_key = "sk", .region = "us-east-1" };
    const out = try registry.ensure_output(registry.ctx, std.testing.allocator, creds, "bkt");
    defer pool.inner.deinit();
    defer pool.cache.deinit();
    try std.testing.expectEqual(&pool.inner, out.pool);
    const in = try registry.ensure_for_bucket(registry.ctx, std.testing.allocator, creds, "bkt");
    try std.testing.expectEqual(&pool.inner, in);
    try std.testing.expectEqual(out, try registry.ensure_output(registry.ctx, std.testing.allocator, creds, "bkt"));

    // Reads holding every input permit leave every upload permit free.
    var permits: [PersistentPool.Inner.capacity]usize = undefined;
    try std.testing.expectEqual(permits.len, try pool.inner.permits.get(io, &permits, 0));
    var up: [engine.OutputLane.capacity]usize = undefined;
    try std.testing.expectEqual(up.len, try out.permits.get(io, &up, 0));
}

test "a rejected filter echoes its expression escaped" {
    const a = std.testing.allocator;
    const expr = "\"a\".\"b\" = 'x\\y'";
    const body = try rejectResponse(a, "filter_parse", "AmbiguousColumn", "expr", expr);
    defer a.free(body);
    try expectStringField(body, "expr", expr);
    try expectStringField(body, "reason", "AmbiguousColumn");
}

test {
    _ = @import("request.zig");
}

test "an engine error names the failed S3 request, its phase, and its error or status and S3 code" {
    const gpa = std.testing.allocator;
    var diag: Diag = .{};
    diag.detail = .{ .s3_request = .{
        .op = .CreateMultipartUpload,
        .phase = .connect,
        .err = error.ConnectTimeout,
        .attempts = 3,
    } };
    const timed_out = try engineError(gpa, error.CreateMultipartFailed, &diag);
    defer gpa.free(timed_out);
    try std.testing.expectEqualStrings("{\"error\":\"engine\",\"reason\":\"CreateMultipartFailed\"," ++
        "\"s3\":{\"op\":\"CreateMultipartUpload\",\"phase\":\"connect\",\"attempts\":3," ++
        "\"error\":\"ConnectTimeout\"}}", timed_out);

    var refused: zpq.core.diag.S3Failure = .{ .op = .CreateMultipartUpload, .phase = .response, .status = 503 };
    refused.code.set("SlowDown");
    diag.detail = .{ .s3_request = refused };
    const slow = try engineError(gpa, error.CreateMultipartFailed, &diag);
    defer gpa.free(slow);
    try std.testing.expectEqualStrings("{\"error\":\"engine\",\"reason\":\"CreateMultipartFailed\"," ++
        "\"s3\":{\"op\":\"CreateMultipartUpload\",\"phase\":\"response\",\"attempts\":1,\"status\":503," ++
        "\"code\":\"SlowDown\"}}", slow);
}
