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
    /// Owned by an embedding context and joined only when that context closes.
    worker_io: ?std.Io = null,

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
    return handleStaged(io, allocator, env, body, pool, legacy, null);
}

/// An embedding caller may retain publication until its response is fully allocated. It must commit or abort.
pub fn handleStaged(
    io: std.Io,
    allocator: std.mem.Allocator,
    env: std.process.Environ,
    body: []const u8,
    pool: *PersistentPool,
    legacy: ?Legacy,
    staged: ?*zpq.output_publication.OutputPublication,
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
            if (err == error.BadFieldType or err == error.ConflictingSqlFields)
                return rejectResponse(allocator, "bad_json", @errorName(err), "field", bad_field);
            return std.fmt.allocPrint(allocator, "{{\"error\":\"bad_json\",\"reason\":\"{s}\"}}", .{@errorName(err)});
        };
        defer req.deinit();

        // Output codec: "snappy", "zstd", "gzip", "lz4"/"lz4_raw" (both LZ4_RAW;
        // nothing compresses to the deprecated LZ4 codec), or "uncompressed" —
        // parity with the CLI's --codec. Absent, re-encoded output is snappy and
        // byte copies keep the source codec. Any other name is refused, as the CLI
        // refuses it: a misspelt "zsdt" wrote snappy.
        const output_codec: ?schema.CompressionCodec = if (req.output_codec) |s| blk: {
            if (std.ascii.eqlIgnoreCase(s, "zstd")) break :blk .ZSTD;
            if (std.ascii.eqlIgnoreCase(s, "uncompressed")) break :blk .UNCOMPRESSED;
            if (std.ascii.eqlIgnoreCase(s, "gzip")) break :blk .GZIP;
            if (std.ascii.eqlIgnoreCase(s, "lz4") or std.ascii.eqlIgnoreCase(s, "lz4_raw")) break :blk .LZ4_RAW;
            if (std.ascii.eqlIgnoreCase(s, "snappy")) break :blk .SNAPPY;
            return rejectResponse(allocator, "bad_json", "BadOutputCodec", "output_codec", s);
        } else null;

        // How hard zstd works on the output, as the CLI's --compress-priority. Absent: size (level 3).
        const compress_priority: engine.CompressPriority = if (req.compress_priority) |s|
            std.meta.stringToEnum(engine.CompressPriority, s) orelse
                return rejectResponse(allocator, "bad_json", "BadCompressPriority", "compress_priority", s)
        else
            .size;

        // A budget of 0 is refused, as libzpq's context ceiling of 0 is: it can run nothing.
        const max_memory: ?usize = if (req.max_memory) |m| switch (m) {
            .bytes => |n| n,
            .text => |t| zpq.core.system.parseSizeString(t) catch 0,
        } else null;
        if (max_memory == 0) return std.fmt.allocPrint(
            allocator,
            "{{\"error\":\"bad_json\",\"reason\":\"BadFieldType\",\"field\":\"max_memory\"}}",
            .{},
        );

        var row_group: engine.RowGroupTarget = .{};
        if (req.row_group_size) |n| row_group.rows = n;
        if (req.row_group_bytes) |b| row_group.bytes = switch (b) {
            .bytes => |n| n,
            .text => |t| zpq.core.system.parseSizeString(t) catch 0,
        };
        if (row_group.bytes == 0) return std.fmt.allocPrint(
            allocator,
            "{{\"error\":\"bad_json\",\"reason\":\"BadFieldType\",\"field\":\"row_group_bytes\"}}",
            .{},
        );

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
        const runs = req.sql != null or req.aggregate != null or req.group_by != null or req.output_url != null;
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
            .parallelism = req.parallelism,
            .filter = req.filter,
            .output = req.output_url,
            .fsync = req.fsync orelse zpq.output_publication.default_fsync,
            .columns = req.columns,
            .select = req.select,
            .aggregate = req.aggregate,
            .codec = output_codec,
            .compress_priority = compress_priority,
            .scan_all = req.scan_all,
            .trust_stats = req.trust_stats,
            .cache = req.cache,
            .unordered = req.unordered,
            .max_memory = if (profiler != null) budget -| prof_opts.reservedBytes() else budget,
            .max_memory_explicit = max_memory,
            .row_group = row_group,
            .group_by = req.group_by,
            .column_order = req.column_order,
            .bloom_filters = bloom_filters,
            .bloom_filters_enabled = req.bloom_filters_enabled,
        };
        qa.profiler = profiler;

        // An answer returned inline is built whole, beside the query. On Lambda it can be no larger than the 6 MB a
        // response may be (a larger one was posted and refused, with no reason given). Elsewhere the floor of 1 GiB
        // keeps every answer an embedding host took before (built three times over then, once now) under a budget
        // set for the scan, not the answer.
        const on_lambda = zpq.core.system.onLambda(env);
        const default_cap = if (on_lambda) lambda_result_cap else @max(budget, default_result_floor);
        const result_cap: ResultCap = .{ .bytes = req.max_result_bytes orelse default_cap, .lambda = on_lambda };
        if (req.sql) |sql| {
            const response = try runSql(io, allocator, env, pool, qa, sql, result_cap);
            const p = profiler orelse return response;
            return attachProfile(io, allocator, env, pool, p, req.profile_url, budget, response);
        }

        // or group-by wins, then write, then the legacy single-file diagnostics.
        if (qa.aggregate != null or qa.group_by != null) {
            const response = try runAggregate(io, allocator, env, pool, qa, result_cap);
            const p = profiler orelse return response;
            return attachProfile(io, allocator, env, pool, p, req.profile_url, budget, response);
        }
        if (qa.output != null) {
            const response = try runWrite(io, allocator, env, pool, qa, staged);
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

// Each caller owns diagnostics and parsing state; concurrent requests must never share query state.
threadlocal var sql_message: [16 * 1024]u8 = undefined;
threadlocal var sql_message_len: usize = 0;

fn captureSqlMessage(message_in: []const u8) void {
    const message = zpq.frontend.withoutCommand(message_in);
    const n = @min(message.len, sql_message.len - sql_message_len);
    @memcpy(sql_message[sql_message_len..][0..n], message[0..n]);
    sql_message_len += n;
}

fn runSql(
    io: std.Io,
    allocator: std.mem.Allocator,
    env: std.process.Environ,
    pool: *PersistentPool,
    options: engine.QueryArgs,
    sql: []const u8,
    result_cap: ResultCap,
) ![]u8 {
    if (comptime !@hasDecl(zpq.core.expr.sql_parser, "parseSqlQuery")) {
        return rejectResponse(allocator, "engine", "SqlNotCompiledIn", "message", "SQL is not compiled in");
    } else {
        const parser = zpq.core.expr.sql_parser;
        const old_parser = parser.diagnostic_fn;
        const old_frontend = zpq.frontend.diagnostic_fn;
        parser.diagnostic_fn = captureSqlMessage;
        zpq.frontend.diagnostic_fn = captureSqlMessage;
        defer {
            parser.diagnostic_fn = old_parser;
            zpq.frontend.diagnostic_fn = old_frontend;
        }
        sql_message_len = 0;
        var diag: Diag = .{};
        var parse_arena = std.heap.ArenaAllocator.init(allocator);
        defer parse_arena.deinit();
        var capped: Capped = .{ .parent = allocator, .cap = result_cap.memory() };
        const result_memory = capped.allocator();
        const sql_arena = parse_arena.allocator();
        const answer = executeSql(io, allocator, result_memory, sql_arena, env, pool, options, sql, &diag);
        return result_cap.admit(allocator, answer catch |err| {
            if (capped.hit) return resultTooLarge(allocator, result_cap);
            if (err == error.OutOfMemory) return err;
            zpq.frontend.queryError(err, &diag);
            const base = try engineError(allocator, err, &diag);
            defer allocator.free(base);
            var out: std.Io.Writer.Allocating = .init(allocator);
            defer out.deinit();
            try out.writer.writeAll(base[0 .. base.len - 1]);
            try out.writer.writeAll(",\"message\":");
            // A diagnostic ends with the newline the CLI prints it with; a JSON string has no use for it.
            const said = std.mem.trimEnd(u8, sql_message[0..sql_message_len], "\n");
            const message = if (said.len > 0) said else @errorName(err);
            try engine.writeJsonQuoted(&out.writer, message);
            try out.writer.writeByte('}');
            return out.toOwnedSlice();
        });
    }
}

/// The largest response the Lambda Runtime API takes (6 MB), less room for the envelope.
const lambda_result_cap = 6 * 1000 * 1000 - 64 * 1024;
/// The least an inline answer may take by default off Lambda (`max_result_bytes` sets it per request).
const default_result_floor = 1 << 30;

/// The most an inline answer may take, and whether that is Lambda's limit (which `max_result_bytes` cannot raise).
const ResultCap = struct {
    bytes: usize,
    lambda: bool,

    /// What building an answer of `bytes` may hold: a growing buffer has slack past what it holds, and the old and
    /// the new one both while it moves, so a cap on what is held would refuse answers well under `bytes`.
    fn memory(self: ResultCap) usize {
        return self.bytes *| 3;
    }

    /// `response`, or `resultTooLarge` in its place when it is longer than the cap.
    fn admit(self: ResultCap, allocator: std.mem.Allocator, response: []u8) ![]u8 {
        if (response.len <= self.bytes) return response;
        allocator.free(response);
        return resultTooLarge(allocator, self);
    }
};

/// An allocator that holds at most `cap` bytes at once: an answer built for the response on it fails as too large
/// (`resultTooLarge`) rather than growing without bound, as one built whole three times over did.
const Capped = struct {
    parent: std.mem.Allocator,
    cap: usize,
    held: usize = 0,
    /// Some allocation was refused for the cap.
    hit: bool = false,

    fn allocator(self: *Capped) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn admit(self: *Capped, more: usize) bool {
        if (self.held +| more > self.cap) {
            self.hit = true;
            return false;
        }
        return true;
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *Capped = @ptrCast(@alignCast(ctx));
        if (!self.admit(len)) return null;
        const p = self.parent.rawAlloc(len, alignment, ra) orelse return null;
        self.held += len;
        return p;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) bool {
        const self: *Capped = @ptrCast(@alignCast(ctx));
        if (new_len > memory.len and !self.admit(new_len - memory.len)) return false;
        if (!self.parent.rawResize(memory, alignment, new_len, ra)) return false;
        self.held = self.held - memory.len + new_len;
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) ?[*]u8 {
        const self: *Capped = @ptrCast(@alignCast(ctx));
        if (new_len > memory.len and !self.admit(new_len - memory.len)) return null;
        const p = self.parent.rawRemap(memory, alignment, new_len, ra) orelse return null;
        self.held = self.held - memory.len + new_len;
        return p;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *Capped = @ptrCast(@alignCast(ctx));
        self.held -|= memory.len;
        self.parent.rawFree(memory, alignment, ra);
    }
};

fn resultTooLarge(allocator: std.mem.Allocator, cap: ResultCap) ![]u8 {
    const hint = if (cap.lambda)
        "a Lambda response holds at most 6 MB: write the answer to output_url"
    else
        "raise the request's max_result_bytes (a byte count) past this answer, or write the answer to output_url";
    return std.fmt.allocPrint(
        allocator,
        "{{\"error\":\"result_too_large\",\"reason\":\"ResultTooLarge\",\"max_result_bytes\":{d}," ++
            "\"hint\":\"{s}\"}}",
        .{ cap.bytes, hint },
    );
}

fn executeSql(
    io: std.Io,
    allocator: std.mem.Allocator,
    result_memory: std.mem.Allocator,
    arena: std.mem.Allocator,
    env: std.process.Environ,
    pool: *PersistentPool,
    options: engine.QueryArgs,
    sql: []const u8,
    diag: *Diag,
) ![]u8 {
    const pq = try zpq.core.expr.sql_parser.parseSqlQuery(arena, sql);
    const inputs = try zpq.frontend.expandGlob(arena, io, pq.table_name, env);
    if (inputs.len == 0) {
        zpq.frontend.noSqlMatches(pq.table_name);
        return error.NoMatches;
    }
    var qa = options;
    qa.inputs = inputs;
    qa.filter = pq.filter_str;
    qa.select = pq.select_str;
    qa.aggregate = pq.aggregate_str;
    qa.group_by = pq.group_by_str;
    qa.select_cols = pq.select_cols;
    qa.result_order = pq.result_order;
    qa.empty_scalar = pq.group_by_str == null and (pq.result_order.limit == 0 or pq.result_order.offset > 0);
    qa.quiet_diagnostics = true;
    qa.diag = diag;
    var registry = persistentPoolAdapter(pool);
    const ctx: engine.Context = .{
        .gpa = allocator,
        .env = env,
        .io = io,
        .pool_registry = &registry,
        .meta_cache = pool.metaCache(),
        .worker_io = pool.worker_io,
    };
    var out: std.Io.Writer.Allocating = .init(result_memory);
    defer out.deinit();
    try out.writer.writeAll("{\"ok\":true,\"ndjson\":\"");
    // Quoted into the response as they are printed: an NDJSON copy beside it held the answer twice, under one cap.
    var quote_buf: [4096]u8 = undefined;
    var rows: JsonQuoting = .init(&out.writer, &quote_buf);
    var decimal: engine.DecimalColumns = .{ .gpa = allocator };
    defer decimal.deinit();
    if (qa.aggregate != null or qa.group_by != null) {
        qa.text_output = .{ .format = .jsonl, .sink = &rows.writer };
        var result = try engine.runQuery(ctx, qa);
        defer result.deinit(allocator);
        const answer = result.aggregate.output;
        for (result.aggregate.answer_types, 0..) |ty, ci| if (ty.decimal) {
            const name = if (answer.group_cols) |cols| cols[ci] else answer.aggs[ci].alias;
            try decimal.names.append(allocator, try allocator.dupe(u8, name));
        };
    } else {
        qa.decimal_columns = &decimal;
        _ = try engine.runPrintSink(
            ctx,
            qa,
            .jsonl,
            if (pq.result_order.limit) |n| @intCast(n) else null,
            1,
            @intCast(pq.result_order.offset),
            &rows.writer,
        );
    }
    try rows.finish();
    try out.writer.writeByte('"');
    try writeDecimalList(&out.writer, decimal.names.items);
    try out.writer.writeAll("}");
    return out.toOwnedSlice();
}

/// A writer that writes what it is given into `inner` as the body of a JSON string (`engine.writeJsonString`). A
/// UTF-8 sequence split between two writes is held until it is whole, so it is not read as two invalid ones.
const JsonQuoting = struct {
    inner: *std.Io.Writer,
    carry: [4]u8 = undefined,
    carry_len: usize = 0,
    writer: std.Io.Writer,

    fn init(inner: *std.Io.Writer, buffer: []u8) JsonQuoting {
        return .{ .inner = inner, .writer = .{ .buffer = buffer, .vtable = &.{ .drain = drain } } };
    }

    /// Flush, and write a sequence the input ended inside of as the invalid bytes it is.
    fn finish(self: *JsonQuoting) std.Io.Writer.Error!void {
        try self.writer.flush();
        try engine.writeJsonString(self.inner, self.carry[0..self.carry_len]);
        self.carry_len = 0;
    }

    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *JsonQuoting = @fieldParentPtr("writer", w);
        try self.escape(w.buffered());
        w.end = 0;
        var n: usize = 0;
        for (data[0 .. data.len - 1]) |d| {
            try self.escape(d);
            n += d.len;
        }
        const last = data[data.len - 1];
        for (0..splat) |_| try self.escape(last);
        return n + last.len * splat;
    }

    fn escape(self: *JsonQuoting, bytes: []const u8) std.Io.Writer.Error!void {
        var s = bytes;
        if (self.carry_len > 0) {
            const want = sequenceLen(self.carry[0]) - self.carry_len;
            const take = @min(want, s.len);
            @memcpy(self.carry[self.carry_len..][0..take], s[0..take]);
            self.carry_len += take;
            s = s[take..];
            if (take < want) return;
            try engine.writeJsonString(self.inner, self.carry[0..self.carry_len]);
            self.carry_len = 0;
        }
        const whole = wholePrefix(s);
        try engine.writeJsonString(self.inner, s[0..whole]);
        @memcpy(self.carry[0 .. s.len - whole], s[whole..]);
        self.carry_len = s.len - whole;
    }

    fn sequenceLen(lead: u8) usize {
        return std.unicode.utf8ByteSequenceLength(lead) catch 1;
    }

    /// `s` up to a sequence its last bytes begin and do not finish.
    fn wholePrefix(s: []const u8) usize {
        for (1..4) |k| {
            if (k > s.len) break;
            const b = s[s.len - k];
            if (b & 0xc0 == 0x80) continue;
            return if (sequenceLen(b) > k) s.len - k else s.len;
        }
        return s.len;
    }
};

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
pub fn publicationError(allocator: std.mem.Allocator, err: anyerror) ![]u8 {
    return engineError(allocator, err, &.{});
}

fn engineError(allocator: std.mem.Allocator, err: anyerror, diag: *const Diag) ![]u8 {
    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();
    const w = &aw.writer;
    // Sink details are CLI diagnostics; keep the public service/ABI error vocabulary stable.
    const public_err = switch (err) {
        error.NoSpaceLeft, error.BrokenPipe, error.CloseFailed => error.WriteFailed,
        else => err,
    };
    try w.print("{{\"error\":\"engine\",\"reason\":\"{s}\"", .{@errorName(public_err)});
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
    result_cap: ResultCap,
) ![]u8 {
    // 1-row S3 output not yet supported (Stage 5b); engine.runQuery
    // gives aggregate precedence over output, so qa.output is ignored.
    const t_start = nowMonoNs();

    var registry = persistentPoolAdapter(pool);
    var diag: Diag = .{};
    var args = qa;
    args.diag = &diag;

    var capped: Capped = .{ .parent = allocator, .cap = result_cap.memory() };
    var aw: std.Io.Writer.Allocating = .init(capped.allocator());
    defer aw.deinit();
    const Envelope = struct {
        writer: *std.Io.Writer,
        started: i64,
        pool: *PersistentPool,
        failure: ?anyerror = null,
        fn write(ctx: *anyopaque, ar: engine.AggResult) !void {
            const e: *@This() = @ptrCast(@alignCast(ctx));
            const total_ms = @divTrunc(nowMonoNs() - e.started, std.time.ns_per_ms);
            aggregateResponse(e.writer, ar, total_ms, e.pool) catch |err| {
                e.failure = err;
                return err;
            };
        }
    };
    var envelope: Envelope = .{ .writer = &aw.writer, .started = t_start, .pool = pool };
    args.aggregate_sink = .{ .ctx = &envelope, .write_fn = Envelope.write };
    if (pool.per_call_stats) pool.resetStats();
    var result = engine.runQuery(.{
        .gpa = allocator,
        .env = env,
        .io = io,
        .pool_registry = &registry,
        .meta_cache = pool.metaCache(),
        .worker_io = pool.worker_io,
    }, args) catch |err| {
        if (capped.hit) return resultTooLarge(allocator, result_cap);
        if (envelope.failure) |failure| return failure;
        return engineError(allocator, err, &diag);
    };
    defer result.deinit(allocator);
    if (args.output != null) aggregateResponse(
        &aw.writer,
        result.aggregate,
        @divTrunc(nowMonoNs() - t_start, std.time.ns_per_ms),
        pool,
    ) catch |err| {
        if (capped.hit) return resultTooLarge(allocator, result_cap);
        return err;
    };
    return result_cap.admit(allocator, try aw.toOwnedSlice());
}

/// `,"decimal":[...]`: the answer's DECIMAL columns, which a reader parses exactly (python/zpq.py does); nothing when
/// there are none.
fn writeDecimalNames(w: *std.Io.Writer, ar: engine.AggResult) !void {
    var sep: []const u8 = ",\"decimal\":[";
    for (ar.answer_types, 0..) |ty, ci| if (ty.decimal) {
        try w.writeAll(sep);
        sep = ",";
        try engine.writeJsonQuoted(w, if (ar.output.group_cols) |cols| cols[ci] else ar.output.aggs[ci].alias);
    };
    if (sep.len == 1) try w.writeByte(']');
}

/// The DECIMAL columns of a SQL answer, as `writeDecimalNames` writes them.
fn writeDecimalList(w: *std.Io.Writer, names: []const []const u8) !void {
    if (names.len == 0) return;
    try w.writeAll(",\"decimal\":[");
    for (names, 0..) |name, i| {
        if (i > 0) try w.writeByte(',');
        try engine.writeJsonQuoted(w, name);
    }
    try w.writeByte(']');
}

/// `runAggregate`'s response for `ar`, onto `w`.
fn aggregateResponse(w: *std.Io.Writer, ar: engine.AggResult, total_ms: i64, pool: *PersistentPool) !void {
    if (ar.output.group_cols != null) {
        try w.print("{{\"ok\":true,\"files_in\":{d},\"rows_in\":{d},\"rows_scanned\":{d},\"rows_matched\":{d},\"row_groups_in\":{d},\"row_groups_pruned\":{d},\"row_groups_full_match\":{d},\"cols_stat_pruned\":{d},\"bytes_in\":{d},\"agg\":", .{
            ar.files_in,      ar.rows_in,           ar.rows_scanned,          ar.rows_matched,
            ar.row_groups_in, ar.row_groups_pruned, ar.row_groups_full_match, ar.cols_stat_pruned,
            ar.bytes_in,
        });
        try engine.writeAggregateJsonValue(w, ar.output, ar.text_kinds, ar.output.rowCount());
    } else {
        try w.print("{{\"ok\":true,\"files_in\":{d},\"rows_in\":{d},\"rows_scanned\":{d},\"rows_matched\":{d},\"row_groups_in\":{d},\"row_groups_pruned\":{d},\"row_groups_full_match\":{d},\"cols_stat_pruned\":{d},\"bytes_in\":{d},\"agg\":{{", .{
            ar.files_in,      ar.rows_in,           ar.rows_scanned,          ar.rows_matched,
            ar.row_groups_in, ar.row_groups_pruned, ar.row_groups_full_match, ar.cols_stat_pruned,
            ar.bytes_in,
        });
        for (ar.output.aggs, 0..) |item, i| {
            if (i > 0) try w.writeByte(',');
            try engine.writeJsonQuoted(w, item.alias);
            try w.writeByte(':');
            try engine.writeTypedAggValueJson(w, item.value, engine.kindAt(ar.text_kinds, i));
        }
        try w.writeByte('}');
    }
    try writeDecimalNames(w, ar);
    // As the CLI summary's: wall-clock phases, then each worker phase summed over the workers.
    const ms = std.time.ns_per_ms;
    try w.print(",\"total_ms\":{d},\"wall_ms\":{{\"open\":{d},\"parse\":{d},\"scan\":{d},\"footer\":{d}}},\"worker_ms\":{{\"decode\":{d},\"filter\":{d},\"fold\":{d},\"encode\":0,\"sink\":{d}}},", .{
        total_ms,
        ar.timings.read_ns / ms,
        ar.timings.parse_ns / ms,
        ar.timings.decode_wall_ns / ms,
        ar.timings.footer_ns / ms,
        ar.timings.core.decode_ns / ms,
        ar.timings.core.eval_ns / ms,
        ar.timings.core.encode_ns / ms,
        ar.timings.core.sink_ns / ms,
    });
    try engine.writeMembershipJson(w, ar.membership);
    try w.writeByte(',');
    try writePoolStats(w, pool.stats());
    try w.writeByte('}');
}

/// The write handler — the iceberg-compaction shape (filter + project + write). Same engine path as the CLI's
/// `zpq query 's3://...' -o 's3://...'`, with a JSON envelope.
fn runWrite(
    io: std.Io,
    allocator: std.mem.Allocator,
    env: std.process.Environ,
    pool: *PersistentPool,
    qa: engine.QueryArgs,
    staged: ?*zpq.output_publication.OutputPublication,
) ![]u8 {
    const t_start = nowMonoNs();

    var registry = persistentPoolAdapter(pool);
    var diag: Diag = .{};
    var args = qa;
    args.diag = &diag;
    var local_publication: zpq.output_publication.OutputPublication = .{};
    const publication = staged orelse &local_publication;
    publication.fsync = qa.fsync;
    var written = false;
    defer if (!written or staged == null) publication.abort();
    args.output = publication.prepare(io, qa.output, qa.inputs) catch |err|
        return engineError(allocator, err, &diag);
    args.output_fd = publication.fd;

    if (pool.per_call_stats) pool.resetStats();
    const result = engine.runQuery(.{
        .gpa = allocator,
        .env = env,
        .io = io,
        .pool_registry = &registry,
        .meta_cache = pool.metaCache(),
        .worker_io = pool.worker_io,
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
    // Allocate the response before publishing: even an allocation failure must preserve an earlier target.
    const response = try writeResponse(allocator, output_url_str, wr, total_ms, pool.stats(), cache_stats);
    if (staged == null) publication.commit(io) catch |err| {
        allocator.free(response);
        return engineError(allocator, err, &diag);
    };
    written = true;
    return response;
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
        ",\"files_in\":{d},\"rows_in\":{d},\"rows_scanned\":{d},\"rows_matched\":{d},\"bytes_in\":{d},\"bytes_out\":{d},\"row_groups_in\":{d},\"row_groups_kept\":{d},\"total_ms\":{d},\"wall_ms\":{{\"open\":{d},\"parse\":{d},\"scan\":{d},\"footer\":{d},\"mp_await\":{d},\"mp_complete\":{d}}},\"worker_ms\":{{\"decode\":{d},\"filter\":{d},\"fold\":0,\"encode\":{d},\"sink\":{d}}},\"meta_cache\":{{\"hits\":{d},\"misses\":{d},\"revalidations\":{d},\"invalidations\":{d},\"inserts\":{d},\"evictions\":{d}}},",
        .{
            wr.files_in,
            wr.rows_in,
            wr.rows_scanned,
            wr.rows_matched,
            wr.bytes_in,
            wr.bytes_out,
            wr.row_groups_in,
            wr.row_groups_kept,
            total_ms,
            wr.timings.read_ns / std.time.ns_per_ms,
            wr.timings.parse_ns / std.time.ns_per_ms,
            wr.timings.decode_wall_ns / std.time.ns_per_ms,
            wr.timings.footer_ns / std.time.ns_per_ms,
            wr.timings.mp_await_ns / std.time.ns_per_ms,
            wr.timings.mp_complete_ns / std.time.ns_per_ms,
            wr.timings.core.decode_ns / std.time.ns_per_ms,
            wr.timings.core.eval_ns / std.time.ns_per_ms,
            wr.timings.core.encode_ns / std.time.ns_per_ms,
            wr.timings.core.sink_ns / std.time.ns_per_ms,
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

test "a released grouped Parquet answer has no phantom object in the service envelope" {
    const a = std.testing.allocator;
    var pool: PersistentPool = .sized(256 << 20);
    defer pool.deinit();
    const ar: engine.AggResult = .{
        .files_in = 1,
        .rows_in = 5,
        .rows_scanned = 5,
        .rows_matched = 5,
        .bytes_in = 0,
        .bytes_out = 0,
        .row_groups_in = 1,
        .row_groups_pruned = 0,
        .row_groups_full_match = 0,
        .cols_stat_pruned = 0,
        .timings = .{},
        .output = .{ .group_cols = &.{ "k", "c", "s" }, .typed = .{ .rows = 5, .columns = &.{} } },
    };
    var writer: std.Io.Writer.Allocating = .init(a);
    defer writer.deinit();
    try aggregateResponse(&writer.writer, ar, 0, &pool);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, writer.written(), .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 0), parsed.value.object.get("agg").?.array.items.len);
    try std.testing.expect(std.mem.indexOf(u8, writer.written(), "\"agg\":[],") != null);
}

test "the write response escapes the output name" {
    const a = std.testing.allocator;
    const wr: engine.WriteResult = .{
        .codec = .SNAPPY,
        .files_in = 1,
        .rows_in = 3,
        .rows_scanned = 3,
        .rows_matched = 2,
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

test "regression: an inline answer past max_result_bytes fails as too large, before it outgrows the budget" {
    const a = std.testing.allocator;
    const fixture = "ci/fixtures/parquet/nullable_mix.parquet";
    std.Io.Dir.cwd().access(std.testing.io, fixture, .{}) catch return error.SkipZigTest;
    var pool: PersistentPool = .sized(256 << 20);
    defer pool.deinit();
    const Case = struct { body: []const u8, too_large: bool };
    for ([_]Case{
        .{ .body = "{\"inputs\":[\"ci/fixtures/parquet/nullable_mix.parquet\"],\"aggregate\":\"count(*) AS n\"," ++
            "\"group_by\":\"id\",\"max_result_bytes\":64}", .too_large = true },
        .{
            .body = "{\"sql\":\"SELECT id FROM 'ci/fixtures/parquet/nullable_mix.parquet'\",\"max_result_bytes\":16}",
            .too_large = true,
        },
        .{ .body = "{\"sql\":\"SELECT id FROM 'ci/fixtures/parquet/nullable_mix.parquet'\"}", .too_large = false },
        .{ .body = "{\"inputs\":[\"ci/fixtures/parquet/nullable_mix.parquet\"],\"aggregate\":\"count(*) AS n\"," ++
            "\"group_by\":\"id\"}", .too_large = false },
    }) |c| {
        const response = try handle(std.testing.io, a, .empty, c.body, &pool, null);
        defer a.free(response);
        if (c.too_large) {
            try expectStringField(response, "reason", "ResultTooLarge");
        } else try std.testing.expect(std.mem.startsWith(u8, response, "{\"ok\":true"));
    }
}

test "regression: max_result_bytes counts the answer once: one of exactly that many bytes is returned" {
    const a = std.testing.allocator;
    const fixture = "ci/fixtures/parquet/nullable_mix.parquet";
    std.Io.Dir.cwd().access(std.testing.io, fixture, .{}) catch return error.SkipZigTest;
    var pool: PersistentPool = .sized(256 << 20);
    defer pool.deinit();
    const sql = "{\"sql\":\"SELECT * FROM 'ci/fixtures/parquet/nullable_mix.parquet'\"";
    const whole = try handle(std.testing.io, a, .empty, sql ++ "}", &pool, null);
    defer a.free(whole);
    try std.testing.expect(std.mem.startsWith(u8, whole, "{\"ok\":true"));
    // The rows and their quoted copy were held under one cap: an answer needed about twice its size.
    for ([_]usize{ whole.len, whole.len - 1 }) |cap| {
        const body = try std.fmt.allocPrint(a, "{s},\"max_result_bytes\":{d}}}", .{ sql, cap });
        defer a.free(body);
        const response = try handle(std.testing.io, a, .empty, body, &pool, null);
        defer a.free(response);
        if (cap == whole.len) try std.testing.expectEqualStrings(whole, response) else {
            try expectStringField(response, "reason", "ResultTooLarge");
        }
    }
}

test "JsonQuoting escapes as writeJsonString does, a UTF-8 sequence split between writes included" {
    const a = std.testing.allocator;
    const text = "a\"b\\ \xc3\xa9 \xe2\x82\xac \xf0\x9f\x98\x80 \xff end \xe2\x82";
    var want: std.Io.Writer.Allocating = .init(a);
    defer want.deinit();
    try engine.writeJsonString(&want.writer, text);
    for ([_]usize{ 0, 1, 3 }) |buffer_len| {
        var got: std.Io.Writer.Allocating = .init(a);
        defer got.deinit();
        var buf: [3]u8 = undefined;
        var q: JsonQuoting = .init(&got.writer, buf[0..buffer_len]);
        for (text) |c| try q.writer.writeByte(c);
        try q.finish();
        try std.testing.expectEqualStrings(want.written(), got.written());
    }
}

test "regression: an unknown output_codec is refused, and an SQL error names no CLI command" {
    const a = std.testing.allocator;
    var pool: PersistentPool = .sized(256 << 20);
    defer pool.deinit();
    // A misspelt codec wrote snappy.
    const codec = try handle(std.testing.io, a, .empty, "{\"inputs\":[\"x.parquet\"],\"output_url\":\"o.parquet\"," ++
        "\"output_codec\":\"zsdt\"}", &pool, null);
    defer a.free(codec);
    try expectStringField(codec, "reason", "BadOutputCodec");
    try expectStringField(codec, "output_codec", "zsdt");
    const missing = "{\"sql\":\"SELECT * FROM 'no/such/file.parquet'\"}";
    const sql = try handle(std.testing.io, a, .empty, missing, &pool, null);
    defer a.free(sql);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, sql, .{});
    defer parsed.deinit();
    const message = parsed.value.object.get("message").?.string;
    try std.testing.expect(!std.mem.startsWith(u8, message, "zpq query:"));
    try std.testing.expect(!std.mem.endsWith(u8, message, "\n"));
    try std.testing.expect(std.mem.indexOf(u8, message, "no/such/file.parquet") != null);
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
    // The test-bearing files under this root, listed by build.zig (tests.gen.zig).
    _ = @import("tests.gen.zig");
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

test "sink error details retain the service WriteFailed reason" {
    for ([_]anyerror{ error.NoSpaceLeft, error.BrokenPipe, error.CloseFailed }) |err| {
        const response = try engineError(std.testing.allocator, err, &.{});
        defer std.testing.allocator.free(response);
        try std.testing.expectEqualStrings("{\"error\":\"engine\",\"reason\":\"WriteFailed\"}", response);
    }
}
