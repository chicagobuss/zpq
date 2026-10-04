//! ZPQ Lambda binary entry point.
//!
//! Constraints established by docs/lambda_capabilities.md:
//!   - io_uring is unavailable (AWS seccomp returns ENOSYS).
//!   - Kernel is AL2 5.10, not AL2023 6.x — no epoll_pwait2, no clone3.
//!   - epoll/eventfd2/timerfd_create/signalfd4/mlock are allowed.
//!   - SO_ZEROCOPY and TCP_FASTOPEN setsockopt allowed.
//!
//! Lifecycle (production / S3 path):
//!   1. Receive `{"s3_url": "...", "filter": "...optional..."}`.
//!   2. Suffix GET for the last 64 KB to discover file size + footer.
//!   3. If the footer is bigger than 64 KB, fetch the rest.
//!   4. Parse FileMetaData. If filter is present, parse it against
//!      the schema.
//!   5. For each row group: prune via the filter's stats. For
//!      survivors, range-fetch the int8 column AND any filter
//!      columns. Coalesce nearby ranges.
//!   6. Decode each column into typed slices, build a SelectionVector,
//!      run filter.eval, then aggregate matched int8 values.
//!   7. Return JSON envelope with row_groups_pruned + matched count
//!      + min/max/sum.

const std = @import("std");
const zpq = @import("zpq");
const nowMonoNs = zpq.clock.monoNs;
const runtime = @import("runtime.zig");
const request = @import("request.zig");
// Lambda's per-file fetch+scan helper; distinct from the core
// multi-file orchestrator at `core.scan` which does the SHARED
// agg pipeline both the CLI and Lambda call into.
const scan = @import("scan.zig");
const core_scan = zpq.core.scan;
const Diag = zpq.core.diag.Diag;
const engine = zpq.engine;

const schema = zpq.core.schema;
const metadata = zpq.core.parquet.metadata;
const column_mod = zpq.core.parquet.column;
const fastpath = zpq.core.writer.fastpath;
const streaming = zpq.core.writer.streaming;
const encoder = zpq.core.writer.encoder;
const consumer = zpq.core.consumer;
const thrift = zpq.core.thrift;
const s3 = zpq.io.s3;
const multipart_sink = zpq.io.multipart_sink;
const coalescer = zpq.io.coalescer;
const filter_ast = zpq.core.filter.ast;
const expr_parser = zpq.core.expr.parser;
const expr_agg = zpq.core.expr.agg;
const filter_parser = zpq.core.filter.parser;
const filter_prune = zpq.core.filter.prune;
const filter_selection = zpq.core.filter.selection;
const filter_eval = zpq.core.filter.eval;
const partition = zpq.core.filter.partition;
const schema_tree = zpq.core.parquet.schema_tree;

const TAIL_SIZE: u64 = 64 * 1024;
const COALESCE_GAP: u64 = 64 * 1024;
const TARGET_COLUMN: []const u8 = "int8";

/// Persistent state that lives for the lifetime of one Lambda
/// container (across invocations): connection pool + metadata cache.
/// `s3.Pool` keeps a bounded request permit queue plus a host-keyed
/// idle LRU, so changing input/output buckets no longer tears down
/// the whole warm pool. Output uploads take permits of their own over
/// the same connections (`engine.OutputLane`). `meta_cache` caches
/// parquet footers keyed by (bucket, key) and revalidates with
/// `If-None-Match`.
const PersistentPool = struct {
    const Inner = s3.Pool(POOL_SIZE);
    /// Capacity tuned for typical Lambda fan-out (up to a few dozen
    /// distinct files in a single invocation, plus carryover across
    /// warm calls). Each entry stores a parquet footer (~hundreds of
    /// bytes to a few KB), so even at the cap the cache is sub-MB.
    const META_CACHE_CAPACITY: usize = 128;

    inner: Inner = undefined,
    cache: zpq.io.meta_cache.MetaCache = undefined,
    initialized: bool = false,
    output: engine.OutputLane = undefined,
    output_initialized: bool = false,

    pub fn ensureForBucket(
        self: *PersistentPool,
        gpa: std.mem.Allocator,
        creds: s3.Credentials,
        bucket: []const u8,
    ) !*Inner {
        _ = creds;
        _ = bucket;
        if (self.initialized) return &self.inner;

        try self.inner.init(gpa);
        self.cache.init(gpa, META_CACHE_CAPACITY);
        self.initialized = true;
        return &self.inner;
    }

    pub fn ensureOutput(
        self: *PersistentPool,
        gpa: std.mem.Allocator,
        creds: s3.Credentials,
        bucket: []const u8,
    ) !*engine.OutputLane {
        const inner = try self.ensureForBucket(gpa, creds, bucket);
        if (!self.output_initialized) {
            self.output.init(inner);
            self.output_initialized = true;
        }
        return &self.output;
    }

    pub fn metaCache(self: *PersistentPool) ?*zpq.io.meta_cache.MetaCache {
        return if (self.initialized) &self.cache else null;
    }

    /// Zero the connection-pool counters, so `stats` after a query reports that invocation alone.
    pub fn resetStats(self: *PersistentPool) void {
        if (self.initialized) self.inner.resetStats();
    }

    pub fn stats(self: *PersistentPool) Inner.Stats {
        return if (self.initialized) self.inner.snapshotStats() else .{};
    }
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const env = init.minimal.environ;
    const io = init.io;

    var client = runtime.Client.fromEnv(allocator, env) catch |err| {
        std.debug.print("zpq lambda: runtime client init failed: {s}\n", .{@errorName(err)});
        return err;
    };
    defer client.deinit();

    var pool: PersistentPool = .{};

    while (true) {
        var inv = client.nextInvocation() catch |err| {
            std.debug.print("zpq lambda: poll error {s}\n", .{@errorName(err)});
            const ts: std.os.linux.timespec = .{ .sec = 1, .nsec = 0 };
            _ = std.os.linux.nanosleep(&ts, null);
            continue;
        };
        defer inv.deinit(allocator);

        const response = handle(io, allocator, env, &inv, &pool) catch |err| {
            client.postError(inv.request_id, "HandlerError", @errorName(err)) catch |perr| {
                std.debug.print("zpq lambda: postError failed: {s}\n", .{@errorName(perr)});
            };
            continue;
        };
        defer allocator.free(response);

        client.postResponse(inv.request_id, response) catch |err| {
            std.debug.print("zpq lambda: postResponse failed: {s}\n", .{@errorName(err)});
        };
    }
}

fn handle(
    io: std.Io,
    allocator: std.mem.Allocator,
    env: std.process.Environ,
    inv: *const runtime.Invocation,
    pool: *PersistentPool,
) ![]u8 {
    if (inv.body.len == 0) {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"empty_body\"}}", .{});
    }

    const trimmed = std.mem.trim(u8, inv.body, " \r\n\t");

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

        const max_memory: ?usize = if (req.max_memory) |m| switch (m) {
            .bytes => |n| n,
            .text => |t| zpq.core.system.parseSizeString(t) catch return std.fmt.allocPrint(
                allocator,
                "{{\"error\":\"bad_json\",\"reason\":\"BadFieldType\",\"field\":\"max_memory\"}}",
                .{},
            ),
        } else null;

        // Single source of query options: the JSON event maps onto the
        // SAME `engine.QueryArgs` the CLI's flags populate (src/cli/main.zig).
        // Convention: CLI `--scan-all` ↔ JSON `"scan_all"` ↔ field `scan_all`.
        // A new option is one field on QueryArgs + one parse line on each
        // surface — the handlers below pass `qa` straight through to the
        // engine, so options like `scan_all` flow without touching them.
        const qa: engine.QueryArgs = .{
            .inputs = req.inputs,
            .filter = req.filter,
            .output = req.output_url,
            .columns = req.columns,
            .select = req.select,
            .aggregate = req.aggregate,
            .codec = output_codec,
            .scan_all = req.scan_all,
            .trust_stats = req.trust_stats,
            .max_memory = max_memory orelse zpq.core.system.discoverAvailableMemory(env),
            .group_by = req.group_by,
            .column_order = req.column_order,
        };

        // or group-by wins, then write, then the legacy single-file diagnostics.
        if (qa.aggregate != null or qa.group_by != null) return try lambdaAggregate(io, allocator, env, pool, qa);
        if (qa.output != null) return try lambdaWrite(io, allocator, env, pool, qa);
        if (req.inputs.len > 0)
            return try handleS3(allocator, env, req.inputs[0], qa.filter);
        return try aggregateInt8(allocator, inv.body, null);
    }

    // Raw-byte fallback used by the in-process integration test.
    return try aggregateInt8(allocator, inv.body, null);
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
    try w.writeByte('}');
    return aw.toOwnedSlice();
}

/// Adapter that lets engine.runQuery use the lambda's PersistentPool
/// (warm-container connection reuse) without engine knowing about it.
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

/// Lambda's aggregate handler — one shape for single-file and multi-
/// file. Same code path as the CLI's `zpq query 's3://...'`, just with
/// a runtime API JSON envelope instead of stdout.
fn lambdaAggregate(
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

    pool.resetStats();
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
    try writePoolStats(w, pool.stats());
    try w.writeByte('}');
    return aw.toOwnedSlice();
}

/// Lambda's write handler — the iceberg-compaction shape (filter +
/// project + write S3-to-S3). Same engine path as the CLI's
/// `zpq query 's3://...' -o 's3://...'`, with the Lambda runtime API
/// as the envelope.
fn lambdaWrite(
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

    pool.resetStats();
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
    try writePoolStats(w, stats);
    try w.writeByte('}');
    return aw.toOwnedSlice();
}

/// The `"pool"` member: this invocation's connection reuse, idle evictions and retries. Cold TLS handshakes against
/// warm reuse, and stale or backed-off retries, are the first things to read when warm-path latency changes.
fn writePoolStats(w: *std.Io.Writer, stats: s3.Pool(POOL_SIZE).Stats) !void {
    try w.print(
        "\"pool\":{{\"acquires\":{d},\"opens\":{d},\"reuses\":{d},\"discards\":{d},\"idle_evictions\":{d}," ++
            "\"stale_retries\":{d},\"backoff_retries\":{d},\"max_idle_ms\":{d},\"acquire_wait_ms\":{d},\"acquire_lock_ms\":{d}}}",
        .{
            stats.acquires,
            stats.opens,
            stats.reuses,
            stats.discards,
            stats.idle_evictions,
            stats.stale_retries,
            stats.backoff_retries,
            stats.max_idle_ns / std.time.ns_per_ms,
            stats.acquire_wait_ns / std.time.ns_per_ms,
            stats.acquire_lock_ns / std.time.ns_per_ms,
        },
    );
}

/// `{"error":kind,"reason":reason,"<key>":value}`: an early-exit response naming the field or text it rejects. `kind`
/// and `reason` are fixed names; `value` comes from the request, so it is escaped.
fn rejectResponse(allocator: std.mem.Allocator, kind: []const u8, reason: []const u8, key: []const u8, value: []const u8) ![]u8 {
    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();
    const w = &aw.writer;
    try w.print("{{\"error\":\"{s}\",\"reason\":\"{s}\",\"{s}\":", .{ kind, reason, key });
    try engine.writeJsonQuoted(w, value);
    try w.writeByte('}');
    return aw.toOwnedSlice();
}

fn handleS3(
    allocator: std.mem.Allocator,
    env: std.process.Environ,
    s3_url: []const u8,
    filter_str: ?[]const u8,
) ![]u8 {
    const url = s3.Url.parse(s3_url) catch |err| {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"bad_s3_url\",\"reason\":\"{s}\"}}", .{@errorName(err)});
    };
    const creds = s3.Credentials.fromEnv(env) catch |err| {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"no_credentials\",\"reason\":\"{s}\"}}", .{@errorName(err)});
    };

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var client = s3.Client.init(a, creds, url.bucket) catch |err| {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"client_init\",\"reason\":\"{s}\"}}", .{@errorName(err)});
    };
    defer client.deinit();

    // 1. Tail GET to discover total size and pull the footer.
    const tail_resp = client.get(a, url.key, s3.Range.suffix(TAIL_SIZE)) catch |err| {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"tail_fetch\",\"reason\":\"{s}\"}}", .{@errorName(err)});
    };
    if (tail_resp.status != 206 and tail_resp.status != 200) {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"tail_status\",\"status\":{d}}}", .{tail_resp.status});
    }

    const total_size = parseTotalFromContentRange(tail_resp.header("Content-Range")) catch |err| {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"bad_content_range\",\"reason\":\"{s}\"}}", .{@errorName(err)});
    };

    // 2. Allocate sparse file buffer; stamp the tail.
    const file_buf = try allocator.alloc(u8, total_size);
    defer allocator.free(file_buf);
    const tail_start = total_size - tail_resp.body.len;
    @memcpy(file_buf[tail_start..], tail_resp.body);

    // 3. Locate the footer and fetch the missing prefix if needed.
    if (tail_resp.body.len < 8) return error.TailTooSmall;
    const tail = tail_resp.body;
    if (!std.mem.eql(u8, tail[tail.len - 4 ..], "PAR1")) return error.NotParquet;
    const footer_len: u64 = std.mem.readInt(u32, tail[tail.len - 8 ..][0..4], .little);
    // The length is the file's claim; one longer than the object would underflow the start below.
    if (footer_len + 8 > total_size) return error.NotParquet;
    const footer_actual_start = total_size - 8 - footer_len;
    if (footer_actual_start < tail_start) {
        const need = try client.get(a, url.key, s3.Range.span(footer_actual_start, tail_start - 1));
        if (need.status != 206) return error.RangeStatus;
        @memcpy(file_buf[footer_actual_start..tail_start], need.body);
    }
    const head = try client.get(a, url.key, s3.Range.span(0, 7));
    if (head.status != 206) return error.RangeStatus;
    @memcpy(file_buf[0..head.body.len], head.body);

    var meta = try metadata.open(a, file_buf);
    defer meta.deinit(a);

    // 4. Parse the filter (if any) against the schema.
    var filter: ?filter_ast.Filter = null;
    if (filter_str) |fs| {
        if (fs.len > 0) {
            filter = filter_parser.parse(a, fs, &meta, null) catch |err| {
                return rejectResponse(allocator, "filter_parse", @errorName(err), "expr", fs);
            };
        }
    }

    // 5. Find target column index (the column we aggregate over).
    const target_idx = metadata.findColumnIndex(&meta, TARGET_COLUMN) orelse {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"target_missing\"}}", .{});
    };

    // 6. Collect filter columns (if any). Dedup against target.
    var filter_cols: std.ArrayList(usize) = .empty;
    if (filter) |f| try f.collectColumns(&filter_cols, a);

    // Dedup + ensure target is included.
    var fetch_cols: std.ArrayList(usize) = .empty;
    try fetch_cols.append(a, target_idx);
    for (filter_cols.items) |ci| {
        if (std.mem.indexOfScalar(usize, fetch_cols.items, ci) == null) {
            try fetch_cols.append(a, ci);
        }
    }

    // 7. Walk row groups: prune (if filter), then fetch + decode +
    //    eval + aggregate.
    var rg_pruned: usize = 0;
    var rows_seen: i64 = 0;
    var rows_matched: i64 = 0;
    var min_v: i32 = std.math.maxInt(i32);
    var max_v: i32 = std.math.minInt(i32);
    var sum: i64 = 0;

    for (meta.row_groups.items) |rg| {
        if (filter) |f| {
            const decision = filter_prune.pruneRowGroup(&rg, f, &meta);
            if (decision == .skip) {
                rg_pruned += 1;
                continue;
            }
        }

        // Fetch all needed column chunks for this row group.
        var ranges: std.ArrayList(coalescer.Range) = .empty;
        for (fetch_cols.items) |ci| {
            const col_meta = rg.columns.items[ci].meta_data orelse continue;
            const start: u64 = if (col_meta.dictionary_page_offset) |dp|
                @intCast(dp)
            else
                @intCast(col_meta.data_page_offset);
            const len: u64 = @intCast(col_meta.total_compressed_size);
            try ranges.append(a, .{ .start = start, .end = start + len });
        }
        const merged = try coalescer.Coalescer.coalesce(a, ranges.items, COALESCE_GAP);
        for (merged) |r| {
            if (r.start >= tail_start) continue;
            const fetch_end_excl = @min(r.end, tail_start);
            const resp = try client.get(a, url.key, s3.Range.span(r.start, fetch_end_excl - 1));
            if (resp.status != 206) return error.RangeStatus;
            @memcpy(file_buf[r.start..fetch_end_excl], resp.body);
        }

        // Decode each fetch_col into a typed slice. For our demo,
        // values for the target are always INT32 (int8 logical type
        // stored as INT32 physical).
        var rg_arena = std.heap.ArenaAllocator.init(allocator);
        defer rg_arena.deinit();
        const ra = rg_arena.allocator();

        const num_rows: usize = @intCast(rg.num_rows);
        var sel = try filter_selection.SelectionVector.init(ra, num_rows);

        // Decode columns referenced by the filter; build a Batch.
        const col_count = fetch_cols.items.len;
        var batch_cols: std.ArrayList(filter_eval.Batch.Column) = .empty;
        try batch_cols.ensureTotalCapacity(ra, col_count);
        var lookup = try ra.alloc(?usize, meta.schema.items.len);
        @memset(lookup, null);

        var target_values: ?[]const i32 = null;

        for (fetch_cols.items, 0..) |ci, batch_pos| {
            const col_meta = rg.columns.items[ci].meta_data orelse return error.ColumnMetaMissing;
            const chunk_start: usize = if (col_meta.dictionary_page_offset) |dp| @intCast(dp) else @intCast(col_meta.data_page_offset);
            const chunk_len: usize = @intCast(col_meta.total_compressed_size);
            if (chunk_start + chunk_len > file_buf.len) return error.InvalidColumnOffsets;
            const chunk = file_buf[chunk_start .. chunk_start + chunk_len];

            // Use the column-chunk's full path_in_schema so nested
            // columns (struct.field) resolve to correct max_def.
            // Single-element paths (flat columns) work identically.
            const levels = meta.getColumnLevels(col_meta.path_in_schema.items);

            // For flat / struct columns, num_values == num_rows.
            // For LIST/MAP, num_values is the LEAF count which can
            // exceed num_rows. Use it directly as the per-call buffer
            // size; the existing flat path is unchanged.
            const n_leaves: usize = @intCast(col_meta.num_values);

            const pt = col_meta.type;
            const decoded: filter_eval.Batch.Column = switch (pt) {
                .INT32 => blk: {
                    const c = try consumer.decodeColumnT(i32, ra, chunk, col_meta.codec, levels, n_leaves);
                    if (ci == target_idx) target_values = c.values;
                    break :blk .{ .i32 = c };
                },
                .INT64 => .{ .i64 = try consumer.decodeColumnT(i64, ra, chunk, col_meta.codec, levels, n_leaves) },
                .FLOAT => .{ .f32 = try consumer.decodeColumnT(f32, ra, chunk, col_meta.codec, levels, n_leaves) },
                .DOUBLE => .{ .f64 = try consumer.decodeColumnT(f64, ra, chunk, col_meta.codec, levels, n_leaves) },
                .BYTE_ARRAY => .{ .string = try consumer.decodeColumnT([]const u8, ra, chunk, col_meta.codec, levels, n_leaves) },
                .BOOLEAN => .{ .boolean = try consumer.decodeColumnT(bool, ra, chunk, col_meta.codec, levels, n_leaves) },
                .FIXED_LEN_BYTE_ARRAY => .{ .string = try consumer.decodeFlbaColumn(ra, chunk, col_meta.codec, levels, n_leaves, consumer.flbaWidth(meta.getColumnSchema(col_meta.path_in_schema.items))) },
                else => return error.UnsupportedColumnType,
            };
            try consumer.checkRowShape(decoded, num_rows);
            try batch_cols.append(ra, decoded);
            lookup[ci] = batch_pos;
        }

        const batch: filter_eval.Batch = .{ .cols = batch_cols.items, .num_rows = num_rows };

        // Apply filter (or leave selection all-active).
        if (filter) |f| try filter_eval.evaluate(f, &batch, &sel, lookup, ra);

        // Aggregate target values masked by selection.
        const tv = target_values orelse return error.TargetNotDecoded;
        for (tv, 0..) |v, i| {
            if (sel.isActive(i)) {
                if (v < min_v) min_v = v;
                if (v > max_v) max_v = v;
                sum += v;
                rows_matched += 1;
            }
            rows_seen += 1;
        }
    }

    // Emit response. Min/max are only meaningful when rows_matched > 0.
    if (rows_matched == 0) {
        return std.fmt.allocPrint(
            allocator,
            "{{\"ok\":true,\"column\":\"{s}\",\"rows_seen\":{d},\"rows_matched\":0,\"row_groups_pruned\":{d},\"row_groups\":{d}}}",
            .{ TARGET_COLUMN, rows_seen, rg_pruned, meta.row_groups.items.len },
        );
    }
    return std.fmt.allocPrint(
        allocator,
        "{{\"ok\":true,\"column\":\"{s}\",\"rows_seen\":{d},\"rows_matched\":{d},\"min\":{d},\"max\":{d},\"sum\":{d},\"row_groups_pruned\":{d},\"row_groups\":{d}}}",
        .{ TARGET_COLUMN, rows_seen, rows_matched, min_v, max_v, sum, rg_pruned, meta.row_groups.items.len },
    );
}

const POOL_SIZE: usize = s3.MAX_PARTS;

fn initPool(
    self: *s3.Pool(POOL_SIZE),
    arena: std.mem.Allocator,
    creds: s3.Credentials,
    bucket: []const u8,
) !void {
    _ = creds;
    _ = bucket;
    try self.init(arena);
}

fn parseTotalFromContentRange(cr_or_null: ?[]const u8) !u64 {
    const cr = cr_or_null orelse return error.NoContentRange;
    const slash = std.mem.indexOfScalar(u8, cr, '/') orelse return error.BadContentRange;
    const total = std.mem.trim(u8, cr[slash + 1 ..], " \t");
    if (total.len == 0 or total[0] == '*') return error.BadContentRange;
    return std.fmt.parseInt(u64, total, 10) catch error.BadContentRange;
}

/// Local-fixture path: decode the whole file (it's already in memory).
/// Filter not supported on this path — used only by integration tests.
fn aggregateInt8(allocator: std.mem.Allocator, file_bytes: []const u8, _: ?filter_ast.Filter) ![]u8 {
    if (file_bytes.len < 12) {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"too_small\",\"len\":{d}}}", .{file_bytes.len});
    }

    var meta = metadata.open(allocator, file_bytes) catch |err| {
        return std.fmt.allocPrint(
            allocator,
            "{{\"error\":\"open_failed\",\"reason\":\"{s}\",\"len\":{d}}}",
            .{ @errorName(err), file_bytes.len },
        );
    };
    defer meta.deinit(allocator);

    const target_idx = metadata.findColumnIndex(&meta, TARGET_COLUMN) orelse {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"target_missing\"}}", .{});
    };

    var total_rows: i64 = 0;
    var min_v: i32 = std.math.maxInt(i32);
    var max_v: i32 = std.math.minInt(i32);
    var sum: i64 = 0;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const path_arr: [1][]const u8 = .{TARGET_COLUMN};
    const levels = meta.getColumnLevels(&path_arr);

    for (meta.row_groups.items) |rg| {
        _ = arena.reset(.retain_capacity);
        const col = rg.columns.items[target_idx].meta_data orelse return error.ColumnMetaMissing;
        const chunk_start: usize = if (col.dictionary_page_offset) |dp| @intCast(dp) else @intCast(col.data_page_offset);
        const chunk_len: usize = @intCast(col.total_compressed_size);
        if (chunk_start + chunk_len > file_bytes.len) return error.InvalidColumnOffsets;
        const chunk = file_bytes[chunk_start .. chunk_start + chunk_len];

        var reader = column_mod.ColumnChunkReader(i32).init(chunk, col.codec, levels, arena.allocator());
        reader.value_budget = @intCast(col.num_values);
        var batch: [4096]i32 = undefined;
        var def_batch: [4096]u32 = undefined;
        const max_def: u32 = @intCast(levels.max_def);
        while (true) {
            const n = if (max_def > 0)
                try reader.decodeWithLevels(&batch, &def_batch)
            else
                try reader.decode(&batch);
            if (n == 0) break;
            for (batch[0..n], 0..) |v, i| {
                if (max_def > 0 and def_batch[i] < max_def) continue; // null
                if (v < min_v) min_v = v;
                if (v > max_v) max_v = v;
                sum += v;
                total_rows += 1;
            }
            // For REQUIRED, total_rows incremented inside loop too.
        }
    }

    return std.fmt.allocPrint(
        allocator,
        "{{\"ok\":true,\"column\":\"{s}\",\"rows\":{d},\"min\":{d},\"max\":{d},\"sum\":{d},\"row_groups\":{d}}}",
        .{ TARGET_COLUMN, total_rows, min_v, max_v, sum, meta.row_groups.items.len },
    );
}

/// Parse `body` as one JSON object and return its string field `key`, failing unless both hold.
fn expectStringField(body: []const u8, key: []const u8, want: []const u8) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, body, .{});
    defer parsed.deinit();
    const got = parsed.value.object.get(key) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(want, got.string);
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
    _ = @import("runtime.zig");
    _ = @import("scan.zig");
}
