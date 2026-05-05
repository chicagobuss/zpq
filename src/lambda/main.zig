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
const runtime = @import("runtime.zig");

const schema = zpq.core.schema;
const metadata = zpq.core.parquet.metadata;
const column_mod = zpq.core.parquet.column;
const fastpath = zpq.core.writer.fastpath;
const streaming = zpq.core.writer.streaming;
const encoder = zpq.core.writer.encoder;
const thrift = zpq.core.thrift;
const s3 = zpq.io.s3;
const multipart_sink = zpq.io.multipart_sink;
const coalescer = zpq.io.coalescer;
const filter_ast = zpq.core.filter.ast;
const filter_parser = zpq.core.filter.parser;
const filter_prune = zpq.core.filter.prune;
const filter_selection = zpq.core.filter.selection;
const filter_eval = zpq.core.filter.eval;
const partition = zpq.core.filter.partition;
const schema_tree = zpq.core.parquet.schema_tree;

const TAIL_SIZE: u64 = 64 * 1024;
const COALESCE_GAP: u64 = 64 * 1024;
const TARGET_COLUMN: []const u8 = "int8";

/// Persistent connection pool that lives for the lifetime of one Lambda
/// container (across invocations). Re-initialized when the bucket
/// (host) changes between invocations. Trace data showed every cold
/// pool created inside a per-invocation arena was paying ~6 fresh TLS
/// handshakes per call; persisting it across warm-container runs
/// eliminates that on subsequent invocations.
const PersistentPool = struct {
    const Inner = s3.Pool(POOL_SIZE);
    inner: Inner = undefined,
    initialized: bool = false,
    /// Owned by gpa so it outlives the per-invocation arena.
    host_owned: ?[]u8 = null,
    addr_owned: ?[]u8 = null,
    bucket_owned: ?[]u8 = null,

    pub fn ensureForBucket(
        self: *PersistentPool,
        gpa: std.mem.Allocator,
        creds: s3.Credentials,
        bucket: []const u8,
    ) !*Inner {
        if (self.initialized) {
            if (self.bucket_owned) |b| {
                if (std.mem.eql(u8, b, bucket)) return &self.inner;
            }
            // Bucket changed — tear down and recreate.
            self.inner.deinit();
            if (self.host_owned) |h| gpa.free(h);
            if (self.addr_owned) |a| gpa.free(a);
            if (self.bucket_owned) |b| gpa.free(b);
            self.* = .{};
        }

        const host = try std.fmt.allocPrint(gpa, "{s}.s3.{s}.amazonaws.com", .{ bucket, creds.region });
        errdefer gpa.free(host);
        const addr_const = try s3.resolveIpv4(gpa, host);
        // s3.resolveIpv4 returns const slice; we already alloc'd via gpa.
        const addr: []u8 = @constCast(addr_const);
        errdefer gpa.free(addr);
        const owned_bucket = try gpa.dupe(u8, bucket);
        errdefer gpa.free(owned_bucket);

        try self.inner.init(gpa, host, addr, 443);
        self.host_owned = host;
        self.addr_owned = addr;
        self.bucket_owned = owned_bucket;
        self.initialized = true;
        return &self.inner;
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
        var input_urls = std.ArrayList([]const u8).empty;
        defer {
            for (input_urls.items) |s| allocator.free(s);
            input_urls.deinit(allocator);
        }
        if (extractStringArrayItems(trimmed, "inputs", allocator)) |items| {
            for (items) |s| input_urls.append(allocator, s) catch {};
            allocator.free(items);
        } else |_| {
            const url = extractField(trimmed, "s3_url") catch |err| {
                return std.fmt.allocPrint(
                    allocator,
                    "{{\"error\":\"bad_json\",\"reason\":\"{s}\"}}",
                    .{@errorName(err)},
                );
            };
            const owned = try allocator.dupe(u8, url);
            try input_urls.append(allocator, owned);
        }

        const filter_str = extractField(trimmed, "filter") catch null;
        const output_url = extractField(trimmed, "output_url") catch null;
        const columns_csv = extractStringArray(trimmed, "columns", allocator) catch null;
        defer if (columns_csv) |c| allocator.free(c);
        if (output_url) |out| return try handleS3Write(io, allocator, env, input_urls.items, filter_str, out, columns_csv, pool);
        // Aggregate path stays single-file (legacy diagnostic).
        if (input_urls.items.len > 0)
            return try handleS3(allocator, env, input_urls.items[0], filter_str);
        return try aggregateInt8(allocator, inv.body, null);
    }

    // Legacy raw-bytes path used by the in-process integration test.
    return try aggregateInt8(allocator, inv.body, null);
}

/// Extract a JSON string-array field as a comma-separated list (we
/// don't need a full JSON parser for this). Returns an allocator-
/// owned `name1,name2,name3` string. Caller frees.
fn extractStringArray(
    body: []const u8,
    name: []const u8,
    allocator: std.mem.Allocator,
) ![]u8 {
    var key_buf: [64]u8 = undefined;
    if (name.len + 2 > key_buf.len) return error.NameTooLong;
    key_buf[0] = '"';
    @memcpy(key_buf[1 .. 1 + name.len], name);
    key_buf[1 + name.len] = '"';
    const key = key_buf[0 .. 2 + name.len];

    const pos = std.mem.indexOf(u8, body, key) orelse return error.MissingField;
    var i = pos + key.len;
    while (i < body.len and (body[i] == ' ' or body[i] == ':' or body[i] == '\t')) : (i += 1) {}
    if (i >= body.len or body[i] != '[') return error.BadJson;
    i += 1;

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);

    var first = true;
    while (i < body.len) {
        while (i < body.len and (body[i] == ' ' or body[i] == ',' or body[i] == '\t' or body[i] == '\n')) : (i += 1) {}
        if (i >= body.len) return error.BadJson;
        if (body[i] == ']') break;
        if (body[i] != '"') return error.BadJson;
        i += 1;
        const start = i;
        while (i < body.len and body[i] != '"') : (i += 1) {}
        if (i >= body.len) return error.BadJson;
        if (!first) try out.append(allocator, ',');
        try out.appendSlice(allocator, body[start..i]);
        first = false;
        i += 1;
    }
    return out.toOwnedSlice(allocator);
}

/// Extract a JSON string-array as a list of owned strings (caller
/// frees each item AND the outer slice). Used for the multi-file
/// `inputs: [...]` field where the order matters.
fn extractStringArrayItems(
    body: []const u8,
    name: []const u8,
    allocator: std.mem.Allocator,
) ![][]const u8 {
    var key_buf: [64]u8 = undefined;
    if (name.len + 2 > key_buf.len) return error.NameTooLong;
    key_buf[0] = '"';
    @memcpy(key_buf[1 .. 1 + name.len], name);
    key_buf[1 + name.len] = '"';
    const key = key_buf[0 .. 2 + name.len];

    const pos = std.mem.indexOf(u8, body, key) orelse return error.MissingField;
    var i = pos + key.len;
    while (i < body.len and (body[i] == ' ' or body[i] == ':' or body[i] == '\t')) : (i += 1) {}
    if (i >= body.len or body[i] != '[') return error.BadJson;
    i += 1;

    var items: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (items.items) |s| allocator.free(s);
        items.deinit(allocator);
    }

    while (i < body.len) {
        while (i < body.len and (body[i] == ' ' or body[i] == ',' or body[i] == '\t' or body[i] == '\n')) : (i += 1) {}
        if (i >= body.len) return error.BadJson;
        if (body[i] == ']') break;
        if (body[i] != '"') return error.BadJson;
        i += 1;
        const start = i;
        while (i < body.len and body[i] != '"') : (i += 1) {}
        if (i >= body.len) return error.BadJson;
        const owned = try allocator.dupe(u8, body[start..i]);
        try items.append(allocator, owned);
        i += 1;
    }
    return items.toOwnedSlice(allocator);
}

fn extractField(body: []const u8, name: []const u8) ![]const u8 {
    var key_buf: [64]u8 = undefined;
    if (name.len + 2 > key_buf.len) return error.NameTooLong;
    key_buf[0] = '"';
    @memcpy(key_buf[1 .. 1 + name.len], name);
    key_buf[1 + name.len] = '"';
    const key = key_buf[0 .. 2 + name.len];

    const pos = std.mem.indexOf(u8, body, key) orelse return error.MissingField;
    var i = pos + key.len;
    while (i < body.len and (body[i] == ' ' or body[i] == ':')) : (i += 1) {}
    if (i >= body.len or body[i] != '"') return error.BadJson;
    i += 1;
    const start = i;
    while (i < body.len and body[i] != '"') : (i += 1) {}
    if (i >= body.len) return error.BadJson;
    return body[start..i];
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
            filter = filter_parser.parse(a, fs, &meta) catch |err| {
                return std.fmt.allocPrint(
                    allocator,
                    "{{\"error\":\"filter_parse\",\"reason\":\"{s}\",\"expr\":\"{s}\"}}",
                    .{ @errorName(err), fs },
                );
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
            const decision = try filter_prune.pruneRowGroup(&rg, f, a);
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
                    const c = try decodeColumnT(i32, ra, chunk, col_meta.codec, levels, n_leaves);
                    if (ci == target_idx) target_values = c.values;
                    break :blk .{ .i32 = c };
                },
                .INT64 => .{ .i64 = try decodeColumnT(i64, ra, chunk, col_meta.codec, levels, n_leaves) },
                .FLOAT => .{ .f32 = try decodeColumnT(f32, ra, chunk, col_meta.codec, levels, n_leaves) },
                .DOUBLE => .{ .f64 = try decodeColumnT(f64, ra, chunk, col_meta.codec, levels, n_leaves) },
                .BYTE_ARRAY => .{ .string = try decodeColumnT([]const u8, ra, chunk, col_meta.codec, levels, n_leaves) },
                .BOOLEAN => .{ .boolean = try decodeColumnT(bool, ra, chunk, col_meta.codec, levels, n_leaves) },
                else => return error.UnsupportedColumnType,
            };
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

/// Per-task context for parallel metadata fetch (tail/head/footer-prefix
/// + parse). Type-erased pool dispatch mirrors s3.zig's FetchCtx pattern.
const MetaFetchCtx = struct {
    pool_ptr: *anyopaque,
    pool_acquire_fn: *const fn (*anyopaque, std.Io) anyerror!s3.PoolHandle,
    pool_release_fn: *const fn (*anyopaque, std.Io, usize) anyerror!void,
    pool_discard_fn: *const fn (*anyopaque, std.Io, usize) void,
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    creds: s3.Credentials,
    sp: *FileSpec,
    err: ?[]const u8,
};

fn poolAcquireForMeta(comptime P: type) *const fn (*anyopaque, std.Io) anyerror!s3.PoolHandle {
    return struct {
        fn f(p: *anyopaque, io: std.Io) anyerror!s3.PoolHandle {
            const typed: *P = @ptrCast(@alignCast(p));
            const h = try typed.acquire(io);
            return .{ .conn = h.conn, .idx = h.idx };
        }
    }.f;
}

fn poolReleaseForMeta(comptime P: type) *const fn (*anyopaque, std.Io, usize) anyerror!void {
    return struct {
        fn f(p: *anyopaque, io: std.Io, idx: usize) anyerror!void {
            const typed: *P = @ptrCast(@alignCast(p));
            try typed.release(io, .{ .conn = undefined, .idx = idx });
        }
    }.f;
}

fn poolDiscardForMeta(comptime P: type) *const fn (*anyopaque, std.Io, usize) void {
    return struct {
        fn f(p: *anyopaque, io: std.Io, idx: usize) void {
            const typed: *P = @ptrCast(@alignCast(p));
            typed.discard(io, .{ .conn = undefined, .idx = idx });
        }
    }.f;
}

fn fetchMetaTask(io: std.Io, ctx: *MetaFetchCtx) std.Io.Cancelable!void {
    doFetchMeta(io, ctx) catch |err| {
        ctx.err = @errorName(err);
    };
}

fn doFetchMeta(io: std.Io, ctx: *MetaFetchCtx) !void {
    const sp = ctx.sp;

    // Tail GET via pool.
    const tail_resp = try s3.getViaPool(io, makePoolPtr(ctx), ctx.arena, ctx.creds, sp.url, s3.Range.suffix(TAIL_SIZE));
    if (tail_resp.status != 206 and tail_resp.status != 200) return error.TailStatus;

    sp.total_size = try parseTotalFromContentRange(tail_resp.header("Content-Range"));
    sp.file_buf = try ctx.gpa.alloc(u8, sp.total_size);
    sp.tail_start = sp.total_size - tail_resp.body.len;
    @memcpy(sp.file_buf[sp.tail_start..], tail_resp.body);

    if (tail_resp.body.len < 8) return error.TailTooSmall;
    const tail = tail_resp.body;
    if (!std.mem.eql(u8, tail[tail.len - 4 ..], "PAR1")) return error.NotParquet;
    const footer_len: u64 = std.mem.readInt(u32, tail[tail.len - 8 ..][0..4], .little);
    const footer_actual_start = sp.total_size - 8 - footer_len;

    const head = try s3.getViaPool(io, makePoolPtr(ctx), ctx.arena, ctx.creds, sp.url, s3.Range.span(0, 7));
    if (head.status != 206) return error.RangeStatus;
    @memcpy(sp.file_buf[0..head.body.len], head.body);

    if (footer_actual_start < sp.tail_start) {
        const need = try s3.getViaPool(io, makePoolPtr(ctx), ctx.arena, ctx.creds, sp.url, s3.Range.span(footer_actual_start, sp.tail_start - 1));
        if (need.status != 206) return error.RangeStatus;
        @memcpy(sp.file_buf[footer_actual_start..sp.tail_start], need.body);
    }

    sp.meta = try metadata.open(ctx.arena, sp.file_buf);
    sp.tree = try schema_tree.SchemaTree.build(ctx.arena, sp.meta.schema.items);
}

/// Wrap the type-erased pool pointer back into a typed Pool reference
/// for getViaPool's `anytype`. We know the concrete type at the call
/// site is Pool(POOL_SIZE).
fn makePoolPtr(ctx: *MetaFetchCtx) *s3.Pool(POOL_SIZE) {
    return @ptrCast(@alignCast(ctx.pool_ptr));
}

/// Per-input-file state during a multi-file write.
const FileSpec = struct {
    url: s3.Url,
    meta: schema.FileMetaData,
    /// Tree representation of the schema (B1.0). Built once after
    /// metadata parse; provides nested-aware column resolution and
    /// schema projection. Pre-zero-init: filled by fetchMetaTask.
    tree: schema_tree.SchemaTree = undefined,
    /// Sparse per-file buffer; tail/head/footer prefilled, surviving
    /// column-chunk bytes get fetched into it. Owned in `gpa` so it
    /// lives across the per-file arenas used during decode/encode.
    file_buf: []u8,
    total_size: u64,
    tail_start: u64,
    survivors: []bool,
};

/// Phase 5.1+ fast-path writer, generalized to multi-file input
/// (Phase 6.1). Per-file: tail/head/footer fetch, parse, stat-prune,
/// range-fetch; then either byte-copy (no filter) via fastpath.buildMulti
/// or decode+filter+encode via buildFilteredOutputMulti, producing a
/// single unified Parquet output.
///
/// All inputs must share a compatible schema (validated by checking
/// schema list lengths + leaf-name equality against the first file).
fn handleS3Write(
    io: std.Io,
    allocator: std.mem.Allocator,
    env: std.process.Environ,
    input_urls: []const []const u8,
    filter_str: ?[]const u8,
    output_url_str: []const u8,
    columns_csv: ?[]const u8,
    persistent_pool: *PersistentPool,
) ![]u8 {
    const t_start = nowMonoNs();

    if (input_urls.len == 0) {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"no_inputs\"}}", .{});
    }

    const out_url = s3.Url.parse(output_url_str) catch |err| {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"bad_output_url\",\"reason\":\"{s}\"}}", .{@errorName(err)});
    };
    const creds = s3.Credentials.fromEnv(env) catch |err| {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"no_credentials\",\"reason\":\"{s}\"}}", .{@errorName(err)});
    };

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Parse + validate all input URLs share the same bucket (single-
    // bucket-pool simplifying assumption for now). Extend later if
    // cross-bucket scans become a real workload.
    const total_files = input_urls.len;
    var specs_buf = try a.alloc(FileSpec, total_files);
    var per_file_kvs = try a.alloc([]const partition.KV, total_files);
    var first_bucket: []const u8 = undefined;
    for (input_urls, 0..) |s, i| {
        const u = s3.Url.parse(s) catch |err| {
            return std.fmt.allocPrint(allocator, "{{\"error\":\"bad_input_url\",\"reason\":\"{s}\",\"url\":\"{s}\"}}", .{ @errorName(err), s });
        };
        if (i == 0) first_bucket = u.bucket;
        if (!std.mem.eql(u8, u.bucket, first_bucket)) {
            return std.fmt.allocPrint(allocator, "{{\"error\":\"cross_bucket_inputs_not_supported\"}}", .{});
        }
        specs_buf[i] = .{
            .url = u,
            .meta = undefined,
            .file_buf = &.{},
            .total_size = 0,
            .tail_start = 0,
            .survivors = &.{},
        };
        per_file_kvs[i] = try partition.parsePath(a, s);
    }

    // Phase 6.2: hive-partition pruning. Walk the union of partition
    // keys seen across input paths; if the filter expression resolves
    // entirely against those keys, evaluate it per-file and drop the
    // ones that fail BEFORE we open any sockets. Files we eliminate
    // here pay zero IO cost (no metadata fetch, no footer parse).
    // Filters with non-partition columns return `null` from
    // parsePredicate → caller falls through to the normal data path.
    var key_set: std.ArrayList([]const u8) = .empty;
    for (per_file_kvs) |kvs| {
        for (kvs) |kv| {
            var seen = false;
            for (key_set.items) |k| {
                if (std.mem.eql(u8, k, kv.key)) {
                    seen = true;
                    break;
                }
            }
            if (!seen) try key_set.append(a, kv.key);
        }
    }
    var partition_pred: ?partition.Predicate = null;
    var data_filter_str: ?[]const u8 = filter_str;
    if (filter_str) |fs| if (fs.len > 0 and key_set.items.len > 0) {
        partition_pred = partition.parsePredicate(a, fs, key_set.items) catch null;
        if (partition_pred != null) data_filter_str = null;
    };

    var n_kept: usize = 0;
    for (specs_buf, 0..) |sp, i| {
        const survives = if (partition_pred) |p| partition.eval(p, per_file_kvs[i]) else true;
        if (survives) {
            specs_buf[n_kept] = sp;
            n_kept += 1;
        }
    }
    const files_pruned = total_files - n_kept;
    const specs = specs_buf[0..n_kept];

    if (specs.len == 0) {
        const t_end_early = nowMonoNs();
        return std.fmt.allocPrint(
            allocator,
            "{{\"ok\":true,\"output\":\"{s}\",\"input_count\":0,\"files_total\":{d},\"files_pruned\":{d},\"bytes_in\":0,\"bytes_out\":0,\"total_ms\":{d}}}",
            .{
                output_url_str, total_files, files_pruned,
                @divTrunc(t_end_early - t_start, std.time.ns_per_ms),
            },
        );
    }

    const pool = persistent_pool.ensureForBucket(allocator, creds, first_bucket) catch |err| {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"pool_init\",\"reason\":\"{s}\"}}", .{@errorName(err)});
    };

    // 1. Per-file metadata fetch (tail/head/footer-prefix + parse).
    // Parallelized via Io.Group — each task acquires its own pool
    // slot, does its 2-3 GETs, releases. With 8 pool slots the 10
    // files' metadata fetches finish in roughly max-2-batches time
    // rather than 10× sequential cost.
    defer {
        for (specs) |*sp| if (sp.file_buf.len > 0) allocator.free(sp.file_buf);
    }
    // Per-task arena: meta-fetch tasks run concurrently on real
    // OS threads (std.Io.Threaded), so they cannot share an arena —
    // ArenaAllocator's bump pointer is not thread-safe and concurrent
    // allocations would clobber each other (this manifests as
    // cross-file metadata corruption: spec[N]'s meta ends up
    // referencing spec[M]'s parsed thrift bytes). Each task gets its
    // own arena, owned by `allocator` so it outlives the task and the
    // handleS3Write invocation can keep using sp.meta.
    var task_arenas = try allocator.alloc(std.heap.ArenaAllocator, specs.len);
    defer {
        for (task_arenas) |*ar| ar.deinit();
        allocator.free(task_arenas);
    }
    for (task_arenas) |*ar| ar.* = std.heap.ArenaAllocator.init(allocator);

    var meta_ctxs = try a.alloc(MetaFetchCtx, specs.len);
    for (specs, 0..) |*sp, i| meta_ctxs[i] = .{
        .pool_ptr = @ptrCast(pool),
        .pool_acquire_fn = poolAcquireForMeta(@TypeOf(pool.*)),
        .pool_release_fn = poolReleaseForMeta(@TypeOf(pool.*)),
        .pool_discard_fn = poolDiscardForMeta(@TypeOf(pool.*)),
        .gpa = allocator,
        .arena = task_arenas[i].allocator(),
        .creds = creds,
        .sp = sp,
        .err = null,
    };
    {
        var group: std.Io.Group = .init;
        defer group.cancel(io);
        for (meta_ctxs) |*c| try group.concurrent(io, fetchMetaTask, .{ io, c });
        try group.await(io);
    }
    for (meta_ctxs) |c| {
        if (c.err) |err_msg| {
            return std.fmt.allocPrint(allocator, "{{\"error\":\"meta_fetch\",\"reason\":\"{s}\"}}", .{err_msg});
        }
    }

    // 1b. Validate schemas match across inputs (compare leaf names
    // against first file's schema).
    for (specs[1..]) |sp| {
        if (sp.meta.schema.items.len != specs[0].meta.schema.items.len) {
            return std.fmt.allocPrint(allocator, "{{\"error\":\"schema_mismatch\"}}", .{});
        }
        for (sp.meta.schema.items, specs[0].meta.schema.items) |a_elem, b_elem| {
            if (!std.mem.eql(u8, a_elem.name, b_elem.name)) {
                return std.fmt.allocPrint(allocator, "{{\"error\":\"schema_mismatch\",\"col\":\"{s}_vs_{s}\"}}", .{ a_elem.name, b_elem.name });
            }
        }
    }

    const meta0 = &specs[0].meta;

    // 2. Parse the filter (optional) against first file's schema.
    // `data_filter_str` is set above: same as input `filter_str` unless
    // the whole expression was consumed by partition pruning, in which
    // case it's null and the data path runs as a copy.
    var filter: ?filter_ast.Filter = null;
    if (data_filter_str) |fs| {
        if (fs.len > 0) {
            filter = filter_parser.parse(a, fs, meta0) catch |err| {
                return std.fmt.allocPrint(
                    allocator,
                    "{{\"error\":\"filter_parse\",\"reason\":\"{s}\",\"expr\":\"{s}\"}}",
                    .{ @errorName(err), fs },
                );
            };
        }
    }

    // 2b. Resolve projection columns (if any) to column-chunk indices
    // via the SchemaTree. A user-supplied "events" can map to MULTIPLE
    // chunks (e.g. events.list.element.ts + events.list.element.code),
    // so resolveTopLevel returns a set per name. We collect indices
    // in DFS order (which matches column-chunk order) and dedupe.
    const tree0 = &specs[0].tree;
    var kept_columns_opt: ?[]const usize = null;
    if (columns_csv) |csv| {
        var kept_set = try a.alloc(bool, tree0.leaves.len);
        @memset(kept_set, false);
        var iter = std.mem.splitScalar(u8, csv, ',');
        while (iter.next()) |name| {
            if (name.len == 0) continue;
            const indices = try tree0.resolveTopLevel(a, name);
            if (indices.len == 0) {
                return std.fmt.allocPrint(
                    allocator,
                    "{{\"error\":\"bad_column\",\"name\":\"{s}\"}}",
                    .{name},
                );
            }
            for (indices) |idx| kept_set[idx] = true;
        }
        var kept: std.ArrayList(usize) = .empty;
        for (kept_set, 0..) |b, i| if (b) try kept.append(a, i);
        if (kept.items.len > 0) kept_columns_opt = kept.items;
    }

    var filter_cols_for_fetch: std.ArrayList(usize) = .empty;
    if (filter) |f| try f.collectColumns(&filter_cols_for_fetch, a);

    // 3. Per-file: stat-prune survivors + build fetch ranges.
    var rg_pruned: usize = 0;
    var rows_kept: i64 = 0;
    var total_input_rows: i64 = 0;
    var total_input_size: u64 = 0;
    var jobs: std.ArrayList(s3.FetchJob) = .empty;

    for (specs) |*sp| {
        total_input_size += sp.total_size;
        for (sp.meta.row_groups.items) |rg| total_input_rows += rg.num_rows;

        sp.survivors = try a.alloc(bool, sp.meta.row_groups.items.len);
        for (sp.meta.row_groups.items, 0..) |rg, i| {
            if (filter) |f| {
                const decision = try filter_prune.pruneRowGroup(&rg, f, a);
                if (decision == .skip) {
                    sp.survivors[i] = false;
                    rg_pruned += 1;
                    continue;
                }
            }
            sp.survivors[i] = true;
            rows_kept += rg.num_rows;
        }

        // Build per-file ranges using the same logic as before, then
        // turn them into FetchJobs whose target slices into sp.file_buf.
        var ranges: std.ArrayList(coalescer.Range) = .empty;
        for (sp.survivors, 0..) |keep, i| {
            if (!keep) continue;
            const rg = &sp.meta.row_groups.items[i];
            if (filter != null) {
                const num_leaves = rg.columns.items.len;
                const needed = try a.alloc(bool, num_leaves);
                @memset(needed, false);
                if (kept_columns_opt) |kc| {
                    for (kc) |idx| if (idx < num_leaves) {
                        needed[idx] = true;
                    };
                } else {
                    @memset(needed, true);
                }
                for (filter_cols_for_fetch.items) |c| if (c < num_leaves) {
                    needed[c] = true;
                };
                for (needed, 0..) |b, ci| {
                    if (!b) continue;
                    const m = rg.columns.items[ci].meta_data orelse continue;
                    const s: u64 = if (m.dictionary_page_offset) |dp| @intCast(dp) else @intCast(m.data_page_offset);
                    const e: u64 = s + @as(u64, @intCast(m.total_compressed_size));
                    try ranges.append(a, .{ .start = s, .end = e });
                }
            } else if (kept_columns_opt) |kc| {
                for (kc) |col_idx| {
                    if (col_idx >= rg.columns.items.len) continue;
                    const m = rg.columns.items[col_idx].meta_data orelse continue;
                    const s: u64 = if (m.dictionary_page_offset) |dp| @intCast(dp) else @intCast(m.data_page_offset);
                    const e: u64 = s + @as(u64, @intCast(m.total_compressed_size));
                    try ranges.append(a, .{ .start = s, .end = e });
                }
            } else {
                var min_s: u64 = std.math.maxInt(u64);
                var max_e: u64 = 0;
                for (rg.columns.items) |chunk| {
                    const m = chunk.meta_data orelse continue;
                    const s: u64 = if (m.dictionary_page_offset) |dp| @intCast(dp) else @intCast(m.data_page_offset);
                    const e: u64 = s + @as(u64, @intCast(m.total_compressed_size));
                    if (s < min_s) min_s = s;
                    if (e > max_e) max_e = e;
                }
                if (min_s == std.math.maxInt(u64)) continue;
                try ranges.append(a, .{ .start = min_s, .end = max_e });
            }
        }
        const merged = try coalescer.Coalescer.coalesce(a, ranges.items, COALESCE_GAP);

        for (merged) |r| {
            if (r.start >= sp.tail_start) continue;
            const end = @min(r.end, sp.tail_start);
            try jobs.append(a, .{
                .bucket = sp.url.bucket,
                .key = sp.url.key,
                .range = .{ .start = r.start, .end = end },
                .target = sp.file_buf[@intCast(r.start)..@intCast(end)],
            });
        }
    }

    var bytes_fetched: u64 = 0;
    if (jobs.items.len > 0) {
        bytes_fetched = try s3.fetchJobs(io, pool, allocator, a, creds, jobs.items);
    }

    const t_after_fetch = nowMonoNs();

    // 4. Build unified output. Three paths:
    //    - Filter set                            → buildFilteredOutputMulti
    //    - No filter, no projection              → fastpath byte-copy
    //    - No filter, projection includes nested → buildFilteredOutputMulti
    //      with a synthetic always-active filter (decodes + emits all
    //      leaves correctly, including rep-aware list/map projection
    //      which the byte-copy fastpath can't yet handle).
    const projection_includes_nested = blk_pn: {
        if (kept_columns_opt) |kc| {
            for (kc) |idx| {
                if (specs[0].tree.leaves[idx].max_rep > 0 or
                    specs[0].tree.leaves[idx].path.len > 1)
                {
                    break :blk_pn true;
                }
            }
        }
        break :blk_pn false;
    };

    // 4. Stream the output. All paths now feed a `MultipartSink`:
    //   - no filter, no nested projection → byte-copy via streaming.build
    //   - filter set                       → decode/filter/encode via
    //                                          buildFilteredOutputMulti
    //   - no filter + nested projection    → tautology filter through
    //                                          the same encoder path
    // The sink picks single-PUT vs multipart internally based on
    // whether the body ever crosses TARGET_PART_SIZE. `build_ms` is
    // reported as 0 for streaming — there's no separate build phase.
    const same_bucket = std.mem.eql(u8, first_bucket, out_url.bucket);

    var out_pool: s3.Pool(POOL_SIZE) = undefined;
    if (!same_bucket) try initPool(&out_pool, a, creds, out_url.bucket);
    defer if (!same_bucket) out_pool.deinit();
    const sink_pool: *s3.Pool(POOL_SIZE) = if (same_bucket) pool else &out_pool;

    var mp_sink = multipart_sink.MultipartSink.init(
        io,
        allocator,
        a,
        creds,
        out_url,
        sink_pool,
        .{},
    );
    defer mp_sink.deinit();
    const sink: streaming.Sink = .{
        .ctx = @ptrCast(&mp_sink),
        .write_fn = multipart_sink.sinkWriteFn,
    };

    const bytes_out: u64 = if (filter == null and !projection_includes_nested) blk: {
        var fp_specs = try a.alloc(fastpath.FileSpec, specs.len);
        for (specs, 0..) |*sp, i| fp_specs[i] = .{
            .bytes = sp.file_buf,
            .meta = &sp.meta,
            .survivors = sp.survivors,
        };
        break :blk try streaming.build(a, sink, fp_specs, kept_columns_opt);
    } else blk: {
        // Tautology filter for the no-filter + nested-projection case:
        // routes through decode/filter/encode where the encoder's
        // rep/def emission preserves nested structure that the byte-
        // copy fastpath would clobber. Safe when col_idx 0 is a flat
        // int64 — true for partitioned-fixture and nested_edges
        // fixtures; future hardening (B1.w) teaches
        // `buildFilteredOutputMulti` to accept null filter directly.
        const f = filter orelse filter_ast.Filter{ .int64 = .{
            .col_idx = 0,
            .op = .GtEq,
            .value = std.math.minInt(i64),
        } };
        break :blk try buildFilteredOutputMulti(a, allocator, specs, f, kept_columns_opt, sink);
    };
    try mp_sink.close();

    const t_after_build = t_after_fetch;
    const t_end = nowMonoNs();
    const upload_mode: []const u8 = if (same_bucket) "streaming_pooled" else "streaming_fresh";

    return std.fmt.allocPrint(
        allocator,
        "{{\"ok\":true,\"output\":\"{s}\",\"input_count\":{d},\"files_total\":{d},\"files_pruned\":{d},\"bytes_in\":{d},\"bytes_fetched\":{d},\"bytes_out\":{d},\"row_groups_pruned\":{d},\"rows_in\":{d},\"rows_kept\":{d},\"upload\":\"{s}\",\"fetch_ms\":{d},\"build_ms\":{d},\"put_ms\":{d},\"total_ms\":{d}}}",
        .{
            output_url_str,
            specs.len,
            total_files,
            files_pruned,
            total_input_size, bytes_fetched, bytes_out,
            rg_pruned, total_input_rows, rows_kept, upload_mode,
            @divTrunc(t_after_fetch - t_start, std.time.ns_per_ms),
            @divTrunc(t_after_build - t_after_fetch, std.time.ns_per_ms),
            @divTrunc(t_end - t_after_build, std.time.ns_per_ms),
            @divTrunc(t_end - t_start, std.time.ns_per_ms),
        },
    );
}

/// Multi-file decode + filter + encode. Iterates over `specs` (each
/// with its own meta + file_buf + survivors), produces one unified
/// Parquet output streamed to `sink`. Returns total bytes written.
fn buildFilteredOutputMulti(
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    specs: []const FileSpec,
    filter: filter_ast.Filter,
    kept_columns_opt: ?[]const usize,
    sink: streaming.Sink,
) !u64 {
    const MAGIC: [4]u8 = .{ 'P', 'A', 'R', '1' };

    if (specs.len == 0) return error.NoInputs;
    const meta0 = &specs[0].meta;
    const num_leaves = meta0.row_groups.items[0].columns.items.len;

    // Compute kept_set + fetch_set against first file's schema (all
    // inputs share schema by construction).
    var kept_set = try arena.alloc(bool, num_leaves);
    @memset(kept_set, false);
    if (kept_columns_opt) |kc| {
        for (kc) |idx| if (idx < num_leaves) {
            kept_set[idx] = true;
        };
    } else {
        @memset(kept_set, true);
    }

    var filter_cols: std.ArrayList(usize) = .empty;
    try filter.collectColumns(&filter_cols, arena);

    var fetch_set = try arena.alloc(bool, num_leaves);
    @memset(fetch_set, false);
    for (kept_set, 0..) |b, i| if (b) {
        fetch_set[i] = true;
    };
    for (filter_cols.items) |c| if (c < num_leaves) {
        fetch_set[c] = true;
    };

    var kept_in_order: std.ArrayList(usize) = .empty;
    for (kept_set, 0..) |b, i| if (b) try kept_in_order.append(arena, i);

    var offset: u64 = 0;
    try sink.write(&MAGIC);
    offset += MAGIC.len;

    var new_row_groups: std.ArrayListUnmanaged(schema.RowGroup) = .empty;
    var total_rows: i64 = 0;

    for (specs) |sp| {
        const enc_count = try encodeFilteredFile(
            arena,
            gpa,
            &sp,
            &fetch_set,
            kept_in_order.items,
            filter,
            sink,
            &offset,
            &new_row_groups,
        );
        total_rows += enc_count;
    }

    // Build the output schema via the tree. projectSubset preserves
    // GROUP ancestors of every kept leaf along with their LIST/MAP
    // annotations, which means downstream readers reassemble nested
    // structures correctly.
    const kept_u32 = try arena.alloc(u32, kept_in_order.items.len);
    for (kept_in_order.items, 0..) |idx, i| kept_u32[i] = @intCast(idx);
    const projected_tree = try specs[0].tree.projectSubset(arena, kept_u32);
    const new_schema = try projected_tree.writeFlatThrift(arena);

    const new_meta: schema.FileMetaData = .{
        .version = meta0.version,
        .schema = new_schema,
        .num_rows = total_rows,
        .created_by = meta0.created_by,
        .row_groups = new_row_groups,
    };

    var w: thrift.Writer = .init(arena);
    defer w.deinit();
    try new_meta.write(&w);
    const footer_bytes = w.bytes();

    try sink.write(footer_bytes);
    offset += footer_bytes.len;

    var len_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_bytes, @intCast(footer_bytes.len), .little);
    try sink.write(&len_bytes);
    offset += len_bytes.len;
    try sink.write(&MAGIC);
    offset += MAGIC.len;

    return offset;
}

/// Per-file decode/filter/encode of all surviving row groups. Pushes
/// each encoded column chunk's bytes to `sink` and bumps `offset.*` to
/// match. Appends a `RowGroup` per surviving group to `new_row_groups`
/// with offsets resolved to the running stream position.
/// Returns the count of surviving rows across this file's RGs.
fn encodeFilteredFile(
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    sp: *const FileSpec,
    fetch_set: *const []bool,
    kept_in_order: []const usize,
    filter: filter_ast.Filter,
    sink: streaming.Sink,
    offset: *u64,
    new_row_groups: *std.ArrayListUnmanaged(schema.RowGroup),
) !i64 {
    const meta = &sp.meta;
    const num_leaves = meta.row_groups.items[0].columns.items.len;
    var total_rows: i64 = 0;

    for (sp.survivors, 0..) |keep, rg_idx| {
        if (!keep) continue;
        const rg = &meta.row_groups.items[rg_idx];
        const num_rows: usize = @intCast(rg.num_rows);

        var rg_arena_state = std.heap.ArenaAllocator.init(gpa);
        defer rg_arena_state.deinit();
        const ra = rg_arena_state.allocator();

        var batch_cols: std.ArrayList(filter_eval.Batch.Column) = .empty;
        var lookup = try ra.alloc(?usize, meta.schema.items.len);
        @memset(lookup, null);

        var batch_pos_for_col = try ra.alloc(?usize, num_leaves);
        @memset(batch_pos_for_col, null);

        for (fetch_set.*, 0..) |needed, ci| {
            if (!needed) continue;
            const col = &rg.columns.items[ci];
            const col_meta = col.meta_data orelse return error.ColumnMetaMissing;
            const start: usize = if (col_meta.dictionary_page_offset) |dp| @intCast(dp) else @intCast(col_meta.data_page_offset);
            const len: usize = @intCast(col_meta.total_compressed_size);
            if (start + len > sp.file_buf.len) return error.MissingChunkBytes;
            const chunk = sp.file_buf[start .. start + len];

            const levels = meta.getColumnLevels(col_meta.path_in_schema.items);

            const n_leaves: usize = @intCast(col_meta.num_values);
            const decoded: filter_eval.Batch.Column = switch (col_meta.type) {
                .INT32 => .{ .i32 = try decodeColumnT(i32, ra, chunk, col_meta.codec, levels, n_leaves) },
                .INT64 => .{ .i64 = try decodeColumnT(i64, ra, chunk, col_meta.codec, levels, n_leaves) },
                .FLOAT => .{ .f32 = try decodeColumnT(f32, ra, chunk, col_meta.codec, levels, n_leaves) },
                .DOUBLE => .{ .f64 = try decodeColumnT(f64, ra, chunk, col_meta.codec, levels, n_leaves) },
                .BYTE_ARRAY => .{ .string = try decodeColumnT([]const u8, ra, chunk, col_meta.codec, levels, n_leaves) },
                .BOOLEAN => .{ .boolean = try decodeColumnT(bool, ra, chunk, col_meta.codec, levels, n_leaves) },
                else => return error.UnsupportedColumnType,
            };
            batch_pos_for_col[ci] = batch_cols.items.len;
            lookup[ci] = batch_cols.items.len;
            try batch_cols.append(ra, decoded);
        }

        const batch: filter_eval.Batch = .{ .cols = batch_cols.items, .num_rows = num_rows };
        var sel = try filter_selection.SelectionVector.init(ra, num_rows);
        try filter_eval.evaluate(filter, &batch, &sel, lookup, ra);

        const surviving_count = sel.count();
        if (surviving_count == 0) continue;

        var rg_columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
        try rg_columns.ensureTotalCapacity(arena, kept_in_order.len);
        var rg_total: i64 = 0;

        for (kept_in_order) |kept_ci| {
            const batch_pos = batch_pos_for_col[kept_ci] orelse return error.MissingDecodedColumn;
            const filtered = try encoder.applySelection(arena, batch_cols.items[batch_pos], &sel);

            // Find the schema-element matching this column chunk's
            // path. For flat columns this is just `schema.items[ci+1]`;
            // for nested it must be looked up via path_in_schema since
            // intermediate GROUP nodes shift the indexing.
            const cm = rg.columns.items[kept_ci].meta_data orelse return error.ColumnMetaMissing;
            const leaf_elem = meta.getColumnSchema(cm.path_in_schema.items) orelse return error.SchemaLookupFailed;
            const enc = try encoder.encodeColumn(arena, .{
                .values = filtered,
                .schema_elem = &leaf_elem,
                .path_in_schema = cm.path_in_schema.items,
            });

            const col_start_in_file: i64 = @intCast(offset.*);
            var em = enc.meta;
            em.data_page_offset = col_start_in_file;
            try sink.write(enc.bytes);
            offset.* += enc.bytes.len;
            rg_total += @intCast(enc.bytes.len);

            try rg_columns.append(arena, .{
                .file_path = null,
                .file_offset = col_start_in_file,
                .meta_data = em,
            });
        }

        try new_row_groups.append(arena, .{
            .columns = rg_columns,
            .total_byte_size = rg_total,
            .num_rows = @intCast(surviving_count),
        });
        total_rows += @intCast(surviving_count);
    }

    return total_rows;
}

fn nowMonoNs() i64 {
    var ts: std.os.linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, ts.sec) * std.time.ns_per_s + @as(i64, ts.nsec);
}

const POOL_SIZE: usize = s3.MAX_PARTS;

fn initPool(
    self: *s3.Pool(POOL_SIZE),
    arena: std.mem.Allocator,
    creds: s3.Credentials,
    bucket: []const u8,
) !void {
    const host = try std.fmt.allocPrint(arena, "{s}.s3.{s}.amazonaws.com", .{ bucket, creds.region });
    const addr_v4 = try s3.resolveIpv4(arena, host);
    try self.init(arena, host, addr_v4, 443);
}

fn decodeAll(comptime T: type, reader: anytype, out: []T) !void {
    var written: usize = 0;
    while (written < out.len) {
        const n = try reader.decode(out[written..]);
        if (n == 0) break;
        written += n;
    }
    if (written != out.len) return error.ShortDecode;
}

fn decodeAllWithLevels(comptime T: type, reader: anytype, values: []T, def_levels: []u32) !void {
    var written: usize = 0;
    while (written < values.len) {
        const n = try reader.decodeWithLevels(values[written..], def_levels[written..]);
        if (n == 0) break;
        written += n;
    }
    if (written != values.len) return error.ShortDecode;
}

/// Decode an entire column chunk into a ColumnT(T) view: values plus
/// def_levels (OPTIONAL) and optionally rep_levels (nested
/// list/map). The caller passes `num_leaves` — for flat / struct
/// columns this equals the row group's num_rows, but for nested
/// columns it equals the column chunk's `num_values` from its
/// metadata (which counts LEAVES, not logical rows).
fn decodeColumnT(
    comptime T: type,
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
) !filter_eval.ColumnT(T) {
    const values = try arena.alloc(T, num_leaves);
    var reader = column_mod.ColumnChunkReader(T).init(chunk, codec, levels, arena);
    if (levels.max_rep > 0) {
        const def_levels = try arena.alloc(u32, num_leaves);
        const rep_levels = try arena.alloc(u32, num_leaves);
        var written: usize = 0;
        while (written < num_leaves) {
            const n = try reader.decodeWithRepLevels(values[written..], def_levels[written..], rep_levels[written..]);
            if (n == 0) break;
            written += n;
        }
        if (written != num_leaves) return error.ShortDecode;
        return .{
            .values = values,
            .def_levels = def_levels,
            .max_def = @intCast(levels.max_def),
            .rep_levels = rep_levels,
            .max_rep = @intCast(levels.max_rep),
        };
    }
    if (levels.max_def > 0) {
        const def_levels = try arena.alloc(u32, num_leaves);
        try decodeAllWithLevels(T, &reader, values, def_levels);
        return .{ .values = values, .def_levels = def_levels, .max_def = @intCast(levels.max_def) };
    }
    try decodeAll(T, &reader, values);
    return .{ .values = values };
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
        const chunk = file_bytes[chunk_start .. chunk_start + chunk_len];

        var reader = column_mod.ColumnChunkReader(i32).init(chunk, col.codec, levels, arena.allocator());
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

test {
    _ = @import("runtime.zig");
}
