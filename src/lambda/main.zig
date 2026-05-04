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
const encoder = zpq.core.writer.encoder;
const thrift = zpq.core.thrift;
const s3 = zpq.io.s3;
const coalescer = zpq.io.coalescer;
const filter_ast = zpq.core.filter.ast;
const filter_parser = zpq.core.filter.parser;
const filter_prune = zpq.core.filter.prune;
const filter_selection = zpq.core.filter.selection;
const filter_eval = zpq.core.filter.eval;

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
        const url = extractField(trimmed, "s3_url") catch |err| {
            return std.fmt.allocPrint(
                allocator,
                "{{\"error\":\"bad_json\",\"reason\":\"{s}\"}}",
                .{@errorName(err)},
            );
        };
        const filter_str = extractField(trimmed, "filter") catch null;
        const output_url = extractField(trimmed, "output_url") catch null;
        const columns_csv = extractStringArray(trimmed, "columns", allocator) catch null;
        defer if (columns_csv) |c| allocator.free(c);
        if (output_url) |out| return try handleS3Write(io, allocator, env, url, filter_str, out, columns_csv, pool);
        return try handleS3(allocator, env, url, filter_str);
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

            const path = meta.schema.items[ci + 1].name;
            const path_arr: [1][]const u8 = .{path};
            const levels = meta.getColumnLevels(&path_arr);

            const pt = col_meta.type;
            const decoded: filter_eval.Batch.Column = switch (pt) {
                .INT32 => blk: {
                    const out = try ra.alloc(i32, num_rows);
                    var reader = column_mod.ColumnChunkReader(i32).init(chunk, col_meta.codec, levels, ra);
                    try decodeAll(i32, &reader, out);
                    if (ci == target_idx) target_values = out;
                    break :blk .{ .i32 = out };
                },
                .INT64 => blk: {
                    const out = try ra.alloc(i64, num_rows);
                    var reader = column_mod.ColumnChunkReader(i64).init(chunk, col_meta.codec, levels, ra);
                    try decodeAll(i64, &reader, out);
                    break :blk .{ .i64 = out };
                },
                .FLOAT => blk: {
                    const out = try ra.alloc(f32, num_rows);
                    var reader = column_mod.ColumnChunkReader(f32).init(chunk, col_meta.codec, levels, ra);
                    try decodeAll(f32, &reader, out);
                    break :blk .{ .f32 = out };
                },
                .DOUBLE => blk: {
                    const out = try ra.alloc(f64, num_rows);
                    var reader = column_mod.ColumnChunkReader(f64).init(chunk, col_meta.codec, levels, ra);
                    try decodeAll(f64, &reader, out);
                    break :blk .{ .f64 = out };
                },
                .BYTE_ARRAY => blk: {
                    const out = try ra.alloc([]const u8, num_rows);
                    var reader = column_mod.ColumnChunkReader([]const u8).init(chunk, col_meta.codec, levels, ra);
                    try decodeAll([]const u8, &reader, out);
                    break :blk .{ .string = out };
                },
                .BOOLEAN => blk: {
                    const out = try ra.alloc(bool, num_rows);
                    var reader = column_mod.ColumnChunkReader(bool).init(chunk, col_meta.codec, levels, ra);
                    try decodeAll(bool, &reader, out);
                    break :blk .{ .boolean = out };
                },
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

/// Phase 5.1 fast-path writer. Same read-side as handleS3 (fetch
/// footer + parse metadata + parse filter), but instead of decoding
/// values, builds a survivors bitmask from row-group-level pruning,
/// range-fetches the surviving row groups' bytes, runs
/// fastpath.build, and PUTs the result to `output_url`.
///
/// Limitations of the fast path:
///   - Pruning is stat-based only. Any filter clause that would
///     normally narrow rows *within* a surviving row group is
///     ignored — that row group is copied whole. Per-row filtering
///     waits for Phase 5.4 (decoder + re-encoder).
///   - Page-index and bloom-filter offsets are dropped from the
///     output; downstream readers fall back to row-group-level
///     pruning.
fn handleS3Write(
    io: std.Io,
    allocator: std.mem.Allocator,
    env: std.process.Environ,
    input_url_str: []const u8,
    filter_str: ?[]const u8,
    output_url_str: []const u8,
    columns_csv: ?[]const u8,
    persistent_pool: *PersistentPool,
) ![]u8 {
    const t_start = nowMonoNs();

    const in_url = s3.Url.parse(input_url_str) catch |err| {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"bad_input_url\",\"reason\":\"{s}\"}}", .{@errorName(err)});
    };
    const out_url = s3.Url.parse(output_url_str) catch |err| {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"bad_output_url\",\"reason\":\"{s}\"}}", .{@errorName(err)});
    };
    const creds = s3.Credentials.fromEnv(env) catch |err| {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"no_credentials\",\"reason\":\"{s}\"}}", .{@errorName(err)});
    };

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const pool = persistent_pool.ensureForBucket(allocator, creds, in_url.bucket) catch |err| {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"pool_init\",\"reason\":\"{s}\"}}", .{@errorName(err)});
    };

    // 1. Tail GET via pool. Whole metadata-fetch sequence (tail/head/
    // optional footer-prefix) shares one pooled connection.
    const tail_resp = try s3.getViaPool(io, pool, a, creds, in_url, s3.Range.suffix(TAIL_SIZE));
    if (tail_resp.status != 206 and tail_resp.status != 200) {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"tail_status\",\"status\":{d}}}", .{tail_resp.status});
    }
    const total_size = try parseTotalFromContentRange(tail_resp.header("Content-Range"));

    const file_buf = try allocator.alloc(u8, total_size);
    defer allocator.free(file_buf);
    const tail_start = total_size - tail_resp.body.len;
    @memcpy(file_buf[tail_start..], tail_resp.body);

    if (tail_resp.body.len < 8) return error.TailTooSmall;
    const tail = tail_resp.body;
    if (!std.mem.eql(u8, tail[tail.len - 4 ..], "PAR1")) return error.NotParquet;
    const footer_len: u64 = std.mem.readInt(u32, tail[tail.len - 8 ..][0..4], .little);
    const footer_actual_start = total_size - 8 - footer_len;

    const head = try s3.getViaPool(io, pool, a, creds, in_url, s3.Range.span(0, 7));
    if (head.status != 206) return error.RangeStatus;
    @memcpy(file_buf[0..head.body.len], head.body);

    if (footer_actual_start < tail_start) {
        const need = try s3.getViaPool(io, pool, a, creds, in_url, s3.Range.span(footer_actual_start, tail_start - 1));
        if (need.status != 206) return error.RangeStatus;
        @memcpy(file_buf[footer_actual_start..tail_start], need.body);
    }

    var meta = try metadata.open(a, file_buf);
    defer meta.deinit(a);

    // 2. Parse the filter (optional).
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

    // 2b. Resolve projection columns (if any) to schema indices.
    var kept_columns_opt: ?[]const usize = null;
    if (columns_csv) |csv| {
        var kept = std.ArrayList(usize).empty;
        var iter = std.mem.splitScalar(u8, csv, ',');
        while (iter.next()) |name| {
            if (name.len == 0) continue;
            const idx = metadata.findColumnIndex(&meta, name) orelse {
                return std.fmt.allocPrint(
                    allocator,
                    "{{\"error\":\"bad_column\",\"name\":\"{s}\"}}",
                    .{name},
                );
            };
            try kept.append(a, idx);
        }
        if (kept.items.len > 0) kept_columns_opt = kept.items;
    }

    // 3. Build survivors[] via stat-level pruning.
    const survivors = try a.alloc(bool, meta.row_groups.items.len);
    var rg_pruned: usize = 0;
    var rows_kept: i64 = 0;
    for (meta.row_groups.items, 0..) |rg, i| {
        if (filter) |f| {
            const decision = try filter_prune.pruneRowGroup(&rg, f, a);
            if (decision == .skip) {
                survivors[i] = false;
                rg_pruned += 1;
                continue;
            }
        }
        survivors[i] = true;
        rows_kept += rg.num_rows;
    }

    // 4. Range-fetch surviving row groups' bytes (coalesced).
    // - With filter: fetch (filter cols ∪ kept cols) per RG. Decode
    //   path needs filter cols to evaluate; encode path needs kept
    //   cols to write.
    // - Without filter, with projection: per kept column, one range
    //   per RG (byte-copy at column granularity).
    // - Without filter, no projection: span across all columns of
    //   each surviving RG (byte-copy whole RG).
    var ranges: std.ArrayList(coalescer.Range) = .empty;

    var filter_cols_for_fetch: std.ArrayList(usize) = .empty;
    if (filter) |f| try f.collectColumns(&filter_cols_for_fetch, a);

    for (survivors, 0..) |keep, i| {
        if (!keep) continue;
        const rg = &meta.row_groups.items[i];
        if (filter != null) {
            // Mark columns we need: filter cols ∪ kept cols (or all if no projection).
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

    // Truncate any range that overlaps the tail (we already have those
    // bytes from the suffix GET) to avoid refetching.
    var fetch_jobs: std.ArrayList(coalescer.Range) = .empty;
    for (merged) |r| {
        if (r.start >= tail_start) continue;
        const end = @min(r.end, tail_start);
        try fetch_jobs.append(a, .{ .start = r.start, .end = end });
    }

    // The persistent pool already covers the input bucket (we
    // ensured it at the top of this fn). fetchManyRanges + putViaPool
    // share it across phases.

    // Convert coalescer.Range -> s3.Range for the fetch primitive.
    var fetch_s3: std.ArrayList(s3.Range) = .empty;
    for (fetch_jobs.items) |r| try fetch_s3.append(a, .{ .start = r.start, .end = r.end });

    var bytes_fetched: u64 = 0;
    if (fetch_s3.items.len > 0) {
        bytes_fetched = try s3.fetchManyRanges(
            io, pool, allocator, a, creds,
            in_url.bucket, in_url.key, fetch_s3.items, file_buf,
        );
    }

    const t_after_fetch = nowMonoNs();

    // 5. Build the output. With a filter, we decode + filter + encode
    // surviving RGs (correctness — value-level filtering ZPQ couldn't
    // produce in 5.1/5.4a). Without a filter, the byte-copy fastpath
    // path is faster and lossless.
    const out_bytes = if (filter) |f|
        try buildFilteredOutput(a, allocator, file_buf, &meta, survivors, f, kept_columns_opt)
    else
        try fastpath.build(a, file_buf, &meta, survivors, kept_columns_opt);
    const t_after_build = nowMonoNs();

    // 6. PUT (single or multipart based on size).
    // Multipart shares the pool with the read phase iff the output
    // bucket == input bucket; otherwise s3.put fresh-handshakes.
    const same_bucket = std.mem.eql(u8, in_url.bucket, out_url.bucket);
    const upload_mode: []const u8 = if (out_bytes.len < s3.MULTIPART_THRESHOLD) blk: {
        // Small output: single PUT. Reuse the pool when same bucket.
        const put_resp = if (same_bucket)
            try s3.putViaPool(io, pool, a, creds, out_url, out_bytes)
        else
            try s3.put(a, creds, out_url, out_bytes);
        if (put_resp.status != 200) {
            return std.fmt.allocPrint(
                allocator,
                "{{\"error\":\"put_status\",\"status\":{d},\"body\":\"{s}\"}}",
                .{ put_resp.status, put_resp.body },
            );
        }
        break :blk if (same_bucket) "single_pooled" else "single";
    } else if (same_bucket) blk: {
        try s3.uploadMultipart(io, pool, a, allocator, creds, out_url, out_bytes);
        break :blk "multipart_pooled";
    } else blk: {
        // Different bucket: build a fresh local pool for the output host.
        // Doesn't persist across invocations (rare path).
        var out_pool: s3.Pool(POOL_SIZE) = undefined;
        try initPool(&out_pool, a, creds, out_url.bucket);
        defer out_pool.deinit();
        try s3.uploadMultipart(io, &out_pool, a, allocator, creds, out_url, out_bytes);
        break :blk "multipart_fresh";
    };
    const t_end = nowMonoNs();

    return std.fmt.allocPrint(
        allocator,
        "{{\"ok\":true,\"input\":\"{s}\",\"output\":\"{s}\",\"bytes_in\":{d},\"bytes_fetched\":{d},\"bytes_out\":{d},\"row_groups\":{d},\"row_groups_pruned\":{d},\"rows_kept\":{d},\"upload\":\"{s}\",\"fetch_ms\":{d},\"build_ms\":{d},\"put_ms\":{d},\"total_ms\":{d}}}",
        .{
            input_url_str, output_url_str,
            total_size, bytes_fetched, out_bytes.len,
            meta.row_groups.items.len, rg_pruned, rows_kept, upload_mode,
            @divTrunc(t_after_fetch - t_start, std.time.ns_per_ms),
            @divTrunc(t_after_build - t_after_fetch, std.time.ns_per_ms),
            @divTrunc(t_end - t_after_build, std.time.ns_per_ms),
            @divTrunc(t_end - t_start, std.time.ns_per_ms),
        },
    );
}

/// Decode + filter + encode each surviving row group, then assemble
/// a complete Parquet output file. Used when the filter has value-
/// level conditions that aren't fully resolved by row-group stat
/// pruning (which is true for any non-trivial filter).
///
/// `file_buf` must already contain the bytes of every surviving RG's
/// columns referenced by `filter` and (if non-null) `kept_columns_opt`.
/// The caller is responsible for fetching those column ranges before
/// invoking us.
///
/// Output layout: standard Parquet — leading PAR1, then encoded RGs
/// (in input order, but possibly with fewer rows per RG and dropped
/// unkept columns), then footer thrift, then footer length, trailing
/// PAR1.
///
/// Limitations matching encoder.zig: PLAIN encoding, UNCOMPRESSED
/// codec, single page per column, required (non-null) columns only.
fn buildFilteredOutput(
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    file_buf: []const u8,
    meta: *const schema.FileMetaData,
    survivors: []const bool,
    filter: filter_ast.Filter,
    kept_columns_opt: ?[]const usize,
) ![]u8 {
    const MAGIC: [4]u8 = .{ 'P', 'A', 'R', '1' };

    // Compute the union of kept output columns + filter input columns.
    // We need filter cols decoded for evaluation and kept cols encoded
    // for output; they may overlap.
    const num_leaves = meta.row_groups.items[0].columns.items.len;

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

    // Build the list of kept columns in input order (for the output
    // schema and per-RG column order).
    var kept_in_order: std.ArrayList(usize) = .empty;
    for (kept_set, 0..) |b, i| if (b) try kept_in_order.append(arena, i);

    // Output buffer. Reserve enough headroom that we don't constantly
    // reallocate during encode.
    var out: std.ArrayList(u8) = .empty;
    try out.ensureTotalCapacity(arena, 64 * 1024);
    try out.appendSlice(arena, &MAGIC);

    var new_row_groups: std.ArrayListUnmanaged(schema.RowGroup) = .empty;

    var total_rows: i64 = 0;
    for (survivors, 0..) |keep, rg_idx| {
        if (!keep) continue;
        const rg = &meta.row_groups.items[rg_idx];
        const num_rows: usize = @intCast(rg.num_rows);

        // Per-RG arena for decoded values + sel.
        var rg_arena_state = std.heap.ArenaAllocator.init(gpa);
        defer rg_arena_state.deinit();
        const ra = rg_arena_state.allocator();

        // Decode every column in fetch_set, build a Batch.
        var batch_cols: std.ArrayList(filter_eval.Batch.Column) = .empty;
        var lookup = try ra.alloc(?usize, meta.schema.items.len);
        @memset(lookup, null);

        // Track which slot each (input) column index occupies in batch_cols.
        var batch_pos_for_col = try ra.alloc(?usize, num_leaves);
        @memset(batch_pos_for_col, null);

        for (fetch_set, 0..) |needed, ci| {
            if (!needed) continue;
            const col = &rg.columns.items[ci];
            const col_meta = col.meta_data orelse return error.ColumnMetaMissing;
            const start: usize = if (col_meta.dictionary_page_offset) |dp| @intCast(dp) else @intCast(col_meta.data_page_offset);
            const len: usize = @intCast(col_meta.total_compressed_size);
            if (start + len > file_buf.len) return error.MissingChunkBytes;
            const chunk = file_buf[start .. start + len];

            const path_arr: [1][]const u8 = .{meta.schema.items[ci + 1].name};
            const levels = meta.getColumnLevels(&path_arr);

            const decoded: filter_eval.Batch.Column = switch (col_meta.type) {
                .INT32 => blk: {
                    const buf = try ra.alloc(i32, num_rows);
                    var rdr = column_mod.ColumnChunkReader(i32).init(chunk, col_meta.codec, levels, ra);
                    try decodeAll(i32, &rdr, buf);
                    break :blk .{ .i32 = buf };
                },
                .INT64 => blk: {
                    const buf = try ra.alloc(i64, num_rows);
                    var rdr = column_mod.ColumnChunkReader(i64).init(chunk, col_meta.codec, levels, ra);
                    try decodeAll(i64, &rdr, buf);
                    break :blk .{ .i64 = buf };
                },
                .FLOAT => blk: {
                    const buf = try ra.alloc(f32, num_rows);
                    var rdr = column_mod.ColumnChunkReader(f32).init(chunk, col_meta.codec, levels, ra);
                    try decodeAll(f32, &rdr, buf);
                    break :blk .{ .f32 = buf };
                },
                .DOUBLE => blk: {
                    const buf = try ra.alloc(f64, num_rows);
                    var rdr = column_mod.ColumnChunkReader(f64).init(chunk, col_meta.codec, levels, ra);
                    try decodeAll(f64, &rdr, buf);
                    break :blk .{ .f64 = buf };
                },
                .BYTE_ARRAY => blk: {
                    const buf = try ra.alloc([]const u8, num_rows);
                    var rdr = column_mod.ColumnChunkReader([]const u8).init(chunk, col_meta.codec, levels, ra);
                    try decodeAll([]const u8, &rdr, buf);
                    break :blk .{ .string = buf };
                },
                .BOOLEAN => blk: {
                    const buf = try ra.alloc(bool, num_rows);
                    var rdr = column_mod.ColumnChunkReader(bool).init(chunk, col_meta.codec, levels, ra);
                    try decodeAll(bool, &rdr, buf);
                    break :blk .{ .boolean = buf };
                },
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
        if (surviving_count == 0) continue; // drop empty RG

        // Encode each kept column with the selection applied.
        var rg_columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
        try rg_columns.ensureTotalCapacity(arena, kept_in_order.items.len);
        var rg_total: i64 = 0;

        for (kept_in_order.items) |kept_ci| {
            const batch_pos = batch_pos_for_col[kept_ci] orelse return error.MissingDecodedColumn;
            const filtered = try encoder.applySelection(arena, batch_cols.items[batch_pos], &sel);

            const path_arr: [1][]const u8 = .{meta.schema.items[kept_ci + 1].name};
            const enc = try encoder.encodeColumn(arena, .{
                .values = filtered,
                .schema_elem = &meta.schema.items[kept_ci + 1],
                .path_in_schema = &path_arr,
            });

            // Patch absolute file offset for the data page.
            const col_start_in_file: i64 = @intCast(out.items.len);
            var em = enc.meta;
            em.data_page_offset = col_start_in_file;
            try out.appendSlice(arena, enc.bytes);
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

    // Build new schema. Leaves are forced to REQUIRED because we
    // don't emit definition levels in the encoded pages — surviving
    // values from filter eval are always non-null, so REQUIRED is
    // semantically correct.
    const new_schema = try cloneSchemaAsRequired(arena, meta.schema, kept_in_order.items);

    const new_meta: schema.FileMetaData = .{
        .version = meta.version,
        .schema = new_schema,
        .num_rows = total_rows,
        .created_by = meta.created_by,
        .row_groups = new_row_groups,
    };

    var w: thrift.Writer = .init(arena);
    defer w.deinit();
    try new_meta.write(&w);

    const footer_start: usize = out.items.len;
    try out.appendSlice(arena, w.bytes());
    const footer_len: u32 = @intCast(out.items.len - footer_start);

    var len_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_bytes, footer_len, .little);
    try out.appendSlice(arena, &len_bytes);
    try out.appendSlice(arena, &MAGIC);

    return out.toOwnedSlice(arena);
}

/// Build a new schema list for the filtered-output path: root +
/// only the kept leaves, with each leaf's repetition_type forced
/// to REQUIRED. Required because our encoder doesn't emit def levels.
fn cloneSchemaAsRequired(
    arena: std.mem.Allocator,
    src: std.ArrayListUnmanaged(schema.SchemaElement),
    kept: []const usize,
) !std.ArrayListUnmanaged(schema.SchemaElement) {
    var out: std.ArrayListUnmanaged(schema.SchemaElement) = .empty;
    try out.ensureTotalCapacity(arena, 1 + kept.len);

    var new_root = src.items[0];
    new_root.num_children = @intCast(kept.len);
    try out.append(arena, new_root);

    for (kept) |idx| {
        if (idx + 1 >= src.items.len) return error.BadColumnIndex;
        var leaf = src.items[idx + 1];
        leaf.repetition_type = .REQUIRED;
        try out.append(arena, leaf);
    }
    return out;
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
        while (true) {
            const n = try reader.decode(&batch);
            if (n == 0) break;
            for (batch[0..n]) |v| {
                if (v < min_v) min_v = v;
                if (v > max_v) max_v = v;
                sum += v;
            }
            total_rows += @intCast(n);
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
