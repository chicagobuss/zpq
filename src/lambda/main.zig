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

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const env = init.minimal.environ;
    const io = init.io;

    var client = runtime.Client.fromEnv(allocator, env) catch |err| {
        std.debug.print("zpq lambda: runtime client init failed: {s}\n", .{@errorName(err)});
        return err;
    };
    defer client.deinit();

    while (true) {
        var inv = client.nextInvocation() catch |err| {
            std.debug.print("zpq lambda: poll error {s}\n", .{@errorName(err)});
            const ts: std.os.linux.timespec = .{ .sec = 1, .nsec = 0 };
            _ = std.os.linux.nanosleep(&ts, null);
            continue;
        };
        defer inv.deinit(allocator);

        const response = handle(io, allocator, env, &inv) catch |err| {
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
        if (output_url) |out| return try handleS3Write(io, allocator, env, url, filter_str, out);
        return try handleS3(allocator, env, url, filter_str);
    }

    // Legacy raw-bytes path used by the in-process integration test.
    return try aggregateInt8(allocator, inv.body, null);
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

    var client = s3.Client.init(a, creds, in_url.bucket) catch |err| {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"client_init\",\"reason\":\"{s}\"}}", .{@errorName(err)});
    };
    defer client.deinit();

    // 1. Tail GET for total size + footer.
    const tail_resp = try client.get(a, in_url.key, s3.Range.suffix(TAIL_SIZE));
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

    // The leading PAR1 magic is needed by metadata.open AND will be
    // used by the fast path (it copies row group bytes which start
    // after the magic). Pull head + any footer prefix the tail missed.
    const head = try client.get(a, in_url.key, s3.Range.span(0, 7));
    if (head.status != 206) return error.RangeStatus;
    @memcpy(file_buf[0..head.body.len], head.body);

    if (footer_actual_start < tail_start) {
        const need = try client.get(a, in_url.key, s3.Range.span(footer_actual_start, tail_start - 1));
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
    var ranges: std.ArrayList(coalescer.Range) = .empty;
    for (survivors, 0..) |keep, i| {
        if (!keep) continue;
        const rg = &meta.row_groups.items[i];
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
    const merged = try coalescer.Coalescer.coalesce(a, ranges.items, COALESCE_GAP);

    // Truncate any range that overlaps the tail (we already have those
    // bytes from the suffix GET) to avoid refetching.
    var fetch_jobs: std.ArrayList(coalescer.Range) = .empty;
    for (merged) |r| {
        if (r.start >= tail_start) continue;
        const end = @min(r.end, tail_start);
        try fetch_jobs.append(a, .{ .start = r.start, .end = end });
    }

    // Lazy-init pool sized to MAX_PARTS=8 — one shared pool serves the
    // read phase (fetchManyRanges) and write phase (uploadMultipart).
    // When input bucket == output bucket (typical), this means the same
    // 8 TLS connections cover the full Lambda invocation.
    var pool: s3.Pool(POOL_SIZE) = undefined;
    try initPool(&pool, a, creds, in_url.bucket);
    defer pool.deinit();

    // Convert coalescer.Range -> s3.Range for the fetch primitive.
    var fetch_s3: std.ArrayList(s3.Range) = .empty;
    for (fetch_jobs.items) |r| try fetch_s3.append(a, .{ .start = r.start, .end = r.end });

    var bytes_fetched: u64 = 0;
    if (fetch_s3.items.len > 0) {
        bytes_fetched = try s3.fetchManyRanges(
            io, &pool, allocator, a, creds,
            in_url.bucket, in_url.key, fetch_s3.items, file_buf,
        );
    }

    const t_after_fetch = nowMonoNs();

    // 5. Build the fast-path output.
    const out_bytes = try fastpath.build(a, file_buf, &meta, survivors);
    const t_after_build = nowMonoNs();

    // 6. PUT (single or multipart based on size).
    // Multipart shares the pool with the read phase iff the output
    // bucket == input bucket; otherwise s3.put fresh-handshakes.
    const same_bucket = std.mem.eql(u8, in_url.bucket, out_url.bucket);
    const upload_mode: []const u8 = if (out_bytes.len < s3.MULTIPART_THRESHOLD) blk: {
        const put_resp = try s3.put(a, creds, out_url, out_bytes);
        if (put_resp.status != 200) {
            return std.fmt.allocPrint(
                allocator,
                "{{\"error\":\"put_status\",\"status\":{d},\"body\":\"{s}\"}}",
                .{ put_resp.status, put_resp.body },
            );
        }
        break :blk "single";
    } else if (same_bucket) blk: {
        try s3.uploadMultipart(io, &pool, a, allocator, creds, out_url, out_bytes);
        break :blk "multipart_pooled";
    } else blk: {
        // Different bucket: build a fresh pool for the output host.
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
