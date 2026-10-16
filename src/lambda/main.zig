//! ZPQ Lambda binary entry point.
//!
//! Constraints established by docs/lambda_capabilities.md:
//!   - io_uring is unavailable (AWS seccomp returns ENOSYS).
//!   - Kernel is AL2 5.10, not AL2023 6.x — no epoll_pwait2, no clone3.
//!   - epoll/eventfd2/timerfd_create/signalfd4/mlock are allowed.
//!   - SO_ZEROCOPY and TCP_FASTOPEN setsockopt allowed.
//!
//! Lifecycle (production / S3 path):
//!   1. Before the first `/next`, open `ZPQ_S3_PRECONNECT` connections (default 4) to the
//!      regional endpoint and park them. Failures are ignored.
//!   2. Receive `{"s3_url": "...", "filter": "...optional..."}`.
//!   3. One suffix GET (`engine.footerProbeFor`) for the file size and footer. An explicit
//!      `ZPQ_S3_FOOTER_PROBE` cannot exceed the memory budget.
//!   4. If the footer is bigger than that suffix, fetch the missing prefix. The leading
//!      PAR1 is checked only when a fetched range already covers offset 0.
//!   5. Parse FileMetaData. If filter is present, parse it against
//!      the schema.
//!   6. For each row group: prune via the filter's stats. For
//!      survivors, range-fetch the int8 column AND any filter
//!      columns. Coalesce nearby ranges.
//!   7. Decode each column into typed slices, build a SelectionVector,
//!      run filter.eval, then aggregate matched int8 values.
//!   8. Return JSON envelope with row_groups_pruned + matched count
//!      + min/max/sum.

const std = @import("std");
const zpq = @import("zpq");
const nowMonoNs = zpq.clock.monoNs;
const runtime = @import("runtime.zig");
const service = @import("service");
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

pub const std_options: std.Options = .{ .signal_stack_size = zpq.core.spawn.signal_stack_size };

const COALESCE_GAP: u64 = 64 * 1024;
const TARGET_COLUMN: []const u8 = "int8";

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const env = init.minimal.environ;
    const io = init.io;
    zpq.local_fs.raiseOpenFileLimit();

    var client = runtime.Client.fromEnv(allocator, env) catch |err| {
        std.debug.print("zpq lambda: runtime client init failed: {s}\n", .{@errorName(err)});
        return err;
    };
    defer client.deinit();

    var pool: service.PersistentPool = .sized(zpq.core.system.discoverAvailableMemory(env));
    service.preconnectS3(io, allocator, env, &pool);

    while (true) {
        var inv = client.nextInvocation() catch |err| {
            std.debug.print("zpq lambda: poll error {s}\n", .{@errorName(err)});
            const ts: std.os.linux.timespec = .{ .sec = 1, .nsec = 0 };
            _ = std.os.linux.nanosleep(&ts, null);
            continue;
        };
        defer inv.deinit(allocator);

        // A second short of the invocation's deadline: S3 requests stop there, and the caller hears why rather than
        // nothing when the sandbox is stopped (`Watchdog`).
        const left_ms: ?u64 = if (inv.deadline_ms) |d| @intCast(@max(0, d - zpq.clock.realtimeMs() - 1000)) else null;
        zpq.io.tls.setInvocationDeadline(left_ms);
        defer zpq.io.tls.setInvocationDeadline(null);
        var watchdog: Watchdog = .{ .client = &client, .request_id = inv.request_id };
        const watcher: ?std.Thread = if (left_ms) |ms| std.Thread.spawn(.{}, Watchdog.run, .{ &watchdog, ms }) catch
            null else null;
        defer if (watcher) |t| watchdog.stop(t);

        const response = service.handle(io, allocator, env, inv.body, &pool, legacy) catch |err| {
            if (watchdog.claim()) client.postError(inv.request_id, "HandlerError", @errorName(err)) catch |perr| {
                std.debug.print("zpq lambda: postError failed: {s}\n", .{@errorName(perr)});
            };
            continue;
        };
        defer allocator.free(response);
        if (!watchdog.claim()) {
            std.debug.print("zpq lambda: answered past the deadline; the invocation was failed already\n", .{});
            continue;
        }

        // Past the runtime API's limit the caller got AWS's generic payload error and no reason: say why, and how to
        // get the answer instead. One the runtime refuses, or drops while it is sent, is reported the same way.
        if (response.len > runtime.MAX_POST_BYTES) {
            var buf: [256]u8 = undefined;
            const why = std.fmt.bufPrint(&buf, "the response is {d} bytes, past the {d} a Lambda response may be: " ++
                "write the answer to output_url", .{ response.len, runtime.MAX_POST_BYTES }) catch "ResultTooLarge";
            postFailure(&client, inv.request_id, "ResultTooLarge", why);
            continue;
        }
        client.postResponse(inv.request_id, response) catch |err| {
            std.debug.print("zpq lambda: postResponse failed: {s}\n", .{@errorName(err)});
            postFailure(&client, inv.request_id, "ResponseNotPosted", @errorName(err));
        };
    }
}

/// Fails the invocation through /error once its deadline is a second away and the handler has not answered: a query
/// still computing then was stopped with the sandbox, and the caller was told nothing. Whichever of it and the loop
/// claims the invocation first posts for it.
const Watchdog = struct {
    client: *runtime.Client,
    request_id: []const u8,
    /// 0 unanswered, 1 claimed by the loop, 2 by the watchdog.
    owner: std.atomic.Value(u32) = .init(0),
    /// Futex word: 1 once the loop is done with the invocation.
    done: std.atomic.Value(u32) = .init(0),

    fn run(self: *Watchdog, ms: u64) void {
        const until = zpq.clock.monoNs() + @as(i64, @intCast(ms * std.time.ns_per_ms));
        while (self.done.load(.acquire) == 0) {
            const left = until - zpq.clock.monoNs();
            if (left <= 0) break;
            const ts: std.os.linux.timespec = .{
                .sec = @intCast(@divTrunc(left, std.time.ns_per_s)),
                .nsec = @intCast(@mod(left, std.time.ns_per_s)),
            };
            _ = std.os.linux.futex_4arg(&self.done.raw, .{ .cmd = .WAIT, .private = true }, 0, &ts);
        }
        if (self.done.load(.acquire) != 0) return;
        if (self.owner.cmpxchgStrong(0, 2, .acq_rel, .acquire) != null) return;
        postFailure(self.client, self.request_id, "DeadlineExceeded", "the query did not finish within the " ++
            "function's timeout: raise the timeout, or narrow the query (a filter, fewer columns, an output_url)");
    }

    /// Whether the loop may answer the invocation: false once the watchdog has failed it.
    fn claim(self: *Watchdog) bool {
        return self.owner.cmpxchgStrong(0, 1, .acq_rel, .acquire) == null or self.owner.load(.acquire) == 1;
    }

    fn stop(self: *Watchdog, t: std.Thread) void {
        self.done.store(1, .release);
        _ = std.os.linux.futex_3arg(&self.done.raw, .{ .cmd = .WAKE, .private = true }, 1);
        t.join();
    }
};

fn postFailure(client: *runtime.Client, request_id: []const u8, error_type: []const u8, message: []const u8) void {
    client.postError(request_id, error_type, message) catch |err| {
        std.debug.print("zpq lambda: postError failed: {s}\n", .{@errorName(err)});
    };
}

/// Requests that neither aggregate nor write: the single-file int8 diagnostics, over S3 or (a raw parquet body, for the
/// in-process integration test) the event itself.
fn legacy(
    allocator: std.mem.Allocator,
    env: std.process.Environ,
    input: ?[]const u8,
    filter: ?[]const u8,
    body: []const u8,
) anyerror![]u8 {
    if (input) |url| return handleS3(allocator, env, url, filter);
    return aggregateInt8(allocator, body, null);
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
    var creds = s3.Credentials.fromEnv(env) catch |err| {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"no_credentials\",\"reason\":\"{s}\"}}", .{@errorName(err)});
    };
    creds.path_style = true;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var client = s3.Client.init(a, creds, url.bucket) catch |err| {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"client_init\",\"reason\":\"{s}\"}}", .{@errorName(err)});
    };
    defer client.deinit();

    // 1. One suffix GET for the size and the footer. A footer that does not fit costs one more GET below.
    // One file. The explicit probe cannot exceed the budget-derived maximum.
    const probe = engine.footerProbeFor(env, @as(u64, zpq.core.system.discoverAvailableMemory(env)), 1);
    const tail_resp = client.get(a, url.key, s3.Range.suffix(probe)) catch |err| {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"tail_fetch\",\"reason\":\"{s}\"}}", .{@errorName(err)});
    };
    if (tail_resp.status != 206 and tail_resp.status != 200) {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"tail_status\",\"status\":{d}}}", .{tail_resp.status});
    }

    const total_size = parseTotalFromContentRange(tail_resp.header("Content-Range")) catch |err| {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"bad_content_range\",\"reason\":\"{s}\"}}", .{@errorName(err)});
    };

    // The total is the server's word: one smaller than the suffix it sent cannot be.
    if (tail_resp.body.len > total_size) {
        const refusal = "{{\"error\":\"bad_content_range\",\"reason\":\"BodyLongerThanTotal\"}}";
        return std.fmt.allocPrint(allocator, refusal, .{});
    }

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
        if (need.body.len != tail_start - footer_actual_start) return error.RangeStatus;
        // The prefix itself starts at offset 0. Check it here: nothing later re-reads these bytes.
        if (footer_actual_start == 0 and !metadata.hasLeadingMagic(need.body)) return error.NotParquet;
        @memcpy(file_buf[footer_actual_start..tail_start], need.body);
    }
    // The suffix included offset 0. Otherwise the leading magic waits for a data range that covers it.
    if (tail_start == 0 and !metadata.hasLeadingMagic(file_buf)) return error.NotParquet;

    var meta = try metadata.openFooter(a, file_buf[@intCast(footer_actual_start)..@intCast(total_size - 8)]);
    defer meta.deinit(a);

    // 4. Parse the filter (if any) against the schema.
    var filter: ?filter_ast.Filter = null;
    if (filter_str) |fs| {
        if (fs.len > 0) {
            filter = filter_parser.parse(a, fs, &meta, null) catch |err| {
                return service.rejectResponse(allocator, "filter_parse", @errorName(err), "expr", fs);
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
            if (r.start == 0 and !metadata.hasLeadingMagic(file_buf)) return error.NotParquet;
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

const POOL_SIZE = service.POOL_SIZE;

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

test {
    _ = @import("runtime.zig");
    _ = @import("scan.zig");
}
