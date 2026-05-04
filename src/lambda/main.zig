//! ZPQ Lambda binary entry point.
//!
//! Constraints established by docs/lambda_capabilities.md:
//!   - io_uring is unavailable (AWS seccomp returns ENOSYS).
//!   - Kernel is AL2 5.10, not AL2023 6.x — no epoll_pwait2, no clone3.
//!   - epoll/eventfd2/timerfd_create/signalfd4/mlock are allowed.
//!   - SO_ZEROCOPY and TCP_FASTOPEN setsockopt allowed.
//!
//! This binary intentionally excludes io_uring code at compile time via
//! `build_options.lambda`. The in-tree epoll backend is the only event
//! loop driver linked here.
//!
//! Lifecycle (production / S3 path):
//!   1. Receive `{"s3_url": "s3://bucket/key"}` invocation event.
//!   2. Suffix GET for the last 64 KB to discover file size + footer.
//!   3. If the footer is bigger than 64 KB, fetch the rest.
//!   4. Plan: which column chunks does the query need?
//!   5. Coalesce nearby chunk ranges; fetch each.
//!   6. Stitch fetched bytes into a sparse file buffer; decode.
//!   7. Return aggregate stats.
//!
//! Legacy (local fixture) path: if the body looks like raw Parquet
//! bytes (doesn't start with `{`), decode directly. Used by the
//! in-process integration test.

const std = @import("std");
const zpq = @import("zpq");
const runtime = @import("runtime.zig");

const metadata = zpq.core.parquet.metadata;
const column_mod = zpq.core.parquet.column;
const s3 = zpq.io.s3;
const coalescer = zpq.io.coalescer;

const TAIL_SIZE: u64 = 64 * 1024;
const COALESCE_GAP: u64 = 64 * 1024;

pub fn main(init: std.process.Init.Minimal) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .{};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var client = runtime.Client.fromEnv(allocator, init.environ) catch |err| {
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

        const response = handle(allocator, init.environ, &inv) catch |err| {
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
    allocator: std.mem.Allocator,
    env: std.process.Environ,
    inv: *const runtime.Invocation,
) ![]u8 {
    if (inv.body.len == 0) {
        return std.fmt.allocPrint(allocator, "{{\"error\":\"empty_body\"}}", .{});
    }

    const trimmed = std.mem.trim(u8, inv.body, " \r\n\t");
    if (trimmed.len > 0 and trimmed[0] == '{') {
        const url = extractS3Url(trimmed) catch |err| {
            return std.fmt.allocPrint(
                allocator,
                "{{\"error\":\"bad_json\",\"reason\":\"{s}\"}}",
                .{@errorName(err)},
            );
        };
        return try handleS3(allocator, env, url);
    }

    // Legacy raw-bytes path used by the in-process integration test.
    return try aggregateInt8(allocator, inv.body);
}

fn extractS3Url(body: []const u8) ![]const u8 {
    const key = "\"s3_url\"";
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

/// Production S3 path. Range-fetches only the bytes the query needs.
fn handleS3(allocator: std.mem.Allocator, env: std.process.Environ, s3_url: []const u8) ![]u8 {
    const url = s3.Url.parse(s3_url) catch |err| {
        return std.fmt.allocPrint(
            allocator,
            "{{\"error\":\"bad_s3_url\",\"reason\":\"{s}\"}}",
            .{@errorName(err)},
        );
    };
    const creds = s3.Credentials.fromEnv(env) catch |err| {
        return std.fmt.allocPrint(
            allocator,
            "{{\"error\":\"no_credentials\",\"reason\":\"{s}\"}}",
            .{@errorName(err)},
        );
    };

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // 1. Suffix GET — last 64 KB. Tells us total file size via
    //    Content-Range, and usually contains the entire footer.
    const tail_resp = s3.get(a, creds, url, s3.Range.suffix(TAIL_SIZE)) catch |err| {
        return std.fmt.allocPrint(
            allocator,
            "{{\"error\":\"tail_fetch_failed\",\"reason\":\"{s}\"}}",
            .{@errorName(err)},
        );
    };
    if (tail_resp.status != 206 and tail_resp.status != 200) {
        return std.fmt.allocPrint(
            allocator,
            "{{\"error\":\"tail_status\",\"status\":{d},\"body\":\"{s}\"}}",
            .{ tail_resp.status, tail_resp.body[0..@min(tail_resp.body.len, 256)] },
        );
    }

    const total_size = parseTotalFromContentRange(tail_resp.header("Content-Range")) catch |err| {
        return std.fmt.allocPrint(
            allocator,
            "{{\"error\":\"bad_content_range\",\"reason\":\"{s}\"}}",
            .{@errorName(err)},
        );
    };

    // 2. Allocate the sparse file buffer and stamp the tail in.
    //    On Linux, allocator.alloc backs the buffer with mmap'd pages
    //    that are lazily committed on first write — physical RSS only
    //    grows for pages we actually populate.
    const file_buf = try allocator.alloc(u8, total_size);
    defer allocator.free(file_buf);

    const tail_start = total_size - tail_resp.body.len;
    @memcpy(file_buf[tail_start..], tail_resp.body);

    // 3. Locate the footer. Last 8 bytes: footer_length (4 LE) + "PAR1".
    if (tail_resp.body.len < 8) return error.TailTooSmall;
    const tail = tail_resp.body;
    if (!std.mem.eql(u8, tail[tail.len - 4 ..], "PAR1")) return error.NotParquet;
    const footer_len: u64 = std.mem.readInt(u32, tail[tail.len - 8 ..][0..4], .little);
    // Parquet layout:
    //   [0..4]                                 leading magic "PAR1"
    //   [4..total_size-8]                      row groups + footer
    //   [total_size-8..total_size-4]           footer_len u32 LE
    //   [total_size-4..total_size]             trailing magic "PAR1"
    const footer_actual_start = total_size - 8 - footer_len;

    // 4. If footer extends before the tail we already fetched, get the rest.
    if (footer_actual_start < tail_start) {
        const need_start = footer_actual_start;
        const need_end = tail_start; // exclusive
        const need_resp = try s3.get(
            a,
            creds,
            url,
            s3.Range.span(need_start, need_end - 1),
        );
        if (need_resp.status != 206) return error.RangeStatus;
        @memcpy(file_buf[need_start..need_end], need_resp.body);
    }

    // 5. Always fetch the leading magic so metadata.open's validation
    //    passes. Cheap (8 bytes) and avoids special-casing the parser.
    const head_resp = try s3.get(a, creds, url, s3.Range.span(0, 7));
    if (head_resp.status != 206) return error.RangeStatus;
    @memcpy(file_buf[0..head_resp.body.len], head_resp.body);

    // 6. Parse metadata.
    var meta = try metadata.open(a, file_buf);
    defer meta.deinit(a);

    const target = "int8";
    const col_idx = metadata.findColumnIndex(&meta, target) orelse {
        return std.fmt.allocPrint(
            allocator,
            "{{\"error\":\"column_missing\",\"name\":\"{s}\"}}",
            .{target},
        );
    };

    // 7. Plan column chunk ranges across all row groups.
    var ranges: std.ArrayList(coalescer.Range) = .empty;
    for (meta.row_groups.items) |rg| {
        const col = rg.columns.items[col_idx].meta_data orelse continue;
        const start: u64 = if (col.dictionary_page_offset) |dp|
            @intCast(dp)
        else
            @intCast(col.data_page_offset);
        const len: u64 = @intCast(col.total_compressed_size);
        try ranges.append(a, .{ .start = start, .end = start + len });
    }

    // 8. Coalesce nearby ranges to reduce request count.
    const merged = try coalescer.Coalescer.coalesce(a, ranges.items, COALESCE_GAP);

    // 9. Fetch each merged range. Skip pieces the tail already covered.
    var fetched_bytes: u64 = 0;
    for (merged) |r| {
        // If the tail already covers this whole range, skip.
        if (r.start >= tail_start) continue;
        const fetch_end_excl = @min(r.end, tail_start);
        const resp = try s3.get(a, creds, url, s3.Range.span(r.start, fetch_end_excl - 1));
        if (resp.status != 206) return error.RangeStatus;
        @memcpy(file_buf[r.start..fetch_end_excl], resp.body);
        fetched_bytes += resp.body.len;
    }

    // 10. Decode using the now-populated sparse buffer. The decoder
    //     only touches the bytes we've fetched; the rest is undefined.
    const result = try aggregateInt8WithMeta(allocator, file_buf, &meta, col_idx);

    // Append fetch stats so we can see the win in the response.
    return std.fmt.allocPrint(
        allocator,
        "{s}",
        .{result},
    );
}

fn parseTotalFromContentRange(cr_or_null: ?[]const u8) !u64 {
    const cr = cr_or_null orelse return error.NoContentRange;
    // Format: "bytes X-Y/Z"
    const slash = std.mem.indexOfScalar(u8, cr, '/') orelse return error.BadContentRange;
    const total = std.mem.trim(u8, cr[slash + 1 ..], " \t");
    if (total.len == 0 or total[0] == '*') return error.BadContentRange;
    return std.fmt.parseInt(u64, total, 10) catch error.BadContentRange;
}

fn aggregateInt8(allocator: std.mem.Allocator, file_bytes: []const u8) ![]u8 {
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

    const target = "int8";
    const col_idx = metadata.findColumnIndex(&meta, target) orelse {
        return std.fmt.allocPrint(
            allocator,
            "{{\"error\":\"column_missing\",\"name\":\"{s}\"}}",
            .{target},
        );
    };
    return try aggregateInt8WithMeta(allocator, file_bytes, &meta, col_idx);
}

fn aggregateInt8WithMeta(
    allocator: std.mem.Allocator,
    file_bytes: []const u8,
    meta: *const zpq.core.schema.FileMetaData,
    col_idx: usize,
) ![]u8 {
    var total_rows: i64 = 0;
    var min_v: i32 = std.math.maxInt(i32);
    var max_v: i32 = std.math.minInt(i32);
    var sum: i64 = 0;
    var bytes_decoded: usize = 0;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const target = "int8";
    const path: [1][]const u8 = .{target};
    const levels = meta.getColumnLevels(&path);

    for (meta.row_groups.items) |rg| {
        _ = arena.reset(.retain_capacity);

        const col = rg.columns.items[col_idx].meta_data orelse return error.ColumnMetaMissing;
        const chunk_start: usize = if (col.dictionary_page_offset) |dp|
            @intCast(dp)
        else
            @intCast(col.data_page_offset);
        const chunk_len: usize = @intCast(col.total_compressed_size);
        if (chunk_start + chunk_len > file_bytes.len) return error.ChunkOutOfRange;
        const chunk = file_bytes[chunk_start .. chunk_start + chunk_len];

        var reader = column_mod.ColumnChunkReader(i32).init(
            chunk,
            col.codec,
            levels,
            arena.allocator(),
        );

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
        bytes_decoded += @intCast(col.total_uncompressed_size);
    }

    return std.fmt.allocPrint(
        allocator,
        "{{\"ok\":true,\"column\":\"{s}\",\"rows\":{d},\"min\":{d},\"max\":{d},\"sum\":{d},\"bytes_decoded\":{d},\"row_groups\":{d}}}",
        .{ target, total_rows, min_v, max_v, sum, bytes_decoded, meta.row_groups.items.len },
    );
}

test {
    _ = @import("runtime.zig");
}
