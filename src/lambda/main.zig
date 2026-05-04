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
//! Lifecycle:
//!   1. Init the runtime API client.
//!   2. Long-poll for invocations.
//!   3. For each invocation: extract the s3_url, fetch the file via
//!      our SigV4 + HTTPS + range-GET stack, decode one column, post
//!      the response.
//!   4. Repeat. Process exits on fatal errors only.
//!
//! Event body shape:
//!     {"s3_url": "s3://bucket/key"}    (production path — fetches from S3)
//! or  raw Parquet bytes                 (legacy local-fixture path used by
//!                                        the in-process integration test)

const std = @import("std");
const zpq = @import("zpq");
const runtime = @import("runtime.zig");

const metadata = zpq.core.parquet.metadata;
const column_mod = zpq.core.parquet.column;
const s3 = zpq.io.s3;

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

    // Try parsing as the production JSON envelope first. If it looks
    // like JSON (starts with `{`), extract s3_url and fetch from S3.
    // Otherwise treat the body as raw Parquet bytes (legacy local
    // integration-test path).
    const trimmed = std.mem.trim(u8, inv.body, " \r\n\t");
    const file_bytes = if (trimmed.len > 0 and trimmed[0] == '{') blk: {
        const url = extractS3Url(trimmed) catch |err| {
            return std.fmt.allocPrint(
                allocator,
                "{{\"error\":\"bad_json\",\"reason\":\"{s}\"}}",
                .{@errorName(err)},
            );
        };
        const fetched = fetchS3File(allocator, env, url) catch |err| {
            return std.fmt.allocPrint(
                allocator,
                "{{\"error\":\"s3_fetch_failed\",\"reason\":\"{s}\",\"url\":\"{s}\"}}",
                .{ @errorName(err), url },
            );
        };
        break :blk fetched;
    } else inv.body;
    defer if (file_bytes.ptr != inv.body.ptr) allocator.free(file_bytes);

    return try aggregateInt8(allocator, file_bytes);
}

fn extractS3Url(body: []const u8) ![]const u8 {
    // Hand-rolled JSON sniff for {"s3_url": "..."}. Fully-validated
    // parsing isn't worth the API surface for one field.
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

fn fetchS3File(
    allocator: std.mem.Allocator,
    env: std.process.Environ,
    s3_url: []const u8,
) ![]u8 {
    const url = try s3.Url.parse(s3_url);
    const creds = try s3.Credentials.fromEnv(env);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const resp = try s3.get(arena.allocator(), creds, url, null);
    if (resp.status != 200) {
        // Pull the response body into stderr (Lambda CloudWatch picks
        // it up) so we can see what S3 actually said.
        std.debug.print("zpq lambda: s3 status={d}, body={s}\n", .{ resp.status, resp.body });
        return error.S3Status;
    }

    // Copy out of the arena into a fresh allocator-owned slice so the
    // caller doesn't need to manage the arena lifetime.
    return try allocator.dupe(u8, resp.body);
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

    var total_rows: i64 = 0;
    var min_v: i32 = std.math.maxInt(i32);
    var max_v: i32 = std.math.minInt(i32);
    var sum: i64 = 0;
    var bytes_decoded: usize = 0;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

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
