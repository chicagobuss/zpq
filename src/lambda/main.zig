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
//!   3. For each invocation: decode the request body as a Parquet file,
//!      compute stats over a target column, post the response.
//!   4. Repeat. Process exits on fatal errors only.

const std = @import("std");
const zpq = @import("zpq");
const runtime = @import("runtime.zig");

const metadata = zpq.core.parquet.metadata;
const column_mod = zpq.core.parquet.column;

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

        const response = handle(allocator, &inv) catch |err| {
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

/// Per-invocation handler.
///
/// Treats the event body as a complete Parquet file. Opens the footer,
/// finds the configured column ("int8" by default for our benchmark
/// fixture, overridable via the AWS_LAMBDA_FUNCTION_NAME suffix or
/// future config), decodes it across all row groups, and returns a
/// JSON envelope with row count + min/max/sum.
///
/// Phase 1.7: this is the first end-to-end demonstration that the
/// decode pipeline works inside Lambda. Real S3 input lands in a
/// follow-up.
fn handle(allocator: std.mem.Allocator, inv: *const runtime.Invocation) ![]u8 {
    if (inv.body.len < 12) {
        return std.fmt.allocPrint(
            allocator,
            "{{\"error\":\"empty_body\",\"len\":{d}}}",
            .{inv.body.len},
        );
    }

    var meta = metadata.open(allocator, inv.body) catch |err| {
        return std.fmt.allocPrint(
            allocator,
            "{{\"error\":\"open_failed\",\"reason\":\"{s}\",\"len\":{d}}}",
            .{ @errorName(err), inv.body.len },
        );
    };
    defer meta.deinit(allocator);

    const target = "int8";
    const col_idx = metadata.findColumnIndex(&meta, target) orelse {
        return std.fmt.allocPrint(
            allocator,
            "{{\"error\":\"column_missing\",\"name\":\"{s}\",\"rows\":{d}}}",
            .{ target, meta.num_rows },
        );
    };

    // Aggregate across row groups.
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
        // Reset the arena per row group so memory doesn't grow unboundedly.
        _ = arena.reset(.retain_capacity);

        const col = rg.columns.items[col_idx].meta_data orelse return error.ColumnMetaMissing;
        const chunk_start: usize = if (col.dictionary_page_offset) |dp|
            @intCast(dp)
        else
            @intCast(col.data_page_offset);
        const chunk_len: usize = @intCast(col.total_compressed_size);
        if (chunk_start + chunk_len > inv.body.len) return error.ChunkOutOfRange;
        const chunk = inv.body[chunk_start .. chunk_start + chunk_len];

        var reader = column_mod.ColumnChunkReader(i32).init(
            chunk,
            col.codec,
            levels,
            arena.allocator(),
        );

        // Stream-decode in 4K batches; keeps the working set small.
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
