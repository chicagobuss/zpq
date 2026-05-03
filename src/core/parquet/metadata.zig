//! Parquet file metadata: open the footer, expose row groups, prune.
//!
//! Schema parsing already lives in `src/core/schema.zig` (the Thrift-
//! encoded structs). This file is the thin layer that:
//!   1. Validates the leading + trailing PAR1 magic.
//!   2. Reads the 4-byte little-endian footer length.
//!   3. Slices the footer bytes and hands them to schema.FileMetaData.read.
//!   4. Exposes a stats-based row-group pruner for predicate pushdown.
//!
//! Input is a `[]const u8` covering the whole file. The caller decides
//! how those bytes got there (mmap, S3 GET, in-memory). For the Lambda
//! S3 path we'll later add a "footer-only fetch" helper that does a HEAD
//! + ranged GET, but the metadata layer itself stays I/O-agnostic.

const std = @import("std");
const schema = @import("../schema.zig");
const thrift = @import("../thrift.zig");

const MAGIC: [4]u8 = .{ 'P', 'A', 'R', '1' };
const MAGIC_LEN: usize = MAGIC.len;
const FOOTER_LEN_LEN: usize = 4;

pub const Error = error{
    TooSmall,
    BadMagic,
    FooterTooLarge,
    BadMetadata,
};

/// Parse the file footer and return a fully-realized FileMetaData.
/// Caller owns the result and must call `deinit`. The metadata holds
/// slices that point *into* `file_bytes`, so `file_bytes` must outlive
/// the FileMetaData.
pub fn open(
    allocator: std.mem.Allocator,
    file_bytes: []const u8,
) !schema.FileMetaData {
    if (file_bytes.len < MAGIC_LEN * 2 + FOOTER_LEN_LEN) return error.TooSmall;

    if (!std.mem.eql(u8, file_bytes[0..MAGIC_LEN], &MAGIC)) return error.BadMagic;
    const trailing_start = file_bytes.len - MAGIC_LEN;
    if (!std.mem.eql(u8, file_bytes[trailing_start..], &MAGIC)) return error.BadMagic;

    const footer_len_start = trailing_start - FOOTER_LEN_LEN;
    const footer_len = std.mem.readInt(
        u32,
        file_bytes[footer_len_start..][0..4],
        .little,
    );

    if (footer_len > file_bytes.len - MAGIC_LEN * 2 - FOOTER_LEN_LEN) return error.FooterTooLarge;

    const footer_start = footer_len_start - footer_len;
    const footer_bytes = file_bytes[footer_start..footer_len_start];

    var reader = thrift.Reader.init(footer_bytes);
    return schema.FileMetaData.read(allocator, &reader) catch return error.BadMetadata;
}

/// Resolve a top-level column path (for now: just a single name; nested
/// support arrives with definition-level handling) to its index in a
/// row group's column list. Returns null if the path doesn't match any
/// column. The schema is interpreted using the file's schema list, but
/// for flat schemas the index in `rg.columns` is also the index of the
/// matching SchemaElement minus 1 (the root has no column chunk).
pub fn findColumnIndex(
    file: *const schema.FileMetaData,
    column_name: []const u8,
) ?usize {
    // Skip schema[0] (root). The schema is in DFS order; for flat
    // schemas every non-root element is a leaf and the index in
    // row_group.columns matches.
    var leaf_idx: usize = 0;
    for (file.schema.items[1..]) |elem| {
        // For a flat schema, only leaves are present; we treat each
        // non-root element as a column. Nested schemas need a different
        // walk — Phase 2.
        if (std.mem.eql(u8, elem.name, column_name)) return leaf_idx;
        leaf_idx += 1;
    }
    return null;
}

/// Decision returned by the row-group pruner. `keep` if the row group
/// might contain matching rows; `skip` if its stats prove it can't.
/// `unknown` if the stats are missing/insufficient to decide.
pub const Decision = enum { keep, skip, unknown };

/// Equality pruner: returns `skip` iff the column's `[min, max]` stats
/// range doesn't include `needle`. Comparison is bytewise — correct for
/// `min_value`/`max_value` (which use the column's natural ordering)
/// but not for the deprecated unsigned `min`/`max`. We prefer
/// `min_value`/`max_value` and fall back to the deprecated fields only
/// for backward compatibility.
pub fn pruneEqual(
    rg: *const schema.RowGroup,
    column_index: usize,
    needle: []const u8,
) Decision {
    if (column_index >= rg.columns.items.len) return .unknown;
    const meta = rg.columns.items[column_index].meta_data orelse return .unknown;
    const stats = meta.statistics orelse return .unknown;

    const min = stats.min_value orelse stats.min orelse return .unknown;
    const max = stats.max_value orelse stats.max orelse return .unknown;

    if (std.mem.lessThan(u8, needle, min)) return .skip;
    if (std.mem.lessThan(u8, max, needle)) return .skip;
    return .keep;
}

/// Range pruner: returns `skip` iff the column's `[min, max]` doesn't
/// overlap the half-open interval `[lo, hi)`.
pub fn pruneRange(
    rg: *const schema.RowGroup,
    column_index: usize,
    lo: []const u8,
    hi: []const u8,
) Decision {
    if (column_index >= rg.columns.items.len) return .unknown;
    const meta = rg.columns.items[column_index].meta_data orelse return .unknown;
    const stats = meta.statistics orelse return .unknown;

    const min = stats.min_value orelse stats.min orelse return .unknown;
    const max = stats.max_value orelse stats.max orelse return .unknown;

    // Disjoint if max < lo OR min >= hi.
    if (std.mem.lessThan(u8, max, lo)) return .skip;
    if (!std.mem.lessThan(u8, min, hi)) return .skip;
    return .keep;
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "open rejects too-small input" {
    try testing.expectError(error.TooSmall, open(testing.allocator, ""));
    try testing.expectError(error.TooSmall, open(testing.allocator, "PAR1"));
}

test "open rejects bad magic" {
    var bytes: [16]u8 = undefined;
    @memcpy(bytes[0..4], "NOPE");
    @memset(bytes[4..12], 0);
    @memcpy(bytes[12..], "PAR1");
    try testing.expectError(error.BadMagic, open(testing.allocator, &bytes));
}

test "open the bench fixture" {
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{fixture_path});
            return;
        }
        return err;
    };
    defer testing.allocator.free(file_bytes);

    const t0 = nowNs();
    var meta = try open(testing.allocator, file_bytes);
    defer meta.deinit(testing.allocator);
    const elapsed_us = @divTrunc(nowNs() - t0, std.time.ns_per_us);

    try testing.expect(meta.num_rows > 0);
    try testing.expect(meta.row_groups.items.len > 0);
    try testing.expect(meta.schema.items.len > 1); // root + at least one column

    // Diagnostic: show what we parsed. Useful when working on later phases.
    std.debug.print(
        "\n[metadata] {s}: {d} rows, {d} row groups, {d} schema elems, parse {d} us\n",
        .{ fixture_path, meta.num_rows, meta.row_groups.items.len, meta.schema.items.len, elapsed_us },
    );
    if (meta.row_groups.items.len > 0) {
        const rg0 = &meta.row_groups.items[0];
        std.debug.print("[metadata] row group 0: {d} rows, {d} columns\n", .{ rg0.num_rows, rg0.columns.items.len });
        for (rg0.columns.items, 0..) |col, i| {
            const m = col.meta_data orelse continue;
            const path = if (m.path_in_schema.items.len > 0) m.path_in_schema.items[0] else "?";
            std.debug.print(
                "  col[{d}] {s} type={s} codec={s} encodings={d} num_values={d} compressed={d} uncompressed={d}\n",
                .{ i, path, @tagName(m.type), @tagName(m.codec), m.encodings.items.len, m.num_values, m.total_compressed_size, m.total_uncompressed_size },
            );
        }
    }
}

test "pruneEqual skips when stats range excludes the value" {
    // Build a synthetic RowGroup with one column whose stats say min='A', max='C'.
    var rg: schema.RowGroup = .{
        .columns = .empty,
        .total_byte_size = 0,
        .num_rows = 0,
    };
    defer rg.columns.deinit(testing.allocator);

    const meta: schema.ColumnMetaData = .{
        .type = .BYTE_ARRAY,
        .encodings = .empty,
        .path_in_schema = .empty,
        .codec = .UNCOMPRESSED,
        .num_values = 100,
        .total_uncompressed_size = 1024,
        .total_compressed_size = 1024,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = .{
            .min_value = "A",
            .max_value = "C",
        },
    };
    try rg.columns.append(testing.allocator, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = meta,
    });

    try testing.expectEqual(Decision.keep, pruneEqual(&rg, 0, "B"));
    try testing.expectEqual(Decision.skip, pruneEqual(&rg, 0, "Z"));
    try testing.expectEqual(Decision.skip, pruneEqual(&rg, 0, "0"));
    try testing.expectEqual(Decision.unknown, pruneEqual(&rg, 99, "X")); // bad index
}

// Read a file fully into memory using raw syscalls. Std.Io.Dir would
// require an Io vtable; the test suite doesn't have one set up.
fn readFileSlice(path: []const u8, allocator: std.mem.Allocator) ![]u8 {
    const linux = std.os.linux;
    var path_z: [256]u8 = undefined;
    if (path.len + 1 > path_z.len) return error.PathTooLong;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;

    const r_open = linux.openat(linux.AT.FDCWD, @ptrCast(&path_z[0]), .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    const fd: linux.fd_t = signedOrError(r_open) catch return error.FileNotFound;
    defer _ = linux.close(fd);

    // Get size via lseek(SEEK_END), then rewind. 0.16 stdlib dropped
    // fstat in favor of statx; lseek is the simplest path.
    const SEEK_END: usize = 2;
    const SEEK_SET: usize = 0;
    const end_pos = linux.lseek(fd, 0, SEEK_END);
    if (errIs(end_pos)) return error.SeekFailed;
    _ = linux.lseek(fd, 0, SEEK_SET);
    const size: usize = @intCast(end_pos);

    const buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);

    var off: usize = 0;
    while (off < size) {
        const n = linux.read(fd, buf[off..].ptr, size - off);
        if (errIs(n)) return error.ReadFailed;
        const bytes: usize = @intCast(n);
        if (bytes == 0) break;
        off += bytes;
    }
    return buf;
}

fn errIs(r: usize) bool {
    const signed: isize = @bitCast(r);
    return signed >= -4095 and signed < 0;
}

fn signedOrError(r: usize) error{SyscallFailed}!std.os.linux.fd_t {
    if (errIs(r)) return error.SyscallFailed;
    return @intCast(@as(isize, @bitCast(r)));
}

/// Monotonic nanoseconds. std.time.nanoTimestamp moved to std.Io.Clock
/// in 0.16; for tests we go direct to clock_gettime.
fn nowNs() i128 {
    var ts: std.os.linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + ts.nsec;
}
