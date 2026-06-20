//! Decode-path microbenchmark.
//!
//! Times N iterations of `consumer.decodeColumnT` against a single
//! column-chunk from a parquet file. Per-iteration arena reset. Strips
//! the surrounding glob/mmap/aggregate noise so we can see the decode
//! hot path in isolation.
//!
//! Usage:
//!   microbench <parquet.path> <column.name> [<runs>] [<warmup>]
//!
//! Reports JSON to stdout:
//!   {"name": "int64_random", "type": "INT64", "encodings": ["PLAIN_DICTIONARY"],
//!    "rows": 1048576, "compressed_bytes": 1234567, "uncompressed_bytes": ...,
//!    "runs": 7, "min_ns": 12345, "median_ns": 12500, "p95_ns": 13000,
//!    "ns_per_value": 11.93, "rows_per_sec": 83870000,
//!    "decoded_mb_per_sec": 670.0}
//!
//! Iterates ALL row groups concatenated — gives a more stable number
//! than picking a single RG, which can vary a lot in size.

const std = @import("std");
const linux = std.os.linux;
const zpq = @import("zpq");

const schema = zpq.core.schema;
const metadata = zpq.core.parquet.metadata;
const consumer = zpq.core.consumer;


pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = iter.next();
    const path = iter.next() orelse {
        std.debug.print("usage: microbench <parquet> <column> [runs=7] [warmup=2]\n", .{});
        return error.BadArgs;
    };
    const col_name = iter.next() orelse {
        std.debug.print("missing column name\n", .{});
        return error.BadArgs;
    };
    const runs: usize = if (iter.next()) |s| (std.fmt.parseInt(usize, s, 10) catch 7) else 7;
    const warmup: usize = if (iter.next()) |s| (std.fmt.parseInt(usize, s, 10) catch 2) else 2;

    // mmap the file.
    const map = try mmapFile(path);
    defer munmapFile(map);
    const file_bytes = map.bytes;

    var meta = try metadata.open(arena, file_bytes);

    // Find the column. Microbench operates on flat schemas only —
    // benchmark_100mb.parquet is flat. Fail loudly if it's nested.
    var col_idx: ?usize = null;
    for (meta.schema.items[1..], 0..) |elem, i| {
        if (std.mem.eql(u8, elem.name, col_name)) {
            col_idx = i;
            break;
        }
    }
    const ci = col_idx orelse {
        std.debug.print("column not found: {s}\n", .{col_name});
        return error.NoColumn;
    };

    // Aggregate the column across all RGs into one logical "decode all
    // bytes" measurement. Different RGs may have different encodings or
    // sizes — we average over them.
    const elem = meta.schema.items[ci + 1];
    const path_in_schema_buf = try arena.alloc([]const u8, 1);
    path_in_schema_buf[0] = elem.name;
    const levels = meta.getColumnLevels(path_in_schema_buf);

    var total_compressed: u64 = 0;
    var total_uncompressed: u64 = 0;
    var total_values: u64 = 0;
    var encodings: std.ArrayList([]const u8) = .empty;
    var codec: schema.CompressionCodec = .UNCOMPRESSED;
    var phys_type: schema.Type = .INT32;

    for (meta.row_groups.items) |rg| {
        const cm = rg.columns.items[ci].meta_data orelse continue;
        total_compressed += @intCast(cm.total_compressed_size);
        total_uncompressed += @intCast(cm.total_uncompressed_size);
        total_values += @intCast(cm.num_values);
        codec = cm.codec;
        phys_type = cm.type;
        for (cm.encodings.items) |e| {
            const en_str = encodingName(e);
            var seen = false;
            for (encodings.items) |x| if (std.mem.eql(u8, x, en_str)) {
                seen = true;
                break;
            };
            if (!seen) try encodings.append(arena, en_str);
        }
    }

    // Run benchmark. For each iteration: per-RG decode, sum the time.
    var samples = try arena.alloc(u64, runs);

    // Warmup runs (discarded).
    var w: usize = 0;
    while (w < warmup) : (w += 1) {
        _ = try benchOnce(gpa, &meta, file_bytes, ci, levels, phys_type);
    }

    var i: usize = 0;
    while (i < runs) : (i += 1) {
        samples[i] = try benchOnce(gpa, &meta, file_bytes, ci, levels, phys_type);
    }

    std.sort.pdq(u64, samples, {}, std.sort.asc(u64));
    const min_ns = samples[0];
    const median_ns = samples[samples.len / 2];
    const p95_idx = (samples.len * 95) / 100;
    const p95_ns = samples[if (p95_idx >= samples.len) samples.len - 1 else p95_idx];

    const ns_per_value = if (total_values > 0)
        @as(f64, @floatFromInt(min_ns)) / @as(f64, @floatFromInt(total_values))
    else
        0;
    const rows_per_sec = if (min_ns > 0)
        @as(f64, @floatFromInt(total_values)) * 1e9 / @as(f64, @floatFromInt(min_ns))
    else
        0;
    const decoded_mb_per_sec = if (min_ns > 0)
        @as(f64, @floatFromInt(total_uncompressed)) * 1e3 / @as(f64, @floatFromInt(min_ns))
    else
        0;

    var stdout: StdoutWriter = .{};
    defer stdout.flush();
    try stdout.print("{{\"name\":\"{s}\",\"type\":\"{s}\",\"codec\":\"{s}\"", .{
        col_name, @tagName(phys_type), @tagName(codec),
    });
    try stdout.print(",\"encodings\":[", .{});
    for (encodings.items, 0..) |e, k| {
        if (k > 0) try stdout.print(",", .{});
        try stdout.print("\"{s}\"", .{e});
    }
    try stdout.print("],\"max_def\":{d},\"max_rep\":{d}", .{ levels.max_def, levels.max_rep });
    try stdout.print(",\"rows\":{d},\"compressed_bytes\":{d},\"uncompressed_bytes\":{d}", .{
        total_values, total_compressed, total_uncompressed,
    });
    try stdout.print(",\"runs\":{d},\"min_ns\":{d},\"median_ns\":{d},\"p95_ns\":{d}", .{
        runs, min_ns, median_ns, p95_ns,
    });
    try stdout.print(",\"ns_per_value\":{d:.3},\"rows_per_sec\":{d:.0},\"decoded_mb_per_sec\":{d:.1}}}\n", .{
        ns_per_value, rows_per_sec, decoded_mb_per_sec,
    });
}

/// One benchmark iteration: decode the column across every RG. Returns
/// total nanoseconds. Per-iteration arena reset so allocations don't
/// pollute timings (decode allocates temporary buffers per RG).
fn benchOnce(
    gpa: std.mem.Allocator,
    meta: *const schema.FileMetaData,
    file_bytes: []const u8,
    col_idx: usize,
    levels: schema.Levels,
    phys_type: schema.Type,
) !u64 {
    var iter_arena = std.heap.ArenaAllocator.init(gpa);
    defer iter_arena.deinit();
    const a = iter_arena.allocator();

    const t0 = nowMonoNs();
    for (meta.row_groups.items) |rg| {
        const cm = rg.columns.items[col_idx].meta_data orelse continue;
        const start: usize = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
        const len: usize = @intCast(cm.total_compressed_size);
        if (start + len > file_bytes.len) return error.MissingChunkBytes;
        const chunk = file_bytes[start .. start + len];
        const num_values: usize = @intCast(cm.num_values);

        switch (phys_type) {
            .INT32 => _ = try consumer.decodeColumnT(i32, a, chunk, cm.codec, levels, num_values),
            .INT64 => _ = try consumer.decodeColumnT(i64, a, chunk, cm.codec, levels, num_values),
            .FLOAT => _ = try consumer.decodeColumnT(f32, a, chunk, cm.codec, levels, num_values),
            .DOUBLE => _ = try consumer.decodeColumnT(f64, a, chunk, cm.codec, levels, num_values),
            .BYTE_ARRAY => _ = try consumer.decodeColumnT([]const u8, a, chunk, cm.codec, levels, num_values),
            .BOOLEAN => _ = try consumer.decodeColumnT(bool, a, chunk, cm.codec, levels, num_values),
            else => return error.UnsupportedType,
        }
    }
    return @intCast(nowMonoNs() - t0);
}

fn encodingName(e: schema.Encoding) []const u8 {
    return @tagName(e);
}

fn nowMonoNs() i64 {
    var ts: linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, ts.sec) * std.time.ns_per_s + @as(i64, ts.nsec);
}

const MmapFile = struct {
    bytes: []const u8,
};

fn mmapFile(path: []const u8) !MmapFile {
    var path_z: [4096]u8 = undefined;
    if (path.len + 1 > path_z.len) return error.PathTooLong;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;

    const r_open = linux.openat(linux.AT.FDCWD, @ptrCast(&path_z[0]), .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    const r_open_signed: isize = @bitCast(r_open);
    if (r_open_signed < 0) return error.OpenFailed;
    const fd: linux.fd_t = @intCast(r_open_signed);
    defer _ = linux.close(fd);

    const SEEK_END: usize = 2;
    const end_pos = linux.lseek(fd, 0, SEEK_END);
    const size: usize = @intCast(end_pos);
    if (size == 0) return error.EmptyFile;

    const r_map = linux.mmap(
        null,
        size,
        .{ .READ = true },
        .{ .TYPE = .PRIVATE },
        @intCast(fd),
        0,
    );
    const r_signed: isize = @bitCast(r_map);
    if (r_signed < 0) return error.MmapFailed;

    const ptr: [*]const u8 = @ptrFromInt(r_map);
    return .{ .bytes = ptr[0..size] };
}

fn munmapFile(m: MmapFile) void {
    _ = linux.munmap(@constCast(@ptrCast(m.bytes.ptr)), m.bytes.len);
}

const StdoutWriter = struct {
    fd: linux.fd_t = 1,
    buf: [4096]u8 = undefined,
    pos: usize = 0,

    pub fn writeAll(self: *StdoutWriter, bytes: []const u8) !void {
        var i: usize = 0;
        while (i < bytes.len) {
            const space = self.buf.len - self.pos;
            const n = @min(bytes.len - i, space);
            @memcpy(self.buf[self.pos..][0..n], bytes[i..][0..n]);
            self.pos += n;
            i += n;
            if (self.pos == self.buf.len) self.flush();
        }
    }

    pub fn print(self: *StdoutWriter, comptime fmt: []const u8, args: anytype) !void {
        var tmp: [1024]u8 = undefined;
        const out = try std.fmt.bufPrint(&tmp, fmt, args);
        try self.writeAll(out);
    }

    pub fn flush(self: *StdoutWriter) void {
        if (self.pos == 0) return;
        var written: usize = 0;
        while (written < self.pos) {
            const r = linux.write(self.fd, self.buf[written..].ptr, self.pos - written);
            const n: isize = @bitCast(r);
            if (n <= 0) break;
            written += @intCast(n);
        }
        self.pos = 0;
    }
};

