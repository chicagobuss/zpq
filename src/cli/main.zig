//! ZPQ CLI binary entry point.
//!
//! Subcommands:
//!   zpq query <input.parquet> --output <out.parquet>
//!         [--filter EXPR] [--columns COL1,COL2,...]
//!         [--codec snappy|zstd|uncompressed]
//!     — local-file query: decode → filter → re-encode → write.
//!       JSON envelope with phase timings goes to stderr.
//!
//!   zpq conform <file.parquet>   — emit a JSON report of what ZPQ
//!                                  sees in this file. Used by the
//!                                  conformance runner against the
//!                                  apache/parquet-testing corpus.

const std = @import("std");
const zpq = @import("zpq");
const query = @import("query.zig");

const metadata = zpq.core.parquet.metadata;
const schema = zpq.core.schema;
const column = zpq.core.parquet.column;
const schema_tree = zpq.core.parquet.schema_tree;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    var iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = iter.next(); // skip program name
    const cmd = iter.next() orelse {
        std.debug.print(
            \\usage:
            \\  zpq query <input.parquet> --output <out.parquet>
            \\            [--filter EXPR] [--columns COL1,COL2,...]
            \\            [--codec snappy|zstd|uncompressed]
            \\  zpq conform <file.parquet>
            \\
        , .{});
        return;
    };
    if (std.mem.eql(u8, cmd, "query")) {
        try runQuery(gpa, &iter);
        return;
    }
    if (std.mem.eql(u8, cmd, "conform")) {
        const path = iter.next() orelse {
            std.debug.print("conform: missing path\n", .{});
            return;
        };
        try runConform(gpa, path);
        return;
    }
    std.debug.print("unknown subcommand: {s}\n", .{cmd});
}

fn runQuery(gpa: std.mem.Allocator, iter: *std.process.Args.Iterator) !void {
    var input: ?[]const u8 = null;
    var output: ?[]const u8 = null;
    var filter: ?[]const u8 = null;
    var columns_csv: ?[]const u8 = null;
    var codec: schema.CompressionCodec = .SNAPPY;

    // First positional after "query" is the input path; subsequent
    // tokens are flag/value pairs.
    while (iter.next()) |tok| {
        if (std.mem.eql(u8, tok, "--output") or std.mem.eql(u8, tok, "-o")) {
            output = iter.next();
        } else if (std.mem.eql(u8, tok, "--filter") or std.mem.eql(u8, tok, "-f")) {
            filter = iter.next();
        } else if (std.mem.eql(u8, tok, "--columns") or std.mem.eql(u8, tok, "-c")) {
            columns_csv = iter.next();
        } else if (std.mem.eql(u8, tok, "--codec")) {
            const v = iter.next() orelse continue;
            if (std.ascii.eqlIgnoreCase(v, "zstd")) codec = .ZSTD;
            if (std.ascii.eqlIgnoreCase(v, "uncompressed")) codec = .UNCOMPRESSED;
            // anything else stays SNAPPY (default)
        } else if (input == null) {
            input = tok;
        } else {
            std.debug.print("zpq query: unexpected arg: {s}\n", .{tok});
            return error.BadArgs;
        }
    }

    const in = input orelse {
        std.debug.print("zpq query: missing <input.parquet>\n", .{});
        return error.BadArgs;
    };
    const out = output orelse {
        std.debug.print("zpq query: missing --output <path>\n", .{});
        return error.BadArgs;
    };

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const cols: ?[]const []const u8 = if (columns_csv) |csv|
        try splitCsv(arena, csv)
    else
        null;

    const t_start = nowMonoNs();
    const result = try query.run(gpa, .{
        .input = in,
        .output = out,
        .filter = filter,
        .columns = cols,
        .codec = codec,
    });
    const total_ms = @divTrunc(nowMonoNs() - t_start, std.time.ns_per_ms);

    // JSON envelope to stderr — same shape as the Lambda response.
    var w: StdoutWriter = .{ .fd = 2 };
    defer w.flush();
    try w.print(
        \\{{"ok":true,"input":"
    , .{});
    try writeJsonString(&w, in);
    try w.print(
        \\","output":"
    , .{});
    try writeJsonString(&w, out);
    try w.print(
        \\","codec":"{s}","rows_in":{d},"rows_kept":{d},"bytes_in":{d},"bytes_out":{d},"row_groups_in":{d},"row_groups_kept":{d},"total_ms":{d},"phase":{{"read_ms":{d},"parse_ms":{d},"decode_ms":{d},"eval_ms":{d},"encode_ms":{d},"sink_ms":{d},"footer_ms":{d}}}}}
        \\
    , .{
        @tagName(codec),
        result.rows_in,
        result.rows_kept,
        result.bytes_in,
        result.bytes_out,
        result.row_groups_in,
        result.row_groups_kept,
        total_ms,
        result.timings.read_ns / std.time.ns_per_ms,
        result.timings.parse_ns / std.time.ns_per_ms,
        result.timings.core.decode_ns / std.time.ns_per_ms,
        result.timings.core.eval_ns / std.time.ns_per_ms,
        result.timings.core.encode_ns / std.time.ns_per_ms,
        result.timings.core.sink_ns / std.time.ns_per_ms,
        result.timings.footer_ns / std.time.ns_per_ms,
    });
}

fn splitCsv(arena: std.mem.Allocator, csv: []const u8) ![]const []const u8 {
    var out: std.ArrayList([]const u8) = .empty;
    var iter = std.mem.splitScalar(u8, csv, ',');
    while (iter.next()) |s| {
        const trimmed = std.mem.trim(u8, s, " \t");
        if (trimmed.len > 0) try out.append(arena, trimmed);
    }
    return out.items;
}

fn nowMonoNs() i64 {
    var ts: std.os.linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, ts.sec) * std.time.ns_per_s + @as(i64, ts.nsec);
}

/// Open a parquet file via ZPQ and emit a JSON report describing what
/// we saw — schema leaves, row count, and a per-column-chunk decode
/// status (ok/error). Output goes to stdout. Caller (Python harness)
/// compares against pyarrow.
fn runConform(gpa: std.mem.Allocator, path: []const u8) !void {
    const file_bytes = readFile(gpa, path) catch |err| {
        try jsonFatal(gpa, "open_failed", @errorName(err));
        return;
    };
    defer gpa.free(file_bytes);

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const meta = metadata.open(arena, file_bytes) catch |err| {
        try jsonFatal(gpa, "metadata_open_failed", @errorName(err));
        return;
    };

    var w: StdoutWriter = .{};
    defer w.flush();

    try w.writeAll("{\"path\":\"");
    try writeJsonString(&w, path);
    try w.print("\",\"num_rows\":{d},\"num_row_groups\":{d},\"num_schema_elems\":{d},\"created_by\":\"", .{ meta.num_rows, meta.row_groups.items.len, meta.schema.items.len });
    if (meta.created_by) |cb| try writeJsonString(&w, cb);

    // Build the schema tree — gives nested-aware leaf info. If this
    // fails, the file's schema is something we can't represent.
    const tree_result = schema_tree.SchemaTree.build(arena, meta.schema.items);
    if (tree_result) |tree| {
        try w.print("\",\"tree_leaves\":{d},\"leaves\":[", .{tree.leaves.len});
        var first_leaf = true;
        for (tree.leaves) |leaf| {
            if (!first_leaf) try w.writeAll(",");
            first_leaf = false;
            try w.writeAll("{\"name\":\"");
            try writeJsonString(&w, leaf.name);
            try w.writeAll("\",\"path\":\"");
            // Joined path for easy comparison with pyarrow / hardwood.
            for (leaf.path, 0..) |seg, i| {
                if (i > 0) try w.writeAll(".");
                try writeJsonString(&w, seg);
            }
            try w.writeAll("\",\"type\":\"");
            try w.writeAll(@tagName(leaf.type));
            try w.print("\",\"max_def\":{d},\"max_rep\":{d},\"column_index\":{d}}}", .{ leaf.max_def, leaf.max_rep, leaf.column_index });
        }
    } else |err| {
        try w.print("\",\"tree_build_failed\":\"{s}\",\"leaves\":[", .{@errorName(err)});
    }
    try w.writeAll("],\"decode\":[");

    // Try to decode every flat leaf in the first row group.
    var first_dec = true;
    if (meta.row_groups.items.len > 0) {
        const rg0 = &meta.row_groups.items[0];
        for (rg0.columns.items, 0..) |chunk, ci| {
            if (!first_dec) try w.writeAll(",");
            first_dec = false;
            try w.writeAll("{\"col_idx\":");
            try w.print("{d}", .{ci});
            try w.writeAll(",\"name\":\"");
            const cm = chunk.meta_data orelse {
                try w.writeAll("?\",\"status\":\"missing_meta\"}");
                continue;
            };
            const col_name = if (cm.path_in_schema.items.len > 0) cm.path_in_schema.items[0] else "?";
            try writeJsonString(&w, col_name);
            try w.writeAll("\",\"type\":\"");
            try w.writeAll(@tagName(cm.type));
            try w.writeAll("\",\"codec\":\"");
            try w.writeAll(@tagName(cm.codec));
            try w.writeAll("\",\"num_values\":");
            try w.print("{d}", .{cm.num_values});
            try w.writeAll(",\"status\":\"");

            const status = tryDecodeColumn(arena, file_bytes, &meta, rg0, chunk);
            try writeJsonString(&w, status);
            try w.writeAll("\"}");
        }
    }
    try w.writeAll("]}\n");
}

fn tryDecodeColumn(
    arena: std.mem.Allocator,
    file_bytes: []const u8,
    meta: *const schema.FileMetaData,
    rg: *const schema.RowGroup,
    chunk: schema.ColumnChunk,
) []const u8 {
    _ = rg;
    const cm = chunk.meta_data orelse return "missing_meta";
    const start: usize = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
    const len: usize = @intCast(cm.total_compressed_size);
    if (start + len > file_bytes.len) return "out_of_range";
    const chunk_bytes = file_bytes[start .. start + len];

    // Use the full path_in_schema (multi-element for nested columns)
    // so getColumnLevels walks the tree and computes max_def/max_rep
    // correctly, instead of treating every column as flat.
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    const num_rows: usize = @intCast(cm.num_values);

    return switch (cm.type) {
        .INT32 => decodeOne(i32, arena, chunk_bytes, cm.codec, levels, num_rows),
        .INT64 => decodeOne(i64, arena, chunk_bytes, cm.codec, levels, num_rows),
        .FLOAT => decodeOne(f32, arena, chunk_bytes, cm.codec, levels, num_rows),
        .DOUBLE => decodeOne(f64, arena, chunk_bytes, cm.codec, levels, num_rows),
        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => decodeOne([]const u8, arena, chunk_bytes, cm.codec, levels, num_rows),
        .BOOLEAN => decodeOne(bool, arena, chunk_bytes, cm.codec, levels, num_rows),
        .INT96 => "skip_int96",
    };
}

fn decodeOne(
    comptime T: type,
    arena: std.mem.Allocator,
    chunk_bytes: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_rows: usize,
) []const u8 {
    var rdr = column.ColumnChunkReader(T).init(chunk_bytes, codec, levels, arena);
    const values = arena.alloc(T, num_rows) catch return "alloc_failed";
    if (levels.max_rep > 0) {
        const def_levels = arena.alloc(u32, num_rows) catch return "alloc_failed";
        const rep_levels = arena.alloc(u32, num_rows) catch return "alloc_failed";
        var written: usize = 0;
        while (written < num_rows) {
            const n = rdr.decodeWithRepLevels(values[written..], def_levels[written..], rep_levels[written..]) catch |e| return @errorName(e);
            if (n == 0) break;
            written += n;
        }
        if (written != num_rows) return "short_decode";
    } else if (levels.max_def > 0) {
        const def_levels = arena.alloc(u32, num_rows) catch return "alloc_failed";
        var written: usize = 0;
        while (written < num_rows) {
            const n = rdr.decodeWithLevels(values[written..], def_levels[written..]) catch |e| return @errorName(e);
            if (n == 0) break;
            written += n;
        }
        if (written != num_rows) return "short_decode";
    } else {
        var written: usize = 0;
        while (written < num_rows) {
            const n = rdr.decode(values[written..]) catch |e| return @errorName(e);
            if (n == 0) break;
            written += n;
        }
        if (written != num_rows) return "short_decode";
    }
    return "ok";
}

fn jsonFatal(gpa: std.mem.Allocator, kind: []const u8, reason: []const u8) !void {
    var w: StdoutWriter = .{};
    defer w.flush();
    _ = gpa;
    try w.writeAll("{\"error\":\"");
    try writeJsonString(&w, kind);
    try w.writeAll("\",\"reason\":\"");
    try writeJsonString(&w, reason);
    try w.writeAll("\"}\n");
}

fn writeJsonString(w: *StdoutWriter, s: []const u8) !void {
    for (s) |c| {
        switch (c) {
            '"' => try w.writeAll("\\\""),
            '\\' => try w.writeAll("\\\\"),
            '\n' => try w.writeAll("\\n"),
            '\r' => try w.writeAll("\\r"),
            '\t' => try w.writeAll("\\t"),
            else => {
                if (c < 0x20) {
                    var buf: [8]u8 = undefined;
                    const n = std.fmt.bufPrint(&buf, "\\u{x:0>4}", .{c}) catch unreachable;
                    try w.writeAll(n);
                } else {
                    try w.writeAll(&[_]u8{c});
                }
            },
        }
    }
}

/// Tiny buffered writer using direct linux.write syscalls — avoids
/// the std.Io vtable so this binary stays self-contained. fd
/// defaults to stdout (1); set fd=2 for stderr.
const StdoutWriter = struct {
    const linux = std.os.linux;
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
        var tmp: [512]u8 = undefined;
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

fn readFile(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    const linux = std.os.linux;
    var path_z: [1024]u8 = undefined;
    if (path.len + 1 > path_z.len) return error.PathTooLong;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;

    const r_open = linux.openat(linux.AT.FDCWD, @ptrCast(&path_z[0]), .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    const r_open_signed: isize = @bitCast(r_open);
    if (r_open_signed < 0) return error.OpenFailed;
    const fd: linux.fd_t = @intCast(r_open_signed);
    defer _ = linux.close(fd);

    const SEEK_END: usize = 2;
    const SEEK_SET: usize = 0;
    const end_pos = linux.lseek(fd, 0, SEEK_END);
    _ = linux.lseek(fd, 0, SEEK_SET);
    const size: usize = @intCast(end_pos);

    const buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);

    var off: usize = 0;
    while (off < size) {
        const r = linux.read(fd, buf[off..].ptr, size - off);
        const n: isize = @bitCast(r);
        if (n <= 0) break;
        off += @intCast(n);
    }
    if (off != size) return error.ShortRead;
    return buf;
}
