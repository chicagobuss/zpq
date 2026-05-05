//! `zpq query` — local-file query subcommand.
//!
//! Decodes a parquet file from disk, applies an optional filter +
//! column projection, and writes a fresh parquet file to disk. Same
//! engine used by the Lambda binary (encoder, dictionary writer,
//! delta encoder, snappy/zstd codecs) but driven by a single-file
//! orchestrator instead of the multi-file streaming sink + S3 pool.
//!
//! The point of this binary is local iteration: write a query, run
//! it on a workstation, see it work end-to-end without round-
//! tripping through AWS Lambda.
//!
//! The orchestrator is deliberately simpler than `lambda/main.zig`'s:
//! one file, no S3, no parallel fetcher workers, no multipart sink.
//! For full streaming-input + cross-file parallelism, deploy to
//! Lambda. The CLI's job is "fast feedback for one query at a time."

const std = @import("std");
const zpq = @import("zpq");

const schema = zpq.core.schema;
const metadata = zpq.core.parquet.metadata;
const column_mod = zpq.core.parquet.column;
const schema_tree = zpq.core.parquet.schema_tree;
const thrift = zpq.core.thrift;
const filter_ast = zpq.core.filter.ast;
const filter_parser = zpq.core.filter.parser;
const filter_prune = zpq.core.filter.prune;
const filter_selection = zpq.core.filter.selection;
const filter_eval = zpq.core.filter.eval;
const encoder = zpq.core.writer.encoder;
const fastpath = zpq.core.writer.fastpath;

const PAR1: [4]u8 = .{ 'P', 'A', 'R', '1' };

pub const Args = struct {
    input: []const u8,
    output: []const u8,
    filter: ?[]const u8 = null,
    columns: ?[]const []const u8 = null,
    codec: schema.CompressionCodec = .SNAPPY,
};

pub const Timings = struct {
    read_ns: u64 = 0,
    parse_ns: u64 = 0,
    decode_ns: u64 = 0,
    eval_ns: u64 = 0,
    encode_ns: u64 = 0,
    write_ns: u64 = 0,
};

pub const Result = struct {
    rows_in: i64,
    rows_kept: i64,
    bytes_in: u64,
    bytes_out: u64,
    row_groups_in: usize,
    row_groups_kept: usize,
    timings: Timings,
};

/// Run a local-file query. Returns a Result with size + timing
/// information. All bytes are read/written via raw linux syscalls
/// to avoid pulling in std.fs's Io vtable plumbing.
pub fn run(gpa: std.mem.Allocator, args: Args) !Result {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var t: Timings = .{};

    // 1. Read input file.
    const t_read_start = nowMonoNs();
    const file_bytes = try readFile(gpa, args.input);
    defer gpa.free(file_bytes);
    t.read_ns = @intCast(nowMonoNs() - t_read_start);

    const t_parse_start = nowMonoNs();
    var meta = try metadata.open(arena, file_bytes);
    const tree = try schema_tree.SchemaTree.build(arena, meta.schema.items);

    // 2. Resolve column projection. Names → leaf indices via the
    // schema tree (handles nested structs/lists/maps correctly).
    var kept_set: ?[]bool = null;
    if (args.columns) |cols| {
        const num_leaves = tree.leaves.len;
        const set = try arena.alloc(bool, num_leaves);
        @memset(set, false);
        for (cols) |name| {
            const indices = try tree.resolveTopLevel(arena, name);
            if (indices.len == 0) {
                std.debug.print("zpq query: column not found: {s}\n", .{name});
                return error.UnknownColumn;
            }
            for (indices) |idx| set[idx] = true;
        }
        kept_set = set;
    }

    // 3. Parse filter (against the schema; column names → indices).
    var filter_opt: ?filter_ast.Filter = null;
    if (args.filter) |expr| {
        if (expr.len > 0) {
            filter_opt = try filter_parser.parse(arena, expr, &meta);
        }
    }
    t.parse_ns = @intCast(nowMonoNs() - t_parse_start);

    // 4. Open output file.
    const out_fd = try createFile(args.output);
    defer _ = std.os.linux.close(out_fd);

    var out_offset: u64 = 0;
    try writeAll(out_fd, &PAR1);
    out_offset += PAR1.len;

    // 5. Build per-leaf bool vectors that mirror what the Lambda
    // engine uses: `kept_set` (output columns) and `fetch_set`
    // (output columns ∪ filter input columns).
    const num_leaves = meta.row_groups.items[0].columns.items.len;
    const kept_arr = try arena.alloc(bool, num_leaves);
    if (kept_set) |s| {
        @memcpy(kept_arr, s);
    } else {
        @memset(kept_arr, true);
    }
    const fetch_arr = try arena.alloc(bool, num_leaves);
    @memcpy(fetch_arr, kept_arr);
    if (filter_opt) |f| {
        var filter_cols: std.ArrayList(usize) = .empty;
        try f.collectColumns(&filter_cols, arena);
        for (filter_cols.items) |ci| if (ci < num_leaves) {
            fetch_arr[ci] = true;
        };
    }

    var kept_in_order: std.ArrayList(usize) = .empty;
    for (kept_arr, 0..) |b, i| if (b) try kept_in_order.append(arena, i);

    // 6. Iterate row groups.
    var rows_in: i64 = 0;
    var rows_kept: i64 = 0;
    var rg_kept: usize = 0;
    var new_row_groups: std.ArrayListUnmanaged(schema.RowGroup) = .empty;

    for (meta.row_groups.items, 0..) |*src_rg, rg_idx| {
        rows_in += src_rg.num_rows;

        // Stat-prune.
        if (filter_opt) |f| {
            if ((try filter_prune.pruneRowGroup(src_rg, f, arena)) == .skip) continue;
        }

        if (filter_opt) |f| {
            const surviving = try processFilteredRG(
                arena,
                gpa,
                file_bytes,
                &meta,
                src_rg,
                fetch_arr,
                kept_in_order.items,
                f,
                args.codec,
                out_fd,
                &out_offset,
                &new_row_groups,
                &t,
            );
            rows_kept += surviving;
            if (surviving > 0) rg_kept += 1;
        } else {
            try processFastpathRG(
                arena,
                file_bytes,
                src_rg,
                kept_set,
                out_fd,
                &out_offset,
                &new_row_groups,
                &t,
            );
            rows_kept += src_rg.num_rows;
            rg_kept += 1;
        }
        _ = rg_idx;
    }

    // 7. Build footer.
    const t_write_start = nowMonoNs();

    var new_schema = meta.schema;
    if (kept_set) |_| {
        const kept_u32 = try arena.alloc(u32, kept_in_order.items.len);
        for (kept_in_order.items, 0..) |idx, i| kept_u32[i] = @intCast(idx);
        const projected = try tree.projectSubset(arena, kept_u32);
        new_schema = try projected.writeFlatThrift(arena);
    }

    const new_meta: schema.FileMetaData = .{
        .version = meta.version,
        .schema = new_schema,
        .num_rows = rows_kept,
        .created_by = meta.created_by,
        .row_groups = new_row_groups,
    };

    var w: thrift.Writer = .init(arena);
    defer w.deinit();
    try new_meta.write(&w);
    const footer_bytes = w.bytes();
    try writeAll(out_fd, footer_bytes);
    out_offset += footer_bytes.len;

    var len_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_bytes, @intCast(footer_bytes.len), .little);
    try writeAll(out_fd, &len_bytes);
    out_offset += len_bytes.len;
    try writeAll(out_fd, &PAR1);
    out_offset += PAR1.len;

    t.write_ns += @intCast(nowMonoNs() - t_write_start);

    return .{
        .rows_in = rows_in,
        .rows_kept = rows_kept,
        .bytes_in = file_bytes.len,
        .bytes_out = out_offset,
        .row_groups_in = meta.row_groups.items.len,
        .row_groups_kept = rg_kept,
        .timings = t,
    };
}

/// Encoder path: decode → filter → re-encode → write. Mirrors the
/// Lambda's encodeOneRG but with a plain file_fd output.
fn processFilteredRG(
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    file_bytes: []const u8,
    meta: *const schema.FileMetaData,
    rg: *const schema.RowGroup,
    fetch_set: []const bool,
    kept_in_order: []const usize,
    filter: filter_ast.Filter,
    codec: schema.CompressionCodec,
    out_fd: std.os.linux.fd_t,
    out_offset: *u64,
    new_row_groups: *std.ArrayListUnmanaged(schema.RowGroup),
    t: *Timings,
) !i64 {
    const num_rows: usize = @intCast(rg.num_rows);
    const num_leaves = rg.columns.items.len;

    var rg_arena_state = std.heap.ArenaAllocator.init(gpa);
    defer rg_arena_state.deinit();
    const ra = rg_arena_state.allocator();

    // Decode every column in fetch_set.
    const t_decode_start = nowMonoNs();
    var batch_cols: std.ArrayList(filter_eval.Batch.Column) = .empty;
    var lookup = try ra.alloc(?usize, meta.schema.items.len);
    @memset(lookup, null);
    var batch_pos_for_col = try ra.alloc(?usize, num_leaves);
    @memset(batch_pos_for_col, null);

    for (fetch_set, 0..) |needed, ci| {
        if (!needed) continue;
        const col_meta = rg.columns.items[ci].meta_data orelse return error.ColumnMetaMissing;
        const start: usize = if (col_meta.dictionary_page_offset) |dp| @intCast(dp) else @intCast(col_meta.data_page_offset);
        const len: usize = @intCast(col_meta.total_compressed_size);
        if (start + len > file_bytes.len) return error.MissingChunkBytes;
        const chunk = file_bytes[start .. start + len];

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
    const t_decode_end = nowMonoNs();
    t.decode_ns += @intCast(t_decode_end - t_decode_start);

    // Eval.
    const batch: filter_eval.Batch = .{ .cols = batch_cols.items, .num_rows = num_rows };
    var sel = try filter_selection.SelectionVector.init(ra, num_rows);
    try filter_eval.evaluate(filter, &batch, &sel, lookup, ra);
    const t_eval_end = nowMonoNs();
    t.eval_ns += @intCast(t_eval_end - t_decode_end);

    const surviving = sel.count();
    if (surviving == 0) return 0;

    // Encode kept columns + write to file.
    var rg_columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try rg_columns.ensureTotalCapacity(arena, kept_in_order.len);
    var rg_total: i64 = 0;

    for (kept_in_order) |kept_ci| {
        const batch_pos = batch_pos_for_col[kept_ci] orelse return error.MissingDecodedColumn;
        const filtered = try encoder.applySelection(arena, batch_cols.items[batch_pos], &sel);

        const cm = rg.columns.items[kept_ci].meta_data orelse return error.ColumnMetaMissing;
        const leaf_elem = meta.getColumnSchema(cm.path_in_schema.items) orelse return error.SchemaLookupFailed;
        const t_enc_start = nowMonoNs();
        const enc = try encoder.encodeColumn(arena, .{
            .values = filtered,
            .schema_elem = &leaf_elem,
            .path_in_schema = cm.path_in_schema.items,
            .codec = codec,
        });
        const t_enc_end = nowMonoNs();
        t.encode_ns += @intCast(t_enc_end - t_enc_start);

        const col_start: i64 = @intCast(out_offset.*);
        var em = enc.meta;
        em.data_page_offset += col_start;
        if (em.dictionary_page_offset) |dpo| em.dictionary_page_offset = dpo + col_start;

        try writeAll(out_fd, enc.bytes);
        const t_write_end = nowMonoNs();
        t.write_ns += @intCast(t_write_end - t_enc_end);

        out_offset.* += enc.bytes.len;
        rg_total += @intCast(enc.bytes.len);

        try rg_columns.append(arena, .{
            .file_path = null,
            .file_offset = col_start,
            .meta_data = em,
        });
    }

    try new_row_groups.append(arena, .{
        .columns = rg_columns,
        .total_byte_size = rg_total,
        .num_rows = @intCast(surviving),
    });

    return @intCast(surviving);
}

/// Fastpath: byte-copy the surviving column chunks into the output
/// file, clone the row-group metadata with offsets shifted.
fn processFastpathRG(
    arena: std.mem.Allocator,
    file_bytes: []const u8,
    src_rg: *const schema.RowGroup,
    kept_set: ?[]bool,
    out_fd: std.os.linux.fd_t,
    out_offset: *u64,
    new_row_groups: *std.ArrayListUnmanaged(schema.RowGroup),
    t: *Timings,
) !void {
    const t_write_start = nowMonoNs();

    if (kept_set == null) {
        // Whole-RG byte-copy.
        const range = fastpath_helpers.rowGroupByteRange(src_rg) orelse {
            t.write_ns += @intCast(nowMonoNs() - t_write_start);
            return;
        };
        const new_start = out_offset.*;
        try writeAll(out_fd, file_bytes[range.start .. range.start + range.len]);
        out_offset.* += range.len;
        const delta: i64 = @as(i64, @intCast(new_start)) - @as(i64, @intCast(range.start));
        const cloned = try fastpath_helpers.cloneRowGroupShifted(arena, src_rg, delta);
        try new_row_groups.append(arena, cloned);
    } else {
        // Per-column byte-copy.
        const kept = kept_set.?;
        var new_cols: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
        var rg_total: i64 = 0;
        for (kept, 0..) |b, ci| {
            if (!b) continue;
            const src_chunk = src_rg.columns.items[ci];
            const m = src_chunk.meta_data orelse return error.InvalidColumnOffsets;
            const src_start: usize = if (m.dictionary_page_offset) |d| @intCast(d) else @intCast(m.data_page_offset);
            const src_len: usize = @intCast(m.total_compressed_size);
            if (src_start + src_len > file_bytes.len) return error.InvalidColumnOffsets;

            const new_col_start = out_offset.*;
            try writeAll(out_fd, file_bytes[src_start .. src_start + src_len]);
            out_offset.* += src_len;

            const delta: i64 = @as(i64, @intCast(new_col_start)) - @as(i64, @intCast(src_start));
            var new_chunk = src_chunk;
            new_chunk.offset_index_offset = null;
            new_chunk.offset_index_length = null;
            new_chunk.column_index_offset = null;
            new_chunk.column_index_length = null;
            if (new_chunk.meta_data) |*nm| {
                nm.data_page_offset += delta;
                if (nm.dictionary_page_offset) |d| nm.dictionary_page_offset = d + delta;
                if (nm.index_page_offset) |d| nm.index_page_offset = d + delta;
            }
            if (new_chunk.meta_data) |nm| new_chunk.file_offset = nm.data_page_offset;
            try new_cols.append(arena, new_chunk);
            rg_total += @intCast(src_len);
        }
        try new_row_groups.append(arena, .{
            .columns = new_cols,
            .total_byte_size = rg_total,
            .num_rows = src_rg.num_rows,
        });
    }

    t.write_ns += @intCast(nowMonoNs() - t_write_start);
}

// Helpers borrowed from streaming.zig — not pub there, so we
// reimplement the small ones here.
const fastpath_helpers = struct {
    const ByteRange = struct { start: usize, len: usize };

    fn rowGroupByteRange(rg: *const schema.RowGroup) ?ByteRange {
        if (rg.columns.items.len == 0) return null;
        var min_start: u64 = std.math.maxInt(u64);
        var max_end: u64 = 0;
        for (rg.columns.items) |chunk| {
            const m = chunk.meta_data orelse continue;
            const start: u64 = if (m.dictionary_page_offset) |d| @intCast(d) else @intCast(m.data_page_offset);
            const end: u64 = start + @as(u64, @intCast(m.total_compressed_size));
            if (start < min_start) min_start = start;
            if (end > max_end) max_end = end;
        }
        if (min_start == std.math.maxInt(u64)) return null;
        return .{ .start = @intCast(min_start), .len = @intCast(max_end - min_start) };
    }

    fn cloneRowGroupShifted(
        arena: std.mem.Allocator,
        src: *const schema.RowGroup,
        delta: i64,
    ) !schema.RowGroup {
        var cols: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
        try cols.ensureTotalCapacity(arena, src.columns.items.len);
        for (src.columns.items) |chunk| {
            var new_chunk = chunk;
            new_chunk.offset_index_offset = null;
            new_chunk.offset_index_length = null;
            new_chunk.column_index_offset = null;
            new_chunk.column_index_length = null;
            if (new_chunk.meta_data) |*m| {
                m.data_page_offset += delta;
                if (m.dictionary_page_offset) |d| m.dictionary_page_offset = d + delta;
                if (m.index_page_offset) |d| m.index_page_offset = d + delta;
            }
            if (new_chunk.meta_data) |m| new_chunk.file_offset = m.data_page_offset;
            try cols.append(arena, new_chunk);
        }
        return .{
            .columns = cols,
            .total_byte_size = src.total_byte_size,
            .num_rows = src.num_rows,
        };
    }
};

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
        var written: usize = 0;
        while (written < num_leaves) {
            const n = try reader.decodeWithLevels(values[written..], def_levels[written..]);
            if (n == 0) break;
            written += n;
        }
        if (written != num_leaves) return error.ShortDecode;
        return .{ .values = values, .def_levels = def_levels, .max_def = @intCast(levels.max_def) };
    }
    var written: usize = 0;
    while (written < num_leaves) {
        const n = try reader.decode(values[written..]);
        if (n == 0) break;
        written += n;
    }
    if (written != num_leaves) return error.ShortDecode;
    return .{ .values = values };
}

// ============================================================
// Tiny syscall-driven file I/O — same approach as cli/main.zig's
// readFile, mirrored here so query.zig is self-contained.
// ============================================================

fn readFile(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    const linux = std.os.linux;
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

fn createFile(path: []const u8) !std.os.linux.fd_t {
    const linux = std.os.linux;
    var path_z: [4096]u8 = undefined;
    if (path.len + 1 > path_z.len) return error.PathTooLong;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;

    const r = linux.openat(linux.AT.FDCWD, @ptrCast(&path_z[0]), .{
        .ACCMODE = .WRONLY,
        .CREAT = true,
        .TRUNC = true,
        .CLOEXEC = true,
    }, 0o644);
    const rs: isize = @bitCast(r);
    if (rs < 0) return error.OpenFailed;
    return @intCast(rs);
}

fn writeAll(fd: std.os.linux.fd_t, bytes: []const u8) !void {
    const linux = std.os.linux;
    var off: usize = 0;
    while (off < bytes.len) {
        const r = linux.write(fd, bytes[off..].ptr, bytes.len - off);
        const n: isize = @bitCast(r);
        if (n <= 0) return error.WriteFailed;
        off += @intCast(n);
    }
}

fn nowMonoNs() i64 {
    var ts: std.os.linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, ts.sec) * std.time.ns_per_s + @as(i64, ts.nsec);
}
