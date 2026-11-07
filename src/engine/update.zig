//! `UPDATE 'file.parquet' SET col = literal [, ...] WHERE predicate`: the matching rows' values set by rewriting only
//! the data pages that hold them (`page_edit`), everything else range-copied from the file (shared on a filesystem
//! that clones blocks). A rewritten page keeps the offsets after it congruent modulo `fastpath.share_block` through
//! a pad in its header, so every later byte stays shareable. Page indexes go after the row groups (every
//! ColumnIndex, then every OffsetIndex), then the Bloom filters, then the footer, rewritten field by field so that
//! nothing zpq does not model is lost (`thrift_patch`). In place, the result is published by an atomic rename over
//! the file; with an output path the file is left as it is.

const std = @import("std");
const schema = @import("../core/schema.zig");
const thrift = @import("../core/thrift.zig");
const metadata = @import("../core/parquet/metadata.zig");
const statistics = @import("../core/parquet/statistics.zig");
const decimal_mod = @import("../core/parquet/decimal.zig");
const bloom = @import("../core/parquet/bloom.zig");
const filter_ast = @import("../core/filter/ast.zig");
const filter_parser = @import("../core/filter/parser.zig");
const syntax = @import("../core/expr/syntax.zig");
const filter_eval = @import("../core/filter/eval.zig");
const filter_prune = @import("../core/filter/prune.zig");
const selection = @import("../core/filter/selection.zig");
const consumer_column = @import("../core/consumer/column.zig");
const page_edit = @import("../core/writer/page_edit.zig");
const patch = @import("../core/writer/thrift_patch.zig");
const streaming = @import("../core/writer/streaming.zig");
const local_fs = @import("../local_fs.zig");
const OutputPublication = @import("../output_publication.zig").OutputPublication;
const FdSink = @import("write.zig").FdSink;

/// One `SET column = value`: the assignment as a filter comparison (`column = literal`), which binds the literal to
/// the column's type exactly as a WHERE comparison would, or NULL.
pub const Set = struct {
    column: []const u8,
    /// Null: SET column = NULL.
    assignment: ?syntax.Predicate,
};

pub const Request = struct {
    target: []const u8,
    /// Null: in place.
    output: ?[]const u8 = null,
    where: syntax.Predicate,
    sets: []const Set,
    fsync: bool = true,
};

pub const Summary = struct {
    rows_in: u64 = 0,
    row_groups_pruned: u64 = 0,
    rows_matched: u64 = 0,
    /// Matched rows whose value differed from the one set, over all SET columns.
    values_changed: u64 = 0,
    pages_rewritten: u64 = 0,
    /// Bytes range-copied from the file, and of those the ones the kernel's range copy took.
    copied_bytes: u64 = 0,
    kernel_bytes: u64 = 0,
    /// Zero bytes in the rewritten pages' header pads.
    pad_bytes: u64 = 0,
    bytes_out: u64 = 0,
    /// Nothing changed, so nothing was written.
    unchanged: bool = true,
};

/// Why an UPDATE was refused, for the caller's message.
pub const Problem = struct {
    buf: [512]u8 = undefined,
    len: usize = 0,

    pub fn text(self: *const Problem) []const u8 {
        return self.buf[0..self.len];
    }

    fn set(self: *Problem, comptime fmt: []const u8, args: anytype) void {
        const written: []const u8 = std.fmt.bufPrint(&self.buf, fmt, args) catch self.buf[0..];
        self.len = written.len;
    }
};

/// Refusals: an UPDATE zpq will not run on this file. Exit 2.
pub const Refused = error{UpdateRefused};

const Assignment = struct {
    col: usize,
    value: page_edit.Value,
    name: []const u8,
};

/// A page replaced in the output: its bytes in the file and what goes there instead.
const Replacement = struct {
    src_off: u64,
    src_len: usize,
    bytes: []const u8,
};

/// What one chunk's edits change about it.
const ChunkEdit = struct {
    rg: usize,
    col: usize,
    assignment: *const Assignment,
    /// Per data page of the chunk (in order), its edit, or null.
    pages: []?page_edit.Edited,
    compressed_delta: i64 = 0,
    uncompressed_delta: i64 = 0,
    null_delta: i64 = 0,
    unencoded_delta: i64 = 0,
    def_hist_delta: []i64,
    replaced_min: bool = false,
    replaced_max: bool = false,
};

pub fn run(gpa: std.mem.Allocator, io: std.Io, req: Request, problem: *Problem) !Summary {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var summary: Summary = .{};

    if (std.mem.startsWith(u8, req.target, "s3://") or (req.output != null and std.mem.startsWith(u8, req.output.?, "s3://"))) {
        problem.set("UPDATE needs a local file; S3 objects cannot be renamed over", .{});
        return error.UpdateRefused;
    }
    const map = try local_fs.mapFile(req.target);
    defer map.unmap();
    const file: []const u8 = map.bytes;
    var meta = metadata.open(a, file) catch |err| {
        if (err == error.EncryptedFile) {
            problem.set("UPDATE of an encrypted file is not supported", .{});
            return error.UpdateRefused;
        }
        return err;
    };

    const filter = try filter_parser.bindSyntax(a, req.where, &meta, null);
    const assignments = try bindSets(a, &meta, req.sets, problem);

    // The rows each row group's WHERE matches, ascending.
    const n_rg = meta.row_groups.items.len;
    const matches = try a.alloc([]const u32, n_rg);
    const n_leaves = metadata.leafCount(&meta);
    const src: @import("../core/consumer/types.zig").RGSrc = .{ .bytes = file, .byte_origin = 0, .fd = map.fd, .file_offsets = true };
    for (meta.row_groups.items, matches) |*rg, *m| {
        m.* = &.{};
        const rows: usize = @intCast(rg.num_rows);
        summary.rows_in += rows;
        const decision = filter_prune.pruneRowGroup(rg, filter, &meta);
        if (decision == .skip) {
            summary.row_groups_pruned += 1;
            continue;
        }
        var rg_arena = std.heap.ArenaAllocator.init(gpa);
        defer rg_arena.deinit();
        const ra = rg_arena.allocator();
        var sel = try selection.SelectionVector.init(ra, rows);
        if (decision != .always_match) {
            var cols: std.ArrayList(usize) = .empty;
            try filter.collectColumns(&cols, ra);
            const lookup = try ra.alloc(?usize, n_leaves);
            @memset(lookup, null);
            var batch_cols: std.ArrayList(filter_eval.Batch.Column) = .empty;
            for (cols.items) |ci| {
                if (lookup[ci] != null) continue;
                lookup[ci] = batch_cols.items.len;
                try batch_cols.append(ra, try consumer_column.decodeWriteColumn(ra, rg, &meta, src, ci, .{}, null));
            }
            const batch: filter_eval.Batch = .{ .cols = batch_cols.items, .num_rows = rows };
            try filter_eval.evaluate(filter, &batch, &sel, lookup, ra);
        }
        var hit: std.ArrayList(u32) = .empty;
        var it = sel.iterator();
        while (it.next()) |r| try hit.append(a, @intCast(r));
        m.* = hit.items;
        summary.rows_matched += hit.items.len;
    }

    // Each matched chunk's pages, edited.
    var replacements: std.ArrayList(Replacement) = .empty;
    var edits: std.ArrayList(ChunkEdit) = .empty;
    for (matches, 0..) |rows, rgi| {
        if (rows.len == 0) continue;
        const rg = &meta.row_groups.items[rgi];
        for (assignments) |*as| {
            if (try editChunk(a, file, &meta, rg, rgi, as, rows, &replacements, &summary, problem)) |e|
                try edits.append(a, e);
        }
    }
    if (edits.items.len == 0) return summary;
    summary.unchanged = false;

    std.mem.sort(Replacement, replacements.items, {}, struct {
        fn lt(_: void, x: Replacement, y: Replacement) bool {
            return x.src_off < y.src_off;
        }
    }.lt);
    const layout = try Layout.init(a, replacements.items);

    // The data region: every byte up to the last chunk's end, as it lies, but the pages replaced.
    var data_end: u64 = 0;
    for (meta.row_groups.items) |rg| for (rg.columns.items) |c| {
        const cm = c.meta_data orelse continue;
        const start: u64 = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
        data_end = @max(data_end, start + @as(u64, @intCast(cm.total_compressed_size)));
    };

    var publication: OutputPublication = .{ .fsync = req.fsync };
    defer publication.abort();
    const in_place = if (req.output) |o| local_fs.sameFile(o, req.target) catch false else true;
    _ = try publication.prepare(io, if (in_place) req.target else req.output.?, &.{});
    const out_fd = publication.fd.?;
    const share = if (map.fd) |in_fd| try local_fs.sharesBlocks(in_fd, out_fd) else false;
    var fd_sink: FdSink = .{ .fd = out_fd, .share = share };
    var out: streaming.SinkOut = .{ .sink = fd_sink.sink() };

    var pos: u64 = 0;
    for (layout.reps) |rep| {
        summary.kernel_bytes += try out.copyRange(map.fd, pos, file[@intCast(pos)..@intCast(rep.src_off)]);
        summary.copied_bytes += rep.src_off - pos;
        try out.write(rep.bytes);
        pos = rep.src_off + rep.src_len;
    }
    summary.kernel_bytes += try out.copyRange(map.fd, pos, file[@intCast(pos)..@intCast(data_end)]);
    summary.copied_bytes += data_end - pos;

    const placed = try placeIndexesAndFilters(a, &out, file, &meta, edits.items, layout);
    const footer = try patchFooter(a, file, &meta, edits.items, layout, placed);
    try out.write(footer);
    var tail: [8]u8 = undefined;
    std.mem.writeInt(u32, tail[0..4], std.math.cast(u32, footer.len) orelse return error.FooterTooLarge, .little);
    @memcpy(tail[4..8], "PAR1");
    try out.write(&tail);
    summary.bytes_out = out.pos();

    // The file must be what was read: a write under it since would be lost by the rename.
    try map.intact(req.target);
    try publication.commit(io);
    return summary;
}

fn bindSets(a: std.mem.Allocator, meta: *const schema.FileMetaData, sets: []const Set, problem: *Problem) ![]Assignment {
    const out = try a.alloc(Assignment, sets.len);
    for (sets, out, 0..) |s, *as, i| {
        const col = filter_parser.resolveCol(meta, s.column, null) catch |err| {
            problem.set("SET {s}: no such column", .{s.column});
            return err;
        };
        for (out[0..i]) |prev| if (prev.col == col) {
            problem.set("SET {s}: the column is set twice", .{s.column});
            return error.UpdateRefused;
        };
        const elem = metadata.leafSchemaElement(meta, col) orelse return error.BadMetadata;
        const path = (try metadata.leafPathSegments(a, meta, col)) orelse return error.BadMetadata;
        const levels = meta.getColumnLevels(path);
        if (levels.max_rep > 0) {
            problem.set("SET {s}: UPDATE of a repeated (LIST/MAP) column is not supported", .{s.column});
            return error.UpdateRefused;
        }
        const physical = elem.type orelse return error.BadMetadata;
        if (physical == .INT96 or schema.isFloat16(elem)) {
            problem.set("SET {s}: UPDATE of an {s} column is not supported", .{ s.column, if (physical == .INT96) "INT96" else "FLOAT16" });
            return error.UpdateRefused;
        }
        as.* = .{ .col = col, .value = .null, .name = s.column };
        const assignment = s.assignment orelse {
            if (levels.max_def == 0) {
                problem.set("SET {s} = NULL: the column is REQUIRED", .{s.column});
                return error.UpdateRefused;
            }
            continue;
        };
        const bound = filter_parser.bindSyntax(a, assignment, meta, null) catch {
            problem.set("SET {s}: the value is not one the column's type holds", .{s.column});
            return error.UpdateRefused;
        };
        as.value = (try valueOf(a, bound, col, elem)) orelse {
            problem.set("SET {s}: the value is not one the column's type holds exactly", .{s.column});
            return error.UpdateRefused;
        };
    }
    return out;
}

/// The physical value an `col = literal` comparison bound to, when it is exactly one the column stores.
fn valueOf(a: std.mem.Allocator, f: filter_ast.Filter, col: usize, elem: schema.SchemaElement) !?page_edit.Value {
    const physical = elem.type.?;
    switch (f) {
        inline .int32, .int64, .uint64, .float, .double, .string, .boolean => |leaf, tag| {
            if (leaf.col_idx != col or leaf.op != .Eq or !leaf.stored) return null;
            return switch (tag) {
                .int32 => if (physical == .INT32) .{ .i32 = leaf.value } else null,
                .int64 => if (physical == .INT64) .{ .i64 = leaf.value } else null,
                .uint64 => switch (physical) {
                    .INT32 => if (std.math.cast(u32, leaf.value)) |v| .{ .i32 = @bitCast(v) } else null,
                    .INT64 => .{ .i64 = @bitCast(leaf.value) },
                    else => null,
                },
                .float => if (physical == .FLOAT) .{ .f32 = leaf.value } else null,
                .double => if (physical == .DOUBLE) .{ .f64 = leaf.value } else null,
                .string => blk: {
                    if (physical == .FIXED_LEN_BYTE_ARRAY) {
                        const w: usize = @intCast(elem.type_length orelse break :blk null);
                        if (leaf.value.len != w) break :blk null;
                    } else if (physical != .BYTE_ARRAY) break :blk null;
                    break :blk .{ .bytes = leaf.value };
                },
                .boolean => if (physical == .BOOLEAN) .{ .boolean = leaf.value } else null,
                else => unreachable,
            };
        },
        .decimal => |leaf| {
            if (leaf.col_idx != col or leaf.op != .Eq) return null;
            const kind = decimal_mod.kindFromSchema(&elem) orelse return null;
            const bound = std.math.powi(i128, 10, @intCast(kind.precision)) catch return null;
            if (leaf.value >= bound or leaf.value <= -bound) return null;
            return switch (kind.physical) {
                .INT32 => if (std.math.cast(i32, leaf.value)) |v| .{ .i32 = v } else null,
                .INT64 => if (std.math.cast(i64, leaf.value)) |v| .{ .i64 = v } else null,
                .FIXED_LEN_BYTE_ARRAY, .BYTE_ARRAY => .{ .bytes = try a.dupe(u8, decimalBytes(leaf.value, if (kind.physical == .FIXED_LEN_BYTE_ARRAY) kind.byte_width else 0) orelse return null) },
                else => null,
            };
        },
        else => return null,
    }
}

/// Big-endian two's complement of `v` in `width` bytes, or the fewest that hold it when `width` is 0. Borrowed from a
/// buffer the next call overwrites.
fn decimalBytes(v: i128, width: u8) ?[]const u8 {
    const S = struct {
        threadlocal var buf: [16]u8 = undefined;
    };
    const decimal_buf = &S.buf;
    std.mem.writeInt(i128, decimal_buf, v, .big);
    if (width != 0) {
        if (width > 16) return null;
        const cut = 16 - width;
        const sign: u8 = if (v < 0) 0xff else 0;
        for (decimal_buf[0..cut]) |b| if (b != sign) return null;
        if (width < 16 and (decimal_buf[cut] & 0x80 != 0) != (v < 0)) return null;
        return decimal_buf[cut..];
    }
    var cut: usize = 0;
    while (cut < 15) : (cut += 1) {
        const sign: u8 = if (v < 0) 0xff else 0;
        if (decimal_buf[cut] != sign or ((decimal_buf[cut + 1] & 0x80 != 0) != (v < 0))) break;
    }
    return decimal_buf[cut..];
}

/// The edits `rows` of row group `rgi` need in `as`'s column: replacements appended, null when no value changes.
fn editChunk(
    a: std.mem.Allocator,
    file: []const u8,
    meta: *const schema.FileMetaData,
    rg: *const schema.RowGroup,
    rgi: usize,
    as: *const Assignment,
    rows: []const u32,
    replacements: *std.ArrayList(Replacement),
    summary: *Summary,
    problem: *Problem,
) !?ChunkEdit {
    const c = &rg.columns.items[as.col];
    const cm = c.meta_data orelse return error.ColumnMetaMissing;
    const elem = meta.getColumnSchema(cm.path_in_schema.items) orelse return error.BadMetadata;
    const start: u64 = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
    const pages = page_edit.walkPages(a, file, start, @intCast(cm.total_compressed_size)) catch |err| {
        if (err == error.UnsupportedPage) problem.set("SET {s}: a page of row group {d} cannot be read", .{ as.name, rgi });
        return if (err == error.UnsupportedPage) error.UpdateRefused else err;
    };
    const elem_ptr = try a.create(schema.SchemaElement);
    elem_ptr.* = elem;
    const chunk: page_edit.Chunk = .{
        .elem = elem_ptr,
        .levels = meta.getColumnLevels(cm.path_in_schema.items),
        .codec = cm.codec,
        .type_length = if (elem.type_length) |t| @intCast(t) else 0,
    };
    var dict: page_edit.Dictionary = .{ .page = if (pages.len > 0 and pages[0].header.type == .DICTIONARY_PAGE) pages[0] else null };
    const data_pages = if (dict.page != null) pages[1..] else pages;
    const bounds: page_edit.Bounds = if (cm.statistics) |st| .{ .min = st.min_value, .max = st.max_value } else .{};
    const max_def: usize = @intCast(chunk.levels.max_def);
    var e: ChunkEdit = .{
        .rg = rgi,
        .col = as.col,
        .assignment = as,
        .pages = try a.alloc(?page_edit.Edited, data_pages.len),
        .def_hist_delta = try a.alloc(i64, max_def + 1),
    };
    @memset(e.pages, null);
    @memset(e.def_hist_delta, 0);
    var at: usize = 0;
    var any = false;
    for (data_pages, e.pages) |p, *slot| {
        const lo = at;
        while (at < rows.len and rows[at] < p.first_row + p.rows) at += 1;
        if (at == lo) continue;
        const local = try a.alloc(u32, at - lo);
        for (local, rows[lo..at]) |*l, r| l.* = @intCast(r - p.first_row);
        const edited = page_edit.editDataPage(a, chunk, &dict, file, p, local, as.value, bounds) catch |err| switch (err) {
            error.UnsupportedPage, error.PadFieldUnreachable => {
                problem.set("SET {s}: a page of row group {d} ({s}) cannot be rewritten in place", .{
                    as.name, rgi, if (p.encoding()) |enc| @tagName(enc) else "?",
                });
                return error.UpdateRefused;
            },
            else => return err,
        } orelse continue;
        any = true;
        slot.* = edited;
        try replace(a, replacements, &e, p, edited.built, summary);
        summary.values_changed += edited.changed;
        e.null_delta += @as(i64, @intCast(edited.nulls_after)) - @as(i64, @intCast(edited.nulls_before));
        e.unencoded_delta += edited.unencoded_after - edited.unencoded_before;
        for (e.def_hist_delta, edited.def_hist_before, edited.def_hist_after) |*d, b, af| d.* += @as(i64, @intCast(af)) - @as(i64, @intCast(b));
        e.replaced_min = e.replaced_min or edited.replaced_min;
        e.replaced_max = e.replaced_max or edited.replaced_max;
    }
    if (!any) return null;
    if (try dict.build(a, chunk)) |built| try replace(a, replacements, &e, dict.page.?, built, summary);
    return e;
}

fn replace(
    a: std.mem.Allocator,
    replacements: *std.ArrayList(Replacement),
    e: *ChunkEdit,
    p: page_edit.SourcePage,
    built: page_edit.Built,
    summary: *Summary,
) !void {
    const bytes = try page_edit.finish(a, built, p.total_len);
    try replacements.append(a, .{ .src_off = p.offset, .src_len = p.total_len, .bytes = bytes });
    const header_len = bytes.len - built.body.len;
    summary.pages_rewritten += 1;
    summary.pad_bytes += bytes.len - (built.head.len + 1 + built.body.len);
    e.compressed_delta += @as(i64, @intCast(bytes.len)) - @as(i64, @intCast(p.total_len));
    e.uncompressed_delta += @as(i64, @intCast(header_len + built.uncompressed)) -
        @as(i64, @intCast(p.header_len + @as(usize, @intCast(p.header.uncompressed_page_size))));
}

/// Where the source's bytes land in the output: each moves by the size changes of the replaced pages before it.
const Layout = struct {
    /// Sorted by source offset.
    reps: []const Replacement,
    /// `after[i]`: the size change of replacements 0..i together.
    after: []const i64,

    fn init(a: std.mem.Allocator, reps: []const Replacement) !Layout {
        const after = try a.alloc(i64, reps.len);
        var d: i64 = 0;
        for (reps, after) |r, *x| {
            d += @as(i64, @intCast(r.bytes.len)) - @as(i64, @intCast(r.src_len));
            x.* = d;
        }
        return .{ .reps = reps, .after = after };
    }

    /// Replacements starting before `off`.
    fn before(self: Layout, off: i64) usize {
        var lo: usize = 0;
        var hi: usize = self.reps.len;
        while (lo < hi) {
            const mid = (lo + hi) / 2;
            if (@as(i64, @intCast(self.reps[mid].src_off)) < off) lo = mid + 1 else hi = mid;
        }
        return lo;
    }

    fn moved(self: Layout, off: i64) i64 {
        const n = self.before(off);
        return off + if (n == 0) 0 else self.after[n - 1];
    }

    fn replacementAt(self: Layout, off: u64) ?Replacement {
        if (off > std.math.maxInt(i64)) return null;
        const n = self.before(@intCast(off));
        if (n < self.reps.len and self.reps[n].src_off == off) return self.reps[n];
        return null;
    }
};

const Span = struct { offset: i64, length: i32 };

/// Each chunk's page index and Bloom filter as placed in the output, by row group and column.
const Placed = struct {
    column_index: [][]?Span,
    offset_index: [][]?Span,
    bloom: [][]?Span,
};

fn editOf(edits: []const ChunkEdit, rg: usize, col: usize) ?*const ChunkEdit {
    for (edits) |*e| if (e.rg == rg and e.col == col) return e;
    return null;
}

fn sourceBytes(file: []const u8, off: ?i64, len: ?i32) ?[]const u8 {
    const o = off orelse return null;
    const l = len orelse return null;
    if (o < 0 or l <= 0) return null;
    const start: usize = @intCast(o);
    const n: usize = @intCast(l);
    if (start > file.len or n > file.len - start) return null;
    return file[start..][0..n];
}

/// Every ColumnIndex (by column, then row group), then every OffsetIndex, then every Bloom filter, after the data.
fn placeIndexesAndFilters(
    a: std.mem.Allocator,
    out: *streaming.SinkOut,
    file: []const u8,
    meta: *const schema.FileMetaData,
    edits: []const ChunkEdit,
    layout: Layout,
) !Placed {
    const rgs = meta.row_groups.items;
    var placed: Placed = .{
        .column_index = try a.alloc([]?Span, rgs.len),
        .offset_index = try a.alloc([]?Span, rgs.len),
        .bloom = try a.alloc([]?Span, rgs.len),
    };
    var cols: usize = 0;
    for (rgs, 0..) |rg, i| {
        cols = @max(cols, rg.columns.items.len);
        inline for (.{ "column_index", "offset_index", "bloom" }) |f| {
            @field(placed, f)[i] = try a.alloc(?Span, rg.columns.items.len);
            @memset(@field(placed, f)[i], null);
        }
    }
    for (0..cols) |ci| for (rgs, 0..) |rg, ri| {
        if (ci >= rg.columns.items.len) continue;
        const c = rg.columns.items[ci];
        const raw = sourceBytes(file, c.column_index_offset, c.column_index_length) orelse continue;
        const bytes = if (editOf(edits, ri, ci)) |e| try patchColumnIndex(a, raw, meta, &rg, e) else raw;
        placed.column_index[ri][ci] = .{ .offset = @intCast(out.pos()), .length = @intCast(bytes.len) };
        try out.write(bytes);
    };
    for (0..cols) |ci| for (rgs, 0..) |rg, ri| {
        if (ci >= rg.columns.items.len) continue;
        const c = rg.columns.items[ci];
        const raw = sourceBytes(file, c.offset_index_offset, c.offset_index_length) orelse continue;
        const bytes = try patchOffsetIndex(a, raw, editOf(edits, ri, ci), layout);
        placed.offset_index[ri][ci] = .{ .offset = @intCast(out.pos()), .length = @intCast(bytes.len) };
        try out.write(bytes);
    };
    for (0..cols) |ci| for (rgs, 0..) |rg, ri| {
        if (ci >= rg.columns.items.len) continue;
        const cm = rg.columns.items[ci].meta_data orelse continue;
        const raw = bloomBytes(file, cm) orelse continue;
        const bytes = if (editOf(edits, ri, ci)) |e| try insertIntoBloom(a, raw, e.assignment.value) else raw;
        placed.bloom[ri][ci] = .{ .offset = @intCast(out.pos()), .length = @intCast(bytes.len) };
        try out.write(bytes);
    };
    return placed;
}

/// A chunk's Bloom filter, header and bitset, when its offset names one zpq can read.
fn bloomBytes(file: []const u8, cm: schema.ColumnMetaData) ?[]const u8 {
    const off = cm.bloom_filter_offset orelse return null;
    if (off < 0 or off >= file.len) return null;
    const at = file[@intCast(off)..];
    const h = bloom.parseHeader(at) orelse return null;
    const total = h.len + h.num_bytes;
    if (total > at.len) return null;
    if (cm.bloom_filter_length) |l| if (l != total) return null;
    return at[0..total];
}

/// `raw` with the value's hash inserted: the filter keeps answering "maybe" for every value it held, the old one
/// included, which a false positive allows.
fn insertIntoBloom(a: std.mem.Allocator, raw: []const u8, value: page_edit.Value) ![]const u8 {
    if (value == .null) return raw;
    const h = bloom.parseHeader(raw).?;
    const out = try a.dupe(u8, raw);
    const bits = out[h.len..];
    var buf: [8]u8 = undefined;
    const hash = bloom.hashBytes(value.plain(&buf));
    const blocks = bits.len / bloom.block_bytes;
    const i: usize = @intCast(bloom.blockIndex(blocks, hash));
    const blk = bits[i * bloom.block_bytes ..][0..bloom.block_bytes];
    const x: u32 = @truncate(hash);
    inline for (bloom.salt, 0..) |s, k| {
        const word = std.mem.readInt(u32, blk[k * 4 ..][0..4], .little);
        const bit: u5 = @intCast((x *% s) >> 27);
        std.mem.writeInt(u32, blk[k * 4 ..][0..4], word | (@as(u32, 1) << bit), .little);
    }
    return out;
}

/// The chunk's OffsetIndex with its locations moved, and an edited page's size (and byte-array total) its new one.
fn patchOffsetIndex(a: std.mem.Allocator, raw: []const u8, edit: ?*const ChunkEdit, layout: Layout) ![]const u8 {
    const fields = try patch.fieldsOf(a, raw);
    const locs_field = patch.find(fields, 1) orelse return raw;
    const l = try patch.list(locs_field.raw);
    var r = thrift.Reader.init(l.body);
    var changed = false;
    const encoded = try a.alloc([]const u8, l.len);
    for (encoded) |*enc| {
        const start = r.pos;
        var loc = try schema.PageLocation.read(&r);
        const end = r.pos;
        var moved = loc;
        moved.offset = layout.moved(loc.offset);
        if (layout.replacementAt(@intCast(loc.offset))) |rep| moved.compressed_page_size = @intCast(rep.bytes.len);
        if (std.meta.eql(moved, loc)) {
            enc.* = l.body[start..end];
            continue;
        }
        changed = true;
        loc = moved;
        var w = thrift.Writer.init(a);
        try loc.write(&w);
        enc.* = w.bytes();
    }
    var b = patch.Builder.init(a);
    try b.keep(fields);
    if (changed) try b.putStructList(1, encoded);
    if (edit) |e| if (e.unencoded_delta != 0) if (patch.find(fields, 2)) |f| {
        if (try patchI64List(a, f.raw, e, struct {
            fn per(ed: *const page_edit.Edited) i64 {
                return ed.unencoded_after - ed.unencoded_before;
            }
        }.per)) |list| {
            try b.putI64List(2, list);
            changed = true;
        } else b.drop(2);
    };
    if (!changed) return raw;
    return b.encode();
}

/// A per-page list<i64> with each edited page's entry moved by `delta(page)`; null when it is not one per page.
fn patchI64List(a: std.mem.Allocator, raw: []const u8, e: *const ChunkEdit, comptime delta: fn (*const page_edit.Edited) i64) !?[]i64 {
    const values = try readI64s(a, raw);
    if (values.len != e.pages.len) return null;
    for (values, e.pages) |*v, p| if (p) |*ed| {
        v.* += delta(ed);
    };
    return values;
}

fn readI64s(a: std.mem.Allocator, raw: []const u8) ![]i64 {
    const l = try patch.list(raw);
    var r = thrift.Reader.init(l.body);
    const out = try a.alloc(i64, l.len);
    for (out) |*v| v.* = try r.readZigZag(i64);
    return out;
}

/// An edited chunk's ColumnIndex: its edited pages' entries from their new statistics, the boundary order kept only
/// where it still holds, and the definition-level histograms moved by the edit.
fn patchColumnIndex(a: std.mem.Allocator, raw: []const u8, meta: *const schema.FileMetaData, rg: *const schema.RowGroup, e: *const ChunkEdit) ![]const u8 {
    var r = thrift.Reader.init(raw);
    var ci = try schema.ColumnIndex.read(a, &r);
    const n = e.pages.len;
    if (ci.null_pages.items.len != n or ci.min_values.items.len != n or ci.max_values.items.len != n) return raw;
    for (e.pages, 0..) |p, k| if (p) |ed| {
        const all_null = ed.nulls_after == ed.rows;
        ci.null_pages.items[k] = all_null;
        ci.min_values.items[k] = if (all_null) "" else ed.stats.min_value orelse "";
        ci.max_values.items[k] = if (all_null) "" else ed.stats.max_value orelse "";
        if (ci.null_counts) |*nc| if (nc.items.len == n) {
            nc.items[k] = @intCast(ed.nulls_after);
        };
        if (ci.nan_counts) |*nc| if (nc.items.len == n) {
            nc.items[k] = ed.stats.nan_count orelse 0;
        };
    };
    const cm = rg.columns.items[e.col].meta_data.?;
    const elem = meta.getColumnSchema(cm.path_in_schema.items);
    if (ci.boundary_order != .UNORDERED and !stillOrdered(&ci, cm.type, elem, ci.boundary_order == .ASCENDING))
        ci.boundary_order = .UNORDERED;
    var w = thrift.Writer.init(a);
    try ci.write(&w);
    // Fields the model leaves out (the level histograms, 6 and 7) come back from the source, the definition levels'
    // moved by each edited page's change.
    const src_fields = try patch.fieldsOf(a, raw);
    var b = patch.Builder.init(a);
    try b.keep(try patch.fieldsOf(a, w.bytes()));
    for (src_fields) |f| if (f.id != 1 and f.id != 2 and f.id != 3 and f.id != 4 and f.id != 5 and f.id != 8) try b.put(f);
    if (patch.find(src_fields, 7)) |f| {
        const hist = try readI64s(a, f.raw);
        const width = e.def_hist_delta.len;
        if (hist.len == n * width) {
            for (e.pages, 0..) |p, k| if (p) |ed| for (0..width) |l| {
                hist[k * width + l] += @as(i64, @intCast(ed.def_hist_after[l])) - @as(i64, @intCast(ed.def_hist_before[l]));
            };
            try b.putI64List(7, hist);
        } else b.drop(7);
    }
    return b.encode();
}

/// Whether the non-null pages' bounds still run in the declared direction.
fn stillOrdered(ci: *const schema.ColumnIndex, physical: schema.Type, elem: ?schema.SchemaElement, ascending: bool) bool {
    var prev: ?usize = null;
    for (ci.null_pages.items, 0..) |is_null, k| {
        if (is_null) continue;
        if (prev) |p| {
            const lo = if (ascending) p else k;
            const hi = if (ascending) k else p;
            if (statistics.boundsInverted(physical, elem, ci.min_values.items[lo], ci.min_values.items[hi])) return false;
            if (statistics.boundsInverted(physical, elem, ci.max_values.items[lo], ci.max_values.items[hi])) return false;
        }
        prev = k;
    }
    return true;
}

/// The footer, every field as the source wrote it but the offsets the edits moved and what they changed.
fn patchFooter(
    a: std.mem.Allocator,
    file: []const u8,
    meta: *const schema.FileMetaData,
    edits: []const ChunkEdit,
    layout: Layout,
    placed: Placed,
) ![]const u8 {
    const footer_len = std.mem.readInt(u32, file[file.len - 8 ..][0..4], .little);
    const footer = file[file.len - 8 - footer_len .. file.len - 8];
    const fields = try patch.fieldsOf(a, footer);
    var b = patch.Builder.init(a);
    try b.keep(fields);
    const rg_field = patch.find(fields, 4) orelse return error.BadMetadata;
    const rg_elems = try patch.structElements(a, rg_field.raw);
    if (rg_elems.len != meta.row_groups.items.len) return error.BadMetadata;
    const new_rgs = try a.alloc([]const u8, rg_elems.len);
    for (rg_elems, new_rgs, 0..) |raw, *new, ri| new.* = try patchRowGroup(a, raw, meta, ri, edits, layout, placed);
    try b.putStructList(4, new_rgs);
    return b.encode();
}

fn patchRowGroup(
    a: std.mem.Allocator,
    raw: []const u8,
    meta: *const schema.FileMetaData,
    ri: usize,
    edits: []const ChunkEdit,
    layout: Layout,
    placed: Placed,
) ![]const u8 {
    const rg = &meta.row_groups.items[ri];
    const fields = try patch.fieldsOf(a, raw);
    var b = patch.Builder.init(a);
    try b.keep(fields);
    const cols_field = patch.find(fields, 1) orelse return error.BadMetadata;
    const col_elems = try patch.structElements(a, cols_field.raw);
    if (col_elems.len != rg.columns.items.len) return error.BadMetadata;
    const new_cols = try a.alloc([]const u8, col_elems.len);
    var compressed: i64 = 0;
    var uncompressed: i64 = 0;
    var edited_cols: std.ArrayList(usize) = .empty;
    for (col_elems, new_cols, 0..) |c, *new, ci| {
        const e = editOf(edits, ri, ci);
        if (e) |ed| {
            compressed += ed.compressed_delta;
            uncompressed += ed.uncompressed_delta;
            try edited_cols.append(a, ci);
        }
        new.* = try patchColumnChunk(a, c, meta, ri, ci, e, layout, placed);
    }
    try b.putStructList(1, new_cols);
    if (patch.find(fields, 2)) |f| try b.putInt(2, i64, (try patch.int(i64, f)) + uncompressed);
    if (patch.find(fields, 5)) |f| try b.putInt(5, i64, layout.moved(try patch.int(i64, f)));
    if (patch.find(fields, 6)) |f| try b.putInt(6, i64, (try patch.int(i64, f)) + compressed);
    // A sort order over an edited column no longer holds, as far as this writer can tell.
    if (patch.find(fields, 4)) |f| {
        for (try patch.structElements(a, f.raw)) |sc| {
            const sf = try patch.fieldsOf(a, sc);
            const idx = patch.find(sf, 1) orelse continue;
            const col = try patch.int(i32, idx);
            if (std.mem.indexOfScalar(usize, edited_cols.items, @intCast(@max(col, 0))) != null) {
                b.drop(4);
                break;
            }
        }
    }
    return b.encode();
}

fn patchColumnChunk(
    a: std.mem.Allocator,
    raw: []const u8,
    meta: *const schema.FileMetaData,
    ri: usize,
    ci: usize,
    edit: ?*const ChunkEdit,
    layout: Layout,
    placed: Placed,
) ![]const u8 {
    const rg = &meta.row_groups.items[ri];
    const fields = try patch.fieldsOf(a, raw);
    var b = patch.Builder.init(a);
    try b.keep(fields);
    if (patch.find(fields, 2)) |f| try b.putInt(2, i64, layout.moved(try patch.int(i64, f)));
    inline for (.{ .{ "offset_index", 4, 5 }, .{ "column_index", 6, 7 } }) |spec| {
        if (@field(placed, spec[0])[ri][ci]) |s| {
            try b.putInt(spec[1], i64, s.offset);
            try b.putInt(spec[2], i32, s.length);
        } else {
            b.drop(spec[1]);
            b.drop(spec[2]);
        }
    }
    if (patch.find(fields, 3)) |f| try b.putStruct(3, try patchColumnMeta(a, f.raw, meta, rg, ci, edit, layout, placed.bloom[ri][ci]));
    return b.encode();
}

fn patchColumnMeta(
    a: std.mem.Allocator,
    raw: []const u8,
    meta: *const schema.FileMetaData,
    rg: *const schema.RowGroup,
    ci: usize,
    edit: ?*const ChunkEdit,
    layout: Layout,
    filter: ?Span,
) ![]const u8 {
    const fields = try patch.fieldsOf(a, raw);
    var b = patch.Builder.init(a);
    try b.keep(fields);
    inline for (.{ 9, 10, 11 }) |id| {
        if (patch.find(fields, id)) |f| try b.putInt(id, i64, layout.moved(try patch.int(i64, f)));
    }
    if (filter) |s| {
        try b.putInt(14, i64, s.offset);
        try b.putInt(15, i32, s.length);
    } else {
        b.drop(14);
        b.drop(15);
    }
    const e = edit orelse return b.encode();
    if (patch.find(fields, 6)) |f| try b.putInt(6, i64, (try patch.int(i64, f)) + e.uncompressed_delta);
    if (patch.find(fields, 7)) |f| try b.putInt(7, i64, (try patch.int(i64, f)) + e.compressed_delta);
    const cm = rg.columns.items[ci].meta_data.?;
    if (cm.statistics) |st| {
        const elem = meta.getColumnSchema(cm.path_in_schema.items);
        var w = thrift.Writer.init(a);
        const new = chunkStats(st, cm.type, elem, e);
        try new.write(&w);
        try b.putStruct(12, w.bytes());
    }
    if (patch.find(fields, 16)) |f| {
        if (try patchSizeStatistics(a, f.raw, e)) |s| try b.putStruct(16, s) else b.drop(16);
    }
    return b.encode();
}

/// The chunk's statistics after its edits: the null count moved, the bounds widened to every edited page's, exactness
/// cleared where a bound's value may have been edited away. The deprecated pair and the distinct count are dropped, as
/// is a float's NaN count, which this writer does not recount.
fn chunkStats(st: schema.Statistics, physical: schema.Type, elem: ?schema.SchemaElement, e: *const ChunkEdit) schema.Statistics {
    var out = st;
    out.min = null;
    out.max = null;
    out.distinct_count = null;
    if (physical == .FLOAT or physical == .DOUBLE) out.nan_count = null;
    if (out.null_count) |nc| out.null_count = nc + e.null_delta;
    var min_moved = false;
    var max_moved = false;
    if (out.min_value != null and out.max_value != null) {
        for (e.pages) |p| if (p) |ed| {
            if (ed.stats.min_value) |m| if (statistics.boundsInverted(physical, elem, out.min_value.?, m)) {
                out.min_value = m;
                out.is_min_value_exact = ed.stats.is_min_value_exact;
                min_moved = true;
            };
            if (ed.stats.max_value) |m| if (statistics.boundsInverted(physical, elem, m, out.max_value.?)) {
                out.max_value = m;
                out.is_max_value_exact = ed.stats.is_max_value_exact;
                max_moved = true;
            };
        };
    }
    if (e.replaced_min and !min_moved and out.min_value != null) out.is_min_value_exact = false;
    if (e.replaced_max and !max_moved and out.max_value != null) out.is_max_value_exact = false;
    return out;
}

/// SizeStatistics moved by the edit: the byte-array total and the definition-level histogram. Null when its shape is
/// not one this writer can move exactly; the field is then dropped.
fn patchSizeStatistics(a: std.mem.Allocator, raw: []const u8, e: *const ChunkEdit) !?[]const u8 {
    const fields = try patch.fieldsOf(a, raw);
    var b = patch.Builder.init(a);
    try b.keep(fields);
    if (patch.find(fields, 1)) |f| try b.putInt(1, i64, (try patch.int(i64, f)) + e.unencoded_delta);
    if (patch.find(fields, 3)) |f| {
        const hist = try readI64s(a, f.raw);
        if (hist.len != e.def_hist_delta.len) return null;
        for (hist, e.def_hist_delta) |*h, d| h.* += d;
        try b.putI64List(3, hist);
    }
    return try b.encode();
}

test "UPDATE rewrites only the pages it changes, keeps every other byte at its offset modulo the block, and reads back" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    const fixture = "ci/fixtures/parquet/filtered_records_ordered.parquet";
    std.Io.Dir.cwd().access(io, fixture, .{}) catch return error.SkipZigTest;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const src_bytes = try local_fs.readFile(a, fixture);
    const target = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/t.parquet", .{tmp.sub_path});
    const again = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/again.parquet", .{tmp.sub_path});
    const fd = try local_fs.createFile(target);
    try local_fs.writeAll(fd, src_bytes);
    local_fs.close(fd);

    // One value in place, and ten NULLs, whose levels change the page's size: its pad keeps the rest in place.
    const sets = [_]Set{
        .{ .column = "f", .assignment = try filter_parser.parseSyntax(a, "f = 42") },
        .{ .column = "k", .assignment = null },
    };
    const req: Request = .{ .target = target, .where = try filter_parser.parseSyntax(a, "rid >= 5000 AND rid < 5010"), .sets = &sets };
    var problem: Problem = .{};
    const s = try run(gpa, io, req, &problem);
    try std.testing.expectEqual(@as(u64, 10), s.rows_matched);
    try std.testing.expect(s.pages_rewritten >= 2 and s.pad_bytes > 0);
    const got = try local_fs.readFile(a, target);
    // The same request from the same bytes writes the same bytes.
    const fd2 = try local_fs.createFile(again);
    try local_fs.writeAll(fd2, src_bytes);
    local_fs.close(fd2);
    var req2 = req;
    req2.target = again;
    _ = try run(gpa, io, req2, &problem);
    try std.testing.expectEqualSlices(u8, got, try local_fs.readFile(a, again));

    const before = try metadata.open(a, src_bytes);
    const after = try metadata.open(a, got);
    var edited_pages: usize = 0;
    for (before.row_groups.items, after.row_groups.items) |rb, ra| for (rb.columns.items, ra.columns.items) |cb, ca| {
        const mb = cb.meta_data.?;
        const ma = ca.meta_data.?;
        const sb: u64 = @intCast(mb.dictionary_page_offset orelse mb.data_page_offset);
        const sa: u64 = @intCast(ma.dictionary_page_offset orelse ma.data_page_offset);
        try std.testing.expectEqual(sb % fastpath_block, sa % fastpath_block);
        const pb = try page_edit.walkPages(a, src_bytes, sb, @intCast(mb.total_compressed_size));
        const pa = try page_edit.walkPages(a, got, sa, @intCast(ma.total_compressed_size));
        try std.testing.expectEqual(pb.len, pa.len);
        for (pb, pa) |x, y| {
            try std.testing.expectEqual(x.offset % fastpath_block, y.offset % fastpath_block);
            if (std.mem.eql(u8, x.bytes(src_bytes), y.bytes(got))) continue;
            edited_pages += 1;
            // A rewritten page's header ends in the pad, if any, in short form: tag, length, zeros, STOP.
            const fields = try patch.fieldsOf(a, got[@intCast(y.offset)..][0..y.header_len]);
            const last = fields[fields.len - 1];
            if (last.id == page_edit.pad_field_id) try std.testing.expectEqual(thrift.Type.Binary, last.type);
        }
    };
    try std.testing.expectEqual(@as(usize, s.pages_rewritten), edited_pages);

    // Every value equals the source's, but the rows matched: f 42 and k NULL there.
    const src: @import("../core/consumer/types.zig").RGSrc = .{ .bytes = got, .byte_origin = 0 };
    const ref: @import("../core/consumer/types.zig").RGSrc = .{ .bytes = src_bytes, .byte_origin = 0 };
    for (before.row_groups.items, after.row_groups.items) |*rb, *ra| {
        const rid = (try consumer_column.decodeWriteColumn(a, ra, &after, src, 2, .{}, null)).i64;
        const f_new = (try consumer_column.decodeWriteColumn(a, ra, &after, src, 1, .{}, null)).i32;
        const f_old = (try consumer_column.decodeWriteColumn(a, rb, &before, ref, 1, .{}, null)).i32;
        const k_new = (try consumer_column.decodeWriteColumn(a, ra, &after, src, 0, .{}, null)).i64;
        const k_old = (try consumer_column.decodeWriteColumn(a, rb, &before, ref, 0, .{}, null)).i64;
        for (0..rid.values.len) |i| {
            const present = struct {
                fn at(d: ?[]const u32, j: usize) bool {
                    return if (d) |l| l[j] == 1 else true;
                }
            }.at;
            const hit = present(rid.def_levels, i) and rid.values[i] >= 5000 and rid.values[i] < 5010;
            if (hit) {
                try std.testing.expect(present(f_new.def_levels, i) and f_new.values[i] == 42);
                try std.testing.expect(!present(k_new.def_levels, i));
                continue;
            }
            try std.testing.expectEqual(present(f_old.def_levels, i), present(f_new.def_levels, i));
            if (present(f_old.def_levels, i)) try std.testing.expectEqual(f_old.values[i], f_new.values[i]);
            try std.testing.expectEqual(present(k_old.def_levels, i), present(k_new.def_levels, i));
            if (present(k_old.def_levels, i)) try std.testing.expectEqual(k_old.values[i], k_new.values[i]);
        }
    }

    // A value the column's type does not hold exactly is refused before anything is written.
    const bad = [_]Set{.{ .column = "f", .assignment = try filter_parser.parseSyntax(a, "f = 1.5") }};
    var refused: Problem = .{};
    try std.testing.expectError(error.UpdateRefused, run(gpa, io, .{ .target = target, .where = req.where, .sets = &bad }, &refused));
    try std.testing.expect(std.mem.indexOf(u8, refused.text(), "exactly") != null or std.mem.indexOf(u8, refused.text(), "holds") != null);
}

const fastpath_block = @import("../core/writer/fastpath.zig").share_block;
