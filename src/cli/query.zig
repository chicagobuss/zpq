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
const schema_tree = zpq.core.parquet.schema_tree;
const thrift = zpq.core.thrift;
const filter_ast = zpq.core.filter.ast;
const filter_parser = zpq.core.filter.parser;
const filter_prune = zpq.core.filter.prune;
const expr_ast = zpq.core.expr.ast;
const expr_parser = zpq.core.expr.parser;
const consumer = zpq.core.consumer;
const streaming = zpq.core.writer.streaming;

const PAR1: [4]u8 = .{ 'P', 'A', 'R', '1' };

pub const Args = struct {
    input: []const u8,
    output: []const u8,
    filter: ?[]const u8 = null,
    columns: ?[]const []const u8 = null,
    /// Comma-separated SELECT expressions. Mutually exclusive with
    /// `columns`. When set, the output schema is flat — one leaf per
    /// item, named by `AS alias` or by the bare column name. Supports
    /// arithmetic on numeric columns; see `core/expr/`.
    select: ?[]const u8 = null,
    codec: schema.CompressionCodec = .SNAPPY,
};

pub const Timings = struct {
    read_ns: u64 = 0,
    parse_ns: u64 = 0,
    /// Decode/eval/encode/sink phase counters. Same shape lambda uses;
    /// CLI's "write" phase corresponds to `core.sink_ns`.
    core: consumer.Timings = .{},
    /// Footer write tail.
    footer_ns: u64 = 0,
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

    if (args.select != null and args.columns != null) return error.ConflictingFlags;

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

    // 2b. Parse --select if given. Each item becomes either a
    // passthrough (for bare column refs) or a computed entry (for
    // expressions). When --select is in play, the output schema is
    // FLAT — one leaf per item, no nested projection. The CLI rejects
    // mixing --select with --columns above; future work can unify.
    const select_items: ?[]expr_ast.SelectItem = if (args.select) |s|
        try expr_parser.parseSelect(arena, s, &meta)
    else
        null;

    // 3. Parse filter (against the schema; column names → indices).
    var filter_opt: ?filter_ast.Filter = null;
    if (args.filter) |expr| {
        if (expr.len > 0) {
            filter_opt = try filter_parser.parse(arena, expr, &meta);
        }
    }
    t.parse_ns = @intCast(nowMonoNs() - t_parse_start);

    // 4. Open output file. Wrap the fd in a streaming.Sink so the
    // shared per-RG consumer (`core.consumer`) writes through the same
    // interface lambda's multipart-S3 sink presents.
    const out_fd = try createFile(args.output);
    defer _ = std.os.linux.close(out_fd);
    var fd_sink: FdSink = .{ .fd = out_fd };
    const sink: streaming.Sink = .{
        .ctx = @ptrCast(&fd_sink),
        .write_fn = FdSink.writeFn,
    };

    var out_offset: u64 = 0;
    try sink.write(&PAR1);
    out_offset += PAR1.len;

    // 5. Build per-leaf bool vectors and the output_specs list.
    //
    // Three modes:
    //   - --select given: one OutputCol per select item. Bare column
    //     refs without alias collapse to `passthrough`; everything
    //     else is `computed`. fetch_arr is the union of every
    //     referenced column.
    //   - --columns given (or no projection): every kept column
    //     becomes a passthrough OutputCol. fetch_arr = kept_arr ∪
    //     filter_cols.
    const num_leaves = meta.row_groups.items[0].columns.items.len;
    const kept_arr = try arena.alloc(bool, num_leaves);
    if (kept_set) |s| {
        @memcpy(kept_arr, s);
    } else {
        @memset(kept_arr, true);
    }
    const fetch_arr = try arena.alloc(bool, num_leaves);
    @memset(fetch_arr, false);

    var output_specs: std.ArrayList(consumer.OutputCol) = .empty;
    var any_computed = false;

    if (select_items) |items| {
        for (items) |item| {
            switch (item.expr) {
                .col_ref => |c| {
                    if (item.alias) |alias| {
                        // Passthrough with rename: still requires the
                        // encode path so the output schema picks up
                        // the alias. Treat as computed (eval is a
                        // memcpy in this case anyway).
                        try output_specs.append(arena, .{ .computed = .{ .expr = item.expr, .alias = alias } });
                        any_computed = true;
                    } else {
                        try output_specs.append(arena, .{ .passthrough = c.col_idx });
                    }
                    if (c.col_idx < num_leaves) fetch_arr[c.col_idx] = true;
                },
                else => {
                    const alias = item.alias orelse return error.MissingAlias;
                    try output_specs.append(arena, .{ .computed = .{ .expr = item.expr, .alias = alias } });
                    any_computed = true;
                    collectExprCols(item.expr, fetch_arr);
                },
            }
        }
    } else {
        // --columns or default: passthrough every kept leaf in DFS
        // order so the output respects the source's column ordering.
        for (kept_arr, 0..) |b, i| if (b) {
            try output_specs.append(arena, .{ .passthrough = i });
            fetch_arr[i] = true;
        };
    }

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

    // Whole-file RGSrc: every column chunk lives somewhere in
    // `file_bytes` at its absolute `data_page_offset`. Pass byte_origin=0
    // so the consumer's `col_off = dpo - byte_origin` resolves directly.
    const rg_src: consumer.RGSrc = .{ .bytes = file_bytes, .byte_origin = 0 };

    // For the no-filter copy path: bool[]-shaped projection (or null
    // when there's no projection — the consumer then does a single
    // bounding-box write). The encoder path uses output_specs.
    const copy_kept_set: ?[]const bool = if (kept_set != null) kept_arr else null;

    // Encoder is required when there's a real predicate or when the
    // output includes any computed (or aliased) column. Otherwise
    // copyRG can byte-copy.
    const need_encoder = filter_opt != null or any_computed;

    for (meta.row_groups.items) |*src_rg| {
        rows_in += src_rg.num_rows;

        // Stat-prune.
        if (filter_opt) |f| {
            if ((try filter_prune.pruneRowGroup(src_rg, f, arena)) == .skip) continue;
        }

        const out = if (need_encoder) try consumer.encodeRG(
            arena,
            gpa,
            src_rg,
            &meta,
            rg_src,
            filter_opt,
            fetch_arr,
            output_specs.items,
            sink,
            &out_offset,
            args.codec,
            &t.core,
        ) else if (copy_kept_set != null) try consumer.copyRG(
            arena,
            src_rg,
            rg_src,
            copy_kept_set,
            sink,
            &out_offset,
            &t.core,
        ) else blk: {
            // No filter, no projection: build a tight whole-RG window
            // before calling copyRG so the consumer's no-projection
            // path emits a single bounding-box sink.write.
            const range = wholeRGSpan(src_rg) orelse break :blk consumer.RGOut{ .surviving_rows = 0, .rg = null };
            const tight: consumer.RGSrc = .{
                .bytes = file_bytes[range.start .. range.start + range.len],
                .byte_origin = range.start,
            };
            break :blk try consumer.copyRG(
                arena,
                src_rg,
                tight,
                null,
                sink,
                &out_offset,
                &t.core,
            );
        };

        if (out.rg) |new_rg| try new_row_groups.append(arena, new_rg);
        rows_kept += out.surviving_rows;
        if (out.surviving_rows > 0) rg_kept += 1;
    }

    // 7. Build footer.
    const t_footer_start = nowMonoNs();

    var new_schema = meta.schema;
    if (select_items != null) {
        // --select: flat schema, one leaf per OutputCol.
        new_schema = try buildSelectSchema(arena, &meta, output_specs.items);
    } else if (kept_set) |_| {
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
    try sink.write(footer_bytes);
    out_offset += footer_bytes.len;

    var len_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_bytes, @intCast(footer_bytes.len), .little);
    try sink.write(&len_bytes);
    out_offset += len_bytes.len;
    try sink.write(&PAR1);
    out_offset += PAR1.len;

    t.footer_ns += @intCast(nowMonoNs() - t_footer_start);

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

/// Walk an expression and mark every referenced column index in
/// `fetch_arr`. Used so the consumer's decode pass loads only the
/// columns the select expressions actually need.
fn collectExprCols(e: expr_ast.Expr, fetch_arr: []bool) void {
    switch (e) {
        .literal => {},
        .col_ref => |c| if (c.col_idx < fetch_arr.len) {
            fetch_arr[c.col_idx] = true;
        },
        .binop => |b| {
            collectExprCols(b.left.*, fetch_arr);
            collectExprCols(b.right.*, fetch_arr);
        },
    }
}

/// Build a flat output schema for the `--select` path. The Parquet
/// thrift schema is DFS-ordered: schema[0] is the root group with
/// `num_children = N`, followed by N leaves. For passthrough specs we
/// copy the input column's SchemaElement (preserving converted_type /
/// logical_type metadata where present); for computed specs we
/// synthesize a REQUIRED leaf with type INT64 or DOUBLE.
fn buildSelectSchema(
    arena: std.mem.Allocator,
    meta: *const schema.FileMetaData,
    specs: []const consumer.OutputCol,
) !std.ArrayListUnmanaged(schema.SchemaElement) {
    var out: std.ArrayListUnmanaged(schema.SchemaElement) = .empty;
    try out.ensureTotalCapacity(arena, specs.len + 1);

    // Root group.
    try out.append(arena, .{
        .type = null,
        .type_length = null,
        .repetition_type = null,
        .name = "schema",
        .num_children = @intCast(specs.len),
        .converted_type = null,
        .logical_type = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    });

    for (specs) |spec| {
        switch (spec) {
            .passthrough => |ci| {
                // Look up the input leaf via the first RG's column
                // chunk metadata — its path_in_schema points at the
                // SchemaElement. For a flat input this is just one
                // hop; for nested inputs we'd need to flatten, but the
                // CLI rejects --select with --columns above so the
                // expression parser only resolves to flat columns.
                const cm = meta.row_groups.items[0].columns.items[ci].meta_data orelse return error.ColumnMetaMissing;
                const elem = meta.getColumnSchema(cm.path_in_schema.items) orelse return error.SchemaLookupFailed;
                var copy = elem;
                copy.num_children = 0;
                try out.append(arena, copy);
            },
            .computed => |c| {
                try out.append(arena, .{
                    .type = c.expr.typeOf().toParquet(),
                    .type_length = null,
                    .repetition_type = .REQUIRED,
                    .name = c.alias,
                    .num_children = 0,
                    .converted_type = null,
                    .logical_type = null,
                    .scale = null,
                    .precision = null,
                    .field_id = null,
                });
            },
        }
    }
    return out;
}

/// Bounding-box span of an RG in the source file: [min_col_start, max_col_end).
/// The no-projection copy path slices `file_bytes[range]` and hands it
/// to `consumer.copyRG` with `kept_set = null` so the consumer emits a
/// single sink.write covering every column.
const ByteRange = struct { start: usize, len: usize };

fn wholeRGSpan(rg: *const schema.RowGroup) ?ByteRange {
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

/// Local-file `streaming.Sink`. Same shape lambda's multipart-S3 sink
/// presents, backed by a plain file descriptor + retry-on-short-write
/// loop. The consumer doesn't care which one it talks to.
const FdSink = struct {
    fd: std.os.linux.fd_t,

    fn writeFn(ctx: *anyopaque, bytes: []const u8) anyerror!void {
        const self: *FdSink = @ptrCast(@alignCast(ctx));
        return writeAll(self.fd, bytes);
    }
};

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
