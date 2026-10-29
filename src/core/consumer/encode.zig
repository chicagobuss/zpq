const metadata = @import("../parquet/metadata.zig");
const agg_dense = @import("../expr/agg_dense.zig");
const AggTarget = @import("target.zig").AggTarget;
const AggRowGroup = types.AggRowGroup;
const RGSrc = types.RGSrc;
const cacheDictionary = @import("workspace.zig").cacheDictionary;
const streamByteArray = @import("plan.zig").streamByteArray;
const streamWindowRows = @import("plan.zig").streamWindowRows;
const DecodeOptions = @import("../parquet/column.zig").DecodeOptions;
const DecodeScratch = @import("../parquet/column.zig").DecodeScratch;
const decodeAggColumnCoded = @import("agg_column.zig").decodeAggColumnCoded;
const SliceReader = @import("slice_reader.zig").SliceReader;
const compression = @import("../parquet/compression.zig");
const fastpath = @import("../writer/fastpath.zig");
const std = @import("std");
const builtin = @import("builtin");
const schema = @import("../schema.zig");
const filter_eval = @import("../filter/eval.zig");
const expr_agg = @import("../expr/agg.zig");
const core_memory = @import("../memory.zig");
const encoder = @import("../writer/encoder.zig");
const streaming = @import("../writer/streaming.zig");
const page_index = @import("../writer/page_index.zig");
const thrift = @import("../thrift.zig");
const decimal_mod = @import("../parquet/decimal.zig");
const WriterStatsPolicy = @import("../parquet/writer_statistics.zig").Policy;
const spawn_util = @import("../spawn.zig");
const profile = @import("../../profile.zig");
const nowMonoNs = @import("../../clock.zig").monoNs;
const types = @import("types.zig");
const OutputCol = types.OutputCol;
const decimalElem = types.decimalElem;
const RGOut = types.RGOut;
const Timings = types.Timings;
const output = @import("output.zig");
const OutputAggregator = output.OutputAggregator;
const bufRows = output.bufRows;
const appendRows = output.appendRows;
const BufRange = output.BufRange;
const joinRows = output.joinRows;
const BloomWrite = @import("copy.zig").BloomWrite;
const WORKER_STACK_SIZE: usize = 1 << 20;

/// Encode the aggregator's accumulated rows as ONE output row group,
/// write to sink, and return RGOut. Caller should `resetAggregator()`
/// afterward if continuing. Returns null RG when num_rows == 0.
pub fn encodeAggregator(
    agg: *OutputAggregator,
    out_arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    sink: streaming.Sink,
    out_offset: *u64,
    output_codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    timings: *Timings,
    // Column-encode parallelism: 0 = auto (min(n_cols, cpus)). Pass 1 to
    // force serial column encode when the CALLER is already parallel across
    // row groups, so we don't oversubscribe (N_rg_workers × N_col_workers).
    encode_workers: usize,
    // Whether to synthesize + write the page index (ColumnIndex/OffsetIndex)
    // inline after each chunk. MUST be false when writing to a relative-offset
    // buffer that is later placed at a nonzero file offset: the OffsetIndex's
    // serialized PageLocation.offset would stay buffer-relative and mislead
    // readers. Direct-to-file callers (absolute out_offset) pass true.
    emit_page_index: bool,
    bloom_write: BloomWrite,
) !RGOut {
    if (agg.num_rows == 0) return .{ .surviving_rows = 0, .rg = null };

    var rg_arena_state = std.heap.ArenaAllocator.init(gpa);
    defer rg_arena_state.deinit();
    const ra = rg_arena_state.allocator();

    const n_specs = agg.cols.len;

    // A Batch.Column per output col over the aggregator's typed buffers (pointing into agg.arena).
    const batch_cols = try ra.alloc(filter_eval.Batch.Column, n_specs);
    for (batch_cols, 0..) |*bc, i| bc.* = try aggBatchColumn(agg, i, ra);

    // Parallel encode of all output columns. Same shape as the per-RG
    // path: spawn workers up to min(n_specs, cpus), each pulls a slice
    // of indices via stride. Results indexed by spec position so the
    // sequential write phase below preserves output order.
    const cpus = std.Thread.getCpuCount() catch 2;
    const auto_workers: usize = @min(n_specs, @max(@as(usize, 2), cpus));
    const num_workers: usize = if (encode_workers > 0) @min(n_specs, encode_workers) else auto_workers;

    const results = try ra.alloc(?encoder.EncodedColumn, n_specs);
    @memset(results, null);

    var task_arenas: ?[]std.heap.ArenaAllocator = null;
    defer if (task_arenas) |arenas| {
        for (arenas) |*ar| ar.deinit();
    };

    // Pre-pass: FLBA-backed decimals don't fit a Batch.Column lane, so
    // encode them directly (a small subset; serial is fine). The encode
    // loops below skip any index already filled here.
    for (agg.cols, 0..) |cbuf, i| {
        if (cbuf == .i128 and agg.schema_elems[i].type == .FIXED_LEN_BYTE_ARRAY) {
            const b = cbuf.i128;
            results[i] = try encoder.encodeDecimalFlba(ra, .{
                .values = b.values.items,
                .def_levels = if (b.max_def > 0) b.def_levels.items else null,
                .max_def = b.max_def,
            }, &agg.schema_elems[i], agg.paths[i], output_codec, zstd);
        }
    }

    var rg_columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try rg_columns.ensureTotalCapacity(out_arena, n_specs);
    var rg_total: i64 = 0;
    const placer: ChunkPlacer = .{
        .out_arena = out_arena,
        .sink = sink,
        .out_offset = out_offset,
        .timings = timings,
        .emit_page_index = emit_page_index,
        .columns = &rg_columns,
        .total = &rg_total,
    };

    const t_enc_start = nowMonoNs();
    if (num_workers <= 1) {
        // Serial: write each chunk as soon as it is encoded and drop its working memory (hash tables, index and
        // payload buffers, the compressor's output), so a row group holds one column's encoding at a time.
        for (0..n_specs) |i| {
            var col_arena = std.heap.ArenaAllocator.init(gpa);
            defer col_arena.deinit();
            const enc = results[i] orelse
                try encodeAggColumn(agg, i, col_arena.allocator(), output_codec, zstd, bloom_write, timings);
            try placer.place(enc, &agg.schema_elems[i]);
        }
    } else {
        const arenas = try ra.alloc(std.heap.ArenaAllocator, num_workers);
        for (arenas) |*ar| ar.* = std.heap.ArenaAllocator.init(gpa);
        task_arenas = arenas;

        const ctxs = try ra.alloc(AggEncodeCtx, num_workers);
        const threads = try ra.alloc(std.Thread, num_workers);
        // Initialize every context before spawning: a partial spawn failure runs the rest inline, so none may be
        // half-built.
        for (0..num_workers) |w| {
            ctxs[w] = .{
                .arena = arenas[w].allocator(),
                .start = w,
                .stride = num_workers,
                .batch_cols = batch_cols,
                .schema_elems = agg.schema_elems,
                .paths = agg.paths,
                .codec = output_codec,
                .zstd = zstd,
                .bloom_write = bloom_write,
                .results = results,
                .err = null,
            };
        }

        var spawned: usize = 0;
        for (0..num_workers) |w| {
            // Not `try`: an early return would unwind while earlier workers are still encoding into `arenas`, which
            // the caller releases.
            threads[w] = spawn_util.spawn(.{ .stack_size = WORKER_STACK_SIZE }, aggEncodeWorker, .{&ctxs[w]}) catch break;
            spawned += 1;
        }
        // Every context that missed its thread must run here, or its columns go unencoded and the write phase fails
        // with MissingEncodeResult.
        for (spawned..num_workers) |w| aggEncodeWorker(&ctxs[w]);
        for (threads[0..spawned]) |t| t.join();
        for (ctxs) |c| if (c.err) |err| return err;
        const t_enc_end = nowMonoNs();
        timings.encode_ns += @intCast(t_enc_end - t_enc_start);
        profile.record(timings.prof, .encode, t_enc_start, t_enc_end, profile.args(.{ n_specs, agg.num_rows }));
        // Sequential write phase, in output order.
        for (results, agg.schema_elems) |maybe_enc, *elem| {
            try placer.place(maybe_enc orelse return error.MissingEncodeResult, elem);
        }
    }

    return .{
        .surviving_rows = @intCast(agg.num_rows),
        .rg = .{
            .columns = rg_columns,
            .total_byte_size = rg_total,
            .num_rows = @intCast(agg.num_rows),
        },
    };
}

/// Output column `i` of `agg` as a Batch.Column over its typed buffer. DECIMAL lanes narrow the unscaled i128 back to
/// their source physical width (exact: decoded from i32/i64 and widened) for the integer encoder, the kept DECIMAL
/// schema element carrying the logical type; an FLBA decimal gets an empty placeholder (`encodeAggColumn` encodes it).
fn aggBatchColumn(agg: *const OutputAggregator, i: usize, ra: std.mem.Allocator) !filter_eval.Batch.Column {
    const cbuf = agg.cols[i];
    return switch (cbuf) {
        .i32 => |b| .{ .i32 = .{
            .values = b.values.items,
            .def_levels = if (b.max_def > 0) b.def_levels.items else null,
            .max_def = b.max_def,
            .codes = if (b.coded) b.codes.items else null,
            .dict = b.dict,
        } },
        .i64 => |b| .{ .i64 = .{
            .values = b.values.items,
            .def_levels = if (b.max_def > 0) b.def_levels.items else null,
            .max_def = b.max_def,
            .codes = if (b.coded) b.codes.items else null,
            .dict = b.dict,
        } },
        .f32 => |b| .{ .f32 = .{
            .values = b.values.items,
            .def_levels = if (b.max_def > 0) b.def_levels.items else null,
            .max_def = b.max_def,
            .codes = if (b.coded) b.codes.items else null,
            .dict = b.dict,
        } },
        .f64 => |b| .{ .f64 = .{
            .values = b.values.items,
            .def_levels = if (b.max_def > 0) b.def_levels.items else null,
            .max_def = b.max_def,
            .codes = if (b.coded) b.codes.items else null,
            .dict = b.dict,
        } },
        .string => |b| .{ .string = .{
            .values = b.values.items,
            .def_levels = if (b.max_def > 0) b.def_levels.items else null,
            .max_def = b.max_def,
            .codes = if (b.coded) b.codes.items else null,
            .dict = b.dict,
        } },
        .boolean => |b| .{ .boolean = .{
            .values = b.values.items,
            .def_levels = if (b.max_def > 0) b.def_levels.items else null,
            .max_def = b.max_def,
            .codes = if (b.coded) b.codes.items else null,
            .dict = b.dict,
        } },
        // Lossless DECIMAL: narrow the unscaled i128 back to its
        // source physical width (exact — the values were decoded
        // from i32/i64 and widened) and feed the existing integer
        // encoder. The kept DECIMAL schema element makes the footer
        // (and `meta.type`) carry the logical type through.
        .i128 => |b| blk: {
            const phys = agg.schema_elems[i].type orelse return error.UnsupportedColumnType;
            const dl = if (b.max_def > 0) b.def_levels.items else null;
            switch (phys) {
                .INT32 => {
                    const vals = try ra.alloc(i32, b.values.items.len);
                    for (b.values.items, 0..) |v, j| vals[j] = @intCast(v);
                    break :blk .{ .i32 = .{ .values = vals, .def_levels = dl, .max_def = b.max_def } };
                },
                .INT64 => {
                    const vals = try ra.alloc(i64, b.values.items.len);
                    for (b.values.items, 0..) |v, j| vals[j] = @intCast(v);
                    break :blk .{ .i64 = .{ .values = vals, .def_levels = dl, .max_def = b.max_def } };
                },
                // FLBA decimals can't ride an integer lane — placeholder
                // here; the pre-pass below encodes them via encodeDecimalFlba.
                .FIXED_LEN_BYTE_ARRAY => break :blk .{ .i64 = .{ .values = &.{} } },
                else => return error.UnsupportedColumnType,
            }
        },
    };
}

/// Encode output column `i` of `agg` on `arena`.
fn encodeAggColumn(
    agg: *const OutputAggregator,
    i: usize,
    arena: std.mem.Allocator,
    output_codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    bloom_write: BloomWrite,
    timings: *Timings,
) !encoder.EncodedColumn {
    const t0 = nowMonoNs();
    const span = profile.Span.begin(timings.prof);
    const e = if (agg.cols[i] == .i128 and agg.schema_elems[i].type == .FIXED_LEN_BYTE_ARRAY) blk: {
        const b = agg.cols[i].i128;
        break :blk try encoder.encodeDecimalFlba(arena, .{
            .values = b.values.items,
            .def_levels = if (b.max_def > 0) b.def_levels.items else null,
            .max_def = b.max_def,
        }, &agg.schema_elems[i], agg.paths[i], output_codec, zstd);
    } else try encoder.encodeColumnSegments(arena, .{
        .values = try aggBatchColumn(agg, i, arena),
        .schema_elem = &agg.schema_elems[i],
        .path_in_schema = agg.paths[i],
        .codec = output_codec,
        .zstd = zstd,
        .bloom_fpp = bloom_write.at(i),
        .dictionary_bloom = bloom_write.dictionary_bloom,
        .parquet_version = bloom_write.parquet_version,
    });
    if (span.t != null) {
        const em = e.meta;
        span.end(.encode_chunk, profile.args(.{
            i, output_codec, em.total_uncompressed_size, em.total_compressed_size, em.num_values,
        }));
    }
    const t1 = nowMonoNs();
    timings.encode_ns += @intCast(t1 - t0);
    profile.record(timings.prof, .encode, t0, t1, profile.args(.{ 1, agg.num_rows }));
    return e;
}

/// Encodes and places a row group's output columns one at a time, in output order, so a caller can project, encode
/// and drop each column before the next (the per-row-group re-encode's working set is then one column, not a row
/// group). Offsets are relative to `out_offset`; no page index (see `encodeAggregator`'s `emit_page_index`).
pub const RGEncoder = struct {
    columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty,
    total: i64 = 0,
    out_arena: std.mem.Allocator,
    sink: streaming.Sink,
    out_offset: *u64,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan = .{},
    bloom_write: BloomWrite,
    timings: *Timings,

    /// Encode `agg`'s column `i` (its rows complete) on `arena` and write it; `arena` may be released once this
    /// returns, the chunk's metadata being cloned into `out_arena`.
    pub fn column(self: *RGEncoder, agg: *const OutputAggregator, i: usize, arena: std.mem.Allocator) !void {
        if (self.columns.capacity == 0) try self.columns.ensureTotalCapacity(self.out_arena, agg.cols.len);
        const enc = try encodeAggColumn(agg, i, arena, self.codec, self.zstd, self.bloom_write, self.timings);
        const placer: ChunkPlacer = .{
            .out_arena = self.out_arena,
            .sink = self.sink,
            .out_offset = self.out_offset,
            .timings = self.timings,
            .emit_page_index = false,
            .columns = &self.columns,
            .total = &self.total,
        };
        try placer.place(enc, &agg.schema_elems[i]);
    }

    pub fn finish(self: *RGEncoder, num_rows: usize) RGOut {
        if (num_rows == 0) return .{ .surviving_rows = 0, .rg = null };
        return .{
            .surviving_rows = @intCast(num_rows),
            .rg = .{ .columns = self.columns, .total_byte_size = self.total, .num_rows = @intCast(num_rows) },
        };
    }
};


/// The PLAIN size, in bits, of the present values in rows `lo..hi` of an output buffer written as `elem`: its physical
/// width (a BOOLEAN one bit, a FIXED_LEN_BYTE_ARRAY its length), a BYTE_ARRAY 4 bytes plus its length. What output row
/// groups are cut by: a property of the rows, however they were decoded or batched.
pub fn plainBits(buf: *const OutputAggregator.ColumnBuf, elem: *const schema.SchemaElement, lo: usize, hi: usize) u64 {
    const fixed: ?u64 = switch (elem.type orelse .BYTE_ARRAY) {
        .BOOLEAN => 1,
        .INT32, .FLOAT => 32,
        .INT64, .DOUBLE => 64,
        .INT96 => 96,
        .FIXED_LEN_BYTE_ARRAY => @as(u64, @intCast(@max(0, elem.type_length orelse 0))) * 8,
        .BYTE_ARRAY => null,
    };
    switch (buf.*) {
        inline else => |b| {
            const levels: ?[]const u32 = if (b.max_def > 0) b.def_levels.items[lo..hi] else null;
            if (fixed) |w| return w * encoder.countPresentRows(hi - lo, levels, b.max_def);
            if (comptime @TypeOf(b.dict) != []const []const u8) {
                return 0;
            } else {
                var bits: u64 = 0;
                for (lo..hi) |r| {
                    if (levels) |l| if (l[r - lo] != b.max_def) continue;
                    const v = if (b.coded) b.dict[b.codes.items[r]] else b.values.items[r];
                    bits += (4 + @as(u64, v.len)) * 8;
                }
                return bits;
            }
        },
    }
}

/// Per row, the PLAIN bits of every row before it (`plainBits` over all of `agg`'s columns, summed): `n + 1` running
/// totals, so the bits of rows `lo..hi` are `prefix[hi] - prefix[lo]` and the row a byte target falls in is found by
/// search. The sums are `plainBits`'s exactly.
pub fn plainBitsPrefix(arena: std.mem.Allocator, agg: *const OutputAggregator) ![]u64 {
    const n = agg.num_rows;
    const prefix = try arena.alloc(u64, n + 1);
    @memset(prefix, 0);
    // Fixed-width columns without NULLs take the same bits every row: added once per row in the running sum.
    var per_row: u64 = 0;
    for (agg.cols, agg.schema_elems) |*buf, *elem| {
        const fixed: ?u64 = switch (elem.type orelse .BYTE_ARRAY) {
            .BOOLEAN => 1,
            .INT32, .FLOAT => 32,
            .INT64, .DOUBLE => 64,
            .INT96 => 96,
            .FIXED_LEN_BYTE_ARRAY => @as(u64, @intCast(@max(0, elem.type_length orelse 0))) * 8,
            .BYTE_ARRAY => null,
        };
        switch (buf.*) {
            inline else => |b| {
                if (fixed) |w| if (b.max_def == 0) {
                    per_row += w;
                    continue;
                };
                for (0..n) |r| {
                    if (b.max_def > 0 and b.def_levels.items[r] != b.max_def) continue;
                    prefix[r + 1] += fixed orelse if (comptime @TypeOf(b.dict) != []const []const u8) 0 else blk: {
                        const v = if (b.coded) b.dict[b.codes.items[r]] else b.values.items[r];
                        break :blk (4 + @as(u64, v.len)) * 8;
                    };
                }
            },
        }
    }
    for (1..n + 1) |r| prefix[r] += prefix[r - 1] + per_row;
    return prefix;
}

/// An encoded chunk placed at the sink's offset `out_offset.*`: its metadata copied into `out_arena` with offsets made
/// absolute, and its pages (offsets still relative to the chunk) copied there too, for its page index.
pub fn placeChunk(
    out_arena: std.mem.Allocator,
    sink: streaming.Sink,
    out_offset: *u64,
    enc: encoder.EncodedColumn,
) !struct { chunk: schema.ColumnChunk, pages: []const encoder.PageInfo } {
    const start: i64 = @intCast(out_offset.*);
    var em = try cloneColumnMeta(out_arena, enc.meta);
    em.data_page_offset += start;
    if (em.dictionary_page_offset) |dpo| em.dictionary_page_offset = dpo + start;
    const pages = try out_arena.dupe(encoder.PageInfo, enc.pages);
    for (pages) |*p| {
        if (p.min) |v| p.min = try out_arena.dupe(u8, v);
        if (p.max) |v| p.max = try out_arena.dupe(u8, v);
    }
    try enc.writeTo(sink);
    out_offset.* += enc.byteLen();
    return .{
        .chunk = .{
            .file_path = null,
            .file_offset = start,
            .meta_data = em,
            .bloom_out = if (enc.bloom.len > 0) try out_arena.dupe(u8, enc.bloom) else &.{},
        },
        .pages = pages,
    };
}

/// Writes encoded chunks to the sink in output order: metadata deep-cloned into `out_arena` (which outlives the
/// encoders' arenas) with offsets made absolute, and, direct to a file, a synthesised page index after each chunk.
const ChunkPlacer = struct {
    out_arena: std.mem.Allocator,
    sink: streaming.Sink,
    out_offset: *u64,
    timings: *Timings,
    emit_page_index: bool,
    columns: *std.ArrayListUnmanaged(schema.ColumnChunk),
    total: *i64,

    fn place(self: ChunkPlacer, enc: encoder.EncodedColumn, elem: *const schema.SchemaElement) !void {
        const col_start_in_file: i64 = @intCast(self.out_offset.*);
        var em = try cloneColumnMeta(self.out_arena, enc.meta);
        em.data_page_offset += col_start_in_file;
        if (em.dictionary_page_offset) |dpo| em.dictionary_page_offset = dpo + col_start_in_file;
        const t_sink_start = nowMonoNs();
        try enc.writeTo(self.sink);
        const t_sink_end = nowMonoNs();
        self.timings.sink_ns += @intCast(t_sink_end - t_sink_start);
        profile.record(self.timings.prof, .sink_write, t_sink_start, t_sink_end, profile.args(.{enc.byteLen()}));
        self.out_offset.* += enc.byteLen();
        self.total.* += enc.meta.total_uncompressed_size;

        var chunk: schema.ColumnChunk = .{
            .file_path = null,
            .file_offset = col_start_in_file,
            .meta_data = em,
            // Placed after the last row group, so it must outlive this row group's arena and the encoder's.
            .bloom_out = if (enc.bloom.len > 0) try self.out_arena.dupe(u8, enc.bloom) else &.{},
        };
        // Straight to a file the chunk's page index can follow it, where its pages' offsets are final.
        if (self.emit_page_index) {
            var so: streaming.SinkOut = .{ .sink = self.sink, .offset = @intCast(self.out_offset.*) };
            try page_index.writeInline(self.out_arena, &so, &chunk, enc.pages, elem);
            self.out_offset.* = so.offset;
        }
        try self.columns.append(self.out_arena, chunk);
    }
};

const AggEncodeCtx = struct {
    arena: std.mem.Allocator,
    start: usize,
    stride: usize,
    batch_cols: []const filter_eval.Batch.Column,
    schema_elems: []const schema.SchemaElement,
    paths: []const []const []const u8,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    bloom_write: BloomWrite,
    results: []?encoder.EncodedColumn,
    err: ?anyerror,
};

fn aggEncodeWorker(ctx: *AggEncodeCtx) void {
    var i = ctx.start;
    while (i < ctx.batch_cols.len) : (i += ctx.stride) {
        if (ctx.results[i] != null) continue; // FLBA decimal done in pre-pass
        ctx.results[i] = encoder.encodeColumnSegments(ctx.arena, .{
            .values = ctx.batch_cols[i],
            .schema_elem = &ctx.schema_elems[i],
            .path_in_schema = ctx.paths[i],
            .codec = ctx.codec,
            .zstd = ctx.zstd,
            .bloom_fpp = ctx.bloom_write.at(i),
            .dictionary_bloom = ctx.bloom_write.dictionary_bloom,
            .parquet_version = ctx.bloom_write.parquet_version,
        }) catch |err| {
            ctx.err = err;
            return;
        };
    }
}

/// Deep-clone a ColumnMetaData's inner slices into `arena`. Used by
/// the parallel-encode write phase to migrate metadata out of per-task
/// arenas (which get freed at end of RG processing) into the longer-
/// lived `out_arena` (which survives until footer write).
///
/// What gets duped:
///   - `encodings.items` and `path_in_schema.items` containers (POD
///     element arrays — safe to copy header-by-header). The strings
///     inside path_in_schema already live in long-lived storage
///     (source-meta arena for passthrough, parser arena for computed).
///   - `statistics.{min,max,min_value,max_value}` byte slices: for
///     BYTE_ARRAY columns these point into per-task arena, so we
///     `arena.dupe`. For numeric stats they're page_allocator-owned
///     and could be borrowed, but uniform dupe keeps the code simple.
fn cloneColumnMeta(
    arena: std.mem.Allocator,
    src: schema.ColumnMetaData,
) !schema.ColumnMetaData {
    var encodings: schema.EncodingList = .empty;
    try encodings.appendSlice(arena, src.encodings.items);

    var path_list: schema.StringList = .empty;
    try path_list.appendSlice(arena, src.path_in_schema.items);

    var stats: ?schema.Statistics = null;
    if (src.statistics) |st| {
        stats = .{
            .max = if (st.max) |v| try arena.dupe(u8, v) else null,
            .min = if (st.min) |v| try arena.dupe(u8, v) else null,
            .null_count = st.null_count,
            .distinct_count = st.distinct_count,
            .max_value = if (st.max_value) |v| try arena.dupe(u8, v) else null,
            .min_value = if (st.min_value) |v| try arena.dupe(u8, v) else null,
            .is_max_value_exact = st.is_max_value_exact,
            .is_min_value_exact = st.is_min_value_exact,
            .nan_count = st.nan_count,
        };
    }

    return .{
        .type = src.type,
        .encodings = encodings,
        .path_in_schema = path_list,
        .codec = src.codec,
        .num_values = src.num_values,
        .total_uncompressed_size = src.total_uncompressed_size,
        .total_compressed_size = src.total_compressed_size,
        .data_page_offset = src.data_page_offset,
        .index_page_offset = src.index_page_offset,
        .dictionary_page_offset = src.dictionary_page_offset,
        .statistics = stats,
        .encoding_stats = if (src.encoding_stats) |es| try es.clone(arena) else null,
    };
}

/// The schema leaf an aggregate output column is written under. An exact DECIMAL is annotated DECIMAL(p, s), both
/// ways (logical and converted type), at the physical width `Dec.physical` gives its precision.
fn aggLeafElem(out_col: expr_agg.OutputCol) schema.SchemaElement {
    var elem: schema.SchemaElement = .{
        .type = out_col.parquet_type,
        .type_length = out_col.type_length,
        .repetition_type = if (out_col.nullable) .OPTIONAL else .REQUIRED,
        .name = out_col.name,
        .num_children = 0,
        .converted_type = out_col.converted_type,
        .logical_type = out_col.logical_type,
        .logical_type_raw = out_col.logical_type_raw,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    if (out_col.dec) |d| decimalElem(&elem, d);
    return elem;
}


/// Rows from which `writeAggregateRows` encodes columns on several threads: fewer encode faster than threads start.
const parallel_encode_rows = 1 << 16;

/// Rows per row group of a written answer. A bigger answer is cut at these fixed boundaries, never by `-j` or memory,
/// so its row groups' columns encode on every worker and its readers can split it the same way; a smaller one is the
/// single row group it always was.
pub const answer_rg_rows: usize = 1 << 17;

/// Rows `lo..hi` of an answer column: its values and levels, nothing else (an answer column is never coded,
/// repeated or carrying a decimal's exact view).
fn sliceOutputCol(col: expr_agg.OutputCol, lo: usize, hi: usize) expr_agg.OutputCol {
    var out = col;
    switch (out.col) {
        inline else => |*c| {
            std.debug.assert(c.codes == null and c.rep_levels == null and c.unscaled == null);
            c.values = c.values[lo..hi];
            if (c.def_levels) |d| {
                c.def_levels = d[lo..hi];
                c.has_nulls = std.mem.indexOfScalar(u32, d[lo..hi], 0) != null;
            }
        },
    }
    return out;
}

fn encodeAggOutputCol(
    arena: std.mem.Allocator,
    out_col: expr_agg.OutputCol,
    leaf_elem: *const schema.SchemaElement,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    dictionary_bloom: bool,
    parquet_version: schema.ParquetVersion,
) !encoder.EncodedColumn {
    const path_buf = try arena.alloc([]const u8, 1);
    path_buf[0] = out_col.name;
    return encoder.encodeColumnSegments(arena, .{
        .values = out_col.col,
        .schema_elem = leaf_elem,
        .path_in_schema = path_buf,
        .codec = codec,
        .zstd = zstd,
        .distinct = out_col.distinct,
        .dictionary_bloom = dictionary_bloom,
        .parquet_version = parquet_version,
    });
}

/// How `writeAggregateRows` may encode a big answer's columns together: on up to `workers` threads (the query's), with
/// the columns in flight at once charged at most `budget` bytes (`aggEncodeCharge`). The default encodes them one at
/// a time, as every column of a small answer is.
pub const AggEncode = struct {
    parquet_version: schema.ParquetVersion = .v2,
    dictionary_bloom: bool = true,
    workers: usize = 1,
    budget: u64 = 0,
    /// The query's ledger: what the columns in flight hold is drawn from it before they encode, a wave narrowing to
    /// what it takes, and a column it cannot take alone refused. Null: the waves' `budget` alone, and a column alone
    /// uncharged.
    ledger: ?*expr_agg.SharedBudget = null,
};

/// `meta` with everything it points to copied into `arena`, every other field as it is: it outlives the wave arena
/// its encoding was made in. Unlike `cloneColumnMeta`, which builds a copy field by field, a field the encoder sets in
/// the future cannot be lost here.
fn ownColumnMeta(arena: std.mem.Allocator, meta: schema.ColumnMetaData) !schema.ColumnMetaData {
    var out = meta;
    out.encodings = .empty;
    try out.encodings.appendSlice(arena, meta.encodings.items);
    out.path_in_schema = .empty;
    try out.path_in_schema.appendSlice(arena, meta.path_in_schema.items);
    if (meta.statistics) |st| {
        var copy = st;
        inline for (.{ "max", "min", "max_value", "min_value" }) |f| {
            if (@field(st, f)) |v| @field(copy, f) = try arena.dupe(u8, v);
        }
        out.statistics = copy;
    }
    if (meta.encoding_stats) |es| out.encoding_stats = try es.clone(arena);
    return out;
}

/// What encoding `col` may hold at its peak: three times its values and levels, the estimate the re-encode path
/// slices row groups by (a dictionary attempt's table and indices, the page, its compressed copy).
fn aggEncodeCharge(col: expr_agg.OutputCol) u64 {
    const bytes: u64 = switch (col.col) {
        .string => |c| blk: {
            var n: u64 = c.values.len * @sizeOf([]const u8);
            for (c.values) |v| n += v.len;
            break :blk n + if (c.def_levels) |d| d.len * @sizeOf(u32) else 0;
        },
        inline else => |c| c.values.len * @sizeOf(@TypeOf(c.values[0])) +
            if (c.def_levels) |d| d.len * @sizeOf(u32) else 0,
    };
    return bytes *| 3;
}

/// A run of an answer's columns encoded together, one thread a column, each into an arena of its own that holds the
/// encoding until `reset`.
const AggColumnWave = struct {
    arenas: []std.heap.ArenaAllocator,
    results: []anyerror!encoder.EncodedColumn,
    tasks: []Task,

    const Task = struct {
        cols: []const expr_agg.OutputCol = &.{},
        arenas: []std.heap.ArenaAllocator,
        results: []anyerror!encoder.EncodedColumn,
        codec: schema.CompressionCodec,
        zstd: compression.ZstdPlan,
        cursor: *std.atomic.Value(usize),
        dictionary_bloom: bool,
        parquet_version: schema.ParquetVersion,

        /// Claim columns until none remain: one a failed spawn left unclaimed is taken by a thread that did start.
        fn run(t: *Task) void {
            while (true) {
                const i = t.cursor.fetchAdd(1, .monotonic);
                if (i >= t.cols.len) return;
                const elem = aggLeafElem(t.cols[i]);
                t.results[i] = encodeAggOutputCol(
                    t.arenas[i].allocator(),
                    t.cols[i],
                    &elem,
                    t.codec,
                    t.zstd,
                    t.dictionary_bloom,
                    t.parquet_version,
                );
            }
        }
    };

    /// Room for waves of up to `width` columns, taken from `arena` before anything is written, so a wave allocates
    /// nothing there.
    fn init(arena: std.mem.Allocator, width: usize) !AggColumnWave {
        const arenas = try arena.alloc(std.heap.ArenaAllocator, width);
        for (arenas) |*a| a.* = .init(std.heap.page_allocator);
        return .{
            .arenas = arenas,
            .results = try arena.alloc(anyerror!encoder.EncodedColumn, width),
            .tasks = try arena.alloc(Task, width),
        };
    }

    /// Encode `cols`, at most `arenas.len` of them, one thread a column.
    fn run(
        w: *AggColumnWave,
        cols: []const expr_agg.OutputCol,
        codec: schema.CompressionCodec,
        zstd: compression.ZstdPlan,
        dictionary_bloom: bool,
        parquet_version: schema.ParquetVersion,
    ) void {
        std.debug.assert(cols.len <= w.arenas.len);
        var cursor = std.atomic.Value(usize).init(0);
        const tasks = w.tasks[0..cols.len];
        for (tasks) |*t| t.* = .{
            .cols = cols,
            .arenas = w.arenas,
            .results = w.results,
            .codec = codec,
            .zstd = zstd,
            .cursor = &cursor,
            .dictionary_bloom = dictionary_bloom,
            .parquet_version = parquet_version,
        };
        if (!spawn_util.runTree(Task, tasks, .{ .stack_size = WORKER_STACK_SIZE }, Task.run)) Task.run(&tasks[0]);
    }

    /// Drop what the last wave's columns hold, keeping the pages for the next wave: what is kept is at most what one
    /// wave held, which the budget admitted.
    fn reset(w: *AggColumnWave) void {
        for (w.arenas) |*a| _ = a.reset(.retain_capacity);
    }

    fn deinit(w: *AggColumnWave) void {
        for (w.arenas) |*a| a.deinit();
    }
};

/// Write an aggregate result — one row, or one per GROUP BY group — as a
/// parquet file with one column-chunk per `OutputCol` per row group
/// (avg-style aggs already split into two cols upstream): one row group of
/// `num_rows` values, or row groups of `answer_rg_rows` for a bigger answer,
/// their chunks encoded together. The output of an aggregate workload is a parquet
/// that downstream consumers (Iceberg/Athena/Polars/another zpq invocation)
/// read like any other file.
///
/// Returns total bytes written. The sink may be an FdSink (CLI) or
/// MultipartSink (Lambda) — same protocol either way.
pub fn writeAggregateRows(
    arena: std.mem.Allocator,
    sink: streaming.Sink,
    output_cols: []const expr_agg.OutputCol,
    num_rows: i64,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    encode: AggEncode,
) !u64 {
    const together = (output_cols.len > 1 or num_rows > answer_rg_rows) and num_rows >= parallel_encode_rows;
    return writeAggregateRowsEncoded(
        arena,
        sink,
        output_cols,
        num_rows,
        answer_rg_rows,
        codec,
        zstd,
        if (together) encode else .{ .ledger = encode.ledger, .dictionary_bloom = encode.dictionary_bloom, .parquet_version = encode.parquet_version },
    );
}

/// `writeAggregateRows` under `encode`, whatever the answer's size, in row groups of `rg_rows` (`answer_rg_rows`); the
/// bytes written are the same for every `encode`.
fn writeAggregateRowsEncoded(
    arena: std.mem.Allocator,
    sink: streaming.Sink,
    output_cols: []const expr_agg.OutputCol,
    num_rows: i64,
    rg_rows: usize,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    encode: AggEncode,
) !u64 {
    var file = try AggregateFile.init(arena, sink);
    defer file.deinit();
    try appendAggregateRows(&file, output_cols, num_rows, rg_rows, codec, zstd, encode);
    return file.finish(output_cols);
}

/// One deterministic aggregate file, fed complete fixed row groups in bounded windows. The caller keeps a window
/// alive through `append` and may then free it; only owned footer metadata survives. The last window may be short.
pub const AggregateFile = struct {
    arena: std.mem.Allocator,
    sink: streaming.Sink,
    off: u64 = 4,
    rows: i64 = 0,
    parquet_version: schema.ParquetVersion = .v2,
    groups: std.ArrayListUnmanaged(schema.RowGroup) = .empty,
    cached_wave: ?AggColumnWave = null,
    cached_width: usize = 0,
    cached_alone: std.heap.ArenaAllocator = .init(std.heap.page_allocator),
    cached_ledger: ?*expr_agg.SharedBudget = null,
    cached_charge: usize = 0,
    cached_unit_charge: usize = 0,

    pub fn init(arena: std.mem.Allocator, sink: streaming.Sink) !AggregateFile {
        try sink.write("PAR1");
        return .{ .arena = arena, .sink = sink };
    }

    pub fn deinit(f: *AggregateFile) void {
        f.releaseWorkspace();
    }

    fn releaseWorkspace(f: *AggregateFile) void {
        if (f.cached_wave) |*w| w.deinit();
        f.cached_wave = null;
        f.cached_alone.deinit();
        f.cached_alone = .init(std.heap.page_allocator);
        if (f.cached_ledger) |l| l.release(f.cached_charge);
        f.cached_ledger = null;
        f.cached_charge = 0;
        f.cached_width = 0;
        f.cached_unit_charge = 0;
    }

    /// Reuse one bounded wave for four row groups of three integer columns. Its full scratch upper bound stays
    /// charged until deinit, including retained pages between windows. A refused reservation keeps the usual waves.
    pub fn reserveIntegerWorkspace(f: *AggregateFile, encode: AggEncode) !void {
        return f.reserveWorkspace(encode, answer_rg_rows * 12 * 3);
    }

    /// A short-key window owns at most 16 payload bytes, one slice and one definition level per key row.
    pub fn reserveByteWorkspace(f: *AggregateFile, encode: AggEncode) !void {
        return f.reserveWorkspace(encode, answer_rg_rows * 36 * 3);
    }

    fn reserveWorkspace(f: *AggregateFile, encode: AggEncode, unit_charge: usize) !void {
        std.debug.assert(f.cached_width == 0 and f.cached_charge == 0);
        const admitted = encode.budget / unit_charge;
        if (admitted == 0) return;
        var width = @max(1, @min(@min(encode.workers, 12), admitted));
        // A trailing single unit uses its own arena beside every retained wave arena.
        if (width > 1 and width == admitted) width -= 1;
        const charge = (width + @intFromBool(width > 1)) * unit_charge;
        if (encode.ledger) |l| _ = l.drawExact(charge) orelse return;
        f.cached_ledger = encode.ledger;
        f.cached_charge = charge;
        f.cached_unit_charge = unit_charge;
        if (width > 1) f.cached_wave = try AggColumnWave.init(f.arena, width);
        f.cached_width = width;
    }

    pub fn append(
        f: *AggregateFile,
        cols: []const expr_agg.OutputCol,
        rows: usize,
        codec: schema.CompressionCodec,
        zstd: compression.ZstdPlan,
        encode: AggEncode,
    ) !void {
        try appendAggregateRows(f, cols, @intCast(rows), answer_rg_rows, codec, zstd, encode);
    }

    /// `cols` supplies names and types only, and may have empty value slices.
    pub fn finish(f: *AggregateFile, cols: []const expr_agg.OutputCol) !u64 {
        return aggregateFooter(f.arena, f.sink, cols, f.rows, f.groups, f.off, f.parquet_version);
    }
};

fn appendAggregateRows(
    file: *AggregateFile,
    output_cols: []const expr_agg.OutputCol,
    num_rows: i64,
    rg_rows: usize,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    options: AggEncode,
) !void {
    const arena = file.arena;
    file.parquet_version = options.parquet_version;
    const sink = file.sink;
    var off = file.off;
    var encode = options;

    // Each row group's columns in order, row group after row group: the units encoded and written below. One row
    // group is the columns themselves.
    const total_rows: usize = @intCast(num_rows);
    const n_rg = @max(1, std.math.divCeil(usize, total_rows, rg_rows) catch unreachable);
    const units: []const expr_agg.OutputCol = if (n_rg == 1) output_cols else blk: {
        const all = try arena.alloc(expr_agg.OutputCol, n_rg * output_cols.len);
        for (0..n_rg) |g| {
            const lo = g * rg_rows;
            const hi = @min(total_rows, lo + rg_rows);
            for (output_cols, 0..) |c, i| all[g * output_cols.len + i] = sliceOutputCol(c, lo, hi);
        }
        break :blk all;
    };
    var new_row_groups: std.ArrayListUnmanaged(schema.RowGroup) = .empty;
    try new_row_groups.ensureTotalCapacity(arena, n_rg);

    var rg_columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    var rg_total: i64 = 0;
    // Columns are encoded in waves, in order: as many at once as the workers and the budget admit, each on a thread. A
    // column the budget cannot hold beside the one before it starts a wave alone, which encodes as the serial loop
    // does. Each wave's columns are then written, and any error returned, in column order, so the file and the error
    // are the serial loop's; their metadata is copied out and the wave's memory freed before the next. A column alone
    // encodes on memory given back once it is written, not on `arena`, which kept every column's until the end.
    if (file.cached_width != 0) {
        for (units) |unit| {
            // New shapes must not silently outgrow a prepaid unit in release builds. Return the reservation and
            // retained arenas first, then use the ordinary per-wave ledger draws for this and later windows.
            if (aggEncodeCharge(unit) > file.cached_unit_charge) {
                file.releaseWorkspace();
                break;
            }
        }
    }
    if (file.cached_width != 0) {
        encode.ledger = null;
        encode.workers = @min(encode.workers, file.cached_width);
    }
    const width = @min(encode.workers, units.len);
    var wave: ?AggColumnWave = file.cached_wave orelse
        if (width > 1) try AggColumnWave.init(arena, width) else @as(?AggColumnWave, null);
    defer if (file.cached_width == 0) if (wave) |*w| w.deinit();
    var own_alone = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer own_alone.deinit();
    const alone = if (file.cached_width != 0) &file.cached_alone else &own_alone;
    var drawn: u64 = 0;
    defer if (encode.ledger) |l| l.release(drawn);
    var wave_lo: usize = 0;
    var wave_hi: usize = 0;
    for (units, 0..) |out_col, i| {
        if (i == wave_hi) {
            if (encode.ledger) |l| l.release(drawn);
            drawn = 0;
            wave_lo = i;
            wave_hi = i + 1;
            if (wave) |*w| {
                w.reset();
                var charged: u64 = aggEncodeCharge(out_col);
                while (wave_hi < units.len and wave_hi - i < width) : (wave_hi += 1) {
                    const c = aggEncodeCharge(units[wave_hi]);
                    if (charged +| c > encode.budget) break;
                    charged += c;
                }
                // Drawn whole, or narrowed a column at a time to what the ledger takes.
                if (encode.ledger) |l| while (wave_hi - i > 1) : (wave_hi -= 1) {
                    if (l.drawExact(charged) != null) {
                        drawn = charged;
                        break;
                    }
                    charged -= aggEncodeCharge(units[wave_hi - 1]);
                };
                if (wave_hi - i > 1) w.run(units[i..wave_hi], codec, zstd, encode.dictionary_bloom, encode.parquet_version);
            }
            if (wave_hi - i == 1) if (encode.ledger) |l| {
                drawn = l.drawExact(aggEncodeCharge(out_col)) orelse return error.ExceededMemoryBudget;
            };
        }
        const leaf_elem = aggLeafElem(out_col);
        const in_wave = wave != null and wave_hi - wave_lo > 1;
        defer if (!in_wave) {
            _ = alone.reset(if (file.cached_width != 0) .retain_capacity else .free_all);
        };
        const enc = if (in_wave)
            try wave.?.results[i - wave_lo]
        else
            try encodeAggOutputCol(alone.allocator(), out_col, &leaf_elem, codec, zstd, encode.dictionary_bloom, encode.parquet_version);
        const col_start: i64 = @intCast(off);
        var em = try ownColumnMeta(arena, enc.meta);
        em.data_page_offset += col_start;
        if (em.dictionary_page_offset) |dpo| em.dictionary_page_offset = dpo + col_start;
        // The encoder orders min/max signed; an unsigned column's type-defined order is not, so drop the bounds.
        if (schema.isUnsignedInt(leaf_elem)) if (em.statistics) |*st| {
            st.min = null;
            st.max = null;
            st.min_value = null;
            st.max_value = null;
        };
        try enc.writeTo(sink);
        off += enc.byteLen();
        rg_total += em.total_uncompressed_size;
        try rg_columns.append(arena, .{
            .file_path = null,
            .file_offset = col_start,
            .meta_data = em,
            .bloom_out = try arena.dupe(u8, enc.bloom),
        });
        if (rg_columns.items.len == output_cols.len) {
            const g = new_row_groups.items.len;
            const g_rows = if (n_rg == 1) num_rows else @as(i64, @intCast(@min(total_rows - g * rg_rows, rg_rows)));
            new_row_groups.appendAssumeCapacity(.{
                .columns = rg_columns,
                .total_byte_size = rg_total,
                .num_rows = g_rows,
            });
            rg_columns = .empty;
            rg_total = 0;
        }
    }

    // No columns, no units: the one row group is still written, empty.
    if (new_row_groups.items.len == 0)
        new_row_groups.appendAssumeCapacity(.{
            .columns = rg_columns,
            .total_byte_size = rg_total,
            .num_rows = num_rows,
        });
    try file.groups.appendSlice(arena, new_row_groups.items);
    file.rows += num_rows;
    file.off = off;
}

fn aggregateFooter(
    arena: std.mem.Allocator,
    sink: streaming.Sink,
    output_cols: []const expr_agg.OutputCol,
    num_rows: i64,
    new_row_groups: std.ArrayListUnmanaged(schema.RowGroup),
    start: u64,
    parquet_version: schema.ParquetVersion,
) !u64 {
    var out: streaming.SinkOut = .{ .sink = sink, .offset = @intCast(start) };
    try fastpath.placeFilters(&out, new_row_groups.items);
    var off: u64 = out.offset;
    var new_schema: std.ArrayListUnmanaged(schema.SchemaElement) = .empty;
    try new_schema.append(arena, .{
        .type = null,
        .type_length = null,
        .repetition_type = null,
        .name = "schema",
        .num_children = @intCast(output_cols.len),
        .converted_type = null,
        .logical_type = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    for (output_cols) |out_col| try new_schema.append(arena, aggLeafElem(out_col));

    const new_meta: schema.FileMetaData = .{
        .version = parquet_version.footer(),
        .schema = new_schema,
        .num_rows = num_rows,
        .created_by = schema.ZPQ_CREATED_BY,
        .row_groups = new_row_groups,
        .column_orders = try schema.FileMetaData.outputColumnOrders(arena, output_cols.len, null, &.{}),
    };

    var w: thrift.Writer = .init(arena);
    defer w.deinit();
    try new_meta.write(&w);
    const footer_bytes = w.bytes();
    try sink.write(footer_bytes);
    off += footer_bytes.len;

    var len_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_bytes, std.math.cast(u32, footer_bytes.len) orelse return error.TooLarge, .little);
    try sink.write(&len_bytes);
    off += len_bytes.len;
    try sink.write("PAR1");
    off += 4;

    return off;
}


fn aggAnswerCols(a: std.mem.Allocator, n: usize, bad_row: ?usize) ![]expr_agg.OutputCol {
    const uniq = try a.alloc(i64, n);
    const reps = try a.alloc(i64, n);
    const fl = try a.alloc(f64, n);
    const strs = try a.alloc([]const u8, n);
    const decs = try a.alloc(i128, n);
    const defs = try a.alloc(u32, n);
    for (0..n) |i| {
        uniq[i] = @bitCast(@as(u64, i) *% 0x9E3779B97F4A7C15);
        reps[i] = @intCast(i % 37);
        fl[i] = @as(f64, @floatFromInt(i % 1000)) * 0.25;
        strs[i] = try std.fmt.allocPrint(a, "v{d}", .{i % 500});
        decs[i] = @intCast(i % 9000);
        defs[i] = @intFromBool(i % 7 != 0);
    }
    if (bad_row) |r| decs[r] = 1 << 40;
    const cols = try a.alloc(expr_agg.OutputCol, 5);
    cols[0] = .{
        .name = "u",
        .col = .{ .i64 = .{ .values = uniq, .max_def = 0 } },
        .parquet_type = .INT64,
        .nullable = false,
    };
    cols[1] = .{
        .name = "r",
        .col = .{ .i64 = .{ .values = reps, .max_def = 0 } },
        .parquet_type = .INT64,
        .nullable = false,
    };
    cols[2] = .{
        .name = "d",
        .col = .{ .i128 = .{ .values = decs, .max_def = 0 } },
        .parquet_type = .INT32,
        .nullable = false,
        .dec = .{ .precision = 9, .scale = 2 },
    };
    cols[3] = .{
        .name = "f",
        .col = .{ .f64 = .{ .values = fl, .def_levels = defs, .max_def = 1, .has_nulls = true } },
        .parquet_type = .DOUBLE,
        .nullable = true,
    };
    cols[4] = .{
        .name = "s",
        .col = .{ .string = .{ .values = strs, .def_levels = defs, .max_def = 1, .has_nulls = true } },
        .parquet_type = .BYTE_ARRAY,
        .nullable = true,
    };
    return cols;
}

/// Keeps what is written; fails the write numbered `fail_at` (0 is the magic), if set.
const AnswerCapture = struct {
    a: std.mem.Allocator,
    bytes: std.ArrayList(u8) = .empty,
    writes: usize = 0,
    fail_at: ?usize = null,

    fn sink(c: *AnswerCapture) streaming.Sink {
        return .{ .ctx = @ptrCast(c), .write_fn = write };
    }

    fn write(ctx: *anyopaque, b: []const u8) anyerror!void {
        const c: *AnswerCapture = @ptrCast(@alignCast(ctx));
        defer c.writes += 1;
        if (c.fail_at == c.writes) return error.TestSinkFailed;
        try c.bytes.appendSlice(c.a, b);
    }
};

test "writeAggregateRows: columns encoded on threads write the serial bytes, whatever spawns fail" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 70_000;
    const cols = try aggAnswerCols(a, n, null);
    for ([_]schema.CompressionCodec{ .SNAPPY, .ZSTD, .UNCOMPRESSED }) |codec| {
        var serial: AnswerCapture = .{ .a = a };
        const want = try writeAggregateRowsEncoded(a, serial.sink(), cols, n, answer_rg_rows, codec, .{}, .{});
        try std.testing.expectEqual(@as(u64, serial.bytes.items.len), want);
        // Every column in one wave; two columns' charge a wave, so waves of two and lone columns; and spawns failing.
        const two = aggEncodeCharge(cols[0]) + aggEncodeCharge(cols[1]);
        const Run = struct { encode: AggEncode, fail_after: ?usize = null };
        for ([_]Run{
            .{ .encode = .{ .workers = 8, .budget = std.math.maxInt(u64) } },
            .{ .encode = .{ .workers = 3, .budget = two } },
            .{ .encode = .{ .workers = 8, .budget = std.math.maxInt(u64) }, .fail_after = 0 },
            .{ .encode = .{ .workers = 8, .budget = std.math.maxInt(u64) }, .fail_after = 2 },
        }) |run| {
            if (run.fail_after) |k| spawn_util.injectFailureAfter(k);
            defer spawn_util.resetFailure();
            var par: AnswerCapture = .{ .a = a };
            const got = try writeAggregateRowsEncoded(a, par.sink(), cols, n, answer_rg_rows, codec, .{}, run.encode);
            try std.testing.expectEqual(want, got);
            try std.testing.expectEqualSlices(u8, serial.bytes.items, par.bytes.items);
        }
    }
}

test "writeAggregateRows: on threads, the serial error and the bytes written before it" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 1000;
    // Column 2 cannot encode; write 1 (column 0) fails, or write 2 (column 1), or none does.
    const cols = try aggAnswerCols(a, n, 600);
    for ([_]?usize{ null, 1, 2 }) |fail_at| {
        var serial: AnswerCapture = .{ .a = a, .fail_at = fail_at };
        const want = writeAggregateRowsEncoded(a, serial.sink(), cols, n, answer_rg_rows, .SNAPPY, .{}, .{});
        var par: AnswerCapture = .{ .a = a, .fail_at = fail_at };
        const all: AggEncode = .{ .workers = 8, .budget = std.math.maxInt(u64) };
        const got = writeAggregateRowsEncoded(a, par.sink(), cols, n, answer_rg_rows, .SNAPPY, .{}, all);
        try std.testing.expectError(if (fail_at == null) error.UnsupportedType else error.TestSinkFailed, want);
        try std.testing.expectEqual(want, got);
        try std.testing.expectEqualSlices(u8, serial.bytes.items, par.bytes.items);
    }
}

fn writeAnswerOnThreads(a: std.mem.Allocator) !void {
    var arena_state = std.heap.ArenaAllocator.init(a);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const cols = try aggAnswerCols(arena, 300, null);
    var out: AnswerCapture = .{ .a = arena };
    _ = try writeAggregateRowsEncoded(
        arena,
        out.sink(),
        cols,
        300,
        answer_rg_rows,
        .SNAPPY,
        .{},
        .{ .workers = 4, .budget = 1 << 40 },
    );
}

test "writeAggregateRows: one worker, or a budget no two columns fit, starts no thread" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 70_000;
    const cols = try aggAnswerCols(a, n, null);
    for ([_]AggEncode{ .{ .workers = 1, .budget = std.math.maxInt(u64) }, .{ .workers = 16, .budget = 0 } }) |encode| {
        // A spawn would fail and be counted.
        spawn_util.injectFailureAfter(0);
        defer spawn_util.resetFailure();
        var out: AnswerCapture = .{ .a = a };
        _ = try writeAggregateRows(a, out.sink(), cols, n, .SNAPPY, .{}, encode);
        try std.testing.expectEqual(@as(usize, 0), spawn_util.failuresInjected());
    }
}

test "writeAggregateRows: a column encoded alone still draws from the ledger" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const cols = try aggAnswerCols(a, 1000, null);
    // One column, and few rows: no waves, but the column is the ledger's to refuse.
    var ledger: expr_agg.SharedBudget = .{ .limit = 1 };
    var out: AnswerCapture = .{ .a = a };
    const encode: AggEncode = .{ .workers = 16, .budget = std.math.maxInt(u64), .ledger = &ledger };
    const one = writeAggregateRows(a, out.sink(), cols[0..1], 1000, .SNAPPY, .{}, encode);
    try std.testing.expectError(error.ExceededMemoryBudget, one);
    ledger.limit = 1 << 30;
    _ = try writeAggregateRows(a, out.sink(), cols, 1000, .SNAPPY, .{}, encode);
    try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}

test "writeAggregateRows: on threads, an allocation failure frees everything" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, writeAnswerOnThreads, .{});
}

/// The bytes between a written answer's leading magic and its footer: its column chunks, in order.
fn answerChunks(a: std.mem.Allocator, file: []const u8) ![]const u8 {
    const meta = try answerFooter(a, file);
    const footer_len = std.mem.readInt(u32, file[file.len - 8 ..][0..4], .little);
    var end = file.len - 8 - footer_len;
    for (meta.row_groups.items) |rg| for (rg.columns.items) |c| {
        if (c.meta_data) |cm| {
            if (cm.bloom_filter_offset) |off| end = @min(end, @as(usize, @intCast(off)));
        }
    };
    return file[4..end];
}

fn answerFooter(a: std.mem.Allocator, file: []const u8) !schema.FileMetaData {
    const footer_len = std.mem.readInt(u32, file[file.len - 8 ..][0..4], .little);
    var reader = thrift.Reader.init(file[file.len - 8 - footer_len .. file.len - 8]);
    return schema.FileMetaData.read(a, &reader);
}

test "writeAggregateRows: big answers use fixed row groups with serial chunks at every worker count" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 1000;
    const rg = 300;
    const cols = try aggAnswerCols(a, n, null);
    var serial: AnswerCapture = .{ .a = a };
    _ = try writeAggregateRowsEncoded(a, serial.sink(), cols, n, rg, .SNAPPY, .{}, .{});
    const meta = try answerFooter(a, serial.bytes.items);
    try std.testing.expectEqual(@as(i64, n), meta.num_rows);
    try std.testing.expectEqual(@as(usize, 4), meta.row_groups.items.len);
    for (meta.row_groups.items, [_]i64{ 300, 300, 300, 100 }) |g, rows| {
        try std.testing.expectEqual(rows, g.num_rows);
        try std.testing.expectEqual(cols.len, g.columns.items.len);
    }
    // Row group g's chunks are what its rows encode to as an answer of their own, in order.
    var want: std.ArrayList(u8) = .empty;
    for (0..4) |g| {
        const lo = g * rg;
        const hi = @min(n, lo + rg);
        const part = try a.alloc(expr_agg.OutputCol, cols.len);
        for (cols, part) |c, *p| p.* = sliceOutputCol(c, lo, hi);
        var alone: AnswerCapture = .{ .a = a };
        _ = try writeAggregateRowsEncoded(a, alone.sink(), part, @intCast(hi - lo), rg, .SNAPPY, .{}, .{});
        try want.appendSlice(a, try answerChunks(a, alone.bytes.items));
    }
    try std.testing.expectEqualSlices(u8, want.items, try answerChunks(a, serial.bytes.items));
    // The same file whatever the workers, the budget's waves, or failing spawns.
    const two = aggEncodeCharge(sliceOutputCol(cols[0], 0, rg)) + aggEncodeCharge(sliceOutputCol(cols[1], 0, rg));
    const Run = struct { encode: AggEncode, fail_after: ?usize = null };
    for ([_]Run{
        .{ .encode = .{ .workers = 16, .budget = std.math.maxInt(u64) } },
        .{ .encode = .{ .workers = 3, .budget = two } },
        .{ .encode = .{ .workers = 7, .budget = std.math.maxInt(u64) }, .fail_after = 1 },
    }) |run| {
        if (run.fail_after) |k| spawn_util.injectFailureAfter(k);
        defer spawn_util.resetFailure();
        var par: AnswerCapture = .{ .a = a };
        _ = try writeAggregateRowsEncoded(a, par.sink(), cols, n, rg, .SNAPPY, .{}, run.encode);
        try std.testing.expectEqualSlices(u8, serial.bytes.items, par.bytes.items);
    }
}

test "writeAggregateRows: an answer of a row group's rows or fewer is one row group" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const cols = try aggAnswerCols(a, 300, null);
    for ([_]usize{ 0, 1, 300 }) |n| {
        const part = try a.alloc(expr_agg.OutputCol, cols.len);
        for (cols, part) |c, *p| p.* = sliceOutputCol(c, 0, n);
        var out: AnswerCapture = .{ .a = a };
        _ = try writeAggregateRowsEncoded(a, out.sink(), part, @intCast(n), 300, .SNAPPY, .{}, .{});
        const meta = try answerFooter(a, out.bytes.items);
        try std.testing.expectEqual(@as(usize, 1), meta.row_groups.items.len);
        try std.testing.expectEqual(@as(i64, @intCast(n)), meta.row_groups.items[0].num_rows);
    }
}

fn writeRowGroupsOnThreads(a: std.mem.Allocator) !void {
    var arena_state = std.heap.ArenaAllocator.init(a);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const cols = try aggAnswerCols(arena, 300, null);
    var out: AnswerCapture = .{ .a = arena };
    _ = try writeAggregateRowsEncoded(
        arena,
        out.sink(),
        cols,
        300,
        128,
        .SNAPPY,
        .{},
        .{ .workers = 4, .budget = 1 << 40 },
    );
}

test "writeAggregateRows: row groups on threads, an allocation failure frees everything" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, writeRowGroupsOnThreads, .{});
}

fn writeAnswerWindows(a: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const cols = try aggAnswerCols(alloc, 1000, null);
    var whole: AnswerCapture = .{ .a = alloc };
    _ = try writeAggregateRowsEncoded(alloc, whole.sink(), cols, 1000, 300, .SNAPPY, .{}, .{});
    var windows: AnswerCapture = .{ .a = alloc };
    var file = try AggregateFile.init(alloc, windows.sink());
    defer file.deinit();
    for ([_][2]usize{ .{ 0, 600 }, .{ 600, 1000 } }) |range| {
        const part = try alloc.alloc(expr_agg.OutputCol, cols.len);
        for (cols, part) |c, *p| p.* = sliceOutputCol(c, range[0], range[1]);
        try appendAggregateRows(
            &file,
            part,
            @intCast(range[1] - range[0]),
            300,
            .SNAPPY,
            .{},
            .{ .workers = 4, .budget = 1 << 30 },
        );
    }
    _ = try file.finish(cols);
    try std.testing.expectEqualSlices(u8, whole.bytes.items, windows.bytes.items);
}

test "writeAggregateRows: bounded windows write the whole answer's bytes and free every failed allocation" {
    try writeAnswerWindows(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, writeAnswerWindows, .{});
}

fn cachedIntegerWindows(a: std.mem.Allocator) !void {
    var budget: expr_agg.SharedBudget = .{ .limit = 1 << 30 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const all = try aggAnswerCols(alloc, 1201, null);
    var cols = [_]expr_agg.OutputCol{ all[0], all[1], all[0] };
    cols[2].name = "s";
    var whole: AnswerCapture = .{ .a = alloc };
    _ = try writeAggregateRowsEncoded(alloc, whole.sink(), &cols, 1201, 300, .SNAPPY, .{}, .{});
    var windows: AnswerCapture = .{ .a = alloc };
    var file = try AggregateFile.init(alloc, windows.sink());
    defer file.deinit();
    const encode: AggEncode = .{ .workers = 11, .budget = 128 << 20, .ledger = &budget };
    try file.reserveIntegerWorkspace(encode);
    try std.testing.expectEqual(@as(usize, 12 * answer_rg_rows * 12 * 3), file.cached_charge);
    try std.testing.expectEqual(file.cached_charge, budget.used.load(.monotonic));
    for ([_][2]usize{ .{ 0, 1200 }, .{ 1200, 1201 } }) |range| {
        var part: [3]expr_agg.OutputCol = undefined;
        for (cols, &part) |c, *p| p.* = sliceOutputCol(c, range[0], range[1]);
        try appendAggregateRows(&file, &part, @intCast(range[1] - range[0]), 300, .SNAPPY, .{}, encode);
    }
    _ = try file.finish(&cols);
    try std.testing.expectEqualSlices(u8, whole.bytes.items, windows.bytes.items);
    // A later wider shape returns every retained arena before drawing its ordinary per-wave workspace.
    file.releaseWorkspace();
    var fallback: AnswerCapture = .{ .a = alloc };
    var wider = try AggregateFile.init(alloc, fallback.sink());
    defer wider.deinit();
    try wider.reserveIntegerWorkspace(encode);
    wider.cached_unit_charge = 1;
    try appendAggregateRows(&wider, &cols, 1201, 300, .SNAPPY, .{}, encode);
    _ = try wider.finish(&cols);
    try std.testing.expectEqualSlices(u8, whole.bytes.items, fallback.bytes.items);
    try std.testing.expectEqual(@as(usize, 0), wider.cached_charge);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "writeAggregateRows: retained integer encode scratch stays charged through windows and unwinds on failure" {
    try cachedIntegerWindows(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, cachedIntegerWindows, .{});
}


test "cloneColumnMeta keeps every statistics field, the bounds' exactness among them" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const st: schema.Statistics = .{
        .max = "b",
        .min = "a",
        .null_count = 3,
        .distinct_count = 7,
        .max_value = "y",
        .min_value = "x",
        .is_max_value_exact = false,
        .is_min_value_exact = true,
        .nan_count = 2,
    };
    const cm: schema.ColumnMetaData = .{
        .type = .BYTE_ARRAY,
        .encodings = .empty,
        .path_in_schema = .empty,
        .codec = .UNCOMPRESSED,
        .num_values = 10,
        .total_uncompressed_size = 0,
        .total_compressed_size = 0,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = st,
    };
    const got = (try cloneColumnMeta(a, cm)).statistics.?;
    inline for (@typeInfo(schema.Statistics).@"struct".field_names) |name| {
        const want = @field(st, name);
        const have = @field(got, name);
        if (@TypeOf(want) == ?[]const u8) {
            try std.testing.expectEqualStrings(want.?, have.?);
        } else try std.testing.expectEqual(want, have);
    }
}

test "plainBitsPrefix: every range's bits are plainBits' over the same rows, whatever the lanes and NULLs" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 257;
    var agg: OutputAggregator = .{
        .arena = a,
        .cols = try a.alloc(OutputAggregator.ColumnBuf, 4),
        .schema_elems = try a.alloc(schema.SchemaElement, 4),
        .paths = &.{},
        .num_rows = n,
        .captured = true,
    };
    const base: schema.SchemaElement = .{
        .type = .INT64,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "c",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    for (agg.schema_elems) |*e| e.* = base;
    agg.schema_elems[1].type = .BYTE_ARRAY;
    agg.schema_elems[2].type = .BYTE_ARRAY;
    agg.schema_elems[3].type = .BOOLEAN;
    agg.cols[0] = .{ .i64 = .{} };
    agg.cols[1] = .{ .string = .{} };
    agg.cols[2] = .{ .string = .{ .coded = true, .dict = &.{ "x", "yyyy", "" }, .max_def = 1 } };
    agg.cols[3] = .{ .boolean = .{ .max_def = 1 } };
    for (0..n) |r| {
        try agg.cols[0].i64.values.append(a, @intCast(r));
        try agg.cols[1].string.values.append(a, "abcdefghij"[0 .. r % 11]);
        try agg.cols[2].string.codes.append(a, @intCast(r % 3));
        try agg.cols[2].string.def_levels.append(a, @intFromBool(r % 4 != 0));
        try agg.cols[3].boolean.values.append(a, r % 2 == 0);
        try agg.cols[3].boolean.def_levels.append(a, @intFromBool(r % 5 != 0));
    }
    const prefix = try plainBitsPrefix(a, &agg);
    try testing.expectEqual(@as(usize, n + 1), prefix.len);
    for ([_][2]usize{ .{ 0, n }, .{ 0, 0 }, .{ 3, 4 }, .{ 17, 200 }, .{ 100, n } }) |lohi| {
        var want: u64 = 0;
        for (agg.cols, agg.schema_elems) |*c, *e| want += plainBits(c, e, lohi[0], lohi[1]);
        try testing.expectEqual(want, prefix[lohi[1]] - prefix[lohi[0]]);
    }
}


test "writeAggregateRows preserves the automatic Bloom policy for small and parallel answers" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    for ([_]usize{ 1000, 70000 }) |n| {
        const cols = try aggAnswerCols(a, n, null);
        for ([_]bool{ false, true }) |enabled| for ([_]usize{ 1, 3 }) |workers| {
            var capture: AnswerCapture = .{ .a = a };
            _ = try writeAggregateRows(a, capture.sink(), cols, @intCast(n), .SNAPPY, .{}, .{
                .dictionary_bloom = enabled,
                .workers = workers,
                .budget = std.math.maxInt(u64),
            });
            const meta = try answerFooter(a, capture.bytes.items);
            const repeated = meta.row_groups.items[0].columns.items[1].meta_data.?;
            try std.testing.expect(repeated.dictionary_page_offset != null);
            try std.testing.expectEqual(enabled, repeated.bloom_filter_offset != null);
        };
    }
}


test "grouped key dictionaries keep later-key codes with and without dense lanes" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var ints: [256]i32 = undefined;
    var names: [256][]const u8 = undefined;
    for (&ints, &names, 0..) |*k, *v, i| {
        k.* = @intCast(i % 2);
        v.* = try std.fmt.allocPrint(a, "dictionary-value-{d}", .{i % 64});
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "a", .parquet_type = .INT32, .nullable = false, .col = .{ .i32 = .{ .values = &ints } } },
        .{ .name = "b", .parquet_type = .BYTE_ARRAY, .nullable = false, .col = .{ .string = .{ .values = &names } } },
    };
    var capture: AnswerCapture = .{ .a = a };
    _ = try writeAggregateRows(a, capture.sink(), &cols, ints.len, .SNAPPY, .{}, .{});
    const meta = try metadata.open(a, capture.bytes.items);
    const rg = &meta.row_groups.items[0];
    const src: RGSrc = .{ .bytes = capture.bytes.items, .byte_origin = 0 };
    var lanes: agg_dense.Lanes = undefined;
    var target: AggTarget = .{ .grouped = .{
        .calls = &.{},
        .keys = &.{
            .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } },
            .{ .col_ref = .{ .col_idx = 1, .physical_type = .BYTE_ARRAY, .expr_type = .str } },
        },
        .dense = &lanes,
        .table = undefined,
        .pos_base = 0,
    } };
    try testing.expect(target.codedKey(0));
    try testing.expect(target.codedKey(1));
    try testing.expect(!target.codedKey(2));
    target.grouped.dense = null;
    try testing.expect(target.codedKey(0));
    try testing.expect(target.codedKey(1));
    const in: AggRowGroup = .{
        .rg = rg,
        .meta = &meta,
        .src = src,
        .filter = null,
        .read = &.{ true, true },
        .consumed = &.{ true, true },
    };
    try testing.expect(cacheDictionary(in, 1) != null);
    try testing.expect(!streamByteArray(in, target, 1));
    try testing.expectEqual(std.math.maxInt(usize), streamWindowRows(in, target, ints.len));
    const cm = rg.columns.items[1].meta_data.?;
    const start: usize = @intCast(cm.dictionary_page_offset.?);
    const chunk = capture.bytes.items[start..][0..@intCast(cm.total_compressed_size)];
    for ([_]bool{ false, true }) |force| {
        const options: DecodeOptions = .{ .encoded = true, .any_dictionary = force and target.codedKey(1) };
        var scratch = DecodeScratch.init(testing.allocator);
        defer scratch.deinit();
        const col = (try decodeAggColumnCoded(a, chunk, cm, &meta, options, &scratch)).?;
        try testing.expectEqual(force, col.codes() != null);
        if (!force) continue;
        for ([_]usize{ 1, 17, 64, 256 }) |step| {
            var reader = (try SliceReader.init(a, testing.allocator, rg, &meta, src, 1, options)).?;
            defer reader.deinit();
            var slice = std.heap.ArenaAllocator.init(testing.allocator);
            defer slice.deinit();
            var done: usize = 0;
            while (done < names.len) {
                _ = slice.reset(.free_all);
                const got = try reader.next(slice.allocator(), step);
                const codes = got.codes().?;
                try testing.expect(got.string.dict.len > names.len / 8);
                for (codes, 0..) |code, i|
                    try testing.expectEqualStrings(names[done + i], got.string.dict[code]);
                done += codes.len;
            }
        }
    }
    // A slice spanning the dictionary/PLAIN boundary must gather its prefix without rereading pages.
    var mixed: std.ArrayList(u8) = .empty;
    var coded_body: std.ArrayList(u8) = .empty;
    try coded_body.append(a, 1);
    try coded_body.appendSlice(a, try @import("../parquet/encoding/hybrid_rle.zig").encode(a, &.{ 0, 1, 0, 1 }, 1));
    const dict_values = [_]i32{ 10, 20 };
    const plain_values = [_]i32{ 30, 40, 50, 60 };
    const bodies = [_][]const u8{ std.mem.asBytes(&dict_values), coded_body.items, std.mem.asBytes(&plain_values) };
    for (bodies, 0..) |body, i| {
        var w = thrift.Writer.init(a);
        const h: schema.PageHeader = .{
            .type = if (i == 0) .DICTIONARY_PAGE else .DATA_PAGE,
            .uncompressed_page_size = @intCast(body.len),
            .compressed_page_size = @intCast(body.len),
            .crc = null,
            .data_page_header_v2 = null,
            .data_page_header = if (i == 0) null else .{
                .num_values = 4,
                .encoding = if (i == 1) .RLE_DICTIONARY else .PLAIN,
                .definition_level_encoding = .RLE,
                .repetition_level_encoding = .RLE,
            },
            .dictionary_page_header = if (i == 0) .{ .num_values = 2, .encoding = .PLAIN, .is_sorted = null } else null,
        };
        try h.write(&w);
        try mixed.appendSlice(a, w.bytes());
        try mixed.appendSlice(a, body);
    }
    var mixed_rg = rg.*;
    mixed_rg.columns.items = try a.dupe(schema.ColumnChunk, rg.columns.items);
    const mixed_cm = &mixed_rg.columns.items[0].meta_data.?;
    mixed_cm.codec = .UNCOMPRESSED;
    mixed_cm.dictionary_page_offset = 0;
    mixed_cm.data_page_offset = 0;
    mixed_cm.total_compressed_size = @intCast(mixed.items.len);
    mixed_cm.num_values = 8;
    const expected = [_]i32{ 10, 20, 10, 20, 30, 40, 50, 60 };
    for ([_]usize{ 1, 3, 5, 8 }) |step| {
        var r = (try SliceReader.init(
            a,
            testing.allocator,
            &mixed_rg,
            &meta,
            .{ .bytes = mixed.items, .byte_origin = 0 },
            0,
            .{ .encoded = true, .any_dictionary = true },
        )).?;
        defer r.deinit();
        var done: usize = 0;
        while (done < expected.len) {
            const c = (try r.next(a, step)).i32;
            const n = if (c.codes) |codes| codes.len else c.values.len;
            for (0..n) |i| try testing.expectEqual(
                expected[done + i],
                if (c.codes) |codes| c.dict[codes[i]] else c.values[i],
            );
            done += n;
        }
    }
    const Trial = struct {
        fn run(gpa: std.mem.Allocator, bytes: []const u8, m: *const schema.FileMetaData) !void {
            var arena = std.heap.ArenaAllocator.init(gpa);
            defer arena.deinit();
            const alloc = arena.allocator();
            var reader = (try SliceReader.init(
                alloc,
                gpa,
                &m.row_groups.items[0],
                m,
                .{ .bytes = bytes, .byte_origin = 0 },
                1,
                .{ .encoded = true, .any_dictionary = true },
            )).?;
            defer reader.deinit();
            var slice = std.heap.ArenaAllocator.init(gpa);
            defer slice.deinit();
            while (reader.left != 0) {
                _ = slice.reset(.free_all);
                const c = try reader.next(slice.allocator(), 17);
                try std.testing.expect(c.codes() != null);
            }
        }
    };
    try testing.checkAllAllocationFailures(testing.allocator, Trial.run, .{ capture.bytes.items, &meta });
}

