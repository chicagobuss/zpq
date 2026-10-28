const page_index = @import("../core/writer/page_index.zig");
const filter_selection = @import("../core/filter/selection.zig");
const Diag = @import("../core/diag.zig").Diag;
const compression = @import("../core/parquet/compression.zig");
const std = @import("std");
const nowMonoNs = @import("../clock.zig").monoNs;
const profile = @import("../profile.zig");
const schema = @import("../core/schema.zig");
const consumer = @import("../core/consumer.zig");
const core_memory = @import("../core/memory.zig");
const huge_pages = @import("../core/huge_pages.zig");
const system = @import("../core/system.zig");
const filter_ast = @import("../core/filter/ast.zig");
const filter_eval = @import("../core/filter/eval.zig");
const scan = @import("../core/scan.zig");
const encoder = @import("../core/writer/encoder.zig");
const streaming = @import("../core/writer/streaming.zig");
const fastpath = @import("../core/writer/fastpath.zig");
const metadata = @import("../core/parquet/metadata.zig");
const types = @import("types.zig");
const RowGroupTarget = types.RowGroupTarget;
const Context = types.Context;
const QueryArgs = types.QueryArgs;

const BufSink = struct {
    buf: *std.ArrayListUnmanaged(u8),
    gpa: std.mem.Allocator,
    fn writeFn(ctx: *anyopaque, bytes: []const u8) anyerror!void {
        const self: *BufSink = @ptrCast(@alignCast(ctx));
        try self.buf.appendSlice(self.gpa, bytes);
    }
};

/// Address space one re-encoded row group reserves for its working memory (committed only as touched; past it, the
/// general allocator).
pub const ENCODE_REGION_RESERVE: usize = 4 << 30;

/// A re-encode producer's working memory, two huge-page regions kept from one row group to the next so their pages
/// are faulted once per write rather than once per row group: the projected rows in `rows`, and in `work` first the
/// decoded columns, then, once those are dead, the encoded chunks. A row group's working set is tens to hundreds of
/// MiB of short-lived buffers, which on the general allocator cost a page fault per 4 KiB on every thread.
pub const WorkRegions = struct {
    rows: huge_pages.Region,
    work: huge_pages.Region,
    /// Claimed by one producer at a time; there are as many as producers can run at once.
    busy: std.atomic.Value(bool) = .init(false),

    pub fn init(gpa: std.mem.Allocator) WorkRegions {
        return .{
            .rows = .init(gpa, ENCODE_REGION_RESERVE),
            .work = .init(gpa, ENCODE_REGION_RESERVE),
        };
    }

    pub fn deinit(self: *WorkRegions) void {
        self.rows.deinit();
        self.work.deinit();
    }

    fn claim(all: []WorkRegions) ?*WorkRegions {
        for (all) |*r| if (r.busy.cmpxchgStrong(false, true, .acquire, .monotonic) == null) return r;
        return null;
    }
};

pub const RGJob = struct {
    bytes: []const u8, // source file bytes containing this RG
    meta: *const schema.FileMetaData, // owning file's metadata (shared, read-only)
    rg: *const schema.RowGroup, // the source row group
    /// Stats prove every row passes the filter (Decision.always_match) and the
    /// query is byte-copy-eligible → copy the chunk verbatim instead of
    /// decode→filter→encode. Set only when safe (see bytecopy_ok in runWrite).
    passthrough: bool = false,
    /// Input file and row group (counted across files), for profile spans.
    file_index: usize = 0,
    rg_index: usize = 0,
    /// The input's fetch gate (`scan.Input.gate`), and this row group's index in its file.
    gate: ?scan.RowGroupGate = null,
    file_rg: usize = 0,

    /// Wait until the row group's bytes are in, when they are still being fetched.
    fn arrive(self: RGJob) !void {
        if (self.gate) |g| try g.wait(g.ctx, self.file_rg);
    }

    /// The row group's bytes are no longer needed: the fetch window may have them back.
    fn leave(self: RGJob) void {
        if (self.gate) |g| g.done(g.ctx, self.file_rg, 1);
    }
};

/// Row group `rg` of `in` is skipped: give its fetch window share back as if it had been read.
pub fn skipRowGroup(in: scan.Input, rg: usize) void {
    if (in.gate) |g| g.done(g.ctx, rg, 1);
}

const EncodedRG = struct {
    bytes: []u8 = &.{}, // a byte-copied RG's bytes (gpa-owned; writer frees after sink.write)
    rg: ?schema.RowGroup = null, // its footer meta, offsets relative to 0 (meta_arena-owned)
    /// A re-encoded row group's kept rows, decoded, for the writer to cut into output row groups.
    piece: ?*Piece = null,
    /// Too big for a producer to decode at once: the writer decodes it a slice at a time in its turn
    /// (`projectRGSliced`), and releases its input after.
    deferred: bool = false,
    surviving: i64 = 0,
    err: ?anyerror = null,
    /// Where `err` was raised when it is a decode error (`consumer.decodeFailure`).
    failure: ?consumer.DecodeSite = null,
    /// The producer's decode/eval/encode time for this row group, summed by the writer.
    timings: consumer.Timings = .{},
};

/// Rows kept from an input row group (or a slice of one), decoded and projected, waiting for the writer to cut them
/// into output row groups (`Assembler`). Where pieces start and end never shows in the output: only the rows do.
pub const Piece = struct {
    agg: consumer.OutputAggregator,
    /// Holds `agg`'s rows, on the write's ledger; freed with the last reference: its maker's, which the assembler
    /// takes over and drops once every row is sealed, and each encode job's whose row group points into it.
    arena: std.heap.ArenaAllocator,
    refs: std.atomic.Value(u32) = .init(1),
    /// Its rows' running PLAIN bits (`consumer.plainBitsPrefix`), made by its producer, for the writer's cuts.
    bits_prefix: []const u64 = &.{},
    /// Where it goes back when freed, its memory kept for the next piece; null: freed for good.
    pool: ?*PiecePool = null,

    fn create(gpa: std.mem.Allocator, rows_alloc: std.mem.Allocator) !*Piece {
        const p = try gpa.create(Piece);
        p.* = .{ .agg = undefined, .arena = .init(rows_alloc) };
        return p;
    }

    fn ref(p: *Piece) void {
        _ = p.refs.fetchAdd(1, .monotonic);
    }

    fn unref(p: *Piece, gpa: std.mem.Allocator) void {
        if (p.refs.fetchSub(1, .acq_rel) != 1) return;
        if (p.pool) |pool| pool.give(p) else p.destroy(gpa);
    }

    fn destroy(p: *Piece, gpa: std.mem.Allocator) void {
        p.arena.deinit();
        gpa.destroy(p);
    }
};

/// Freed pieces with their arenas reset but their memory kept, for the producers' next pieces: a piece's rows are
/// megabytes the general allocator maps fresh and unmaps whole, a fault per page and, unmapped while encode threads
/// run, a TLB shootdown on every one of them. Idle pieces keep at most 32 MiB together, still on the ledger.
const PiecePool = struct {
    io: std.Io = undefined,
    mutex: std.Io.Mutex = .init,
    free: std.ArrayListUnmanaged(*Piece) = .empty,
    gpa: std.mem.Allocator,
    rows_alloc: std.mem.Allocator = undefined,
    ledger: ?*core_memory.SharedBudget = null,
    retained: usize = 0,
    limit: usize = 32 << 20,

    /// A piece with no rows and a reference count of one, its arena's memory reused where the pool has one.
    fn take(pool: *PiecePool) !*Piece {
        pool.mutex.lockUncancelable(pool.io);
        const reused = pool.free.pop();
        if (reused) |p| pool.retained -= p.arena.queryCapacity();
        pool.mutex.unlock(pool.io);
        const p = reused orelse try Piece.create(pool.gpa, pool.rows_alloc);
        p.refs.store(1, .monotonic);
        p.pool = pool;
        return p;
    }

    fn give(pool: *PiecePool, p: *Piece) void {
        if (pool.ledger) |l| if (l.tight()) return p.destroy(pool.gpa);
        if (p.arena.queryCapacity() > pool.limit) return p.destroy(pool.gpa);
        _ = p.arena.reset(.retain_capacity);
        pool.mutex.lockUncancelable(pool.io);
        defer pool.mutex.unlock(pool.io);
        const bytes = p.arena.queryCapacity();
        if (bytes > pool.limit - pool.retained) return p.destroy(pool.gpa);
        pool.free.append(pool.gpa, p) catch return p.destroy(pool.gpa);
        pool.retained += bytes;
    }

    pub fn deinit(pool: *PiecePool) void {
        for (pool.free.items) |p| p.destroy(pool.gpa);
        pool.free.deinit(pool.gpa);
    }
};

pub const Slot = struct {
    enc: EncodedRG = .{},
    /// Private per-producer arena for this RG's (small, long-lived) footer
    /// metadata — no cross-thread sharing, so no locking. Freed after the
    /// footer is written. The bulk bytes live on gpa and are freed early.
    meta_arena: std.heap.ArenaAllocator,
    done: std.atomic.Value(bool) = .init(false),
};

/// Decode, filter and project one row group into a piece: its kept rows, decoded, on `rows_alloc`. The working memory
/// is `regions`, emptied again before returning.
fn projectRGToPiece(
    gpa: std.mem.Allocator,
    pool: *PiecePool,
    regions: *WorkRegions,
    job: RGJob,
    filter: ?filter_ast.Filter,
    fetch_arr: []const bool,
    output_specs: []const consumer.OutputCol,
    decode_options: consumer.DecodeOptions,
    prof: ?*profile.Thread,
    /// The producer's share, what the filter's columns take decoded, and what every fetched column takes whole.
    fit: struct { share: usize, early: usize, rows: usize },
) !EncodedRG {
    // `rows` holds what lives for the whole row group (the filter's columns and selection); each output column is
    // decoded on `work`, rewound before the next, so the working set is one column rather than the row group.
    const work_region = &regions.work;
    defer work_region.rewind();
    defer regions.rows.rewind();
    var scratch = std.heap.ArenaAllocator.init(regions.rows.allocator());
    defer scratch.deinit();
    var timings: consumer.Timings = .{ .prof = prof };
    const piece = try pool.take();
    errdefer piece.unref(gpa);
    piece.agg = try consumer.initOutputAggregator(piece.arena.allocator(), job.meta, output_specs);
    piece.agg.keep_codes = true;
    var projected: consumer.ProjectedRG = undefined;
    try projected.init(
        &piece.agg,
        scratch.allocator(),
        job.rg,
        job.meta,
        .{ .bytes = job.bytes, .byte_origin = 0 },
        filter,
        fetch_arr,
        output_specs,
        &timings,
        decode_options,
    );
    defer projected.deinit();
    // The kept rows' share of the row group, beside the filter's columns, is what the piece will hold.
    const num_rows: usize = @intCast(@max(1, job.rg.num_rows));
    const kept = @as(u128, fit.rows) * projected.surviving / num_rows;
    if (fit.early + kept > fit.share) return error.PieceTooLarge;
    if (projected.surviving > 0) for (0..output_specs.len) |i| {
        work_region.rewind();
        var col_arena = std.heap.ArenaAllocator.init(work_region.allocator());
        defer col_arena.deinit();
        const span = profile.Span.begin(prof);
        try projected.projectColumn(&piece.agg, i, col_arena.allocator(), &timings);
        span.end(.project, profile.args(.{ projected.surviving, 1 }));
    };
    piece.agg.num_rows = projected.surviving;
    piece.bits_prefix = try consumer.plainBitsPrefix(piece.arena.allocator(), &piece.agg);
    timings.prof = null;
    return .{ .piece = piece, .surviving = @intCast(projected.surviving), .timings = timings };
}

/// Byte-copy a fully-passing row group's kept columns into a buffer (no decode
/// or re-encode) — the filter fast path. Offsets are relative to the buffer; the writer places it.
fn copyRGToBuffer(
    gpa: std.mem.Allocator,
    meta_alloc: std.mem.Allocator,
    job: RGJob,
    kept_set: ?[]const bool,
    prof: ?*profile.Thread,
    bloom_leaves: []const ?f64,
    dictionary_bloom: bool,
) !EncodedRG {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(gpa);
    var bufsink = BufSink{ .buf = &buf, .gpa = gpa };
    const sink: streaming.Sink = .{ .ctx = @ptrCast(&bufsink), .write_fn = BufSink.writeFn };
    var local_off: u64 = 0;
    var timings: consumer.Timings = .{ .prof = prof };
    const src: consumer.RGSrc = .{ .bytes = job.bytes, .byte_origin = 0 };
    const out = try consumer.copyRG(meta_alloc, job.rg, src, kept_set, sink, &local_off, &timings, false, null, .{
        .fpp = bloom_leaves,
        .dictionary_bloom = dictionary_bloom,
        .meta = job.meta,
        .scratch = gpa,
    });
    const owned = try buf.toOwnedSlice(gpa);
    timings.prof = null;
    return .{ .bytes = owned, .rg = out.rg, .surviving = out.surviving_rows, .timings = timings };
}


/// The writer's cutter: takes pieces in input order, holds their rows until an output row group's worth is in, then
/// seals them as an `EncodeJob`. Jobs encode beside one another and beside the producers' reads; the writer writes
/// them in the order they were sealed, so the file is the one encoding them one at a time would write. A piece may
/// end one row group and start the next.
const Assembler = struct {
    w: *WinCtx,
    target: RowGroupTarget,
    ranges: std.ArrayListUnmanaged(Range) = .empty,
    rows: usize = 0,
    bits: u64 = 0,
    /// Pieces whose rows are all in `ranges`: the assembler's own reference to each goes once they are sealed.
    done: std.ArrayListUnmanaged(*Piece) = .empty,
    /// The piece being fed, until all its rows are in `ranges` (it is then in `done`).
    current: ?*Piece = null,
    /// Sealed row groups not yet written, oldest first.
    pending: std.ArrayListUnmanaged(*EncodeJob) = .empty,
    pending_bytes: u64 = 0,

    const Range = struct { piece: *Piece, lo: usize, hi: usize };

    fn init(w: *WinCtx) Assembler {
        return .{ .w = w, .target = w.rg_target };
    }

    /// Wait for every job still encoding (their pieces and memory are theirs until then), and free what is left.
    fn deinit(a: *Assembler) void {
        const w = a.w;
        for (a.pending.items) |job| {
            while (!job.done.load(.acquire)) w.completions.waitUncancelable(w.io);
            job.destroy();
        }
        for (a.done.items) |p| p.unref(w.gpa);
        if (a.current) |p| p.unref(w.gpa);
        a.ranges.deinit(w.gpa);
        a.done.deinit(w.gpa);
        a.pending.deinit(w.gpa);
    }

    fn bitsOf(p: *const Piece, lo: usize, hi: usize) u64 {
        return p.bits_prefix[hi] - p.bits_prefix[lo];
    }

    /// Take `p`'s rows, sealing every row group they complete. The assembler owns `p` from here.
    fn feed(a: *Assembler, p: *Piece) !void {
        std.debug.assert(a.current == null);
        const n = p.agg.num_rows;
        if (n == 0) {
            p.unref(a.w.gpa);
            return;
        }
        a.current = p;
        const target_bits = a.target.bytes *| 8;
        var lo: usize = 0;
        while (lo < n) {
            var hi = @min(n, lo + (a.target.rows - a.rows));
            var bits = bitsOf(p, lo, hi);
            const room = target_bits -| a.bits;
            if (bits >= room) {
                // The row that reaches the byte target closes the row group: the first `hi` past `lo` whose rows
                // reach it, by search over the running totals.
                const goal = p.bits_prefix[lo] + room;
                var lo_s = lo + 1;
                var hi_s = hi;
                while (lo_s < hi_s) {
                    const mid = lo_s + (hi_s - lo_s) / 2;
                    if (p.bits_prefix[mid] >= goal) hi_s = mid else lo_s = mid + 1;
                }
                hi = lo_s;
                bits = bitsOf(p, lo, hi);
            }
            try a.ranges.append(a.w.gpa, .{ .piece = p, .lo = lo, .hi = hi });
            if (hi == n) {
                try a.done.append(a.w.gpa, p);
                a.current = null;
            }
            a.rows += hi - lo;
            a.bits += bits;
            lo = hi;
            if (a.rows >= a.target.rows or a.bits >= target_bits) try a.seal();
        }
        try a.writeReady(false);
    }

    /// Seal the rows held as one output row group and submit its ready column/page work to the executor.
    fn seal(a: *Assembler) !void {
        if (a.rows == 0) return;
        const w = a.w;
        const job = try EncodeJob.create(w, a.rows);
        var started = false;
        defer if (!started) job.destroy();
        // Only the ranges here: the job joins them into columns on its own thread.
        job.ranges = try w.gpa.dupe(Range, a.ranges.items);
        // The job holds every piece its rows point into until it has encoded them.
        var pieces: std.ArrayListUnmanaged(*Piece) = .empty;
        defer pieces.deinit(w.gpa);
        for (a.ranges.items) |r| {
            if (std.mem.indexOfScalar(*Piece, pieces.items, r.piece) == null) try pieces.append(w.gpa, r.piece);
        }
        job.pieces = try w.gpa.dupe(*Piece, pieces.items);
        for (job.pieces) |p| p.ref();
        job.charge = a.bits / 8 *| 3;
        try a.pending.append(w.gpa, job);
        a.pending_bytes += job.charge;
        started = true;
        for (a.done.items) |p| p.unref(w.gpa);
        a.done.clearRetainingCapacity();
        a.ranges.clearRetainingCapacity();
        a.rows = 0;
        a.bits = 0;
        a.start(job) catch |e| {
            // Admission may fail while placing an older job, before this one has a runnable task.
            const unstarted = a.pending.pop().?;
            std.debug.assert(unstarted == job);
            a.pending_bytes -= job.charge;
            started = false;
            return e;
        };
    }

    /// Admission and execution share one bound. Placement releases the reservation; completed jobs still count.
    fn start(a: *Assembler, job: *EncodeJob) !void {
        const w = a.w;
        const executor = w.executor.?;
        while (a.pending.items.len > 1) {
            const room = if (w.memory) |m| m.budget.available() >= job.charge else true;
            if (executor.admits(a.pending.items.len, a.pending_bytes) and room) break;
            try a.writeFront();
        }
        job.reserve();
        job.permit = .encoder;
        job.trim_scratch = w.writer_turns > 0;
        executor.submit(.{ .begin = job });
    }

    /// Write the oldest pending row group once it has encoded.
    fn writeFront(a: *Assembler) !void {
        const w = a.w;
        const job = a.pending.orderedRemove(0);
        a.pending_bytes -= job.charge;
        defer job.destroy();
        while (!job.done.load(.acquire)) w.completions.waitUncancelable(w.io);
        try job.place();
    }

    /// Write the pending row groups that have encoded, in order; all of them, waiting, when `all`.
    fn writeReady(a: *Assembler, all: bool) !void {
        while (a.pending.items.len > 0) {
            if (!all and !a.pending.items[0].done.load(.acquire)) return;
            try a.writeFront();
        }
    }

    /// A small tail must not pin a wide producer's entire piece beside the next producer. Drain its encodings,
    /// then copy only carried rows; this never seals the output group or changes its row/byte boundary.
    fn compactCarry(a: *Assembler) !void {
        if (a.rows == 0) return;
        try a.writeReady(true);
        const w = a.w;
        const p = try Piece.create(w.gpa, w.pieceAlloc());
        errdefer p.unref(w.gpa);
        const pa = p.arena.allocator();
        p.agg = .{
            .arena = pa,
            .cols = try pa.alloc(consumer.OutputAggregator.ColumnBuf, w.out_elems.len),
            .schema_elems = try pa.dupe(schema.SchemaElement, w.out_elems),
            .paths = try pa.dupe([]const []const u8, w.out_paths),
            .num_rows = a.rows,
            .captured = true,
        };
        for (p.agg.cols, a.ranges.items[0].piece.agg.cols) |*dst, src| dst.* = switch (src) {
            inline else => |_, tag| @unionInit(consumer.OutputAggregator.ColumnBuf, @tagName(tag), .{}),
        };
        var scratch = std.heap.ArenaAllocator.init(w.gpa);
        defer scratch.deinit();
        for (a.ranges.items) |range| {
            const n = range.hi - range.lo;
            var sel = try filter_selection.SelectionVector.init(w.gpa, n);
            defer sel.deinit();
            for (p.agg.cols, range.piece.agg.cols) |*dst, src| {
                try consumer.appendRows(
                    dst,
                    consumer.bufRows(src, range.lo, range.hi),
                    &sel,
                    n,
                    pa,
                    scratch.allocator(),
                );
                _ = scratch.reset(.retain_capacity);
            }
        }
        p.bits_prefix = try consumer.plainBitsPrefix(pa, &p.agg);
        std.debug.assert(p.bits_prefix[a.rows] == a.bits);
        for (a.done.items) |old| old.unref(w.gpa);
        a.done.clearRetainingCapacity();
        a.done.appendAssumeCapacity(p);
        a.ranges.clearRetainingCapacity();
        a.ranges.appendAssumeCapacity(.{ .piece = p, .lo = 0, .hi = a.rows });
    }

    fn shouldCompact(a: *const Assembler) bool {
        if (a.rows == 0 or a.done.items.len == 0) return false;
        var held: u64 = 0;
        for (a.done.items) |p| held +|= p.arena.queryCapacity();
        const live = a.bits / 8;
        if (a.w.memory) |m| if (m.budget.available() < live *| 3) return false;
        return held > @max(64 << 20, live *| 2);
    }

    /// Seal what is held and write every row group: before a byte copy, and at the end.
    fn flush(a: *Assembler) !void {
        try a.seal();
        try a.writeReady(true);
    }
};

/// The encode estimate the hashagg answer writer charges a column by: three times its values and levels (a dictionary
/// attempt's table and indices, the pages, their compressed copies).
fn encodeCharge(col: filter_eval.Batch.Column) u64 {
    const bytes: u64 = switch (col) {
        inline else => |c| blk: {
            const T = @typeInfo(@TypeOf(c.values)).pointer.child;
            var n: u64 = c.rowCount() * @sizeOf(T);
            if (T == []const u8) {
                if (c.codes) |k| {
                    for (k) |code| n += c.dict[code].len;
                } else for (c.values) |v| n += v.len;
            }
            break :blk n + if (c.def_levels) |d| d.len * @sizeOf(u32) else 0;
        },
    };
    return bytes *| 3;
}

/// An output row group the assembler sealed: its column/page tasks share the executor, then it is placed by
/// the writer in its turn (`place`). Its memory is its own arena and the pieces it holds references to.
const EncodeJob = struct {
    w: *WinCtx,
    /// Its encoding's memory, made when it runs: on a free `WorkRegions` (the producers' warm pages, shared with them;
    /// the job holds it until the writer has placed it), else fresh pages.
    arena: ?std.heap.ArenaAllocator = null,
    regions: ?*WorkRegions = null,
    region_budgets: [2]?*core_memory.SharedBudget = .{ null, null },
    rows: usize,
    /// The pieces' rows it is made of, in order (`Assembler.Range`), joined a wave of columns at a time as it runs.
    ranges: []const Assembler.Range = &.{},
    pieces: []const *Piece = &.{},
    encs: []encoder.EncodedColumn = &.{},
    /// What its encoding is estimated to hold (three times its PLAIN bytes), for the writer's admission.
    charge: u64 = 0,
    reserved: usize = 0,
    err: ?anyerror = null,
    err_col: ?usize = null,
    encode_ns: u64 = 0,
    permit: enum { none, encoder } = .none,
    trim_scratch: bool = false,
    done: std.atomic.Value(bool) = .init(false),
    state: ?*EncodeState = null,
    started_ns: i64 = 0,

    fn create(w: *WinCtx, rows: usize) !*EncodeJob {
        const job = try w.gpa.create(EncodeJob);
        job.* = .{ .w = w, .rows = rows };
        return job;
    }

    fn reserve(job: *EncodeJob) void {
        const m = job.w.memory orelse return;
        job.reserved = @intCast(job.charge);
        // A row group alone retains the existing lone-column allowance. The reservation now lives through
        // placement, so completed chunks cannot disappear from the ledger between encode waves and sink writes.
        if (m.budget.drawExact(job.reserved) == null) m.budget.force(job.reserved);
    }

    /// Its memory, claimed by its first ready task: the arena its chunks stay on (a region's `rows`), and what each
    /// wave works on (its `work`, rewound after every wave), or fresh pages for both.
    fn claimMemory(job: *EncodeJob) struct { keep: std.mem.Allocator, work: ?*huge_pages.Region } {
        const w = job.w;
        if (WorkRegions.claim(w.regions)) |r| {
            job.regions = r;
            job.region_budgets = .{ r.rows.budget, r.work.budget };
            r.rows.budget = null;
            r.work.budget = null;
            job.arena = .init(r.rows.allocator());
            return .{ .keep = job.arena.?.allocator(), .work = &r.work };
        }
        job.arena = .init(w.encode_pages.allocator());
        return .{ .keep = job.arena.?.allocator(), .work = null };
    }

    /// Free a job the writer is done with, or one never started (its piece references then go here).
    fn destroy(job: *EncodeJob) void {
        const w = job.w;
        if (!job.done.load(.acquire)) for (job.pieces) |p| p.unref(w.gpa);
        if (job.state) |state| state.deinit();
        if (job.arena) |*a| a.deinit();
        if (w.memory) |m| m.budget.release(job.reserved);
        if (job.regions) |r| {
            r.rows.rewind();
            r.work.rewind();
            r.rows.budget = job.region_budgets[0];
            r.work.budget = job.region_budgets[1];
            // Retained pages outlive the job's reservation. Charge their actual high water or return them now.
            if (job.permit == .none or job.trim_scratch) {
                // Sliced input needs the room for its next batch, rather than warm encoder scratch.
                r.rows.trim();
                r.work.trim();
            } else {
                if (!r.rows.admit(r.rows.touched)) r.rows.trim();
                if (!r.work.admit(r.work.touched)) r.work.trim();
            }
            if (w.memory) |m| if (m.budget.tight()) {
                r.rows.trim();
                r.work.trim();
            };
            r.busy.store(false, .release);
        }
        w.gpa.free(job.ranges);
        w.gpa.free(job.pieces);
        w.gpa.destroy(job);
    }

    fn begin(job: *EncodeJob) void {
        job.started_ns = nowMonoNs();
        EncodeState.start(job) catch |e| job.finish(e);
    }

    fn finish(job: *EncodeJob, err: ?anyerror) void {
        const w = job.w;
        job.err = err;
        if (job.state) |state| state.deinit();
        for (job.pieces) |p| p.unref(w.gpa);
        const t1 = nowMonoNs();
        job.encode_ns = @intCast(t1 - job.started_ns);
        profile.record(
            profile.threadOf(w.profiler, .encode),
            .encode,
            job.started_ns,
            t1,
            profile.args(.{ w.out_elems.len, job.rows }),
        );
        _ = w.executor.?.active.fetchSub(1, .acq_rel);
        // Publication may destroy the job immediately after this store.
        job.done.store(true, .release);
        w.completions.post(w.io);
    }

    /// Write the encoded chunks in column order at the sink's offset, their page indexes recorded for the end of the
    /// file, and the row group into the footer.
    fn place(job: *EncodeJob) !void {
        const w = job.w;
        w.core.encode_ns += job.encode_ns;
        if (job.err) |e| {
            if (e == error.DecimalValueExceedsPrecision) if (job.err_col) |ci| {
                Diag.setColumn(w.diag, w.out_elems[ci].name);
            };
            return e;
        }
        const prof = profile.threadOf(w.profiler, .writer);
        const rg_index = w.new_row_groups.items.len;
        var columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
        try columns.ensureTotalCapacity(w.arena, job.encs.len);
        var total: i64 = 0;
        for (job.encs, 0..) |enc, ci| {
            const t_sink = nowMonoNs();
            const placed = try consumer.placeChunk(w.arena, w.sink, &w.out_offset, enc);
            const t_sunk = nowMonoNs();
            w.sink_ns += @intCast(t_sunk - t_sink);
            profile.record(prof, .sink_write, t_sink, t_sunk, profile.args(.{enc.bytes.len}));
            profile.record(prof, .encode_chunk, t_sink, t_sunk, profile.args(.{
                ci, w.codec, enc.meta.total_uncompressed_size, enc.meta.total_compressed_size, enc.meta.num_values,
            }));
            columns.appendAssumeCapacity(placed.chunk);
            total += enc.meta.total_uncompressed_size;
            try w.page_indexes.add(w.arena, rg_index, ci, placed.pages, &w.out_elems[ci]);
        }
        try w.new_row_groups.append(w.arena, .{
            .columns = columns,
            .total_byte_size = total,
            .num_rows = @intCast(job.rows),
        });
        w.rows_kept += @intCast(job.rows);
        w.rgs_kept += 1;
    }
};

/// A bounded queue shared by every output row group. Workers only run ready work: the final task in a phase
/// releases the next phase, so no worker blocks waiting for another worker (including at -j1).
const EncodeExecutor = struct {
    w: *WinCtx,
    queue: std.Io.Queue(Work),
    buffer: []Work,
    group: std.Io.Group = .init,
    workers: usize,
    byte_limit: u64,
    active: std.atomic.Value(usize) = .init(0),
    slots: std.Io.Semaphore,
    granted: std.atomic.Value(usize),

    const Work = union(enum) { begin: *EncodeJob, phase: *EncodeTask };

    fn init(e: *EncodeExecutor, w: *WinCtx) !void {
        const workers: usize = @min(64, @max(1, w.cpu_budget));
        const buffer = try w.gpa.alloc(Work, workers * (workers + 1));
        e.* = .{
            .w = w,
            .queue = .init(buffer),
            .buffer = buffer,
            .workers = workers,
            .slots = .{ .permits = if (w.encode_during_decode) 1 else workers },
            .granted = .init(if (w.encode_during_decode) 1 else workers),
            .byte_limit = if (w.memory) |m| @min(m.budget.limit / 2, workers *| (64 << 20)) else workers *| (64 << 20),
        };
        errdefer e.deinit();
        for (0..workers) |_| try e.group.concurrent(w.io, worker, .{e});
    }

    fn allowThreads(e: *EncodeExecutor, count: usize) void {
        const before = e.granted.fetchMax(count, .acq_rel);
        if (before >= count) return;
        for (before..count) |_| e.slots.post(e.w.io);
    }

    fn allowParallel(e: *EncodeExecutor) void {
        e.allowThreads(e.workers);
    }

    /// Called while every producer turn is held, so producer completion cannot race the temporary boost.
    fn restrainThreads(e: *EncodeExecutor, count: usize) void {
        const before = e.granted.swap(count, .acq_rel);
        for (count..@max(count, before)) |_| e.slots.waitUncancelable(e.w.io);
    }

    fn deinit(e: *EncodeExecutor) void {
        e.allowParallel();
        e.queue.close(e.w.io);
        e.group.await(e.w.io) catch {};
        e.w.gpa.free(e.buffer);
    }

    fn admits(e: *const EncodeExecutor, count: usize, bytes: u64) bool {
        // Wide groups already supply many columns/pages; fewer simultaneous groups avoid retaining idle scratch.
        const groups = if (e.w.out_elems.len > e.workers) @max(1, e.workers / 4) else e.workers;
        return count <= groups and bytes <= e.byte_limit;
    }

    fn submit(e: *EncodeExecutor, work: Work) void {
        if (work == .begin) _ = e.active.fetchAdd(1, .acq_rel);
        e.queue.putOneUncancelable(e.w.io, work) catch unreachable;
    }

    fn worker(e: *EncodeExecutor) void {
        while (true) {
            const work = e.queue.getOneUncancelable(e.w.io) catch return;
            e.slots.waitUncancelable(e.w.io);
            defer e.slots.post(e.w.io);
            switch (work) {
                .begin => |job| job.begin(),
                .phase => |task| {
                    const state = task.shared.state;
                    if (task.run()) {
                        e.submit(.{ .phase = task });
                        continue;
                    }
                    if (state.remaining.fetchSub(1, .acq_rel) == 1) {
                        state.advance() catch |err| state.job.finish(err);
                    }
                },
            }
        }
    }
};

/// Each task owns its allocations until the whole wave has completed. Plans/pages may be consumed by another
/// worker, but their arena is never mutated by two tasks. Codec contexts belong to the persistent worker threads.
const EncodeTask = struct {
    shared: *Shared,
    arena: std.heap.ArenaAllocator,
    err: ?anyerror = null,

    const Shared = struct {
        state: *EncodeState,
        inputs: []const encoder.ColumnInput,
        plans: []encoder.Plan,
        pages: [][]encoder.BuiltPage,
        work: []const [2]u32 = &.{},
        phase: enum { plan, pages } = .plan,
        cursor: std.atomic.Value(usize) = .init(0),
        failed: std.atomic.Value(bool) = .init(false),
    };

    /// One column or four pages per queue turn keeps a fast cohort from consuming a whole row group while its other
    /// tasks wait behind unrelated groups. The fixed cohort bounds queued descriptors and arena ownership.
    fn run(t: *EncodeTask) bool {
        const shared = t.shared;
        const a = t.arena.allocator();
        const quantum: usize = if (shared.phase == .plan) 1 else 4;
        for (0..quantum) |_| {
            if (shared.failed.load(.monotonic)) return false;
            const i = shared.cursor.fetchAdd(1, .monotonic);
            switch (shared.phase) {
                .plan => {
                    if (i >= shared.inputs.len) return false;
                    shared.plans[i] = encoder.plan(a, shared.inputs[i]) catch |err| {
                        t.fail(err);
                        return false;
                    };
                },
                .pages => {
                    if (i >= shared.work.len) return false;
                    const cp = shared.work[i];
                    shared.pages[cp[0]][cp[1]] = shared.plans[cp[0]].page(a, cp[1]) catch |err| {
                        t.fail(err);
                        return false;
                    };
                },
            }
        }
        return true;
    }

    fn fail(t: *EncodeTask, err: anyerror) void {
        t.err = err;
        t.shared.failed.store(true, .monotonic);
    }
};

const EncodeState = struct {
    job: *EncodeJob,
    keep: std.mem.Allocator,
    work_alloc: std.mem.Allocator,
    work_region: ?*huge_pages.Region,
    charges: []u64,
    inputs: []encoder.ColumnInput,
    parts: []consumer.BufRange,
    tasks: []EncodeTask,
    shared: EncodeTask.Shared = undefined,
    remaining: std.atomic.Value(usize) = .init(0),
    wave: ?std.heap.ArenaAllocator = null,
    lo: usize = 0,
    hi: usize = 0,

    fn start(job: *EncodeJob) !void {
        const w = job.w;
        const memory = job.claimMemory();
        const ja = memory.keep;
        const state = try ja.create(EncodeState);
        state.* = .{
            .job = job,
            .keep = ja,
            .work_alloc = if (memory.work) |r| r.allocator() else w.encode_pages.allocator(),
            .work_region = memory.work,
            .charges = &.{},
            .inputs = &.{},
            .parts = &.{},
            .tasks = &.{},
        };
        job.state = state;
        const n_cols = w.out_elems.len;
        state.charges = try ja.alloc(u64, n_cols);
        for (state.charges, 0..) |*charge, i| {
            charge.* = 0;
            for (job.ranges) |r| charge.* +|= encodeCharge(consumer.bufRows(r.piece.agg.cols[i], r.lo, r.hi));
        }
        state.inputs = try ja.alloc(encoder.ColumnInput, n_cols);
        for (state.inputs, 0..) |*in, i| in.* = .{
            .values = undefined,
            .schema_elem = &w.out_elems[i],
            .path_in_schema = w.out_paths[i],
            .codec = w.codec,
            .zstd = w.zstd,
            .bloom_fpp = if (i < w.bloom_outputs.len) w.bloom_outputs[i] else null,
            .dictionary_bloom = w.dictionary_bloom,
        };
        job.encs = try ja.alloc(encoder.EncodedColumn, n_cols);
        state.parts = try ja.alloc(consumer.BufRange, job.ranges.len);
        state.tasks = try ja.alloc(EncodeTask, w.executor.?.workers);
        for (state.tasks) |*task| task.* = .{ .shared = &state.shared, .arena = .init(w.encode_pages.allocator()) };
        try state.nextWave();
    }

    fn deinit(state: *EncodeState) void {
        for (state.tasks) |*task| task.arena.deinit();
        state.tasks = &.{};
        if (state.wave) |*wave| wave.deinit();
        state.wave = null;
    }

    fn dispatch(state: *EncodeState, count: usize) void {
        const executor = state.job.w.executor.?;
        const active = @max(1, executor.active.load(.acquire));
        const helpers = std.math.divCeil(usize, executor.workers, active) catch 1;
        const tasks = state.tasks[0..@max(1, @min(helpers, count))];
        state.remaining.store(tasks.len, .monotonic);
        // The queue holds every admitted group's cohort plus its initial task. No task waits for queue room
        // needed by its own descendants, even when every worker completes a different group's phase together.
        for (tasks) |*task| executor.submit(.{ .phase = task });
    }

    fn nextWave(state: *EncodeState) !void {
        const job = state.job;
        const w = job.w;
        if (state.lo == state.inputs.len) {
            job.finish(null);
            return;
        }
        var hi = state.lo + 1;
        var charge = state.charges[state.lo];
        while (hi < state.inputs.len and hi - state.lo < 4) : (hi += 1) {
            if (w.memory) |m| if (charge +| state.charges[hi] > m.budget.available() +| job.reserved) break;
            charge +|= state.charges[hi];
        }
        state.hi = hi;
        state.wave = .init(state.work_alloc);
        const wa = state.wave.?.allocator();
        for (state.inputs[state.lo..hi], state.lo..) |*in, i| {
            for (job.ranges, state.parts) |r, *part| part.* = .{
                .buf = &r.piece.agg.cols[i],
                .lo = r.lo,
                .hi = r.hi,
            };
            in.values = try consumer.joinRows(wa, state.parts);
        }
        const inputs = state.inputs[state.lo..hi];
        state.shared = .{
            .state = state,
            .inputs = inputs,
            .plans = try wa.alloc(encoder.Plan, inputs.len),
            .pages = try wa.alloc([]encoder.BuiltPage, inputs.len),
        };
        state.dispatch(inputs.len);
    }

    fn advance(state: *EncodeState) !void {
        for (state.tasks) |task| if (task.err) |err| return err;
        const shared = &state.shared;
        const wa = state.wave.?.allocator();
        if (shared.phase == .plan) {
            var count: usize = 0;
            for (shared.plans, shared.pages) |*plan, *pages| {
                pages.* = try wa.alloc(encoder.BuiltPage, plan.pageCount());
                count += plan.pageCount();
            }
            const work = try wa.alloc([2]u32, count);
            var k: usize = 0;
            for (shared.plans, 0..) |*plan, ci| for (0..plan.pageCount()) |pi| {
                work[k] = .{ @intCast(ci), @intCast(pi) };
                k += 1;
            };
            shared.work = work;
            shared.phase = .pages;
            shared.cursor.store(0, .monotonic);
            state.dispatch(count);
            return;
        }
        for (shared.plans, shared.pages, state.lo..) |*plan, pages, ci| {
            state.job.encs[ci] = plan.finish(state.keep, pages) catch |err| {
                state.job.err_col = ci;
                return err;
            };
        }
        for (state.tasks) |*task| _ = task.arena.reset(.retain_capacity);
        state.wave.?.deinit();
        state.wave = null;
        if (state.work_region) |region| region.rewind();
        state.lo = state.hi;
        try state.nextWave();
    }
};

fn valueRows(cols: []const filter_eval.Batch.Column) usize {
    if (cols.len == 0) return 0;
    return switch (cols[0]) {
        inline else => |c| c.rowCount(),
    };
}

pub const WinCtx = struct {
    const writer = winWriter;
    const producer = winProducer;

    gpa: std.mem.Allocator,
    io: std.Io = undefined, // set in runWindowed once the runtime exists
    worker_io: ?std.Io = null,
    jobs: []const RGJob,
    filter: ?filter_ast.Filter,
    fetch_arr: []const bool,
    output_specs: []const consumer.OutputCol,
    kept_set: ?[]const bool, // for byte-copy passthrough jobs
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan = .{},
    decode_options: consumer.DecodeOptions,
    /// Where output row groups are cut (`RowGroupTarget`).
    rg_target: RowGroupTarget = .{},
    /// Per leaf: read by the filter or a computed output, so decoded before the filter runs.
    early_arr: []const bool = &.{},
    /// The output columns' schema leaves and paths, as every piece's aggregator has them.
    out_elems: []const schema.SchemaElement = &.{},
    out_paths: []const []const []const u8 = &.{},
    /// Producer turns the writer holds while it slices a row group alone.
    writer_turns: usize = 0,
    executor: ?*EncodeExecutor = null,
    encode_during_decode: bool = false,
    producer_left: std.atomic.Value(usize) = .init(0),
    /// Freed producers' pieces, kept for reuse.
    piece_pool: PiecePool = .{ .gpa = undefined },
    /// The Bloom filters asked for: per output column for re-encoded row groups, per source leaf for copied ones.
    bloom_outputs: []const ?f64 = &.{},
    bloom_leaves: []const ?f64 = &.{},
    dictionary_bloom: bool = true,
    slots: []Slot,
    /// One per producer that can run at once and one per encode job that can be pending, and one more: whoever starts
    /// work claims a free one, so an encode job lands on the pages a finished producer leaves warm.
    regions: []WorkRegions,
    window: *std.Io.Semaphore, // the in-flight output budget, in `win_unit` permits (`jobUnits`)
    win_unit: u64,
    win_units: usize,
    completions: *std.Io.Semaphore, // "a slot finished, re-check" signal
    cpu: std.Io.Semaphore = .{}, // producers running at once; sized in runWindowed
    producers: usize = 0, // `cpu`'s permits
    /// The CPU budget (`-j`, or the host's), which the producers' permits are at most.
    cpu_budget: usize = 1,

    // Writer-owned (single writer → no races); read back after the fan-out.
    sink: streaming.Sink,
    out_offset: u64,
    new_row_groups: *std.ArrayListUnmanaged(schema.RowGroup),
    arena: std.mem.Allocator, // writer-only append target
    /// Re-encoded chunks' page indexes, written after the row groups.
    page_indexes: *page_index.Deferred,
    rows_kept: i64 = 0,
    rgs_kept: usize = 0,
    sink_ns: u64 = 0,
    /// Producers' decode/eval/encode time, summed as their row groups drain.
    core: consumer.Timings = .{},
    first_err: ?anyerror = null,
    first_failure: ?consumer.DecodeSite = null,

    inflight: std.atomic.Value(usize) = .init(0),
    peak_bytes: std.atomic.Value(usize) = .init(0),
    profiler: ?*profile.Profiler = null,
    /// What one producer's decoded rows may take; a row group past it is decoded in slices (`projectRGSliced`).
    decode_share: usize = std.math.maxInt(usize),
    diag: ?*Diag = null,
    /// The query's ledger, through which pieces and byte-copied row groups in flight are allocated, and which the
    /// producers' regions draw from (`runWriteOver`). Null: `gpa`, uncharged.
    memory: ?*core_memory.Charged = null,
    /// Under a ledger, what the writer's slices are decoded on, straight from the OS: the write's ledger, with
    /// `memory.lean_decode` more room (the ceiling's slack) while the writer slices alone, its producers idle; or,
    /// over fetched inputs, where producers go on beside it, a ledger of `lean_decode` bytes of its own (`lean`).
    slicing: ?*core_memory.Charged = null,
    lean: ?*core_memory.Charged = null,
    /// The one of them the row group being sliced draws from.
    slicing_now: ?*core_memory.Charged = null,
    /// Page buffers for sliced row groups, kept for the write and released with it.
    slice_pool: ?*consumer.SliceScratch = null,
    /// Pieces and encodings are fresh memory, waiting or worked on whole: in huge pages, a fault per 2 MiB rather
    /// than per 4 KiB. One advisor per child, at a fixed address, as the arenas over them hold it until they are freed.
    piece_pages: huge_pages.HugePageAdvisor = .{ .child = std.heap.page_allocator },
    slicing_pages: huge_pages.HugePageAdvisor = .{ .child = std.heap.page_allocator },
    lean_pages: huge_pages.HugePageAdvisor = .{ .child = std.heap.page_allocator },
    encode_pages: huge_pages.HugePageAdvisor = .{ .child = std.heap.page_allocator },

    /// What a deferred row group's slices draw from (`slicing`, `lean`): the writer alone has the write's ledger and
    /// the slack's room past it. Beside producers still fetching it has the write's ledger while that holds more than
    /// the slack's worth, its own small ledger only once the write's is that short: never a wait on the producers,
    /// whose fetches the writer's turn may be what frees.
    fn slicingFor(w: *WinCtx, alone: bool) ?*core_memory.Charged {
        const m = w.slicing orelse return null;
        if (alone or m.budget.available() > core_memory.lean_decode) return m;
        return w.lean;
    }

    /// What pieces and copied row groups are allocated through.
    pub fn outAlloc(w: *WinCtx) std.mem.Allocator {
        return if (w.memory) |m| m.allocator() else w.gpa;
    }

    /// What a piece's rows are allocated through: `outAlloc` in huge pages.
    pub fn pieceAlloc(w: *WinCtx) std.mem.Allocator {
        return w.piece_pages.allocator();
    }

    fn trimIdle(w: *WinCtx) void {
        for (w.regions) |*r| {
            if (r.busy.cmpxchgStrong(false, true, .acquire, .monotonic) != null) continue;
            r.rows.trim();
            r.work.trim();
            r.busy.store(false, .release);
        }
    }

    /// What a sliced row group's pieces are allocated through: the ledger it slices on, in huge pages.
    fn slicePieceAlloc(w: *WinCtx) std.mem.Allocator {
        const m = w.slicing_now orelse return w.pieceAlloc();
        return if (m == w.lean) w.lean_pages.allocator() else w.slicing_pages.allocator();
    }

    /// `err` was a refused draw from the query's ledger, by `regions` or the output: the row group may still be
    /// decoded a slice at a time, in the writer's turn.
    fn refused(w: *WinCtx, regions: *WorkRegions, err: anyerror) bool {
        if (err != error.OutOfMemory or w.memory == null) return false;
        const any = regions.rows.refused or regions.work.refused or w.memory.?.refused.load(.monotonic);
        regions.rows.refused = false;
        regions.work.refused = false;
        return any;
    }
};

fn winProducer(w: *WinCtx, i: usize) void {
    const ma = w.slots[i].meta_arena.allocator();
    const prof = profile.threadOf(w.profiler, .encode);
    const span = profile.Span.begin(prof);
    defer span.end(.encode_rg, profile.args(.{ w.jobs[i].file_index, w.jobs[i].rg_index, w.jobs[i].passthrough }));
    // `cpu` admits no more producers than there are regions, so a claim fails only if that invariant breaks; then
    // the job pays for fresh ones.
    var spare: ?WorkRegions = null;
    defer if (spare) |*r| r.deinit();
    const regions = WorkRegions.claim(w.regions) orelse blk: {
        spare = .init(w.gpa);
        break :blk &spare.?;
    };
    defer regions.busy.store(false, .release);
    const enc = produceRG(w, i, ma, regions, prof) catch |e| deferred: {
        // Refused its memory beside the other row groups in flight: its regions go back, and the writer decodes it a
        // slice at a time in its turn, when the row groups before it have gone. A column with no sliced form refuses.
        if (w.refused(regions, e) and slicesWhole(w.jobs[i], w.fetch_arr)) {
            regions.rows.trim();
            regions.work.trim();
            break :deferred EncodedRG{ .deferred = true };
        }
        w.jobs[i].leave();
        w.slots[i].enc = .{ .err = e, .failure = consumer.decodeFailure() };
        w.slots[i].done.store(true, .release);
        w.completions.post(w.io);
        return;
    };
    // A deferred row group's input is the writer's to release.
    if (!enc.deferred) w.jobs[i].leave();
    // Near the ledger's limit a producer keeps no pages between row groups: they would stand beside the writer's
    // slices and the other producers' row groups, charged and idle.
    if (w.memory) |m| if (m.budget.tight()) {
        regions.rows.trim();
        regions.work.trim();
    };
    const held = enc.bytes.len + if (enc.piece) |p| p.arena.queryCapacity() else 0;
    profile.gauge(prof, .buffered_bytes, @intCast(held));
    const now = w.inflight.fetchAdd(held, .acq_rel) + held;
    var p = w.peak_bytes.load(.monotonic);
    while (now > p) p = w.peak_bytes.cmpxchgWeak(p, now, .monotonic, .monotonic) orelse break;
    w.slots[i].enc = enc;
    w.slots[i].done.store(true, .release);
    w.completions.post(w.io);
}

/// Job `i`'s output: a byte copy, deferred to the writer when it is too big to decode at once, or its kept rows.
fn produceRG(w: *WinCtx, i: usize, ma: std.mem.Allocator, regions: *WorkRegions, prof: ?*profile.Thread) !EncodedRG {
    const job = w.jobs[i];
    try job.arrive();
    if (job.passthrough) return copyRGToBuffer(
        w.outAlloc(),
        ma,
        job,
        w.kept_set,
        prof,
        w.bloom_leaves,
        w.dictionary_bloom,
    );
    // The filter's (and computed outputs') columns must fit; the kept rows are checked once the filter has run.
    const early = decodeBytes(job, w.early_arr);
    if (try sliceRG(job, w.fetch_arr, early * 2, w.decode_share)) return .{ .deferred = true };
    return projectRGToPiece(
        w.gpa,
        &w.piece_pool,
        regions,
        job,
        w.filter,
        w.fetch_arr,
        w.output_specs,
        w.decode_options,
        prof,
        .{ .share = w.decode_share, .early = early, .rows = decodeBytes(job, w.fetch_arr) },
    ) catch |e| switch (e) {
        // More rows kept than the share holds: the writer decodes the row group a slice at a time.
        error.PieceTooLarge => {
            if (!slicesWhole(job, w.fetch_arr)) return error.RowGroupTooLarge;
            return .{ .deferred = true };
        },
        else => return e,
    };
}

/// What decoding `cols` of `job`'s row group whole takes (`consumer.decodeEstimate`).
fn decodeBytes(job: RGJob, cols: []const bool) usize {
    return consumer.decodeEstimate(.{
        .rg = job.rg,
        .meta = job.meta,
        .src = .{ .bytes = job.bytes, .byte_origin = 0 },
        .filter = null,
        .read = cols,
        .consumed = cols,
    }, cols, &.{}, false);
}

/// Whether every column `job` reads has a sliced form, so its row group can be decoded a slice at a time.
fn slicesWhole(job: RGJob, fetch_arr: []const bool) bool {
    for (fetch_arr, 0..) |needed, ci| {
        if (!needed or ci >= job.rg.columns.items.len) continue;
        if (!consumer.SliceReader.supports(job.rg, job.meta, ci)) return false;
    }
    return true;
}

/// Whether `job`'s row group is decoded a slice of rows at a time (`projectRGSliced`): what it needs, `est`, would
/// take more than `share`. One that would, with a column that has no sliced form, is refused before anything decodes.
fn sliceRG(job: RGJob, fetch_arr: []const bool, est: usize, share: usize) error{RowGroupTooLarge}!bool {
    const rg = job.rg;
    if (rg.num_rows <= 0) return false;
    if (est <= share) return false;
    for (fetch_arr, 0..) |needed, ci| {
        if (!needed or ci >= rg.columns.items.len) continue;
        if (!consumer.SliceReader.supports(rg, job.meta, ci)) return error.RowGroupTooLarge;
    }
    return true;
}

/// The column readers of a sliced row group, positioned `skip` rows in. Built in place: the readers hold the address
/// of `pages`.
const SliceReaders = struct {
    pages: std.heap.ArenaAllocator,
    readers: std.ArrayList(consumer.SliceReader) = .empty,
    lookup: []?usize,

    fn init(self: *SliceReaders, w: *WinCtx, gpa: std.mem.Allocator, job: RGJob, skip: usize) !void {
        self.* = .{ .pages = .init(gpa), .lookup = &.{} };
        errdefer self.deinit();
        const pa = self.pages.allocator();
        self.lookup = try pa.alloc(?usize, w.fetch_arr.len);
        @memset(self.lookup, null);
        const src: consumer.RGSrc = .{ .bytes = job.bytes, .byte_origin = 0 };
        const pool = try consumer.poolOn(&w.slice_pool, gpa);
        var n_cols: usize = 0;
        for (w.fetch_arr, 0..) |needed, ci| {
            if (needed and ci < job.rg.columns.items.len) n_cols += 1;
        }
        pool.rewind();
        try pool.reserve(n_cols);
        for (w.fetch_arr, 0..) |needed, ci| {
            if (!needed or ci >= job.rg.columns.items.len) continue;
            var r = try consumer.SliceReader.initPooled(pa, gpa, job.rg, job.meta, src, ci, w.decode_options, pool) orelse
                return error.UnsupportedColumnType; // `sliceRG` checked every column
            errdefer r.deinit();
            if (skip > 0) {
                var skipped = std.heap.ArenaAllocator.init(gpa);
                defer skipped.deinit();
                try r.skip(skipped.allocator(), skip);
            }
            self.lookup[ci] = self.readers.items.len;
            try self.readers.append(pa, r);
        }
    }

    fn deinit(self: *SliceReaders) void {
        for (self.readers.items) |*r| r.deinit();
        self.pages.deinit();
    }
};

/// Decode, filter and project `job`'s row group a slice of rows at a time on the writer, in its turn, each slice's
/// kept rows a piece for the assembler: neither the decoded row group nor its output is ever held whole. Slices are
/// sized by what rows took so far, within half the ledger's room. A slice the ledger cannot hold refuses the write.
/// None of this shows in the output, whose row groups the assembler cuts from the rows alone.
fn projectRGSliced(w: *WinCtx, job: RGJob, prof: ?*profile.Thread, asm_: *Assembler) !void {
    const gpa = if (w.slicing_now) |m| m.allocator() else w.gpa;
    // The ledger these slices draw from. The writer's lean ledger is its own 32 MiB, so its carry is an eighth of
    // that and not of the query ceiling: anything more would leave the slice no room to decode.
    const allowance = if (w.slicing_now) |m| blk: {
        if (w.lean != null and m == w.lean.?) break :blk core_memory.lean_decode;
        break :blk m.budget.limit;
    } else w.decode_options.expand_limit;
    defer if (w.slice_pool) |pool| pool.trimAbove(core_memory.retainedCarry(allowance));
    const rg = job.rg;
    const num_rows: usize = @intCast(@max(0, rg.num_rows));
    const est = consumer.decodeEstimate(.{
        .rg = rg,
        .meta = job.meta,
        .src = .{ .bytes = job.bytes, .byte_origin = 0 },
        .filter = null,
        .read = w.fetch_arr,
        .consumed = w.fetch_arr,
    }, w.fetch_arr, &.{}, false) * 3;
    const ledger: ?*const core_memory.SharedBudget = if (w.slicing_now) |m| m.budget else null;
    const per_row = @max(1, est / @max(1, num_rows));
    var slice_rows = std.math.clamp(w.decode_share / per_row, 1, num_rows);
    // Under a ledger, the footer's estimate can understate values by any factor: start small and size each slice by
    // what the rows before it took.
    if (ledger != null) slice_rows = @min(slice_rows, 64);

    var timings: consumer.Timings = .{ .prof = prof };
    defer {
        w.core.decode_ns += timings.decode_ns;
        w.core.eval_ns += timings.eval_ns;
        w.core.encode_ns += timings.encode_ns;
    }
    var readers: SliceReaders = undefined;
    try readers.init(w, gpa, job, 0);
    defer readers.deinit();
    var done: usize = 0;
    // Per-slice working memory (the decoded slice and its selection). The piece keeps the rows; this keeps the carry.
    var slice = std.heap.ArenaAllocator.init(gpa);
    defer slice.deinit();
    while (done < num_rows) {
        // A sealed group keeps its pieces and its encode reservation until that encode finishes. The next slice
        // on top of them is what fills an explicit budget, so drain first. Those rows already fit; this is not a retry.
        if (ledger) |l| if (l.tight()) {
            w.trimIdle();
            try asm_.writeReady(true);
        };
        const n = @min(slice_rows, num_rows - done);
        // Keep at most the carry. The piece holds this slice's rows; retaining the slice's high water as well
        // charged both, and the next wider page grew that node again.
        _ = slice.reset(.{ .retain_with_limit = core_memory.retainedCarry(allowance) });
        const piece = try sliceToPiece(w, job, &readers, &slice, n, &timings);
        done += n;
        if (ledger) |l| {
            // `per_row` is three times the footer estimate, which kept every slice at a handful of rows. A wide
            // piece's arena then costs more than the row group it is filling, and the rows never get sealed.
            const foot_row = @max(1, (est / 3) / @max(1, num_rows));
            const took = @max(foot_row, (slice.queryCapacity() + piece.arena.queryCapacity()) / n);
            // Each piece owns column buffers and schema metadata. Shrinking the next batch below the initial 64
            // rows makes that fixed cost dominate, until an unfinished output row group can exhaust the ledger.
            // One slice stays within one output row group, never cheaper than the footer's per-row estimate, and
            // at most doubles: a run of tiny values must not size the next slice for every remaining row.
            const byte_cap = @min(w.decode_share, @as(usize, @intCast(w.rg_target.bytes)));
            const grew = @min(num_rows, @max(64, @min(byte_cap, l.available() / 2) / took));
            slice_rows = @min(grew, slice_rows *| 2);
        }
        try asm_.feed(piece);
    }
}

/// The next `n` rows of `readers` as a piece: decoded and filtered on `slice`, their kept rows projected onto the
/// piece's own memory, which the assembler holds until they are written.
fn sliceToPiece(
    w: *WinCtx,
    job: RGJob,
    readers: *SliceReaders,
    slice: *std.heap.ArenaAllocator,
    n: usize,
    timings: *consumer.Timings,
) !*Piece {
    const sa = slice.allocator();
    const cols = try sa.alloc(filter_eval.Batch.Column, readers.readers.items.len);
    const t0 = nowMonoNs();
    for (readers.readers.items, cols) |*r, *c| c.* = try r.next(sa, n);
    const t1 = nowMonoNs();
    timings.decode_ns += @intCast(t1 - t0);
    const batch: filter_eval.Batch = .{ .cols = cols, .num_rows = n };
    var sel = try filter_selection.SelectionVector.init(sa, n);
    if (w.filter) |f| try filter_eval.evaluate(f, &batch, &sel, readers.lookup, sa);
    const piece = try Piece.create(w.gpa, w.slicePieceAlloc());
    errdefer piece.destroy(w.gpa);
    piece.agg = try consumer.initOutputAggregator(piece.arena.allocator(), job.meta, w.output_specs);
    _ = try consumer.appendSliceRows(&piece.agg, &batch, readers.lookup, &sel, w.output_specs, sa);
    piece.bits_prefix = try consumer.plainBitsPrefix(piece.arena.allocator(), &piece.agg);
    timings.eval_ns += @intCast(nowMonoNs() - t1);
    return piece;
}

/// In-order writer: takes slots 0..N, feeding their kept rows to the assembler (which writes each output row group
/// as it completes) or writing their byte copies, frees their bytes and posts their window permits.
fn winWriter(w: *WinCtx) void {
    const prof = profile.threadOf(w.profiler, .writer);
    var asm_ = Assembler.init(w);
    defer asm_.deinit();
    var next: usize = 0;
    while (next < w.slots.len) : (next += 1) {
        while (!w.slots[next].done.load(.acquire)) w.completions.waitUncancelable(w.io);
        const enc = w.slots[next].enc;
        w.core.decode_ns += enc.timings.decode_ns;
        w.core.eval_ns += enc.timings.eval_ns;
        w.core.encode_ns += enc.timings.encode_ns;
        const held = enc.bytes.len + if (enc.piece) |p| p.arena.queryCapacity() else 0;
        if (enc.err) |e| {
            if (w.first_err == null) {
                // Encodes of earlier rows report in output order, even if this later producer fails first.
                asm_.flush() catch |earlier| {
                    w.first_err = earlier;
                };
                if (w.first_err == null) {
                    w.first_err = e;
                    w.first_failure = enc.failure;
                }
            }
            if (enc.piece) |p| p.unref(w.gpa);
        } else if (w.first_err != null) {
            if (enc.piece) |p| p.unref(w.gpa);
            if (enc.deferred) w.jobs[next].leave();
        } else if (enc.piece) |p| {
            // The window bounds rows waiting for the writer; held by the assembler they count toward a row group.
            asm_.feed(p) catch |e| {
                w.first_err = e;
            };
            if (w.first_err == null and next + 1 < w.slots.len and w.out_elems.len > w.executor.?.workers and
                asm_.shouldCompact()) asm_.compactCarry() catch |e| {
                w.first_err = e;
            };
        } else if (enc.deferred) {
            defer w.jobs[next].leave();
            // Under a ledger the slices are decoded alone: the writer takes every producer's turn, so those running
            // finish and none starts, and their regions give their pages back, before it slices. Not over fetched
            // inputs: a producer may be waiting for fetch room this row group holds until the writer is done with it.
            const turns = if (w.memory != null and w.jobs[next].gate == null) w.producers else 0;
            for (0..turns) |_| w.cpu.waitUncancelable(w.io);
            defer for (0..turns) |_| w.cpu.post(w.io);
            w.writer_turns = turns;
            defer w.writer_turns = 0;
            // Restore encode admission before returning the producer turns. Idle workers own no slot permits.
            const restrained = w.executor.?.granted.load(.acquire);
            defer if (turns > 0) w.executor.?.restrainThreads(restrained);
            // Encode jobs may still hold regions: only idle ones are trimmed.
            if (turns > 0) {
                w.trimIdle();
                w.executor.?.allowParallel();
            }
            // Alone, the writer has the slack's room for one decode beside what the write holds; beside producers, it
            // has that room as a ledger of its own.
            const slack = if (turns > 0) core_memory.lean_decode else 0;
            if (w.slicing) |m| m.budget.limit += slack;
            defer if (w.slicing) |m| {
                m.budget.limit -= slack;
            };
            w.slicing_now = w.slicingFor(turns > 0);
            projectRGSliced(w, w.jobs[next], prof, &asm_) catch |e| {
                w.first_err = if (w.slicing_now) |m| m.refusal(e) else e;
                w.first_failure = consumer.decodeFailure();
            };
        } else if (enc.rg) |rg| {
            // A byte copy: the rows before it make the row group they are, then it goes as it came.
            asm_.flush() catch |e| {
                w.first_err = e;
            };
            if (w.first_err == null and enc.surviving > 0) {
                var rgm = rg;
                consumer.rebaseRowGroupOffsets(&rgm, @intCast(w.out_offset));
                const t0 = nowMonoNs();
                w.sink.write(enc.bytes) catch |e| {
                    w.first_err = e;
                };
                const t1 = nowMonoNs();
                w.sink_ns += @intCast(t1 - t0);
                profile.record(prof, .sink_write, t0, t1, profile.args(.{enc.bytes.len}));
                if (w.first_err == null) {
                    w.out_offset += enc.bytes.len;
                    w.rows_kept += enc.surviving;
                    w.rgs_kept += 1;
                    w.new_row_groups.append(w.arena, rgm) catch |e| {
                        w.first_err = e;
                    };
                }
            }
        }
        if (enc.bytes.len > 0) w.outAlloc().free(enc.bytes);
        profile.gauge(prof, .buffered_bytes, -@as(i64, @intCast(held)));
        _ = w.inflight.fetchSub(held, .acq_rel);
        for (0..windowPermits(WinCtx, w, next)) |_| w.window.post(w.io);
    }
    if (w.first_err == null) asm_.flush() catch |e| {
        w.first_err = e;
    };
}

pub fn runWindowedReencode(w: *WinCtx, async_budget: usize) !void {
    return runWindowed(WinCtx, w, w.jobs.len, async_budget);
}

/// Drive the fan-out: `Ctx.writer` on a dedicated thread plus `async_budget` concurrent `Ctx.producer` calls on pool
/// threads, admitting a new producer only once the window has room (memory back-pressure) and a running one has
/// finished (CPU budget).
///
/// The writer's thread counts against the executor's `async_limit`, and a producer submitted at that limit runs inline
/// on this dispatching thread, which then admits nothing until it finishes, idling every other slot. So the CPU budget
/// is the `cpu` semaphore, and the limit leaves room for the writer and for as many producers again that have released
/// their permit but not yet their thread. Exceeding that takes more than `async_budget` producers finishing within the
/// moment a thread takes to return, which only near-empty row groups do, and their inline run is as short.
pub fn runWindowed(comptime Ctx: type, w: *Ctx, n_jobs: usize, async_budget: usize) !void {
    // A write's encode jobs run on the pool too, beside the producers.
    const encoders = if (@hasField(Ctx, "cpu_budget")) w.cpu_budget else 0;
    const borrowed = if (@hasField(Ctx, "worker_io")) w.worker_io else null;
    var threaded: ?std.Io.Threaded = null;
    defer if (threaded) |*runtime| runtime.deinit();
    if (borrowed) |io| {
        w.io = io;
    } else {
        threaded = .init(w.gpa, .{ .async_limit = .limited(1 + 2 * (async_budget + encoders)) });
        w.io = threaded.?.io();
    }
    const producer_count = if (Ctx == WinCtx) (if (async_budget >= w.cpu_budget and w.cpu_budget > 1)
        async_budget - 1
    else
        async_budget) else async_budget;
    w.cpu = .{ .permits = producer_count };
    if (@hasField(Ctx, "producers")) w.producers = producer_count;
    var executor: EncodeExecutor = undefined;
    if (Ctx == WinCtx) {
        w.encode_during_decode = producer_count < async_budget;
        w.producer_left.store(n_jobs, .monotonic);
        try executor.init(w);
        w.executor = &executor;
    }
    defer if (Ctx == WinCtx) {
        executor.deinit();
        w.executor = null;
    };
    if (@hasField(Ctx, "piece_pool")) w.piece_pool.io = w.io;

    const Run = struct {
        fn producer(ctx: *Ctx, i: usize) void {
            defer ctx.cpu.post(ctx.io);
            Ctx.producer(ctx, i);
            if (Ctx == WinCtx) {
                const left = ctx.producer_left.fetchSub(1, .acq_rel) - 1;
                const e = ctx.executor.?;
                e.allowThreads(e.workers - @min(left, @min(ctx.producers, e.workers - 1)));
            }
        }
    };
    var group: std.Io.Group = .init;
    defer group.cancel(w.io);
    try group.concurrent(w.io, Ctx.writer, .{w});
    // No fallible dispatch exit after the writer starts: even a failed producer publishes its own slot, and later
    // slots must still be dispatched. Cancellation is observed at await only after every slot has a producer.
    for (0..n_jobs) |i| {
        // One dispatching thread takes every permit, so no two producers each hold part of what both need.
        for (0..windowPermits(Ctx, w, i)) |_| w.window.waitUncancelable(w.io);
        w.cpu.waitUncancelable(w.io);
        // The host runtime may have a smaller async limit than this query's ready tasks. Concurrent dispatch
        // must not run a producer inline on the dispatcher while its writer waits for another input slot.
        if (borrowed != null) {
            group.concurrent(w.io, Run.producer, .{ w, i }) catch group.async(w.io, Run.producer, .{ w, i });
        } else group.async(w.io, Run.producer, .{ w, i });
    }
    var wait_error: ?anyerror = null;
    group.await(w.io) catch |err| {
        wait_error = err;
        group.cancel(w.io);
    };
    if (w.first_err) |e| {
        // The error is reported from this thread: where a producer or the writer raised it goes with it.
        if (@hasField(Ctx, "first_failure")) consumer.setDecodeFailure(w.first_failure);
        return e;
    }
    if (wait_error) |err| return err;
}

/// A byte copy that changes codec: producers recompress whole row groups in parallel, and the in-order writer places
/// them through `consumer.copyRG`, which writes straight to the sink at final offsets, so page indexes carry over
/// exactly as on the serial copy.
pub const RecompressCtx = struct {
    const writer = recompressWriter;
    const producer = recompressProducer;

    gpa: std.mem.Allocator,
    io: std.Io = undefined, // set in runWindowed once the runtime exists
    worker_io: ?std.Io = null,
    jobs: []const RGJob,
    kept: ?[]const bool,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan = .{},
    /// Threads each producer recompresses its row group's pages with: the CPU budget shared among the row groups that
    /// run at once, so a file of one row group still uses all of it.
    page_workers: usize = 1,
    /// Bloom filter probability per leaf (`--bloom-filter`), for copied chunks that carry none.
    bloom_leaves: []const ?f64 = &.{},
    dictionary_bloom: bool = true,
    slots: []RecompressSlot,
    window: *std.Io.Semaphore,
    /// The window's permit size and count (`jobUnits`).
    win_unit: u64,
    win_units: usize,
    completions: *std.Io.Semaphore,
    cpu: std.Io.Semaphore = .{},

    // Writer-owned; read back after the fan-out.
    sink: streaming.Sink,
    out_offset: u64,
    new_row_groups: *std.ArrayListUnmanaged(schema.RowGroup),
    arena: std.mem.Allocator, // writer-only: footer metadata
    rows_kept: i64 = 0,
    rgs_kept: usize = 0,
    core: consumer.Timings = .{},
    first_err: ?anyerror = null,
    profiler: ?*profile.Profiler = null,
    /// The recompressed output one producer may gather; a row group that could write more is deferred to the writer.
    share: u64 = std.math.maxInt(u64),
};

pub const RecompressSlot = struct {
    /// Private to the producer until `done`, then to the writer, which frees it once the row group is written.
    arena: std.heap.ArenaAllocator,
    chunks: []?consumer.RecompressedChunk = &.{},
    /// Too big to gather (`recompressedSize`): the writer recompresses it straight into the sink.
    deferred: bool = false,
    encode_ns: u64 = 0,
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),
};

fn recompressProducer(w: *RecompressCtx, i: usize) void {
    const prof = profile.threadOf(w.profiler, .encode);
    const span = profile.Span.begin(prof);
    defer span.end(.encode_rg, profile.args(.{ w.jobs[i].file_index, w.jobs[i].rg_index, false }));
    const slot = &w.slots[i];
    var timings: consumer.Timings = .{ .prof = prof };
    const job = w.jobs[i];
    // The input is released by the writer once it has placed the row group: it still copies the chunks that keep
    // their codec, and carried Bloom filters, from it.
    if (job.arrive()) |_| {
        // Output that could pass this producer's share (a codec that expands, as to UNCOMPRESSED) is not gathered
        // here: the writer recompresses it page by page straight into the sink in its turn.
        if (recompressedSize(job.rg, w.kept, w.codec) > w.share) {
            slot.deferred = true;
        } else if (consumer.recompressRowGroup(
            slot.arena.allocator(),
            w.gpa,
            job.rg,
            .{ .bytes = job.bytes, .byte_origin = 0 },
            w.kept,
            w.codec,
            w.zstd,
            &timings,
            w.page_workers,
        )) |chunks| {
            slot.chunks = chunks;
        } else |e| slot.err = e;
    } else |e| slot.err = e;
    slot.encode_ns = timings.encode_ns;
    slot.done.store(true, .release);
    w.completions.post(w.io);
}

/// The most a recompressing copy of `rg` can write: a chunk that changes codec at its uncompressed size (the bound of
/// an UNCOMPRESSED rewrite, and above what a compressing codec writes), any other as it is.
fn recompressedSize(rg: *const schema.RowGroup, kept: ?[]const bool, codec: schema.CompressionCodec) u64 {
    var total: u64 = 0;
    for (rg.columns.items, 0..) |chunk, ci| {
        if (kept) |k| if (ci >= k.len or !k[ci]) continue;
        const m = chunk.meta_data orelse continue;
        const size = if (m.codec == codec)
            m.total_compressed_size
        else
            @max(m.total_compressed_size, m.total_uncompressed_size);
        total += @intCast(@max(0, size));
    }
    return total;
}

fn recompressWriter(w: *RecompressCtx) void {
    const prof = profile.threadOf(w.profiler, .writer);
    w.core.prof = prof;
    defer w.core.prof = null;
    for (w.slots, w.jobs) |*slot, job| {
        while (!slot.done.load(.acquire)) w.completions.waitUncancelable(w.io);
        w.core.encode_ns += slot.encode_ns;
        if (slot.err) |e| {
            if (w.first_err == null) w.first_err = e;
        } else if (w.first_err == null) {
            const rg_span = profile.Span.begin(prof);
            defer rg_span.end(.copy_rg, profile.args(.{job.rg.total_byte_size}));
            if (consumer.copyRG(
                w.arena,
                job.rg,
                .{ .bytes = job.bytes, .byte_origin = 0 },
                w.kept,
                w.sink,
                &w.out_offset,
                &w.core,
                true,
                .{
                    .codec = w.codec,
                    .zstd = w.zstd,
                    .scratch = w.gpa,
                    .done = if (slot.deferred) null else slot.chunks,
                },
                .{
                    .fpp = w.bloom_leaves,
                    .meta = job.meta,
                    .scratch = w.gpa,
                    .dictionary_bloom = w.dictionary_bloom,
                },
            )) |out| {
                if (out.rg) |rg| w.new_row_groups.append(w.arena, rg) catch |e| {
                    w.first_err = e;
                };
                w.rows_kept += out.surviving_rows;
                if (out.surviving_rows > 0) w.rgs_kept += 1;
            } else |e| w.first_err = e;
        }
        job.leave();
        slot.arena.deinit();
        slot.arena = std.heap.ArenaAllocator.init(w.gpa);
        for (0..jobUnits(job, w.win_unit, w.win_units)) |_| w.window.post(w.io);
    }
}

const WriteWindow = struct {
    workers: usize,
    window: usize,
    /// The byte budget for buffered output in units of `unit` bytes: a row group in flight holds `jobUnits` of them.
    unit: u64,
    units: usize,
    /// The CPU budget `workers` was taken from (they are fewer when the window holds fewer row groups).
    budget: usize,
};

/// The write fan-out's two knobs:
///   - concurrency: explicit -j, else the Lambda- and quota-aware CPU budget;
///   - window: row groups in flight, from a byte budget for buffered output in every environment, so a large -j or
///     large row groups cannot retain many GB. On Lambda we own the container (about half the tier); elsewhere stay
///     polite (a modest cap, a small fraction of what is available).
/// A tight byte budget may cut the workers below the CPU budget: idle workers beat exceeding the memory cap.
pub fn writeWindow(ctx: Context, args: QueryArgs, jobs: []const RGJob) WriteWindow {
    const async_budget = if (args.parallelism > 0) args.parallelism else system.discoverAvailableParallelism(ctx.env);
    var sum_bytes: u64 = 0;
    for (jobs) |j| sum_bytes += @intCast(@max(@as(i64, 0), j.rg.total_byte_size));
    const avg_rg = @max(@as(u64, 1), sum_bytes / @max(@as(u64, 1), jobs.len));
    // Never more than a quarter of the query's budget: the S3 fetch window takes another (`s3FetchWindow`), and the
    // rest is for the producers' decode and encode working sets.
    const inflight_budget: u64 = @min(@as(u64, args.max_memory) / 4, if (system.onLambda(ctx.env))
        system.discoverAvailableMemory(ctx.env) / 2
    else
        @min(@as(u64, 256) * 1024 * 1024, system.discoverAvailableMemory(ctx.env) / 8));
    const window = system.windowCapacity(inflight_budget, avg_rg, jobs.len);
    const unit = @max(@as(u64, 1), inflight_budget / window_units);
    return .{
        .workers = @min(async_budget, window),
        .window = window,
        .unit = unit,
        .units = @intCast(@max(@as(u64, 1), inflight_budget / unit)),
        .budget = async_budget,
    };
}

/// Permits the write window's byte budget is cut into.
const window_units = 1024;

/// The window's permits a row group in flight holds: its own size, not the average, so a few large row groups among
/// many small ones cannot all be buffered at once (they held several times the budget behind a slow sink). Never
/// more than the whole window, so a row group larger than the budget still runs, alone.
fn jobUnits(job: RGJob, unit: u64, units: usize) usize {
    const bytes: u64 = @intCast(@max(@as(i64, 0), job.rg.total_byte_size));
    return @intCast(std.math.clamp(std.math.divCeil(u64, bytes, unit) catch 1, 1, units));
}

/// `jobUnits` for job `i` of a write fan-out context; one permit for a context that counts row groups instead.
fn windowPermits(comptime Ctx: type, w: *const Ctx, i: usize) usize {
    if (!@hasField(Ctx, "win_unit")) return 1;
    return jobUnits(w.jobs[i], w.win_unit, w.win_units);
}

/// True when some kept chunk of some row group is stored in another codec than `codec`.
pub fn anyChunkNeedsRecompress(
    metas: []const schema.FileMetaData,
    kept: ?[]const bool,
    codec: schema.CompressionCodec,
) bool {
    for (metas) |*m| for (m.row_groups.items) |*rg| for (rg.columns.items, 0..) |*col, ci| {
        if (kept) |k| if (ci >= k.len or !k[ci]) continue;
        const md = col.meta_data orelse continue;
        if (md.codec != codec) return true;
    };
    return false;
}

/// True when every kept (output) column chunk of `rg` is already stored in
/// `codec` — the precondition for a byte-copy passthrough to honor a requested
/// --codec without re-encoding.
pub fn keptColumnsUseCodec(rg: *const schema.RowGroup, kept: []const bool, codec: schema.CompressionCodec) bool {
    for (rg.columns.items, 0..) |*col, ci| {
        if (ci < kept.len and !kept[ci]) continue;
        const md = col.meta_data orelse return false;
        if (md.codec != codec) return false;
    }
    return true;
}

test "a byte-copied row group its ledger refuses holds nothing after" {
    const a = std.testing.allocator;
    const bytes = metadata.readFileSlice("ci/fixtures/parquet/overflow_rows.parquet", a) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer a.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(a);
    defer arena_state.deinit();
    const meta = try metadata.open(arena_state.allocator(), bytes);
    const job: RGJob = .{ .bytes = bytes, .meta = &meta, .rg = &meta.row_groups.items[0] };
    const kept: [5]bool = @splat(true);
    var refused: usize = 0;
    // Refused part way through the copy (its buffer grown, then refused), or not at all.
    for ([_]usize{ 4 << 10, 16 << 10, 1 << 20 }) |limit| {
        var ledger: core_memory.SharedBudget = .{ .limit = limit };
        var charged: core_memory.Charged = .{ .child = a, .budget = &ledger };
        const ca = charged.allocator();
        if (copyRGToBuffer(ca, arena_state.allocator(), job, &kept, null, &.{}, true)) |out| {
            ca.free(out.bytes);
        } else |err| {
            try std.testing.expectEqual(error.ExceededMemoryBudget, charged.refusal(err));
            refused += 1;
        }
        try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
    }
    try std.testing.expectEqual(@as(usize, 2), refused);
}

test "a deferred write row group slices on the write's ledger while it has room, else beside it on its own" {
    var write_ledger: core_memory.SharedBudget = .{ .limit = 1 << 30 };
    var lean_ledger: core_memory.SharedBudget = .{ .limit = core_memory.lean_decode };
    var main: core_memory.Charged = .{ .child = std.testing.allocator, .budget = &write_ledger };
    var lean: core_memory.Charged = .{ .child = std.testing.allocator, .budget = &lean_ledger };
    var w: WinCtx = undefined;
    w.slicing = &main;
    w.lean = &lean;
    // Alone, the writer always has the write's ledger (and the slack's room past it).
    try std.testing.expectEqual(&main, w.slicingFor(true).?);
    // Beside producers still fetching: the write's ledger while it has more than the slack's worth left.
    try std.testing.expectEqual(&main, w.slicingFor(false).?);
    _ = write_ledger.used.fetchAdd(write_ledger.limit - core_memory.lean_decode, .monotonic);
    try std.testing.expectEqual(&lean, w.slicingFor(false).?);
    try std.testing.expectEqual(&main, w.slicingFor(true).?);
    w.slicing = null;
    try std.testing.expectEqual(@as(?*core_memory.Charged, null), w.slicingFor(false));
}

test "regression: a row group in flight holds the write window's bytes for its own size, at most the whole window" {
    var rgs: [4]schema.RowGroup = undefined;
    for (&rgs, [_]i64{ 0, 1, 33 << 20, 4 << 30 }) |*rg, size| {
        rg.* = .{ .columns = .empty, .total_byte_size = size, .num_rows = 1 };
    }
    const job = struct {
        fn of(rg: *const schema.RowGroup) RGJob {
            return .{ .bytes = &.{}, .meta = undefined, .rg = rg };
        }
    }.of;
    const unit: u64 = (256 << 20) / window_units;
    try std.testing.expectEqual(@as(usize, 1), jobUnits(job(&rgs[0]), unit, window_units));
    try std.testing.expectEqual(@as(usize, 1), jobUnits(job(&rgs[1]), unit, window_units));
    try std.testing.expectEqual(@as(usize, 132), jobUnits(job(&rgs[2]), unit, window_units));
    // Larger than the whole budget: admitted alone, never waiting for permits that do not exist.
    try std.testing.expectEqual(@as(usize, window_units), jobUnits(job(&rgs[3]), unit, window_units));
}

test "windowed fan-out runs budget producers at once on pool threads beside a writer that holds its thread" {
    const Fake = struct {
        const Self = @This();
        const writer = fakeWriter;
        const producer = fakeProducer;

        gpa: std.mem.Allocator,
        io: std.Io = undefined,
        window: *std.Io.Semaphore,
        cpu: std.Io.Semaphore = .{},
        first_err: ?anyerror = null,
        budget: usize,
        n: usize,
        dispatcher: std.Thread.Id,
        finished: std.Io.Semaphore = .{},
        active: std.atomic.Value(usize) = .init(0),
        peak: std.atomic.Value(usize) = .init(0),
        inline_runs: std.atomic.Value(usize) = .init(0),

        fn fakeWriter(self: *Self) void {
            for (0..self.n) |_| {
                self.finished.waitUncancelable(self.io);
                self.window.post(self.io);
            }
        }

        fn fakeProducer(self: *Self, i: usize) void {
            if (std.Thread.getCurrentId() == self.dispatcher) _ = self.inline_runs.fetchAdd(1, .monotonic);
            _ = self.peak.fetchMax(self.active.fetchAdd(1, .acq_rel) + 1, .monotonic);
            // The first `budget` producers wait to see one another: one run inline on the dispatching thread stops
            // admission, so the rest never start and the wait times out short of the budget. The rest stand for real
            // row groups, which take far longer than a pool thread does to come back after releasing its permit.
            if (i < self.budget) {
                var waited: usize = 0;
                while (self.active.load(.acquire) < self.budget and waited < 2000) : (waited += 1)
                    self.io.sleep(.fromMilliseconds(1), .awake) catch {};
            } else self.io.sleep(.fromMilliseconds(10), .awake) catch {};
            _ = self.active.fetchSub(1, .acq_rel);
            self.finished.post(self.io);
        }
    };

    for ([_]usize{ 1, 2, 3 }) |budget| {
        var window: std.Io.Semaphore = .{ .permits = 4 };
        var f: Fake = .{
            .gpa = std.testing.allocator,
            .window = &window,
            .budget = budget,
            .n = 12,
            .dispatcher = std.Thread.getCurrentId(),
        };
        try runWindowed(Fake, &f, f.n, budget);
        try std.testing.expectEqual(@as(usize, 0), f.inline_runs.load(.monotonic));
        try std.testing.expectEqual(budget, f.peak.load(.monotonic));
    }
}


test "persistent encode executor cleans up allocation failures with serial and parallel workers" {
    const Run = struct {
        fn run(gpa: std.mem.Allocator, limited: bool, parallel: bool) !void {
            var values: [20_001]f64 = undefined;
            for (&values, 0..) |*value, i| value.* = @as(f64, @floatFromInt(i)) * 1.319;
            var elems: [5]schema.SchemaElement = @splat(.{
                .name = "x",
                .type = .DOUBLE,
                .repetition_type = .REQUIRED,
                .type_length = null,
                .num_children = null,
                .scale = null,
                .precision = null,
                .field_id = null,
            });
            var paths: [5][]const []const u8 = @splat(&.{"x"});
            var cols: [5]consumer.OutputAggregator.ColumnBuf = @splat(.{ .f64 = .{
                .values = .fromOwnedSlice(&values),
            } });
            var piece: Piece = .{
                .arena = .init(std.testing.allocator),
                .agg = .{
                    .arena = gpa,
                    .cols = &cols,
                    .schema_elems = &elems,
                    .paths = &paths,
                    .num_rows = values.len,
                    .captured = true,
                },
            };
            defer piece.arena.deinit();
            var expected_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer expected_arena.deinit();
            const expected = try encoder.encodeColumn(expected_arena.allocator(), .{
                .values = .{ .f64 = .{ .values = &values } },
                .schema_elem = &elems[0],
                .path_in_schema = paths[0],
                .codec = .UNCOMPRESSED,
            });
            var completions: std.Io.Semaphore = .{};
            var w: WinCtx = undefined;
            w.gpa = gpa;
            w.io = std.testing.io;
            w.memory = null;
            w.cpu_budget = if (limited or parallel) 4 else 1;
            w.encode_during_decode = limited;
            w.rg_target = .{};
            w.regions = &.{};
            w.encode_pages = .{ .child = gpa };
            w.out_elems = &elems;
            w.out_paths = &paths;
            w.codec = .UNCOMPRESSED;
            w.zstd = .{};
            w.bloom_outputs = &.{};
            w.profiler = null;
            w.completions = &completions;
            var executor: EncodeExecutor = undefined;
            try executor.init(&w);
            defer executor.deinit();
            w.executor = &executor;
            for (0..3) |_| {
                if (limited) {
                    executor.allowParallel();
                    executor.restrainThreads(1);
                    try std.testing.expectEqual(@as(usize, 1), executor.granted.load(.acquire));
                }
                const job = try EncodeJob.create(&w, values.len);
                defer job.destroy();
                job.ranges = try gpa.dupe(Assembler.Range, &.{.{ .piece = &piece, .lo = 0, .hi = values.len }});
                executor.submit(.{ .begin = job });
                while (!job.done.load(.acquire)) completions.waitUncancelable(w.io);
                if (job.err) |err| return err;
                try std.testing.expectEqual(@as(usize, 5), job.encs.len);
                for (job.encs) |enc| {
                    try std.testing.expectEqualSlices(u8, expected.bytes, enc.bytes);
                    try std.testing.expectEqual(expected.pages.len, enc.pages.len);
                }
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Run.run, .{ false, false });
    try Run.run(std.testing.allocator, true, false);
    // A parallel cohort has scheduling-dependent arena growth, so sweep an atomic failure threshold rather than
    // use FailingAllocator's non-atomic counters and deterministic-allocation-count requirement.
    const Fail = struct {
        calls: std.atomic.Value(usize) = .init(0),
        failed: std.atomic.Value(bool) = .init(false),
        after: usize = std.math.maxInt(usize),

        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{
                .alloc = alloc,
                .resize = std.mem.Allocator.noResize,
                .remap = std.mem.Allocator.noRemap,
                .free = free,
            } };
        }
        fn alloc(ptr: *anyopaque, n: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.calls.fetchAdd(1, .monotonic) >= self.after) {
                self.failed.store(true, .monotonic);
                return null;
            }
            return std.testing.allocator.rawAlloc(n, alignment, ra);
        }
        fn free(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
            std.testing.allocator.rawFree(memory, alignment, ra);
        }
    };
    var count: Fail = .{};
    try Run.run(count.allocator(), false, true);
    var failures: usize = 0;
    for (0..count.calls.load(.monotonic) * 2) |limit| {
        var fail: Fail = .{ .after = limit };
        Run.run(fail.allocator(), false, true) catch |err| {
            try std.testing.expectEqual(error.OutOfMemory, err);
            try std.testing.expect(fail.failed.load(.monotonic));
            failures += 1;
        };
    }
    try std.testing.expect(failures > 10);
}

test "wide carried rows release their original piece without changing values under allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            var elems: [2]schema.SchemaElement = @splat(.{
                .name = "x",
                .type = .INT32,
                .repetition_type = .REQUIRED,
                .type_length = null,
                .num_children = null,
                .scale = null,
                .precision = null,
                .field_id = null,
            });
            elems[1].name = "s";
            elems[1].type = .BYTE_ARRAY;
            var paths = [_][]const []const u8{ &.{"x"}, &.{"s"} };
            var w: WinCtx = undefined;
            w.gpa = gpa;
            w.memory = null;
            w.rg_target = .{};
            w.piece_pages = .{ .child = gpa };
            w.out_elems = &elems;
            w.out_paths = &paths;
            var a = Assembler.init(&w);
            defer a.deinit();
            const p = try Piece.create(gpa, gpa);
            var owned = true;
            defer if (owned) p.unref(gpa);
            const pa = p.arena.allocator();
            const cols = try pa.alloc(consumer.OutputAggregator.ColumnBuf, 2);
            cols[0] = .{ .i32 = .{ .values = .fromOwnedSlice(try pa.dupe(i32, &.{ 11, 12, 13, 14, 15, 16 })) } };
            const dict = try pa.alloc([]const u8, 3);
            for (dict, [_][]const u8{ "alpha", "beta", "gamma" }) |*value, text| value.* = try pa.dupe(u8, text);
            cols[1] = .{ .string = .{
                .coded = true,
                .codes = .fromOwnedSlice(try pa.dupe(u32, &.{ 1, 0, 2, 0, 1, 2 })),
                .dict = dict,
            } };
            p.agg = .{
                .arena = pa,
                .cols = cols,
                .schema_elems = &elems,
                .paths = &paths,
                .num_rows = 6,
                .captured = true,
            };
            p.bits_prefix = try consumer.plainBitsPrefix(pa, &p.agg);
            try a.done.append(gpa, p);
            owned = false;
            try a.ranges.append(gpa, .{ .piece = p, .lo = 2, .hi = 6 });
            a.rows = 4;
            a.bits = p.bits_prefix[6] - p.bits_prefix[2];
            const bits = a.bits;
            try a.compactCarry();
            try std.testing.expectEqual(@as(usize, 1), a.ranges.items.len);
            const carry = a.ranges.items[0];
            try std.testing.expectEqual(@as(usize, 0), carry.lo);
            try std.testing.expectEqual(@as(usize, 4), carry.hi);
            try std.testing.expectEqualSlices(i32, &.{ 13, 14, 15, 16 }, carry.piece.agg.cols[0].i32.values.items);
            const strings = carry.piece.agg.cols[1].string.values.items;
            for (strings, [_][]const u8{ "gamma", "alpha", "beta", "gamma" }) |value, text| {
                try std.testing.expectEqualStrings(text, value);
            }
            try std.testing.expectEqual(bits, carry.piece.bits_prefix[4]);
        }
    }.run, .{});
}

test "idle pieces stay bounded, reuse their rows and remain charged until freed" {
    const gpa = std.testing.allocator;
    var ledger: core_memory.SharedBudget = .{ .limit = 1 << 20 };
    var charged: core_memory.Charged = .{ .child = gpa, .budget = &ledger };
    var pool: PiecePool = .{ .gpa = gpa, .rows_alloc = charged.allocator(), .io = std.testing.io, .limit = 20_000 };
    {
        defer pool.deinit();
        const p = try pool.take();
        const q = try pool.take();
        _ = try p.arena.allocator().alloc(u8, 8192);
        _ = try q.arena.allocator().alloc(u8, 8192);
        p.unref(gpa);
        q.unref(gpa);
        try std.testing.expect(pool.retained > 0 and pool.retained <= pool.limit);
        try std.testing.expect(ledger.used.load(.monotonic) >= pool.retained);
        const reused = try pool.take();
        try std.testing.expect(reused == p or reused == q);
        try std.testing.expectEqual(@as(usize, 0), pool.retained);
        reused.unref(gpa);
    }
    try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}

test "piece allocation failures release their rows" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            var pool: PiecePool = .{ .gpa = gpa, .rows_alloc = gpa, .io = std.testing.io, .limit = 0 };
            defer pool.deinit();
            const p = try pool.take();
            defer p.unref(gpa);
            _ = try p.arena.allocator().alloc(u8, 8192);
        }
    }.run, .{});
}

test "encode reservations cover placement and retained regions become charged pages" {
    const gpa = std.testing.allocator;
    var ledger: core_memory.SharedBudget = .{ .limit = 64 << 20 };
    var charged: core_memory.Charged = .{ .child = gpa, .budget = &ledger };
    var regions = [_]WorkRegions{.init(gpa)};
    regions[0].rows.budget = &ledger;
    regions[0].work.budget = &ledger;
    var w: WinCtx = undefined;
    w.gpa = gpa;
    w.regions = &regions;
    w.memory = &charged;
    w.encode_pages = .{ .child = charged.allocator() };
    const job = try EncodeJob.create(&w, 1);
    job.charge = 8 << 20;
    job.permit = .encoder;
    job.reserve();
    const memory = job.claimMemory();
    _ = try memory.keep.alloc(u8, 3 << 20);
    try std.testing.expectEqual(@as(usize, 8 << 20), ledger.used.load(.monotonic));
    job.destroy();
    try std.testing.expect(ledger.used.load(.monotonic) >= 3 << 20);
    const inline_job = try EncodeJob.create(&w, 1);
    inline_job.charge = 8 << 20;
    inline_job.reserve();
    _ = try inline_job.claimMemory().keep.alloc(u8, 3 << 20);
    inline_job.destroy();
    try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
    regions[0].deinit();
    try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}

test "a byte-copy sink error wins over later allocation and producer errors" {
    const gpa = std.testing.allocator;
    var allocations = std.testing.FailingAllocator.init(gpa, .{ .fail_index = 0 });
    var calls: usize = 0;
    const rg: schema.RowGroup = .{ .columns = .empty, .total_byte_size = 0, .num_rows = 1 };
    const jobs = [_]RGJob{
        .{ .bytes = &.{}, .meta = undefined, .rg = &rg },
        .{ .bytes = &.{}, .meta = undefined, .rg = &rg },
    };
    var slots = [_]Slot{
        .{ .meta_arena = .init(gpa), .enc = .{ .rg = rg, .surviving = 1 }, .done = .init(true) },
        .{ .meta_arena = .init(gpa), .enc = .{ .err = error.OutOfMemory }, .done = .init(true) },
    };
    var row_groups: std.ArrayListUnmanaged(schema.RowGroup) = .empty;
    var window: std.Io.Semaphore = .{};
    var w: WinCtx = undefined;
    w.gpa = gpa;
    w.io = std.testing.io;
    w.rg_target = .{};
    w.core = .{};
    w.slots = &slots;
    w.jobs = &jobs;
    w.profiler = null;
    w.first_err = null;
    w.first_failure = null;
    w.out_offset = 4;
    w.rows_kept = 0;
    w.rgs_kept = 0;
    w.sink_ns = 0;
    w.inflight = .init(0);
    w.window = &window;
    w.win_unit = 1;
    w.win_units = 1;
    w.arena = allocations.allocator();
    w.new_row_groups = &row_groups;
    w.sink = .{ .ctx = &calls, .write_fn = struct {
        fn write(ctx: *anyopaque, _: []const u8) !void {
            const count: *usize = @ptrCast(@alignCast(ctx));
            count.* += 1;
            return error.DiskQuota;
        }
    }.write };
    winWriter(&w);
    try std.testing.expectEqual(error.DiskQuota, w.first_err.?);
    try std.testing.expectEqual(@as(usize, 1), calls);
    try std.testing.expectEqual(@as(usize, 0), row_groups.items.len);
    try std.testing.expectEqual(@as(i64, 0), w.rows_kept);
    try std.testing.expect(!allocations.has_induced_failure);
}

test "a failed older encode cancels the unstarted job at count and byte bounds under allocation failures" {
    for ([_]bool{ false, true }) |byte_bound| {
        try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
            fn run(gpa: std.mem.Allocator, bytes: bool) !void {
                var w: WinCtx = undefined;
                w.gpa = gpa;
                w.io = std.testing.io;
                w.rg_target = .{};
                w.core = .{};
                w.cpu_budget = if (bytes) 8 else 1;
                var executor: EncodeExecutor = .{
                    .w = &w,
                    .queue = undefined,
                    .buffer = &.{},
                    .workers = w.cpu_budget,
                    .slots = .{},
                    .granted = .init(w.cpu_budget),
                    .byte_limit = if (bytes) 16 else 1 << 20,
                };
                w.executor = &executor;
                w.memory = null;
                w.out_elems = &.{};
                var a = Assembler.init(&w);
                defer a.deinit();
                const older = try EncodeJob.create(&w, 1);
                var owned = true;
                defer if (owned) older.destroy();
                older.charge = 24;
                older.err = error.DiskQuota;
                older.done.store(true, .release);
                try a.pending.append(gpa, older);
                owned = false;
                a.pending_bytes = older.charge;
                a.rows = 1;
                a.bits = 64;
                if (a.seal()) |_| return error.TestUnexpectedResult else |e| switch (e) {
                    error.DiskQuota => {},
                    else => return e,
                }
                try std.testing.expectEqual(@as(usize, 0), a.pending.items.len);
                try std.testing.expectEqual(@as(u64, 0), a.pending_bytes);
            }
        }.run, .{byte_bound});
    }
}


test "windowed dispatch drains a producer failure despite cancellation midway through its window" {
    const Fake = struct {
        const writer = write;
        const producer = produce;
        const n = 32;
        gpa: std.mem.Allocator = std.testing.allocator,
        io: std.Io = undefined,
        window: *std.Io.Semaphore,
        cpu: std.Io.Semaphore = .{},
        completions: std.Io.Semaphore = .{},
        failed: std.Io.Semaphore = .{},
        slots: [n]std.atomic.Value(bool) = @splat(.init(false)),
        first_err: ?anyerror = null,
        answer: ?anyerror = null,
        finished: usize = 0,
        budget: usize,

        fn produce(self: *@This(), i: usize) void {
            self.io.sleep(.fromMilliseconds(1), .awake) catch {};
            self.slots[i].store(true, .release);
            self.completions.post(self.io);
        }
        fn write(self: *@This()) void {
            for (0..n) |i| {
                while (!self.slots[i].load(.acquire)) self.completions.waitUncancelable(self.io);
                if (i == n / 2) {
                    self.first_err = error.InjectedProducerFailure;
                    self.failed.post(self.io);
                }
                self.finished += 1;
                self.window.post(self.io);
            }
        }
        fn run(self: *@This()) void {
            runWindowed(@This(), self, n, self.budget) catch |err| {
                self.answer = err;
            };
        }
    };
    for ([_]usize{ 1, 4, 16 }) |budget| {
        var window: std.Io.Semaphore = .{ .permits = 2 };
        var fake: Fake = .{ .window = &window, .budget = budget };
        var group: std.Io.Group = .init;
        try group.concurrent(std.testing.io, Fake.run, .{&fake});
        fake.failed.waitUncancelable(std.testing.io);
        group.cancel(std.testing.io);
        try std.testing.expectEqual(error.InjectedProducerFailure, fake.answer.?);
        try std.testing.expectEqual(Fake.n, fake.finished);
    }
}

