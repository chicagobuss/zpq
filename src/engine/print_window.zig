//! Row output without ORDER BY or LIMIT, its row groups decoded in parallel. Producers decode, filter and project a
//! row group each into a slot of their own; one writer prints the slots in input order through the serial printer, so
//! the bytes are those of `-j1`. A row group the serial loop would print in slices, or one refused memory beside the
//! others in flight, is printed by the writer in its turn exactly as that loop prints it.
const std = @import("std");
const profile = @import("../profile.zig");
const schema = @import("../core/schema.zig");
const core_memory = @import("../core/memory.zig");
const huge_pages = @import("../core/huge_pages.zig");
const consumer = @import("../core/consumer.zig");
const Diag = @import("../core/diag.zig").Diag;
const nowMonoNs = @import("../clock.zig").monoNs;
const system = @import("../core/system.zig");
const filter_ast = @import("../core/filter/ast.zig");
const filter_prune = @import("../core/filter/prune.zig");
const engine_types = @import("types.zig");
const engine_write_window = @import("write_window.zig");
const engine_print = @import("print.zig");
const Context = engine_types.Context;
const QueryArgs = engine_types.QueryArgs;
const OpenedInputs = @import("input.zig").OpenedInputs;
const RGJob = engine_write_window.RGJob;
const skipRowGroup = engine_write_window.skipRowGroup;
const runWindowed = engine_write_window.runWindowed;
const StdoutWriter = engine_print.StdoutWriter;
const SlicedPrint = engine_print.SlicedPrint;

pub const Counts = struct { groups: usize, pruned: usize };

/// The row groups not pruned, in input order, when they are worth printing in parallel; null leaves the print to the
/// serial loop. Pruned row groups are counted, given back to their fetch windows and profiled here either way.
pub fn plan(
    ctx: Context,
    arena: std.mem.Allocator,
    args: QueryArgs,
    opened: OpenedInputs,
    metas: []const schema.FileMetaData,
    filter: ?filter_ast.Filter,
    limit: ?usize,
    parquet: bool,
) !?Plan {
    // A limit stops at a row group whose rows are not known before it is read, and slices to the rows still wanted;
    // a Parquet print cuts its own row groups. Both stay serial.
    if (limit != null or parquet) return null;
    const threads = if (args.parallelism > 0) args.parallelism else system.discoverAvailableParallelism(ctx.env);
    if (threads <= 1) return null;
    const prof = profile.threadOf(args.profiler, .main);
    var jobs: std.ArrayList(RGJob) = .empty;
    var counts: Counts = .{ .groups = 0, .pruned = 0 };
    for (opened.inputs, metas, 0..) |in, *meta, file_i| {
        for (meta.row_groups.items, 0..) |*rg, rg_i| {
            counts.groups += 1;
            if (filter) |f| if (!args.scan_all) {
                if (filter_prune.pruneRowGroup(rg, f, meta) == .skip) {
                    counts.pruned += 1;
                    skipRowGroup(in, rg_i);
                    const span = profile.Span.begin(prof);
                    span.end(.row_group, profile.args(.{ file_i, rg_i, 0, rg.num_rows }));
                    continue;
                }
            };
            try jobs.append(arena, .{
                .bytes = in.bytes,
                .meta = meta,
                .rg = rg,
                .file_index = file_i,
                .rg_index = rg_i,
                .gate = in.gate,
                .file_rg = rg_i,
            });
        }
    }
    return .{ .jobs = jobs.items, .threads = threads, .counts = counts };
}

pub const Plan = struct {
    jobs: []const RGJob,
    threads: usize,
    counts: Counts,
};

/// Print `p.jobs` through `out`, the writer's printer: what the serial loop does, row group by row group, with the
/// decoding ahead of it spread over the producers.
pub fn run(
    ctx: Context,
    arena: std.mem.Allocator,
    args: QueryArgs,
    p: Plan,
    template: *consumer.OutputAggregator,
    filter: ?filter_ast.Filter,
    fetch_arr: []const bool,
    output_specs: []const consumer.OutputCol,
    out: SlicedPrint,
    core: *consumer.Timings,
) !void {
    if (p.jobs.len == 0) return;
    // Producers hold half the budget, the serial printer's slices the other half (`SlicedPrint.budget`). Each row
    // group's decoded working set and its rows waiting for the writer draw from it.
    var ledger: core_memory.SharedBudget = .{ .limit = out.budget };
    defer if (args.profiler) |prof| prof.noteLedger(ledger.peak.load(.monotonic), ledger.limit);
    // As many producers as the average row group's decode fits beside the others, so the ledger rarely refuses one.
    var need: u64 = 0;
    for (p.jobs) |job| need +|= estimate(job, fetch_arr);
    need = @max(1, need / p.jobs.len);
    const producers = std.math.clamp(ledger.limit / need, 1, @min(p.threads, p.jobs.len));
    if (producers <= 1) return serial(ctx, args, p, template, filter, fetch_arr, output_specs, out);

    // A row group's decode is tens to hundreds of MiB of short-lived buffers: kept from one row group to the next, its
    // pages are faulted once per producer rather than once per row group on every thread.
    const scratch = try arena.alloc(Scratch, producers);
    for (scratch) |*r| {
        r.* = .{ .region = .init(ctx.gpa, engine_write_window.ENCODE_REGION_RESERVE) };
        r.region.budget = &ledger;
        r.region.allowance = huge_pages.huge_page_size;
    }
    defer for (scratch) |*r| r.region.deinit();
    const slots = try arena.alloc(Slot, p.jobs.len);
    for (slots) |*s| s.* = .{ .charged = .{ .child = ctx.gpa, .budget = &ledger } };
    // Twice the producers: one row group each being decoded, one each decoded and waiting for the writer.
    var window: std.Io.Semaphore = .{ .permits = @min(p.jobs.len, 2 * producers) };
    var completions: std.Io.Semaphore = .{ .permits = 0 };
    var w: PrintCtx = .{
        .gpa = ctx.gpa,
        .worker_io = ctx.worker_io,
        .jobs = p.jobs,
        .slots = slots,
        .scratch = scratch,
        .ledger = &ledger,
        .window = &window,
        .completions = &completions,
        .template = template,
        .filter = filter,
        .fetch_arr = fetch_arr,
        .output_specs = output_specs,
        .whole_budget = out.budget,
        // Text from the first row on: an offset's skipped rows are counted off by the writer, a row at a time.
        .preformat = out.writer.parquet == null and out.writer.columnar == null and out.writer.skip_remaining == 0,
        .out = out,
        .profiler = args.profiler,
    };
    w.out.counts = &w.counts;
    try runWindowed(PrintCtx, &w, p.jobs.len, producers);
    core.decode_ns += w.counts.decode_ns;
    core.eval_ns += w.counts.eval_ns;
    core.rows_scanned += w.counts.rows_scanned;
    core.rows_matched += w.counts.rows_matched;
}

/// The serial loop over planned row groups, for a budget that holds only one at a time.
fn serial(
    ctx: Context,
    args: QueryArgs,
    p: Plan,
    agg: *consumer.OutputAggregator,
    filter: ?filter_ast.Filter,
    fetch_arr: []const bool,
    output_specs: []const consumer.OutputCol,
    out: SlicedPrint,
) !void {
    const prof = profile.threadOf(args.profiler, .main);
    for (p.jobs) |job| {
        const span = profile.Span.begin(prof);
        defer span.end(.row_group, profile.args(.{ job.file_index, job.file_rg, 2, job.rg.num_rows }));
        try job.arrive();
        defer job.leave();
        try engine_print.printRowGroup(ctx.gpa, agg, job.rg, job.meta, src(job), filter, fetch_arr, output_specs, out);
    }
}

fn src(job: RGJob) consumer.RGSrc {
    return .{ .bytes = job.bytes, .byte_origin = 0 };
}

/// What the serial printer weighs a row group by: its fetched columns decoded, then buffered for printing.
fn estimate(job: RGJob, fetch_arr: []const bool) usize {
    return consumer.decodeEstimate(.{
        .rg = job.rg,
        .meta = job.meta,
        .src = src(job),
        .filter = null,
        .read = fetch_arr,
        .consumed = fetch_arr,
    }, fetch_arr, &.{}, false) *| 2;
}

const Slot = struct {
    /// Draws this row group's decode and rows from the producers' ledger; its own, so a refusal is this slot's.
    charged: core_memory.Charged,
    /// The rows, private to the producer until `done`, then the writer's, which frees them once printed.
    arena: ?std.heap.ArenaAllocator = null,
    agg: consumer.OutputAggregator = undefined,
    matched: usize = 0,
    /// Left to the writer, which prints it as the serial loop does, in its turn; its input is the writer's to release.
    deferred: bool = false,
    timings: consumer.Timings = .{},
    /// The rows printed by the producer (`PrintCtx.preformat`), each a JSON array's with its separating comma; the
    /// complete ones counted in `rows`. Up to the failing value when `err` came from printing it, as -j1 prints.
    text: ?std.Io.Writer.Allocating = null,
    rows: usize = 0,
    /// The column printing failed on.
    diag: Diag = .{},
    err: ?anyerror = null,
    /// The chunk the producer last decoded: where an error raised in it, or in printing its rows, is reported.
    failure: ?consumer.DecodeSite = null,
    done: std.atomic.Value(bool) = .init(false),
};

/// A producer's decode memory. Claimed by one producer at a time; there are as many as can run at once.
const Scratch = struct {
    region: huge_pages.Region,
    busy: std.atomic.Value(bool) = .init(false),

    fn claim(all: []Scratch) ?*Scratch {
        for (all) |*r| if (r.busy.cmpxchgStrong(false, true, .acquire, .monotonic) == null) return r;
        return null;
    }
};

const PrintCtx = struct {
    pub const writer = printWriter;
    pub const producer = printProducer;

    gpa: std.mem.Allocator,
    io: std.Io = undefined, // set in runWindowed once the runtime exists
    worker_io: ?std.Io = null,
    jobs: []const RGJob,
    slots: []Slot,
    scratch: []Scratch,
    ledger: *core_memory.SharedBudget,
    window: *std.Io.Semaphore,
    completions: *std.Io.Semaphore,
    cpu: std.Io.Semaphore = .{},
    template: *consumer.OutputAggregator,
    filter: ?filter_ast.Filter,
    fetch_arr: []const bool,
    output_specs: []const consumer.OutputCol,
    /// A row group whose `estimate` passes it is printed in slices by the serial printer, so the writer takes it.
    whole_budget: usize,
    /// Producers print their rows to text, so the writer only copies it out in order.
    preformat: bool,
    /// After a failure nothing more prints: producers still in the window pass their row groups over.
    stop: std.atomic.Value(bool) = .init(false),
    profiler: ?*profile.Profiler = null,

    // Writer-owned.
    out: SlicedPrint,
    counts: consumer.Timings = .{},
    first_err: ?anyerror = null,
    first_failure: ?consumer.DecodeSite = null,
};

fn printProducer(w: *PrintCtx, i: usize) void {
    const slot = &w.slots[i];
    const job = w.jobs[i];
    defer {
        slot.done.store(true, .release);
        w.completions.post(w.io);
    }
    if (w.stop.load(.acquire)) {
        job.leave();
        return;
    }
    const prof = profile.threadOf(w.profiler, .scan);
    const span = profile.Span.begin(prof);
    defer span.end(.row_group, profile.args(.{ job.file_index, job.file_rg, 2, job.rg.num_rows }));
    if (estimate(job, w.fetch_arr) > w.whole_budget) {
        slot.deferred = true;
        return;
    }
    job.arrive() catch |e| {
        slot.err = e;
        job.leave();
        return;
    };
    // `cpu` admits no more producers than there are regions, so a claim fails only if that invariant breaks; then
    // the row group decodes on the general heap.
    const scratch = Scratch.claim(w.scratch);
    defer if (scratch) |r| {
        // Near the ledger's limit a producer keeps no pages between row groups: they would stand beside the rows
        // waiting for the writer, charged and idle.
        r.region.rewind();
        if (w.ledger.tight()) r.region.trim();
        r.busy.store(false, .release);
    };
    const decode = if (scratch) |r| r.region.allocator() else slot.charged.allocator();
    slot.arena = .init(slot.charged.allocator());
    slot.timings.prof = prof;
    defer slot.timings.prof = null;
    slot.matched = project(w, i, decode) catch |e| {
        slot.arena.?.deinit();
        slot.arena = null;
        const refused = e == error.OutOfMemory and (slot.charged.refused.load(.monotonic) or
            if (scratch) |r| r.region.refused else false);
        if (scratch) |r| r.region.refused = false;
        // Refused beside the row groups in flight, or values expanding past what a whole row group may hold: the
        // writer prints it alone, as the serial loop would.
        if (refused or e == error.DecodeExceededBudget) {
            slot.deferred = true;
            return;
        }
        slot.err = e;
        slot.failure = consumer.decodeFailure();
        job.leave();
        return;
    };
    slot.failure = consumer.decodeFailure();
    job.leave();
    if (!w.preformat) return;
    format(w, slot) catch |e| {
        // Text refused beside the rows in flight: the writer prints the rows, which are still whole.
        if (slot.charged.refused.load(.monotonic)) {
            slot.text.?.deinit();
            slot.text = null;
            return;
        }
        slot.err = e;
    };
    // The rows are text now.
    slot.arena.?.deinit();
    slot.arena = null;
}

fn format(w: *PrintCtx, slot: *Slot) !void {
    slot.text = .init(slot.charged.allocator());
    var out: StdoutWriter = .{ .sink = &slot.text.?.writer, .json_array = w.out.writer.json_array, .diag = &slot.diag };
    // Every row as if one came before it: the writer drops the separating comma of the answer's first.
    var printed: usize = 1;
    defer slot.rows = printed - 1;
    const printing = engine_print.flushAggregator(&slot.agg, w.out.names, &out, w.out.is_jsonl, null, &printed);
    // What was printed before a failing value is printed then, as -j1 prints it.
    const flushed = out.flush();
    try printing;
    try flushed;
}

/// Copy a producer's text out: the rows the serial printer would print here, byte for byte.
fn writeText(w: *PrintCtx, text: []const u8, rows: usize) !void {
    const out = w.out.writer;
    const bytes = if (out.json_array and w.out.total_printed.* == 0 and text.len > 0) text[1..] else text;
    if (rows > 0 and out.output_started_ns == 0) out.output_started_ns = nowMonoNs();
    try out.writeAll(bytes);
    w.out.total_printed.* += rows;
}

fn project(w: *PrintCtx, i: usize, decode: std.mem.Allocator) !usize {
    const slot = &w.slots[i];
    const job = w.jobs[i];
    const a = slot.arena.?.allocator();
    const t = w.template;
    const cols = try a.alloc(consumer.OutputAggregator.ColumnBuf, t.cols.len);
    for (cols, t.cols) |*c, tc| c.* = switch (tc) {
        inline else => |_, tag| @unionInit(consumer.OutputAggregator.ColumnBuf, @tagName(tag), .{}),
    };
    slot.agg = .{
        .arena = a,
        .cols = cols,
        .schema_elems = t.schema_elems,
        .paths = t.paths,
        .num_rows = 0,
        .captured = t.captured,
    };
    return consumer.appendProjectedRGWithOptions(
        &slot.agg,
        decode,
        job.rg,
        job.meta,
        src(job),
        w.filter,
        w.fetch_arr,
        w.output_specs,
        &slot.timings,
        w.out.decode_options,
    );
}

fn printWriter(w: *PrintCtx) void {
    const prof = profile.threadOf(w.profiler, .writer);
    w.counts.prof = prof;
    defer w.counts.prof = null;
    for (w.slots, w.jobs) |*slot, job| {
        while (!slot.done.load(.acquire)) w.completions.waitUncancelable(w.io);
        w.counts.decode_ns += slot.timings.decode_ns;
        w.counts.eval_ns += slot.timings.eval_ns;
        // Nothing prints past a failure.
        if (w.first_err == null) {
            if (slot.deferred) {
                // Its producer passed it on before waiting for its bytes (a gate already waited on, by one refused
                // memory, returns at once). A failed fetch fails here, before any of them is read.
                if (job.arrive()) |_| {
                    printDeferred(w, job) catch |e| fail(w, e, consumer.decodeFailure());
                } else |e| fail(w, e, null);
            } else if (slot.arena != null or slot.text != null) {
                w.counts.rows_scanned += @intCast(job.rg.num_rows);
                w.counts.rows_matched += slot.matched;
                const span = profile.Span.begin(prof);
                const o = w.out;
                const printed = if (slot.text) |*t|
                    writeText(w, t.written(), slot.rows)
                else
                    engine_print.flushAggregator(&slot.agg, o.names, o.writer, o.is_jsonl, null, o.total_printed);
                printed catch |e| fail(w, e, slot.failure);
                span.end(.sink_write, profile.args(.{slot.matched}));
            }
            if (slot.err) |e| {
                if (slot.diag.column.len > 0) Diag.setColumn(w.out.writer.diag, slot.diag.column.get());
                fail(w, e, slot.failure);
            }
        }
        if (slot.deferred) job.leave();
        if (slot.arena) |*a| a.deinit();
        slot.arena = null;
        if (slot.text) |*t| t.deinit();
        slot.text = null;
        w.window.post(w.io);
    }
}

/// A row group no producer took, its bytes in, printed as the serial loop prints it.
fn printDeferred(w: *PrintCtx, job: RGJob) !void {
    const t = w.template;
    try engine_print.printRowGroup(w.gpa, t, job.rg, job.meta, src(job), w.filter, w.fetch_arr, w.output_specs, w.out);
}

fn fail(w: *PrintCtx, e: anyerror, site: ?consumer.DecodeSite) void {
    if (w.first_err != null) return;
    w.first_err = e;
    w.first_failure = site;
    w.stop.store(true, .release);
}
