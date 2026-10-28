const std = @import("std");
const ndv = @import("../ndv.zig");
const schema = @import("../schema.zig");
const consumer = @import("../consumer.zig");
const agg_plan = @import("../agg_plan.zig");
const expr_agg = @import("../expr/agg.zig");
const expr_ast = @import("../expr/ast.zig");
const testing = std.testing;

pub const Error = error{
    NoInputs,
    /// A serialized group key did not match the framing the writer used.
    BadGroupKey,
    SchemaMismatch,
    EmptyAggregate,
    AggSumOverflow,
    GroupKeyAliasRequired,
    UnknownColumn,
    ExceededMemoryBudget,
    /// Two output columns (aggregate aliases, GROUP BY keys, or one of each) share a name.
    DuplicateOutputColumn,
    /// An output column name refers to both a GROUP BY key and an aggregate.
    AmbiguousOutputColumn,
    /// A SQL result column outside an aggregate is no GROUP BY key.
    ColumnMustBeGrouped,
    /// A HAVING literal its result column cannot compare with (a string against a number, or the reverse).
    HavingTypeMismatch,
} || std.mem.Allocator.Error;

/// One file's contribution to a multi-file scan. Bytes can come from
/// mmap (CLI), an S3 range-fetch into a buffer (Lambda / S3-source CLI),
/// or a literal slice (tests). The orchestrator does not care.
pub const Input = struct {
    /// Display name used in error messages. Path or URL.
    name: []const u8,
    bytes: []const u8,
    /// Logical source size. S3 inputs may compact fetched ranges into
    /// `bytes`; keep reporting the object size in query results.
    logical_size: u64 = 0,
    /// Set while `bytes` are still being read in (`io/local_reader.zig`): a worker waits on it before touching a row
    /// group, and reports each finished work item so a done row group's pages can go back.
    gate: ?RowGroupGate = null,
    /// The whole file as mapped, while `bytes` are read in behind `gate`: what a planning read made before any gate
    /// opens takes (the NDV union's dictionaries, which read buffers the reader had not filled, so the plan hung on
    /// I/O progress and read mode), and what a scan run again after a fast path's refusal reads, the reader's window
    /// having moved on. Null without a gate, and for S3 inputs.
    plan_bytes: ?[]const u8 = null,
    /// Beside a gate: the dictionary bytes `MultiAggArgs.dict_probe` asked for are read into `bytes` ahead of every row
    /// group, and planning reads them there once this says they are in, so a reader's input plans from the same
    /// dictionaries a mapped one does.
    dict_wait: ?ndv.Wait = null,

    /// A local file read in behind gates (`io/local_reader.zig`), its mapping beside it; not an S3 input's window.
    pub fn readerBacked(self: Input) bool {
        return self.gate != null and self.plan_bytes != null and !std.mem.startsWith(u8, self.name, "s3://");
    }

    /// Readable again from the start, as a scan run again after a refusal must read it (`runMultiAggregate`).
    pub fn rescannable(self: Input) bool {
        return self.gate == null or self.plan_bytes != null;
    }
};

pub fn allRescannable(inputs: []const Input) bool {
    for (inputs) |in| if (!in.rescannable()) return false;
    return true;
}

/// A fast path that charges its peak to the query's budget where the established path it stands in for does not
/// charge the same way. Each is armed only over rescannable inputs, and a scan it took that the budget refused runs
/// again without it (`runMultiAggregate`): a budget the established path fits never fails the query.
pub const FastPath = enum {
    grouped_distinct_bitmap,
    shared_strings,
    integer_records,
    msd_records,
    dense,
    spill,
    pairs,
    partitioned,
    private_runs,
    /// A plain grouping the cost rules chose over a bounded cut (`fewEntries`, `selective_share`) from facts that
    /// cannot bound its table: its refusal runs the cut instead.
    plain_rule,
    /// Not fast paths but what the scan held: tables filled by several workers off a shared cursor, whose peak depends
    /// on which worker claimed which row groups (a table past a power of two doubles), so whether the budget refuses
    /// them depends on timing; and whether shared partitions, one copy of each group, could answer instead.
    worker_tables,
    partitions_fit,
    /// What the refusal says of one worker: several workers' tables held copies of the same keys, the merge refused,
    /// or on one worker the query plans a cut (`oneWorkerCuts`), so one worker may fit the budget they did not.
    /// Without it a single table must hold what they held and the rows they had not reached, and the scan on one
    /// worker is not run (it would refuse, many times slower).
    one_table_may_fit,
    /// The workers' decode memory came out of the query's ledger beside the rest: one worker, decoding one row group
    /// at a time, holds the least of it.
    decode_memory,
    /// The merge of several workers' tables was refused its maps and lists: one worker's table needs none.
    merge_memory,
};

pub const RowGroupGate = struct {
    ctx: *anyopaque,
    wait: *const fn (ctx: *anyopaque, rg: usize) anyerror!void,
    /// One of the row group's `items` work items is finished, read or skipped.
    done: *const fn (ctx: *anyopaque, rg: usize, items: usize) void,
};

/// Asks for the bytes planning reads from input `file` (`ndv.probeRanges`) to be read first: the reader reads them ahead
/// of every row group (`Input.dict_wait`); a mapped file is advised to read them in at once rather than fault them one
/// by one. Called for every probe range before `ReadStart`.
pub const DictProbe = struct {
    ctx: *anyopaque,
    add: *const fn (ctx: *anyopaque, file: usize, rg: usize, start: u64, end: u64) anyerror!void,
};

/// Starts the reads the scan's gates wait on (`Input.gate`). Called once every `DictProbe` range is asked for and before
/// planning reads any of them: planning never reads a reader's input through its mapping, so its reads queue behind
/// nothing but the probes. Idempotent; a scan given none reads as its inputs are.
pub const ReadStart = struct {
    ctx: *anyopaque,
    start: *const fn (ctx: *anyopaque) anyerror!void,
    /// Stops the reads and gives their buffers back before a scan runs again over the mappings (`runMultiAggregate`).
    abandon: ?*const fn (ctx: *anyopaque) void = null,
    /// Planning has read every probe it will: what was probed only for planning can go back.
    planned: ?*const fn (ctx: *anyopaque) void = null,
};

pub const BorrowedSpan = struct {
    lo: usize,
    hi: usize,

    pub fn contains(spans: []const BorrowedSpan, value: []const u8) bool {
        const at = @intFromPtr(value.ptr);
        var lo: usize = 0;
        var hi = spans.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (spans[mid].lo <= at) lo = mid + 1 else hi = mid;
        }
        return lo > 0 and at < spans[lo - 1].hi and value.len <= spans[lo - 1].hi - at;
    }
};

/// What one GROUP BY result column holds. `nullable` is false only for counts, matching the 1-row output.
pub const GroupColType = struct {
    lane: enum { i64, f64, string, avg, dec, avg_dec },
    /// The DECIMAL type of a `dec` column, or of an `avg_dec` mean.
    dec: ?expr_ast.Dec = null,
    nullable: bool,
    /// The column whose values pass through unchanged (a bare key, min/max of a column), to type the output by.
    source: ?schema.SchemaElement = null,
};

/// One unit of parallel work: a single row group within one input file,
/// optionally restricted to a subset of the query's aggregates/columns.
pub const WorkItem = struct {
    subgroup: ?consumer.RowSubgroup = null,
    position: ?u64 = null,
    file: usize,
    rg: usize,
    agg_start: usize,
    agg_len: usize,
    /// What this item's aggregates read; a row group statistics prove fully matching reads only `columns.consumed`.
    columns: agg_plan.Columns,
    accumulators: []expr_agg.Accumulator,
};

pub const RecordOrdinals = struct {
    pub fn rows(metas: []const schema.FileMetaData) ?usize {
        var n: usize = 0;
        for (metas) |meta| for (meta.row_groups.items) |rg| {
            n = std.math.add(usize, n, std.math.cast(usize, rg.num_rows) orelse return null) catch return null;
        };
        return n;
    }

    /// Work is in input/RG order, but may omit pruned sources. Prefixes include those omissions and empty groups;
    /// neither a survivor count nor gathered position can become an original ordinal.
    pub fn fill(metas: []const schema.FileMetaData, work: []const WorkItem, starts: []u32) bool {
        if (starts.len != work.len or (rows(metas) orelse return false) >= @as(usize, 1) << 31) return false;
        var base: usize = 0;
        var at: usize = 0;
        for (metas, 0..) |meta, fi| for (meta.row_groups.items, 0..) |rg, ri| {
            while (at < work.len and work[at].file == fi and work[at].rg == ri) : (at += 1)
                starts[at] = @intCast(base);
            base += @intCast(rg.num_rows);
        };
        return at == work.len;
    }
};

pub const ColSource = union(enum) {
    key: usize,
    agg: usize,
};

/// A GROUP BY output column, bound once per query: the key or aggregate that fills it and the name it prints under.
/// The JSON and Parquet outputs both take their columns from these.
pub const BoundOutputColumn = struct {
    source: ColSource,
    name: []const u8,
    /// Original expression, reconstructed from retained keys only after the aggregate. The source key is one
    /// retained integer dependency, used for lane classification; names and original projection order stay bound.
    derived: ?expr_ast.Expr = null,
};

/// Zig's 16 MiB default makes worker creation expensive for short scans. Safe to shrink: these workers decode, and
/// every nesting path here is bounded.
pub const WORKER_STACK_SIZE: usize = 1 << 20;

/// Rows a build task claims at a time.
pub const row_block = 4096;

test "owner original ordinals include pruned and empty row groups across files before compact narrowing" {
    const Make = struct {
        fn rg(n: i64) schema.RowGroup {
            return .{ .num_rows = n, .columns = .empty, .total_byte_size = 0 };
        }
        fn meta(rgs: []schema.RowGroup) schema.FileMetaData {
            var list: std.ArrayListUnmanaged(schema.RowGroup) = .initBuffer(rgs);
            list.items = rgs;
            return .{
                .version = 1,
                .schema = .empty,
                .num_rows = 0,
                .created_by = null,
                .row_groups = list,
            };
        }
        fn work(file: usize, rg_index: usize) WorkItem {
            return .{
                .file = file,
                .rg = rg_index,
                .agg_start = 0,
                .agg_len = 1,
                .accumulators = &.{},
                .columns = .{ .read = &.{}, .consumed = &.{}, .preserve = &.{} },
            };
        }
    };
    var rgs0 = [_]schema.RowGroup{ Make.rg(10), Make.rg(0), Make.rg(20) };
    var rgs1 = [_]schema.RowGroup{ Make.rg(5), Make.rg(7) };
    var metas = [_]schema.FileMetaData{ Make.meta(&rgs0), Make.meta(&rgs1) };
    const work = [_]WorkItem{ Make.work(0, 2), Make.work(1, 1) };
    var starts: [2]u32 = undefined;
    try testing.expectEqual(@as(?usize, 42), RecordOrdinals.rows(&metas));
    try testing.expect(RecordOrdinals.fill(&metas, &work, &starts));
    try testing.expectEqualDeep([_]u32{ 10, 35 }, starts);
    try testing.expect(!RecordOrdinals.fill(&metas, &.{ work[1], work[0] }, &starts));
    try testing.expect(!RecordOrdinals.fill(&metas, &work, starts[0..1]));
    rgs0[0].num_rows = 1 << 31;
    try testing.expect(!RecordOrdinals.fill(&metas, &work, &starts));
    rgs0[0].num_rows = -1;
    try testing.expectEqual(null, RecordOrdinals.rows(&metas));
    rgs0[0].num_rows = std.math.maxInt(i64);
    rgs0[2].num_rows = std.math.maxInt(i64);
    try testing.expectEqual(null, RecordOrdinals.rows(&metas));
}

