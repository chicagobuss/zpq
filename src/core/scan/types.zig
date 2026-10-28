const std = @import("std");
const schema = @import("../schema.zig");
const consumer = @import("../consumer.zig");
const agg_plan = @import("../agg_plan.zig");
const expr_agg = @import("../expr/agg.zig");
const expr_ast = @import("../expr/ast.zig");
const testing = std.testing;

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

