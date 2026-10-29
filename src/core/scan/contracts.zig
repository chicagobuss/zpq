const std = @import("std");
const ndv = @import("../ndv.zig");
const consumer = @import("../consumer.zig");
const agg_plan = @import("../agg_plan.zig");
const expr_agg = @import("../expr/agg.zig");
const expr_ast = @import("../expr/ast.zig");

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

pub const RowGroupGate = struct {
    ctx: *anyopaque,
    wait: *const fn (ctx: *anyopaque, rg: usize) anyerror!void,
    /// One of the row group's `items` work items is finished, read or skipped.
    done: *const fn (ctx: *anyopaque, rg: usize, items: usize) void,
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

