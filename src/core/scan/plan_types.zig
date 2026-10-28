const std = @import("std");
const schema = @import("../schema.zig");
const agg_plan = @import("../agg_plan.zig");
const expr_agg = @import("../expr/agg.zig");
const expr_ast = @import("../expr/ast.zig");
const filter_ast = @import("../filter/ast.zig");
const NdvStat = @import("../ndv.zig").Stat;
const scan_types = @import("types.zig");
const MultiAggArgs = scan_types.MultiAggArgs;
const WorkItem = scan_types.WorkItem;
const BoundOutputColumn = scan_types.BoundOutputColumn;

/// Bound query and footer facts only: choosing a strategy neither allocates nor reads the input.
pub const PlanInputs = struct {
    ndv_stat: ?NdvStat = null,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    work: []const WorkItem,
    calls: []const expr_agg.AggCall,
    keys: ?[]const expr_ast.Expr,
    outputs: ?[]const BoundOutputColumn,
    filter_cols: []const bool,
    workers: usize,
    /// Some input is read in behind the reader's gates (`Input.readerBacked`), which holds a window of its own.
    readers: bool = false,
    filter: ?filter_ast.Filter = null,
    /// The share of rows the filter passes in the first row group the scan reads, counted (`sampledPassShare`).
    sampled_pass: ?f64 = null,
    /// Exact retained-domain reservation, only when the existing dense fold can consume this plan.
    dense_bytes: ?usize = null,
    /// The count was asked for but the inputs' bytes do not hold that row group (S3, explained from its footer): the
    /// estimate decides alone, as the execution reading them would not.
    sample_unread: bool = false,
};

pub const Reason = enum {
    small_ndv,
    unknown_ndv,
    census_needs_uniqueness_evidence,
    census_fixed_width_bail,
    census_short_string_composite,
    census_short_string_payload,
    high_cardinality_strings,
    worker_copy_memory,
    dictionary_union,
    dictionary_union_budget,
    dictionary_union_incomplete,
    state_memory_unknown,
    partitioned_forced,
    partitioned_ineligible,
    sketch_disabled,
    sketch_memory_limit,
    sketch_ineligible,
    sketch_count_cut,
    keys_look_unique,
    filter_small,
    ordered_blocks,
    first_keys_cut,
    plain_scan,
    census_disabled,
    census_memory_or_work_limit,
    census_fused,
    census_on_sketch_bail,
    census_distinct,
    result_cut,
    result_cut_unordered,
    result_cut_secondary_keys,
    result_cut_float,
    result_cut_unsupported,
    result_cut_fallible_payload,
    few_dictionary_entries,
    repeated_dictionary_entries,
    selective_filter,
};

pub const AggregateDispatch = struct {
    strategy: enum { scalar, plain_hash, partitioned_hash, sketch, first_keys },
    use_blocks: bool,
    tables: usize,
    count_columns: bool,
    sketch_fits: bool,
    keys_unique: bool,
    filter_small: bool,
    count_agg: ?usize,
    census_allowed: bool,
    census_fused: bool,
    census_cell_repeat_gate: bool = false,
    /// `count_agg` is a count(DISTINCT) the census certifies (`certifiedTopK`), not a COUNT.
    distinct_census: bool = false,
};

/// What a plan was made from, for `--explain`. Rows and row groups are exact; the pass share is an estimate.
pub const PlanFacts = struct {
    rows: u64 = 0,
    row_groups: usize = 0,
    /// The share of rows the filter passes, from row-group bounds (`passShare`); null without a filter.
    pass_share: ?f64 = null,
    /// The share it passes in the first row group the scan reads, counted there; null where no rule asked.
    sampled_pass: ?f64 = null,
    /// Some input is read behind the reader's gates (`Input.readerBacked`).
    readers: bool = false,
    /// The dictionary probe asked of the reader: its ranges, their page-aligned bytes, and whether its cap sent the
    /// reader's inputs to their footers instead.
    probe_ranges: usize = 0,
    probe_bytes: u64 = 0,
    probe_capped: bool = false,
    /// Where the group bound came from, and why; null without a GROUP BY bound to plan from.
    planning: ?Planning = null,
    /// The strategies whose exactness preconditions hold for this query; the cost rules chose `dispatch.strategy`.
    admissible: std.EnumSet(@FieldType(AggregateDispatch, "strategy")) = .{},
};

/// The facts a GROUP BY was planned from: the dictionaries' union, their headers, or the footers alone, and why. A plan
/// can differ by read mode (`--explain` says which it made): a reader's input unions only where a choice turns on it.
pub const Planning = struct {
    source: enum { footers, dictionary_headers, dictionary_union },
    why: enum {
        /// The union ran over mapped inputs.
        mapped,
        /// The union ran over the dictionaries a reader's probe read first.
        reader_probe,
        /// `ZPQ_NDV_UNION=0`.
        union_disabled,
        /// A first-keys cut whose footers say its codes resolve many entries: no bound can change it.
        prefix_cut,
        /// Beside a reader, no choice turns on the union (no count cut, dense fold or count(DISTINCT)).
        reader_no_choice,
        /// The union was wanted for the dense fold alone, which the footers or headers say would decline it.
        fold_declines,
        /// The reader's probe of the dictionaries passed its cap.
        probe_capped,
        /// The union's input or memory gates declined it.
        union_budget,
        /// The union began and gave up: a key past its gates, or a union no choice could use past its size.
        union_incomplete,
        /// Some key has no dictionary in every row group, or the input is remote: footers and headers only.
        no_dictionaries,
    },
};

/// Fixed-size decision; census and plain fallback remain conditional runtime transitions.
pub const ScanPlan = struct {
    ndv_stat: ?NdvStat = null,
    plain_bytes: ?u64 = null,
    group_bytes: ?u64 = null,
    dispatch: AggregateDispatch,
    reasons: std.EnumSet(Reason) = .{},
    facts: PlanFacts = .{},
    /// The filter rule's estimate wants the measured pass share (`PlanInputs.sampled_pass`) before it commits.
    wants_sample: bool = false,
};

pub const AggregateExplanation = struct {
    plan: ScanPlan,
    /// Names copied into the caller's arena: parsed footer scratch may already be gone.
    column_names: []const []const u8,
    columns: agg_plan.Columns,
    count_columns: ?agg_plan.Columns,
    workers: usize,
    work_items: usize,
    /// Preferred bounded record executor, subject to its ledger draw and runtime key/decode guards.
    record_path: ?RecordAdmission = null,
    dense: bool = false,

    pub fn strategyName(self: AggregateExplanation) []const u8 {
        if (self.record_path) |r| if (r.layout == .shared_string_table) return "shared_string_table";
        return if (self.dense) "dense" else @tagName(self.plan.dispatch.strategy);
    }
};

pub const RecordAdmission = struct {
    layout: enum {
        integer_group_stream,
        integer_sum_top_h,
        integer_count_top_h,
        packed_count_top_h,
        byte_group_stream,
        byte_sum_top_h,
        byte_count_top_h,
        msd_count_top_h,
        integer_distinct_top_h,
        integer_distinct,
        byte_distinct,
        pair_distinct,
        grouped_distinct_bitmap,
        shared_string_table,
    },
    reservation: usize,
    partitions: usize = 2048,
};

