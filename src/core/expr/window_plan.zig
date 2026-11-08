//! What a query with window functions is made of, between the SQL layer that reads it (`sql_windows.zig`) and the
//! pass that computes it (`engine/window.zig`): plain data, by column name.

pub const Func = enum { row_number, rank, dense_rank, lag, lead, sum, count, count_star, avg, min, max };

pub const Bound = union(enum) {
    unbounded_preceding,
    preceding: u32,
    current_row,
    following: u32,
    unbounded_following,
};

/// `rows`: a ROWS frame between `start` and `end`. Otherwise the frame is a RANGE one: the default (the partition's
/// start through the last peer of the row, or the whole partition when there is no ORDER BY), or, with `end` at
/// `unbounded_following`, the whole partition.
pub const Frame = struct {
    rows: bool = false,
    start: Bound = .unbounded_preceding,
    end: Bound = .current_row,
};

/// What `lag` and `lead` give past the partition's edge when asked for another value.
pub const Default = union(enum) { null, int: i64, float: f64, string: []const u8 };

/// A call whose argument is a column of the pass's input, by name.
pub const Call = struct {
    func: Func,
    arg: ?[]const u8 = null,
    offset: u32 = 1,
    default: Default = .null,
    frame: Frame = .{},
};

pub const SortKey = struct {
    column: []const u8,
    descending: bool = false,
    /// Null: the default, NULLs last.
    nulls_first: ?bool = null,
};

/// The calls that share one `(PARTITION BY, ORDER BY)`: one pass over the input sorted by it.
pub const Pass = struct {
    sort: []const SortKey,
    partition: []const []const u8,
    order: []const []const u8,
    calls: []const Call,
    /// The name of each call's result column, in order.
    results: []const []const u8,
};

pub const Plan = struct {
    /// The query without its window calls: every column the windows and the rest need as `c0`, `c1`, ..., ordered by
    /// the first pass's sort.
    input_sql: []const u8,
    passes: []const Pass,
    /// The select list and the tail (ORDER BY / LIMIT / OFFSET) of the query over the last pass's file.
    final_items: []const u8,
    final_tail: []const u8,
};
