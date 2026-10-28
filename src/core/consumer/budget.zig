const std = @import("std");
const blocking = @import("../blocking.zig");
const expr_agg = @import("../expr/agg.zig");
const decimal_mod = @import("../parquet/decimal.zig");
const AggRowGroup = @import("types.zig").AggRowGroup;

/// The memory a query's scan workers' decoded row groups may take together (`scan.runMultiAggregate` gives it half of
/// `max_memory`). A worker reserves a row group's estimate (`decodeEstimate`) before decoding it and gives it back
/// after, so a row group that does not fit beside the others waits for them. One bigger than a worker's share decodes
/// staged — the filter and keys, then one aggregate's columns at a time on memory handed back after each — and one
/// that does not fit the whole budget even so is refused before anything is allocated, rather than killed by the OS.
pub const DecodeBudget = struct {
    limit: usize,
    workers: usize = 1,
    used: std.atomic.Value(usize) = .init(0),
    /// Bumped by every release; a worker whose row group does not fit sleeps on it until one does.
    released: std.atomic.Value(u32) = .init(0),
    /// Workers asleep in `acquire`: a release with none wakes nobody, and costs no system call.
    waiters: std.atomic.Value(u32) = .init(0),

    /// What one worker may hold without staging: its even share.
    pub fn share(self: *const DecodeBudget) usize {
        return self.limit / @max(1, self.workers);
    }

    /// Others hold the budget for a whole row group's decode, milliseconds at least, so a worker that does not fit
    /// sleeps until a release rather than spin or poll on a core a decoding worker needs (Lambda has two).
    pub fn acquire(self: *DecodeBudget, bytes: usize) error{RowGroupTooLarge}!void {
        if (bytes > self.limit) return error.RowGroupTooLarge;
        if (self.tryAcquire(bytes)) return;
        _ = self.waiters.fetchAdd(1, .seq_cst);
        defer _ = self.waiters.fetchSub(1, .seq_cst);
        while (true) {
            // Read `released` before trying: a release between the two moves it, and the wait returns at once.
            const seen = self.released.load(.seq_cst);
            if (self.tryAcquire(bytes)) return;
            blocking.io.futexWaitUncancelable(u32, &self.released.raw, seen);
        }
    }

    pub fn release(self: *DecodeBudget, bytes: usize) void {
        _ = self.used.fetchSub(bytes, .release);
        _ = self.released.fetchAdd(1, .seq_cst);
        if (self.waiters.load(.seq_cst) > 0) blocking.io.futexWake(u32, &self.released.raw, std.math.maxInt(u32));
    }

    /// `acquire` without waiting: false, holding nothing, when the bytes do not fit now. For optional scratch whose
    /// caller has a path that needs none.
    pub fn tryAcquire(self: *DecodeBudget, bytes: usize) bool {
        while (true) {
            const cur = self.used.load(.monotonic);
            if (cur + bytes > self.limit) return false;
            if (self.used.cmpxchgWeak(cur, cur + bytes, .acquire, .monotonic) == null) return true;
        }
    }
};

/// Bytes decoding `cols` of `in` takes, from its metadata: each leaf's values in its batch lane (a string as a slice
/// plus its page bytes, which the values point into), its levels, and a computed argument's intermediate columns.
/// An estimate, kept on the high side; it decides waiting and staging, not correctness. Saturating: each footer count
/// is bounded (`metadata.max_row_count`), but a wide schema sums many of them.
pub fn decodeEstimate(in: AggRowGroup, cols: []const bool, calls: []const expr_agg.AggCall, grouped: bool) usize {
    const rows: usize = @intCast(@max(0, in.rg.num_rows));
    var total: usize = rows / 8 + 64; // selection
    for (cols, 0..) |needed, ci| {
        if (!needed or ci >= in.rg.columns.items.len) continue;
        const cm = in.rg.columns.items[ci].meta_data orelse continue;
        const n: usize = @intCast(@max(0, cm.num_values));
        const levels = in.meta.getColumnLevels(cm.path_in_schema.items);
        const se = in.meta.getColumnSchema(cm.path_in_schema.items);
        const decimal = if (se) |e| decimal_mod.exactKind(&e) != null else false;
        const lane: usize = switch (cm.type) {
            .BOOLEAN => 1,
            .INT32 => if (decimal) 12 else 8, // a decimal's f64 view beside its integers; unsigned widens to 8
            .FLOAT, .DOUBLE, .INT96 => 8,
            .INT64 => if (decimal) 16 else 8,
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => 24, // slice, or a decimal's f64 view and i128
        };
        total +|= n *| lane;
        if (cm.type == .BYTE_ARRAY or cm.type == .FIXED_LEN_BYTE_ARRAY) {
            total +|= @intCast(@max(0, cm.total_uncompressed_size));
        }
        if (levels.max_def > 0) total +|= n *| 4;
        if (levels.max_rep > 0) total +|= n *| 4;
    }
    // A computed argument materializes a 16-byte column per operator; a grouped scan a group id per row.
    for (calls) |c| if (c.arg) |a| if (a != .col_ref) {
        total +|= rows *| 16 *| a.depth();
    };
    return total +| if (grouped) rows *| 4 else 0;
}


test "a decode budget's waiting worker sleeps until a release makes room, then holds its bytes" {
    var budget: DecodeBudget = .{ .limit = 100 };
    try budget.acquire(80);
    const Waiter = struct {
        fn run(b: *DecodeBudget, got: *std.atomic.Value(bool)) void {
            b.acquire(50) catch return;
            got.store(true, .release);
        }
    };
    var got: std.atomic.Value(bool) = .init(false);
    const t = try std.Thread.spawn(.{}, Waiter.run, .{ &budget, &got });
    for (0..10) |_| {
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        try std.testing.expect(!got.load(.acquire));
    }
    budget.release(80);
    t.join();
    try std.testing.expect(got.load(.acquire));
    try std.testing.expectEqual(@as(usize, 50), budget.used.load(.monotonic));
    try std.testing.expectError(error.RowGroupTooLarge, budget.acquire(101));
    budget.release(50);
}

