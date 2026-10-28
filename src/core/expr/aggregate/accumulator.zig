const std = @import("std");
// Known exception: the facade owns writable thread-local state until the later contract pass.
const agg_threadlocals = @import("../agg.zig");
const Error = @import("types.zig").Error;
const testing = std.testing;
const SharedBudget = @import("../../memory.zig").SharedBudget;
const DistinctSet = @import("distinct.zig").DistinctSet;
const types = @import("types.zig");
const AggCall = types.AggCall;
const DecSum = types.DecSum;
const AvgState = types.AvgState;
const DistinctAcc = @import("distinct.zig").DistinctAcc;
const KeyArena = @import("keys.zig").KeyArena;
const keyEql = @import("../../bytes.zig").keyEql;
const foldExtremeF = @import("scalar.zig").foldExtremeF;

/// Accumulator state. One per AggCall, lives across all RGs of all
/// input files. Tagged union so the dispatcher picks the right
/// kernel at update time without re-checking AggFunc + arg type.
pub const Accumulator = union(enum) {
    count: u64,
    distinct: DistinctAcc,
    /// Integer sum, i128. Keeps i64-column sums precise on big datasets
    /// (10^9 rows × 10^9 values = 10^18) and carries unsigned-64 sums and
    /// i64-overflowing sums (e.g. wide DELTA columns) without wrapping. The
    /// JSON output emits the full i128; only the 1-row result *parquet* column
    /// is INT64 and errors `AggIntTooWide` past i64 range.
    /// `null` until a non-null value is folded in: SQL's sum over no values is
    /// NULL, not 0.
    sum_i: ?i128,
    sum_f: ?f64,
    /// Min/max — `null` means no values seen yet. Distinguishes
    /// "all rows filtered out" from "min was 0". i128 so an unsigned-64
    /// extremum (up to 2^64-1) is representable; signed i64 values widen in.
    min_i: ?i128,
    min_f: ?f64,
    max_i: ?i128,
    max_f: ?f64,
    /// String (BYTE_ARRAY/UTF8) min/max — bytewise/unsigned comparison
    /// (Parquet's TYPE_DEFINED order; == DuckDB's binary collation). The
    /// slice is OWNED: dup'd into the persist allocator on each new winner
    /// so it outlives the per-RG decode arena and the cross-worker merge.
    /// `null` = no value seen yet.
    min_bytes: ?[]const u8,
    max_bytes: ?[]const u8,
    min_arena: ArenaBytes,
    max_arena: ArenaBytes,
    /// Avg always lands as f64. Internal sum is f64 even for int
    /// input — for huge int sums this loses precision past 2^53;
    /// document the caveat. DECIMAL avg is exact: `avg_d`.
    avg: AvgState,
    /// Exact DECIMAL folds over unscaled values (the scale is the call's): sum and avg keep a wide sum and a count,
    /// min/max the extreme seen, null until one is.
    sum_d: DecSum,
    avg_d: DecSum,
    min_d: ?i128,
    max_d: ?i128,

    pub const ArenaBytes = struct {
        store: *KeyArena,
        ref: ?u32 = null,
        key: ?u32 = null,
        skip: u8 = 0,

        pub fn bytes(self: ArenaBytes) ?[]const u8 {
            return if (self.ref) |ref| self.store.get(ref)[self.skip..] else null;
        }

        pub fn consider(self: *ArenaBytes, value: []const u8, is_min: bool) !void {
            if (self.bytes()) |cur| {
                const order = std.mem.order(u8, value, cur);
                if (if (is_min) order != .lt else order != .gt) return;
            }
            if (self.key) |key| {
                const framed = self.store.get(key);
                if (framed.len == value.len + 5 and framed[0] == 1 and
                    std.mem.readInt(u32, framed[1..5], .little) == value.len and keyEql(framed[5..], value))
                {
                    self.ref = key;
                    self.skip = 5;
                    return;
                }
            }
            self.ref = try self.store.append(value);
            self.skip = 0;
        }
    };

    pub fn stringValue(self: Accumulator) ?[]const u8 {
        return switch (self) {
            .min_bytes, .max_bytes => |b| b,
            .min_arena, .max_arena => |b| b.bytes(),
            else => null,
        };
    }

    pub fn init(call: AggCall) Accumulator {
        return switch (call.func) {
            .count => .{ .count = 0 },
            .count_distinct => .{ .distinct = .{} },
            .sum => switch (call.result) {
                .i64 => .{ .sum_i = null },
                .f64 => .{ .sum_f = null },
                .dec => .{ .sum_d = .{} },
                .bytes, .avg_f64, .avg_dec => unreachable,
            },
            .min => switch (call.result) {
                .i64 => .{ .min_i = null },
                .f64 => .{ .min_f = null },
                .bytes => .{ .min_bytes = null },
                .dec => .{ .min_d = null },
                .avg_f64, .avg_dec => unreachable,
            },
            .max => switch (call.result) {
                .i64 => .{ .max_i = null },
                .f64 => .{ .max_f = null },
                .bytes => .{ .max_bytes = null },
                .dec => .{ .max_d = null },
                .avg_f64, .avg_dec => unreachable,
            },
            .avg => if (call.result == .avg_dec) .{ .avg_d = .{} } else .{ .avg = .{ .sum = 0, .count = 0 } },
        };
    }

    /// Combine `src` into `dst`. Both must be the same variant — used
    /// to merge per-thread / per-file accumulators back into a single
    /// final result after a parallel multi-file scan. A null min/max
    /// means "no values seen"; merging skips it.
    /// A count past i64 (footers whose bounded row counts add up past it across files) is `RowCountTooLarge`.
    pub fn merge(dst: *Accumulator, src: Accumulator, allocator: std.mem.Allocator) !void {
        const strings: ?bool = switch (dst.*) {
            .min_bytes, .min_arena => true,
            .max_bytes, .max_arena => false,
            else => null,
        };
        if (strings) |is_min| {
            const sv = src.stringValue() orelse return;
            const cur = dst.stringValue();
            const wins = cur == null or (if (is_min)
                std.mem.order(u8, sv, cur.?) == .lt
            else
                std.mem.order(u8, sv, cur.?) == .gt);
            const loser = if (wins) dst.* else src;
            switch (loser) {
                .min_bytes, .max_bytes => |b| if (b) |owned| allocator.free(owned),
                else => {},
            }
            if (wins) dst.* = src;
            return;
        }
        switch (dst.*) {
            .count => |*c| c.* = try addRowCount(c.*, src.count),
            .distinct => |*d| try d.mergeFrom(src.distinct, allocator),
            .sum_i => |*s| if (src.sum_i) |sv| {
                s.* = (s.* orelse 0) + sv;
            },
            .sum_f => |*s| if (src.sum_f) |sv| {
                s.* = (s.* orelse 0) + sv;
            },
            .min_i => |*m| if (src.min_i) |sv| {
                if (m.* == null or sv < m.*.?) m.* = sv;
            },
            .min_f => |*m| if (src.min_f) |sv| foldExtremeF(true, m, sv),
            .max_i => |*m| if (src.max_i) |sv| {
                if (m.* == null or sv > m.*.?) m.* = sv;
            },
            .max_f => |*m| if (src.max_f) |sv| foldExtremeF(false, m, sv),
            // Pointer copy: both src and the merged dst hold slices dup'd
            // into the shared persist allocator. We free the loser to prevent leaks.
            .min_bytes => |*m| if (src.min_bytes) |sv| {
                if (m.* == null) {
                    m.* = sv;
                } else {
                    const ord = std.mem.order(u8, sv, m.*.?);
                    if (ord == .lt) {
                        allocator.free(m.*.?);
                        m.* = sv;
                    } else {
                        allocator.free(sv);
                    }
                }
            },
            .max_bytes => |*m| if (src.max_bytes) |sv| {
                if (m.* == null) {
                    m.* = sv;
                } else {
                    const ord = std.mem.order(u8, sv, m.*.?);
                    if (ord == .gt) {
                        allocator.free(m.*.?);
                        m.* = sv;
                    } else {
                        allocator.free(sv);
                    }
                }
            },
            .min_arena, .max_arena => unreachable,
            .avg => |*a| {
                a.sum += src.avg.sum;
                a.isum += src.avg.isum;
                a.count += src.avg.count;
            },
            .sum_d => |*d| d.add(src.sum_d.get(), src.sum_d.n),
            .avg_d => |*d| d.add(src.avg_d.get(), src.avg_d.n),
            .min_d => |*m| if (src.min_d) |sv| {
                if (m.* == null or sv < m.*.?) m.* = sv;
            },
            .max_d => |*m| if (src.max_d) |sv| {
                if (m.* == null or sv > m.*.?) m.* = sv;
            },
        }
    }
};

pub fn addRowCount(a: u64, b: u64) error{RowCountTooLarge}!u64 {
    const sum = std.math.add(u64, a, b) catch return error.RowCountTooLarge;
    if (sum > std.math.maxInt(i64)) return error.RowCountTooLarge;
    return sum;
}

/// A destination still being scanned must copy borrowed winners before its source arena can be reset.
pub fn mergePersistent(
    dst: *Accumulator,
    src: *Accumulator,
    allocator: std.mem.Allocator,
    merge: anytype,
) !void {
    switch (dst.*) {
        .min_arena, .max_arena => |*value| {
            if (src.stringValue()) |bytes| try value.consider(bytes, dst.* == .min_arena);
            switch (src.*) {
                .min_bytes, .max_bytes => |b| if (b) |owned| allocator.free(owned),
                else => {},
            }
            if (src.* == .min_arena) src.min_arena.ref = null;
            if (src.* == .max_arena) src.max_arena.ref = null;
            if (src.* == .min_bytes) src.min_bytes = null;
            if (src.* == .max_bytes) src.max_bytes = null;
        },
        else => {
            // Fixed-key destinations keep their established owned-string payload.
            if (src.* == .min_arena or src.* == .max_arena) {
                const bytes = if (src.stringValue()) |b| try allocator.dupe(u8, b) else null;
                src.* = if (src.* == .min_arena) .{ .min_bytes = bytes } else .{ .max_bytes = bytes };
            }
            try merge(dst, src, allocator);
        },
    }
}


test "round 4: copy merge into an empty accumulator keeps the source budget" {
    const a = testing.allocator;
    var budget = SharedBudget{ .limit = 1 << 20, .block = 1 };
    var src: Accumulator = .{ .distinct = .initEmpty(&budget) };
    defer src.distinct.deinit(a);
    var dst: Accumulator = .{ .distinct = .{} };
    defer dst.distinct.deinit(a);
    for (0..4) |v| try src.distinct.insertU64(a, v);
    budget.limit = budget.used.load(.monotonic);
    try testing.expectError(error.ExceededMemoryBudget, dst.merge(src, a));
    try testing.expectEqual(@as(usize, 4), src.distinct.count());
    try testing.expectEqual(budget.limit, budget.used.load(.monotonic));
}

test "round 4: copy merge keeps an installed destination budget" {
    const a = testing.allocator;
    var src_budget = SharedBudget{ .limit = 1 << 20, .block = 1 };
    var dst_budget = SharedBudget{ .limit = @sizeOf(DistinctSet) / 2, .block = 1 };
    var src: Accumulator = .{ .distinct = .initEmpty(&src_budget) };
    defer src.distinct.deinit(a);
    var dst: Accumulator = .{ .distinct = .initEmpty(&dst_budget) };
    defer dst.distinct.deinit(a);
    for (0..4) |v| try src.distinct.insertU64(a, v);
    try testing.expectError(error.ExceededMemoryBudget, dst.merge(src, a));
    try testing.expect(dst.distinct.budgetPtr() == &dst_budget);
    try testing.expectEqual(@as(usize, 0), dst.distinct.count());
    try testing.expectEqual(@as(usize, 0), dst_budget.used.load(.monotonic));
}


/// What a query's string min/max winners hold, drawn from the query's ledger: each work item keeps an owned
/// copy of its winner until the merge, and a value can be any size. A copy is drawn before it is made and given back
/// when the value it replaces is freed; the merge frees losers uncounted, so `settle` sets what the survivors hold.
pub const WinnerCharge = struct {
    ledger: ?*SharedBudget,
    held: std.atomic.Value(usize) = .init(0),

    pub fn adjust(self: *WinnerCharge, before: usize, after: usize) error{ExceededMemoryBudget}!void {
        const l = self.ledger orelse return;
        if (after > before) {
            if (l.drawExact(after - before) == null) return error.ExceededMemoryBudget;
            _ = self.held.fetchAdd(after - before, .monotonic);
        } else if (before > after) {
            l.release(before - after);
            _ = self.held.fetchSub(before - after, .monotonic);
        }
    }

    /// The merge has freed the losers: what is drawn is what `keep` bytes of winners hold.
    pub fn settle(self: *WinnerCharge, keep: usize) void {
        const l = self.ledger orelse return;
        const held = self.held.swap(keep, .monotonic);
        if (held > keep) l.release(held - keep) else l.force(keep - held);
    }
};

/// `cand` as `slot`'s owned winner in place of the one it held. Drawn before the copy, so the copy and the value it
/// replaces are both charged while both are live; a refusal or a failed copy leaves the slot holding live bytes.
pub fn winnerBytes(len: usize) usize {
    if (len == 0) return 0;
    return std.mem.alignForward(usize, len, 16) +| 16;
}

pub fn persistWinner(persist: std.mem.Allocator, slot: *?[]const u8, cand: []const u8) Error!void {
    const charge = agg_threadlocals.winner_charge;
    if (charge) |c| try c.adjust(0, winnerBytes(cand.len));
    const owned = persist.dupe(u8, cand) catch |err| {
        if (charge) |c| c.adjust(winnerBytes(cand.len), 0) catch unreachable;
        return err;
    };
    if (slot.*) |cur| {
        persist.free(cur);
        if (charge) |c| c.adjust(winnerBytes(cur.len), 0) catch unreachable;
    }
    slot.* = owned;
}

