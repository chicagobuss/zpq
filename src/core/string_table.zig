//! One query-wide exact string identity, shared by full integer GROUP BY and scalar string DISTINCT.
//! Rows carry their hash into a partition lock; only a new global key writes bytes into a chunk arena.
const std = @import("std");
const agg = @import("expr/agg.zig");
const eval = @import("filter/eval.zig");
const selection = @import("filter/selection.zig");
const SelectedBatch = @import("selected_batch.zig").SelectedBatch;
const partagg = @import("partagg.zig");
const memory = @import("memory.zig");
const hw = @import("hw.zig");
const spawn = @import("spawn.zig");

pub fn batchRows() usize {
    const rows = hw.l2BytesPerWorkerOr(256 << 10) / 4 / 16;
    return std.math.floorPowerOfTwo(usize, std.math.clamp(rows, 256, 16384));
}

pub const State = struct {
    gpa: std.mem.Allocator,
    ledger: *memory.SharedBudget,
    calls: []const agg.AggCall,
    key_col: usize,
    keys_only: bool,
    parts: []partagg.Part,
    locals: []Sink,
    shift: u6,
    credit: usize,

    pub fn setupBytes(workers: usize, limit: usize) usize {
        const parts: usize = @as(usize, 1) << partagg.partBits(limit);
        return parts * @sizeOf(partagg.Part) + workers * (@sizeOf(Sink) + batchRows() * 16 +
            (parts * 2 + 1) * @sizeOf(u32));
    }

    pub fn init(
        gpa: std.mem.Allocator,
        ledger: *memory.SharedBudget,
        calls: []const agg.AggCall,
        key_col: usize,
        keys_only: bool,
        workers: usize,
    ) !?State {
        const credit = ledger.drawExact(setupBytes(workers, ledger.limit)) orelse return null;
        errdefer ledger.release(credit);
        const bits = partagg.partBits(ledger.limit);
        const parts = try gpa.alloc(partagg.Part, @as(usize, 1) << bits);
        errdefer gpa.free(parts);
        for (parts) |*p| {
            p.* = .{ .table = .init(gpa, 0) };
            p.table.shared = ledger;
            p.table.compact.configure(calls);
        }
        const locals = try gpa.alloc(Sink, workers);
        for (locals) |*local| local.* = .{};
        errdefer {
            for (locals) |*local| local.deinit(gpa);
            gpa.free(locals);
        }
        for (locals) |*local| {
            local.hashes = try gpa.alloc(u64, batchRows());
            local.order = try gpa.alloc(u32, batchRows());
            local.ids = try gpa.alloc(u32, batchRows());
            local.counts = try gpa.alloc(u32, parts.len + 1);
            local.at = try gpa.alloc(u32, parts.len);
        }
        return .{
            .gpa = gpa,
            .ledger = ledger,
            .calls = calls,
            .key_col = key_col,
            .keys_only = keys_only,
            .parts = parts,
            .locals = locals,
            .shift = @intCast(64 - @as(u7, bits)),
            .credit = credit,
        };
    }

    pub fn bind(self: *State) void {
        for (self.locals, 0..) |*local, i| {
            local.state = self;
            local.phase = i * self.parts.len / self.locals.len;
        }
    }

    pub fn count(self: *const State) usize {
        var n: usize = 0;
        for (self.parts) |*p| n += p.table.groupCount();
        return n;
    }

    pub fn deinit(self: *State) void {
        const Task = struct {
            state: *State,
            cursor: *std.atomic.Value(usize),
            fn run(t: *@This()) void {
                while (true) {
                    const p = t.cursor.fetchAdd(1, .monotonic);
                    if (p >= t.state.parts.len) return;
                    t.state.parts[p].table.deinit();
                }
            }
        };
        var cursor: std.atomic.Value(usize) = .init(0);
        var storage: [64]Task = undefined;
        const tasks = storage[0..@max(1, @min(self.locals.len, storage.len))];
        for (tasks) |*t| t.* = .{ .state = self, .cursor = &cursor };
        if (tasks.len == 1 or !spawn.runTree(Task, tasks, .{ .stack_size = 1 << 20 }, Task.run)) tasks[0].run();
        for (self.locals) |*local| local.deinit(self.gpa);
        self.gpa.free(self.locals);
        self.gpa.free(self.parts);
        self.ledger.release(self.credit);
        self.* = undefined;
    }
};

pub const Sink = struct {
    state: *State = undefined,
    phase: usize = 0,
    hashes: []u64 = &.{},
    order: []u32 = &.{},
    ids: []u32 = &.{},
    counts: []u32 = &.{},
    at: []u32 = &.{},

    fn deinit(self: *Sink, gpa: std.mem.Allocator) void {
        gpa.free(self.hashes);
        gpa.free(self.order);
        gpa.free(self.ids);
        gpa.free(self.counts);
        gpa.free(self.at);
    }

    pub fn startErased(_: *anyopaque, _: u64, _: usize) void {}
    pub fn batchErased(ctx: *anyopaque, view: SelectedBatch, lookup: []const ?usize) anyerror!void {
        const self: *Sink = @ptrCast(@alignCast(ctx));
        return self.batch(view, lookup);
    }

    pub fn batch(self: *Sink, view: SelectedBatch, lookup: []const ?usize) !void {
        try view.validate();
        if (self.state.key_col >= lookup.len) return error.BadColumn;
        const ci = lookup[self.state.key_col] orelse return error.BadColumn;
        if (ci >= view.batch.cols.len or view.batch.cols[ci] != .string) return error.BadColumn;
        // Validate every original index before any mutation; integer folds permit non-monotone gathered views.
        var maximum: u64 = view.batch.num_rows -| 1;
        if (view.input_rows) |rows| {
            maximum = 0;
            for (rows) |r| maximum = @max(maximum, r);
        }
        _ = std.math.add(u64, view.input_base, maximum) catch return error.BadColumn;
        var charged: memory.Charged = .{ .child = std.heap.page_allocator, .budget = self.state.ledger };
        var scratch = std.heap.ArenaAllocator.init(charged.allocator());
        defer scratch.deinit();
        self.windows(view, lookup, ci, &scratch) catch |err| return charged.refusal(err);
    }

    fn windows(
        self: *Sink,
        view: SelectedBatch,
        lookup: []const ?usize,
        ci: usize,
        scratch: *std.heap.ArenaAllocator,
    ) !void {
        var off: usize = 0;
        while (off < view.batch.num_rows) {
            const n = @min(self.hashes.len, view.batch.num_rows - off);
            {
                defer _ = scratch.reset(.retain_capacity);
                const a = scratch.allocator();
                const cols = try a.alloc(eval.Batch.Column, view.batch.cols.len);
                for (cols, view.batch.cols) |*out, col| out.* = try col.sliceRows(off, n);
                const window_batch: eval.Batch = .{ .cols = cols, .num_rows = n };
                var sel = try selection.SelectionVector.init(a, n);
                if (view.selected) |selected| for (0..n) |r| if (!selected.isActive(off + r)) sel.set(r, false);
                const key = cols[ci];
                @memset(self.counts, 0);
                for (0..n) |r| {
                    if (!sel.isActive(r)) continue;
                    const raw = try stringAt(key, r);
                    if (self.state.keys_only and raw == null) {
                        sel.set(r, false);
                        continue;
                    }
                    const hash = std.hash.Wyhash.hash(0, raw orelse &.{});
                    self.hashes[r] = hash;
                    self.counts[@as(usize, @intCast(hash >> self.state.shift)) + 1] += 1;
                }
                for (1..self.counts.len) |p| self.counts[p] += self.counts[p - 1];
                @memcpy(self.at, self.counts[0..self.at.len]);
                for (0..n) |r| if (sel.isActive(r)) {
                    const p: usize = @intCast(self.hashes[r] >> self.state.shift);
                    self.order[self.at[p]] = @intCast(r);
                    self.at[p] += 1;
                };
                for (self.state.calls) |call| if (call.arg) |arg| {
                    if (call.func == .count) continue;
                    const at_col = lookup[arg.col_ref.col_idx] orelse return error.BadColumn;
                    cols[at_col] = try materializeValues(a, cols[at_col], &sel);
                };
                var preps = try agg.prepareGrouped(a, self.state.calls, &window_batch, lookup, &sel);
                defer preps.deinit();
                // Stagger workers rather than row offsets: RG/window boundaries are commonly powers of two.
                const first = self.phase;
                self.phase = (first + 1) % self.state.parts.len;
                for (0..self.state.parts.len) |step| {
                    const p = (first + step) % self.state.parts.len;
                    const rows = self.order[self.counts[p]..self.counts[p + 1]];
                    if (rows.len == 0) continue;
                    const part = &self.state.parts[p];
                    partagg.PartAgg.lock(part);
                    defer partagg.PartAgg.unlock(part);
                    for (rows) |r| {
                        const ordinal = view.input_base + if (view.input_rows) |input| input[off + r] else off + r;
                        self.ids[r] = try part.table.getOrInsertStringHashed(
                            try stringAt(key, r),
                            self.state.calls,
                            ordinal,
                            self.hashes[r],
                        );
                    }
                    try preps.fold(
                        self.state.gpa,
                        part.table.accumulators.items,
                        self.ids,
                        rows,
                        self.state.calls,
                        if (part.table.compact.stride != 0) &part.table.compact else null,
                    );
                }
            }
            off += n;
        }
    }
};

// Prepared integer kernels read row values. Keep key codes intact and widen only selected numeric payloads;
// null and rejected rows may carry poison codes and must never index a dictionary.
fn materializeValues(
    a: std.mem.Allocator,
    col: eval.Batch.Column,
    sel: *const selection.SelectionVector,
) !eval.Batch.Column {
    return switch (col) {
        inline .i32, .i64 => |c, tag| blk: {
            if (!c.codesOnly()) break :blk col;
            const codes = c.codes.?;
            const values = try a.alloc(@TypeOf(c.dict[0]), codes.len);
            @memset(values, 0);
            for (0..sel.len) |r| if (sel.isActive(r) and !agg.isRowNull(col, r)) {
                if (codes[r] >= c.dict.len) return error.BadColumn;
                values[r] = c.dict[codes[r]];
            };
            var out = c;
            out.values = values;
            break :blk @unionInit(eval.Batch.Column, @tagName(tag), out);
        },
        else => col,
    };
}

fn stringAt(col: eval.Batch.Column, row: usize) !?[]const u8 {
    if (agg.isRowNull(col, row)) return null;
    const c = col.string;
    if (c.codes) |codes| {
        if (row >= codes.len or codes[row] >= c.dict.len) return error.BadColumn;
        return c.dict[codes[row]];
    }
    if (row >= c.values.len) return error.BadColumn;
    return c.values[row];
}

const testing = std.testing;

test "shared strings keep one global identity across callbacks and gathered NULL empty selected poison" {
    const a = testing.allocator;
    var ledger: memory.SharedBudget = .{ .limit = 64 << 20 };
    var state = (try State.init(a, &ledger, &.{}, 0, false, 2)).?;
    state.bind();
    {
        defer state.deinit();
        const codes = [_]u32{ 0, 1, 99, 999, 2, 1 };
        const defs = [_]u32{ 1, 1, 0, 1, 1, 1 };
        const col: eval.Batch.Column = .{ .string = .{
            .values = &.{},
            .dict = &.{ "", "long\x00key", "other" },
            .codes = &codes,
            .def_levels = &defs,
            .max_def = 1,
        } };
        const batch: eval.Batch = .{ .cols = &.{col}, .num_rows = codes.len };
        var mask = [_]u64{0b110111};
        const selected: selection.SelectionVector = .{ .allocator = a, .mask = &mask, .len = codes.len };
        const view: SelectedBatch = .{
            .batch = &batch,
            .selected = &selected,
            .input_rows = &.{ 9, 7, 5, 3, 1, 0 },
            .input_base = 100,
        };
        try state.locals[0].batch(view, &.{0});
        try state.locals[1].batch(.{ .batch = &batch, .selected = &selected }, &.{0});
        try testing.expectEqual(@as(usize, 4), state.count());
        var seen: usize = 0;
        for (state.parts) |*part| for (0..part.table.groupCount()) |g| {
            try testing.expect(part.table.first.items[g] < 6);
            seen += 1;
        };
        try testing.expectEqual(@as(usize, 4), seen);
    }
    try testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
    var only = (try State.init(a, &ledger, &.{}, 0, true, 1)).?;
    only.bind();
    {
        defer only.deinit();
        const col: eval.Batch.Column = .{ .string = .{
            .values = &.{ "", "ignored", "x", "x" },
            .def_levels = &.{ 1, 0, 1, 1 },
            .max_def = 1,
        } };
        const batch: eval.Batch = .{ .cols = &.{col}, .num_rows = 4 };
        try only.locals[0].batch(.{ .batch = &batch }, &.{0});
        try only.locals[0].batch(.{ .batch = &batch }, &.{0});
        try testing.expectEqual(@as(usize, 2), only.count());
    }
    try testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}

test "shared strings return all occupied ledger credit after mid-insert refusal" {
    const a = testing.allocator;
    var ledger: memory.SharedBudget = .{ .limit = 4 << 20 };
    const occupied = ledger.drawExact(1 << 20).?;
    defer ledger.release(occupied);
    const raw = try a.alloc(u8, 70000 * 64);
    defer a.free(raw);
    var keys: [64][]const u8 = undefined;
    for (&keys, 0..) |*key, i| {
        const bytes = raw[i * 70000 ..][0..70000];
        @memset(bytes, @intCast(i));
        key.* = bytes;
    }
    {
        var state = (try State.init(a, &ledger, &.{}, 0, false, 1)).?;
        defer state.deinit();
        state.bind();
        const batch: eval.Batch = .{ .cols = &.{.{ .string = .{ .values = &keys } }}, .num_rows = keys.len };
        try testing.expectError(error.ExceededMemoryBudget, state.locals[0].batch(.{ .batch = &batch }, &.{0}));
        try testing.expect(ledger.used.load(.monotonic) <= ledger.limit);
    }
    try testing.expectEqual(occupied, ledger.used.load(.monotonic));
}

fn failingSetup(a: std.mem.Allocator) !void {
    // FailingAllocator's byte counters are not atomic. Keep two-worker setup coverage but retire on this thread;
    // the ordinary allocator tests exercise parallel retirement.
    spawn.injectFailureAfter(0);
    defer spawn.resetFailure();
    var ledger: memory.SharedBudget = .{ .limit = 4 << 20 };
    defer testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic)) catch unreachable;
    var state = (try State.init(a, &ledger, &.{}, 0, true, 2)).?;
    defer state.deinit();
    state.bind();
    const batch: eval.Batch = .{
        .cols = &.{.{ .string = .{ .values = &.{ "first", "second", "third" } } }},
        .num_rows = 3,
    };
    try state.locals[0].batch(.{ .batch = &batch }, &.{0});
}

test "shared strings unwind every failed setup and table allocation" {
    try testing.checkAllAllocationFailures(testing.allocator, failingSetup, .{});
}

test "shared strings materialize coded integer payloads once without reading rejected or NULL poison" {
    const a = testing.allocator;
    var ledger: memory.SharedBudget = .{ .limit = 16 << 20 };
    const col = @import("expr/ast.zig").Expr{ .col_ref = .{
        .col_idx = 1,
        .physical_type = .INT64,
        .expr_type = .i64,
    } };
    const calls = [_]agg.AggCall{
        .{ .func = .count, .alias = "c", .result = .i64, .arg = null, .where = null },
        .{ .func = .sum, .alias = "s", .result = .i64, .arg = col, .where = null },
    };
    {
        var state = (try State.init(a, &ledger, &calls, 0, false, 1)).?;
        defer state.deinit();
        state.bind();
        const cols = [_]eval.Batch.Column{
            .{ .string = .{ .values = &.{ "a", "a", "b", "b" } } },
            .{ .i64 = .{
                .values = &.{},
                .codes = &.{ 0, 999, 99, 1 },
                .dict = &.{ -7, 10 },
                .def_levels = &.{ 1, 1, 0, 1 },
                .max_def = 1,
            } },
        };
        var mask = [_]u64{0b1101};
        const sel: selection.SelectionVector = .{ .allocator = a, .len = 4, .mask = &mask };
        const batch: eval.Batch = .{ .cols = &cols, .num_rows = 4 };
        try state.locals[0].batch(.{ .batch = &batch, .selected = &sel }, &.{ 0, 1 });
        try testing.expectEqual(@as(usize, 2), state.count());
        for (state.parts) |*part| for (0..part.table.groupCount()) |g| {
            const is_a = std.mem.eql(u8, part.table.keyAt(g)[5..], "a");
            try testing.expectEqual(@as(u64, if (is_a) 1 else 2), part.table.readAcc(g, 0, 2).count);
            try testing.expectEqual(@as(i128, if (is_a) -7 else 10), part.table.readAcc(g, 1, 2).sum_i.?);
        };
    }
    try testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}
