//! Coordinator primitives. Transport owns connections; query arenas own every decoded value.
const std = @import("std");
const V = std.json.Value;
const A = std.mem.Allocator;

/// Fractional runs cover each input independently of the worker's actual footer count.
pub const Run = struct { input: usize, start: u64, end: u64, of: u64 };
pub const Split = struct { runs: []const Run };

pub fn split(a: A, counts: []const u64, workers: usize) ![]const Split {
    if (workers == 0 or counts.len == 0) return error.NoWorkersOrInputs;
    var total: u64 = 0;
    for (counts) |count| total = try std.math.add(u64, total, @max(1, count));
    const n: usize = @intCast(@min(workers, total));
    const out = try a.alloc(Split, n);
    var input: usize = 0;
    var file_base: u64 = 0;
    for (out, 0..) |*part, k| {
        const lo: u64 = @intCast(@as(u128, k) * total / n);
        const hi: u64 = @intCast(@as(u128, k + 1) * total / n);
        var runs: std.ArrayList(Run) = .empty;
        var pos = lo;
        while (pos < hi) {
            const units = @max(1, counts[input]);
            const file_end = file_base + units;
            if (pos >= file_end) {
                file_base = file_end;
                input += 1;
                continue;
            }
            const end = @min(hi, file_end);
            try runs.append(a, .{ .input = input, .start = pos - file_base, .end = end - file_base, .of = units });
            pos = end;
        }
        part.* = .{ .runs = try runs.toOwnedSlice(a) };
    }
    return out;
}

/// No provider types cross the seam. Every call returns bytes allocated from its own request arena.
pub const Transport = struct {
    ctx: *anyopaque,
    prepared_v1: ?*bool = null,
    aligned_v1: ?*bool = null,
    invoke: *const fn (*anyopaque, A, usize, []const u8) anyerror![]const u8,
};

pub fn parse(a: A, bytes: []const u8) !V {
    return std.json.parseFromSliceLeaky(V, a, bytes, .{ .parse_numbers = false });
}

fn member(v: V, key: []const u8) !V {
    if (v != .object) return error.InvalidWorkerResponse;
    return v.object.get(key) orelse error.InvalidWorkerResponse;
}
fn string(v: V) ![]const u8 {
    if (v != .string) return error.InvalidWorkerResponse;
    return v.string;
}
fn array(v: V) ![]V {
    if (v != .array) return error.InvalidWorkerResponse;
    return v.array.items;
}
fn object() V {
    return .{ .object = .{} };
}
fn put(a: A, v: *V, key: []const u8, value: V) !void {
    try v.object.put(a, key, value);
}
fn seq(a: A) V {
    return .{ .array = std.json.Array.init(a) };
}
fn text(v: []const u8) V {
    return .{ .string = v };
}
fn number(a: A, n: anytype) !V {
    return .{ .number_string = try std.fmt.allocPrint(a, "{d}", .{n}) };
}
fn equal(a: A, left: V, right: V) !bool {
    const l = try std.json.Stringify.valueAlloc(a, left, .{});
    const r = try std.json.Stringify.valueAlloc(a, right, .{});
    return std.mem.eql(u8, l, r);
}

pub fn partialPayload(a: A, request: V, part: Split) ![]const u8 {
    const inputs = try array(try member(request, "inputs"));
    var top = object();
    for (request.object.keys(), request.object.values()) |key, value| {
        var omit = false;
        for ([_][]const u8{ "inputs", "aggregate", "group_by", "filter", "sql", "row_groups", "operation", "plan_version", "plan", "join" }) |k| {
            if (std.mem.eql(u8, key, k)) omit = true;
        }
        if (!omit) try put(a, &top, key, value);
    }
    var partial = object();
    var urls = seq(a);
    var ranges = seq(a);
    for (part.runs) |r| {
        if (r.input >= inputs.len) return error.InvalidSplit;
        try urls.array.append(inputs[r.input]);
        var range = seq(a);
        for ([_]u64{ r.start, r.end, r.of }) |n| try range.array.append(try number(a, n));
        try ranges.array.append(range);
    }
    try put(a, &partial, "inputs", urls);
    try put(a, &partial, "row_groups", ranges);
    try put(a, &partial, "aggregate", try member(request, "aggregate"));
    for ([_][]const u8{ "group_by", "filter", "join" }) |key| {
        if (request.object.get(key)) |v| try put(a, &partial, key, v);
    }
    const joined = if (request.object.get("join")) |v| v != .null else false;
    try put(a, &top, "operation", text(if (joined) "partial_join_aggregate" else "partial_aggregate"));
    try put(a, &top, "plan_version", try number(a, 1));
    try put(a, &top, "partial", partial);
    return std.json.Stringify.valueAlloc(a, top, .{});
}

fn alignedPayload(a: A, request: V, selection: V) ![]const u8 {
    // Share the partial envelope; preparation supplies exact physical ranges instead of fractions.
    const inputs = try array(try member(request, "inputs"));
    const runs = try a.alloc(Run, inputs.len);
    for (runs, 0..) |*r, i| r.* = .{ .input = i, .start = 0, .end = 1, .of = 1 };
    var top = try parse(a, try partialPayload(a, request, .{ .runs = runs }));
    var partial = try member(top, "partial");
    try put(a, &partial, "row_groups", try member(selection, "row_groups"));
    try put(a, &partial, "alignment", try member(selection, "alignment"));
    try put(a, &top, "partial", partial);
    try put(a, &top, "operation", text("partial_aligned_join_aggregate"));
    return std.json.Stringify.valueAlloc(a, top, .{});
}

fn checkWorker(answer: V) !void {
    if (answer != .object) return error.InvalidWorkerResponse;
    if (answer.object.get("error") == null) return;
    const reason = answer.object.get("reason") orelse return error.WorkerFailed;
    if (reason != .string) return error.WorkerFailed;
    inline for (.{ error.SchemaMismatch, error.PlanMismatch, error.RowGroupOutOfRange, error.UnmergeableAggregate, error.UnsupportedPlanVersion, error.ConflictingPlanFields, error.PlanTooDeep, error.InvalidBoundPlan, error.JoinNotLocal, error.MemoryLimitExceeded, error.JoinBuildExceedsBudget, error.ReplicatedJoinNeedsS3, error.JoinBuildReadFailed, error.JoinBuildNoSnapshot, error.JoinIncompleteBuild, error.JoinBuildNotUnique, error.DuplicateBuildKey, error.JoinKeyType, error.JoinPayloadType, error.ExceededMemoryBudget, error.AlignedJoinUnordered, error.AlignedJoinMissingStats, error.AlignedJoinKeyType, error.AlignedJoinNoKeys, error.AlignedJoinNeedsS3, error.AlignedJoinBadRange, error.AlignedJoinSnapshotChanged }) |err| {
        if (std.mem.eql(u8, reason.string, @errorName(err))) return err;
    }
    if (std.mem.eql(u8, reason.string, "MissingField")) return error.WorkerLacksPartialAggregate;
    return error.WorkerRefused;
}

pub fn agree(a: A, answers: []const V) !void {
    if (answers.len == 0) return error.NoWorkerAnswers;
    for (answers) |answer| {
        try checkWorker(answer);
        const version = try member(answer, "partial_version");
        if (version != .number_string or !std.mem.eql(u8, version.number_string, "1"))
            return error.UnsupportedPartialVersion;
        _ = try member(answer, "agg");
        _ = try string(try member(answer, "schema_fingerprint"));
        _ = try string(try member(answer, "plan_digest"));
        _ = try array(try member(answer, "aggregates"));
        _ = try array(try member(answer, "groups"));
        if (!try equal(a, try member(answer, "schema_fingerprint"), try member(answers[0], "schema_fingerprint")))
            return error.SchemaMismatch;
        if (!try equal(a, try member(answer, "plan_digest"), try member(answers[0], "plan_digest")))
            return error.PlanMismatch;
        for ([_][]const u8{ "aggregates", "groups" }) |key| {
            if (!try equal(a, try member(answer, key), try member(answers[0], key))) return error.MergeMismatch;
        }
    }
}

// Preserve decimal JSON numbers rather than round each partial through f64. Bounded arithmetic refuses overflow.
const Decimal = struct {
    coefficient: i256,
    scale: i32,
    fn read(v: V) !Decimal {
        if (v != .number_string) return error.InvalidAggregateState;
        const raw = v.number_string;
        const e = std.mem.indexOfAny(u8, raw, "eE") orelse raw.len;
        const exp = if (e < raw.len) try std.fmt.parseInt(i32, raw[e + 1 ..], 10) else 0;
        var coefficient: i256 = 0;
        var scale: i32 = 0;
        var point = false;
        for (raw[0..e]) |ch| {
            if (ch == '-' or ch == '+') continue;
            if (ch == '.') {
                point = true;
                continue;
            }
            if (ch < '0' or ch > '9') return error.InvalidAggregateState;
            coefficient = try std.math.add(i256, try std.math.mul(i256, coefficient, 10), ch - '0');
            if (point) scale += 1;
        }
        if (raw[0] == '-') coefficient = -coefficient;
        return .{ .coefficient = coefficient, .scale = try std.math.sub(i32, scale, exp) };
    }
    fn aligned(self: Decimal, scale: i32) !i256 {
        const delta = try std.math.sub(i32, scale, self.scale);
        if (delta > 76) return error.Overflow;
        var c = self.coefficient;
        var n = delta;
        while (n > 0) : (n -= 1) c = try std.math.mul(i256, c, 10);
        return c;
    }
    fn value(self: Decimal, a: A) !V {
        return .{ .number_string = try std.fmt.allocPrint(a, "{d}e{d}", .{ self.coefficient, -@as(i64, self.scale) }) };
    }
};
fn add(a: A, left: V, right: V) !V {
    if (left == .null) return right;
    if (right == .null) return left;
    const l = try Decimal.read(left);
    const r = try Decimal.read(right);
    const scale = @max(l.scale, r.scale);
    return (Decimal{ .coefficient = try std.math.add(i256, try l.aligned(scale), try r.aligned(scale)), .scale = scale }).value(a);
}
fn compare(left: V, right: V) !std.math.Order {
    if (left == .number_string and right == .number_string) {
        const l = try Decimal.read(left);
        const r = try Decimal.read(right);
        const scale = @max(l.scale, r.scale);
        return std.math.order(try l.aligned(scale), try r.aligned(scale));
    }
    if (left == .string and right == .string) return std.mem.order(u8, left.string, right.string);
    if (left == .bool and right == .bool) return std.math.order(@intFromBool(left.bool), @intFromBool(right.bool));
    return error.InvalidAggregateState;
}
fn fold(a: A, acc: *V, row: V, aggs: []V) !void {
    for (aggs, 0..) |agg, i| {
        const key = try std.fmt.allocPrint(a, "_a{d}", .{i});
        const fn_name = try string(try member(agg, "function"));
        const v = try member(row, key);
        const old = acc.object.get(key) orelse .null;
        const result = if (std.mem.eql(u8, fn_name, "avg")) blk: {
            var state = object();
            const sum = try add(a, if (old == .null) .null else try member(old, "sum"), try member(v, "sum"));
            const count = try add(a, if (old == .null) .null else try member(old, "count"), try member(v, "count"));
            try put(a, &state, "sum", sum);
            try put(a, &state, "count", count);
            break :blk state;
        } else if (std.mem.eql(u8, fn_name, "sum") or std.mem.eql(u8, fn_name, "count"))
            try add(a, old, v)
        else if (std.mem.eql(u8, fn_name, "min") or std.mem.eql(u8, fn_name, "max")) blk: {
            if (v == .null) break :blk old;
            if (old == .null) break :blk v;
            const ordering = try compare(v, old);
            break :blk if ((ordering == .lt and std.mem.eql(u8, fn_name, "min")) or
                (ordering == .gt and std.mem.eql(u8, fn_name, "max"))) v else old;
        } else return error.UnsupportedMergeFunction;
        try put(a, acc, key, result);
    }
}
fn finish(a: A, acc: V, aggs: []V, groups: []V) !V {
    var out = object();
    for (groups, 0..) |name, i| {
        try put(a, &out, try string(name), try member(acc, try std.fmt.allocPrint(a, "_g{d}", .{i})));
    }
    for (aggs, 0..) |agg, i| {
        var v = try member(acc, try std.fmt.allocPrint(a, "_a{d}", .{i}));
        if (std.mem.eql(u8, try string(try member(agg, "function")), "avg")) {
            const sum = try member(v, "sum");
            const count = try member(v, "count");
            const n = try std.fmt.parseFloat(f64, count.number_string);
            v = if (sum == .null or n == 0) .null else .{ .float = try std.fmt.parseFloat(f64, sum.number_string) / n };
        }
        try put(a, &out, try string(try member(agg, "name")), v);
    }
    return out;
}
pub fn merge(a: A, answers: []const V) !V {
    try agree(a, answers);
    const aggs = try array(try member(answers[0], "aggregates"));
    const groups = try array(try member(answers[0], "groups"));
    for (groups) |group| _ = try string(group);
    for (aggs) |agg| {
        const function = try string(try member(agg, "function"));
        _ = try string(try member(agg, "name"));
        var supported = false;
        for ([_][]const u8{ "count", "sum", "min", "max", "avg" }) |name| {
            if (std.mem.eql(u8, function, name)) supported = true;
        }
        if (!supported) return error.UnsupportedMergeFunction;
    }
    if (groups.len == 0) {
        var acc = object();
        for (answers) |answer| try fold(a, &acc, try member(answer, "agg"), aggs);
        return finish(a, acc, aggs, groups);
    }
    var table: std.array_hash_map.String(V) = .{};
    for (answers) |answer| for (try array(try member(answer, "agg"))) |row| {
        var keys = seq(a);
        for (groups, 0..) |_, i| try keys.array.append(try member(row, try std.fmt.allocPrint(a, "_g{d}", .{i})));
        const encoded = try std.json.Stringify.valueAlloc(a, keys, .{});
        const entry = try table.getOrPut(a, encoded);
        if (!entry.found_existing) {
            entry.value_ptr.* = object();
            for (keys.array.items, 0..) |key, i| try put(a, entry.value_ptr, try std.fmt.allocPrint(a, "_g{d}", .{i}), key);
        }
        try fold(a, entry.value_ptr, row, aggs);
    };
    var out = seq(a);
    for (table.values()) |acc| try out.array.append(try finish(a, acc, aggs, groups));
    return out;
}

test "fanout: fractional split is contiguous, balanced and includes empty inputs" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const runs = try split(arena.allocator(), &.{ 3, 0, 2 }, 2);
    try std.testing.expectEqual(2, runs.len);
    try std.testing.expectEqualDeep(Run{ .input = 0, .start = 0, .end = 3, .of = 3 }, runs[0].runs[0]);
    try std.testing.expectEqual(2, runs[1].runs.len);
    try std.testing.expectEqual(1, runs[1].runs[0].of);
    // Planning huge footer hints must not allocate or iterate once per row group.
    try std.testing.expectEqual(3, (try split(arena.allocator(), &.{1 << 50}, 3)).len);
}

test "fanout: exact wide and decimal sum, AVG partial state, schema refusal" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const template =
        \\{"partial_version":1,"schema_fingerprint":"s","plan_digest":"p","groups":[],"aggregates":[{"function":"sum","name":"s"},{"function":"avg","name":"a"}],"agg":{"_a0":9007199254740993.01,"_a1":{"sum":12,"count":3}}}
    ;
    const v = try parse(a, template);
    const out = try merge(a, &.{ v, v });
    const sum = try Decimal.read(try member(out, "s"));
    try std.testing.expectEqual(@as(i256, 1801439850948198602), sum.coefficient);
    try std.testing.expectEqual(@as(f64, 4), (try member(out, "a")).float);
    var bad = try parse(a, template);
    try put(a, &bad, "schema_fingerprint", text("different"));
    try std.testing.expectError(error.SchemaMismatch, merge(a, &.{ v, bad }));
}

pub const Options = struct {
    workers: usize = 32,
    concurrency: usize = 128,
    counts: ?[]const u64 = null,
    order_by: ?[]const u8 = null,
    limit: ?usize = null,
};

pub const InvokeStat = struct { issue_ns: i64 = 0, elapsed_ns: i64 = 0, request_bytes: usize = 0, response_bytes: usize = 0 };
pub const ScatterResult = struct { answers: []V, stats: []InvokeStat, decode_ns: i64 };

const Slot = struct {
    arena: std.heap.ArenaAllocator,
    payload: []const u8,
    response: ?[]const u8 = null,
    failure: ?anyerror = null,
    stat: InvokeStat = .{},
};
const Scatter = struct {
    transport: Transport,
    slots: []Slot,
    started_ns: i64,
    cursor: std.atomic.Value(usize) = .init(0),
    fn work(self: *Scatter) void {
        while (true) {
            const i = self.cursor.fetchAdd(1, .monotonic);
            if (i >= self.slots.len) return;
            const slot = &self.slots[i];
            const begin_ns = @import("../clock.zig").monoNs();
            slot.response = self.transport.invoke(self.transport.ctx, slot.arena.allocator(), i, slot.payload) catch |err| {
                slot.failure = err;
                continue;
            };
            slot.stat = .{ .issue_ns = begin_ns - self.started_ns, .elapsed_ns = @import("../clock.zig").monoNs() - begin_ns, .request_bytes = slot.payload.len, .response_bytes = slot.response.?.len };
        }
    }
};

/// a is query scratch; backing is a thread-safe, ledger-charged allocator for independent invoke arenas.
pub fn scatter(a: A, backing: A, transport: Transport, payloads: []const []const u8, concurrency: usize) !ScatterResult {
    if (concurrency == 0) return error.NoWorkersOrInputs;
    const slots = try a.alloc(Slot, payloads.len);
    for (slots, payloads) |*slot, payload| slot.* = .{ .arena = .init(backing), .payload = payload };
    defer for (slots) |*slot| slot.arena.deinit();
    var state: Scatter = .{ .transport = transport, .slots = slots, .started_ns = @import("../clock.zig").monoNs() };
    const threads = try a.alloc(std.Thread, @min(concurrency, payloads.len));
    var started: usize = 0;
    // A spawn failure still joins every thread holding a pointer into this stack frame.
    defer for (threads[0..started]) |thread| thread.join();
    for (threads) |*thread| {
        thread.* = try std.Thread.spawn(.{}, Scatter.work, .{&state});
        started += 1;
    }
    for (threads[0..started]) |thread| thread.join();
    started = 0;
    const decode_started_ns = @import("../clock.zig").monoNs();
    const answers = try a.alloc(V, slots.len);
    const stats = try a.alloc(InvokeStat, slots.len);
    for (slots, answers, stats) |slot, *answer, *stat| {
        stat.* = slot.stat;
        if (slot.failure) |err| return err;
        // parse alloc_always detaches strings from the soon-to-be-freed response arena.
        answer.* = try std.json.parseFromSliceLeaky(V, a, slot.response.?, .{
            .parse_numbers = false,
            .allocate = .alloc_always,
        });
    }
    return .{ .answers = answers, .stats = stats, .decode_ns = @import("../clock.zig").monoNs() - decode_started_ns };
}

const Order = struct { name: []const u8, desc: bool };
pub fn order(a: A, value: *V, order_by: ?[]const u8, limit: ?usize) !void {
    if (value.* != .array) return;
    var orders: std.ArrayList(Order) = .empty;
    if (order_by) |raw| {
        var start: usize = 0;
        var quoted = false;
        for (raw, 0..) |ch, i| {
            if (ch == '"') quoted = !quoted;
            if ((ch == ',' and !quoted) or i == raw.len - 1) {
                const end = if (ch == ',') i else i + 1;
                var item = std.mem.trim(u8, raw[start..end], " \t\r\n");
                var desc = false;
                if (std.mem.lastIndexOfScalar(u8, item, ' ')) |space| {
                    const suffix = std.mem.trim(u8, item[space + 1 ..], " \t");
                    if (std.ascii.eqlIgnoreCase(suffix, "asc") or std.ascii.eqlIgnoreCase(suffix, "desc")) {
                        desc = std.ascii.eqlIgnoreCase(suffix, "desc");
                        item = std.mem.trim(u8, item[0..space], " \t");
                    }
                }
                if (item.len >= 2 and item[0] == '"' and item[item.len - 1] == '"') item = item[1 .. item.len - 1];
                if (item.len == 0) return error.InvalidOrderBy;
                for (value.array.items) |row| _ = member(row, item) catch return error.UnknownOrderColumn;
                try orders.append(a, .{ .name = item, .desc = desc });
                start = i + 1;
            }
        }
        if (quoted) return error.InvalidOrderBy;
    }
    // Fallible comparisons are resolved before sorting so a refusal cannot be hidden by a comparator.
    for (orders.items) |key| {
        var prior: ?V = null;
        for (value.array.items) |row| {
            const v = try member(row, key.name);
            if (v == .null) continue;
            if (prior) |p| _ = try compare(p, v);
            prior = v;
        }
    }
    const Context = struct {
        keys: []const Order,
        fn less(self: @This(), l: V, r: V) bool {
            for (self.keys) |key| {
                const lv = l.object.get(key.name).?;
                const rv = r.object.get(key.name).?;
                if (lv == .null and rv == .null) continue;
                if (lv == .null) return false;
                if (rv == .null) return true;
                const cmp = compare(lv, rv) catch unreachable;
                if (cmp != .eq) return if (key.desc) cmp == .gt else cmp == .lt;
            }
            return false;
        }
    };
    std.mem.sort(V, value.array.items, Context{ .keys = orders.items }, Context.less);
    if (limit) |n| value.array.items.len = @min(n, value.array.items.len);
}

pub fn run(a: A, backing: A, transport: Transport, request: V, opts: Options) !V {
    const plan_started_ns = @import("../clock.zig").monoNs();
    if (opts.workers == 0 or opts.concurrency == 0 or opts.concurrency > 256) return error.NoWorkersOrInputs;
    if (request != .object) return error.UnsupportedFanoutQuery;
    for ([_][]const u8{ "sql", "row_groups" }) |key| if (request.object.get(key)) |v| {
        if (v != .null) return error.UnsupportedFanoutQuery;
    };
    const inputs = try array(try member(request, "inputs"));
    if (inputs.len == 0) return error.NoWorkersOrInputs;
    const aggregate = try string(try member(request, "aggregate"));
    if (std.mem.trim(u8, aggregate, " \t\r\n").len == 0) return error.EmptyAggregate;
    var patterned = false;
    for (inputs) |url| if (std.mem.indexOfScalar(u8, try string(url), '*') != null) {
        patterned = true;
    };
    if (patterned) if (request.object.get("join")) |v| {
        if (v != .null) return error.JoinGlobNotSupported;
    };
    var payloads: std.ArrayList([]const u8) = .empty;
    var prepared: ?V = null;
    const joined = if (request.object.get("join")) |v| v != .null else false;
    const strategy = if (request.object.get("join_strategy")) |v| try string(v) else "auto";
    if (!std.mem.eql(u8, strategy, "auto") and !std.mem.eql(u8, strategy, "aligned") and !std.mem.eql(u8, strategy, "replicated")) return error.UnsupportedJoinStrategy;
    if (!joined and !std.mem.eql(u8, strategy, "auto")) return error.AlignedJoinNeedsJoin;
    var alignment_prepared = false;
    var aligned = false;
    if (joined and !std.mem.eql(u8, strategy, "replicated")) {
        var supported = if (transport.aligned_v1) |known| known.* else false;
        if (!supported) {
            const cap = try parse(a, try transport.invoke(transport.ctx, a, 0, "{\"operation\":\"capabilities\"}"));
            const version = if (cap == .object) cap.object.get("aligned_join_version") else null;
            supported = if (version) |v| v == .number_string and std.mem.eql(u8, v.number_string, "1") else false;
            if (supported) if (transport.aligned_v1) |known| {
                known.* = true;
            };
        }
        if (!supported and std.mem.eql(u8, strategy, "aligned")) return error.UnsupportedPlanVersion;
        if (supported) {
            var prep_request = request;
            prep_request.object = try request.object.clone(a);
            try put(a, &prep_request, "operation", text("prepare_aggregate"));
            try put(a, &prep_request, "plan_version", try number(a, 1));
            try put(a, &prep_request, "workers", try number(a, opts.workers));
            const selection = try parse(a, try transport.invoke(transport.ctx, a, 0, try std.json.Stringify.valueAlloc(a, prep_request, .{})));
            alignment_prepared = true;
            var usable = true;
            checkWorker(selection) catch |err| switch (err) {
                error.AlignedJoinUnordered, error.AlignedJoinMissingStats, error.AlignedJoinKeyType, error.AlignedJoinNoKeys, error.AlignedJoinNeedsS3 => {
                    if (std.mem.eql(u8, strategy, "aligned")) return err;
                    usable = false;
                },
                else => return err,
            };
            if (usable) {
                const acknowledged = try member(selection, "aligned_join_version");
                if (acknowledged != .number_string or !std.mem.eql(u8, acknowledged.number_string, "1")) return error.UnsupportedPlanVersion;
                for (try array(try member(selection, "selections"))) |s| try payloads.append(a, try alignedPayload(a, request, s));
                if (payloads.items.len == 0) return error.NoWorkerAnswers;
                aligned = true;
            }
        }
    }
    if (aligned) {} else if (patterned) {
        if (transport.prepared_v1 == null or !transport.prepared_v1.?.*) {
            const cap = try parse(a, try transport.invoke(transport.ctx, a, 0, "{\"operation\":\"capabilities\"}"));
            const version = try member(cap, "prepare_aggregate_version");
            if (version != .number_string or !std.mem.eql(u8, version.number_string, "1")) return error.UnsupportedPlanVersion;
            var supports = false;
            for (try array(try member(cap, "structured_plan_versions"))) |v| {
                if (v == .number_string and std.mem.eql(u8, v.number_string, "1")) supports = true;
            }
            if (!supports) return error.UnsupportedPlanVersion;
            if (transport.prepared_v1) |known| known.* = true;
        }
        var prep_request = request;
        // Clone the map before overriding an operation; caller's request remains unchanged.
        prep_request.object = try request.object.clone(a);
        try put(a, &prep_request, "operation", text("prepare_aggregate"));
        try put(a, &prep_request, "plan_version", try number(a, 1));
        prepared = try parse(a, try transport.invoke(transport.ctx, a, 0, try std.json.Stringify.valueAlloc(a, prep_request, .{})));
        const p = prepared.?;
        const pv = try member(p, "plan_version");
        if (pv != .number_string or !std.mem.eql(u8, pv.number_string, "1")) return error.UnsupportedPlanVersion;
        const urls = try array(try member(p, "inputs"));
        const values = try array(try member(p, "row_group_counts"));
        const counts = try a.alloc(u64, values.len);
        for (values, counts) |v, *n| n.* = try std.fmt.parseInt(u64, v.number_string, 10);
        if (counts.len != urls.len) return error.InvalidWorkerResponse;
        // Exact zero-row ranges still invoke workers, validating empty inputs and their schemas.
        for (try split(a, counts, opts.workers)) |part| {
            var payload = object();
            for (request.object.keys(), request.object.values()) |key, v| {
                if (std.mem.eql(u8, key, "aggregate") or std.mem.eql(u8, key, "group_by") or
                    std.mem.eql(u8, key, "filter") or std.mem.eql(u8, key, "inputs")) continue;
                try put(a, &payload, key, v);
            }
            var selected = seq(a);
            var ranges = seq(a);
            for (part.runs) |r| {
                try selected.array.append(urls[r.input]);
                var range = seq(a);
                try range.array.append(try number(a, r.start));
                try range.array.append(try number(a, if (counts[r.input] == 0) 0 else r.end));
                try ranges.array.append(range);
            }
            try put(a, &payload, "inputs", selected);
            try put(a, &payload, "row_groups", ranges);
            try put(a, &payload, "plan", try member(p, "plan"));
            try put(a, &payload, "plan_version", try number(a, 1));
            try payloads.append(a, try std.json.Stringify.valueAlloc(a, payload, .{}));
        }
    } else {
        const counts = opts.counts orelse blk: {
            const hints = try a.alloc(u64, inputs.len);
            @memset(hints, @intCast(opts.workers / inputs.len + @intFromBool(opts.workers % inputs.len != 0)));
            break :blk hints;
        };
        if (counts.len != inputs.len) return error.InvalidCounts;
        for (try split(a, counts, opts.workers)) |part| try payloads.append(a, try partialPayload(a, request, part));
    }
    const planned_ns = @import("../clock.zig").monoNs();
    const scattered = try scatter(a, backing, transport, payloads.items, opts.concurrency);
    const answers = scattered.answers;
    const scattered_ns = @import("../clock.zig").monoNs();
    // Structured-plan responses predate partial metadata: attach the preparation's merge contract.
    if (prepared) |p| for (answers) |*answer| {
        _ = try member(answer.*, "agg");
        try put(a, answer, "partial_version", try number(a, 1));
        try put(a, answer, "schema_fingerprint", text("prepared"));
        try put(a, answer, "plan_digest", text("prepared"));
        try put(a, answer, "aggregates", try member(p, "aggregates"));
        try put(a, answer, "groups", try member(p, "groups"));
    };
    var result = try merge(a, answers);
    try order(a, &result, opts.order_by, opts.limit);
    var out = object();
    try put(a, &out, "agg", result);
    try put(a, &out, "workers", try number(a, answers.len));
    try put(a, &out, "plan_ms", .{ .float = @as(f64, @floatFromInt(planned_ns - plan_started_ns)) / 1e6 });
    try put(a, &out, "decode_ms", .{ .float = @as(f64, @floatFromInt(scattered.decode_ns)) / 1e6 });
    try put(a, &out, "scatter_ms", .{ .float = @as(f64, @floatFromInt(scattered_ns - planned_ns)) / 1e6 });
    try put(a, &out, "merge_ms", .{ .float = @as(f64, @floatFromInt(@import("../clock.zig").monoNs() - scattered_ns)) / 1e6 });
    try put(a, &out, "footer_ms", if (patterned or alignment_prepared) .null else .{ .integer = 0 });
    try put(a, &out, "check_ms", .{ .integer = 0 });
    var payload_bytes: u64 = 0;
    for (payloads.items) |payload| payload_bytes += payload.len;
    try put(a, &out, "request_bytes", try number(a, payload_bytes));
    try put(a, &out, "protocol", text(if (aligned) "aligned" else if (patterned) "plan" else "partial"));
    var stats = seq(a);
    var response_bytes: usize = 0;
    for (answers, scattered.stats) |answer, stat| {
        try stats.array.append(try workerStats(a, answer, stat));
        response_bytes += stat.response_bytes;
    }
    try put(a, &out, "response_bytes", try number(a, response_bytes));
    try put(a, &out, "worker_stats", stats);
    return out;
}

test "fanout: native scatter codec merge and global top k" {
    const Mock = struct {
        calls: std.atomic.Value(usize) = .init(0),
        fn invoke(ctx: *anyopaque, a: A, _: usize, body: []const u8) ![]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            _ = self.calls.fetchAdd(1, .monotonic);
            const req = try parse(a, body);
            const partial = try member(req, "partial");
            _ = try member(partial, "inputs");
            return a.dupe(u8,
                \\{"partial_version":1,"schema_fingerprint":"s","plan_digest":"p","groups":["key"],"aggregates":[{"function":"sum","name":"s"}],"agg":[{"_g0":"a","_a0":1},{"_g0":null,"_a0":9},{"_g0":"b","_a0":2}]}
            );
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var mock: Mock = .{};
    const req = try parse(a, "{\"inputs\":[\"x\"],\"aggregate\":\"sum(x) AS s\",\"group_by\":\"key\"}");
    const out = try run(a, std.heap.smp_allocator, .{ .ctx = &mock, .invoke = Mock.invoke }, req, .{ .workers = 4, .concurrency = 2, .order_by = "s DESC", .limit = 1 });
    try std.testing.expectEqual(4, mock.calls.load(.monotonic));
    const rows = try array(try member(out, "agg"));
    try std.testing.expectEqual(1, rows.len);
    try std.testing.expect((try member(rows[0], "key")) == .null);
    try std.testing.expectEqual(@as(i256, 36), (try Decimal.read(try member(rows[0], "s"))).coefficient);
}

test "fanout: refusal and transport failures never produce partial answers" {
    const Mock = struct {
        fn invoke(_: *anyopaque, _: A, _: usize, _: []const u8) anyerror![]const u8 {
            return error.InvokeHttpFailed;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var ctx: u8 = 0;
    const req = try parse(alloc, "{\"inputs\":[\"missing\"],\"aggregate\":\"count(*)\"}");
    try std.testing.expectError(error.InvokeHttpFailed, run(alloc, std.heap.smp_allocator, .{ .ctx = &ctx, .invoke = Mock.invoke }, req, .{ .workers = 4, .concurrency = 2 }));
    const old = try parse(alloc, "{\"error\":\"bad_json\",\"reason\":\"MissingField\"}");
    try std.testing.expectError(error.WorkerLacksPartialAggregate, agree(alloc, &.{old}));
}

test "fanout: splits preserve coverage for every footer count and malformed hints refuse" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    for ([_]u64{ 0, 1, 3, 8, 100 }) |hint| {
        const parts = try split(alloc, &.{hint}, 16);
        for (0..33) |actual| {
            var covered: [33]u8 = @splat(0);
            for (parts) |part| for (part.runs) |r| {
                const lo: usize = @intCast(r.start * actual / r.of);
                const hi: usize = @intCast(r.end * actual / r.of);
                for (covered[lo..hi]) |*n| n.* += 1;
            };
            for (covered[0..actual]) |n| try std.testing.expectEqual(1, n);
        }
    }
    try std.testing.expectError(error.NoWorkersOrInputs, split(alloc, &.{1}, 0));
    try std.testing.expectError(error.Overflow, split(alloc, &.{ std.math.maxInt(u64), 1 }, 1));
}

test "fanout: only explicit immutable transport acknowledgement is cached; bool versions refuse" {
    const Mock = struct {
        caps: std.atomic.Value(usize) = .init(0),
        bad: bool = false,
        fn invoke(ctx: *anyopaque, alloc: A, _: usize, bytes: []const u8) ![]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const req = try parse(alloc, bytes);
            if (req.object.get("operation")) |op| {
                if (std.mem.eql(u8, op.string, "capabilities")) {
                    _ = self.caps.fetchAdd(1, .monotonic);
                    return alloc.dupe(u8, if (self.bad)
                        "{\"prepare_aggregate_version\":true,\"structured_plan_versions\":[1]}"
                    else
                        "{\"prepare_aggregate_version\":1,\"structured_plan_versions\":[1]}");
                }
                return alloc.dupe(u8, "{\"plan_version\":1,\"plan\":{},\"inputs\":[\"x\"],\"row_group_counts\":[1],\"aggregates\":[{\"function\":\"count\",\"name\":\"n\"}],\"groups\":[]}");
            }
            return alloc.dupe(u8, "{\"agg\":{\"_a0\":1}}");
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const req = try parse(alloc, "{\"inputs\":[\"x*\"],\"aggregate\":\"count(*) AS n\"}");
    var mock: Mock = .{};
    var known = false;
    const transport: Transport = .{ .ctx = &mock, .invoke = Mock.invoke, .prepared_v1 = &known };
    for (0..3) |_| _ = try run(alloc, std.heap.smp_allocator, transport, req, .{ .workers = 1 });
    try std.testing.expectEqual(1, mock.caps.load(.monotonic));
    for (0..3) |_| _ = try run(alloc, std.heap.smp_allocator, .{ .ctx = &mock, .invoke = Mock.invoke }, req, .{ .workers = 1 });
    try std.testing.expectEqual(4, mock.caps.load(.monotonic));
    mock.bad = true;
    try std.testing.expectError(error.UnsupportedPlanVersion, run(alloc, std.heap.smp_allocator, .{ .ctx = &mock, .invoke = Mock.invoke }, req, .{ .workers = 1 }));
}

fn workerStats(a: A, answer: V, measured: InvokeStat) !V {
    var stats = object();
    for ([_][]const u8{ "total_ms", "rows_in", "bytes_in", "row_groups_in", "files_in", "wall_ms", "worker_ms" }) |key| {
        try put(a, &stats, key, answer.object.get(key) orelse .null);
    }
    const total = answer.object.get("total_ms") orelse .null;
    try put(a, &stats, "duration_ms", total);
    var billed: V = .null;
    if (total != .null) {
        if (total != .number_string) return error.InvalidWorkerResponse;
        const ms = try std.fmt.parseFloat(f64, total.number_string);
        if (!std.math.isFinite(ms) or ms < 0 or ms >= @as(f64, @floatFromInt(std.math.maxInt(u64))))
            return error.InvalidWorkerResponse;
        billed = try number(a, @as(u64, @intFromFloat(@ceil(ms))));
    }
    try put(a, &stats, "billed_ms", billed);
    try put(a, &stats, "issue_ms", .{ .float = @as(f64, @floatFromInt(measured.issue_ns)) / 1e6 });
    try put(a, &stats, "invoke_ms", .{ .float = @as(f64, @floatFromInt(measured.elapsed_ns)) / 1e6 });
    try put(a, &stats, "request_bytes", try number(a, measured.request_bytes));
    try put(a, &stats, "response_bytes", try number(a, measured.response_bytes));
    return stats;
}

test "fanout: worker statistics retain cost and phase counters without partial rows" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const answer = try parse(a, "{\"total_ms\":12.2,\"rows_in\":9,\"agg\":[{\"_g0\":\"large result\"}],\"wall_ms\":{\"fetch\":1}}");
    const stats = try workerStats(a, answer, .{ .issue_ns = 2_000_000, .elapsed_ns = 25_000_000, .request_bytes = 30, .response_bytes = 40 });
    try std.testing.expect(stats.object.get("agg") == null);
    try std.testing.expectEqual(@as(i256, 13), (try Decimal.read(try member(stats, "billed_ms"))).coefficient);
    try std.testing.expectEqual(@as(f64, 25), (try member(stats, "invoke_ms")).float);
    try std.testing.expectEqual(@as(f64, 2), (try member(stats, "issue_ms")).float);
    try std.testing.expectEqualStrings("9", (try member(stats, "rows_in")).number_string);
    try std.testing.expect((try member(stats, "wall_ms")) == .object);
}

test "fanout: a legacy worker must not silently ignore a replicated join" {
    const Old = struct {
        fn invoke(_: *anyopaque, a: A, _: usize, bytes: []const u8) ![]const u8 {
            const req = try parse(a, bytes);
            if (req.object.get("inputs") != null) return error.OldWorkerOpenedInput;
            if (std.mem.eql(u8, (try member(req, "operation")).string, "partial_aggregate")) {
                // A legacy aggregate worker would execute this count while ignoring the unknown join member.
                return a.dupe(u8, "{\"partial_version\":1,\"schema_fingerprint\":\"s\",\"plan_digest\":\"p\",\"groups\":[],\"aggregates\":[{\"function\":\"count\",\"name\":\"n\"}],\"agg\":{\"_a0\":100}}");
            }
            return a.dupe(u8, "{\"error\":\"bad_json\",\"reason\":\"BadFieldType\",\"field\":\"operation\"}");
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var ctx: u8 = 0;
    const req = try parse(a, "{\"inputs\":[\"missing.parquet\"],\"aggregate\":\"count(*) AS n\",\"join\":{\"kind\":\"inner\",\"fk\":\"k\",\"build\":{\"path\":\"s3://b/d\",\"key\":\"k\"}}}");
    try std.testing.expectError(error.WorkerRefused, run(a, std.heap.smp_allocator, .{ .ctx = &ctx, .invoke = Old.invoke }, req, .{ .workers = 1 }));
}

test "fanout: empty grouped answers cannot acknowledge an unsupported merge function" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const answer = try parse(a, "{\"partial_version\":1,\"schema_fingerprint\":\"s\",\"plan_digest\":\"p\",\"groups\":[\"k\"],\"aggregates\":[{\"function\":\"median\",\"name\":\"m\"}],\"agg\":[]}");
    try std.testing.expectError(error.UnsupportedMergeFunction, merge(a, &.{answer}));
}

test "fanout aligned: only explicit version acknowledgement is cached; physical ranges use a guarded operation" {
    const Mock = struct {
        caps: usize = 0,
        plans: usize = 0,
        bad: bool = false,
        fn invoke(raw: *anyopaque, a: A, _: usize, bytes: []const u8) ![]const u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const payload = try parse(a, bytes);
            const operation = try string(try member(payload, "operation"));
            if (std.mem.eql(u8, operation, "capabilities")) {
                self.caps += 1;
                return a.dupe(u8, if (self.bad) "{\"aligned_join_version\":true}" else "{\"aligned_join_version\":1}");
            }
            if (std.mem.eql(u8, operation, "prepare_aggregate")) {
                self.plans += 1;
                return a.dupe(u8, "{\"aligned_join_version\":1,\"selections\":[{\"alignment\":{\"low\":1,\"high\":9,\"build\":{\"start\":0,\"end\":1},\"build_snapshot\":{\"etag\":\"e\",\"size\":100},\"probes\":[{\"etag\":\"e\",\"size\":100}]},\"row_groups\":[[0,1]]}]}");
            }
            if (!std.mem.eql(u8, operation, "partial_aligned_join_aggregate")) return error.InvalidWorkerResponse;
            _ = try member(try member(payload, "partial"), "alignment");
            return a.dupe(u8, "{\"partial_version\":1,\"schema_fingerprint\":\"s\",\"plan_digest\":\"p\",\"aggregates\":[{\"function\":\"count\",\"name\":\"n\"}],\"groups\":[],\"agg\":{\"_a0\":1}}");
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const req = try parse(a, "{\"inputs\":[\"x\"],\"aggregate\":\"count(*) AS n\",\"join_strategy\":\"aligned\",\"join\":{}}");
    var mock: Mock = .{};
    var known = false;
    const transport: Transport = .{ .ctx = &mock, .invoke = Mock.invoke, .aligned_v1 = &known };
    for (0..3) |_| _ = try run(a, std.heap.smp_allocator, transport, req, .{ .workers = 1 });
    try std.testing.expectEqual(@as(usize, 1), mock.caps);
    try std.testing.expectEqual(@as(usize, 3), mock.plans);
    known = false;
    mock.bad = true;
    try std.testing.expectError(error.UnsupportedPlanVersion, run(a, std.heap.smp_allocator, transport, req, .{ .workers = 1 }));
    try std.testing.expect(!known);
    try std.testing.expectEqual(@as(usize, 3), mock.plans);
}
