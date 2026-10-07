//! IN-list evaluation microbenchmark.
//!
//! Times `filter.eval.evaluate` on `col IN (k literals)` over one in-memory batch, for three columns: integers drawn
//! from a sparse domain, integers from a dense one, and strings. Literals are half values the batch holds, half not.
//! Instructions and cycles are read from perf_event_open counters around the timed loop only (user space), so parsing
//! the list and building the batch don't count.
//!
//! Usage: in_bench [rows=262144] [min_ms=400] [column=all: sparse|dense|str] [ks=1,8,64,1000,8000]
//! Prints one JSON line per (column, k).

const std = @import("std");
const linux = std.os.linux;
const zpq = @import("zpq");
const schema = zpq.core.schema;
const filter = zpq.core.filter;
const nowMonoNs = zpq.clock.monoNs;

const default_ks = [_]usize{ 1, 8, 64, 1000, 8000 };

const Counter = struct {
    fd: ?linux.fd_t,

    fn open(config: linux.PERF.COUNT.HW) Counter {
        var attr: linux.perf_event_attr = .{ .type = .HARDWARE, .config = @backingInt(config) };
        attr.flags.exclude_kernel = true;
        attr.flags.exclude_hv = true;
        const rc = linux.perf_event_open(&attr, 0, -1, -1, 0);
        if (linux.errno(rc) != .SUCCESS) return .{ .fd = null };
        return .{ .fd = @intCast(rc) };
    }

    fn read(self: Counter) u64 {
        const fd = self.fd orelse return 0;
        var v: u64 = 0;
        _ = linux.read(fd, std.mem.asBytes(&v), 8);
        return v;
    }
};

fn leaf(name: []const u8, t: schema.Type) schema.SchemaElement {
    return .{
        .type = t,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = name,
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var it = std.process.Args.Iterator.init(init.minimal.args);
    _ = it.next();
    const rows: usize = if (it.next()) |s| try std.fmt.parseInt(usize, s, 10) else 1 << 18;
    const min_ms: u64 = if (it.next()) |s| try std.fmt.parseInt(u64, s, 10) else 400;
    const only_col: ?[]const u8 = if (it.next()) |s| (if (std.mem.eql(u8, s, "all")) null else s) else null;
    var ks_list: std.ArrayList(usize) = .empty;
    if (it.next()) |s| {
        var parts = std.mem.splitScalar(u8, s, ',');
        while (parts.next()) |p| try ks_list.append(a, try std.fmt.parseInt(usize, p, 10));
    } else try ks_list.appendSlice(a, &default_ks);

    var meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .empty,
        .num_rows = 0,
        .created_by = null,
        .row_groups = .empty,
    };
    var root = leaf("root", .BOOLEAN);
    root.num_children = 3;
    try meta.schema.append(a, root);
    try meta.schema.append(a, leaf("sparse", .INT64));
    try meta.schema.append(a, leaf("dense", .INT64));
    try meta.schema.append(a, leaf("str", .BYTE_ARRAY));

    var prng = std.Random.DefaultPrng.init(42);
    const r = prng.random();
    const sparse = try a.alloc(i64, rows);
    const dense = try a.alloc(i64, rows);
    const strs = try a.alloc([]const u8, rows);
    for (sparse, dense, strs) |*s, *d, *t| {
        s.* = r.intRangeLessThan(i64, 0, 1 << 40);
        d.* = r.intRangeLessThan(i64, 0, 1 << 16);
        t.* = try std.fmt.allocPrint(a, "k{d:0>9}", .{r.uintLessThan(u32, 1 << 20)});
    }
    const cols = [_]filter.eval.Batch.Column{
        .{ .i64 = .{ .values = sparse } },
        .{ .i64 = .{ .values = dense } },
        .{ .string = .{ .values = strs } },
    };
    const lookup = [_]?usize{ 0, 1, 2 };
    const batch: filter.eval.Batch = .{ .cols = &cols, .num_rows = rows };

    const instr = Counter.open(.INSTRUCTIONS);
    const cycles = Counter.open(.CPU_CYCLES);
    var out_buf: [512]u8 = undefined;

    for ([_][]const u8{ "sparse", "dense", "str" }) |col| for (ks_list.items) |k| {
        if (only_col) |c| if (!std.mem.eql(u8, c, col)) continue;
        var expr: std.ArrayList(u8) = .empty;
        try expr.print(a, "{s} IN (", .{col});
        for (0..k) |i| {
            if (i > 0) try expr.appendSlice(a, ", ");
            const from_data = i % 2 == 0;
            const row = r.uintLessThan(usize, rows);
            if (std.mem.eql(u8, col, "str")) {
                if (from_data) try expr.print(a, "'{s}'", .{strs[row]}) else try expr.print(a, "'x{d}'", .{i});
            } else {
                const v: i64 = if (std.mem.eql(u8, col, "sparse"))
                    (if (from_data) sparse[row] else r.intRangeLessThan(i64, 0, 1 << 40))
                else
                    (if (from_data) dense[row] else r.intRangeLessThan(i64, 0, 1 << 16));
                try expr.print(a, "{d}", .{v});
            }
        }
        try expr.append(a, ')');
        var parse_arena = std.heap.ArenaAllocator.init(gpa);
        defer parse_arena.deinit();
        const f = try filter.parser.parse(parse_arena.allocator(), expr.items, &meta, null);

        var eval_arena = std.heap.ArenaAllocator.init(gpa);
        defer eval_arena.deinit();
        var sel = try filter.selection.SelectionVector.init(gpa, rows);
        defer sel.deinit();
        // One untimed pass warms the caches; then whole passes until `min_ms` has gone by.
        try filter.eval.evaluate(f, &batch, &sel, &lookup, eval_arena.allocator());
        const kept = sel.count();
        var iters: u64 = 0;
        const instr0 = instr.read();
        const c0 = cycles.read();
        const t0 = nowMonoNs();
        while (true) {
            @memset(sel.mask, ~@as(u64, 0));
            if (rows % 64 != 0) sel.mask[sel.mask.len - 1] = (@as(u64, 1) << @intCast(rows % 64)) - 1;
            try filter.eval.evaluate(f, &batch, &sel, &lookup, eval_arena.allocator());
            _ = eval_arena.reset(.retain_capacity);
            iters += 1;
            if (nowMonoNs() - t0 >= min_ms * std.time.ns_per_ms and iters >= 3) break;
        }
        const ns: f64 = @floatFromInt(nowMonoNs() - t0);
        const di: f64 = @floatFromInt(instr.read() - instr0);
        const dc: f64 = @floatFromInt(cycles.read() - c0);
        const n: f64 = @floatFromInt(iters * rows);
        const ipc = if (dc == 0) 0 else di / dc;
        const line = try std.fmt.bufPrint(
            &out_buf,
            "{{\"col\":\"{s}\",\"k\":{d},\"rows\":{d},\"iters\":{d},\"kept\":{d},\"ns_per_row\":{d:.3}," ++
                "\"instr_per_row\":{d:.2},\"cycles_per_row\":{d:.2},\"ipc\":{d:.2}}}\n",
            .{ col, k, rows, iters, kept, ns / n, di / n, dc / n, ipc },
        );
        _ = linux.write(1, line.ptr, line.len);
    };
}
