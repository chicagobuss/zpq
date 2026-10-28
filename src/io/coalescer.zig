const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Range = struct {
    start: u64,
    end: u64,

    pub fn len(self: Range) u64 {
        return self.end - self.start;
    }
};

pub const Coalescer = struct {
    /// Coalesce a list of ranges into a smaller list of merged ranges.
    /// Ranges that are within `gap_threshold` bytes of each other are merged.
    /// The input `ranges` slice is not modified, but the returned slice is allocated.
    ///
    /// The input ranges do NOT need to be sorted; this function will sort them internally if needed,
    /// but since we can't easily clone/sort without allocation, the caller should ideally provide sorted ranges
    /// OR we allocate a temp buffer for sorting.
    ///
    /// For this implementation, we assume the caller passes potentially unsorted ranges, so we allocate a copy to sort.
    pub fn coalesce(allocator: Allocator, ranges: []const Range, gap_threshold: u64) ![]const Range {
        if (ranges.len == 0) return &[_]Range{};

        var sorted = try allocator.alloc(Range, ranges.len);
        defer allocator.free(sorted);
        @memcpy(sorted, ranges);

        std.mem.sort(Range, sorted, {}, struct {
            fn lessThan(_: void, a: Range, b: Range) bool {
                return a.start < b.start;
            }
        }.lessThan);

        var merged = std.ArrayList(Range).empty;
        // We probably won't use more ranges than we started with
        try merged.ensureTotalCapacity(allocator, ranges.len);

        var current = sorted[0];

        for (sorted[1..]) |next| {
            // Check for overlap or proximity
            // Note: Range is [start, end), so end is exclusive?
            // Standard parquet usage usually implies [start, start + len).
            // Let's assume inclusive start, exclusive end for standard zig slice semantics.
            // But wait, our usage in Executor implies 'end' is exclusive (start + len).

            // Gap = next.start - current.end
            // careful with underflow if next starts before current ends (overlap)
            if (next.start <= current.end + gap_threshold) {
                // Merge
                if (next.end > current.end) {
                    current.end = next.end;
                }
            } else {
                merged.appendAssumeCapacity(current);
                current = next;
            }
        }
        merged.appendAssumeCapacity(current);

        return merged.toOwnedSlice(allocator);
    }
};

/// Unmeasured S3 latency uses this gap: wide enough to join nearby column chunks, under the cap below.
pub const s3_gap_default: u64 = 1 << 20;
/// Cap on `bandwidth × first-byte latency`.
pub const s3_gap_max: u64 = 2 << 20;

/// Gap for one S3 object. `rtt_ns == 0` has not been measured, so the default applies. A measured latency uses
/// `min(s3_gap_max, bytes_per_s × rtt)`.
pub fn s3CoalesceGap(rtt_ns: u64, bytes_per_s: u64) u64 {
    if (rtt_ns == 0 or bytes_per_s == 0) return s3_gap_default;
    const gap = bytes_per_s * rtt_ns / std.time.ns_per_s;
    return @min(s3_gap_max, gap);
}

/// One range in a dispatch. `object` is the file: a merge never crosses it. `group` is the row group that will hold
/// the bytes. `gap_cap` is that object's latency limit.
pub const Piece = struct {
    object: u32,
    group: u32,
    start: u64,
    end: u64,
    gap_cap: u64,
};

/// A span the dispatch will fetch and hold. `group` is the row group whose buffer contains it.
pub const PlannedSpan = struct {
    object: u32,
    group: u32,
    start: u64,
    end: u64,

    pub fn len(self: PlannedSpan) u64 {
        return self.end - self.start;
    }
};

/// One plan for every range fetched together. The wave target is `ceil(spans / in_flight)`, counted across objects.
/// Touching ranges in one object (and, for a window, one row group) join first. The remaining gaps are sorted once
/// by size, then object, then offset, and walked in that order: a merge deletes one gap and does not change any
/// other gap's size. A gap is kept only while it fits the latency cap, `amp_cap` (twice the original selected bytes
/// of this dispatch), and `window_cap` when this dispatch is a window. A window also refuses a merge that would
/// cross row groups, so each group's buffer is exactly the spans it is charged for. Call this once. Planning the
/// resulting spans again would treat gap bytes as selected and read more than `amp_cap`.
pub fn coalesceDispatch(
    allocator: Allocator,
    pieces: []const Piece,
    in_flight: usize,
    amp_cap: u64,
    window_cap: ?u64,
) ![]const PlannedSpan {
    return coalesceDispatchInner(allocator, pieces, in_flight, amp_cap, window_cap, null);
}

fn coalesceDispatchInner(
    allocator: Allocator,
    pieces: []const Piece,
    in_flight: usize,
    amp_cap: u64,
    window_cap: ?u64,
    visits: ?*usize,
) ![]const PlannedSpan {
    if (pieces.len == 0) return &.{};

    const Node = struct {
        object: u32,
        group: u32,
        start: u64,
        end: u64,
        gap_cap: u64,
    };
    var live: std.ArrayList(Node) = .empty;
    defer live.deinit(allocator);
    try live.ensureTotalCapacity(allocator, pieces.len);
    const order = try allocator.alloc(usize, pieces.len);
    defer allocator.free(order);
    for (order, 0..) |*slot, i| slot.* = i;
    std.mem.sort(usize, order, pieces, struct {
        fn lessThan(p: []const Piece, a: usize, b: usize) bool {
            if (p[a].object != p[b].object) return p[a].object < p[b].object;
            return p[a].start < p[b].start;
        }
    }.lessThan);

    var max_group: u32 = 0;
    for (pieces) |p| max_group = @max(max_group, p.group);
    const charge = try allocator.alloc(u64, @as(usize, max_group) + 1);
    defer allocator.free(charge);
    @memset(charge, 0);

    for (order) |idx| {
        const p = pieces[idx];
        if (live.items.len != 0) {
            const cur = &live.items[live.items.len - 1];
            const same = cur.object == p.object and (window_cap == null or cur.group == p.group);
            if (same and p.start <= cur.end) {
                if (p.end > cur.end) {
                    charge[cur.group] +|= p.end - cur.end;
                    cur.end = p.end;
                }
                cur.gap_cap = @min(cur.gap_cap, p.gap_cap);
                continue;
            }
        }
        // Saturating here and below: each piece lies in a chunk footer open checked, but a footer can claim many huge
        // ones, and a sum past u64 must read as over every cap rather than wrap under one.
        charge[p.group] +|= p.end - p.start;
        live.appendAssumeCapacity(.{
            .object = p.object,
            .group = p.group,
            .start = p.start,
            .end = p.end,
            .gap_cap = p.gap_cap,
        });
    }

    const Gap = struct {
        size: u64,
        object: u32,
        offset: u64,
        left: u32,
    };
    var gaps: std.ArrayList(Gap) = .empty;
    defer gaps.deinit(allocator);
    try gaps.ensureTotalCapacity(allocator, live.items.len);
    for (live.items[0..live.items.len -| 1], 0..) |s, i| {
        const nxt = live.items[i + 1];
        if (s.object != nxt.object) continue;
        if (window_cap != null and s.group != nxt.group) continue;
        if (nxt.start < s.end) continue;
        gaps.appendAssumeCapacity(.{
            .size = nxt.start - s.end,
            .object = s.object,
            .offset = s.start,
            .left = @intCast(i),
        });
    }
    std.mem.sort(Gap, gaps.items, {}, struct {
        fn lessThan(_: void, a: Gap, b: Gap) bool {
            if (a.size != b.size) return a.size < b.size;
            if (a.object != b.object) return a.object < b.object;
            return a.offset < b.offset;
        }
    }.lessThan);

    const n = live.items.len;
    const parent = try allocator.alloc(u32, n);
    defer allocator.free(parent);
    const span_len = try allocator.alloc(u64, n);
    defer allocator.free(span_len);
    const cap = try allocator.alloc(u64, n);
    defer allocator.free(cap);
    const root_group = try allocator.alloc(u32, n);
    defer allocator.free(root_group);
    const accepted = try allocator.alloc(bool, n);
    defer allocator.free(accepted);
    @memset(accepted, false);
    var total: u64 = 0;
    for (live.items, 0..) |s, i| {
        parent[i] = @intCast(i);
        span_len[i] = s.end - s.start;
        cap[i] = s.gap_cap;
        root_group[i] = s.group;
        total +|= span_len[i];
    }

    const flight = @max(in_flight, 1);
    var count = n;
    for (gaps.items) |g| {
        if (std.math.divCeil(usize, count, flight) catch unreachable <= 1) break;
        if (visits) |v| v.* += 1;
        const left = findRoot(parent, g.left);
        const right = findRoot(parent, g.left + 1);
        const here = @min(cap[left], cap[right]);
        if (g.size > here) continue;
        if (total +| g.size > amp_cap) continue;
        if (window_cap) |limit| {
            const span = span_len[left] +| g.size +| span_len[right];
            if (span > limit or charge[root_group[left]] +| g.size > limit) continue;
        }
        accepted[g.left] = true;
        span_len[left] = span_len[left] +| g.size +| span_len[right];
        cap[left] = here;
        charge[root_group[left]] +|= g.size;
        total +|= g.size;
        parent[right] = left;
        count -= 1;
    }

    var out: std.ArrayList(PlannedSpan) = .empty;
    errdefer out.deinit(allocator);
    var i: usize = 0;
    while (i < n) {
        const first = live.items[i];
        var end = first.end;
        var j = i;
        while (j + 1 < n and accepted[j]) {
            j += 1;
            end = live.items[j].end;
        }
        try out.append(allocator, .{
            .object = first.object,
            .group = first.group,
            .start = first.start,
            .end = end,
        });
        i = j + 1;
    }
    return out.toOwnedSlice(allocator);
}

fn findRoot(parent: []u32, start: u32) u32 {
    var i = start;
    while (parent[i] != i) {
        const p = parent[i];
        parent[i] = parent[p];
        i = p;
    }
    return i;
}

/// One object, one row group. Tests and any caller that already holds a single file's ranges.
pub fn coalesceMinWaves(
    allocator: Allocator,
    ranges: []const Range,
    gap_cap: u64,
    wave: usize,
    byte_cap: u64,
) ![]const Range {
    const pieces = try allocator.alloc(Piece, ranges.len);
    defer allocator.free(pieces);
    for (ranges, pieces) |r, *p| {
        p.* = .{ .object = 0, .group = 0, .start = r.start, .end = r.end, .gap_cap = gap_cap };
    }
    const planned = try coalesceDispatch(allocator, pieces, wave, byte_cap, null);
    defer allocator.free(planned);
    const out = try allocator.alloc(Range, planned.len);
    for (planned, out) |s, *r| r.* = .{ .start = s.start, .end = s.end };
    return out;
}

pub fn overlapsAny(r: Range, others: []const Range) bool {
    for (others) |o| {
        if (r.start < o.end and o.start < r.end) return true;
    }
    return false;
}

test "a dispatch over pieces whose lengths sum past 2^64 plans them without overflow" {
    // Three files, each claiming a chunk of 2^62.8 bytes: each lies within u64, their sum does not. Disjoint pieces of
    // one file cannot get there; the files' totals add up in the same counters.
    const big: u64 = 7 << 60;
    var pieces: [6]Piece = undefined;
    for (0..3) |f| {
        const obj: u32 = @intCast(f);
        pieces[2 * f] = .{ .object = obj, .group = obj, .start = 0, .end = big / 2, .gap_cap = 1 << 20 };
        pieces[2 * f + 1] = .{ .object = obj, .group = obj, .start = big / 2 + 10, .end = big, .gap_cap = 1 << 20 };
    }
    for ([_]?u64{ null, 1 << 30 }) |window| {
        const planned = try coalesceDispatch(std.testing.allocator, &pieces, 1, std.math.maxInt(u64), window);
        defer std.testing.allocator.free(planned);
        // Every byte still planned, each span within its file; a window keeps each piece apart.
        var covered: [3]u64 = @splat(0);
        for (planned) |sp| covered[sp.object] += sp.end - sp.start;
        for (covered) |c| try std.testing.expect(c >= big - 10);
        if (window != null) try std.testing.expectEqual(@as(usize, 6), planned.len);
    }
}

test "coalescer: basic merging" {
    const allocator = std.testing.allocator;

    const input = [_]Range{
        .{ .start = 0, .end = 100 },
        .{ .start = 150, .end = 200 }, // Gap 50
        .{ .start = 300, .end = 400 }, // Gap 100
    };

    // Gaps:
    // 0..100 -> 150..200 (gap 50)
    // 150..200 -> 300..400 (gap 100)

    // Threshold 60: Should merge first two
    // 0..200, 300..400
    const res1 = try Coalescer.coalesce(allocator, &input, 60);
    defer allocator.free(res1);

    try std.testing.expectEqual(@as(usize, 2), res1.len);
    try std.testing.expectEqual(@as(u64, 0), res1[0].start);
    try std.testing.expectEqual(@as(u64, 200), res1[0].end);
    try std.testing.expectEqual(@as(u64, 300), res1[1].start);
    try std.testing.expectEqual(@as(u64, 400), res1[1].end);
}

test "coalescer: unsorted input" {
    const allocator = std.testing.allocator;

    const input = [_]Range{
        .{ .start = 300, .end = 400 },
        .{ .start = 0, .end = 100 },
        .{ .start = 150, .end = 200 },
    };

    // Threshold 60: Should merge 0..100 and 150..200 -> 0..200
    const res = try Coalescer.coalesce(allocator, &input, 60);
    defer allocator.free(res);

    try std.testing.expectEqual(@as(usize, 2), res.len);
    try std.testing.expectEqual(@as(u64, 0), res[0].start);
    try std.testing.expectEqual(@as(u64, 200), res[0].end);
}

test "coalescer: large gap" {
    const allocator = std.testing.allocator;
    const input = [_]Range{
        .{ .start = 0, .end = 100 },
        .{ .start = 200, .end = 300 },
    };

    // Threshold 50: No merge
    const res = try Coalescer.coalesce(allocator, &input, 50);
    defer allocator.free(res);

    try std.testing.expectEqual(@as(usize, 2), res.len);
}

test "s3 coalesce gap is 1 MiB until latency is measured, then bandwidth times that latency" {
    try std.testing.expectEqual(@as(u64, 1 << 20), s3CoalesceGap(0, 80_000_000));
    try std.testing.expectEqual(@as(u64, 1 << 20), s3CoalesceGap(25 * std.time.ns_per_ms, 0));
    // 80 MB/s × 25 ms = 2_000_000, under the 2 MiB cap.
    try std.testing.expectEqual(@as(u64, 2_000_000), s3CoalesceGap(25 * std.time.ns_per_ms, 80_000_000));
    // A huge product stops at 2 MiB.
    try std.testing.expectEqual(s3_gap_max, s3CoalesceGap(std.time.ns_per_s, 80_000_000));
    // A short measured latency stays small: 80 MB/s × 1 ms.
    try std.testing.expectEqual(@as(u64, 80_000), s3CoalesceGap(std.time.ns_per_ms, 80_000_000));
}

test "s3 coalesce leaves a wave that already fits, and will not read gaps past twice the selected bytes" {
    const allocator = std.testing.allocator;
    // Three chunks, two 100-byte gaps. Three ranges fit in a wave of 4, so the gaps stay.
    const fits = [_]Range{
        .{ .start = 0, .end = 100 },
        .{ .start = 200, .end = 300 },
        .{ .start = 400, .end = 500 },
    };
    const kept = try coalesceMinWaves(allocator, &fits, 1 << 20, 4, 10 << 20);
    defer allocator.free(kept);
    try std.testing.expectEqual(@as(usize, 3), kept.len);

    // Five ranges do not fit in a wave of 2. The smallest gap (50) merges; the 900-byte gap would pass the
    // latency cap but not a budget of twice the 500 selected bytes.
    const wide = [_]Range{
        .{ .start = 0, .end = 100 },
        .{ .start = 150, .end = 250 },
        .{ .start = 1_150, .end = 1_250 },
        .{ .start = 2_150, .end = 2_250 },
        .{ .start = 3_150, .end = 3_250 },
    };
    const selected: u64 = 500;
    const merged = try coalesceMinWaves(allocator, &wide, 1 << 20, 2, selected * 2);
    defer allocator.free(merged);
    try std.testing.expectEqual(@as(usize, 4), merged.len);
    try std.testing.expectEqual(@as(u64, 0), merged[0].start);
    try std.testing.expectEqual(@as(u64, 250), merged[0].end);
}

test "a dispatch of many row groups merges the gap inside each group and not the hole between groups" {
    const allocator = std.testing.allocator;
    // 49 row groups, two ranges each. The gap inside a group is small; the hole before the next group is not.
    // Together that is 98 ranges, ceil(98 / 16) = 7 waves, so the small gaps merge. One group on its own is
    // already one wave and must not.
    const groups = 49;
    const col: u64 = 10_000;
    var ranges: [groups * 2]Range = undefined;
    var at: u64 = 0;
    for (0..groups) |g| {
        ranges[g * 2] = .{ .start = at, .end = at + col };
        at += col + 4096;
        ranges[g * 2 + 1] = .{ .start = at, .end = at + col };
        at += col + (8 << 20);
    }
    const selected: u64 = groups * 2 * col;
    const merged = try coalesceMinWaves(allocator, &ranges, 2 << 20, 16, selected * 2);
    defer allocator.free(merged);
    try std.testing.expectEqual(groups, merged.len);
    try std.testing.expectEqual(@as(u64, 0), merged[0].start);
    try std.testing.expectEqual(col + 4096 + col, merged[0].end);

    const alone = try coalesceMinWaves(allocator, ranges[0..2], 2 << 20, 16, 4000);
    defer allocator.free(alone);
    try std.testing.expectEqual(@as(usize, 2), alone.len);
}

test "sparse ranges stay inside twice the selected bytes" {
    const allocator = std.testing.allocator;
    // Fifty tiny columns, each pair split by a pad the 2× cap cannot pay for.
    const n = 50;
    const col: u64 = 64;
    const pad: u64 = 950 * 1024;
    var ranges: [n]Range = undefined;
    var at: u64 = 0;
    for (0..n) |i| {
        ranges[i] = .{ .start = at, .end = at + col };
        at += col + pad;
    }
    const selected: u64 = n * col;
    const merged = try coalesceMinWaves(allocator, &ranges, 2 << 20, 16, selected * 2);
    defer allocator.free(merged);
    try std.testing.expectEqual(n, merged.len);
    var got: u64 = 0;
    for (merged) |r| got += r.len();
    try std.testing.expectEqual(selected, got);
}

fn spanBytes(spans: []const PlannedSpan) u64 {
    var n: u64 = 0;
    for (spans) |s| n += s.len();
    return n;
}

test "a dispatch of eight objects merges each row group's gap and not the hole between objects" {
    const allocator = std.testing.allocator;
    // Eight files, two row groups, two selected columns. Each file's four ranges already fit in one wave, so a
    // per-file plan merges nothing. Together they are two waves. The 262,210-byte gap inside each row group merges;
    // the hole between row groups is past the latency cap.
    const objects = 8;
    const col: u64 = 262210;
    const intra: u64 = 262210;
    const inter: u64 = 8 << 20;
    var pieces: [objects * 4]Piece = undefined;
    for (0..objects) |o| {
        var at: u64 = 4;
        for (0..2) |g| {
            const base = o * 4 + g * 2;
            pieces[base] = .{
                .object = @intCast(o),
                .group = @intCast(g),
                .start = at,
                .end = at + col,
                .gap_cap = 2_000_000,
            };
            at += col + intra;
            pieces[base + 1] = .{
                .object = @intCast(o),
                .group = @intCast(g),
                .start = at,
                .end = at + col,
                .gap_cap = 2_000_000,
            };
            at += col + inter;
        }
    }
    const selected: u64 = objects * 4 * col;
    const planned = try coalesceDispatch(allocator, &pieces, 16, selected * 2, null);
    defer allocator.free(planned);
    try std.testing.expectEqual(@as(usize, 16), planned.len);
    try std.testing.expectEqual(selected + 16 * intra, spanBytes(planned));
    // One file on its own is already a single wave.
    const alone = try coalesceDispatch(allocator, pieces[0..4], 16, 4 * col * 2, null);
    defer allocator.free(alone);
    try std.testing.expectEqual(@as(usize, 4), alone.len);
}

test "a window accepts a funded gap only while the row group's charge still fits" {
    const allocator = std.testing.allocator;
    // Seventeen selected columns, each pair split by the same 1,572,994-byte pad. One merge costs that pad and makes
    // the group's charge 10,488,100. A 9 MiB window refuses it; an 11 MiB window takes exactly one.
    const col: u64 = 524418;
    const pad: u64 = 1_572_994;
    const n = 17;
    var pieces: [n]Piece = undefined;
    var at: u64 = 4;
    for (0..n) |i| {
        pieces[i] = .{ .object = 0, .group = 0, .start = at, .end = at + col, .gap_cap = 2 << 20 };
        at += col + pad;
    }
    const selected: u64 = n * col;
    const amp = selected * 2;
    const tight = try coalesceDispatch(allocator, &pieces, 16, amp, 9216 * 1024);
    defer allocator.free(tight);
    try std.testing.expectEqual(n, tight.len);
    try std.testing.expectEqual(selected, spanBytes(tight));
    for (tight) |s| try std.testing.expect(s.len() <= 9216 * 1024);

    const open = try coalesceDispatch(allocator, &pieces, 16, amp, 11264 * 1024);
    defer allocator.free(open);
    try std.testing.expectEqual(@as(usize, 16), open.len);
    try std.testing.expectEqual(selected + pad, spanBytes(open));
    for (open) |s| try std.testing.expect(s.len() <= 11264 * 1024);
}

test "one plan charges gap bytes once, so a second pass would read past twice the selected bytes" {
    const allocator = std.testing.allocator;
    // Ten columns of 100 bytes. Six gaps of 50, then three of 400. In flight of 2 still wants merges after the small
    // gaps are gone. Twice the original 1,000 bytes stops before the second large gap. Planning again from the spans,
    // as if those gap bytes were selected, would take it.
    var pieces: [10]Piece = undefined;
    var at: u64 = 0;
    for (0..10) |i| {
        pieces[i] = .{ .object = 0, .group = 0, .start = at, .end = at + 100, .gap_cap = 10_000 };
        const gap: u64 = if (i < 6) 50 else 400;
        at += 100 + gap;
    }
    const selected: u64 = 1000;
    const once = try coalesceDispatch(allocator, &pieces, 2, selected * 2, null);
    defer allocator.free(once);
    try std.testing.expectEqual(@as(usize, 3), once.len);
    try std.testing.expectEqual(@as(u64, 1700), spanBytes(once));

    var again_src: [3]Piece = undefined;
    for (once, &again_src) |s, *p| {
        p.* = .{ .object = 0, .group = 0, .start = s.start, .end = s.end, .gap_cap = 10_000 };
    }
    const twice = try coalesceDispatch(allocator, &again_src, 2, spanBytes(once) * 2, null);
    defer allocator.free(twice);
    try std.testing.expect(spanBytes(twice) > selected * 2);
}

/// The rescan planner, kept so a faster walk can be checked against the same spans. Not used on the fetch path.
fn coalesceDispatchOracle(
    allocator: Allocator,
    pieces: []const Piece,
    in_flight: usize,
    amp_cap: u64,
    window_cap: ?u64,
) ![]const PlannedSpan {
    if (pieces.len == 0) return &.{};
    const Live = struct {
        object: u32,
        group: u32,
        start: u64,
        end: u64,
        gap_cap: u64,
    };
    var live: std.ArrayList(Live) = .empty;
    defer live.deinit(allocator);
    try live.ensureTotalCapacity(allocator, pieces.len);
    const order = try allocator.alloc(usize, pieces.len);
    defer allocator.free(order);
    for (order, 0..) |*slot, i| slot.* = i;
    std.mem.sort(usize, order, pieces, struct {
        fn lessThan(p: []const Piece, a: usize, b: usize) bool {
            if (p[a].object != p[b].object) return p[a].object < p[b].object;
            return p[a].start < p[b].start;
        }
    }.lessThan);
    var max_group: u32 = 0;
    for (pieces) |p| max_group = @max(max_group, p.group);
    const charge = try allocator.alloc(u64, @as(usize, max_group) + 1);
    defer allocator.free(charge);
    @memset(charge, 0);
    for (order) |idx| {
        const p = pieces[idx];
        if (live.items.len != 0) {
            const cur = &live.items[live.items.len - 1];
            const same = cur.object == p.object and (window_cap == null or cur.group == p.group);
            if (same and p.start <= cur.end) {
                if (p.end > cur.end) {
                    charge[cur.group] +|= p.end - cur.end;
                    cur.end = p.end;
                }
                cur.gap_cap = @min(cur.gap_cap, p.gap_cap);
                continue;
            }
        }
        charge[p.group] +|= p.end - p.start;
        live.appendAssumeCapacity(.{
            .object = p.object,
            .group = p.group,
            .start = p.start,
            .end = p.end,
            .gap_cap = p.gap_cap,
        });
    }
    var total: u64 = 0;
    for (live.items) |s| total +|= s.end - s.start;
    while (true) {
        const wave_target = std.math.divCeil(usize, live.items.len, @max(in_flight, 1)) catch unreachable;
        if (wave_target <= 1) break;
        var best_i: ?usize = null;
        var best_gap: u64 = std.math.maxInt(u64);
        for (live.items[0 .. live.items.len - 1], 0..) |s, i| {
            const nxt = live.items[i + 1];
            if (s.object != nxt.object) continue;
            if (window_cap != null and s.group != nxt.group) continue;
            if (nxt.start < s.end) continue;
            const gap = nxt.start - s.end;
            if (gap > @min(s.gap_cap, nxt.gap_cap)) continue;
            if (total +| gap > amp_cap) continue;
            if (window_cap) |limit| {
                const span = (s.end - s.start) +| gap +| (nxt.end - nxt.start);
                if (span > limit or charge[s.group] +| gap > limit) continue;
            }
            if (gap < best_gap) {
                best_gap = gap;
                best_i = i;
            }
        }
        const i = best_i orelse break;
        const here = live.items[i];
        live.items[i].end = live.items[i + 1].end;
        live.items[i].gap_cap = @min(here.gap_cap, live.items[i + 1].gap_cap);
        _ = live.orderedRemove(i + 1);
        total +|= best_gap;
        charge[here.group] +|= best_gap;
    }
    const out = try allocator.alloc(PlannedSpan, live.items.len);
    for (live.items, out) |s, *dst| {
        dst.* = .{ .object = s.object, .group = s.group, .start = s.start, .end = s.end };
    }
    return out;
}

fn expectSameSpans(got: []const PlannedSpan, want: []const PlannedSpan) !void {
    try std.testing.expectEqual(want.len, got.len);
    for (got, want) |g, w| {
        try std.testing.expectEqual(w.object, g.object);
        try std.testing.expectEqual(w.group, g.group);
        try std.testing.expectEqual(w.start, g.start);
        try std.testing.expectEqual(w.end, g.end);
    }
}

test "a sorted gap walk matches the rescan planner" {
    const allocator = std.testing.allocator;
    var rng = std.Random.DefaultPrng.init(0x9c5828e);
    const random = rng.random();
    for (0..48) |_| {
        const objects = random.intRangeAtMost(u32, 1, 4);
        const per = random.intRangeAtMost(usize, 1, 24);
        const pieces = try allocator.alloc(Piece, objects * per);
        defer allocator.free(pieces);
        var n: usize = 0;
        var selected: u64 = 0;
        for (0..objects) |o| {
            var at: u64 = random.intRangeAtMost(u64, 0, 20);
            for (0..per) |_| {
                const len = random.intRangeAtMost(u64, 1, 40);
                const group = random.intRangeAtMost(u32, 0, 3);
                pieces[n] = .{
                    .object = @intCast(o),
                    .group = group,
                    .start = at,
                    .end = at + len,
                    .gap_cap = random.intRangeAtMost(u64, 0, 50),
                };
                selected += len;
                n += 1;
                const hole = random.intRangeAtMost(u64, 0, 30);
                if (random.boolean()) at += len + hole else at += hole;
            }
        }
        const window: ?u64 = switch (random.intRangeAtMost(u8, 0, 3)) {
            0 => null,
            1 => random.intRangeAtMost(u64, 1, 80),
            2 => random.intRangeAtMost(u64, 81, 4_000),
            else => 1 << 20,
        };
        const amp = switch (random.intRangeAtMost(u8, 0, 2)) {
            0 => selected / 2,
            1 => selected * 2,
            else => selected * 8,
        };
        const flight = random.intRangeAtMost(usize, 1, 8);
        const got = try coalesceDispatch(allocator, pieces, flight, amp, window);
        defer allocator.free(got);
        const want = try coalesceDispatchOracle(allocator, pieces, flight, amp, window);
        defer allocator.free(want);
        try expectSameSpans(got, want);
    }
}

test "a span longer than an S3 range split still merges by span count" {
    const allocator = std.testing.allocator;
    // Three exact 8 MiB ranges, one byte apart. Touching ranges would already be one span. Two in flight
    // stops after one merge, leaving two spans.
    const span: u64 = 8 << 20;
    const pieces = [_]Piece{
        .{ .object = 0, .group = 0, .start = 0, .end = span, .gap_cap = span },
        .{ .object = 0, .group = 0, .start = span + 1, .end = span * 2 + 1, .gap_cap = span },
        .{ .object = 0, .group = 0, .start = span * 2 + 2, .end = span * 3 + 2, .gap_cap = span },
    };
    const got = try coalesceDispatch(allocator, &pieces, 2, span * 6, null);
    defer allocator.free(got);
    const want = try coalesceDispatchOracle(allocator, &pieces, 2, span * 6, null);
    defer allocator.free(want);
    try expectSameSpans(got, want);
    try std.testing.expectEqual(@as(usize, 2), got.len);
}

test "planning eight objects of 128 row groups inspects each gap once" {
    const allocator = std.testing.allocator;
    const objects = 8;
    const groups = 128;
    const cols = 32;
    const col: u64 = 2048;
    const hole: u64 = 256;
    const n: usize = objects * groups * cols;
    const pieces = try allocator.alloc(Piece, n);
    defer allocator.free(pieces);
    var k: usize = 0;
    for (0..objects) |o| {
        var at: u64 = 0;
        for (0..groups) |g| {
            for (0..cols) |_| {
                pieces[k] = .{
                    .object = @intCast(o),
                    .group = @intCast(g),
                    .start = at,
                    .end = at + col,
                    .gap_cap = 2 << 20,
                };
                k += 1;
                at += col + hole;
            }
        }
    }
    var visits: usize = 0;
    const selected: u64 = n * col;
    const planned = try coalesceDispatchInner(allocator, pieces, 16, selected * 2, null, &visits);
    defer allocator.free(planned);
    const bound: usize = 4 * n * @as(usize, std.math.log2_int_ceil(usize, n));
    try std.testing.expect(visits <= bound);
    try std.testing.expectEqual(@as(usize, 16), planned.len);
    try std.testing.expectEqual(selected + (n - 16) * hole, spanBytes(planned));
}
