const std = @import("std");
const filter_selection = @import("../../filter/selection.zig");

/// `acc` plus the selected values, in the one order every float sum and avg folds in, whatever the decode slicing,
/// the worker count, the decoder (dictionary or plain) or the target: a row group's rows in 64-row blocks aligned
/// to its first row, the blocks added to `acc` in row order, each block's sum taken alone: over 16 fixed lanes and
/// a fixed pairwise reduction when all 64 rows are selected, else its selected rows in row order. A slice starts on a
/// block boundary (see `consumer.sliceRows`) and carries `acc` on, so cutting a row group changes no bit. The lane
/// count is fixed rather than the target's vector width, so x86_64 and arm64 builds agree too.
pub fn sumBlocksF64(acc: f64, vals: []const f64, sel: *const filter_selection.SelectionVector) f64 {
    var total = acc;
    var w: usize = 0;
    while ((w + 1) * 64 <= vals.len) : (w += 1) {
        const word = sel.mask[w];
        if (word == 0) continue;
        const block = vals[w * 64 ..][0..64];
        total += if (word == std.math.maxInt(u64)) fullBlockSum(block) else partialBlockSum(block, word);
    }
    const tail = vals.len - w * 64;
    if (tail != 0) {
        const word = sel.mask[w] & ((@as(u64, 1) << @intCast(tail)) - 1);
        if (word != 0) total += partialBlockSum(vals[w * 64 ..], word);
    }
    return total;
}

/// `sumBlocksF64` over the rows `sel` selects when their values were gathered, in row order, into `vals`: each block
/// takes the next values it selects, so the bits are those of summing them in place.
pub fn sumBlocksGatheredF64(acc: f64, vals: []const f64, sel: *const filter_selection.SelectionVector) f64 {
    var total = acc;
    var at: usize = 0;
    const full_words = sel.len / 64;
    for (sel.mask[0 .. (sel.len + 63) / 64], 0..) |mask, w| {
        const word = if (w < full_words) mask else mask & ((@as(u64, 1) << @intCast(sel.len % 64)) - 1);
        if (word == 0) continue;
        const n = @popCount(word);
        if (word == std.math.maxInt(u64)) {
            total += fullBlockSum(vals[at..][0..64]);
        } else {
            var sum = vals[at];
            for (vals[at + 1 ..][0 .. n - 1]) |v| sum += v;
            total += sum;
        }
        at += n;
    }
    return total;
}

/// `sumBlocksF64` over a dictionary-coded column, `dict[codes[r]]` for row r: the same blocks, the same bits, without
/// materializing the values.
pub fn sumBlocksCodedF64(
    acc: f64,
    codes: []const u32,
    dict: []const f64,
    sel: *const filter_selection.SelectionVector,
) f64 {
    var total = acc;
    var block: [64]f64 = undefined;
    var w: usize = 0;
    while ((w + 1) * 64 <= codes.len) : (w += 1) {
        const word = sel.mask[w];
        if (word == 0) continue;
        const c = codes[w * 64 ..][0..64];
        if (word == std.math.maxInt(u64)) {
            for (&block, c) |*b, k| b.* = dict[k];
            total += fullBlockSum(&block);
        } else total += partialCodedSum(c, dict, word);
    }
    const tail = codes.len - w * 64;
    if (tail != 0) {
        const word = sel.mask[w] & ((@as(u64, 1) << @intCast(tail)) - 1);
        if (word != 0) total += partialCodedSum(codes[w * 64 ..], dict, word);
    }
    return total;
}

inline fn partialCodedSum(codes: []const u32, dict: []const f64, word: u64) f64 {
    var bits = word;
    var sum = dict[codes[@ctz(bits)]];
    bits &= bits - 1;
    while (bits != 0) : (bits &= bits - 1) sum += dict[codes[@ctz(bits)]];
    return sum;
}

/// `sumBlocksF64` over `n` rows all holding `v`, all selected, in O(n / 64): every full block sums to the same value.
/// Bit for bit what decoding the chunk and summing it gives, so a constant chunk may answer from its statistics.
pub fn sumConstantF64(acc: f64, v: f64, n: u64) f64 {
    var total = acc;
    const block: [64]f64 = @splat(v);
    const b = fullBlockSum(&block);
    for (0..n / 64) |_| total += b;
    if (n % 64 != 0) {
        var t = v;
        for (1..n % 64) |_| t += v;
        total += t;
    }
    return total;
}

const VF16 = @Vector(16, f64);

inline fn fullBlockSum(block: *const [64]f64) f64 {
    var lanes: VF16 = block[0..16].*;
    lanes += @as(VF16, block[16..32].*);
    lanes += @as(VF16, block[32..48].*);
    lanes += @as(VF16, block[48..64].*);
    const l: [16]f64 = lanes;
    var half: [8]f64 = undefined;
    for (&half, 0..) |*h, i| h.* = l[i] + l[i + 8];
    var quarter: [4]f64 = undefined;
    for (&quarter, 0..) |*q, i| q.* = half[i] + half[i + 4];
    return (quarter[0] + quarter[2]) + (quarter[1] + quarter[3]);
}

inline fn partialBlockSum(block: []const f64, word: u64) f64 {
    var bits = word;
    var sum = block[@ctz(bits)];
    bits &= bits - 1;
    while (bits != 0) : (bits &= bits - 1) sum += block[@ctz(bits)];
    return sum;
}

test "regression: sumBlocksF64 gives the same bits however a row group is cut into 64-row-aligned slices" {
    const gpa = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0x5eed);
    const rnd = prng.random();
    const n = 64 * 37 + 21;
    const vals = try gpa.alloc(f64, n);
    defer gpa.free(vals);
    for (vals) |*v| {
        const magnitude = std.math.pow(f64, 10, @floatFromInt(rnd.intRangeAtMost(i32, -3, 16)));
        v.* = (rnd.float(f64) - 0.5) * magnitude;
    }
    for ([_]u32{ 1, 2, 7, 100 }) |keep_one_in| {
        var sel = try filter_selection.SelectionVector.init(gpa, n);
        defer sel.deinit();
        for (0..n) |i| sel.set(i, keep_one_in == 1 or rnd.uintLessThan(u32, keep_one_in) == 0);
        const whole = sumBlocksF64(0, vals, &sel);
        for ([_]usize{ 64, 128, 64 * 5, 64 * 36 }) |slice| {
            var acc: f64 = 0;
            var lo: usize = 0;
            while (lo < n) : (lo += slice) {
                const hi = @min(n, lo + slice);
                var part = try filter_selection.SelectionVector.init(gpa, hi - lo);
                defer part.deinit();
                for (lo..hi) |i| part.set(i - lo, sel.isActive(i));
                acc = sumBlocksF64(acc, vals[lo..hi], &part);
            }
            try std.testing.expectEqual(@as(u64, @bitCast(whole)), @as(u64, @bitCast(acc)));
        }
    }
}

test "regression: sumConstantF64 is sumBlocksF64 over the constant's rows" {
    const gpa = std.testing.allocator;
    for ([_]f64{ 0.1, -3.3e-5, 1e300, 7 }) |v| for ([_]u64{ 1, 63, 64, 65, 1000, 64 * 50 }) |n| {
        const vals = try gpa.alloc(f64, n);
        defer gpa.free(vals);
        @memset(vals, v);
        var sel = try filter_selection.SelectionVector.init(gpa, n);
        defer sel.deinit();
        const want = sumBlocksF64(0.25, vals, &sel);
        try std.testing.expectEqual(@as(u64, @bitCast(want)), @as(u64, @bitCast(sumConstantF64(0.25, v, n))));
    };
}

test "regression: sumBlocksCodedF64 is sumBlocksF64 over the column the codes stand for" {
    const gpa = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(9);
    const rnd = prng.random();
    const dict = [_]f64{ 1e16, -3.25, 0.1, 7e-9, -1e16 };
    const n = 64 * 9 + 13;
    const codes = try gpa.alloc(u32, n);
    defer gpa.free(codes);
    const vals = try gpa.alloc(f64, n);
    defer gpa.free(vals);
    for (codes, vals) |*c, *v| {
        c.* = rnd.uintLessThan(u32, dict.len);
        v.* = dict[c.*];
    }
    for ([_]u32{ 1, 3 }) |keep| {
        var sel = try filter_selection.SelectionVector.init(gpa, n);
        defer sel.deinit();
        for (0..n) |i| sel.set(i, keep == 1 or rnd.uintLessThan(u32, keep) == 0);
        const want = sumBlocksF64(0, vals, &sel);
        const got = sumBlocksCodedF64(0, codes, &dict, &sel);
        try std.testing.expectEqual(@as(u64, @bitCast(want)), @as(u64, @bitCast(got)));
    }
}

test "regression: sumBlocksGatheredF64 sums gathered values in their rows' blocks" {
    const gpa = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(11);
    const rnd = prng.random();
    const n = 64 * 7 + 29;
    const vals = try gpa.alloc(f64, n);
    defer gpa.free(vals);
    for (vals) |*v| {
        const magnitude = std.math.pow(f64, 10, @floatFromInt(rnd.intRangeAtMost(i32, -3, 16)));
        v.* = (rnd.float(f64) - 0.5) * magnitude;
    }
    for ([_]u32{ 1, 2, 50 }) |keep| {
        var sel = try filter_selection.SelectionVector.init(gpa, n);
        defer sel.deinit();
        for (0..n) |i| sel.set(i, keep == 1 or rnd.uintLessThan(u32, keep) == 0);
        var gathered: std.ArrayList(f64) = .empty;
        defer gathered.deinit(gpa);
        for (vals, 0..) |v, i| if (sel.isActive(i)) try gathered.append(gpa, v);
        const want = sumBlocksF64(0, vals, &sel);
        const got = sumBlocksGatheredF64(0, gathered.items, &sel);
        try std.testing.expectEqual(@as(u64, @bitCast(want)), @as(u64, @bitCast(got)));
    }
}

