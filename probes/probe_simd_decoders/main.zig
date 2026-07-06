const std = @import("std");
const linux = std.os.linux;

// RDTSC to measure exact CPU cycles
inline fn rdtsc() u64 {
    var lo: u32 = 0;
    var hi: u32 = 0;
    asm volatile ("rdtsc" : [lo] "={eax}" (lo), [hi] "={edx}" (hi));
    return (@as(u64, hi) << 32) | lo;
}

// ============================================================
// 1. ZIGZAG DECODING BENCHMARKS
// ============================================================

fn zigzagScalar(dest: []i32, src: []const u32) void {
    for (src, 0..) |v, i| {
        const sign = 0 -% (v & 1);
        dest[i] = @bitCast((v >> 1) ^ sign);
    }
}

// Portable @Vector zigzag utilizing LLVM's AVX-512 target-lowering
fn zigzagVector(dest: []i32, src: []const u32) void {
    const LANES = 16;
    var i: usize = 0;
    const ones: @Vector(LANES, u32) = @splat(1);
    const zeros: @Vector(LANES, u32) = @splat(0);
    const neg_ones: @Vector(LANES, i32) = @splat(-1);

    while (i + LANES <= src.len) : (i += LANES) {
        const v: @Vector(LANES, u32) = src[i..][0..LANES].*;
        const mask = (v & ones) != zeros;
        const shifted: @Vector(LANES, i32) = @bitCast(v >> @splat(1));
        const res = @select(i32, mask, shifted ^ neg_ones, shifted);
        dest[i..][0..LANES].* = res;
    }
}

// AVX-512 inline assembly utilizing vptestmd + vpxord{k}
fn zigzagAvx512(dest: []i32, src: []const u32) void {
    const LANES = 16;
    var i: usize = 0;
    const ones: @Vector(LANES, u32) = @splat(1);
    const neg_ones: @Vector(LANES, i32) = @splat(-1);

    while (i + LANES <= src.len) : (i += LANES) {
        const v: @Vector(LANES, u32) = src[i..][0..LANES].*;
        var res: @Vector(LANES, i32) = undefined;
        asm volatile (
            \\vptestmd %[ones], %[v], %%k1
            \\vpsrld $1, %[v], %[res]
            \\vpxord %[neg_ones], %[res], %[res]{%%k1}
            : [res] "=v" (res),
            : [v] "v" (v),
              [ones] "v" (ones),
              [neg_ones] "v" (neg_ones),
            : "k1"
        );
        dest[i..][0..LANES].* = res;
    }
}

// 8-bit zigzag using scalar branchless
fn zigzag8Scalar(dest: []i8, src: []const u8) void {
    for (src, 0..) |v, i| {
        const sign = 0 -% (v & 1);
        dest[i] = @bitCast(@as(u8, @intCast(v >> 1)) ^ @as(u8, @truncate(sign)));
    }
}

// 8-bit zigzag using GF(2) affine transformation instruction vgf2p8affineqb
fn zigzag8Gfni(dest: []i8, src: []const u8) void {
    const LANES = 16;
    var i: usize = 0;
    const matrix: u64 = 0x0305091121418101;
    const matrix_vec: @Vector(2, u64) = @splat(matrix);

    while (i + LANES <= src.len) : (i += LANES) {
        const v: @Vector(LANES, u8) = src[i..][0..LANES].*;
        var res: @Vector(LANES, i8) = undefined;
        asm volatile (
            \\vgf2p8affineqb $0, %[matrix], %[v], %[res]
            : [res] "=x" (res),
            : [v] "x" (v),
              [matrix] "x" (matrix_vec),
        );
        dest[i..][0..LANES].* = res;
    }
}

// ============================================================
// 2. BIT UNPACKING BENCHMARKS (DELTA_BINARY_PACKED)
// ============================================================

// Scalar byte-by-byte bit unpacking (similar to current zpq delta_binary_packed)
fn unpackScalar(dest: []u32, src: []const u8, bw: u8) void {
    var bit_buffer: u128 = 0;
    var bits_in_buffer: u8 = 0;
    var pos: usize = 0;
    const mask = if (bw == 32) std.math.maxInt(u32) else (@as(u32, 1) << @intCast(bw)) - 1;

    for (dest, 0..) |_, i| {
        while (bits_in_buffer < bw) {
            bit_buffer |= @as(u128, src[pos]) << @intCast(bits_in_buffer);
            bits_in_buffer += 8;
            pos += 1;
        }
        dest[i] = @intCast(bit_buffer & mask);
        bit_buffer >>= @intCast(bw);
        bits_in_buffer -= bw;
    }
}

// Comptime-specialized unrolled bit unpacker (similar to hybrid_rle unpackFastFixed)
fn unpackFastFixed(comptime bw: u8, dest: []u32, src: []const u8) void {
    const mask: u32 = comptime if (bw == 32) std.math.maxInt(u32) else (@as(u32, 1) << bw) - 1;
    var i: usize = 0;
    while (i + 32 <= dest.len) : (i += 32) {
        const batch_start_byte = (i * @as(usize, bw)) / 8;
        inline for (0..32) |k| {
            const start_bit: usize = k * bw;
            const byte_off = start_bit / 8;
            const bit_off: u6 = @intCast(start_bit % 8);
            const word = std.mem.readInt(u64, src[batch_start_byte + byte_off ..][0..8], .little);
            dest[i + k] = @as(u32, @intCast((word >> bit_off) & mask));
        }
    }
}

// Dispatcher for the fast unrolled unpacker
fn unpackFast(dest: []u32, src: []const u8, bw: u8) void {
    switch (bw) {
        inline 1...32 => |b| unpackFastFixed(b, dest, src),
        else => unreachable,
    }
}

// ============================================================
// 3. DICTIONARY GATHER BENCHMARKS
// ============================================================

fn gatherScalar(dest: []i64, dictionary: []const i64, indices: []const u32) void {
    for (indices, 0..) |idx, i| {
        dest[i] = dictionary[idx];
    }
}

fn gatherUnrolled(dest: []i64, dictionary: []const i64, indices: []const u32) void {
    var i: usize = 0;
    const N = 8;
    while (i + N <= indices.len) {
        inline for (0..N) |k| {
            dest[i + k] = dictionary[indices[i + k]];
        }
        i += N;
    }
    while (i < indices.len) : (i += 1) {
        dest[i] = dictionary[indices[i]];
    }
}

fn gatherVector(dest: []i64, dictionary: []const i64, indices: []const u32) void {
    const LANES = 8;
    var i: usize = 0;
    while (i + LANES <= indices.len) : (i += LANES) {
        const idx: @Vector(LANES, u32) = indices[i..][0..LANES].*;
        var vals: @Vector(LANES, i64) = undefined;
        inline for (0..LANES) |k| {
            vals[k] = dictionary[idx[k]];
        }
        dest[i..][0..LANES].* = vals;
    }
}

// ============================================================
// MAIN RUNNER & TIMINGS
// ============================================================

pub fn main() !void {
    const N = 65536;
    var prng = std.Random.DefaultPrng.init(42);
    const rand = prng.random();

    std.debug.print("--- SIMD DECODING PROBE ---\n\n", .{});

    // 1. ZIGZAG TIMINGS
    {
        std.debug.print("== 1. 32-bit Zigzag Decoding (N = {d}) ==\n", .{N});
        const src = try std.heap.page_allocator.alloc(u32, N);
        const dest = try std.heap.page_allocator.alloc(i32, N);
        defer std.heap.page_allocator.free(src);
        defer std.heap.page_allocator.free(dest);

        for (src) |*v| v.* = rand.int(u32);

        // Warmup
        zigzagScalar(dest, src);
        zigzagVector(dest, src);
        zigzagAvx512(dest, src);

        // Scalar
        {
            const t0 = rdtsc();
            var k: usize = 0;
            while (k < 1000) : (k += 1) {
                zigzagScalar(dest, src);
                std.mem.doNotOptimizeAway(dest);
            }
            const t1 = rdtsc();
            std.debug.print("Scalar Branchless:     {d:>6.3} cycles/val\n", .{@as(f64, @floatFromInt(t1 - t0)) / @as(f64, @floatFromInt(N * 1000))});
        }

        // Portable Vector (@select)
        {
            const t0 = rdtsc();
            var k: usize = 0;
            while (k < 1000) : (k += 1) {
                zigzagVector(dest, src);
                std.mem.doNotOptimizeAway(dest);
            }
            const t1 = rdtsc();
            std.debug.print("Portable Vector:       {d:>6.3} cycles/val\n", .{@as(f64, @floatFromInt(t1 - t0)) / @as(f64, @floatFromInt(N * 1000))});
        }

        // AVX-512 Assembly
        {
            const t0 = rdtsc();
            var k: usize = 0;
            while (k < 1000) : (k += 1) {
                zigzagAvx512(dest, src);
                std.mem.doNotOptimizeAway(dest);
            }
            const t1 = rdtsc();
            std.debug.print("AVX-512 Assembly:      {d:>6.3} cycles/val\n", .{@as(f64, @floatFromInt(t1 - t0)) / @as(f64, @floatFromInt(N * 1000))});
        }
    }

    {
        std.debug.print("\n== 2. 8-bit Zigzag Decoding (N = {d}) ==\n", .{N});
        const src = try std.heap.page_allocator.alloc(u8, N);
        const dest = try std.heap.page_allocator.alloc(i8, N);
        defer std.heap.page_allocator.free(src);
        defer std.heap.page_allocator.free(dest);

        for (src) |*v| v.* = rand.int(u8);

        zigzag8Scalar(dest, src);
        zigzag8Gfni(dest, src);

        // Scalar
        {
            const t0 = rdtsc();
            var k: usize = 0;
            while (k < 1000) : (k += 1) {
                zigzag8Scalar(dest, src);
                std.mem.doNotOptimizeAway(dest);
            }
            const t1 = rdtsc();
            std.debug.print("Scalar:                {d:>6.3} cycles/val\n", .{@as(f64, @floatFromInt(t1 - t0)) / @as(f64, @floatFromInt(N * 1000))});
        }

        // GFNI Assembly
        {
            const t0 = rdtsc();
            var k: usize = 0;
            while (k < 1000) : (k += 1) {
                zigzag8Gfni(dest, src);
                std.mem.doNotOptimizeAway(dest);
            }
            const t1 = rdtsc();
            std.debug.print("GFNI Assembly (GF2):   {d:>6.3} cycles/val\n", .{@as(f64, @floatFromInt(t1 - t0)) / @as(f64, @floatFromInt(N * 1000))});
        }
    }

    // 2. BIT UNPACKING TIMINGS
    {
        std.debug.print("\n== 3. Bit Unpacking (N = 32768) ==\n", .{});
        const packed_bytes = try std.heap.page_allocator.alloc(u8, 32768 * 4);
        const dest = try std.heap.page_allocator.alloc(u32, 32768);
        defer std.heap.page_allocator.free(packed_bytes);
        defer std.heap.page_allocator.free(dest);

        for (packed_bytes) |*v| v.* = rand.int(u8);

        const widths = [_]u8{ 1, 3, 5, 9, 12, 17, 23, 31 };
        for (widths) |bw| {
            std.debug.print("Width = {d:>2} bits:\n", .{bw});

            // Scalar unpacking
            {
                const t0 = rdtsc();
                var k: usize = 0;
                while (k < 200) : (k += 1) {
                    unpackScalar(dest, packed_bytes, bw);
                    std.mem.doNotOptimizeAway(dest);
                }
                const t1 = rdtsc();
                std.debug.print("  Scalar Byte-by-Byte: {d:>6.3} cycles/val\n", .{@as(f64, @floatFromInt(t1 - t0)) / @as(f64, @floatFromInt(dest.len * 200))});
            }

            // Comptime fast unpacking
            {
                const t0 = rdtsc();
                var k: usize = 0;
                while (k < 200) : (k += 1) {
                    unpackFast(dest, packed_bytes, bw);
                    std.mem.doNotOptimizeAway(dest);
                }
                const t1 = rdtsc();
                std.debug.print("  Comptime Unrolled:   {d:>6.3} cycles/val\n", .{@as(f64, @floatFromInt(t1 - t0)) / @as(f64, @floatFromInt(dest.len * 200))});
            }
        }
    }

    // 3. DICTIONARY GATHER TIMINGS
    {
        std.debug.print("\n== 4. Dictionary Gather (N = {d}) ==\n", .{N});
        const dict_sizes = [_]usize{ 256, 4096, 16384 };
        const dest = try std.heap.page_allocator.alloc(i64, N);
        const indices = try std.heap.page_allocator.alloc(u32, N);
        defer std.heap.page_allocator.free(dest);
        defer std.heap.page_allocator.free(indices);

        for (dict_sizes) |dict_size| {
            std.debug.print("Dict size = {d:>5}:\n", .{dict_size});
            const dict = try std.heap.page_allocator.alloc(i64, dict_size);
            defer std.heap.page_allocator.free(dict);

            for (dict) |*v| v.* = rand.int(i64);
            for (indices) |*v| v.* = rand.intRangeAtMost(u32, 0, @intCast(dict_size - 1));

            // Scalar gather
            {
                const t0 = rdtsc();
                var k: usize = 0;
                while (k < 500) : (k += 1) {
                    gatherScalar(dest, dict, indices);
                    std.mem.doNotOptimizeAway(dest);
                }
                const t1 = rdtsc();
                std.debug.print("  Scalar Loop:         {d:>6.3} cycles/val\n", .{@as(f64, @floatFromInt(t1 - t0)) / @as(f64, @floatFromInt(N * 500))});
            }

            // Unrolled gather
            {
                const t0 = rdtsc();
                var k: usize = 0;
                while (k < 500) : (k += 1) {
                    gatherUnrolled(dest, dict, indices);
                    std.mem.doNotOptimizeAway(dest);
                }
                const t1 = rdtsc();
                std.debug.print("  Unrolled Loop:       {d:>6.3} cycles/val\n", .{@as(f64, @floatFromInt(t1 - t0)) / @as(f64, @floatFromInt(N * 500))});
            }

            // Vector gather
            {
                const t0 = rdtsc();
                var k: usize = 0;
                while (k < 500) : (k += 1) {
                    gatherVector(dest, dict, indices);
                    std.mem.doNotOptimizeAway(dest);
                }
                const t1 = rdtsc();
                std.debug.print("  Vector Loop (@Vector):{d:>6.3} cycles/val\n", .{@as(f64, @floatFromInt(t1 - t0)) / @as(f64, @floatFromInt(N * 500))});
            }
        }
    }
}
