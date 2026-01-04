const std = @import("std");

fn readBitPacked8(data: []const u8, width: u5, out: []u64) void {
    const Container = u64;
    const total_bytes = (width * 8 + 7) / 8;
    var buf = [_]u8{0} ** 8;
    @memcpy(buf[0..@min(8, total_bytes)], data[0..@min(8, total_bytes)]);
    const bits = std.mem.readInt(Container, &buf, .little);
    
    const v_bits: @Vector(8, Container) = @splat(bits);
    var shifts: [8]std.math.Log2Int(Container) = undefined;
    inline for (0..8) |i| shifts[i] = @intCast(i * width);
    const v_shifts: @Vector(8, std.math.Log2Int(Container)) = shifts;
    const mask: Container = (@as(Container, 1) << width) - 1;
    const v_res = (v_bits >> v_shifts) & @as(@Vector(8, Container), @splat(mask));
    
    inline for (0..8) |i| out[i] = @intCast(v_res[i]);
}

fn readBitPacked32Unrolled(comptime width: u5, data: []const u8, out: []u64) void {
    inline for (0..4) |group| {
        inline for (0..8) |i| {
            const val_bit_idx = (group * 8 + i) * width;
            const val_byte_idx = val_bit_idx / 8;
            const val_bit_off: u3 = @intCast(val_bit_idx % 8);
            
            const raw = std.mem.readInt(u64, data[val_byte_idx..][0..8], .little);
            out[group * 8 + i] = (raw >> val_bit_off) & ((@as(u64, 1) << width) - 1);
        }
    }
}

pub fn main() !void {
    const iters = 1_000_000;
    const width = 5;
    const data = [_]u8{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22} ** 128;
    var out: [32]u64 = undefined;
    var sum: u64 = 0;
    
    var timer = try std.time.Timer.start();
    
    // Bench 8-value SIMD (called 4 times)
    timer.reset();
    for (0..iters) |i| {
        const d_off = i % 100;
        readBitPacked8(data[d_off..], width, out[0..8]);
        readBitPacked8(data[d_off + 5 ..], width, out[8..16]);
        readBitPacked8(data[d_off + 10 ..], width, out[16..24]);
        readBitPacked8(data[d_off + 15 ..], width, out[24..32]);
        sum += out[0] + out[15] + out[31];
    }
    const t8 = timer.read();
    
    std.mem.doNotOptimizeAway(sum);
    sum = 0;

    // Bench 32-value unrolled
    timer.reset();
    for (0..iters) |i| {
        const d_off = i % 100;
        readBitPacked32Unrolled(width, data[d_off..], out[0..32]);
        sum += out[0] + out[15] + out[31];
    }
    const t32 = timer.read();
    
    std.mem.doNotOptimizeAway(sum);

    std.debug.print("8-value SIMD x4:  {d: >10}ns ({d:.2}ms)\n", .{t8, @as(f64, @floatFromInt(t8)) / 1_000_000.0});
    std.debug.print("32-value Unrolled: {d: >10}ns ({d:.2}ms)\n", .{t32, @as(f64, @floatFromInt(t32)) / 1_000_000.0});
    std.debug.print("Speedup: {d:.2}x\n", .{@as(f64, @floatFromInt(t8)) / @as(f64, @floatFromInt(t32))});
}
