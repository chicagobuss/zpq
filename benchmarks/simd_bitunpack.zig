const std = @import("std");
const zpq = @import("zpq");
const RleDecoder = zpq.rle.RleDecoder;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const num_values: usize = 1_000_000;
    const bit_widths = [_]u8{ 1, 3, 10, 20, 32 };
    
    inline for (bit_widths) |bit_width| {
        std.debug.print("\n" ++ ("=" ** 40) ++ "\n", .{});
        std.debug.print("BIT WIDTH: {d}\n", .{bit_width});
        std.debug.print("=" ** 40 ++ "\n", .{});

        const data_len = (num_values * bit_width + 7) / 8 + 10; // Extra room for header
        const data = try allocator.alloc(u8, data_len);
        defer allocator.free(data);

        // Mock bit-packed data: 0, 1, 2, ... repeated
        const groups = num_values / 8;
        const header = (groups << 1) | 1;
        
        var pos: usize = 0;
        var h_val: usize = header;
        while (h_val >= 0x80) {
            data[pos] = @intCast((h_val & 0x7F) | 0x80);
            h_val >>= 7;
            pos += 1;
        }
        data[pos] = @intCast(h_val);
        pos += 1;
        
        var bits: u256 = 0;
        var bit_count: u16 = 0;
        const mask = (@as(u256, 1) << @intCast(bit_width)) - 1;
        for (0..num_values) |i| {
            const val = i % (@as(usize, 1) << @intCast(bit_width));
            bits |= @as(u256, val & mask) << @intCast(bit_count);
            bit_count += bit_width;
            while (bit_count >= 8) {
                data[pos] = @intCast(bits & 0xFF);
                pos += 1;
                bits >>= 8;
                bit_count -= 8;
            }
        }
        if (bit_count > 0) {
            data[pos] = @intCast(bits & 0xFF);
        }

        std.debug.print("Benchmarking scalar RLE decoder (bit_width={d}, values={d})...\n", .{ bit_width, num_values });

        var timer = try std.time.Timer.start();
        var sum: u64 = 0;
        
        // Warm up
        var dec = RleDecoder.init(data, bit_width);
        while (try dec.next()) |val| {
            sum += val;
        }
        
        timer.reset();
        const iterations: usize = 100;
        for (0..iterations) |_| {
            dec = RleDecoder.init(data, bit_width);
            while (try dec.next()) |val| {
                sum += val;
            }
        }
        var elapsed = timer.read();
        var avg_ns = elapsed / iterations;
        var throughput = @as(f64, @floatFromInt(num_values)) / (@as(f64, @floatFromInt(avg_ns)) / 1_000_000_000.0);

        std.debug.print("Scalar Average time: {d:.4}ms\n", .{@as(f64, @floatFromInt(avg_ns)) / 1_000_000.0});
        std.debug.print("Scalar Throughput:   {d:.2} MVal/s\n", .{throughput / 1_000_000.0});

        std.debug.print("\nBenchmarking nextBatch RLE decoder (bit_width={d}, values={d})...\n", .{ bit_width, num_values });
        
        const batch_size = 1024;
        var batch_buf = try allocator.alloc(u64, batch_size);
        defer allocator.free(batch_buf);

        timer.reset();
        for (0..iterations) |_| {
            dec = RleDecoder.init(data, bit_width);
            while (true) {
                const n = try dec.nextBatch(batch_buf);
                if (n == 0) break;
                for (batch_buf[0..n]) |val| {
                    sum += val;
                }
            }
        }
        elapsed = timer.read();
        avg_ns = elapsed / iterations;
        throughput = @as(f64, @floatFromInt(num_values)) / (@as(f64, @floatFromInt(avg_ns)) / 1_000_000_000.0);

        std.debug.print("Batch Average time:  {d:.4}ms\n", .{@as(f64, @floatFromInt(avg_ns)) / 1_000_000.0});
        std.debug.print("Batch Throughput:    {d:.2} MVal/s\n", .{throughput / 1_000_000.0});

        std.debug.print("(ignore) sum: {d}\n", .{sum});
    }

    // Null Expansion Benchmark
    std.debug.print("\n" ++ ("*" ** 40) ++ "\n", .{});
    std.debug.print("NULL EXPANSION BENCHMARK\n", .{});
    std.debug.print("*" ** 40 ++ "\n", .{});

    const val_count = 1_000_000;
    const compact_values = try allocator.alloc(u64, val_count);
    defer allocator.free(compact_values);
    for (0..val_count) |i| compact_values[i] = i;

    const out_buffer = try allocator.alloc(?u64, val_count * 2);
    defer allocator.free(out_buffer);

    const mask: u8 = 0b10101010; // 50% nulls
    
    std.debug.print("Benchmarking expandNullsBatch8 (50% nulls)...\n", .{});
    
    var timer = try std.time.Timer.start();
    const iterations = 1000;
    for (0..iterations) |_| {
        var v_idx: usize = 0;
        var o_idx: usize = 0;
        while (o_idx + 8 <= out_buffer.len and v_idx + 8 <= compact_values.len) {
            v_idx += zpq.core.simd.expandNullsBatch8(u64, compact_values[v_idx..], mask, out_buffer[o_idx .. o_idx + 8]);
            o_idx += 8;
        }
    }
    const elapsed = timer.read();
    const avg_ns = elapsed / iterations;
    const total_vals = val_count;
    const throughput = @as(f64, @floatFromInt(total_vals)) / (@as(f64, @floatFromInt(avg_ns)) / 1_000_000_000.0);

    std.debug.print("Average time: {d:.4}ms\n", .{@as(f64, @floatFromInt(avg_ns)) / 1_000_000.0});
    std.debug.print("Throughput:   {d:.2} MVal/s\n", .{throughput / 1_000_000.0});
}

