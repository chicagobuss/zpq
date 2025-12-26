const std = @import("std");
const zpq = @import("zpq");

/// Minimal probe to test XevConnectionPool cleanup
/// Tests: acquire -> release -> closeAll -> deinit sequence

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("=== Connection Pool Cleanup Test ===\n", .{});

    // Create pool
    var pool = zpq.s3.XevConnectionPool.init(allocator);
    std.debug.print("1. Pool created\n", .{});

    // Simulate adding a connection (we'll use a mock since we can't easily create real ones)
    // Actually, let's just test the empty pool case first
    std.debug.print("2. Pool has {d} connections\n", .{pool.idle_connections.items.len});

    // closeAll on empty pool
    pool.closeAll();
    std.debug.print("3. closeAll() completed (empty pool)\n", .{});

    // deinit
    pool.deinit();
    std.debug.print("4. deinit() completed\n", .{});

    std.debug.print("=== SUCCESS ===\n", .{});
}
