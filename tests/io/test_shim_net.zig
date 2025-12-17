const std = @import("std");
const shim = @import("../../vendor/libxev/src/shim_net.zig");

pub fn main() !void {
    // 1. Test Parsing 127.0.0.1:80
    const addr = try shim.Address.parseIp4("127.0.0.1", 80);
    
    std.debug.print("Parsed 127.0.0.1:80\n", .{});
    std.debug.print("Family: {}\n", .{addr.in.family});
    std.debug.print("Port: {}\n", .{addr.in.port}); // Should be BE
    std.debug.print("Addr: {x}\n", .{addr.in.addr});

    // 2. Verify Port Endianness (80 = 0x0050)
    // Network Order (BE): 00 50.
    // Native u16 (LE): 50 00 (20480).
    // shim stores as BE.
    // If we read as u16 on LE, we see 0x5000 (20480).
    if (addr.in.port != std.mem.nativeToBig(u16, 80)) {
         std.debug.print("ERROR: Port mismatch! Expected {}, got {}\n", .{std.mem.nativeToBig(u16, 80), addr.in.port});
         return error.TestFailed;
    }

    // 3. Verify Addr Endianness
    // 127.0.0.1 -> Bytes: 7F 00 00 01
    // shim.Address.in.addr is u32.
    const bytes = std.mem.asBytes(&addr.in.addr);
    std.debug.print("Bytes: {x} {x} {x} {x}\n", .{bytes[0], bytes[1], bytes[2], bytes[3]});
    
    if (bytes[0] != 127 or bytes[1] != 0 or bytes[2] != 0 or bytes[3] != 1) {
        std.debug.print("ERROR: Addr Bytes mismatch! Expected 7F 00 00 01\n", .{});
        return error.TestFailed;
    }

    std.debug.print("Shim OK!\n", .{});
}

