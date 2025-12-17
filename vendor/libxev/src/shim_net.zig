const std = @import("std");

pub const Address = extern union {
    any: std.posix.sockaddr,
    in: std.posix.sockaddr.in,
    in6: std.posix.sockaddr.in6,

    pub fn parseIp4(ip: []const u8, port: u16) !Address {
        const addr = try std.Io.net.Ip4Address.parse(ip, port);
        // bytes is [4]u8 network order.
        // sockaddr_in.addr is u32.
        const addr_u32 = @as(u32, @bitCast(addr.bytes));

        return .{
            .in = .{
                .family = std.posix.AF.INET,
                .port = std.mem.nativeToBig(u16, port),
                .addr = addr_u32,
                .zero = [_]u8{0} ** 8,
            }
        };
    }

    pub fn initPosix(ptr: *const anyopaque) Address {
        const sa = @as(*const std.posix.sockaddr, @ptrCast(@alignCast(ptr)));
        var addr: Address = undefined;
        switch (sa.family) {
            std.posix.AF.INET => addr.in = @as(*const std.posix.sockaddr.in, @ptrCast(@alignCast(ptr))).*,
            std.posix.AF.INET6 => addr.in6 = @as(*const std.posix.sockaddr.in6, @ptrCast(@alignCast(ptr))).*,
            else => addr.any = sa.*,
        }
        return addr;
    }

    pub fn getOsSockLen(self: Address) std.posix.socklen_t {
        return switch (self.any.family) {
            std.posix.AF.INET => @sizeOf(std.posix.sockaddr.in),
            std.posix.AF.INET6 => @sizeOf(std.posix.sockaddr.in6),
            else => @sizeOf(std.posix.sockaddr),
        };
    }
};

