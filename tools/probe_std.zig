const std = @import("std");

pub fn main() void {
    const linux = std.os.linux;
    const posix = std.posix;

    std.debug.print("Checking std.os.linux...\n", .{});
    // Check for getErrno or similar
    if (@hasDecl(linux, "getErrno")) {
        std.debug.print("linux.getErrno exists\n", .{});
    } else {
        std.debug.print("linux.getErrno INVALID\n", .{});
    }

    // Check return type of recvmsg / accept4 to see if they return usize/int or error union
    const recvmsg_ret = @typeInfo(@TypeOf(linux.recvmsg)).@"fn".return_type.?;
    std.debug.print("linux.recvmsg return type: {any}\n", .{recvmsg_ret});

    const accept4_ret = @typeInfo(@TypeOf(linux.accept4)).@"fn".return_type.?;
    std.debug.print("linux.accept4 return type: {any}\n", .{accept4_ret});

    // Inspect how typical syscalls return errors in this version
    // Usually it's usize and we cast to E enum or check if it's in -4095 range.
    
    std.debug.print("std.os.linux.E decls:\n", .{});
    inline for (@typeInfo(linux.E).@"enum".fields) |f| {
        std.debug.print("{s}\n", .{f.name});
    }

    // Check specific posix errors often missing
    std.debug.print("posix.AcceptError: {any}\n", .{posix.AcceptError});
}
