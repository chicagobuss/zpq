//! Winsock operations used by the in-process test servers on Windows.
const std = @import("std");
const clock = @import("../clock.zig");
const ws = std.os.windows.ws2_32;
pub const fd_t = usize;
pub const socklen_t = u32;
pub const sockaddr = ws.sockaddr;
pub const AF = ws.AF;
pub const SOCK = ws.SOCK;
pub const SOL = ws.SOL;
pub const SO = ws.SO;
pub const TCP = ws.TCP;
pub const IPPROTO = ws.IPPROTO;
pub const MSG = struct {};
pub const SHUT = struct {
    pub const RDWR = 2;
    pub const WR = 1;
};
pub const POLL = struct {
    pub const IN: i16 = 0x100;
    pub const OUT: i16 = 0x10;
};
pub const pollfd = extern struct { fd: fd_t, events: i16, revents: i16 = 0 };
pub const timespec = struct { sec: i64, nsec: i64 };
pub const timeval = extern struct { sec: i64, usec: i64 };
pub const E = enum { SUCCESS, INTR, AGAIN, CONNABORTED, OTHER };

extern "ws2_32" fn WSAStartup(u16, *anyopaque) callconv(.winapi) c_int;
extern "ws2_32" fn WSAGetLastError() callconv(.winapi) c_int;
extern "ws2_32" fn socket(c_int, c_int, c_int) callconv(.winapi) usize;
extern "ws2_32" fn closesocket(usize) callconv(.winapi) c_int;
extern "ws2_32" fn bind(usize, *const sockaddr, c_int) callconv(.winapi) c_int;
extern "ws2_32" fn listen(usize, c_int) callconv(.winapi) c_int;
extern "ws2_32" fn connect(usize, *const sockaddr, c_int) callconv(.winapi) c_int;
extern "ws2_32" fn getsockname(usize, *sockaddr, *c_int) callconv(.winapi) c_int;
extern "ws2_32" fn accept(usize, ?*sockaddr, ?*c_int) callconv(.winapi) usize;
extern "ws2_32" fn shutdown(usize, c_int) callconv(.winapi) c_int;
extern "ws2_32" fn setsockopt(usize, c_int, c_int, *const anyopaque, c_int) callconv(.winapi) c_int;
extern "ws2_32" fn WSAPoll([*]pollfd, u32, c_int) callconv(.winapi) c_int;
extern "ws2_32" fn recv(usize, [*]u8, c_int, c_int) callconv(.winapi) c_int;
extern "ws2_32" fn send(usize, [*]const u8, c_int, c_int) callconv(.winapi) c_int;

var started: std.atomic.Value(u8) = .init(0);
fn startup() bool {
    if (started.cmpxchgStrong(0, 1, .acq_rel, .acquire) == null) {
        var data: [512]u8 align(8) = undefined;
        const ok = WSAStartup(0x202, &data) == 0;
        started.store(if (ok) 2 else 3, .release);
        return ok;
    }
    while (started.load(.acquire) == 1) std.atomic.spinLoopHint();
    return started.load(.acquire) == 2;
}

pub fn errno(value: anytype) E {
    if (value != -1) return .SUCCESS;
    return switch (WSAGetLastError()) {
        10004 => .INTR,
        10035 => .AGAIN,
        10053 => .CONNABORTED,
        else => .OTHER,
    };
}

pub const system = struct {
    pub fn socket(family: u32, kind: u32, protocol: u32) isize {
        if (!startup()) return -1;
        return @bitCast(@call(.auto, @field(@import("win_test_socket.zig"), "socket"), .{ @as(c_int, @intCast(family)), @as(c_int, @intCast(kind)), @as(c_int, @intCast(protocol)) }));
    }
    pub fn socketpair(_: u32, _: u32, _: u32, pair: *[2]fd_t) isize {
        if (!startup()) return -1;
        const listener = @call(.auto, @field(@import("win_test_socket.zig"), "socket"), .{ 2, 1, 6 });
        if (listener == std.math.maxInt(usize)) return -1;
        defer _ = closesocket(listener);
        var addr: sockaddr.in = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f000001) };
        if (@call(.auto, @field(@import("win_test_socket.zig"), "bind"), .{ listener, @as(*const sockaddr, @ptrCast(&addr)), @as(c_int, @sizeOf(sockaddr.in)) }) != 0) return -1;
        if (@call(.auto, @field(@import("win_test_socket.zig"), "listen"), .{ listener, 1 }) != 0) return -1;
        var len: c_int = @sizeOf(sockaddr.in);
        if (@call(.auto, @field(@import("win_test_socket.zig"), "getsockname"), .{ listener, @as(*sockaddr, @ptrCast(&addr)), &len }) != 0) return -1;
        const client = @call(.auto, @field(@import("win_test_socket.zig"), "socket"), .{ 2, 1, 6 });
        if (client == std.math.maxInt(usize)) return -1;
        if (@call(.auto, @field(@import("win_test_socket.zig"), "connect"), .{ client, @as(*const sockaddr, @ptrCast(&addr)), @as(c_int, @sizeOf(sockaddr.in)) }) != 0) {
            _ = closesocket(client);
            return -1;
        }
        const server = @call(.auto, @field(@import("win_test_socket.zig"), "accept"), .{ listener, null, null });
        if (server == std.math.maxInt(usize)) {
            _ = closesocket(client);
            return -1;
        }
        pair.* = .{ client, server };
        return 0;
    }
    pub fn close(fd: fd_t) c_int {
        return closesocket(fd);
    }
    pub fn bind(fd: fd_t, addr: *const sockaddr, len: u32) c_int {
        return @call(.auto, @field(@import("win_test_socket.zig"), "bind"), .{ fd, addr, @as(c_int, @intCast(len)) });
    }
    pub fn listen(fd: fd_t, backlog: c_int) c_int {
        return @call(.auto, @field(@import("win_test_socket.zig"), "listen"), .{ fd, backlog });
    }
    pub fn getsockname(fd: fd_t, addr: *sockaddr, len: *socklen_t) c_int {
        return @call(.auto, @field(@import("win_test_socket.zig"), "getsockname"), .{ fd, addr, @as(*c_int, @ptrCast(len)) });
    }
    pub fn accept(fd: fd_t, addr: ?*sockaddr, len: ?*c_int) isize {
        return @bitCast(@call(.auto, @field(@import("win_test_socket.zig"), "accept"), .{ fd, addr, len }));
    }
    pub fn shutdown(fd: fd_t, how: c_int) c_int {
        return @call(.auto, @field(@import("win_test_socket.zig"), "shutdown"), .{ fd, how });
    }
    pub fn setsockopt(fd: fd_t, level: u32, opt: u32, value: *const anyopaque, len: usize) c_int {
        if (level == SOL.SOCKET and (opt == SO.RCVTIMEO or opt == SO.SNDTIMEO)) {
            const tv: *const timeval = @ptrCast(@alignCast(value));
            const ms: u32 = @intCast(@max(0, tv.sec * 1000 + @divTrunc(tv.usec, 1000)));
            return @call(.auto, @field(@import("win_test_socket.zig"), "setsockopt"), .{ fd, @as(c_int, @intCast(level)), @as(c_int, @intCast(opt)), @as(*const anyopaque, @ptrCast(&ms)), @as(c_int, @sizeOf(u32)) });
        }
        if (level == SOL.SOCKET and opt == SO.LINGER) {
            const old: *const [2]c_int = @ptrCast(@alignCast(value));
            const linger: [2]u16 = .{ @intCast(old[0]), @intCast(old[1]) };
            return @call(.auto, @field(@import("win_test_socket.zig"), "setsockopt"), .{ fd, @as(c_int, @intCast(level)), @as(c_int, @intCast(opt)), @as(*const anyopaque, @ptrCast(&linger)), @as(c_int, @sizeOf(@TypeOf(linger))) });
        }
        return @call(.auto, @field(@import("win_test_socket.zig"), "setsockopt"), .{ fd, @as(c_int, @intCast(level)), @as(c_int, @intCast(opt)), value, @as(c_int, @intCast(len)) });
    }
    pub fn poll(fds: anytype, len: usize, timeout_ms: c_int) c_int {
        return WSAPoll(@ptrCast(fds), @intCast(len), timeout_ms);
    }
    pub fn read(fd: fd_t, bytes: [*]u8, len: usize) c_int {
        return recv(fd, bytes, @intCast(@min(len, std.math.maxInt(c_int))), 0);
    }
    pub fn write(fd: fd_t, bytes: [*]const u8, len: usize) c_int {
        return send(fd, bytes, @intCast(@min(len, std.math.maxInt(c_int))), 0);
    }
    pub fn sendto(fd: fd_t, bytes: [*]const u8, len: usize, _: u32, _: ?*const sockaddr, _: u32) c_int {
        return send(fd, bytes, @intCast(@min(len, std.math.maxInt(c_int))), 0);
    }
    pub fn nanosleep(ts: *const timespec, _: ?*timespec) c_int {
        clock.sleepNs(@intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec));
        return 0;
    }
};
