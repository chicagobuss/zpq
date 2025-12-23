/// Centralized logging for ZPQ.
///
/// Usage:
///   const log = @import("log.zig");
///   log.debug("my message: {d}", .{value});
///   log.info("something happened", .{});
///
/// All zpq log output is prefixed with [zpq] for easy identification,
/// especially when mixed with vendored library output.
///
/// Log levels are controlled by the standard Zig log level mechanism.
/// In debug builds, all levels are shown by default.
/// In release builds, only warn and err are shown.
const std = @import("std");

/// Scoped loggers for different subsystems
pub const core = std.log.scoped(.zpq_core);
pub const s3 = std.log.scoped(.zpq_s3);
pub const dns = std.log.scoped(.zpq_dns);
pub const tls = std.log.scoped(.zpq_tls);
pub const http = std.log.scoped(.zpq_http);
pub const io = std.log.scoped(.zpq_io);

/// Default zpq logger (for general use)
const default = std.log.scoped(.zpq);

/// Convenience functions that use the default zpq scope
pub fn debug(comptime fmt: []const u8, args: anytype) void {
    default.debug(fmt, args);
}

pub fn info(comptime fmt: []const u8, args: anytype) void {
    default.info(fmt, args);
}

pub fn warn(comptime fmt: []const u8, args: anytype) void {
    default.warn(fmt, args);
}

pub fn err(comptime fmt: []const u8, args: anytype) void {
    default.err(fmt, args);
}
