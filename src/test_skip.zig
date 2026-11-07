//! Named skips. A unit test the target cannot run says why in a form the check runner records and its verdict judges
//! against the host that ran it: a platform's skips are excused by what the receipt shows of that host, not by a list
//! of test names that falls behind as tests are added.
//!
//!     if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "epoll");
//!
//! The condition is known at compile time, as `builtin.os.tag != .linux` was, so a target that lacks the need never
//! analyzes the rest of the test (Linux-only calls would not compile there). `skipNeeding` refuses to compile where the
//! target meets the need, so a reason is never named for a skip that cannot happen. Zig's test runner prints the test's number and name, then whatever the test prints, then `SKIP`, so the
//! log line reads
//!
//!     12/40 io.epoll.test.timer fires within deadline...[skip needs linux: epoll] SKIP
//!
//! `parseLine` reads that line back (`tools/check/receipt.zig`), and the verdict accepts the skip only when the host
//! the receipt records does not meet the need. A skip with no reason, or one a Linux host reports for a Linux need,
//! never passes. Only a run whose stderr is not a terminal prints the number and name first; the check runner always
//! captures through pipes.

const std = @import("std");
const builtin = @import("builtin");

/// What a test needs of the host that no setup supplies. Each need is judged against the host the receipt records.
pub const Need = enum {
    /// The Linux kernel interfaces the test drives: epoll, io_uring, mremap, madvise and mincore residency, procfs.
    linux,

    /// Whether the OS named `os_name` (`uname`'s sysname) meets the need.
    pub fn metBy(self: Need, os_name: []const u8) bool {
        return switch (self) {
            .linux => std.ascii.eqlIgnoreCase(os_name, "linux"),
        };
    }

    fn holds(self: Need) bool {
        return switch (self) {
            .linux => builtin.os.tag == .linux,
        };
    }
};

// The runner prints `...` after a test's name; the reason follows it and `SKIP` ends the line.
const open = "...[skip needs ";
const close = "] SKIP";

/// The text `require` prints between the runner's `...` and `SKIP`.
pub fn mark(comptime need: Need, comptime what: []const u8) []const u8 {
    comptime {
        if (what.len == 0 or std.mem.indexOfAny(u8, what, "]\r\n") != null)
            @compileError("a skip reason is one line, non-empty, without ']'");
    }
    return open[3..] ++ @tagName(need) ++ ": " ++ what ++ close[0 .. close.len - "SKIP".len];
}

/// Whether the target lacks `need`. Call it as `comptime lacks(...)` in the test's first `if`.
pub fn lacks(comptime need: Need) bool {
    return !need.holds();
}

/// The skip for a target that lacks `need`; `what` names the interface the test drives.
pub fn skipNeeding(comptime need: Need, comptime what: []const u8) error{SkipZigTest} {
    comptime if (need.holds()) @compileError("this target meets the need; there is nothing to skip");
    std.debug.print("{s}", .{comptime mark(need, what)});
    return error.SkipZigTest;
}

pub const Parsed = struct { name: []const u8, need: Need, what: []const u8 };

/// A runner line `N/M name...[skip needs NEED: WHAT] SKIP` as its parts; null for any other line, including a plain
/// `...SKIP` and a need this build does not know (which then counts as a skip with no reason).
pub fn parseLine(line: []const u8) ?Parsed {
    if (!std.mem.endsWith(u8, line, close)) return null;
    const at = std.mem.lastIndexOf(u8, line, open) orelse return null;
    const space = std.mem.indexOfScalar(u8, line, ' ') orelse return null;
    if (space + 1 > at) return null;
    const body = line[at + open.len .. line.len - close.len];
    const colon = std.mem.indexOf(u8, body, ": ") orelse return null;
    const need = std.meta.stringToEnum(Need, body[0..colon]) orelse return null;
    return .{ .name = line[space + 1 .. at], .need = need, .what = body[colon + 2 ..] };
}

const testing = std.testing;

test "a named skip reads back as the runner printed it" {
    const printed = "12/40 io.epoll.test.timer fires within deadline..." ++ comptime mark(.linux, "epoll") ++ "SKIP";
    try testing.expectEqualStrings("12/40 io.epoll.test.timer fires within deadline...[skip needs linux: epoll] SKIP", printed);
    const p = parseLine(printed).?;
    try testing.expectEqualStrings("io.epoll.test.timer fires within deadline", p.name);
    try testing.expectEqual(Need.linux, p.need);
    try testing.expectEqualStrings("epoll", p.what);
    // A name with dots, spaces and its own ellipsis still ends where the reason starts.
    const odd = parseLine("1/2 a test... of things...[skip needs linux: x: y] SKIP").?;
    try testing.expectEqualStrings("a test... of things", odd.name);
    try testing.expectEqualStrings("x: y", odd.what);
}

test "a skip line that names no known need parses as nothing" {
    try testing.expect(parseLine("3/9 plain...SKIP") == null);
    try testing.expect(parseLine("3/9 plain...OK") == null);
    try testing.expect(parseLine("3/9 t...[skip needs windows: wsa] SKIP") == null);
    try testing.expect(parseLine("3/9 t...[skip needs linux epoll] SKIP") == null);
    try testing.expect(parseLine("[skip needs linux: x] SKIP") == null);
    try testing.expect(parseLine("") == null);
}

test "a skip's need is met by the OS that provides it, whatever the case uname gives" {
    try testing.expect(Need.linux.metBy("Linux"));
    try testing.expect(Need.linux.metBy("linux"));
    try testing.expect(!Need.linux.metBy("Darwin"));
    try testing.expect(!Need.linux.metBy(""));
}

test "a skip is not taken where the target meets the need" {
    try testing.expectEqual(builtin.os.tag != .linux, comptime lacks(.linux));
}
