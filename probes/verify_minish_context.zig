const std = @import("std");
const minish = @import("../vendor/minish/src/lib.zig");
const gen = minish.gen;

// Simple property: reversing a string twice is identity
fn reverseTwice(allocator: std.mem.Allocator, s: []const u8) !void {
    const r1 = try reverse(allocator, s);
    defer allocator.free(r1);
    const r2 = try reverse(allocator, r1);
    defer allocator.free(r2);
    try std.testing.expectEqualStrings(s, r2);
}

fn reverse(allocator: std.mem.Allocator, s: []const u8) ![]u8 {
    const res = try allocator.alloc(u8, s.len);
    for (s, 0..) |c, i| {
        res[s.len - 1 - i] = c;
    }
    return res;
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const string_gen = gen.string(.{ .min_len = 0, .max_len = 100, .charset = .alphanumeric });

    const Context = struct {
        allocator: std.mem.Allocator,
        pub fn run(self: @This(), s: []const u8) !void {
            try reverseTwice(self.allocator, s);
        }
    };
    const ctx = Context{ .allocator = allocator };

    // minish.check expects a function, not a bound method.
    // We need to see if minish supports passing a context/struct with a run/check method.
    // Inspecting runner.zig...
    try minish.check(allocator, string_gen, ctx, .{ .num_runs = 100 });
    std.debug.print("Minish probe passed!\n", .{});
}
