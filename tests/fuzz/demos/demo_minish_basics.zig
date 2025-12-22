const std = @import("std");
const minish = @import("minish");
const gen = minish.gen;
const combinators = minish.combinators;

// --------------------------------------------------------------------------
// 1. Integer Range & Shrinking
// --------------------------------------------------------------------------
// Property: Integers between 10 and 20 are always < 100.
// This should always PASS.
fn prop_integers_in_range(val: i32) !void {
    try std.testing.expect(val < 100);
}

// Property: All generated integers are even.
// This should FAIL and SHRINK to 1.
fn prop_all_even(val: u32) !void {
    try std.testing.expect(val % 2 == 0);
}

// --------------------------------------------------------------------------
// 2. List & Sorting
// --------------------------------------------------------------------------
// Property: Reversing a list twice restores the original.
// Context-aware: needs allocator to reverse the list.
fn prop_reverse_list(ctx: Context, list: []const u8) !void {
    // Clone first because we're about to mutate
    const copy = try ctx.allocator.dupe(u8, list);
    defer ctx.allocator.free(copy);

    std.mem.reverse(u8, copy);
    std.mem.reverse(u8, copy);

    try std.testing.expectEqualSlices(u8, list, copy);
}

const Context = struct {
    allocator: std.mem.Allocator,

    // Wrapper for prop_reverse_list to match Minish runner expectation
    pub fn run(self: @This(), list: []const u8) !void {
        try prop_reverse_list(self, list);
    }
};

// --------------------------------------------------------------------------
// 4. "The Trap" - A subtle bug that property testing finds easily
// --------------------------------------------------------------------------
// Imagine a function that encodes a string but crashes on specific
// rare edge cases, like a string starting with "!!"
fn fragile_encoder(s: []const u8) !void {
    // Hidden bug:
    if (s.len >= 2 and s[0] == '!' and s[1] == '!') {
        return error.InvalidInput; // Simulating a crash/error
    }
}

// --------------------------------------------------------------------------
// 5. "The Shrinker" - Showing off the power of minimization
// --------------------------------------------------------------------------
// Property: all numbers are less than 90.
// Input: 0..100
// Failure: 90..100
// Minish should find a failure (e.g., 95) and shrink it down to 90.
fn prop_less_than_90(val: u32) !void {
    try std.testing.expect(val < 90);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n=== Demo 1: Simple Integer Property (Pass) ===\n", .{});
    // gen.int signature: fn (comptime T: type) Generator(T)
    // Or intRange: fn (comptime T: type, comptime min: T, comptime max: T) Generator(T)
    const int_gen = gen.intRange(i32, 10, 20);
    try minish.check(allocator, int_gen, prop_integers_in_range, .{ .num_runs = 50 });
    std.debug.print("Passed!\n", .{});

    // std.debug.print("\n=== Demo 2: Shrinking (Expect Fail) ===\n", .{});
    // Uncomment to see failure:
    // const u32_gen = gen.int(u32, .{ .min = 0, .max = 100 });
    // try minish.check(allocator, u32_gen, prop_all_even, .{ .num_runs = 50 });

    std.debug.print("\n=== Demo 3: Context-Aware List Property ===\n", .{});
    // gen.list returns []const T, allocated via the test case arena?
    // Or does it assume allocator is passed?
    // Signature: list(comptime T: type, comptime element_gen: Generator(T), comptime min_len: usize, comptime max_len: usize) Generator([]const T)
    const list_gen = gen.list(u8, gen.int(u8), 0, 50);
    const ctx = Context{ .allocator = allocator };
    try minish.check(allocator, list_gen, ctx, .{ .num_runs = 50 });
    std.debug.print("Passed!\n", .{});

    std.debug.print("\n=== Demo 4: 'The Trap' (Finding Edge Cases) ===\n", .{});
    // With 100 runs, it's highly likely to hit "!!" at the start if we generate
    // random strings.
    const trap_gen = gen.string(.{ .min_len = 0, .max_len = 10, .charset = .ascii });
    // We expect this to fail occasionally. To make it deterministic for demo,
    // we could force a seed or just show it running.
    // Uncomment to run:
    // try minish.check(allocator, trap_gen, fragile_encoder, .{ .num_runs = 1000 });
    _ = trap_gen; // Suppress unused var

    std.debug.print("\n=== Demo 5: 'The Shrinker' (Minimization) ===\n", .{});
    // This will fail on any number >= 90.
    // Minish should report "Minimal failing input: 90"
    const shrink_gen = gen.intRange(u32, 0, 100);
    // Uncomment to run:
    // try minish.check(allocator, shrink_gen, prop_less_than_90, .{ .num_runs = 100 });
    _ = shrink_gen; // Suppress unused var
}

test "run demos" {
    try main();
}
