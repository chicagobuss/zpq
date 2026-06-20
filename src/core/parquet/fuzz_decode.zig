//! Fuzz harness for the Parquet read path — the front door for untrusted,
//! adversarial files.
//!
//! Contract under test: parsing *arbitrary* bytes must never crash, read
//! out of bounds, or overflow — it must either return a fully-formed
//! structure or a clean error. ZPQ ships ReleaseFast (Zig's runtime safety
//! checks OFF), so these targets earn their keep by running under the
//! safety-checked test build: any OOB / overflow / UB reached on hostile
//! input surfaces here as a clean trap instead of silent corruption in
//! production. This is the cheapest recovery of the memory-safety guarantee
//! a Rust build would give for free.
//!
//! Two layers, narrowest blast radius first:
//!   1. footer framing — the magic + footer-length math in `metadata.open`
//!      (offset arithmetic that must not underflow on a hostile length).
//!   2. thrift metadata decode — the varint/zigzag/struct reader driving
//!      `schema.FileMetaData.read`, the rich target where a malformed field
//!      table could walk off the end of the footer slice.
//!
//! TWO DRIVERS, because Zig 0.16.0's coverage-guided fuzzer is currently
//! unusable (its bundled test_runner.zig fails to compile in `-ffuzz` mode:
//! it passes `@errorReturnTrace()` to `std.debug.writeStackTrace`, which
//! wants the *other* StackTrace type). Until a toolchain fix lands:
//!   - `fuzzLite*` — a reproducible PRNG loop that runs in the normal
//!     safety-checked suite (`zig build test`) on every PR. Real input
//!     variety, deterministic seed, works in the normal test suite.
//!   - `std.testing.fuzz` targets — wired and ready; `just fuzz` lights
//!     them up the moment `--fuzz` compiles again.

const std = @import("std");
const metadata = @import("metadata.zig");
const schema = @import("../schema.zig");
const thrift = @import("../thrift.zig");

// --- Targets (shared by both drivers) ----------------------------------

/// Footer framing: magic check + footer-length math + thrift parse.
fn tryOpen(arena: std.mem.Allocator, bytes: []const u8) void {
    // FileMetaData or a clean error are both valid; traps are not.
    _ = metadata.open(arena, bytes) catch {};
}

/// Thrift metadata decode, bypassing the framing so random bytes land
/// directly in the varint/struct reader (the richer target).
fn tryThrift(arena: std.mem.Allocator, bytes: []const u8) void {
    var reader = thrift.Reader.init(bytes);
    _ = schema.FileMetaData.read(arena, &reader) catch {};
}

// --- Driver A: reproducible PRNG loop (works on current toolchain) ------

const LITE_ITERS: usize = 20_000;
const LITE_MAX_LEN: usize = 4096;
const LITE_SEED: u64 = 0x2026_06_12; // fixed → reproducible failures

test "fuzz-lite: randomized decode inputs stay crash-free" {
    var prng = std.Random.DefaultPrng.init(LITE_SEED);
    const rand = prng.random();
    var scratch: [LITE_MAX_LEN]u8 = undefined;

    var i: usize = 0;
    while (i < LITE_ITERS) : (i += 1) {
        const n = rand.intRangeAtMost(usize, 0, scratch.len);
        const bytes = scratch[0..n];
        rand.bytes(bytes);

        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();

        // Raw bytes straight into the thrift reader (no magic gate).
        tryThrift(arena.allocator(), bytes);

        // Wrap with valid magic so the footer-length math is exercised
        // (otherwise random bytes bounce off the BadMagic check).
        if (n >= 12) {
            @memcpy(bytes[0..4], "PAR1");
            @memcpy(bytes[n - 4 .. n], "PAR1");
            tryOpen(arena.allocator(), bytes);
        }
    }
}

// --- Driver B: std.testing.fuzz (coverage-guided; pending toolchain fix) -

const FuzzBuf = [64 * 1024]u8;

fn fuzzFooterFraming(buf: *FuzzBuf, smith: *std.testing.Smith) anyerror!void {
    @disableInstrumentation();
    const n = smith.slice(buf);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    tryOpen(arena.allocator(), buf[0..n]);
}

test "fuzz: parquet footer framing tolerates arbitrary bytes" {
    const buf = try std.testing.allocator.create(FuzzBuf);
    defer std.testing.allocator.destroy(buf);
    try std.testing.fuzz(buf, fuzzFooterFraming, .{});
}

fn fuzzThriftMetadata(buf: *FuzzBuf, smith: *std.testing.Smith) anyerror!void {
    @disableInstrumentation();
    const n = smith.slice(buf);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    tryThrift(arena.allocator(), buf[0..n]);
}

test "fuzz: thrift FileMetaData decode tolerates arbitrary bytes" {
    const buf = try std.testing.allocator.create(FuzzBuf);
    defer std.testing.allocator.destroy(buf);
    try std.testing.fuzz(buf, fuzzThriftMetadata, .{});
}

// --- Deterministic seed cases ------------------------------------------
// Lock the framing guards as plain regression tests, independent of any
// fuzzer running.

test "footer framing: degenerate inputs return clean errors" {
    const a = std.testing.allocator;

    // Too small to hold even the magic + footer-length frame.
    try std.testing.expectError(error.TooSmall, metadata.open(a, ""));
    try std.testing.expectError(error.TooSmall, metadata.open(a, "PAR1"));

    // Frame-sized but wrong magic.
    {
        const bad = [_]u8{0} ** 12;
        try std.testing.expectError(error.BadMagic, metadata.open(a, &bad));
    }

    // Valid PAR1 magic at both ends, but footer_len (0xFFFFFFFF) far
    // exceeds the available bytes — the guard at metadata.zig:51 must
    // reject this before the slice underflows.
    {
        var b = [_]u8{0} ** 16;
        @memcpy(b[0..4], "PAR1");
        @memcpy(b[8..12], &[_]u8{ 0xFF, 0xFF, 0xFF, 0xFF });
        @memcpy(b[12..16], "PAR1");
        try std.testing.expectError(error.FooterTooLarge, metadata.open(a, &b));
    }
}
