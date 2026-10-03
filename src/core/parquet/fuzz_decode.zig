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
//! Three layers, narrowest blast radius first:
//!   1. footer framing — the magic + footer-length math in `metadata.open`
//!      (offset arithmetic that must not underflow on a hostile length).
//!   2. thrift metadata decode — the varint/zigzag/struct reader driving
//!      `schema.FileMetaData.read`, the rich target where a malformed field
//!      table could walk off the end of the footer slice.
//!   3. legacy LZ4 (codec 5) pages — the Hadoop/block/frame fallback chain,
//!      whose length fields come straight from the file.
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
const compression = @import("compression.zig");
const lz4 = @import("lz4.zig");

// --- Targets (shared by both drivers) ----------------------------------

/// Footer framing: magic check + footer-length math + thrift parse.
fn tryOpen(arena: std.mem.Allocator, bytes: []const u8) void {
    // FileMetaData or a clean error are both valid; traps are not.
    _ = metadata.open(arena, bytes) catch {};
}

/// Legacy LZ4 page decode: the whole fallback chain plus each container parser on its own, into an `out` the
/// caller sized (the page header's claim, which an attacker also controls).
fn tryLz4Legacy(arena: std.mem.Allocator, src: []const u8, out: []u8) void {
    compression.decompressInto(arena, src, .LZ4, out) catch {};
    lz4.uncompressHadoop(src, out) catch {};
    _ = lz4.uncompressFrame(src, out) catch {};
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

const LZ4_ITERS: usize = 20_000;
const LZ4_SEED: u64 = 0x2026_10_03;

/// Seeds for the LZ4 mutator: well-formed Hadoop (multi-chunk), bare-block and frame pages, so mutations land
/// next to valid structure instead of bouncing off the first length check.
fn lz4Seeds(arena: std.mem.Allocator) !struct { pages: [5][]const u8, sizes: [5]usize } {
    const line = "parquet-mr wrote hadoop framing; parquet-cpp wrote bare blocks; arrow-rs wrote frames. ";
    const text = line ++ line ++ line ++ line;
    const block = try compression.compress(arena, text, .LZ4_RAW);
    // One chunk split over three pieces (one empty) under a single length, then an ordinary one-piece chunk.
    var hadoop: std.ArrayList(u8) = .empty;
    var word: [4]u8 = undefined;
    var off: usize = 0;
    for ([_]usize{ 100, 0, 150, text.len - 250 }, 0..) |n, i| {
        if (i == 0 or i == 3) {
            std.mem.writeInt(u32, &word, @intCast(if (i == 0) 250 else n), .big);
            try hadoop.appendSlice(arena, &word);
        }
        const part = try compression.compress(arena, text[off..][0..n], .LZ4_RAW);
        std.mem.writeInt(u32, &word, @intCast(part.len), .big);
        try hadoop.appendSlice(arena, &word);
        try hadoop.appendSlice(arena, part);
        off += n;
    }
    return .{
        .pages = .{ hadoop.items, block, &lz4.frame_all_checks, &lz4.frame_plain, &lz4.frame_linked },
        .sizes = .{ text.len, text.len, 41, 41, 37 },
    };
}

test "fuzz-lite: mutated legacy LZ4 pages decode or fail cleanly" {
    var seed_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer seed_arena.deinit();
    const seeds = try lz4Seeds(seed_arena.allocator());
    // The unmutated seeds must decode, or the loop below would only ever exercise error paths.
    for (seeds.pages, seeds.sizes) |page, size| {
        const out = try seed_arena.allocator().alloc(u8, size);
        try compression.decompressInto(seed_arena.allocator(), page, .LZ4, out);
    }

    var prng = std.Random.DefaultPrng.init(LZ4_SEED);
    const rand = prng.random();
    var buf: [1024]u8 = undefined;
    var out_buf: [1024]u8 = undefined;
    const lengths = [_]u32{ 0, 1, 7, 8, 41, 0x7FFF_FFFF, 0x8000_0000, 0xFFFF_FFFF };

    var i: usize = 0;
    while (i < LZ4_ITERS) : (i += 1) {
        const pick = rand.uintLessThan(usize, seeds.pages.len);
        const seed = seeds.pages[pick];
        var n = seed.len;
        @memcpy(buf[0..n], seed);

        var m = rand.intRangeAtMost(usize, 1, 4);
        while (m > 0) : (m -= 1) switch (rand.uintLessThan(u8, 6)) {
            0 => if (n > 0) {
                buf[rand.uintLessThan(usize, n)] ^= @as(u8, 1) << rand.int(u3);
            },
            1 => if (n > 0) {
                buf[rand.uintLessThan(usize, n)] = rand.int(u8);
            },
            // A hostile length field, either endianness, anywhere a 4-byte field fits.
            2 => if (n >= 4) {
                const at = rand.uintLessThan(usize, n - 3);
                const v = if (rand.boolean()) lengths[rand.uintLessThan(usize, lengths.len)] else rand.int(u32);
                std.mem.writeInt(u32, buf[at..][0..4], v, if (rand.boolean()) .big else .little);
            },
            3 => n = rand.uintAtMost(usize, n), // truncate
            4 => if (n < buf.len) { // insert a random byte
                const at = rand.uintAtMost(usize, n);
                std.mem.copyBackwards(u8, buf[at + 1 .. n + 1], buf[at..n]);
                buf[at] = rand.int(u8);
                n += 1;
            },
            else => if (n > 0) { // delete a byte
                const at = rand.uintLessThan(usize, n);
                std.mem.copyForwards(u8, buf[at .. n - 1], buf[at + 1 .. n]);
                n -= 1;
            },
        };

        // The page header's size is attacker-controlled too: honest, off by a little, or arbitrary.
        const honest = seeds.sizes[pick];
        const out_len = switch (rand.uintLessThan(u8, 3)) {
            0 => honest,
            1 => @min(out_buf.len, (honest + rand.uintAtMost(usize, 8)) -| 4),
            else => rand.uintAtMost(usize, out_buf.len),
        };

        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        tryLz4Legacy(arena.allocator(), buf[0..n], out_buf[0..out_len]);
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

fn fuzzLz4Legacy(buf: *FuzzBuf, smith: *std.testing.Smith) anyerror!void {
    @disableInstrumentation();
    const n = smith.slice(buf);
    if (n < 2) return;
    // The first two bytes pick the claimed page size; the rest is the page.
    const out_len = std.mem.readInt(u16, buf[0..2], .little);
    const out = try std.testing.allocator.alloc(u8, out_len);
    defer std.testing.allocator.free(out);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    tryLz4Legacy(arena.allocator(), buf[2..n], out);
}

test "fuzz: legacy LZ4 page decode tolerates arbitrary bytes" {
    const buf = try std.testing.allocator.create(FuzzBuf);
    defer std.testing.allocator.destroy(buf);
    try std.testing.fuzz(buf, fuzzLz4Legacy, .{});
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
        const bad: [12]u8 = @splat(0);
        try std.testing.expectError(error.BadMagic, metadata.open(a, &bad));
    }

    // Valid PAR1 magic at both ends, but footer_len (0xFFFFFFFF) far
    // exceeds the available bytes — the guard at metadata.zig:51 must
    // reject this before the slice underflows.
    {
        var b: [16]u8 = @splat(0);
        @memcpy(b[0..4], "PAR1");
        @memcpy(b[8..12], &[_]u8{ 0xFF, 0xFF, 0xFF, 0xFF });
        @memcpy(b[12..16], "PAR1");
        try std.testing.expectError(error.FooterTooLarge, metadata.open(a, &b));
    }
}
