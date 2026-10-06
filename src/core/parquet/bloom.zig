//! Parquet split-block Bloom filters (SBBF): the on-disk format, the probe, and the writer-side builder.
//!
//! Spec: parquet-format BloomFilter.md. A chunk's filter is a thrift `BloomFilterHeader` (numBytes, BLOCK, XXHASH,
//! UNCOMPRESSED) followed by `numBytes` of bitset: z blocks of eight little-endian u32 words. A value is hashed with
//! XXH64 (seed 0) over its PLAIN bytes, BYTE_ARRAY without the length prefix, as parquet-java, Arrow C++, arrow-rs,
//! DuckDB and Hardwood all do. The upper 32 hash bits pick a block by fastrange, the lower 32 pick one bit per word.
//!
//! A filter has false positives only, so "absent" is a proof and anything else must keep the row group. This module
//! knows nothing about queries; `filter/membership.zig` decides what to probe.

const std = @import("std");
const builtin = @import("builtin");
const thrift = @import("../thrift.zig");

pub const salt = [8]u32{
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
    0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31,
};

pub const block_bytes = 32;

/// The cap parquet-java, Arrow C++ and arrow-rs apply; a header claiming more is treated as corrupt.
pub const max_bitset_bytes: u32 = 128 * 1024 * 1024;

/// Bytes enough for any header `writeHeader` emits (and any canonical one): 1 + 5 + 13.
pub const max_header_bytes = 19;

pub const Header = struct {
    num_bytes: u32,
    /// Serialized header length; the bitset starts here.
    len: usize,
};

/// Parse the header at the start of `bytes`. Null, never an error, for anything this reader cannot use: another
/// algorithm, hash or compression, a truncated header, or a bitset size that is not a positive multiple of a block.
pub fn parseHeader(bytes: []const u8) ?Header {
    var r = thrift.Reader.init(bytes);
    r.readStructBegin();
    var num_bytes: ?i32 = null;
    var algorithm = false;
    var hash = false;
    var compression = false;
    while (true) {
        const f = r.readFieldBegin() catch return null;
        if (f.type == .Stop) break;
        switch (f.id) {
            1 => {
                if (f.type != .I32) return null;
                num_bytes = r.readZigZag(i32) catch return null;
            },
            2, 3, 4 => {
                if (f.type != .Struct) return null;
                // Each is a union whose only member this reader implements is the empty struct at id 1.
                if (!(unionIsMemberOne(&r) catch return null)) return null;
                switch (f.id) {
                    2 => algorithm = true,
                    3 => hash = true,
                    else => compression = true,
                }
            },
            else => r.skip(f.type) catch return null,
        }
    }
    const n = num_bytes orelse return null;
    if (!algorithm or !hash or !compression) return null;
    if (n <= 0 or @rem(n, block_bytes) != 0 or @as(u32, @intCast(n)) > max_bitset_bytes) return null;
    return .{ .num_bytes = @intCast(n), .len = r.pos };
}

fn unionIsMemberOne(r: *thrift.Reader) !bool {
    const saved = r.last_field_id;
    r.last_field_id = 0;
    defer r.last_field_id = saved;
    var one = false;
    var other = false;
    while (true) {
        const f = try r.readFieldBegin();
        if (f.type == .Stop) break;
        if (f.id == 1 and f.type == .Struct) one = true else other = true;
        try r.skip(f.type);
    }
    return one and !other;
}

/// The bitset of a filter whose header and bitset start `bytes`, or null when unusable or truncated.
pub fn bitset(bytes: []const u8) ?[]const u8 {
    const h = parseHeader(bytes) orelse return null;
    if (h.num_bytes > bytes.len - h.len) return null;
    return bytes[h.len..][0..h.num_bytes];
}

/// Serialize the header every known writer emits: numBytes, then the three one-member unions in field order.
pub fn writeHeader(num_bytes: u32, buf: *[max_header_bytes]u8) []const u8 {
    var w: std.Io.Writer = .fixed(buf);
    w.writeByte(0x15) catch unreachable; // field 1, i32
    w.writeUleb128(@as(u64, num_bytes) << 1) catch unreachable; // zigzag of a non-negative i32
    w.writeAll(&.{ 0x1c, 0x1c, 0x00, 0x00, 0x1c, 0x1c, 0x00, 0x00, 0x1c, 0x1c, 0x00, 0x00, 0x00 }) catch unreachable;
    return w.buffered();
}

/// The canonical header (`writeHeader`'s) of a filter whose header plus bitset is `total_len` bytes long, or null
/// when none fits. A reader that has verified a file's writer emits this shape can locate any block of any of its
/// filters from `bloom_filter_length` alone, without reading the header.
pub fn canonicalHeader(total_len: u64, buf: *[max_header_bytes]u8) ?[]const u8 {
    var vlen: u64 = 1;
    while (vlen <= 5) : (vlen += 1) {
        if (total_len < 14 + vlen) return null;
        const nb = total_len - 14 - vlen;
        if (nb == 0 or nb % block_bytes != 0 or nb > max_bitset_bytes) continue;
        const hdr = writeHeader(@intCast(nb), buf);
        if (hdr.len == 14 + vlen) return hdr;
    }
    return null;
}

pub fn hashBytes(b: []const u8) u64 {
    return std.hash.XxHash64.hash(0, b);
}

/// fastrange: the block an XXH64 hash selects among `num_blocks`.
pub fn blockIndex(num_blocks: u64, h: u64) u64 {
    return ((h >> 32) * num_blocks) >> 32;
}

/// One bit per word of a block, from the low 32 bits of a hash. It depends on nothing else, so a probe of one value
/// against many filters computes it once.
pub const Mask = @Vector(8, u32);

const salt_v: Mask = salt;

pub fn mask(h: u64) Mask {
    const x: Mask = @splat(@as(u32, @truncate(h)));
    const shift: @Vector(8, u5) = @truncate((x *% salt_v) >> @as(Mask, @splat(27)));
    return @as(Mask, @splat(1)) << shift;
}

/// Whether every bit of `m` is set in `block`: one 32-byte load and a vector AND/compare.
pub fn blockHas(block: *const [block_bytes]u8, m: Mask) bool {
    var w: Mask = @bitCast(block.*);
    if (comptime builtin.cpu.arch.endian() == .big) w = @byteSwap(w);
    return @reduce(.And, (w & m) == m);
}

/// A probe of one hash, prepared once and reused against any number of bitsets.
pub const Probe = struct {
    hi: u64,
    m: Mask,

    pub fn init(h: u64) Probe {
        return .{ .hi = h >> 32, .m = mask(h) };
    }

    /// The block index this probe reads in a filter of `num_blocks`.
    pub fn block(self: Probe, num_blocks: u64) u64 {
        return (self.hi * num_blocks) >> 32;
    }

    pub fn mightContain(self: Probe, bits: []const u8) bool {
        const i: usize = @intCast(self.block(bits.len / block_bytes));
        return blockHas(bits[i * block_bytes ..][0..block_bytes], self.m);
    }
};

/// The spec's block_check, word by word: the reference the vector path is tested against.
pub fn mightContainScalar(bits: []const u8, h: u64) bool {
    const i: usize = @intCast(blockIndex(bits.len / block_bytes, h));
    const blk = bits[i * block_bytes ..][0..block_bytes];
    const x: u32 = @truncate(h);
    inline for (salt, 0..) |s, k| {
        const word = std.mem.readInt(u32, blk[k * 4 ..][0..4], .little);
        const bit: u5 = @intCast((x *% s) >> 27);
        if (word & (@as(u32, 1) << bit) == 0) return false;
    }
    return true;
}

/// The bytes a filter for `ndv` distinct values needs at false-positive probability `fpp`: the spec's
/// `-8 n / ln(1 - fpp^(1/8))` bits, rounded up to a power of two (Arrow C++ rejects any other size) within one block
/// and `max_bitset_bytes`.
pub fn optimalNumBytes(ndv: u64, fpp: f64) u32 {
    std.debug.assert(fpp > 0 and fpp < 1);
    const bits = -8.0 * @as(f64, @floatFromInt(ndv)) / std.math.log1p(-std.math.pow(f64, fpp, 1.0 / 8.0));
    const bytes = bits / 8.0;
    if (bytes >= @as(f64, @floatFromInt(max_bitset_bytes))) return max_bitset_bytes;
    const at_least: u32 = @max(block_bytes, @as(u32, @intFromFloat(@ceil(bytes))));
    return std.math.ceilPowerOfTwoAssert(u32, at_least);
}

/// Builds one chunk's filter: sized generously for an upper bound on its distinct values, filled, then folded down to
/// the fewest blocks that still meet the target probability on the bits actually set. With a power-of-two block count
/// the block a hash picks among n/2 is the one it picks among n, halved, so OR-ing adjacent block pairs gives exactly
/// the filter those values would have filled at half the size.
pub const Builder = struct {
    blocks: [][8]u32,
    num_blocks: usize,

    pub fn init(gpa: std.mem.Allocator, ndv_upper_bound: u64, fpp: f64) !Builder {
        const n = optimalNumBytes(ndv_upper_bound, fpp) / block_bytes;
        const blocks = try gpa.alloc([8]u32, n);
        @memset(blocks, @splat(0));
        return .{ .blocks = blocks, .num_blocks = n };
    }

    pub fn deinit(self: *Builder, gpa: std.mem.Allocator) void {
        gpa.free(self.blocks);
        self.* = undefined;
    }

    pub fn insert(self: *Builder, h: u64) void {
        const i: usize = @intCast(blockIndex(self.num_blocks, h));
        const w: Mask = self.blocks[i];
        self.blocks[i] = w | mask(h);
    }

    /// Halve while the halved filter's estimated false-positive probability stays at or below `fpp`.
    pub fn foldToFpp(self: *Builder, fpp: f64) void {
        while (self.num_blocks > 1 and self.foldedFpp() <= fpp) self.fold();
    }

    /// The mean over the halved filter's blocks of the chance all eight probe bits of an absent value are set: the
    /// product of each word's fraction of set bits.
    fn foldedFpp(self: *const Builder) f64 {
        var sum: f64 = 0;
        var b: usize = 0;
        while (b < self.num_blocks) : (b += 2) {
            const l: Mask = self.blocks[b];
            const r: Mask = self.blocks[b + 1];
            const counts: [8]u32 = @popCount(l | r);
            var p: f64 = 1;
            for (counts) |c| p *= @as(f64, @floatFromInt(c)) / 32.0;
            sum += p;
        }
        return sum / @as(f64, @floatFromInt(self.num_blocks / 2));
    }

    /// Block j becomes blocks 2j | 2j+1. Ascending order is safe: block j is read, as half of j/2, before written.
    fn fold(self: *Builder) void {
        const half = self.num_blocks / 2;
        for (0..half) |j| {
            const l: Mask = self.blocks[2 * j];
            const r: Mask = self.blocks[2 * j + 1];
            self.blocks[j] = l | r;
        }
        self.num_blocks = half;
    }

    pub fn numBytes(self: *const Builder) u32 {
        return @intCast(self.num_blocks * block_bytes);
    }

    /// Header and bitset as the file stores them, in one `gpa` allocation.
    pub fn serialize(self: *const Builder, gpa: std.mem.Allocator) ![]u8 {
        var hbuf: [max_header_bytes]u8 = undefined;
        const hdr = writeHeader(self.numBytes(), &hbuf);
        const out = try gpa.alloc(u8, hdr.len + self.numBytes());
        @memcpy(out[0..hdr.len], hdr);
        for (self.blocks[0..self.num_blocks], 0..) |blk, i| {
            const at = out[hdr.len + i * block_bytes ..][0..block_bytes];
            for (blk, 0..) |word, k| std.mem.writeInt(u32, at[k * 4 ..][0..4], word, .little);
        }
        return out;
    }
};

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

fn hashLe(comptime T: type, v: T) u64 {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buf, v, .little);
    return hashBytes(&buf);
}

test "xxhash64 seed 0 matches the reference implementation" {
    // From python-xxhash 4.0.1 (libxxhash), independent of zig std.
    const cases = [_]struct { in: []const u8, want: u64 }{
        .{ .in = "", .want = 0xef46db3751d8e999 },
        .{ .in = "a", .want = 0xd24ec4f1a98c6e5b },
        .{ .in = "abc", .want = 0x44bc2cf5ad770999 },
        .{ .in = "hello", .want = 0x26c7827d889f6da3 },
        .{ .in = "parquet", .want = 0x3c9d29275c52e429 },
        .{ .in = "0123456789abcdef0123456789abcdef!", .want = 0x8afff4daac4e677e },
    };
    for (cases) |c| try testing.expectEqual(c.want, hashBytes(c.in));
    try testing.expectEqual(@as(u64, 0xd756d7b62fc50bf1), hashLe(i32, 42));
    try testing.expectEqual(@as(u64, 0x85d136adb773c6c9), hashLe(i64, -1));
}

test "vector mask and probe agree with the spec's scalar block_check" {
    var prng = std.Random.DefaultPrng.init(0x5bbf);
    const r = prng.random();
    // Random bitsets of several densities and sizes, random hashes: the two checks must never disagree.
    for ([_]usize{ 1, 2, 7, 64, 1000 }) |z| {
        const bits = try testing.allocator.alloc(u8, z * block_bytes);
        defer testing.allocator.free(bits);
        for ([_]u8{ 0x00, 0x11, 0x77, 0xfe, 0xff }) |density| {
            for (bits) |*b| b.* = r.int(u8) | density;
            for (0..2000) |_| {
                const h = r.int(u64);
                try testing.expectEqual(mightContainScalar(bits, h), Probe.init(h).mightContain(bits));
            }
        }
    }
    // The mask sets exactly one bit per word, where the spec says.
    const h: u64 = 0x0123456789abcdef;
    const m: [8]u32 = mask(h);
    for (m, salt) |word, s| {
        try testing.expectEqual(@as(u32, 1), @popCount(word));
        try testing.expectEqual(@as(u32, 1) << @intCast((@as(u32, @truncate(h)) *% s) >> 27), word);
    }
}

test "parquet-testing bloom_filter.xxhash.bin: the inserted strings are present, others nearly all absent" {
    const metadata = @import("metadata.zig");
    const path = "data/parquet-testing/data/bloom_filter.xxhash.bin";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |e| {
        if (e == error.FileNotFound) return error.SkipZigTest;
        return e;
    };
    defer testing.allocator.free(bytes);
    const bits = bitset(bytes) orelse return error.TestUnexpectedResult;
    for ([_][]const u8{ "hello", "parquet", "bloom", "filter" }) |s| {
        try testing.expect(Probe.init(hashBytes(s)).mightContain(bits));
        try testing.expect(mightContainScalar(bits, hashBytes(s)));
    }
    var misses: usize = 0;
    var buf: [16]u8 = undefined;
    for (0..1000) |i| {
        const h = hashBytes(std.fmt.bufPrint(&buf, "absent-{d}", .{i}) catch unreachable);
        try testing.expectEqual(mightContainScalar(bits, h), Probe.init(h).mightContain(bits));
        if (!Probe.init(h).mightContain(bits)) misses += 1;
    }
    try testing.expect(misses > 990);
    // The deprecated murmur3 file has another header and must read as "no filter", not as an error.
    const old = metadata.readFileSlice("data/parquet-testing/data/bloom_filter.bin", testing.allocator) catch return;
    defer testing.allocator.free(old);
    try testing.expect(bitset(old) == null);
}

test "parseHeader takes only BLOCK / XXHASH / UNCOMPRESSED and sane sizes" {
    var buf: [max_header_bytes]u8 = undefined;
    const good = writeHeader(32, &buf);
    const h = parseHeader(good) orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u32, 32), h.num_bytes);
    try testing.expectEqual(good.len, h.len);
    var copy: [max_header_bytes]u8 = undefined;
    // Another hash (union member 2), another algorithm, another compression.
    for ([_]usize{ 3, 7, 11 }) |at| {
        @memcpy(copy[0..good.len], good);
        copy[at] = 0x2c;
        try testing.expect(parseHeader(copy[0..good.len]) == null);
    }
    // numBytes 31: not a whole block. Negative, zero, and past the cap.
    @memcpy(copy[0..good.len], good);
    copy[1] = 0x3e;
    try testing.expect(parseHeader(copy[0..good.len]) == null);
    for ([_]i64{ -32, 0, @as(i64, max_bitset_bytes) + 32 }) |n| {
        var w: std.Io.Writer = .fixed(&copy);
        try w.writeByte(0x15);
        try w.writeUleb128(@as(u64, @bitCast((n << 1) ^ (n >> 63))));
        try w.writeAll(good[2..]);
        try testing.expect(parseHeader(w.buffered()) == null);
    }
    // Truncated header and truncated bitset.
    try testing.expect(parseHeader(good[0..5]) == null);
    try testing.expect(bitset(good) == null);
}

test "canonicalHeader inverts writeHeader across varint widths" {
    var a: [max_header_bytes]u8 = undefined;
    var b: [max_header_bytes]u8 = undefined;
    for ([_]u32{ 32, 64, 1024, 1 << 13, 1 << 20, 1 << 27 }) |nb| {
        const hdr = writeHeader(nb, &a);
        const back = canonicalHeader(hdr.len + nb, &b) orelse return error.TestUnexpectedResult;
        try testing.expectEqualSlices(u8, hdr, back);
        try testing.expectEqual(nb, parseHeader(back).?.num_bytes);
    }
    try testing.expect(canonicalHeader(10, &b) == null);
}

test "optimalNumBytes follows the spec formula, power-of-two and clamped" {
    try testing.expectEqual(@as(u32, 32), optimalNumBytes(0, 0.01));
    try testing.expectEqual(@as(u32, 32), optimalNumBytes(1, 0.01));
    // 1% at 8 bits per word: about 9.6 bits a value, so 1000 values want ~1.2 KB -> 2 KiB.
    try testing.expectEqual(@as(u32, 2048), optimalNumBytes(1000, 0.01));
    try testing.expectEqual(max_bitset_bytes, optimalNumBytes(1 << 40, 0.01));
}

test "folding is exact: a folded filter equals one built at the smaller size" {
    var prng = std.Random.DefaultPrng.init(11);
    const r = prng.random();
    var hs: [3000]u64 = undefined;
    for (&hs) |*h| h.* = r.int(u64);

    var big = try Builder.init(testing.allocator, 1 << 20, 0.01);
    defer big.deinit(testing.allocator);
    for (hs) |h| big.insert(h);
    big.foldToFpp(0.01);

    var direct = Builder{ .blocks = try testing.allocator.alloc([8]u32, big.num_blocks), .num_blocks = big.num_blocks };
    defer direct.deinit(testing.allocator);
    @memset(direct.blocks, @splat(0));
    for (hs) |h| direct.insert(h);
    for (big.blocks[0..big.num_blocks], direct.blocks) |x, y| try testing.expectEqual(x, y);

    // Every inserted value is still present, the probability the fold aimed for holds on fresh values, and the size
    // is within one fold of what the formula gives for the true distinct count.
    const bytes = try big.serialize(testing.allocator);
    defer testing.allocator.free(bytes);
    const bits = bitset(bytes) orelse return error.TestUnexpectedResult;
    for (hs) |h| try testing.expect(Probe.init(h).mightContain(bits));
    var fp: usize = 0;
    for (0..100_000) |_| fp += @intFromBool(Probe.init(r.int(u64)).mightContain(bits));
    try testing.expect(fp < 1500);
    const want = optimalNumBytes(hs.len, 0.01);
    try testing.expect(big.numBytes() >= want / 2 and big.numBytes() <= want * 2);
}

test "builder output is a filter other readers' layout: canonical header, little-endian words" {
    var b = try Builder.init(testing.allocator, 4, 0.01);
    defer b.deinit(testing.allocator);
    for ([_][]const u8{ "hello", "parquet", "bloom", "filter" }) |s| b.insert(hashBytes(s));
    const bytes = try b.serialize(testing.allocator);
    defer testing.allocator.free(bytes);
    var buf: [max_header_bytes]u8 = undefined;
    const hdr = canonicalHeader(bytes.len, &buf) orelse return error.TestUnexpectedResult;
    try testing.expectEqualSlices(u8, hdr, bytes[0..hdr.len]);
    const bits = bitset(bytes).?;
    for ([_][]const u8{ "hello", "parquet", "bloom", "filter" }) |s| {
        try testing.expect(mightContainScalar(bits, hashBytes(s)));
    }
}
