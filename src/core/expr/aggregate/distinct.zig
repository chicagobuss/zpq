const std = @import("std");
const keyEql = @import("../../bytes.zig").keyEql;

pub const NUM_PARTS: usize = 64;
pub const NUMERIC_PARTS: usize = 16;

/// A value's partition, from the middle of its hash: each partition's map takes its fingerprint from the top bits and
/// its slot from the low ones, and fixing either inside a partition would make its probes collide more.
pub inline fn partOf(h: u64, parts: usize) usize {
    return @intCast((h >> 32) & (parts - 1));
}

pub const INLINE_CAP: usize = 2;
pub const InlineU64 = struct { len: u8, vals: [INLINE_CAP]u64 };
pub const InlineI128 = struct { len: u8, vals: [INLINE_CAP]i128 };
pub const InlineBytes = struct { len: u8, vals: [INLINE_CAP][]const u8 };

/// Hashes travel with owned string keys, so partition routing, table growth and merge do not rescan bytes.
pub const DistinctBytes = struct {
    const Key = struct { bytes: []const u8, hash: u64 };
    const Context = struct {
        pub fn hash(_: @This(), key: Key) u64 {
            return key.hash;
        }
        pub fn eql(_: @This(), a: Key, b: Key) bool {
            return a.hash == b.hash and keyEql(a.bytes, b.bytes);
        }
    };
    const Map = std.HashMapUnmanaged(Key, void, Context, std.hash_map.default_max_load_percentage);
    map: Map = .empty,
    pub const empty: DistinctBytes = .{};
    pub fn count(s: DistinctBytes) u32 {
        return s.map.count();
    }
    pub fn deinit(s: *DistinctBytes, a: std.mem.Allocator) void {
        s.map.deinit(a);
    }
    pub fn ensureTotalCapacity(s: *DistinctBytes, a: std.mem.Allocator, n: u32) !void {
        try s.map.ensureTotalCapacity(a, n);
    }
    pub fn ensureUnusedCapacity(s: *DistinctBytes, a: std.mem.Allocator, n: u32) !void {
        try s.map.ensureUnusedCapacity(a, n);
    }
    pub fn putAssumeCapacity(s: *DistinctBytes, bytes: []const u8, _: void) void {
        s.putHashed(bytes, std.hash.Wyhash.hash(0, bytes));
    }
    fn putHashed(s: *DistinctBytes, bytes: []const u8, hash: u64) void {
        s.map.putAssumeCapacity(.{ .bytes = bytes, .hash = hash }, {});
    }
    const Result = struct { key_ptr: *[]const u8, found_existing: bool };
    fn getHashedAssumeCapacity(s: *DistinctBytes, bytes: []const u8, hash: u64) Result {
        const got = s.map.getOrPutAssumeCapacity(.{ .bytes = bytes, .hash = hash });
        return .{ .key_ptr = &got.key_ptr.bytes, .found_existing = got.found_existing };
    }
    fn removeHashed(s: *DistinctBytes, bytes: []const u8, hash: u64) void {
        _ = s.map.remove(.{ .bytes = bytes, .hash = hash });
    }
    const KeyIterator = struct {
        inner: Map.KeyIterator,
        pub fn next(it: *@This()) ?*[]const u8 {
            const key = it.inner.next() orelse return null;
            return &key.bytes;
        }
    };
    pub fn keyIterator(s: *const DistinctBytes) KeyIterator {
        return .{ .inner = s.map.keyIterator() };
    }
};
