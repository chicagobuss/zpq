//! In-process metadata cache for S3-like fetches.
//!
//! Holds a bounded LRU of `(bucket, key) → entry`, where each entry
//! captures enough information to reconstruct the sparse file_buf
//! that `engine.doFetchMeta` produces — total file size, the trailing
//! footer bytes, and the offset where they belong in the file. The
//! cache also carries the object's ETag so callers can issue
//! `If-None-Match` conditional GETs on hit.
//!
//! Lifetime model: the cache is owned by the warm Lambda container
//! (lives on the PersistentPool), and entries live until evicted by
//! LRU. Capacity is by entry count; payload is small (parquet footers
//! are ~hundreds of bytes typically, low-thousands at the high end).
//!
//! Concurrency: a single mutex guards the LRU. Cache hot path is one
//! lookup + one move-to-MRU per metadata fetch, which is negligible
//! next to network round trips.
//!
//! This module is sans-IO. It does not perform any HTTP calls or
//! validation — the caller is responsible for issuing the conditional
//! GET, deciding what to do with a 304 vs 200 response, and updating
//! the cache accordingly.

const std = @import("std");
const Io = std.Io;

pub const Stats = struct {
    hits: u64 = 0,
    misses: u64 = 0,
    revalidations: u64 = 0,
    invalidations: u64 = 0,
    inserts: u64 = 0,
    evictions: u64 = 0,
};

pub const Entry = struct {
    /// Owned bytes — duped at insert time.
    bucket: []u8,
    key: []u8,
    etag: []u8,
    /// File size when the entry was captured. ETag is the source of
    /// truth for "is this still valid"; total_size is just rebuilt
    /// for the consumer's sparse buffer allocation.
    total_size: u64,
    /// Where `footer_bytes` sit in the source file: footer trailer
    /// (PAR1 + 4-byte length) lives at `[total_size - 8, total_size)`
    /// and the encoded thrift starts at `footer_offset`.
    footer_offset: u64,
    /// Owned. The contiguous bytes covering
    /// `[footer_offset, total_size)` — i.e. the thrift footer plus
    /// the trailing 4-byte length and PAR1 magic.
    footer_bytes: []u8,
    /// Owned. Byte ranges of this version of the object kept for reuse (whole Bloom filters), keyed by offset. The
    /// ETag that revalidates the footer vouches for them too.
    blobs: std.ArrayListUnmanaged(Blob) = .empty,
    /// LRU ordering — appended to the tail on insert/touch.
    lru_node: std.DoublyLinkedList.Node = .{},
};

pub const Blob = struct {
    offset: u64,
    bytes: []u8,
};

pub const MetaCache = struct {
    allocator: std.mem.Allocator,
    mutex: Io.Mutex = .init,
    entries: std.DoublyLinkedList = .{},
    /// Hot-path lookup. Keys are `<bucket>\0<key>` joined; values are
    /// `*Entry` — pointers stay stable across LRU moves because each
    /// entry is heap-allocated.
    by_key: std.StringHashMapUnmanaged(*Entry) = .empty,
    len: usize = 0,
    capacity: usize,
    stats: Stats = .{},
    /// Bytes all entries' blobs may hold together. Footers are not counted: they are tiny and bounded by `capacity`.
    blob_budget: usize = default_blob_budget,
    blob_bytes: usize = 0,

    pub const default_blob_budget: usize = 64 << 20;

    pub fn init(self: *MetaCache, allocator: std.mem.Allocator, capacity: usize) void {
        self.* = .{
            .allocator = allocator,
            .mutex = .init,
            .entries = .{},
            .by_key = .empty,
            .len = 0,
            .capacity = capacity,
            .stats = .{},
        };
    }

    pub fn deinit(self: *MetaCache) void {
        // Free hashmap keys (each is an owned `<bucket>\0<key>` joined
        // allocation) and the entry payloads. Iterate the hashmap for
        // keys, the LRU list for entries — they're parallel views over
        // the same set.
        var it = self.by_key.iterator();
        while (it.next()) |kv| self.allocator.free(kv.key_ptr.*);
        self.by_key.deinit(self.allocator);

        var node = self.entries.first;
        while (node) |n| {
            const e: *Entry = @alignCast(@fieldParentPtr("lru_node", n));
            node = n.next;
            self.destroyEntry(e);
        }
        self.entries = .{};
        self.len = 0;
    }

    fn destroyEntry(self: *MetaCache, e: *Entry) void {
        self.dropBlobs(e);
        self.allocator.free(e.bucket);
        self.allocator.free(e.key);
        self.allocator.free(e.etag);
        self.allocator.free(e.footer_bytes);
        self.allocator.destroy(e);
    }

    /// Build the lookup key. Returns a slice into `out_buf` valid
    /// for the duration of the call only — the hashmap key is stored
    /// alongside the entry's owned `bucket` + `key` (joined into a
    /// fresh allocation at insert time).
    fn cacheKey(out_buf: []u8, bucket: []const u8, key: []const u8) ?[]const u8 {
        const need = bucket.len + 1 + key.len;
        if (need > out_buf.len) return null;
        @memcpy(out_buf[0..bucket.len], bucket);
        out_buf[bucket.len] = 0;
        @memcpy(out_buf[bucket.len + 1 ..][0..key.len], key);
        return out_buf[0..need];
    }

    /// Look up an entry. Returns a pointer to the live entry or null.
    /// Callers should NOT mutate the returned entry; on a hit, the
    /// cache will update the LRU position via `touch`.
    pub fn get(self: *MetaCache, io: Io, bucket: []const u8, key: []const u8) ?*const Entry {
        var key_buf: [4096]u8 = undefined;
        const ck = cacheKey(&key_buf, bucket, key) orelse {
            self.mutex.lockUncancelable(io);
            self.stats.misses += 1;
            self.mutex.unlock(io);
            return null;
        };

        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        const entry_ptr = self.by_key.get(ck) orelse {
            self.stats.misses += 1;
            return null;
        };

        // Touch — move to tail (MRU).
        self.entries.remove(&entry_ptr.lru_node);
        self.entries.append(&entry_ptr.lru_node);
        self.stats.hits += 1;
        return entry_ptr;
    }

    /// Note that a previously-cached entry was just successfully
    /// revalidated by a 304 response. Updates stats only — the entry
    /// data is unchanged.
    pub fn noteRevalidated(self: *MetaCache, io: Io) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.stats.revalidations += 1;
    }

    /// Insert (or replace) an entry. The slices passed in are duped;
    /// the cache owns the resulting allocations.
    ///
    /// On capacity overflow the oldest entry is evicted.
    pub fn put(
        self: *MetaCache,
        io: Io,
        bucket: []const u8,
        key: []const u8,
        etag: []const u8,
        total_size: u64,
        footer_offset: u64,
        footer_bytes: []const u8,
    ) !void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        // Build the joined key for the hashmap. Owned allocation.
        const joined_key = try std.mem.concat(self.allocator, u8, &.{ bucket, &.{0}, key });
        errdefer self.allocator.free(joined_key);

        // Replace path: evict any existing entry under the same key.
        if (self.by_key.fetchRemove(joined_key)) |kv| {
            const old: *Entry = kv.value;
            self.entries.remove(&old.lru_node);
            self.len -= 1;
            self.stats.invalidations += 1;
            // joined_key from kv was allocated previously; free it.
            self.allocator.free(kv.key);
            self.destroyEntry(old);
        }

        // Allocate fresh entry.
        const entry = try self.allocator.create(Entry);
        errdefer self.allocator.destroy(entry);

        const bucket_owned = try self.allocator.dupe(u8, bucket);
        errdefer self.allocator.free(bucket_owned);
        const key_owned = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_owned);
        const etag_owned = try self.allocator.dupe(u8, etag);
        errdefer self.allocator.free(etag_owned);
        const footer_owned = try self.allocator.dupe(u8, footer_bytes);
        errdefer self.allocator.free(footer_owned);

        entry.* = .{
            .bucket = bucket_owned,
            .key = key_owned,
            .etag = etag_owned,
            .total_size = total_size,
            .footer_offset = footer_offset,
            .footer_bytes = footer_owned,
            .lru_node = .{},
        };

        try self.by_key.put(self.allocator, joined_key, entry);
        self.entries.append(&entry.lru_node);
        self.len += 1;
        self.stats.inserts += 1;

        // Evict from the head (LRU side) if over capacity.
        while (self.len > self.capacity) {
            const first_node = self.entries.popFirst() orelse break;
            const evicted: *Entry = @alignCast(@fieldParentPtr("lru_node", first_node));
            self.len -= 1;
            self.stats.evictions += 1;

            // Build the same joined key for removal.
            const evict_joined = try std.mem.concat(self.allocator, u8, &.{ evicted.bucket, &.{0}, evicted.key });
            defer self.allocator.free(evict_joined);
            if (self.by_key.fetchRemove(evict_joined)) |kv| {
                self.allocator.free(kv.key);
            }
            self.destroyEntry(evicted);
        }
    }

    /// Drop a known-stale entry. Called when a conditional GET
    /// returned 200 (object replaced) — caller will repopulate via
    /// `put` afterwards.
    pub fn invalidate(self: *MetaCache, io: Io, bucket: []const u8, key: []const u8) void {
        var key_buf: [4096]u8 = undefined;
        const ck = cacheKey(&key_buf, bucket, key) orelse return;

        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        if (self.by_key.fetchRemove(ck)) |kv| {
            const e: *Entry = kv.value;
            self.entries.remove(&e.lru_node);
            self.len -= 1;
            self.stats.invalidations += 1;
            self.allocator.free(kv.key);
            self.destroyEntry(e);
        }
    }

    fn dropBlobs(self: *MetaCache, e: *Entry) void {
        for (e.blobs.items) |b| {
            self.blob_bytes -= b.bytes.len;
            self.allocator.free(b.bytes);
        }
        e.blobs.deinit(self.allocator);
        e.blobs = .empty;
    }

    fn entryFor(self: *MetaCache, bucket: []const u8, key: []const u8, etag: []const u8) ?*Entry {
        var key_buf: [4096]u8 = undefined;
        const ck = cacheKey(&key_buf, bucket, key) orelse return null;
        const e = self.by_key.get(ck) orelse return null;
        return if (std.mem.eql(u8, e.etag, etag)) e else null;
    }

    /// A copy, in `out`, of the `len` bytes at `offset` kept for this version (`etag`) of the object, or null.
    pub fn getBlob(
        self: *MetaCache,
        io: Io,
        bucket: []const u8,
        key: []const u8,
        etag: []const u8,
        offset: u64,
        len: usize,
        out: std.mem.Allocator,
    ) ?[]u8 {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const e = self.entryFor(bucket, key, etag) orelse return null;
        for (e.blobs.items) |b| {
            if (b.offset == offset and b.bytes.len == len) return out.dupe(u8, b.bytes) catch null;
        }
        return null;
    }

    /// Copy a cached blob into `dest` while the cache lock is held. A borrowed pointer would dangle: a later insert
    /// drops the least recently used blobs. The caller owns `dest` and frees it with the wave that charged it.
    pub fn copyBlob(
        self: *MetaCache,
        io: Io,
        bucket: []const u8,
        key: []const u8,
        etag: []const u8,
        offset: u64,
        dest: []u8,
    ) bool {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const e = self.entryFor(bucket, key, etag) orelse return false;
        for (e.blobs.items) |b| {
            if (b.offset == offset and b.bytes.len == dest.len) {
                @memcpy(dest, b.bytes);
                return true;
            }
        }
        return false;
    }

    /// Whether `copyBlob` would find this range. The check and the copy are separate: an insert between them can
    /// drop the blob, and the copy then misses.
    pub fn hasBlob(
        self: *MetaCache,
        io: Io,
        bucket: []const u8,
        key: []const u8,
        etag: []const u8,
        offset: u64,
        len: usize,
    ) bool {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const e = self.entryFor(bucket, key, etag) orelse return false;
        for (e.blobs.items) |b| {
            if (b.offset == offset and b.bytes.len == len) return true;
        }
        return false;
    }

    /// Keep `bytes`, read at `offset` from this version (`etag`) of an object whose footer is cached, for later
    /// queries. Best effort: past the budget, blobs of the least recently used objects go first, and a blob that
    /// would need more than a quarter of the budget is not kept.
    pub fn putBlob(
        self: *MetaCache,
        io: Io,
        bucket: []const u8,
        key: []const u8,
        etag: []const u8,
        offset: u64,
        bytes: []const u8,
    ) void {
        if (bytes.len > self.blob_budget / 4) return;
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const e = self.entryFor(bucket, key, etag) orelse return;
        for (e.blobs.items) |b| if (b.offset == offset) return;
        var node = self.entries.first;
        while (self.blob_bytes + bytes.len > self.blob_budget) {
            const n = node orelse return;
            node = n.next;
            const victim: *Entry = @alignCast(@fieldParentPtr("lru_node", n));
            if (victim != e) self.dropBlobs(victim);
        }
        const owned = self.allocator.dupe(u8, bytes) catch return;
        e.blobs.append(self.allocator, .{ .offset = offset, .bytes = owned }) catch {
            self.allocator.free(owned);
            return;
        };
        self.blob_bytes += owned.len;
    }

    pub fn snapshotStats(self: *MetaCache) Stats {
        // No lock — Stats is POD and slightly stale reads are fine
        // for telemetry. Atomics aren't used elsewhere in pool.zig
        // either; same shape on purpose.
        return self.stats;
    }
};

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "MetaCache miss/insert/hit cycle" {
    var cache: MetaCache = undefined;
    cache.init(testing.allocator, 4);
    defer cache.deinit();

    const io = Io.Threaded.global_single_threaded.io();

    try testing.expect(cache.get(io, "b1", "k1") == null);
    try testing.expectEqual(@as(u64, 1), cache.snapshotStats().misses);

    const footer = "FOOTER_BYTES_FOR_TEST";
    try cache.put(io, "b1", "k1", "etag-v1", 1024, 1003, footer);
    try testing.expectEqual(@as(u64, 1), cache.snapshotStats().inserts);

    const e = cache.get(io, "b1", "k1") orelse return error.TestUnexpectedNull;
    try testing.expectEqualStrings("etag-v1", e.etag);
    try testing.expectEqual(@as(u64, 1024), e.total_size);
    try testing.expectEqual(@as(u64, 1003), e.footer_offset);
    try testing.expectEqualStrings(footer, e.footer_bytes);
    try testing.expectEqual(@as(u64, 1), cache.snapshotStats().hits);
}

test "MetaCache LRU eviction at capacity" {
    var cache: MetaCache = undefined;
    cache.init(testing.allocator, 2);
    defer cache.deinit();

    const io = Io.Threaded.global_single_threaded.io();

    try cache.put(io, "b", "k1", "e1", 100, 90, "f1");
    try cache.put(io, "b", "k2", "e2", 200, 190, "f2");
    try cache.put(io, "b", "k3", "e3", 300, 290, "f3"); // evicts k1

    try testing.expect(cache.get(io, "b", "k1") == null); // evicted
    try testing.expect(cache.get(io, "b", "k2") != null);
    try testing.expect(cache.get(io, "b", "k3") != null);
    try testing.expectEqual(@as(u64, 1), cache.snapshotStats().evictions);
}

test "MetaCache invalidate drops entry" {
    var cache: MetaCache = undefined;
    cache.init(testing.allocator, 4);
    defer cache.deinit();

    const io = Io.Threaded.global_single_threaded.io();

    try cache.put(io, "b", "k", "e", 100, 90, "f");
    try testing.expect(cache.get(io, "b", "k") != null);
    cache.invalidate(io, "b", "k");
    try testing.expect(cache.get(io, "b", "k") == null);
}

test "MetaCache blobs: kept per version, dropped with their entry, bounded by the budget" {
    var cache: MetaCache = undefined;
    cache.init(testing.allocator, 4);
    defer cache.deinit();
    cache.blob_budget = 64;
    const io = Io.Threaded.global_single_threaded.io();

    // No footer cached, nothing kept.
    cache.putBlob(io, "b", "k", "e1", 10, "0123456789");
    try testing.expect(cache.getBlob(io, "b", "k", "e1", 10, 10, testing.allocator) == null);

    try cache.put(io, "b", "k", "e1", 100, 90, "f");
    cache.putBlob(io, "b", "k", "e1", 10, "0123456789");
    const got = cache.getBlob(io, "b", "k", "e1", 10, 10, testing.allocator) orelse return error.TestUnexpectedNull;
    defer testing.allocator.free(got);
    try testing.expectEqualStrings("0123456789", got);
    // Another version of the object, another offset or length: no hit.
    try testing.expect(cache.getBlob(io, "b", "k", "e2", 10, 10, testing.allocator) == null);
    try testing.expect(cache.getBlob(io, "b", "k", "e1", 11, 10, testing.allocator) == null);
    try testing.expect(cache.getBlob(io, "b", "k", "e1", 10, 9, testing.allocator) == null);
    // Too big for a quarter of the budget.
    cache.putBlob(io, "b", "k", "e1", 50, &@as([17]u8, @splat(0)));
    try testing.expectEqual(@as(usize, 10), cache.blob_bytes);

    // Past the budget, the least recently used object's blobs go first.
    try cache.put(io, "b", "k2", "x", 100, 90, "f");
    cache.putBlob(io, "b", "k2", "x", 0, &@as([16]u8, @splat(1)));
    cache.putBlob(io, "b", "k2", "x", 16, &@as([16]u8, @splat(1)));
    cache.putBlob(io, "b", "k2", "x", 32, &@as([16]u8, @splat(1)));
    cache.putBlob(io, "b", "k2", "x", 48, &@as([16]u8, @splat(1)));
    try testing.expect(cache.blob_bytes <= 64);
    try testing.expect(cache.getBlob(io, "b", "k", "e1", 10, 10, testing.allocator) == null);

    // Replacing an entry frees its blobs.
    try cache.put(io, "b", "k2", "y", 100, 90, "f");
    try testing.expectEqual(@as(usize, 0), cache.blob_bytes);
}
