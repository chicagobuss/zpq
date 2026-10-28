const std = @import("std");
const count_certificate = @import("../../count_certificate.zig");

pub fn splitMix64(x: u64) u64 {
    var z = x +% 0x9E3779B97F4A7C15;
    z = (z ^ (z >> 30)) *% 0xBF58476D1CE4E5B9;
    z = (z ^ (z >> 27)) *% 0x94D049BB133111EB;
    return z ^ (z >> 31);
}

/// The certified top-k scan's view of a table (`GroupTable.sketch`): each key falls in one cell by its hash, the same
/// cell in every table. In the count pass no key gets a group and each row adds one to its cell, so after summing every
/// table's cells a key's total is at most its cell's; saturating, so a sum stays an upper bound. In an admit pass
/// only keys in `admit` cells get groups, and every other row is dropped.
pub const CountSketch = struct {
    cells: []u32,
    hot: ?*count_certificate.Hot = null,
    /// `cells.len == 1 << (64 - shift)`; a key's cell is the top bits of its hash (`sketchHash`, or a fixed table's).
    shift: u6,
    /// The admit pass's cells, one bit each; null in the count pass.
    admit: ?[]const u64 = null,
    /// Set when the cells could not separate the keys: the census decides instead, and `cells` and `admit` are unused.
    census: ?*Census = null,
    /// Hash byte keys whole rather than sampled (`sketchHash`): a count(DISTINCT) census's bound is only as tight as
    /// its fingerprints are distinct, and long keys that differ between the samples would share one.
    whole_keys: bool = false,

    /// The hash a byte key is routed by.
    pub fn keyHash(self: *const CountSketch, key: []const u8) u64 {
        return if (self.whole_keys) std.hash.Wyhash.hash(0, key) else sketchHash(key);
    }

    /// A count pass: rows only add to the cells (or the census), and no aggregate needs its inputs.
    pub fn counting(self: *const CountSketch) bool {
        if (self.admit != null) return false;
        return if (self.census) |c| c.admitted == null else true;
    }

    /// The sink id a key with `hash`, asked for at `pos`, takes instead of a group; null when it gets a group.
    pub inline fn route(self: *CountSketch, hash: u64, pos: u64, key: []const u8) !?u32 {
        if (self.hot) |hot| if (hot.plan.slot(key)) |id| {
            hot.first[id] = @min(hot.first[id], pos);
            return sinkId(@intCast(self.cells.len + id));
        };
        if (self.census) |c| if (!c.off) {
            if (c.admitted == null) return try c.slot(hash);
            return if (c.admits(hash, pos)) null else sinkId(0);
        };
        const cell = self.cellOf(hash);
        return if (self.admits(cell)) null else sinkId(cell);
    }

    pub fn cellOf(self: *const CountSketch, hash: u64) u32 {
        return @intCast(hash >> self.shift);
    }

    /// The id a row of `cell` takes instead of a group, counted down from the top of the id space so it never meets
    /// a group an admit pass makes later.
    pub fn sinkId(cell: u32) u32 {
        return std.math.maxInt(u32) - cell;
    }

    pub fn admits(self: *const CountSketch, cell: u32) bool {
        const a = self.admit orelse return false;
        return (a[cell / 64] >> @intCast(cell % 64)) & 1 != 0;
    }
};

/// The count pass a sketched top-k falls back to when its cells are too even to bound anything (near-unique keys).
/// Each row records 32 identity bits; its owner partition supplies another 8. Colliding keys stay in one class,
/// so a class's complete count bounds every key under it, as a cell's
/// does, but almost exactly. In the count pass a key takes a slot id (`slot`) naming its hash in `batch`, and
/// `sinkSketchRows` records a fingerprint per row and adds it to its cell too, so one count pass serves both. In the
/// admit pass a key gets a group when its fingerprint is in `admitted`, or when it is asked for in the prefix: any
/// work item before `prefix_item`, or a row before `prefix_row` of that item.
pub const Census = struct {
    pub const PART_BITS = 8;
    pub const N_PARTS = 1 << PART_BITS;
    /// A 512-byte producer frontier. Owner bits live in the partition, never in each row's record.
    pub const CHUNK = 512 / @sizeOf(u32);
    pub const Chunk = [CHUNK]u32;
    /// Chunks are carved from slabs this many at a time, so recording rarely calls the allocator.
    pub const SLAB = 64;
    pub const max_items = 1 << 24;
    /// Rows a worker records before its own cells judge whether the census is needed (`dropped`).
    pub const check_rows = 1 << 18;
    /// The same for a count(DISTINCT) census (`distinct`): skew shows in far fewer rows, and every row read before an
    /// abandoned census gives up is wasted.
    pub const distinct_check_rows = 1 << 16;

    gpa: std.mem.Allocator,
    /// Rows recorded while scanning each work item, by item; shared by every worker's census.
    item_rows: []u64,
    /// The hashes of this batch's keys, by slot.
    batch: std.ArrayList(u64) = .empty,
    /// Filled chunks of entries, by partition.
    full: [N_PARTS]std.ArrayList(*Chunk) = @splat(.empty),
    /// Where every chunk lives; `slab_used` of the last are handed out.
    slabs: std.ArrayList([]Chunk) = .empty,
    slab_used: usize = 0,
    /// The chunk each partition is filling, and how much of it is filled.
    open: [N_PARTS]?*Chunk = @splat(null),
    fill: [N_PARTS]u16 = @splat(0),
    recorded: u64 = 0,
    /// Riding along a cell count pass: once `check_rows` rows in, a worker whose own cells already separate the cut
    /// (`cellsSeparate`) sets `dropped`, and every census stops (`off`) and frees what it holds, as the cells will
    /// do. Null for a census counting alone.
    dropped: ?*std.atomic.Value(bool) = null,
    /// Every worker's cells as each checks, judged once `quorum` have (`judge`): a worker's slice of clustered rows,
    /// its cells still sparse, can separate a cut the whole input's cells do not. Null judges a worker's own cells.
    merged: ?*MergedCells = null,
    /// This worker's slot in `merged`.
    worker: usize = 0,
    cut_k: usize = 0,
    admit_n: usize = 0,
    /// Ranking by count(DISTINCT): the judgment is whether the census is worth finishing, not whether the cells can
    /// stand in for it. When this worker's `admit_n` fullest cells hold over a quarter of its rows, admitting their
    /// keys reads most of the input again, so `dropped` abandons the cut and the scan runs as usual.
    distinct: bool = false,
    check_after: u64 = check_rows,
    /// Tests: the fingerprint bits kept below the partition's, fewer to make keys collide.
    fp_mask: u64 = std.math.maxInt(u32),
    checked: bool = false,
    /// A count(DISTINCT) census has judged its first keys asked for (`sampleSkew`).
    sampled: bool = false,
    off: bool = false,
    /// Set for the admit pass.
    admitted: ?*const FingerprintSet = null,
    prefix_item: u64 = 0,
    prefix_row: u64 = 0,

    pub const MergedCells = struct {
        /// A copy of each worker's cells as it checks, by worker; summed by the worker completing the quorum.
        slots: [][]u32,
        /// Workers whose cells must be in before the merged ones judge the cut; zero never judges.
        quorum: usize,
        contributed: std.atomic.Value(usize) = .init(0),
    };

    pub fn fingerprint(self: *const Census, hash: u64) u64 {
        return (hash >> 16) & (0xff_00_0000_0000 | (self.fp_mask & std.math.maxInt(u32)));
    }

    /// Frees every chunk; safe to call again.
    pub fn deinit(self: *Census) void {
        self.batch.deinit(self.gpa);
        self.batch = .empty;
        const off = self.off;
        self.stop();
        self.off = off;
    }

    /// Forgets a partition's filled chunks; their memory goes with the slabs.
    pub fn freeChunks(self: *Census, f: *std.ArrayList(*Chunk)) void {
        f.deinit(self.gpa);
        f.* = .empty;
    }

    fn newChunk(self: *Census) !*Chunk {
        if (self.slabs.items.len == 0 or self.slab_used == SLAB) {
            try self.slabs.ensureUnusedCapacity(self.gpa, 1);
            self.slabs.appendAssumeCapacity(try self.gpa.alloc(Chunk, SLAB));
            self.slab_used = 0;
        }
        defer self.slab_used += 1;
        return &self.slabs.items[self.slabs.items.len - 1][self.slab_used];
    }

    /// The fingerprints recorded in partition `p`: whole chunks, then the open one's filled part.
    pub fn partLen(self: *const Census, p: usize) usize {
        return self.full[p].items.len * CHUNK + self.fill[p];
    }

    pub fn slot(self: *Census, hash: u64) !u32 {
        const i = self.batch.items.len;
        if (self.distinct) if (self.dropped) |d| {
            if (i % 4096 == 0 and d.load(.monotonic)) return error.CensusAbandoned;
            if (!self.sampled and i == self.check_after) try self.sampleSkew();
        };
        try self.batch.append(self.gpa, hash);
        return std.math.maxInt(u32) - @as(u32, @intCast(i));
    }

    pub fn hasPrefix(self: *const Census) bool {
        return self.prefix_item != 0 or self.prefix_row != 0;
    }

    /// `pos` is a `firstPosition`: the work item above, the row in its row group below.
    pub fn admits(self: *const Census, hash: u64, pos: u64) bool {
        const item = pos >> 32;
        if (item < self.prefix_item or (item == self.prefix_item and (pos & 0xffff_ffff) < self.prefix_row)) return true;
        return self.admitted.?.contains(self.fingerprint(hash));
    }

    /// Once, `check_rows` in: drops the census for every worker if this worker's cells already separate the cut, or
    /// for a count(DISTINCT) ranking (`distinct`) show the cut would admit most of the input.
    pub fn judge(self: *Census, cells: []const u32) !void {
        self.checked = true;
        const ranked = try self.gpa.alloc(u32, self.admit_n + 1);
        defer self.gpa.free(ranked);
        fullestCells(cells, ranked);
        if (self.distinct) {
            var held: u64 = 0;
            for (ranked[0..self.admit_n]) |c| held += c;
            if (held *| 4 <= self.recorded) return;
        } else if (self.merged) |m| {
            @memcpy(m.slots[self.worker], cells);
            // The worker completing the quorum judges, on slots every contributor has finished writing.
            if (m.contributed.fetchAdd(1, .acq_rel) + 1 != m.quorum) return;
            const sum = try self.gpa.alloc(u32, cells.len);
            defer self.gpa.free(sum);
            @memset(sum, 0);
            for (m.slots) |cells_in| if (cells_in.len != 0) for (sum, cells_in) |*o, c| {
                o.* +|= c;
            };
            fullestCells(sum, ranked);
            if (!cellsSeparate(ranked, self.cut_k, self.admit_n)) return;
        } else if (!cellsSeparate(ranked, self.cut_k, self.admit_n)) return;
        self.dropped.?.store(true, .monotonic);
        self.stop();
    }

    /// The row path asks a slot of every row before any reaches the cells, a whole row group at a time: judged from the
    /// hashes asked for so far, a skewed census is abandoned before the rest are. A coded batch asks once a code, so
    /// its hashes repeat too little to abandon anything, and `judge` decides from the cells as usual.
    pub fn sampleSkew(self: *Census) !void {
        self.sampled = true;
        const hashes = try self.gpa.dupe(u64, self.batch.items);
        defer self.gpa.free(hashes);
        std.mem.sortUnstable(u64, hashes, {}, std.sort.asc(u64));
        const runs = try self.gpa.alloc(u64, hashes.len);
        defer self.gpa.free(runs);
        var n: usize = 0;
        var i: usize = 0;
        while (i < hashes.len) {
            var j = i + 1;
            while (j < hashes.len and hashes[j] == hashes[i]) j += 1;
            runs[n] = j - i;
            n += 1;
            i = j;
        }
        std.mem.sortUnstable(u64, runs[0..n], {}, std.sort.desc(u64));
        var held: u64 = 0;
        for (runs[0..@min(n, self.admit_n)]) |r| held += r;
        if (held *| 4 <= hashes.len) return;
        self.dropped.?.store(true, .monotonic);
        return error.CensusAbandoned;
    }

    /// Stops recording and frees the fingerprints; the batch's slots stay readable.
    pub fn stop(self: *Census) void {
        self.off = true;
        for (&self.full, &self.open) |*f, *o| {
            self.freeChunks(f);
            o.* = null;
        }
        for (self.slabs.items) |slab| self.gpa.free(slab);
        self.slabs.deinit(self.gpa);
        self.slabs = .empty;
        self.slab_used = 0;
    }

    pub fn record(self: *Census, hash: u64) !void {
        const fp = self.fingerprint(hash);
        const p: usize = @intCast(fp >> 40);
        if (self.open[p] == null or self.fill[p] == CHUNK) {
            if (self.open[p]) |c| {
                try self.full[p].append(self.gpa, c);
                self.open[p] = null;
            }
            self.open[p] = try self.newChunk();
            self.fill[p] = 0;
        }
        self.open[p].?[self.fill[p]] = @truncate(fp);
        self.fill[p] += 1;
        self.recorded += 1;
    }
};

/// The `out.len` fullest of `cells`, fullest first: a min-heap of them, so most cells cost one compare.
pub fn fullestCells(cells: []const u32, out: []u32) void {
    const n = out.len;
    std.debug.assert(cells.len >= n);
    @memcpy(out, cells[0..n]);
    const Heap = struct {
        fn down(h: []u32, start: usize) void {
            var i = start;
            while (true) {
                const l = 2 * i + 1;
                if (l >= h.len) return;
                const c = if (l + 1 < h.len and h[l + 1] < h[l]) l + 1 else l;
                if (h[i] <= h[c]) return;
                std.mem.swap(u32, &h[i], &h[c]);
                i = c;
            }
        }
    };
    var i = n / 2;
    while (i > 0) {
        i -= 1;
        Heap.down(out, i);
    }
    for (cells[n..]) |v| if (v > out[0]) {
        out[0] = v;
        Heap.down(out, 0);
    };
    std.mem.sortUnstable(u32, out, {}, std.sort.desc(u32));
}

/// Whether cells, sorted fullest first, can certify a cut of `k` by admitting the `admit_n` fullest: `bound`, the
/// fullest cell left out, must sit well below the k-th fullest, or the keys are too even for it to separate them.
pub fn cellsSeparate(ranked: []const u32, k: usize, admit_n: usize) bool {
    const bound = ranked[admit_n];
    return bound == 0 or ranked[k - 1] / 2 > bound;
}

/// The fingerprints a census admits. A bit per value of a fingerprint's low `filter_bits` turns most rows away with
/// one load.
pub const FingerprintSet = struct {
    pub const filter_bits = 20;
    filter: []const u64,
    map: std.AutoHashMapUnmanaged(u64, void),

    pub fn contains(self: *const FingerprintSet, fp: u64) bool {
        const b = fp & ((1 << filter_bits) - 1);
        if ((self.filter[b / 64] >> @intCast(b % 64)) & 1 == 0) return false;
        return self.map.contains(fp);
    }
};

/// A sketched string table's cell hash: the length and four 8-byte loads (the first after a lone string key's 5-byte
/// frame, the middle, the last two), not a pass over a long key. Keys that differ only elsewhere share a cell, which
/// loosens the bound and never breaks it.
pub fn sketchHash(key: []const u8) u64 {
    if (key.len < 21) return std.hash_map.hashString(key);
    // A short composite key is hashed whole: sampling it lets keys that differ only between the samples (a short
    // string beside a number) share cells and fingerprints, which loosens every bound built on them. A lone string
    // keeps the samples, as its unframed form (`rawSketchHash`) does: whole, a URL key's count pass cost a fifth more.
    if (key.len <= full_hash_max and !loneString(key)) return std.hash.Wyhash.hash(0, key);
    const l = key.len;
    return sketchMix(l, load64(key, 5), load64(key, l / 2 - 4), load64(key, l - 16), load64(key, l - 8));
}

/// Composite serialized keys up to this many bytes are hashed whole (`sketchHash`); longer ones are sampled.
const full_hash_max = 64;

/// Whether a serialized key is one present string's frame (`[1][u32 len]` then the bytes) and nothing more.
fn loneString(key: []const u8) bool {
    return key[0] == 1 and std.mem.readInt(u32, key[1..5], .little) == key.len - 5;
}

/// `sketchHash` of a lone present string key `s` (frame `[1][u32 len]` then `s`), at least 16 bytes, unframed.
pub fn rawSketchHash(s: []const u8) u64 {
    const l = s.len + 5;
    return sketchMix(l, load64(s, 0), load64(s, l / 2 - 9), load64(s, s.len - 16), load64(s, s.len - 8));
}

/// The whole-key hash (`CountSketch.whole_keys`) of a lone present string key `s`, given unframed.
pub fn wholeRawHash(s: []const u8) u64 {
    var frame: [5]u8 = undefined;
    frame[0] = 1;
    std.mem.writeInt(u32, frame[1..5], @intCast(s.len), .little);
    var h = std.hash.Wyhash.init(0);
    h.update(&frame);
    h.update(s);
    return h.final();
}

fn sketchMix(len: usize, a: u64, b: u64, c: u64, d: u64) u64 {
    const x = (a *% 0x9E3779B97F4A7C15) ^ (b *% 0xC2B2AE3D27D4EB4F) ^ (c *% 0x165667B19E3779F9) ^
        (d *% 0xD6E8FEB86659FD93) ^ @as(u64, len);
    return splitMix64(x);
}

fn load64(b: []const u8, at: usize) u64 {
    return std.mem.readInt(u64, b[at..][0..8], .little);
}

test "fullestCells picks the fullest cells, fullest first" {
    var prng = std.Random.DefaultPrng.init(7);
    const cells = try std.testing.allocator.alloc(u32, 5000);
    defer std.testing.allocator.free(cells);
    for (cells) |*c| c.* = prng.random().uintLessThan(u32, 300);
    const sorted = try std.testing.allocator.dupe(u32, cells);
    defer std.testing.allocator.free(sorted);
    std.mem.sortUnstable(u32, sorted, {}, std.sort.desc(u32));
    for ([_]usize{ 1, 2, 17, 161, 5000 }) |n| {
        var out: [5000]u32 = undefined;
        fullestCells(cells, out[0..n]);
        try std.testing.expectEqualSlices(u32, sorted[0..n], out[0..n]);
    }
}

test "short composite keys hash whole: keys differing only between the samples get distinct fingerprints" {
    // (i64, string) composites whose strings differ only in a byte the sampled hash never reads.
    var a: [9 + 5 + 20]u8 = undefined;
    a[0] = 1;
    std.mem.writeInt(u64, a[1..9], 42, .little);
    a[9] = 1;
    std.mem.writeInt(u32, a[10..14], 20, .little);
    @memset(a[14..], 'x');
    var b = a;
    b[14 + 3] = 'y';
    try std.testing.expect(!loneString(&a));
    try std.testing.expect(sketchHash(&a) != sketchHash(&b));
    // A lone string keeps the sampled hash its unframed form computes.
    var lone: [5 + 30]u8 = undefined;
    lone[0] = 1;
    std.mem.writeInt(u32, lone[1..5], 30, .little);
    for (lone[5..], 0..) |*c, i| c.* = @intCast('a' + i % 26);
    try std.testing.expect(loneString(&lone));
    try std.testing.expectEqual(rawSketchHash(lone[5..]), sketchHash(&lone));
    try std.testing.expectEqual(std.hash.Wyhash.hash(0, &lone), wholeRawHash(lone[5..]));
}

