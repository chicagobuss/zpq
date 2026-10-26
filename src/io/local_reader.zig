//! Explicit local reads that feed the scan workers row group by row group (Linux).
//!
//! A local input is otherwise one read-only `mmap` the decode workers page-fault in: one fault per page, taken
//! by the very threads that should be decoding. Here the planned column-chunk ranges of each row group are read
//! explicitly into an anonymous buffer laid out like the file (no offset changes, and pages never planned are never
//! allocated), in row-group order, ahead of the workers. A worker waits on a row group's gate before touching it, and
//! once every work item of a row group is done its pages go back with MADV_DONTNEED, so a scan of a file far bigger
//! than memory holds a bounded window.
//!
//! The file stays mapped read-only beside the buffer: the footer is parsed from the mapping, Bloom filters and
//! dictionaries are probed through it, and `mincore` on it says how much of the file the page cache already holds.
//!
//! Backends (`Mode`): `pread` threads, or one io_uring per reader thread. `Policy.choose` picks one; `ZPQ_IO_PROFILE`
//! overrides it. `auto` maps a planned read that is already mostly cached. A cold one that fits the reader uses
//! `O_DIRECT` io_uring, wide or narrow. Anything in between is buffered, which is also what `--cache` asks for, so
//! the next query finds the file warm. The buffered descriptor stays open beside the direct one. A file system that
//! refuses the open, or a finished read that comes back `EINVAL`, uses that descriptor. Nothing else in flight is
//! retargeted.
const std = @import("std");
const builtin = @import("builtin");
const core_memory = @import("../core/memory.zig");
const linux = std.os.linux;
const posix = std.posix;

pub const Mode = enum {
    /// The plain mapping, faulted in by the workers.
    mmap,
    /// `threads` reader threads, each a blocking pread(2) of one `block` at a time.
    pread,
    /// `threads` reader threads, each with its own io_uring keeping `qd` reads of `block` in flight.
    /// O_DIRECT only when `Config.direct` is set, and then on a descriptor this reader opened.
    uring,
};

pub const Config = struct {
    mode: Mode = .pread,
    threads: usize = 8,
    qd: u32 = 32,
    block: u64 = 1 << 20,
    /// Bytes read ahead of the workers and not yet released.
    window: u64 = 1 << 30,
    /// The query's ledger, which a row group's bytes are drawn from as it is admitted to the window and given back to
    /// when it is released: a row group waits for room there as it does in the window, and goes alone when nothing is
    /// outstanding, drawn whatever the ledger holds. Null: the window alone.
    ledger: ?*core_memory.SharedBudget = null,
    /// Test hook: the read of this unit fails as an I/O error would.
    fail_unit: ?usize = null,
    /// Test hook: the ring of reader thread `i` fails to set up when bit `i` is set, as seccomp or memlock can make it.
    fail_rings: u64 = 0,
    /// Test hook: the `O_DIRECT` sibling open fails, as tmpfs and some FUSE do.
    fail_direct_open: bool = false,
    /// io_uring reads with O_DIRECT. The reader opens that descriptor itself.
    direct: bool = false,
};

/// How a query reads local files: the mode `ZPQ_IO_PROFILE` names (`mmap`, `pread`, `uring`), or with `auto` (the
/// default) the backend this host and these files suit.
pub const Policy = struct {
    profile: Profile = .auto,
    /// `--cache` and the libzpq `"cache"` field. Read through the page cache, never `O_DIRECT`.
    cache: bool = false,
    /// `ZPQ_IO_WINDOW_MIB`; null sizes the window from the query's budget (`windowFor`).
    window: ?u64 = null,
    threads: usize = 8,

    pub const Profile = enum { auto, mmap, pread, uring };

    pub fn fromEnv(env: std.process.Environ) Policy {
        var p: Policy = .{};
        if (env.getPosix("ZPQ_IO_PROFILE")) |s| p.profile = std.meta.stringToEnum(Profile, s) orelse .auto;
        if (env.getPosix("ZPQ_IO_WINDOW_MIB")) |s| if (std.fmt.parseInt(u64, s, 10)) |m| {
            p.window = @max(1, m) << 20;
        } else |_| {};
        if (env.getPosix("ZPQ_IO_THREADS")) |s| p.threads = @max(1, std.fmt.parseInt(usize, s, 10) catch p.threads);
        // Lambda's file systems are small and warm, and its sandbox may refuse io_uring: always the mapping there.
        if (env.getPosix("AWS_LAMBDA_FUNCTION_NAME") != null and p.profile == .auto) p.profile = .mmap;
        return p;
    }

    /// The share of the files' bytes a query must read before `auto` treats it as a full scan.
    pub const wide_scan: f64 = 0.5;
    /// Below this share of the planned bytes, the read is cold.
    pub const cold_resident: f64 = 0.2;
    /// At or above this share, the planned read stays on the mapping.
    pub const warm_resident: f64 = 0.8;

    /// The mode for a query of which `resident` (0..1) of the planned bytes are in the page cache and `coverage`
    /// (0..1) is the share of the inputs those bytes are. A mostly resident planned read stays on the mapping.
    /// A cold one uses `O_DIRECT` io_uring for both a wide and a narrow read, when the kernel allows a ring and
    /// the row group fits the reader (`Choice.withinReader`). Between those, a wide scan uses buffered io_uring
    /// and a narrow one pread. `cache` is that buffered path even when the read is cold, so the page cache fills.
    pub fn choose(self: Policy, resident: f64, coverage: f64) Choice {
        switch (self.profile) {
            .mmap => return .{ .mode = .mmap, .reason = if (self.cache) .page_cache else .explicit },
            .pread => return .{ .mode = .pread, .reason = if (self.cache) .page_cache else .explicit },
            // An explicit uring that the kernel refuses stays on pread. Without `cache` it is O_DIRECT.
            .uring => return .{
                .mode = if (uringAvailable()) .uring else .pread,
                .reason = if (self.cache) .page_cache else if (uringAvailable()) .explicit else .no_uring,
            },
            .auto => {},
        }
        if (resident >= warm_resident) return .{ .mode = .mmap, .reason = .cached };
        if (builtin.os.tag != .linux) return .{ .mode = .mmap, .reason = .not_linux };
        const cold = resident < cold_resident;
        if (cold and !self.cache and uringAvailable()) return .{ .mode = .uring, .reason = .cold_direct };
        const fill = self.cache and cold;
        if (coverage >= wide_scan and uringAvailable()) {
            return .{ .mode = .uring, .reason = if (fill) .page_cache else .cold_buffered };
        }
        if (coverage >= wide_scan) return .{ .mode = .pread, .reason = if (fill) .page_cache else .no_uring };
        return .{ .mode = .pread, .reason = if (fill) .page_cache else .cold_buffered };
    }
};

/// Why `auto` (or an explicit profile) picked a mode. EXPLAIN and the profile summary report it.
pub const Reason = enum(u8) {
    explicit = 0,
    cached = 1,
    cold_buffered = 2,
    no_uring = 3,
    over_budget = 4,
    not_linux = 5,
    remote = 6,
    /// The mapping, because the reader was not in play (descriptors given back, or nothing local to weigh).
    mapping = 7,
    /// Cold planned read, `O_DIRECT` io_uring. Appended so earlier profile traces keep their reason numbers.
    cold_direct = 8,
    /// `--cache` or `"cache": true`: through the page cache, never `O_DIRECT`.
    page_cache = 9,

    pub fn phrase(self: Reason) []const u8 {
        return switch (self) {
            .explicit => "profile asked for this",
            .cached => "cached",
            .cold_buffered => "buffered",
            .no_uring => "io_uring unavailable",
            .over_budget => "row group over the memory budget",
            .not_linux => "not linux",
            .remote => "remote input",
            .mapping => "read through the mapping",
            .cold_direct => "cold, O_DIRECT where the file allows it; --cache reads through the page cache",
            .page_cache => "through the page cache (--cache)",
        };
    }
};

pub const Choice = struct {
    mode: Mode,
    reason: Reason,

    /// O_DIRECT io_uring: the explicit profile, or a cold planned read. Buffered io_uring is not this.
    pub fn directRead(self: Choice) bool {
        return self.mode == .uring and (self.reason == .explicit or self.reason == .cold_direct);
    }

    /// A row group the reader cannot hold is mapped, including an explicit direct profile.
    pub fn withinReader(self: Choice, fits_reader: bool) Choice {
        if (fits_reader) return self;
        return .{ .mode = .mmap, .reason = .over_budget };
    }
};

var uring_probe: std.atomic.Value(u8) = .init(0); // 0 unknown, 1 yes, 2 no

/// Whether an io_uring can be set up here: seccomp (containers, sandboxes) or `kernel.io_uring_disabled` can refuse it.
pub fn uringAvailable() bool {
    if (builtin.os.tag != .linux) return false;
    switch (uring_probe.load(.monotonic)) {
        1 => return true,
        2 => return false,
        else => {},
    }
    // The reader opens rings of `Config.qd` entries (32). A 2-entry probe can succeed where that ring will not,
    // and EXPLAIN would then name a backend execute does not use.
    var ring = linux.IoUring.init(32, 0) catch {
        uring_probe.store(2, .monotonic);
        return false;
    };
    ring.deinit();
    uring_probe.store(1, .monotonic);
    return true;
}

pub const Residency = struct {
    /// Estimated share of this file's planned pages the page cache holds. Zero when mincore could not tell.
    share: f64 = 0,
    /// Planned pages of this file, including a sample that failed: those count as not cached.
    pages: u64 = 0,
};

/// `pages` is the planned total, not how many mincore calls ran. A short sample still yields `share`.
fn residencyFrom(hits: u64, seen: u64, total: u64) Residency {
    if (total == 0 or seen == 0) return .{ .pages = total };
    return .{
        .share = @as(f64, @floatFromInt(hits)) / @as(f64, @floatFromInt(seen)),
        .pages = total,
    };
}

/// Running planned-page weight. `share` is 0 when nothing was planned.
pub const Weight = struct {
    pages: f64 = 0,
    hits: f64 = 0,

    pub fn add(self: *Weight, part: Residency) void {
        const n: f64 = @floatFromInt(part.pages);
        self.pages += n;
        self.hits += part.share * n;
    }

    pub fn share(self: Weight) f64 {
        return if (self.pages == 0) 0 else self.hits / self.pages;
    }
};

/// Sum of per-file estimates, each weighted by that file's planned pages. An unknown file (`share` 0, `pages` set)
/// pulls the total down instead of disappearing from it.
pub fn weightShare(parts: []const Residency) f64 {
    var weight: Weight = .{};
    for (parts) |part| weight.add(part);
    return weight.share();
}

/// Past this many pages a planned-range sample strides, and a stride is not proof the bytes are cold.
/// 4 GiB of 4 KiB pages. A query's planned ranges are usually under it and are mincore'd in full, so a warm
/// slice of a cold file still maps.
const dense_pages: u64 = 1 << 20;

/// Residency of `ranges` inside `bytes` (the mapping of one file). Page-aligned, overlapping ranges merged.
/// Every page when there are at most `dense_pages` of them; otherwise an even stride. A failed mincore counts
/// the planned pages as not cached.
pub fn sampleRanges(
    gpa: std.mem.Allocator,
    bytes: []align(std.heap.page_size_min) const u8,
    ranges: []const Range,
) Residency {
    if (builtin.os.tag != .linux or bytes.len == 0 or ranges.len == 0) return .{};
    const page = pageLen();
    const limit = std.mem.alignForward(u64, bytes.len, page);
    const aligned = gpa.alloc(Range, ranges.len) catch return .{};
    defer gpa.free(aligned);
    var n: usize = 0;
    for (ranges) |r| {
        if (r.end <= r.start) continue;
        const start = r.start / page * page;
        if (start >= bytes.len) continue;
        var end = std.mem.alignForward(u64, r.end, page);
        if (end > limit) end = limit;
        if (end <= start) continue;
        aligned[n] = .{ .start = start, .end = end };
        n += 1;
    }
    if (n == 0) return .{};
    std.mem.sort(Range, aligned[0..n], {}, struct {
        fn less(_: void, a: Range, b: Range) bool {
            return a.start < b.start;
        }
    }.less);
    var m: usize = 0;
    for (aligned[1..n]) |r| {
        if (r.start <= aligned[m].end) {
            if (r.end > aligned[m].end) aligned[m].end = r.end;
        } else {
            m += 1;
            aligned[m] = r;
        }
    }
    const merged = aligned[0 .. m + 1];
    var pages: u64 = 0;
    for (merged) |r| pages += (r.end - r.start) / page;
    if (pages == 0) return .{};
    if (pages <= dense_pages) {
        var hits: u64 = 0;
        var seen: u64 = 0;
        for (merged) |r| {
            const c = countResident(bytes, r.start, r.end) orelse return .{ .pages = pages };
            hits += c.hits;
            seen += c.pages;
        }
        return residencyFrom(hits, seen, pages);
    }
    const sparse = sampleStride(bytes, merged, pages) orelse return .{ .pages = pages };
    return residencyFrom(sparse.hits, sparse.pages, pages);
}

const PageCount = struct { hits: u64, pages: u64 };

fn countResident(bytes: []align(std.heap.page_size_min) const u8, start: u64, end: u64) ?PageCount {
    const page = pageLen();
    var vec: [128]u8 = undefined;
    var off = start;
    var hits: u64 = 0;
    var pages: u64 = 0;
    while (off < end) {
        const n_pages: u64 = @min(vec.len, (end - off) / page);
        const p: [*]u8 = @constCast(bytes.ptr + off);
        if (linux.errno(linux.mincore(p, n_pages * page, &vec)) != .SUCCESS) return null;
        for (vec[0..n_pages]) |v| {
            pages += 1;
            hits += @intFromBool(v & 1 == 1);
        }
        off += n_pages * page;
    }
    return .{ .hits = hits, .pages = pages };
}

fn sampleStride(bytes: []align(std.heap.page_size_min) const u8, merged: []const Range, pages: u64) ?PageCount {
    const page = pageLen();
    const want: u64 = 2048;
    const stride = @max(pages / want, 1);
    var index: u64 = 0;
    var hits: u64 = 0;
    var seen: u64 = 0;
    var vec: [1]u8 = undefined;
    // The same positions as walking every page and keeping `index % stride == 0`, without touching the rest.
    for (merged) |r| {
        const n_pages = (r.end - r.start) / page;
        const end_index = index + n_pages;
        var sample_at = index + (stride - index % stride) % stride;
        while (sample_at < end_index) : (sample_at += stride) {
            const off = r.start + (sample_at - index) * page;
            const p: [*]u8 = @constCast(bytes.ptr + off);
            if (linux.errno(linux.mincore(p, page, &vec)) != .SUCCESS) return null;
            seen += 1;
            hits += @intFromBool(vec[0] & 1 == 1);
        }
        index = end_index;
    }
    if (seen == 0) return null;
    return .{ .hits = hits, .pages = seen };
}

fn openSibling(fd: i32) ?i32 {
    var path_buf: [64]u8 = undefined;
    const path = std.fmt.bufPrintSentinel(&path_buf, "/proc/self/fd/{d}", .{fd}, 0) catch return null;
    const flags: linux.O = .{ .ACCMODE = .RDONLY, .CLOEXEC = true, .DIRECT = true };
    const rc = linux.open(path, flags, 0);
    if (linux.errno(rc) != .SUCCESS) return null;
    return @intCast(rc);
}

pub const Range = struct { start: u64, end: u64 };
const Probe = struct { rg: u32, r: Range };

/// The runtime base page (64 KiB on some aarch64 Linux): mincore reports, and madvise rounds lengths up to, these.
fn pageLen() u64 {
    return std.heap.pageSize();
}

/// What a scan worker calls around each work item (see `scan.Input.gate`).
pub const Gate = @import("../core/scan.zig").RowGroupGate;

pub const File = struct {
    reader: *Reader,
    fd: i32,
    size: u64,
    /// The descriptor opened without `O_DIRECT`, kept when `fd` is direct. -1 when there is only one.
    buffered_fd: i32 = -1,
    /// Opened O_DIRECT. Stays true when a later read falls back; `read_fallback` records that.
    direct: bool,
    /// A whole unit was pread through `buffered_fd`: the ring never started, a submit gave up, or the direct
    /// read returned `EINVAL`. The short tail of a direct completion does not set this. The direct descriptor
    /// stays as it is: other reads already submitted still name it.
    read_fallback: std.atomic.Value(bool) = .init(false),
    /// `fd` is the reader's to close (`Reader.openFile`), not borrowed (`Reader.borrowFile`).
    owned: bool = false,
    /// `buffered_fd` is the reader's to close. False when it is borrowed, or when there is none.
    own_buffered: bool = false,
    /// Anonymous, laid out like the file; only planned pages are ever written.
    buf: []align(std.heap.page_size_min) u8,
    /// 0 until the row group's reads are in, 1 once they are, 2 when a read failed.
    rg_ready: []std.atomic.Value(u32) = &.{},
    rg_items_done: []std.atomic.Value(u32) = &.{},
    rg_units_left: []std.atomic.Value(u32) = &.{},
    /// Per row group, `ranges[rg_first[rg]..rg_first[rg + 1]]` are its coalesced, page-aligned reads.
    rg_first: []u32 = &.{},
    ranges: std.ArrayList(Range) = .empty,
    rg_bytes: []u64 = &.{},
    /// The union of every row group's ranges cut where the set of row groups covering it changes, sorted. A segment
    /// is released when the last row group covering it is done: coalescing across a gap can take in another row
    /// group's bytes (Arrow writes every row group's page indexes together after the data), so one row group's
    /// ranges say nothing about whose pages they are.
    segs: std.ArrayList(Range) = .empty,
    seg_refs: []std.atomic.Value(u32) = &.{},
    /// Ranges planning reads before any row group (`Reader.probe`), each held by its row group: page-aligned, read by
    /// the first units of all, and never read again by a row group's own.
    probes: std.ArrayList(Probe) = .empty,
    probe_left: std.atomic.Value(u32) = .init(0),
    /// Per row group: no range of its own (pruned), so only its probes are held, for planning alone.
    rg_pruned: []bool = &.{},
    /// Per row group: its pages are given back, by `done` or `releasePruned`, whichever comes first.
    rg_released: []std.atomic.Value(bool) = &.{},
    /// As `rg_ready`, for every probe of the file at once.
    probe_ready: std.atomic.Value(u32) = .init(1),

    pub fn bytes(f: *const File) []const u8 {
        return f.buf[0..@intCast(f.size)];
    }

    pub fn gate(f: *File) Gate {
        return .{ .ctx = f, .wait = gateWait, .done = gateDone };
    }

    /// What `ndv.File.wait` calls: every probe of the file is in. The row group does not matter, as a row group's own
    /// units leave out the probed pages of every row group.
    pub fn probeWait(ctx: *anyopaque, _: usize) anyerror!void {
        const f: *File = @ptrCast(@alignCast(ctx));
        try awaitReady(f, &f.probe_ready);
    }

    fn gateWait(ctx: *anyopaque, rg: usize) anyerror!void {
        const f: *File = @ptrCast(@alignCast(ctx));
        if (rg >= f.rg_ready.len) return;
        try awaitReady(f, &f.rg_ready[rg]);
        try awaitReady(f, &f.probe_ready);
    }

    fn awaitReady(f: *File, r: *std.atomic.Value(u32)) !void {
        var v = r.load(.acquire);
        if (v == 1) return;
        const t0 = nowNs();
        while (v == 0) {
            _ = linux.futex_4arg(&r.raw, .{ .cmd = .WAIT, .private = true }, 0, null);
            v = r.load(.acquire);
        }
        _ = f.reader.stats.stall_ns.fetchAdd(nowNs() - t0, .monotonic);
        _ = f.reader.stats.stalls.fetchAdd(1, .monotonic);
        if (v != 1) return error.LocalReadFailed;
    }

    /// One of row group `rg`'s `items` work items is done; the last one hands the row group's pages back.
    fn gateDone(ctx: *anyopaque, rg: usize, items: usize) void {
        const f: *File = @ptrCast(@alignCast(ctx));
        if (rg >= f.rg_items_done.len) return;
        const prev = f.rg_items_done[rg].fetchAdd(1, .acq_rel);
        if (prev + 1 != items) return;
        // A failed row group may never have been admitted, so it has no charge to give back; its pages, read or
        // not, go with the buffer at teardown.
        if (f.rg_ready[rg].load(.acquire) != 1) return;
        f.release(rg);
    }

    /// Give row group `rg`'s pages and window charge back, once.
    fn release(f: *File, rg: usize) void {
        if (f.rg_released[rg].swap(true, .acq_rel)) return;
        const rd = f.reader;
        // Segments never straddle a range's ends, so a range is exactly the run of segments starting inside it.
        for (f.ranges.items[f.rg_first[rg]..f.rg_first[rg + 1]]) |r| {
            var s = std.sort.lowerBound(Range, f.segs.items, r.start, struct {
                fn order(start: u64, x: Range) std.math.Order {
                    return std.math.order(start, x.start);
                }
            }.order);
            while (s < f.segs.items.len and f.segs.items[s].start < r.end) : (s += 1) {
                if (f.seg_refs[s].fetchSub(1, .acq_rel) != 1) continue;
                const seg = f.segs.items[s];
                _ = linux.madvise(f.buf.ptr + seg.start, @intCast(seg.end - seg.start), linux.MADV.DONTNEED);
            }
        }
        _ = rd.released.fetchAdd(f.rg_bytes[rg], .release);
        if (rd.cfg.ledger) |l| {
            l.release(f.rg_bytes[rg]);
            _ = rd.ledger_held.fetchSub(f.rg_bytes[rg], .monotonic);
        }
        _ = rd.rel_gen.fetchAdd(1, .release);
        _ = linux.futex_3arg(&rd.rel_gen.raw, .{ .cmd = .WAKE, .private = true }, std.math.maxInt(i32));
    }
};

/// One read: `len` bytes at `off`. `charge` is the window bytes it accounts for (its row group's on the first unit),
/// and `ticket` its row group's turn to be admitted.
/// A probe unit (`File.probes`) is outside the window: planning waits on it before any row group can be released.
const Unit = struct { file: u32, rg: u32, off: u64, len: u32, charge: u64 = 0, ticket: u32 = 0, probe: bool = false };

pub const Stats = struct {
    bytes: std.atomic.Value(u64) = .init(0),
    stall_ns: std.atomic.Value(u64) = .init(0),
    stalls: std.atomic.Value(u64) = .init(0),
    window_waits: std.atomic.Value(u64) = .init(0),
};

/// What a test substitutes for `io_uring_enter`. `submit` calls the kernel with at most that many SQEs, so a
/// partial accept is a real submission. The refusals do not call the kernel: the SQ head does not move.
const Inject = union(enum) { again, busy, intr, submit: u32 };

const Faults = struct {
    inject: []const Inject = &.{},
    at: usize = 0,
    cqe_replays: u8 = 0,
    wait_intr: u8 = 0,
    early_ready: bool = false,
    fail_pread: bool = false,
    preads: u32 = 0,
    /// Wait to copy until at least this many CQEs are ready, so one batch holds several units.
    hold_batch: u32 = 0,
    /// How many CQEs the last copy took.
    batch_n: u32 = 0,
    /// The first CQE of a multi-completion batch reports a short read, so its repair pread runs.
    short_first: bool = false,
    /// The first positive completion reports this many bytes, so the repair pread starts at an unaligned offset.
    /// 0 does not inject.
    short_got: u32 = 0,
    /// Set once that repair pread runs. `repair_at` is the offset it started at.
    repaired: bool = false,
    repair_at: u64 = 0,
    /// Completions left to report as `EINVAL`, as a direct read the file system accepted the open for and then
    /// refused. The unit is pread through the buffered descriptor.
    reject_direct: u8 = 0,
    inflight_at_deinit: usize = std.math.maxInt(usize),
};

pub const Reader = struct {
    gpa: std.mem.Allocator,
    cfg: Config,
    files: std.ArrayList(*File) = .empty,
    units: std.ArrayList(Unit) = .empty,
    threads: std.ArrayList(std.Thread) = .empty,
    cancel: std.atomic.Value(bool) = .init(false),
    released: std.atomic.Value(u64) = .init(0),
    submitted: std.atomic.Value(u64) = .init(0),
    /// What the admitted row groups drew from `cfg.ledger` and have not given back.
    ledger_held: std.atomic.Value(u64) = .init(0),
    rel_gen: std.atomic.Value(u32) = .init(0),
    next_unit: std.atomic.Value(usize) = .init(0),
    /// Row groups are admitted to the window one at a time, in plan order: the ticket of the next to go, and how many
    /// were handed out.
    admit_next: std.atomic.Value(u32) = .init(0),
    tickets: u32 = 0,
    stats: Stats = .{},
    /// Reader threads by the backend they actually got: a uring thread whose ring setup failed reads with pread.
    uring_threads: u32 = 0,
    pread_threads: u32 = 0,
    laid_out: bool = false,
    /// Probe units not yet read, over every file: no row group's read is admitted before they are all in, so planning's
    /// small reads never queue at the device behind the window's large ones.
    probes_out: std.atomic.Value(u32) = .init(0),
    /// Test builds only. The reader thread and the test share it; release builds have no field.
    faults: if (builtin.is_test) ?*Faults else void = if (builtin.is_test) null else {},

    pub fn create(gpa: std.mem.Allocator, cfg: Config) !*Reader {
        std.debug.assert(cfg.mode != .mmap);
        const r = try gpa.create(Reader);
        r.* = .{ .gpa = gpa, .cfg = cfg };
        return r;
    }

    /// Open `path` for reading into a buffer of `size` bytes.
    pub fn openFile(self: *Reader, path: []const u8, size: u64) !*File {
        var path_z: [std.fs.max_path_bytes]u8 = undefined;
        if (path.len >= path_z.len) return error.NameTooLong;
        @memcpy(path_z[0..path.len], path);
        path_z[path.len] = 0;
        const rc = linux.open(@ptrCast(&path_z), .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
        if (linux.errno(rc) != .SUCCESS) return error.OpenFailed;
        const fd: i32 = @intCast(rc);
        return self.addFile(fd, size, false);
    }

    /// Read the file open on `fd` into a buffer of `size` bytes: the very file the caller opened, whatever its path
    /// names by now. Borrowed: the caller closes it, after `destroy`. A direct read opens another descriptor and
    /// leaves this one's flags alone.
    pub fn borrowFile(self: *Reader, fd: i32, size: u64) !*File {
        return self.addFile(fd, size, true);
    }

    fn addFile(self: *Reader, fd: i32, size: u64, borrowed: bool) !*File {
        // O_DIRECT is a new open of `/proc/self/fd/N`, never F_SETFL on `fd`. The buffered descriptor stays: a read
        // the direct one rejects is pread through it, and a borrowed descriptor is never closed or retargeted.
        // A file system that refuses the open (tmpfs, some FUSE) has no second descriptor and reads buffered.
        var read_fd = fd;
        var buffered_fd: i32 = -1;
        var owned = !borrowed;
        var own_buffered = false;
        var direct = false;
        errdefer {
            if (owned) _ = linux.close(read_fd);
            if (own_buffered) _ = linux.close(buffered_fd);
        }
        if (self.cfg.direct and !self.cfg.fail_direct_open) {
            if (openSibling(fd)) |dfd| {
                buffered_fd = fd;
                own_buffered = !borrowed;
                read_fd = dfd;
                owned = true;
                direct = true;
            }
        }

        const f = try self.gpa.create(File);
        errdefer self.gpa.destroy(f);
        const map_len: usize = @intCast(@max(pageLen(), std.mem.alignForward(u64, size, pageLen())));
        const buf = try posix.mmap(null, map_len, .{ .READ = true, .WRITE = true }, .{
            .TYPE = .PRIVATE,
            .ANONYMOUS = true,
            .NORESERVE = true,
        }, -1, 0);
        errdefer posix.munmap(buf);
        // THP on this buffer backed the gaps between projected chunks too: RSS 3-7x for nothing.
        _ = linux.madvise(buf.ptr, buf.len, linux.MADV.NOHUGEPAGE);
        f.* = .{
            .reader = self,
            .fd = read_fd,
            .size = size,
            .buffered_fd = buffered_fd,
            .direct = direct,
            .owned = owned,
            .own_buffered = own_buffered,
            .buf = buf,
        };
        try self.files.append(self.gpa, f);
        return f;
    }

    /// Declare a file's row groups: `ranges_per_rg[rg]` (null when pruned) must be read before the row group is
    /// scanned. They are sorted and coalesced here, and cut into reads at `start`, after any `probe`.
    pub fn planFile(self: *Reader, f: *File, ranges_per_rg: []const ?[]Range, gap: u64) !void {
        const n = ranges_per_rg.len;
        f.rg_ready = try self.gpa.alloc(std.atomic.Value(u32), n);
        f.rg_items_done = try self.gpa.alloc(std.atomic.Value(u32), n);
        f.rg_units_left = try self.gpa.alloc(std.atomic.Value(u32), n);
        f.rg_first = try self.gpa.alloc(u32, n + 1);
        f.rg_bytes = try self.gpa.alloc(u64, n);
        f.rg_pruned = try self.gpa.alloc(bool, n);
        f.rg_released = try self.gpa.alloc(std.atomic.Value(bool), n);
        for (ranges_per_rg, 0..) |opt, rg| {
            f.rg_first[rg] = @intCast(f.ranges.items.len);
            f.rg_items_done[rg] = .init(0);
            f.rg_bytes[rg] = 0;
            f.rg_released[rg] = .init(false);
            f.rg_pruned[rg] = opt == null;
            const rs = opt orelse continue;
            std.mem.sort(Range, rs, {}, rangeLess);
            try coalesce(self.gpa, &f.ranges, rs, f.size, gap);
        }
        f.rg_first[n] = @intCast(f.ranges.items.len);
    }

    /// Have `[start, end)` of row group `rg` read before any row group's data, for planning to read through
    /// `File.probeWait` (dictionary pages, `ndv.probeRanges`). The row group holds the probed pages and gives them back
    /// with its own; its own reads leave out every probed page of the file. Between `planFile` and `start`.
    pub fn probe(self: *Reader, f: *File, rg: usize, lo: u64, end: u64) !void {
        std.debug.assert(!self.laid_out and rg + 1 < f.rg_first.len);
        const hi = @min(end, f.size);
        if (hi <= lo) return;
        try f.probes.append(self.gpa, .{ .rg = @intCast(rg), .r = .{
            .start = std.mem.alignBackward(u64, lo, pageLen()),
            .end = std.mem.alignForward(u64, hi, pageLen()),
        } });
    }

    fn rangeLess(_: void, a: Range, b: Range) bool {
        return a.start < b.start;
    }

    /// Append sorted `rs`, clipped to `size`, to `out` as whole pages (O_DIRECT needs them, and they make overlaps with
    /// other row groups exact), merging ranges at most `gap` apart.
    fn coalesce(gpa: std.mem.Allocator, out: *std.ArrayList(Range), rs: []const Range, size: u64, gap: u64) !void {
        var cur: ?Range = null;
        for (rs) |r| {
            const end = @min(r.end, size);
            if (end <= r.start) continue;
            const clipped: Range = .{
                .start = std.mem.alignBackward(u64, r.start, pageLen()),
                .end = std.mem.alignForward(u64, end, pageLen()),
            };
            if (cur) |*c| {
                if (clipped.start <= c.end + gap) {
                    c.end = @max(c.end, clipped.end);
                    continue;
                }
                try out.append(gpa, c.*);
            }
            cur = clipped;
        }
        if (cur) |c| try out.append(gpa, c);
    }

    /// Append the reads of `[lo, hi)` in blocks.
    fn addUnits(self: *Reader, u: Unit, lo: u64, hi: u64) !u32 {
        var off = lo;
        var n: u32 = 0;
        while (off < hi) : (n += 1) {
            const len = @min(self.cfg.block, hi - off);
            var unit = u;
            unit.off = off;
            unit.len = @intCast(len);
            try self.units.append(self.gpa, unit);
            off += len;
        }
        return n;
    }

    /// Cut every file's ranges into reads: all probes first, then each row group's ranges less the probed pages, in
    /// plan order. A row group's ranges take in its probes, so its release gives their pages back.
    fn layOut(self: *Reader) !void {
        if (self.laid_out) return;
        self.laid_out = true;
        const files = self.files.items;
        const merged = try self.gpa.alloc(std.ArrayList(Range), files.len);
        @memset(merged, .empty);
        defer {
            for (merged) |*m| m.deinit(self.gpa);
            self.gpa.free(merged);
        }
        for (files, merged, 0..) |f, *m, fi| {
            if (f.rg_first.len == 0) continue;
            const rs = try self.gpa.alloc(Range, f.probes.items.len);
            defer self.gpa.free(rs);
            for (rs, f.probes.items) |*r, p| r.* = p.r;
            std.mem.sort(Range, rs, {}, rangeLess);
            try coalesce(self.gpa, m, rs, f.size, 0);
            var n: u32 = 0;
            for (m.items) |r| n += try self.addUnits(.{ .file = @intCast(fi), .rg = 0, .off = 0, .len = 0, .probe = true }, r.start, r.end);
            f.probe_left = .init(n);
            f.probe_ready = .init(if (n == 0) 1 else 0);
            _ = self.probes_out.fetchAdd(n, .monotonic);
        }
        for (files, merged, 0..) |f, m, fi| {
            if (f.rg_first.len == 0) continue;
            const n = f.rg_first.len - 1;
            // Each row group's ranges again, with its probes taken in.
            var held: std.ArrayList(Range) = .empty;
            errdefer held.deinit(self.gpa);
            var scratch: std.ArrayList(Range) = .empty;
            defer scratch.deinit(self.gpa);
            // In row-group order, so each row group takes its own probes off the front: linear in the probes.
            std.mem.sort(Probe, f.probes.items, {}, struct {
                fn lt(_: void, a: Probe, b: Probe) bool {
                    return a.rg < b.rg;
                }
            }.lt);
            var next_probe: usize = 0;
            for (0..n) |rg| {
                scratch.clearRetainingCapacity();
                try scratch.appendSlice(self.gpa, f.ranges.items[f.rg_first[rg]..f.rg_first[rg + 1]]);
                while (next_probe < f.probes.items.len and f.probes.items[next_probe].rg == rg) : (next_probe += 1)
                    try scratch.append(self.gpa, f.probes.items[next_probe].r);
                std.mem.sort(Range, scratch.items, {}, rangeLess);
                f.rg_first[rg] = @intCast(held.items.len);
                try coalesce(self.gpa, &held, scratch.items, f.size, 0);
                var units: u32 = 0;
                const first_unit = self.units.items.len;
                const own: Unit = .{ .file = @intCast(fi), .rg = @intCast(rg), .off = 0, .len = 0 };
                for (held.items[f.rg_first[rg]..]) |r| {
                    // The parts of `r` no probe covers; probes are sorted and disjoint.
                    var lo = r.start;
                    for (m.items) |p| {
                        if (p.end <= lo or p.start >= r.end) continue;
                        if (p.start > lo) {
                            units += try self.addUnits(own, lo, p.start);
                            f.rg_bytes[rg] += p.start - lo;
                        }
                        lo = @max(lo, p.end);
                    }
                    if (lo < r.end) {
                        units += try self.addUnits(own, lo, r.end);
                        f.rg_bytes[rg] += r.end - lo;
                    }
                }
                if (units > 0) {
                    self.units.items[first_unit].charge = f.rg_bytes[rg];
                    for (self.units.items[first_unit..]) |*u| u.ticket = self.tickets;
                    self.tickets += 1;
                }
                f.rg_units_left[rg] = .init(units);
                f.rg_ready[rg] = .init(if (units == 0) 1 else 0);
            }
            f.rg_first[n] = @intCast(held.items.len);
            f.ranges.deinit(self.gpa);
            f.ranges = held;
            held = .empty;
            try self.planSegments(f);
        }
    }

    /// Cut the union of `f`'s ranges into segments covered by one fixed set of row groups each (see `File.segs`).
    fn planSegments(self: *Reader, f: *File) !void {
        const Ev = struct { pos: u64, rg: u32, open: bool };
        var evs: std.ArrayList(Ev) = .empty;
        defer evs.deinit(self.gpa);
        for (0..f.rg_first.len - 1) |rg| for (f.ranges.items[f.rg_first[rg]..f.rg_first[rg + 1]]) |r| {
            try evs.append(self.gpa, .{ .pos = r.start, .rg = @intCast(rg), .open = true });
            try evs.append(self.gpa, .{ .pos = r.end, .rg = @intCast(rg), .open = false });
        };
        std.mem.sort(Ev, evs.items, {}, struct {
            fn lt(_: void, a: Ev, b: Ev) bool {
                return a.pos < b.pos;
            }
        }.lt);
        var refs: std.ArrayList(u32) = .empty;
        defer refs.deinit(self.gpa);
        // A row group's own ranges are disjoint and never touch, so it is open at most once at a time.
        var open: u32 = 0;
        var i: usize = 0;
        while (i < evs.items.len) {
            const pos = evs.items[i].pos;
            while (i < evs.items.len and evs.items[i].pos == pos) : (i += 1) {
                if (evs.items[i].open) open += 1 else open -= 1;
            }
            if (open > 0 and i < evs.items.len) {
                try f.segs.append(self.gpa, .{ .start = pos, .end = evs.items[i].pos });
                try refs.append(self.gpa, open);
            }
        }
        f.seg_refs = try self.gpa.alloc(std.atomic.Value(u32), refs.items.len);
        for (f.seg_refs, refs.items) |*r, n| r.* = .init(n);
    }

    pub fn start(self: *Reader) !void {
        try self.layOut();
        const n = @max(1, self.cfg.threads);
        // Reserved first: a spawned thread missing from the list would never be joined.
        try self.threads.ensureUnusedCapacity(self.gpa, n);
        var entries: u16 = 1;
        while (entries < self.cfg.qd) entries *= 2;
        for (0..n) |i| {
            // Rings are set up here rather than in each thread, so what every thread reads with is known (and
            // reported) once start returns: setup can fail per ring (seccomp, memlock) after a probe succeeded.
            const fail = i < 64 and (self.cfg.fail_rings >> @intCast(i)) & 1 == 1;
            var ring: ?linux.IoUring = if (self.cfg.mode == .uring and !fail)
                linux.IoUring.init(entries, 0) catch null
            else
                null;
            const t = std.Thread.spawn(.{ .stack_size = 1 << 20 }, threadMain, .{ self, ring }) catch |err| {
                if (ring) |*r| r.deinit();
                if (self.threads.items.len > 0) return; // fewer readers, same work
                return err;
            };
            self.threads.appendAssumeCapacity(t);
            if (ring != null) self.uring_threads += 1 else self.pread_threads += 1;
        }
    }

    /// Planning is done with the probes: a pruned row group, read for planning alone, gives its probed pages back now
    /// rather than at teardown (the scan never touches it). After the probes are in, before any worker runs.
    pub fn releasePruned(self: *Reader) void {
        for (self.files.items) |f| for (f.rg_pruned, 0..) |pruned, rg| {
            if (pruned and f.rg_ready[rg].load(.acquire) == 1 and f.probe_ready.load(.acquire) == 1) f.release(rg);
        };
    }

    /// Stops reading and gives every buffered page back, before a scan run again over the files' mappings
    /// (`scan.runMultiAggregate`): that scan is an exact restart, and must not hold this window beside its own. Only
    /// once no worker waits on a gate. `destroy` still tears down.
    pub fn abandon(self: *Reader) void {
        self.cancel.store(true, .release);
        _ = self.rel_gen.fetchAdd(1, .release);
        _ = linux.futex_3arg(&self.rel_gen.raw, .{ .cmd = .WAKE, .private = true }, std.math.maxInt(i32));
        for (self.threads.items) |t| t.join();
        self.threads.clearRetainingCapacity();
        for (self.files.items) |f| _ = linux.madvise(f.buf.ptr, f.buf.len, linux.MADV.DONTNEED);
        self.giveBackLedger();
    }

    /// The pages every admitted row group still holds are gone: so is their charge.
    fn giveBackLedger(self: *Reader) void {
        const l = self.cfg.ledger orelse return;
        l.release(self.ledger_held.swap(0, .monotonic));
    }

    /// Joins the reader threads (abandoning reads not yet made), then unmaps and closes.
    pub fn destroy(self: *Reader) void {
        self.cancel.store(true, .release);
        _ = self.rel_gen.fetchAdd(1, .release);
        _ = linux.futex_3arg(&self.rel_gen.raw, .{ .cmd = .WAKE, .private = true }, std.math.maxInt(i32));
        for (self.threads.items) |t| t.join();
        self.giveBackLedger();
        for (self.files.items) |f| {
            posix.munmap(f.buf);
            if (f.owned) _ = linux.close(f.fd);
            if (f.own_buffered) _ = linux.close(f.buffered_fd);
            self.gpa.free(f.rg_ready);
            self.gpa.free(f.rg_items_done);
            self.gpa.free(f.rg_units_left);
            self.gpa.free(f.rg_first);
            self.gpa.free(f.rg_bytes);
            self.gpa.free(f.seg_refs);
            self.gpa.free(f.rg_pruned);
            self.gpa.free(f.rg_released);
            f.ranges.deinit(self.gpa);
            f.segs.deinit(self.gpa);
            f.probes.deinit(self.gpa);
            self.gpa.destroy(f);
        }
        self.files.deinit(self.gpa);
        self.units.deinit(self.gpa);
        self.threads.deinit(self.gpa);
        self.gpa.destroy(self);
    }

    fn unitDone(self: *Reader, u: Unit) void {
        const f = self.files.items[u.file];
        // The file's bytes, not the page padding past its end; the window charge stays page-rounded.
        _ = self.stats.bytes.fetchAdd(@min(u.len, f.size -| u.off), .monotonic);
        if (u.probe) {
            if (f.probe_left.fetchSub(1, .acq_rel) == 1) {
                f.probe_ready.store(1, .release);
                _ = linux.futex_3arg(&f.probe_ready.raw, .{ .cmd = .WAKE, .private = true }, std.math.maxInt(i32));
            }
            if (self.probes_out.fetchSub(1, .acq_rel) == 1) {
                _ = self.rel_gen.fetchAdd(1, .release);
                _ = linux.futex_3arg(&self.rel_gen.raw, .{ .cmd = .WAKE, .private = true }, std.math.maxInt(i32));
            }
            return;
        }
        if (f.rg_units_left[u.rg].fetchSub(1, .acq_rel) == 1) {
            f.rg_ready[u.rg].store(1, .release);
            _ = linux.futex_3arg(&f.rg_ready[u.rg].raw, .{ .cmd = .WAKE, .private = true }, std.math.maxInt(i32));
        }
    }

    /// A read failed: every row group still waiting fails its workers, and no more reads are made.
    fn failAll(self: *Reader) void {
        self.cancel.store(true, .release);
        for (self.files.items) |f| {
            for (f.rg_ready) |*r| failReady(r);
            failReady(&f.probe_ready);
        }
    }

    fn failReady(r: *std.atomic.Value(u32)) void {
        if (r.load(.acquire) == 0) {
            r.store(2, .release);
            _ = linux.futex_3arg(&r.raw, .{ .cmd = .WAKE, .private = true }, std.math.maxInt(i32));
        }
    }

    /// Whether unit `u` may be read now, taking its row group's window charge if it is the first. First units go
    /// strictly in ticket order, so a later row group can never take the room an earlier one needs (an ordered scan
    /// waits on the earlier one first); with one admitter at a time and releases only shrinking what is outstanding,
    /// the check and the charge are one reservation. A first unit goes when the window has room, or when nothing is
    /// outstanding, so a row group bigger than the window still reads, alone. The rest of a row group's units go once
    /// it is admitted, whatever room is left: holding them back would wait on a release only that row group can give,
    /// and letting them go before would read a row group the window has no room for.
    fn admit(self: *Reader, u: Unit) bool {
        if (self.cancel.load(.acquire)) return false;
        if (u.probe) return true;
        if (self.probes_out.load(.acquire) != 0) return false;
        const next = self.admit_next.load(.acquire);
        if (u.charge == 0) return next > u.ticket;
        if (next != u.ticket) return false;
        // Only this admitter adds to `submitted`, and only admitted row groups add to `released`.
        const out = self.submitted.load(.monotonic) -| self.released.load(.acquire);
        if (out + u.charge > self.cfg.window and out != 0) return false;
        if (self.cfg.ledger) |l| {
            if (l.drawExact(u.charge) == null) {
                if (out != 0) return false;
                l.force(u.charge);
            }
            _ = self.ledger_held.fetchAdd(u.charge, .monotonic);
        }
        _ = self.submitted.fetchAdd(u.charge, .monotonic);
        self.admit_next.store(u.ticket + 1, .release);
        _ = self.rel_gen.fetchAdd(1, .release);
        _ = linux.futex_3arg(&self.rel_gen.raw, .{ .cmd = .WAKE, .private = true }, std.math.maxInt(i32));
        return true;
    }

    fn waitRelease(self: *Reader) void {
        _ = self.stats.window_waits.fetchAdd(1, .monotonic);
        const g = self.rel_gen.load(.acquire);
        const ts: linux.timespec = .{ .sec = 0, .nsec = 1_000_000 };
        _ = linux.futex_4arg(&self.rel_gen.raw, .{ .cmd = .WAIT, .private = true }, g, &ts);
    }

    fn threadMain(self: *Reader, ring: ?linux.IoUring) void {
        const ran = if (ring) |r| self.uringRun(r) else self.preadRun();
        ran catch self.failAll();
    }

    pub const Backends = struct {
        uring_threads: u32,
        pread_threads: u32,
        direct_files: u32,
        buffered_files: u32,
    };

    /// What the reads actually went through. Thread counts are known once `start` returns; a direct file counts as
    /// buffered once a whole unit was pread (`read_fallback`), which is known once that read has finished.
    pub fn backends(self: *const Reader) Backends {
        var direct: u32 = 0;
        for (self.files.items) |f| {
            const stayed = f.direct and !f.read_fallback.load(.monotonic);
            direct += @intFromBool(stayed);
        }
        return .{
            .uring_threads = self.uring_threads,
            .pread_threads = self.pread_threads,
            .direct_files = direct,
            .buffered_files = @as(u32, @intCast(self.files.items.len)) - direct,
        };
    }

    /// Read unit `i` with a blocking pread, retrying short reads; the tail of the file reads short by design.
    fn readUnit(self: *Reader, i: usize) !void {
        if (self.cfg.fail_unit == i) return error.ReadFailed;
        if (builtin.is_test) if (self.faults) |f| {
            f.preads += 1;
            if (f.fail_pread) return error.ReadFailed;
        };
        const u = self.units.items[i];
        const f = self.files.items[u.file];
        try preadAll(f, u.off, @min(u.off + u.len, f.size));
        // The whole unit went through the buffered descriptor. A direct completion's short tail calls `preadAll`
        // itself and does not land here, so that file stays direct.
        if (f.buffered_fd >= 0) f.read_fallback.store(true, .release);
    }

    fn preadRun(self: *Reader) !void {
        while (!self.cancel.load(.acquire)) {
            const i = self.next_unit.fetchAdd(1, .monotonic);
            if (i >= self.units.items.len) return;
            const u = self.units.items[i];
            while (!self.admit(u)) {
                if (self.cancel.load(.acquire)) return;
                self.waitRelease();
            }
            try self.readUnit(i);
            self.unitDone(u);
        }
    }

    fn uringRun(self: *Reader, ring_in: linux.IoUring) !void {
        var ring = ring_in;
        defer ring.deinit();
        var nheld: usize = 0;
        var inflight: usize = 0;
        const units = self.units.items;
        const held = try self.gpa.alloc(usize, @max(self.cfg.qd, 1));
        defer self.gpa.free(held);
        const tries = try self.gpa.alloc(u8, held.len);
        defer self.gpa.free(tries);
        // Runs before the frees and before `ring.deinit`. A submitted read of a regular file completes;
        // closing the fd before that store lands can write a buffer already given back.
        defer {
            reapSubmitted(&ring, held, tries, &nheld, &inflight);
            if (builtin.is_test) if (self.faults) |f| {
                f.inflight_at_deinit = inflight;
            };
        }
        var cqes: [64]linux.io_uring_cqe = undefined;
        // A unit claimed off the shared cursor but not yet admitted by the window.
        var pending: ?usize = null;
        var drained = false;
        // A submit refusal past the bound: unconsumed SQEs were pread, and what the kernel accepted is drained here.
        var draining = false;
        var stalled_err: ?anyerror = null;
        while (true) {
            if (draining and inflight == 0) {
                if (stalled_err) |e| return e;
                try self.finishHeld(pending);
                return;
            }
            const cancelled = self.cancel.load(.acquire);
            while (!cancelled and !draining and inflight < self.cfg.qd) {
                const idx = pending orelse blk: {
                    if (drained) break;
                    const i = self.next_unit.fetchAdd(1, .monotonic);
                    if (i >= units.len) {
                        drained = true;
                        break;
                    }
                    break :blk i;
                };
                const u = units[idx];
                if (!self.admit(u)) {
                    pending = idx;
                    break;
                }
                pending = null;
                if (self.cfg.fail_unit == idx) return error.ReadFailed;
                const f = self.files.items[u.file];
                held[nheld] = idx;
                tries[nheld] = 0;
                nheld += 1;
                const buf = f.buf[@intCast(u.off)..][0..u.len];
                _ = try ring.read(idx, f.fd, .{ .buffer = buf }, u.off);
                inflight += 1;
            }
            if (inflight == 0) {
                if ((drained and pending == null) or cancelled) return;
                self.waitRelease();
                continue;
            }
            const n = self.takeCqes(&ring, &cqes) catch |err| switch (err) {
                // Unconsumed SQEs are dropped and pread. Ones an earlier enter accepted stay in `held` until their CQE.
                error.UringStalled => {
                    self.dropUnconsumed(&ring, held, tries, &nheld, &inflight) catch |e| {
                        stalled_err = e;
                    };
                    draining = true;
                    continue;
                },
                else => |e| return e,
            };
            // The whole batch is already off the CQ. Count it before any pread or resubmit can fail.
            inflight -= n;
            if (builtin.is_test) if (self.faults) |fault| {
                fault.batch_n = n;
            };
            for (cqes[0..n], 0..) |*c, i| {
                if (builtin.is_test) if (self.faults) |fault| if (fault.short_first and i == 0 and n >= 2) {
                    fault.short_first = false;
                    c.res = 0;
                };
                if (builtin.is_test) if (self.faults) |fault| if (fault.short_got != 0 and c.res > 0) {
                    c.res = @intCast(fault.short_got);
                    fault.short_got = 0;
                };
                const idx: usize = @intCast(c.user_data);
                const u = units[idx];
                const f = self.files.items[u.file];
                if (builtin.is_test) if (self.faults) |fault| if (fault.reject_direct > 0 and f.buffered_fd >= 0) {
                    fault.reject_direct -= 1;
                    c.res = -@as(i32, @backingInt(linux.E.INVAL));
                };
                const again = c.res == -@as(i32, @backingInt(linux.E.AGAIN));
                const replay = builtin.is_test and self.faults != null and self.faults.?.cqe_replays > 0;
                const slot = heldSlot(held[0..nheld], idx);
                if (again or replay) {
                    if (replay) self.faults.?.cqe_replays -= 1;
                    if (slot) |s| {
                        tries[s] += 1;
                        if (tries[s] < uring_retry_bound) {
                            // The same buffer and offset: the kernel asked us to try this read again.
                            const buf = f.buf[@intCast(u.off)..][0..u.len];
                            _ = try ring.read(idx, f.fd, .{ .buffer = buf }, u.off);
                            inflight += 1;
                            continue;
                        }
                        heldRemove(held, tries, &nheld, s);
                    }
                    try self.readUnit(idx);
                    self.unitDone(u);
                    continue;
                }
                // A finished direct read the file system rejected. This unit is pread through the buffered
                // descriptor. `fd` is left alone: a sibling still in the kernel is a read of that descriptor.
                const rejected = c.res == -@as(i32, @backingInt(linux.E.INVAL));
                if (rejected and f.buffered_fd >= 0) {
                    if (slot) |s| heldRemove(held, tries, &nheld, s);
                    try self.readUnit(idx);
                    f.read_fallback.store(true, .release);
                    self.unitDone(u);
                    continue;
                }
                // `-EIO` and any other failure. Siblings in this batch are already counted; still-submitted reads
                // are reaped.
                if (c.res < 0) return error.ReadFailed;
                if (slot) |s| heldRemove(held, tries, &nheld, s);
                const want = @min(@as(u64, u.len), f.size - u.off);
                // O_DIRECT reads the file's unaligned tail short; anything else short is finished with pread.
                const got: u64 = @intCast(c.res);
                if (got < want) {
                    if (builtin.is_test) if (self.faults) |fault| {
                        fault.repaired = true;
                        fault.repair_at = u.off + got;
                        fault.preads += 1;
                        if (fault.fail_pread) return error.ReadFailed;
                    };
                    try preadAll(f, u.off + got, u.off + want);
                }
                self.unitDone(u);
            }
        }
    }

    /// The unit claimed but not yet queued, then the shared cursor. A read the kernel accepted is not here:
    /// its buffer stays with the ring until the completion arrives.
    fn finishHeld(self: *Reader, pending: ?usize) !void {
        if (pending) |i| {
            const u = self.units.items[i];
            while (!self.admit(u)) {
                if (self.cancel.load(.acquire)) return;
                self.waitRelease();
            }
            try self.readUnit(i);
            self.unitDone(u);
        }
        try self.preadRun();
    }

    /// Publish queued SQEs and wait for one completion. A submit-side `EAGAIN` or `EBUSY` counts even when an
    /// earlier enter in this wait already moved the SQ head. Past the bound the caller drops SQEs still between
    /// head and tail. `EINTR` does not count. An empty completion batch waits too.
    fn takeCqes(self: *Reader, ring: *linux.IoUring, cqes: []linux.io_uring_cqe) !u32 {
        var refused: u8 = 0;
        while (true) {
            const ready = ring.flush_sq();
            _ = self.ringEnter(ring, ready) catch |err| switch (err) {
                error.SignalInterrupt => {
                    if (self.cancel.load(.acquire)) return error.ReadFailed;
                    std.Thread.yield() catch {};
                    continue;
                },
                error.SystemResources, error.CompletionQueueOvercommitted => {
                    // Nothing queued: this is a wait. A positive ready is a submit the kernel refused.
                    if (ready > 0) {
                        refused += 1;
                        if (refused >= uring_retry_bound) return error.UringStalled;
                    }
                    if (self.cancel.load(.acquire)) return error.ReadFailed;
                    std.Thread.yield() catch {};
                    continue;
                },
                else => |e| return e,
            };
            if (builtin.is_test) if (self.faults) |f| if (f.wait_intr > 0) {
                f.wait_intr -= 1;
                if (self.rowGroupReady()) f.early_ready = true;
                if (self.cancel.load(.acquire)) return error.ReadFailed;
                std.Thread.yield() catch {};
                continue;
            };
            // A test gathers several completions into one copy. Ready CQEs are not submitted again.
            if (builtin.is_test) if (self.faults) |f| if (f.hold_batch > 1) {
                const queued = ring.cq_ready();
                if (queued > 0 and queued < f.hold_batch) {
                    std.Thread.yield() catch {};
                    continue;
                }
            };
            const n = ring.copy_cqes(cqes, 0) catch |err| switch (err) {
                error.SignalInterrupt, error.SystemResources, error.CompletionQueueOvercommitted => {
                    if (self.cancel.load(.acquire)) return error.ReadFailed;
                    std.Thread.yield() catch {};
                    continue;
                },
                else => |e| return e,
            };
            if (n != 0) return n;
            if (self.cancel.load(.acquire)) return error.ReadFailed;
            std.Thread.yield() catch {};
        }
    }

    fn ringEnter(self: *Reader, ring: *linux.IoUring, ready: u32) !u32 {
        if (builtin.is_test) if (self.faults) |f| if (f.at < f.inject.len) {
            const inj = f.inject[f.at];
            f.at += 1;
            switch (inj) {
                .again => return error.SystemResources,
                .busy => return error.CompletionQueueOvercommitted,
                .intr => return error.SignalInterrupt,
                .submit => |n| return ring.enter(@min(ready, n), 1, linux.IORING_ENTER_GETEVENTS),
            }
        };
        return ring.enter(ready, 1, linux.IORING_ENTER_GETEVENTS);
    }

    /// SQEs the kernel has not consumed (`head`..`tail`) are taken back and read with pread. `held` keeps the ones
    /// an earlier enter accepted; those buffers stay with the ring until the completion.
    fn dropUnconsumed(
        self: *Reader,
        ring: *linux.IoUring,
        held: []usize,
        tries: []u8,
        nheld: *usize,
        inflight: *usize,
    ) !void {
        const dropped_n = splitUnconsumed(ring, held, tries, nheld);
        if (inflight.* >= dropped_n) inflight.* -= dropped_n else inflight.* = 0;
        rewindSq(ring);
        for (held[nheld.*..][0..dropped_n]) |idx| {
            try self.readUnit(idx);
            self.unitDone(self.units.items[idx]);
        }
    }

    /// Every exit: SQEs the kernel never took are dropped, then submitted reads are reaped to `inflight == 0`.
    /// Nothing is submitted and nothing is published. A regular-file read completes, so the wait ends. `EINTR`,
    /// `EAGAIN` and `EBUSY` on the wait are retried; CQEs already queued are copied without entering.
    fn reapSubmitted(
        ring: *linux.IoUring,
        held: []usize,
        tries: []u8,
        nheld: *usize,
        inflight: *usize,
    ) void {
        const dropped_n = splitUnconsumed(ring, held, tries, nheld);
        if (inflight.* >= dropped_n) inflight.* -= dropped_n else inflight.* = 0;
        rewindSq(ring);
        var cqes: [64]linux.io_uring_cqe = undefined;
        while (inflight.* > 0) {
            if (ring.cq_ready() == 0) {
                _ = ring.enter(0, 1, linux.IORING_ENTER_GETEVENTS) catch {
                    std.Thread.yield() catch {};
                    continue;
                };
            }
            const n = ring.copy_cqes(&cqes, 0) catch {
                std.Thread.yield() catch {};
                continue;
            };
            if (n == 0) {
                std.Thread.yield() catch {};
                continue;
            }
            const got: usize = n;
            inflight.* = if (got >= inflight.*) 0 else inflight.* - got;
        }
    }

    fn rowGroupReady(self: *Reader) bool {
        for (self.files.items) |f| {
            for (f.rg_ready) |ready| if (ready.load(.acquire) != 0) return true;
        }
        return false;
    }
};

/// Submit refusals before the kernel has the reads, and -EAGAIN completions after the buffer is no longer in
/// flight. A wait interrupted by EINTR is not counted. A protocol budget, not a file property.
const uring_retry_bound: u8 = 8;

fn splitUnconsumed(ring: *linux.IoUring, held: []usize, tries: []u8, nheld: *usize) usize {
    var end = nheld.*;
    var i: usize = 0;
    while (i < end) {
        if (sqHolds(ring, held[i])) {
            end -= 1;
            std.mem.swap(usize, &held[i], &held[end]);
            std.mem.swap(u8, &tries[i], &tries[end]);
        } else i += 1;
    }
    const dropped = nheld.* - end;
    nheld.* = end;
    return dropped;
}

fn rewindSq(ring: *linux.IoUring) void {
    const head = @atomicLoad(u32, ring.sq.head, .acquire);
    @atomicStore(u32, ring.sq.tail, head, .release);
    ring.sq.sqe_head = head;
    ring.sq.sqe_tail = head;
}

fn sqHolds(ring: *linux.IoUring, idx: usize) bool {
    const want: u64 = idx;
    var i = @atomicLoad(u32, ring.sq.head, .acquire);
    const tail = @atomicLoad(u32, ring.sq.tail, .acquire);
    while (i != tail) : (i +%= 1) {
        const at = ring.sq.array[i & ring.sq.mask];
        if (ring.sq.sqes[at].user_data == want) return true;
    }
    // `read` queues here before `flush_sq` publishes the shared array. A failure between those two
    // leaves them unsubmitted; waiting on a completion for one never returns.
    var q = ring.sq.sqe_head;
    const qtail = ring.sq.sqe_tail;
    while (q != qtail) : (q +%= 1) {
        if (ring.sq.sqes[q & ring.sq.mask].user_data == want) return true;
    }
    return false;
}

fn heldSlot(held: []const usize, idx: usize) ?usize {
    for (held, 0..) |h, i| if (h == idx) return i;
    return null;
}

fn heldRemove(held: []usize, tries: []u8, n: *usize, slot: usize) void {
    n.* -= 1;
    held[slot] = held[n.*];
    tries[slot] = tries[n.*];
}

/// pread `[lo, hi)` of `f` into its buffer. A direct file keeps its buffered descriptor for this: the tail past
/// the last full page is not a legal `O_DIRECT` read, and neither is a completion of `EINVAL`. `f.fd` stays as the
/// ring submitted it.
fn preadAll(f: *File, lo: u64, hi: u64) !void {
    const fd = if (f.buffered_fd >= 0) f.buffered_fd else f.fd;
    var off = lo;
    while (off < hi) {
        const len = std.mem.alignForward(u64, hi - off, pageLen());
        const rc = linux.pread(fd, f.buf.ptr + off, @intCast(@min(len, f.buf.len - off)), @intCast(off));
        switch (linux.errno(rc)) {
            .SUCCESS => {},
            .INTR => continue,
            else => return error.ReadFailed,
        }
        if (rc == 0) return error.ReadFailed; // the file shrank under us
        off += rc;
    }
}

fn nowNs() u64 {
    var ts: linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

const testing = std.testing;

fn testFile(tmp: *testing.TmpDir, name: []const u8, data: []const u8, buf: []u8) ![]const u8 {
    var f = try tmp.dir.createFile(testing.io, name, .{});
    defer f.close(testing.io);
    try f.writeStreamingAll(testing.io, data);
    return std.fmt.bufPrint(buf, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name });
}

test "every backend reads exactly the planned bytes, row group by row group, within any window" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var prng = std.Random.DefaultPrng.init(0x10ad);
    const r = prng.random();
    const size: usize = (4 << 20) + 1234; // an unaligned tail, which O_DIRECT reads short
    const data = try testing.allocator.alloc(u8, size);
    defer testing.allocator.free(data);
    r.bytes(data);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);

    for ([_]Mode{ .pread, .uring }) |mode| for ([_]u64{ 1 << 30, 4096 }) |window| {
        if (mode == .uring and !uringAvailable()) continue;
        const rd = try Reader.create(testing.allocator, .{
            .mode = mode,
            .threads = 3,
            .block = 64 << 10,
            .window = window,
        });
        defer rd.destroy();
        const f = try rd.openFile(path, size);
        // Eight row groups over consecutive eighths of the file, each wanting a few ranges of it; one pruned.
        var plans: [8]?[]Range = undefined;
        var store: [8][4]Range = undefined;
        for (&plans, 0..) |*p, g| {
            if (g == 5) {
                p.* = null;
                continue;
            }
            const lo: u64 = g * size / 8;
            const hi: u64 = (g + 1) * size / 8;
            for (&store[g]) |*rg| {
                const a = r.intRangeLessThan(u64, lo, hi);
                rg.* = .{ .start = a, .end = @min(hi, a + r.intRangeAtMost(u64, 1, 300_000)) };
            }
            p.* = &store[g];
        }
        try rd.planFile(f, &plans, 4096);
        try rd.start();
        const gate = f.gate();
        for (plans, 0..) |p, g| {
            try gate.wait(gate.ctx, g);
            if (p) |rs| for (rs) |rg| {
                const lo: usize = @intCast(rg.start);
                const hi: usize = @intCast(rg.end);
                try testing.expectEqualSlices(u8, data[lo..hi], f.bytes()[lo..hi]);
            };
            gate.done(gate.ctx, g, 1);
        }
        try testing.expectEqual(@as(u64, 0), rd.submitted.load(.monotonic) - rd.released.load(.monotonic));
    };
}

test "probed ranges are read first, once, and given back with their row groups" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var prng = std.Random.DefaultPrng.init(0x9e0b);
    const size: usize = (2 << 20) + 777;
    const data = try testing.allocator.alloc(u8, size);
    defer testing.allocator.free(data);
    prng.random().bytes(data);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    const page = pageLen();
    for ([_]Mode{ .pread, .uring }) |mode| for ([_]u64{ 1 << 30, 4096 }) |window| {
        if (mode == .uring and !uringAvailable()) continue;
        const rd = try Reader.create(testing.allocator, .{ .mode = mode, .threads = 3, .block = 32 << 10, .window = window });
        defer rd.destroy();
        const f = try rd.openFile(path, size);
        // Four row groups of a quarter each, one pruned; each probes the first 10 000 bytes of its quarter (a
        // dictionary page at the start of a chunk), and the last also a range no row group reads.
        const q = size / 4;
        var store: [4][1]Range = undefined;
        var plans: [4]?[]Range = undefined;
        for (&store, &plans, 0..) |*st, *p, g| {
            st[0] = .{ .start = g * q + 100, .end = (g + 1) * q };
            p.* = if (g == 2) null else st;
        }
        try rd.planFile(f, &plans, 0);
        for (0..4) |g| try rd.probe(f, g, g * q + 100, g * q + 10_100);
        try rd.probe(f, 3, size - 50, size + 4096);
        try rd.start();
        // The probes are read before any row group's data is asked for.
        try File.probeWait(f, 0);
        for (0..4) |g| {
            const lo = g * q + 100;
            try testing.expectEqualSlices(u8, data[lo .. lo + 10_000], f.bytes()[lo .. lo + 10_000]);
        }
        try testing.expectEqualSlices(u8, data[size - 50 ..], f.bytes()[size - 50 ..]);
        // Planning is done: the pruned row group's probe pages go back, all but any page another row group covers.
        rd.releasePruned();
        const inner = std.mem.alignForward(u64, 2 * q + 100, page) + page;
        if (inner + page <= 2 * q + 10_100) try testing.expectEqual(@as(u8, 0), f.bytes()[inner]);
        const gate = f.gate();
        for (plans, 0..) |p, g| {
            try gate.wait(gate.ctx, g);
            if (p) |rs| for (rs) |r| {
                try testing.expectEqualSlices(u8, data[r.start..r.end], f.bytes()[r.start..r.end]);
            };
            gate.done(gate.ctx, g, 1);
        }
        // Every probed page is read exactly once, by a probe; no row group's own reads take it again.
        const pages = try testing.allocator.alloc(u8, (size + page - 1) / page);
        defer testing.allocator.free(pages);
        @memset(pages, 0);
        for (rd.units.items) |u| {
            var off = u.off;
            while (off < u.off + u.len) : (off += page) pages[off / page] += 1;
        }
        for (f.probes.items) |pr| {
            var off = pr.r.start;
            while (off < pr.r.end) : (off += page) try testing.expectEqual(@as(u8, 1), pages[off / page]);
        }
        try testing.expectEqual(@as(u64, 0), rd.submitted.load(.monotonic) - rd.released.load(.monotonic));
    };
}

test "an abandoned reader stops, gives its pages back, and still tears down" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 1 << 20);
    defer testing.allocator.free(data);
    @memset(data, 9);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    for ([_]Mode{ .pread, .uring }) |mode| {
        if (mode == .uring and !uringAvailable()) continue;
        // A one-page window: most row groups wait on releases nobody makes when the reader is abandoned.
        const rd = try Reader.create(testing.allocator, .{ .mode = mode, .threads = 2, .block = 16 << 10, .window = 4096 });
        defer rd.destroy();
        const f = try rd.openFile(path, data.len);
        var store: [8][1]Range = undefined;
        var plans: [8]?[]Range = undefined;
        for (&plans, &store, 0..) |*p, *st, g| {
            st[0] = .{ .start = g * (128 << 10), .end = (g + 1) * (128 << 10) };
            p.* = st;
        }
        try rd.planFile(f, &plans, 0);
        try rd.probe(f, 0, 0, 100);
        try rd.start();
        try File.probeWait(f, 0);
        const gate = f.gate();
        try gate.wait(gate.ctx, 0);
        rd.abandon();
        try testing.expectEqual(@as(usize, 0), rd.threads.items.len);
        // The pages are given back: what is left reads as the anonymous mapping's zeros.
        try testing.expectEqual(@as(u8, 0), f.bytes()[0]);
    }
}

test "a failed probe fails planning's wait and every row group's" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 1 << 20);
    defer testing.allocator.free(data);
    @memset(data, 3);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    for ([_]Mode{ .pread, .uring }) |mode| {
        if (mode == .uring and !uringAvailable()) continue;
        // Unit 0 is the first probe's: probes go before every row group's reads.
        const rd = try Reader.create(testing.allocator, .{ .mode = mode, .threads = 2, .block = 16 << 10, .fail_unit = 0 });
        defer rd.destroy();
        const f = try rd.openFile(path, data.len);
        var store: [2][1]Range = undefined;
        var plans: [2]?[]Range = undefined;
        for (&plans, &store, 0..) |*p, *st, g| {
            st[0] = .{ .start = g * (512 << 10), .end = (g + 1) * (512 << 10) };
            p.* = st;
        }
        try rd.planFile(f, &plans, 0);
        try rd.probe(f, 1, 512 << 10, (512 << 10) + 100);
        try rd.start();
        try testing.expectError(error.LocalReadFailed, File.probeWait(f, 1));
        const gate = f.gate();
        for (0..2) |g| try testing.expectError(error.LocalReadFailed, gate.wait(gate.ctx, g));
    }
}

test "a failed read fails the row groups waiting on it, and teardown neither hangs nor leaks" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 1 << 20);
    defer testing.allocator.free(data);
    @memset(data, 7);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    for ([_]Mode{ .pread, .uring }) |mode| {
        if (mode == .uring and !uringAvailable()) continue;
        const rd = try Reader.create(testing.allocator, .{
            .mode = mode,
            .threads = 2,
            .block = 16 << 10,
            .fail_unit = 5,
        });
        defer rd.destroy();
        const f = try rd.openFile(path, data.len);
        var store: [4][1]Range = undefined;
        var plans: [4]?[]Range = undefined;
        for (&plans, &store, 0..) |*p, *st, g| {
            st[0] = .{ .start = g * (256 << 10), .end = (g + 1) * (256 << 10) };
            p.* = st;
        }
        try rd.planFile(f, &plans, 0);
        try rd.start();
        const gate = f.gate();
        // 16 units of 16 KiB per row group: unit 5 is row group 0's.
        try testing.expectError(error.LocalReadFailed, gate.wait(gate.ctx, 0));
        // The rest either read before the failure or fail too; none waits forever.
        for (1..4) |g| gate.wait(gate.ctx, g) catch |err| try testing.expectEqual(error.LocalReadFailed, err);
    }
}

test "a row group's release keeps pages other row groups' ranges still cover" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var prng = std.Random.DefaultPrng.init(0x1d8);
    const size: usize = 2 << 20;
    const data = try testing.allocator.alloc(u8, size);
    defer testing.allocator.free(data);
    prng.random().bytes(data);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);

    // Arrow's layout: each row group's data, then every row group's column indexes, then every offset index. With a
    // gap this wide each row group's index ranges coalesce over all the others' (and its data over its neighbours').
    const n = 8;
    var store: [n][3]Range = undefined;
    var plans: [n]?[]Range = undefined;
    for (&store, &plans, 0..) |*st, *p, g| {
        st[0] = .{ .start = g * (128 << 10) + 100, .end = (g + 1) * (128 << 10) + 3000 };
        st[1] = .{ .start = (1 << 20) + g * 300, .end = (1 << 20) + g * 300 + 300 };
        st[2] = .{ .start = (1 << 20) + 9000 + g * 300, .end = (1 << 20) + 9000 + g * 300 + 300 };
        p.* = st;
    }
    for ([_]Mode{ .pread, .uring }) |mode| for ([_]bool{ false, true }) |reverse| {
        if (mode == .uring and !uringAvailable()) continue;
        const window: u64 = if (reverse) 1 << 30 else 4096;
        const rd = try Reader.create(testing.allocator, .{
            .mode = mode,
            .threads = 3,
            .block = 16 << 10,
            .window = window,
        });
        defer rd.destroy();
        const f = try rd.openFile(path, size);
        try rd.planFile(f, &plans, 1 << 20);
        try rd.start();
        const gate = f.gate();
        var order: [n]usize = undefined;
        for (&order, 0..) |*o, i| o.* = if (reverse) n - 1 - i else i;
        if (reverse) for (0..n) |g| try gate.wait(gate.ctx, g);
        for (order, 0..) |g, i| {
            if (!reverse) try gate.wait(gate.ctx, g);
            // Every row group read and not yet done still sees all its bytes.
            for (if (reverse) order[i..] else order[i .. i + 1]) |live| for (store[live]) |r| {
                const lo: usize = @intCast(r.start);
                const hi: usize = @intCast(r.end);
                try testing.expectEqualSlices(u8, data[lo..hi], f.bytes()[lo..hi]);
            };
            gate.done(gate.ctx, g, 1);
        }
        for (f.seg_refs) |*r| try testing.expectEqual(@as(u32, 0), r.load(.monotonic));
        try testing.expectEqual(@as(u64, 0), rd.submitted.load(.monotonic) - rd.released.load(.monotonic));
    };
}

test "an allocation failing anywhere in open, plan or start leaks nothing and leaves no thread behind" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 256 << 10);
    defer testing.allocator.free(data);
    @memset(data, 3);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator, p: []const u8, size: usize) !void {
            const rd = try Reader.create(gpa, .{ .mode = .pread, .threads = 3, .block = 16 << 10 });
            defer rd.destroy();
            const f = try rd.openFile(p, size);
            var store: [2][1]Range = .{ .{.{ .start = 0, .end = 100_000 }}, .{.{ .start = 90_000, .end = size }} };
            var plans: [2]?[]Range = .{ &store[0], &store[1] };
            try rd.planFile(f, &plans, 4096);
            try rd.start();
            const gate = f.gate();
            for (0..2) |g| {
                try gate.wait(gate.ctx, g);
                gate.done(gate.ctx, g, 1);
            }
        }
    }.run, .{ path, data.len });
}

test "threads whose ring fails to set up read with pread, and say so" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    if (!uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var prng = std.Random.DefaultPrng.init(0xf1);
    const data = try testing.allocator.alloc(u8, 1 << 20);
    defer testing.allocator.free(data);
    prng.random().bytes(data);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    for ([_]struct { fail: u64, uring: u32 }{ .{ .fail = 0b101, .uring = 1 }, .{ .fail = 0b111, .uring = 0 } }) |c| {
        const rd = try Reader.create(testing.allocator, .{
            .mode = .uring,
            .threads = 3,
            .block = 16 << 10,
            .fail_rings = c.fail,
        });
        defer rd.destroy();
        const f = try rd.openFile(path, data.len);
        var store: [4][1]Range = undefined;
        var plans: [4]?[]Range = undefined;
        for (&plans, &store, 0..) |*p, *st, g| {
            st[0] = .{ .start = g * (256 << 10) + 7, .end = (g + 1) * (256 << 10) - 7 };
            p.* = st;
        }
        try rd.planFile(f, &plans, 0);
        try rd.start();
        const b = rd.backends();
        try testing.expectEqual(c.uring, b.uring_threads);
        try testing.expectEqual(3 - c.uring, b.pread_threads);
        try testing.expectEqual(@as(u32, 1), b.direct_files + b.buffered_files);
        try testing.expectEqual(f.direct, b.direct_files == 1);
        const gate = f.gate();
        for (store, 0..) |st, g| {
            try gate.wait(gate.ctx, g);
            const lo: usize = @intCast(st[0].start);
            const hi: usize = @intCast(st[0].end);
            try testing.expectEqualSlices(u8, data[lo..hi], f.bytes()[lo..hi]);
            gate.done(gate.ctx, g, 1);
        }
    }
}

test "a direct file whose rings never start is counted through the page cache" {
    if (builtin.os.tag != .linux or !uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 128 << 10);
    defer testing.allocator.free(data);
    @memset(data, 4);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "norings.bin", data, &path_buf);
    const rd = try Reader.create(testing.allocator, .{
        .mode = .uring,
        .threads = 1,
        .block = 64 << 10,
        .direct = true,
        .fail_rings = 1,
    });
    defer rd.destroy();
    const f = try rd.openFile(path, data.len);
    if (!f.direct) return error.SkipZigTest;
    var range = [_]Range{.{ .start = 0, .end = data.len }};
    var plans = [_]?[]Range{&range};
    try rd.planFile(f, &plans, 0);
    try rd.start();
    const gate = f.gate();
    try gate.wait(gate.ctx, 0);
    try testing.expectEqualSlices(u8, data, f.bytes()[0..data.len]);
    try testing.expect(f.read_fallback.load(.acquire));
    const b = rd.backends();
    try testing.expectEqual(@as(u32, 0), b.uring_threads);
    try testing.expectEqual(@as(u32, 1), b.pread_threads);
    try testing.expectEqual(@as(u32, 0), b.direct_files);
    try testing.expectEqual(@as(u32, 1), b.buffered_files);
    gate.done(gate.ctx, 0, 1);
}

fn virtualBytesForTest() !u64 {
    const rc = linux.open("/proc/self/maps", .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (linux.errno(rc) != .SUCCESS) return error.OpenFailed;
    const fd: i32 = @intCast(rc);
    defer _ = linux.close(fd);
    var buf: [128 << 10]u8 = undefined;
    const nr = linux.read(fd, &buf, buf.len);
    if (linux.errno(nr) != .SUCCESS or nr == buf.len) return error.ReadFailed;
    var total: u64 = 0;
    var lines = std.mem.splitScalar(u8, buf[0..nr], '\n');
    while (lines.next()) |line| {
        const dash = std.mem.indexOfScalar(u8, line, '-') orelse continue;
        const space = std.mem.indexOfScalar(u8, line, ' ') orelse return error.ReadFailed;
        const lo = try std.fmt.parseInt(u64, line[0..dash], 16);
        const hi = try std.fmt.parseInt(u64, line[dash + 1 .. space], 16);
        total += hi - lo;
    }
    return total;
}

test "failed reader open returns its anonymous mapping too" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", "x", &path_buf);
    const rd = try Reader.create(testing.allocator, .{ .mode = .pread });
    var failing = testing.FailingAllocator.init(testing.allocator, .{ .fail_index = 1 });
    rd.gpa = failing.allocator();
    defer rd.destroy();
    _ = try virtualBytesForTest();
    const before = try virtualBytesForTest();
    // File alloc succeeds; files.append fails after a 32 MiB mapping has been made.
    try testing.expectError(error.OutOfMemory, rd.openFile(path, 32 << 20));
    const after = try virtualBytesForTest();
    // Ignore a small allocator/stack VMA change; the failed open's mapping must be gone.
    try testing.expect(after <= before + (1 << 20));
}

test "failed row group gates do not release bytes never admitted" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 256 << 10);
    defer testing.allocator.free(data);
    @memset(data, 3);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    const rd = try Reader.create(testing.allocator, .{ .mode = .pread, .threads = 3, .window = 4096, .fail_unit = 0 });
    defer rd.destroy();
    const f = try rd.openFile(path, data.len);
    var store: [2][1]Range = .{ .{.{ .start = 0, .end = 128 << 10 }}, .{.{ .start = 128 << 10, .end = 256 << 10 }} };
    var plans: [2]?[]Range = .{ &store[0], &store[1] };
    try rd.planFile(f, &plans, 0);
    try rd.start();
    const gate = f.gate();
    for (0..2) |g| try testing.expectError(error.LocalReadFailed, gate.wait(gate.ctx, g));
    // End the I/O failure before examining the scan workers' required error callbacks.
    for (rd.threads.items) |t| t.join();
    rd.threads.clearRetainingCapacity();
    for (0..2) |g| gate.done(gate.ctx, g, 1);
    try testing.expect(rd.released.load(.acquire) <= rd.submitted.load(.acquire));
}

test "continuation reads wait until their row group is admitted" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 8 << 20);
    defer testing.allocator.free(data);
    @memset(data, 7);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    for ([_]Mode{ .pread, .uring }) |mode| {
        if (mode == .uring and !uringAvailable()) continue;
        try continuationsWaitFor(mode, path, data.len);
    }
}

fn continuationsWaitFor(mode: Mode, path: []const u8, size: usize) !void {
    const rd = try Reader.create(testing.allocator, .{ .mode = mode, .threads = 3, .block = 64 << 10, .window = 4096 });
    defer rd.destroy();
    const f = try rd.openFile(path, size);
    var store: [2][1]Range = .{ .{.{ .start = 0, .end = 4 << 20 }}, .{.{ .start = 4 << 20, .end = 8 << 20 }} };
    var plans: [2]?[]Range = .{ &store[0], &store[1] };
    try rd.planFile(f, &plans, 0);
    try rd.start();
    const gate = f.gate();
    try gate.wait(gate.ctx, 0);
    // Keep the first row group's window charge: the second cannot yet be admitted.
    // Its continuation units must not read behind its waiting first unit.
    const deadline = nowNs() + 200 * std.time.ns_per_ms;
    while (nowNs() < deadline and rd.stats.bytes.load(.acquire) <= rd.submitted.load(.acquire)) {
        var ts: linux.timespec = .{ .sec = 0, .nsec = std.time.ns_per_ms };
        _ = linux.nanosleep(&ts, null);
    }
    const read = rd.stats.bytes.load(.acquire);
    const charged = rd.submitted.load(.acquire);
    try testing.expectEqual(@as(u64, 4 << 20), charged);
    try testing.expectEqual(charged, read);
    gate.done(gate.ctx, 0, 1);
    try gate.wait(gate.ctx, 1);
    gate.done(gate.ctx, 1, 1);
}

test "local read bytes exclude EOF padding without shrinking the window charge" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", "x", &path_buf);
    for ([_]Mode{ .pread, .uring }) |mode| {
        if (mode == .uring and !uringAvailable()) continue;
        const rd = try Reader.create(testing.allocator, .{ .mode = mode, .threads = 1 });
        defer rd.destroy();
        const f = try rd.openFile(path, 1);
        var ranges = [_]Range{.{ .start = 0, .end = 1 }};
        const plans = [_]?[]Range{&ranges};
        try rd.planFile(f, &plans, 0);
        try rd.start();
        const gate = f.gate();
        try gate.wait(gate.ctx, 0);
        try testing.expectEqual(@as(u64, 1), rd.stats.bytes.load(.acquire));
        try testing.expectEqual(pageLen(), rd.submitted.load(.acquire));
        gate.done(gate.ctx, 0, 1);
        try testing.expectEqual(pageLen(), rd.released.load(.acquire));
    }
}

fn wholeShare(gpa: std.mem.Allocator, bytes: []align(std.heap.page_size_min) const u8) f64 {
    if (bytes.len == 0) return 0;
    const ranges = [_]Range{.{ .start = 0, .end = bytes.len }};
    return sampleRanges(gpa, bytes, &ranges).share;
}

test "planned-range residency bounds its allocation and partial-page windows" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const page = std.heap.pageSize();
    const mapping = try posix.mmap(null, 4096 * page + 1, .{ .READ = true, .WRITE = true }, .{
        .TYPE = .PRIVATE,
        .ANONYMOUS = true,
        .NORESERVE = true,
    }, -1, 0);
    defer posix.munmap(mapping);
    @memset(mapping, 1);
    var storage: [2048]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&storage);
    try testing.expectEqual(@as(f64, 1), wholeShare(bounded.allocator(), mapping));
    bounded.reset();
    try testing.expectEqual(@as(f64, 1), wholeShare(bounded.allocator(), mapping[0 .. 32 * page - 1]));
    bounded.reset();
    try testing.expectEqual(@as(f64, 0), wholeShare(bounded.allocator(), mapping[0..0]));
}

test "auto maps a warm file, buffers a partly warm one, and reads a cold one direct" {
    const auto: Policy = .{};
    const pinned: Policy = .{ .profile = .pread };
    try testing.expectEqual(Mode.pread, pinned.choose(0, 1).mode);
    try testing.expectEqual(Reason.explicit, pinned.choose(0, 1).reason);
    const asked: Policy = .{ .profile = .uring };
    if (uringAvailable()) {
        const direct = asked.choose(0, 0.1);
        try testing.expectEqual(Mode.uring, direct.mode);
        try testing.expect(direct.directRead());
    }
    const hot = auto.choose(Policy.warm_resident, 1);
    try testing.expectEqual(Mode.mmap, hot.mode);
    try testing.expectEqual(Reason.cached, hot.reason);
    try testing.expect(!hot.directRead());
    if (builtin.os.tag != .linux) return;
    if (uringAvailable()) {
        const cold_wide = auto.choose(0, 0.9);
        try testing.expectEqual(Mode.uring, cold_wide.mode);
        try testing.expectEqual(Reason.cold_direct, cold_wide.reason);
        try testing.expect(cold_wide.directRead());
        try testing.expectEqualStrings(
            "cold, O_DIRECT where the file allows it; --cache reads through the page cache",
            cold_wide.reason.phrase(),
        );
        const cold_narrow = auto.choose(Policy.cold_resident - 0.01, 0.1);
        try testing.expectEqual(Mode.uring, cold_narrow.mode);
        try testing.expect(cold_narrow.directRead());
        const warm_wide = auto.choose(0.5, 0.9);
        try testing.expectEqual(Mode.uring, warm_wide.mode);
        try testing.expectEqual(Reason.cold_buffered, warm_wide.reason);
        try testing.expect(!warm_wide.directRead());
    } else {
        try testing.expectEqual(Mode.pread, auto.choose(0, 0.9).mode);
        try testing.expectEqual(Reason.no_uring, auto.choose(0, 0.9).reason);
        try testing.expect(!auto.choose(0, 0.9).directRead());
    }
    const warm_narrow = auto.choose(Policy.cold_resident, 0.2);
    try testing.expectEqual(Mode.pread, warm_narrow.mode);
    try testing.expectEqual(Reason.cold_buffered, warm_narrow.reason);
    try testing.expect(!warm_narrow.directRead());
}

test "--cache reads through the page cache and never O_DIRECT" {
    const cache: Policy = .{ .cache = true };
    const wide = cache.choose(0, 0.9);
    const narrow = cache.choose(0, 0.1);
    try testing.expect(!wide.directRead());
    try testing.expect(!narrow.directRead());
    try testing.expectEqual(Reason.page_cache, wide.reason);
    try testing.expectEqual(Reason.page_cache, narrow.reason);
    try testing.expectEqualStrings("through the page cache (--cache)", wide.reason.phrase());
    if (builtin.os.tag == .linux and uringAvailable()) {
        try testing.expectEqual(Mode.uring, wide.mode);
    } else if (builtin.os.tag == .linux) {
        try testing.expectEqual(Mode.pread, wide.mode);
    }
    try testing.expectEqual(Mode.pread, narrow.mode);
    const hot = cache.choose(Policy.warm_resident, 1);
    try testing.expectEqual(Mode.mmap, hot.mode);
    try testing.expectEqual(Reason.cached, hot.reason);
    const asked: Policy = .{ .profile = .uring, .cache = true };
    try testing.expect(!asked.choose(0, 1).directRead());
    try testing.expectEqual(Reason.page_cache, asked.choose(0, 1).reason);
}

test "a cold read that does not fit the reader is mapped" {
    const auto: Policy = .{};
    const unknown = auto.choose(0, 0.9);
    if (builtin.os.tag == .linux and uringAvailable()) {
        try testing.expect(unknown.directRead());
        try testing.expectEqual(Reason.cold_direct, unknown.reason);
    } else {
        try testing.expect(!unknown.directRead());
    }
    const sparse = residencyFrom(0, 32, 10_000);
    try testing.expectEqual(@as(u64, 10_000), sparse.pages);
    const from_sample = auto.choose(sparse.share, 0.9);
    try testing.expectEqual(unknown.directRead(), from_sample.directRead());
    const failed: Residency = .{ .pages = 10 };
    try testing.expectEqual(@as(f64, 0), failed.share);
    const mapped = unknown.withinReader(false);
    try testing.expectEqual(Mode.mmap, mapped.mode);
    try testing.expectEqual(Reason.over_budget, mapped.reason);
    try testing.expect(!mapped.directRead());
    const explicit: Policy = .{ .profile = .uring };
    if (uringAvailable()) {
        const asked = explicit.choose(0, 0.2);
        try testing.expect(asked.directRead());
        const over = asked.withinReader(false);
        try testing.expectEqual(Mode.mmap, over.mode);
        try testing.expectEqual(Reason.over_budget, over.reason);
    }
}

test "residency weights a file by its planned pages, and an unknown sample is not cached" {
    const page = 4096;
    const cold: Residency = .{ .share = 0, .pages = (8 << 30) / page };
    const hot: Residency = .{ .share = 1, .pages = (64 << 20) / page };
    try testing.expect(weightShare(&.{ cold, hot }) < 0.8);
    const unknown: Residency = .{ .share = 0, .pages = hot.pages };
    const hidden = weightShare(&.{ hot, unknown });
    try testing.expect(hidden < 0.8);
    try testing.expect(hidden > 0.4);
}

test "residency follows the planned ranges, not the rest of the file" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const page = std.heap.pageSize();
    const pages: usize = 512;
    const size = page * pages;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, size);
    defer testing.allocator.free(data);
    @memset(data, 7);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "ranges.bin", data, &path_buf);
    try forgetFile(path, size);
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;
    const rc = linux.open(path_z[0..path.len :0], .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (linux.errno(rc) != .SUCCESS) return error.OpenFailed;
    const fd: i32 = @intCast(rc);
    defer _ = linux.close(fd);
    const warm_bytes = page * 4;
    const scratch = try testing.allocator.alloc(u8, warm_bytes);
    defer testing.allocator.free(scratch);
    const got = linux.pread(fd, scratch.ptr, warm_bytes, 0);
    if (linux.errno(got) != .SUCCESS) return error.ReadFailed;
    const map = try posix.mmap(null, size, .{ .READ = true }, .{ .TYPE = .SHARED }, fd, 0);
    defer posix.munmap(map);
    const warm_r = [_]Range{.{ .start = 0, .end = warm_bytes }};
    const warm = sampleRanges(testing.allocator, map, &warm_r);
    try testing.expectEqual(@as(u64, 4), warm.pages);
    try testing.expect(warm.share >= 0.8);
    const whole = wholeShare(testing.allocator, map);
    try testing.expect(whole < 0.8);
    const cached = (Policy{}).choose(warm.share, 0.2);
    try testing.expectEqual(Reason.cached, cached.reason);
    try testing.expectEqual(Mode.mmap, cached.mode);
}

fn readWhole(path: []const u8, data: []const u8, cfg: Config) !void {
    const rd = try Reader.create(testing.allocator, cfg);
    defer rd.destroy();
    const f = try rd.openFile(path, data.len);
    var range = [_]Range{.{ .start = 0, .end = data.len }};
    var plans = [_]?[]Range{&range};
    try rd.planFile(f, &plans, 0);
    try rd.start();
    const gate = f.gate();
    try gate.wait(gate.ctx, 0);
    try testing.expectEqualSlices(u8, data, f.bytes()[0..data.len]);
    gate.done(gate.ctx, 0, 1);
}

test "pread, buffered io_uring and direct io_uring read the same bytes" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var prng = std.Random.DefaultPrng.init(0x10);
    const data = try testing.allocator.alloc(u8, 1 << 20);
    defer testing.allocator.free(data);
    prng.random().bytes(data);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    try readWhole(path, data, .{ .mode = .pread, .threads = 2, .block = 64 << 10 });
    if (!uringAvailable()) return;
    try readWhole(path, data, .{ .mode = .uring, .threads = 2, .block = 64 << 10, .direct = false });
    try readWhole(path, data, .{ .mode = .uring, .threads = 2, .block = 64 << 10, .direct = true });
}

test "io_uring retries a refused enter and resubmits an EAGAIN completion" {
    if (builtin.os.tag != .linux or !uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 64 << 10);
    defer testing.allocator.free(data);
    @memset(data, 3);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "retry.bin", data, &path_buf);
    const inject = [_]Inject{ .intr, .intr, .again, .busy };
    var faults: Faults = .{ .inject = &inject, .cqe_replays = 1 };
    try readFaulted(path, data, &faults);
    try testing.expectEqual(@as(usize, 4), faults.at);
    try testing.expectEqual(@as(u8, 0), faults.cqe_replays);
    try testing.expectEqual(@as(u32, 0), faults.preads);
}

fn readFaulted(path: []const u8, data: []const u8, faults: *Faults) !void {
    const rd = try Reader.create(testing.allocator, .{
        .mode = .uring,
        .threads = 1,
        .block = 64 << 10,
        .direct = false,
    });
    defer rd.destroy();
    rd.faults = faults;
    const f = try rd.openFile(path, data.len);
    var range = [_]Range{.{ .start = 0, .end = data.len }};
    var plans = [_]?[]Range{&range};
    try rd.planFile(f, &plans, 0);
    const expect_fail = faults.fail_pread;
    try rd.start();
    const gate = f.gate();
    if (expect_fail) {
        try testing.expectError(error.LocalReadFailed, gate.wait(gate.ctx, 0));
        return;
    }
    try gate.wait(gate.ctx, 0);
    try testing.expectEqualSlices(u8, data, f.bytes()[0..data.len]);
    gate.done(gate.ctx, 0, 1);
}

test "io_uring past the retry bound finishes the unit with pread" {
    if (builtin.os.tag != .linux or !uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 64 << 10);
    defer testing.allocator.free(data);
    @memset(data, 5);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "bound.bin", data, &path_buf);
    var again: [16]Inject = undefined;
    for (&again) |*step| step.* = .again;
    var refused: Faults = .{ .inject = &again };
    try readFaulted(path, data, &refused);
    try testing.expectEqual(@as(usize, uring_retry_bound), refused.at);
    try testing.expect(refused.preads > 0);
    var replays: Faults = .{ .cqe_replays = 100 };
    try readFaulted(path, data, &replays);
    try testing.expectEqual(@as(u8, 100 - uring_retry_bound), replays.cqe_replays);
    var refuse_pread: Faults = .{ .inject = &again, .fail_pread = true };
    try readFaulted(path, data, &refuse_pread);
    var replay_pread: Faults = .{ .cqe_replays = 100, .fail_pread = true };
    try readFaulted(path, data, &replay_pread);
    const wide_data = try testing.allocator.alloc(u8, 192 << 10);
    defer testing.allocator.free(wide_data);
    @memset(wide_data, 5);
    var wide_buf: [256]u8 = undefined;
    const wide = try testFile(&tmp, "partial.bin", wide_data, &wide_buf);
    const partial_inject = [_]Inject{
        .{ .submit = 1 }, .again, .again, .again, .again, .again, .again, .again, .again,
    };
    var partial: Faults = .{ .inject = &partial_inject };
    try readFaulted(wide, wide_data, &partial);
    try testing.expectEqual(@as(u32, 2), partial.preads);
}

test "a short repair pread failure reaps the rest of its completion batch" {
    if (builtin.os.tag != .linux or !uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 128 << 10);
    defer testing.allocator.free(data);
    @memset(data, 7);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "batch.bin", data, &path_buf);
    var faults: Faults = .{ .hold_batch = 2, .short_first = true, .fail_pread = true };
    try readFaulted(path, data, &faults);
    try testing.expect(faults.batch_n >= 2);
    try testing.expectEqual(@as(usize, 0), faults.inflight_at_deinit);
}

test "a submitted io_uring read is not published while its wait is interrupted" {
    if (builtin.os.tag != .linux or !uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 192 << 10);
    defer testing.allocator.free(data);
    @memset(data, 6);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "inflight.bin", data, &path_buf);
    const rd = try Reader.create(testing.allocator, .{
        .mode = .uring,
        .threads = 1,
        .block = 64 << 10,
        .direct = false,
    });
    defer rd.destroy();
    var faults: Faults = .{ .wait_intr = 4 };
    rd.faults = &faults;
    const f = try rd.openFile(path, data.len);
    var range = [_]Range{.{ .start = 0, .end = data.len }};
    var plans = [_]?[]Range{&range};
    try rd.planFile(f, &plans, 0);
    try rd.start();
    const gate = f.gate();
    try gate.wait(gate.ctx, 0);
    try testing.expect(!faults.early_ready);
    try testing.expectEqual(@as(u8, 0), faults.wait_intr);
    try testing.expectEqual(@as(u64, 0), rd.released.load(.acquire));
    try testing.expectEqualSlices(u8, data, f.bytes()[0..data.len]);
    gate.done(gate.ctx, 0, 1);
}

test "a file that cannot open O_DIRECT is still read through the page cache" {
    if (builtin.os.tag != .linux or !uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 64 << 10);
    defer testing.allocator.free(data);
    @memset(data, 5);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "nodirect.bin", data, &path_buf);
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;
    const rc = linux.open(path_z[0..path.len :0], .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (linux.errno(rc) != .SUCCESS) return error.OpenFailed;
    const fd: i32 = @intCast(rc);
    defer _ = linux.close(fd);
    const before = linux.fcntl(fd, linux.F.GETFL, 0);
    try testing.expect(linux.errno(before) == .SUCCESS);
    const rd = try Reader.create(testing.allocator, .{
        .mode = .uring,
        .threads = 1,
        .direct = true,
        .fail_direct_open = true,
    });
    defer rd.destroy();
    const f = try rd.borrowFile(fd, data.len);
    try testing.expect(!f.direct);
    try testing.expectEqual(fd, f.fd);
    try testing.expect(!f.owned);
    try testing.expectEqual(@as(i32, -1), f.buffered_fd);
    const after = linux.fcntl(fd, linux.F.GETFL, 0);
    try testing.expectEqual(before, after);
    var range = [_]Range{.{ .start = 0, .end = data.len }};
    var plans = [_]?[]Range{&range};
    try rd.planFile(f, &plans, 0);
    try rd.start();
    const gate = f.gate();
    try gate.wait(gate.ctx, 0);
    try testing.expectEqualSlices(u8, data, f.bytes()[0..data.len]);
    gate.done(gate.ctx, 0, 1);
}

test "a finished direct read of EINVAL is pread, and the direct fd stays open" {
    if (builtin.os.tag != .linux or !uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 128 << 10);
    defer testing.allocator.free(data);
    @memset(data, 4);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "reject.bin", data, &path_buf);
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;
    const rc = linux.open(path_z[0..path.len :0], .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (linux.errno(rc) != .SUCCESS) return error.OpenFailed;
    const fd: i32 = @intCast(rc);
    defer _ = linux.close(fd);
    const before = linux.fcntl(fd, linux.F.GETFL, 0);
    try testing.expect(linux.errno(before) == .SUCCESS);
    const rd = try Reader.create(testing.allocator, .{
        .mode = .uring,
        .threads = 1,
        .block = 64 << 10,
        .direct = true,
    });
    defer rd.destroy();
    var faults: Faults = .{ .hold_batch = 2, .reject_direct = 1 };
    rd.faults = &faults;
    const f = try rd.borrowFile(fd, data.len);
    if (!f.direct) return error.SkipZigTest;
    var range = [_]Range{.{ .start = 0, .end = data.len }};
    var plans = [_]?[]Range{&range};
    try rd.planFile(f, &plans, 0);
    try rd.start();
    const gate = f.gate();
    try gate.wait(gate.ctx, 0);
    try testing.expectEqualSlices(u8, data, f.bytes()[0..data.len]);
    try testing.expect(f.read_fallback.load(.acquire));
    try testing.expect(faults.batch_n >= 2);
    const backs = rd.backends();
    try testing.expectEqual(@as(u32, 0), backs.direct_files);
    try testing.expectEqual(@as(u32, 1), backs.buffered_files);
    const after = linux.fcntl(fd, linux.F.GETFL, 0);
    try testing.expectEqual(before, after);
    const live = linux.fcntl(f.fd, linux.F.GETFL, 0);
    try testing.expect(linux.errno(live) == .SUCCESS);
    gate.done(gate.ctx, 0, 1);
}

test "an unaligned direct tail is read through the buffered fd and stays direct" {
    if (builtin.os.tag != .linux or !uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, (64 << 10) + 100);
    defer testing.allocator.free(data);
    @memset(data, 8);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "tail.bin", data, &path_buf);
    const rd = try Reader.create(testing.allocator, .{
        .mode = .uring,
        .threads = 1,
        .block = 64 << 10,
        .direct = true,
    });
    defer rd.destroy();
    const f = try rd.openFile(path, data.len);
    if (!f.direct) return error.SkipZigTest;
    var range = [_]Range{.{ .start = 0, .end = data.len }};
    var plans = [_]?[]Range{&range};
    try rd.planFile(f, &plans, 0);
    try rd.start();
    const gate = f.gate();
    try gate.wait(gate.ctx, 0);
    try testing.expectEqualSlices(u8, data, f.bytes()[0..data.len]);
    try testing.expect(!f.read_fallback.load(.acquire));
    const backs = rd.backends();
    try testing.expectEqual(@as(u32, 1), backs.direct_files);
    try testing.expectEqual(@as(u32, 0), backs.buffered_files);
    gate.done(gate.ctx, 0, 1);
}

test "a positive short completion repairs from an unaligned offset and stays direct" {
    if (builtin.os.tag != .linux or !uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 64 << 10);
    defer testing.allocator.free(data);
    @memset(data, 2);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "short.bin", data, &path_buf);
    const rd = try Reader.create(testing.allocator, .{
        .mode = .uring,
        .threads = 1,
        .block = 64 << 10,
        .direct = true,
    });
    defer rd.destroy();
    var faults: Faults = .{ .short_got = 100 };
    rd.faults = &faults;
    const f = try rd.openFile(path, data.len);
    if (!f.direct) return error.SkipZigTest;
    var range = [_]Range{.{ .start = 0, .end = data.len }};
    var plans = [_]?[]Range{&range};
    try rd.planFile(f, &plans, 0);
    try rd.start();
    const gate = f.gate();
    try gate.wait(gate.ctx, 0);
    try testing.expectEqualSlices(u8, data, f.bytes()[0..data.len]);
    try testing.expect(faults.repaired);
    try testing.expect(faults.repair_at % pageLen() != 0);
    try testing.expectEqual(@as(u64, 100), faults.repair_at);
    try testing.expect(!f.read_fallback.load(.acquire));
    const b = rd.backends();
    try testing.expectEqual(@as(u32, 1), b.direct_files);
    try testing.expectEqual(@as(u32, 0), b.buffered_files);
    gate.done(gate.ctx, 0, 1);
}

test "direct io opens its own fd and does not change a borrowed one" {
    if (builtin.os.tag != .linux or !uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 64 << 10);
    defer testing.allocator.free(data);
    @memset(data, 9);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;
    const rc = linux.open(path_z[0..path.len :0], .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (linux.errno(rc) != .SUCCESS) return error.OpenFailed;
    const fd: i32 = @intCast(rc);
    defer _ = linux.close(fd);
    const before = linux.fcntl(fd, linux.F.GETFL, 0);
    try testing.expect(linux.errno(before) == .SUCCESS);
    const rd = try Reader.create(testing.allocator, .{ .mode = .uring, .threads = 1, .direct = true });
    defer rd.destroy();
    const f = try rd.borrowFile(fd, data.len);
    const after = linux.fcntl(fd, linux.F.GETFL, 0);
    try testing.expectEqual(before, after);
    if (f.direct) {
        try testing.expect(f.fd != fd);
        try testing.expect(f.owned);
        try testing.expectEqual(fd, f.buffered_fd);
        try testing.expect(!f.own_buffered);
    } else {
        try testing.expectEqual(fd, f.fd);
        try testing.expect(!f.owned);
        try testing.expectEqual(@as(i32, -1), f.buffered_fd);
    }
    var range = [_]Range{.{ .start = 0, .end = data.len }};
    var plans = [_]?[]Range{&range};
    try rd.planFile(f, &plans, 0);
    try rd.start();
    const gate = f.gate();
    try gate.wait(gate.ctx, 0);
    try testing.expectEqualSlices(u8, data, f.bytes()[0..data.len]);
    gate.done(gate.ctx, 0, 1);
}

fn fileResidency(path: []const u8, size: usize) !f64 {
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    if (path.len >= path_z.len) return error.NameTooLong;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;
    const rc = linux.open(path_z[0..path.len :0], .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (linux.errno(rc) != .SUCCESS) return error.OpenFailed;
    const fd: i32 = @intCast(rc);
    defer _ = linux.close(fd);
    const map = try posix.mmap(null, size, .{ .READ = true }, .{ .TYPE = .SHARED }, fd, 0);
    defer posix.munmap(map);
    return wholeShare(testing.allocator, map);
}

fn forgetFile(path: []const u8, size: usize) !void {
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    if (path.len >= path_z.len) return error.NameTooLong;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;
    const rc = linux.open(path_z[0..path.len :0], .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (linux.errno(rc) != .SUCCESS) return error.OpenFailed;
    const fd: i32 = @intCast(rc);
    defer _ = linux.close(fd);
    _ = linux.fsync(fd);
    _ = linux.fadvise(fd, 0, 0, linux.POSIX_FADV.DONTNEED);
    const map = try posix.mmap(null, size, .{ .READ = true }, .{ .TYPE = .SHARED }, fd, 0);
    defer posix.munmap(map);
    _ = linux.madvise(map.ptr, map.len, linux.MADV.DONTNEED);
}

test "a buffered read of a cold file leaves it cached for the next choice" {
    if (builtin.os.tag != .linux or !uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const page = std.heap.pageSize();
    const size = page * 128;
    const data = try testing.allocator.alloc(u8, size);
    defer testing.allocator.free(data);
    @memset(data, 4);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    try forgetFile(path, size);
    const cold = try fileResidency(path, size);
    try readWhole(path, data, .{ .mode = .uring, .threads = 2, .block = 64 << 10, .direct = false });
    const warm = try fileResidency(path, size);
    // This read was buffered on purpose. Auto on a cold file is O_DIRECT and would have left it cold.
    const auto: Policy = .{};
    const first = auto.choose(cold, 1);
    if (cold < Policy.cold_resident) {
        try testing.expectEqual(Reason.cold_direct, first.reason);
        try testing.expect(first.directRead());
    }
    try testing.expect(warm >= Policy.warm_resident);
    try testing.expect(warm > cold);
    const second = auto.choose(warm, 1);
    try testing.expectEqual(Mode.mmap, second.mode);
    try testing.expectEqual(Reason.cached, second.reason);
}

test "row groups draw their bytes from the query's ledger as they are admitted, and give them back as released" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const size: usize = 1 << 20;
    const data = try testing.allocator.alloc(u8, size);
    defer testing.allocator.free(data);
    @memset(data, 7);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    for ([_]Mode{ .pread, .uring }) |mode| for ([_]bool{ false, true }) |abandon| {
        if (mode == .uring and !uringAvailable()) continue;
        // Room for one row group of four at a time: each waits on the one before it, and none is refused.
        var ledger: core_memory.SharedBudget = .{ .limit = size / 4 + 4096 };
        _ = ledger.used.fetchAdd(4096, .monotonic);
        const cfg: Config = .{ .mode = mode, .threads = 2, .window = 1 << 30, .ledger = &ledger };
        const rd = try Reader.create(testing.allocator, cfg);
        defer rd.destroy();
        const f = try rd.openFile(path, size);
        var store: [4][1]Range = undefined;
        var plans: [4]?[]Range = undefined;
        for (&plans, &store, 0..) |*p, *st, g| {
            st[0] = .{ .start = g * size / 4, .end = (g + 1) * size / 4 };
            p.* = st;
        }
        try rd.planFile(f, &plans, 4096);
        try rd.start();
        const gate = f.gate();
        for (0..4) |g| {
            try gate.wait(gate.ctx, g);
            try testing.expect(ledger.used.load(.monotonic) <= ledger.limit + size / 4);
            const lo = g * size / 4;
            try testing.expectEqualSlices(u8, data[lo..][0 .. size / 4], f.bytes()[lo..][0 .. size / 4]);
            if (abandon and g == 1) break;
            gate.done(gate.ctx, g, 1);
        }
        if (abandon) rd.abandon();
        try testing.expectEqual(@as(usize, 4096), ledger.used.load(.monotonic));
    };
}
