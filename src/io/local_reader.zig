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
//! Backends (`Mode`): `pread` threads, or one io_uring per reader thread reading with O_DIRECT. `Policy.choose` picks
//! one; `ZPQ_IO_PROFILE` overrides it.
const std = @import("std");
const builtin = @import("builtin");
const linux = std.os.linux;
const posix = std.posix;

pub const Mode = enum {
    /// The plain mapping, faulted in by the workers.
    mmap,
    /// `threads` reader threads, each a blocking pread(2) of one `block` at a time.
    pread,
    /// `threads` reader threads, each with its own io_uring keeping `qd` O_DIRECT reads of `block` in flight.
    uring,
};

pub const Config = struct {
    mode: Mode = .pread,
    threads: usize = 8,
    qd: u32 = 32,
    block: u64 = 1 << 20,
    /// Bytes read ahead of the workers and not yet released.
    window: u64 = 1 << 30,
    /// Test hook: the read of this unit fails as an I/O error would.
    fail_unit: ?usize = null,
    /// Test hook: the ring of reader thread `i` fails to set up when bit `i` is set, as seccomp or memlock can make it.
    fail_rings: u64 = 0,
};

/// How a query reads local files: the mode `ZPQ_IO_PROFILE` names (`mmap`, `pread`, `uring`), or with `auto` (the
/// default) the backend this host and these files suit.
pub const Policy = struct {
    profile: Profile = .auto,
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

    /// The mode for files of which `resident` (0..1) is in the page cache and `coverage` (0..1) the share a query
    /// plans to read. Mostly resident files read fastest as they are. Otherwise io_uring where the kernel allows it;
    /// failing that, pread for a narrow read, but the plain mapping for a wide one: on a full scan its readahead
    /// beats blocking reads, and beat advising the planned ranges MADV_WILLNEED ahead of the workers too.
    pub fn choose(self: Policy, resident: f64, coverage: f64) Mode {
        return switch (self.profile) {
            .mmap => .mmap,
            .pread => .pread,
            .uring => if (uringAvailable()) .uring else .pread,
            .auto => if (builtin.os.tag != .linux or resident >= 0.8)
                .mmap
            else if (uringAvailable())
                .uring
            else if (coverage >= wide_scan)
                .mmap
            else
                .pread,
        };
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
    var ring = linux.IoUring.init(2, 0) catch {
        uring_probe.store(2, .monotonic);
        return false;
    };
    ring.deinit();
    uring_probe.store(1, .monotonic);
    return true;
}

/// The share (0..1) of `bytes`' pages the page cache holds, by `mincore`; 0 when it can't tell. Past a few thousand
/// pages it samples evenly spaced windows: mincore costs a page-cache lookup per page, milliseconds over a file of
/// tens of thousands, which every local aggregate would pay before its first decode.
pub fn residentShare(gpa: std.mem.Allocator, bytes: []align(std.heap.page_size_min) const u8) f64 {
    if (builtin.os.tag != .linux or bytes.len == 0) return 0;
    const page = pageLen();
    const pages = (bytes.len + page - 1) / page;
    const windows: usize = 64;
    const window_pages: usize = 32;
    const sampled = if (pages <= windows * window_pages) pages else windows * window_pages;
    const vec = gpa.alloc(u8, sampled) catch return 0;
    defer gpa.free(vec);
    if (sampled == pages) {
        if (linux.errno(linux.mincore(@constCast(bytes.ptr), bytes.len, vec.ptr)) != .SUCCESS) return 0;
    } else {
        const stride = pages / windows;
        for (0..windows) |w| {
            const first = w * stride;
            const out = vec[w * window_pages ..][0..window_pages];
            const p: [*]u8 = @constCast(bytes.ptr + first * page);
            if (linux.errno(linux.mincore(p, window_pages * page, out.ptr)) != .SUCCESS) return 0;
        }
    }
    var n: usize = 0;
    for (vec) |v| n += v & 1;
    return @as(f64, @floatFromInt(n)) / @as(f64, @floatFromInt(sampled));
}

pub const Range = struct { start: u64, end: u64 };

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
    /// Opened O_DIRECT; false when the mode is pread or the file system refused it.
    direct: bool,
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

    pub fn bytes(f: *const File) []const u8 {
        return f.buf[0..@intCast(f.size)];
    }

    pub fn gate(f: *File) Gate {
        return .{ .ctx = f, .wait = gateWait, .done = gateDone };
    }

    fn gateWait(ctx: *anyopaque, rg: usize) anyerror!void {
        const f: *File = @ptrCast(@alignCast(ctx));
        if (rg >= f.rg_ready.len) return;
        const r = &f.rg_ready[rg];
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
        _ = rd.rel_gen.fetchAdd(1, .release);
        _ = linux.futex_3arg(&rd.rel_gen.raw, .{ .cmd = .WAKE, .private = true }, std.math.maxInt(i32));
    }
};

/// One read: `len` bytes at `off`. `charge` is the window bytes it accounts for (its row group's on the first unit),
/// and `ticket` its row group's turn to be admitted.
const Unit = struct { file: u32, rg: u32, off: u64, len: u32, charge: u64 = 0, ticket: u32 = 0 };

pub const Stats = struct {
    bytes: std.atomic.Value(u64) = .init(0),
    stall_ns: std.atomic.Value(u64) = .init(0),
    stalls: std.atomic.Value(u64) = .init(0),
    window_waits: std.atomic.Value(u64) = .init(0),
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
    rel_gen: std.atomic.Value(u32) = .init(0),
    next_unit: std.atomic.Value(usize) = .init(0),
    /// Row groups are admitted to the window one at a time, in plan order: the ticket of the next to go, and how many
    /// were handed out.
    admit_next: std.atomic.Value(u32) = .init(0),
    tickets: u32 = 0,
    stats: Stats = .{},
    /// O_DIRECT opens refused (tmpfs, some FUSE): those files read through the page cache instead.
    direct_refused: bool = false,
    /// Reader threads by the backend they actually got: a uring thread whose ring setup failed reads with pread.
    uring_threads: u32 = 0,
    pread_threads: u32 = 0,

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
        var direct = self.cfg.mode == .uring;
        var rc = linux.open(@ptrCast(&path_z), .{ .ACCMODE = .RDONLY, .CLOEXEC = true, .DIRECT = direct }, 0);
        if (direct and linux.errno(rc) == .INVAL) {
            direct = false;
            self.direct_refused = true;
            rc = linux.open(@ptrCast(&path_z), .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
        }
        if (linux.errno(rc) != .SUCCESS) return error.OpenFailed;
        const fd: i32 = @intCast(rc);
        errdefer _ = linux.close(fd);

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
        f.* = .{ .reader = self, .fd = fd, .size = size, .direct = direct, .buf = buf };
        try self.files.append(self.gpa, f);
        return f;
    }

    /// Declare a file's row groups: `ranges_per_rg[rg]` (null when pruned) must be read before the row group is
    /// scanned. They are sorted and coalesced here.
    pub fn planFile(self: *Reader, f: *File, ranges_per_rg: []const ?[]Range, gap: u64) !void {
        const n = ranges_per_rg.len;
        f.rg_ready = try self.gpa.alloc(std.atomic.Value(u32), n);
        f.rg_items_done = try self.gpa.alloc(std.atomic.Value(u32), n);
        f.rg_units_left = try self.gpa.alloc(std.atomic.Value(u32), n);
        f.rg_first = try self.gpa.alloc(u32, n + 1);
        f.rg_bytes = try self.gpa.alloc(u64, n);
        const file_idx: u32 = for (self.files.items, 0..) |x, i| {
            if (x == f) break @intCast(i);
        } else unreachable;
        for (ranges_per_rg, 0..) |opt, rg| {
            f.rg_first[rg] = @intCast(f.ranges.items.len);
            f.rg_items_done[rg] = .init(0);
            f.rg_bytes[rg] = 0;
            var units: u32 = 0;
            const first_unit = self.units.items.len;
            if (opt) |rs| {
                std.mem.sort(Range, rs, {}, struct {
                    fn lt(_: void, a: Range, b: Range) bool {
                        return a.start < b.start;
                    }
                }.lt);
                var cur: ?Range = null;
                for (rs) |r| {
                    const end = @min(r.end, f.size);
                    if (end <= r.start) continue;
                    // Whole pages: O_DIRECT needs them, and they make overlaps with other row groups exact.
                    const clipped: Range = .{
                        .start = std.mem.alignBackward(u64, r.start, pageLen()),
                        .end = std.mem.alignForward(u64, end, pageLen()),
                    };
                    if (cur) |*c| {
                        if (clipped.start <= c.end + gap) {
                            c.end = @max(c.end, clipped.end);
                            continue;
                        }
                        try f.ranges.append(self.gpa, c.*);
                    }
                    cur = clipped;
                }
                if (cur) |c| try f.ranges.append(self.gpa, c);
                for (f.ranges.items[f.rg_first[rg]..]) |r| {
                    var off = r.start;
                    const end = r.end;
                    while (off < end) {
                        const len = @min(self.cfg.block, end - off);
                        try self.units.append(self.gpa, .{
                            .file = file_idx,
                            .rg = @intCast(rg),
                            .off = off,
                            .len = @intCast(len),
                        });
                        f.rg_bytes[rg] += len;
                        units += 1;
                        off += len;
                    }
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
        f.rg_first[n] = @intCast(f.ranges.items.len);
        try self.planSegments(f);
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

    /// Joins the reader threads (abandoning reads not yet made), then unmaps and closes.
    pub fn destroy(self: *Reader) void {
        self.cancel.store(true, .release);
        _ = self.rel_gen.fetchAdd(1, .release);
        _ = linux.futex_3arg(&self.rel_gen.raw, .{ .cmd = .WAKE, .private = true }, std.math.maxInt(i32));
        for (self.threads.items) |t| t.join();
        for (self.files.items) |f| {
            posix.munmap(f.buf);
            _ = linux.close(f.fd);
            self.gpa.free(f.rg_ready);
            self.gpa.free(f.rg_items_done);
            self.gpa.free(f.rg_units_left);
            self.gpa.free(f.rg_first);
            self.gpa.free(f.rg_bytes);
            self.gpa.free(f.seg_refs);
            f.ranges.deinit(self.gpa);
            f.segs.deinit(self.gpa);
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
        if (f.rg_units_left[u.rg].fetchSub(1, .acq_rel) == 1) {
            f.rg_ready[u.rg].store(1, .release);
            _ = linux.futex_3arg(&f.rg_ready[u.rg].raw, .{ .cmd = .WAKE, .private = true }, std.math.maxInt(i32));
        }
    }

    /// A read failed: every row group still waiting fails its workers, and no more reads are made.
    fn failAll(self: *Reader) void {
        self.cancel.store(true, .release);
        for (self.files.items) |f| for (f.rg_ready) |*r| {
            if (r.load(.acquire) == 0) {
                r.store(2, .release);
                _ = linux.futex_3arg(&r.raw, .{ .cmd = .WAKE, .private = true }, std.math.maxInt(i32));
            }
        };
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
        const next = self.admit_next.load(.acquire);
        if (u.charge == 0) return next > u.ticket;
        if (next != u.ticket) return false;
        // Only this admitter adds to `submitted`, and only admitted row groups add to `released`.
        const out = self.submitted.load(.monotonic) -| self.released.load(.acquire);
        if (out + u.charge > self.cfg.window and out != 0) return false;
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

    pub const Backends = struct { uring_threads: u32, pread_threads: u32, direct_files: u32, buffered_files: u32 };

    /// What the reads actually went through, valid once `start` returns.
    pub fn backends(self: *const Reader) Backends {
        var direct: u32 = 0;
        for (self.files.items) |f| direct += @intFromBool(f.direct);
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
        const u = self.units.items[i];
        const f = self.files.items[u.file];
        try preadAll(f, u.off, @min(u.off + u.len, f.size));
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
        const units = self.units.items;
        var inflight: usize = 0;
        var cqes: [64]linux.io_uring_cqe = undefined;
        // A unit claimed off the shared cursor but not yet admitted by the window.
        var pending: ?usize = null;
        var drained = false;
        while (true) {
            const cancelled = self.cancel.load(.acquire);
            while (!cancelled and inflight < self.cfg.qd) {
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
                _ = try ring.read(idx, f.fd, .{ .buffer = f.buf[@intCast(u.off)..][0..u.len] }, u.off);
                inflight += 1;
            }
            if (inflight == 0) {
                if ((drained and pending == null) or cancelled) return;
                self.waitRelease();
                continue;
            }
            _ = try ring.submit_and_wait(1);
            const n = try ring.copy_cqes(&cqes, 0);
            for (cqes[0..n]) |c| {
                inflight -= 1;
                const u = units[@intCast(c.user_data)];
                const f = self.files.items[u.file];
                const want = @min(@as(u64, u.len), f.size - u.off);
                if (c.res < 0) return error.ReadFailed;
                // O_DIRECT reads the file's unaligned tail short; anything else short is finished with pread.
                const got: u64 = @intCast(c.res);
                if (got < want) try preadAll(f, u.off + got, u.off + want);
                self.unitDone(u);
            }
        }
    }
};

/// pread `[lo, hi)` of `f` into its buffer. With O_DIRECT the offset and length must be page multiples, which every
/// unit is but the last page of the file; a direct read of it returns what there is.
fn preadAll(f: *File, lo: u64, hi: u64) !void {
    var off = lo;
    while (off < hi) {
        const len = std.mem.alignForward(u64, hi - off, pageLen());
        const rc = linux.pread(f.fd, f.buf.ptr + off, @intCast(@min(len, f.buf.len - off)), @intCast(off));
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
    if (builtin.os.tag != .linux or !uringAvailable()) return error.SkipZigTest;
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

test "residentShare bounds sampled allocation and partial-page windows" {
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
    try testing.expectEqual(@as(f64, 1), residentShare(bounded.allocator(), mapping));
    bounded.reset();
    try testing.expectEqual(@as(f64, 1), residentShare(bounded.allocator(), mapping[0 .. 32 * page - 1]));
    bounded.reset();
    try testing.expectEqual(@as(f64, 0), residentShare(bounded.allocator(), mapping[0..0]));
}

test "auto never reads a wide scan with pread" {
    const auto: Policy = .{};
    // Cached: the mapping, whatever the share read.
    try testing.expectEqual(Mode.mmap, auto.choose(0.9, 1.0));
    const pinned: Policy = .{ .profile = .pread };
    try testing.expectEqual(Mode.pread, pinned.choose(0.0, 1.0));
    if (builtin.os.tag != .linux) return;
    const wide = auto.choose(0.0, 0.99);
    const narrow = auto.choose(0.0, 0.2);
    if (uringAvailable()) {
        try testing.expectEqual(Mode.uring, wide);
        try testing.expectEqual(Mode.uring, narrow);
    } else {
        try testing.expectEqual(Mode.mmap, wide);
        try testing.expectEqual(Mode.pread, narrow);
    }
}
