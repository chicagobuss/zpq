//! Process-lifetime cache topology, discovered once. CPUID describes the executing x86 CPU; Linux sysfs fills in
//! other logical CPUs and the sharing sets, without assuming a uniform topology or changing thread affinity.
//! Unknown sizes stay zero and unknown sharing sets stay empty. Execution policy, headroom and forced paths belong
//! to callers. See docs/hardware-execution.md for the process-wide environment overrides.
//! Hardware may change performance only: never row-group boundaries, page sizes, float summation blocks, merge
//! order or anything else that reaches output bytes or float bits.

const std = @import("std");
const builtin = @import("builtin");

/// Linux's fixed affinity mask capacity. An unrepresentable CPU is reported as unknown, never aliased to CPU 0.
pub const cpu_capacity = std.os.linux.CPU_SETSIZE * 8;
pub const CpuSet = std.StaticBitSet(cpu_capacity);
pub const Level = enum { l1d, l2, l3 };
pub const Cache = struct {
    bytes: usize = 0,
    line_bytes: usize = 0,
    /// Logical CPUs that can share this cache, including SMT siblings. Empty when the OS provides no mapping.
    cpus: CpuSet = .empty,
    /// CPUID's maximum sharing count when a concrete sharing set is unavailable.
    max_sharing: usize = 0,

    pub fn bytesPerWorker(self: Cache) usize {
        return self.bytes / @max(1, if (self.cpus.count() != 0) self.cpus.count() else self.max_sharing);
    }
};
pub const Cpu = struct {
    l1d: Cache = .{},
    l2: Cache = .{},
    l3: Cache = .{},

    pub fn cache(self: *const Cpu, level: Level) *const Cache {
        return switch (level) {
            .l1d => &self.l1d,
            .l2 => &self.l2,
            .l3 => &self.l3,
        };
    }
};

/// A calling thread's test fixture; never changes the discovered host or another query's fixture.
pub threadlocal var test_cpu: ?Cpu = null;
var state: std.atomic.Value(u8) = .init(0);
var detected: [cpu_capacity]Cpu = @splat(.{});
var process_l1d: usize = 0;
var process_line: usize = 0;
var process_l2: usize = 0;
var process_l3: usize = 0;
const unknown: Cpu = .{};

/// Diagnostics only; execution policies use the stable process-affinity minimum below.
pub fn forCpu(id: usize) *const Cpu {
    while (true) switch (state.load(.acquire)) {
        2 => return if (id < detected.len) &detected[id] else &unknown,
        1 => std.atomic.spinLoopHint(),
        else => if (state.cmpxchgStrong(0, 1, .acq_rel, .acquire) == null) {
            discover();
            state.store(2, .release);
        },
    };
}

/// Diagnostics only: the CPU on which this caller happens to execute can change between calls.
pub fn current() *const Cpu {
    if (test_cpu) |*cpu| return cpu;
    return forCpu(currentId());
}

/// Stable L1 share and widest cache line in the captured affinity; zero L1 means unknown.
pub fn l1dBytesPerWorker() usize {
    if (test_cpu) |cpu| return cpu.l1d.bytesPerWorker();
    _ = forCpu(0);
    return process_l1d;
}
pub fn cacheLineBytes() usize {
    if (test_cpu) |cpu| return if (cpu.l1d.line_bytes != 0) cpu.l1d.line_bytes else std.atomic.cache_line;
    _ = forCpu(0);
    return if (process_line != 0) process_line else std.atomic.cache_line;
}

/// Minimum per-worker L2 across the process affinity mask, captured once at discovery. Stable on hybrid CPUs and
/// conservative about idle SMT siblings. Zero means unknown (including any unknown allowed CPU). Hardware may
/// change performance only, never output bytes, page/row-group boundaries, float summation blocks or merge order.
pub fn l2BytesPerWorker() usize {
    if (test_cpu) |cpu| return cpu.l2.bytesPerWorker();
    _ = forCpu(0);
    return process_l2;
}

/// An explicit execution-policy fallback for platforms without cache/affinity discovery, including macOS arm64.
pub fn l2BytesPerWorkerOr(default: usize) usize {
    const bytes = l2BytesPerWorker();
    return if (bytes == 0) default else bytes;
}

/// Minimum whole L3 across the captured process affinity, divided by the query's active workers. This assumes all
/// workers compete in the smallest allowed L3 domain, conservatively ignoring any benefit from spreading domains.
/// Unknown stays zero; scheduling cannot change this hint. Like L2 budgets, it may affect performance only.
pub fn l3BytesPerActiveWorker(workers: usize) usize {
    if (test_cpu) |cpu| return cpu.l3.bytes / @max(1, workers);
    _ = forCpu(0);
    return process_l3 / @max(1, workers);
}

fn affinityL3Minimum(cpus: []const Cpu, affinity: CpuSet) usize {
    var minimum: usize = std.math.maxInt(usize);
    var it = affinity.iterator(.{});
    while (it.next()) |id| {
        if (id >= cpus.len) return 0;
        minimum = @min(minimum, cpus[id].l3.bytes);
    }
    return if (minimum == std.math.maxInt(usize)) 0 else minimum;
}

fn affinityMinimum(cpus: []const Cpu, affinity: CpuSet) usize {
    var minimum: usize = std.math.maxInt(usize);
    var it = affinity.iterator(.{});
    while (it.next()) |id| {
        if (id >= cpus.len) return 0;
        minimum = @min(minimum, cpus[id].l2.bytesPerWorker());
    }
    return if (minimum == std.math.maxInt(usize)) 0 else minimum;
}

fn currentId() usize {
    if (builtin.os.tag == .linux) {
        var id: usize = 0;
        if (std.os.linux.getcpu(&id, null) == 0) return id;
        return cpu_capacity;
    }
    return 0;
}

fn discover() void {
    const id = currentId();
    if (id < detected.len) detected[id] = fromCpuid();
    if (builtin.os.tag == .linux) {
        var buf: [4096]u8 = undefined;
        const possible = readFile("/sys/devices/system/cpu/online", &buf);
        const cpus = if (possible) |text| parseCpuList(text) else null;
        if (cpus) |set| scanSysfs(set, &detected, id, true);
    }
    // Sizes are independent of the host, so tests can run a different cache budget on the same machine. Sharing
    // remains the actual topology; a test_cpu fixture can replace that too without process-global mutation.
    inline for (std.meta.tags(Level)) |level| {
        const name = switch (level) {
            .l1d => "ZPQ_HW_L1D_BYTES",
            .l2 => "ZPQ_HW_L2_BYTES",
            .l3 => "ZPQ_HW_L3_BYTES",
        };
        if (std.c.getenv(name)) |raw| if (parseSize(std.mem.span(raw))) |bytes| {
            for (&detected) |*cpu| switch (level) {
                .l1d => cpu.l1d.bytes = bytes,
                .l2 => cpu.l2.bytes = bytes,
                .l3 => cpu.l3.bytes = bytes,
            };
        };
    }
    if (builtin.os.tag == .linux) {
        var mask: std.os.linux.cpu_set_t = undefined;
        // Query the main thread's mask, not an initializing worker's possibly narrower mask.
        if (std.os.linux.sched_getaffinity(std.os.linux.getpid(), @sizeOf(@TypeOf(mask)), &mask) == 0) {
            const affinity: CpuSet = .{ .masks = mask };
            process_l1d = std.math.maxInt(usize);
            var it = affinity.iterator(.{});
            while (it.next()) |cpu_id| {
                process_l1d = @min(process_l1d, detected[cpu_id].l1d.bytesPerWorker());
                process_line = @max(process_line, detected[cpu_id].l1d.line_bytes);
            }
            if (process_l1d == std.math.maxInt(usize)) process_l1d = 0;
            process_l2 = affinityMinimum(&detected, .{ .masks = mask });
            process_l3 = affinityL3Minimum(&detected, .{ .masks = mask });
        }
    }
}

fn readFile(path: [:0]const u8, buf: []u8) ?[]const u8 {
    if (builtin.os.tag != .linux) return null;
    const fd = std.posix.openat(std.posix.AT.FDCWD, path, .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0) catch
        return null;
    defer _ = std.posix.system.close(fd);
    const n = std.posix.read(fd, buf) catch return null;
    return if (n == buf.len) null else std.mem.trim(u8, buf[0..n], " \t\r\n");
}

/// Fill `out` for every CPU in `online` from sysfs, over whatever CPUID already put in `out[seeded]`. With `share`, a
/// CPU that an earlier one's L1d, L2 and L3 (where it has one) all list as a sharer takes that CPU's record: it sits
/// behind those same caches, so sysfs would describe them identically. That is an SMT sibling, and it halves the
/// files a process opens (about 4 us each, serial, before the first query plans). The CPUID-seeded CPU lends no
/// record, and keeps what `readSysfs` would have kept of its own: its sizes and CPUID sharing counts.
fn scanSysfs(online: CpuSet, out: []Cpu, seeded: usize, share: bool) void {
    var donors: CpuSet = .empty;
    var it = online.iterator(.{});
    next: while (it.next()) |cpu| {
        if (cpu >= out.len) continue;
        if (share) {
            var prior = donors.iterator(.{});
            while (prior.next()) |d| if (sharesAll(&out[d], cpu)) {
                const own = out[cpu];
                out[cpu] = out[d];
                inline for (comptime std.meta.tags(Level)) |level| {
                    const mine = own.cache(level);
                    const got: *Cache = switch (level) {
                        .l1d => &out[cpu].l1d,
                        .l2 => &out[cpu].l2,
                        .l3 => &out[cpu].l3,
                    };
                    if (mine.bytes != 0) got.bytes = mine.bytes;
                    got.max_sharing = mine.max_sharing;
                }
                continue :next;
            };
        }
        readSysfs(cpu, &out[cpu]);
        if (cpu != seeded) donors.set(cpu);
    }
}

/// Whether `cpu` shares every cache `owner` has: L1d and L2 always, L3 where `owner` has one.
fn sharesAll(owner: *const Cpu, cpu: usize) bool {
    if (!owner.l1d.cpus.isSet(cpu) or !owner.l2.cpus.isSet(cpu)) return false;
    const no_l3 = owner.l3.bytes == 0 and owner.l3.cpus.count() == 0;
    return no_l3 or owner.l3.cpus.isSet(cpu);
}

fn readSysfs(cpu: usize, out: *Cpu) void {
    var path: [192]u8 = undefined;
    var buf: [4096]u8 = undefined;
    var index: usize = 0;
    while (true) : (index += 1) {
        const prefix = "/sys/devices/system/cpu/cpu{d}/cache/index{d}/{s}";
        const type_path = std.fmt.bufPrintSentinel(&path, prefix, .{ cpu, index, "type" }, 0) catch return;
        const kind = readFile(type_path, &buf) orelse break;
        if (!std.mem.eql(u8, kind, "Data") and !std.mem.eql(u8, kind, "Unified")) continue;
        const level_path = std.fmt.bufPrintSentinel(&path, prefix, .{ cpu, index, "level" }, 0) catch return;
        const num = std.fmt.parseInt(u8, readFile(level_path, &buf) orelse continue, 10) catch continue;
        const cache: *Cache = switch (num) {
            1 => &out.l1d,
            2 => &out.l2,
            3 => &out.l3,
            else => continue,
        };
        const size_path = std.fmt.bufPrintSentinel(&path, prefix, .{ cpu, index, "size" }, 0) catch return;
        if (cache.bytes == 0) {
            if (readFile(size_path, &buf)) |text| cache.bytes = parseSize(text) orelse 0;
        }
        const line_path = std.fmt.bufPrintSentinel(
            &path,
            prefix,
            .{ cpu, index, "coherency_line_size" },
            0,
        ) catch return;
        if (readFile(line_path, &buf)) |text| cache.line_bytes = std.fmt.parseInt(usize, text, 10) catch 0;
        const share_path = std.fmt.bufPrintSentinel(&path, prefix, .{ cpu, index, "shared_cpu_list" }, 0) catch return;
        if (readFile(share_path, &buf)) |text| cache.cpus = parseCpuList(text) orelse .empty;
    }
}

fn parseSize(text: []const u8) ?usize {
    if (text.len == 0) return null;
    const scale: usize = switch (text[text.len - 1]) {
        'K' => 1024,
        'M' => 1024 * 1024,
        else => 1,
    };
    const digits = if (scale == 1) text else text[0 .. text.len - 1];
    const n = std.fmt.parseInt(usize, digits, 10) catch return null;
    return std.math.mul(usize, n, scale) catch null;
}

fn parseCpuList(text: []const u8) ?CpuSet {
    var out: CpuSet = .empty;
    var parts = std.mem.splitScalar(u8, std.mem.trim(u8, text, " \t\r\n"), ',');
    while (parts.next()) |part| {
        var ends = std.mem.splitScalar(u8, part, '-');
        const first = std.fmt.parseInt(usize, ends.next() orelse return null, 10) catch return null;
        const last = if (ends.next()) |s| std.fmt.parseInt(usize, s, 10) catch return null else first;
        if (ends.next() != null or last < first or last >= cpu_capacity) return null;
        out.setRangeValue(.{ .start = first, .end = last + 1 }, true);
    }
    return out;
}

fn cpuid(leaf: u32, sub: u32) [4]u32 {
    if (builtin.cpu.arch != .x86 and builtin.cpu.arch != .x86_64) return @splat(0);
    var eax: u32 = undefined;
    var ebx: u32 = undefined;
    var ecx: u32 = undefined;
    var edx: u32 = undefined;
    asm volatile ("cpuid"
        : [_] "={eax}" (eax),
          [_] "={ebx}" (ebx),
          [_] "={ecx}" (ecx),
          [_] "={edx}" (edx),
        : [_] "{eax}" (leaf),
          [_] "{ecx}" (sub),
    );
    return .{ eax, ebx, ecx, edx };
}

fn fromCpuid() Cpu {
    var out: Cpu = .{};
    const leaf: u32 = if (cpuid(0x80000000, 0)[0] >= 0x8000001d) 0x8000001d else if (cpuid(0, 0)[0] >= 4) 4 else return out;
    var sub: u32 = 0;
    while (sub < 32) : (sub += 1) {
        const regs = cpuid(leaf, sub);
        const kind = regs[0] & 31;
        if (kind == 0) break;
        if (kind != 1 and kind != 3) continue;
        const cache: *Cache = switch ((regs[0] >> 5) & 7) {
            1 => &out.l1d,
            2 => &out.l2,
            3 => &out.l3,
            else => continue,
        };
        cache.* = decodeLeaf(regs) orelse .{};
    }
    return out;
}

fn decodeLeaf(regs: [4]u32) ?Cache {
    var bytes = @as(usize, (regs[1] & 0xfff) + 1);
    bytes = std.math.mul(usize, bytes, ((regs[1] >> 12) & 0x3ff) + 1) catch return null;
    bytes = std.math.mul(usize, bytes, (regs[1] >> 22) + 1) catch return null;
    bytes = std.math.mul(usize, bytes, @as(usize, regs[2]) + 1) catch return null;
    return .{ .bytes = bytes, .line_bytes = (regs[1] & 0xfff) + 1, .max_sharing = ((regs[0] >> 14) & 0xfff) + 1 };
}

test "hardware cache fixtures preserve sizes, SMT sharing and noncontiguous CCD domains" {
    const l2 = decodeLeaf(.{ 3 | (2 << 5) | (1 << 14), 63 | (7 << 22), 2047, 0 }).?;
    try std.testing.expectEqual(@as(usize, 1 << 20), l2.bytes);
    try std.testing.expectEqual(@as(usize, 64), l2.line_bytes);
    try std.testing.expectEqual(@as(usize, 512 << 10), l2.bytesPerWorker());
    try std.testing.expectEqual(@as(?usize, 1280 << 10), parseSize("1280K"));
    const domain = parseCpuList("0-7,16-23\n").?;
    try std.testing.expectEqual(@as(usize, 16), domain.count());
    try std.testing.expect(domain.isSet(23) and !domain.isSet(8));
    try std.testing.expect(parseCpuList("8-2") == null);
    try std.testing.expect(parseCpuList("0-1000000") == null);
    test_cpu = .{ .l2 = .{ .bytes = 1280 << 10, .cpus = parseCpuList("3,19").? } };
    defer test_cpu = null;
    try std.testing.expectEqual(@as(usize, 640 << 10), l2BytesPerWorker());
}

test "hardware process budget is the affinity minimum on hybrid CPUs, unknown included" {
    const cpus = [_]Cpu{
        .{ .l2 = .{ .bytes = 1280 << 10, .max_sharing = 2 } },
        .{ .l2 = .{ .bytes = 2 << 20, .max_sharing = 4 } },
        .{},
    };
    try std.testing.expectEqual(@as(usize, 512 << 10), affinityMinimum(&cpus, parseCpuList("0-1").?));
    try std.testing.expectEqual(@as(usize, 640 << 10), affinityMinimum(&cpus, parseCpuList("0").?));
    try std.testing.expectEqual(@as(usize, 0), affinityMinimum(&cpus, parseCpuList("0-2").?));
    test_cpu = .{};
    defer test_cpu = null;
    try std.testing.expectEqual(@as(usize, 0), l2BytesPerWorker());
    try std.testing.expectEqual(@as(usize, 12345), l2BytesPerWorkerOr(12345));
}

test "hardware active-worker L3 hints use a stable affinity minimum, including unknown domains" {
    const cpus = [_]Cpu{ .{ .l3 = .{ .bytes = 32 << 20 } }, .{ .l3 = .{ .bytes = 96 << 20 } }, .{} };
    try std.testing.expectEqual(@as(usize, 32 << 20), affinityL3Minimum(&cpus, parseCpuList("0-1").?));
    try std.testing.expectEqual(@as(usize, 96 << 20), affinityL3Minimum(&cpus, parseCpuList("1").?));
    try std.testing.expectEqual(@as(usize, 0), affinityL3Minimum(&cpus, parseCpuList("0-2").?));
    test_cpu = cpus[0];
    defer test_cpu = null;
    try std.testing.expectEqual(@as(usize, 32 << 20), l3BytesPerActiveWorker(1));
    try std.testing.expectEqual(@as(usize, 8 << 20), l3BytesPerActiveWorker(4));
    try std.testing.expectEqual(@as(usize, 2 << 20), l3BytesPerActiveWorker(16));
    test_cpu = .{};
    try std.testing.expectEqual(@as(usize, 0), l3BytesPerActiveWorker(4));
}

test "hardware discovery that shares a sibling's record finds what reading every CPU finds" {
    if (builtin.os.tag != .linux) return;
    var buf: [4096]u8 = undefined;
    const online = parseCpuList(readFile("/sys/devices/system/cpu/online", &buf) orelse return) orelse return;
    const full = try std.testing.allocator.alloc(Cpu, cpu_capacity);
    defer std.testing.allocator.free(full);
    const shared = try std.testing.allocator.alloc(Cpu, cpu_capacity);
    defer std.testing.allocator.free(shared);
    // Seed as discovery does, on the executing CPU and on another online one, so both roles of the seeded CPU are
    // covered: the first online CPU is a donor candidate, a later one a recipient.
    var seeds = online.iterator(.{});
    var tried: usize = 0;
    while (seeds.next()) |seed| : (tried += 1) {
        if (tried == 3) break;
        @memset(full, .{});
        @memset(shared, .{});
        full[seed] = fromCpuid();
        shared[seed] = full[seed];
        scanSysfs(online, full, seed, false);
        scanSysfs(online, shared, seed, true);
        for (full, shared, 0..) |want, got, cpu| std.testing.expect(std.meta.eql(want, got)) catch |err| {
            std.debug.print("cpu {d} (seed {d}) differs\n", .{ cpu, seed });
            return err;
        };
    }
}
