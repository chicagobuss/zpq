//! Process-lifetime cache topology, discovered once. CPUID describes the executing x86 CPU; Linux sysfs fills in
//! other logical CPUs and the sharing sets, without assuming a uniform topology or changing thread affinity. macOS
//! describes its caches through sysctl, and aarch64 Linux without a sysfs line size through CTR_EL0.
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

/// An explicit execution-policy fallback for hosts whose L2 could not be sized, such as a Linux VM whose sysfs
/// lists caches without a `size`.
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

/// A per-worker L2 share typical of the target architecture, for a policy that needs a size when discovery found
/// none: a 1.25 MiB L2 shared by two SMT siblings on x86, and the 1 MiB per core of Neoverse servers and Apple
/// efficiency clusters on aarch64. It is a performance assumption only, like every other value here.
pub const default_l2_bytes_per_worker: usize = switch (builtin.cpu.arch) {
    .x86, .x86_64 => 640 << 10,
    .aarch64, .aarch64_be => 1 << 20,
    else => 256 << 10,
};

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
    // The CPUs execution may use. Linux reads its affinity mask below; macOS has none, so every logical CPU.
    var affinity: ?CpuSet = null;
    if (builtin.os.tag == .linux) {
        var buf: [4096]u8 = undefined;
        const possible = readFile("/sys/devices/system/cpu/online", &buf);
        const cpus = if (possible) |text| parseCpuList(text) else null;
        if (cpus) |set| {
            scanSysfs(set, &detected, id, true);
            if (comptime builtin.cpu.arch == .aarch64) fillLineBytes(set, &detected, ctrLineBytes(ctrEl0()));
        }
    }
    if (comptime builtin.os.tag.isDarwin()) {
        const sysctl: Sysctl = .{};
        const cpu = fromSysctl(sysctl, detected[id]);
        const count = @min(cpu_capacity, sysctl.int("hw.logicalcpu") orelse 1);
        @memset(detected[0..count], cpu);
        var all: CpuSet = .empty;
        all.setRangeValue(.{ .start = 0, .end = count }, true);
        affinity = all;
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
        if (std.os.linux.sched_getaffinity(std.os.linux.getpid(), @sizeOf(@TypeOf(mask)), &mask) == 0)
            affinity = .{ .masks = mask };
    }
    if (affinity) |allowed| {
        process_l1d = std.math.maxInt(usize);
        var it = allowed.iterator(.{});
        while (it.next()) |cpu_id| {
            process_l1d = @min(process_l1d, detected[cpu_id].l1d.bytesPerWorker());
            process_line = @max(process_line, detected[cpu_id].l1d.line_bytes);
        }
        if (process_l1d == std.math.maxInt(usize)) process_l1d = 0;
        process_l2 = affinityMinimum(&detected, allowed);
        process_l3 = affinityL3Minimum(&detected, allowed);
    }
}

/// Give every CPU in `online` without a discovered L1d line `line` bytes: aarch64 Linux takes it from CTR_EL0
/// when sysfs has no `coherency_line_size`, as some VMs' do not.
fn fillLineBytes(online: CpuSet, out: []Cpu, line: usize) void {
    var it = online.iterator(.{});
    while (it.next()) |cpu| {
        if (cpu >= out.len) continue;
        if (out[cpu].l1d.line_bytes == 0) out[cpu].l1d.line_bytes = line;
    }
}

/// The smallest data-cache line in the hierarchy, from CTR_EL0.DminLine: log2 of its length in 4-byte words.
fn ctrLineBytes(ctr: u64) usize {
    return @as(usize, 4) << @intCast((ctr >> 16) & 0xf);
}

fn ctrEl0() u64 {
    if (comptime builtin.cpu.arch != .aarch64) return 0;
    // Linux lets EL0 read CTR_EL0, trapping and emulating it where an erratum requires.
    return asm volatile ("mrs %[ctr], ctr_el0"
        : [ctr] "=r" (-> u64),
    );
}

/// macOS cache sizes by sysctl name. Integer values are 4 or 8 bytes depending on the name.
const Sysctl = struct {
    fn int(_: Sysctl, name: [:0]const u8) ?usize {
        var value: [8]u8 align(8) = @splat(0);
        var len: usize = value.len;
        if (std.c.sysctlbyname(name, &value, &len, null, 0) != 0) return null;
        return switch (len) {
            4 => std.mem.readInt(u32, value[0..4], builtin.cpu.arch.endian()),
            8 => std.math.cast(usize, std.mem.readInt(u64, &value, builtin.cpu.arch.endian())),
            else => null,
        };
    }

    /// `hw.cacheconfig`: how many logical CPUs share memory, then each cache level from L1 on.
    fn sharing(_: Sysctl, level: usize) ?usize {
        var config: [16]u64 = @splat(0);
        var len: usize = @sizeOf(@TypeOf(config));
        if (std.c.sysctlbyname("hw.cacheconfig", &config, &len, null, 0) != 0) return null;
        return if (level < len / @sizeOf(u64) and config[level] != 0) std.math.cast(usize, config[level]) else null;
    }
};

/// The cache record macOS describes, over what CPUID found. A hybrid Apple chip reports each performance level,
/// whose L2 is shared by `cpusperl2` of its cores. Darwin maps no CPU to a level and has no affinity, so any worker
/// may run on any level: each cache is the smallest per-worker share across levels, as the affinity minimum is
/// elsewhere. Otherwise, and on Intel Macs, `hw.cacheconfig` gives the sharing. Apple cores run one thread each, so
/// an unreported L1d sharing is one CPU.
fn fromSysctl(sysctl: anytype, seed: Cpu) Cpu {
    var out = seed;
    const line = sysctl.int("hw.cachelinesize") orelse 0;
    var l1d: Cache = .{};
    var l2: Cache = .{};
    for (0..sysctl.int("hw.nperflevels") orelse 0) |level| {
        var name: [64]u8 = undefined;
        const prefix = "hw.perflevel{d}.{s}";
        const size = std.fmt.bufPrintSentinel(&name, prefix, .{ level, "l2cachesize" }, 0) catch break;
        const bytes = sysctl.int(size) orelse continue;
        const sharing = std.fmt.bufPrintSentinel(&name, prefix, .{ level, "cpusperl2" }, 0) catch break;
        const level_l2: Cache = .{ .bytes = bytes, .max_sharing = sysctl.int(sharing) orelse 1 };
        if (l2.bytes == 0 or level_l2.bytesPerWorker() < l2.bytesPerWorker()) l2 = level_l2;
        const l1d_size = std.fmt.bufPrintSentinel(&name, prefix, .{ level, "l1dcachesize" }, 0) catch break;
        if (sysctl.int(l1d_size)) |b| if (l1d.bytes == 0 or b < l1d.bytes) {
            l1d = .{ .bytes = b, .max_sharing = 1 };
        };
    }
    if (l2.bytes != 0) {
        if (l1d.bytes != 0) out.l1d = l1d;
        out.l2 = l2;
    } else {
        if (sysctl.int("hw.l1dcachesize")) |bytes| out.l1d = .{ .bytes = bytes, .max_sharing = sysctl.sharing(1) orelse 1 };
        if (sysctl.int("hw.l2cachesize")) |bytes| out.l2 = .{ .bytes = bytes, .max_sharing = sysctl.sharing(2) orelse 1 };
    }
    if (sysctl.int("hw.l3cachesize")) |l3| {
        if (l3 != 0) out.l3 = .{ .bytes = l3, .max_sharing = sysctl.sharing(3) orelse 1 };
    }
    for ([_]*Cache{ &out.l1d, &out.l2, &out.l3 }, [_]Cache{ seed.l1d, seed.l2, seed.l3 }) |cache, cpuid_cache| {
        if (cache.bytes == 0) cache.* = cpuid_cache;
        if (line != 0 and cache.bytes != 0) cache.line_bytes = line;
    }
    return out;
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

/// sysctl answers by name, for `fromSysctl`.
const SysctlFixture = struct {
    ints: []const struct { []const u8, usize },
    config: []const usize = &.{},

    fn int(self: SysctlFixture, name: []const u8) ?usize {
        for (self.ints) |entry| if (std.mem.eql(u8, entry[0], name)) return entry[1];
        return null;
    }
    fn sharing(self: SysctlFixture, level: usize) ?usize {
        return if (level < self.config.len and self.config[level] != 0) self.config[level] else null;
    }
};

test "hardware macOS sysctl takes the smallest Apple performance level and falls back to the whole-machine names" {
    // Apple M5 Pro: six "Super" cores share a 16 MiB L2, and two clusters of six "Performance" cores 8 MiB each.
    const m5: SysctlFixture = .{ .ints = &.{
        .{ "hw.cachelinesize", 128 },
        .{ "hw.l1dcachesize", 64 << 10 },
        .{ "hw.l2cachesize", 8 << 20 },
        .{ "hw.nperflevels", 2 },
        .{ "hw.perflevel0.l1dcachesize", 128 << 10 },
        .{ "hw.perflevel0.l2cachesize", 16 << 20 },
        .{ "hw.perflevel0.cpusperl2", 6 },
        .{ "hw.perflevel1.l1dcachesize", 64 << 10 },
        .{ "hw.perflevel1.l2cachesize", 8 << 20 },
        .{ "hw.perflevel1.cpusperl2", 6 },
    }, .config = &.{ 18, 1, 6 } };
    const apple = fromSysctl(m5, .{});
    try std.testing.expectEqual(@as(usize, 64 << 10), apple.l1d.bytesPerWorker());
    try std.testing.expectEqual(@as(usize, (8 << 20) / 6), apple.l2.bytesPerWorker());
    try std.testing.expectEqual(@as(usize, 128), apple.l1d.line_bytes);
    try std.testing.expectEqual(@as(usize, 0), apple.l3.bytes);
    const cpus = [_]Cpu{ apple, apple };
    try std.testing.expectEqual(@as(usize, (8 << 20) / 6), affinityMinimum(&cpus, parseCpuList("0-1").?));

    // An Intel Mac (i7-7700HQ): no performance levels; two SMT threads share L1d and L2, eight the L3.
    const intel: SysctlFixture = .{ .ints = &.{
        .{ "hw.cachelinesize", 64 },
        .{ "hw.l1dcachesize", 32 << 10 },
        .{ "hw.l2cachesize", 256 << 10 },
        .{ "hw.l3cachesize", 6 << 20 },
    }, .config = &.{ 8, 2, 2, 8 } };
    const x86 = fromSysctl(intel, .{});
    try std.testing.expectEqual(@as(usize, 16 << 10), x86.l1d.bytesPerWorker());
    try std.testing.expectEqual(@as(usize, 128 << 10), x86.l2.bytesPerWorker());
    try std.testing.expectEqual(@as(usize, 6 << 20), x86.l3.bytes);
    try std.testing.expectEqual(@as(usize, 64), x86.l2.line_bytes);

    // Without hw.cacheconfig a cache is one CPU's; a name sysctl lacks keeps what CPUID found.
    const sparse: SysctlFixture = .{ .ints = &.{.{ "hw.l2cachesize", 4 << 20 }} };
    const seed: Cpu = .{ .l1d = .{ .bytes = 48 << 10, .line_bytes = 64, .max_sharing = 2 } };
    const merged = fromSysctl(sparse, seed);
    try std.testing.expectEqual(@as(usize, 4 << 20), merged.l2.bytesPerWorker());
    try std.testing.expect(std.meta.eql(seed.l1d, merged.l1d));
    try std.testing.expect(std.meta.eql(seed, fromSysctl(SysctlFixture{ .ints = &.{} }, seed)));
}

test "hardware aarch64 line size comes from CTR_EL0 only where sysfs gave none" {
    try std.testing.expectEqual(@as(usize, 64), ctrLineBytes(0x8444c004)); // Neoverse N1
    try std.testing.expectEqual(@as(usize, 128), ctrLineBytes(5 << 16));
    var cpus = [_]Cpu{ .{ .l1d = .{ .bytes = 64 << 10, .line_bytes = 128 } }, .{}, .{} };
    fillLineBytes(parseCpuList("0-1").?, &cpus, 64);
    try std.testing.expectEqual(@as(usize, 128), cpus[0].l1d.line_bytes);
    try std.testing.expectEqual(@as(usize, 64), cpus[1].l1d.line_bytes);
    try std.testing.expectEqual(@as(usize, 0), cpus[2].l1d.line_bytes);
}

test "hardware discovery sizes L2 and the cache line on macOS and finds a line on aarch64 Linux" {
    const discovered = forCpu(0);
    if (comptime builtin.os.tag.isDarwin()) {
        if (std.c.getenv("ZPQ_HW_L2_BYTES") == null) try std.testing.expect(l2BytesPerWorker() != 0);
        try std.testing.expect(discovered.l2.bytes != 0);
        try std.testing.expectEqual(Sysctl.int(.{}, "hw.cachelinesize").?, cacheLineBytes());
    } else if (comptime builtin.os.tag == .linux and builtin.cpu.arch == .aarch64) {
        try std.testing.expect(process_line != 0);
        try std.testing.expect(std.math.isPowerOfTwo(process_line));
    } else {
        // x86 sizes come from CPUID and sysfs, which the sibling-sharing test above already reads.
        return error.SkipZigTest;
    }
}
