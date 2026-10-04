const std = @import("std");
const builtin = @import("builtin");

/// Discover available system memory in bytes, returning:
/// - 75% of AWS Lambda memory if running in a Lambda container.
/// - 80% of MemAvailable (from /proc/meminfo) or freeram on Linux.
/// - 50% of total physical memory (via sysctl) on macOS.
/// - 512 MB fallback on other platforms or on failure.
pub fn discoverAvailableMemory(env: ?std.process.Environ) usize {
    // 1. AWS Lambda Environment
    if (getLambdaMemory(env)) |mem| {
        return mem;
    }

    // 2. OS-specific discovery
    if (builtin.os.tag == .linux) {
        return linuxDiscover();
    } else if (builtin.os.tag == .macos) {
        return macosDiscover();
    }

    // Default fallback
    return 512 * 1024 * 1024;
}

/// Lambda meters CPU time at one vCPU per this many MB of memory.
pub const LAMBDA_MB_PER_VCPU: usize = 1769;

/// Effective usable CPU parallelism: the CPUs this process may run on (`getCpuCount` reads the affinity mask), capped
/// by the cgroup CPU quota rounded up.
///
/// Lambda shows neither the quota nor a tight mask: every tier up to 3008 MB has 2 CPUs in its mask (6 at 10240 MB),
/// and `/sys/fs/cgroup` is not mounted, yet CPU time is metered per memory tier. There the tier stands in for the
/// quota, rounded up like it: 3008 MB is ~1.7 vCPU of time, and rounding down left the write path one decode thread.
pub fn discoverAvailableParallelism(env: ?std.process.Environ) usize {
    const visible = std.Thread.getCpuCount() catch 1;
    return resolveParallelism(visible, cgroupCpuQuota(), lambdaMemoryMb(env));
}

/// `visible` CPUs capped by `quota_cpus` when a cgroup quota is set, else on Lambda by the memory tier's vCPUs.
pub fn resolveParallelism(visible: usize, quota_cpus: ?usize, lambda_mem_mb: ?usize) usize {
    const tier_cpus = if (lambda_mem_mb) |mb| std.math.divCeil(usize, mb, LAMBDA_MB_PER_VCPU) catch 1 else visible;
    const limit = quota_cpus orelse tier_cpus;
    return @max(1, @min(visible, limit));
}

fn lambdaMemoryMb(env: ?std.process.Environ) ?usize {
    const e = env orelse return null;
    const val_str = e.getPosix("AWS_LAMBDA_FUNCTION_MEMORY_SIZE") orelse return null;
    return std.fmt.parseInt(usize, val_str, 10) catch null;
}

/// CPUs granted by the tightest cgroup CPU quota over this process, or null when none is set or none is readable.
/// cgroup v2 limits nest, so the process's own cgroup and every ancestor count; for v1 only the `cpu` mount root is
/// read, which inside a container is the container's own group.
fn cgroupCpuQuota() ?usize {
    if (builtin.os.tag != .linux) return null;
    var buf: [4096]u8 = undefined;
    const self_cgroup = readSmallFile("/proc/self/cgroup", &buf) orelse return null;
    var lines = std.mem.tokenizeScalar(u8, self_cgroup, '\n');
    const v2_path = while (lines.next()) |line| {
        if (std.mem.startsWith(u8, line, "0::")) break line[3..];
    } else null;

    if (v2_path) |rel| {
        var best: ?usize = null;
        var dir = std.mem.trimEnd(u8, rel, "/");
        while (true) {
            var path_buf: [std.fs.max_path_bytes]u8 = undefined;
            const path = std.fmt.bufPrintSentinel(&path_buf, "/sys/fs/cgroup{s}/cpu.max", .{dir}, 0) catch break;
            var file_buf: [64]u8 = undefined;
            if (readSmallFile(path, &file_buf)) |text| {
                if (parseCpuMax(text)) |n| best = @min(best orelse n, n);
            }
            if (dir.len == 0) break;
            dir = dir[0 .. std.mem.lastIndexOfScalar(u8, dir, '/') orelse 0];
        }
        // A hybrid host lists a v2 group but may keep the cpu controller on v1.
        if (best) |n| return n;
    }

    var quota_buf: [32]u8 = undefined;
    var period_buf: [32]u8 = undefined;
    for ([_][2][:0]const u8{
        .{ "/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "/sys/fs/cgroup/cpu/cpu.cfs_period_us" },
        .{ "/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us", "/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us" },
    }) |files| {
        const quota = readSmallFile(files[0], &quota_buf) orelse continue;
        const period = readSmallFile(files[1], &period_buf) orelse continue;
        return parseCfsQuota(quota, period);
    }
    return null;
}

/// cgroup v2 `cpu.max` ("$QUOTA $PERIOD", quota `max` when unlimited) as whole CPUs, rounded up.
fn parseCpuMax(text: []const u8) ?usize {
    var it = std.mem.tokenizeAny(u8, text, " \n");
    const quota = it.next() orelse return null;
    if (std.mem.eql(u8, quota, "max")) return null;
    return parseCfsQuota(quota, it.next() orelse "100000");
}

/// cgroup v1 `cpu.cfs_quota_us` over `cpu.cfs_period_us` as whole CPUs, rounded up; a quota of -1 is unlimited.
fn parseCfsQuota(quota_text: []const u8, period_text: []const u8) ?usize {
    const quota = std.fmt.parseInt(i64, std.mem.trim(u8, quota_text, " \n"), 10) catch return null;
    const period = std.fmt.parseInt(i64, std.mem.trim(u8, period_text, " \n"), 10) catch return null;
    if (quota <= 0 or period <= 0) return null;
    return @max(1, @as(usize, @intCast(@divFloor(quota + period - 1, period))));
}

fn readSmallFile(path: [:0]const u8, buf: []u8) ?[]const u8 {
    const fd = std.posix.openatZ(std.posix.AT.FDCWD, path, .{ .ACCMODE = .RDONLY }, 0) catch return null;
    defer _ = std.c.close(fd);
    const n = std.posix.read(fd, buf) catch return null;
    return buf[0..n];
}

test "Lambda parallelism rounds the memory tier's vCPUs up, capped by the visible CPUs" {
    // The masks Lambda reports: 2 CPUs up to 3008 MB, 6 at 10240 MB.
    try std.testing.expectEqual(@as(usize, 1), resolveParallelism(2, null, 1024));
    try std.testing.expectEqual(@as(usize, 1), resolveParallelism(2, null, 1769));
    try std.testing.expectEqual(@as(usize, 2), resolveParallelism(2, null, 3008));
    try std.testing.expectEqual(@as(usize, 6), resolveParallelism(6, null, 10240));
    // The tier never grants more than the mask shows.
    try std.testing.expectEqual(@as(usize, 2), resolveParallelism(2, null, 10240));
    try std.testing.expectEqual(@as(usize, 1), resolveParallelism(1, null, 0));
}

test "a cgroup quota caps parallelism and takes precedence over the Lambda tier" {
    try std.testing.expectEqual(@as(usize, 2), resolveParallelism(8, 2, null));
    try std.testing.expectEqual(@as(usize, 4), resolveParallelism(4, 16, null));
    try std.testing.expectEqual(@as(usize, 1), resolveParallelism(2, 1, 3008));
    try std.testing.expectEqual(@as(usize, 2), resolveParallelism(2, 2, 1024));
    // No quota and not on Lambda: every visible CPU.
    try std.testing.expectEqual(@as(usize, 8), resolveParallelism(8, null, null));
}

test "discovered parallelism is at least one and never above the visible CPUs" {
    const n = discoverAvailableParallelism(null);
    try std.testing.expect(n >= 1);
    try std.testing.expect(n <= (std.Thread.getCpuCount() catch 1));
}

test "cgroup quota files parse to whole CPUs, rounded up" {
    try std.testing.expectEqual(@as(?usize, 2), parseCpuMax("150000 100000\n"));
    try std.testing.expectEqual(@as(?usize, 1), parseCpuMax("50000 100000\n"));
    try std.testing.expectEqual(@as(?usize, 4), parseCpuMax("400000 100000"));
    try std.testing.expectEqual(@as(?usize, null), parseCpuMax("max 100000\n"));
    try std.testing.expectEqual(@as(?usize, null), parseCpuMax(""));
    try std.testing.expectEqual(@as(?usize, 3), parseCfsQuota("250000\n", "100000\n"));
    try std.testing.expectEqual(@as(?usize, null), parseCfsQuota("-1\n", "100000\n"));
    try std.testing.expectEqual(@as(?usize, null), parseCfsQuota("garbage", "100000"));
}

/// True when running inside an AWS Lambda container (memory tier is known and
/// the whole container is ours). Callers use this to size in-flight memory
/// generously from the owned tier, vs. staying polite on a shared machine.
pub fn onLambda(env: ?std.process.Environ) bool {
    const e = env orelse return false;
    return e.getPosix("AWS_LAMBDA_FUNCTION_MEMORY_SIZE") != null;
}

/// Maximum jobs admitted to a byte-buffering pipeline at once. The byte
/// budget is authoritative: when even one average job exceeds it we still
/// admit one so the pipeline can make progress, but requested worker
/// parallelism must not raise this cap.
pub fn windowCapacity(inflight_budget: u64, avg_job_bytes: u64, job_count: usize) usize {
    if (job_count == 0) return 0;
    const slots = @max(@as(u64, 1), inflight_budget / @max(@as(u64, 1), avg_job_bytes));
    return @intCast(@min(slots, @as(u64, @intCast(job_count))));
}

fn getLambdaMemory(env: ?std.process.Environ) ?usize {
    const e = env orelse return null;
    if (e.getPosix("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")) |val_str| {
        if (std.fmt.parseInt(usize, val_str, 10) catch null) |val| {
            return val * 1024 * 1024 * 3 / 4; // 75% of memory tier
        }
    }
    return null;
}

test "window capacity keeps byte budget authoritative over worker count" {
    const mb: u64 = 1024 * 1024;

    // A high requested -j must not turn a 256 MiB budget into an 8 GiB
    // window when average row groups are 128 MiB.
    try std.testing.expectEqual(@as(usize, 2), windowCapacity(256 * mb, 128 * mb, 100));
    // Always admit one oversized row group so the pipeline makes progress.
    try std.testing.expectEqual(@as(usize, 1), windowCapacity(64 * mb, 128 * mb, 100));
    // Never create more slots than there are jobs.
    try std.testing.expectEqual(@as(usize, 3), windowCapacity(256 * mb, 1 * mb, 3));
    try std.testing.expectEqual(@as(usize, 0), windowCapacity(256 * mb, 1 * mb, 0));
}

fn linuxDiscover() usize {
    if (readLinuxMeminfo()) |mem| {
        return mem * 8 / 10; // 80% of MemAvailable
    }
    if (queryLinuxSysinfo()) |mem| {
        return mem * 8 / 10; // 80% of free ram
    }
    return 512 * 1024 * 1024;
}

fn readLinuxMeminfo() ?usize {
    const fd = std.posix.openatZ(std.posix.AT.FDCWD, "/proc/meminfo", .{ .ACCMODE = .RDONLY }, 0) catch return null;
    defer _ = std.c.close(fd);
    var buf: [2048]u8 = undefined;
    const bytes = std.posix.read(fd, &buf) catch return null;
    var it = std.mem.tokenizeAny(u8, buf[0..bytes], "\n");
    while (it.next()) |line| {
        if (std.mem.startsWith(u8, line, "MemAvailable:")) {
            var parts = std.mem.tokenizeAny(u8, line["MemAvailable:".len..], " \t");
            const num_str = parts.next() orelse continue;
            const val = std.fmt.parseInt(usize, num_str, 10) catch continue;
            return val * 1024; // Convert kB to bytes
        }
    }
    return null;
}

fn queryLinuxSysinfo() ?usize {
    const linux = struct {
        const sysinfo_t = extern struct {
            uptime: c_long,
            loads: [3]c_ulong,
            totalram: c_ulong,
            freeram: c_ulong,
            sharedram: c_ulong,
            bufferram: c_ulong,
            totalswap: c_ulong,
            freeswap: c_ulong,
            procs: u16,
            pad: u16,
            totalhigh: c_ulong,
            freehigh: c_ulong,
            mem_unit: u32,
            _f: [20 - 2 * @sizeOf(c_ulong) - @sizeOf(u32)]u8,
        };
        extern fn sysinfo(info: *sysinfo_t) callconv(.c) c_int;
    };

    var info: linux.sysinfo_t = undefined;
    if (linux.sysinfo(&info) == 0) {
        return @as(usize, @intCast(info.freeram)) * @as(usize, @intCast(info.mem_unit));
    }
    return null;
}

fn macosDiscover() usize {
    const CTL_HW = 6;
    const HW_MEMSIZE = 24;
    var memsize: u64 = 0;
    var len: usize = @sizeOf(u64);
    var mib = [_]c_int{ CTL_HW, HW_MEMSIZE };
    const sysctl = struct {
        extern fn sysctl(name: [*]const c_int, namelen: c_uint, oldp: ?*anyopaque, oldlenp: ?*usize, newp: ?*anyopaque, newlen: usize) callconv(.c) c_int;
    }.sysctl;

    if (sysctl(&mib, 2, &memsize, &len, null, 0) == 0) {
        return @intCast(memsize * 5 / 10); // 50% of total physical memory
    }
    return 512 * 1024 * 1024;
}

pub fn parseSizeString(s: []const u8) !usize {
    if (s.len == 0) return error.InvalidSize;

    var end_num: usize = 0;
    while (end_num < s.len) : (end_num += 1) {
        const c = s[end_num];
        if ((c >= '0' and c <= '9') or c == '.') {
            continue;
        }
        break;
    }

    if (end_num == 0) return error.InvalidSize;

    const num_str = s[0..end_num];
    const suffix = s[end_num..];

    const val_float = std.fmt.parseFloat(f64, num_str) catch return error.InvalidSize;

    var multiplier: f64 = 1.0;
    if (suffix.len > 0) {
        if (std.ascii.eqlIgnoreCase(suffix, "b")) {
            multiplier = 1.0;
        } else if (std.ascii.eqlIgnoreCase(suffix, "kb") or std.ascii.eqlIgnoreCase(suffix, "k")) {
            multiplier = 1024.0;
        } else if (std.ascii.eqlIgnoreCase(suffix, "mb") or std.ascii.eqlIgnoreCase(suffix, "m")) {
            multiplier = 1024.0 * 1024.0;
        } else if (std.ascii.eqlIgnoreCase(suffix, "gb") or std.ascii.eqlIgnoreCase(suffix, "g")) {
            multiplier = 1024.0 * 1024.0 * 1024.0;
        } else if (std.ascii.eqlIgnoreCase(suffix, "tb") or std.ascii.eqlIgnoreCase(suffix, "t")) {
            multiplier = 1024.0 * 1024.0 * 1024.0 * 1024.0;
        } else {
            return error.InvalidSize;
        }
    }

    const result = val_float * multiplier;
    if (result < 0 or result > @as(f64, @floatFromInt(std.math.maxInt(usize)))) {
        return error.InvalidSize;
    }
    return @intFromFloat(result);
}
