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

/// Effective usable CPU parallelism.
///
/// On AWS Lambda the vCPU allocation is proportional to the memory tier
/// (~1 full vCPU per 1769 MB) and is throttled far below the *host* core
/// count that `std.Thread.getCpuCount()` reports. Fanning work across host
/// cores there oversubscribes the throttle and slows things down — measured
/// as a real regression on the re-encode write path. So on Lambda we derive
/// cores from the memory tier; everywhere else we trust `getCpuCount()`.
pub fn discoverAvailableParallelism(env: ?std.process.Environ) usize {
    const host = std.Thread.getCpuCount() catch 1;
    if (env) |e| {
        if (e.getPosix("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")) |val_str| {
            if (std.fmt.parseInt(usize, val_str, 10) catch null) |mem_mb| {
                const vcpus = @max(@as(usize, 1), mem_mb / 1769);
                return @min(vcpus, host);
            }
        }
    }
    return host;
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
