// probe_lambda_caps: enumerate what AWS Lambda actually allows us to do.
//
// One binary, two modes:
//   - CLI:    runs probes once, prints JSON to stdout, exits.
//   - Lambda: detects via AWS_LAMBDA_RUNTIME_API, polls runtime API, returns the
//             same JSON as the invocation response. Re-runs probes per invocation
//             so we capture both cold (first invoke) and warm (subsequent) state.
//
// Probes are non-fatal: each one captures its errno on failure and continues.
// Output schema: see docs/lambda_capabilities.md (schema_version 1).

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const linux = std.os.linux;

const SCHEMA_VERSION: u32 = 1;

// ============================================================
// Thin syscall wrappers — std.posix shed most of these in 0.16.0.
// We talk to linux.* directly and unwrap errno ourselves so the probe
// has zero dependencies beyond std + the kernel.
// ============================================================

const SysError = error{SyscallFailed};

fn sysClose(fd: linux.fd_t) void {
    _ = linux.close(fd);
}

fn sysWrite(fd: linux.fd_t, buf: []const u8) SysError!usize {
    const r = linux.write(fd, buf.ptr, buf.len);
    if (callOk(r) != null) return error.SyscallFailed;
    return @intCast(r);
}

fn sysRead(fd: linux.fd_t, buf: []u8) SysError!usize {
    const r = linux.read(fd, buf.ptr, buf.len);
    if (callOk(r) != null) return error.SyscallFailed;
    return @intCast(r);
}

fn sysSocket(domain: u32, sock_type: u32, protocol: u32) SysError!linux.fd_t {
    const r = linux.socket(domain, sock_type, protocol);
    if (callOk(r) != null) return error.SyscallFailed;
    return @intCast(@as(isize, @bitCast(r)));
}

fn sysConnect(fd: linux.fd_t, addr: *const anyopaque, len: linux.socklen_t) SysError!void {
    const r = linux.connect(fd, addr, len);
    if (callOk(r) != null) return error.SyscallFailed;
}

fn sysReadlink(path: [*:0]const u8, buf: []u8) SysError![]u8 {
    const r = linux.readlink(path, buf.ptr, buf.len);
    if (callOk(r) != null) return error.SyscallFailed;
    return buf[0..@intCast(r)];
}

fn fdFromUsize(r: usize) linux.fd_t {
    return @intCast(@as(isize, @bitCast(r)));
}

fn pathToZ(path: []const u8, buf: *[256]u8) ![*:0]const u8 {
    if (path.len + 1 > buf.len) return error.PathTooLong;
    @memcpy(buf[0..path.len], path);
    buf[path.len] = 0;
    return @ptrCast(&buf[0]);
}

const O_RDONLY: u32 = 0;
const O_WRONLY: u32 = 1;
const O_RDWR: u32 = 2;
const O_CREAT: u32 = 0o100;
const O_TRUNC: u32 = 0o1000;
const O_CLOEXEC: u32 = 0o2000000;

fn sysOpenRead(path: []const u8) SysError!linux.fd_t {
    var pbuf: [256]u8 = undefined;
    const z = pathToZ(path, &pbuf) catch return error.SyscallFailed;
    const r = linux.openat(linux.AT.FDCWD, z, .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (callOk(r) != null) return error.SyscallFailed;
    return @intCast(@as(isize, @bitCast(r)));
}

fn sysOpenCreate(path: []const u8) SysError!linux.fd_t {
    var pbuf: [256]u8 = undefined;
    const z = pathToZ(path, &pbuf) catch return error.SyscallFailed;
    const r = linux.openat(linux.AT.FDCWD, z, .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true, .CLOEXEC = true }, 0o644);
    if (callOk(r) != null) return error.SyscallFailed;
    return @intCast(@as(isize, @bitCast(r)));
}

fn sysUnlink(path: []const u8) void {
    var pbuf: [256]u8 = undefined;
    const z = pathToZ(path, &pbuf) catch return;
    _ = linux.unlinkat(linux.AT.FDCWD, z, 0);
}

fn sysFsync(fd: linux.fd_t) void {
    _ = linux.fsync(fd);
}

// std.time.timestamp/nanoTimestamp moved to std.Io.Clock in 0.16. For a probe
// binary we just call clock_gettime directly.
fn nowNs() i128 {
    var ts: linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + ts.nsec;
}

fn unixSeconds() i64 {
    var ts: linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = linux.clock_gettime(.REALTIME, &ts);
    return ts.sec;
}

pub fn main(init: std.process.Init.Minimal) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .{};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const env = init.environ;
    const runtime_api = env.getPosix("AWS_LAMBDA_RUNTIME_API");
    if (runtime_api) |api| {
        try runLambda(allocator, env, api);
    } else {
        try runCli(allocator, env);
    }
}

// ============================================================
// Mode dispatchers
// ============================================================

fn runCli(allocator: std.mem.Allocator, env: std.process.Environ) !void {
    const json = try collectAll(allocator, env);
    defer allocator.free(json);

    var off: usize = 0;
    while (off < json.len) {
        const n = sysWrite(linux.STDOUT_FILENO, json[off..]) catch break;
        if (n == 0) break;
        off += n;
    }
    _ = sysWrite(linux.STDOUT_FILENO, "\n") catch {};
}

fn runLambda(allocator: std.mem.Allocator, env: std.process.Environ, runtime_api: []const u8) !void {
    const colon = std.mem.indexOfScalar(u8, runtime_api, ':') orelse runtime_api.len;
    const host = runtime_api[0..colon];
    const port_str = if (colon < runtime_api.len) runtime_api[colon + 1 ..] else "80";
    const port = std.fmt.parseInt(u16, port_str, 10) catch 80;

    while (true) {
        const event = getNextInvocation(allocator, host, port) catch |err| {
            std.debug.print("probe_lambda_caps: poll error {any}\n", .{err});
            const ts: linux.timespec = .{ .sec = 1, .nsec = 0 };
            _ = linux.nanosleep(&ts, null);
            continue;
        };
        defer allocator.free(event.body);
        defer allocator.free(event.request_id);

        const json = collectAll(allocator, env) catch |err| {
            const msg = std.fmt.allocPrint(allocator, "{{\"error\":\"collect_failed:{any}\"}}", .{err}) catch continue;
            defer allocator.free(msg);
            postResponse(allocator, host, port, event.request_id, msg) catch {};
            continue;
        };
        defer allocator.free(json);

        postResponse(allocator, host, port, event.request_id, json) catch |err| {
            std.debug.print("probe_lambda_caps: post error {any}\n", .{err});
        };
    }
}

// ============================================================
// Top-level collection
// ============================================================

fn collectAll(allocator: std.mem.Allocator, env: std.process.Environ) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);

    var w: JsonWriter = .{ .out = &buf, .alloc = allocator };

    const start_ns: i128 = nowNs();

    try w.beginObject();
    try w.field("schema_version");
    try w.intValue(SCHEMA_VERSION);

    try w.field("timestamp_unix");
    try w.intValue(@as(i64, @intCast(unixSeconds())));

    try w.field("arch");
    try w.stringValue(@tagName(builtin.target.cpu.arch));

    try w.field("environment");
    try collectEnvironment(&w, env);

    try w.field("system");
    try collectSystem(&w, allocator);

    try w.field("security");
    try collectSecurity(&w, allocator);

    try w.field("syscalls");
    try collectSyscalls(&w, allocator);

    try w.field("network");
    try collectNetwork(&w, allocator, env);

    try w.field("memory");
    try collectMemory(&w);

    try w.field("cpu_affinity");
    try collectAffinity(&w, allocator);

    try w.field("tmpfs");
    try collectTmpfs(&w, allocator);

    const end_ns: i128 = nowNs();
    try w.field("probe_duration_ms");
    try w.intValue(@as(i64, @intCast(@divTrunc(end_ns - start_ns, std.time.ns_per_ms))));

    try w.endObject();

    return buf.toOwnedSlice(allocator);
}

// ============================================================
// Section: environment (Lambda env vars)
// ============================================================

fn getEnvOpt(env: std.process.Environ, key: []const u8) ?[]const u8 {
    if (env.getPosix(key)) |v| return v;
    return null;
}

fn collectEnvironment(w: *JsonWriter, env: std.process.Environ) !void {
    try w.beginObject();
    const is_lambda = env.getPosix("AWS_LAMBDA_RUNTIME_API") != null;
    try w.field("is_lambda");
    try w.boolValue(is_lambda);

    try w.field("lambda_runtime_api");
    try w.optionalString(getEnvOpt(env, "AWS_LAMBDA_RUNTIME_API"));

    try w.field("function_name");
    try w.optionalString(getEnvOpt(env, "AWS_LAMBDA_FUNCTION_NAME"));

    try w.field("function_version");
    try w.optionalString(getEnvOpt(env, "AWS_LAMBDA_FUNCTION_VERSION"));

    try w.field("memory_limit_mb");
    try w.optionalString(getEnvOpt(env, "AWS_LAMBDA_FUNCTION_MEMORY_SIZE"));

    try w.field("execution_env");
    try w.optionalString(getEnvOpt(env, "AWS_EXECUTION_ENV"));

    try w.field("region");
    try w.optionalString(getEnvOpt(env, "AWS_REGION"));

    try w.field("log_group");
    try w.optionalString(getEnvOpt(env, "AWS_LAMBDA_LOG_GROUP_NAME"));

    try w.field("log_stream");
    try w.optionalString(getEnvOpt(env, "AWS_LAMBDA_LOG_STREAM_NAME"));

    try w.endObject();
}

// ============================================================
// Section: system (uname, /proc/cpuinfo, /proc/meminfo, /etc/os-release)
// ============================================================

fn collectSystem(w: *JsonWriter, allocator: std.mem.Allocator) !void {
    try w.beginObject();

    var uts: linux.utsname = undefined;
    const uname_r = linux.uname(&uts);
    try w.field("uname_ok");
    try w.boolValue(callOk(uname_r) == null);
    try w.field("uname");
    try w.beginObject();
    try w.field("sysname");
    try w.stringValue(std.mem.sliceTo(&uts.sysname, 0));
    try w.field("nodename");
    try w.stringValue(std.mem.sliceTo(&uts.nodename, 0));
    try w.field("release");
    try w.stringValue(std.mem.sliceTo(&uts.release, 0));
    try w.field("version");
    try w.stringValue(std.mem.sliceTo(&uts.version, 0));
    try w.field("machine");
    try w.stringValue(std.mem.sliceTo(&uts.machine, 0));
    try w.endObject();

    try w.field("page_size");
    try w.intValue(@as(i64, @intCast(std.heap.pageSize())));

    try w.field("os_release");
    try w.fileSliceValue(allocator, "/etc/os-release", 4096);

    try w.field("proc_version");
    try w.fileSliceValue(allocator, "/proc/version", 1024);

    try w.field("cpuinfo_summary");
    try collectCpuinfoSummary(w, allocator);

    try w.field("meminfo");
    try collectMeminfo(w, allocator);

    try w.field("rlimits");
    try collectRlimits(w);

    try w.endObject();
}

fn collectCpuinfoSummary(w: *JsonWriter, allocator: std.mem.Allocator) !void {
    try w.beginObject();
    const text = readFileAlloc(allocator, "/proc/cpuinfo", 1 << 20) catch {
        try w.field("error");
        try w.stringValue("cannot_read");
        try w.endObject();
        return;
    };
    defer allocator.free(text);

    var processor_count: u32 = 0;
    var model_name: []const u8 = "";
    var flags: []const u8 = "";
    var cpu_mhz: []const u8 = "";

    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| {
        if (std.mem.startsWith(u8, line, "processor\t")) processor_count += 1;
        if (std.mem.startsWith(u8, line, "model name") and model_name.len == 0) {
            if (std.mem.indexOfScalar(u8, line, ':')) |i| model_name = std.mem.trim(u8, line[i + 1 ..], " \t");
        }
        if (std.mem.startsWith(u8, line, "flags") and flags.len == 0) {
            if (std.mem.indexOfScalar(u8, line, ':')) |i| flags = std.mem.trim(u8, line[i + 1 ..], " \t");
        }
        if (std.mem.startsWith(u8, line, "Features") and flags.len == 0) {
            if (std.mem.indexOfScalar(u8, line, ':')) |i| flags = std.mem.trim(u8, line[i + 1 ..], " \t");
        }
        if (std.mem.startsWith(u8, line, "cpu MHz") and cpu_mhz.len == 0) {
            if (std.mem.indexOfScalar(u8, line, ':')) |i| cpu_mhz = std.mem.trim(u8, line[i + 1 ..], " \t");
        }
    }

    try w.field("processors");
    try w.intValue(processor_count);
    try w.field("model_name");
    try w.stringValue(model_name);
    try w.field("cpu_mhz");
    try w.stringValue(cpu_mhz);
    try w.field("flags");
    try w.stringValue(flags);
    try w.endObject();
}

fn collectMeminfo(w: *JsonWriter, allocator: std.mem.Allocator) !void {
    try w.beginObject();
    const text = readFileAlloc(allocator, "/proc/meminfo", 64 * 1024) catch {
        try w.field("error");
        try w.stringValue("cannot_read");
        try w.endObject();
        return;
    };
    defer allocator.free(text);

    const interesting = [_][]const u8{
        "MemTotal:",       "MemAvailable:", "MemFree:",   "Hugepagesize:",
        "HugePages_Total:", "HugePages_Free:", "AnonPages:", "Mapped:",
    };
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| {
        for (interesting) |key| {
            if (std.mem.startsWith(u8, line, key)) {
                const tail = std.mem.trim(u8, line[key.len..], " \t");
                try w.field(key[0 .. key.len - 1]);
                try w.stringValue(tail);
            }
        }
    }
    try w.endObject();
}

fn collectRlimits(w: *JsonWriter) !void {
    try w.beginObject();
    const Pair = struct { name: []const u8, res: linux.rlimit_resource };
    const pairs = [_]Pair{
        .{ .name = "AS", .res = .AS },
        .{ .name = "CORE", .res = .CORE },
        .{ .name = "CPU", .res = .CPU },
        .{ .name = "DATA", .res = .DATA },
        .{ .name = "FSIZE", .res = .FSIZE },
        .{ .name = "NOFILE", .res = .NOFILE },
        .{ .name = "NPROC", .res = .NPROC },
        .{ .name = "STACK", .res = .STACK },
    };
    for (pairs) |p| {
        const lim = posix.getrlimit(p.res) catch {
            try w.field(p.name);
            try w.stringValue("error");
            continue;
        };
        try w.field(p.name);
        try w.beginObject();
        try w.field("cur");
        try writeRlimUnsigned(w, lim.cur);
        try w.field("max");
        try writeRlimUnsigned(w, lim.max);
        try w.endObject();
    }
    try w.endObject();
}

fn writeRlimUnsigned(w: *JsonWriter, v: u64) !void {
    if (v == std.math.maxInt(u64)) {
        try w.stringValue("infinity");
    } else {
        try w.intValue(@as(i64, @intCast(@min(v, std.math.maxInt(i64)))));
    }
}

// ============================================================
// Section: security (seccomp, no_new_privs, capabilities)
// ============================================================

fn collectSecurity(w: *JsonWriter, allocator: std.mem.Allocator) !void {
    try w.beginObject();

    const status = readFileAlloc(allocator, "/proc/self/status", 64 * 1024) catch null;
    defer if (status) |s| allocator.free(s);

    var seccomp_mode: ?i64 = null;
    var no_new_privs: ?i64 = null;
    var cap_eff: []const u8 = "";

    if (status) |s| {
        var lines = std.mem.splitScalar(u8, s, '\n');
        while (lines.next()) |line| {
            if (std.mem.startsWith(u8, line, "Seccomp:")) {
                if (std.mem.indexOfScalar(u8, line, ':')) |i| {
                    const v = std.mem.trim(u8, line[i + 1 ..], " \t");
                    seccomp_mode = std.fmt.parseInt(i64, v, 10) catch null;
                }
            } else if (std.mem.startsWith(u8, line, "NoNewPrivs:")) {
                if (std.mem.indexOfScalar(u8, line, ':')) |i| {
                    const v = std.mem.trim(u8, line[i + 1 ..], " \t");
                    no_new_privs = std.fmt.parseInt(i64, v, 10) catch null;
                }
            } else if (std.mem.startsWith(u8, line, "CapEff:")) {
                if (std.mem.indexOfScalar(u8, line, ':')) |i| cap_eff = std.mem.trim(u8, line[i + 1 ..], " \t");
            }
        }
    }

    try w.field("seccomp_mode");
    if (seccomp_mode) |m| try w.intValue(m) else try w.nullValue();
    try w.field("seccomp_meaning");
    try w.stringValue(switch (seccomp_mode orelse -1) {
        0 => "disabled",
        1 => "strict",
        2 => "filter",
        else => "unknown",
    });
    try w.field("no_new_privs");
    if (no_new_privs) |m| try w.intValue(m) else try w.nullValue();
    try w.field("cap_eff_hex");
    try w.stringValue(cap_eff);

    try w.field("ns_pid");
    try w.linkTargetValue(allocator, "/proc/self/ns/pid");
    try w.field("ns_net");
    try w.linkTargetValue(allocator, "/proc/self/ns/net");
    try w.field("ns_user");
    try w.linkTargetValue(allocator, "/proc/self/ns/user");
    try w.field("ns_mnt");
    try w.linkTargetValue(allocator, "/proc/self/ns/mnt");

    try w.endObject();
}

// ============================================================
// Section: syscalls (the headline question)
// ============================================================

fn collectSyscalls(w: *JsonWriter, allocator: std.mem.Allocator) !void {
    _ = allocator;
    try w.beginArray();

    // io_uring_setup: the headline one.
    {
        var params: linux.io_uring_params = std.mem.zeroes(linux.io_uring_params);
        const r = linux.io_uring_setup(1, &params);
        try emitSyscall(w, "io_uring_setup", r, &.{ .{ .key = "entries", .val = "1" } });
        if (callOk(r) == null) {
            sysClose(fdFromUsize(r));
        }
    }

    // userfaultfd: useful for userspace page-fault handling tricks.
    {
        const r = linux.syscall1(.userfaultfd, 0);
        try emitSyscall(w, "userfaultfd", r, &.{});
        if (callOk(r) == null) sysClose(fdFromUsize(r));
    }

    // eventfd2(0, 0)
    {
        const r = linux.syscall2(.eventfd2, 0, 0);
        try emitSyscall(w, "eventfd2", r, &.{});
        if (callOk(r) == null) sysClose(fdFromUsize(r));
    }

    // timerfd_create(CLOCK_MONOTONIC, 0)
    {
        const CLOCK_MONOTONIC: usize = 1;
        const r = linux.syscall2(.timerfd_create, CLOCK_MONOTONIC, 0);
        try emitSyscall(w, "timerfd_create", r, &.{});
        if (callOk(r) == null) sysClose(fdFromUsize(r));
    }

    // signalfd4: pass an empty sigset; we just want to know if the syscall is reachable.
    {
        var mask: linux.sigset_t = std.mem.zeroes(linux.sigset_t);
        const r = linux.syscall4(.signalfd4, @as(usize, @bitCast(@as(isize, -1))), @intFromPtr(&mask), @sizeOf(linux.sigset_t), 0);
        try emitSyscall(w, "signalfd4", r, &.{ .{ .key = "fd", .val = "-1" } });
        if (callOk(r) == null) sysClose(fdFromUsize(r));
    }

    // pidfd_open(getpid(), 0)
    {
        const pid = linux.getpid();
        const r = linux.syscall2(.pidfd_open, @as(usize, @intCast(pid)), 0);
        try emitSyscall(w, "pidfd_open", r, &.{});
        if (callOk(r) == null) sysClose(fdFromUsize(r));
    }

    // epoll_create1(0): should always work; baseline.
    {
        const r = linux.syscall1(.epoll_create1, 0);
        try emitSyscall(w, "epoll_create1", r, &.{});
        if (callOk(r) == null) sysClose(fdFromUsize(r));
    }

    // epoll_pwait2 with bogus fd: tells us syscall reachability without needing real epoll fd
    {
        const r = linux.syscall6(.epoll_pwait2, @as(usize, @bitCast(@as(isize, -1))), 0, 0, 0, 0, 0);
        try emitSyscall(w, "epoll_pwait2", r, &.{ .{ .key = "fd", .val = "-1 (probe)" } });
    }

    // clone3 with size=0: returns EINVAL if syscall reachable, EPERM/ENOSYS if blocked
    {
        const r = linux.syscall2(.clone3, 0, 0);
        try emitSyscall(w, "clone3", r, &.{ .{ .key = "size", .val = "0 (probe)" } });
    }

    // perf_event_open: needed for some profiling. Pass invalid args; reachability check.
    {
        var pe: extern struct {
            type: u32 = 0,
            size: u32 = 0,
            config: u64 = 0,
            sample_period_or_freq: u64 = 0,
            sample_type: u64 = 0,
            read_format: u64 = 0,
            flags: u64 = 0,
            wakeup_events_or_watermark: u32 = 0,
            bp_type: u32 = 0,
            bp_addr_or_config1: u64 = 0,
            bp_len_or_config2: u64 = 0,
            branch_sample_type: u64 = 0,
            sample_regs_user: u64 = 0,
            sample_stack_user: u32 = 0,
            clockid: i32 = 0,
            sample_regs_intr: u64 = 0,
            aux_watermark: u32 = 0,
            sample_max_stack: u16 = 0,
            __reserved_2: u16 = 0,
        } = .{};
        pe.size = @sizeOf(@TypeOf(pe));
        const r = linux.syscall5(.perf_event_open, @intFromPtr(&pe), 0, @as(usize, @bitCast(@as(isize, -1))), @as(usize, @bitCast(@as(isize, -1))), 0);
        try emitSyscall(w, "perf_event_open", r, &.{});
        if (callOk(r) == null) sysClose(fdFromUsize(r));
    }

    // bpf(BPF_PROG_LOAD-ish): pass cmd=0 (BPF_MAP_CREATE) with null attr — checks reachability.
    {
        const r = linux.syscall3(.bpf, 0, 0, 0);
        try emitSyscall(w, "bpf", r, &.{ .{ .key = "cmd", .val = "0" } });
    }

    // ptrace(PTRACE_TRACEME): a request that, if seccomp blocks ptrace family, fails fast.
    {
        const r = linux.syscall4(.ptrace, 0, 0, 0, 0); // PTRACE_TRACEME=0
        try emitSyscall(w, "ptrace_traceme", r, &.{});
    }

    // mlock probe: pass a small buffer; useful for whether we can pin pages.
    {
        var pin: [4096]u8 align(4096) = undefined;
        const r = linux.syscall2(.mlock, @intFromPtr(&pin[0]), 4096);
        try emitSyscall(w, "mlock_4k", r, &.{});
        if (callOk(r) == null) {
            _ = linux.syscall2(.munlock, @intFromPtr(&pin[0]), 4096);
        }
    }

    try w.endArray();
}

const KV = struct { key: []const u8, val: []const u8 };

fn emitSyscall(w: *JsonWriter, name: []const u8, r: usize, args: []const KV) !void {
    try w.beginObject();
    try w.field("name");
    try w.stringValue(name);
    if (callOk(r)) |errno| {
        try w.field("supported");
        try w.boolValue(false);
        try w.field("errno");
        try w.intValue(errno);
        try w.field("errno_name");
        try w.stringValue(errnoName(errno));
    } else {
        try w.field("supported");
        try w.boolValue(true);
        const signed: isize = @bitCast(r);
        try w.field("retval");
        try w.intValue(@intCast(signed));
    }
    if (args.len > 0) {
        try w.field("args");
        try w.beginObject();
        for (args) |kv| {
            try w.field(kv.key);
            try w.stringValue(kv.val);
        }
        try w.endObject();
    }
    try w.endObject();
}

// ============================================================
// Section: network (resolv.conf, S3 reach, setsockopt)
// ============================================================

fn collectNetwork(w: *JsonWriter, allocator: std.mem.Allocator, env: std.process.Environ) !void {
    try w.beginObject();

    try w.field("resolv_conf");
    try w.fileSliceValue(allocator, "/etc/resolv.conf", 4096);

    try w.field("nsswitch_conf");
    try w.fileSliceValue(allocator, "/etc/nsswitch.conf", 4096);

    try w.field("hosts");
    try w.fileSliceValue(allocator, "/etc/hosts", 4096);

    _ = env;
    // DNS resolution: 0.16.0 removed std.net entirely (moved to std.Io.Net which
    // requires an Io vtable). For a static probe we just inspect the resolver
    // config files above and do a synthetic TCP connect to a known reachable IP.
    // 1.1.1.1:443 is a globally reachable TLS port that exercises egress
    // without depending on Lambda having S3 routing yet.
    {
        try w.field("tcp_egress_test");
        try w.beginObject();
        const sock = sysSocket(linux.AF.INET, linux.SOCK.STREAM | linux.SOCK.CLOEXEC, linux.IPPROTO.TCP) catch {
            try w.field("ok");
            try w.boolValue(false);
            try w.field("err");
            try w.stringValue("socket_failed");
            try w.endObject();
            return;
        };
        defer sysClose(sock);

        var addr: linux.sockaddr.in = std.mem.zeroes(linux.sockaddr.in);
        addr.family = linux.AF.INET;
        addr.port = std.mem.nativeToBig(u16, 443);
        addr.addr = std.mem.nativeToBig(u32, (1 << 24) | (1 << 16) | (1 << 8) | 1); // 1.1.1.1

        const c0 = nowNs();
        const r = linux.connect(sock, @ptrCast(&addr), @sizeOf(linux.sockaddr.in));
        const c1 = nowNs();
        try w.field("target");
        try w.stringValue("1.1.1.1:443");
        try w.field("connect_ms");
        try w.intValue(@as(i64, @intCast(@divTrunc(c1 - c0, std.time.ns_per_ms))));
        if (callOk(r)) |errno| {
            try w.field("ok");
            try w.boolValue(false);
            try w.field("errno");
            try w.intValue(errno);
            try w.field("errno_name");
            try w.stringValue(errnoName(errno));
        } else {
            try w.field("ok");
            try w.boolValue(true);
        }
        try w.endObject();
    }

    try w.field("setsockopt");
    try collectSetsockopt(w);

    try w.field("ipv6_loopback");
    try collectIpv6Loopback(w);

    try w.endObject();
}

fn collectSetsockopt(w: *JsonWriter) !void {
    try w.beginObject();

    const sock = sysSocket(linux.AF.INET, linux.SOCK.STREAM | linux.SOCK.CLOEXEC, linux.IPPROTO.TCP) catch {
        try w.field("error");
        try w.stringValue("socket_failed");
        try w.endObject();
        return;
    };
    defer sysClose(sock);

    const probes = [_]struct {
        name: []const u8,
        level: i32,
        opt: u32,
        val: i32,
    }{
        .{ .name = "SO_REUSEPORT", .level = linux.SOL.SOCKET, .opt = linux.SO.REUSEPORT, .val = 1 },
        .{ .name = "SO_REUSEADDR", .level = linux.SOL.SOCKET, .opt = linux.SO.REUSEADDR, .val = 1 },
        .{ .name = "TCP_NODELAY", .level = linux.IPPROTO.TCP, .opt = 1, .val = 1 }, // TCP_NODELAY=1
        .{ .name = "TCP_FASTOPEN", .level = linux.IPPROTO.TCP, .opt = 23, .val = 1 }, // TCP_FASTOPEN=23
        .{ .name = "SO_INCOMING_CPU", .level = linux.SOL.SOCKET, .opt = 49, .val = 0 }, // SO_INCOMING_CPU=49
        .{ .name = "SO_ZEROCOPY", .level = linux.SOL.SOCKET, .opt = 60, .val = 1 }, // SO_ZEROCOPY=60
    };
    for (probes) |p| {
        const v = p.val;
        const r = linux.setsockopt(sock, p.level, p.opt, std.mem.asBytes(&v).ptr, @sizeOf(i32));
        try w.field(p.name);
        if (callOk(r)) |errno| {
            try w.beginObject();
            try w.field("ok");
            try w.boolValue(false);
            try w.field("errno");
            try w.intValue(errno);
            try w.field("errno_name");
            try w.stringValue(errnoName(errno));
            try w.endObject();
        } else {
            try w.stringValue("ok");
        }
    }
    try w.endObject();
}

fn collectIpv6Loopback(w: *JsonWriter) !void {
    try w.beginObject();
    const sock = sysSocket(linux.AF.INET6, linux.SOCK.STREAM | linux.SOCK.CLOEXEC, linux.IPPROTO.TCP) catch {
        try w.field("socket_ok");
        try w.boolValue(false);
        try w.field("error");
        try w.stringValue("socket_failed");
        try w.endObject();
        return;
    };
    defer sysClose(sock);
    try w.field("socket_ok");
    try w.boolValue(true);
    try w.endObject();
}

// ============================================================
// Section: memory (mmap variants, madvise hints)
// ============================================================

fn collectMemory(w: *JsonWriter) !void {
    try w.beginObject();

    // mmap_anon_4k
    {
        const ptr = linux.mmap(null, 4096, linux.PROT{ .READ = true, .WRITE = true }, .{ .TYPE = .PRIVATE, .ANONYMOUS = true }, -1, 0);
        try w.field("mmap_anon_4k");
        if (callOk(ptr)) |errno| {
            try emitErrno(w, errno);
        } else {
            try w.stringValue("ok");
            _ = linux.munmap(@ptrFromInt(ptr), 4096);
        }
    }

    // mmap_anon_2m_hugetlb (request a 2MB huge page)
    {
        const HUGETLB_FLAG_2MB: linux.MAP = .{
            .TYPE = .PRIVATE,
            .ANONYMOUS = true,
            .HUGETLB = true,
        };
        const ptr = linux.mmap(null, 2 * 1024 * 1024, linux.PROT{ .READ = true, .WRITE = true }, HUGETLB_FLAG_2MB, -1, 0);
        try w.field("mmap_anon_2m_hugetlb");
        if (callOk(ptr)) |errno| {
            try emitErrno(w, errno);
        } else {
            try w.stringValue("ok");
            _ = linux.munmap(@ptrFromInt(ptr), 2 * 1024 * 1024);
        }
    }

    // mmap + populate
    {
        const ptr = linux.mmap(null, 4096, linux.PROT{ .READ = true, .WRITE = true }, .{ .TYPE = .PRIVATE, .ANONYMOUS = true, .POPULATE = true }, -1, 0);
        try w.field("mmap_populate");
        if (callOk(ptr)) |errno| {
            try emitErrno(w, errno);
        } else {
            try w.stringValue("ok");
            _ = linux.munmap(@ptrFromInt(ptr), 4096);
        }
    }

    // madvise(MADV_HUGEPAGE) on a 4K anon mapping
    {
        const ptr_u = linux.mmap(null, 2 * 1024 * 1024, linux.PROT{ .READ = true, .WRITE = true }, .{ .TYPE = .PRIVATE, .ANONYMOUS = true }, -1, 0);
        try w.field("madvise_hugepage");
        if (callOk(ptr_u)) |errno| {
            try emitErrno(w, errno);
        } else {
            const r = linux.madvise(@ptrFromInt(ptr_u), 2 * 1024 * 1024, std.posix.MADV.HUGEPAGE);
            if (callOk(r)) |errno| try emitErrno(w, errno) else try w.stringValue("ok");
            _ = linux.munmap(@ptrFromInt(ptr_u), 2 * 1024 * 1024);
        }
    }

    // madvise(MADV_DONTNEED)
    {
        const ptr_u = linux.mmap(null, 4096, linux.PROT{ .READ = true, .WRITE = true }, .{ .TYPE = .PRIVATE, .ANONYMOUS = true }, -1, 0);
        try w.field("madvise_dontneed");
        if (callOk(ptr_u)) |errno| {
            try emitErrno(w, errno);
        } else {
            const r = linux.madvise(@ptrFromInt(ptr_u), 4096, std.posix.MADV.DONTNEED);
            if (callOk(r)) |errno| try emitErrno(w, errno) else try w.stringValue("ok");
            _ = linux.munmap(@ptrFromInt(ptr_u), 4096);
        }
    }

    try w.endObject();
}

fn emitErrno(w: *JsonWriter, errno: i32) !void {
    try w.beginObject();
    try w.field("ok");
    try w.boolValue(false);
    try w.field("errno");
    try w.intValue(errno);
    try w.field("errno_name");
    try w.stringValue(errnoName(errno));
    try w.endObject();
}

// ============================================================
// Section: cpu_affinity
// ============================================================

fn collectAffinity(w: *JsonWriter, allocator: std.mem.Allocator) !void {
    _ = allocator;
    try w.beginObject();

    var mask: [16]u64 = std.mem.zeroes([16]u64);
    const r = linux.syscall3(.sched_getaffinity, 0, @sizeOf(@TypeOf(mask)), @intFromPtr(&mask));
    try w.field("sched_getaffinity");
    if (callOk(r)) |errno| {
        try emitErrno(w, errno);
        try w.field("affinity_cpu_count");
        try w.nullValue();
    } else {
        try w.stringValue("ok");
        var count: u32 = 0;
        for (mask) |word| count += @popCount(word);
        try w.field("affinity_cpu_count");
        try w.intValue(count);
    }

    // sched_setaffinity to whatever we read back (round-trip). If it fails we're locked.
    if (callOk(r) == null) {
        const r2 = linux.syscall3(.sched_setaffinity, 0, @sizeOf(@TypeOf(mask)), @intFromPtr(&mask));
        try w.field("sched_setaffinity_self");
        if (callOk(r2)) |errno| try emitErrno(w, errno) else try w.stringValue("ok");
    }

    // Online CPUs as Lambda sees them via /sys/devices/system/cpu/online
    {
        var fbuf: [64]u8 = undefined;
        const fd_or = sysOpenRead("/sys/devices/system/cpu/online");
        if (fd_or) |fd| {
            defer sysClose(fd);
            const n = sysRead(fd, &fbuf) catch 0;
            try w.field("cpu_online_raw");
            try w.stringValue(std.mem.trim(u8, fbuf[0..n], "\n \t"));
        } else |_| {
            try w.field("cpu_online_raw");
            try w.stringValue("missing");
        }
    }

    try w.endObject();
}

// ============================================================
// Section: tmpfs (sequential write throughput)
// ============================================================

fn collectTmpfs(w: *JsonWriter, allocator: std.mem.Allocator) !void {
    try w.beginObject();

    const path = "/tmp/zpq_probe_write_test.bin";
    const SIZE: usize = 64 * 1024 * 1024;

    try w.field("path");
    try w.stringValue(path);

    const buf = allocator.alloc(u8, 1 * 1024 * 1024) catch {
        try w.field("error");
        try w.stringValue("alloc_failed");
        try w.endObject();
        return;
    };
    defer allocator.free(buf);
    @memset(buf, 0xab);

    const fd = sysOpenCreate(path) catch {
        try w.field("create_error");
        try w.stringValue("open_failed");
        try w.endObject();
        return;
    };
    defer sysClose(fd);
    defer sysUnlink(path);

    const start = nowNs();
    var written: usize = 0;
    while (written < SIZE) {
        const n = sysWrite(fd, buf) catch {
            try w.field("write_error");
            try w.stringValue("write_failed");
            try w.endObject();
            return;
        };
        if (n == 0) break;
        written += n;
    }
    sysFsync(fd);
    const end = nowNs();

    const ms = @divTrunc(end - start, std.time.ns_per_ms);
    try w.field("write_bytes");
    try w.intValue(@as(i64, @intCast(written)));
    try w.field("write_ms");
    try w.intValue(@as(i64, @intCast(ms)));
    if (ms > 0) {
        const mb_per_s: i64 = @intCast(@divTrunc(@as(i128, @intCast(written)) * 1000, ms * 1024 * 1024));
        try w.field("write_mb_per_s");
        try w.intValue(mb_per_s);
    }

    try w.endObject();
}

// ============================================================
// Helpers: errno, callOk
// ============================================================

fn callOk(r: usize) ?i32 {
    const signed: isize = @bitCast(r);
    if (signed >= -4095 and signed < 0) return @intCast(-signed);
    return null;
}

fn errnoName(errno: i32) []const u8 {
    return switch (errno) {
        1 => "EPERM",
        2 => "ENOENT",
        4 => "EINTR",
        5 => "EIO",
        9 => "EBADF",
        11 => "EAGAIN",
        12 => "ENOMEM",
        13 => "EACCES",
        14 => "EFAULT",
        16 => "EBUSY",
        17 => "EEXIST",
        19 => "ENODEV",
        22 => "EINVAL",
        24 => "EMFILE",
        38 => "ENOSYS",
        48 => "EADDRINUSE",
        49 => "EADDRNOTAVAIL",
        92 => "ENOPROTOOPT",
        93 => "EPROTONOSUPPORT",
        95 => "EOPNOTSUPP",
        97 => "EAFNOSUPPORT",
        99 => "EADDRNOTAVAIL_2",
        110 => "ETIMEDOUT",
        111 => "ECONNREFUSED",
        else => "EUNKNOWN",
    };
}

fn readFileAlloc(allocator: std.mem.Allocator, path: []const u8, max: usize) ![]u8 {
    const fd = sysOpenRead(path) catch return error.OpenFailed;
    defer sysClose(fd);
    var list: std.ArrayList(u8) = .empty;
    errdefer list.deinit(allocator);
    var tmp: [4096]u8 = undefined;
    var total: usize = 0;
    while (total < max) {
        const n = sysRead(fd, &tmp) catch break;
        if (n == 0) break;
        try list.appendSlice(allocator, tmp[0..n]);
        total += n;
    }
    return list.toOwnedSlice(allocator);
}

// ============================================================
// JSON writer (hand-rolled, predictable output)
// ============================================================

const JsonWriter = struct {
    out: *std.ArrayList(u8),
    alloc: std.mem.Allocator,
    needs_comma: bool = false,
    in_object: bool = false,

    fn writeRaw(self: *JsonWriter, s: []const u8) !void {
        try self.out.appendSlice(self.alloc, s);
    }

    fn maybeComma(self: *JsonWriter) !void {
        if (self.needs_comma) try self.writeRaw(",");
        self.needs_comma = false;
    }

    fn beginObject(self: *JsonWriter) !void {
        try self.maybeComma();
        try self.writeRaw("{");
        self.needs_comma = false;
    }
    fn endObject(self: *JsonWriter) !void {
        try self.writeRaw("}");
        self.needs_comma = true;
    }
    fn beginArray(self: *JsonWriter) !void {
        try self.maybeComma();
        try self.writeRaw("[");
        self.needs_comma = false;
    }
    fn endArray(self: *JsonWriter) !void {
        try self.writeRaw("]");
        self.needs_comma = true;
    }

    fn field(self: *JsonWriter, name: []const u8) !void {
        try self.maybeComma();
        try self.writeRaw("\"");
        try writeEscaped(self.out, self.alloc, name);
        try self.writeRaw("\":");
        self.needs_comma = false;
    }

    fn stringValue(self: *JsonWriter, s: []const u8) !void {
        try self.maybeComma();
        try self.writeRaw("\"");
        try writeEscaped(self.out, self.alloc, s);
        try self.writeRaw("\"");
        self.needs_comma = true;
    }

    fn optionalString(self: *JsonWriter, s: ?[]const u8) !void {
        if (s) |v| try self.stringValue(v) else try self.nullValue();
    }

    fn intValue(self: *JsonWriter, n: i64) !void {
        try self.maybeComma();
        var buf: [32]u8 = undefined;
        const out = try std.fmt.bufPrint(&buf, "{d}", .{n});
        try self.writeRaw(out);
        self.needs_comma = true;
    }

    fn boolValue(self: *JsonWriter, b: bool) !void {
        try self.maybeComma();
        try self.writeRaw(if (b) "true" else "false");
        self.needs_comma = true;
    }

    fn nullValue(self: *JsonWriter) !void {
        try self.maybeComma();
        try self.writeRaw("null");
        self.needs_comma = true;
    }

    fn fileSliceValue(self: *JsonWriter, alloc: std.mem.Allocator, path: []const u8, max: usize) !void {
        const data = readFileAlloc(alloc, path, max) catch {
            try self.nullValue();
            return;
        };
        defer alloc.free(data);
        try self.stringValue(data);
    }

    fn linkTargetValue(self: *JsonWriter, alloc: std.mem.Allocator, path: []const u8) !void {
        _ = alloc;
        var path_z: [256]u8 = undefined;
        if (path.len + 1 > path_z.len) {
            try self.nullValue();
            return;
        }
        @memcpy(path_z[0..path.len], path);
        path_z[path.len] = 0;
        var buf: [256]u8 = undefined;
        const target = sysReadlink(@ptrCast(&path_z[0]), &buf) catch {
            try self.nullValue();
            return;
        };
        try self.stringValue(target);
    }
};

fn writeEscaped(out: *std.ArrayList(u8), alloc: std.mem.Allocator, s: []const u8) !void {
    for (s) |c| {
        switch (c) {
            '"' => try out.appendSlice(alloc, "\\\""),
            '\\' => try out.appendSlice(alloc, "\\\\"),
            '\n' => try out.appendSlice(alloc, "\\n"),
            '\r' => try out.appendSlice(alloc, "\\r"),
            '\t' => try out.appendSlice(alloc, "\\t"),
            0...8, 11, 12, 14...31 => {
                var buf: [8]u8 = undefined;
                const n = std.fmt.bufPrint(&buf, "\\u{x:0>4}", .{c}) catch unreachable;
                try out.appendSlice(alloc, n);
            },
            else => try out.append(alloc, c),
        }
    }
}

// ============================================================
// Lambda runtime API helpers (cribbed from probes/lambda_hello)
// ============================================================

const InvocationEvent = struct { body: []u8, request_id: []u8 };

fn getNextInvocation(allocator: std.mem.Allocator, host: []const u8, port: u16) !InvocationEvent {
    const sock = try connectToHost(host, port);
    defer sysClose(sock);
    _ = try sysWrite(sock, "GET /2018-06-01/runtime/invocation/next HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    var tmp: [8192]u8 = undefined;
    while (true) {
        const n = sysRead(sock, &tmp) catch break;
        if (n == 0) break;
        try buf.appendSlice(allocator, tmp[0..n]);
    }

    const response = buf.items;
    const header_end = std.mem.indexOf(u8, response, "\r\n\r\n") orelse return error.Malformed;
    const headers = response[0..header_end];
    const body = response[header_end + 4 ..];

    var req_id: []const u8 = "unknown";
    var lines = std.mem.splitSequence(u8, headers, "\r\n");
    while (lines.next()) |line| {
        if (std.ascii.startsWithIgnoreCase(line, "lambda-runtime-aws-request-id:")) {
            req_id = std.mem.trim(u8, line["lambda-runtime-aws-request-id:".len..], " \t");
            break;
        }
    }
    return .{ .body = try allocator.dupe(u8, body), .request_id = try allocator.dupe(u8, req_id) };
}

fn postResponse(allocator: std.mem.Allocator, host: []const u8, port: u16, id: []const u8, body: []const u8) !void {
    const sock = try connectToHost(host, port);
    defer sysClose(sock);
    const req = try std.fmt.allocPrint(
        allocator,
        "POST /2018-06-01/runtime/invocation/{s}/response HTTP/1.1\r\nHost: localhost\r\nContent-Length: {d}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{s}",
        .{ id, body.len, body },
    );
    defer allocator.free(req);
    var off: usize = 0;
    while (off < req.len) {
        const n = try sysWrite(sock, req[off..]);
        if (n == 0) break;
        off += n;
    }
    var drain: [1024]u8 = undefined;
    _ = sysRead(sock, &drain) catch {};
}

fn connectToHost(host: []const u8, port: u16) !linux.fd_t {
    const sock = try sysSocket(linux.AF.INET, linux.SOCK.STREAM | linux.SOCK.CLOEXEC, linux.IPPROTO.TCP);
    errdefer sysClose(sock);
    var parts: [4]u8 = undefined;
    var i: usize = 0;
    var iter = std.mem.splitScalar(u8, host, '.');
    while (iter.next()) |p| : (i += 1) {
        if (i >= 4) break;
        parts[i] = std.fmt.parseInt(u8, p, 10) catch 0;
    }
    var addr = std.mem.zeroes(linux.sockaddr.in);
    addr.family = linux.AF.INET;
    addr.port = std.mem.nativeToBig(u16, port);
    const ip: u32 = (@as(u32, parts[0]) << 24) | (@as(u32, parts[1]) << 16) | (@as(u32, parts[2]) << 8) | parts[3];
    addr.addr = std.mem.nativeToBig(u32, ip);
    try sysConnect(sock, @ptrCast(&addr), @sizeOf(linux.sockaddr.in));
    return sock;
}
