//! Decompression probe: times zpq's own codec entry point (`compression.decompressInto`) on page payloads dumped
//! from a real file, so codec speed can be read apart from scan, decode and page-fault noise.
//!
//! Usage: probe_decompress <dir> [reps=20] [threads=1] [mode=reuse|fresh]
//!   <dir>/index.tsv lines: name, page_type, codec, compressed_len, uncompressed_len (one page payload per file).
//!   reuse: every page decompresses into one pre-touched buffer per thread (codec speed alone).
//!   fresh: every page decompresses into a newly mapped buffer (codec plus first-touch faults).
//! Prints best-of-reps wall time, GB/s of output, and TSC cycles per output byte (aggregate over threads).

const std = @import("std");
const linux = std.os.linux;
const zpq = @import("zpq");
const compression = zpq.core.parquet.compression;
const schema = zpq.core.schema;

inline fn rdtsc() u64 {
    var lo: u32 = 0;
    var hi: u32 = 0;
    asm volatile ("rdtsc"
        : [lo] "={eax}" (lo),
          [hi] "={edx}" (hi),
    );
    return (@as(u64, hi) << 32) | lo;
}

const Page = struct { src: []const u8, out_len: usize, codec: schema.CompressionCodec };

fn readFile(gpa: std.mem.Allocator, path: []const u8) ![]u8 {
    var path_z: [4096]u8 = undefined;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;
    const r = linux.openat(linux.AT.FDCWD, @ptrCast(&path_z[0]), .{ .ACCMODE = .RDONLY }, 0);
    if (@as(isize, @bitCast(r)) < 0) return error.OpenFailed;
    const fd: linux.fd_t = @intCast(r);
    defer _ = linux.close(fd);
    const size: usize = @intCast(linux.lseek(fd, 0, 2));
    _ = linux.lseek(fd, 0, 0);
    const buf = try gpa.alloc(u8, size);
    var got: usize = 0;
    while (got < size) {
        const n = linux.read(fd, buf[got..].ptr, size - got);
        if (@as(isize, @bitCast(n)) <= 0) return error.ReadFailed;
        got += n;
    }
    return buf;
}

fn codecOf(name: []const u8) ?schema.CompressionCodec {
    inline for (.{ "SNAPPY", "GZIP", "ZSTD", "LZ4", "LZ4_RAW", "UNCOMPRESSED" }) |c| {
        if (std.mem.eql(u8, name, c)) return @field(schema.CompressionCodec, c);
    }
    return null;
}

fn nowNs() u64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * 1_000_000_000 + @as(u64, @intCast(ts.nsec));
}

const Ctx = struct {
    pages: []const Page,
    max_out: usize,
    fresh: bool,
    err: ?anyerror = null,
};

fn runPass(ctx: *Ctx, buf: []u8) void {
    var scratch_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer scratch_arena.deinit();
    for (ctx.pages) |p| {
        if (ctx.fresh) {
            const prot: std.posix.PROT = .{ .READ = true, .WRITE = true };
            const m = std.posix.mmap(null, p.out_len, prot, .{ .TYPE = .PRIVATE, .ANONYMOUS = true }, -1, 0) catch |e| {
                ctx.err = e;
                return;
            };
            defer std.posix.munmap(m);
            compression.decompressInto(scratch_arena.allocator(), p.src, p.codec, m[0..p.out_len]) catch |e| {
                ctx.err = e;
                return;
            };
        } else {
            compression.decompressInto(scratch_arena.allocator(), p.src, p.codec, buf[0..p.out_len]) catch |e| {
                ctx.err = e;
                return;
            };
        }
    }
}

fn worker(ctx: *Ctx, buf: []u8, start: *std.atomic.Value(u32)) void {
    while (start.load(.acquire) == 0) std.atomic.spinLoopHint();
    runPass(ctx, buf);
}

pub fn main(init: std.process.Init) !void {
    const gpa = std.heap.page_allocator;
    _ = init.gpa;
    var it = std.process.Args.Iterator.init(init.minimal.args);
    _ = it.next();
    const dir = it.next() orelse return error.BadArgs;
    const reps: usize = if (it.next()) |s| try std.fmt.parseInt(usize, s, 10) else 20;
    const threads: usize = if (it.next()) |s| try std.fmt.parseInt(usize, s, 10) else 1;
    const fresh = if (it.next()) |s| std.mem.eql(u8, s, "fresh") else false;

    var path_buf: [4096]u8 = undefined;
    const index = try readFile(gpa, try std.fmt.bufPrint(&path_buf, "{s}/index.tsv", .{dir}));
    var pages: std.ArrayList(Page) = .empty;
    var in_total: usize = 0;
    var out_total: usize = 0;
    var max_out: usize = 0;
    var lines = std.mem.tokenizeScalar(u8, index, '\n');
    while (lines.next()) |line| {
        var f = std.mem.tokenizeScalar(u8, line, '\t');
        const name = f.next().?;
        _ = f.next();
        const codec = codecOf(f.next().?) orelse continue;
        _ = f.next();
        const out_len = try std.fmt.parseInt(usize, f.next().?, 10);
        const src = try readFile(gpa, try std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ dir, name }));
        try pages.append(gpa, .{ .src = src, .out_len = out_len, .codec = codec });
        in_total += src.len;
        out_total += out_len;
        max_out = @max(max_out, out_len);
    }

    const bufs = try gpa.alloc([]u8, threads);
    for (bufs) |*b| {
        b.* = try gpa.alloc(u8, max_out + 64);
        @memset(b.*, 1);
    }
    var ctxs = try gpa.alloc(Ctx, threads);
    for (ctxs) |*c| c.* = .{ .pages = pages.items, .max_out = max_out, .fresh = fresh };

    var best_ns: u64 = std.math.maxInt(u64);
    var best_tsc: u64 = std.math.maxInt(u64);
    for (0..reps) |_| {
        var start = std.atomic.Value(u32).init(0);
        const hs = try gpa.alloc(std.Thread, threads);
        defer gpa.free(hs);
        for (hs, 0..) |*h, i| h.* = try std.Thread.spawn(.{}, worker, .{ &ctxs[i], bufs[i], &start });
        const t0 = nowNs();
        const c0 = rdtsc();
        start.store(1, .release);
        for (hs) |h| h.join();
        const c1 = rdtsc();
        const t1 = nowNs();
        for (ctxs) |c| if (c.err) |e| return e;
        best_ns = @min(best_ns, t1 - t0);
        best_tsc = @min(best_tsc, c1 - c0);
    }
    const total_out: f64 = @floatFromInt(out_total * threads);
    const ms = @as(f64, @floatFromInt(best_ns)) / 1e6;
    const gbs = total_out / @as(f64, @floatFromInt(best_ns));
    const tsc_per_b = @as(f64, @floatFromInt(best_tsc)) * @as(f64, @floatFromInt(threads)) / total_out;
    const fmt = "{s} pages={d} in={d} out={d} threads={d} mode={s} best={d:.3} ms {d:.2} GB/s {d:.3} tsc/B/thread\n";
    std.debug.print(fmt, .{
        dir, pages.items.len, in_total, out_total, threads, if (fresh) "fresh" else "reuse", ms, gbs, tsc_per_b,
    });
}
