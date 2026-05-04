// Bakeoff probe: std.Io.Threaded + Io.Group → 8 concurrent S3 multipart uploads.
//
// Workload:
//   1. Read benchmark file (155 MB) from local disk.
//   2. CreateMultipartUpload → upload_id (sequential).
//   3. 8 parallel UploadPart tasks via Io.Group, each with its own
//      DNS resolution + TLS handshake + S3 client.
//   4. CompleteMultipartUpload (sequential).
//   5. Report wallclock for the parallel section + total + MB/s.
//
// Deliberately disposable: each task does its own DNS + handshake (no
// pooling). That's the pessimistic test — if Io.Threaded is good enough
// without pooling, with pooling it'll only get better.
//
// Inputs (env): AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION,
//               AWS_SESSION_TOKEN (optional), AWS_S3_BUCKET.
// Optional env: BAKEOFF_FILE (defaults to data/benchmark/benchmark_100mb.parquet).

const std = @import("std");
const linux = std.os.linux;
const Io = std.Io;
const zpq = @import("zpq");

const tls = zpq.io.tls;
const http = zpq.io.http;
const sigv4 = zpq.io.sigv4;
const s3 = zpq.io.s3;

const PART_COUNT: usize = 8;
const DEFAULT_FILE = "data/benchmark_100mb.parquet";

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const arena = init.arena.allocator();
    const gpa = init.gpa;
    const env = init.minimal.environ;

    const creds = try s3.Credentials.fromEnv(env);
    const bucket = env.getPosix("AWS_S3_BUCKET") orelse {
        std.debug.print("error: AWS_S3_BUCKET not set\n", .{});
        return error.MissingBucket;
    };
    const file_path = env.getPosix("BAKEOFF_FILE") orelse DEFAULT_FILE;

    std.debug.print("[setup] reading {s}\n", .{file_path});
    const file_data = try readFile(arena, file_path);
    std.debug.print("[setup] {d} bytes loaded\n", .{file_data.len});

    // --- Slice into PART_COUNT parts ---
    var part_slices: [PART_COUNT][]const u8 = undefined;
    const part_size = (file_data.len + PART_COUNT - 1) / PART_COUNT;
    for (0..PART_COUNT) |i| {
        const start = i * part_size;
        const end = @min(start + part_size, file_data.len);
        part_slices[i] = file_data[start..end];
    }

    // --- Fresh key ---
    const ts: i64 = nowRealSec();
    const key = try std.fmt.allocPrint(arena, "bakeoff/{d}/threaded.parquet", .{ts});
    std.debug.print("[setup] target s3://{s}/{s}\n", .{ bucket, key });

    // --- 1. CreateMultipartUpload (sequential) ---
    const t_create_start = nowMonoNs();
    const upload_id = try createMultipart(arena, creds, bucket, key);
    const t_create_end = nowMonoNs();
    std.debug.print(
        "[create] upload_id={s}  ({d:.3}s)\n",
        .{ upload_id, nsToS(t_create_end - t_create_start) },
    );

    // --- 2. Parallel UploadPart via Io.Group ---
    var etag_storage: [PART_COUNT][512]u8 = undefined;
    var etag_lens: [PART_COUNT]usize = .{0} ** PART_COUNT;
    var task_durations: [PART_COUNT]i64 = .{0} ** PART_COUNT;

    var ctxs: [PART_COUNT]PartCtx = undefined;
    for (0..PART_COUNT) |i| {
        ctxs[i] = PartCtx{
            .gpa = gpa,
            .creds = creds,
            .bucket = bucket,
            .key = key,
            .upload_id = upload_id,
            .part_number = @intCast(i + 1),
            .body = part_slices[i],
            .etag_buf = &etag_storage[i],
            .etag_len = &etag_lens[i],
            .duration_ns_out = &task_durations[i],
        };
    }

    const t_par_start = nowMonoNs();
    var group: Io.Group = .init;
    defer group.cancel(io);
    for (&ctxs) |*ctx_ptr| group.async(io, uploadPart, .{ io, ctx_ptr });
    try group.await(io);
    const t_par_end = nowMonoNs();

    // Gather etags / detect failures.
    var failed: bool = false;
    for (0..PART_COUNT) |i| {
        if (etag_lens[i] == 0) {
            std.debug.print("[uploadPart] part {d} FAILED (no ETag)\n", .{i + 1});
            failed = true;
        }
    }
    if (failed) return error.PartUploadFailed;

    // --- 3. CompleteMultipartUpload (sequential) ---
    const t_complete_start = nowMonoNs();
    try completeMultipart(arena, creds, bucket, key, upload_id, &etag_storage, &etag_lens);
    const t_complete_end = nowMonoNs();

    // --- 4. Report ---
    const par_ns: u64 = @intCast(t_par_end - t_par_start);
    const total_ns: u64 = @intCast(t_complete_end - t_create_start);
    const par_mbps = mbPerSec(file_data.len, par_ns);
    const total_mbps = mbPerSec(file_data.len, total_ns);

    std.debug.print("\n=== bakeoff_threaded ===\n", .{});
    std.debug.print("file       : {s} ({d} bytes)\n", .{ file_path, file_data.len });
    std.debug.print("parts      : {d} of ~{d} bytes\n", .{ PART_COUNT, part_size });
    std.debug.print("create     : {d:.3}s\n", .{nsToS(t_create_end - t_create_start)});
    std.debug.print("parallel   : {d:.3}s   {d:.1} MB/s\n", .{ nsToS(@intCast(par_ns)), par_mbps });
    std.debug.print("complete   : {d:.3}s\n", .{nsToS(t_complete_end - t_complete_start)});
    std.debug.print("total      : {d:.3}s   {d:.1} MB/s\n", .{ nsToS(@intCast(total_ns)), total_mbps });
    std.debug.print("per-part durations (s):", .{});
    for (task_durations) |d| std.debug.print(" {d:.3}", .{nsToS(d)});
    std.debug.print("\n", .{});
}

// ============================================================
// Parallel task
// ============================================================

const PartCtx = struct {
    gpa: std.mem.Allocator,
    creds: s3.Credentials,
    bucket: []const u8,
    key: []const u8,
    upload_id: []const u8,
    part_number: u32,
    body: []const u8,
    etag_buf: *[512]u8,
    etag_len: *usize,
    duration_ns_out: *i64,
};

fn uploadPart(io: Io, ctx: *PartCtx) Io.Cancelable!void {
    _ = io; // we use blocking syscalls inside this thread; io is for cooperative tasks
    const t_start = nowMonoNs();
    defer ctx.duration_ns_out.* = @intCast(nowMonoNs() - t_start);

    var arena_state = std.heap.ArenaAllocator.init(ctx.gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const host = std.fmt.allocPrint(
        arena,
        "{s}.s3.{s}.amazonaws.com",
        .{ ctx.bucket, ctx.creds.region },
    ) catch |e| return logErr(ctx.part_number, "fmt host", e);

    const path = std.fmt.allocPrint(arena, "/{s}", .{ctx.key}) catch |e|
        return logErr(ctx.part_number, "fmt path", e);

    const query = std.fmt.allocPrint(
        arena,
        "partNumber={d}&uploadId={s}",
        .{ ctx.part_number, ctx.upload_id },
    ) catch |e| return logErr(ctx.part_number, "fmt query", e);

    const path_with_query = std.fmt.allocPrint(arena, "{s}?{s}", .{ path, query }) catch |e|
        return logErr(ctx.part_number, "fmt path+query", e);

    const addr_v4 = resolveIpv4(arena, host) catch |e|
        return logErr(ctx.part_number, "dns", e);

    var conn = tls.Connection.connect(arena, addr_v4, 443, host) catch |e|
        return logErr(ctx.part_number, "tls.connect", e);
    defer conn.deinit();

    const signer: sigv4.SigV4 = .{
        .region = ctx.creds.region,
        .access_key = ctx.creds.access_key,
        .secret_key = ctx.creds.secret_key,
        .session_token = ctx.creds.session_token,
    };
    const signed = signer.sign(
        arena,
        "PUT",
        host,
        path,
        query,
        &.{},
        ctx.body,
        .{ .use_unsigned_payload = true },
    ) catch |e| return logErr(ctx.part_number, "sign", e);

    var headers: std.ArrayList(http.Header) = .empty;
    for (signed) |h| (headers.append(arena, .{ .name = h.name, .value = h.value }) catch |e|
        return logErr(ctx.part_number, "header alloc", e));

    const resp = http.sendRequest(arena, &conn, .{
        .method = .PUT,
        .host = host,
        .path = path_with_query,
        .headers = headers.items,
        .body = ctx.body,
    }) catch |e| return logErr(ctx.part_number, "http.send", e);

    if (resp.status != 200) {
        std.debug.print("[uploadPart {d}] status {d}: {s}\n", .{ ctx.part_number, resp.status, resp.body });
        return;
    }

    const etag = resp.header("ETag") orelse {
        std.debug.print("[uploadPart {d}] no ETag header\n", .{ctx.part_number});
        return;
    };
    if (etag.len > ctx.etag_buf.len) {
        std.debug.print("[uploadPart {d}] etag too long ({d} bytes)\n", .{ ctx.part_number, etag.len });
        return;
    }
    @memcpy(ctx.etag_buf[0..etag.len], etag);
    ctx.etag_len.* = etag.len;
}

fn logErr(part: u32, what: []const u8, e: anyerror) void {
    std.debug.print("[uploadPart {d}] {s}: {s}\n", .{ part, what, @errorName(e) });
}

// ============================================================
// CreateMultipartUpload (POST /key?uploads)
// ============================================================

fn createMultipart(
    arena: std.mem.Allocator,
    creds: s3.Credentials,
    bucket: []const u8,
    key: []const u8,
) ![]const u8 {
    const host = try std.fmt.allocPrint(arena, "{s}.s3.{s}.amazonaws.com", .{ bucket, creds.region });
    const path = try std.fmt.allocPrint(arena, "/{s}", .{key});
    const query = "uploads=";
    const path_with_query = try std.fmt.allocPrint(arena, "{s}?{s}", .{ path, query });

    const t0 = nowMonoNs();
    const addr_v4 = try resolveIpv4(arena, host);
    const t1 = nowMonoNs();
    var conn = try tls.Connection.connect(arena, addr_v4, 443, host);
    const t2 = nowMonoNs();
    defer conn.deinit();

    const signer: sigv4.SigV4 = .{
        .region = creds.region,
        .access_key = creds.access_key,
        .secret_key = creds.secret_key,
        .session_token = creds.session_token,
    };
    const signed = try signer.sign(arena, "POST", host, path, query, &.{}, "", .{});

    var headers: std.ArrayList(http.Header) = .empty;
    for (signed) |h| try headers.append(arena, .{ .name = h.name, .value = h.value });

    const resp = try http.sendRequest(arena, &conn, .{
        .method = .POST,
        .host = host,
        .path = path_with_query,
        .headers = headers.items,
        .body = "",
    });
    const t3 = nowMonoNs();

    std.debug.print(
        "[create] dns={d:.3}s tls={d:.3}s http={d:.3}s status={d} body_len={d}\n",
        .{ nsToS(t1 - t0), nsToS(t2 - t1), nsToS(t3 - t2), resp.status, resp.body.len },
    );

    if (resp.status != 200) {
        std.debug.print("[create] status {d}: {s}\n", .{ resp.status, resp.body });
        return error.CreateFailed;
    }

    return try extractXml(arena, resp.body, "UploadId");
}

// ============================================================
// CompleteMultipartUpload (POST /key?uploadId=X with XML body)
// ============================================================

fn completeMultipart(
    arena: std.mem.Allocator,
    creds: s3.Credentials,
    bucket: []const u8,
    key: []const u8,
    upload_id: []const u8,
    etag_storage: *const [PART_COUNT][512]u8,
    etag_lens: *const [PART_COUNT]usize,
) !void {
    const host = try std.fmt.allocPrint(arena, "{s}.s3.{s}.amazonaws.com", .{ bucket, creds.region });
    const path = try std.fmt.allocPrint(arena, "/{s}", .{key});
    const query = try std.fmt.allocPrint(arena, "uploadId={s}", .{upload_id});
    const path_with_query = try std.fmt.allocPrint(arena, "{s}?{s}", .{ path, query });

    // Build XML body.
    var body: std.ArrayList(u8) = .empty;
    try body.appendSlice(arena, "<CompleteMultipartUpload>");
    for (0..PART_COUNT) |i| {
        const etag = etag_storage[i][0..etag_lens[i]];
        const piece = try std.fmt.allocPrint(
            arena,
            "<Part><PartNumber>{d}</PartNumber><ETag>{s}</ETag></Part>",
            .{ i + 1, etag },
        );
        try body.appendSlice(arena, piece);
    }
    try body.appendSlice(arena, "</CompleteMultipartUpload>");

    const addr_v4 = try resolveIpv4(arena, host);
    var conn = try tls.Connection.connect(arena, addr_v4, 443, host);
    defer conn.deinit();

    const signer: sigv4.SigV4 = .{
        .region = creds.region,
        .access_key = creds.access_key,
        .secret_key = creds.secret_key,
        .session_token = creds.session_token,
    };
    const signed = try signer.sign(arena, "POST", host, path, query, &.{}, body.items, .{});

    var headers: std.ArrayList(http.Header) = .empty;
    for (signed) |h| try headers.append(arena, .{ .name = h.name, .value = h.value });

    const resp = try http.sendRequest(arena, &conn, .{
        .method = .POST,
        .host = host,
        .path = path_with_query,
        .headers = headers.items,
        .body = body.items,
    });

    if (resp.status != 200) {
        std.debug.print("[complete] status {d}: {s}\n", .{ resp.status, resp.body });
        return error.CompleteFailed;
    }
    // S3 may return 200 OK with an XML <Error> body — quick sanity check.
    if (std.mem.indexOf(u8, resp.body, "<Error>") != null) {
        std.debug.print("[complete] xml error in 200 body: {s}\n", .{resp.body});
        return error.CompleteFailed;
    }
}

// ============================================================
// Tiny XML extractor. Returns the text between <tag>...</tag>.
// Allocates from the arena.
// ============================================================

fn extractXml(arena: std.mem.Allocator, xml: []const u8, tag: []const u8) ![]const u8 {
    const open = try std.fmt.allocPrint(arena, "<{s}>", .{tag});
    const close = try std.fmt.allocPrint(arena, "</{s}>", .{tag});
    const start = std.mem.indexOf(u8, xml, open) orelse return error.XmlTagMissing;
    const after_open = start + open.len;
    const end = std.mem.indexOfPos(u8, xml, after_open, close) orelse return error.XmlTagMissing;
    return try arena.dupe(u8, xml[after_open..end]);
}

// ============================================================
// File read (whole-file, bounded by Lambda memory tier).
// ============================================================

fn readFile(arena: std.mem.Allocator, path: []const u8) ![]u8 {
    var path_buf: [512]u8 = undefined;
    if (path.len + 1 > path_buf.len) return error.PathTooLong;
    @memcpy(path_buf[0..path.len], path);
    path_buf[path.len] = 0;

    const fd = linux.openat(linux.AT.FDCWD, @ptrCast(&path_buf[0]), .{ .ACCMODE = .RDONLY }, 0);
    const ifd: isize = @bitCast(fd);
    if (ifd < 0) return error.OpenFailed;
    const tfd: linux.fd_t = @intCast(ifd);
    defer _ = linux.close(tfd);

    var stx: linux.Statx = undefined;
    const empty: [*:0]const u8 = "";
    const mask: linux.STATX = .{ .SIZE = true };
    const sr = linux.statx(tfd, empty, linux.AT.EMPTY_PATH, mask, &stx);
    if (@as(isize, @bitCast(sr)) < 0) return error.StatFailed;
    const size: usize = @intCast(stx.size);

    const buf = try arena.alloc(u8, size);
    var off: usize = 0;
    while (off < size) {
        const r = linux.read(tfd, buf.ptr + off, size - off);
        const ir: isize = @bitCast(r);
        if (ir <= 0) return error.ReadFailed;
        off += @intCast(ir);
    }
    return buf;
}

// ============================================================
// DNS via libc getaddrinfo (matches s3.zig). Returns dotted-quad IPv4.
// ============================================================

const c = struct {
    extern fn getaddrinfo(
        node: [*:0]const u8,
        service: ?[*:0]const u8,
        hints: ?*const addrinfo,
        res: *?*addrinfo,
    ) c_int;
    extern fn freeaddrinfo(res: *addrinfo) void;

    const addrinfo = extern struct {
        flags: c_int,
        family: c_int,
        socktype: c_int,
        protocol: c_int,
        addrlen: u32,
        addr: ?*sockaddr,
        canonname: ?[*:0]u8,
        next: ?*addrinfo,
    };
    const sockaddr = extern struct {
        family: u16,
        port: u16,
        addr: u32,
        zero: [8]u8,
    };
    const AF_INET: c_int = 2;
};

fn resolveIpv4(arena: std.mem.Allocator, host: []const u8) ![]const u8 {
    const host_z = try arena.dupeZ(u8, host);
    var hints = std.mem.zeroes(c.addrinfo);
    hints.family = c.AF_INET;
    var result: ?*c.addrinfo = null;
    const rc = c.getaddrinfo(host_z, null, &hints, &result);
    if (rc != 0 or result == null) return error.DnsFailed;
    defer c.freeaddrinfo(result.?);
    const sa = result.?.addr orelse return error.DnsFailed;
    const ip = std.mem.bigToNative(u32, sa.addr);
    return std.fmt.allocPrint(arena, "{d}.{d}.{d}.{d}", .{
        (ip >> 24) & 0xff,
        (ip >> 16) & 0xff,
        (ip >> 8) & 0xff,
        ip & 0xff,
    });
}

// ============================================================
// Time helpers
// ============================================================

fn nowMonoNs() i64 {
    var ts: linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, ts.sec) * 1_000_000_000 + @as(i64, ts.nsec);
}

fn nowRealSec() i64 {
    var ts: linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = linux.clock_gettime(.REALTIME, &ts);
    return @as(i64, ts.sec);
}

fn nsToS(ns: i128) f64 {
    return @as(f64, @floatFromInt(ns)) / 1.0e9;
}

fn mbPerSec(bytes: usize, ns: u64) f64 {
    if (ns == 0) return 0;
    const secs = @as(f64, @floatFromInt(ns)) / 1.0e9;
    const mb = @as(f64, @floatFromInt(bytes)) / (1024.0 * 1024.0);
    return mb / secs;
}
