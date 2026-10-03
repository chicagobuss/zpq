//! Build-time fetcher for one pinned prebuilt archive.
//!
//!   fetch_prebuilt <url> <sha256-hex> <out-path>
//!
//! Streams `url` into an unnamed temporary file next to `out-path`, hashing
//! as it writes, and materializes `out-path` only when the digest matches.
//! A failed or interrupted download therefore never leaves a file that a
//! later build could mistake for a good one. build.zig runs this as a Run
//! step, so the verified result lives in the Zig cache, keyed on the URL
//! and digest.

const std = @import("std");
const Sha256 = std.crypto.hash.sha2.Sha256;

const offline_hint = "To build without network access, fill vendor/boring_tls/prebuilt/<arch>-<os>/ with " ++
    "tools/r2-fetch-artifacts.sh where the URL is reachable.";

pub fn main(init: std.process.Init) !u8 {
    const io = init.io;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len != 4) {
        std.log.err("usage: fetch_prebuilt <url> <sha256-hex> <out-path>", .{});
        return 2;
    }
    const url = args[1];
    const want = args[2];
    const out_path = args[3];

    var client: std.http.Client = .{ .allocator = init.gpa, .io = io };
    defer client.deinit();
    // Honour HTTP(S)_PROXY like curl does.
    try client.initDefaultProxies(init.arena.allocator(), init.environ_map);

    var atomic = try std.Io.Dir.cwd().createFileAtomic(io, out_path, .{ .replace = true });
    defer atomic.deinit(io);

    var file_buf: [64 * 1024]u8 = undefined;
    var file_writer = atomic.file.writer(io, &file_buf);
    var hash_buf: [64 * 1024]u8 = undefined;
    var hashed = file_writer.interface.hashed(Sha256.init(.{}), &hash_buf);

    const result = client.fetch(.{
        .location = .{ .url = url },
        .response_writer = &hashed.writer,
    }) catch |err| {
        std.log.err("fetching {s}: {t}\n" ++ offline_hint, .{ url, err });
        return 1;
    };
    if (result.status != .ok) {
        std.log.err("fetching {s}: HTTP {d}\n" ++ offline_hint, .{ url, @intFromEnum(result.status) });
        return 1;
    }
    try hashed.writer.flush();
    try file_writer.interface.flush();

    const got = std.fmt.bytesToHex(hashed.hasher.finalResult(), .lower);
    if (!std.ascii.eqlIgnoreCase(&got, want)) {
        std.log.err(
            \\sha256 mismatch for {s}
            \\  pinned: {s}
            \\  served: {s}
            \\The artifact behind this URL changed. If that was intended, update
            \\vendor/boring_tls/prebuilt.sha256.
        , .{ url, want, &got });
        return 1;
    }
    try atomic.replace(io);
    return 0;
}
