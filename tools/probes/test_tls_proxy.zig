const std = @import("std");
// Note: This path might need adjustment depending on where this is run from
// Since we are in tools/probes, we go up two levels to root.
const TlsAdapter = @import("../../src/zpq/s3_legacy/tls_adapter.zig").TlsAdapter;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const host = "localhost";
    const port = 9443;
    const cert_path = "cert.pem";

    std.debug.print("Loading cert from {s}...\n", .{cert_path});
    // Assume cert.pem is in CWD (root)
    const cert_content = try std.fs.cwd().readFileAlloc(cert_path, allocator, @enumFromInt(1024 * 1024));
    defer allocator.free(cert_content);

    std.debug.print("Connecting to {s}:{d}...\n", .{host, port});
    const list = try std.Io.net.getAddressList(allocator, host, port);
    defer list.deinit();
    const addr = list.addrs[0];

    const stream = try std.Io.net.tcpConnectToAddress(addr);
    const fd = stream.handle;
    defer stream.close();

    std.debug.print("Connected (FD {d})\n", .{fd});

    // Init TLS
    var adapter = try TlsAdapter.init(allocator, fd, host, cert_content);
    defer adapter.deinit();

    // Write
    const req = "HEAD /mock-bucket/mock-key HTTP/1.1\r\nHost: localhost:9443\r\nConnection: close\r\n\r\n";
    std.debug.print("Writing request ({d} bytes)...\n", .{req.len});
    _ = try adapter.write(req);
    std.debug.print("Wrote request.\n", .{});

    // Read
    var buf: [1024]u8 = undefined;
    std.debug.print("Reading response...\n", .{});
    const n = try adapter.read(&buf);
    std.debug.print("Read {d} bytes.\n", .{n});
    if (n > 0) {
        std.debug.print("Response:\n{s}\n", .{buf[0..n]});
    }
}

