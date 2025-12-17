const std = @import("std");
const Io = std.Io;
const net = Io.net;

pub const TlsAdapter = struct {
    allocator: std.mem.Allocator,
    
    // Low-level IO
    stream: net.Stream,
    threaded: Io.Threaded,
    io: Io,
    
    // Buffered Readers/Writers (Concrete Types)
    net_reader: net.Stream.Reader,
    net_writer: net.Stream.Writer,
    
    // Buffers for the underlying network reader/writer
    // Must be large enough for TLS records (16KB + overhead)
    read_buf: [18 * 1024]u8 = undefined,
    write_buf: [18 * 1024]u8 = undefined,
    
    // TLS Internal Buffers (required by Client.Options)
    tls_read_buf: [18 * 1024]u8 = undefined,
    tls_write_buf: [18 * 1024]u8 = undefined,
    
    // Randomness for handshake
    entropy: [176]u8 = undefined,
    
    // TLS
    client: std.crypto.tls.Client,
    bundle: std.crypto.Certificate.Bundle,

    pub fn init(allocator: std.mem.Allocator, fd: std.posix.fd_t, host: []const u8, trusted_cert: ?[]const u8) !*TlsAdapter {
        // Allocate self on heap to ensure stable pointers
        const self = try allocator.create(TlsAdapter);
        errdefer allocator.destroy(self);
        
        self.allocator = allocator;
        
        // Setup IO
        self.threaded = Io.Threaded.init(allocator);
        errdefer self.threaded.deinit();
        self.io = self.threaded.io();
        
        // Setup Bundle
        self.bundle = std.crypto.Certificate.Bundle{};
        
        const ts = try std.posix.clock_gettime(std.posix.CLOCK.REALTIME);
        const ns = @as(i96, ts.sec) * std.time.ns_per_s + ts.nsec;
        const now = Io.Timestamp{ .nanoseconds = ns };
        
        try self.bundle.rescan(allocator, self.io, now);
        errdefer self.bundle.deinit(allocator);
        
        if (trusted_cert) |cert| {
            try addCertToBundle(&self.bundle, allocator, cert, ts.sec);
        }
        
        // Setup Stream
        self.stream = net.Stream{
            .socket = .{
                .handle = fd,
                .address = undefined,
            },
        };
        
        // Setup Reader/Writer
        self.net_reader = self.stream.reader(self.io, &self.read_buf);
        self.net_writer = self.stream.writer(self.io, &self.write_buf);
        
        // Setup Entropy
        std.crypto.random.bytes(&self.entropy);
        
        // Init TLS Client
        const options = std.crypto.tls.Client.Options{
            .host = .{ .explicit = host },
            .ca = .{ .bundle = self.bundle },
            .write_buffer = &self.tls_write_buf,
            .read_buffer = &self.tls_read_buf,
            .entropy = &self.entropy,
            .realtime_now_seconds = ts.sec,
        };
        
        self.client = try std.crypto.tls.Client.init(
            &self.net_reader.interface, 
            &self.net_writer.interface, 
            options
        );
        
        // Flush the initial ClientHello
        try self.net_writer.interface.flush();
        
        return self;
    }
    
    pub fn deinit(self: *TlsAdapter) void {
        // Cleanup
        self.bundle.deinit(self.allocator);
        self.threaded.deinit();
        self.allocator.destroy(self);
    }
    
    pub fn read(self: *TlsAdapter, buffer: []u8) !usize {
        var buffers = [1][]u8{buffer};
        return self.client.reader.readVec(&buffers);
    }
    
    pub fn write(self: *TlsAdapter, buffer: []const u8) !usize {
        return self.client.writer.write(buffer);
    }
};

fn addCertToBundle(bundle: *std.crypto.Certificate.Bundle, allocator: std.mem.Allocator, cert_pem: []const u8, now_sec: i64) !void {
    const begin_marker = "-----BEGIN CERTIFICATE-----";
    const end_marker = "-----END CERTIFICATE-----";

    var start_index: usize = 0;
    while (std.mem.indexOfPos(u8, cert_pem, start_index, begin_marker)) |begin_marker_start| {
        const cert_start = begin_marker_start + begin_marker.len;
        const cert_end = std.mem.indexOfPos(u8, cert_pem, cert_start, end_marker) orelse return error.MissingEndCertificateMarker;

        start_index = cert_end + end_marker.len;

        const encoded_cert = std.mem.trim(u8, cert_pem[cert_start..cert_end], " \t\r\n");

        const decoded_start: u32 = @intCast(bundle.bytes.items.len);

        const decoder = std.base64.standard.decoderWithIgnore(" \t\r\n");
        const max_size = encoded_cert.len; // Safe upper bound (Base64 is 4 chars -> 3 bytes, so len is > needed)

        try bundle.bytes.ensureUnusedCapacity(allocator, max_size);
        const dest_buf = bundle.bytes.allocatedSlice()[decoded_start..];

        const decoded_len = try decoder.decode(dest_buf, encoded_cert);
        bundle.bytes.items.len += decoded_len;

        try bundle.parseCert(allocator, decoded_start, now_sec);
    }
}
