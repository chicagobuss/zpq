const std = @import("std");
const zpq = @import("zpq");

test "core: snappy roundtrip" {
    // We don't have a snappy compressor in zpq yet (it uses Snappy mostly for reading).
    // But we can test the decompressor with a known encoded string.
    // "Wiki" encoded in snappy
    const input = [_]u8{ 4, 0x0c, 'W', 'i', 'k', 'i' };
    var buf: [100]u8 = undefined;
    const len = try zpq.core.snappy.uncompress(&input, &buf);
    try std.testing.expectEqual(@as(usize, 4), len);
    try std.testing.expectEqualStrings("Wiki", buf[0..len]);
}

test "core: rle decoding" {
    // RLE Run: 8 values of 5. Header: (8 << 1) | 0 = 16 (0x10). Data: 5.
    const data = [_]u8{ 0x10, 0x05 };
    var dec = zpq.core.rle.RleDecoder.init(&data, 3);

    var i: usize = 0;
    while (i < 8) : (i += 1) {
        const val = try dec.next();
        try std.testing.expectEqual(@as(?u64, 5), val);
    }
    try std.testing.expectEqual(@as(?u64, null), try dec.next());
}

test "core: thrift varint" {
    const data = [_]u8{ 0x85, 0x02 }; // 261 in varint
    var reader = zpq.core.thrift.Reader.init(&data);
    const val = try reader.readVarInt(u32);
    try std.testing.expectEqual(@as(u32, 261), val);
}

test "core: sigv4 plausible" {
    // We can't easily test the full signature without a fixed clock,
    // but we can verify the signer doesn't crash and produces an Authorization header.
    const allocator = std.testing.allocator;
    const signer = zpq.s3.sigv4.SigV4{
        .region = "us-east-1",
        .access_key = "AKIAEXAMPLE",
        .secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    };

    var headers: std.ArrayList(std.http.Header) = .empty;
    defer {
        for (headers.items) |h| {
            allocator.free(h.value);
        }
        headers.deinit(allocator);
    }

    const uri = try std.Uri.parse("https://examplebucket.s3.amazonaws.com/test.txt");

    try signer.sign(allocator, "GET", uri, &headers, "");

    var found_auth = false;
    for (headers.items) |h| {
        if (std.ascii.eqlIgnoreCase(h.name, "Authorization")) {
            found_auth = true;
            try std.testing.expect(std.mem.startsWith(u8, h.value, "AWS4-HMAC-SHA256"));
        }
    }
    try std.testing.expect(found_auth);
}
