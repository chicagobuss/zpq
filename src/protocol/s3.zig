const std = @import("std");
const http = @import("http.zig");
const sigv4 = @import("sigv4.zig");

/// S3 Protocol implementation.
/// Focuses on Sans-I/O request construction for common S3 operations.
pub const S3 = struct {
    signer: sigv4.SigV4,
    bucket: []const u8,
    region: []const u8,

    pub const Options = struct {
        use_tls: bool = true,
        use_path_style: bool = false,
        clock_offset: i64 = 0,
        use_unsigned_payload: bool = false,
    };

    pub fn init(bucket: []const u8, region: []const u8, access_key: []const u8, secret_key: []const u8, session_token: ?[]const u8) S3 {
        return .{
            .signer = .{
                .region = region,
                .access_key = access_key,
                .secret_key = secret_key,
                .session_token = session_token,
            },
            .bucket = bucket,
            .region = region,
        };
    }

    /// Formats a GET request with an optional Range header.
    /// Returns the signed headers that should be sent.
    pub fn formatGetRequest(
        self: S3,
        allocator: std.mem.Allocator,
        key: []const u8,
        range: ?struct { start: u64, end: u64 },
        options: Options,
    ) ![]sigv4.SigV4.Header {
        const host = try self.getHost(allocator, options);
        defer allocator.free(host);

        const path = try self.getPath(allocator, key, options);
        defer allocator.free(path);

        var extra_headers = std.ArrayListUnmanaged(sigv4.SigV4.Header){};
        defer extra_headers.deinit(allocator); // shallow deinit

        var range_val: ?[]u8 = null;
        defer if (range_val) |v| allocator.free(v);

        if (range) |r| {
            range_val = try std.fmt.allocPrint(allocator, "bytes={d}-{d}", .{ r.start, r.end - 1 });
            try extra_headers.append(allocator, .{ .name = "Range", .value = range_val.? });
        }


        return self.signer.sign(
            allocator,
            "GET",
            host,
            path,
            null,
            extra_headers.items,
            "",
            .{ .clock_offset = options.clock_offset, .use_unsigned_payload = options.use_unsigned_payload },
        );
    }

    /// Formats a HEAD request to fetch object metadata (like size).
    pub fn formatHeadRequest(
        self: S3,
        allocator: std.mem.Allocator,
        key: []const u8,
        options: Options,
    ) ![]sigv4.SigV4.Header {
        const host = try self.getHost(allocator, options);
        defer allocator.free(host);

        const path = try self.getPath(allocator, key, options);
        defer allocator.free(path);

        var extra_headers = std.ArrayListUnmanaged(sigv4.SigV4.Header){};
        defer extra_headers.deinit(allocator);


        return self.signer.sign(
            allocator,
            "HEAD",
            host,
            path,
            null,
            extra_headers.items,
            "",
            .{ .clock_offset = options.clock_offset, .use_unsigned_payload = options.use_unsigned_payload },
        );
    }

    pub fn formatPutRequest(self: S3, allocator: std.mem.Allocator, key: []const u8, payload: []const u8, options: Options) ![]sigv4.SigV4.Header {
        const path = try self.getPath(allocator, key, options);
        defer allocator.free(path);

        const host = try self.getHost(allocator, options);
        defer allocator.free(host);

        var extra_headers = std.ArrayListUnmanaged(sigv4.SigV4.Header){};
        defer extra_headers.deinit(allocator);


        return self.signer.sign(
            allocator,
            "PUT",
            host,
            path,
            null,
            extra_headers.items,
            payload,
            .{ .clock_offset = options.clock_offset, .use_unsigned_payload = options.use_unsigned_payload },
        );
    }

    pub fn formatDeleteRequest(self: S3, allocator: std.mem.Allocator, key: []const u8, options: Options) ![]sigv4.SigV4.Header {
        const path = try self.getPath(allocator, key, options);
        defer allocator.free(path);

        const host = try self.getHost(allocator, options);
        defer allocator.free(host);

        var extra_headers = std.ArrayListUnmanaged(sigv4.SigV4.Header){};
        defer extra_headers.deinit(allocator);


        return self.signer.sign(
            allocator,
            "DELETE",
            host,
            path,
            null,
            extra_headers.items,
            "",
            .{ .clock_offset = options.clock_offset, .use_unsigned_payload = options.use_unsigned_payload },
        );
    }

    fn getHost(self: S3, allocator: std.mem.Allocator, options: Options) ![]u8 {
        if (options.use_path_style) {
            return try std.fmt.allocPrint(allocator, "s3.{s}.amazonaws.com", .{self.region});
        } else {
            return try std.fmt.allocPrint(allocator, "{s}.s3.{s}.amazonaws.com", .{ self.bucket, self.region });
        }
    }

    fn getPath(self: S3, allocator: std.mem.Allocator, key: []const u8, options: Options) ![]u8 {
        if (options.use_path_style) {
            if (key.len > 0 and key[0] == '/') {
                return try std.fmt.allocPrint(allocator, "/{s}{s}", .{ self.bucket, key });
            } else {
                return try std.fmt.allocPrint(allocator, "/{s}/{s}", .{ self.bucket, key });
            }
        } else {
            if (key.len > 0 and key[0] == '/') {
                return try allocator.dupe(u8, key);
            } else {
                return try std.fmt.allocPrint(allocator, "/{s}", .{key});
            }
        }
    }

    pub fn formatInitiateMultipartRequest(
        self: S3,
        allocator: std.mem.Allocator,
        key: []const u8,
        options: Options,
    ) ![]sigv4.SigV4.Header {
        const path = try self.getPath(allocator, key, options);
        defer allocator.free(path);

        const host = try self.getHost(allocator, options);
        defer allocator.free(host);

        var extra_headers = std.ArrayListUnmanaged(sigv4.SigV4.Header){};
        defer extra_headers.deinit(allocator);


        try extra_headers.append(allocator, .{ .name = "Content-Type", .value = "application/octet-stream" });

        return self.signer.sign(
            allocator,
            "POST",
            host,
            path,
            "uploads=", // Query string
            extra_headers.items,
            "", // No payload
            .{ .clock_offset = options.clock_offset, .use_unsigned_payload = options.use_unsigned_payload },
        );
    }

    pub fn formatUploadPartRequest(
        self: S3,
        allocator: std.mem.Allocator,
        key: []const u8,
        upload_id: []const u8,
        part_number: u32,
        payload: []const u8,
        options: Options,
    ) ![]sigv4.SigV4.Header {
        const path = try self.getPath(allocator, key, options);
        defer allocator.free(path);

        const query = try std.fmt.allocPrint(allocator, "partNumber={d}&uploadId={s}", .{ part_number, upload_id });
        defer allocator.free(query);

        const host = try self.getHost(allocator, options);
        defer allocator.free(host);

        var extra_headers = std.ArrayListUnmanaged(sigv4.SigV4.Header){};
        defer extra_headers.deinit(allocator);

        return self.signer.sign(
            allocator,
            "PUT",
            host,
            path,
            query,
            extra_headers.items,
            payload,
            .{ .clock_offset = options.clock_offset, .use_unsigned_payload = options.use_unsigned_payload },
        );
    }

    pub fn formatCompleteMultipartRequest(
        self: S3,
        allocator: std.mem.Allocator,
        key: []const u8,
        upload_id: []const u8,
        parts: []const Part, // Expected to be already sorted
        options: Options,
    ) !struct { headers: []sigv4.SigV4.Header, body: []const u8 } {
        const path = try self.getPath(allocator, key, options);
        defer allocator.free(path);

        const query = try std.fmt.allocPrint(allocator, "uploadId={s}", .{upload_id});
        defer allocator.free(query);

        const host = try self.getHost(allocator, options);
        defer allocator.free(host);

        // Generate XML body
        var xml = std.ArrayListUnmanaged(u8){};
        defer xml.deinit(allocator);

        try xml.appendSlice(allocator, "<CompleteMultipartUpload>");
        for (parts) |p| {
            var buf: [256]u8 = undefined;
            const s = try std.fmt.bufPrint(&buf, "<Part><PartNumber>{d}</PartNumber><ETag>{s}</ETag></Part>", .{ p.part_number, p.etag });
            try xml.appendSlice(allocator, s);
        }
        try xml.appendSlice(allocator, "</CompleteMultipartUpload>");

        const body = try xml.toOwnedSlice(allocator);
        errdefer allocator.free(body);

        var extra_headers = std.ArrayListUnmanaged(sigv4.SigV4.Header){};
        defer extra_headers.deinit(allocator);

        
        try extra_headers.append(allocator, .{ .name = "Content-Type", .value = "application/xml" });

        const headers = try self.signer.sign(
            allocator,
            "POST",
            host,
            path,
            query,
            extra_headers.items,
            body,
            .{ .clock_offset = options.clock_offset, .use_unsigned_payload = options.use_unsigned_payload },
        );

        return .{ .headers = headers, .body = body };
    }

    pub const Part = struct {
        part_number: u32,
        etag: []const u8,
    };

    pub fn parseUploadId(allocator: std.mem.Allocator, xml: []const u8) ![]const u8 {
        if (std.mem.indexOf(u8, xml, "<UploadId>")) |start| {
            const id_start = start + "<UploadId>".len;
            if (std.mem.indexOf(u8, xml[id_start..], "</UploadId>")) |end| {
                return allocator.dupe(u8, xml[id_start .. id_start + end]);
            }
        }
        return error.UploadIdNotFound;
    }
};
