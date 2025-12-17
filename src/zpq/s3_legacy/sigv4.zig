const std = @import("std");

/// Minimal AWS SigV4 implementation for S3.
/// Focuses on "header signing" only (no query params), as that's all we need for S3 GET/HEAD.
pub const SigV4 = struct {
    region: []const u8,
    service: []const u8 = "s3",
    access_key: []const u8,
    secret_key: []const u8,
    session_token: ?[]const u8 = null,

    /// Signs an HTTP request by adding the necessary headers.
    pub fn sign(
        self: SigV4,
        allocator: std.mem.Allocator,
        method: []const u8,
        uri: std.Uri,
        headers: *std.ArrayList(std.http.Header),
        payload: []const u8,
    ) !void {
        // 1. Create Date (ISO8601 Basic Format: YYYYMMDDTHHMMSSZ)
        const ts = try std.posix.clock_gettime(std.posix.CLOCK.REALTIME);
        const now = ts.sec;
        var date_buf: [16]u8 = undefined;
        const iso_date = try fmtIso8601(now, &date_buf);
        const date_short = iso_date[0..8]; 

        // 2. Add Required Headers
        // Add Host Header (Required for SigV4)
        if (uri.host) |h| {
            const host_val = if (uri.port) |p|
                try std.fmt.allocPrint(allocator, "{s}:{d}", .{h.percent_encoded, p})
            else
                try allocator.dupe(u8, h.percent_encoded);
            try headers.append(allocator, .{ .name = "Host", .value = host_val });
        }

        try headers.append(allocator, .{ .name = "X-Amz-Date", .value = try allocator.dupe(u8, iso_date) });
        
        var payload_hash_buf: [64]u8 = undefined;
        const payload_hash = if (payload.len == 0)
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        else
            try hashSha256Hex(payload, &payload_hash_buf);

        try headers.append(allocator, .{ .name = "x-amz-content-sha256", .value = try allocator.dupe(u8, payload_hash) });

        if (self.session_token) |token| {
            try headers.append(allocator, .{ .name = "X-Amz-Security-Token", .value = try allocator.dupe(u8, token) });
        }

        // 3. Create Canonical Request
        var canonical_req = std.ArrayList(u8){};
        defer canonical_req.deinit(allocator);
        
        // Custom Writer for Unmanaged ArrayList with Error Propagation
        const WriterContext = struct {
            vtable: std.Io.Writer.VTable,
            writer: std.Io.Writer,
            list: *std.ArrayList(u8),
            allocator: std.mem.Allocator,
            last_error: ?anyerror = null,

            fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
                const ctx: *@This() = @fieldParentPtr("writer", w);
                const res = ctx.drainImpl(data, splat) catch |err| {
                    ctx.last_error = err;
                    return error.WriteFailed;
                };
                return res;
            }
            
            fn drainImpl(ctx: *@This(), data: []const []const u8, splat: usize) !usize {
                 if (data.len == 0) return 0;
                 var total: usize = 0;
                 // Write all but last
                 for (data[0 .. data.len - 1]) |chunk| {
                     try ctx.list.appendSlice(ctx.allocator, chunk);
                     total += chunk.len;
                 }
                 // Write last element splat times
                 const last = data[data.len - 1];
                 for (0..splat) |_| {
                     try ctx.list.appendSlice(ctx.allocator, last);
                     total += last.len;
                 }
                 return total;
            }

            pub fn print(ctx: *@This(), comptime fmt: []const u8, args: anytype) !void {
                 ctx.writer.print(fmt, args) catch |err| {
                     if (err == error.WriteFailed and ctx.last_error != null) return ctx.last_error.?;
                     return err;
                 };
            }
            
            pub fn writeAll(ctx: *@This(), bytes: []const u8) !void {
                ctx.writer.writeAll(bytes) catch |err| {
                     if (err == error.WriteFailed and ctx.last_error != null) return ctx.last_error.?;
                     return err;
                 };
            }
        };

        var ctx = WriterContext{
            .vtable = .{ .drain = WriterContext.drain },
            .writer = undefined,
            .list = &canonical_req,
            .allocator = allocator,
        };
        ctx.writer = .{
            .vtable = &ctx.vtable,
            .buffer = &.{},
        };

        // Method
        try ctx.print("{s}\n", .{method});
        
        // Canonical URI
        const path = uri.path.percent_encoded;
            
        if (path.len == 0) try ctx.writeAll("/\n") else try ctx.print("{s}\n", .{path});

        // Canonical Query String
        try ctx.writeAll("\n"); 

        // Canonical Headers
        var canonical_headers_list = try std.ArrayList(HeaderRef).initCapacity(allocator, headers.items.len);
        defer canonical_headers_list.deinit(allocator);

        for (headers.items) |h| {
            const trimmed_val = std.mem.trim(u8, h.value, " \t");
            try canonical_headers_list.append(allocator, .{ .name = h.name, .value = trimmed_val });
        }

        std.sort.pdq(HeaderRef, canonical_headers_list.items, {}, headerLessThan);

        var signed_headers = std.ArrayList(u8){};
        defer signed_headers.deinit(allocator);
        
        for (canonical_headers_list.items) |h| {
            var lower_name_buf: [128]u8 = undefined;
            const lower_name = std.ascii.lowerString(&lower_name_buf, h.name);
            
            try ctx.print("{s}:{s}\n", .{lower_name, h.value});
            
            if (signed_headers.items.len > 0) try signed_headers.append(allocator, ';');
            try signed_headers.appendSlice(allocator, lower_name);
        }
        try ctx.writeAll("\n"); 

        try ctx.print("{s}\n", .{signed_headers.items});
        try ctx.print("{s}", .{payload_hash});

        // 4. Create String to Sign
        const algorithm = "AWS4-HMAC-SHA256";
        const credential_scope = try std.fmt.allocPrint(allocator, "{s}/{s}/{s}/aws4_request", .{date_short, self.region, self.service});
        defer allocator.free(credential_scope);

        var canonical_req_hash_buf: [64]u8 = undefined;
        const canonical_req_hash = try hashSha256Hex(canonical_req.items, &canonical_req_hash_buf);

        const string_to_sign = try std.fmt.allocPrint(allocator, "{s}\n{s}\n{s}\n{s}", .{
            algorithm,
            iso_date,
            credential_scope,
            canonical_req_hash,
        });
        defer allocator.free(string_to_sign);

        // 5. Calculate Signature
        var k_secret = try std.ArrayList(u8).initCapacity(allocator, 4 + self.secret_key.len);
        defer k_secret.deinit(allocator);
        try k_secret.appendSlice(allocator, "AWS4");
        try k_secret.appendSlice(allocator, self.secret_key);

        var k_date: [32]u8 = undefined;
        try hmacSha256(k_secret.items, date_short, &k_date);

        var k_region: [32]u8 = undefined;
        try hmacSha256(&k_date, self.region, &k_region);

        var k_service: [32]u8 = undefined;
        try hmacSha256(&k_region, self.service, &k_service);

        var k_signing: [32]u8 = undefined;
        try hmacSha256(&k_service, "aws4_request", &k_signing);

        var signature: [32]u8 = undefined;
        try hmacSha256(&k_signing, string_to_sign, &signature);
        
        var signature_hex: [64]u8 = undefined;
        signature_hex = std.fmt.bytesToHex(signature, .lower);

        // 6. Add Authorization Header
        const auth_header = try std.fmt.allocPrint(allocator, 
            "{s} Credential={s}/{s}, SignedHeaders={s}, Signature={s}",
            .{algorithm, self.access_key, credential_scope, signed_headers.items, &signature_hex}
        );
        
        try headers.append(allocator, .{ .name = "Authorization", .value = auth_header });
    }
};

const HeaderRef = struct {
    name: []const u8,
    value: []const u8,
};

fn headerLessThan(context: void, lhs: HeaderRef, rhs: HeaderRef) bool {
    _ = context;
    return std.ascii.lessThanIgnoreCase(lhs.name, rhs.name);
}

fn fmtIso8601(ts: i64, buf: *[16]u8) ![]const u8 {
    const es = std.time.epoch.EpochSeconds{ .secs = @intCast(ts) };
    const day = es.getEpochDay();
    const yd = day.calculateYearDay();
    const md = yd.calculateMonthDay();
    
    const year_val = yd.year;
    const month_val = md.month.numeric();
    const day_val = md.day_index + 1;
    
    const day_seconds = es.getDaySeconds();
    const hour = day_seconds.getHoursIntoDay();
    const minute = day_seconds.getMinutesIntoHour();
    const second = day_seconds.getSecondsIntoMinute();

    return std.fmt.bufPrint(buf, "{d:0>4}{d:0>2}{d:0>2}T{d:0>2}{d:0>2}{d:0>2}Z", .{
        year_val, month_val, day_val, hour, minute, second
    });
}

fn hashSha256Hex(data: []const u8, out_hex: *[64]u8) ![]const u8 {
    var out: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(data, &out, .{});
    out_hex.* = std.fmt.bytesToHex(out, .lower);
    return out_hex;
}

fn hmacSha256(key: []const u8, data: []const u8, out: *[32]u8) !void {
    std.crypto.auth.hmac.sha2.HmacSha256.create(out, data, key);
}
