const std = @import("std");
const zpq = @import("../../zpq.zig");
const schema = zpq.schema;
const ColumnReader = zpq.column.ColumnReader;
const RleDecoder = zpq.rle.RleDecoder;
const Decoder = zpq.decoder.Decoder;
const simd = @import("simd.zig");

pub fn BatchReader(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        column_reader: ColumnReader,
        
        // Current state
        current_page: ?zpq.column.Page = null,
        active_pages: std.ArrayListUnmanaged(zpq.column.Page) = .{},
        values_remaining_in_page: usize = 0,
        
        // Page decoders
        rle_decoder: ?RleDecoder = null,
        plain_decoder: ?Decoder = null,
        def_levels_decoder: ?RleDecoder = null,
        
        // Dictionary (if any)
        dictionary: ?[]const T = null,
        
        // Metadata
        max_def_level: u16 = 0,
        max_rep_level: u16 = 0,
        column_type: schema.Type,
        type_length: ?i32 = null,

        pub fn init(allocator: std.mem.Allocator, column_reader: ColumnReader, column_type: schema.Type, max_def_level: u16, max_rep_level: u16, type_length: ?i32) Self {
            return Self{
                .allocator = allocator,
                .column_reader = column_reader,
                .column_type = column_type,
                .max_def_level = max_def_level,
                .max_rep_level = max_rep_level,
                .type_length = type_length,
            };
        }

        pub fn deinit(self: *Self) void {
            for (self.active_pages.items) |*p| p.deinit(self.allocator);
            self.active_pages.deinit(self.allocator);
            if (self.dictionary) |d| {
                if (T == []const u8) {
                    for (d) |s| self.allocator.free(s);
                }
                self.allocator.free(d);
            }
        }

        pub fn nextBatch(self: *Self, buffer: []?T) !usize {
            // Free pages from PREVIOUS batch, except the one we are currently reading from
            var page_idx: usize = 0;
            while (page_idx < self.active_pages.items.len) {
                var p = &self.active_pages.items[page_idx];
                // If this is the current page, keep it
                if (self.current_page != null and p.data.ptr == self.current_page.?.data.ptr) {
                    page_idx += 1;
                    continue;
                }
                p.deinit(self.allocator);
                _ = self.active_pages.swapRemove(page_idx);
            }

            var out_pos: usize = 0;
            while (out_pos < buffer.len) {
                if (self.values_remaining_in_page == 0) {
                    if (!try self.loadNextPage()) break;
                }

                const count = @min(buffer.len - out_pos, self.values_remaining_in_page);
                
                // --- VECTORIZED PATHS ---
                
                // 1. Non-nullable Dictionary-encoded
                if (self.max_def_level == 0 and self.rle_decoder != null and self.dictionary != null) {
                    var indices: [1024]u64 = undefined;
                    const to_read = @min(count, indices.len);
                    const n = try self.rle_decoder.?.nextBatch(indices[0..to_read]);
                    
                    const dict = self.dictionary.?;
                    for (0..n) |i| {
                        buffer[out_pos + i] = dict[indices[i]];
                    }
                    out_pos += n;
                    self.values_remaining_in_page -= n;
                    continue;
                }

                // 2. Non-nullable PLAIN-encoded (primitives)
                if (self.max_def_level == 0 and self.plain_decoder != null and T != []const u8) {
                    var values_buf: [1024]T = undefined;
                    const to_read = @min(count, values_buf.len);
                    const n = try self.plain_decoder.?.readBatch(values_buf[0..to_read]);
                    if (n == 0) break;
                    
                    for (0..n) |i| {
                        buffer[out_pos + i] = values_buf[i];
                    }
                    out_pos += n;
                    self.values_remaining_in_page -= n;
                    continue;
                }

                // 3. Nullable Dictionary-encoded
                if (self.max_def_level > 0 and self.def_levels_decoder != null and self.rle_decoder != null and self.dictionary != null) {
                    // Use expandNullsBatch8 logic
                    var batch_size: usize = 0;
                    while (batch_size < count) {
                        const batch_rem = @min(8, count - batch_size);
                        var def_levels: [8]u64 = undefined;
                        const n_def = try self.def_levels_decoder.?.nextBatch(def_levels[0..batch_rem]);
                        if (n_def == 0) break;

                        // Identify how many values we actually need to pull from data stream
                        var mask: u8 = 0;
                        var values_needed: u8 = 0;
                        for (0..n_def) |i| {
                            if (def_levels[i] == self.max_def_level) {
                                mask |= (@as(u8, 1) << @intCast(i));
                                values_needed += 1;
                            }
                        }

                        if (values_needed > 0) {
                            var indices: [8]u64 = undefined;
                            const n_idx = try self.rle_decoder.?.nextBatch(indices[0..values_needed]);
                            if (n_idx < values_needed) return error.EndOfStream;

                            // Map indices to dictionary values
                            var compact_vals: [8]T = undefined;
                            const dict = self.dictionary.?;
                            for (0..n_idx) |i| {
                                compact_vals[i] = dict[indices[i]];
                            }

                            // Expand into the output buffer
                            var tmp_out: [8]?T = undefined;
                            _ = simd.expandNullsBatch8(T, compact_vals[0..n_idx], mask, &tmp_out);
                            @memcpy(buffer[out_pos + batch_size .. out_pos + batch_size + n_def], tmp_out[0..n_def]);
                        } else {
                            @memset(buffer[out_pos + batch_size .. out_pos + batch_size + n_def], null);
                        }
                        
                        batch_size += n_def;
                        self.values_remaining_in_page -= n_def;
                    }
                    out_pos += batch_size;
                    continue;
                }

                // --- SCALAR FALLBACK ---
                for (0..count) |_| {
                    buffer[out_pos] = try self.nextValue();
                    out_pos += 1;
                    self.values_remaining_in_page -= 1;
                }
            }
            return out_pos;
        }

        fn loadNextPage(self: *Self) !bool {
            self.current_page = null;

            while (try self.column_reader.next(self.allocator)) |const_page| {
                var page = const_page;
                if (page.header.type == .DICTIONARY_PAGE) {
                    try self.loadDictionary(page);
                    page.deinit(self.allocator);
                    continue;
                }

                if (page.header.type == .DATA_PAGE) {
                    try self.active_pages.append(self.allocator, page);
                    self.current_page = page;
                    const dph = page.header.data_page_header.?;
                    self.values_remaining_in_page = @intCast(dph.num_values);
                    try self.initPageDecoders(page);
                    return true;
                }
                
                page.deinit(self.allocator);
            }
            return false;
        }

        fn loadDictionary(self: *Self, page: zpq.column.Page) !void {
            var decoder = zpq.decoder.Decoder.init(page.data);
            var items = std.ArrayListUnmanaged(T){};
            errdefer items.deinit(self.allocator);

            while (decoder.hasMore()) {
                if (T == []const u8) {
                    const val = if (self.column_type == .FIXED_LEN_BYTE_ARRAY)
                        try decoder.readFixedLenByteArray(@intCast(self.type_length.?))
                    else
                        try decoder.readByteArray();
                    const owned = try self.allocator.dupe(u8, val);
                    try items.append(self.allocator, owned);
                } else if (T == [12]u8) {
                    try items.append(self.allocator, try decoder.readInt96());
                } else if (T == i32) {
                    try items.append(self.allocator, @intCast(try decoder.readInt32()));
                } else if (T == i64 or T == u64) {
                    try items.append(self.allocator, @intCast(try decoder.readInt64()));
                } else if (T == f32) {
                    try items.append(self.allocator, try decoder.readFloat());
                } else if (T == f64) {
                    try items.append(self.allocator, try decoder.readDouble());
                } else {
                    return error.UnsupportedTypeForDictionary;
                }
            }
            if (self.dictionary) |d| {
                if (T == []const u8) for (d) |s| self.allocator.free(s);
                self.allocator.free(d);
            }
            self.dictionary = try items.toOwnedSlice(self.allocator);
        }

        fn initPageDecoders(self: *Self, page: zpq.column.Page) !void {
            const dph = page.header.data_page_header.?;
            var data_slice = page.data;

            // Repetition levels
            if (self.max_rep_level > 0) {
                if (data_slice.len < 4) return error.MalformedPage;
                const len = std.mem.readInt(u32, data_slice[0..4], .little);
                if (data_slice.len < 4 + len) return error.MalformedPage;
                // Skipping repetition levels for now as ZPQ doesn't support nested arrays
                data_slice = data_slice[4 + len ..];
            }

            // Definition levels
            if (self.max_def_level > 0) {
                if (data_slice.len < 4) return error.MalformedPage;
                const len = std.mem.readInt(u32, data_slice[0..4], .little);
                if (data_slice.len < 4 + len) return error.MalformedPage;
                const def_level_data = data_slice[4 .. 4 + len];
                data_slice = data_slice[4 + len ..];

                const bit_width = std.math.log2_int(u32, try std.math.ceilPowerOfTwo(u32, @as(u32, self.max_def_level) + 1));
                self.def_levels_decoder = RleDecoder.init(def_level_data, @intCast(bit_width));
            } else {
                self.def_levels_decoder = null;
            }

            // Data Encodings
            if (dph.encoding == .RLE_DICTIONARY or dph.encoding == .PLAIN_DICTIONARY) {
                if (data_slice.len > 0) {
                    const bit_width = data_slice[0];
                    self.rle_decoder = RleDecoder.init(data_slice[1..], bit_width);
                    self.plain_decoder = null;
                }
            } else if (dph.encoding == .PLAIN) {
                self.plain_decoder = Decoder.init(data_slice);
                self.rle_decoder = null;
            } else {
                return error.UnsupportedEncoding;
            }
        }

        fn decodePlainScalar(self: *Self) !T {
            const dec = &self.plain_decoder.?;
            if (T == []const u8) {
                if (self.column_type == .FIXED_LEN_BYTE_ARRAY) {
                    return try dec.readFixedLenByteArray(@intCast(self.type_length.?));
                }
                return try dec.readByteArray();
            } else if (T == [12]u8) {
                return try dec.readInt96();
            } else if (T == i32) {
                return @intCast(try dec.readInt32());
            } else if (T == i64 or T == u64) {
                return @intCast(try dec.readInt64());
            } else if (T == f32) {
                return try dec.readFloat();
            } else if (T == f64) {
                return try dec.readDouble();
            } else {
                return error.UnsupportedTypeForPlain;
            }
        }

        fn nextValue(self: *Self) !?T {
            if (self.max_def_level > 0) {
                const dl = (try self.def_levels_decoder.?.next()) orelse return null;
                if (dl < self.max_def_level) return null;
            }

            if (self.rle_decoder) |*rd| {
                const idx = (try rd.next()) orelse return error.EndOfStream;
                if (self.dictionary) |dict| {
                    if (idx >= dict.len) return error.InvalidDictionaryIndex;
                    return dict[idx];
                }
                return error.MissingDictionary;
            }

            if (self.plain_decoder != null) {
                return try self.decodePlainScalar();
            }

            return error.UnsupportedEncoding;
        }
    };
}
