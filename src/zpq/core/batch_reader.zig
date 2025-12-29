const std = @import("std");
const zpq = @import("../../zpq.zig");
const schema = zpq.schema;
const ColumnReader = zpq.column.ColumnReader;
const RleDecoder = zpq.rle.RleDecoder;
const simd = @import("simd.zig");

pub fn BatchReader(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        column_reader: ColumnReader,
        
        // Current state
        current_page: ?zpq.column.Page = null,
        values_remaining_in_page: usize = 0,
        
        // Page decoders
        rle_decoder: ?RleDecoder = null,
        def_levels_decoder: ?RleDecoder = null,
        
        // Dictionary (if any)
        dictionary: ?[]const T = null,
        
        // Metadata
        max_def_level: u16 = 0,
        column_type: schema.Type,

        pub fn init(allocator: std.mem.Allocator, column_reader: ColumnReader, column_type: schema.Type, max_def_level: u16) Self {
            return Self{
                .allocator = allocator,
                .column_reader = column_reader,
                .column_type = column_type,
                .max_def_level = max_def_level,
            };
        }

        pub fn deinit(self: *Self) void {
            if (self.current_page) |*p| p.deinit(self.allocator);
            if (self.dictionary) |d| {
                if (T == []const u8) {
                    for (d) |s| self.allocator.free(s);
                }
                self.allocator.free(d);
            }
        }

        pub fn nextBatch(self: *Self, buffer: []?T) !usize {
            var out_pos: usize = 0;
            while (out_pos < buffer.len) {
                if (self.values_remaining_in_page == 0) {
                    if (!try self.loadNextPage()) break;
                }

                const count = @min(buffer.len - out_pos, self.values_remaining_in_page);
                
                // Vectorized path for non-nullable dictionary-encoded columns
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

                // Fallback to scalar
                for (0..count) |_| {
                    buffer[out_pos] = try self.nextValue();
                    out_pos += 1;
                    self.values_remaining_in_page -= 1;
                }
            }
            return out_pos;
        }

        fn loadNextPage(self: *Self) !bool {
            if (self.current_page) |*p| {
                p.deinit(self.allocator);
                self.current_page = null;
            }

            while (try self.column_reader.next(self.allocator)) |const_page| {
                var page = const_page;
                if (page.header.type == .DICTIONARY_PAGE) {
                    try self.loadDictionary(page);
                    page.deinit(self.allocator);
                    continue;
                }

                if (page.header.type == .DATA_PAGE) {
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
                    const val = try decoder.readByteArray();
                    const owned = try self.allocator.dupe(u8, val);
                    try items.append(self.allocator, owned);
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
            if (self.dictionary) |d| self.allocator.free(d);
            self.dictionary = try items.toOwnedSlice(self.allocator);
        }

        fn initPageDecoders(self: *Self, page: zpq.column.Page) !void {
            const dph = page.header.data_page_header.?;
            var data_slice = page.data;

            // Skip repetition levels (not supported yet)
            // if (max_rep > 0) ...

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

            // Data
            if (dph.encoding == .RLE_DICTIONARY or dph.encoding == .PLAIN_DICTIONARY) {
                if (data_slice.len > 0) {
                    const bit_width = data_slice[0];
                    self.rle_decoder = RleDecoder.init(data_slice[1..], bit_width);
                }
            } else {
                self.rle_decoder = null; // PLAIN or other
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

            // PLAIN fallback
            // TODO: implement PLAIN decoding from current_page.data
            return error.UnsupportedEncoding;
        }
    };
}
