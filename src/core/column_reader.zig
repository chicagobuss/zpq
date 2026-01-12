const std = @import("std");
const schema = @import("schema.zig");
const thrift = @import("thrift.zig");
const io = @import("../io/interface.zig");
const rle = @import("rle.zig");
const decompress_mod = @import("decompress.zig");
const simd = @import("simd.zig");
const selection_mod = @import("selection.zig");
const column_batch = @import("column_batch.zig");

/// AnyColumnReader is a type-erased wrapper for specialized ColumnReaders.
pub const AnyColumnReader = union(enum) {
    bool: ColumnReader(bool),
    i32: ColumnReader(i32),
    i64: ColumnReader(i64),
    f32: ColumnReader(f32),
    f64: ColumnReader(f64),
    byte_array: ColumnReader([]const u8),
    int96: ColumnReader([12]u8),

    pub fn init(allocator: std.mem.Allocator, arena: *std.heap.ArenaAllocator, source: io.RandomAccessSource, meta: schema.ColumnMetaData, max_def: u8, max_rep: u8) !AnyColumnReader {
        return switch (meta.type) {
            .BOOLEAN => .{ .bool = ColumnReader(bool).init(allocator, arena, source, meta, max_def, max_rep) },
            .INT32 => .{ .i32 = ColumnReader(i32).init(allocator, arena, source, meta, max_def, max_rep) },
            .INT64 => .{ .i64 = ColumnReader(i64).init(allocator, arena, source, meta, max_def, max_rep) },
            .FLOAT => .{ .f32 = ColumnReader(f32).init(allocator, arena, source, meta, max_def, max_rep) },
            .DOUBLE => .{ .f64 = ColumnReader(f64).init(allocator, arena, source, meta, max_def, max_rep) },
            .BYTE_ARRAY => .{ .byte_array = ColumnReader([]const u8).init(allocator, arena, source, meta, max_def, max_rep) },
            .INT96 => .{ .int96 = ColumnReader([12]u8).init(allocator, arena, source, meta, max_def, max_rep) },
            .FIXED_LEN_BYTE_ARRAY => return error.UnsupportedType, // TODO: Special case for fixed len
        };
    }

    pub fn readBatchInto(self: *AnyColumnReader, col: *const column_batch.Column, count: usize, selection: *selection_mod.SelectionVector) !usize {
        return switch (self.*) {
            .bool => try self.bool.readBatch(col.data.bool[0..count], col.validity, selection),
            .i32 => try self.i32.readBatch(col.data.i32[0..count], col.validity, selection),
            .i64 => try self.i64.readBatch(col.data.i64[0..count], col.validity, selection),
            .f32 => try self.f32.readBatch(col.data.f32[0..count], col.validity, selection),
            .f64 => try self.f64.readBatch(col.data.f64[0..count], col.validity, selection),
            .byte_array => try self.byte_array.readBatch(col.data.byte_array[0..count], col.validity, selection),
            .int96 => return error.UnsupportedType, // TODO: Map to timestamp?
        };
    }
};

pub fn ColumnReader(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        arena: *std.heap.ArenaAllocator,

        // Parquet Metadata
        column_meta: schema.ColumnMetaData,
        physical_type: schema.Type,
        max_def: u8,
        max_rep: u8,

        // I/O State
        source: io.RandomAccessSource,
        chunk_offset: u64,
        chunk_size: u64,

        // State
        offset_in_chunk: u64 = 0,
        dictionary: ?[]const T = null,
        current_page: ?Page = null,
        bit_offset: u3 = 0, // Used for bit-packed booleans

        // Decoders
        def_decoder: ?rle.RleDecoder = null,
        rep_decoder: ?rle.RleDecoder = null,

        pub const Page = struct {
            header: schema.PageHeader,
            data: []const u8,
            values_read: u32 = 0,
            pos: usize = 0, // Current position in uncompressed data
            data_decoder: ?rle.RleDecoder = null,
        };

        pub fn init(allocator: std.mem.Allocator, arena: *std.heap.ArenaAllocator, source: io.RandomAccessSource, meta: schema.ColumnMetaData, max_def: u8, max_rep: u8) Self {
            const chunk_offset = if (meta.dictionary_page_offset) |offset| offset else meta.data_page_offset;
            return .{
                .allocator = allocator,
                .arena = arena,
                .column_meta = meta,
                .physical_type = meta.type,
                .max_def = max_def,
                .max_rep = max_rep,
                .source = source,
                .chunk_offset = @intCast(chunk_offset),
                .chunk_size = @intCast(meta.total_compressed_size),
            };
        }

        fn loadNextPage(self: *Self) !bool {
            if (self.offset_in_chunk >= self.chunk_size) return false;

            // 1. Read PageHeader
            var header_buf: [256]u8 = undefined;
            const n = try self.source.readAt(self.chunk_offset + self.offset_in_chunk, &header_buf);
            if (n == 0) return false;

            var reader = thrift.Reader.init(&header_buf);
            const header = try schema.PageHeader.read(&reader);
            const header_size = reader.pos;

            self.offset_in_chunk += header_size;

            // 2. Read Page Data
            const compressed_size: usize = @intCast(header.compressed_page_size);
            const uncompressed_size: usize = @intCast(header.uncompressed_page_size);

            const compressed_data = try self.allocator.alloc(u8, compressed_size);
            defer self.allocator.free(compressed_data);

            const read_n = try self.source.readAt(self.chunk_offset + self.offset_in_chunk, compressed_data);
            if (read_n < compressed_size) return error.UnexpectedEOF;

            self.offset_in_chunk += compressed_size;

            // 3. Decompress
            const alignment = comptime std.mem.Alignment.fromByteUnits(@alignOf(T));
            const uncompressed_data = try self.arena.allocator().alignedAlloc(u8, alignment, uncompressed_size);
            _ = try decompress_mod.decompress(self.column_meta.codec, compressed_data, uncompressed_data);

            if (header.type == .DICTIONARY_PAGE) {
                try self.loadDictionary(header, uncompressed_data);
                return try self.loadNextPage();
            }

            self.current_page = Page{
                .header = header,
                .data = uncompressed_data,
                .values_read = 0,
                .pos = 0,
            };
            self.bit_offset = 0;

            try self.initDecoders();
            return true;
        }

        fn loadDictionary(self: *Self, header: schema.PageHeader, uncompressed_data: []const u8) !void {
            const num_values: usize = @intCast(header.dictionary_page_header.?.num_values);
            if (T == []const u8) {
                const dict_array = try self.arena.allocator().alloc([]const u8, num_values);
                var pos: usize = 0;
                for (0..num_values) |i| {
                    if (pos + 4 > uncompressed_data.len) return error.DictionaryTruncated;
                    const len = std.mem.readInt(u32, uncompressed_data[pos..][0..4], .little);
                    pos += 4;
                    if (pos + len > uncompressed_data.len) return error.DictionaryTruncated;
                    dict_array[i] = uncompressed_data[pos..][0..len];
                    pos += len;
                }
                self.dictionary = dict_array;
            } else {
                const bytes_needed = num_values * @sizeOf(T);
                if (uncompressed_data.len < bytes_needed) return error.DictionaryTooSmall;
                const aligned_data: []align(@alignOf(T)) const u8 = @alignCast(uncompressed_data);
                self.dictionary = std.mem.bytesAsSlice(T, aligned_data[0..bytes_needed]);
            }
        }

        fn initDecoders(self: *Self) !void {
            var page = &self.current_page.?;
            var page_pos: usize = 0;

            if (self.max_def > 0) {
                const len = std.mem.readInt(u32, page.data[page_pos..][0..4], .little);
                page_pos += 4;
                const bit_width = std.math.log2_int_ceil(u8, self.max_def + 1);
                self.def_decoder = rle.RleDecoder.init(page.data[page_pos .. page_pos + len], bit_width);
                page_pos += len;
            }
            if (self.max_rep > 0) {
                const len = std.mem.readInt(u32, page.data[page_pos..][0..4], .little);
                page_pos += 4;
                const bit_width = std.math.log2_int_ceil(u8, self.max_rep + 1);
                self.rep_decoder = rle.RleDecoder.init(page.data[page_pos .. page_pos + len], bit_width);
                page_pos += len;
            }

            page.pos = page_pos;

            const enc = page.header.data_page_header.?.encoding;
            if (enc == .PLAIN_DICTIONARY or enc == .RLE_DICTIONARY) {
                if (page.pos >= page.data.len) return error.UnexpectedEOF;
                const bit_width = page.data[page.pos];
                page.pos += 1;
                if (page.pos < page.data.len) {
                    self.current_page.?.data_decoder = rle.RleDecoder.init(page.data[page.pos..], bit_width);
                }
            }
        }

        /// Decodes a batch of values into the provided slice.
        /// Respects the selection vector: only decodes into out[i] if selection.isActive(i).
        pub fn readBatch(self: *Self, out: []T, validity: ?[]u64, selection: *selection_mod.SelectionVector) !usize {
            var i: usize = 0;
            while (i < out.len) {
                // Ensure page is loaded
                if (self.current_page == null or self.current_page.?.values_read >= self.current_page.?.header.data_page_header.?.num_values) {
                    if (!try self.loadNextPage()) return i;
                }

                const page = &self.current_page.?;
                const num_values: u32 = @intCast(page.header.data_page_header.?.num_values);
                const remaining_in_page = num_values - page.values_read;
                const count = @min(out.len - i, @as(usize, @intCast(remaining_in_page)));

                // Handle NULLs and Selection
                // Handle NULLs and Selection (Fast Nullable Path)
                if (self.max_def > 0 or !selection.allSet(selection.len)) {
                    // 1. Decode Definition Levels for the whole batch range
                    // We alloc u64 to match RleDecoder.nextBatch signature, even though levels might fit in u8.
                    // RleDecoder.nextBatch decodes into u64.
                    const def_levels = try self.arena.allocator().alloc(u64, count);
                    // No defer free - arena will clean up.

                    if (self.max_def > 0) {
                         const n_defs = try self.def_decoder.?.nextBatch(def_levels);
                         if (n_defs != count) return error.UnexpectedEOF;
                    } else {
                        @memset(def_levels, 0); 
                    }

                    var processed: usize = 0;
                    while (processed < count) {
                        const start_idx = i + processed;
                        
                        // Classify start state
                        // State: 0=Null, 1=Present&Selected, 2=Present&Skipped
                        const is_present = (self.max_def == 0) or (def_levels[processed] == self.max_def);
                        const is_selected = selection.isActive(start_idx);
                        
                        const StartState = enum { Null, PresentSelected, PresentSkipped };
                        const start_state: StartState = if (!is_present) .Null 
                                                      else if (is_selected) .PresentSelected 
                                                      else .PresentSkipped;

                        // Find run length
                        var run_len: usize = 1;
                        while (processed + run_len < count) {
                            const cur_idx = i + processed + run_len;
                            const cur_present = (self.max_def == 0) or (def_levels[processed + run_len] == self.max_def);
                            const cur_selected = selection.isActive(cur_idx);
                            const cur_state: StartState = if (!cur_present) .Null 
                                                        else if (cur_selected) .PresentSelected 
                                                        else .PresentSkipped;
                            
                            if (cur_state != start_state) break;
                            run_len += 1;
                        }

                        // Execute Bulk Action
                        switch (start_state) {
                            .PresentSelected => {
                                // Read directly into output at correct positions
                                // This is tricky because `out` is contiguous but `out` indices might not be if we skipped some.
                                // Wait, `out` is the dense output buffer.
                                // If we have a run of PresentSelected, we write to `out[start_idx .. start_idx + run_len]`.
                                // AND we advance the data stream.
                                try self.readValuesBatch(out[start_idx..][0..run_len]);
                                if (validity) |v| {
                                    // Mark these as valid
                                    for (0..run_len) |k| setValidity(v, start_idx + k, true);
                                }
                            },
                            .PresentSkipped => {
                                // Skip values in data stream
                                try self.skipValues(run_len);
                                // Validity for these does not matter as they are filtered out
                                // but if we must be precise, we don't touch validity.
                            },
                            .Null => {
                                // Do nothing to data stream (no value exists)
                                // Write nulls to output (if needed) or just mark validity
                                if (validity) |v| {
                                    for (0..run_len) |k| setValidity(v, start_idx + k, false);
                                }
                                // Zero-init output for safety?
                                if (@typeInfo(T) != .pointer) {
                                     if (T == bool) {
                                         @memset(out[start_idx..][0..run_len], false);
                                     } else {
                                         @memset(out[start_idx..][0..run_len], 0); 
                                     }
                                }
                            },
                        }
                        processed += run_len;
                    }
                    
                    self.current_page.?.values_read += @intCast(count);
                    i += count;
                    continue;
                }

                // Super fast path: Required column + No Filter
                try self.readValuesBatch(out[i..i+count]);
                self.current_page.?.values_read += @intCast(count);
                i += count;
            }
            return i;
        }

        fn readValue(self: *Self) !T {
            const page = &self.current_page.?;
            const encoding = page.header.data_page_header.?.encoding;

            switch (encoding) {
                .PLAIN => {
                    if (T == bool) {
                        const byte = page.data[page.pos];
                        const val = ((byte >> self.bit_offset) & 1) == 1;
                        if (self.bit_offset == 7) {
                            self.bit_offset = 0;
                            page.pos += 1;
                        } else {
                            self.bit_offset += 1;
                        }
                        return val;
                    }

                    const size = @sizeOf(T);
                    if (page.pos + size > page.data.len) return error.UnexpectedEOF;

                    if (@typeInfo(T) == .int) {
                        const val = std.mem.readInt(T, page.data[page.pos..][0..size], .little);
                        page.pos += size;
                        return val;
                    } else if (@typeInfo(T) == .float) {
                        const IntT = std.meta.Int(.unsigned, @bitSizeOf(T));
                        const bits = std.mem.readInt(IntT, page.data[page.pos..][0..size], .little);
                        page.pos += size;
                        return @bitCast(bits);
                    } else if (T == []const u8) {
                        const len = std.mem.readInt(u32, page.data[page.pos..][0..4], .little);
                        page.pos += 4;
                        if (page.pos + len > page.data.len) return error.UnexpectedEOF;
                        const val = page.data[page.pos..][0..len];
                        page.pos += len;
                        return val;
                    } else {
                        @compileError("Unsupported type T: " ++ @typeName(T));
                    }
                },
                .PLAIN_DICTIONARY, .RLE_DICTIONARY => {
                    if (self.dictionary == null) return error.MissingDictionary;
                    var decoder = &(self.current_page.?.data_decoder.?);
                    const idx = try decoder.next() orelse return error.UnexpectedEOF;
                    if (idx >= self.dictionary.?.len) return error.InvalidDictionaryIndex;
                    return self.dictionary.?[idx];
                },
                else => return error.UnsupportedEncoding,
            }
        }

        fn readValuesBatch(self: *Self, out: []T) !void {
            const page = &self.current_page.?;
            const encoding = page.header.data_page_header.?.encoding;
            const count = out.len;

            switch (encoding) {
                .PLAIN => {
                    if (T == bool) {
                        for (0..count) |k| out[k] = try self.readValue();
                    } else if (T == []const u8) {
                        for (0..count) |k| out[k] = try self.readValue();
                    } else if (T == [12]u8) {
                        const byte_count = count * 12;
                        if (page.pos + byte_count > page.data.len) return error.UnexpectedEOF;
                        const src = page.data[page.pos..][0..byte_count];
                        const dest = std.mem.sliceAsBytes(out);
                        @memcpy(dest, src);
                        self.current_page.?.pos += byte_count;
                    } else {
                        // Bulk copy for fixed-width
                        const size = @sizeOf(T);
                        const byte_count = count * size;
                        if (page.pos + byte_count > page.data.len) return error.UnexpectedEOF;
                        const src = page.data[page.pos..][0..byte_count];
                        const dest = std.mem.sliceAsBytes(out);
                        @memcpy(dest, src);
                        self.current_page.?.pos += byte_count;
                    }
                },
                .PLAIN_DICTIONARY, .RLE_DICTIONARY => {
                    var decoder = &(self.current_page.?.data_decoder.?);
                    const n = try decoder.nextBatchT(T, out, self.dictionary);
                    if (n < count) return error.UnexpectedEOF;
                },
                else => return error.UnsupportedEncoding,
            }
        }


        fn skipValues(self: *Self, count: usize) !void {
            const page = &self.current_page.?;
            const encoding = page.header.data_page_header.?.encoding;

            switch (encoding) {
                .PLAIN => {
                     // Calculate bytes to skip
                     if (T == bool) {
                         // Bit offsets...
                         const total_bits = count;
                         // We are at bit_offset.
                         // new_bit_offset = bit_offset + count
                         const current_total_bits = page.pos * 8 + self.bit_offset;
                         const new_total = current_total_bits + total_bits;
                         page.pos = new_total / 8;
                         self.bit_offset = @intCast(new_total % 8);
                     } else if (T == []const u8) {
                         // Strings - must read lengths to find total skip bytes (slow skip)
                         for (0..count) |_| {
                             if (page.pos + 4 > page.data.len) return error.UnexpectedEOF;
                             const len = std.mem.readInt(u32, page.data[page.pos..][0..4], .little);
                             page.pos += 4 + len;
                         }
                     } else {
                         // Fixed width - fast skip
                         const size = if (T == [12]u8) 12 else @sizeOf(T);
                         page.pos += count * size;
                     }
                },
                .PLAIN_DICTIONARY, .RLE_DICTIONARY => {
                    var decoder = &(self.current_page.?.data_decoder.?);
                    try decoder.skip(@intCast(count));
                },
                else => return error.UnsupportedEncoding,
            }
        }

        inline fn setValidity(mask: []u64, index: usize, active: bool) void {
            const word_idx = index / 64;
            const bit_idx: u6 = @intCast(index % 64);
            if (active) {
                mask[word_idx] |= (@as(u64, 1) << bit_idx);
            } else {
                mask[word_idx] &= ~(@as(u64, 1) << bit_idx);
            }
        }
    };
}

test "column reader plain int32" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    // Mock meta
    const meta = schema.ColumnMetaData{
        .type = .INT32,
        .encodings = .{},
        .path_in_schema = .{},
        .codec = .UNCOMPRESSED,
        .num_values = 4,
        .total_uncompressed_size = 16,
        .total_compressed_size = 16,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
    };

    // Use null for source as we are manually setting up the page
    var reader = ColumnReader(i32).init(allocator, &arena, undefined, meta, 0, 0);
    
    // Manually set up a page
    const data = try arena.allocator().alloc(u8, 16);
    std.mem.writeInt(i32, data[0..4], 10, .little);
    std.mem.writeInt(i32, data[4..8], 20, .little);
    std.mem.writeInt(i32, data[8..12], 30, .little);
    std.mem.writeInt(i32, data[12..16], 40, .little);

    reader.current_page = .{
        .header = .{
            .type = .DATA_PAGE,
            .uncompressed_page_size = 16,
            .compressed_page_size = 16,
            .crc = null,
            .data_page_header = .{
                .num_values = 4,
                .encoding = .PLAIN,
                .definition_level_encoding = .PLAIN,
                .repetition_level_encoding = .PLAIN,
            },
            .dictionary_page_header = null,
        },
        .data = data,
        .values_read = 0,
        .pos = 0,
        .data_decoder = null,
    };

    var out: [4]i32 = undefined;
    var selection = try selection_mod.SelectionVector.init(allocator, 4);
    defer selection.deinit();

    const n = try reader.readBatch(&out, null, &selection);
    try std.testing.expectEqual(@as(usize, 4), n);
    try std.testing.expectEqual(@as(i32, 10), out[0]);
    try std.testing.expectEqual(@as(i32, 20), out[1]);
    try std.testing.expectEqual(@as(i32, 30), out[2]);
    try std.testing.expectEqual(@as(i32, 40), out[3]);
}

test "column reader nullable skipped" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    // Mock meta
    // Values: [10, NULL, 20, 30, NULL, 40]
    // Def levels: [1, 0, 1, 1, 0, 1]
    const meta = schema.ColumnMetaData{
        .type = .INT32,
        .encodings = .{},
        .path_in_schema = .{},
        .codec = .UNCOMPRESSED,
        .num_values = 6,
        .total_uncompressed_size = 32,
        .total_compressed_size = 32,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
    };

    var reader = ColumnReader(i32).init(allocator, &arena, undefined, meta, 1, 0);
    
    // Manually set up a page with nullable data
    const page_data = try arena.allocator().alloc(u8, 100);
    var pos: usize = 0;
    
    // Def level length (4 bytes)
    std.mem.writeInt(u32, page_data[pos..][0..4], 2, .little); // 2 bytes of RLE data
    pos += 4;
    
    // RLE Header for literals: (1 << 1) | 1 = 3 (1 group of 8 literals)
    // Even though we only need 6, RLE works in groups of 8 for bit-packing
    page_data[pos] = 0x03; 
    pos += 1;
    // Def levels (1 byte): 1, 0, 1, 1, 0, 1, 0, 0 -> 0b00101101 = 0x2D
    page_data[pos] = 0x2D; 
    pos += 1;
    
    std.mem.writeInt(i32, page_data[pos..][0..4], 10, .little); pos += 4;
    std.mem.writeInt(i32, page_data[pos..][0..4], 20, .little); pos += 4;
    std.mem.writeInt(i32, page_data[pos..][0..4], 30, .little); pos += 4;
    std.mem.writeInt(i32, page_data[pos..][0..4], 40, .little); pos += 4;
    
    reader.current_page = .{
        .header = .{
            .type = .DATA_PAGE,
            .uncompressed_page_size = @intCast(pos),
            .compressed_page_size = @intCast(pos),
            .crc = null,
            .data_page_header = .{
                .num_values = 6,
                .encoding = .PLAIN,
                .definition_level_encoding = .RLE, 
                .repetition_level_encoding = .PLAIN,
            },
            .dictionary_page_header = null,
        },
        .data = page_data[0..pos],
        .values_read = 0,
        .pos = 0, 
        .data_decoder = null,
    };
    try reader.initDecoders();

    var out: [6]i32 = undefined;
    var validity = try arena.allocator().alloc(u64, 1);
    @memset(validity, 0);
    
    var selection = try selection_mod.SelectionVector.init(allocator, 6);
    defer selection.deinit();
    
    // Select: 0, 2, 4. Skip: 1, 3, 5.
    selection.clearAll();
    selection.set(0, true);
    selection.set(2, true);
    selection.set(4, true);
    
    const n = try reader.readBatch(&out, validity, &selection);
    
    try std.testing.expectEqual(@as(usize, 6), n);
    
    // Check Index 0: 10
    try std.testing.expectEqual(@as(i32, 10), out[0]);
    try std.testing.expect((validity[0] & 1) != 0);

    // Check Index 2: 20
    try std.testing.expectEqual(@as(i32, 20), out[2]);
    try std.testing.expect((validity[0] & (1<<2)) != 0);
    
    // Check Index 4: NULL
    try std.testing.expect((validity[0] & (1<<4)) == 0);
    
    // Ensure we consumed everything correctly
    try std.testing.expectEqual(@as(u32, 6), reader.current_page.?.values_read);
}
