const std = @import("std");
const schema = @import("schema.zig");
const thrift = @import("thrift.zig");
const io = @import("../io/interface.zig");
const rle = @import("rle.zig");
const file_mod = @import("file.zig");
const filter_mod = @import("filter.zig");
const selection_mod = @import("selection.zig");

/// ColumnReader is a specialized, monomorphized reader for a specific Zig type T.
/// It uses comptime reflection to generate the optimal decoding path for T
/// based on the Parquet physical type and encoding.
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

            // For RLE/dictionary encoded data
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
            const decompress_mod = @import("decompress.zig");
            const alignment = comptime std.mem.Alignment.fromByteUnits(@alignOf(T));
            const uncompressed_data = try self.arena.allocator().alignedAlloc(u8, alignment, uncompressed_size);
            _ = try decompress_mod.decompress(self.column_meta.codec, compressed_data, uncompressed_data);

            if (header.type == .DICTIONARY_PAGE) {
                const num_values: usize = @intCast(header.dictionary_page_header.?.num_values);
                
                if (T == []const u8) {
                     // Alloc array of slices
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
                    // Fixed width types (Int/Float)
                    const bytes_needed = num_values * @sizeOf(T);
                    if (uncompressed_data.len < bytes_needed) return error.DictionaryTooSmall;

                    // Align and cast dictionary data
                    const dict_slice = std.mem.bytesAsSlice(T, uncompressed_data[0..bytes_needed]);
                    self.dictionary = dict_slice;
                }

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

        fn initDecoders(self: *Self) !void {
            var page = &self.current_page.?;
            var page_pos: usize = 0; // This is the offset into `page.data` for the current page's decoders

            // Initialize level decoders
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

            page.pos = page_pos; // Update page.pos to where actual data starts

            // Initialize data decoder for Dictionary encoding
            const enc = page.header.data_page_header.?.encoding;
            if (enc == .PLAIN_DICTIONARY or enc == .RLE_DICTIONARY) {
                if (page.pos >= page.data.len) return error.UnexpectedEOF;

                // Read bit width (1 byte)
                const bit_width = page.data[page.pos];
                page.pos += 1;
                // std.debug.print("initDecoders: Page {d} - Encoding {s}, bit_width={d}, data_len={d}, pos={d}\n", 
                //    .{self.current_page.?.values_read, @tagName(enc), bit_width, page.data.len, page.pos});

                // Initialize RLE decoder for indices
                if (page.pos < page.data.len) {
                    self.current_page.?.data_decoder = rle.RleDecoder.init(page.data[page.pos..], bit_width);
                }
            }
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
                        @compileError("Unsupported type T for PLAIN decoding: " ++ @typeName(T));
                    }
                },
                .PLAIN_DICTIONARY, .RLE_DICTIONARY => {
                    if (self.dictionary == null) return error.MissingDictionary;
                    var decoder = &(self.current_page.?.data_decoder.?);
                    const idx = try decoder.next() orelse return error.UnexpectedEOF;

                    if (idx >= self.dictionary.?.len) return error.InvalidDictionaryIndex;
                    return self.dictionary.?[idx];
                },
                else => {
                    return error.UnsupportedEncoding;
                },
            }
        }

        /// Reads the next value from the column, decoding and decompressing as needed.
        pub fn next(self: *Self) !?T {
            if (self.current_page == null or self.current_page.?.values_read >= self.current_page.?.header.data_page_header.?.num_values) {
                if (!try self.loadNextPage()) return null;
            }

            self.current_page.?.values_read += 1;

            if (self.max_def > 0) {
                const def = try self.def_decoder.?.next() orelse 0;
                if (def < self.max_def) return null;
            }

            return try self.readValue();
        }

        /// Resets the current page to the beginning (rewinds).
        /// Used after evaluating filters to allow re-reading for projection.
        pub fn resetPage(self: *Self) void {
            if (self.current_page) |*page| {
                page.pos = 0;
                page.values_read = 0;
                self.bit_offset = 0; // Reset bit offset for booleans
                self.initDecoders() catch {}; // Should not fail on valid data already loaded
            }
        }

        /// Skips the next `n` values.
        pub fn skip(self: *Self, n: usize) !usize {
            var skipped: usize = 0;
            while (skipped < n) {
                // Ensure page is loaded
                while (self.current_page == null or self.current_page.?.values_read >= self.current_page.?.header.data_page_header.?.num_values) {
                    if (!try self.loadNextPage()) return skipped;
                }

                const page = &self.current_page.?;
                const num_values: u32 = @intCast(page.header.data_page_header.?.num_values);
                const remaining_in_page = num_values - page.values_read;
                const count = @min(n - skipped, @as(usize, @intCast(remaining_in_page)));

                // Handle Levels
                var non_null_count: usize = count;
                if (self.max_def > 0) {
                    // We must read definition levels to know how many actual data values to skip
                    non_null_count = 0;
                    var k: usize = 0;
                    while (k < count) : (k += 1) {
                        const def = try self.def_decoder.?.next() orelse break; // Should not break if logic is correct
                        if (def == self.max_def) {
                            non_null_count += 1;
                        }
                    }
                }

                if (non_null_count > 0) {
                    switch (page.header.data_page_header.?.encoding) {
                        .PLAIN => {
                            if (T == bool) {
                                // Skip bits
                                const total_bits = @as(usize, self.bit_offset) + non_null_count;
                                page.pos += total_bits / 8;
                                self.bit_offset = @intCast(total_bits % 8);
                            } else if (T == []const u8) {
                                // PLAIN strings are length-prefixed
                                for (0..non_null_count) |_| {
                                    if (page.pos + 4 > page.data.len) return error.UnexpectedEOF;
                                    const len = std.mem.readInt(u32, page.data[page.pos..][0..4], .little);
                                    page.pos += 4 + len;
                                }
                            } else {
                                const size = @sizeOf(T);
                                page.pos += non_null_count * size;
                            }
                        },
                        .PLAIN_DICTIONARY, .RLE_DICTIONARY => {
                            var decoder = &(self.current_page.?.data_decoder.?);
                            // RleDecoder doesn't strictly have a 'skip' (yet), so we next() in loop
                            // TODO: Add skip to RleDecoder
                            var k: usize = 0;
                            while (k < non_null_count) : (k += 1) {
                                _ = try decoder.next();
                            }
                        },
                        else => return error.UnsupportedEncoding,
                    }
                }

                page.values_read += @intCast(count);
                skipped += count;
            }
            return skipped;
        }

        /// Decodes a batch of values into the provided slice.
        pub fn nextBatch(self: *Self, out: []T) !usize {
            var i: usize = 0;
            while (i < out.len) {
                // Ensure page is loaded
                while (self.current_page == null or self.current_page.?.values_read >= self.current_page.?.header.data_page_header.?.num_values) {
                    if (!try self.loadNextPage()) return i;
                }

                const page = &self.current_page.?;
                const encoding = page.header.data_page_header.?.encoding;
                const num_values: u32 = @intCast(page.header.data_page_header.?.num_values);
                const remaining_in_page = num_values - page.values_read;
                const count = @min(out.len - i, @as(usize, @intCast(remaining_in_page)));

                // Handle NULLs if necessary
                if (self.max_def > 0) {
                    // Slow path for now: decode one by one
                    // TODO: Optimize RLE batch decoding for levels
                    var k: usize = 0;
                    while (k < count) : (k += 1) {
                        const def = try self.def_decoder.?.next() orelse break;
                        if (def == self.max_def) {
                            out[i + k] = try self.readValue();
                        } else {
                            // Zero-init for NULLs? Or allow caller to handle?
                            // For now we assume caller handles validity bitmap separately if needed.
                            // But here we are returning values, how do we signify NULL?
                            // Standard practice: return values, validity bitmap separate.
                            // But this signature `out: []T` assumes no NULLs or specific sentinel.
                            // Let's stick to simple loop calling next() for nullable columns for now.
                            out[i + k] = (try self.next()) orelse continue; // Skip NULL? No, next() returns ?T
                            // If next() returns null, it means it was a NULL value.
                            // Use next() directly for correct behavior.
                            break;
                        }
                    }
                    // Fallback to strict loop if implemented this way
                    const start = i;
                    while (i < start + count) {
                        if (try self.next()) |val| {
                            out[i] = val;
                            i += 1;
                        } else {
                            // It's a NULL. If T can't represent NULL, what do we do?
                            // For now, we just skip incrementing i? No, that would shift data.
                            // We probably need a validity bitmap output for nextBatch.
                            // Leaving as is: specific specialized batch read loop for Required columns only.
                            return error.BatchReadNotSupportedForNullable;
                        }
                    }
                    continue;
                }

                // Fast path for Required columns (max_def == 0)
                switch (encoding) {
                    .PLAIN => {
                        if (T == bool) {
                            // Bit-packed booleans
                            var k: usize = 0;
                            while (k < count) : (k += 1) {
                                out[i + k] = try self.readValue();
                            }
                            page.values_read += @intCast(count);
                        } else {
                            // Bulk copy
                            const size = @sizeOf(T);
                            const byte_count = count * size;
                            if (page.pos + byte_count > page.data.len) return error.UnexpectedEOF;

                            const src = page.data[page.pos..][0..byte_count];
                            const dest = std.mem.sliceAsBytes(out[i..][0..count]);
                            @memcpy(dest, src);

                            page.pos += byte_count;
                            page.values_read += @intCast(count);
                        }
                    },
                    .PLAIN_DICTIONARY, .RLE_DICTIONARY => {
                        // Decode 'count' indices
                        var k: usize = 0;
                        var decoder = &(self.current_page.?.data_decoder.?);
                        while (k < count) : (k += 1) {
                            const idx = try decoder.next() orelse return error.UnexpectedEOF;
                            if (idx >= self.dictionary.?.len) return error.InvalidDictionaryIndex;
                            out[i + k] = self.dictionary.?[idx];
                        }
                        page.values_read += @intCast(count);
                    },
                    else => return error.UnsupportedEncoding,
                }
                i += count;
            }
            return i;
        }

        /// Evaluates a filter against the next batch of values, updating the selection vector.
        pub fn evaluate(self: *Self, filter: filter_mod.Filter, selection: *selection_mod.SelectionVector) !void {
            const pivot: T = switch (filter) {
                .Int32 => |f| if (T == i32) f.val else return error.TypeMismatch,
                .Int64 => |f| if (T == i64) f.val else return error.TypeMismatch,
                .Float => |f| if (T == f32) f.val else return error.TypeMismatch,
                .Double => |f| if (T == f64) f.val else return error.TypeMismatch,
                .Bool => |f| if (T == bool) f.val else return error.TypeMismatch,
                .ByteArray => |f| if (T == []const u8) f.val else return error.TypeMismatch,
            };
            const pred = switch (filter) {
                inline else => |f| f.pred,
            };

            var i: usize = 0;
            while (i < selection.len) {
                // Ensure page is loaded
                while (self.current_page == null or self.current_page.?.values_read >= self.current_page.?.header.data_page_header.?.num_values) {
                    if (!try self.loadNextPage()) {
                        // EOF reached. Mark remaining rows as invalid.
                        while (i < selection.len) : (i += 1) {
                            selection.set(i, false);
                        }
                        return;
                    }
                }

                const page = &self.current_page.?;
                const num_values: u32 = @intCast(page.header.data_page_header.?.num_values);
                const remaining_in_page = num_values - page.values_read;
                const count = @min(selection.len - i, @as(usize, @intCast(remaining_in_page)));

                // Handle Levels if nullable (max_def > 0)
                if (self.max_def > 0) {
                    var k: usize = 0;
                    while (k < count) : (k += 1) {
                        const def = try self.def_decoder.?.next() orelse break;
                        if (def == self.max_def) {
                            const val = try self.readValue();
                            const match = checkPredicate(val, pred, pivot);
                            selection.set(i + k, match);
                        } else {
                            selection.set(i + k, false);
                        }
                    }
                    page.values_read += @intCast(k);
                    i += k;
                    continue;
                }

                // Required columns (max_def == 0)
                switch (page.header.data_page_header.?.encoding) {
                    .PLAIN => {
                        var k: usize = 0;
                        while (k < count) : (k += 1) {
                            const val = try self.readValue();
                            const match = checkPredicate(val, pred, pivot);
                            selection.set(i + k, match);
                        }
                        page.values_read += @intCast(count);
                    },
                    .PLAIN_DICTIONARY, .RLE_DICTIONARY => {
                        var decoder = &(self.current_page.?.data_decoder.?);
                        var k: usize = 0;
                        while (k < count) : (k += 1) {
                            const idx = try decoder.next() orelse return error.UnexpectedEOF;
                            if (idx >= self.dictionary.?.len) return error.InvalidDictionaryIndex;
                            const val = self.dictionary.?[idx];
                            const match = checkPredicate(val, pred, pivot);
                            selection.set(i + k, match);
                        }
                        page.values_read += @intCast(count);
                    },
                    else => return error.UnsupportedEncoding,
                }
                i += count;
            }
        }

        inline fn checkPredicate(val: T, pred: filter_mod.Predicate, pivot: T) bool {
            if (T == bool) {
                const i_val = @intFromBool(val);
                const i_pivot = @intFromBool(pivot);
                return switch (pred) {
                    .Eq => val == pivot,
                    .Neq => val != pivot,
                    .Gt => i_val > i_pivot,
                    .Lt => i_val < i_pivot,
                    .Gte => i_val >= i_pivot,
                    .Lte => i_val <= i_pivot,
                };
            }
            if (T == []const u8) {
                return switch (pred) {
                    .Eq => std.mem.eql(u8, val, pivot),
                    .Neq => !std.mem.eql(u8, val, pivot),
                    .Gt => std.mem.order(u8, val, pivot) == .gt,
                    .Lt => std.mem.order(u8, val, pivot) == .lt,
                    .Gte => std.mem.order(u8, val, pivot) != .lt,
                    .Lte => std.mem.order(u8, val, pivot) != .gt,
                };
            }
            return switch (pred) {
                .Eq => val == pivot,
                .Neq => val != pivot,
                .Gt => val > pivot,
                .Lt => val < pivot,
                .Gte => val >= pivot,
                .Lte => val <= pivot,
            };
        }

        /// Reads values selected by the selection vector into 'out'.
        /// Returns number of values written.
        pub fn readSelected(self: *Self, selection: selection_mod.SelectionVector, out: []T) !usize {
            var out_idx: usize = 0;
            var scan_idx: usize = 0;

            if (selection.len == 0) return 0;

            var current_run_start = scan_idx;
            var looking_for_active = selection.isActive(0);

            // Iterate to find runs
            while (scan_idx < selection.len and out_idx < out.len) {
                const is_active = selection.isActive(scan_idx);

                if (is_active != looking_for_active) {
                    // Run ended
                    const run_len = scan_idx - current_run_start;
                    if (looking_for_active) {
                        // Read active run
                        for (0..run_len) |_| {
                            if (out_idx >= out.len) break;
                            if (try self.next()) |val| {
                                out[out_idx] = val;
                                out_idx += 1;
                            }
                        }
                    } else {
                        // Skip inactive run
                        _ = try self.skip(run_len);
                    }
                    current_run_start = scan_idx;
                    looking_for_active = is_active;
                }
                scan_idx += 1;
            }

            // Final run
            const run_len = scan_idx - current_run_start;
            if (run_len > 0) {
                if (looking_for_active) {
                    for (0..run_len) |_| {
                        if (out_idx >= out.len) break;
                        if (try self.next()) |val| {
                            out[out_idx] = val;
                            out_idx += 1;
                        }
                    }
                } else {
                    _ = try self.skip(run_len);
                }
            }

            return out_idx;
        }
    };
}

pub const MemorySource = struct {
    buffer: []const u8,

    pub fn init(buffer: []const u8) MemorySource {
        return .{ .buffer = buffer };
    }

    pub fn randomAccessSource(self: *MemorySource) io.RandomAccessSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .readAt = readAt,
                .size = size,
                .close = close,
                .getSlice = getSlice,
            },
        };
    }

    fn readAt(ptr: *anyopaque, offset: u64, buf: []u8) anyerror!usize {
        const self: *MemorySource = @ptrCast(@alignCast(ptr));
        if (offset >= self.buffer.len) return 0;
        const available = self.buffer.len - offset;
        const to_read = @min(@as(usize, @intCast(buf.len)), @as(usize, @intCast(available)));
        @memcpy(buf[0..to_read], self.buffer[offset .. offset + to_read]);
        return to_read;
    }

    fn size(ptr: *anyopaque) u64 {
        const self: *MemorySource = @ptrCast(@alignCast(ptr));
        return self.buffer.len;
    }

    fn close(ptr: *anyopaque) void {
        _ = ptr;
    }

    fn getSlice(ptr: *anyopaque, offset: u64, len: u64) ?[]const u8 {
        const self: *MemorySource = @ptrCast(@alignCast(ptr));
        if (offset + len > self.buffer.len) return null;
        return self.buffer[offset .. offset + len];
    }
};

/// ParquetReader(T) analyzes a user-defined struct at COMPTIME.
/// It creates a specialized, branchless reader for exactly the fields in T.
pub fn ParquetReader(comptime T: type) type {
    const type_info = @typeInfo(T);
    const fields = type_info.@"struct".fields;
    
    return struct {
        const Self = @This();
        file: *file_mod.ParquetFile,
        allocator: std.mem.Allocator,

        // One Arena per Row Group as per step_back_architecture_01_05.md
        row_group_arena: std.heap.ArenaAllocator,

        // Tuple of ColumnReaders, one for each field in T
        column_readers: ColumnReaderTuple(T),
        selection_mask: [fields.len]bool,
        column_buffers: ?[]const []u8 = null,

        current_row: usize = 0,
        total_rows: usize,

        pub fn init(allocator: std.mem.Allocator, file: *file_mod.ParquetFile) !*Self {
            const self = try allocator.create(Self);
            self.* = Self{
                .file = file,
                .allocator = allocator,
                .row_group_arena = std.heap.ArenaAllocator.init(allocator),
                .column_readers = undefined,
                .selection_mask = [_]bool{true} ** fields.len,
                .current_row = 0,
                .total_rows = @intCast(file.metadata.num_rows),
                .column_buffers = null,
            };

            try self.initColumnReaders();
            return self;
        }

        pub fn deinit(self: *Self) void {
            if (self.column_buffers) |bufs| {
                for (bufs) |buf| self.allocator.free(buf);
                self.allocator.free(bufs);
            }
            self.row_group_arena.deinit();
            self.allocator.destroy(self);
        }

        pub fn setProjection(self: *Self, mask: [fields.len]bool) void {
            self.selection_mask = mask;
        }

        fn prefetchRowGroup(self: *Self) !void {
            if (self.file.metadata.row_groups.items.len == 0) return;
            const row_group = self.file.metadata.row_groups.items[0]; 
            
            var ranges = std.ArrayListUnmanaged(io.Range){};
            defer ranges.deinit(self.allocator);
            var buffers = std.ArrayListUnmanaged([]u8){};
            defer buffers.deinit(self.allocator);
            
            // Collect ranges for all selected columns
            inline for (fields, 0..) |field, i| {
                if (self.selection_mask[i]) {
                    const col_idx = try self.findColumnIndex(field.name);
                    const col_meta = row_group.columns.items[col_idx].meta_data.?;
                    const offset = if (col_meta.dictionary_page_offset) |o| o else col_meta.data_page_offset;
                    const len = col_meta.total_compressed_size;
                    
                    try ranges.append(self.allocator, .{ .start = @intCast(offset), .end = @intCast(offset + len) });
                    const buf = try self.allocator.alloc(u8, @intCast(len));
                    try buffers.append(self.allocator, buf);
                }
            }
            
            if (ranges.items.len > 0) {
                try self.file.source.readRanges(ranges.items, buffers.items);
            }
            
            // Wrap each buffer in a MemorySource and update ColumnReaders
            var bufs = try self.allocator.alloc([]u8, ranges.items.len);
            @memcpy(bufs, buffers.items);
            self.column_buffers = bufs;
            
            var buf_idx: usize = 0;
            inline for (fields, 0..) |_, i| {
                if (self.selection_mask[i]) {
                    const buf = bufs[buf_idx];
                    buf_idx += 1;
                    
                    const mem_source_ptr = try self.row_group_arena.allocator().create(MemorySource);
                    mem_source_ptr.* = MemorySource.init(buf);
                    
                    self.column_readers[i].source = mem_source_ptr.randomAccessSource();
                    self.column_readers[i].chunk_offset = 0;
                }
            }
        }

        fn initColumnReaders(self: *Self) !void {
            inline for (fields, 0..) |field, i| {
                // Find the column index in Parquet metadata that matches field.name
                const col_idx = try self.findColumnIndex(field.name);
                const row_group = self.file.metadata.row_groups.items[0]; // Start with first row group
                const col_meta = row_group.columns.items[col_idx].meta_data.?;

                // Get max levels from schema
                const levels = self.file.metadata.getColumnLevels(&.{field.name});

                self.column_readers[i] = ColumnReader(field.type).init(
                    self.allocator,
                    &self.row_group_arena,
                    self.file.source,
                    col_meta,
                    @intCast(levels.max_def),
                    @intCast(levels.max_rep),
                );
            }
        }

        fn findColumnIndex(self: *const Self, name: []const u8) !usize {
            for (self.file.metadata.schema.items, 0..) |element, i| {
                if (std.mem.eql(u8, element.name, name)) {
                    // This is slightly complex because schema is a tree,
                    // but for flat files, schema index (minus root) correlates to column chunks.
                    return i - 1;
                }
            }
            return error.ColumnNotFound;
        }

        /// Reads the next "row" as the struct T.
        /// This is the monomorphized "Polars Killer" loop.
        pub fn next(self: *Self) !?T {
            var row: T = undefined;
            var any_data = false;

            inline for (fields, 0..) |field, i| {
                if (try self.column_readers[i].next()) |val| {
                    @field(row, field.name) = val;
                    any_data = true;
                }
            }

            if (!any_data) return null;
            return row;
        }

        /// Reads a batch of rows into 'out', applying filters.
        pub fn nextBatch(self: *Self, out_buf: []T, filters: []const filter_mod.Filter) !usize {
            const remaining = self.total_rows - self.current_row;
            if (remaining == 0) return 0;

            // Ensure we have prefetched the row group
            if (self.column_buffers == null) {
                try self.prefetchRowGroup();
            }

            const limit = @min(out_buf.len, remaining);
            const out = out_buf[0..limit];

            const transport = @import("../io/transport.zig");
            if (transport.global_logger) |l| l.log(.trace, "reader: nextBatch current_row={d} limit={d}", .{ self.current_row, limit });

            // 1. Initialize Selection Vector
            var selection = try selection_mod.SelectionVector.init(self.allocator, out.len);
            defer selection.deinit();

            // 2. Filter Phase
            for (filters) |filter| {
                const col_idx = switch (filter) {
                    inline else => |f| f.col_idx,
                };

                // Find matching column reader
                inline for (fields, 0..) |_, i| {
                    if (i == col_idx) {
                        try self.column_readers[i].evaluate(filter, &selection);
                        self.column_readers[i].resetPage();
                    }
                }

                // Early exit if everything filtered
                if (selection.count() == 0) {
                    self.current_row += limit;
                    return 0;
                }
            }

            // 3. Project Phase (Row-Oriented with Run-Skipping)
            var out_idx: usize = 0;
            var scan_idx: usize = 0;
            var current_run_start: usize = 0;
            var looking_for_active = selection.isActive(0);

            // Iterate to find runs
            while (scan_idx < selection.len and out_idx < out.len) {
                const is_active = selection.isActive(scan_idx);

                if (is_active != looking_for_active) {
                    // Run ended
                    const run_len = scan_idx - current_run_start;
                    if (looking_for_active) {
                        // Active: Read rows
                        for (0..run_len) |_| {
                            if (out_idx >= out.len) break;

                            // Read all columns for this row
                            inline for (fields, 0..) |field, i| {
                                if (self.selection_mask[i]) {
                                    if (try self.column_readers[i].next()) |val| {
                                        @field(out[out_idx], field.name) = val;
                                    }
                                }
                            }
                            out_idx += 1;
                        }
                    } else {
                        // Inactive: Skip rows
                        inline for (fields, 0..) |_, i| {
                            if (self.selection_mask[i]) {
                                _ = try self.column_readers[i].skip(run_len);
                            }
                        }
                    }
                    current_run_start = scan_idx;
                    looking_for_active = is_active;
                }
                scan_idx += 1;
            }

            // Final run
            const run_len = scan_idx - current_run_start;
            if (run_len > 0) {
                if (looking_for_active) {
                    for (0..run_len) |_| {
                        if (out_idx >= out.len) break;
                        inline for (fields, 0..) |field, i| {
                            if (self.selection_mask[i]) {
                                if (try self.column_readers[i].next()) |val| {
                                    @field(out[out_idx], field.name) = val;
                                }
                            }
                        }
                        out_idx += 1;
                    }
                } else {
                    inline for (fields, 0..) |_, i| {
                        if (self.selection_mask[i]) {
                            _ = try self.column_readers[i].skip(run_len);
                        }
                    }
                }
            }

            self.current_row += limit;
            return out_idx;
        }
    };
}

/// Helper to generate a tuple type for ColumnReaders
fn ColumnReaderTuple(comptime T: type) type {
    const fields = @typeInfo(T).@"struct".fields;
    var types: [fields.len]type = undefined;
    inline for (fields, 0..) |field, i| {
        types[i] = ColumnReader(field.type);
    }
    return std.meta.Tuple(&types);
}
