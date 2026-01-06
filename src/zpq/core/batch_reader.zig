const std = @import("std");
const schema = @import("schema.zig");
const column = @import("column.zig");
const ColumnReader = column.ColumnReader;
const RleDecoder = @import("rle.zig").RleDecoder;
const Decoder = @import("decoder.zig").Decoder;
const simd = @import("simd.zig");
const file = @import("file.zig");
const SelectionVector = @import("selection.zig").SelectionVector;

/// Decoder for BYTE_STREAM_SPLIT encoding.
/// Data is split into N streams (one per byte of the type width).
/// To reconstruct value i: take byte 0 from stream[0][i], byte 1 from stream[1][i], etc.
/// Only valid for fixed-width numeric types (f32, f64, i32, i64, etc.)
pub fn ByteStreamSplitDecoder(comptime T: type) type {
    // BYTE_STREAM_SPLIT only makes sense for fixed-width types
    const width = comptime blk: {
        if (T == f32 or T == i32 or T == u32) break :blk 4;
        if (T == f64 or T == i64 or T == u64) break :blk 8;
        if (T == [12]u8) break :blk 12; // INT96
        // For unsupported types, use 0 to signal incompatibility
        break :blk 0;
    };

    return struct {
        data: []const u8,
        num_values: usize,
        pos: usize = 0,

        pub fn init(data: []const u8, num_values: usize) @This() {
            return .{ .data = data, .num_values = num_values };
        }

        pub fn next(self: *@This()) ?T {
            if (width == 0) return null; // Unsupported type
            if (self.pos >= self.num_values) return null;

            const stride = self.num_values;
            var bytes: [if (width > 0) width else 1]u8 = undefined;

            // Reconstruct value by gathering one byte from each stream
            inline for (0..width) |b| {
                const offset = b * stride + self.pos;
                if (offset >= self.data.len) return null;
                bytes[b] = self.data[offset];
            }

            self.pos += 1;

            // Return the reconstructed value
            if (T == [12]u8) {
                return bytes;
            } else {
                return @bitCast(bytes);
            }
        }
    };
}

pub fn BatchReader(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        column_reader: ColumnReader,

        // Current state
        current_page: ?column.Page = null,
        active_pages: std.ArrayListUnmanaged(column.Page) = .{},
        values_remaining_in_page: usize = 0,
        current_page_index: usize = 0, // Track which page we're on (0-based, data pages only)

        // Bit offset for BOOLEAN columns (bit-packed: 8 values per byte)
        bool_bit_offset: u3 = 0,

        // Page decoders
        rle_decoder: ?RleDecoder = null,
        plain_decoder: ?Decoder = null,
        def_levels_decoder: ?RleDecoder = null,
        byte_stream_split_decoder: ?ByteStreamSplitDecoder(T) = null,

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
            // Free ColumnReader's reusable decompression buffer
            self.column_reader.deinit();
        }

        /// Find an index in the dictionary for a given value.
        /// Find an index in the dictionary for a given value.
        pub fn findInDictionary(self: *Self, value: T) ?u64 {
            const dict = self.dictionary orelse return null;
            for (dict, 0..) |item, i| {
                if (T == []const u8) {
                    if (std.mem.eql(u8, item, value)) return @intCast(i);
                } else if (T == f32 or T == f64) {
                    // Primitive equality is fine for benches, but NaN handling would be needed for prod
                    if (item == value) return @intCast(i);
                } else {
                    if (item == value) return @intCast(i);
                }
            }
            return null;
        }

        /// Check if this column uses dictionary encoding.
        pub fn hasDictionary(self: *const Self) bool {
            return self.dictionary != null;
        }

        /// Scan dictionary indices directly and build selection vector.
        /// This is MUCH faster than decoding strings for dictionary-encoded columns.
        /// Must call after loading at least one page (to initialize dictionary).
        /// Returns number of values read.
        /// Scan dictionary indices and directly append matching row positions to selection.
        /// Uses RLE-aware scanning: matching runs bulk-append, non-matching runs skip.
        /// Returns number of values scanned.
        pub fn scanDictMatchesToSelection(self: *Self, target_idx: u64, base_row: usize, max_rows: usize, selection: *SelectionVector) !usize {
            var rows_scanned: usize = 0;

            while (rows_scanned < max_rows) {
                if (self.values_remaining_in_page == 0) {
                    if (!try self.loadNextPage()) break;
                }

                const count = @min(max_rows - rows_scanned, self.values_remaining_in_page);
                if (count == 0) break;

                // Non-nullable path: use RLE-aware scan
                if (self.max_def_level == 0) {
                    if (self.rle_decoder) |*rle_dec| {
                        const n = try rle_dec.scanMatchingPositions(
                            @intCast(count),
                            target_idx,
                            base_row + rows_scanned,
                            selection,
                        );
                        self.values_remaining_in_page -= n;
                        rows_scanned += n;
                        continue;
                    }
                }

                // Nullable path: must handle def levels
                if (self.def_levels_decoder) |*def_dec| {
                    var def_levels: [1024]u64 = undefined;
                    const n_def = try def_dec.nextBatch(def_levels[0..count]);
                    if (n_def == 0) break;

                    // Count present values (SIMD-friendly loop - compiler vectorizes)
                    var num_present: usize = 0;
                    const max_def = self.max_def_level;
                    for (def_levels[0..n_def]) |dl| {
                        num_present += @intFromBool(dl == max_def);
                    }

                    // Fast path: if ALL values are present (no nulls), use RLE-aware scan
                    if (num_present == n_def and self.rle_decoder != null) {
                        const n = try self.rle_decoder.?.scanMatchingPositions(
                            @intCast(n_def),
                            target_idx,
                            base_row + rows_scanned,
                            selection,
                        );
                        self.values_remaining_in_page -= n;
                        rows_scanned += n;
                        continue;
                    }

                    // Slow path: some nulls, must check each def level
                    if (num_present > 0 and self.rle_decoder != null) {
                        var indices: [1024]u64 = undefined;
                        const n_idx = try self.rle_decoder.?.nextBatch(indices[0..num_present]);

                        var idx_pos: usize = 0;
                        for (def_levels[0..n_def], 0..) |dl, row| {
                            if (dl == max_def and idx_pos < n_idx) {
                                if (indices[idx_pos] == target_idx) {
                                    try selection.append(base_row + rows_scanned + row);
                                }
                                idx_pos += 1;
                            }
                        }
                    }

                    self.values_remaining_in_page -= n_def;
                    rows_scanned += n_def;
                    continue;
                }

                break;
            }

            return rows_scanned;
        }

        pub fn scanDictIndicesIntoBatch(self: *Self, target_idx: u64, selection: *simd.SelectionVector, batch_size: usize) !usize {
            if (self.values_remaining_in_page == 0) {
                if (!try self.loadNextPage()) return 0;
            }

            const count = @min(batch_size, self.values_remaining_in_page);
            if (count == 0) return 0;

            // Non-nullable path: no def levels, just compare indices
            if (self.max_def_level == 0) {
                if (self.rle_decoder) |*rle_dec| {
                    var indices: [1024]u64 = undefined;
                    const n = try rle_dec.nextBatch(indices[0..count]);

                    // SIMD path: process 8 indices at a time
                    var i: usize = 0;
                    const simd_limit = n & ~@as(usize, 7); // Round down to multiple of 8
                    while (i < simd_limit) : (i += 8) {
                        const v_indices: @Vector(8, u64) = indices[i..][0..8].*;
                        const v_target: @Vector(8, u64) = @splat(target_idx);
                        const matches: u8 = @bitCast(v_indices == v_target);
                        if (matches != 0) {
                            // Set matching bits in selection vector
                            const byte_idx = i / 8;
                            selection.mask[byte_idx] |= matches;
                            selection.set_count += @popCount(matches);
                        }
                    }
                    // Scalar tail
                    while (i < n) : (i += 1) {
                        if (indices[i] == target_idx) selection.setBitIndices(i);
                    }

                    self.values_remaining_in_page -= n;
                    return n;
                }
            }

            // Nullable path: read indices upfront, then single pass over def levels
            if (self.def_levels_decoder) |*def_dec| {
                var def_levels: [1024]u64 = undefined;
                const n_def = try def_dec.nextBatch(def_levels[0..count]);
                if (n_def == 0) return 0;

                // Count present values first (compiler will vectorize this)
                var num_present: usize = 0;
                const max_def = self.max_def_level;
                for (def_levels[0..n_def]) |dl| {
                    num_present += @intFromBool(dl == max_def);
                }

                if (num_present > 0 and self.rle_decoder != null) {
                    var indices: [1024]u64 = undefined;
                    const n_idx = try self.rle_decoder.?.nextBatch(indices[0..num_present]);

                    // Single pass: scan def_levels, consume indices as we find present values
                    var idx_pos: usize = 0;
                    for (def_levels[0..n_def], 0..) |dl, row| {
                        if (dl == max_def and idx_pos < n_idx) {
                            if (indices[idx_pos] == target_idx) {
                                selection.setBitIndices(row);
                            }
                            idx_pos += 1;
                        }
                    }
                }

                self.values_remaining_in_page -= n_def;
                return n_def;
            }

            return 0;
        }

        pub fn skip(self: *Self, count: usize) !void {
            var remaining = count;
            while (remaining > 0) {
                if (self.values_remaining_in_page == 0) {
                    if (!try self.loadNextPage()) return;
                }

                const to_skip = @min(remaining, self.values_remaining_in_page);

                // 1. Skip Definition Levels and count present values
                // Optimized: use skipAndCountMatching for RLE-encoded def levels
                var values_to_skip_in_data = to_skip;
                if (self.def_levels_decoder) |*d| {
                    // Use optimized RLE skip that counts matching values in O(runs) not O(values)
                    values_to_skip_in_data = try d.skipAndCountMatching(@intCast(to_skip), self.max_def_level);
                }

                // 2. Skip Data Values
                if (values_to_skip_in_data > 0) {
                    if (self.rle_decoder) |*r| {
                        try r.skip(@intCast(values_to_skip_in_data));
                    } else if (self.plain_decoder) |*p| {
                        if (self.column_type == .BYTE_ARRAY) {
                            for (0..values_to_skip_in_data) |_| try p.skipByteArray();
                        } else if (self.column_type == .FIXED_LEN_BYTE_ARRAY) {
                            try p.skipFixedLenByteArray(values_to_skip_in_data * @as(usize, @intCast(self.type_length.?)));
                        } else if (self.column_type == .BOOLEAN) {
                            // BOOLEAN is bit-packed (1 bit per value), must track bit offset
                            p.skipBoolsWithOffset(values_to_skip_in_data, &self.bool_bit_offset);
                        } else {
                            const width: usize = if (T == [12]u8) 12 else @sizeOf(T);
                            try p.skip(values_to_skip_in_data * width);
                        }
                    }
                }

                self.values_remaining_in_page -= to_skip;
                remaining -= to_skip;
            }
        }

        pub fn nextBatchRaw(self: *Self, buffer: []T) !usize {
            if (self.max_def_level > 0) return error.CannotUseRawBatchOnNullableColumn;

            // Free pages from PREVIOUS batch
            self.freeInactivePages();

            var out_pos: usize = 0;
            while (out_pos < buffer.len) {
                if (self.values_remaining_in_page == 0) {
                    if (!try self.loadNextPage()) break;
                }

                const count = @min(buffer.len - out_pos, self.values_remaining_in_page);

                if (self.max_def_level == 0) {
                    if (self.dictionary) |dict| {
                        if (self.rle_decoder) |*rle| {
                            const n = try rle.nextBatchT(T, buffer[out_pos .. out_pos + count], dict);
                            out_pos += n;
                            self.values_remaining_in_page -= n;
                            continue;
                        }
                    }
                }

                // Plain fast-path
                if (self.plain_decoder) |*plain| {
                    if (T != []const u8) {
                        const n = try plain.readBatch(buffer[out_pos .. out_pos + count]);
                        if (n == 0) break;
                        out_pos += n;
                        self.values_remaining_in_page -= n;
                        continue;
                    }
                }

                // Fallback (scalar)
                buffer[out_pos] = (try self.nextValue()).?;
                out_pos += 1;
                self.values_remaining_in_page -= 1;
            }
            return out_pos;
        }

        fn freeInactivePages(self: *Self) void {
            var page_idx: usize = 0;
            while (page_idx < self.active_pages.items.len) {
                var p = &self.active_pages.items[page_idx];
                if (self.current_page != null and p.data.ptr == self.current_page.?.data.ptr) {
                    page_idx += 1;
                    continue;
                }
                p.deinit(self.allocator);
                _ = self.active_pages.swapRemove(page_idx);
            }
        }

        pub fn nextBatch(self: *Self, buffer: []?T) !usize {
            // Free pages from PREVIOUS batch
            self.freeInactivePages();

            var out_pos: usize = 0;
            while (out_pos < buffer.len) {
                if (self.values_remaining_in_page == 0) {
                    if (!try self.loadNextPage()) break;
                }

                const count = @min(buffer.len - out_pos, self.values_remaining_in_page);

                // --- VECTORIZED PATHS ---

                // 1. Non-nullable Dictionary-encoded
                if (self.max_def_level == 0) {
                    if (self.dictionary) |dict| {
                        if (self.rle_decoder) |*rle| {
                            const n = try rle.nextBatchOptT(T, buffer[out_pos .. out_pos + count], dict);
                            out_pos += n;
                            self.values_remaining_in_page -= n;
                            continue;
                        }
                    }
                }

                // 2. Non-nullable PLAIN-encoded (primitives)
                if (self.max_def_level == 0) {
                    if (self.plain_decoder) |*plain| {
                        if (T == bool) {
                            // BOOLEAN is bit-packed, need offset-aware read
                            const to_read = @min(count, 1024);
                            var values_buf: [1024]bool = undefined;
                            const n = plain.readBoolBatchWithOffset(values_buf[0..to_read], &self.bool_bit_offset);
                            if (n == 0) break;

                            for (0..n) |i| {
                                buffer[out_pos + i] = values_buf[i];
                            }
                            out_pos += n;
                            self.values_remaining_in_page -= n;
                            continue;
                        } else if (T != []const u8) {
                            const to_read = @min(count, 1024); // Limited for internal buffer
                            var values_buf: [1024]T = undefined;
                            const n = try plain.readBatch(values_buf[0..to_read]);
                            if (n == 0) break;

                            for (0..n) |i| {
                                buffer[out_pos + i] = values_buf[i];
                            }
                            out_pos += n;
                            self.values_remaining_in_page -= n;
                            continue;
                        }
                    }
                }

                // 3. Nullable Dictionary-encoded
                if (self.max_def_level > 0 and self.def_levels_decoder != null and self.rle_decoder != null and self.dictionary != null) {
                    var batch_size: usize = 0;
                    while (batch_size < count) {
                        const batch_rem = @min(8, count - batch_size);
                        var def_levels: [8]u64 = undefined;
                        const n_def = try self.def_levels_decoder.?.nextBatch(def_levels[0..batch_rem]);
                        if (n_def == 0) break;

                        const mask = if (n_def == 8)
                            simd.defLevelsToMask8(def_levels, self.max_def_level)
                        else blk: {
                            var m: u8 = 0;
                            for (0..n_def) |i| {
                                if (def_levels[i] == self.max_def_level) {
                                    m |= (@as(u8, 1) << @intCast(i));
                                }
                            }
                            break :blk m;
                        };

                        const values_needed = @popCount(mask);

                        if (values_needed > 0) {
                            var indices: [8]u64 = undefined;
                            const n_idx = try self.rle_decoder.?.nextBatch(indices[0..values_needed]);
                            if (n_idx < values_needed) return error.EndOfStream;

                            var compact_vals: [8]T = undefined;
                            const dict = self.dictionary.?;
                            for (0..n_idx) |i| {
                                compact_vals[i] = dict[indices[i]];
                            }

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

                // 4. Nullable PLAIN-encoded (primitives)
                if (self.max_def_level > 0 and self.def_levels_decoder != null and self.plain_decoder != null and T != []const u8 and T != [12]u8) {
                    var batch_size: usize = 0;
                    while (batch_size < count) {
                        const batch_rem = @min(8, count - batch_size);
                        var def_levels: [8]u64 = undefined;
                        const n_def = try self.def_levels_decoder.?.nextBatch(def_levels[0..batch_rem]);
                        if (n_def == 0) break;

                        const mask = if (n_def == 8)
                            simd.defLevelsToMask8(def_levels, self.max_def_level)
                        else blk: {
                            var m: u8 = 0;
                            for (0..n_def) |i| {
                                if (def_levels[i] == self.max_def_level) {
                                    m |= (@as(u8, 1) << @intCast(i));
                                }
                            }
                            break :blk m;
                        };

                        const values_needed = @popCount(mask);

                        if (values_needed > 0) {
                            var compact_vals: [8]T = undefined;
                            const n_plain = if (T == bool)
                                self.plain_decoder.?.readBoolBatchWithOffset(compact_vals[0..values_needed], &self.bool_bit_offset)
                            else
                                try self.plain_decoder.?.readBatch(compact_vals[0..values_needed]);
                            if (n_plain < values_needed) return error.EndOfStream;

                            var tmp_out: [8]?T = undefined;
                            _ = simd.expandNullsBatch8(T, compact_vals[0..n_plain], mask, &tmp_out);
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

        /// Read a batch of values, but only materializing rows set in the selection vector.
        /// Unselected rows are skipped in the underlying data stream.
        /// buffer should be large enough to hold all selected values (sel.count).
        /// returns number of selected values materialized.
        pub fn nextBatchSelected(self: *Self, buffer: []?T, selection: *const simd.SelectionVector, n: usize) !usize {
            // Fast path: if all rows are selected, just use nextBatch directly
            if (selection.count() == n) {
                return try self.nextBatch(buffer[0..n]);
            }

            // Fast path: if no rows are selected, just skip
            if (selection.count() == 0) {
                try self.skip(n);
                return 0;
            }

            // Slow path: selective materialization
            var selected_pos: usize = 0;
            var i: usize = 0;
            while (i < n) {
                if (self.values_remaining_in_page == 0) {
                    if (!try self.loadNextPage()) break;
                }

                const batch_rem = @min(n - i, self.values_remaining_in_page);

                // Identify runs of selected/unselected rows
                var j: usize = 0;
                while (j < batch_rem) {
                    const is_selected = selection.isSet(i + j);
                    var run_len: usize = 1;
                    while (j + run_len < batch_rem and selection.isSet(i + j + run_len) == is_selected) {
                        run_len += 1;
                    }

                    if (is_selected) {
                        // Materialize run
                        const materialized = try self.nextBatch(buffer[selected_pos .. selected_pos + run_len]);
                        selected_pos += materialized;
                    } else {
                        // Skip run
                        try self.skip(run_len);
                    }
                    j += run_len;
                }
                i += batch_rem;
            }
            return selected_pos;
        }

        fn loadNextPage(self: *Self) !bool {
            const trace_enabled = if (std.posix.getenv("ZPQ_TRACE")) |v| std.mem.eql(u8, v, "1") else false;

            self.current_page = null;

            const start = std.time.Instant.now() catch null;

            while (try self.column_reader.next(self.allocator)) |const_page| {
                var page = const_page;
                if (page.header.type == .DICTIONARY_PAGE) {
                    try self.loadDictionary(page);
                    page.deinit(self.allocator);
                    continue;
                }

                if (page.header.type == .DATA_PAGE) {
                    try self.active_pages.append(self.allocator, page);
                    self.current_page = self.active_pages.items[self.active_pages.items.len - 1];
                    const dph = self.current_page.?.header.data_page_header.?;
                    self.values_remaining_in_page = @intCast(dph.num_values);
                    try self.initPageDecoders(self.current_page.?);
                    self.current_page_index += 1;
                    // Reset bit offset for new page (BOOLEAN is bit-packed)
                    self.bool_bit_offset = 0;

                    if (trace_enabled) {
                        if (start) |s| {
                            const now = std.time.Instant.now() catch s;
                            const elapsed_ns = now.since(s);
                            std.debug.print("[PAGE] loadNextPage: {d:.2}ms values={d}\n", .{
                                @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0,
                                self.values_remaining_in_page,
                            });
                        }
                    }
                    return true;
                }

                page.deinit(self.allocator);
            }
            return false;
        }

        /// Skip an entire data page without decompressing.
        /// Returns the number of values in the skipped page, or null if no more pages.
        /// NOTE: Dictionary pages ARE loaded (needed for future decoding).
        pub fn skipNextPage(self: *Self) !?usize {
            self.current_page = null;
            self.values_remaining_in_page = 0;

            while (true) {
                // Peek at next page header to decide whether to skip or load
                const header = try self.column_reader.skipPage() orelse return null;

                if (header.type == .DICTIONARY_PAGE) {
                    // Dictionary pages MUST be loaded - we need to rewind and load it
                    // Since we can't rewind after skipPage, we need a different approach
                    // For now, this is a limitation - skipNextPage works best when
                    // dictionary is already loaded from earlier pages
                    continue;
                }

                if (header.type == .DATA_PAGE) {
                    const dph = header.data_page_header.?;
                    const num_values: usize = @intCast(dph.num_values);
                    self.current_page_index += 1;
                    return num_values;
                }

                // Skip other page types (index pages, etc.)
            }
        }

        /// Skip to a target row efficiently using page-level skipping.
        /// Reads page headers without decompressing to skip entire pages.
        /// When we find the page containing target_row, we seek back and load it.
        /// Returns the row position after skipping (should equal target_row on success).
        pub fn skipToRow(self: *Self, target_row: usize, current_row: usize) !usize {
            const trace_enabled = if (std.posix.getenv("ZPQ_TRACE")) |v| std.mem.eql(u8, v, "1") else false;

            if (target_row <= current_row) return current_row;

            if (trace_enabled) {
                std.debug.print("[SKIP] skipToRow called: target={d} current={d} remaining={d}\n", .{ target_row, current_row, self.values_remaining_in_page });
            }

            var row = current_row;

            // If we have values in current page, skip within it first
            if (self.values_remaining_in_page > 0) {
                const skip_in_page = @min(target_row - row, self.values_remaining_in_page);
                try self.skipValuesInCurrentPage(skip_in_page);
                row += skip_in_page;
                if (row >= target_row) return row;
            }

            // Skip whole pages by reading headers only (no decompression!)
            // Track page offsets so we can seek back to load the target page
            var page_start_offset: u64 = 0;
            var pages_skipped: usize = 0;

            while (row < target_row) {
                // Save offset BEFORE reading this page header
                page_start_offset = self.column_reader.getOffset();

                const header = try self.column_reader.skipPage() orelse break;

                if (header.type == .DICTIONARY_PAGE) {
                    // Need to load dictionary - seek back and load it properly
                    // loadNextPage will load the dictionary AND then the first data page
                    self.column_reader.seekTo(page_start_offset);
                    if (!try self.loadNextPage()) break;

                    // loadNextPage loaded both dictionary AND data page
                    // Now we have values_remaining_in_page set correctly
                    // Skip within this page to target_row
                    const page_values = self.values_remaining_in_page;
                    if (row + page_values <= target_row) {
                        // Skip this entire page
                        row += page_values;
                        self.values_remaining_in_page = 0;
                        self.current_page = null;
                        pages_skipped += 1;
                    } else {
                        // This page contains our target row
                        const skip_in_page = target_row - row;
                        try self.skipValuesInCurrentPage(skip_in_page);
                        row = target_row;
                        break;
                    }
                    continue;
                }

                if (header.type == .DATA_PAGE) {
                    const dph = header.data_page_header orelse continue;
                    const page_values: usize = @intCast(dph.num_values);

                    if (trace_enabled) {
                        std.debug.print("[SKIP] DATA_PAGE: page_values={d} row={d} target={d}\n", .{ page_values, row, target_row });
                    }

                    if (row + page_values <= target_row) {
                        // Skip this entire page (no decompression!)
                        row += page_values;
                        self.current_page_index += 1;
                        pages_skipped += 1;
                    } else {
                        // This page contains our target row
                        // Seek back to load it, then skip within the page
                        self.column_reader.seekTo(page_start_offset);
                        if (!try self.loadNextPage()) break;

                        // Now skip within this page to target_row
                        const skip_in_page = target_row - row;
                        try self.skipValuesInCurrentPage(skip_in_page);
                        row = target_row;
                        break;
                    }
                }
            }

            // Debug output (only if ZPQ_TRACE=1)
            if (trace_enabled and pages_skipped > 0) {
                std.debug.print("[SKIP] pages_skipped={d} final_row={d}\n", .{ pages_skipped, row });
            }

            return row;
        }

        /// Skip entire pages until we've skipped at least `count` values.
        /// Returns the actual number of values skipped (may be more than count).
        /// This is much faster than skip() when skipping large ranges.
        pub fn skipPages(self: *Self, count: usize) !usize {
            var skipped: usize = 0;

            // First, skip remaining values in current page
            if (self.values_remaining_in_page > 0) {
                const to_skip = @min(count, self.values_remaining_in_page);
                try self.skipValuesInCurrentPage(to_skip);
                skipped += to_skip;
            }

            // Skip entire pages by loading them (handles dict) then discarding
            while (skipped < count) {
                if (!try self.loadNextPage()) break;

                const page_values = self.values_remaining_in_page;
                skipped += page_values;
                self.values_remaining_in_page = 0;
                self.current_page = null;
            }

            return skipped;
        }

        fn skipValuesInCurrentPage(self: *Self, count: usize) !void {
            const to_skip = @min(count, self.values_remaining_in_page);

            // Skip def levels and count present values
            var values_to_skip_in_data = to_skip;
            if (self.def_levels_decoder) |*d| {
                values_to_skip_in_data = try d.skipAndCountMatching(@intCast(to_skip), self.max_def_level);
            }

            // Skip data values
            if (values_to_skip_in_data > 0) {
                if (self.rle_decoder) |*r| {
                    try r.skip(@intCast(values_to_skip_in_data));
                } else if (self.plain_decoder) |*p| {
                    if (self.column_type == .BYTE_ARRAY) {
                        for (0..values_to_skip_in_data) |_| try p.skipByteArray();
                    } else if (self.column_type == .FIXED_LEN_BYTE_ARRAY) {
                        try p.skipFixedLenByteArray(values_to_skip_in_data * @as(usize, @intCast(self.type_length.?)));
                    } else if (self.column_type == .BOOLEAN) {
                        p.skipBoolsWithOffset(values_to_skip_in_data, &self.bool_bit_offset);
                    } else {
                        const width: usize = if (T == [12]u8) 12 else @sizeOf(T);
                        try p.skip(values_to_skip_in_data * width);
                    }
                }
            }

            self.values_remaining_in_page -= to_skip;
        }

        /// Get the current page index (0-based, data pages only).
        /// Returns the number of data pages loaded so far.
        pub fn getPageIndex(self: *const Self) usize {
            return self.current_page_index;
        }

        /// Check if we're at a page boundary (about to load a new page).
        /// Returns true if the next read will trigger loading a new page.
        pub fn isAtPageBoundary(self: *const Self) bool {
            return self.values_remaining_in_page == 0;
        }

        fn loadDictionary(self: *Self, page: column.Page) !void {
            var decoder = Decoder.init(page.data);
            var items = std.ArrayListUnmanaged(T){};
            errdefer items.deinit(self.allocator);

            while (decoder.hasMore()) {
                switch (self.column_type) {
                    .BYTE_ARRAY => {
                        const val = try decoder.readByteArray();
                        const owned = try self.allocator.dupe(u8, val);
                        if (T == []const u8) {
                            try items.append(self.allocator, owned);
                        } else {
                            return error.IncompatibleTypeForDictionary;
                        }
                    },
                    .FIXED_LEN_BYTE_ARRAY => {
                        const val = try decoder.readFixedLenByteArray(@intCast(self.type_length.?));
                        const owned = try self.allocator.dupe(u8, val);
                        if (T == []const u8) {
                            try items.append(self.allocator, owned);
                        } else {
                            return error.IncompatibleTypeForDictionary;
                        }
                    },
                    .INT32 => {
                        const val = try decoder.readInt32();
                        if (T == i64 or T == u64) {
                            try items.append(self.allocator, @intCast(val));
                        } else if (T == i32) {
                            try items.append(self.allocator, val);
                        } else {
                            return error.IncompatibleTypeForDictionary;
                        }
                    },
                    .INT64 => {
                        const val = try decoder.readInt64();
                        if (T == i64 or T == u64) {
                            try items.append(self.allocator, val);
                        } else {
                            return error.IncompatibleTypeForDictionary;
                        }
                    },
                    .INT96 => {
                        if (T == [12]u8) {
                            try items.append(self.allocator, try decoder.readInt96());
                        } else {
                            return error.IncompatibleTypeForDictionary;
                        }
                    },
                    .FLOAT => {
                        if (T == f32) {
                            try items.append(self.allocator, try decoder.readFloat());
                        } else {
                            return error.IncompatibleTypeForDictionary;
                        }
                    },
                    .DOUBLE => {
                        if (T == f64) {
                            try items.append(self.allocator, try decoder.readDouble());
                        } else {
                            return error.IncompatibleTypeForDictionary;
                        }
                    },
                    else => return error.UnsupportedTypeForDictionary,
                }
            }
            if (self.dictionary) |d| {
                if (T == []const u8) for (d) |s| self.allocator.free(s);
                self.allocator.free(d);
            }
            self.dictionary = try items.toOwnedSlice(self.allocator);
        }

        fn initPageDecoders(self: *Self, page: column.Page) !void {
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
                    self.byte_stream_split_decoder = null;
                }
            } else if (dph.encoding == .PLAIN) {
                self.plain_decoder = Decoder.init(data_slice);
                self.rle_decoder = null;
                self.byte_stream_split_decoder = null;
            } else if (dph.encoding == .BYTE_STREAM_SPLIT) {
                self.byte_stream_split_decoder = ByteStreamSplitDecoder(T).init(data_slice, @intCast(dph.num_values));
                self.plain_decoder = null;
                self.rle_decoder = null;
            } else {
                return error.UnsupportedEncoding;
            }
        }

        fn decodePlainScalar(self: *Self) !T {
            const dec = &self.plain_decoder.?;
            if (T == []const u8) {
                if (self.column_type == .FIXED_LEN_BYTE_ARRAY) {
                    const bytes = try dec.readFixedLenByteArray(@intCast(self.type_length.?));
                    return @ptrCast(bytes);
                }
                const bytes = try dec.readByteArray();
                return @ptrCast(bytes);
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

        pub fn nextValue(self: *Self) !?T {
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

            if (self.byte_stream_split_decoder) |*bss| {
                return bss.next() orelse error.EndOfStream;
            }

            return error.UnsupportedEncoding;
        }
    };
}
