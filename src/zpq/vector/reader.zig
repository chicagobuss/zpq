const std = @import("std");
const batch = @import("batch.zig");
const column = @import("../core/column.zig");
const schema = @import("../core/schema.zig");
const rle = @import("../core/rle.zig");
const decoder = @import("../core/decoder.zig");
const simd = @import("../core/simd.zig");
const file = @import("../core/file.zig");


const Vector = batch.Vector;
const VectorType = batch.VectorType;

pub const VectorColumnReader = struct {
    allocator: std.mem.Allocator,
    column_reader: column.ColumnReader,
    
    // Metadata
    col_type: schema.Type,
    max_def_level: u16,
    max_rep_level: u16,
    
    // State
    current_page: ?column.Page = null,
    active_pages: std.ArrayListUnmanaged(column.Page) = .{},
    values_remaining_in_page: usize = 0,
    
    // Decoders
    rle_decoder: ?rle.RleDecoder = null,
    plain_decoder: ?decoder.Decoder = null,
    def_levels_decoder: ?rle.RleDecoder = null,
    
    // Dictionary
    dictionary_vector: ?*Vector = null,

    pub fn init(allocator: std.mem.Allocator, column_reader: column.ColumnReader, col_type: schema.Type, max_def: u16, max_rep: u16) VectorColumnReader {
        return .{
            .allocator = allocator,
            .column_reader = column_reader,
            .col_type = col_type,
            .max_def_level = max_def,
            .max_rep_level = max_rep,
        };
    }

    pub fn deinit(self: *VectorColumnReader) void {
        for (self.active_pages.items) |*p| p.deinit(self.allocator);
        self.active_pages.deinit(self.allocator);
        if (self.dictionary_vector) |d| {
            d.deinit();
            self.allocator.destroy(d);
        }
        self.column_reader.deinit();
    }

    pub fn nextBatch(self: *VectorColumnReader, batch_size: usize) !Vector {
        // --- ZERO COPY PATH CHECK ---
        // Conditions:
        // 1. Uncompressed Page (implicitly handled if self.current_page.data IS the mapped memory)
        // 2. PLAIN Encoding
        // 3. No Nulls (max_def_level == 0) - or handled separately?
        //    (Nullable plain is: [def_levels][values]. If def_levels present, values are non-contiguous if nulls exist? 
        //     Actually PLAIN with nulls: Run of def levels, then Run of values. Values only exist for non-nulls.
        //     So we can't just pointer-cast the whole array to align with rows.
        //     We'd need a selection vector or validity mask. 
        //     For "Zero Copy Vector", we typically mean the VALUES array points there. 
        //     But strict zero-copy usually implies [Int, Int, Int] matching [Row, Row, Row].
        //     If nulls exist, it's [Int, Int] matching [Row1, Row3].
        //     So Zero Copy implies contiguous validity usually or no nulls.)
        // For now, strict zero-copy only for Non-Nullable PLAIN.
        
        if (self.max_def_level == 0) {
            if (self.values_remaining_in_page == 0) {
                 if (!try self.loadNextPage()) return error.EndOfStream;
                 // After loading, check again
            }
            
            // Re-check after potential page load
            if (self.plain_decoder != null and self.values_remaining_in_page >= batch_size) {
                 // We can potentially zero-copy slice this!
                 // Currently Vector struct expects a slice of bytes that represents the contiguous array.
                 // PLAIN encoding for simple types (i32, i64, f64) IS a contiguous array.
                 
                 // We need access to the underlying slice in plain_decoder.
                 // `packet.data` is available? `plain_decoder.buffer` is private?
                 // `plain_decoder` is `Decoder` struct. It has `buffer: []const u8`.
                 
                 const width: usize = switch(self.col_type) {
                     .INT32, .FLOAT => 4,
                     .INT64, .DOUBLE => 8,
                     // others...
                     else => 0,
                 };
                 
                 if (width > 0) {
                     const dec = &self.plain_decoder.?;
                     const needed_bytes = batch_size * width;
                     const remaining = dec.data.len - dec.pos;
                     
                     if (remaining >= needed_bytes) {
                         // ZERO COPY SUCCESS!
                         const slice = dec.data[dec.pos..][0..needed_bytes];
                         
                         // Advance decoder
                         dec.pos += needed_bytes;
                         self.values_remaining_in_page -= batch_size;
                         
                         return Vector{
                             .type = switch(self.col_type) {
                                 .INT32 => .i32,
                                 .INT64 => .i64,
                                 .FLOAT, .DOUBLE => .f64, // simplified mapping
                                 else => .i32,
                             },
                             .len = batch_size,
                             .data = slice,
                             .validity = null,
                             .capacity = 0, // Not owned
                             .allocator = null, // Not owned
                         };
                     }
                 }
            }
        }

        // --- STANDARD PATH (ALLOCATING) ---
        
        // Prepare output vector
        // For now, simplify to just I32. Real implementation needs dynamic dispatch on type.
        // Assuming I32 for prototype.
        
        const width = 4; // i32
        const byte_size = batch_size * width;
        const data = try self.allocator.alloc(u8, byte_size);
        errdefer self.allocator.free(data);
        
        var validity: ?[]u8 = null;
        if (self.max_def_level > 0) {
            // Bitmask for validity (1 bit per value)
            const validity_bytes = (batch_size + 7) / 8;
            validity = try self.allocator.alloc(u8, validity_bytes);
            @memset(validity.?, 0); // Default to all null
        }
        errdefer if (validity) |v| self.allocator.free(v);

        var values_read: usize = 0;
        
        while (values_read < batch_size) {
            if (self.values_remaining_in_page == 0) {
                if (!try self.loadNextPage()) break;
            }

            const count = @min(batch_size - values_read, self.values_remaining_in_page);
            const offset = values_read;

            // --- DECODING LOGIC ---
            
            // 1. Definition Levels (Validity)
            var current_def_levels: [1024]u64 = undefined;
            var num_defined: usize = 0;

            if (self.max_def_level > 0) {
                 if (self.def_levels_decoder) |*def_dec| {
                     // Read def levels for this batch chunk
                     var chunk_size = count;
                     if (chunk_size > 1024) chunk_size = 1024;
                     
                     const actual_count = chunk_size;
                     const n_def = try def_dec.nextBatch(current_def_levels[0..actual_count]);
                     
                     // Build validity bitmap
                     if (validity) |v_map| {
                         for (0..n_def) |i| {
                             if (current_def_levels[i] == self.max_def_level) {
                                 num_defined += 1;
                                 const global_idx = offset + i;
                                 v_map[global_idx / 8] |= @as(u8, 1) << @intCast(global_idx % 8);
                             }
                         }
                     }
                     
                     // If Dictionary Encoded
                     if (self.dictionary_vector != null) {
                         if (num_defined > 0 and self.rle_decoder != null) {
                             var indices: [1024]u64 = undefined;
                             const n_idx = try self.rle_decoder.?.nextBatch(indices[0..num_present_hack(current_def_levels[0..n_def], self.max_def_level)]);
                             _ = n_idx; // Assert or use in debug

                             
                             // Map indices -> output values (INT32)
                             // This actually DECODES the dictionary. 
                             // Optimized Vector mode would keep it dictionary encoded if possible.
                             // For this prototype, we materialize to I32.
                             const dict_vals = self.dictionary_vector.?.values(i32);
                             const out_slice = std.mem.bytesAsSlice(i32, data);
                             
                             var idx_pos: usize = 0;
                             for (0..n_def) |i| {
                                 if (current_def_levels[i] == self.max_def_level) {
                                     out_slice[offset + i] = dict_vals[indices[idx_pos]];
                                     idx_pos += 1;
                                 } else {
                                     out_slice[offset + i] = 0; // null
                                 }
                             }
                         }
                     }
                     // If Plain Encoded
                     else if (self.plain_decoder != null) {
                          // SKIP for now
                     }
                     
                     values_read += n_def;
                     self.values_remaining_in_page -= n_def;
                     continue;
                 }
            } else {
                // Non-nullable
                // Dictionary
                if (self.dictionary_vector != null and self.rle_decoder != null) {
                    var indices: [1024]u64 = undefined;
                    // Read up to 1024
                    const to_read = @min(count, 1024);
                    const n_idx = try self.rle_decoder.?.nextBatch(indices[0..to_read]);
                    
                    const dict_vals = self.dictionary_vector.?.values(i32);
                    const out_slice = std.mem.bytesAsSlice(i32, data);
                    
                    for (0..n_idx) |i| {
                        out_slice[offset + i] = dict_vals[indices[i]];
                    }
                    
                    values_read += n_idx;
                    self.values_remaining_in_page -= n_idx;
                    continue;
                }
                
                // Plain (Fallback copy for small chunks or mixed)
                 if (self.plain_decoder != null) {
                     const width_p = 4; // assume i32
                     const to_read = count; // small chunk
                     const needed = to_read * width_p;
                     // copy
                     const dec = &self.plain_decoder.?;
                      if (dec.data.len - dec.pos >= needed) {
                          const slice = dec.data[dec.pos..][0..needed];
                          @memcpy(data[offset*width_p .. (offset+to_read)*width_p], slice);
                          dec.pos += needed;
                         values_read += to_read;
                         self.values_remaining_in_page -= to_read;
                         continue;
                      }
                 }
            }
            
            // If we got here and didn't continue, break to avoid infinite loop in incomplete impl
            break;
        }

        // Return vector
        return Vector{
            .type = .i32,
            .len = values_read,
            .data = data, // allocator owned
            .validity = validity,
            .capacity = byte_size,
            .allocator = self.allocator,
        };
    }

    
    // Helper to count present
    fn num_present_hack(levels: []const u64, max_def: u16) usize {
        var n: usize = 0;
        for (levels) |l| if (l == max_def) { n += 1; };
        return n;
    }

    fn loadNextPage(self: *VectorColumnReader) !bool {
        // Similar to BatchReader.loadNextPage
        // ... (Simplified for brevity of initial impl)
        // Only implementing DICTIONARY and DATA_PAGE handling
        
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
                 
                 // Init decoders
                 // (Assume RLE_DICTIONARY for prototype)
                 var data_slice = self.current_page.?.data;
                 
                 // Def levels
                 if (self.max_def_level > 0) {
                     const len = std.mem.readInt(u32, data_slice[0..4], .little);
                     const def_data = data_slice[4 .. 4 + len];
                     data_slice = data_slice[4 + len ..];
                     const hw = std.math.log2_int(u32, try std.math.ceilPowerOfTwo(u32, @as(u32, self.max_def_level) + 1));
                     self.def_levels_decoder = rle.RleDecoder.init(def_data, @intCast(hw));
                 }
                 
                 // Data
                 if (dph.encoding == .RLE_DICTIONARY or dph.encoding == .PLAIN_DICTIONARY) {
                     const bit_width = data_slice[0];
                     self.rle_decoder = rle.RleDecoder.init(data_slice[1..], bit_width);
                 }
                 
                 return true;
             }
             page.deinit(self.allocator);
        }
        return false;
    }
    
    fn loadDictionary(self: *VectorColumnReader, page: column.Page) !void {
        // assume i32 for prototype
        const count: usize = @intCast(page.header.dictionary_page_header.?.num_values);
        // ... read int32s ...
        var d = decoder.Decoder.init(page.data);
        const bytes = try self.allocator.alloc(u8, count * 4);
        const ints = std.mem.bytesAsSlice(i32, bytes);
        for (0..count) |i| {
            ints[i] = try d.readInt32();
        }
        
        const vec = try self.allocator.create(Vector);
        vec.* = Vector{
            .type = .i32,
            .len = count,
            .data = bytes,
            .capacity = bytes.len,
            .allocator = self.allocator,
        };
        self.dictionary_vector = vec;
    }
};

test "integration: read simple.parquet id column" {
    const allocator = std.testing.allocator;
    
    // Open file
    var f = try file.ParquetFile.open(allocator, "data/simple.parquet");
    defer f.deinit();
    try f.readFooter();
    
    // Get column chunk for "id" (col 0)
    // We assume row group 0
    const row_group = f.metadata.?.row_groups.items[0];
    const col_chunk = row_group.columns.items[0];
    const col_meta = col_chunk.meta_data.?;
    
    // Initialize ColumnReader
    const col_reader = try column.ColumnReader.init(f.source, col_chunk);
    
    // Initialize VectorColumnReader
    // id is INT32, max_def=1 (optional), max_rep=0
    var vec_reader = VectorColumnReader.init(allocator, col_reader, col_meta.type, 1, 0);
    defer vec_reader.deinit();
    
    // Read batch
    const vec = try vec_reader.nextBatch(1024);
    // Vector is allocated. Need to verify and free.
    // Note: Vector struct doesn't have valid deinit because we defined it slightly weird in batch.zig vs here.
    // batch.zig Vector.deinit checks allocator.
    var v = vec; // workaround for const/mut
    defer v.deinit();
    
    try std.testing.expectEqual(@as(usize, 3), vec.len);
    try std.testing.expectEqual(batch.VectorType.i32, vec.type);
    
    const vals = vec.values(i32);
    try std.testing.expectEqual(@as(i32, 1), vals[0]);
    try std.testing.expectEqual(@as(i32, 2), vals[1]);
    try std.testing.expectEqual(@as(i32, 3), vals[2]);
    
    // Check validity
    // "id" is optional in schema, and all values are present in this file.
    if (vec.validity) |validity| {
        // bit 0, 1, 2 should be set
        // byte 0 should be 0b00000111 = 7
        try std.testing.expectEqual(@as(u8, 7), validity[0]);
    }
}

