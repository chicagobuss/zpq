const std = @import("std");
const batch = @import("batch.zig");
const schema = @import("../core/schema.zig");
const page_writer = @import("../core/page_writer.zig");
const thrift = @import("../core/thrift.zig");

const Vector = batch.Vector;
const RecordBatch = batch.RecordBatch;

/// Generic Parquet Writer for Vector batches.
/// WriterType must implement `writeAll(bytes: []const u8) !void`.
pub fn VectorParquetWriter(comptime WriterType: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        writer: WriterType,
        
        // State
        current_offset: u64,
        num_rows_total: i64,
        
        // Buffering current row group
        row_group_buffer: std.ArrayListUnmanaged(RecordBatch) = .{},
        current_rg_rows: i64 = 0,
        
        // Metadata accumulator
        schema_elements: std.ArrayListUnmanaged(schema.SchemaElement) = .{},
        row_groups: std.ArrayListUnmanaged(schema.RowGroup) = .{},
        
        // Configuration
        compression: schema.CompressionCodec = .UNCOMPRESSED,
        
        /// Initialize writer.
        /// `schema_elems` should be the flattened schema elements (root + columns).
        /// Start offset is usually 4 (after PAR1) unless appending? 
        /// Standard writer writes PAR1 first.
        pub fn init(
            allocator: std.mem.Allocator, 
            writer: WriterType, 
            schema_elems: []const schema.SchemaElement
        ) !Self {
            // Copy schema
            var elems = std.ArrayListUnmanaged(schema.SchemaElement){};
            try elems.appendSlice(allocator, schema_elems);
            
            // Write Magic
            try writer.writeAll("PAR1");
            
            return Self{
                .allocator = allocator,
                .writer = writer,
                .current_offset = 4,
                .num_rows_total = 0,
                .schema_elements = elems,
            };
        }
        
        pub fn deinit(self: *Self) void {
            // Free buffered batches
            for (self.row_group_buffer.items) |*b| {
                b.deinit();
            }
            self.row_group_buffer.deinit(self.allocator);
            
            self.schema_elements.deinit(self.allocator);
            
            // Deep free metadata
            for (self.row_groups.items) |*rg| {
                for (rg.columns.items) |*col| {
                    if (col.meta_data) |*md| {
                        md.encodings.deinit(self.allocator);
                        for (md.path_in_schema.items) |p| {
                            self.allocator.free(p);
                        }
                        md.path_in_schema.deinit(self.allocator);
                    }
                }
                rg.columns.deinit(self.allocator);
            }
            self.row_groups.deinit(self.allocator);
        }
        
        /// Write a batch. Takes ownership of the batch (will free on flush/deinit).
        pub fn writeBatch(self: *Self, batch_in: RecordBatch) !void {
            try self.row_group_buffer.append(self.allocator, batch_in);
            self.current_rg_rows += @intCast(batch_in.len);
        }
        
        /// Flush buffered batches to a new Row Group.
        pub fn flushRowGroup(self: *Self) !void {
            if (self.row_group_buffer.items.len == 0) return;
            
            const num_rows = self.current_rg_rows;
            var total_byte_size: i64 = 0;
            var columns = std.ArrayListUnmanaged(schema.ColumnChunk){};
            errdefer {
                // cleanup columns if fail
                for (columns.items) |*c| { 
                     if (c.meta_data) |*md| {
                         md.encodings.deinit(self.allocator);
                         md.path_in_schema.deinit(self.allocator);
                     }
                }
                columns.deinit(self.allocator);
            }
            
            // Assume schema structure: [0] = root, [1..N] = columns. 
            // We iterate over columns (1..N).
            const num_cols = self.schema_elements.items.len - 1;
            
            // We must encode each column across all buffered batches.
            for (0..num_cols) |col_idx| {
                const schema_idx = col_idx + 1; // skip root
                const col_def = self.schema_elements.items[schema_idx];
                const col_name = col_def.name;
                const col_type = col_def.type.?;
                
                // Initialize ColumnWriter
                var cw = page_writer.ColumnWriter.init(self.allocator, col_type, self.compression);
                defer cw.deinit();
                
                // Write all batches for this column
                for (self.row_group_buffer.items) |rb| {
                    const vec = rb.column(col_idx);
                    try writeVectorToColumn(vec, &cw);
                }
                
                // Flush page buffer
                // TODO: Verify if writeVectorToColumn handles flushing pages internally?
                // ColumnWriter usually flushes pages when full OR we force it?
                // The implementation in page_writer.zig `writeInt32Plain` calls `flushPage(.PLAIN)`.
                // So pages are accumulated in `cw.pages`.
                
                // Calculate buffer size needed
                var buf_size: usize = 0;
                for (cw.pages.items) |p| {
                    buf_size += p.data.len;
                    // Header size is unknown until written? 
                    // Use writePagesToBufferUnmanaged approach.
                }
                // Actually we just stream them.
                
                // We need to write to `self.writer` but `cw.writeToFile` expects `std.fs.File`.
                // We need a helper to write to generic writer.
                
                // Generic write:
                // We can construct a specialized writer or just serialize to memory buffer then writeAll.
                // Since `WriterType` writes bytes, let's buffer the whole column chunk (pages) then write.
                // Usually chunks are 10s-100s MB max, maybe smaller.
                
                var chunk_buffer = std.ArrayListUnmanaged(u8){};
                defer chunk_buffer.deinit(self.allocator);
                
                try cw.writePagesToBufferUnmanaged(&chunk_buffer, self.allocator);
                
                // Write to output
                try self.writer.writeAll(chunk_buffer.items);
                
                // Meta info
                const start_offset = self.current_offset;
                const bytes_len = chunk_buffer.items.len;
                self.current_offset += bytes_len;
                total_byte_size += @intCast(bytes_len); // This is compressed size + headers.
                
                // Construct ColumnChunk metadata
                 var encodings = std.ArrayListUnmanaged(schema.Encoding){};
                 try encodings.append(self.allocator, .PLAIN); // Simplified
                 // TODO: get actual encodings from CW
                 
                 var path = std.ArrayListUnmanaged([]const u8){};
                 try path.append(self.allocator, try self.allocator.dupe(u8, col_name));

                 const chunk = schema.ColumnChunk{
                    .file_path = null,
                    .file_offset = @intCast(start_offset),
                    .meta_data = schema.ColumnMetaData{
                        .type = col_type,
                        .encodings = encodings,
                        .path_in_schema = path,
                        .codec = self.compression,
                        .num_values = num_rows,
                        .total_uncompressed_size = cw.total_uncompressed_size,
                        .total_compressed_size = cw.total_compressed_size,
                        .data_page_offset = @intCast(start_offset), // Simplified assumption
                        .index_page_offset = null,
                        .dictionary_page_offset = null,
                    },
                };
                try columns.append(self.allocator, chunk);
            }
            
            // Record Row Group
            try self.row_groups.append(self.allocator, schema.RowGroup{
                .columns = columns,
                .total_byte_size = total_byte_size,
                .num_rows = num_rows,
            });
            
            self.num_rows_total += num_rows;
            
            // Clear buffer
            for (self.row_group_buffer.items) |*b| b.deinit();
            self.row_group_buffer.clearRetainingCapacity();
            self.current_rg_rows = 0;
        }
        
        pub fn finish(self: *Self) !void {
            if (self.current_rg_rows > 0) {
                try self.flushRowGroup();
            }
            
            // Write Footer
            const metadata = schema.FileMetaData{
                .version = 2,
                .schema = self.schema_elements,
                .num_rows = self.num_rows_total,
                .created_by = "zpq-vector",
                .row_groups = self.row_groups,
            };
            
            var thrift_writer = thrift.Writer.init(self.allocator);
            defer thrift_writer.deinit();
            try metadata.write(&thrift_writer);
            const footer_bytes = thrift_writer.bytes();
            const footer_len: u32 = @intCast(footer_bytes.len);
            
            try self.writer.writeAll(footer_bytes);
            try self.writer.writeAll(&std.mem.toBytes(footer_len));
            try self.writer.writeAll("PAR1");
        }
        
        fn writeVectorToColumn(vec: Vector, cw: *page_writer.ColumnWriter) !void {
             switch (cw.column_type) {
                 .INT32 => {
                     // Expect i32 vector
                     if (vec.type != .i32) return error.TypeMismatch;
                     const vals = vec.values(i32);
                     try cw.writeInt32Plain(vals);
                 },
                 .INT64 => {
                     if (vec.type != .i64) return error.TypeMismatch;
                     const vals = vec.values(i64);
                     try cw.writeInt64Plain(vals);
                 },
                 .FLOAT => {
                     if (vec.type != .f64) return error.TypeMismatch; // Vector uses f64 for both?
                     // Wait, VectorType has f64. If schema is FLOAT, we need to cast?
                     // Vector supports f64. If schema is FLOAT, we should probably have casted earlier or handle here.
                     // For now assume perfect match or error.
                     const vals = vec.values(f64);
                     
                     // If target is FLOAT but we have DOUBLE?
                     // Need to convert.
                     const vals32 = try cw.allocator.alloc(f32, vals.len);
                     defer cw.allocator.free(vals32);
                     for (vals, 0..) |v, i| vals32[i] = @floatCast(v);
                     try cw.writeFloatPlain(vals32);
                 },
                 .DOUBLE => {
                     if (vec.type != .f64) return error.TypeMismatch;
                     const vals = vec.values(f64);
                     try cw.writeDoublePlain(vals);
                 },
                 .BYTE_ARRAY => {
                    // Expect string vector? Vector has 'string' type?
                    // Vector definition: 
                    //     string,     // Byte array / string view
                    //     data is Slice of StringView (len + ptr) ??
                    // NO, wait. Let's check Vector definition in batch.zig.
                    if (vec.type != .string) return error.TypeMismatch;
                    
                    // Vector string data format:
                    // Currently `data` is just bytes.
                    // Implementation of string vector is likely Arrow-style: offsets + data.
                    // OR simple: `values(T)` returns slice of T.
                    // For strings, maybe `[]const u8` slices?
                    // Let's check batch.zig Vector struct.
                    
                    // batch.zig:
                    //     string,     // Byte array / string view
                    //     data: []const u8
                    //     interpretation:
                    //     - string: Slice of StringView (len + ptr)
                     
                     // We need the StringView struct definition.
                     // It is unmanaged.
                     // Actually maybe we just used `[]const u8` (slice) which is 16 bytes.
                     // `[]const []const u8` ? No, that's slice of slices.
                     // A slice is (ptr, len).
                     
                     if (!std.mem.isAligned(@intFromPtr(vec.data.ptr), @alignOf([]const u8))) return error.UnalignedData;
                     const aligned_data = @as([]align(@alignOf([]const u8)) const u8, @alignCast(vec.data));
                     const slices = std.mem.bytesAsSlice([]const u8, aligned_data);
                     try cw.writeByteArrayPlain(slices);
                 },
                 else => return error.NotImplemented,
             }
        }
    };
}

test "VectorParquetWriter local" {
    const allocator = std.testing.allocator;
    const path = "test_vector_writer.parquet";
    
    const file = try std.fs.cwd().createFile(path, .{});

    
    // Schema
    const schema_elems = &[_]schema.SchemaElement{
        .{ 
            .name = "schema", 
            .num_children = 1, 
            .repetition_type = null, 
            .type = null,
            .type_length = null,
            .scale = null,
            .precision = null,
            .field_id = null
        },
        .{ 
            .name = "id", 
            .type = .INT32, 
            .repetition_type = .REQUIRED, 
            .field_id = 1,
            .num_children = null, 
            .type_length = null,
            .scale = null,
            .precision = null
        },
    };
    
    // Init Vector Writer
    var vpw = try VectorParquetWriter(std.fs.File).init(allocator, file, schema_elems);
    defer vpw.deinit();
    
    // Create Batch
    const count = 1000;
    const bytes = try allocator.alignedAlloc(u8, std.mem.Alignment.fromByteUnits(4), count * 4);
    defer allocator.free(bytes); // Manual free with correct alignment
    
    const data = std.mem.bytesAsSlice(i32, bytes);
    for (0..count) |i| data[i] = @intCast(i);
    
    const vec = Vector{
        .type = .i32,
        .len = count,
        .data = bytes,
        .allocator = null, // Disable auto-free in Vector to avoid alignment mismatch
        .capacity = 0,
    };
    
    // Manually construct batch (Vector takes ownership of data)
    var cols = try allocator.alloc(Vector, 1);
    cols[0] = vec;
    
    const rb = RecordBatch{
        .len = count,
        .columns = cols,
        .allocator = allocator,
    };
    
    // Write
    try vpw.writeBatch(rb);
    try vpw.finish();
    
    file.close();
    
    // Verify
    const f_read = try std.fs.cwd().openFile(path, .{});
    defer f_read.close();
    const stat = try f_read.stat();
    try std.testing.expect(stat.size > 100); // Should be reasonably large
    
    // Cleanup
    try std.fs.cwd().deleteFile(path);
}
