//! Schema fixtures shared by tests in more than one module. Imported only from tests, so none of this is compiled
//! into a binary or exposed through the zpq module. Fixtures used by a single module stay beside its tests.

const std = @import("std");
const schema = @import("../schema.zig");

pub fn group(name: []const u8, num_children: i32) schema.SchemaElement {
    return .{ .type = null, .type_length = null, .repetition_type = .REQUIRED, .name = name, .num_children = num_children, .scale = null, .precision = null, .field_id = null };
}

pub fn leaf(name: []const u8, t: schema.Type) schema.SchemaElement {
    return .{ .type = t, .type_length = null, .repetition_type = .REQUIRED, .name = name, .num_children = null, .scale = null, .precision = null, .field_id = null };
}

/// `root{ r{key: INT64, name: BYTE_ARRAY}, key: INT64, amount: INT64, a{x: INT32}, b{x: INT32} }`.
/// Leaves in order: r.key, r.name, key, amount, a.x, b.x. The top-level `key` shares its leaf name with `r.key`
/// and sits after it; `x` names a leaf in two groups and none at the top level.
pub fn sharedLeafNameMeta(arena: std.mem.Allocator) !schema.FileMetaData {
    var meta: schema.FileMetaData = .{ .version = 1, .schema = .empty, .num_rows = 0, .created_by = null, .row_groups = .empty };
    try meta.schema.appendSlice(arena, &.{
        group("schema", 5),
        group("r", 2),
        leaf("key", .INT64),
        leaf("name", .BYTE_ARRAY),
        leaf("key", .INT64),
        leaf("amount", .INT64),
        group("a", 1),
        leaf("x", .INT32),
        group("b", 1),
        leaf("x", .INT32),
    });
    return meta;
}

/// One flat column `c` with the given stats, 100 rows, in a file whose schema declares `elem`.
pub const StatsColumn = struct {
    rg: schema.RowGroup,
    fm: schema.FileMetaData,

    pub fn init(a: std.mem.Allocator, elem: schema.SchemaElement, stats: schema.Statistics) !StatsColumn {
        var path: schema.StringList = .empty;
        try path.append(a, "c");
        var rg: schema.RowGroup = .{ .columns = .empty, .total_byte_size = 0, .num_rows = 100 };
        try rg.columns.append(a, .{ .file_path = null, .file_offset = 0, .meta_data = .{
            .type = elem.type.?,
            .encodings = .empty,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 0,
            .total_compressed_size = 0,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = stats,
        } });
        var items: std.ArrayListUnmanaged(schema.SchemaElement) = .empty;
        try items.append(a, .{
            .type = null,
            .type_length = null,
            .repetition_type = null,
            .name = "schema",
            .num_children = 1,
            .scale = null,
            .precision = null,
            .field_id = null,
        });
        var c = elem;
        c.name = "c";
        c.num_children = 0;
        try items.append(a, c);
        return .{
            .rg = rg,
            .fm = .{ .version = 1, .schema = items, .num_rows = 100, .created_by = null, .row_groups = .empty },
        };
    }
};

/// An OPTIONAL leaf `c` of physical type `t` with no annotation.
pub fn statsLeaf(t: schema.Type) schema.SchemaElement {
    return .{
        .type = t,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "c",
        .num_children = 0,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
}
