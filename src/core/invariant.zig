//! Invariant checks, and the policy for which kind to use where.
//!
//! Two tiers. The choice between them is deliberate, because we ship
//! ReleaseFast everywhere (so `std.debug.assert` is stripped from the
//! binary strangers actually run):
//!
//!   * `assert(cond)` — a *programmer* invariant: false only when ZPQ
//!     violated its own internal contract. Wraps `std.debug.assert`,
//!     so it fires under Debug / ReleaseSafe (unit tests, the decode
//!     fuzzer, CI) and is **stripped under ReleaseFast**. Use freely on
//!     hot internal paths where an always-on check would cost — it
//!     guards our own runs, documents the invariant, and gives the
//!     fuzzer a tripwire. It does **not** protect the field.
//!
//!   * always-on `error` returns — for any invariant whose violation
//!     would emit *corrupt output* or mishandle *hostile input*. These
//!     must survive into the ReleaseFast binary, so they are
//!     never asserts; they are `if (!cond) return error.X`. The write
//!     path's footer/row-group consistency guard is the canonical case:
//!     a footer that disagrees with the emitted bytes is corrupt output,
//!     and strict readers reject it. That class must fail loudly in every
//!     build.
//!
//! Rule of thumb: if user input can trigger it, it's an `error`; if only
//! internal code can, it's an `assert`. When unsure, make it an error.

const std = @import("std");
const schema = @import("schema.zig");

/// Debug/ReleaseSafe-only programmer-invariant assert. Stripped under
/// ReleaseFast — never use for anything reachable by user input.
pub inline fn assert(ok: bool) void {
    std.debug.assert(ok);
}

/// Number of leaf (primitive) columns in a flat thrift schema. A leaf
/// carries a physical `type`; group nodes — the root, structs, and
/// LIST/MAP wrappers — have `type == null`. This is exactly how many
/// column chunks each row group must contain, so it's the anchor for the
/// footer/row-group consistency guard on the write path.
pub fn footerLeafCount(schema_items: []const schema.SchemaElement) usize {
    var n: usize = 0;
    for (schema_items) |el| {
        if (el.type != null) n += 1;
    }
    return n;
}

test "footerLeafCount counts only primitives, not group nodes" {
    const t = std.testing;
    // root(group) + 2 primitives + a struct group wrapping 1 primitive.
    const leaf = struct {
        fn p(name: []const u8, ty: ?schema.Type, nc: ?i32) schema.SchemaElement {
            return .{
                .type = ty,
                .type_length = null,
                .repetition_type = if (ty == null) null else .REQUIRED,
                .name = name,
                .num_children = nc,
                .scale = null,
                .precision = null,
                .field_id = null,
            };
        }
    }.p;
    // root(group) + 2 primitives + a struct group wrapping 1 primitive.
    const items = [_]schema.SchemaElement{
        leaf("root", null, 3),
        leaf("a", .INT32, null),
        leaf("b", .BYTE_ARRAY, null),
        leaf("s", null, 1),
        leaf("x", .DOUBLE, null),
    };
    try t.expectEqual(@as(usize, 3), footerLeafCount(&items));
}
