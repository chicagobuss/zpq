//! Zig 0.17's WASI bootstrap sets SP to allocation.ptr + 64 KiB, before
//! the reserved stack rather than after it. Put the requested downward-growing
//! stack before that pointer. Keep the original allocation for TLS/metadata.
const std = @import("std");
const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;
const backing = std.heap.c_allocator;
pub fn allocator(stack_size: usize) Allocator {
    const pad = std.mem.alignForward(usize, @max(stack_size, std.wasm.page_size), std.wasm.page_size);
    return .{ .ptr = @ptrFromInt(pad), .vtable = &vtable };
}
const vtable: Allocator.VTable = .{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };
fn alloc(ctx: *anyopaque, len: usize, alignment: Alignment, ra: usize) ?[*]u8 {
    const pad = @intFromPtr(ctx);
    if (pad % alignment.toByteUnits() != 0) return null;
    const total = std.math.add(usize, pad, len) catch return null;
    const base = backing.rawAlloc(total, alignment, ra) orelse return null;
    return base + pad;
}
fn resize(_: *anyopaque, _: []u8, _: Alignment, _: usize, _: usize) bool {
    return false;
}
fn remap(_: *anyopaque, _: []u8, _: Alignment, _: usize, _: usize) ?[*]u8 {
    return null;
}
fn free(ctx: *anyopaque, buf: []u8, alignment: Alignment, ra: usize) void {
    const pad = @intFromPtr(ctx);
    backing.rawFree((buf.ptr - pad)[0 .. buf.len + pad], alignment, ra);
}
