//! Build-time copy of Mach-O static archives with every symbol hidden and their weak functions made local.
//!
//!   hide_symbols <in.a> <out.a> [<in.a> <out.a> ...]
//!
//! Sets N_PEXT on each defined external symbol of every 64-bit Mach-O member, which is what clang emits for
//! `-fvisibility=hidden` and what ld64's `-load_hidden` does at link time; Zig's Mach-O linker has no such option.
//! The symbols still resolve between the archives and the rest of the link, but an executable no longer exports them.
//!
//! A weak function of default visibility (an inline function the C++ compiler emitted in each object that uses it)
//! becomes local to its object instead, as BoringSSL's C build has them (`OPENSSL_INLINE` is `static inline` there).
//! Hidden is not enough: code compiled for default visibility takes such a function's address through the GOT, and
//! Zig 0.17's linker gives every GOT entry of a weak symbol a weak binding for dyld, hidden or not
//! (src/link/MachO/dyld_info/bind.zig, `WeakBind`). No object may reference another's copy, or the step fails. Weak
//! data (C++ type info) keeps one coalesced copy, so it is only hidden, as are weak definitions compiled hidden: code
//! compiled for hidden visibility reaches them directly.
//!
//! Only flag bytes change, so each archive keeps its layout and its table of contents.

const std = @import("std");
const macho = std.macho;

pub fn main(init: std.process.Init) !u8 {
    const io = init.io;
    const arena = init.arena.allocator();
    const args = try init.minimal.args.toSlice(arena);
    if (args.len < 3 or args.len % 2 == 0) {
        std.log.err("usage: hide_symbols <in.a> <out.a> [<in.a> <out.a> ...]", .{});
        return 2;
    }
    const cwd = std.Io.Dir.cwd();
    var names: Names = .{};
    const archives = try arena.alloc([]u8, args.len / 2);
    for (archives, 0..) |*bytes, i| {
        bytes.* = try cwd.readFileAlloc(io, args[1 + 2 * i], arena, .limited(1 << 30));
        hideArchive(arena, bytes.*, &names) catch |err| {
            std.log.err("{s}: {t}", .{ args[1 + 2 * i], err });
            return 1;
        };
    }
    var it = names.localized.keyIterator();
    while (it.next()) |name| if (names.undefined.contains(name.*)) {
        std.log.err("{s} is weak, so made local, but another object references it", .{name.*});
        return 1;
    };
    for (archives, 0..) |bytes, i| try cwd.writeFile(io, .{ .sub_path = args[2 + 2 * i], .data = bytes });
    return 0;
}

const Names = struct {
    localized: std.StringHashMapUnmanaged(void) = .empty,
    undefined: std.StringHashMapUnmanaged(void) = .empty,
};

fn hideArchive(gpa: std.mem.Allocator, bytes: []u8, names: *Names) !void {
    if (!std.mem.startsWith(u8, bytes, "!<arch>\n")) return error.NotAnArchive;
    var pos: usize = 8;
    while (pos < bytes.len) {
        if (bytes.len - pos < 60) return error.TruncatedArchive;
        const header = bytes[pos..][0..60];
        const size = try std.fmt.parseInt(usize, std.mem.trimEnd(u8, header[48..58], " "), 10);
        const body = pos + 60;
        if (size > bytes.len - body) return error.TruncatedArchive;
        // BSD long names, "#1/<len>", sit at the start of the member's data.
        const name_len = if (std.mem.startsWith(u8, header, "#1/"))
            try std.fmt.parseInt(usize, std.mem.trimEnd(u8, header[3..16], " "), 10)
        else
            0;
        if (name_len > size) return error.TruncatedArchive;
        const name: []const u8 = if (name_len > 0) bytes[body..][0..name_len] else header[0..16];
        if (!std.mem.startsWith(u8, name, "__.SYMDEF")) try hideObject(gpa, bytes[body + name_len .. body + size], names);
        pos = body + size + (size & 1);
    }
}

fn hideObject(gpa: std.mem.Allocator, object: []u8, names: *Names) !void {
    if (object.len < @sizeOf(macho.mach_header_64) or
        std.mem.readInt(u32, object[0..4], .little) != macho.MH_MAGIC_64) return error.NotAMachO64Object;
    const ncmds = std.mem.readInt(u32, object[16..20], .little);
    // Section n (from 1) holds code when it has either instructions attribute.
    var code_sections: std.StaticBitSet(256) = .empty;
    var nsect: usize = 0;
    var symtab: ?usize = null;
    var off: usize = @sizeOf(macho.mach_header_64);
    for (0..ncmds) |_| {
        if (object.len - off < 8) return error.TruncatedObject;
        const cmd = std.mem.readInt(u32, object[off..][0..4], .little);
        const cmdsize = std.mem.readInt(u32, object[off + 4 ..][0..4], .little);
        if (cmdsize < 8 or cmdsize > object.len - off) return error.TruncatedObject;
        if (cmd == @backingInt(macho.LC.SEGMENT_64)) {
            const n = std.mem.readInt(u32, object[off + 64 ..][0..4], .little);
            if (n > (cmdsize - @sizeOf(macho.segment_command_64)) / @sizeOf(macho.section_64)) return error.TruncatedObject;
            for (0..n) |s| {
                const sect = object[off + @sizeOf(macho.segment_command_64) + s * @sizeOf(macho.section_64) ..];
                nsect += 1;
                if (nsect > 255) return error.TooManySections;
                const code = macho.S_ATTR_SOME_INSTRUCTIONS | macho.S_ATTR_PURE_INSTRUCTIONS;
                if (std.mem.readInt(u32, sect[64..68], .little) & code != 0) code_sections.set(nsect);
            }
        } else if (cmd == @backingInt(macho.LC.SYMTAB)) {
            if (cmdsize < @sizeOf(macho.symtab_command)) return error.TruncatedObject;
            symtab = off;
        }
        off += cmdsize;
    }
    const cmd = symtab orelse return;
    const symoff = std.mem.readInt(u32, object[cmd + 8 ..][0..4], .little);
    const nsyms = std.mem.readInt(u32, object[cmd + 12 ..][0..4], .little);
    const stroff = std.mem.readInt(u32, object[cmd + 16 ..][0..4], .little);
    const strsize = std.mem.readInt(u32, object[cmd + 20 ..][0..4], .little);
    if (symoff > object.len or nsyms > (object.len - symoff) / @sizeOf(macho.nlist_64) or
        stroff > object.len or strsize > object.len - stroff) return error.TruncatedObject;
    const strtab = object[stroff..][0..strsize];
    for (0..nsyms) |i| {
        const nlist = object[symoff + i * @sizeOf(macho.nlist_64) ..][0..@sizeOf(macho.nlist_64)];
        const n_type = &nlist[4];
        if (n_type.* & macho.N_STAB != 0 or n_type.* & macho.N_EXT == 0) continue;
        const strx = std.mem.readInt(u32, nlist[0..4], .little);
        if (strx >= strtab.len) return error.TruncatedObject;
        const name = std.mem.sliceTo(strtab[strx..], 0);
        const desc = std.mem.readInt(u16, nlist[6..8], .little);
        if (n_type.* & macho.N_TYPE == macho.N_UNDF) {
            // A common symbol is undefined with a size; it would stay global.
            if (std.mem.readInt(u64, nlist[8..16], .little) != 0) return error.CommonSymbol;
            try names.undefined.put(gpa, name, {});
        } else if (desc & macho.N_WEAK_DEF != 0 and n_type.* & macho.N_PEXT == 0 and
            n_type.* & macho.N_TYPE == macho.N_SECT and code_sections.isSet(nlist[5]))
        {
            n_type.* &= ~@as(u8, macho.N_EXT | macho.N_PEXT);
            std.mem.writeInt(u16, nlist[6..8], desc & ~(macho.N_WEAK_DEF | macho.N_WEAK_REF), .little);
            try names.localized.put(gpa, name, {});
        } else {
            n_type.* |= macho.N_PEXT;
        }
    }
}
