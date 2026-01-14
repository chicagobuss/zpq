const std = @import("std");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();

    var client = std.http.Client{ .allocator = allocator, .io = threaded.io() };
    defer client.deinit();

    const uri = try std.Uri.parse("http://example.com");
    var req = try client.request(.GET, uri, .{});
    defer req.deinit();

    // Probe Request
    std.debug.print("Request Fields/Decls:\n", .{});
    const RequestType = @TypeOf(req);
    switch (@typeInfo(RequestType)) {
        .@"struct" => |s| {
             inline for (s.fields) |f| {
                 std.debug.print("  Field: {s}: {s}\n", .{f.name, @typeName(f.type)});
             }
             inline for (s.decls) |d| {
                 std.debug.print("    Decl: {s}\n", .{d.name});
             }
        },
        else => {},
    }

    // Probe VTable
    if (@hasField(RequestType, "reader")) {
         const ReaderType = @TypeOf(req.reader);
         if (@hasField(ReaderType, "in")) {
             const InPtrType = @TypeOf(req.reader.in);
             const InStructType = @typeInfo(InPtrType).pointer.child;
             if (@hasField(InStructType, "vtable")) {
                 const VTablePtrType = @TypeOf(@as(InStructType, undefined).vtable);
                 const VTableType = @typeInfo(VTablePtrType).pointer.child;
                 std.debug.print("VTable Fields:\n", .{});
                 switch (@typeInfo(VTableType)) {
                    .@"struct" => |s| {
                        inline for (s.fields) |f| {
                            std.debug.print("  {s}: {s}\n", .{f.name, @typeName(f.type)});
                        }
                    },
                    else => {},
                 }
             }
         }
    }
}
