const std = @import("std");
const zpq = @import("zpq");
const xev = zpq.s3.dns.xev;
const dns = zpq.s3.dns;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("--- Mock S3 Loopback Test (Final Check) ---\n", .{});

    const small_s3_path = std.process.getEnvVarOwned(allocator, "ZPQ_TEST_S3_PATH") catch |err| {
        std.debug.print("Error: Could not find ZPQ_TEST_S3_PATH in environment ({}). Please set it or use .env file.\n", .{err});
        return;
    };
    defer allocator.free(small_s3_path);

    std.debug.print("Running cat on {s}...\n", .{small_s3_path});

    var pf = zpq.s3.factory.openFile(allocator, small_s3_path, true) catch |err| {
        std.debug.print("Factory failed: {}\n", .{err});
        return;
    };
    defer pf.close();

    std.debug.print("Footer read...\n", .{});
    pf.readFooter() catch |err| {
        std.debug.print("Footer read failed: {}\n", .{err});
        return;
    };

    std.debug.print("Footer size: {d}\n", .{pf.file_size});

    if (pf.metadata) |meta| {
        std.debug.print("Row Groups: {d}\n", .{meta.row_groups.items.len});
    }
    std.process.exit(0);
}
