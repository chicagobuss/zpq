const std = @import("std");
const testing = std.testing;
const metadata = @import("../parquet/metadata.zig");

pub fn loadFixture(path: []const u8) !?[]u8 {
    return metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{path});
            return null;
        }
        return err;
    };
}


pub const full_match_fixture = "ci/fixtures/parquet/full_match.parquet";

pub fn loadFullMatchFixture() !?[]u8 {
    return metadata.readFileSlice(full_match_fixture, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{full_match_fixture});
            return null;
        }
        return err;
    };
}

