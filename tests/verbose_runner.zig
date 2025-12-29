const std = @import("std");
const builtin = @import("builtin");

pub fn main() void {
    const test_fn_list = builtin.test_functions;
    var ok_count: usize = 0;
    var skip_count: usize = 0;
    var fail_count: usize = 0;

    std.debug.print("\n[ VERBOSE TEST RUNNER ]\n", .{});
    std.debug.print("Found {d} tests\n\n", .{test_fn_list.len});

    for (test_fn_list, 0..) |test_fn, i| {
        std.debug.print("[{d}/{d}] [ RUN  ] {s}\n", .{ i + 1, test_fn_list.len, test_fn.name });

        // Note: we don't capture logs here so that std.debug.print output
        // from the tests themselves flows directly to stderr.

        if (test_fn.func()) |_| {
            ok_count += 1;
            std.debug.print("[      ] [  OK  ] {s}\n", .{test_fn.name});
        } else |err| {
            if (err == error.SkipZigTest) {
                skip_count += 1;
                std.debug.print("[      ] [ SKIP ] {s}\n", .{test_fn.name});
            } else {
                fail_count += 1;
                std.debug.print("[      ] [ FAIL ] {s} (Error: {any})\n", .{ test_fn.name, err });
                if (@errorReturnTrace()) |trace| {
                    std.debug.dumpStackTrace(trace);
                }
            }
        }
    }

    std.debug.print("\n[ SUMMARY ]\n", .{});
    std.debug.print("Passed:  {d}\n", .{ok_count});
    std.debug.print("Skipped: {d}\n", .{skip_count});
    std.debug.print("Failed:  {d}\n", .{fail_count});

    if (fail_count > 0) {
        std.process.exit(1);
    }
}
