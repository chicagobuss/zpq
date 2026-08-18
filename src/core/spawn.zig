// ! Thread spawning with a test-only failure seam. ! ! Every parallel region must survive `Thread.spawn` failing
// mid-loop, while ! the threads already running still hold pointers into state the unwinding ! caller is about to free.
// Real spawn failure cannot be provoked cleanly ! (`RLIMIT_NPROC` is process-wide and would break the test runner;
// memory ! pressure is not reproducible), so tests inject it here instead. In release ! `builtin.is_test` is false and
// this folds to a direct `Thread.spawn`.

const std = @import("std");
const builtin = @import("builtin");

/// After this many successful spawns, fail the rest. `null` disables injection. Test-only — never set outside a test.
var fail_after: ?usize = null;
/// Successful spawns since the last `resetFailure`.
var success_count: usize = 0;
/// Spawns actually refused since the last `resetFailure`. Lets a test assert its injection was not vacuous: a region
/// that never spawned would otherwise pass unchanged while testing nothing.
var injected_count: usize = 0;

/// Fail every spawn after `n` successes. Call `resetFailure` when the test is done, or the next test in the same binary
/// inherits the injection.
pub fn injectFailureAfter(n: usize) void {
    std.debug.assert(builtin.is_test);
    fail_after = n;
    success_count = 0;
    injected_count = 0;
}

pub fn resetFailure() void {
    fail_after = null;
    success_count = 0;
    injected_count = 0;
}

/// How many spawns the seam has refused since injection was armed.
pub fn failuresInjected() usize {
    return injected_count;
}

/// Spawns are issued from one thread at a time (the loop that owns the parallel region), so the counter needs no
/// synchronization.
pub fn spawn(
    config: std.Thread.SpawnConfig,
    comptime function: anytype,
    args: anytype,
) std.Thread.SpawnError!std.Thread {
    if (builtin.is_test) {
        if (fail_after) |limit| {
            if (success_count >= limit) {
                injected_count += 1;
                // What real thread-table exhaustion (EAGAIN) surfaces, so callers are exercised on the error they will
                // actually see.
                return error.SystemResources;
            }
            success_count += 1;
        }
    }
    return std.Thread.spawn(config, function, args);
}
