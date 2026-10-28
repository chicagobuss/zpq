const std = @import("std");
const testing = std.testing;
const consumer = @import("../consumer.zig");
const Worker = @import("worker_contracts.zig").Worker;
const firstWorkerFailure = @import("worker.zig").firstWorkerFailure;
const runWorkers = @import("worker.zig").runWorkers;

/// Puts back the row and row-group counts a discarded pass added.
pub fn restoreCounts(workers: []Worker, saved: []const Worker) void {
    for (workers, saved) |*w, s| {
        w.rows_in = s.rows_in;
        w.rows_scanned = s.rows_scanned;
        w.rows_matched = s.rows_matched;
        w.rgs_in = s.rgs_in;
        w.rgs_pruned = s.rgs_pruned;
        w.rgs_full_match = s.rgs_full_match;
    }
}

/// One pass of `workers` over every work item, from a fresh cursor.
/// Clears an optional phase's expected failures and partial decode evidence before its exact retry.
/// Other failures still propagate. `runPhase` chooses refusals canonically before deciding to retry.
pub fn clearWorkerErrors(workers: []Worker, expected: anyerror) !void {
    const exact_retry = expected == error.ExceededMemoryBudget or expected == error.CensusAbandoned;
    for (workers) |*w| if (w.err) |we| if (we == expected or
        (exact_retry and consumer.isDecodeError(we)))
    {
        // The optional census scans exactly from the start next. Its partial projection cannot make a payload
        // error final while an earlier refused/abandoned unit may conceal another error. Trial memory is never
        // the query's refusal: discard the trial and let the established scan decide what fits.
        w.err = null;
    };
    if (firstWorkerFailure(workers, null)) |failure| return failure.err;
}

pub fn runPhase(workers: []Worker, cursor: *std.atomic.Value(usize)) !void {
    cursor.store(0, .monotonic);
    runWorkers(workers);
    if (phaseFailure(workers)) |err| return err;
}

pub fn phaseFailure(workers: []const Worker) ?anyerror {
    for (workers) |w| if (w.err) |err| if (err == error.CensusAbandoned) return err;
    return if (firstWorkerFailure(workers, null)) |failure| failure.err else null;
}

test "clearWorkerErrors: a phase's expected failure is cleared on every worker, any other is returned" {
    var ws: [3]Worker = undefined;
    for (&ws) |*w| {
        w.err = null;
        w.failure_work = null;
        w.failure = null;
    }
    // The census trial's budget on one worker, a corrupt page on a later one: the page's error is the query's.
    ws[0].err = error.ExceededMemoryBudget;
    ws[2].err = error.CorruptPage;
    try testing.expectError(error.CorruptPage, clearWorkerErrors(&ws, error.ExceededMemoryBudget));
    try testing.expectEqual(@as(?anyerror, null), ws[0].err);
    ws[2].err = error.ExceededMemoryBudget;
    ws[1].err = error.ExceededMemoryBudget;
    try clearWorkerErrors(&ws, error.ExceededMemoryBudget);
    for (ws) |w| try testing.expectEqual(@as(?anyerror, null), w.err);
}
