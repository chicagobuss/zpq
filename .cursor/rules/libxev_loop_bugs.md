# libxev Event Loop Bugs and Gotchas

This document captures hard-won lessons from debugging libxev event loop issues.

---

## BUG: loop.stop() permanently breaks the loop

**Date discovered**: 2025-12-26  
**Severity**: Critical  
**Affected code**: `src/zpq/io/s3/xev_source.zig` - `BatchContext.signalDone()`

### Symptoms

- First iteration of parallel S3 requests works correctly (~100-150ms, data fetched)
- Subsequent iterations complete in <2ms with **zero data fetched**
- New connections are created but never complete their work
- `loop.run(.until_done)` returns immediately

### Root Cause

The `BatchContext` was calling `loop.stop()` when all requests in a batch completed:

```zig
// BROKEN CODE
pub fn signalDone(self: *@This()) void {
    self.remaining -= 1;
    if (self.remaining == 0) {
        self.loop.stop();  // <-- THIS IS THE BUG
    }
}
```

**The problem**: `loop.stop()` sets a **permanent** flag (`flags.stopped = true`) that causes all future `loop.run()` calls to exit immediately without processing any events.

From `vendor/libxev/src/backend/io_uring.zig`:
```zig
pub fn stop(self: *Loop) void {
    self.flags.stopped = true;  // Permanent! Never reset!
}

// In the run loop:
if (self.flags.stopped) break;  // Exits immediately
```

### The Fix

Do NOT call `loop.stop()`. The loop naturally exits when `active == 0`:

```zig
// FIXED CODE
pub fn signalDone(self: *@This()) void {
    self.remaining -= 1;
    // NOTE: We do NOT call loop.stop() here because that permanently stops the loop
    // and breaks subsequent iterations. The loop will naturally exit when all
    // active completions are done (active == 0).
}
```

### How to Detect This Bug

Add logging before `loop.run()`:
```zig
log.debug("running loop (loop.stopped={})", .{self.loop.stopped()});
```

If `loop.stopped=true` before you start, the loop was poisoned by a previous `stop()` call.

### When IS loop.stop() Appropriate?

Only use `loop.stop()` when you want to **permanently** shut down the event loop and never use it again (e.g., application shutdown, fatal error handling).

---

## Pattern: Completion-based loop exit

libxev's `.until_done` mode works by tracking active completions:

```zig
// Loop exits when:
if (self.active == 0 and self.submissions.empty()) break;
```

**Key insight**: When a callback returns `.disarm`, the completion becomes inactive. If you want the loop to continue, you must arm new completions (schedule new reads/writes) before returning.

The TLS connection's `pump()` function does this correctly:
```zig
fn pump(self: *Self) void {
    // After processing, schedule the next read
    if (!self.pending_read and !self.idling) {
        self.pending_read = true;
        self.tcp.read(self.loop, &self.c_read, ...);  // Arms new completion
    }
}
```

When `idling = true`, no new completions are armed, so `active` decrements to 0 and the loop exits naturally.

---

## Pattern: Debugging loop.active count

To understand why the loop is (or isn't) running:

```zig
log.debug("before tcp.write: loop.active={d}", .{self.loop.active});
self.tcp.write(self.loop, &self.c_write, ...);
log.debug("after tcp.write: loop.active={d}", .{self.loop.active});
```

Expected behavior:
- `tcp.write()` increments `active` by 1
- When the write callback fires and returns `.disarm`, `active` decrements by 1
- If the callback schedules a new read, `active` stays at 1

---

## Checklist: Debugging "loop exits too early"

1. [ ] Check `loop.stopped()` - was `loop.stop()` called?
2. [ ] Check `loop.active` before and after scheduling operations
3. [ ] Verify callbacks are arming new completions (calling `pump()` or scheduling reads/writes)
4. [ ] Check for `idling = true` preventing new reads from being scheduled
5. [ ] Verify `pending_read`/`pending_write` flags are being reset properly on connection reuse

---

## Checklist: Debugging "loop never exits"

1. [ ] Check if a read is pending on a closed connection (EOF not handled)
2. [ ] Check if `idling` is never set to true after request completion
3. [ ] Check for callback returning `.rearm` when it should return `.disarm`
4. [ ] Verify error paths call `signalDone()` or otherwise clean up
