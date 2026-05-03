# Event Loop Design

The contract for `src/io/`. Read this before writing or modifying any
event-loop code.

## Provenance

Studied before writing:
- **libxev `src/backend/epoll.zig`** — closest competitor; ~600 LoC.
- **tigerbeetle `src/io/linux.zig`** — single-threaded, deterministic,
  production-tested at scale. Closest in spirit to ZPQ.
- **`std.os.linux.IoUring`** (Zig 0.16 stdlib) — thin syscall wrapper
  we'll layer Phase B on top of, not replace.

We're not adopting either implementation wholesale. We *are* adopting
their hard-won design decisions where they agree, and choosing
tigerbeetle's idioms where they differ.

## Decisions

### 1. Run model: `run()` + `run_for_ns(timeout)`, no `tick()`

```zig
pub fn run(self: *Loop) RunError!void;            // single non-blocking pass
pub fn run_for_ns(self: *Loop, ns: u63) RunError!void;  // block up to timeout
```

`run()` flushes pending submissions, drains ready completions, returns.
`run_for_ns()` loops until either the deadline expires or a completion
fires. Both invoke callbacks synchronously.

**Rejected alternative**: libxev's `loop.run(.until_done)`. That API
encourages "fire and forget" callers and requires the loop to know
about completion lifetimes. Tigerbeetle's split lets the *caller* own
the orchestration (the S3 streamer, the multipart coordinator) and use
the loop as a primitive.

### 2. Completion is caller-allocated, holds an `Operation` tagged union

```zig
pub const Completion = struct {
    op: Operation,
    userdata: ?*anyopaque,
    callback: *const fn (?*anyopaque, *Loop, *Completion, Result) void,
    // Internal fields for queue linkage and state — caller does not touch.
    next: ?*Completion = null,
    state: State = .dead,
};

pub const Operation = union(enum) {
    timer: struct { deadline_ns: u64 },
    connect: struct { fd: fd_t, addr: *const anyopaque, addrlen: u32 },
    recv:    struct { fd: fd_t, buffer: []u8 },
    send:    struct { fd: fd_t, buffer: []const u8 },
    // Phase A stops here. Add ops as the code that needs them lands.
};

pub const Result = union(enum) {
    timer:   void,
    connect: ConnectError!void,
    recv:    RecvError!usize,
    send:    SendError!usize,
};
```

Caller owns the storage; the loop never allocates per-operation.
Inlined ops match both libxev and tigerbeetle.

### 3. No reentrancy guards; deferred-callback pattern instead

`run()` works in two phases:
1. Drain `completed` queue: pop each Completion, invoke its callback.
2. `epoll_wait` for new events; for each event, mark the Completion
   ready and push to `completed` queue. Do **not** invoke the callback
   inline.

This means a callback that calls `loop.submit(another_completion)` is
safe — the new Completion goes on the submissions queue and is picked
up next iteration. No "nested runs not allowed" error.

This is the single most important design choice in the whole loop.
It's the reason tigerbeetle gets away without recursion guards, and the
reason libxev needs them.

### 4. Submissions queued, not applied immediately

```zig
pub fn submit(self: *Loop, completion: *Completion) void;
```

`submit()` puts the completion in `submissions`, sets state to
`.adding`, increments `active`. The actual `epoll_ctl(EPOLL_CTL_ADD)`
syscall happens at the start of the next `run()` call.

**Why deferred**: lets a callback submit follow-up ops without
recursive `epoll_ctl` calls. Also lets us batch SQ submissions in
Phase B without changing the API.

### 5. Level-triggered, EPOLLONESHOT

Phase A uses `EPOLLIN | EPOLLOUT | EPOLLONESHOT` on each registration.

- **Level-triggered** because edge-triggered requires draining until
  EAGAIN on every event — fine for high-throughput servers, overkill
  for our 8-concurrent-multipart-upload workload, and easy to get
  wrong.
- **`EPOLLONESHOT`** because each Completion represents one operation.
  After fire, epoll auto-disarms; we re-arm via `EPOLL_CTL_MOD` if and
  when the caller re-submits. Avoids the bookkeeping of separately
  tracking which fd is registered.

### 6. Multi-FD bookkeeping: stash the Completion pointer in epoll_event.data

`struct epoll_event` has a `data` union (u64 / ptr / fd). We store the
`*Completion` directly via `data.ptr`, retrieve it on `epoll_wait`.

No hashmap, no array, no scanning. The kernel does the lookup for us.

### 7. Cancellation: deferred to Phase A.5

Phase A does not support cancellation. The Lambda use case (one
S3-to-S3 stream per invocation, all completions complete before exit)
doesn't need it. Add when we have:
- A timeout that needs to abort an in-flight `recv` (Phase B-ish).
- A pipeline that needs to drop a column read mid-flight.

When we add it, we'll do it tigerbeetle-style: a `.cancel` operation
that targets another Completion by pointer.

### 8. Phase A op surface

Minimum viable for Lambda's runtime-API loop and the future S3 sink:
- `timer`     — via `timerfd_create` + epoll. One-shot. (Phase A
  smoke test: schedule a timer, wait for it to fire.)
- `connect`   — non-blocking `connect()` + epoll for OUT.
- `recv`      — partial-read aware. Caller responsible for retry on
  short reads.
- `send`      — partial-write aware. Same.

Not in Phase A:
- `accept`, `fsync`, `openat`, `pread`/`pwrite`, `splice`,
  `recv_zerocopy`, multishot. These ship when the code that needs
  them lands.

### 9. Loop API surface (final)

```zig
pub const Loop = struct {
    pub fn init(allocator: std.mem.Allocator) InitError!Loop;
    pub fn deinit(self: *Loop) void;

    pub fn submit(self: *Loop, completion: *Completion) void;

    pub fn run(self: *Loop) RunError!void;
    pub fn run_for_ns(self: *Loop, ns: u63) RunError!void;

    pub fn active(self: *const Loop) usize;  // in-flight count
};
```

Six functions. That's the contract.

### 10. File layout

```
src/io/
  loop.zig      Comptime backend selector. Resolves Loop = epoll | iouring | kqueue
                based on (target_os, build_options.lambda).
  epoll.zig     Phase A. The implementation.
  iouring.zig   Phase B (when CLI hot path lands).
  kqueue.zig    Phase C (when macOS dev needs it).
  strategy.zig  IOStrategy duck-typing trait + MemoryReader (already exists).
```

For Phase A, `iouring.zig` and `kqueue.zig` don't exist. The selector
in `loop.zig` `@compileError`s on those targets so we don't ship a
binary that imports a non-existent file.

## What this design explicitly rejects

- **Thread pools.** libxev has one for blocking ops. We don't. Lambda
  is CPU-metered; spawning threads we don't control is wasteful. The
  CLI's hot path is bounded enough not to need it.
- **`std.Io.Net` integration.** The vtable model is wrong shape for our
  static-binary, no-runtime story. Stdlib `IoUring` is fine because
  it's a syscall wrapper, not an interface.
- **Multishot recv (Phase A).** Saves syscalls but only matters at
  high message rates. Our 8 connections × multipart-chunk pace doesn't
  exercise it.
- **General-purpose timer wheel.** Phase A schedules each timer as its
  own `timerfd_create` + `EPOLL_CTL_ADD`. With our handful of
  timeouts at any moment, that's cheaper than maintaining a heap.

## What this design defers

- **Cancellation.** Tigerbeetle-style `.cancel` op when the use case appears.
- **Edge-triggered mode.** If multishot recv ever lands, edge-triggered
  becomes natural; revisit then.
- **Cross-thread submission.** Loop is single-threaded. Multi-thread
  designs can put a `Loop` per worker.

## Test plan

The Phase A `epoll.zig` ships with:
1. **Self-pipe test**: create `pipe2(O_NONBLOCK)`, register read end with
   the loop, submit a write, run until completion fires. Verifies the
   epoll register/wait/dispatch path end-to-end.
2. **Timer test**: submit a 1 ms timer, run, verify it fires within
   2× the deadline.
3. **Concurrent ops test**: submit two timers with different deadlines,
   verify they fire in order.

Lambda smoke test: `src/lambda/main.zig` schedules a 1 ms timer on
startup, runs the loop until it fires, prints success. Confirms the
binary works in AWS.
