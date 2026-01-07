# Zig 0.16.x Master Rules

## Data Structures
- **ArrayList**: `std.ArrayList(T)` acts like `ArrayListUnmanaged` (doesn't store allocator for operations).
    - **CRITICAL**: You MUST pass the allocator to `deinit(allocator)`, `appendSlice(allocator, ...)`, `appendNTimes(allocator, ...)`.
    - Use `initCapacity(allocator, ...)` where possible.
- **StringHashMap**: Use `std.StringHashMap`.
- **Std Lib**: `std.io` is currently renamed to `std.Io` (Type). Check introspection if imports fail.

## Time
- **Timestamp**: Use `std.time.timestamp()` for seconds (i64).
- **MilliTimestamp**: Use `std.time.milliTimestamp()` for ms (i64).

## Build System
- **addExecutable**: Returns `*std.Build.Step.Compile`.
- **root_module**: Use `exe.root_module.addImport("name", module)` instead of `exe.addModule`.
- **Optimization**: Use `optimize = b.standardOptimizeOption(.{})`.

## libxev
- **Usage**: Design for `xev`. It provides the Event Loop.
- **Completion**: Operations return `void` and take a callback + completion struct.
- **ThreadPool**: Use `xev.ThreadPool` for blocking tasks (DNS, file I/O if not async).

## General
- **Naming**: Zig 0.16.x is stricter. Fix issues as they arise (case sensitivity etc).
- **Circular Imports**: Be careful between `zpq.zig` (root) and submodules.
    - **CRITICAL**: Use relative imports (e.g., `@import("../protocol/s3.zig")`) inside submodules to avoid going through `zpq` root which causes dependency cycles.
