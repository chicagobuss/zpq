# ZPQ Vendor Changes: Minish

We have vendored `minish` from `references/minish` to `vendor/minish`. 
The following patches have been applied to support Zig 0.16.dev and our specific testing needs.

## 1. Zig 0.16.dev Compatibility
**Files:** `src/minish/gen.zig`, `src/minish/runner.zig`

*   **`std.meta.Int`**: Replaced `@Type(.{ .int = ... })` with `std.meta.Int(.unsigned, bits)` in `gen.zig`. The `@Type` builtin is no longer the preferred way to construct integer types from info in recent Zig versions, or usage details changed.
*   **`std.time`**: Replaced `std.time.milliTimestamp()` (removed) with `std.time.Instant.now()` and a hashing step in `runner.zig` to generate a seed.

## 2. Stateful Property Contexts
**File:** `src/minish/runner.zig`

*   **Method Injection**: Modified the `check` (and `shrink`) logic to support passing a `struct` instance as the `test_fn`.
    *   If `test_fn` is a struct with a `pub fn run(self, val) !void` method, it is called as `test_fn.run(value)`.
    *   This allows us to inject `std.mem.Allocator` or other context (like `libxev` loops) into property tests, which is essential for ZPQ's allocator-heavy architecture.
    *   *Original Minish only supported pure functions `fn(T) !void`.*

