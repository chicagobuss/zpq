# Plan 02: Libxev & BoringTLS Contingency

## Criticism
**"Libxev / BoringTLS Bridge is a Single Point of Failure."**
Depending on a single-maintainer library for the core event loop is risky.

## Response
**Valid but Managed.** The risk is real, but the alternative (using the currently broken/incomplete `std.Io` event loop) is worse. `libxev` is the *only* viable path to `io_uring` and `kqueue` performance right now.

## Action Plan

### 1. Define "Exit Criteria"
Explicitly document the conditions under which we will migrate off `libxev` and back to `std`:
- **Condition**: Zig `std.event.Loop` supports `io_uring` (Linux) and `kqueue` (macOS) with feature parity (timeouts, cancellation).
- **Condition**: Zig `std.http.Client` supports fully non-blocking I/O driven by the external loop.

### 2. Abstraction Enforcement
Ensure strict compilation firewalls.
- **Audit**: `grep` the codebase to ensure `xev` types do not leak into `src/zpq/core/` or public APIs.
- **Refactor**: If leaks are found (e.g., `Address` in public structs), wrap them in ZPQ-owned types.

### 3. Upstream Contribution
If we encounter bugs in `libxev` or `boring_tls`, we commit to:
1.  Forking locally for immediate fixes.
2.  Submitting PRs upstream.
3.  Not "stranding" our fork.

## Conclusion
We treat `libxev` as a "polyfill for the future std lib".

