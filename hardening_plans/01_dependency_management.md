# Plan 01: Dependency Management & Build Reproducibility

## Criticism
**"The Spirit of Zig 0.16 is Vaporware Chasing."**
Building on nightly (`0.16.dev`) without a pinned version makes the project fragile and irreproducible.

## Response
**Valid.** We are relying on the user's installed `zig` version, which changes daily. This guarantees breakage.

## Action Plan

### 1. Pin Compiler Version
Create a `.zig-version` file (compatible with `zvm`) and/or `zig_version.txt` in the root.
- **Action**: Commit the exact nightly hash we are currently stable on (e.g., `0.16.0-dev.1234+sha`).
- **Action**: Update `build.zig` to optionally check `builtin.zig_version` and warn or error if it mismatches significantly.

### 2. Update Policy
Document a process for updating the nightly version:
1.  Bump version in `.zig-version`.
2.  Run full test suite.
3.  Fix `std` breakages.
4.  Commit.

### 3. CI Integration
Update the CI workflow (Github Actions) to fetch the *exact* version specified in `.zig-version` rather than `master`.

## Defense of "Spirit of 0.16"
We maintain that aligning with 0.16 patterns (unmanaged) is correct for future-proofing, but we acknowledge that *build stability* requires pinning.

