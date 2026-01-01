# ZPQ Hardening Strategic Roadmap

This document outlines the optimal execution sequence for the 10 hardening plans. Rather than tackling them numerically, we organize them into strategic phases that prioritize **Trust**, **Stability**, **Validation**, and finally **Proof**.

## Phase 1: Foundation of Trust (Immediate)
**Goal:** Establish a stable build environment, close critical security gaps, and align documentation with reality.

1.  **[Plan 01] Dependency Management (Pin Zig)**
    *   **Why First?** "Chasing nightly" invalidates all other verification work. We cannot verify stability if the compiler changes tomorrow.
    *   **Action:** Pin the exact `zig` hash.
2.  **[Plan 10] TLS Security (Critical)**
    *   **Why Now?** Shipping insecure-by-default code is a reputation killer. It invalidates any "production ready" claim.
    *   **Action:** Enable `verify_certificate = true` by default in `Connection.init`.
3.  **[Plan 03] Dependency Honesty & [Plan 02] Contingency**
    *   **Why Now?** These are purely documentation fixes that reframe the project's integrity.
    *   **Action:** Update README to admit dependencies (`libxev`, `boring_tls`) and document the "exit strategy" for `libxev`.

## Phase 2: Core Stability (High Priority)
**Goal:** Ensure the code actually works under pressure before we try to benchmark it.

4.  **[Plan 05] Flow Control (Backpressure)**
    *   **Why Now?** This is a structural defect in the `Connection` class. Benchmarking without backpressure is meaningless because a fast sender will just OOM the test runner.
    *   **Action:** Implement the Write Queue and High-Water Mark in `connection.zig`.
5.  **[Plan 06] Gap Verification**
    *   **Why Now?** "Zero-allocation gap skipping" is a core differentiator. We must prove it works before marketing it further.
    *   **Action:** Write the `test_gap_skipping.zig` integration test with memory assertions.

## Phase 3: Deep Validation
**Goal:** Root out edge-case bugs and justify architectural complexity.

6.  **[Plan 07] Fuzzing Strategy**
    *   **Why Now?** Fuzzing takes time. Starting the harness now allows it to run while we work on other tasks.
    *   **Action:** Create `fuzz_thrift` and `fuzz_http` targets.
7.  **[Plan 04] DNS Justification**
    *   **Why Now?** We need to know if the complex 3-tier DNS stack is worth keeping or if we should delete code to simplify maintenance.
    *   **Action:** Run the `bench_dns` comparison.

## Phase 4: Empirical Proof
**Goal:** Generate the data that proves ZPQ is better/faster/cheaper.

8.  **[Plan 08] Reproducible Benchmarks**
    *   **Why Now?** Now that the code is stable (Phase 2) and secure (Phase 1), we can generate fair numbers.
    *   **Action:** Create the Dockerized benchmark suite.
9.  **[Plan 09] Lambda End-to-End**
    *   **Why Last?** This is the most expensive verification step (requires AWS deploy). It validates the *entire* stack.
    *   **Action:** Build and deploy the `lambda_bench` comparison.

---

## Execution Logic

*   **Dependencies:** Phase 2 (Stability) MUST happen before Phase 4 (Benchmarks). Benchmarking buggy/OOM-prone code provides useless data.
*   **Parallelism:** Phase 3 (Fuzzing) can run in the background while Phase 4 is being built.
*   **Documentation:** Updates to `STATUS.md` should happen at the end of each Phase to reflect the new level of maturity.

