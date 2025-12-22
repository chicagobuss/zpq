# ZPQ Hardening Plan: Overview

This directory contains detailed responses and remediation plans for the critical "Grumpy Code Review" findings. We acknowledge that while the architecture is sound, the rigor required for a "battle-hardened" systems tool is currently lacking in specific areas (security, verification, and flow control).

## Summary of Findings & Actions

| ID | Plan File | Criticism | Severity | Action Summary | Proof Artifact |
|---|---|---|---|---|---|
| **01** | [Dependency Management](./01_dependency_management.md) | Chasing Zig Nightly | Medium | Pin compiler hash; document update policy. | `.zig-version` |
| **02** | [Libxev Contingency](./02_libxev_contingency.md) | Single Point of Failure | Medium | Define "Exit Criteria" for upstream `std.Io` migration. | N/A |
| **03** | [Dependency Honesty](./03_dependency_honesty.md) | "Zero-Dependency" Lie | Low | Reframe as "No Runtime SDK"; Clarify `libc`/`openssl` usage. | N/A |
| **04** | [DNS Justification](./04_dns_complexity_justification.md) | Over-engineered DNS | Low | Benchmark current stack; Keep for "Happy Eyeballs" but document rationale. | [DNS Justification Proof](./04_dns_justification_proof.md) |
| **05** | [Flow Control](./05_flow_control.md) | No Backpressure | **High** | Implement Write Queue & High-Water Mark. | [Backpressure Proof](./05_backpressure_proof.md) |
| **06** | [Gap Verification](./06_gap_skipping_verification.md) | Unverified Features | **High** | Implement `test_gap_skipping.zig` with memory assertions. | [Gap Skip Proof](./06_gap_skip_proof.md) |
| **07** | [Fuzzing Strategy](./07_fuzzing_strategy.md) | No Fuzz Testing | **High** | Integrate `minish` for Thrift & HTTP parsers. | [Fuzzing Proof](./07_fuzzing_proof.md) |
| **08** | [Benchmarks](./08_reproducible_benchmarks.md) | Disingenuous Comparisons | Medium | Create `benches/` with Dockerized repros. | Pending |
| **09** | [Lambda E2E](./09_lambda_end_to_end.md) | Untested "Lambda-First" | **High** | Create `lambda_bench/` for real-world validation. | Pending |
| **10** | [TLS Security](./10_tls_security.md) | Insecure Defaults | **Critical** | **Enable Verification**; Load System Roots. | [TLS Security Proof](./10_tls_security_proof.md) |

## Implementation Strategy

We are tackling these plans in priority order:
1.  **Security & Stability (Complete)**: Implemented Plans 10 (TLS), 05 (Backpressure), and 07 (Fuzzing).
2.  **Verification (In-Progress)**: Plan 06 (Gap Skip - Complete), Plan 09 (Lambda E2E - Started).
3.  **Documentation & Process**: Plan 01 (Pinned), Plan 02, Plan 03, Plan 08.
4.  **Optimization/Justification (Complete)**: Plan 04 (DNS).

