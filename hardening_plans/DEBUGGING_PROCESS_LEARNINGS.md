# Debugging Post-Mortem: Building a Faster Feedback Loop

This document captures "meta-learnings" from the TLS/Transport debugging session. It outlines how we moved from 60-second "stalls" to 5-second "successes."

## The Problem: The "Opaque Hang"
Initially, the async transport layer failed silently. The 60-second `no_output_timeout.py` wrapper was necessary because the process gave no indication of progress or failure after the DNS resolution leg.

## What We Learned (About Fixing Bugs Better)

### 1. The Power of Isolation (Micro-Tests)
Trying to debug TLS inside an S3-factory-inside-a-Parquet-reader is a recipe for despair.
- **Action**: We built `probes/probe_tls_echo.zig` which bypassed AWS entirely.
- **Learning**: If the transport works for Google, but not for S3, the bug is in the AWS config. If it fails for both, the bug is in the `Connection` struct. This "halving the search space" was critical.

### 2. Radical Logging
We transitioned from "silent but correct" to "verbose and screaming."
- **Action**: Added granular debug prints for every TCP read/write and TLS record decryption.
- **Learning**: Seeing `[Connection] Decrypted extra 4096 bytes from BIO` proved that we were missing data in the initial implementation. *Logs are not just for errors—they are for verifying flow.*

### 3. Faster Timeouts = More Iterations
Waiting 60 seconds for a failure is too slow.
- **Action**: Shortened the idle timeout to 5 seconds.
- **Learning**: If a network operation doesn't show *any* log activity within 5 seconds, it is effectively deadlocked. Shortening the timeout forced us to fix the code rather than waiting for "network jitter" that wasn't there.

### 4. Consult the Source (The "Source-First" Mandate)
We found a bug where BoringSSL's `processIncoming` wasn't being called enough.
- **Action**: Searched the web for "async BIO pump patterns" and compared our implementation to the `boring_tls` source.
- **Learning**: Zig 0.16.dev and experimental libraries require us to assume the documentation is incomplete. Reading the library code is faster than guessing parameters.

## New Debugging "Playbook"
These findings have been codified into **`.cursor/rules/01-architecture.mdc` (Section 8)**. Next time we hit a stall, we will:
1.  **Drop to 5s timeouts immediately.**
2.  **Create a `probes/probe_X.zig`** that isolates the failing component.
3.  **Add `std.debug.print`** at the entry and exit of every callback.
4.  **Verify state invariants** (e.g., `write_in_flight`, `handshake_complete`) in every `pump()` call.

## Conclusion
The session was successful because we stopped trying to "fix the code" and started "fixing the feedback loop." Once the loop was fast and the logs were loud, the bugs became obvious.

