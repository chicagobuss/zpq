---
trigger: always_on
---

# Architecture & Principles

## Core Principle: Laziness
- Keep data encoded as long as possible.
- Decode only at boundaries where transformation is required.
- Dictionary encoding should persist through the pipeline.

## I/O Architecture
- **Sans-IO**: Protocol logic (state machines, parsers) MUST be decoupled from socket operations.
- **Transport**: Use the project's `transport.Connection` abstraction. Do not use raw `xev` loops in business logic if possible.

## Network / IO
- **DNS Resolution**: ALWAYS use `xev` based resolution (e.g. `transport.Resolver`). **Do NOT** use `std.net.getAddressList` or `std.c.getaddrinfo` directly as they block or are unstable on HEAD.
- **Signals**: Handle `SIGPIPE` for high-performance IO.

## Memory
- **Arenas**: Use `ArenaAllocator` for request lifecycles to simplify cleanup.

## Legacy Code
- **src_legacy**: Treat as read-only reference. Do not modify unless strictly necessary for migration.
