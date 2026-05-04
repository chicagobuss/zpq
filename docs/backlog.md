# Backlog — durable cross-cutting notes

Things worth doing eventually, but not blocking current work. Each entry
explains the *why* so a future engineer (or future-Claude) can judge
whether it's still relevant.

Per-feature deferred work belongs in that feature's design doc (e.g.
`writer_design.md` has its own future-work tail). This file is for items
that don't have an obvious design-doc home, or that span subsystems.

---

## `src/io/http.zig` — refactor parsing onto `std.http` parsers

**Status:** deferred (post-bakeoff).
**Size:** ~60 LoC delta, mostly deletions.

`std.http` in 0.16 ships standalone `HeadParser`, `ChunkParser`, and
`HeaderIterator`. Our `http.zig` has hand-rolled equivalents:
`parseStatus`, `parseContentLength`, `parseHeaders`,
`parseTransferEncodingChunked`, `chunkedBodyComplete`, `decodeChunked`.

The hand-rolled chunked-transfer code in particular is bug-prone
(subtle CRLF and chunk-extension handling) — the kind of thing
stdlib should own. Replacing those with the stdlib parsers would:

- Remove ~60 LoC of bytes-parsing.
- Eliminate a category of bug we just hit (chunked decode silently
  hanging because we read until close instead of detecting end-chunk).
- Keep the request-builder + response-coordinator code we wrote, which
  is coupled to our `tls.Connection` and is the part that earns its
  keep.

We can NOT replace with `std.http.Client` — that's bound to
`std.Io.net.Stream` + `std.crypto.tls` and we use BoringSSL deliberately
(Tier 1).

**Why deferred:** the chunked fix lands as a working commit. Refactoring
in the middle of an unrelated bakeoff is the wrong time. Pull this in
during a normal cleanup pass once the writer phase has cooled.
