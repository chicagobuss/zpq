# Vendored code

Third-party code built from this tree. Licenses and home pages are in `THIRD_PARTY_NOTICES.md`. Below are the local
changes recorded in git history since each library was imported; the import itself (commit `50d2259`) was not
diffed against upstream here.

## liteparser (`vendor/liteparser`, SQL front end)

- `column_ref` nodes record which parts were quoted in the source (`quoted`, `LP_QUOTED_*` in `liteparser.h`, set
  by the `lp_make_column_ref*` constructors and copied by the node clone), and unparse keeps those parts quoted
  even where SQL would not need it, so `"a"."b"` does not come back as `a.b` and `--query` binds a quoted path the
  way the flag forms do.

## snappy (`vendor/snappy`, google/snappy 1.2.1)

`build.zig`, `build.zig.zon`, `root.zig` and `config.h` are ZPQ's build glue and bindings around the upstream
sources.

- `config.h`: the SSSE3, x86 CRC32 and BMI2 code paths follow the target's CPU features (`__SSSE3__`,
  `__SSE4_2__`, `__BMI2__`) instead of being off; NEON stays off. (`b63f9c4`, `6ca9110`)
- `build.zig`: no longer passes `-mno-avx` on x86. (`b63f9c4`)
- `build.zig`, `root.zig`: `snappy-c.h` is imported through a translate-c step (`snappy_c`) instead of `@cImport`,
  which Zig 0.17 removed; `build.zig.zon` requires Zig 0.17.0.

## boring_tls (`vendor/boring_tls`, Zig bindings over BoringSSL)

- `build.zig` links prebuilt `libcrypto.a`/`libssl.a` from `prebuilt/<triple>/`, fetched from ZPQ's R2 bucket
  when absent (present since the import). The URLs and sha256 digests are now pinned in `prebuilt.sha256`; a missing
  pair is downloaded by a build step (`fetch_prebuilt.zig`) that verifies the digest and leaves the result in the Zig
  cache, instead of by curl into the source tree at configure time.
- `build.zig`, `src/tls.zig`: OpenSSL headers come through a translate-c step instead of `@cImport`, which Zig 0.17
  removed, and the local-prebuilt probe declares its file dependencies to the configure cache, since 0.17 runs the
  configure phase in its own process.
