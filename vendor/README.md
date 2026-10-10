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
- `build.zig`: `pic` and `hidden-symbols` options, which libzpq's archive sets: position-independent, with its
  symbols hidden so the shared library does not export them.

## boring_tls (`vendor/boring_tls`, Zig bindings over BoringSSL)

- `build.zig` links prebuilt `libcrypto.a`/`libssl.a` from `prebuilt/<triple>/`, fetched from ZPQ's R2 bucket
  when absent (present since the import). The URLs and sha256 digests are now pinned in `prebuilt.sha256`; a missing
  pair is downloaded by a build step (`fetch_prebuilt.zig`) that verifies the digest and leaves the result in the Zig
  cache, instead of by curl into the source tree at configure time.
- `build.zig`, `src/tls.zig`: OpenSSL headers come through a translate-c step instead of `@cImport`, which Zig 0.17
  removed, and the local-prebuilt probe declares its file dependencies to the configure cache, since 0.17 runs the
  configure phase in its own process.
- `build.zig`: a `source-build` option compiles BoringSSL from its source package instead, position-independent and
  with its symbols hidden, for libzpq's shared library; on Darwin also with `_DARWIN_C_SOURCE`, without which
  `_XOPEN_SOURCE` hides the BSD types the system headers use.

## zstd (`vendor/zstd`, facebook/zstd 1.5.7)

`vendor/zstd/lib` holds upstream's `lib/common`, `lib/compress`, `lib/decompress`, the v0.5–v0.7 legacy decoders and the
public headers, copied unmodified from the 1.5.7 release (`LICENSE` and `COPYING` beside them); the dictionary builder,
deprecated API and older legacy formats are left out, as the package build that preceded vendoring left them out. The
library is built by `zstdLibrary` in the top-level `build.zig` with the same sources and macros
(`ZSTD_LEGACY_SUPPORT=5`, `ZSTD_MULTITHREAD=1` except on WASI, `ZSTD_DISABLE_ASM` off x86-64). Local changes are kept
as patch files in `vendor/zstd/patches/`, each applied to the tree and listed here.

- `0001-huf-compress4x-avx512.patch` (`lib/compress/huf_compress.c`): `HUF_compress4X_usingCTable_internal` first
  offers the block to `ZPQ_HUF_compress4X_avx512` (`vendor/zstd/zpq/huf4x_avx512.c`, zpq's own code, MIT), an AVX-512
  encoder of the four Huffman streams that writes the same bytes zstd does, and returns its result unless it declines
  with `(size_t)-1`. It declines when the CPU or OS lacks AVX-512 F, BW, VL, VBMI or VBMI2 (checked once, with CPUID
  and XGETBV), when `ZPQ_ZSTD_SCALAR` is set to anything but `0`, and for tables over 11 bits; off x86-64 the file is
  a stub that always declines. Guarded by `ZPQ_HUF4X_AVX512`, which `zstdLibrary` defines: without it the file is
  upstream's. `vendor/zstd/zpq/huf4x_check.c` compares the two encoders for the unit tests, and
  `tools/zstd-avx512` over any corpus.

To check the tree against the release: unpack zstd 1.5.7, apply the patches with `patch -p1`, and compare
`vendor/zstd/lib` with its `lib` (only the files listed above are vendored).

## libdeflate (`vendor/libdeflate`, ebiggers/libdeflate 1.26)

`COPYING`, `libdeflate.h`, `common_defs.h` and the whole of `lib/`, copied from the v1.26 tag. zpq builds
only the decompression sources (`libdeflateLibrary` in `build.zig`: `deflate_decompress.c`, `gzip_decompress.c`,
`crc32.c`, `utils.c` and the x86 and arm `cpu_features.c`, each of which compiles to nothing on other architectures),
to read GZIP pages; the compressor stays vendored for a later GZIP writer. libdeflate picks its CPU-specific paths at
run time (BMI2, AVX2/AVX-512, PCLMUL/VPCLMUL on x86; PMULL/CRC32 on arm) and falls back to portable code.
`ZPQ_GZIP_STD` set to anything but `0` reads GZIP pages with Zig's `std.compress.flate` instead, to compare
the two or to fall back. `tools/gzip-inflate` compares libdeflate with zlib over any corpus, its portable paths
included.

Local changes are kept as patch files in `vendor/libdeflate/patches/`, each applied to the tree and listed here.

- `0001-export-sym-overridable.patch` (`lib/lib_common.h`): a `LIBDEFLATE_EXPORT_SYM` the build defines wins over
  the default-visibility attribute. `libdeflateLibrary` defines it empty for libzpq's archives, which are compiled
  with hidden visibility so the shared library exports its own API alone; without it the file is upstream's.

To check the tree against the release: clone `ebiggers/libdeflate` at `v1.26`, apply the patches with `patch -p1`,
and compare its `lib`, `libdeflate.h`, `common_defs.h` and `COPYING` with `vendor/libdeflate`.

## lz4 (`vendor/lz4`, lz4/lz4 1.10.0)

`lib/lz4.c`, `lib/lz4.h` and `lib/LICENSE` from the v1.10.0 tag, unmodified. zpq builds `lz4.c` (`lz4Library` in
`build.zig`) and calls `LZ4_compress_default` to write LZ4_RAW pages: the format's reference block encoder,
deterministic for a given input. Reading stays with zpq's own decoder (`src/core/parquet/lz4.zig`), which also parses
the Hadoop and frame containers the deprecated LZ4 codec appears in. No local changes.

To check the tree against the release: clone `lz4/lz4` at `v1.10.0` and compare its `lib/lz4.c`, `lib/lz4.h` and
`lib/LICENSE` with `vendor/lz4`.

