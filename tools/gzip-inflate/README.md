# GZIP pages: libdeflate against zlib

zpq reads GZIP pages with the vendored libdeflate (`vendor/libdeflate`). The unit tests compare it with Zig's
`std.compress.flate` over streams the stdlib writes (stored, Huffman-only, levels 1-9, multi-member, sizes around the
block and window limits) and feed both truncated and bit-flipped input. `compare` holds libdeflate to the reference
inflater, the system zlib, over any corpus:

```
tools/gzip-inflate/build.sh /tmp/compare                         # zig cc, against vendor/libdeflate and -lz
python3 -I tools/gzip-inflate/gen_corpus.py /tmp/gz 3000 1         # streams zlib writes: levels 0-9, every strategy, memLevels, multi-member
python3 -I tools/gzip-inflate/extract_pages.py FILE.parquet /tmp/p # or a Parquet file's page bodies (needs pyarrow)
/tmp/compare 3 /tmp/gz/*.gz /tmp/p/*.gz                            # same bytes required; timing per output byte
```

`FALLBACK=1 tools/gzip-inflate/build.sh` also compiles libdeflate's test hook, so that
`LIBDEFLATE_DISABLE_CPU_FEATURES=sse2,pclmulqdq,avx,avx2,bmi2,zmm,avx512bw,avx512vl,vpclmulqdq,avx512_vnni,avx_vnni`
runs its portable code. Exit status: 0 when every stream agrees, 1 on a mismatch, 3 on a file that cannot be read
or that zlib rejects, 4 when nothing was compared.

Follow-up, not in 0.5.0: GZIP *writes* still use the Zig stdlib's deflate. libdeflate's compressor (vendored, not
built) would be several times faster; its output bytes differ from today's, and are deterministic.
