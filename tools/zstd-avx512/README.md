# zstd AVX-512 Huffman encoder: corpus comparison

`vendor/zstd/zpq/huf4x_avx512.c` writes the same bytes as zstd's `HUF_compress4X_usingCTable`, with the same return
value, including the cases where zstd declines a block. The unit tests compare the two over a few thousand generated
literal blocks and a set of whole zstd frames. `compare` does the same at any scale, and over real data:

```
tools/zstd-avx512/build.sh /tmp/compare          # zig cc, -march=native, against vendor/zstd
/tmp/compare fuzz 700000 1                       # generated blocks: uniform, geometric, Zipf-like, skewed, sparse, runs, random
/tmp/compare files 5 pages/*                     # every 128 KiB block of the files, three destination sizes each, then timed
/tmp/compare only avx512 50 pages/x              # one encoder only, under `perf stat`
```

Each block is encoded into three destination sizes (room for anything, the input's size, about half of it), so zstd's
"does not fit" returns are compared too. Uncompressed Parquet page payloads are a good corpus: Huffman-coded literals
are what zstd makes of them. Any mismatch exits non-zero and names the file and offset; so does a file that cannot be read, or a run that compared nothing.

The comparison needs a CPU with AVX-512 F, BW, VL, VBMI and VBMI2; elsewhere `compare` says so and exits 2.
`ZPQ_ZSTD_SCALAR=1` turns the AVX-512 encoder off for a whole process (zpq included), for comparisons like this one.
