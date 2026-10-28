/* zpq: zstd's 4-stream Huffman encoder in AVX-512, writing the same bytes (see huf4x_avx512.c). */
#ifndef ZPQ_HUF4X_AVX512_H
#define ZPQ_HUF4X_AVX512_H

#include <stddef.h>

/* What HUF_compress4X_usingCTable writes for `src` with `CTable` (zstd's HUF_CElt array), byte for byte and with the
 * same return value; or (size_t)-1 when this encoder does not apply (no AVX-512 F/BW/VL/VBMI/VBMI2 on the CPU or in
 * the OS, ZPQ_ZSTD_SCALAR set, or a tableLog over 11), and the caller encodes the block itself. */
size_t ZPQ_HUF_compress4X_avx512(void* dst, size_t dstSize, const void* src, size_t srcSize, const size_t* CTable);

/* For tests: 1 forces zstd's own encoder, 0 restores the automatic choice (AVX-512 when the CPU and OS support it and
 * ZPQ_ZSTD_SCALAR is unset or "0"). Returns whether the AVX-512 encoder is now used. */
int ZPQ_huf4x_select(int mode);

#endif
