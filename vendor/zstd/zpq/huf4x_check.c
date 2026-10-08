/* zpq: compares the AVX-512 4-stream Huffman encoder with zstd's own over generated literal distributions, for the
 * unit tests (linked only into binaries that call it). tools/zstd-avx512 runs the same comparison over a large corpus. */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "../lib/common/huf.h"
#include "../lib/compress/hist.h"
#include "huf4x_avx512.h"

static uint64_t nextRandom(uint64_t* s) {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    return *s;
}

/* One of seven shapes of literal block: uniform over k symbols, geometric, Zipf-like, two symbols skewed, sparse high
   values, runs, near-random. */
static void fill(uint8_t* buf, size_t n, uint64_t* rng) {
    int const kind = (int)(nextRandom(rng) % 7);
    unsigned const k = 2 + (unsigned)(nextRandom(rng) % 255), base = (unsigned)(nextRandom(rng) % 256);
    unsigned const p = 50 + (unsigned)(nextRandom(rng) % 900);
    for (size_t i = 0; i < n; i++) {
        unsigned v;
        switch (kind) {
        case 0: v = (unsigned)(nextRandom(rng) % k); break;
        case 1: v = 0; while ((nextRandom(rng) % 1000) < p && v < 255) v++; break;
        case 2: v = (unsigned)(1000000 / (nextRandom(rng) % 1000000 + 1)) % 256; break;
        case 3: v = (nextRandom(rng) % 100) < 97 ? base : (base + 1) % 256; break;
        case 4: v = (unsigned)((nextRandom(rng) % k) * 251 % 256); break;
        case 5: v = i && (nextRandom(rng) % 8) ? buf[i - 1] : (unsigned)(nextRandom(rng) % 256); break;
        default: v = (unsigned)(nextRandom(rng) % 256); break;
        }
        buf[i] = (uint8_t)v;
    }
}

/* Encodes `iters` generated blocks (12 bytes to 128 KiB) with both encoders into three destination sizes each: room
 * for any output, as much as the input, and about half of it, so zstd's "does not fit" returns are compared too.
 * Returns the blocks whose outputs differ, or -1 when the AVX-512 encoder is not available here; `checked` counts the
 * encodings compared. */
long ZPQ_huf4x_check(unsigned long iters, unsigned long long seed, unsigned long* checked) {
    *checked = 0;
    if (!ZPQ_huf4x_select(0)) return -1;
    size_t const maxN = 128 << 10;
    uint8_t* const src = malloc(maxN);
    uint8_t* const a = malloc(HUF_compressBound(maxN) + 64);
    uint8_t* const b = malloc(HUF_compressBound(maxN) + 64);
    size_t* const ct = malloc(sizeof(size_t) * HUF_CTABLE_SIZE_ST(255));
    uint64_t* const wksp = malloc(sizeof(uint64_t) * HUF_WORKSPACE_SIZE_U64);
    long mismatches = 0;
    uint64_t rng = seed | 1;
    for (unsigned long it = 0; it < iters && src && a && b && ct && wksp; it++) {
        size_t const n = 12 + (size_t)(nextRandom(&rng) % (it % 7 == 0 ? maxN - 12 : 4096));
        fill(src, n, &rng);
        unsigned count[256];
        unsigned maxSym = 255;
        size_t const largest = HIST_count(count, &maxSym, src, n);
        if (HIST_isError(largest) || largest == n || largest <= (n >> 7) + 4) continue; /* zstd stores these */
        unsigned const tableLog =
            HUF_optimalTableLog(HUF_TABLELOG_DEFAULT, n, maxSym, wksp, sizeof(uint64_t) * HUF_WORKSPACE_SIZE_U64,
                                (HUF_CElt*)ct, count, 0);
        size_t const maxBits = HUF_buildCTable_wksp((HUF_CElt*)ct, count, maxSym, tableLog, wksp,
                                                    sizeof(uint64_t) * HUF_WORKSPACE_SIZE_U64);
        if (HUF_isError(maxBits)) continue;
        size_t const caps[3] = {HUF_compressBound(n), n, n / 2 + 16};
        for (int k = 0; k < 3; k++) {
            memset(a, 0xAA, caps[k]);
            memset(b, 0x55, caps[k]);
            ZPQ_huf4x_select(1);
            size_t const ra = HUF_compress4X_usingCTable(a, caps[k], src, n, (const HUF_CElt*)ct, 0);
            ZPQ_huf4x_select(0);
            size_t const rb = HUF_compress4X_usingCTable(b, caps[k], src, n, (const HUF_CElt*)ct, 0);
            (*checked)++;
            if (ra != rb || (!HUF_isError(ra) && memcmp(a, b, ra) != 0)) mismatches++;
        }
    }
    ZPQ_huf4x_select(0);
    free(src);
    free(a);
    free(b);
    free(ct);
    free(wksp);
    return mismatches;
}
