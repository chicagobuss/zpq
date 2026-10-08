/* Compares zpq's AVX-512 4-stream Huffman encoder (vendor/zstd/zpq) with zstd's own, byte for byte, through the patched
 * HUF_compress4X_usingCTable, and times both. See README.md.
 *
 *   compare fuzz ITERS SEED      generated literal blocks (the unit test's generator, at any scale)
 *   compare files REPS FILE...   every 128 KiB block of the files, compared, then timed (median of REPS, TSC ticks)
 *   compare only avx512|zstd REPS FILE   one encoder over the file's blocks, for `perf stat` */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <x86intrin.h>
#include "huf.h"
#include "hist.h"
#include "huf4x_avx512.h"

long ZPQ_huf4x_check(unsigned long iters, unsigned long long seed, unsigned long* checked);

typedef struct { size_t ct[HUF_CTABLE_SIZE_ST(255)]; int ok; } Table;

static Table makeTable(const uint8_t* src, size_t n) {
    Table t;
    t.ok = 0;
    unsigned count[256], maxSym = 255;
    size_t const largest = HIST_count(count, &maxSym, src, n);
    if (HIST_isError(largest) || largest == n || largest <= (n >> 7) + 4) return t; /* zstd stores these */
    static uint64_t wksp[HUF_WORKSPACE_SIZE_U64];
    unsigned const log =
        HUF_optimalTableLog(HUF_TABLELOG_DEFAULT, n, maxSym, wksp, sizeof wksp, (HUF_CElt*)t.ct, count, 0);
    if (HUF_isError(HUF_buildCTable_wksp((HUF_CElt*)t.ct, count, maxSym, log, wksp, sizeof wksp))) return t;
    t.ok = 1;
    return t;
}

static size_t encode(int avx, void* dst, size_t cap, const uint8_t* src, size_t n, const Table* t) {
    ZPQ_huf4x_select(avx ? 0 : 1);
    return HUF_compress4X_usingCTable(dst, cap, src, n, (const HUF_CElt*)t->ct, 0);
}

static uint8_t* readFile(const char* path, size_t* len) {
    FILE* fp = fopen(path, "rb");
    if (!fp) return NULL;
    fseek(fp, 0, SEEK_END);
    *len = (size_t)ftell(fp);
    fseek(fp, 0, SEEK_SET);
    uint8_t* src = malloc(*len ? *len : 1);
    if (fread(src, 1, *len, fp) != *len) { free(src); src = NULL; }
    fclose(fp);
    return src;
}

static int cmp64(const void* x, const void* y) {
    uint64_t const a = *(const uint64_t*)x, b = *(const uint64_t*)y;
    return a < b ? -1 : a > b;
}

#define BLOCK 131072

int main(int argc, char** argv) {
    if (!ZPQ_huf4x_select(0)) { fprintf(stderr, "no AVX-512 VBMI2 here (or ZPQ_ZSTD_SCALAR is set)\n"); return 2; }
    static uint8_t a[2 * BLOCK], b[2 * BLOCK];
    if (argc >= 4 && strcmp(argv[1], "fuzz") == 0) {
        unsigned long checked = 0;
        long const bad = ZPQ_huf4x_check(strtoul(argv[2], 0, 10), strtoull(argv[3], 0, 10), &checked);
        printf("{\"checked\":%lu,\"mismatches\":%ld}\n", checked, bad);
        return bad != 0 || checked == 0;
    }
    if (argc >= 4 && strcmp(argv[1], "files") == 0) {
        int const reps = atoi(argv[2]);
        uint64_t* ts = malloc(sizeof(uint64_t) * (size_t)reps);
        uint64_t tz = 0, tv = 0, bytes = 0;
        long checked = 0, mismatches = 0;
        for (int f = 3; f < argc; f++) {
            size_t len;
            uint8_t* src = readFile(argv[f], &len);
            if (!src) { fprintf(stderr, "cannot read %s\n", argv[f]); return 3; }
            for (size_t off = 0; off < len; off += BLOCK) {
                size_t const n = len - off < BLOCK ? len - off : BLOCK;
                Table const t = makeTable(src + off, n);
                if (!t.ok) continue;
                size_t const caps[3] = {HUF_compressBound(n), n, n / 2 + 16};
                for (int k = 0; k < 3; k++) {
                    size_t const rz = encode(0, a, caps[k], src + off, n, &t);
                    size_t const rv = encode(1, b, caps[k], src + off, n, &t);
                    checked++;
                    if (rz != rv || (!HUF_isError(rz) && memcmp(a, b, rz) != 0)) {
                        if (mismatches++ < 5) fprintf(stderr, "MISMATCH %s @%zu cap=%zu\n", argv[f], off, caps[k]);
                    }
                }
                for (int avx = 0; avx < 2; avx++) {
                    ZPQ_huf4x_select(avx ? 0 : 1);
                    for (int r = 0; r < reps; r++) {
                        uint64_t const s = __rdtsc();
                        HUF_compress4X_usingCTable(avx ? b : a, caps[0], src + off, n, (const HUF_CElt*)t.ct, 0);
                        ts[r] = __rdtsc() - s;
                    }
                    qsort(ts, (size_t)reps, sizeof *ts, cmp64);
                    *(avx ? &tv : &tz) += ts[reps / 2];
                }
                bytes += n;
            }
            free(src);
        }
        if (checked == 0) { fprintf(stderr, "no block of the files was Huffman-coded: nothing compared\n"); return 4; }
        printf("{\"checked\":%ld,\"mismatches\":%ld,\"bytes\":%llu,\"zstd_ticks_per_byte\":%.3f,"
               "\"avx512_ticks_per_byte\":%.3f}\n",
               checked, mismatches, (unsigned long long)bytes, (double)tz / (double)bytes, (double)tv / (double)bytes);
        return mismatches != 0;
    }
    if (argc >= 5 && strcmp(argv[1], "only") == 0) {
        int const avx = strcmp(argv[2], "avx512") == 0, reps = atoi(argv[3]);
        size_t len;
        uint8_t* src = readFile(argv[4], &len);
        if (!src) return 3;
        size_t const nb = (len + BLOCK - 1) / BLOCK;
        Table* tabs = malloc(sizeof(Table) * (nb ? nb : 1));
        for (size_t k = 0; k < nb; k++) {
            size_t const off = k * BLOCK;
            tabs[k] = makeTable(src + off, len - off < BLOCK ? len - off : BLOCK);
        }
        ZPQ_huf4x_select(avx ? 0 : 1);
        size_t sink = 0;
        for (int r = 0; r < reps; r++)
            for (size_t k = 0; k < nb; k++) {
                if (!tabs[k].ok) continue;
                size_t const off = k * BLOCK, n = len - off < BLOCK ? len - off : BLOCK;
                sink += HUF_compress4X_usingCTable(a, HUF_compressBound(n), src + off, n,
                                                   (const HUF_CElt*)tabs[k].ct, 0);
            }
        printf("%zu\n", sink);
        return 0;
    }
    fprintf(stderr, "usage: compare fuzz ITERS SEED | files REPS FILE... | only avx512|zstd REPS FILE\n");
    return 2;
}
