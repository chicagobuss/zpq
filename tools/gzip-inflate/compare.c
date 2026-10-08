/* Inflates gzip page bodies with zpq's vendored libdeflate and with the system zlib (the reference), requires the same
 * bytes, and times both. See README.md.
 *
 *   compare REPS page.gz...   exit 0 when every page agrees; 1 on a mismatch; 3 on a file that cannot be read or that
 *                             zlib itself rejects; 4 when nothing was compared */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <x86intrin.h>
#include <zlib.h>
#include "libdeflate.h"

static unsigned char* readFile(const char* p, size_t* n) {
    FILE* f = fopen(p, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    *n = (size_t)ftell(f);
    fseek(f, 0, SEEK_SET);
    unsigned char* b = malloc(*n ? *n : 1);
    if (fread(b, 1, *n, f) != *n) { free(b); b = NULL; }
    fclose(f);
    return b;
}

static int cmp64(const void* a, const void* b) {
    unsigned long long x = *(const unsigned long long*)a, y = *(const unsigned long long*)b;
    return x < y ? -1 : x > y;
}

/* zlib over every member (windowBits 31: gzip only, as Parquet's GZIP codec). */
static long zlibInflate(const unsigned char* s, size_t n, unsigned char* d, size_t cap) {
    size_t pos = 0, out = 0;
    while (pos < n) {
        z_stream z;
        memset(&z, 0, sizeof z);
        if (inflateInit2(&z, 31) != Z_OK) return -1;
        z.next_in = (unsigned char*)s + pos;
        z.avail_in = (uInt)(n - pos);
        z.next_out = d + out;
        z.avail_out = (uInt)(cap - out);
        int r = inflate(&z, Z_FINISH);
        size_t used = z.total_in, got = z.total_out;
        inflateEnd(&z);
        if (r != Z_STREAM_END) return -1;
        pos += used;
        out += got;
    }
    return (long)out;
}

static struct libdeflate_decompressor* D;

/* As zpq calls it: every member of the input, back to back, filling a buffer of the known size exactly. */
static long ldInflate(const unsigned char* s, size_t n, unsigned char* d, size_t want) {
    size_t pos = 0, out = 0;
    while (pos < n) {
        size_t used = 0, got = 0;
        if (libdeflate_gzip_decompress_ex(D, s + pos, n - pos, d + out, want - out, &used, &got) != LIBDEFLATE_SUCCESS)
            return -1;
        pos += used;
        out += got;
    }
    return out == want ? (long)out : -1;
}

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "usage: compare REPS page.gz...\n"); return 2; }
    int reps = atoi(argv[1]);
    D = libdeflate_alloc_decompressor();
    unsigned long long* ts = malloc(sizeof *ts * (size_t)reps);
    double tz = 0, tl = 0;
    size_t outb = 0;
    long checked = 0, mismatches = 0;
    for (int f = 2; f < argc; f++) {
        size_t n;
        unsigned char* s = readFile(argv[f], &n);
        if (!s) { fprintf(stderr, "cannot read %s\n", argv[f]); return 3; }
        size_t cap = 4 * n + (1u << 20);
        unsigned char *ref = malloc(cap), *d = malloc(cap);
        long rn;
        while ((rn = zlibInflate(s, n, ref, cap)) < 0 && cap < (1ul << 32)) {
            cap *= 4;
            ref = realloc(ref, cap);
            d = realloc(d, cap);
        }
        if (rn < 0) { fprintf(stderr, "zlib rejects %s\n", argv[f]); return 3; }
        long got = ldInflate(s, n, d, (size_t)rn);
        checked++;
        if (got != rn || memcmp(d, ref, (size_t)rn) != 0) {
            if (mismatches++ < 5) fprintf(stderr, "MISMATCH %s\n", argv[f]);
        }
        for (int k = 0; k < 2; k++) {
            for (int r = 0; r < reps; r++) {
                unsigned long long a = __rdtsc();
                if (k == 0) zlibInflate(s, n, d, cap); else ldInflate(s, n, d, (size_t)rn);
                ts[r] = __rdtsc() - a;
            }
            qsort(ts, (size_t)reps, sizeof *ts, cmp64);
            *(k == 0 ? &tz : &tl) += (double)ts[reps / 2];
        }
        outb += (size_t)rn;
        free(s); free(ref); free(d);
    }
    if (checked == 0 || outb == 0) { fprintf(stderr, "nothing compared\n"); return 4; }
    printf("{\"pages\":%ld,\"mismatches\":%ld,\"out_bytes\":%zu,\"zlib_ticks_per_byte\":%.3f,\"libdeflate_ticks_per_byte\":%.3f}\n",
           checked, mismatches, outb, tz / (double)outb, tl / (double)outb);
    return mismatches != 0;
}
