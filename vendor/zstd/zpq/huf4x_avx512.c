/* zpq: zstd's 4-stream Huffman encoder (HUF_compress4X_usingCTable) in AVX-512, writing the same bytes.
 *
 * zstd's stream format: each stream is the LSB-first concatenation of the symbols' codes, taken from the last symbol
 * of its segment to the first, then a single 1 bit (the end mark), written little-endian in ceil(bits / 8) bytes. 4X
 * splits the input into three segments of (n + 3) / 4 bytes and the rest, behind a 6-byte jump table holding the first
 * three streams' sizes.
 *
 * Per 64 symbols, in emission order (reversed with VPERMB): code and length come from 256-entry byte tables with two
 * VPERMI2B and a blend each; adjacent codes merge into pairs and pairs into quads with variable shifts; three
 * log-depth merges (VPSHLDVQ funnel shifts) then concatenate eight quads into one blob of at most 352 bits, so a
 * 64-symbol chunk is two blobs. A stream appends each blob with one shift, a masked store of its complete words and
 * the partial word kept in a register. The four streams are encoded interleaved, as independent dependency chains.
 *
 * Every stream's exact length in bits is computed first (a length lookup and a byte sum per 64 symbols), which places
 * the streams before any is written, reproduces zstd's per-stream "does not fit" rule exactly, and bounds every store
 * to the stream's own bytes.
 *
 * Needs AVX-512 F, BW, VL, VBMI and VBMI2, checked once at run time (CPUID, and XGETBV for the OS saving the ZMM and
 * opmask state); elsewhere, and on every other architecture, zstd's own encoder runs. */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "huf4x_avx512.h"

#if defined(__x86_64__) && (defined(__clang__) || defined(__GNUC__))

#include <cpuid.h>
#include <immintrin.h>

static int cpuHasAvx512Vbmi2(void) {
    unsigned a, b, c, d;
    if (!__get_cpuid(1, &a, &b, &c, &d) || !(c & (1u << 27))) return 0; /* OSXSAVE */
    unsigned lo, hi;
    __asm__ volatile("xgetbv" : "=a"(lo), "=d"(hi) : "c"(0));
    (void)hi;
    if ((lo & 0xE6) != 0xE6) return 0; /* the OS saves SSE, AVX, opmask, ZMM_Hi256 and Hi16_ZMM state */
    if (__get_cpuid_max(0, NULL) < 7) return 0;
    __cpuid_count(7, 0, a, b, c, d);
    const unsigned f_bw_vl = (1u << 16) | (1u << 30) | (1u << 31);
    const unsigned vbmi_vbmi2 = (1u << 1) | (1u << 6);
    return (b & f_bw_vl) == f_bw_vl && (c & vbmi_vbmi2) == vbmi_vbmi2;
}

/* 0 not yet decided, 1 AVX-512, 2 zstd's encoder. Racing first calls decide the same thing. */
static int g_huf4x_state;

static int huf4xActive(void) {
    int s = __atomic_load_n(&g_huf4x_state, __ATOMIC_RELAXED);
    if (s == 0) {
        const char* e = getenv("ZPQ_ZSTD_SCALAR");
        int const scalar = e != NULL && e[0] != '\0' && strcmp(e, "0") != 0;
        s = !scalar && cpuHasAvx512Vbmi2() ? 1 : 2;
        __atomic_store_n(&g_huf4x_state, s, __ATOMIC_RELAXED);
    }
    return s == 1;
}

int ZPQ_huf4x_select(int mode) {
    __atomic_store_n(&g_huf4x_state, mode == 1 ? 2 : 0, __ATOMIC_RELAXED);
    return mode == 1 ? 0 : huf4xActive();
}

#if defined(__clang__)
#pragma clang attribute push(__attribute__((target("avx512f,avx512bw,avx512vl,avx512vbmi,avx512vbmi2"))), \
                             apply_to = function)
#else
#pragma GCC push_options
#pragma GCC target("avx512f,avx512bw,avx512vl,avx512vbmi,avx512vbmi2")
#endif

typedef struct {
    __m512i len[4], clo[4], chi[4]; /* 64-entry slices of the 256 symbols: code length, code low and high bytes */
    uint8_t len8[256];
    uint16_t code16[256];
    unsigned tableLog;
} Tab;

static void buildTab(Tab* t, const size_t* CTable) {
    /* CTable[0] is the header (tableLog, then maxSymbolValue, in its first bytes); CTable[1 + s] holds the code length
       in its low byte and the code in its top `length` bits. */
    uint8_t hdr[sizeof(size_t)];
    memcpy(hdr, CTable, sizeof hdr);
    t->tableLog = hdr[0];
    unsigned const maxSym = hdr[1];
    uint8_t lo[256], hi[256];
    for (unsigned s = 0; s < 256; s++) {
        size_t const elt = s <= maxSym ? CTable[1 + s] : 0;
        unsigned const nb = (unsigned)(elt & 0xFF);
        uint16_t const code = nb ? (uint16_t)(elt >> (64 - nb)) : 0;
        t->len8[s] = (uint8_t)nb;
        t->code16[s] = code;
        lo[s] = (uint8_t)code;
        hi[s] = (uint8_t)(code >> 8);
    }
    for (int k = 0; k < 4; k++) {
        t->len[k] = _mm512_loadu_si512(t->len8 + 64 * k);
        t->clo[k] = _mm512_loadu_si512(lo + 64 * k);
        t->chi[k] = _mm512_loadu_si512(hi + 64 * k);
    }
}

static inline __m512i lookup256(const __m512i* tab, __m512i idx) {
    __m512i const a = _mm512_permutex2var_epi8(tab[0], idx, tab[1]);
    __m512i const b = _mm512_permutex2var_epi8(tab[2], idx, tab[3]);
    return _mm512_mask_blend_epi8(_mm512_movepi8_mask(idx), a, b);
}

/* The whole register shifted left by s < 64 bits, word 0 least significant. */
static inline __m512i shlSmall(__m512i v, __m512i s) {
    return _mm512_shldv_epi64(v, _mm512_alignr_epi64(v, _mm512_setzero_si512(), 7), s);
}

/* Each 64-bit unit x (even lane) and y (odd lane) to the 128-bit unit x | y << len(x); lengths in `len`, the merged
   one in the even lane. The shift is under 64 bits, so in-lane unpacks do the work. */
static inline void mergePairs1(__m512i* v, __m512i* len) {
    __m512i const zero = _mm512_setzero_si512();
    __m512i const y = _mm512_unpackhi_epi64(*v, zero);
    __m512i const lx = _mm512_unpacklo_epi64(*len, *len);
    __m512i const ly = _mm512_unpackhi_epi64(*len, *len);
    __m512i const lo = _mm512_or_si512(_mm512_maskz_mov_epi64(0x55, *v), _mm512_sllv_epi64(y, lx));
    __m512i const hi = _mm512_srlv_epi64(y, _mm512_sub_epi64(_mm512_set1_epi64(64), lx)); /* a count of 64 gives 0 */
    *v = _mm512_mask_blend_epi64(0xAA, lo, _mm512_unpacklo_epi64(zero, hi));
    *len = _mm512_add_epi64(lx, ly);
}

/* 128-bit units into 256-bit ones, one per 256-bit half: len(x) < 128, so the shift is s or 64 + s bits; both
   candidates come from in-half lane shuffles and are blended per lane. */
static inline void mergePairs2(__m512i* v, __m512i* len) {
    __m512i const y = _mm512_maskz_permutex_epi64(0x33, *v, _MM_SHUFFLE(3, 2, 3, 2)); /* lanes 0,1 <- 2,3 */
    __m512i const x = _mm512_maskz_mov_epi64(0x33, *v);
    __m512i const lx = _mm512_permutex_epi64(*len, _MM_SHUFFLE(0, 0, 0, 0));
    __m512i const ly = _mm512_permutex_epi64(*len, _MM_SHUFFLE(2, 2, 2, 2));
    __m512i const s = _mm512_and_si512(lx, _mm512_set1_epi64(63));
    __m512i const y1 = _mm512_maskz_permutex_epi64(0xEE, y, _MM_SHUFFLE(2, 1, 0, 0)); /* up one lane */
    __m512i const y2 = _mm512_maskz_permutex_epi64(0xCC, y, _MM_SHUFFLE(1, 0, 0, 0)); /* up two lanes */
    __m512i const c0 = _mm512_shldv_epi64(y, y1, s);
    __m512i const c1 = _mm512_shldv_epi64(y1, y2, s);
    __mmask8 const q1 = _mm512_cmpge_epu64_mask(lx, _mm512_set1_epi64(64));
    *v = _mm512_or_si512(x, _mm512_mask_blend_epi64(q1, c0, c1));
    *len = _mm512_add_epi64(lx, ly);
}

/* Two 256-bit units into one 512-bit unit: len(x) < 256 is one scalar for the register, so the word shift (0 to 2)
   selects among three VALIGNQ candidates, without a branch. */
static inline void mergePairs4(__m512i* v, __m512i* len) {
    __m512i const zero = _mm512_setzero_si512();
    __m512i const y = _mm512_maskz_shuffle_i64x2(0x0F, *v, *v, _MM_SHUFFLE(3, 2, 3, 2)); /* lanes 0-3 <- 4-7 */
    __m512i const x = _mm512_maskz_mov_epi64(0x0F, *v);
    uint64_t const lx = (uint64_t)_mm_cvtsi128_si64(_mm512_castsi512_si128(*len));
    uint64_t const ly = (uint64_t)_mm_cvtsi128_si64(_mm512_extracti32x4_epi32(*len, 2));
    __m512i const s = _mm512_set1_epi64((long long)(lx & 63));
    __m512i const y1 = _mm512_alignr_epi64(y, zero, 7), y2 = _mm512_alignr_epi64(y, zero, 6);
    __m512i const y3 = _mm512_alignr_epi64(y, zero, 5);
    __mmask8 const k1 = (__mmask8)-(int)((lx >> 6) == 1), k2 = (__mmask8)-(int)((lx >> 6) >= 2);
    __m512i const hi = _mm512_mask_blend_epi64(k2, _mm512_mask_blend_epi64(k1, y, y1), y2);
    __m512i const lo = _mm512_mask_blend_epi64(k2, _mm512_mask_blend_epi64(k1, y1, y2), y3);
    *v = _mm512_or_si512(x, _mm512_shldv_epi64(hi, lo, s));
    *len = _mm512_set1_epi64((long long)(lx + ly));
}

/* The stream's exact length in bits (codes plus the end mark). */
static size_t streamBits(const uint8_t* src, size_t n, const Tab* t) {
    __m512i acc = _mm512_setzero_si512();
    size_t i = 0;
    for (; i + 64 <= n; i += 64) {
        __m512i const l = lookup256(t->len, _mm512_loadu_si512(src + i));
        acc = _mm512_add_epi64(acc, _mm512_sad_epu8(l, _mm512_setzero_si512()));
    }
    size_t b = (size_t)_mm512_reduce_add_epi64(acc);
    for (; i < n; i++) b += t->len8[src[i]];
    return b + 1;
}

/* One code onto a stream writer (acc, bits < 64, out), branch-free: a masked 8-byte store writes the word only once
   it is full, so a stream never writes past its own end and the four write side by side. */
#define PUT(acc, bits, out, v, l)                                                                                    \
    do {                                                                                                             \
        uint64_t const v_ = (v);                                                                                     \
        uint64_t const a_ = acc | (v_ << bits);                                                                      \
        unsigned const nb_ = bits + (unsigned)(l);                                                                   \
        unsigned const adv_ = nb_ >> 6;                                                                              \
        _mm_mask_storeu_epi64(out, (__mmask8)adv_, _mm_cvtsi64_si128((long long)a_));                                \
        uint64_t const c_ = (v_ >> 1) >> (63 - bits);                                                                \
        out += adv_ * 8;                                                                                             \
        acc = adv_ ? c_ : a_;                                                                                        \
        bits = nb_ & 63;                                                                                             \
    } while (0)

/* Append a blob of `len` bits at bit offset `bits` (< 64), the pending bits in lane 0 of `accv`: shift the blob up,
   OR in the pending word, store the complete words (masked) and keep the partial one in a register. */
#define APPEND(accv, bits, out, blob, len)                                                                           \
    do {                                                                                                             \
        __m512i const sh_ = _mm512_or_si512(shlSmall(blob, _mm512_set1_epi64(bits)), accv);                          \
        unsigned const tot_ = bits + (unsigned)(len);                                                                \
        unsigned const nw_ = tot_ >> 6;                                                                              \
        _mm512_mask_storeu_epi64(out, (__mmask8)((1u << nw_) - 1), sh_);                                             \
        out += nw_ * 8;                                                                                              \
        accv = _mm512_maskz_permutexvar_epi64(1, _mm512_set1_epi64(nw_), sh_);                                       \
        bits = tot_ & 63;                                                                                            \
    } while (0)

static inline void finish(uint8_t* out, uint64_t acc, unsigned bits) {
    unsigned const n = (bits + 7) / 8;
    _mm_mask_storeu_epi8(out, (__mmask16)((1u << n) - 1), _mm_cvtsi64_si128((long long)acc));
}

/* The 64 symbols at p, in emission order, as two blobs (symbols 0-31 and 32-63) and their bit lengths. */
static inline __attribute__((always_inline)) void chunkBlobs(const uint8_t* p, const Tab* t, __m512i* b0,
                                                             uint64_t* l0, __m512i* b1, uint64_t* l1) {
    __m512i const rev = _mm512_set_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
                                        22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
                                        41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
                                        60, 61, 62, 63);
    __m512i const zero = _mm512_setzero_si512();
    __m512i const m16 = _mm512_set1_epi32(0xFFFF);
    __m512i const m32 = _mm512_set1_epi64(0xFFFFFFFF);
    __m512i const s = _mm512_permutexvar_epi8(rev, _mm512_loadu_si512(p));
    __m512i const L = lookup256(t->len, s), CL = lookup256(t->clo, s), CH = lookup256(t->chi, s);
    /* Codes and lengths widen to 16 bits (unpacklo/hi keep runs of 8 consecutive symbols together), then merge into
       pairs in 32-bit lanes and quads in 64-bit lanes. */
    __m512i const ca = _mm512_unpacklo_epi8(CL, CH), cb = _mm512_unpackhi_epi8(CL, CH);
    __m512i const la = _mm512_unpacklo_epi8(L, zero), lb = _mm512_unpackhi_epi8(L, zero);
    __m512i const lal = _mm512_and_si512(la, m16), lbl = _mm512_and_si512(lb, m16);
    __m512i const pa = _mm512_or_si512(_mm512_and_si512(ca, m16), _mm512_sllv_epi32(_mm512_srli_epi32(ca, 16), lal));
    __m512i const pb = _mm512_or_si512(_mm512_and_si512(cb, m16), _mm512_sllv_epi32(_mm512_srli_epi32(cb, 16), lbl));
    __m512i const pla = _mm512_add_epi32(lal, _mm512_srli_epi32(la, 16));
    __m512i const plb = _mm512_add_epi32(lbl, _mm512_srli_epi32(lb, 16));
    __m512i const qla = _mm512_and_si512(pla, m32), qlb = _mm512_and_si512(plb, m32);
    __m512i const qa = _mm512_or_si512(_mm512_and_si512(pa, m32), _mm512_sllv_epi64(_mm512_srli_epi64(pa, 32), qla));
    __m512i const qb = _mm512_or_si512(_mm512_and_si512(pb, m32), _mm512_sllv_epi64(_mm512_srli_epi64(pb, 32), qlb));
    __m512i const Qla = _mm512_add_epi64(qla, _mm512_srli_epi64(pla, 32));
    __m512i const Qlb = _mm512_add_epi64(qlb, _mm512_srli_epi64(plb, 32));
    /* unpacklo holds symbols 16k+0..7 of each 128-bit lane, unpackhi 16k+8..15: quads in emission order are
       A[2k], A[2k+1], B[2k], B[2k+1]. */
    __m512i const order = _mm512_set_epi64(11, 10, 3, 2, 9, 8, 1, 0);
    __m512i const order2 = _mm512_set_epi64(15, 14, 7, 6, 13, 12, 5, 4);
    __m512i v0 = _mm512_permutex2var_epi64(qa, order, qb), n0 = _mm512_permutex2var_epi64(Qla, order, Qlb);
    __m512i v1 = _mm512_permutex2var_epi64(qa, order2, qb), n1 = _mm512_permutex2var_epi64(Qla, order2, Qlb);
    mergePairs1(&v0, &n0);
    mergePairs1(&v1, &n1);
    mergePairs2(&v0, &n0);
    mergePairs2(&v1, &n1);
    mergePairs4(&v0, &n0);
    mergePairs4(&v1, &n1);
    *b0 = v0;
    *l0 = (uint64_t)_mm_cvtsi128_si64(_mm512_castsi512_si128(n0));
    *b1 = v1;
    *l1 = (uint64_t)_mm_cvtsi128_si64(_mm512_castsi512_si128(n1));
}

/* The rest of one stream once the interleaved loop is done: whole chunks, then single symbols and the end mark. */
#define TAIL(av, b, o, i, s)                                                                                         \
    do {                                                                                                             \
        while (i >= 64) {                                                                                            \
            __m512i x_, y_;                                                                                          \
            uint64_t lx_, ly_;                                                                                       \
            i -= 64;                                                                                                 \
            chunkBlobs(src[s] + i, t, &x_, &lx_, &y_, &ly_);                                                         \
            APPEND(av, b, o, x_, lx_);                                                                               \
            APPEND(av, b, o, y_, ly_);                                                                               \
        }                                                                                                            \
        uint64_t w_ = (uint64_t)_mm_cvtsi128_si64(_mm512_castsi512_si128(av));                                       \
        while (i > 0) {                                                                                              \
            uint8_t const ch_ = src[s][--i];                                                                         \
            PUT(w_, b, o, t->code16[ch_], t->len8[ch_]);                                                             \
        }                                                                                                            \
        PUT(w_, b, o, 1, 1);                                                                                         \
        finish(o, w_, b);                                                                                            \
    } while (0)

static __attribute__((noinline)) void encode4(uint8_t* const dst[4], const uint8_t* const src[4], const size_t len[4],
                                              const Tab* t) {
    __m512i a0 = _mm512_setzero_si512(), a1 = a0, a2 = a0, a3 = a0;
    unsigned b0 = 0, b1 = 0, b2 = 0, b3 = 0;
    uint8_t *o0 = dst[0], *o1 = dst[1], *o2 = dst[2], *o3 = dst[3];
    size_t i0 = len[0], i1 = len[1], i2 = len[2], i3 = len[3];
    size_t common = len[0] / 64;
    for (int s = 1; s < 4; s++)
        if (len[s] / 64 < common) common = len[s] / 64;
    for (size_t c = 0; c < common; c++) {
        __m512i x0, y0, x1, y1, x2, y2, x3, y3;
        uint64_t lx0, ly0, lx1, ly1, lx2, ly2, lx3, ly3;
        i0 -= 64;
        i1 -= 64;
        i2 -= 64;
        i3 -= 64;
        chunkBlobs(src[0] + i0, t, &x0, &lx0, &y0, &ly0);
        chunkBlobs(src[1] + i1, t, &x1, &lx1, &y1, &ly1);
        chunkBlobs(src[2] + i2, t, &x2, &lx2, &y2, &ly2);
        chunkBlobs(src[3] + i3, t, &x3, &lx3, &y3, &ly3);
        APPEND(a0, b0, o0, x0, lx0);
        APPEND(a1, b1, o1, x1, lx1);
        APPEND(a2, b2, o2, x2, lx2);
        APPEND(a3, b3, o3, x3, lx3);
        APPEND(a0, b0, o0, y0, ly0);
        APPEND(a1, b1, o1, y1, ly1);
        APPEND(a2, b2, o2, y2, ly2);
        APPEND(a3, b3, o3, y3, ly3);
    }
    TAIL(a0, b0, o0, i0, 0);
    TAIL(a1, b1, o1, i1, 1);
    TAIL(a2, b2, o2, i2, 2);
    TAIL(a3, b3, o3, i3, 3);
}

static __attribute__((noinline)) size_t compress4X(void* dst, size_t dstSize, const void* src, size_t srcSize,
                                                   const size_t* CTable) {
    Tab t;
    buildTab(&t, CTable);
    if (t.tableLog > 11) return (size_t)-1; /* codes over 11 bits would overflow a blob */
    /* zstd's rules, in its order: too little room or input, then each stream in turn against the room left. */
    if (dstSize < 6 + 1 + 1 + 1 + 8) return 0;
    if (srcSize < 12) return 0;
    size_t const seg = (srcSize + 3) / 4;
    const uint8_t* const ip = (const uint8_t*)src;
    uint8_t* const ostart = (uint8_t*)dst;
    uint8_t* const oend = ostart + dstSize;
    const uint8_t* sp[4];
    size_t len[4], bytes[4];
    uint8_t* op[4];
    uint8_t* o = ostart + 6;
    for (int s = 0; s < 4; s++) {
        sp[s] = ip + (size_t)s * seg;
        len[s] = s < 3 ? seg : srcSize - 3 * seg;
        size_t const room = (size_t)(oend - o);
        if (room <= 8) return 0;
        size_t const bits = streamBits(sp[s], len[s], &t);
        if (bits / 8 >= room - 8) return 0;
        bytes[s] = (bits + 7) / 8;
        if (bytes[s] > 65535) return 0;
        op[s] = o;
        o += bytes[s];
    }
    encode4(op, sp, len, &t);
    for (int s = 0; s < 3; s++) {
        ostart[2 * s] = (uint8_t)bytes[s];
        ostart[2 * s + 1] = (uint8_t)(bytes[s] >> 8);
    }
    return (size_t)(o - ostart);
}

#undef PUT
#undef APPEND
#undef TAIL

#if defined(__clang__)
#pragma clang attribute pop
#else
#pragma GCC pop_options
#endif

size_t ZPQ_HUF_compress4X_avx512(void* dst, size_t dstSize, const void* src, size_t srcSize, const size_t* CTable) {
    if (!huf4xActive()) return (size_t)-1;
    return compress4X(dst, dstSize, src, srcSize, CTable);
}

#else /* not x86-64: zstd's encoder always */

size_t ZPQ_HUF_compress4X_avx512(void* dst, size_t dstSize, const void* src, size_t srcSize, const size_t* CTable) {
    (void)dst;
    (void)dstSize;
    (void)src;
    (void)srcSize;
    (void)CTable;
    return (size_t)-1;
}

int ZPQ_huf4x_select(int mode) {
    (void)mode;
    return 0;
}

#endif
