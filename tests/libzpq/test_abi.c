/*
 * libzpq's C ABI, end to end: options, the round trip, writes, error responses, invalid arguments, concurrent calls
 * on one context, open/run/close cycles, and resident memory over many calls. Run from the repo root (fixture paths
 * are relative):
 *
 *   tests/libzpq/run.sh            builds the library and this test, plain and under AddressSanitizer
 *   test_abi [loop_calls [cycles]] loop_calls (default 10000) sizes the RSS check, cycles (default 1000) the
 *                                  open/run/close loop
 */
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "zpq.h"

static int failures = 0;
#define CHECK(cond, ...)                                                   \
    do {                                                                   \
        if (!(cond)) {                                                     \
            failures++;                                                    \
            fprintf(stderr, "FAIL %s:%d: %s: ", __FILE__, __LINE__, #cond); \
            fprintf(stderr, __VA_ARGS__);                                  \
            fputc('\n', stderr);                                           \
        }                                                                  \
    } while (0)

static const char *FIXTURE = "ci/fixtures/parquet/mixed_codec.parquet";

/* Run `req`; returns the return code, the response in *out (caller frees with zpq_free). */
static int run(zpq_ctx *c, const char *req, char **out) {
    size_t len = 0;
    int rc = zpq_run(c, req, strlen(req), out, &len);
    /* out_len describes a response; a refusal's message leaves it 0. */
    if (rc == ZPQ_OK && *out) {
        if (strlen(*out) != len) {
            failures++;
            fprintf(stderr, "FAIL: out_len %zu, strlen %zu\n", len, strlen(*out));
        }
    }
    return rc;
}

/* The response's "agg" member, as text: from `"agg":` to the matching close. */
static char *agg_of(const char *resp) {
    const char *p = strstr(resp, "\"agg\":");
    if (!p) return NULL;
    p += 6;
    int depth = 0;
    const char *q = p;
    for (; *q; q++) {
        if (*q == '{' || *q == '[') depth++;
        if (*q == '}' || *q == ']') {
            if (--depth == 0) {
                q++;
                break;
            }
        }
    }
    size_t n = (size_t)(q - p);
    char *s = malloc(n + 1);
    memcpy(s, p, n);
    s[n] = 0;
    return s;
}

static long rss_kb(void) {
    FILE *f = fopen("/proc/self/statm", "r");
    long pages = 0, resident = 0;
    if (!f) return -1;
    if (fscanf(f, "%ld %ld", &pages, &resident) != 2) resident = -1;
    fclose(f);
    return resident * (sysconf(_SC_PAGESIZE) / 1024);
}

/* zpq_open with `options`, NULL on failure (any message freed). */
static zpq_ctx *open_ctx(const char *options) {
    zpq_ctx *c = (zpq_ctx *)1;
    int rc = zpq_open(options, &c, NULL);
    CHECK((rc == ZPQ_OK) == (c != NULL), "rc %d with ctx %p", rc, (void *)c);
    return c;
}

static void test_options(void) {
    CHECK(zpq_abi_version() == 1, "abi %d", zpq_abi_version());
    const char *good[] = {NULL, "", "{}", "{\"max_memory\": 1073741824}", "{\"max_memory\": \"256MB\"}",
                          "{\"preconnect\": false}"};
    for (size_t i = 0; i < sizeof good / sizeof *good; i++) {
        zpq_ctx *c = open_ctx(good[i]);
        CHECK(c != NULL, "options %s refused", good[i] ? good[i] : "NULL");
        zpq_close(c);
    }
    const char *bad[] = {"{",
                         "[]",
                         "{\"max_mem\": 1}",
                         "{\"max_memory\": -5}",
                         "{\"max_memory\": \"lots\"}",
                         "{\"preconnect\": 4}"};
    for (size_t i = 0; i < sizeof bad / sizeof *bad; i++) {
        zpq_ctx *c = (zpq_ctx *)1;
        char *err = (char *)1;
        int rc = zpq_open(bad[i], &c, &err);
        CHECK(rc == ZPQ_EINVAL && c == NULL, "options %s: rc %d", bad[i], rc);
        CHECK(err && strstr(err, "\"error\":\"invalid_argument\""), "options %s: message %s", bad[i], err);
        zpq_free(err);
    }
    /* No out_ctx: refused, with its reason; no out_error wanted: the status alone. */
    char *err = NULL;
    CHECK(zpq_open(NULL, NULL, &err) == ZPQ_EINVAL && err && strstr(err, "NullOutContext"), "%s", err);
    zpq_free(err);
    CHECK(zpq_open("{", NULL, NULL) == ZPQ_EINVAL, "NULL everything");
    /* Static, never freed; every code has one. */
    CHECK(strcmp(zpq_strerror(ZPQ_ECLOSED), "context is closed") == 0, "%s", zpq_strerror(ZPQ_ECLOSED));
    CHECK(strcmp(zpq_strerror(ZPQ_EINTERNAL), "internal failure") == 0, "%s", zpq_strerror(ZPQ_EINTERNAL));
    CHECK(strcmp(zpq_strerror(12345), "unknown status") == 0, "%s", zpq_strerror(12345));
    zpq_close(NULL);
    zpq_free(NULL);
}

static void test_round_trip(zpq_ctx *c) {
    char *out = NULL;
    int rc = run(c,
                 "{\"inputs\":[\"ci/fixtures/parquet/mixed_codec.parquet\"],"
                 "\"aggregate\":\"count(*) AS n, sum(a) AS s, min(b) AS lo\",\"filter\":\"a >= 100\"}",
                 &out);
    CHECK(rc == ZPQ_OK, "rc %d: %s", rc, out);
    CHECK(out && strstr(out, "\"agg\":{\"n\":47900,\"s\":1151971050,\"lo\":\"s0\"}"), "%s", out);
    zpq_free(out);

    /* Grouped. */
    rc = run(c, "{\"inputs\":[\"ci/fixtures/parquet/mixed_codec.parquet\"],\"aggregate\":\"count(*) AS n\","
                "\"group_by\":\"b\",\"filter\":\"a < 3\"}",
             &out);
    CHECK(rc == ZPQ_OK && strstr(out, "\"agg\":[{\"b\":\"s0\",\"n\":1},{\"b\":\"s1\",\"n\":1},{\"b\":\"s2\",\"n\":1}]"),
          "rc %d: %s", rc, out);
    zpq_free(out);
}

static void test_write(zpq_ctx *c) {
    char path[] = "/tmp/libzpq-test-XXXXXX";
    int fd = mkstemp(path);
    CHECK(fd >= 0, "mkstemp");
    close(fd);
    char req[512];
    snprintf(req, sizeof req,
             "{\"inputs\":[\"%s\"],\"output_url\":\"%s\",\"filter\":\"a >= 100\",\"columns\":[\"a\",\"b\"],"
             "\"output_codec\":\"zstd\"}",
             FIXTURE, path);
    char *out = NULL;
    int rc = run(c, req, &out);
    CHECK(rc == ZPQ_OK && strstr(out, "\"rows_kept\":47900"), "rc %d: %s", rc, out);
    zpq_free(out);
    snprintf(req, sizeof req, "{\"inputs\":[\"%s\"],\"aggregate\":\"count(*) AS n, sum(a) AS s\"}", path);
    rc = run(c, req, &out);
    CHECK(rc == ZPQ_OK && strstr(out, "\"agg\":{\"n\":47900,\"s\":1151971050}"), "rc %d: %s", rc, out);
    zpq_free(out);
    unlink(path);
}

static void test_errors(zpq_ctx *c) {
    struct {
        const char *req;
        const char *want;
    } cases[] = {
        {"{\"inputs\":[\"no/such.parquet\"],\"aggregate\":\"count(*) AS n\"}", "\"reason\":\"OpenFailed\""},
        {"{\"inputs\":[\"ci/fixtures/parquet/mixed_codec.parquet\"],\"aggregate\":\"sum(nope) AS s\"}",
         "\"error\":"},
        {"{\"inputs\":[\"ci/fixtures/parquet/mixed_codec.parquet\"],\"aggregate\":\"count(*) AS n\","
         "\"filter\":\"a >\"}",
         "\"error\":"},
        {"{\"inputs\": 5}", "\"field\":\"inputs\""},
        {"{not json", "\"error\":\"bad_json\""},
        {"[1,2]", "\"error\":\"bad_json\""},
        {"", "\"error\":\"empty_body\""},
        {"{\"inputs\":[\"ci/fixtures/parquet/mixed_codec.parquet\"]}", "\"reason\":\"NoOperation\""},
    };
    for (size_t i = 0; i < sizeof cases / sizeof *cases; i++) {
        char *out = NULL;
        int rc = run(c, cases[i].req, &out);
        CHECK(rc == ZPQ_OK, "request %zu rc %d: %s", i, rc, out);
        CHECK(out && strncmp(out, "{\"error\"", 8) == 0 && strstr(out, cases[i].want), "request %zu: %s", i, out);
        zpq_free(out);
    }
    /* A request need not be NUL-terminated: only `len` bytes are read. */
    const char padded[] =
        "{\"inputs\":[\"ci/fixtures/parquet/mixed_codec.parquet\"],\"aggregate\":\"count(*) AS n\"}GARBAGE";
    char *out = NULL;
    size_t len = 0;
    int rc = zpq_run(c, padded, sizeof padded - 1 - 7, &out, &len);
    CHECK(rc == ZPQ_OK && strstr(out, "\"n\":48000"), "rc %d: %s", rc, out);
    zpq_free(out);
}

static void test_invalid_arguments(zpq_ctx *c) {
    /* A refused call clears every output given, then describes the refusal in *out_json; out_len stays 0. */
    char *out = (char *)1;
    size_t len = 7;
    CHECK(zpq_run(NULL, "{}", 2, &out, &len) == ZPQ_EINVAL && out && strstr(out, "NullContext") && len == 0,
          "NULL ctx: %s %zu", out, len);
    zpq_free(out);
    len = 7;
    CHECK(zpq_run(c, "{}", 2, NULL, &len) == ZPQ_EINVAL && len == 0, "NULL out_json: %zu", len);
    out = (char *)1;
    len = 7;
    CHECK(zpq_run(c, NULL, 5, &out, &len) == ZPQ_EINVAL && out && strstr(out, "NullRequest") && len == 0,
          "NULL request: %s", out);
    zpq_free(out);
    CHECK(zpq_run(c, "{}", (size_t)-1, &out, NULL) == ZPQ_EINVAL && out && strstr(out, "BadLength"), "%s", out);
    zpq_free(out);
    /* NULL request of length 0 is the empty request, answered; out_len may be NULL. */
    CHECK(zpq_run(c, NULL, 0, &out, NULL) == ZPQ_OK && out && strstr(out, "empty_body"), "empty request: %s", out);
    zpq_free(out);
}

/* Concurrent calls: each thread runs a mix of queries on the shared context and checks each answer. */
#define THREADS 16
#define CALLS_PER_THREAD 40
static zpq_ctx *shared;
/* Each names a small budget of its own. */
static const char *mix_req[] = {
    "{\"inputs\":[\"ci/fixtures/parquet/mixed_codec.parquet\"],\"aggregate\":\"count(*) AS n, sum(a) AS s\","
    "\"max_memory\":\"16MB\"}",
    "{\"inputs\":[\"ci/fixtures/parquet/mixed_codec.parquet\"],\"aggregate\":\"max(b) AS m\",\"filter\":\"a < 1000\","
    "\"max_memory\":\"16MB\"}",
    "{\"inputs\":[\"ci/fixtures/parquet/mixed_codec.parquet\",\"ci/fixtures/parquet/mixed_codec.parquet\"],"
    "\"aggregate\":\"count(*) AS n\",\"group_by\":\"b\",\"filter\":\"a < 2\",\"max_memory\":\"16MB\"}",
    "{\"inputs\":[\"no/such.parquet\"],\"aggregate\":\"count(*) AS n\",\"max_memory\":\"16MB\"}",
};
static char *mix_want[4];

static void *worker(void *arg) {
    long id = (long)arg;
    for (int i = 0; i < CALLS_PER_THREAD; i++) {
        int k = (int)((id + i) % 4);
        char *out = NULL;
        int rc = run(shared, mix_req[k], &out);
        if (k == 3) {
            CHECK(rc == ZPQ_OK && out && strncmp(out, "{\"error\"", 8) == 0, "thread %ld call %d rc %d", id, i, rc);
        } else {
            char *a = out ? agg_of(out) : NULL;
            CHECK(rc == ZPQ_OK && a && strcmp(a, mix_want[k]) == 0, "thread %ld query %d: %s", id, k, out);
            free(a);
        }
        zpq_free(out);
    }
    return NULL;
}

static void run_threads(zpq_ctx *c) {
    shared = c;
    pthread_t t[THREADS];
    for (long i = 0; i < THREADS; i++) pthread_create(&t[i], NULL, worker, (void *)i);
    for (int i = 0; i < THREADS; i++) pthread_join(t[i], NULL);
}

static void test_concurrent(zpq_ctx *c) {
    for (int k = 0; k < 3; k++) {
        char *out = NULL;
        CHECK(run(c, mix_req[k], &out) == ZPQ_OK, "%s", out);
        mix_want[k] = agg_of(out);
        zpq_free(out);
    }
    /* Warm: the context has answered before. */
    run_threads(c);
    /* Cold: every thread's first call is the fresh context's first. */
    for (int round = 0; round < 8; round++) {
        zpq_ctx *cold = open_ctx(NULL);
        CHECK(cold != NULL, "zpq_open");
        if (!cold) break;
        run_threads(cold);
        zpq_close(cold);
    }
    for (int k = 0; k < 3; k++) free(mix_want[k]);
}

/* Resident memory must not grow with the number of calls: the second half of the loop adds next to nothing. */
static void test_rss_flat(zpq_ctx *c, int calls) {
    const char *req = "{\"inputs\":[\"ci/fixtures/parquet/mixed_codec.parquet\"],\"aggregate\":\"count(*) AS n, "
                      "sum(c) AS s\",\"group_by\":\"b\",\"filter\":\"a >= 40000\"}";
    long at_half = 0;
    long start = rss_kb();
    for (int i = 0; i < calls; i++) {
        char *out = NULL;
        int rc = run(c, req, &out);
        if (rc != ZPQ_OK) {
            CHECK(rc == ZPQ_OK, "call %d rc %d: %s", i, rc, out);
            zpq_free(out);
            return;
        }
        zpq_free(out);
        if (i == calls / 2) at_half = rss_kb();
    }
    long end = rss_kb();
    printf("rss: start %ld KB, after %d calls %ld KB, after %d calls %ld KB\n", start, calls / 2, at_half, calls,
           end);
#ifndef __SANITIZE_ADDRESS__
    /* The second half may not grow resident memory by more than 2 MB (allocator noise). Not under ASan, whose
       quarantine holds freed memory back and grows resident memory by design; there, LeakSanitizer checks. */
    CHECK(end - at_half < 2048, "RSS grew %ld KB over the second %d calls", end - at_half, calls - calls / 2);
#endif
}

static double now_s(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + ts.tv_nsec / 1e9;
}

/* Every context's memory comes back: open, run and close, many times (LeakSanitizer reports anything left). */
static void test_cycles(int n) {
    for (int i = 0; i < n; i++) {
        zpq_ctx *c = open_ctx("{\"max_memory\": \"64MB\"}");
        if (!c) return;
        char *out = NULL;
        int rc = run(c, "{\"inputs\":[\"ci/fixtures/parquet/mixed_codec.parquet\"],\"aggregate\":\"count(*) AS n\"}",
                     &out);
        if (rc != ZPQ_OK || !out || !strstr(out, "\"n\":48000")) {
            CHECK(0, "cycle %d rc %d: %s", i, rc, out);
            zpq_free(out);
            zpq_close(c);
            return;
        }
        zpq_free(out);
        zpq_close(c);
    }
}

int main(int argc, char **argv) {
    int calls = argc > 1 ? atoi(argv[1]) : 10000;
    int cycles = argc > 2 ? atoi(argv[2]) : 1000;
    test_options();
    test_cycles(cycles);
    zpq_ctx *c = open_ctx("{\"max_memory\": \"512MB\"}");
    if (!c) {
        fprintf(stderr, "zpq_open failed\n");
        return 1;
    }
    test_round_trip(c);
    test_write(c);
    test_errors(c);
    test_invalid_arguments(c);
    test_concurrent(c);
    test_rss_flat(c, calls);
    zpq_close(c);
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("libzpq C ABI: all checks passed\n");
    return 0;
}
