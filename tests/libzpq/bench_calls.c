/*
 * N zpq_run calls of one request on one context, for perf counters: counting a run of N and a run of 1, their
 * difference over N - 1 is the cost of a call on a warm context. Usage: bench_calls N 'request json'
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "zpq.h"

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "usage: %s N request_json\n", argv[0]);
        return 2;
    }
    int n = atoi(argv[1]);
    zpq_ctx *c = zpq_open(NULL);
    if (!c) return 1;
    for (int i = 0; i < n; i++) {
        char *out = NULL;
        int rc = zpq_run(c, argv[2], strlen(argv[2]), &out, NULL);
        if (rc != ZPQ_OK) {
            fprintf(stderr, "rc %d: %s\n", rc, out ? out : "");
            return 1;
        }
        zpq_free(out);
    }
    zpq_close(c);
    return 0;
}
