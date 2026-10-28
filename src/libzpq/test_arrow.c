/* Run against python/check_arrow.py's all_types.parquet, plain and under ASan/LSan. */
#include "zpq.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char **argv) {
    assert(argc == 2);
    for (int cycle = 0; cycle < 100; cycle++) {
        for (int mode = 0; mode < 3; mode++) {
            zpq_ctx *ctx = NULL;
            char *error = NULL;
            assert(zpq_open("{\"max_memory\":\"1GB\"}", &ctx, &error) == ZPQ_OK);
            char request[4096];
            int len = snprintf(request, sizeof request,
                "{\"inputs\":[\"%s\"],\"columns\":[\"id\"],\"max_memory\":\"64MB\"%s}",
                argv[1], mode == 2 ? ",\"max_result_bytes\":262144" : "");
            assert(len > 0 && (size_t)len < sizeof request);
            struct ArrowArrayStream stream = {0};
            assert(zpq_run_arrow(ctx, request, (size_t)len, &stream, &error) == ZPQ_OK);
            assert(error == NULL && stream.release);
            if (mode == 0) {
                stream.release(&stream);
                assert(stream.release == NULL);
                zpq_close(ctx);
                continue;
            }
            struct ArrowSchema schema = {0}, another = {0};
            assert(stream.get_schema(&stream, &schema) == 0);
            assert(stream.get_schema(&stream, &another) == 0);
            assert(strcmp(schema.format, "+s") == 0 && schema.n_children == 1);
            assert(strcmp(schema.children[0]->format, "l") == 0);
            another.release(&another);
            struct ArrowArray first = {0};
            assert(stream.get_next(&stream, &first) == 0 && first.release);
            assert(first.length == 32768);
            if (mode == 2) {
                struct ArrowArray next = {0};
                assert(stream.get_next(&stream, &next) != 0 && next.release == NULL);
                const char *message = stream.get_last_error(&stream);
                assert(message && strstr(message, "ResultTooLarge"));
                assert(stream.get_next(&stream, &next) != 0 && next.release == NULL);
            }
            /* Move consumer-owned structures, release stream/context, then read/release their children. */
            struct ArrowArray moved = first;
            first.release = NULL;
            stream.release(&stream);
            zpq_close(ctx);
            const int64_t *values = moved.children[0]->buffers[1];
            assert(values[0] == 0 && values[32767] == 32767);
            assert(strcmp(schema.children[0]->name, "id") == 0);
            schema.release(&schema);
            moved.release(&moved);
            assert(schema.release == NULL && moved.release == NULL);
        }
    }
    puts("Arrow C lifetimes: 300 passed, 0 failed (early release, partial read, mid-stream error)");
    return 0;
}
