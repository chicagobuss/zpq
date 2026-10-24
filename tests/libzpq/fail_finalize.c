/* glibc-only fault shim: reject libzpq's last realloc, after the service has built its response. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stddef.h>
#include <string.h>

extern void *__libc_realloc(void *, size_t);
static int failed;
static _Thread_local int checking;

void *realloc(void *ptr, size_t size) {
    if (ptr && !failed && !checking) {
        checking = 1;
        Dl_info caller;
        int finalize = dladdr(__builtin_return_address(0), &caller) && caller.dli_sname &&
                       strcmp(caller.dli_sname, "zpq_run") == 0;
        checking = 0;
        if (finalize) {
            failed = 1;
            errno = ENOMEM;
            return NULL;
        }
    }
    return __libc_realloc(ptr, size);
}
