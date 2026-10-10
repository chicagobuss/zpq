"""zpq from Python through libzpq's C ABI (include/zpq.h), with ctypes: no compiled extension.

    import zpq
    with zpq.Zpq() as z:
        z.run({"inputs": ["data.parquet"], "aggregate": "count(*) AS n"})

`run` takes the zpq Lambda's request (a dict, or JSON text) and returns its response as a dict; an {"error": ...}
response raises `ZpqError`, unless `check=False`. One `Zpq` serves any number of threads at once: ctypes releases
the GIL for the call, and each query runs on threads of its own, within the context's memory ceiling. `close` refuses
new calls (`ZpqClosed`) and waits for those in flight, including any still waiting for their memory budget, so a
context is never freed under a running query.

The library is found at $ZPQ_LIB, else as libzpq.so (libzpq.dylib on macOS) next to this file, in ../zig-out/lib, or
on the loader path.
"""

import ctypes
import json
import os
import sys
import threading

__all__ = ["Zpq", "ZpqError", "ZpqClosed", "load_library"]

# zpq_status (include/zpq.h).
OK = 0
EINVAL = -1
ENOMEM = -2
ECLOSED = -3
EINTERNAL = -4
ABI_VERSION = 1


class ZpqError(Exception):
    """An {"error": ...} response; `response` holds it whole."""

    def __init__(self, response):
        self.response = response
        super().__init__(json.dumps(response))


class ZpqClosed(ValueError):
    """A call on a `Zpq` that is closing or closed."""


_LIB_NAME = "libzpq.dylib" if sys.platform == "darwin" else "libzpq.so"


def _candidates():
    if os.environ.get("ZPQ_LIB"):
        yield os.environ["ZPQ_LIB"]
    here = os.path.dirname(os.path.abspath(__file__))
    yield os.path.join(here, _LIB_NAME)
    yield os.path.join(here, "..", "zig-out", "lib", _LIB_NAME)
    yield _LIB_NAME


_lib = None
_lib_lock = threading.Lock()


def load_library(path=None):
    """The loaded libzpq, its signatures declared. Loaded once per process."""
    global _lib
    with _lib_lock:
        if _lib is not None:
            return _lib
        errors = []
        for cand in [path] if path else _candidates():
            if cand != _LIB_NAME and not os.path.exists(cand):
                continue
            try:
                lib = ctypes.CDLL(cand)
                break
            except OSError as e:
                errors.append(f"{cand}: {e}")
        else:
            raise OSError(_LIB_NAME + " not found; set ZPQ_LIB. " + "; ".join(errors))
        lib.zpq_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(ctypes.c_void_p)]
        lib.zpq_open.restype = ctypes.c_int
        lib.zpq_run.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t,
                                ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(ctypes.c_size_t)]
        lib.zpq_run.restype = ctypes.c_int
        lib.zpq_free.argtypes = [ctypes.c_void_p]
        lib.zpq_free.restype = None
        lib.zpq_close.argtypes = [ctypes.c_void_p]
        lib.zpq_close.restype = None
        lib.zpq_abi_version.argtypes = []
        lib.zpq_abi_version.restype = ctypes.c_int
        lib.zpq_strerror.argtypes = [ctypes.c_int]
        lib.zpq_strerror.restype = ctypes.c_char_p
        got = lib.zpq_abi_version()
        if got != ABI_VERSION:
            raise OSError(f"libzpq ABI {got}, this binding speaks {ABI_VERSION}")
        _lib = lib
        return lib


class Zpq:
    """A libzpq context: warm S3 connections and footers shared by every `run` on it."""

    def __init__(self, max_memory=None, library=None, *, preconnect=False, budget_wait_ms=None):
        """`max_memory`: the context's memory ceiling, shared by its queries (default: the memory available).
        `budget_wait_ms`: how long a query waits for its budget when its request does not say (default: no limit).
        `preconnect`: open connections to the S3 endpoint now, as the Lambda does during init."""
        self._lib = load_library(library)
        opts = {} if max_memory is None else {"max_memory": max_memory}
        if budget_wait_ms is not None:
            opts["budget_wait_ms"] = budget_wait_ms
        if preconnect:
            opts["preconnect"] = True
        # Calls in flight, and whether close has begun: close waits for the first to drain before freeing the context.
        self._cond = threading.Condition()
        self._inflight = 0
        self._closing = False
        ctx = ctypes.c_void_p()
        err = ctypes.c_void_p()
        rc = self._lib.zpq_open(json.dumps(opts).encode(), ctypes.byref(ctx), ctypes.byref(err))
        self._ctx = ctx.value
        if rc != OK:
            raise self._failure("zpq_open", rc, err, opts)

    def _failure(self, call, rc, msg, detail=None):
        """The exception for status `rc`; frees the message `msg` points to, if any."""
        text = None
        if msg.value:
            try:
                text = ctypes.string_at(msg.value).decode()
            finally:
                self._lib.zpq_free(msg)
        what = f"{call}: {self._lib.zpq_strerror(rc).decode()}" + (f": {text}" if text else "")
        if detail is not None:
            what += f" ({detail!r})"
        if rc == ENOMEM:
            return MemoryError(what)
        if rc == ECLOSED:
            return ZpqClosed(what)
        if rc == EINVAL:
            return ValueError(what)
        return RuntimeError(what)

    def run_raw(self, request):
        """Run `request` (str or bytes of JSON); returns the response text, an {"error": ...} document included."""
        body = request.encode() if isinstance(request, str) else bytes(request)
        with self._cond:
            if self._closing:
                raise ZpqClosed("Zpq is closed")
            self._inflight += 1
        try:
            return self._run(body)
        finally:
            with self._cond:
                self._inflight -= 1
                if self._inflight == 0:
                    self._cond.notify_all()

    def _run(self, body):
        out = ctypes.c_void_p()
        out_len = ctypes.c_size_t()
        rc = self._lib.zpq_run(self._ctx, body, len(body), ctypes.byref(out), ctypes.byref(out_len))
        if rc != OK:
            raise self._failure("zpq_run", rc, out)
        try:
            return ctypes.string_at(out.value, out_len.value).decode()
        finally:
            self._lib.zpq_free(out)

    def run(self, request, check=True):
        """Run `request` (a dict, or JSON text) and return the response as a dict."""
        response = json.loads(self.run_raw(request if isinstance(request, (str, bytes)) else json.dumps(request)))
        if check and isinstance(response, dict) and "error" in response:
            raise ZpqError(response)
        return response

    def close(self):
        """Refuse new calls, wait for those in flight, then free the context. Idempotent."""
        cond = getattr(self, "_cond", None)
        if cond is None:  # __init__ failed before the context existed
            return
        with cond:
            self._closing = True
            while self._inflight:
                cond.wait()
            ctx, self._ctx = self._ctx, None
        if ctx:
            self._lib.zpq_close(ctx)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
