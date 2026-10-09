"""zpq from Python through libzpq's C ABI (include/zpq.h), with ctypes: no compiled extension.

    import zpq
    with zpq.Zpq() as z:
        z.run({"inputs": ["data.parquet"], "aggregate": "count(*) AS n"})

`run` takes the zpq Lambda's request (a dict, or JSON text) and returns its response as a dict; an {"error": ...}
response raises `ZpqError`, unless `check=False`. One `Zpq` serves any number of threads at once: ctypes releases
the GIL for the call, and each query runs on threads of its own.

The library is found at $ZPQ_LIB, else as libzpq.so next to this file, in ../zig-out/lib, or on the loader path.
"""

import ctypes
import json
import os
import threading

__all__ = ["Zpq", "ZpqError", "load_library"]

OK = 0
ERROR_RESPONSE = 1
EINVAL = -1
ENOMEM = -2
ABI_VERSION = 1


class ZpqError(Exception):
    """An {"error": ...} response; `response` holds it whole."""

    def __init__(self, response):
        self.response = response
        super().__init__(json.dumps(response))


def _candidates():
    if os.environ.get("ZPQ_LIB"):
        yield os.environ["ZPQ_LIB"]
    here = os.path.dirname(os.path.abspath(__file__))
    yield os.path.join(here, "libzpq.so")
    yield os.path.join(here, "..", "zig-out", "lib", "libzpq.so")
    yield "libzpq.so"


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
            if cand != "libzpq.so" and not os.path.exists(cand):
                continue
            try:
                lib = ctypes.CDLL(cand)
                break
            except OSError as e:
                errors.append(f"{cand}: {e}")
        else:
            raise OSError("libzpq.so not found; set ZPQ_LIB. " + "; ".join(errors))
        lib.zpq_open.argtypes = [ctypes.c_char_p]
        lib.zpq_open.restype = ctypes.c_void_p
        lib.zpq_run.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t,
                                ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(ctypes.c_size_t)]
        lib.zpq_run.restype = ctypes.c_int
        lib.zpq_free.argtypes = [ctypes.c_void_p]
        lib.zpq_free.restype = None
        lib.zpq_close.argtypes = [ctypes.c_void_p]
        lib.zpq_close.restype = None
        lib.zpq_abi_version.argtypes = []
        lib.zpq_abi_version.restype = ctypes.c_int
        got = lib.zpq_abi_version()
        if got != ABI_VERSION:
            raise OSError(f"libzpq ABI {got}, this binding speaks {ABI_VERSION}")
        _lib = lib
        return lib


class Zpq:
    """A libzpq context: warm S3 connections and footers shared by every `run` on it."""

    def __init__(self, max_memory=None, library=None):
        self._lib = load_library(library)
        opts = {} if max_memory is None else {"max_memory": max_memory}
        self._ctx = self._lib.zpq_open(json.dumps(opts).encode())
        if not self._ctx:
            raise ValueError(f"zpq_open refused options {opts!r}")

    def run_raw(self, request):
        """Run `request` (str or bytes of JSON); returns (return code, response text)."""
        if self._ctx is None:
            raise ValueError("Zpq is closed")
        body = request.encode() if isinstance(request, str) else bytes(request)
        out = ctypes.c_void_p()
        out_len = ctypes.c_size_t()
        rc = self._lib.zpq_run(self._ctx, body, len(body), ctypes.byref(out), ctypes.byref(out_len))
        if rc < 0:
            raise MemoryError("zpq_run: out of memory") if rc == ENOMEM else ValueError(f"zpq_run returned {rc}")
        try:
            text = ctypes.string_at(out.value, out_len.value).decode()
        finally:
            self._lib.zpq_free(out)
        return rc, text

    def run(self, request, check=True):
        """Run `request` (a dict, or JSON text) and return the response as a dict."""
        rc, text = self.run_raw(request if isinstance(request, (str, bytes)) else json.dumps(request))
        response = json.loads(text)
        if check and rc == ERROR_RESPONSE:
            raise ZpqError(response)
        return response

    def close(self):
        """Free the context. No `run` may be in flight."""
        if self._ctx is not None:
            self._lib.zpq_close(self._ctx)
            self._ctx = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
