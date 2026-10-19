"""zpq from Python through libzpq's C ABI (include/zpq.h), with ctypes: no compiled extension.

    import zpq
    with zpq.Zpq() as z:
        z.run({"inputs": ["data.parquet"], "aggregate": "count(*) AS n"})

`run` takes the zpq Lambda's request (a dict, or JSON text) and returns its response as a dict; an {"error": ...}
response raises `ZpqError`, unless `check=False`. One `Zpq` serves any number of threads at once: ctypes releases
the GIL for the call, and each query runs on threads of its own, its declared budget within the context's ceiling.
`close` refuses new calls and ends those still waiting for their memory budget (both `ZpqClosed`), then waits for the
running ones, so a context is never freed under a running query.

SQL requests are EXPERIMENTAL in 0.5.0, with no stability guarantee for their request or response shape.
`query` and `query_raw` are thin pass-throughs to the CLI frontend.

Exact by default: `query` rows and `run`'s `agg` give the values of DECIMAL columns (the response's `decimal` list
names them) as `decimal.Decimal`, every digit kept; other numbers are int or float, as JSON reads them. Pass
`decimal=False` for floats throughout.

The library is found at $ZPQ_LIB, else as libzpq.so (libzpq.dylib on macOS) next to this file, in ../zig-out/lib, or
on the loader path.
"""

import ctypes
import decimal as _decimal
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
        lib.zpq_shutdown.argtypes = [ctypes.c_void_p]
        lib.zpq_shutdown.restype = None
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


def _exact_columns(exact, plain, names):
    """`plain` (rows or a row: dicts parsed as JSON reads them), with the values of columns `names` from `exact`, the
    same JSON parsed with Decimal for its fractional numbers."""
    if isinstance(plain, list):
        return [_exact_columns(e, p, names) for e, p in zip(exact, plain)]
    return {k: (exact[k] if k in names else v) for k, v in plain.items()}


class Zpq:
    """A libzpq context: warm S3 connections and footers shared by every `run` on it."""

    def __init__(self, max_memory=None, library=None, *, preconnect=False, budget_wait_ms=None):
        """`max_memory`: the context's ceiling on its queries' declared budgets (default: the memory available).
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

    def run(self, request, check=True, decimal=True):
        """Run `request` (a dict, or JSON text) and return the response as a dict. With `decimal` (the default), the
        `agg` values of the DECIMAL columns the response names are `decimal.Decimal`."""
        text = self.run_raw(request if isinstance(request, (str, bytes)) else json.dumps(request))
        response = json.loads(text)
        if check and isinstance(response, dict) and "error" in response:
            raise ZpqError(response)
        if decimal and isinstance(response, dict) and response.get("decimal") and "agg" in response:
            exact = json.loads(text, parse_float=_decimal.Decimal)["agg"]
            response["agg"] = _exact_columns(exact, response["agg"], set(response["decimal"]))
        return response

    def query_raw(self, sql, **options):
        """EXPERIMENTAL in 0.5.0: thin CLI SQL pass-through returning exact NDJSON text.

        The SQL request and response shapes have no stability guarantee. Options are common JSON request options.
        """
        if not isinstance(sql, str):
            raise TypeError("sql must be a string")
        return self.run({**options, "sql": sql})["ndjson"]

    def query(self, sql, decimal=True, **options):
        """EXPERIMENTAL in 0.5.0: thin CLI SQL pass-through returning a list of row dicts.

        The SQL request and response shapes have no stability guarantee. `parallelism` selects workers (0: automatic).
        With `decimal` (the default) a DECIMAL column's values are `decimal.Decimal`; `decimal=False` gives floats.
        """
        if not isinstance(sql, str):
            raise TypeError("sql must be a string")
        response = self.run({**options, "sql": sql})
        # NDJSON rows end in "\n" alone: str.splitlines() would also split at U+0085 and other line boundaries,
        # which may appear unescaped inside JSON strings.
        lines = response["ndjson"].split("\n")
        if lines[-1] == "":
            lines.pop()
        exact = set(response.get("decimal") or ()) if decimal else set()
        if not exact:
            return [json.loads(line) for line in lines]
        return [_exact_columns(json.loads(line, parse_float=_decimal.Decimal), json.loads(line), exact)
                for line in lines]

    def close(self):
        """Refuse new calls, end those waiting for their memory budget (`ZpqClosed`), wait for the running ones, then
        free the context. Idempotent."""
        cond = getattr(self, "_cond", None)
        if cond is None:  # __init__ failed before the context existed
            return
        with cond:
            self._closing = True
            if self._ctx:
                # Without it, queries queued for their budget would be waited for here and run.
                self._lib.zpq_shutdown(self._ctx)
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
