"""Optional PyArrow C stream binding. No compiled Python extension or Arrow C++ dependency in libzpq."""
import ctypes
import json


class _Stream(ctypes.Structure):
    _fields_ = [(name, ctypes.c_void_p) for name in
                ("get_schema", "get_next", "get_last_error", "release", "private_data")]


class ArrowReader:
    """A PyArrow reader holding a context call until close/EOF; batches have independent lifetimes."""
    def __init__(self, owner, reader):
        self._owner, self._reader = owner, reader

    @property
    def schema(self):
        return self._reader.schema

    def read_next_batch(self):
        try:
            return self._reader.read_next_batch()
        except BaseException:
            self.close()
            raise

    def read_all(self):
        try:
            return self._reader.read_all()
        finally:
            self.close()

    def __iter__(self):
        try:
            yield from self._reader
        finally:
            self.close()

    def __arrow_c_stream__(self, requested_schema=None):
        """The Arrow PyCapsule stream protocol, so DuckDB, Polars and DataFusion read this reader directly. The consumer
        takes the batches not yet read; its EOF or release ends the context call, as close() does."""
        import pyarrow as pa
        return pa.RecordBatchReader.from_batches(self.schema, self._handoff()).__arrow_c_stream__(requested_schema)

    def _handoff(self):
        # Started at once, so its finally runs even when the consumer releases the stream before reading it.
        def batches():
            try:
                yield
                yield from self._reader
            finally:
                self.close()
        it = batches()
        next(it)
        return it

    def close(self):
        owner, self._owner = self._owner, None
        if owner is not None:
            try:
                self._reader.close()
            finally:
                with owner._cond:
                    owner._inflight -= 1
                    owner._cond.notify_all()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __del__(self):
        self.close()


def open_reader(owner, request):
    import pyarrow as pa
    body = (request if isinstance(request, bytes) else
            request.encode() if isinstance(request, str) else json.dumps(request).encode())
    call = owner._lib.zpq_run_arrow
    call.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t,
                     ctypes.POINTER(_Stream), ctypes.POINTER(ctypes.c_void_p)]
    call.restype = ctypes.c_int
    with owner._cond:
        if owner._closing:
            raise ValueError("Zpq is closed")
        owner._inflight += 1
    stream = _Stream()
    err = ctypes.c_void_p()
    try:
        rc = call(owner._ctx, body, len(body), ctypes.byref(stream), ctypes.byref(err))
        if rc:
            raise owner._failure("zpq_run_arrow", rc, err)
        # PyArrow moves the stream and clears the original release pointer, including when import fails.
        reader = pa.RecordBatchReader._import_from_c(ctypes.addressof(stream))
        return ArrowReader(owner, reader)
    except BaseException:
        if stream.release:
            ctypes.CFUNCTYPE(None, ctypes.POINTER(_Stream))(stream.release)(ctypes.byref(stream))
        with owner._cond:
            owner._inflight -= 1
            owner._cond.notify_all()
        raise
