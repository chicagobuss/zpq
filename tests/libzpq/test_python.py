"""libzpq's Python binding (python/zpq.py) and service (python/zpq_service.py), end to end. Run from the repo root
with ZPQ_LIB pointing at libzpq.so (tests/libzpq/run.sh does)."""

import json
import os
import socket
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))
import zpq  # noqa: E402
import zpq_service  # noqa: E402

FIX = "ci/fixtures/parquet/mixed_codec.parquet"
failures = []


def check(cond, msg):
    if not cond:
        failures.append(msg)
        print("FAIL", msg, file=sys.stderr)


def test_binding(z):
    r = z.run({"inputs": [FIX], "aggregate": "count(*) AS n, sum(a) AS s", "filter": "a >= 100"})
    check(r["ok"] and r["agg"] == {"n": 47900, "s": 1151971050}, f"aggregate {r}")
    check(z.query(f"SELECT count(*) AS n FROM '{FIX}'", parallelism=3) == [{"n": 48000}], "SQL rows")
    check(z.query(f"SELECT * FROM '{FIX}' LIMIT 0") == [], "SQL empty rows")
    check(z.query_raw(f"SELECT a FROM '{FIX}' LIMIT 1") == '{"a":0}\n', "SQL raw bytes")
    # U+0085 is a Unicode line boundary but stays unescaped in NDJSON strings: rows split only at "\n".
    check(z.query(f"SELECT 'a\u0085b' AS \"n\u0085m\" FROM '{FIX}' LIMIT 2") == [{"n\u0085m": "a\u0085b"}] * 2,
          "SQL rows with U+0085")
    prof = z.run({"sql": f"SELECT count(*) AS n FROM '{FIX}'", "profile": True})
    check(prof["ndjson"] == '{"n":48000}\n' and "profile" in prof, "SQL profile")
    # JSON text and bytes work too.
    r2 = z.run(json.dumps({"inputs": [FIX], "aggregate": "count(*) AS n"}).encode())
    check(r2["agg"] == {"n": 48000}, f"bytes request {r2}")
    try:
        z.run({"inputs": ["no/such.parquet"], "aggregate": "count(*) AS n"})
        check(False, "missing file did not raise")
    except zpq.ZpqError as e:
        check(e.response.get("reason") == "OpenFailed", f"error response {e.response}")
    r3 = z.run({"inputs": [FIX]}, check=False)
    check(r3.get("reason") == "NoOperation", f"check=False {r3}")
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "o.parquet")
        w = z.run({"inputs": [FIX], "output_url": out, "select": "a * 2 AS x, b", "filter": "a < 10"})
        check(w["rows_matched"] == 10 and w["rows_scanned"] >= 10, f"write {w}")
        back = z.run({"inputs": [out], "aggregate": "sum(x) AS s"})
        check(back["agg"] == {"s": 90}, f"read back {back}")


def test_boolean_answers(z):
    # A BOOLEAN key and min/max of a BOOLEAN print true/false in every JSON surface, as its rows do.
    import pyarrow as pa
    import pyarrow.parquet as pq
    with tempfile.TemporaryDirectory() as d:
        src = os.path.join(d, "b.parquet")
        pq.write_table(pa.table({"b": pa.array([True, False, True, None], pa.bool_())}), src)
        rows = z.query_raw(f"SELECT b, count(*) AS n FROM '{src}' GROUP BY b")
        check(rows == '{"b":true,"n":2}\n{"b":false,"n":1}\n{"b":null,"n":1}\n', f"bool key rows {rows!r}")
        mm = z.query_raw(f"SELECT min(b) AS mn, max(b) AS mx FROM '{src}'")
        check(mm == '{"mn":false,"mx":true}\n', f"bool min/max rows {mm!r}")
        g = z.run({"inputs": [src], "group_by": "b", "aggregate": "count(*) AS n"})
        check(g["agg"] == [{"b": True, "n": 2}, {"b": False, "n": 1}, {"b": None, "n": 1}]
              and all(type(r["b"]) is not int for r in g["agg"]), f"bool key agg {g['agg']}")
        a = z.run({"inputs": [src], "aggregate": "min(b) AS mn, max(b) AS mx"})
        check(a["agg"] == {"mn": False, "mx": True} and type(a["agg"]["mn"]) is bool, f"bool min/max agg {a['agg']}")
        # A BOOLEAN-typed expression too: a CASE over the column.
        e = z.run({"inputs": [src], "aggregate": "max(CASE WHEN b IS NOT NULL THEN b END) AS mx"})
        check(e["agg"] == {"mx": True} and type(e["agg"]["mx"]) is bool, f"bool expression agg {e['agg']}")
        r = z.query_raw(f"SELECT CASE WHEN b IS NOT NULL THEN b END AS k FROM '{src}'")
        check(r == '{"k":true}\n{"k":false}\n{"k":true}\n{"k":null}\n', f"bool expression rows {r!r}")


def test_decimal_exact(z):
    # DECIMAL columns come back as Decimal, every digit kept; DOUBLE stays float; decimal=False gives floats.
    import pyarrow as pa
    import pyarrow.parquet as pq
    from decimal import Decimal
    with tempfile.TemporaryDirectory() as d:
        src = os.path.join(d, "dec.parquet")
        pq.write_table(pa.table({
            "p": pa.array([Decimal("83332533333.00"), Decimal("0.10"), None], pa.decimal128(18, 2)),
            "f": pa.array([0.5, 0.25, 1.0], pa.float64()),
            "k": pa.array([Decimal("1.5"), Decimal("1.5"), Decimal("2.0")], pa.decimal128(4, 1)),
        }), src)
        rows = z.query(f"SELECT p, f FROM '{src}'")
        check(rows[0] == {"p": Decimal("83332533333.00"), "f": 0.5} and type(rows[0]["p"]) is Decimal
              and type(rows[0]["f"]) is float and rows[2]["p"] is None, f"decimal rows {rows}")
        check(str(rows[1]["p"]) == "0.10", f"decimal scale kept {rows[1]}")
        plain = z.query(f"SELECT p, f FROM '{src}'", decimal=False)
        check(type(plain[0]["p"]) is float, f"decimal=False rows {plain}")
        agg = z.query(f"SELECT sum(p) AS s, min(f) AS m, count(*) AS n FROM '{src}'")
        check(agg == [{"s": Decimal("83332533333.10"), "m": 0.25, "n": 3}] and type(agg[0]["m"]) is float,
              f"decimal SQL aggregate {agg}")
        grp = z.query(f"SELECT k, sum(p) AS s FROM '{src}' GROUP BY k ORDER BY k")
        check(grp == [{"k": Decimal("1.5"), "s": Decimal("83332533333.10")}, {"k": Decimal("2.0"), "s": None}],
              f"decimal key {grp}")
        r = z.run({"inputs": [src], "aggregate": "sum(p) AS s, avg(p) AS a, min(f) AS m, avg_state(p) AS st"})
        # avg over DECIMAL(18,2) is DECIMAL(22,6); avg_state keeps the exact sum at the column's scale.
        check(r["agg"]["s"] == Decimal("83332533333.10") and type(r["agg"]["s"]) is Decimal
              and type(r["agg"]["m"]) is float and type(r["agg"]["a"]) is Decimal
              and str(r["agg"]["a"]) == "41666266666.550000"
              and r["agg"]["st"] == {"sum": Decimal("83332533333.10"), "count": 2},
              f"decimal agg {r['agg']}")
        check(r.get("decimal") == ["s", "a", "st"], f"decimal names {r.get('decimal')}")
        r2 = z.run({"inputs": [src], "aggregate": "sum(p) AS s"}, decimal=False)
        check(type(r2["agg"]["s"]) is float, f"run decimal=False {r2['agg']}")
        # A scale-0 DECIMAL prints as an integer token: still Decimal, in rows, keys and aggregates; an avg_state's
        # count, and a non-DECIMAL integer, stay int. avg over DECIMAL(38,0) keeps the type.
        src0 = os.path.join(d, "dec0.parquet")
        big = Decimal("12345678901234567890123456789012345678")
        pq.write_table(pa.table({"d": pa.array([big, Decimal("2")], pa.decimal128(38, 0)),
                                 "i": pa.array([1, 2], pa.int64())}), src0)
        r0 = z.query(f"SELECT d, i FROM '{src0}'")
        check(r0[0] == {"d": big, "i": 1} and type(r0[0]["d"]) is Decimal and type(r0[0]["i"]) is int,
              f"scale-0 rows {r0}")
        k0 = z.query(f"SELECT d, count(*) AS n FROM '{src0}' GROUP BY d ORDER BY d")
        check(type(k0[0]["d"]) is Decimal and type(k0[0]["n"]) is int, f"scale-0 key {k0}")
        m0 = z.query(f"SELECT min(d) AS m FROM '{src0}'")
        check(m0 == [{"m": Decimal(2)}] and type(m0[0]["m"]) is Decimal, f"scale-0 scalar {m0}")
        s0 = z.run({"inputs": [src0], "aggregate": "min(d) AS m, avg_state(d) AS a, sum(i) AS si, avg(d) AS av"})
        check(type(s0["agg"]["m"]) is Decimal and type(s0["agg"]["a"]["count"]) is int
              and type(s0["agg"]["a"]["sum"]) is Decimal and type(s0["agg"]["si"]) is int
              and s0["agg"]["av"] == Decimal("6172839450617283945061728394506172840")
              and type(s0["agg"]["av"]) is Decimal, f"scale-0 agg {s0['agg']}")
        check(type(z.query(f"SELECT d FROM '{src0}'", decimal=False)[0]["d"]) is int, "scale-0 opt-out")


def test_threads(z):
    # Small budgets, so the calls run at once within the context's ceiling.
    reqs = [
        {"inputs": [FIX], "aggregate": "count(*) AS n, sum(a) AS s", "max_memory": "16MB"},
        {"inputs": [FIX, FIX], "aggregate": "count(*) AS n", "group_by": "b", "filter": "a < 3", "max_memory": "16MB"},
        {"inputs": [FIX], "aggregate": "max(b) AS m", "filter": "a < 1000", "max_memory": "16MB"},
    ]
    want = [z.run(r)["agg"] for r in reqs]
    with ThreadPoolExecutor(16) as ex:
        got = list(ex.map(lambda i: z.run(reqs[i % 3])["agg"], range(480)))
    bad = [i for i, g in enumerate(got) if g != want[i % 3]]
    check(not bad, f"{len(bad)} of {len(got)} concurrent answers differ")


def test_service(z):
    server = zpq_service.make_server(z)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        with urllib.request.urlopen(base + "/health") as resp:
            check(json.load(resp) == {"ok": True}, "health")

        def post(body):
            req = urllib.request.Request(base + "/query", data=json.dumps(body).encode(), method="POST")
            try:
                with urllib.request.urlopen(req) as resp:
                    return resp.status, json.load(resp)
            except urllib.error.HTTPError as e:
                return e.code, json.load(e)

        status, body = post({"inputs": [FIX], "aggregate": "count(*) AS n"})
        check(status == 200 and body["agg"] == {"n": 48000}, f"POST /query {status} {body}")
        status, body = post({"sql": f"SELECT count(*) AS n FROM '{FIX}'"})
        check(status == 200 and body["ndjson"] == '{"n":48000}\n', "SQL POST")
        status, body = post({"inputs": ["no/such.parquet"], "aggregate": "count(*) AS n"})
        check(status == 400 and body["error"] == "engine", f"error POST {status} {body}")
        with ThreadPoolExecutor(8) as ex:
            results = list(ex.map(lambda _: post({"inputs": [FIX], "aggregate": "sum(a) AS s"}), range(64)))
        check(all(s == 200 and b["agg"] == {"s": 1151976000} for s, b in results), "concurrent POSTs")
    finally:
        server.shutdown()
        server.end_reads()
        server.server_close()


SLOW = {"inputs": [FIX] * 200, "aggregate": "sum(a) AS s"}
SLOW_SUM = 200 * 1151976000


def wait_inflight(z, timeout=10):
    """Until a call on `z` is inside zpq_run."""
    end = time.monotonic() + timeout
    while z._inflight == 0:
        if time.monotonic() > end:
            raise TimeoutError("no call in flight")
        time.sleep(0.001)


@contextmanager
def admitted_slow():
    """Hold SLOW at an input GET, which is reached only after its budget is admitted."""
    admitted, release = threading.Event(), threading.Event()
    body = Path(FIX).read_bytes()

    class Input(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def do_GET(self):
            admitted.set()
            if not release.wait(10):
                self.send_error(504, "test did not release input")
                return
            r = self.headers["Range"].removeprefix("bytes=")
            if r.startswith("-"):
                lo, hi = max(0, len(body) - int(r[1:])), len(body) - 1
            else:
                lo, hi = map(int, r.split("-"))
                hi = min(hi, len(body) - 1)
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {lo}-{hi}/{len(body)}")
            self.send_header("Content-Length", str(hi - lo + 1))
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            self.wfile.write(body[lo:hi + 1])

    server = ThreadingHTTPServer(("127.0.0.1", 0), Input)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    env = {"S3_ENDPOINT_URL": f"http://127.0.0.1:{server.server_port}",
           "S3_NO_SIGN_REQUEST": "1", "AWS_REGION": "us-east-1"}
    try:
        with patch.dict(os.environ, env):
            yield {**SLOW, "inputs": ["s3://test/mixed.parquet", *SLOW["inputs"][1:]]}, admitted, release
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join()


def wait_running(admitted, timeout=10):
    # WaitTimeout also occurs behind an earlier queued waiter with no reserved budget; it cannot prove admission.
    if not admitted.wait(timeout):
        raise TimeoutError("SLOW did not reach its post-admission input GET")


def release_when_closing(z, release):
    def unblock():
        end = time.monotonic() + 10
        with z._cond:
            while not z._closing and time.monotonic() < end:
                z._cond.wait(0.01)
            # close has called shutdown and released the condition while waiting for this admitted call.
            check(z._closing and z._inflight > 0, "close did not wait for the admitted call")
        release.set()

    t = threading.Thread(target=unblock)
    t.start()
    return t


def test_close_drains():
    """close() waits for an admitted call, then refuses new ones; the context is never freed under a query."""
    with admitted_slow() as (request, admitted, release):
        z = zpq.Zpq()
        done = {}

        def slow():
            done["agg"] = z.run(request)["agg"]

        t = threading.Thread(target=slow)
        t.start()
        try:
            wait_running(admitted)
            unblock = release_when_closing(z, release)
            z.close()
            unblock.join()
        finally:
            release.set()
            t.join()
            z.close()
        check(done.get("agg") == {"s": SLOW_SUM}, f"in-flight call answered {done}")
        try:
            z.run({"inputs": [FIX], "aggregate": "count(*) AS n"})
            check(False, "run after close did not raise")
        except zpq.ZpqClosed:
            pass
        z.close()  # idempotent


def test_close_ends_waiters():
    """close() ends a query waiting for its memory budget with ZpqClosed; the admitted query answers."""
    with admitted_slow() as (request, admitted, release):
        z = zpq.Zpq(max_memory="64MB")
        got = {}

        def call(name, query):
            try:
                got[name] = z.run(query)["agg"]
            except zpq.ZpqClosed as e:
                got[name] = e

        running = threading.Thread(target=call, args=("running", request))
        running.start()
        waiting = None
        try:
            wait_running(admitted)
            waiting = threading.Thread(target=call, args=("waiting", SLOW))
            waiting.start()
            end = time.monotonic() + 10
            while z._inflight < 2 and time.monotonic() < end:
                time.sleep(0.001)
            check(z._inflight == 2, "second call did not enter the binding")
            unblock = release_when_closing(z, release)
            z.close()
            unblock.join()
        finally:
            release.set()
            running.join()
            if waiting is not None:
                waiting.join()
            z.close()
        check(got.get("running") == {"s": SLOW_SUM}, f"running call answered {got.get('running')}")
        check(isinstance(got.get("waiting"), zpq.ZpqClosed), f"waiting call: {got.get('waiting')!r}")


def serve(z):
    server = zpq_service.make_server(z)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"


def post(base, body):
    req = urllib.request.Request(base + "/query", data=json.dumps(body).encode(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, json.load(resp)
    except urllib.error.HTTPError as e:
        return e.code, json.load(e)


def request_threads():
    return [t for t in threading.enumerate() if "process_request_thread" in t.name and t.is_alive()]


def test_service_shutdown():
    """Stopping the service while a query runs and a keep-alive connection idles: the query answers, the idle
    connection is ended, every request thread is joined, and only then is the context freed."""
    z = zpq.Zpq()
    server, base = serve(z)
    # A keep-alive client that answered once and now idles: stop must not wait on it.
    idle = socket.create_connection(server.server_address, timeout=30)
    body = json.dumps({"inputs": [FIX], "aggregate": "count(*) AS n"}).encode()
    idle.sendall(b"POST /query HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n%s" % (len(body), body))
    check(b" 200 " in idle.recv(65536), "keep-alive request")
    got = {}
    t = threading.Thread(target=lambda: got.update(r=post(base, SLOW)))
    t.start()
    wait_inflight(z)
    # What the context's close sees: no request thread may still be running.
    real_close = z.close
    seen = {}

    def close_checked():
        seen["alive"] = len(request_threads())
        real_close()

    z.close = close_checked
    began = time.monotonic()
    server.shutdown()
    zpq_service.stop(server, z)
    took = time.monotonic() - began
    t.join()
    idle.close()
    status, resp = got.get("r", (None, None))
    check(status == 200 and resp["agg"] == {"s": SLOW_SUM}, f"query across shutdown: {status} {resp}")
    check(seen.get("alive") == 0, f"{seen.get('alive')} request thread(s) alive when the context closed")
    check(not request_threads(), "request threads left after stop")
    check(took < 10, f"stop took {took:.1f}s with an idle keep-alive connection")
    check(z._ctx is None, "context not freed by stop")

    # A handler still running after the context closed answers 503, not a crash.
    server2, base2 = serve(z)
    status, body = post(base2, {"inputs": [FIX], "aggregate": "count(*) AS n"})
    check(status == 503 and body == {"error": "shutting_down"}, f"after close: {status} {body}")
    server2.shutdown()
    zpq_service.stop(server2, z)


def raw(port, head):
    """Send `head` (request line and headers, no body) and return the response's status, body and whether the server
    then closed the connection."""
    with socket.create_connection(("127.0.0.1", port), timeout=10) as sk:
        sk.sendall(head.encode())
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = sk.recv(65536)
            if not chunk:
                break
            data += chunk
        head_b, _, rest = data.partition(b"\r\n\r\n")
        status = int(head_b.split(b" ")[1]) if head_b else None
        length = 0
        for line in head_b.split(b"\r\n")[1:]:
            k, _, v = line.partition(b":")
            if k.strip().lower() == b"content-length":
                length = int(v)
        while len(rest) < length:
            chunk = sk.recv(65536)
            if not chunk:
                break
            rest += chunk
        try:
            closed = sk.recv(1) == b""
        except socket.timeout:
            closed = False
        return status, json.loads(rest[:length] or b"null"), closed


def test_bad_lengths(z):
    server, _ = serve(z)
    port = server.server_address[1]
    try:
        for value, want in [("abc", 400), ("-1", 400), (" 12x", 400), (str(zpq_service.MAX_BODY + 1), 413)]:
            status, body, closed = raw(port, f"POST /query HTTP/1.1\r\nHost: x\r\nContent-Length: {value}\r\n\r\n")
            check(status == want and isinstance(body, dict) and "error" in body and closed,
                  f"Content-Length {value!r}: {status} {body} closed={closed}")
        status, body, closed = raw(port, "POST /query HTTP/1.1\r\nHost: x\r\n\r\n")
        check(status == 411 and closed, f"no Content-Length: {status} {body} closed={closed}")
    finally:
        server.shutdown()
        server.end_reads()
        server.server_close()


def test_bloom_options(z):
    """Bloom options through the library write the CLI's bytes; tools/check/cases/bloom.json checks the CLI's."""
    import subprocess

    import pyarrow as pa
    import pyarrow.parquet as pq

    with tempfile.TemporaryDirectory() as d:
        src = os.path.join(d, "src.parquet")
        low = [(i % 64) * 2 for i in range(4000)]
        pq.write_table(pa.table({"low": pa.array(low, pa.int32()), "high": pa.array(range(4000), pa.int64())}), src,
                       row_group_size=1000)
        for auto, explicit, flags in [(True, False, []), (False, False, ["--no-bloom-filters"]),
                                      (False, True, ["--no-bloom-filters", "--bloom-filter", "high:0.001"])]:
            lib_out, cli_out = os.path.join(d, "lib.parquet"), os.path.join(d, "cli.parquet")
            request = {"inputs": [src], "select": "low,high", "output_url": lib_out, "bloom_filters_enabled": auto}
            if explicit:
                request["bloom_filters"] = ["high:0.001"]
            z.run(request)
            subprocess.run([os.environ.get("ZPQ_BIN", "zig-out/bin/zpq"), "query", src, "--select", "low,high", "-o",
                            cli_out, *flags], check=True)
            check(Path(lib_out).read_bytes() == Path(cli_out).read_bytes(), f"Bloom options {flags} differ from CLI")


def test_binary_text(z):
    """The library prints binary, UUID and annotated byte columns as the CLI does, and names invalid UTF-8; the CLI's
    own output is pinned in tools/check/cases/cli_formats.json."""
    import struct
    import subprocess

    import pyarrow as pa
    import pyarrow.parquet as pq

    cli = os.environ.get("ZPQ_BIN", "zig-out/bin/zpq")
    with tempfile.TemporaryDirectory() as d:
        blob, uuids, invalid = (os.path.join(d, n) for n in ("blob.parquet", "uuid.parquet", "invalid.parquet"))
        pq.write_table(pa.table({"v": [b"\xff", b"\xfe", b"ASCII", b"", None, b"\x00", b"/w==", "\u00e9".encode(),
                                       bytes(range(256)) * 13]}), blob)
        pq.write_table(pa.table({"v": pa.array([b"\xff" * 16, None, bytes(range(16))], pa.binary(16)).cast(pa.uuid())}),
                       uuids)
        fixtures = "ci/fixtures/parquet"
        for src in (blob, uuids, f"{fixtures}/format_geometry.parquet", f"{fixtures}/format_interval.parquet"):
            for sql in (f"SELECT * FROM '{src}'", f"SELECT min(v) AS lo, max(v) AS hi FROM '{src}'",
                        f"SELECT v, count(*) AS n FROM '{src}' GROUP BY v"):
                want = subprocess.run([cli, "query", "--query", sql], capture_output=True, text=True, check=True).stdout
                check(z.query_raw(sql, parallelism=1) == want, f"library text differs from the CLI's: {sql}")
        bad = pa.Array.from_buffers(pa.string(), 2, [None, pa.py_buffer(struct.pack("<iii", 0, 1, 3)),
                                                     pa.py_buffer(b"\xff\xfe\x80")])
        pq.write_table(pa.table({"bad": bad}), invalid)
        for sql in (f"SELECT * FROM '{invalid}'", f"SELECT max(bad) AS hi FROM '{invalid}'"):
            r = z.run({"sql": sql}, check=False)
            check(r.get("reason") == "InvalidUtf8" and r.get("column") == "bad", f"invalid UTF-8 {r}")


def test_options_and_budget():
    for bad in ({"max_memory": "lots"}, {"budget_wait_ms": -1}):
        try:
            zpq.Zpq(**bad)
            check(False, f"bad options accepted: {bad}")
        except ValueError as e:
            check("invalid_argument" in str(e), f"refusal names its reason: {e}")
    with zpq.Zpq(max_memory="64MB", budget_wait_ms=5000) as z:
        try:
            z.run({"inputs": [FIX], "aggregate": "count(*) AS n", "max_memory": "65MB"})
            check(False, "a budget over the ceiling ran")
        except zpq.ZpqError as e:
            check(e.response.get("reason") == "OverCeiling", f"over ceiling {e.response}")
        # Budgets adding up to more than the ceiling: each waits its turn, all complete.
        req = {"inputs": [FIX], "aggregate": "count(*) AS n", "max_memory": "40MB"}
        with ThreadPoolExecutor(8) as ex:
            got = list(ex.map(lambda _: z.run(req), range(16)))
        check(all(r["agg"] == {"n": 48000} for r in got), "queued budgets all answered")


def main():
    test_options_and_budget()
    with zpq.Zpq(max_memory="512MB") as z:
        test_binding(z)
        test_boolean_answers(z)
        test_decimal_exact(z)
        test_threads(z)
        test_service(z)
        test_bad_lengths(z)
        test_bloom_options(z)
        test_binary_text(z)
    test_close_drains()
    test_close_ends_waiters()
    test_service_shutdown()
    if failures:
        print(f"{len(failures)} failure(s)", file=sys.stderr)
        sys.exit(1)
    print("libzpq Python binding and service: all checks passed")


if __name__ == "__main__":
    main()
