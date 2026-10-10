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
        check(w["rows_kept"] == 10, f"write {w}")
        back = z.run({"inputs": [out], "aggregate": "sum(x) AS s"})
        check(back["agg"] == {"s": 90}, f"read back {back}")


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


def test_close_drains():
    """close() waits for a call in flight, then refuses new ones; the context is never freed under a query."""
    z = zpq.Zpq()
    done = {}

    def slow():
        done["agg"] = z.run(SLOW)["agg"]
        done["t"] = time.monotonic()

    t = threading.Thread(target=slow)
    t.start()
    wait_inflight(z)
    z.close()
    closed_at = time.monotonic()
    t.join()
    check(done.get("agg") == {"s": SLOW_SUM}, f"in-flight call answered {done}")
    check(done.get("t", closed_at + 1) <= closed_at, "close returned before the call in flight finished")
    try:
        z.run({"inputs": [FIX], "aggregate": "count(*) AS n"})
        check(False, "run after close did not raise")
    except zpq.ZpqClosed:
        pass
    z.close()  # idempotent


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
        test_threads(z)
        test_service(z)
        test_bad_lengths(z)
    test_close_drains()
    test_service_shutdown()
    if failures:
        print(f"{len(failures)} failure(s)", file=sys.stderr)
        sys.exit(1)
    print("libzpq Python binding and service: all checks passed")


if __name__ == "__main__":
    main()
