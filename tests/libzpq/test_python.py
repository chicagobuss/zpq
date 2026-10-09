"""libzpq's Python binding (python/zpq.py) and service (python/zpq_service.py), end to end. Run from the repo root
with ZPQ_LIB pointing at libzpq.so (tests/libzpq/run.sh does)."""

import json
import os
import sys
import tempfile
import threading
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
    reqs = [
        {"inputs": [FIX], "aggregate": "count(*) AS n, sum(a) AS s"},
        {"inputs": [FIX, FIX], "aggregate": "count(*) AS n", "group_by": "b", "filter": "a < 3"},
        {"inputs": [FIX], "aggregate": "max(b) AS m", "filter": "a < 1000"},
    ]
    want = [z.run(r)["agg"] for r in reqs]
    with ThreadPoolExecutor(16) as ex:
        got = list(ex.map(lambda i: z.run(reqs[i % 3])["agg"], range(480)))
    bad = [i for i, g in enumerate(got) if g != want[i % 3]]
    check(not bad, f"{len(bad)} of {len(got)} concurrent answers differ")


def test_service(z):
    server = zpq_service.ThreadingHTTPServer(("127.0.0.1", 0), zpq_service.make_handler(z))
    server.daemon_threads = True
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
        server.server_close()


def main():
    try:
        zpq.Zpq(max_memory="lots")
        check(False, "bad options accepted")
    except ValueError:
        pass
    with zpq.Zpq(max_memory="512MB") as z:
        test_binding(z)
        test_threads(z)
        test_service(z)
    if failures:
        print(f"{len(failures)} failure(s)", file=sys.stderr)
        sys.exit(1)
    print("libzpq Python binding and service: all checks passed")


if __name__ == "__main__":
    main()
