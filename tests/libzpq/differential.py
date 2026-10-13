"""libzpq against the CLI and the Lambda on the differential corpus (tools/differential.py), byte for byte: every
filter x projection and filter x select written by all three must be the same file, and every filter x aggregate
answer must have identical bytes, including numeric text and key order. The CLI and the Lambda are the built
zig-out/bin binaries; the Lambda runs behind a
fake runtime API in this process. Run from the repo root with ZPQ_LIB pointing at libzpq.so (tests/libzpq/run.sh does).
"""

import http.server
import json
import os
import subprocess
import sys
import tempfile
import threading

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "tools"))
sys.path.insert(0, os.path.join(HERE, "..", "..", "python"))
import differential as corpus  # noqa: E402
import duckdb  # noqa: E402
import zpq  # noqa: E402

CLI = "zig-out/bin/zpq"
LAMBDA = "zig-out/bin/zpq-lambda"


def agg_text(response):
    """The response's `"agg":` member as it was written, from its opening brace to the matching close."""
    start = response.index('"agg":') + len('"agg":')
    depth = 0
    for i in range(start, len(response)):
        if response[i] in "{[":
            depth += 1
        elif response[i] in "}]":
            depth -= 1
            if depth == 0:
                return response[start:i + 1]
    raise ValueError(f"unterminated agg in {response[:200]}")


class Lambda:
    """zpq-lambda behind a fake runtime API: `invoke(event)` hands it one event and returns its response."""

    def __init__(self):
        self.lock = threading.Condition()
        self.event = None
        self.response = None
        outer = self

        class Runtime(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def do_GET(self):
                with outer.lock:
                    while outer.event is None:
                        outer.lock.wait()
                    body, outer.event = outer.event, None
                self.send_response(200)
                self.send_header("Lambda-Runtime-Aws-Request-Id", "req")
                self.send_header("Lambda-Runtime-Deadline-Ms", "9999999999999")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                self.send_response(202)
                self.send_header("Content-Length", "0")
                self.end_headers()
                with outer.lock:
                    outer.response = (self.path, body.decode())
                    outer.lock.notify_all()

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Runtime)
        self.server.daemon_threads = True
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        env = {"PATH": "/usr/bin:/bin", "AWS_LAMBDA_RUNTIME_API": f"127.0.0.1:{self.server.server_port}"}
        self.proc = subprocess.Popen([LAMBDA], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def invoke(self, event):
        with self.lock:
            self.event = json.dumps(event).encode()
            self.response = None
            self.lock.notify_all()
            while self.response is None:
                if not self.lock.wait(timeout=120):
                    raise TimeoutError(f"lambda gave no response to {event}")
            path, body = self.response
        if not path.endswith("/response"):
            raise RuntimeError(f"lambda error for {event}: {body[:200]}")
        return body

    def close(self):
        self.proc.kill()
        self.proc.wait()
        self.server.shutdown()


def main():
    failures = []
    checks = 0
    tmp = tempfile.mkdtemp()
    fixture = os.path.join(tmp, "diff_fixture.parquet")
    corpus.gen_fixture(duckdb.connect(), fixture)
    lam = Lambda()
    z = zpq.Zpq()

    def read(path):
        with open(path, "rb") as f:
            return f.read()

    def write_case(label, req, cli_args):
        nonlocal checks
        checks += 1
        outs = {k: os.path.join(tmp, f"{k}.parquet") for k in ("cli", "lib", "lambda")}
        for p in outs.values():
            if os.path.exists(p):
                os.remove(p)
        r = subprocess.run([CLI, "query", fixture, "-o", outs["cli"]] + cli_args, capture_output=True, text=True)
        cli_err = r.returncode != 0
        lib = z.run(dict(req, output_url=outs["lib"]), check=False)
        lam_resp = json.loads(lam.invoke(dict(req, output_url=outs["lambda"])))
        if cli_err or "error" in lib or "error" in lam_resp:
            # A refusal must be a refusal everywhere.
            if not (cli_err and "error" in lib and "error" in lam_resp):
                refusals = f"cli rc {r.returncode}, libzpq {lib.get('error')}, lambda {lam_resp.get('error')}"
                failures.append(f"{label}: {refusals}")
            return
        assert r.stdout == "", r.stdout
        files = {k: read(p) for k, p in outs.items()}
        if not files["cli"] == files["lib"] == files["lambda"]:
            failures.append(f"{label}: written files differ ({', '.join(f'{k} {len(v)} B' for k, v in files.items())})")

    def agg_case(label, req, cli_args):
        nonlocal checks
        checks += 1
        r = subprocess.run([CLI, "query", fixture] + cli_args, capture_output=True, text=True)
        lib = z.run_raw(json.dumps(req))
        lam_resp = lam.invoke(req)
        if r.returncode != 0:
            if not (lib.startswith('{"error"') and lam_resp.startswith('{"error"')):
                failures.append(f"{label}: the CLI refused ({r.stderr.strip()[:60]}) but libzpq or the lambda answered")
            return
        got = {"cli": r.stdout.rstrip("\n"),
               "lib": agg_text(lib),
               "lambda": agg_text(lam_resp)}
        if not got["cli"] == got["lib"] == got["lambda"]:
            failures.append(f"{label}: agg differs: {got}")

    try:
        for flabel, zf, _ in corpus.FILTERS:
            filt_req = {} if zf is None else {"filter": zf}
            filt_cli = [] if zf is None else ["--filter", zf]
            for plabel, cols in corpus.PROJECTIONS:
                req = dict({"inputs": [fixture]}, **filt_req, **({} if cols is None else {"columns": cols}))
                cli = filt_cli + ([] if cols is None else ["--columns", ",".join(cols)])
                write_case(f"{flabel} x {plabel}", req, cli)
            for slabel, zs, _ in corpus.SELECTS:
                write_case(f"{flabel} x select {slabel}", dict({"inputs": [fixture], "select": zs}, **filt_req),
                           filt_cli + ["--select", zs])
            for alabel, za, _ in corpus.AGGS:
                agg_case(f"{flabel} x agg {alabel}", dict({"inputs": [fixture], "aggregate": za}, **filt_req),
                         filt_cli + ["--aggregate", za])
    finally:
        z.close()
        lam.close()
    for f in failures:
        print("FAIL", f, file=sys.stderr)
    if failures:
        print(f"{len(failures)} of {checks} differ", file=sys.stderr)
        sys.exit(1)
    print(f"libzpq differential: {checks}/{checks} files byte-identical; exact answers agree with CLI and Lambda")


if __name__ == "__main__":
    main()
