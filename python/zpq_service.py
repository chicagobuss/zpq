"""A long-running zpq service: one process, one libzpq context, a stdlib HTTP server.

    python zpq_service.py --port 8099 [--host 127.0.0.1] [--max-memory 4GB]
    curl -s localhost:8099/query -d '{"inputs": ["data.parquet"], "aggregate": "count(*) AS n"}'

POST /query takes the zpq Lambda's request as JSON and returns its response: 200 for an answer, 400 for an
{"error": ...} response (a bad request or a failed query). GET /health returns {"ok": true}. Requests are served on
threads of their own, over one shared context, so its S3 connections and footer cache stay warm between them.
"""

import argparse
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import zpq

MAX_BODY = 16 << 20


def make_handler(z):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            pass

        def _send(self, status, body):
            data = body.encode() if isinstance(body, str) else body
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path == "/health":
                self._send(200, '{"ok":true}')
            else:
                self._send(404, '{"error":"not_found"}')

        def do_POST(self):
            if self.path != "/query":
                self._send(404, '{"error":"not_found"}')
                return
            length = int(self.headers.get("Content-Length") or 0)
            if length > MAX_BODY:
                self._send(413, '{"error":"request_too_large"}')
                return
            body = self.rfile.read(length)
            try:
                rc, text = z.run_raw(body)
            except (MemoryError, ValueError) as e:
                self._send(500, json.dumps({"error": "service", "reason": str(e)}))
                return
            self._send(400 if rc == zpq.ERROR_RESPONSE else 200, text)

    return Handler


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8099)
    ap.add_argument("--max-memory", help="each query's budget when its request names none, e.g. 4GB")
    args = ap.parse_args(argv)
    z = zpq.Zpq(max_memory=args.max_memory)
    server = ThreadingHTTPServer((args.host, args.port), make_handler(z))
    server.daemon_threads = True
    print(f"zpq service on http://{args.host}:{server.server_address[1]}", file=sys.stderr, flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        z.close()


if __name__ == "__main__":
    main()
