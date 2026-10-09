"""A long-running zpq service: one process, one libzpq context, a stdlib HTTP server.

    python zpq_service.py --port 8099 [--host 127.0.0.1] [--max-memory 4GB]
    curl -s localhost:8099/query -d '{"inputs": ["data.parquet"], "aggregate": "count(*) AS n"}'

POST /query takes the zpq Lambda's request as JSON and returns its response: 200 for an answer, 400 for an
{"error": ...} response (a bad request or a failed query). GET /health returns {"ok": true}. Requests are served on
threads of their own, over one shared context, so its S3 connections and footer cache stay warm between them. On
shutdown it stops accepting, ends idle connections, joins every request thread (queries in flight finish and
answer), and only then frees the context.
"""

import argparse
import json
import socket
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import zpq

MAX_BODY = 16 << 20


class Server(ThreadingHTTPServer):
    """A ThreadingHTTPServer whose request threads are joined at close, and which knows its open connections, so
    `stop` can end idle keep-alive ones rather than wait on them."""

    daemon_threads = False
    block_on_close = True

    def __init__(self, *args, **kwargs):
        self._conns = set()
        self._conns_lock = threading.Lock()
        super().__init__(*args, **kwargs)

    def process_request(self, request, client_address):
        with self._conns_lock:
            self._conns.add(request)
        super().process_request(request, client_address)

    def shutdown_request(self, request):
        with self._conns_lock:
            self._conns.discard(request)
        super().shutdown_request(request)

    def end_reads(self):
        """Half-close every open connection for reading: a handler waiting for its next request sees EOF and ends,
        while one running a query still writes its answer."""
        with self._conns_lock:
            conns = list(self._conns)
        for c in conns:
            try:
                c.shutdown(socket.SHUT_RD)
            except OSError:
                pass


def make_server(z, host="127.0.0.1", port=0):
    return Server((host, port), make_handler(z))


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

        def _refuse(self, status, error):
            """Answer without reading the body: the connection then closes, leaving no unread bytes behind."""
            self.close_connection = True
            self.send_response(status)
            data = json.dumps({"error": error}).encode()
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(data)

        def do_POST(self):
            if self.path != "/query":
                self._refuse(404, "not_found")
                return
            text = self.headers.get("Content-Length")
            if text is None:
                self._refuse(411, "length_required")
                return
            try:
                length = int(text)
            except ValueError:
                length = -1
            if length < 0 or not text.strip().isdigit():
                self._refuse(400, "bad_content_length")
                return
            if length > MAX_BODY:
                self._refuse(413, "request_too_large")
                return
            body = self.rfile.read(length)
            try:
                rc, text = z.run_raw(body)
            except zpq.ZpqClosed:
                self._refuse(503, "shutting_down")
                return
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
    server = make_server(z, args.host, args.port)
    print(f"zpq service on http://{args.host}:{server.server_address[1]}", file=sys.stderr, flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop(server, z)


def stop(server, z):
    """Stop accepting, let every request finish, then free the context. `serve_forever` must have returned (or never
    run): call `server.shutdown()` first when it runs on another thread. Idle keep-alive connections are ended rather
    than waited on; `server_close` joins every request thread, so the queries in flight answer before `z.close`
    (which would also wait for them, and answers 503 to any call after it)."""
    server.end_reads()
    server.server_close()
    z.close()


if __name__ == "__main__":
    main()
