#!/usr/bin/env python3
"""Fan one zpq aggregate out over N zpq-lambda workers and merge their answers.

The coordinator makes no call before the scatter. It cuts every input's row groups into N contiguous runs, as
fractions of each input that the workers resolve against their own footers, and sends each worker a
`partial_aggregate` request (PLAN-V1.md, docs/lambda_requests.md). Each worker prepares the query itself, reads its
inputs' footers, and answers its partial with the merge functions, a digest of its plan and a fingerprint of its
inputs' schema. The coordinator merges only when every worker answered the same of all three, and otherwise refuses
naming the inputs that differ. Supplied row-group counts only balance the runs: a wrong count moves work between
workers, never rows out of the answer. COUNT and SUM add, MIN and MAX fold, and explicit AVG partial states are
divided after the merge. With `group_by` the merge is per group. ORDER BY and LIMIT apply to the merged rows, never
to a worker's. Anything whose parts cannot be merged (COUNT(DISTINCT), percentiles and other holistic aggregates,
window functions, expressions over aggregates) each worker refuses while parsing, before it opens an input.

An input that is a `*` pattern is expanded by a worker, so such a query takes the structured plan v1 path instead:
negotiate (on every query, or once per coordinator process for a published worker version), prepare every input,
then scatter the prepared plan.

    fanout.py --function zpq-worker --workers 32 --input s3://b/lineitem/ --aggregate "sum(x) AS s" ...

The same module is a coordinator Lambda handler (`fanout.handler`); the event names the worker function and the
query, as the CLI flags do (see `handler`). Pure stdlib, plus botocore's SigV4 (in the Lambda Python runtime) to
sign invokes. A passed `client` is the boto3-shaped double the tests use.
"""
import argparse
import base64
import concurrent.futures as cf
import decimal
import json
import math
import queue
import re
import socket
import ssl
import sys
import threading
import time
from urllib.parse import quote, urlsplit

PLAN_VERSION = 1
PARTIAL_VERSION = 1
MERGEABLE = ("count", "sum", "min", "max", "avg")
REPORT_RE = re.compile(r"Duration: ([\d.]+) ms\s+Billed Duration: (\d+) ms\s+Memory Size: (\d+) MB\s+"
                       r"Max Memory Used: (\d+) MB(?:\s+Init Duration: ([\d.]+) ms)?")


class Refused(ValueError):
    """The query has a part whose per-worker answers cannot be merged."""


class WorkerFailed(RuntimeError):
    """A worker answered an error; the merged answer would be incomplete."""


# ---- Coordinator output-order option (never sent to a worker as SQL) ----

def split_top(text):
    """Split at commas outside parentheses and quotes."""
    parts, depth, quote, start = [], 0, None, 0
    for i, ch in enumerate(text):
        if quote:
            quote = None if ch == quote else quote
        elif ch in "'\"":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(text[start:i].strip())
            start = i + 1
    parts.append(text[start:].strip())
    return [p for p in parts if p]


# ---- Splitting row groups over workers ----

def partial_runs(counts, workers):
    """The runs `split` makes of `counts`, as (input index, start, end, of) fractions of each input: `of` is the
    count the run was cut from, so a worker reads row groups floor(start·n/of) up to floor(end·n/of) of the n its
    footer holds, and the runs of one input cover it with no gap or overlap whatever n is. A count of 0 is one unit
    read whole, so every input reaches a worker that opens and checks its footer."""
    units = [max(1, c) for c in counts]
    return [[(i, s, e, units[i]) for i, s, e in run] for run in split(units, workers)]


def split(counts, workers):
    """Cut the row groups of all inputs, in order, into at most `workers` contiguous runs of near-equal length.
    Each run is [(input index, start, end)]: row groups start up to but not including end of that input."""
    units = [(i, rg) for i, n in enumerate(counts) for rg in range(n)]
    n = min(workers, len(units))
    runs = []
    for k in range(n):
        run = []
        for i, rg in units[k * len(units) // n:(k + 1) * len(units) // n]:
            if run and run[-1][0] == i:
                run[-1] = (i, run[-1][1], rg + 1)
            else:
                run.append((i, rg, rg + 1))
        runs.append(run)
    return runs


# ---- Merging ----

def add(a, b):
    if a is None or b is None:
        return b if a is None else a
    if isinstance(a, float) or isinstance(b, float):  # NaN or an infinity: no Decimal holds it
        return float(a) + float(b)
    return a + b


def fold(acc, part, aggs):
    for j, (fn, _, _) in enumerate(aggs):
        k, v = f"_a{j}", part.get(f"_a{j}")
        if fn == "avg":
            s = acc.get(k) or {"sum": None, "count": 0}
            acc[k] = {"sum": add(s["sum"], v["sum"]), "count": s["count"] + v["count"]}
        elif fn in ("count", "sum"):
            acc[k] = add(acc.get(k), v)
        elif v is not None:
            have = acc.get(k)
            acc[k] = v if have is None else (min(have, v) if fn == "min" else max(have, v))
        else:
            acc.setdefault(k, None)


def finish(acc, aggs, group):
    row = {name: acc[f"_g{j}"] for j, (_, name) in enumerate(group)}
    for j, (fn, _, name) in enumerate(aggs):
        v = acc.get(f"_a{j}")
        if fn == "avg":
            v = None if not v or v["count"] == 0 or v["sum"] is None else float(v["sum"]) / v["count"]
        row[name] = v
    return row


def merge(answers, aggs, group):
    """Merge the workers' `agg` members: one object each without group_by, else an array of group rows."""
    if not group:
        acc = {}
        for a in answers:
            fold(acc, a, aggs)
        return finish(acc, aggs, group)
    groups = {}
    for a in answers:
        for row in a:
            key = tuple(json.dumps(row[f"_g{j}"], default=str) for j in range(len(group)))
            fold(groups.setdefault(key, {f"_g{j}": row[f"_g{j}"] for j in range(len(group))}), row, aggs)
    return [finish(acc, aggs, group) for acc in groups.values()]


def order(rows, order_by, limit):
    """ORDER BY `name [ASC|DESC], ...` over the output names (NULLs last either way), then LIMIT."""
    for item in reversed(split_top(order_by or "")):
        m = re.match(r'("[^"]+"|.+?)(?:\s+(ASC|DESC))?$', item.strip(), re.I)
        name, desc = m.group(1).strip('"'), (m.group(2) or "").upper() == "DESC"
        if rows and name not in rows[0]:
            raise Refused(f"ORDER BY {name!r}: not an output column ({', '.join(rows[0])})")
        present = sorted((r for r in rows if r[name] is not None), key=lambda r: r[name], reverse=desc)
        rows = present + [r for r in rows if r[name] is None]
    return rows if limit is None else rows[:limit]


# ---- Invoking workers ----

def pinned_version(function, region=None):
    """(account or None, region, name, version) when `function` names an immutable published version (`name:7`,
    or a function ARN ending in `:7`), else None: an alias, `$LATEST` or an unqualified name can change code
    between two calls."""
    head, sep, tail = function.rpartition(":function:")
    account = None
    if sep:  # arn:aws:lambda:REGION:ACCOUNT:function:NAME[:QUALIFIER], or ACCOUNT:function:NAME[:QUALIFIER]
        parts = head.split(":")
        account = parts[-1] if parts[-1].isdigit() else None
        if len(parts) >= 5:
            region = parts[3] or region
        function = tail
    name, _, qualifier = function.partition(":")
    if not qualifier.isdigit():
        return None
    return account, region, name, qualifier


# One Lambda client per (region, pool) for the life of the process, so a warm coordinator keeps its connections.
_CLIENTS = {}
_EXECUTOR = None
_EXECUTOR_N = 0


def cached_client(region, pool):
    key = (region, pool)
    found = _CLIENTS.get(key)
    if found is not None:
        return found
    import boto3
    from botocore.config import Config
    found = boto3.client("lambda", region_name=region, config=Config(
        read_timeout=900, connect_timeout=10, retries={"total_max_attempts": 1}, max_pool_connections=pool))
    _CLIENTS[key] = found
    return found


def executor(n):
    """A thread pool reused across queries, grown when a query needs more workers than the last."""
    global _EXECUTOR, _EXECUTOR_N
    if _EXECUTOR is None or n > _EXECUTOR_N:
        old = _EXECUTOR
        _EXECUTOR_N = max(n, _EXECUTOR_N, 256)
        _EXECUTOR = cf.ThreadPoolExecutor(_EXECUTOR_N)
        if old is not None:
            old.shutdown(wait=False)
    return _EXECUTOR


def _worker_stat(resp, stats):
    """What one worker cost, including the phase split the engine already returns (`wall_ms`, `worker_ms`)."""
    item = dict(stats, total_ms=resp.get("total_ms"), rows_in=resp.get("rows_in"), bytes_in=resp.get("bytes_in"),
                row_groups_in=resp.get("row_groups_in"), files_in=resp.get("files_in"))
    if isinstance(resp.get("wall_ms"), dict):
        item["wall_ms"] = resp["wall_ms"]
    if isinstance(resp.get("worker_ms"), dict):
        item["worker_ms"] = resp["worker_ms"]
    return item


def lambda_endpoint(function, region):
    """`(url, region)` of the Invoke API for `function`. A qualifier (`name:7`, an alias, or the qualifier on an
    ARN) is the `Qualifier` query, so a pinned version stays pinned."""
    head, sep, tail = function.rpartition(":function:")
    if sep:
        parts = head.split(":")
        if len(parts) >= 5 and parts[3]:
            region = parts[3]
        function = tail
    name, _, qualifier = function.partition(":")
    url = f"https://lambda.{region}.amazonaws.com/2015-03-31/functions/{quote(name, safe='')}/invocations"
    if qualifier:
        url += "?Qualifier=" + quote(qualifier, safe="")
    return url, region


def _billed_from_body(resp):
    """Duration the worker reports, rounded up to the millisecond Lambda bills, for when the invoke did not ask for
    the REPORT line. Init duration is not in `total_ms`, so a cold start is a few tens of milliseconds short."""
    total = resp.get("total_ms") if isinstance(resp, dict) else None
    if not isinstance(total, (int, float, decimal.Decimal)):
        return {}
    ms = float(total)
    return {"duration_ms": ms, "billed_ms": math.ceil(ms)}


def _read_invoke(status, headers, body):
    """The Invoke API answers 200 with the function's body, and names a handled or unhandled failure in
    `X-Amz-Function-Error`. Anything else failed before the function ran."""
    if status != 200:
        raise WorkerFailed(f"invoke HTTP {status}: {body[:200]!r}")
    try:
        resp = loads(body)
    except json.JSONDecodeError as e:
        raise WorkerFailed(f"invoke returned non-JSON: {body[:200]!r}") from e
    if hasattr(headers, "items"):
        pairs = headers.items()
    else:
        pairs = headers
    err = None
    for k, v in pairs:
        if str(k).lower() == "x-amz-function-error":
            err = v
    return resp, {"function_error": err, **_billed_from_body(resp)}


# One connection pool per host, for the life of the process. A warm coordinator keeps its TLS connections.
_POOLS = {}
_CREDS = None


def _read_http(sock):
    """One HTTP/1.1 response. Lambda's Invoke API sends Content-Length. Returns status, header pairs, body, and
    whether the connection must be dropped."""
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(65536)
        if not chunk:
            raise OSError("connection closed before headers")
        buf += chunk
        if len(buf) > 1 << 20:
            raise WorkerFailed("invoke response headers too large")
    head, rest = buf.split(b"\r\n\r\n", 1)
    lines = head.split(b"\r\n")
    try:
        status = int(lines[0].split(b" ", 2)[1])
    except (IndexError, ValueError) as e:
        raise WorkerFailed(f"invoke bad status line: {lines[0][:80]!r}") from e
    headers, length, close = [], None, False
    for line in lines[1:]:
        key, _, value = line.partition(b":")
        headers.append((key.decode("latin1"), value.strip().decode("latin1")))
        low = key.lower()
        if low == b"content-length":
            length = int(value.strip())
        elif low == b"connection" and value.strip().lower() == b"close":
            close = True
    if length is None:
        raise WorkerFailed("invoke response had no Content-Length")
    while len(rest) < length:
        chunk = sock.recv(length - len(rest))
        if not chunk:
            raise OSError("connection closed mid-body")
        rest += chunk
    if len(rest) > length:
        close = True  # bytes past the body belong to a later response; don't reuse this socket
    return status, headers, rest[:length], close


class _ConnPool:
    """Keep-alive connections. Checkout never holds the lock across a handshake, so a fan of N opens N connections
    at once instead of one after another. urllib3's pool held the GIL for about 1.5 ms per call once N was large,
    which put the last invoke of 128 ~150 ms behind the first."""

    def __init__(self, host, port, tls, size):
        self.host, self.port, self.tls, self.size = host, port, tls, size
        self.idle = queue.LifoQueue()
        self.n = 0
        self.lock = threading.Lock()
        self.ssl = ssl.create_default_context() if tls else None

    def open(self):
        sock = socket.create_connection((self.host, self.port), timeout=10)
        sock.settimeout(900)
        if self.ssl is not None:
            sock = self.ssl.wrap_socket(sock, server_hostname=self.host)
        return sock

    def checkout(self):
        try:
            return self.idle.get_nowait()
        except queue.Empty:
            pass
        with self.lock:
            if self.n < self.size:
                self.n += 1
                create = True
            else:
                create = False
        if not create:
            return self.idle.get(timeout=900)
        try:
            return self.open()
        except Exception:
            with self.lock:
                self.n -= 1
            raise

    def checkin(self, sock):
        self.idle.put(sock)

    def drop(self, sock):
        try:
            sock.close()
        except OSError:
            pass
        with self.lock:
            self.n -= 1

    def close(self):
        while True:
            try:
                self.drop(self.idle.get_nowait())
            except queue.Empty:
                return

    def post(self, path, header_pairs, body):
        lines = [f"POST {path} HTTP/1.1"]
        have_length = False
        for key, value in header_pairs:
            lines.append(f"{key}: {value}")
            if key.lower() == "content-length":
                have_length = True
        if not have_length:
            lines.append(f"Content-Length: {len(body)}")
        lines.append("Connection: keep-alive")
        raw = ("\r\n".join(lines) + "\r\n\r\n").encode() + body
        sock = self.checkout()
        try:
            return self._exchange(sock, raw)
        except OSError:
            # A sandbox that sat frozen comes back to dead connections. One new connection. The first request may
            # already be running on the worker; its answer is the one we discard, and the retry's is the one merged.
            self.drop(sock)
            try:
                sock = self.open()
            except OSError as e:
                raise WorkerFailed(f"invoke connection failed: {e}") from e
            with self.lock:
                self.n += 1
            try:
                return self._exchange(sock, raw)
            except OSError as e:
                self.drop(sock)
                raise WorkerFailed(f"invoke connection failed: {e}") from e

    def _exchange(self, sock, raw):
        sock.sendall(raw)
        status, headers, payload, close = _read_http(sock)
        if close:
            self.drop(sock)
        else:
            self.checkin(sock)
        return status, headers, payload


def _pool(host, port, tls, size):
    key = (host, port, tls)
    found = _POOLS.get(key)
    if found is None or found.size < size:
        if found is not None:
            found.close()
        found = _ConnPool(host, port, tls, size)
        _POOLS[key] = found
    return found


def _credentials():
    """The default credential chain (the Lambda execution role, or the profile in the environment)."""
    global _CREDS
    if _CREDS is None:
        from botocore.session import Session
        found = Session().get_credentials()
        if found is None:
            raise WorkerFailed("no AWS credentials for invoke")
        _CREDS = found
    return _CREDS


class SignedInvoke:
    """`invoke(payload) -> (response, stats)` as one signed POST on a reused connection.

    No `LogType=Tail`: that asks Lambda for the log tail after the function returns, and at N=128 those extra
    reads sit on the coordinator's critical path (the wait past the worker's own duration grows with N). The
    stats fall back to the `total_ms` the worker already returns."""

    def __init__(self, function, region, pool, credentials=None, endpoint=None):
        self.url, self.region = lambda_endpoint(function, region)
        if endpoint:
            # A test server. The path and query stay what Invoke would see.
            path = self.url.split(".amazonaws.com", 1)[1]
            self.url = endpoint.rstrip("/") + path
        self.credentials = credentials
        parts = urlsplit(self.url)
        self._path = parts.path + (("?" + parts.query) if parts.query else "")
        self._pool = _pool(parts.hostname, parts.port or (443 if parts.scheme == "https" else 80),
                           parts.scheme == "https", pool)
        # Import once, on the calling thread. Doing it inside __call__ would make the first scatter's threads
        # take the import lock one by one.
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest
        self._sign, self._request = SigV4Auth, AWSRequest
        pinned = pinned_version(function, self.region)
        self.capability_key = ("lambda",) + pinned if pinned else None

    def __call__(self, payload):
        body = dumps(payload).encode()
        creds = self.credentials or _credentials()
        frozen = creds.get_frozen_credentials() if hasattr(creds, "get_frozen_credentials") else creds
        # Host has to be on the request before it is signed. Without it the front end answers 400 and never checks
        # the signature.
        req = self._request(method="POST", url=self.url, data=body, headers={
            "Host": self._pool.host, "Content-Type": "application/json"})
        self._sign(frozen, "lambda", self.region).add_auth(req)
        status, headers, data = self._pool.post(self._path, list(req.headers.items()), body)
        return _read_invoke(status, headers, data)


def _boto_invoker(function, region, client):
    """The same contract through a boto3-shaped client, for the tests. Production does not pass one: boto3's
    invoke holds the GIL long enough that the last of 128 calls starts after the first has been running."""

    def invoke(payload):
        r = client.invoke(FunctionName=function, Payload=dumps(payload).encode())
        raw = r["Payload"].read()
        resp = loads(raw)
        stats = {"function_error": r.get("FunctionError"), **_billed_from_body(resp)}
        log = r.get("LogResult")
        if log:
            m = REPORT_RE.search(base64.b64decode(log).decode(errors="replace"))
            if m:
                stats.update(duration_ms=float(m.group(1)), billed_ms=int(m.group(2)), memory_mb=int(m.group(3)),
                             max_memory_mb=int(m.group(4)), init_ms=float(m.group(5)) if m.group(5) else None)
        return resp, stats
    pinned = pinned_version(function, region or client.meta.region_name)
    invoke.capability_key = ("lambda",) + pinned if pinned else None
    return invoke


def lambda_invoker(function, region=None, pool=256, client=None):
    """invoke(payload) -> (response, stats), every call to exactly `function` (qualifier included). When that is a
    published version, `invoke.capability_key` is its identity for the capability cache; otherwise it is None and
    every query negotiates. `client` selects the boto3-shaped path; with none, the signed keep-alive path is used."""
    if client is not None:
        return _boto_invoker(function, region, client)
    return SignedInvoke(function, region or "us-west-2", pool)


def loads(raw):
    # Exact sums: a decimal answer stays a Decimal through the merge.
    return json.loads(raw, parse_float=decimal.Decimal)


def scatter(invoke, payloads):
    t0 = time.perf_counter()
    out = list(executor(max(1, len(payloads))).map(invoke, payloads))
    for resp, _ in out:
        if not isinstance(resp, dict) or "agg" not in resp:
            raise WorkerFailed(dumps(resp)[:600])
    return out, (time.perf_counter() - t0) * 1000


def _inputs_of(payload):
    partial = payload.get("partial") if isinstance(payload, dict) else None
    if isinstance(partial, dict) and isinstance(partial.get("inputs"), list):
        return partial["inputs"]
    if isinstance(payload, dict) and isinstance(payload.get("inputs"), list):
        return payload["inputs"]
    return []


def worker_too_old(resp):
    """A worker from before `partial_aggregate`: 0.5.1 names the unknown operation, 0.5.0 finds no top-level inputs."""
    if not isinstance(resp, dict) or resp.get("error") != "bad_json":
        return False
    if resp.get("reason") == "MissingField":
        return True
    return resp.get("reason") == "BadFieldType" and resp.get("field") == "operation"


def check_partial(resp, payload):
    """`resp` is one worker's partial, or `Refused` naming why it is not. Engine and request errors are refusals;
    anything else (a crash, a non-object) is `WorkerFailed`."""
    named = ", ".join(_inputs_of(payload))
    ok = (isinstance(resp, dict) and resp.get("partial_version") == PARTIAL_VERSION and "agg" in resp
          and isinstance(resp.get("schema_fingerprint"), str) and isinstance(resp.get("plan_digest"), str)
          and isinstance(resp.get("aggregates"), list) and isinstance(resp.get("groups"), list))
    if ok:
        return
    if worker_too_old(resp):
        raise Refused("worker lacks partial_aggregate v1; update the worker before using this coordinator"
                      + (f": {named}" if named else ""))
    if isinstance(resp, dict) and resp.get("error") in ("engine", "bad_json", "bad_request"):
        reason = str(resp.get("reason") or resp.get("error"))
        raise Refused(f"{reason}: {named}" if named else reason)
    raise WorkerFailed((named + ": " if named else "") + dumps(resp)[:600])


def agree(answers, payloads):
    """Every answer is a partial, and they planned the same query over compatible schemas. A mismatch names the inputs."""
    for resp, payload in zip(answers, payloads):
        check_partial(resp, payload)
    first = answers[0]
    for other, payload in zip(answers[1:], payloads[1:]):
        names = _inputs_of(payloads[0]) + _inputs_of(payload)
        named = ", ".join(names)
        for key, what in (("schema_fingerprint", "SchemaMismatch: input schemas differ"),
                          ("plan_digest", "PlanMismatch: workers planned the query differently")):
            if other.get(key) != first.get(key):
                raise Refused(f"{what}: {named}")
        if other.get("aggregates") != first.get("aggregates") or other.get("groups") != first.get("groups"):
            raise Refused(f"workers disagree on aggregates or groups: {named}")


def partial_payloads(request, runs):
    inputs = list(request.get("inputs") or [])
    top = {k: v for k, v in request.items() if k not in
           ("aggregate", "group_by", "filter", "sql", "operation", "plan_version", "plan", "inputs", "row_groups")}
    payloads = []
    for run in runs:
        partial = {
            "inputs": [inputs[i] for i, _, _, _ in run],
            "row_groups": [[s, e, of] for _, s, e, of in run],
            "aggregate": request["aggregate"],
        }
        if request.get("group_by"):
            partial["group_by"] = request["group_by"]
        if request.get("filter"):
            partial["filter"] = request["filter"]
        payloads.append(dict(top, operation="partial_aggregate", plan_version=PLAN_VERSION, partial=partial))
    return payloads


# Capability acknowledgements by `invoke.capability_key`, for this process. lambda_invoker sets a key only for a
# published (immutable) version, so a warm coordinator asks such a worker once; an alias, `$LATEST`, an unqualified
# name or an invoker without a key is asked on every query, before any input is sent.
CAPABILITIES = {}


def negotiate(invoke):
    """Require an exact native capability acknowledgement before sending inputs for preparation or execution."""
    key = getattr(invoke, "capability_key", None)
    if key is not None and key in CAPABILITIES:
        return
    cap, _ = invoke({"operation": "capabilities"})
    if (not isinstance(cap, dict) or type(cap.get("prepare_aggregate_version")) is not int
            or cap["prepare_aggregate_version"] != PLAN_VERSION or not isinstance(cap.get("structured_plan_versions"), list)
            or not any(type(v) is int and v == PLAN_VERSION for v in cap["structured_plan_versions"])):
        raise Refused("worker lacks structured aggregate plan v1; update the worker before using this coordinator")
    if key is not None:
        CAPABILITIES[key] = True


def prepare(invoke, request, inputs=None):
    """The native preparation of `request` (over `inputs` instead of its own, when given). Requires negotiation."""
    payload = dict(request, operation="prepare_aggregate", plan_version=PLAN_VERSION)
    if inputs is not None:
        payload["inputs"] = inputs
    prepared, _ = invoke(payload)
    if not isinstance(prepared, dict) or prepared.get("plan_version") != PLAN_VERSION or "plan" not in prepared:
        raise Refused(f"native aggregate preparation refused: {dumps(prepared)[:600]}")
    return prepared


def run(invoke, request, workers, order_by=None, limit=None, counts=None):
    """Answer `request` over `workers` workers. Explicit inputs take `partial_aggregate` (no call before the scatter);
    an input that is a `*` pattern takes the prepared plan v1 path (`run_prepared`)."""
    inputs = list(request.get("inputs") or [])
    if any("*" in u for u in inputs):
        return run_prepared(invoke, request, workers, order_by, limit, counts)
    t0 = time.perf_counter()
    if request.get("sql") or request.get("row_groups"):
        raise Refused("the coordinator takes `aggregate`/`group_by` requests, not `sql` or `row_groups`")
    if not isinstance(request.get("aggregate"), str) or not request["aggregate"].strip():
        raise Refused("nothing to merge: the request has no `aggregate`")
    if not inputs:
        raise Refused("nothing to merge: the request has no inputs")
    if counts is None:
        hinted = [math.ceil(workers / len(inputs))] * len(inputs)
    else:
        if len(counts) != len(inputs):
            raise ValueError(f"{len(counts)} row-group counts for {len(inputs)} inputs")
        hinted = list(counts)
    runs = partial_runs(hinted, max(1, workers))
    payloads = partial_payloads(request, runs)
    footer_ms = (time.perf_counter() - t0) * 1000
    out, scatter_ms = scatter_partial(invoke, payloads)
    answers = [resp for resp, _ in out]
    agree(answers, payloads)
    aggs = [(a["function"], None, a["name"]) for a in answers[0]["aggregates"]]
    if any(fn not in MERGEABLE for fn, _, _ in aggs):
        raise Refused("worker returned an unsupported merge operation")
    group = [(None, name) for name in answers[0]["groups"]]
    merged = merge([a["agg"] for a in answers], aggs, group)
    rows = order(merged, order_by, limit) if group else merged
    stats = [_worker_stat(resp, s) for resp, s in out]
    return {"agg": rows, "workers": len(payloads), "footer_ms": footer_ms, "scatter_ms": scatter_ms,
            "check_ms": None, "e2e_ms": (time.perf_counter() - t0) * 1000, "worker_stats": stats,
            "protocol": "partial"}


def scatter_partial(invoke, payloads):
    t0 = time.perf_counter()
    out = list(executor(max(1, len(payloads))).map(invoke, payloads))
    for (resp, _), payload in zip(out, payloads):
        check_partial(resp, payload)
    return out, (time.perf_counter() - t0) * 1000


def run_prepared(invoke, request, workers, order_by=None, limit=None, counts=None):
    """The structured-plan v1 path, for inputs that are `*` patterns: negotiate, prepare every input, then scatter."""
    t0 = time.perf_counter()
    if request.get("sql") or request.get("row_groups"):
        raise Refused("the coordinator takes `aggregate`/`group_by` requests, not `sql` or `row_groups`")
    if not isinstance(request.get("aggregate"), str) or not request["aggregate"].strip():
        raise Refused("nothing to merge: the request has no `aggregate`")
    negotiate(invoke)
    inputs = list(request.get("inputs") or [])
    early = counts is not None and inputs and not any("*" in u for u in inputs)
    prepared = prepare(invoke, request, inputs[:1] if early else None)
    aggs = [(a["function"], None, a["name"]) for a in prepared["aggregates"]]
    if any(fn not in MERGEABLE for fn, _, _ in aggs):
        raise Refused("native preparation returned an unsupported merge operation")
    group = [(None, name) for name in prepared["groups"]]
    if not early:
        inputs = prepared["inputs"]
    if counts is None:
        counts = prepared["row_group_counts"]
    elif len(counts) != len(inputs):
        raise ValueError(f"{len(counts)} row-group counts for {len(inputs)} inputs")
    footer_ms = (time.perf_counter() - t0) * 1000

    base = {k: v for k, v in request.items() if k not in
            ("aggregate", "group_by", "filter", "sql", "operation", "plan_version", "plan")}
    base.update(inputs=inputs, plan_version=PLAN_VERSION, plan=prepared["plan"])
    runs = split(counts, workers)
    payloads = [dict(base, inputs=[inputs[i] for i, _, _ in r], row_groups=[[s, e] for _, s, e in r]) for r in runs]
    if not payloads:  # no row groups anywhere: one worker answers the empty table
        payloads = [base]
    check_ms = None
    if early:
        with cf.ThreadPoolExecutor(2) as ex:
            full = ex.submit(lambda: (prepare(invoke, request), time.perf_counter()))
            partial = ex.submit(scatter, invoke, payloads)
            whole, done = full.result()  # a refusal here wins over the scatter's outcome
            check_ms = (done - t0) * 1000 - footer_ms
            out, scatter_ms = partial.result()
        if any(whole.get(k) != prepared.get(k) for k in ("plan", "aggregates", "groups")):
            raise Refused("the full preparation planned the query differently from the first input's")
    else:
        out, scatter_ms = scatter(invoke, payloads)
    merged = merge([resp["agg"] for resp, _ in out], aggs, group)
    rows = order(merged, order_by, limit) if group else merged
    stats = [dict(s, total_ms=resp.get("total_ms"), rows_in=resp.get("rows_in"), bytes_in=resp.get("bytes_in"))
             for resp, s in out]
    return {"agg": rows, "workers": len(payloads), "footer_ms": footer_ms, "scatter_ms": scatter_ms,
            "check_ms": check_ms, "e2e_ms": (time.perf_counter() - t0) * 1000, "worker_stats": stats,
            "protocol": "plan"}


# ---- Output ----

def dumps(v):
    """JSON with Decimals written as the exact numbers they hold."""
    if isinstance(v, decimal.Decimal):
        return str(v) if v.is_finite() else json.dumps(float(v))
    if isinstance(v, dict):
        return "{" + ",".join(f"{json.dumps(k)}:{dumps(x)}" for k, x in v.items()) + "}"
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(dumps(x) for x in v) + "]"
    return json.dumps(v)


def handler(event, _ctx):
    """Coordinator Lambda entry point. Event: {"function": worker function, "workers": N, "request": worker
    request, "order_by"?, "limit"?, "row_group_counts"?}. The answer's decimals come back as JSON numbers."""
    invoke = lambda_invoker(event["function"], pool=max(16, 2 * int(event["workers"])))
    try:
        res = run(invoke, event["request"], int(event["workers"]), event.get("order_by"), event.get("limit"),
                  event.get("row_group_counts"))
    except (Refused, WorkerFailed) as e:
        return {"error": type(e).__name__, "reason": str(e)}
    return json.loads(dumps(res))


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--function", required=True, help="worker Lambda function name or ARN")
    p.add_argument("--region")
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--input", action="append", required=True, help="an input URL; repeat for several")
    p.add_argument("--aggregate", required=True)
    p.add_argument("--group-by")
    p.add_argument("--filter")
    p.add_argument("--order-by", help="over the output names, after the merge")
    p.add_argument("--limit", type=int)
    p.add_argument("--request", default="{}", help="other worker request fields, as a JSON object")
    a = p.parse_args(argv)
    req = dict(json.loads(a.request), inputs=a.input, aggregate=a.aggregate)
    for k, v in (("group_by", a.group_by), ("filter", a.filter)):
        if v:
            req[k] = v
    try:
        res = run(lambda_invoker(a.function, a.region, max(16, 2 * a.workers)), req, a.workers, a.order_by, a.limit)
    except (Refused, WorkerFailed) as e:
        print(f"fanout: {e}", file=sys.stderr)
        return 2
    print(dumps(res))
    return 0


if __name__ == "__main__":
    sys.exit(main())
