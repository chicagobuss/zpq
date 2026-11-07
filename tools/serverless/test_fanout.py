#!/usr/bin/env python3
"""fanout.py against real zpq-lambda workers behind a fake Lambda runtime API, checked against one invocation of
the whole query and against DuckDB.

    python3 tools/serverless/test_fanout.py --lambda zig-out/bin/zpq-lambda --duckdb ~/.cache/zpq-duckdb2/bin-2.0.0/duckdb

Run from the repository root (the fixtures are cwd-relative)."""
import argparse
import http.server
import io
import itertools
import json
import math
import os
import queue
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fanout  # noqa: E402

LAMBDA, DUCKDB = None, None
# full_match: 8 row groups of 200 rows; n is NULL in a fifth of them, s a string, b 16 values, x a double.
FULL = "ci/fixtures/parquet/full_match.parquet"
# group_order: 24 row groups of 500 rows; k a string key, i a bigint, x a double.
GROUPS = "ci/fixtures/parquet/group_order.parquet"


class FakeRuntime:
    """The Lambda runtime API (2018-06-01) for a few zpq-lambda processes: `invoke` queues an event and waits
    for whichever process takes it to post the answer, as `lambda.invoke` waits."""

    def __init__(self, processes=4):
        self.events, self.answers, self.ids = queue.Queue(), {}, itertools.count()
        rt = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_):
                pass

            def reply(self, code, body=b"", headers=()):
                self.send_response(code)
                for k, v in headers:
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                rid, body = rt.events.get()
                self.reply(200, body, [("Lambda-Runtime-Aws-Request-Id", rid),
                                       ("Lambda-Runtime-Deadline-Ms", "9999999999999"),
                                       ("Content-Type", "application/json")])

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                parts = self.path.split("/")  # /2018-06-01/runtime/invocation/<id>/<response|error>
                self.reply(202)
                rid, kind = parts[-2], parts[-1]
                slot = rt.answers.get(rid)
                if slot:
                    slot[1].append(body if kind == "response" else json.dumps({"error": "runtime", "body":
                                                                                body.decode()}).encode())
                    slot[0].set()

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        env = {"AWS_LAMBDA_RUNTIME_API": f"127.0.0.1:{self.server.server_port}", "AWS_LAMBDA_FUNCTION_NAME": "fan",
               "AWS_LAMBDA_FUNCTION_VERSION": "$LATEST", "AWS_LAMBDA_FUNCTION_MEMORY_SIZE": "1024",
               "AWS_REGION": "us-west-2", "PATH": "/usr/bin:/bin"}
        self.procs = [subprocess.Popen([LAMBDA], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                      for _ in range(processes)]

    def invoke(self, payload):
        rid = f"req-{next(self.ids)}"
        done, box = threading.Event(), []
        self.answers[rid] = (done, box)
        self.events.put((rid, json.dumps(payload).encode()))
        if not done.wait(60):
            raise TimeoutError(rid)
        del self.answers[rid]
        return fanout.loads(box[0]), {}

    def close(self):
        for p in self.procs:
            p.kill()
            p.wait()
        self.server.shutdown()
        self.server.server_close()


class FakeLambdaClient:
    """boto3's Lambda client as lambda_invoker uses it: records every (FunctionName, operation) and answers from
    the fake runtime."""

    def __init__(self, rt, region="us-west-2"):
        self.rt, self.calls = rt, []
        self.meta = types.SimpleNamespace(region_name=region)

    def invoke(self, FunctionName, Payload, LogType=None):  # noqa: N803 (boto3's names)
        payload = json.loads(Payload)
        self.calls.append((FunctionName, payload.get("operation")))
        resp, _ = self.rt.invoke(payload)
        return {"Payload": io.BytesIO(fanout.dumps(resp).encode()), "LogResult": ""}


def duckdb(sql):
    out = subprocess.run([DUCKDB, "-no-agent", "-json", "-c", sql], capture_output=True, text=True, check=True,
                         timeout=60).stdout.strip()
    return fanout.loads(out) if out else []


def number(v):
    """DuckDB writes a HUGEINT (a SUM of integers) as a JSON string."""
    if isinstance(v, str):
        try:
            return int(v)
        except ValueError:
            return v
    return v


def same(a, b):
    a, b = number(a), number(b)
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(same(a[k], b[k]) for k in a)
    if a is None or b is None or isinstance(a, (str, bool)) or isinstance(b, (str, bool)):
        return a == b
    if isinstance(a, int) and isinstance(b, int):
        return a == b
    a, b = float(a), float(b)
    return (math.isnan(a) and math.isnan(b)) or math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-9)


def keyed(rows, names):
    return {tuple(fanout.dumps(r[n]) for n in names): r for r in rows}


# (inputs, aggregate, group_by or None, filter or None)
UNGROUPED_AGG = ("count(*) AS c, count(n) AS cn, sum(n) AS sn, min(n) AS mn, max(n) AS xn, avg(n) AS an, "
                 "min(s) AS ms, max(s) AS xs, sum(x) AS sx, avg(x) AS ax, sum(u) AS su")
CASES = [
    ([FULL], UNGROUPED_AGG, None, None),
    ([FULL], UNGROUPED_AGG, None, "ts < 300"),  # most workers' row groups match nothing
    ([FULL], UNGROUPED_AGG, None, "b < 0"),  # no worker's do: counts 0, the rest NULL
    ([FULL, FULL], UNGROUPED_AGG, None, "n > 100"),  # two inputs, a worker's run crossing between them
    ([FULL], "count(*) AS c, count(n) AS cn, sum(n) AS sn, min(n) AS mn, max(n) AS xn, avg(n) AS an, avg(x) AS ax",
     "b", None),
    ([FULL], "count(*) AS c, sum(n) AS sn, avg(n) AS an, min(s) AS ms", "b AS bucket", "ts < 300"),
    ([GROUPS], "count(*) AS c, sum(i) AS si, min(x) AS mx, max(i) AS xi, avg(x) AS ax", "k", None),
    ([GROUPS, GROUPS], "count(*) AS c, sum(x) AS sx, avg(i) AS ai", "k AS key", "i > 5000"),
]
GROUP_NAMES = {None: [], "b": ["b"], "b AS bucket": ["bucket"], "k": ["k"], "k AS key": ["key"]}
ROW_GROUPS = {FULL: 8, GROUPS: 24}


def request(inputs, agg, group, flt):
    req = {"inputs": inputs, "aggregate": agg}
    if group:
        req["group_by"] = group
    if flt:
        req["filter"] = flt
    return req


class Fanout(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rt = FakeRuntime()

    @classmethod
    def tearDownClass(cls):
        cls.rt.close()

    def test_merged_matches_one_invocation_and_duckdb(self):
        for inputs, agg, group, flt in CASES:
            req = request(inputs, agg, group, flt)
            one, _ = self.rt.invoke(req)
            self.assertIn("agg", one, one)
            names = GROUP_NAMES[group]
            where = f" WHERE {flt}" if flt else ""
            groups = " GROUP BY ALL" if group else ""
            files = ", ".join(f"'{f}'" for f in inputs)
            ref = duckdb(f"SELECT {', '.join(filter(None, [group, agg]))} FROM read_parquet([{files}]){where}{groups}")
            for workers in (1, 3, 8, 64):
                with self.subTest(inputs=inputs, agg=agg, group=group, filter=flt, workers=workers):
                    got = fanout.run(self.rt.invoke, req, workers)
                    self.assertEqual(got["protocol"], "partial")
                    self.assertEqual(got["workers"], workers)  # without counts, every worker gets a fraction
                    if group:
                        g, o, d = keyed(got["agg"], names), keyed(one["agg"], names), keyed(ref, names)
                        self.assertEqual(g.keys(), o.keys())
                        self.assertEqual(g.keys(), d.keys())
                        for k in g:
                            self.assertTrue(same(g[k], o[k]), (g[k], o[k]))
                            self.assertTrue(same(g[k], d[k]), (g[k], d[k]))
                    else:
                        self.assertTrue(same(got["agg"], one["agg"]), (got["agg"], one["agg"]))
                        self.assertTrue(same(got["agg"], ref[0]), (got["agg"], ref[0]))

    def test_counts_right_wrong_or_absent_answer_the_same_with_no_call_before_the_scatter(self):
        sent = []

        def invoke(payload):
            sent.append(payload)
            return self.rt.invoke(payload)

        for inputs, agg, group, flt in CASES:
            req = request(inputs, agg, group, flt)
            right = [ROW_GROUPS[f] for f in inputs]
            want = fanout.run(self.rt.invoke, req, 1)
            for workers in (1, 3, 8):
                for counts in (right, None, [c * 3 for c in right], [1] * len(inputs), [0] * len(inputs)):
                    with self.subTest(inputs=inputs, agg=agg, group=group, filter=flt, workers=workers, counts=counts):
                        sent.clear()
                        got = fanout.run(invoke, req, workers, counts=counts)
                        self.assertTrue(all(p["operation"] == "partial_aggregate" for p in sent), sent)
                        self.assertEqual(len(sent), got["workers"])
                        # Every input reaches a worker, so every footer is opened and its schema checked.
                        self.assertEqual({u for p in sent for u in p["partial"]["inputs"]}, set(inputs))
                        if group:
                            g, w = keyed(got["agg"], GROUP_NAMES[group]), keyed(want["agg"], GROUP_NAMES[group])
                            self.assertEqual(g.keys(), w.keys())
                            self.assertTrue(all(same(g[k], w[k]) for k in g), (g, w))
                        else:
                            self.assertTrue(same(got["agg"], want["agg"]), (got["agg"], want["agg"]))

    def test_fractions_of_one_input_cover_it_once_whatever_its_row_group_count(self):
        # FULL holds 8 row groups of 200 rows; thirds of it are row groups [0, 2), [2, 5) and [5, 8).
        seen = []
        for k in range(3):
            resp, _ = self.rt.invoke({"operation": "partial_aggregate", "plan_version": 1, "partial": {
                "inputs": [FULL], "row_groups": [[k, k + 1, 3]], "aggregate": "count(*) AS c"}})
            self.assertEqual(resp.get("partial_version"), 1, resp)
            seen.append((resp["row_groups_in"], resp["agg"]["_a0"]))
        self.assertEqual(seen, [(2, 400), (3, 600), (3, 600)])

    def test_a_glob_input_is_prepared_whole_even_with_counts(self):
        prepared = []

        def invoke(payload):
            if payload.get("operation") == "prepare_aggregate":
                prepared.append(payload["inputs"])
            return self.rt.invoke(payload)

        pattern = os.path.join(os.path.dirname(FULL), "full_matc*.parquet")
        got = fanout.run(invoke, {"inputs": [pattern], "aggregate": "count(*) AS c"}, 2, counts=[8])
        self.assertEqual(prepared, [[pattern]])
        self.assertEqual(got["agg"]["c"], 1600)

    def test_the_capability_answer_is_cached_across_invocations(self):
        operations = []

        def invoke(payload):
            operations.append(payload.get("operation"))
            return self.rt.invoke(payload)

        invoke.capability_key = ("test", "cached-function")
        fanout.CAPABILITIES.pop(invoke.capability_key, None)
        pattern = os.path.join(os.path.dirname(FULL), "full_matc*.parquet")
        req = {"inputs": [pattern], "aggregate": "count(*) AS c, sum(n) AS sn, avg(x) AS ax"}
        answers = [fanout.run(invoke, req, 4)["agg"] for _ in range(3)]
        self.assertEqual(operations.count("capabilities"), 1)
        self.assertEqual(operations.count("prepare_aggregate"), 3)  # one full prepare per query; the ack is cached
        self.assertTrue(all(same(a, answers[0]) for a in answers))
        self.assertTrue(same(answers[0], fanout.run(self.rt.invoke, req, 4)["agg"]))

    def test_mixed_input_schemas_are_refused_with_supplied_counts(self):
        with tempfile.TemporaryDirectory() as tmp:
            dec, big = os.path.join(tmp, "dec.parquet"), os.path.join(tmp, "big.parquet")
            duckdb(f"COPY (SELECT 1.00::DECIMAL(15,2) AS x) TO '{dec}' (FORMAT parquet)")
            duckdb(f"COPY (SELECT 1::BIGINT AS x) TO '{big}' (FORMAT parquet)")
            req = {"inputs": [dec, big], "aggregate": "count(*) AS c", "group_by": "x"}
            for counts in (None, [1, 1], [1, 0]):
                with self.subTest(counts=counts), self.assertRaisesRegex(fanout.Refused, "SchemaMismatch") as caught:
                    fanout.run(self.rt.invoke, req, 2, counts=counts)
                self.assertIn(dec, str(caught.exception))
                self.assertIn(big, str(caught.exception))

    def test_only_a_published_version_caches_the_capability_answer(self):
        pattern = os.path.join(os.path.dirname(FULL), "full_matc*.parquet")
        req = {"inputs": [pattern], "aggregate": "count(*) AS c"}
        arn = "arn:aws:lambda:us-west-2:123456789012:function:zpq-w"
        for function, key in [("zpq-w", None), ("zpq-w:live", None), ("zpq-w:$LATEST", None), (arn + ":prod", None),
                              ("zpq-w:7", ("lambda", None, "us-west-2", "zpq-w", "7")),
                              (arn + ":8", ("lambda", "123456789012", "us-west-2", "zpq-w", "8"))]:
            with self.subTest(function=function):
                client = FakeLambdaClient(self.rt)
                invoke = fanout.lambda_invoker(function, client=client)
                self.assertEqual(invoke.capability_key, key)
                fanout.CAPABILITIES.pop(key, None)
                for _ in range(3):
                    self.assertEqual(fanout.run(invoke, req, 2)["agg"]["c"], 1600)
                self.assertEqual(sum(op == "capabilities" for _, op in client.calls), 3 if key is None else 1)

    def test_a_pinned_version_keeps_preparation_and_partials_on_it(self):
        client = FakeLambdaClient(self.rt)
        invoke = fanout.lambda_invoker("zpq-w:7", client=client)
        fanout.CAPABILITIES.pop(invoke.capability_key, None)
        got = fanout.run(invoke, {"inputs": [FULL, FULL], "aggregate": "count(*) AS c"}, 4, counts=[8, 8])
        self.assertEqual(got["agg"]["c"], 3200)
        self.assertEqual(got["protocol"], "partial")
        self.assertEqual({f for f, _ in client.calls}, {"zpq-w:7"})
        ops = [op for _, op in client.calls]
        self.assertTrue(ops and all(op == "partial_aggregate" for op in ops))
        self.assertEqual(len(ops), got["workers"])

    def test_an_empty_range_merges_as_no_rows(self):
        prepared = fanout.prepare(self.rt.invoke, {"inputs": [FULL], "aggregate": UNGROUPED_AGG})
        aggs = [(a["function"], None, a["name"]) for a in prepared["aggregates"]]
        base = {"inputs": [FULL], "plan_version": 1, "plan": prepared["plan"]}
        empty, _ = self.rt.invoke(dict(base, row_groups=[[5, 5]]))
        whole, _ = self.rt.invoke(base)
        self.assertEqual(empty["agg"]["_a0"], 0)
        merged = fanout.merge([empty["agg"], whole["agg"], empty["agg"]], aggs, [])
        self.assertTrue(same(merged, fanout.merge([whole["agg"]], aggs, [])))
        nothing = fanout.merge([empty["agg"]], aggs, [])
        self.assertEqual(nothing["c"], 0)
        self.assertEqual(nothing["cn"], 0)
        self.assertTrue(all(nothing[k] is None for k in ("sn", "mn", "xn", "an", "ms", "xs", "sx", "ax")))

    def test_order_by_and_limit_apply_after_the_merge(self):
        req = {"inputs": [GROUPS], "aggregate": "sum(i) AS si, count(*) AS c", "group_by": "k"}
        ref = duckdb(f"SELECT k, sum(i) AS si, count(*) AS c FROM '{GROUPS}' GROUP BY k ORDER BY si DESC, k LIMIT 5")
        got = fanout.run(self.rt.invoke, req, 8, order_by="si DESC, k", limit=5)
        self.assertEqual([r["k"] for r in got["agg"]], [r["k"] for r in ref])
        self.assertTrue(all(same(g, d) for g, d in zip(got["agg"], ref)))

    def test_quoted_names_and_filter_literals_are_data_in_partial_requests(self):
        requests = []

        def invoke(payload):
            requests.append(payload)
            return self.rt.invoke(payload)

        req = {"inputs": [FULL], "aggregate":
               "count(*) FILTER (WHERE s != 'O''Reilly)') AS \"count value\", avg(x) AS \"average value\"",
               "group_by": "b AS \"group name\""}
        one, _ = self.rt.invoke(req)
        got = fanout.run(invoke, req, 3)
        self.assertTrue(same(keyed(got["agg"], ["group name"]), keyed(one["agg"], ["group name"])))
        partials = [p for p in requests if p.get("operation") == "partial_aggregate"]
        self.assertEqual(len(partials), got["workers"])
        self.assertTrue(all(p["plan_version"] == 1 and "plan" not in p for p in partials))
        self.assertTrue(all(p["partial"]["aggregate"] == req["aggregate"] and p["partial"]["group_by"] == req["group_by"]
                            for p in partials))
        self.assertTrue(all(not any(k in p for k in ("aggregate", "group_by", "filter", "sql", "inputs")) for p in partials))

    def test_worker_rejects_version_conflicts_and_deep_plans_before_input_io(self):
        for payload, reason in [
            ({"inputs": ["missing.parquet"], "plan_version": 2, "plan": {}}, "UnsupportedPlanVersion"),
            ({"inputs": ["missing.parquet"], "plan": {}}, "UnsupportedPlanVersion"),
            ({"inputs": ["missing.parquet"], "plan_version": 1, "plan": {}, "aggregate": "count(*)"},
             "ConflictingPlanFields"),
        ]:
            response, _ = self.rt.invoke(payload)
            self.assertEqual(response.get("reason"), reason, response)
        legacy, _ = self.rt.invoke({"inputs": [FULL], "aggregate": "count(*) AS n"})
        null_fields, _ = self.rt.invoke({"inputs": [FULL], "aggregate": "count(*) AS n",
                                        "operation": None, "plan": None, "plan_version": None})
        self.assertEqual(legacy.get("agg"), null_fields.get("agg"), null_fields)
        self.assertIn("agg", null_fields)
        deep = None
        for _ in range(512):
            deep = [deep]
        response, _ = self.rt.invoke({"inputs": ["missing.parquet"], "plan_version": 1, "plan": deep})
        self.assertEqual(response.get("reason"), "PlanTooDeep", response)

    def test_an_exact_range_past_the_footer_is_refused(self):
        payload = {"operation": "partial_aggregate", "plan_version": 1, "partial": {
            "inputs": [FULL], "row_groups": [[0, 9]], "aggregate": "count(*) AS c"}}
        resp, _ = self.rt.invoke(payload)
        with self.assertRaisesRegex(fanout.Refused, "RowGroupOutOfRange"):
            fanout.check_partial(resp, payload)

    def test_disagreeing_plans_are_refused(self):
        base = {"partial_version": 1, "agg": {"_a0": 1}, "schema_fingerprint": "aa", "plan_digest": "d1",
                "aggregates": [{"function": "count", "name": "c"}], "groups": []}
        payloads = [{"partial": {"inputs": ["a.parquet"]}}, {"partial": {"inputs": ["b.parquet"]}}]
        with self.assertRaisesRegex(fanout.Refused, "PlanMismatch"):
            fanout.agree([base, dict(base, plan_digest="d2")], payloads)


class Refusals(unittest.TestCase):
    def test_unmergeable_shapes_are_refused_before_execution(self):
        rt = FakeRuntime(processes=1)
        requests = []

        def invoke(payload):
            requests.append(payload)
            return rt.invoke(payload)

        try:
            for agg in ["count(DISTINCT s) AS d", "median(x) AS m", "quantile(x, 0.9) AS q",
                        "sum(x) OVER (PARTITION BY b) AS w", "sum(x) / count(*) AS r", "x AS y"]:
                with self.subTest(agg=agg), self.assertRaises(fanout.Refused) as caught:
                    fanout.run(invoke, {"inputs": ["missing.parquet"], "aggregate": agg}, 4)
                self.assertNotIn('"input"', str(caught.exception))  # no answer about an input: none was opened
            self.assertTrue(all(p.get("operation") == "partial_aggregate" for p in requests))
        finally:
            rt.close()

    def test_old_worker_cannot_acknowledge_unknown_fields(self):
        requests = []

        def old_worker(payload):
            requests.append(payload)
            return {"error": "bad_json", "reason": "MissingField"}, {}

        with self.assertRaisesRegex(fanout.Refused, "partial_aggregate"):
            fanout.run(old_worker, {"inputs": [FULL], "aggregate": "count(*) AS c"}, 4)
        self.assertTrue(requests and all(p.get("operation") == "partial_aggregate" for p in requests))
        self.assertNotIn("inputs", requests[0])
        self.assertEqual(requests[0]["partial"]["inputs"], [FULL])

    def test_capability_versions_require_integers(self):
        for capability in [{"prepare_aggregate_version": True, "structured_plan_versions": [1]},
                           {"prepare_aggregate_version": 1, "structured_plan_versions": [True]}]:
            requests = []

            def invoke(payload):
                requests.append(payload)
                return capability, {}

            with self.assertRaises(fanout.Refused):
                pattern = os.path.join(os.path.dirname(FULL), "full_matc*.parquet")
                fanout.run(invoke, {"inputs": [pattern], "aggregate": "count(*) AS c"}, 4)
            self.assertEqual(requests, [{"operation": "capabilities"}])

    def test_split_is_contiguous_and_even(self):
        self.assertEqual(fanout.split([8], 3), [[(0, 0, 2)], [(0, 2, 5)], [(0, 5, 8)]])
        self.assertEqual(fanout.split([3, 0, 2], 2), [[(0, 0, 2)], [(0, 2, 3), (2, 0, 2)]])
        self.assertEqual(fanout.split([2], 5), [[(0, 0, 1)], [(0, 1, 2)]])
        self.assertEqual(fanout.split([0], 4), [])
        runs = fanout.partial_runs([3, 0, 2], 2)
        covered = {}
        for run in runs:
            for i, s, e, of in run:
                covered.setdefault(i, []).append((s, e, of))
        self.assertEqual(set(covered), {0, 1, 2})
        for parts in covered.values():
            parts = sorted(parts)
            self.assertEqual(parts[0][0], 0)
            self.assertEqual(parts[-1][1], parts[-1][2])
            for left, right in zip(parts, parts[1:]):
                self.assertEqual(left[1], right[0])
                self.assertEqual(left[2], right[2])


class SignedInvokeTest(unittest.TestCase):
    def test_endpoint_keeps_a_pinned_qualifier(self):
        url, region = fanout.lambda_endpoint("zpq-w", "us-west-2")
        self.assertEqual(url, "https://lambda.us-west-2.amazonaws.com/2015-03-31/functions/zpq-w/invocations")
        self.assertEqual(region, "us-west-2")
        url, region = fanout.lambda_endpoint("arn:aws:lambda:eu-west-1:123456789012:function:zpq-w:7", None)
        self.assertEqual(region, "eu-west-1")
        self.assertTrue(url.endswith("/functions/zpq-w/invocations?Qualifier=7"), url)

    def test_a_non_200_and_a_function_error_are_told_apart(self):
        with self.assertRaises(fanout.WorkerFailed):
            fanout._read_invoke(429, {}, b"slow")
        resp, stats = fanout._read_invoke(200, {"X-Amz-Function-Error": "Unhandled"},
                                           b'{"error":"boom","total_ms":12.2}')
        self.assertEqual(resp["error"], "boom")
        self.assertEqual(stats["function_error"], "Unhandled")
        self.assertEqual(stats["billed_ms"], 13)
        self.assertEqual(stats["duration_ms"], 12.2)

    def test_a_hundred_and_twenty_eight_local_invokes_stay_under_the_old_spread(self):
        """boto3 held the GIL for about 1.5 ms per invoke, so the last of 128 started ~150 ms late. A keep-alive
        server that answers immediately should take a fraction of a millisecond each, and the whole fan well under
        that 150 ms."""
        import socket
        from botocore.credentials import Credentials

        def serve(sock):
            f = sock.makefile("rb")
            try:
                while True:
                    if not f.readline():
                        break
                    n = 0
                    while True:
                        h = f.readline()
                        if h in (b"\r\n", b"\n", b""):
                            break
                        if h.lower().startswith(b"content-length:"):
                            n = int(h.split(b":", 1)[1])
                    got = f.read(n) if n else b""
                    if b"slow" in got:
                        time.sleep(0.04)  # hold the request open so the fan's sends overlap, as a worker does
                    body = b'{"agg":{"c":1},"total_ms":1}'
                    sock.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: keep-alive\r\n\r\n%s"
                                 % (len(body), body))
            finally:
                try:
                    sock.close()
                except OSError:
                    pass

        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(256)
        port = listener.getsockname()[1]

        def accept():
            while True:
                try:
                    conn, _ = listener.accept()
                except OSError:
                    return
                threading.Thread(target=serve, args=(conn,), daemon=True).start()

        threading.Thread(target=accept, daemon=True).start()
        try:
            inv = fanout.SignedInvoke(
                "zpq-w", "us-west-2", 64, credentials=Credentials("example-access-key", "s" * 40),
                endpoint=f"http://127.0.0.1:{port}")
            inv({"n": 0})  # the pool's first connection
            t0 = time.perf_counter()
            for i in range(32):
                resp, stats = inv({"n": i})
            each = (time.perf_counter() - t0) / 32 * 1000
            self.assertLess(each, 2, f"one kept-alive invoke took {each:.2f} ms")
            self.assertEqual(resp["agg"]["c"], 1)
            self.assertEqual(stats["billed_ms"], 1)
            out, scatter_ms = fanout.scatter(inv, [{"slow": 1, "n": i} for i in range(128)])
            self.assertEqual(len(out), 128)
            # Pool of 64, so 128 calls are two waves of the 40 ms hold (80 ms). The old invoker added ~150 ms of
            # start skew on top of that and landed near 230 ms; serial issue would take seconds.
            self.assertLess(scatter_ms, 220, f"128 overlapped invokes took {scatter_ms:.0f} ms")
        finally:
            pool = fanout._POOLS.pop((inv._pool.host, inv._pool.port, inv._pool.tls), None)
            if pool is not None:
                pool.close()
            listener.close()


def main():
    global LAMBDA, DUCKDB
    p = argparse.ArgumentParser()
    p.add_argument("--lambda", dest="lam", required=True)
    p.add_argument("--duckdb", required=True)
    a, rest = p.parse_known_args()
    LAMBDA, DUCKDB = os.path.abspath(a.lam), a.duckdb
    unittest.main(argv=[sys.argv[0]] + rest, verbosity=2)


if __name__ == "__main__":
    main()
