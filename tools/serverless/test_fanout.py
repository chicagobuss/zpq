#!/usr/bin/env python3
"""fanout.py against real zpq-lambda workers behind a fake Lambda runtime API, checked against one invocation of
the whole query and against DuckDB.

    python3 tools/serverless/test_fanout.py --lambda zig-out/bin/zpq-lambda --duckdb ~/.cache/zpq-duckdb2/bin-2.0.0/duckdb

Run from the repository root (the fixtures are cwd-relative)."""
import argparse
import http.server
import itertools
import json
import math
import os
import queue
import subprocess
import sys
import threading
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


class Fanout(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rt = FakeRuntime()

    @classmethod
    def tearDownClass(cls):
        cls.rt.close()

    def test_merged_matches_one_invocation_and_duckdb(self):
        for inputs, agg, group, flt in CASES:
            req = {"inputs": inputs, "aggregate": agg}
            if group:
                req["group_by"] = group
            if flt:
                req["filter"] = flt
            one, _ = self.rt.invoke(req)
            self.assertIn("agg", one, one)
            names = {None: [], "b": ["b"], "b AS bucket": ["bucket"], "k": ["k"], "k AS key": ["key"]}[group]
            where = f" WHERE {flt}" if flt else ""
            groups = " GROUP BY ALL" if group else ""
            files = ", ".join(f"'{f}'" for f in inputs)
            ref = duckdb(f"SELECT {', '.join(filter(None, [group, agg]))} FROM read_parquet([{files}]){where}{groups}")
            for workers in (1, 3, 8, 64):
                with self.subTest(inputs=inputs, agg=agg, group=group, filter=flt, workers=workers):
                    got = fanout.run(self.rt.invoke, req, workers)
                    self.assertEqual(got["workers"], min(workers, 8 * inputs.count(FULL) + 24 * inputs.count(GROUPS)))
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
        partials = [p for p in requests if p.get("operation") not in ("capabilities", "prepare_aggregate")]
        self.assertEqual(len(partials), got["workers"])
        self.assertTrue(all(p["plan_version"] == 1 and "plan" in p for p in partials))
        self.assertTrue(all(not any(k in p for k in ("aggregate", "group_by", "filter", "sql")) for p in partials))

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

    def test_a_range_past_the_footer_fails_the_query(self):
        with self.assertRaisesRegex(fanout.WorkerFailed, "RowGroupOutOfRange"):
            fanout.run(self.rt.invoke, {"inputs": [FULL], "aggregate": "count(*) AS c"}, 4, counts=[9])


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
                with self.subTest(agg=agg), self.assertRaises(fanout.Refused):
                    fanout.run(invoke, {"inputs": [FULL], "aggregate": agg}, 4, counts=[8])
            self.assertTrue(all(p.get("operation") in ("capabilities", "prepare_aggregate") for p in requests))
        finally:
            rt.close()

    def test_old_worker_cannot_acknowledge_unknown_fields(self):
        requests = []

        def old_worker(payload):
            requests.append(payload)
            return {"error": "bad_json", "reason": "MissingField"}, {}

        with self.assertRaisesRegex(fanout.Refused, "structured aggregate plan v1"):
            fanout.run(old_worker, {"inputs": [FULL], "aggregate": "count(*) AS c"}, 4)
        self.assertEqual(requests, [{"operation": "capabilities"}])

    def test_capability_versions_require_integers(self):
        for capability in [{"prepare_aggregate_version": True, "structured_plan_versions": [1]},
                           {"prepare_aggregate_version": 1, "structured_plan_versions": [True]}]:
            requests = []

            def invoke(payload):
                requests.append(payload)
                return capability, {}

            with self.assertRaises(fanout.Refused):
                fanout.run(invoke, {"inputs": [FULL], "aggregate": "count(*) AS c"}, 4)
            self.assertEqual(requests, [{"operation": "capabilities"}])

    def test_split_is_contiguous_and_even(self):
        self.assertEqual(fanout.split([8], 3), [[(0, 0, 2)], [(0, 2, 5)], [(0, 5, 8)]])
        self.assertEqual(fanout.split([3, 0, 2], 2), [[(0, 0, 2)], [(0, 2, 3), (2, 0, 2)]])
        self.assertEqual(fanout.split([2], 5), [[(0, 0, 1)], [(0, 1, 2)]])
        self.assertEqual(fanout.split([0], 4), [])


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
