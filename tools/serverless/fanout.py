#!/usr/bin/env python3
"""Fan one zpq aggregate out over N zpq-lambda workers and merge their answers.

The coordinator negotiates structured-plan v1 support, then asks the native frontend to prepare the original
aggregate request and read input row-group counts. It cuts those row groups into N contiguous runs and forwards
the prepared syntax and `row_groups` (docs/lambda_requests.md). COUNT and SUM add, MIN and MAX fold, and explicit
AVG partial states are divided after the merge. With `group_by` the merge is per group. ORDER BY and
LIMIT apply to the merged rows, never to a worker's. Anything whose parts cannot be merged (COUNT(DISTINCT),
percentiles and other holistic aggregates, window functions, expressions over aggregates) is refused during native preparation, before any partial-query execution.

    fanout.py --function zpq-worker --workers 32 --input s3://b/lineitem/ --aggregate "sum(x) AS s" ...

The same module is a coordinator Lambda handler (`fanout.handler`); the event names the worker function and the
query, as the CLI flags do (see `handler`). Pure stdlib, plus boto3 to invoke Lambda.
"""
import argparse
import base64
import concurrent.futures as cf
import decimal
import json
import re
import sys
import time

PLAN_VERSION = 1
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

def lambda_invoker(function, region=None, pool=256):
    """invoke(payload) -> (response, stats) through boto3 `lambda.invoke`."""
    import boto3
    from botocore.config import Config
    lam = boto3.client("lambda", region_name=region, config=Config(
        read_timeout=900, connect_timeout=10, retries={"total_max_attempts": 1}, max_pool_connections=pool))

    def invoke(payload):
        r = lam.invoke(FunctionName=function, Payload=json.dumps(payload).encode(), LogType="Tail")
        resp = loads(r["Payload"].read())
        stats = {"function_error": r.get("FunctionError")}
        m = REPORT_RE.search(base64.b64decode(r.get("LogResult", "")).decode(errors="replace"))
        if m:
            stats.update(duration_ms=float(m.group(1)), billed_ms=int(m.group(2)), memory_mb=int(m.group(3)),
                         max_memory_mb=int(m.group(4)), init_ms=float(m.group(5)) if m.group(5) else None)
        return resp, stats
    return invoke


def loads(raw):
    # Exact sums: a decimal answer stays a Decimal through the merge.
    return json.loads(raw, parse_float=decimal.Decimal)


def scatter(invoke, payloads):
    t0 = time.perf_counter()
    with cf.ThreadPoolExecutor(max(1, len(payloads))) as ex:
        out = list(ex.map(invoke, payloads))
    for resp, _ in out:
        if not isinstance(resp, dict) or "agg" not in resp:
            raise WorkerFailed(dumps(resp)[:600])
    return out, (time.perf_counter() - t0) * 1000


def prepare(invoke, request):
    """Require an exact native capability acknowledgement before sending inputs for preparation or execution."""
    cap, _ = invoke({"operation": "capabilities"})
    if (not isinstance(cap, dict) or type(cap.get("prepare_aggregate_version")) is not int
            or cap["prepare_aggregate_version"] != PLAN_VERSION or not isinstance(cap.get("structured_plan_versions"), list)
            or not any(type(v) is int and v == PLAN_VERSION for v in cap["structured_plan_versions"])):
        raise Refused("worker lacks structured aggregate plan v1; update the worker before using this coordinator")
    prepared, _ = invoke(dict(request, operation="prepare_aggregate", plan_version=PLAN_VERSION))
    if not isinstance(prepared, dict) or prepared.get("plan_version") != PLAN_VERSION or "plan" not in prepared:
        raise Refused(f"native aggregate preparation refused: {dumps(prepared)[:600]}")
    return prepared


def run(invoke, request, workers, order_by=None, limit=None, counts=None):
    """Answer `request` (a worker request with `inputs` and `aggregate`, optionally `group_by` and `filter`) over
    `workers` workers. Optional `counts` overrides scheduling counts; native preparation still validates footers."""
    t0 = time.perf_counter()
    if request.get("sql") or request.get("row_groups"):
        raise Refused("the coordinator takes `aggregate`/`group_by` requests, not `sql` or `row_groups`")
    if not isinstance(request.get("aggregate"), str) or not request["aggregate"].strip():
        raise Refused("nothing to merge: the request has no `aggregate`")
    prepared = prepare(invoke, request)
    aggs = [(a["function"], None, a["name"]) for a in prepared["aggregates"]]
    if any(fn not in MERGEABLE for fn, _, _ in aggs):
        raise Refused("native preparation returned an unsupported merge operation")
    group = [(None, name) for name in prepared["groups"]]
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
    out, scatter_ms = scatter(invoke, payloads)
    merged = merge([resp["agg"] for resp, _ in out], aggs, group)
    rows = order(merged, order_by, limit) if group else merged
    stats = [dict(s, total_ms=resp.get("total_ms"), rows_in=resp.get("rows_in"), bytes_in=resp.get("bytes_in"))
             for resp, s in out]
    return {"agg": rows, "workers": len(payloads), "footer_ms": footer_ms, "scatter_ms": scatter_ms,
            "e2e_ms": (time.perf_counter() - t0) * 1000, "worker_stats": stats}


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
