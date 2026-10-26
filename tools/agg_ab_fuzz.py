#!/usr/bin/env python3
"""A/B aggregate fuzz for encoded execution: random filter x aggregate x GROUP BY queries, each run twice through the
same zpq binary, once as is and once with ZPQ_ENCODED=0 (values materialised, row-by-row folds), over fixtures built to
reach the encoded paths: dictionary columns with nulls, a mid-chunk dictionary fallback, V2 pages, small pages, a page
index, unsigned and decimal columns. Both runs must give the same answer (floats to 1e-9 relative: a histogram sums
count x value) or fail alike.

Usage: .venv/bin/python tools/agg_ab_fuzz.py [--zpq zig-out/bin/zpq] [--queries 600] [--seed 1234] [--dir DIR]
"""
import argparse, decimal, json, os, random, subprocess, sys, tempfile
import numpy as np, pyarrow as pa, pyarrow.parquet as pq


def norm_result(r, group):
    if isinstance(r, list):
        return sorted(r, key=lambda row: tuple(str(row[g]) for g in group))
    return r


def same(a, b):
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(same(a[k], b[k]) for k in a)
    if isinstance(a, bool) or isinstance(b, bool):
        return type(a) is bool and type(b) is bool and a == b
    if isinstance(a, int) and isinstance(b, int):
        return a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return abs(a - b) <= 1e-9 * max(abs(a), abs(b), 1.0)
    return a == b

def fixtures(D):
    rng = np.random.default_rng(7)
    n = 200_000
    def nullify(a, p):
        return pa.array(a, mask=rng.random(n) < p)
    strs = np.array(["alpha", "beta", "", "delta", "eps", "zeta", "eta"])
    t = pa.table({
        "k1": nullify(strs[rng.integers(0, 5, n)], 0.1),
        "k2": pa.array(rng.integers(-3, 4, n).astype(np.int32)),
        "k3": pa.array(rng.random(n) < 0.3),
        "vi": nullify(rng.integers(-50, 50, n).astype(np.int64) * 1_000_000_007, 0.05),
        "vf": pa.array(np.round(rng.normal(0, 10, n), 1)),
        "vn": nullify(np.round(rng.normal(0, 10, n), 2), 0.2),
        "dec": pa.array([decimal.Decimal(int(x)) / 100 for x in rng.integers(0, 2000, n)], pa.decimal128(10, 2)),
        "plain": pa.array(rng.normal(0, 1, n)),
        "s": nullify(np.array([f"s{i:03d}" for i in range(300)])[rng.integers(0, 300, n)], 0.15),
        "u32": pa.array(rng.choice(np.array([0, 1, 2**31, 2**32 - 1], dtype=np.uint32), n)),
        "u64": pa.array(rng.choice(np.array([0, 5, 2**63, 2**64 - 1], dtype=np.uint64), n)),
        "mid": pa.array(rng.integers(0, 3000, n).astype(np.int64)),
        "ts": pa.array(np.cumsum(rng.integers(0, 3, n)).astype(np.int64)),
        "allnull": pa.array([None] * n, pa.float64()),
    })
    out = []
    for name, kw in [("v1", dict(data_page_version="1.0")),
                     ("v2", dict(data_page_version="2.0")),
                     ("fallback", dict(dictionary_pagesize_limit=8 * 1024, data_page_size=16 * 1024)),
                     ("small_pages", dict(data_page_size=4 * 1024, write_page_index=False)),
                     ("pidx", dict(write_page_index=True, data_page_size=8 * 1024))]:
        p = os.path.join(D, f"{name}.parquet")
        if not os.path.exists(p):
            pq.write_table(t, p, row_group_size=60_000, compression="snappy", **kw)
        out.append(p)
    return out

NUM = ["vi", "vf", "vn", "dec", "plain", "k2", "u32", "u64", "mid", "ts", "allnull"]
STR = ["k1", "s"]
FILTERS = ["k1 = 'beta'", "k1 != 'alpha'", "k1 LIKE 'e%'", "k1 IS NULL", "k1 IS NOT NULL", "k2 >= 0", "k2 = -3",
           "vi < 0", "vf > 2.5", "vn <= -1.0", "dec >= 10.5", "dec < 3", "s >= 's150'", "s IN ('s001', 's299')",
           "u32 >= 2147483648", "u64 > 9223372036854775807", "mid BETWEEN 100 AND 400", "k3 = true",
           "plain > 0.5", "allnull IS NULL", "vn IS NULL", "ts < 1000", "s NOT LIKE 's1%'"]
GROUPS = ["k1", "k2", "s", "k3", "u32", "dec", "vn"]

def rand_query(r):
    aggs = []
    for i in range(r.randint(1, 4)):
        f = r.choice(["count", "sum", "min", "max", "avg", "avg_state", "countstar"])
        if f == "countstar":
            a = "count(*)"
        elif f in ("min", "max", "count") and r.random() < 0.3:
            a = f"{f}({r.choice(STR)})"
        else:
            c = r.choice(NUM)
            # Arithmetic, over nullable operands too (`vn`, `vi`): a NULL on either side is NULL, which folds skip.
            if r.random() < 0.2 and f in ("sum", "min", "max", "avg", "avg_state", "count"):
                c = f"{r.choice(['vf', 'dec', 'k2', 'vn', 'vi'])} {r.choice(['*', '+', '-'])} {r.choice(['vf', 'k2', 'mid', 'vn'])}"
            a = f"{f}({c})"
        if r.random() < 0.15:
            a += f" FILTER (WHERE {r.choice(FILTERS)})"
        aggs.append(f"{a} AS a{i}")
    args = ["--aggregate", ", ".join(aggs)]
    if r.random() < 0.6:
        fs = r.sample(FILTERS, r.randint(1, 3))
        joiner = " OR " if r.random() < 0.2 else " AND "
        args += ["--filter", joiner.join(fs)]
    group = []
    if r.random() < 0.45:
        group = r.sample(GROUPS, r.randint(1, 2))
        args += ["--group-by", ", ".join(group)]
    return args, group

def run(b, path, args, env):
    p = subprocess.run([b, "query", path] + args, capture_output=True, text=True, env=env)
    if p.returncode != 0:
        return ("ERR", (p.stderr.strip().splitlines() or [""])[-1][:200])
    return ("OK", [json.loads(line) for line in p.stdout.splitlines() if line.strip()])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--zpq", default="zig-out/bin/zpq")
    ap.add_argument("--queries", type=int, default=600)
    ap.add_argument("--seed", type=int, default=1234)
    ap.add_argument("--dir", default=None, help="fixture directory (kept); default a temporary one")
    a = ap.parse_args()
    tmp = None
    if a.dir:
        os.makedirs(a.dir, exist_ok=True)
        d = a.dir
    else:
        tmp = tempfile.TemporaryDirectory(prefix="zpq-aggfuzz-")
        d = tmp.name
    files = fixtures(d)
    r = random.Random(a.seed)
    on = dict(os.environ)
    on.pop("ZPQ_ENCODED", None)
    off = dict(on, ZPQ_ENCODED="0")
    bad = ok = errs = 0
    for _ in range(a.queries):
        args, group = rand_query(r)
        path = r.choice(files)
        extra = r.choice([[], [], ["-j", "1"], ["--scan-all"], ["--trust-stats"]])
        x = run(a.zpq, path, args + extra, off)
        y = run(a.zpq, path, args + extra, on)
        if x[0] == "ERR" or y[0] == "ERR":
            if x != y:
                bad += 1
                print("MISMATCH(err)", os.path.basename(path), args + extra, "\n   off:", x, "\n   on: ", y)
            else:
                errs += 1
            continue
        X, Y = norm_result(x[1], group), norm_result(y[1], group)
        if not same(X, Y):
            bad += 1
            print("MISMATCH", os.path.basename(path), args + extra, "\n   off:", json.dumps(X)[:500],
                  "\n   on: ", json.dumps(Y)[:500])
        else:
            ok += 1
    print(f"agg_ab_fuzz: {ok} agree, {errs} error-for-error, {bad} mismatches out of {a.queries}")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
