#!/usr/bin/env python3
"""Row order is part of zpq's contract: check it across thread counts.

  tools/order_check.py ZPQ [WORKDIR]

For every output that lists rows (NDJSON and JSON print, -o writes plain, filtered, projected and recompressed, several
inputs) the output must be byte-identical at -j 1, 2, 8 and 32 and list rows in the input's order: files in the order
given, then row groups, then rows (pyarrow reading the same inputs in the same order is the reference). GROUP BY must
list groups where they first appear in the input and give the same bits for every aggregate at every -j, while
--unordered must give the same groups and integer values. Set ZPQ_IO_PROFILE to run the checks on one I/O backend.
"""
import json
from cli_output import read_rows
import os
import subprocess
import sys
import tempfile

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

THREADS = (1, 2, 8, 32)


def make_inputs(d):
    """Two files of many small row groups, keys first appearing out of byte order, floats of mixed magnitudes."""
    import random
    rnd = random.Random(11)
    keys = ["zz", "m", "a", "q", "b", "k", "c"]
    paths = []
    for f in range(2):
        n = 3000
        live = lambda i: keys[: 2 + (i * len(keys)) // n]  # noqa: E731
        ks = [rnd.choice(live(i)) for i in range(n)]
        t = pa.table({
            "id": pa.array([f * 10_000 + i for i in range(n)], pa.int64()),
            "k": ks,
            "v": pa.array([rnd.choice([1e15, 1.0, 1e-4, 3e7]) * (rnd.random() - 0.5) for _ in range(n)]),
            "s": [f"r{f}-{i}" for i in range(n)],
            "n": pa.array([None if i % 13 == 0 else i % 97 for i in range(n)], pa.int32()),
        })
        p = os.path.join(d, f"in{f}.parquet")
        pq.write_table(t, p, row_group_size=137)
        paths.append(p)
    return paths


def make_group_input(d):
    """Big enough (~1M rows) that a scan runs on several workers, so where partial float sums meet can vary: each
    row group starts with a huge value and continues with small ones, which a running sum drops and a fresh one keeps."""
    import random
    rnd = random.Random(3)
    keys = ["zz", "m", "a", "q"]
    n, rg = 1 << 20, 1 << 14
    ks = [keys[min(len(keys) - 1, (i // rg) % 7)] if i % rg else keys[(i // rg) % len(keys)] for i in range(n)]
    vs = [(1e16 if (i // rg) % 2 else -1e16) if i % rg == 0 else rnd.random() for i in range(n)]
    p = os.path.join(d, "groups.parquet")
    pq.write_table(pa.table({"k": ks, "v": vs}), p, row_group_size=rg)
    return p


def run(zpq, args, out=None):
    p = subprocess.run([zpq, "query", *args], capture_output=True)
    if p.returncode != 0:
        raise SystemExit(f"zpq {' '.join(args)} failed: {p.stderr.decode()[-500:]}")
    if out is not None:
        assert not p.stdout, p.stdout
    return p.stdout if out is None else open(out, "rb").read()


def main():
    zpq = os.path.abspath(sys.argv[1])
    d = sys.argv[2] if len(sys.argv) > 2 else tempfile.mkdtemp(prefix="zpq-order.")
    os.makedirs(d, exist_ok=True)
    paths = make_inputs(d)
    ref = pa.concat_tables([pq.read_table(p) for p in paths])
    bad = 0

    def same_everywhere(name, args, out=None, answer=False):
        """`answer`: compare parsed rows across worker counts."""
        nonlocal bad
        outs = {}
        for j in THREADS:
            dst = os.path.join(d, f"{name}.j{j}.parquet") if out else None
            outs[j] = run(zpq, [*args, "-j", str(j)] + (["-o", dst] if dst else []), dst)
            if answer:
                outs[j] = json.dumps(read_rows(outs[j].decode())).encode()
        if len(set(outs.values())) != 1:
            bad += 1
            print(f"FAIL {name}: output differs across -j {THREADS}")
        return outs[1], (os.path.join(d, f"{name}.j1.parquet") if out else None)

    # Printed rows, in input order.
    for fmt in ("jsonl", "json"):
        body, _ = same_everywhere(f"print-{fmt}", [*paths, "--format", fmt, "--filter", "n >= 3"])
        want = ref.filter(pc.greater_equal(ref["n"], 3))["id"].to_pylist()
        if fmt == "jsonl":
            got = [json.loads(line)["id"] for line in body.decode().splitlines() if line.strip()]
        else:
            got = [row["id"] for row in json.loads(body)]
        if got != want:
            bad += 1
            print(f"FAIL print-{fmt}: rows out of input order")

    # Written rows: plain copy, filtered, projected, recompressed; read back in order.
    writes = {
        "copy": ([*paths], None),
        "filtered": ([*paths, "--filter", "n >= 3 AND k != 'a'"],
                     pc.and_(pc.greater_equal(ref["n"], 3), pc.not_equal(ref["k"], "a"))),
        "projected": ([*paths, "--columns", "s,id"], None),
        "recompressed": ([*paths, "--codec", "zstd", "--filter", "id >= 5"], pc.greater_equal(ref["id"], 5)),
    }
    for name, (args, mask) in writes.items():
        _, out = same_everywhere(f"write-{name}", args, out=True)
        got = pq.read_table(out)["id"].to_pylist()
        want = (ref.filter(mask) if mask is not None else ref)["id"].to_pylist()
        if got != want:
            bad += 1
            print(f"FAIL write-{name}: rows out of input order")

    # GROUP BY: first-appearance order, same bits everywhere; --unordered same groups and integers.
    for name, inputs, agg in (
        ("group-by", paths,
         ["--aggregate", "sum(v) AS sv, avg(v) AS av, avg_state(v) AS st, count(*) AS c, min(s) AS ms"]),
        ("group-by-wide", [make_group_input(d)], ["--aggregate", "sum(v) AS sv, avg(v) AS av, count(*) AS c"]),
    ):
        agg = [*agg, "--group-by", "k"]
        body, _ = same_everywhere(name, [*inputs, *agg], answer=True)
        rows = json.loads(body)
        first_seen = []
        for t in inputs:
            for k in pq.read_table(t, columns=["k"])["k"].to_pylist():
                if k not in first_seen:
                    first_seen.append(k)
        got_keys = [r["k"] for r in rows]
        if got_keys != first_seen:
            bad += 1
            print(f"FAIL {name}: groups {got_keys}, want first appearance {first_seen}")
        un = read_rows(run(zpq, [*inputs, *agg, "--unordered", "-j", "32"]).decode())
        if sorted((r["k"], r["c"]) for r in un) != sorted((r["k"], r["c"]) for r in rows):
            bad += 1
            print(f"FAIL {name} --unordered: different groups or counts")

    print(f"order check: {'ok' if bad == 0 else f'{bad} failures'} (-j {', '.join(map(str, THREADS))})")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
