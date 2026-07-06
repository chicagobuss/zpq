#!/usr/bin/env python3
"""Metamorphic self-oracle tests for ZPQ — no external engine as referee.

Two invariants that must hold *regardless of the right answer*, so they catch
silent-wrong-result bugs that a structure-only check (or a missing oracle)
would wave through:

  scan-all : `zpq query ...`  ==  `zpq query ... --scan-all`
             The default path takes stats shortcuts (row-group pruning,
             stats-as-answer); --scan-all disables them and byte-decodes every
             page. They must agree on every query — a divergence means a
             pruning / stats-as-answer bug is emitting wrong answers while the
             slow path is right (or vice-versa). This is the scariest class:
             the fast path lies and nothing else would notice.

  location : `zpq query <local> ...`  ==  `zpq query <s3://...> ...`
             The mmap path and the S3-range path read the same bytes, so
             results must be identical. A divergence is a range-fetch /
             footer-fetch / coalescing bug. Needs --s3 (and the S3_* env the
             SigV4 client reads). Run under --scan-all so the full range path
             is actually exercised rather than short-circuited from the footer.

ZPQ is its own oracle in both: we never ask what the answer *is*, only that
ZPQ agrees with itself across configurations. Random ops are generated exactly
like corpus_diff.py (seeded, schema-derived via DuckDB — DuckDB writes the
queries, it does not referee them), so a failure reproduces with --seed and
prints the offending query.

Usage:
  # scan-all parity, pure local (no network, no S3 creds):
  .venv/bin/python tools/metamorphic.py --local data/parquet-testing/data

  # both invariants, ZPQ also reading the same corpus over the network:
  .venv/bin/python tools/metamorphic.py \
      --local data/parquet-testing/data \
      --s3 s3://rustfs-test-bucket/corpus/parquet-testing \
      --check both --seed 1 --ops 8
"""
import argparse
import json
import os
import random
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# Reuse corpus_diff's seeded, schema-derived query generation and the
# value-normalization helpers — same query distribution, so a metamorphic
# failure and a differential failure speak the same language.
from corpus_diff import columns_of, gen_ops, fold_avg, duck_agg  # noqa: E402
from triangulate import compare_values, ZPQ_BIN  # noqa: E402

try:
    import polars as pl
except ImportError:
    pl = None

TIMEOUT_S = 60


# --- oracle panel (no pyarrow): adjudicates a metamorphic violation by asking
# independent engines the same query. The point of >1 oracle: when the oracles
# themselves disagree, the divergence is *semantics*, not a ZPQ bug — a single
# referee would misassign blame (it did, on the NaN-max case). DuckDB = C++,
# Polars = Rust (its own parquet engine). Both are SQL, both read the local file.

def _scalar(payload):
    """An agg dict has exactly one result column; reduce to its scalar
    (avg folded from {sum,count}) so engines compare apples-to-apples."""
    if not payload:
        return None
    (v,) = payload.values()
    return fold_avg(v)


def polars_agg(path: str, agg: str, where: str | None):
    if pl is None:
        return ("skip", "polars not installed")
    sql = f"SELECT {agg} FROM t" + (f" WHERE {where}" if where else "")
    try:
        rows = pl.SQLContext(t=pl.scan_parquet(path)).execute(sql).collect().to_dicts()
        return ("ok", _scalar(rows[0] if rows else {}))
    except Exception as e:  # noqa: BLE001 — any reader/SQL failure = this engine abstains
        return ("error", f"{type(e).__name__}: {str(e)[:60]}")


def duck_panel(path: str, agg: str, where: str | None):
    try:
        d = duck_agg(path, agg, where)
        return ("ok", _scalar(d)) if d is not None else ("error", "no-result")
    except Exception as e:  # noqa: BLE001
        return ("error", f"{type(e).__name__}: {str(e)[:60]}")


def classify(zf, zs, local, agg, where):
    """zf/zs are ZPQ (status, scalar_or_msg) for fast and --scan-all. Ask the
    oracle panel the same query and return (tag, detail)."""
    od, op = duck_panel(local, agg, where), polars_agg(local, agg, where)
    oks = [v for (s, v) in (od, op) if s == "ok"]
    panel_str = f"duck={od[1]!r} polars={op[1]!r} zpq_fast={zf[1]!r} zpq_scanall={zs[1]!r}"

    if len(oks) >= 2 and not compare_values(oks[0], oks[1]):
        return ("SEMANTICS", f"oracles disagree → no ground truth; {panel_str}")
    if not oks:
        return ("NO-ORACLE", f"no engine could read this column/op; {panel_str}")
    V = oks[0]  # consensus (or the single available oracle)

    fast_ok, full_ok = zf[0] == "ok", zs[0] == "ok"
    fast_hit = fast_ok and compare_values(zf[1], V)
    full_hit = full_ok and compare_values(zs[1], V)

    if fast_ok and full_ok:
        if fast_hit and not full_hit:
            return ("ZPQ-DECODE-BUG", f"decode path disagrees with oracles; {panel_str}")
        if full_hit and not fast_hit:
            return ("ZPQ-STATS-BUG", f"stats path disagrees with oracles; {panel_str}")
        return ("ZPQ-BOTH-WRONG", f"both ZPQ paths disagree with oracles; {panel_str}")
    # one ZPQ path errored — the classic stats-as-answer-over-undecodable case
    if full_ok and not fast_ok:
        return ("STATS-GAP", f"stats path errors, decode matches oracles; {panel_str}")
    # fast answered from stats, decode (--scan-all) errored
    if fast_hit:
        return ("STATS-LUCKY", f"decode-gap but stats value is CORRECT; {panel_str}")
    return ("STATS-WRONG", f"decode-gap AND stats value is WRONG (silent); {panel_str}")


def zpq_agg(path: str, agg: str, where: str | None, *, scan_all: bool):
    """Run one aggregate through ZPQ. Returns ("ok", agg_dict) or
    ("error", message) — the status tag matters: one config erroring while
    the other succeeds is itself a metamorphic violation."""
    cmd = [ZPQ_BIN, "query", path, "--aggregate", agg]
    if where:
        cmd += ["--filter", where]
    if scan_all:
        cmd += ["--scan-all"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=TIMEOUT_S)
    except subprocess.TimeoutExpired:
        return ("error", "timeout")
    if proc.returncode != 0:
        return ("error", (proc.stderr or proc.stdout).strip()[:120])
    try:
        return ("ok", json.loads(proc.stdout).get("agg"))
    except json.JSONDecodeError:
        return ("error", "json-decode")


def agg_equal(a, b) -> bool:
    """Compare two ZPQ agg dicts (avg folded from {sum,count}), NaN/float
    tolerant via triangulate.compare_values."""
    if a is None or b is None:
        return a is None and b is None
    if set(a) != set(b):
        return False
    return all(compare_values(fold_avg(a[k]), fold_avg(b[k])) for k in a)


def compare_runs(label, r_a, r_b):
    """Given two (status, payload) results that must match, return None if
    they agree or a human-readable reason string if they diverge."""
    (sa, pa), (sb, pb) = r_a, r_b
    if sa != sb:
        return f"{label}: status differs ({sa}:{pa!r} vs {sb}:{pb!r})"
    if sa == "error":
        return None  # both errored the same way — consistent, not our bug to judge here
    if not agg_equal(pa, pb):
        return f"{label}: values differ ({pa} vs {pb})"
    return None


def run(files, args):
    do_scan = args.check in ("scan-all", "both")
    do_loc = args.check in ("location", "both")
    if do_loc and not args.s3:
        print("location check needs --s3 (ZPQ reads the corpus over the network)", file=sys.stderr)
        sys.exit(2)

    checked = mism = skipped = 0
    failures = []
    for f in files:
        local = str(f)
        s3_path = f"{args.s3.rstrip('/')}/{f.name}" if args.s3 else None
        try:
            cols = columns_of(local)  # DuckDB introspects the schema to shape queries
        except RuntimeError:
            skipped += 1
            continue
        rng = random.Random(f"{args.seed}:{f.name}")  # stable across processes (tuple.__hash__ is salted)
        for agg, where in gen_ops(local, cols, args.ops, rng):
            if do_scan:
                checked += 1
                fast = zpq_agg(local, agg, where, scan_all=False)
                full = zpq_agg(local, agg, where, scan_all=True)
                reason = compare_runs("scan-all", fast, full)
                if reason:
                    mism += 1
                    if args.panel:
                        tag, detail = classify(_zscalar(fast), _zscalar(full), local, agg, where)
                    else:
                        tag, detail = "SCAN-ALL", reason
                    failures.append((f.name, agg, where, tag, detail))
            if do_loc:
                checked += 1
                # --scan-all on both: force the full range-fetch+decode path on
                # the remote side instead of a footer-only short-circuit. A
                # local-vs-remote divergence is unambiguously a ZPQ range-read
                # bug (same query, same bytes), so no oracle is needed.
                loc = zpq_agg(local, agg, where, scan_all=True)
                rem = zpq_agg(s3_path, agg, where, scan_all=True)
                reason = compare_runs("location", loc, rem)
                if reason:
                    mism += 1
                    failures.append((f.name, agg, where, "LOCATION-BUG", reason))
    return checked, mism, skipped, failures


def _zscalar(r):
    """Reduce a zpq_agg result to (status, scalar) for the panel."""
    return (r[0], _scalar(r[1])) if r[0] == "ok" else r


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--local", required=True, help="local corpus dir (schema introspection + mmap reads)")
    ap.add_argument("--s3", default=None, help="s3:// prefix ZPQ reads over the network (location check)")
    ap.add_argument("--check", choices=["scan-all", "location", "both"], default="scan-all")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--ops", type=int, default=8, help="random ops per file")
    ap.add_argument("--files", type=int, default=0, help="cap files (0 = all)")
    ap.add_argument("--no-panel", dest="panel", action="store_false",
                    help="just report self-inconsistency; skip the duckdb/polars adjudication")
    args = ap.parse_args()

    files = sorted(Path(args.local).glob("*.parquet"))
    if args.files:
        files = files[: args.files]
    if not files:
        print(f"no parquet under {args.local}", file=sys.stderr)
        return 2

    checked, mism, skipped, failures = run(files, args)
    print(f"\nmetamorphic[{args.check}]: seed={args.seed} files={len(files)} "
          f"(skipped {skipped} duckdb-unreadable) checks={checked} violations={mism}")

    # Group by verdict so real ZPQ bugs separate from semantics-divergence and
    # the known stats-as-answer-over-undecodable class.
    by_tag: dict[str, list] = {}
    for name, agg, where, tag, detail in failures:
        by_tag.setdefault(tag, []).append((name, agg, where, detail))
    # Real bugs first, then ambiguous, then known-gap classes.
    order = ["ZPQ-DECODE-BUG", "ZPQ-STATS-BUG", "ZPQ-BOTH-WRONG", "STATS-WRONG",
             "LOCATION-BUG", "SEMANTICS", "STATS-LUCKY", "STATS-GAP", "NO-ORACLE", "SCAN-ALL"]
    for tag in sorted(by_tag, key=lambda t: (order.index(t) if t in order else 99)):
        rows = by_tag[tag]
        print(f"\n[{tag}] x{len(rows)}")
        for name, agg, where, detail in rows:
            w = f" WHERE {where}" if where else ""
            print(f"  {name}: {agg}{w}\n    {detail}")

    if mism:
        bugs = sum(len(by_tag.get(t, [])) for t in
                   ("ZPQ-DECODE-BUG", "ZPQ-STATS-BUG", "ZPQ-BOTH-WRONG", "STATS-WRONG", "LOCATION-BUG"))
        print(f"\nverdict: {bugs} real ZPQ bug(s); {mism - bugs} semantics/known-gap. "
              f"Reproduce: --check {args.check} --seed {args.seed}")
        return 1 if bugs else 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
