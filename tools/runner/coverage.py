#!/usr/bin/env python3
"""Run the retired a01ec00 differential and inventory every counted check.

Usage: coverage.py LEGACY_CHECKOUT OUTPUT.json
The legacy checkout must contain the a01ec00 engine binary and fixtures. Only
the comparator is borrowed from this tree, including its precision fixes.
Every `total += 1` is instrumented, including the unprinted Parquet check.
"""

import ast
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
BASE = "a01ec00edd202bc80d5510131b36ce0a9f1f4bb7"
PRODUCTS = ("where-offset", "where-rare", "all", "having", "tie", "empty")


def legacy_key(line, v):
    # Source sites are pinned to BASE; a changed or unaccounted site fails.
    templates = {
        886: "{flabel}/{plabel}", 950: "select:{slabel}/{flabel}", 984: "agg:{alabel}:{flabel}",
        1046: "groupby:{glabel}:{flabel}", 1096: "groupby-o:{glabel}:{flabel}", 1169: "ordered:{olabel}",
        1206: "spill:{slabel}:g{groups}:j{j}", 1244: "distinct:{dlabel}:p{pairs}:j{j}",
        1302: "having:{i}:j{j}", 1360: "top:{i}:{fmt}", 1413: "top:ties:{order}:{tail}:j{j}",
        1444: "top:gather:{order}:{where_or_all}:{tail}:j{j}", 1479: "top:fallible:{scan_mode}",
        1509: "cut-errors:{i}", 1533: "cut-budget:-j{j}", 1544: "top-rows:budget",
        1555: "group-by:column-before-alias", 1592: "sum-offset:{offset_i}{offset_mode}", 1627: "avg-exact",
        1656: "plain-keys:{i}", 1684: "avg-u64:{i}:encoded-{encoded}", 1722: "key-framing:{i}",
        1770: "typed-o:{col}", 1817: "decimal-o:{dlabel}", 1862: "sliced:{blabel}",
        1894: "sliced:tiny-budget", 1902: "sliced:int96-float16", 1936: "levels-only:{elabel}:{col}",
        1964: "name-clash:{clabel}", 1986: "agg-names:{nlabel}", 2011: "row-order", 2029: "select-order",
        462: "stats:{label}:jsonl", 472: "stats:{label}:parquet", 486: "stats:{label}:{agg_mode}",
        502: "stats:select_u64_alias:{fmt}", 525: "stats:output_orders:{olabel}",
        539: "stats:output_orders:string", 557: "stats:output_orders:mixed", 568: "stats:select_u64_arith",
        735: "dense:{qi}", 767: "dense:many-calls:{n_calls}:{mem}:-j{j}", 776: "dense:budget:{mem}",
        827: "product:{product_shape}", 847: "product:where-offset", 861: "product:{kind}:{mem}:j{j}",
    }
    variant = ""
    if line == 293:
        dataset = "diff" if v["path"] == v["fixture"] else "unit"
        predicates = v["SQL_PRED_DIFF"] if dataset == "diff" else v["SQL_PRED_UNIT"]
        return f"sql_pred:{dataset}:{v['form']}:{predicates.index(v['where'])}", f"j={v['j']}"
    if line == 586:
        shape, check = v["label"].split(" × ")
        shape = {"flba re-encode": "flba-reencode", "floats re-encode": "floats-reencode",
                 "floats recompress": "floats-recompress", "flba copy columns": "flba-copy"}[shape]
        check = {"pyarrow values": "pyarrow", "duckdb values": "duck", "total_byte_size": "bytes",
                 "FLBA encodings": "encodings", "float bounds": "bounds"}[check]
        return f"wire:{shape}:{check}", ""
    values = {**v, "where_or_all": v.get("where") or "all",
              "scan_mode": "scan-all" if v.get("extra") else "default",
              "offset_i": v.get("i", 0) // len(v.get("modes") or [None]),
              "offset_mode": {"": "", " (row folds)": "-rows", " (trust-stats)": "-trust"}.get(v.get("mode")),
              "agg_mode": "agg-trust" if v.get("flags") else "agg",
              "product_shape": PRODUCTS[v["qi"]] if line == 827 else ""}
    if line == 1509:
        variant = f"j={v['j']}"
    elif line == 735:
        variant = f"dense={v['dense']},j={v['j']}"
    elif line == 827:
        variant = f"{v['name']},j={v['j']}"
    elif line == 847:
        variant = "parquet"
    return templates[line].format_map(values), variant


def case_keys(case):
    kind = case.get("kind", "write")
    jobs = case.get("jobs") or [0]
    if kind == "product":
        variants = ["legacy,j=1"] + [f"dense,j={j}" for j in (1, 2, 4, 8, 16)]
        variants += [f"windows,j={j}" for j in (1, 4, 16)] + [f"values,j={j}" for j in (1, 16)]
        if case.get("parquet"):
            variants += ["parquet"]
    elif kind == "dense":
        variants = [f"dense={d},j={j}" for d in ("0", "1") for j in jobs]
    elif kind == "sql" or kind in ("sql_rows", "sql_ordered") and len(jobs) > 1:
        variants = [f"j={j}" for j in jobs]
    else:
        variants = [""]
    return [(case["id"], variant) for variant in variants]


def main():
    legacy, output = Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve()
    source = subprocess.check_output(["git", "show", f"{BASE}:tools/differential.py"], cwd=ROOT, text=True)
    if (legacy / "tools/differential.py").read_text() != source:
        raise SystemExit("legacy source differs from pinned baseline")
    if subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=legacy, text=True).strip() != BASE:
        raise SystemExit("legacy engine checkout differs from pinned baseline")
    observed, sites = [], set()

    def inventory(line, local):
        scope = {**namespace, **local}
        key = legacy_key(line, scope)
        observed.append({"line": line, "id": key[0], "variant": key[1]})

    class Instrument(ast.NodeTransformer):
        def visit_AugAssign(self, node):
            if isinstance(node.target, ast.Name) and node.target.id == "total" and isinstance(node.value, ast.Constant):
                if node.value.value != 1:
                    raise ValueError(f"unknown counter at {node.lineno}")
                sites.add(node.lineno)
                call = ast.parse(f"__inventory({node.lineno}, locals())").body[0]
                return [node, ast.copy_location(call, node)]
            return node

    tree = ast.fix_missing_locations(Instrument().visit(ast.parse(source)))
    sys.path[:0] = [str(ROOT / "tools"), str(legacy / "tools")]
    sys.argv = [str(legacy / "tools/differential.py")]
    os.chdir(legacy)
    namespace = {"__name__": "__main__", "__file__": sys.argv[0], "__inventory": inventory}
    rc = 0
    try:
        exec(compile(tree, sys.argv[0], "exec"), namespace)
    except SystemExit as error:
        rc = error.code or 0
    wanted, extra, files = [], [], {}
    for path in sorted((ROOT / "tools/runner/cases").glob("*.json")):
        cases = json.loads(path.read_text())["cases"]
        keys = [key for case in cases for key in case_keys(case)]
        (extra if path.stem == "oracle_precision" else wanted).extend(keys)
        files[path.name] = len(keys)
    actual = Counter((row["id"], row["variant"]) for row in observed)
    expected = Counter(wanted)
    missing, unexpected = list((expected - actual).elements()), list((actual - expected).elements())
    unseen_sites = sorted(sites - {row["line"] for row in observed})
    report = {"base": BASE, "legacy_sha256": hashlib.sha256(source.encode()).hexdigest(),
              "legacy_exit": rc, "legacy_checks": len(observed), "case_files": files,
              "new_checks": extra, "missing": missing, "unexpected": unexpected,
              "unexecuted_counter_sites": unseen_sites, "checks": observed}
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"coverage: {len(observed)} retired checks; missing={len(missing)} extra={len(unexpected)} "
          f"unexecuted_sites={len(unseen_sites)}; {len(extra)} new checks; {output}", flush=True)
    return 1 if rc or missing or unexpected or unseen_sites else 0


if __name__ == "__main__":
    sys.exit(main())
