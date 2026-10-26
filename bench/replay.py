"""Rescore a scorecard-v1 run (tools/verify's harness, e.g. granite's cold lanes) under this bench's rules.

A replay cannot re-run anything, so a disturbed case is quarantined at once (max_round 0). Old records carry no
PSI, so every wall-time signature counts as a stall and the lane is at best UNCERTIFIED. Only the latest attempt
of each (case, engine, rep) counts, as in the old summary.
"""

import json
from pathlib import Path


def convert(old, out):
    config = json.loads((old.parent / f"{old.name}.config.json").read_text())
    latest = {}
    for line in (old / "results.jsonl").read_text().splitlines():
        r = json.loads(line)
        if r.get("type") == "attempt":
            latest[(r["suite"], r["dataset"], r["query_id"], r["engine"], r["rep"])] = r
    records = []
    for (suite, dataset, qid, engine, rep), r in sorted(latest.items()):
        ok = r["status"] == "ok" and r.get("validation") == "ok"
        status = ("ok" if ok else "wrong" if r.get("validation") == "wrong" else
                  "disturbed" if r["status"] == "environment-rejected" else "failed")
        records.append({"case": f"{suite}/{dataset}/{qid}", "suite": suite, "engine": engine, "lane": "cold",
                        "rep": rep, "round": 0, "status": status, "elapsed_ns": r.get("wall_ns"),
                        "wall_ns": r.get("wall_ns"), "reason": r.get("reason") or r.get("check_detail"),
                        "validation": r.get("validation"), "psi": None, "read_bytes": None,
                        "reasons": r.get("environment_reasons") or []})
    zpq = config["candidates"]["zpq"]
    header = {"schema": "zpq-bench-v1", "lane": "cold", "reps": config["reps"], "host": old.name.split("-")[0],
              "sha": zpq["revision"], "psi": False, "max_round": 0, "replay_of": str(old),
              "engines": {n: {"version": e.get("version")} for n, e in config["engines"].items()}}
    out = out / f"replay-{old.name}"
    out.mkdir(parents=True, exist_ok=True)
    (out / "header.json").write_text(json.dumps(header, indent=1, sort_keys=True) + "\n")
    (out / "journal.jsonl").write_text("".join(json.dumps(r, sort_keys=True) + "\n" for r in records))
    planned = sorted({f"{c['suite']}/{c['dataset']}/{c['query_id']}" for c in config["cases"]})
    (out / "planned.json").write_text(json.dumps(planned) + "\n")
    return out
