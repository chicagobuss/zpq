"""The site's scorecard export: `zpq-scorecard-v1` records from a bench run directory.

`python -m bench site-export RUN --run-id ID --out DIR` reads `RUN/header.json`, `RUN/journal.jsonl` and any
`RUN/rechecks/*/journal.jsonl` and writes what the site's importer reads:

- `composite.json`: one entry per (case, engine) cell with its median, min and max when all `reps` repetitions were
  accepted (status ok and validation ok) and none failed; a cell with a wrong, failed or timed-out repetition lists
  its failures and has no median; a cell short of accepted repetitions only is marked `incomplete`;
- `results.jsonl`: one attempt record per journal row, with the cell it belongs to, whether the environment was valid,
  whether the row was selected for the median, and the engine's version and binary digest from the header;
- `confirmations.jsonl`: the rows of every recheck cohort, unchanged;
- `status.json`: the counts and the cells still incomplete.

It reads saved records only: no engine runs, no network, no clock but the heartbeat (`now_ns` fixes it in tests).
Identifiers are derived from the lane, case and engine, so exporting the same run twice gives the same bytes.
"""
import collections
import hashlib
import json
import statistics
import time
from pathlib import Path

SCHEMA = "zpq-scorecard-v1"
HOT_LANES = ("hot", "hot-fresh")
BAD = ("wrong", "failed", "timeout")


def _lines(path):
    return [json.loads(x) for x in Path(path).read_text().splitlines() if x.strip()]


def _digest(text, n):
    return hashlib.sha256(text.encode()).hexdigest()[:n]


def export_lane(run, run_id, out, lane=None, candidate_sha=None, baseline_sha=None, reps=3, now_ns=None):
    """Write the four files for one run directory into `out`; returns the status record."""
    run, out = Path(run), Path(out)
    head = json.loads((run / "header.json").read_text())
    lane = lane or head["lane"]
    candidate_sha = candidate_sha or head.get("sha")
    baseline_sha = baseline_sha or head.get("baseline_sha")
    rows = _lines(run / "journal.jsonl")
    groups = collections.defaultdict(list)
    for r in rows:
        groups[(r["case"], r["engine"])].append(r)
    composite, attempts = [], []
    for (case, engine), rr in sorted(groups.items()):
        suite, dataset, query_id = case.split("/", 2)
        ok = [r for r in rr if r["status"] == "ok" and r["validation"] == "ok"]
        bad = [r for r in rr if r["status"] in BAD]
        complete = len(ok) == reps and not bad
        cell = _digest(lane + "/" + case + "/" + engine, 24)
        entry = dict(suite=suite, dataset=dataset, query_id=query_id, engine=engine, lane="fresh-process",
                     session_policy="fresh process", schema=SCHEMA, run_id=run_id, checked=len(ok),
                     attempts=len(rr), failures=sorted({r["status"] for r in bad}), unsupported=0,
                     metric="wall_ns", expected_reps=reps)
        if complete:
            times = [r["elapsed_ns"] for r in ok]
            entry.update(median_ns=statistics.median(times), min_ns=min(times), max_ns=max(times))
        elif not bad:
            entry.update(incomplete=f"{len(ok)}/{reps} accepted")
        composite.append(entry)
        engine_head = head["engines"][engine]
        for r in rr:
            valid = r["status"] != "disturbed" and (complete or (bool(bad) and r["status"] != "ok"))
            attempts.append(dict(
                schema=SCHEMA, type="attempt", run_id=run_id, attempt_id=_digest(cell + "/" + str(r["rep"]), 32),
                cell_id=cell, suite=suite, dataset=dataset, query_id=query_id, engine=engine,
                engine_version=engine_head.get("version"), binary_sha256=engine_head.get("sha256"),
                candidate_sha=candidate_sha, baseline_sha=baseline_sha, rep=r["rep"], round=r["round"],
                mode="hot" if lane in HOT_LANES else "cold", lane="fresh-process", source_lane=lane,
                session_policy="fresh process", target="local", status=r["status"], validation=r.get("validation"),
                validation_method=r.get("method"), check_detail=r.get("check"),
                reason=r.get("reason") or r.get("check") or "", env_validity=valid,
                environment_reasons=r.get("reasons") or (["incomplete cohort"] if not complete and not bad else []),
                contention_valid=not r.get("foreign", {}).get("contended", False),
                selected=complete and r["status"] == "ok", expected_reps=reps, wall_ns=r.get("elapsed_ns"),
                owned_cpu_ns=r.get("cpu_ns"), rss_kib=r.get("rss_kib"), rss_scope="per-process-high-water",
                read_bytes=r.get("read_bytes"), rows=r.get("rows"), bytes_out=r.get("bytes_out"), exit=r.get("rc"),
                timestamp_ns=r.get("at_ns"),
                answer_hash=dict(content_sha256=r.get("answer_sha256"), bytes_sha256=None,
                                 scheme="native-canonical-fingerprint"),
                artifact_sha256=None, origin="measured", timer_scope="fresh-process:spawn-through-capture-and-reap"))
    confirmations = []
    for path in sorted((run / "rechecks").glob("*/journal.jsonl")):
        confirmations += _lines(path)
    incomplete = [e for e in composite if "incomplete" in e]
    status = dict(
        schema=SCHEMA, run_id=run_id, phase="finished-incomplete" if incomplete else "finished",
        finished=len(composite), complete=sum(1 for e in composite if e.get("median_ns") is not None),
        cells=len(composite), attempts=len(attempts),
        environment_rejections=sum(r["status"] == "disturbed" for r in rows),
        deferred=[e["suite"] + "/" + e["dataset"] + "/" + e["query_id"] + "/" + e["engine"] for e in incomplete],
        profile=lane, heartbeat_ns=time.time_ns() if now_ns is None else now_ns)
    out.mkdir(parents=True, exist_ok=True)
    (out / "composite.json").write_text(json.dumps(composite, indent=2) + "\n")
    (out / "results.jsonl").write_text("".join(json.dumps(x, sort_keys=True) + "\n" for x in attempts))
    (out / "confirmations.jsonl").write_text("".join(json.dumps(x, sort_keys=True) + "\n" for x in confirmations))
    (out / "status.json").write_text(json.dumps(status, indent=2) + "\n")
    return status
