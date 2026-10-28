"""Export saved measurements without running engines, checking answers or changing source records."""

import collections
import datetime
import statistics
from pathlib import Path

from bench import requests, score


def load_run(spec, request):
    root = Path(spec["path"])
    evidence = []

    def read(name, lines=False):
        path = root / name
        evidence.append({"path": str(path), "sha256": requests.sha256(path.read_bytes())})
        return requests.read(path, lines)

    if (root / "header.json").is_file():
        header = read("header.json")
        if header.get("schema") != "zpq-bench-v1":
            raise ValueError("unsupported native header schema")
        planned = read("planned.json")
        if not isinstance(planned, list) or not all(isinstance(x, str) for x in planned):
            raise ValueError("native planned.json must list all planned case IDs")
        records = read("journal.jsonl", True) if (root / "journal.jsonl").exists() else []
        table = score.score(header, records, planned, header.get("max_round", 2))
        source_lane = header["lane"]
        candidates = {name: {"sha256": value.get("sha256")} for name, value in header["engines"].items()}
        expected = {case: list(header["engines"]) for case in planned}
        reps = {case: header["reps"] for case in planned}
    elif (root / "manifest.jsonl").is_file():
        manifest = read("manifest.jsonl", True)
        if not manifest or manifest[0].get("schema") != "zpq-scorecard-v1":
            raise ValueError("unsupported legacy manifest schema")
        header = manifest[0]
        cfg = header["config"]
        planned = ["/".join(str(c[k]) for k in ("suite", "dataset", "query_id")) for c in cfg["cases"]]
        expected = {case: c.get("engines", list(cfg["engines"])) for case, c in zip(planned, cfg["cases"])}
        reps = {case: c.get("reps", cfg.get("reps", 3)) for case, c in zip(planned, cfg["cases"])}
        records = read("results.jsonl", True) if (root / "results.jsonl").exists() else []
        table = None
        source_lane = cfg.get("profile")
        candidates = cfg.get("candidates", {})
    else:
        raise ValueError("run needs a native header or legacy manifest: " + str(root))
    if len(set(planned)) != len(planned):
        raise ValueError("duplicate planned case")
    if source_lane == "hot":
        normalized = "hot-fresh"
    else:
        normalized = source_lane
    if normalized != spec["lane"]:
        raise ValueError("source/request lane mismatch: " + spec["id"])
    if header.get("host") and header["host"] != request["host"]:
        raise ValueError("source/request host mismatch")
    for name, aliases in (("candidate", ("zpq",)), ("baseline", ("base", "zpq_prev"))):
        for alias in aliases:
            pin = candidates.get(alias, {})
            for field, source_key in (("sha", "revision"), ("binary_sha256", "sha256")):
                supplied, observed = request[name].get(field), pin.get(source_key)
                if supplied and observed and supplied != observed:
                    raise ValueError(f"source/request {name}.{field} mismatch")
    normalized_records = []
    for i, raw in enumerate(records):
        case = raw.get("case") or "/".join(str(raw[k]) for k in ("suite", "dataset", "query_id"))
        if case not in expected or raw["engine"] not in expected[case]:
            raise ValueError("attempt not declared in source plan")
        normalized_records.append({"case": case, "engine": raw["engine"], "raw": raw, "sequence": i})
    return {**spec, "header": header, "planned": planned, "expected": expected, "reps": reps,
            "records": normalized_records, "score": table, "evidence": evidence, "source_lane": source_lane}


def accepted(row):
    return (row.get("status") == "ok" and row.get("validation") == "ok" and
            row.get("env_validity") is not False and row.get("contention_valid") is not False)


def engine_summary(run, case, engine):
    records = [x for x in run["records"] if x["case"] == case and x["engine"] == engine]
    all_case = [x["raw"] for x in run["records"] if x["case"] == case]
    latest = max((x.get("round", 0) for x in all_case), default=0)
    # Legacy rounds rotate repetition order; only the native format uses rounds as replacement cohorts.
    cohort = [x for x in records if x["raw"].get("rep", 0) >= 1 and
              (run["score"] is None or x["raw"].get("round", 0) == latest)]
    good = [x for x in cohort if accepted(x["raw"])]
    identities = [x["raw"]["rep"] for x in good]
    if len(identities) != len(set(identities)):
        raise ValueError("duplicate accepted repetition in a cohort")
    bad = [x for x in cohort if x["raw"].get("status") in ("failed", "wrong", "incorrect", "timeout")]
    state = "failed" if bad else "complete" if len(good) == run["reps"][case] else "pending"
    if run["score"] is not None:
        if case in run["score"]["quarantined"]:
            state = "quarantined"
        elif case in run["score"]["pending_rerun"]:
            state = "pending"
    metric = "elapsed_ns" if run["score"] is not None else "wall_ns"
    times = [x["raw"][metric] for x in good]
    if any(type(t) not in (int, float) or t < 0 for t in times):
        raise ValueError("invalid elapsed time")
    summary = {"origin": "measured", "source_run": run["id"], "source_lane": run["source_lane"],
               "state": state, "expected_reps": run["reps"][case], "n": len(good), "metric_field": metric,
               "median_ms": statistics.median(times) / 1e6 if state == "complete" else None,
               "min_ms": min(times) / 1e6 if times else None, "max_ms": max(times) / 1e6 if times else None,
               "failures": [{"status": x["raw"]["status"], "reason": x["raw"].get("reason")} for x in bad],
               "oracle_verdicts": sorted({str(x["raw"].get("validation")) for x in cohort}),
               "raw_byte_sha256": sorted({h for x in good if (h := x["raw"].get("artifact_sha256") or
                                          x["raw"].get("answer_hash", {}).get("bytes_sha256"))}),
               "raw_hash_count": sum(bool(x["raw"].get("artifact_sha256") or
                                          x["raw"].get("answer_hash", {}).get("bytes_sha256")) for x in good)}
    return summary, {x["sequence"] for x in good} if state == "complete" else set()


def make_export(request_path):
    packet = requests.packet(request_path)
    request = packet["request"]
    if not request["runs"]:
        raise ValueError("export needs saved runs")
    runs = [load_run(spec, request) for spec in request["runs"]]
    choices = request.get("selection", {})
    cells, selections, accepted_sequences = [], {}, collections.defaultdict(set)
    all_cases = set(request["targets"] + request["controls"])
    declared_choices = {(lane, case) for lane, cases in choices.items() for case in cases}
    used_choices = set()
    for lane in sorted(request["lanes"]):
        lane_runs = [r for r in runs if r["lane"] == lane]
        planned = all_cases | {c for r in lane_runs for c in r["planned"]}
        for case in sorted(planned):
            available = [r for r in lane_runs if case in r["planned"]]
            chosen = choices.get(lane, {}).get(case)
            if chosen:
                used_choices.add((lane, case))
                available = [r for r in available if r["id"] == chosen]
                if not available:
                    raise ValueError("selection references a run that does not plan this case/lane")
            if len(available) > 1:
                raise ValueError("ambiguous cohort; select a whole case: " + lane + "/" + case)
            run = available[0] if available else None
            expected = set(request.get("expected_engines", []))
            if run:
                expected.update(run["expected"][case])
                selections[(lane, case)] = run["id"]
            engines = {}
            for engine in sorted(expected):
                if run and engine in run["expected"][case]:
                    engines[engine], seqs = engine_summary(run, case, engine)
                    accepted_sequences[run["id"]].update(seqs)
                else:
                    engines[engine] = {"origin": "measured", "state": "pending", "n": 0,
                                       "median_ms": None, "reason": "no planned result for this engine"}
            if not expected:
                raise ValueError("missing expected_engines for an unmeasured case")
            pair = [engines.get("zpq"), engines.get("base") or engines.get("zpq_prev")]
            hashes = [x.get("raw_byte_sha256", []) for x in pair if x]
            proven = (all(pair) and all(x["state"] == "complete" and x.get("raw_hash_count") == x["n"]
                                       for x in pair) and all(len(h) == 1 for h in hashes))
            cells.append({"case": case, "lane": lane, "engines": engines, "selected_run": run["id"] if run else None,
                          "byte_identical": hashes[0] == hashes[1] if proven else None,
                          "byte_identity_target": "candidate/base outputs", "oracle_byte_identical": None})
    if declared_choices - used_choices:
        raise ValueError("selection includes an unknown case")
    comparison_evidence = []
    for path in request.get("stored_summaries", []):
        rows = requests.read(path)
        if not isinstance(rows, list):
            raise ValueError("stored summaries must be a source composite list")
        comparison_evidence.append({"path": path, "sha256": requests.sha256(Path(path).read_bytes())})
        index = {(c["lane"], c["case"]): c for c in cells}
        for row in rows:
            case = "/".join(str(row[k]) for k in ("suite", "dataset", "query_id"))
            lane = row["lane"]
            if lane == "fresh-process":
                lane = "hot-fresh"
            target = index.get((lane, case))
            existing = target["engines"].get(row["engine"]) if target else None
            if not target or existing and existing["state"] != "pending":
                continue
            field = "legacy_median_ns" if "legacy_median_ns" in row else "median_ns"
            target["engines"][row["engine"]] = {"origin": "stored", "source_run": row["run_id"],
                "source_lane": row["lane"], "state": "stored", "n": row.get("checked"), "metric_field": field,
                "median_ms": row[field] / 1e6 if row.get(field) is not None else None,
                "failures": row.get("failures", []), "unsupported": row.get("unsupported", 0)}
    audit = []
    for run in runs:
        for record in run["records"]:
            selected = selections.get((run["lane"], record["case"])) == run["id"]
            raw = record["raw"]
            reason = ("replaced-cohort" if not selected else "warmup" if raw.get("rep", 0) < 1 else
                      "accepted" if record["sequence"] in accepted_sequences[run["id"]] else "excluded-or-incomplete")
            audit.append({"source_run": run["id"], "source_sequence": record["sequence"],
                          "selected_case_cohort": selected, "disposition": reason, "raw": raw})
    evidence = [e for run in runs for e in run["evidence"]] + comparison_evidence
    times = [x["raw"].get("timestamp_ns", x["raw"].get("at_ns")) for x in audit]
    times = [t for t in times if type(t) is int]
    stamp = lambda t: datetime.datetime.fromtimestamp(t / 1e9, datetime.timezone.utc).isoformat() if t else None
    checkers = {r["header"].get("zpq_check_sha256") for r in runs} - {None}
    report = {"schema": "zpq-bench-result/v1", "run_id": request["id"], "request_sha256": packet["request_sha256"],
              "host": request["host"], "requester": request["requester"], "runner": request.get("runner"),
              "candidate_sha": request["candidate"].get("sha"),
              "binary_sha256": request["candidate"].get("binary_sha256"),
              "baseline_sha": request["baseline"].get("sha"),
              "baseline_binary_sha256": request["baseline"].get("binary_sha256"),
              "corpus_manifest_sha256": request.get("corpus_manifest_sha256"),
              "query_set_sha256": request.get("query_set_sha256"), "evidence_class": "saved-local-scorecard",
              "start_utc": stamp(min(times)) if times else None, "end_utc": stamp(max(times)) if times else None,
              "timestamp_scope": "bounds of recorded attempt timestamps; not host quiet-window bounds",
              "harness_revision": request.get("harness_revision"),
              "checker_sha256": next(iter(checkers)) if len(checkers) == 1 else None,
              "method": {"source_contracts": {r["id"]: r["header"].get("config", {}).get("measurement_contract")
                                              for r in runs}, "selection": choices},
              "environment": {r["id"]: {k: r["header"].get(k) for k in ("kernel", "architecture", "placement")}
                              for r in runs},
              "method_compliance": None, "release_decision": None, "evidence": evidence,
              "missing_request_evidence": packet["missing_evidence"],
              "unknowns": {"oracle_byte_identical": "comparer acceptance does not prove raw oracle bytes",
                           "runner": "requester is not necessarily the executor; runner is explicit or null",
                           "harness_revision": "use per-source locked revision text and module hashes; no new pin inferred",
                           "checker_sha256": "legacy Python oracle differs from the native checker executable",
                           "method_compliance": "offline export preserves source contract; no recertification"},
              "sources": [{"id": r["id"], "source_lane": r["source_lane"], "schema": r["header"]["schema"],
                           "contract": {k: v for k, v in r["header"].items() if k not in ("config", "files")},
                           "configuration": {k: v for k, v in r["header"].get("config", {}).items() if k != "cases"},
                           "planned_cases": r["planned"],
                           "input_pins": {p: pin for c in r["header"].get("config", {}).get("cases", [])
                                          for p, pin in c.get("files", {}).items()}} for r in runs],
              "coverage": {"case_lanes": len(cells), "attempt_records": len(audit),
                           "accepted_attempts": sum(x["disposition"] == "accepted" for x in audit),
                           "engine_states": dict(collections.Counter(e["state"] for c in cells
                                                                      for e in c["engines"].values()))}}
    return request, report, cells, audit


def export(request_path, destination):
    request, report, cells, audit = make_export(request_path)
    root = "zpq/bench/runs/" + request["id"]
    docs = [{"slug": root + "/REQUEST.md", "title": "Benchmark request: " + request["id"],
             "content": "# Benchmark request\n\n```json\n" + requests.encode(request).decode() + "```\n"}]
    report["cell_documents"] = []
    for lane in sorted({c["lane"] for c in cells}):
        rows = [c for c in cells if c["lane"] == lane]
        for start in range(0, len(rows), 100):
            slug = root + f"/{lane}-{start // 100 + 1:02d}.md"
            report["cell_documents"].append(slug)
            body = "".join(requests.encode(c).decode().replace("\n", "") + "\n" for c in rows[start:start + 100])
            docs.append({"slug": slug, "title": request["id"] + ": " + lane,
                         "content": ("# Saved measurement cells\n\nCommon pins: `" + root +
                                     "/REPORT.md`.\n\n```jsonl\n" + body + "```\n")})
    docs.append({"slug": root + "/REPORT.md", "title": "Saved measurement: " + request["id"],
                 "content": ("# Saved measurement export\n\nOffline export; source verdicts preserved, "
                             "no new timing or certification.\n\n```json\n" + requests.encode(report).decode() + "```\n")})
    files = {"request.json": requests.encode(request), "report.json": requests.encode(report),
             "cells.jsonl": b"".join((requests.encode(c).decode().replace("\n", "") + "\n").encode()
                                      for c in cells),
             "attempts.jsonl": b"".join((requests.encode(a).decode().replace("\n", "") + "\n").encode() for a in audit),
             "tracker.json": requests.encode({"schema": "zpq-tracker-bundle/v1", "documents": docs})}
    files["export.json"] = requests.encode({"schema": "zpq-bench-export/v1", "request_sha256": report["request_sha256"],
                                           "files": {k: requests.sha256(v) for k, v in files.items()}})
    destination = Path(destination)
    protected = [Path(request_path)] + [Path(r["path"]) for r in request["runs"]]
    protected += [Path(p) for p in request.get("stored_summaries", [])]
    out = destination.resolve()
    if destination.is_symlink() or any(out == p.resolve() or out in p.resolve().parents or p.resolve() in out.parents
                                       for p in protected):
        raise ValueError("export destination overlaps source evidence")
    # Check the entire package before any write, so retry never mixes two generations.
    for name, data in files.items():
        target = destination / name
        if target.is_symlink() or target.exists() and target.read_bytes() != data:
            raise ValueError("conflicting export: " + str(target))
    destination.mkdir(parents=True, exist_ok=True)
    for name, data in files.items():
        target = destination / name
        if not target.exists():
            with target.open("xb") as f:
                f.write(data)
    return report
