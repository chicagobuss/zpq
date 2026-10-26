#!/usr/bin/env python3
"""Build a scorecard config from the suites whose inputs exist.

Usage: emit_config.py HARNESS SPEC.json CONFIG.json REPORT.json
Exit 0 when at least one suite can be planned, 3 when every requested suite
was skipped, 2 when the suite filter names a suite the profile does not have.
The harness tree is imported; this file does not modify it.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from scorecard_select import classify_built, declared_suites, selected_ids


def _builders(profile: str):
    from bench.suites.local import clickbench, pdsh_cases, reads, study, votes, writes

    cold = {
        "clickbench": lambda: clickbench(False),
        "study": study,
        "pdsh": pdsh_cases,
        "reads-sf1": reads,
        "writes": writes,
        "votes-local": votes,
    }
    if profile == "in-process":
        return {name: cold[name] for name in ("reads-sf1", "clickbench", "study")}
    return cold


def main() -> int:
    harness, spec_path, config_path, report_path = sys.argv[1:5]
    sys.path.insert(0, harness)
    spec = json.loads(Path(spec_path).read_text())
    profile = spec["profile"]
    requested, unknown = declared_suites(profile, spec.get("suites"))
    report = {
        "profile": profile,
        "suites_requested": requested,
        "cases_requested": spec.get("cases"),
        "suites": [],
    }
    if unknown:
        report["unknown_suites"] = unknown
        Path(report_path).write_text(json.dumps(report, indent=2) + "\n")
        print(f"unknown suite for {profile}: {', '.join(unknown)}", file=sys.stderr)
        return 2
    tokens = None
    if spec.get("cases"):
        tokens = [part.strip() for part in spec["cases"].split(",") if part.strip()]
    builders = _builders(profile)
    runnable = []
    kept_ids = []
    for name in requested:
        try:
            cases = builders[name]()
        except Exception as error:
            report["suites"].append({"suite": name, "status": "skipped", "reason": str(error)})
            continue
        decision = classify_built(name, cases, tokens)
        if decision["status"] != "ran":
            report["suites"].append(decision)
            continue
        ids = selected_ids(cases, tokens)
        decision_public = {key: value for key, value in decision.items()}
        report["suites"].append(decision_public)
        runnable.append(name)
        kept_ids.extend(ids)
    if not runnable:
        Path(report_path).write_text(json.dumps(report, indent=2) + "\n")
        print("no suite had its inputs; nothing was planned", flush=True)
        return 3

    from bench.engines import defaults, pin_engine
    from bench.suites.local import cold_profile, inprocess_profile

    binary = spec["binary"]
    reps = spec["reps"]
    if profile == "in-process":
        config = inprocess_profile(binary, spec["library"], runnable, reps, pin=False)
    else:
        config = cold_profile(binary, runnable, spec["engines"].split(","), reps, pin=False)
        control = spec.get("control")
        if control:
            previous = defaults(control)["zpq"]
            previous["session_policy"] = "fresh process"
            config["engines"]["zpq_prev"] = previous
            for case in config["cases"]:
                engines = list(case.get("engines") or [])
                if "zpq_prev" not in engines:
                    case["engines"] = engines + ["zpq_prev"]
    # The harness pins ROOT/.venv, which lives on the harness machine and is not
    # part of this worktree. Point a missing interpreter at the python that is
    # actually emitting the config. Do not create a venv inside the harness tree.
    for name, engine in list(config["engines"].items()):
        python = engine.get("python")
        if not python or not Path(python).is_file():
            engine = {**engine, "python": sys.executable}
        config["engines"][name] = pin_engine(engine)
    if tokens is not None:
        config["cases"] = [case for case in config["cases"]
                           if f"{case['suite']}/{case['dataset']}/{case['query_id']}" in kept_ids]
    skipped = [row["suite"] for row in report["suites"] if row["status"] == "skipped"]
    config["coverage_note"] = (
        f"verify filter ran={','.join(runnable) or '-'} skipped={','.join(skipped) or '-'}; {spec['note']}"
    )
    Path(config_path).write_text(json.dumps(config))
    Path(report_path).write_text(json.dumps(report, indent=2) + "\n")
    print(len(config["cases"]), "cases;", ",".join(config["engines"]), flush=True)
    for row in report["suites"]:
        if row["status"] == "skipped":
            print(f"skip {row['suite']}: {row['reason']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
