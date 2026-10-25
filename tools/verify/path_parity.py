#!/usr/bin/env python3
"""Byte parity of referee output cases under the registered in-process execution overrides."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import time

import duckdb
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "tools/runner/force_knobs.json"


def normalized(value):
    if isinstance(value, float) and math.isnan(value):
        return ("NaN",)
    if isinstance(value, dict):
        return tuple((k, normalized(v)) for k, v in value.items())
    if isinstance(value, (list, tuple)):
        return tuple(normalized(v) for v in value)
    return value


def read_proof(work, fmt):
    if fmt == "json":
        return json.loads((work / "parity-json-proof.json").read_text())
    return json.loads((work / "parity-proof.json").read_text())["parquet"]


def check_evidence(kind, before, after):
    if kind == "unavailable":
        return "no existing trace/EXPLAIN assertion; follow-up"
    owner = {
        "byte_owner": ["byte_fold", "byte_top_fold"],
        "integer_owner": ["integer_fold"],
        "msd_owner": ["msd_fold"],
    }
    if kind in owner:
        assert before["dropped"] == after["dropped"] == 0, "truncated owner evidence"
        a = dict(zip(before["event_names"], before["events"]))
        b = dict(zip(after["event_names"], after["events"]))
        assert sum(a.get(k, 0) for k in owner[kind]) > 0, "default owner did not complete"
        assert sum(b.get(k, 0) for k in owner[kind]) == 0, "disabled owner completed"
        return "owner completed by default and was avoided when disabled"
    a, b = (json.loads(p["explain"]) for p in (before, after))
    if kind == "encoded":
        assert a["encoded"] is True and b["encoded"] is False
    elif kind == "union":
        assert a["ndv"]["union_complete"] is True
        assert b["ndv"]["union_attempted"] is False
    elif kind == "sketch_off":
        assert a["strategy"] == "sketch" and b["sketch"] is None
    elif kind == "census_off":
        assert a["strategy"] == "sketch"
        assert a["sketch"]["census_allowed"] or a["sketch"]["census_fused"]
        assert not b["sketch"]["census_allowed"] and not b["sketch"]["census_fused"]
    else:
        raise AssertionError(f"unknown evidence rule: {kind}")
    return "existing EXPLAIN proves the forced choice"


def run(args):
    knobs = json.loads(REGISTRY.read_text())
    names = {k["name"] for k in knobs}
    engine = (ROOT / "src/engine.zig").read_text()
    switches = set(re.findall(r'envDisables\([^\n]*?"(ZPQ_[A-Z0-9_]+)"', engine))
    assert switches <= names, f"unregistered execution switches: {switches - names}"
    cases = {}
    for path in sorted((ROOT / "tools/runner/cases").glob("*.json")):
        for case in json.loads(path.read_text()).get("cases", []):
            assert case["id"] not in cases, f"duplicate case id: {case['id']}"
            cases[case["id"]] = path
    env = {k: v for k, v in os.environ.items() if k not in names}
    args.out.mkdir(parents=True, exist_ok=False)
    records = []
    con = duckdb.connect()
    con.execute("SET threads=2")
    started = time.monotonic()
    for knob in knobs:
        for index, case_id in enumerate(knob["cases"]):
            modes = [("default", None), ("off", knob["off"])]
            if "on" in knob:
                modes.append(("on", knob["on"]))
            folders = {}
            for mode, value in modes:
                assert time.monotonic() - started < 900, "path-parity stage exceeded 15 minutes"
                work = args.out / f"{knob['name']}-{index}-{mode}"
                cmd = [str(args.referee), "--cases", str(cases[case_id]),
                       "--divergences", "tests/known_divergences.json", "--duckdb", str(args.duckdb),
                       "--pyarrow", os.sys.executable, "--work", str(work), "--parity-case", case_id]
                if value is not None:
                    cmd.extend(["--force", f"{knob['name']}={value}"])
                with (args.out / f"{work.name}.log").open("w") as log:
                    proc = subprocess.run(cmd, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=120)
                assert proc.returncode == 0, f"{knob['name']} {mode}: see {work.name}.log"
                folders[mode] = work
            record = {"knob": knob["name"], "case": case_id, "formats": {}}
            for fmt in ("json", "parquet"):
                reference = (folders["default"] / f"parity.{fmt}").read_bytes()
                for mode, work in folders.items():
                    output = work / f"parity.{fmt}"
                    assert output.read_bytes() == reference, (
                        f"ENGINE DIVERGENCE: {knob['name']} {case_id} {mode} {fmt}; preserve {work}"
                    )
                    if fmt == "parquet":
                        arrow = pq.read_table(output)
                        other = con.execute("SELECT * FROM read_parquet(?)", [str(output)]).fetch_arrow_table()
                        assert normalized(arrow.to_pylist()) == normalized(other.to_pylist()), output
                    else:
                        text = output.read_text()
                        if text.startswith("["):
                            json.loads(text)
                        else:
                            for line in text.splitlines():
                                json.loads(line)
                proof = check_evidence(knob["evidence"], read_proof(folders["default"], fmt),
                                       read_proof(folders["off"], fmt))
                record["formats"][fmt] = {"sha256": hashlib.sha256(reference).hexdigest(), "evidence": proof}
            records.append(record)
            (args.out / "results.json").write_text(json.dumps(records, indent=2) + "\n")
            print(f"PARITY {knob['name']} {case_id}: JSON+Parquet identical", flush=True)
    print(f"path parity: {len(records)} knobs, {len(records) * 2} format checks", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--referee", type=Path, required=True)
    parser.add_argument("--duckdb", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    run(parser.parse_args())
