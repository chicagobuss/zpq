"""Offline benchmark requests and input dependency plans; no engine or storage operations."""

import hashlib
import json
import re
from pathlib import Path


def encode(value):
    return (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def read(path, lines=False):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    def constant(value):
        raise ValueError(f"non-finite JSON value: {value}")

    text = Path(path).read_text()
    parse = lambda s: json.loads(s, object_pairs_hook=pairs, parse_constant=constant)
    return [parse(line) for line in text.splitlines() if line.strip()] if lines else parse(text)


def validate(request):
    if not isinstance(request, dict) or request.get("schema") != "zpq-bench-request/v1":
        raise ValueError("expected zpq-bench-request/v1")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,95}", request.get("id", "")):
        raise ValueError("request id must be a short lowercase slug")
    for key in ("requester", "host", "purpose"):
        if not isinstance(request.get(key), str) or not request[key].strip():
            raise ValueError(f"request needs {key}")
    missing = []
    for name in ("candidate", "baseline"):
        pin = request.get(name)
        if not isinstance(pin, dict):
            raise ValueError(f"request needs an explicit {name} pin object")
        for key, length in (("sha", 40), ("binary_sha256", 64)):
            value = pin.get(key)
            if value is None:
                missing.append(f"{name}.{key}")
            elif not isinstance(value, str) or not re.fullmatch("[0-9a-f]{" + str(length) + "}", value):
                raise ValueError(f"invalid full digest: {name}.{key}")
    for key in ("targets", "controls", "lanes"):
        value = request.get(key)
        if not isinstance(value, list) or not all(isinstance(x, str) and x for x in value):
            raise ValueError(f"request needs a {key} list")
        if len(value) != len(set(value)):
            raise ValueError(f"duplicate {key}")
    if not request["targets"] or not request["lanes"]:
        raise ValueError("request needs targets and lanes")
    if set(request["targets"]) & set(request["controls"]):
        raise ValueError("targets and controls overlap")
    if set(request["lanes"]) - {"cold", "hot-fresh", "engine"}:
        raise ValueError("unsupported request lane")
    runs = request.get("runs")
    if not isinstance(runs, list):
        raise ValueError("request needs a runs list (empty before measurement)")
    ids = set()
    for run in runs:
        if not isinstance(run, dict) or not isinstance(run.get("id"), str) or not run["id"]:
            raise ValueError("run needs an id")
        if run["id"] in ids or run.get("lane") not in request["lanes"] or not isinstance(run.get("path"), str):
            raise ValueError("duplicate run id, missing path or undeclared lane")
        ids.add(run["id"])
    if not isinstance(request.get("selection", {}), dict):
        raise ValueError("selection must map lanes to whole-case run choices")
    for lane, choices in request.get("selection", {}).items():
        if lane not in request["lanes"] or not isinstance(choices, dict):
            raise ValueError("invalid selection lane")
        for case, run in choices.items():
            if not isinstance(case, str) or not isinstance(run, str) or run not in ids:
                raise ValueError("selection needs a known run id for each whole case")
    if not isinstance(request.get("expected_engines", []), list) or not all(
            isinstance(x, str) and x for x in request.get("expected_engines", [])):
        raise ValueError("expected_engines must be a list of names")
    receipt = request.get("correctness_receipt")
    if receipt is None:
        missing.append("correctness_receipt")
    elif not isinstance(receipt, dict) or not all(receipt.get(x) for x in ("path", "sha256", "source_sha")):
        raise ValueError("correctness_receipt needs path, sha256 and source_sha")
    elif receipt["source_sha"] != request["candidate"].get("sha") or receipt.get("tree_clean") is not True:
        raise ValueError("correctness receipt must name the clean candidate SHA")
    for key in ("corpus_manifest_sha256", "query_set_sha256"):
        if not request.get(key):
            missing.append(key)
        elif not re.fullmatch(r"[0-9a-f]{64}", request[key]):
            raise ValueError(f"invalid {key}")
    return missing


def input_plan(request, catalog, inventory):
    """Size matches identify verification work, never trusted local object identity."""
    if catalog.get("schema") != "zpq-corpus-catalog/v1":
        raise ValueError("unsupported corpus catalog")
    dependencies = {c["case"]: c["objects"] for c in catalog["case_dependencies"]}
    objects = {x["sha256"]: x for x in catalog["objects"]}
    if len(objects) != len(catalog["objects"]):
        raise ValueError("duplicate corpus object")
    selected = request["targets"] + request["controls"]
    absent = sorted(set(selected) - dependencies.keys())
    if absent:
        raise ValueError("cases missing from catalog: " + ", ".join(absent))
    if inventory.get("host", "").split(".")[0] != request["host"].split(".")[0]:
        raise ValueError("input inventory host mismatch")
    locations = {f["path"]: f for entry in inventory["inputs"].values() for f in entry["files"]}
    planned = []
    for key in sorted({key for case in selected for key in dependencies[case]}):
        if key not in objects:
            raise ValueError("catalog references an unknown object: " + key)
        obj = objects[key]
        if not re.fullmatch(r"[0-9a-f]{64}", key) or type(obj["bytes"]) is not int or obj["bytes"] < 0:
            raise ValueError("invalid catalog object pin")
        matching = [p for p in obj["source_paths"] if p in locations and locations[p]["bytes"] == obj["bytes"]]
        planned.append({"sha256": key, "bytes": obj["bytes"], "local_paths": matching,
                        "state": "needs-verification" if matching else "missing-or-wrong-size"})
    return {"objects": planned, "required_bytes": sum(x["bytes"] for x in planned),
            "missing_bytes": sum(x["bytes"] for x in planned if not x["local_paths"]),
            "verification_bytes": sum(x["bytes"] for x in planned if x["local_paths"]),
            "additional_download_bytes_after_successful_verification": sum(x["bytes"] for x in planned
                                                                           if not x["local_paths"]),
            "ready": False, "reason": "metadata-only plan; verify present bytes before use; no transfers performed"}


def packet(path, catalog=None, inventory=None):
    request = read(path)
    missing = validate(request)
    result = {"schema": "zpq-bench-packet/v1", "request": request, "request_sha256": sha256(encode(request)),
              "missing_evidence": missing, "packet_complete": not missing, "ready_for_scheduling": False,
              "evidence_note": "field completeness is not scheduling approval; receipt contents and policy need intake"}
    if bool(catalog) != bool(inventory):
        raise ValueError("catalog and inventory must be supplied together")
    if catalog:
        result["input_plan"] = input_plan(request, read(catalog), read(inventory))
        result["ready_for_scheduling"] = False
    return result


def write_packet(path, value):
    data = encode(value)
    path = Path(path)
    if path.exists():
        if path.is_symlink() or path.read_bytes() != data:
            raise ValueError("conflicting output: " + str(path))
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as f:
        f.write(data)
