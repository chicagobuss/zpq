"""Bounded, hash-first, same-host candidate/base checks. No services or scheduler."""

import argparse
import contextlib
import datetime
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import resource
import shlex
import signal
import socket
import statistics
import subprocess
import sys
import time
import uuid


class BudgetExpired(Exception):
    pass


class Deferred(Exception):
    pass


class Mismatch(Exception):
    pass


def encode(value):
    return (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def save(path, value):
    path = Path(path)
    temp = path.with_suffix(path.suffix + ".partial")
    temp.write_bytes(encode(value))
    temp.replace(path)


def digest(path, check=lambda: None):
    sha = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while block := stream.read(1 << 20):
            check()
            sha.update(block)
    return sha.hexdigest()


def read(path):
    return json.loads(Path(path).read_bytes())


def full_hash(value, length=64):
    return isinstance(value, str) and re.fullmatch("[0-9a-f]{" + str(length) + "}", value)


def absolute(value):
    path = Path(value)
    if not path.is_absolute():
        raise ValueError("explicit absolute paths required")
    return path


def inside(root, relative):
    path = root / relative
    if Path(relative).is_absolute() or not path.resolve().is_relative_to(root.resolve()):
        raise ValueError("input escapes configured data root")
    return path


class InputHashes:
    """Host-local content verdicts survive runs; native outputs never use this cache."""

    def __init__(self, root):
        self.root = absolute(root) / "input-hashes"
        self.host = socket.gethostname()
        self.evidence = []

    def identity(self, path):
        path = absolute(path)
        st = path.stat()
        return {"path": str(path), "size": st.st_size, "mtime_ns": st.st_mtime_ns, "inode": st.st_ino}

    def verify(self, path, expected, check=lambda: None):
        check()
        started = time.monotonic()
        identity = self.identity(path)
        key = hashlib.sha256(encode([self.host, identity["path"]])).hexdigest()
        entry_path = self.root / (key + ".json")
        try:
            entry = read(entry_path)
        except (FileNotFoundError, ValueError):
            entry = None
        hit = bool(isinstance(entry, dict) and entry.get("schema") == "zpq-input-hash/v1" and
               entry.get("host") == self.host and entry.get("identity") == identity and
               isinstance(entry.get("verified_at"), str) and full_hash(entry.get("sha256")))
        if not hit:
            observed = digest(path, check)
            check()
            if self.identity(path) != identity:
                raise Mismatch("input changed during checksum: " + str(path))
            entry = {"schema": "zpq-input-hash/v1", "host": self.host, "identity": identity,
                     "sha256": observed, "verified_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}
            self.root.mkdir(parents=True, exist_ok=True)
            # Independent coordinators can verify the same source for different hosts.
            temporary = self.root / (key + "." + uuid.uuid4().hex + ".partial")
            try:
                temporary.write_bytes(encode(entry))
                temporary.replace(entry_path)
            finally:
                temporary.unlink(missing_ok=True)
        check()
        if self.identity(path) != identity:
            raise Mismatch("input changed during checksum lookup: " + str(path))
        row = {**entry, "cache_hit": hit, "expected_sha256": expected,
               "seconds": time.monotonic() - started, "cache_path": str(entry_path)}
        self.evidence.append(row)
        if entry["sha256"] != expected:
            raise Mismatch("input checksum differs: " + str(path))
        return row


def verify_inputs(cfg, host, data, manifest, check=lambda: None, evidence=None):
    cache = InputHashes(host["root"])
    if evidence is not None:
        cache.evidence = evidence
    for name in sorted({c["file"] for c in manifest_cases(manifest, cfg.get("only"))}):
        pin, path = manifest["files"][name], inside(data, name)
        if not full_hash(pin["sha256"]) or path.stat().st_size != pin["bytes"]:
            raise Mismatch("input size/pin differs: " + name)
        # Manifest views are hard links: verify the stable object path, not a fresh alias per request.
        source = absolute(cfg.get("input_objects", {}).get(name, str(path)))
        if not path.samefile(source):
            raise Mismatch("input object/view differs: " + name)
        cache.verify(source, pin["sha256"], check)
    return cache.evidence


def load(config, host, candidate=None, base=None):
    cfg = read(config)
    h = cfg["hosts"][host]
    if h.get("enabled") is not True:
        raise ValueError("host must be explicitly enabled")
    absolute(h["root"])
    absolute(h["lock"])
    if not isinstance(h.get("memory_max"), str) or not h["memory_max"]:
        raise ValueError("host memory_max is required")
    data = absolute(cfg["data"])
    manifest = data / "manifest.json"
    if not full_hash(cfg["manifest_sha256"]):
        raise ValueError("manifest_sha256 is required")
    builds = {}
    for name, override in (("candidate", candidate), ("base", base)):
        pin = dict(h.get(name, cfg.get(name, {})))
        if override:
            pin["path"] = str(absolute(override))
        absolute(pin["path"])
        if not full_hash(pin["sha"], 40) or not full_hash(pin["sha256"]):
            raise ValueError(name + " needs full source and binary hashes")
        for key in ("target", "cpu", "optimize", "strip", "toolchain"):
            if key not in pin:
                raise ValueError(name + " needs explicit build " + key)
        builds[name] = pin
    if any(builds["candidate"][k] != builds["base"][k]
           for k in ("target", "cpu", "optimize", "strip", "toolchain")):
        raise ValueError("candidate/base build tuples differ")
    budget = cfg.get("budget_seconds", 90)
    reps = cfg.get("reps", 3)
    noise = cfg.get("noise_pct", 0)
    if type(budget) not in (int, float) or not math.isfinite(budget) or not 0 < budget <= 120:
        raise ValueError("one shared budget must be >0 and <=120 seconds")
    if type(reps) is not int or not 1 <= reps <= 10:
        raise ValueError("reps must be between 1 and 10")
    if type(noise) not in (int, float) or not math.isfinite(noise) or noise < 0:
        raise ValueError("noise_pct must be a nonnegative declared allowance")
    h = {**h, "builds": builds}
    return cfg, h, data, manifest


class Run:
    def __init__(self, out, deadline):
        self.out, self.deadline = Path(out), deadline
        self.attempts = []
        self.child = None

    def check(self):
        if time.monotonic() >= self.deadline:
            raise BudgetExpired()

    def command(self, argv, stdout, stderr, env=None):
        self.check()
        start = time.monotonic_ns()
        try:
            with Path(stdout).open("wb") as out, Path(stderr).open("wb") as err:
                self.child = subprocess.Popen(argv, stdout=out, stderr=err, env=env, start_new_session=True)
                save(self.out / "active.json", {"pid": self.child.pid})
                # Popen.wait(timeout) backs off to 50 ms sleeps, which rounds every timing up to
                # its next poll; poll at spawn.py's 0.5 ms instead.
                while (rc := self.child.poll()) is None:
                    self.check()
                    time.sleep(0.0005)
                elapsed = time.monotonic_ns() - start
            return rc, elapsed
        finally:
            self.drain()

    def drain(self):
        if self.child is not None:
            # The session belongs to this one command, including any descendants.
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.child.pid, signal.SIGKILL)
            self.child.wait()
            self.child = None
        (self.out / "active.json").unlink(missing_ok=True)

    def probe(self, argv):
        out, err = self.out / "probe.stdout", self.out / "probe.stderr"
        rc, _ = self.command(argv, out, err)
        if rc:
            raise ValueError("probe failed: " + str(argv[0]))
        return out.read_bytes()

    def journal(self, row):
        self.attempts.append(row)
        with (self.out / "attempts.jsonl").open("ab") as stream:
            stream.write(encode(row).replace(b"\n", b"") + b"\n")


@contextlib.contextmanager
def timing_lock(path):
    # Use the host's configured existing lock, shared with the full harness.
    with absolute(path).open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise Deferred("host timing lock held") from None
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def idle(run, host):
    if host.get("idle_command"):
        state = json.loads(run.probe(host["idle_command"]))
    elif sys.platform.startswith("linux"):
        from bench import env
        cpus = host.get("cpus", sorted(os.sched_getaffinity(0)))
        run.check()
        state = env.idle(cpus, seconds=min(0.25, max(0.001, run.deadline - time.monotonic())),
                         threshold=host.get("idle_max_cores", 0.5))
    else:
        raise Deferred("this platform requires a configured idle_command returning JSON quiet/busy_cores")
    run.check()
    if state.get("quiet") is not True:
        raise Deferred("host is busy")
    return state


def manifest_cases(manifest, only=None):
    cases = manifest["cases"]
    ids = [c["id"] for c in cases]
    if not cases or len(ids) != len(set(ids)):
        raise ValueError("manifest needs unique cases")
    for c in cases:
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", c["id"]):
            raise ValueError("case id must be a safe filename")
        if c["file"] not in manifest["files"]:
            raise ValueError("case input must be pinned in manifest")
        if c.get("engines", ["candidate", "base"]) not in (["candidate", "base"], ["candidate"]):
            raise ValueError("case engines must be candidate/base or explicit candidate-only outcome check")
    if only:
        if not set(only) <= set(ids):
            raise ValueError("unknown requested case")
        cases = [c for c in cases if c["id"] in only]
    return cases


def native_argv(case, pin, data, artifact, host):
    source = str(inside(data, case["file"]))
    if case.get("zpq_args") is not None:
        argv = [pin["path"], "query", source, *case["zpq_args"]]
    else:
        sql = case["sql"].replace("{T}", "'" + source.replace("'", "''") + "'")
        argv = [pin["path"], "query", "--query", sql]
    if host.get("threads"):
        argv += ["-j", str(host["threads"])]
    if case["output"] == "parquet":
        argv += ["-o", str(artifact), "--codec", case.get("codec", "snappy")]
    return argv


def expected(case, engine, row):
    want = case.get("expected", {}).get(engine, case.get("expected", {}).get("all"))
    if want:
        for field in ("native_exit", "stdout_sha256", "stderr_sha256"):
            if field not in want or row[field] != want[field]:
                raise Mismatch("native outcome differs: " + case["id"] + "/" + engine + "/" + field)
        if row["native_exit"] and want.get("outcome") != "refusal":
            raise Mismatch("nonzero exit is not a declared refusal")
        row["native_outcome"] = want.get("outcome", "answer")
    elif row["native_exit"] != 0:
        raise Mismatch("unexpected native exit: " + case["id"] + "/" + engine)
    else:
        row["native_outcome"] = "answer"


def parquet_validation(run, case, artifact, source, config):
    validators = config.get("parquet_validators", {})
    if not validators:
        duck = config.get("duckdb", {})
        if not duck.get("path") or not full_hash(duck.get("sha256")):
            raise ValueError("Parquet cases require a pinned DuckDB CLI and an existing PyArrow runtime")
        if config.get("_checked_duckdb") != duck["sha256"]:
            if digest(absolute(duck["path"]), run.check) != duck["sha256"]:
                raise Mismatch("DuckDB validator checksum differs")
            config["_checked_duckdb"] = duck["sha256"]
        sql = case["sql"].replace("{T}", "'" + str(source).replace("'", "''") + "'")
        path = str(artifact).replace("'", "''")
        truth = ("SELECT EXISTS ((SELECT * FROM '" + path + "' EXCEPT ALL (" + sql + ")) UNION ALL ((" +
                 sql + ") EXCEPT ALL SELECT * FROM '" + path + "')) AS bad")
        rc, _ = run.command([duck["path"], "-no-agent", "-init", "/dev/null", "-json", "-c", truth],
                             run.out / "duckdb.stdout", run.out / "duckdb.stderr")
        if rc or read(run.out / "duckdb.stdout") != [{"bad": False}]:
            raise Mismatch("duckdb rejected Parquet output: " + case["id"])
        script = ("import pyarrow,pyarrow.parquet as p,sys,json;"
                  "t=p.read_table(sys.argv[1]);t.validate(full=True);"
                  "print(json.dumps({'pyarrow':pyarrow.__version__,'rows':t.num_rows,'columns':t.num_columns}))")
        rc, _ = run.command([config.get("pyarrow_python", sys.executable), "-c", script, str(artifact)],
                             run.out / "pyarrow.stdout", run.out / "pyarrow.stderr")
        if rc:
            raise Mismatch("pyarrow rejected Parquet output: " + case["id"])
        run.validation_evidence = {"duckdb_binary_sha256": duck["sha256"],
                                   "pyarrow": read(run.out / "pyarrow.stdout")}
        return {"duckdb": "pass", "pyarrow": "pass"}
    # No package installation or oracle substitution during a quick check.
    if not all(validators.get(k) for k in ("duckdb", "pyarrow")):
        raise ValueError("Parquet cases need configured duckdb and pyarrow validator command arrays")
    verdicts = {}
    for name in ("duckdb", "pyarrow"):
        args = [x.replace("{output}", str(artifact)).replace("{input}", str(source))
                .replace("{sql}", case["sql"].replace("{T}", "'" + str(source).replace("'", "''") + "'"))
                for x in validators[name]]
        rc, _ = run.command(args, run.out / (name + ".stdout"), run.out / (name + ".stderr"))
        verdicts[name] = "pass" if rc == 0 else "failed"
        if rc:
            raise Mismatch(name + " rejected Parquet output: " + case["id"])
    run.validation_evidence = {"configured_command_sha256": hashlib.sha256(encode(validators)).hexdigest()}
    return verdicts


def execute(run, config, host, data, case, engine, phase, rep):
    state = idle(run, host)
    source = inside(data, case["file"])
    if phase == "hot-fresh":
        with source.open("rb") as stream:
            while stream.read(1 << 20):
                run.check()
    run.check()
    stem = run.out / (case["id"] + "-" + engine + "-" + phase + "-" + str(rep))
    artifact = Path(str(stem) + (".parquet" if case["output"] == "parquet" else ".stdout"))
    stdout, stderr = Path(str(stem) + ".stdout"), Path(str(stem) + ".stderr")
    row = {"case": case["id"], "engine": engine, "phase": phase, "rep": rep,
           "origin": "measured", "selected": False, "session_policy": "fresh-process",
           "timer_scope": "spawn through native output completion and reap",
           "cache_policy": "input read before hot sample" if phase == "hot-fresh" else "uncontrolled first pass",
           "artifact": str(artifact), "stdout": str(stdout), "stderr": str(stderr), "idle_before": state}
    try:
        environment = dict(os.environ)
        environment.update(case.get("environment", {}))
        from bench import env
        cpus = host.get("cpus", sorted(os.sched_getaffinity(0))) if sys.platform.startswith("linux") else []
        busy_before = env.busy_ticks(cpus) if cpus else None
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        cpu_before = usage.ru_utime + usage.ru_stime
        rc, ns = run.command(native_argv(case, host["builds"][engine], data, artifact, host),
                             stdout, stderr, environment)
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        if cpus:
            row["contention"] = env.foreign_cpu(
                {"wall_ns": ns, "cpu_ns": (usage.ru_utime + usage.ru_stime - cpu_before) * 1e9,
                 "busy_ticks": env.busy_ticks(cpus) - busy_before}, cpus, host.get("idle_max_cores", 0.5))
            if row["contention"]["contended"]:
                raise Deferred("foreign CPU during native execution")
        row.update(native_exit=rc, wall_ns=ns, stdout_sha256=digest(stdout, run.check),
                   stderr_sha256=digest(stderr, run.check), status="observed")
        expected(case, engine, row)
        row["artifact_sha256"] = digest(artifact, run.check) if artifact.exists() else None
        if case["output"] == "parquet" and row["native_outcome"] == "answer":
            row["value_validation"] = parquet_validation(run, case, artifact, source, config)
            row["validation_evidence"] = run.validation_evidence
        row["idle_after"] = idle(run, host)
        row["status"] = "ok"
    except BaseException as error:
        row["status"] = ("timeout" if isinstance(error, BudgetExpired) else
                         "environment-rejected" if isinstance(error, Deferred) else "failed")
        row["reason"] = str(error) or type(error).__name__
        raise
    finally:
        run.journal(row)
    return row


def identity(row):
    return {k: row[k] for k in ("native_exit", "stdout_sha256", "stderr_sha256", "native_outcome",
                                "artifact_sha256")}


def pair_check(case, pair):
    if len(pair) == 2 and identity(pair[0]) != identity(pair[1]):
        reason = case.get("parquet_byte_difference")
        if (case["output"] != "parquet" or not isinstance(reason, str) or not reason.strip()
                or any(r["native_outcome"] != "answer" or
                       r.get("value_validation") != {"duckdb": "pass", "pyarrow": "pass"} for r in pair)
                or any(r["native_exit"] != 0 for r in pair)
                or any(pair[0][k] != pair[1][k] for k in
                       ("native_exit", "stdout_sha256", "stderr_sha256", "native_outcome"))):
            raise Mismatch("candidate/base bytes or native outcomes differ: " + case["id"])


def cells_for(cases, rows, reps, hash_complete, noise_pct):
    cells = []
    for case in cases:
        engines = {}
        for engine in case.get("engines", ["candidate", "base"]):
            attempts = [r for r in rows if r["case"] == case["id"] and r["engine"] == engine]
            good = [r for r in attempts if selected(case, r, rows, hash_complete)]
            complete = len(good) == reps
            times = [r["wall_ns"] / 1e6 for r in good]
            engines[engine] = {"origin": "measured", "state": "complete" if complete else "incomplete",
                               "n": len(times), "expected_reps": reps,
                               "median_ms": statistics.median(times) if complete else None,
                               "min_ms": min(times) if times else None, "max_ms": max(times) if times else None,
                               "native_outcomes": sorted({r.get("native_outcome", "unknown") for r in attempts}),
                               "hashes": sorted({r["artifact_sha256"] for r in attempts
                                                 if r.get("artifact_sha256")})}
        pair = [r for r in rows if r["case"] == case["id"] and r["phase"] == "hash-first" and r["status"] == "ok"]
        raw_identity = identity(pair[0]) == identity(pair[1]) if len(pair) == 2 else None
        ratio = None
        if (len(engines) == 2 and all(e["state"] == "complete" and e["native_outcomes"] == ["answer"]
                                     for e in engines.values())):
            denominator = engines["base"]["median_ms"]
            ratio = engines["candidate"]["median_ms"] / denominator if denominator else None
        cells.append({"case": case["id"], "lane": "hot-fresh", "engines": engines,
                      "byte_identical": raw_identity, "declared_parquet_difference": case.get("parquet_byte_difference"),
                      "candidate_over_base": ratio, "noise_pct": noise_pct,
                      "flagged": ratio is not None and ratio > 1 + noise_pct / 100})
    return cells


def selected(case, row, rows, hash_complete):
    if not hash_complete or row["phase"] != "hot-fresh" or row["status"] != "ok":
        return False
    return all(any(other["case"] == row["case"] and other["rep"] == row["rep"] and
                   other["phase"] == row["phase"] and other["engine"] == engine and other["status"] == "ok"
                   for other in rows) for engine in case.get("engines", ["candidate", "base"]))


def result(run, request, cases, state, reason, hash_complete, started):
    cells = cells_for(cases, run.attempts, request["reps"], hash_complete, request["noise_pct"])
    complete = bool(cells) and all(e["state"] == "complete" for c in cells for e in c["engines"].values())
    flagged = any(c["flagged"] for c in cells)
    verdict = "flagged" if complete and flagged and state == "complete" else state
    by_id = {case["id"]: case for case in cases}
    for row in run.attempts:
        row["selected"] = selected(by_id[row["case"]], row, run.attempts, hash_complete)
    summary = {"schema": "zpq-bench-result/v1", "result_kind": "quick-ab",
               "run_id": "quick-" + hashlib.sha256(str(run.out).encode()).hexdigest()[:16],
               "request_sha256": hashlib.sha256(encode(request)).hexdigest(),
               "host": request["host"], "actual_host": socket.gethostname(),
               "evidence_class": "quick-local", "state": verdict, "reason": reason,
               "hash_pass_complete": hash_complete, "coverage_complete": complete,
               "candidate": request["candidate"], "baseline": request["baseline"],
               "candidate_sha": request["candidate"]["sha"], "baseline_sha": request["baseline"]["sha"],
               "binary_sha256": request["candidate"]["sha256"],
               "baseline_binary_sha256": request["baseline"]["sha256"],
               "corpus_manifest_sha256": request["corpus_manifest_sha256"],
               "input_verification": request.get("input_verification", []),
               "query_set_sha256": request["query_set_sha256"],
               "budget_seconds": request["budget_seconds"], "wall_seconds": time.monotonic() - started,
               "start_utc": request["start_utc"],
               "end_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
               "environment": {"uname": dict(zip(("sysname", "nodename", "release", "version", "machine"),
                                                 os.uname())), "load_after": os.getloadavg()},
               "cache_policy": "first pass uncontrolled; input read before fresh-process hot samples",
               "noise_pct": request["noise_pct"], "timing_lock": request["timing_lock"],
               "planned_cases": len(cases), "attempts": len(run.attempts),
               "selected_timings": sum(r["selected"] for r in run.attempts),
               "requested_hosts": request.get("requested_hosts", []),
               "catalog_cases": request.get("catalog_cases", []),
               "unrequested_cases": request.get("unrequested_cases", []),
               "other_hosts": "unmeasured; this is one host, not fleet acceptance",
               "release_decision": None, "cells": cells}
    summary["coverage"] = {"case_lanes": len(cells), "attempt_records": len(run.attempts),
                           "accepted_attempts": summary["selected_timings"],
                           "engine_states": {state: sum(e["state"] == state for c in cells
                                                        for e in c["engines"].values())
                                             for state in ("complete", "incomplete")}}
    if (run.out / "rivals" / "summary.json").exists():
        summary["rivals"] = read(run.out / "rivals" / "summary.json")
    summary["merge_gate"] = ("not-applicable" if not any(len(c["engines"]) == 2 for c in cells) else
                             "pass" if verdict == "complete" else verdict)
    save(run.out / "report.json", summary)
    (run.out / "cells.jsonl").write_bytes(b"".join(encode(c).replace(b"\n", b"") + b"\n" for c in cells))
    # Keep original events and the final selection as separate records.
    save(run.out / "selection.json", {"rule": "all valid fixed-target hot samples; never fastest",
                                      "attempts": run.attempts})
    return summary


def rivals(run, config, host, data):
    """Optional historical arch comparison shares the deadline; it never adds time to the A/B."""
    runner, duckdb = config["arch_runner"], config["duckdb"]
    if (config.get("only") or host["builds"]["candidate"]["cpu"] != "baseline" or
            host["builds"]["candidate"]["optimize"] != "ReleaseFast" or
            host["builds"]["candidate"]["strip"] is not True):
        raise ValueError("optional arch rivals require the full mini manifest and baseline CPU build")
    for pin in (runner, duckdb):
        if not full_hash(pin["sha256"]) or digest(absolute(pin["path"]), run.check) != pin["sha256"]:
            raise Mismatch("optional rival/runner checksum differs")
    seconds = int(run.deadline - time.monotonic())
    if seconds < 1:
        raise BudgetExpired()
    out = run.out / "rivals"
    out.mkdir()
    argv = [runner["path"], "--data", str(data), "--binary", host["builds"]["candidate"]["path"],
            "--revision", host["builds"]["candidate"]["sha"], "--target", host["builds"]["candidate"]["target"],
            "--duckdb", duckdb["path"], "--duckdb-sha256", duckdb["sha256"],
            "--out", str(out), "--reps", str(config.get("reps", 3)), "--budget-seconds", str(seconds)]
    if host.get("threads"):
        argv += ["--threads", str(host["threads"])]
    if config.get("polars_python"):
        argv += ["--polars-python", config["polars_python"],
                 "--polars-script", str(Path(__file__).with_name("arch_polars.py"))]
    idle(run, host)
    rc, _ = run.command(argv, run.out / "rivals.stdout", run.out / "rivals.stderr")
    if rc:
        raise Mismatch("optional arch check failed; exact rival summary retained")
    summary = read(out / "summary.json")
    for query in summary["queries"]:
        wanted = {"zpq", "duckdb"} | ({"polars"} if config.get("polars_python") else set())
        timings = {r["engine"]: r for r in query["timings"]}
        if any(e not in timings or timings[e]["reps"] != config.get("reps", 3) for e in wanted):
            raise BudgetExpired()


def worker(config_path, host_name, out, deadline):
    cfg, host, data, manifest_path = load(config_path, host_name)
    cfg["parquet_validators"] = host.get("parquet_validators", cfg.get("parquet_validators", {}))
    for name in ("duckdb", "arch_runner", "pyarrow_python"):
        if name in host:
            cfg[name] = host[name]
    out = absolute(out)
    run = Run(out, deadline)
    started = time.monotonic()
    cases, request, hash_complete = [], None, False
    state, reason = "incomplete", "not started"
    def expire(*_):
        raise BudgetExpired()
    old_handler = signal.signal(signal.SIGTERM, expire)
    try:
        manifest = read(manifest_path)
        cases = manifest_cases(manifest, cfg.get("only"))
        request = {"schema": "zpq-bench-quick-request/v1", "host": host_name,
                   "candidate": host["builds"]["candidate"], "baseline": host["builds"]["base"],
                   "corpus_manifest_sha256": cfg["manifest_sha256"],
                   "query_set_sha256": hashlib.sha256(encode(cases)).hexdigest(),
                   "budget_seconds": cfg["budget_seconds"], "reps": cfg.get("reps", 3),
                   "start_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                   "noise_pct": cfg.get("noise_pct", 0), "requested_hosts": cfg.get("requested_hosts", [host_name]),
                   "timing_lock": host["lock"],
                   "resources": {k: host.get(k) for k in ("memory_max", "nice", "cpus", "threads")},
                   "planned_cases": cases, "inputs": manifest["files"],
                   "catalog_cases": [c["id"] for c in manifest["cases"]],
                   "unrequested_cases": [c["id"] for c in manifest["cases"] if c not in cases],
                   "validation_commands": cfg["parquet_validators"],
                   "validation_metadata": cfg.get("validation_metadata"),
                   "rivals_requested": bool(cfg.get("rivals")),
                   "correctness_receipt": cfg.get("correctness_receipt"), "harness_sha256": digest(__file__),
                   "environment_probe_sha256": digest(Path(__file__).with_name("env.py")),
                   "resource_wrapper_sha256": digest(Path(__file__).with_name("arch_remote.sh"))}
        save(out / "request.json", request)
        with timing_lock(request["timing_lock"]):
            if host.get("cpus") and hasattr(os, "sched_setaffinity"):
                os.sched_setaffinity(0, set(host["cpus"]))
            idle(run, host)
            if digest(manifest_path, run.check) != cfg["manifest_sha256"]:
                raise Mismatch("manifest checksum differs")
            request["input_verification"] = []
            verify_inputs(cfg, host, data, manifest, run.check, request["input_verification"])
            for name, pin in host["builds"].items():
                if digest(pin["path"], run.check) != pin["sha256"]:
                    raise Mismatch("binary checksum differs: " + name)
                version = run.probe([pin["path"], "--version"]).decode()
                if pin["sha"][:12] not in version and pin["sha"] not in version:
                    raise Mismatch("binary revision differs: " + name)
                pin["observed_version"] = version.strip()
            reference = read(cfg["reference"]) if cfg.get("reference") else None
            if reference and any(reference[k] != request[k] for k in
                                 ("corpus_manifest_sha256", "query_set_sha256")):
                raise Mismatch("reference inputs or queries differ")
            if reference and any(reference[k]["sha"] != request[k]["sha"] for k in ("candidate", "baseline")):
                raise Mismatch("reference source revisions differ")
            hashes = {}
            for case in cases:
                pair = [execute(run, cfg, host, data, case, e, "hash-first", 0)
                        for e in case.get("engines", ["candidate", "base"])]
                try:
                    pair_check(case, pair)
                except Mismatch as error:
                    for row in pair:
                        row.update(status="incorrect", reason=str(error))
                    raise
                hashes[case["id"]] = {r["engine"]: identity(r) for r in pair}
                if reference and reference["hashes"].get(case["id"]) != hashes[case["id"]]:
                    raise Mismatch("cross-host output hash mismatch: " + case["id"])
            save(out / "hashes.json", {**{k: request[k] for k in
                                         ("candidate", "baseline", "corpus_manifest_sha256", "query_set_sha256")},
                                      "hashes": hashes, "reference_state":
                                      "matched" if reference else "established here; other hosts unchecked"})
            hash_complete = True
            for rep in range(1, request["reps"] + 1):
                for case in cases:
                    engines = case.get("engines", ["candidate", "base"])
                    pair = [execute(run, cfg, host, data, case, e, "hot-fresh", rep)
                            for e in (engines if rep % 2 else engines[::-1])]
                    try:
                        pair_check(case, pair)
                    except Mismatch as error:
                        for row in pair:
                            row.update(status="incorrect", reason=str(error))
                        raise
                    for row in pair:
                        if identity(row) != hashes[case["id"]][row["engine"]]:
                            row.update(status="incorrect", reason="repeated native bytes/outcome changed")
                            raise Mismatch("repeated native bytes/outcome changed: " + case["id"])
            if cfg.get("rivals"):
                rivals(run, cfg, host, data)
            state, reason = "complete", "fixed paired rep target reached"
    except BudgetExpired:
        state, reason = "incomplete", "shared wall-time budget exhausted"
    except Deferred as error:
        state, reason = "deferred", str(error)
    except (Mismatch, ValueError, OSError, KeyError, TypeError) as error:
        state, reason = "failed", str(error)
    finally:
        run.drain()
        # Queries are drained; allow the outer watchdog to collect or reconstruct this final record.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        if request is not None:
            save(out / "request.json", request)
            summary = result(run, request, cases, state, reason, hash_complete, started)
        else:
            summary = {"state": state, "reason": reason, "coverage_complete": False}
            save(out / "report.json", summary)
        signal.signal(signal.SIGTERM, old_handler)
    return 0 if summary["state"] == "complete" else 3 if summary["state"] in ("flagged", "failed") else 2


def supervise(config_path, host_name, out):
    cfg, host, _, _ = load(config_path, host_name)
    out = absolute(out)
    deadline = time.monotonic() + cfg["budget_seconds"]
    argv = [sys.executable, "-m", "bench.quick", "--worker", str(config_path),
            host_name, str(out), str(deadline)]
    if host.get("memory_max"):
        argv = ["bash", str(Path(__file__).with_name("arch_remote.sh")), "quick", host["root"],
                host["memory_max"], str(host.get("nice", 10)), str(host.get("disk_cap_kb", 20_000_000)), *argv]
    else:
        raise ValueError("host memory_max is required")
    with (out / "worker.stdout").open("wb") as stdout, (out / "worker.stderr").open("wb") as stderr:
        environment = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parent.parent) +
                       os.pathsep + os.environ.get("PYTHONPATH", "")}
        child = subprocess.Popen(argv, stdout=stdout, stderr=stderr, start_new_session=True, env=environment)
        try:
            code = child.wait(timeout=max(0.001, deadline - time.monotonic()))
            if not (out / "report.json").exists():
                recover(config_path, host_name, out, deadline, "host worker could not start; see worker.stderr")
                return 2
            return code
        except subprocess.TimeoutExpired:
            child.send_signal(signal.SIGTERM)
            try:
                child.wait(timeout=0.25)
            except subprocess.TimeoutExpired:
                pass
            # Only this run's known child session is eligible for cleanup.
            active = out / "active.json"
            if active.exists():
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(read(active)["pid"], signal.SIGKILL)
            with contextlib.suppress(ProcessLookupError):
                os.killpg(child.pid, signal.SIGKILL)
            child.wait()
            recover(config_path, host_name, out, deadline, "shared wall-time watchdog expired")
            return 2


def recover(config_path, host_name, out, deadline, reason):
    cfg, host, _, path = load(config_path, host_name)
    cases = manifest_cases(read(path), cfg.get("only"))
    request_path = out / "request.json"
    request = read(request_path) if request_path.exists() else {
        "host": host_name, "candidate": host["builds"]["candidate"], "baseline": host["builds"]["base"],
        "corpus_manifest_sha256": cfg["manifest_sha256"],
        "query_set_sha256": hashlib.sha256(encode(cases)).hexdigest(),
        "reps": cfg.get("reps", 3), "noise_pct": cfg.get("noise_pct", 0),
        "budget_seconds": cfg["budget_seconds"], "timing_lock": host["lock"],
        "start_utc": None, "planned_cases": cases}
    if not request_path.exists():
        save(request_path, request)
    run = Run(out, deadline)
    journal = out / "attempts.jsonl"
    if journal.exists():
        for line in journal.read_text().splitlines():
            try:
                run.attempts.append(json.loads(line))
            except json.JSONDecodeError:
                # A terminated final write is retained in the original journal, never treated as an answer.
                break
    result(run, request, cases, "incomplete", reason, False, deadline - cfg["budget_seconds"])


LOCK_EXEC = """import fcntl,os,sys
f=open(sys.argv[1],"a")
try: fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
except BlockingIOError: sys.exit(75)
os.set_inheritable(f.fileno(),True)
os.execvp(sys.argv[2],sys.argv[2:])
"""


def remote(argv, host, lock, timeout=30):
    argv = ["python3", "-c", LOCK_EXEC, lock, *argv]
    return subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", host, shlex.join(argv)],
                          check=True, capture_output=True, timeout=timeout)


def transfer(sources, target, host, lock):
    # Hold the same host lock inside rsync as well: setup must not overlap another timing window.
    launcher = shlex.join(["python3", "-c", LOCK_EXEC, lock, "rsync"])
    subprocess.run(["rsync", "-a", "--checksum", "--protect-args", "--rsync-path", launcher,
                    *map(str, sources), host + ":" + str(target)], check=True, timeout=120)


def remote_input(package, root, path, expected, host, lock, remaining):
    code = """import sys,pathlib,json,time,signal
sys.path.insert(0,sys.argv[1])
from bench.quick import InputHashes,BudgetExpired
deadline=time.monotonic()+float(sys.argv[5])
def check(*_):
    if time.monotonic()>=deadline: raise BudgetExpired()
def expire(*_): raise BudgetExpired()
signal.signal(signal.SIGALRM,expire)
signal.setitimer(signal.ITIMER_REAL,float(sys.argv[5]))
p=pathlib.Path(sys.argv[3])
print(json.dumps(InputHashes(sys.argv[2]).verify(p,sys.argv[4],check) if p.exists() else None))
"""
    if remaining <= 0:
        raise BudgetExpired()
    reply = remote(["python3", "-c", code, str(package), str(root), str(path), expected, str(remaining)],
                   host, lock, timeout=remaining + 5)
    return json.loads(reply.stdout)


def fleet(config_path, host_name, out, candidate=None, base=None, budget_seconds=None):
    cfg, host, data, manifest_path = load(config_path, host_name, candidate, base)
    if budget_seconds is not None:
        if not math.isfinite(budget_seconds) or not 0 < budget_seconds <= 120:
            raise ValueError("one shared budget must be >0 and <=120 seconds")
        cfg["budget_seconds"] = budget_seconds
    out = absolute(out)
    out.mkdir(parents=True, exist_ok=False)
    cfg["hosts"][host_name].update(candidate=host["builds"]["candidate"], base=host["builds"]["base"])
    effective = out / "config.json"
    start = time.monotonic()
    setup_deadline = start + 900
    def setup_check():
        if time.monotonic() >= setup_deadline:
            raise BudgetExpired("input setup deadline exhausted")
    if host_name in (socket.gethostname(), socket.gethostname().split(".")[0], "localhost"):
        save(effective, cfg)
        with timing_lock(host["lock"]):
            if digest(manifest_path, setup_check) != cfg["manifest_sha256"]:
                raise Mismatch("manifest checksum differs before preparation")
            evidence = verify_inputs(cfg, host, data, read(manifest_path), setup_check)
        save(out / "setup.json", {"seconds": time.monotonic() - start, "input_verification": evidence,
                                  "measured_budget_seconds": cfg["budget_seconds"]})
        return supervise(effective, host_name, out)
    # Only cached setup is performed here; the host worker owns the single measured deadline.
    root = absolute(host["root"])
    run_root = root / ("quick-" + uuid.uuid4().hex)
    save(effective, cfg)
    lock = host["lock"]
    remote(["mkdir", "-p", str(run_root), str(root / "inputs"), str(root / "binaries")], host_name, lock)
    package = out / "package" / "bench"
    package.mkdir(parents=True)
    (package / "__init__.py").write_bytes(b"")
    for name in ("quick.py", "env.py", "arch_remote.sh", "arch_polars.py"):
        (package / name).write_bytes(Path(__file__).with_name(name).read_bytes())
    transfer([str(package.parent) + "/"], str(run_root) + "/", host_name, lock)
    manifest = read(manifest_path)
    if digest(manifest_path) != cfg["manifest_sha256"]:
        raise Mismatch("manifest checksum differs before transfer")
    files = {c["file"] for c in manifest_cases(manifest, cfg.get("only"))}
    inputs = [inside(data, n) for n in sorted(files)]
    destination_data = root / "inputs" / cfg["manifest_sha256"]
    objects = root / "inputs" / "objects"
    remote(["mkdir", "-p", str(destination_data), str(objects)], host_name, lock)
    local_hashes = InputHashes(out.parent)
    remote_evidence = []
    cfg["input_objects"] = {}
    for path in inputs:
        rel = path.relative_to(data)
        sha = manifest["files"][str(rel)]["sha256"]
        if not full_hash(sha) or path.stat().st_size != manifest["files"][str(rel)]["bytes"]:
            raise Mismatch("local input size/pin differs before transfer")
        local_hashes.verify(path, sha, setup_check)
        target = objects / sha
        probe = remote_input(run_root, root, target, sha, host_name, lock, setup_deadline - time.monotonic())
        if probe is None:
            transfer([path], target, host_name, lock)
            probe = remote_input(run_root, root, target, sha, host_name, lock, setup_deadline - time.monotonic())
            if probe is None:
                raise Mismatch("transferred input missing")
        remote_evidence.append(probe)
        cfg["input_objects"][str(rel)] = str(target)
        remote(["python3", "-c", "import pathlib,os,sys;p=pathlib.Path(sys.argv[2]);"
                "p.parent.mkdir(parents=True,exist_ok=True);"
                "os.link(sys.argv[1],p) if not p.exists() else None",
                str(target), str(destination_data / rel)], host_name, lock)
    transfer([manifest_path], destination_data / "manifest.json", host_name, lock)
    for name, pin in host["builds"].items():
        if digest(pin["path"]) != pin["sha256"]:
            raise Mismatch("binary checksum differs before transfer")
        target = root / "binaries" / pin["sha256"]
        # rsync compares content; it does not replace or delete another cache directory.
        transfer([pin["path"]], target, host_name, lock)
        cfg["hosts"][host_name][name]["path"] = str(target)
    cfg["data"] = str(destination_data)
    cfg["hosts"][host_name]["root"] = str(root)
    if cfg.get("reference"):
        transfer([absolute(cfg["reference"])], run_root / "reference.json", host_name, lock)
        cfg["reference"] = str(run_root / "reference.json")
    save(effective, cfg)
    transfer([effective], str(run_root) + "/", host_name, lock)
    save(out / "setup.json", {"seconds": time.monotonic() - start, "measured_budget_seconds": cfg["budget_seconds"],
                              "local_input_verification": local_hashes.evidence,
                              "input_verification": remote_evidence,
                              "host": host_name, "remote_root": str(run_root), "evidence_deleted": False})
    command = ["python3", "-m", "bench.quick", "--supervise", str(run_root / "config.json"),
               host_name, str(run_root)]
    code = subprocess.run(["ssh", "-o", "BatchMode=yes", host_name,
                           "cd " + shlex.quote(str(run_root)) + " && " + shlex.join(command)],
                          timeout=cfg["budget_seconds"] + 30).returncode
    subprocess.run(["rsync", "-a", "--protect-args", "--rsync-path",
                    shlex.join(["python3", "-c", LOCK_EXEC, lock, "rsync"]),
                    host_name + ":" + str(run_root) + "/", str(out) + "/"],
                   check=True, timeout=120)
    return code


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in ("--worker", "--supervise"):
        mode, config, host, out, *extra = argv
        return worker(config, host, out, float(extra[0])) if mode == "--worker" else supervise(config, host, out)
    parser = argparse.ArgumentParser(prog="bench quick", description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--candidate")
    parser.add_argument("--base")
    parser.add_argument("--out", required=True)
    parser.add_argument("--budget-seconds", type=float)
    args = parser.parse_args(argv)
    started = time.monotonic()
    try:
        return fleet(args.config, args.host, args.out, args.candidate, args.base, args.budget_seconds)
    except (ValueError, KeyError, OSError, subprocess.SubprocessError, Mismatch, BudgetExpired) as error:
        out = Path(args.out)
        effective = out / "config.json"
        if effective.is_file():
            cfg = read(effective)
            reason = ("setup deferred: host timing lock held" if getattr(error, "returncode", None) == 75 else
                      str(error) or type(error).__name__)
            if not (out / "setup.json").exists():
                save(out / "setup.json", {"state": "failed", "seconds": time.monotonic() - started,
                                          "reason": reason, "measured_budget_seconds": cfg["budget_seconds"]})
            recover(effective, args.host, out, time.monotonic() + cfg["budget_seconds"], reason)
            if getattr(error, "returncode", None) == 75:
                report = read(out / "report.json")
                report.update(state="deferred", merge_gate="deferred")
                save(out / "report.json", report)
        parser.exit(2, "bench quick: " + str(error) + "\n")


if __name__ == "__main__":
    sys.exit(main())
