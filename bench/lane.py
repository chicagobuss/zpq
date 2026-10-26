"""One lane on one host: plan, run, validate, detect stalls, re-run and quarantine. Records go to journal.jsonl.

Rounds. Round 0 runs every case rep-major (`for rep: for case: for engine:`), so one host episode touches one rep
of many cells rather than every rep of one. A case whose cohort was disturbed (a stalled or CPU-contended rep, an
eviction that did not verify, a host short of memory) is re-run whole, every engine and rep, at the end of the
lane: at most twice. A case still disturbed after that is quarantined: no median, listed with its evidence.

Answers live on tmpfs only while they are judged; the journal keeps their hashes and verdicts. A rep whose answer
hashes like an earlier validated answer of the same case and engine takes that verdict; any other answer goes to
`zpq-check compare`, the same comparer the correctness gates use.
"""

import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from bench import engines, env, stalls

ROOT = engines.ROOT
FINAL = ("ok", "failed", "wrong", "timeout")
MIN_MEMORY = 2 << 30


def canonical_hash(path, ordered):
    """sha256 of an answer's rows, value text verbatim: in sequence when the query orders them, else sorted."""
    text = Path(path).read_text().strip()
    if not text:
        rows = []
    elif text[0] == "[":
        rows = json.loads(text, parse_float=str, parse_int=str)
    else:
        rows = [json.loads(line, parse_float=str, parse_int=str) for line in text.splitlines() if line.strip()]
    rows = [json.dumps(list(r.items()) if isinstance(r, dict) else r, sort_keys=False) for r in rows]
    if not ordered:
        rows.sort()
    return hashlib.sha256("\n".join(rows).encode()).hexdigest(), len(rows)


class Judge:
    """A long-lived `zpq-check compare` on the housekeeping CPUs: its oracle hashes each input once."""

    def __init__(self, zpq_check, duckdb, python, cpus, cache):
        argv = [str(zpq_check), "compare", "--duckdb", str(duckdb), "--python", str(python),
                "--threads", str(min(8, len(cpus)))] + (["--cache", str(cache)] if cache else ["--no-cache"])
        self.proc = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, cwd=ROOT,
                                     preexec_fn=lambda: os.sched_setaffinity(0, set(cpus)))

    def __call__(self, case, answer, parquet, text_numbers=False):
        truth = case["truth"]
        spec = {"compare": truth["mode"], "sql": truth["sql"], "setup": truth["setup"], "ranks": truth["ranks"],
                "text_numbers": text_numbers}
        req = {"fixture": case["inputs"][0], "answer": str(answer), "parquet": parquet, "spec": spec}
        self.proc.stdin.write(json.dumps(req) + "\n")
        self.proc.stdin.flush()
        line = self.proc.stdout.readline()
        if not line:
            raise RuntimeError("zpq-check compare exited")
        reply = json.loads(line)
        return reply["ok"], reply.get("reason")

    def close(self):
        self.proc.stdin.close()
        self.proc.wait(timeout=60)


class Spawner:
    """bench/spawn.py inside the engine scope (systemd-run --user --scope, MemoryMax), or in our own cgroup."""

    def __init__(self, python, memory_max, scope=True):
        argv = [python, "-u", str(ROOT / "bench/spawn.py")]
        if scope:
            argv = ["systemd-run", "--user", "--scope", "--quiet", "--collect", "-p", f"MemoryMax={memory_max}",
                    "-p", "MemorySwapMax=0"] + argv
        self.proc = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        self.ready = json.loads(self.proc.stdout.readline())

    def __call__(self, **req):
        self.proc.stdin.write(json.dumps(req) + "\n")
        self.proc.stdin.flush()
        reply = json.loads(self.proc.stdout.readline())
        if "error" in reply:
            raise RuntimeError("spawner: " + reply["error"])
        return reply

    def close(self):
        self.proc.stdin.close()
        self.proc.wait(timeout=30)


def decodes(python, path):
    """PyArrow decodes every page: a written file is valid before its rows are compared."""
    code = ("import sys, pyarrow.parquet as pq; "
            "print(sum(b.num_rows for b in pq.ParquetFile(sys.argv[1]).iter_batches()))")
    done = subprocess.run([python, "-c", code, str(path)], capture_output=True, text=True)
    return (int(done.stdout), None) if done.returncode == 0 else (None, done.stderr.strip()[-300:])


class Lane:
    def __init__(self, run_dir, header, cases, spawner, judge, tmp):
        self.dir, self.header, self.spawn, self.judge, self.tmp = Path(run_dir), header, spawner, judge, Path(tmp)
        self.cases = {c["id"]: c for c in cases}
        self.lane, self.reps, self.threads = header["lane"], header["reps"], len(header["placement"]["timing"])
        self.timing = header["placement"]["timing"]
        self.smoke = header.get("smoke", False)
        self.journal = self.dir / "journal.jsonl"
        self.done = {}
        self.verdicts = {}  # (case, engine) -> {answer hash: (validation, check)}
        if self.journal.exists():
            for line in self.journal.read_text().splitlines():
                r = json.loads(line)
                self.note(r)

    def note(self, r):
        self.done[(r["case"], r["engine"], r["rep"], r["round"])] = r
        if r.get("answer_sha256") and r.get("method") == "full":
            self.verdicts.setdefault((r["case"], r["engine"]), {})[r["answer_sha256"]] = (r["validation"], r["check"])

    def write(self, r):
        with self.journal.open("a") as f:
            f.write(json.dumps(r, sort_keys=True) + "\n")
            f.flush()
            os.fsync(f.fileno())
        self.note(r)

    def condition(self, case):
        """Cold: evict and verify; hot and engine: warm and verify. Then the host must be quiet and have memory."""
        verified, state = (env.evict if self.lane == "cold" else env.warm)(case["inputs"])
        quiet = env.idle(self.timing)
        available = int(next(l.split()[1] for l in Path("/proc/meminfo").read_text().splitlines()
                             if l.startswith("MemAvailable:"))) * 1024
        reasons = [r for r, bad in (("inputs-unverified", not verified), ("busy-before", not quiet["quiet"]),
                                    ("memory-before", available < MIN_MEMORY)) if bad and not self.smoke]
        resident = max((s["resident"] / max(1, s["pages"]) for s in state.values()), default=0)
        return reasons, {"resident_fraction": round(resident, 6), "busy_cores": round(quiet["busy_cores"], 3),
                         "mem_available": available}

    def judge_answer(self, case, engine, answer, parquet):
        """(validation, method, check, answer hash, rows)."""
        if parquet:
            rows, why = decodes(self.header["pyarrow_python"], answer)
            if rows is None:
                return "wrong", "full", "PyArrow rejected the file: " + why, None, None
            digest = engines.sha256(answer)
        else:
            try:
                digest, rows = canonical_hash(answer, bool(case["truth"]["ranks"]))
            except (ValueError, AttributeError) as error:
                return "wrong", "full", f"answer is not JSON rows: {error}"[:300], None, None
        known = self.verdicts.get((case["id"], engine), {})
        if digest in known:
            return known[digest][0], "hash", known[digest][1], digest, rows
        # DuckDB's JSON prints HUGEINT as a string; nothing else may.
        ok, reason = self.judge(case, answer, parquet, self.header["engines"][engine]["kind"] == "duckdb")
        return ("ok" if ok else "wrong"), "full", reason or "", digest, rows

    def record(self, case, engine, rep, rnd, **fields):
        base = {"case": case["id"], "suite": case["suite"], "engine": engine, "lane": self.lane, "rep": rep,
                "round": rnd, "at_ns": time.time_ns(), "status": "failed", "validation": "unchecked"}
        self.write({**base, **fields})

    def fresh(self, case, engine, rep, rnd):
        spec = self.header["engines"][engine]
        work = Path(tempfile.mkdtemp(dir=self.tmp))
        try:
            answer, sink = work / "answer.json", str(work / "out.parquet")
            reasons, cond = self.condition(case)
            if reasons:
                return self.record(case, engine, rep, rnd, status="disturbed", reasons=reasons, conditioning=cond)
            argv, extra, on_stdout = engines.fresh(engine, spec, case, self.threads, answer, sink)
            stdout = answer if on_stdout else work / "stdout"
            reply = self.spawn(argv=argv, env=extra, stdout=str(stdout), stderr=str(work / "stderr"),
                               timeout_s=self.header["timeout_s"], cpus=self.timing)
            self.finish(case, engine, rep, rnd, work, reply, cond, answer, sink, reply["wall_ns"])
        finally:
            shutil.rmtree(work, ignore_errors=True)
            if self.lane == "cold":
                env.evict(case["inputs"], attempts=1)  # the oracle's reads must not warm the next cold rep

    def finish(self, case, engine, rep, rnd, work, reply, cond, answer, sink, elapsed, foreign=None):
        parquet = case["output"] == "parquet"
        foreign = foreign or env.foreign_cpu(reply, self.timing)
        fields = {"wall_ns": reply["wall_ns"], "elapsed_ns": elapsed, "rc": reply["rc"], "cpu_ns": reply["cpu_ns"],
                  "rss_kib": reply["rss_kib"], "read_bytes": reply["read_bytes"], "psi": reply["psi"],
                  "foreign": foreign, "conditioning": cond}
        if reply["timeout"]:
            return self.record(case, engine, rep, rnd, status="timeout", reason="timed out", **fields)
        if reply["rc"] != 0:
            err = (work / "stderr").read_text(errors="replace").strip().splitlines()
            return self.record(case, engine, rep, rnd, reason=(err[-1] if err else f"exit {reply['rc']}")[:300],
                               **fields)
        target = Path(sink) if parquet else Path(answer)
        if not target.is_file():
            return self.record(case, engine, rep, rnd, reason="no answer written", **fields)
        validation, method, check, digest, rows = self.judge_answer(case, engine, target, parquet)
        if self.lane == "cold":
            env.evict(case["inputs"], attempts=1)
        status = "ok" if validation == "ok" else "wrong"
        if status == "ok" and foreign["contended"] and not self.smoke:
            status, fields["reasons"] = "disturbed", ["contention-during"]
        self.record(case, engine, rep, rnd, status=status, validation=validation, method=method, check=check,
                    answer_sha256=digest, rows=rows, bytes_out=target.stat().st_size, **fields)

    def session(self, case, engine, rnd):
        """Engine lane: warm-up plus `reps` queries in one process; zpq runs each as a fresh process instead."""
        spec = self.header["engines"][engine]
        if spec["kind"] == "zpq":
            for rep in range(0, self.reps + 1):
                if (case["id"], engine, rep, rnd) not in self.done:
                    self.fresh(case, engine, rep, rnd)
            return
        if all((case["id"], engine, rep, rnd) in self.done for rep in range(self.reps + 1)):
            return
        work = Path(tempfile.mkdtemp(dir=self.tmp))
        try:
            runs = self.reps + 1
            answers = [str(work / f"answer{i}.json") for i in range(runs)]
            sinks = [str(work / f"out{i}.parquet") for i in range(runs)] if case["output"] == "parquet" else None
            reasons, cond = self.condition(case)
            if reasons:
                for rep in range(runs):
                    self.record(case, engine, rep, rnd, status="disturbed", reasons=reasons, conditioning=cond)
                return
            argv, extra = engines.session(engine, spec, case, self.threads, answers, sinks, work / "session.sql")
            stdout = work / "stdout"
            reply = self.spawn(argv=argv, env=extra, stdout=str(stdout), stderr=str(work / "stderr"),
                               timeout_s=self.header["timeout_s"] * runs, cpus=self.timing)
            try:
                times = (engines.split_session(stdout.read_text(), answers) if spec["kind"] == "duckdb" else
                         json.loads(stdout.read_text())["engine_ns"]) if reply["rc"] == 0 else [None] * runs
            except (ValueError, KeyError) as error:
                reply = {**reply, "rc": reply["rc"] or 1}
                (work / "stderr").write_text(f"session output: {error}")
                times = [None] * runs
            # The session's counters (PSI, CPU, bytes read) cover all its runs; each run carries them.
            foreign = env.foreign_cpu(reply, self.timing)
            for rep, elapsed in enumerate(times):
                self.finish(case, engine, rep, rnd, work, reply, cond, answers[rep], sinks[rep] if sinks else "",
                            elapsed, foreign)
        finally:
            shutil.rmtree(work, ignore_errors=True)

    def run_round(self, ids, rnd, engines_):
        first = 0 if self.lane == "hot" else 1  # hot: rep 0 is the warm-up; cold reps are all timed
        if self.lane == "engine":
            for cid in ids:
                for e in engines_:
                    self.session(self.cases[cid], e, rnd)
            return
        for rep in range(first, self.reps + 1):
            for cid in ids:
                for e in engines_:
                    if (cid, e, rep, rnd) not in self.done:
                        self.fresh(self.cases[cid], e, rep, rnd)

    def run(self, engines_, max_rounds=3):
        ids = list(self.cases)
        for rnd in range(max_rounds):
            self.run_round(ids, rnd, engines_)
            ids = sorted(stalls.disturbed(self.records(), rnd))
            if not ids:
                break
            print(f"bench: round {rnd}: {len(ids)} disturbed cases" + ("; re-running at the end of the lane"
                                                                        if rnd + 1 < max_rounds else ""), flush=True)

    def records(self):
        return list(self.done.values())


def header(args, cases, names, timing, housekeeping):
    sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    dirty = subprocess.run(["git", "status", "--porcelain", "--untracked-files=no", "--", "bench", "tools/check"],
                           cwd=ROOT, capture_output=True, text=True).stdout.strip()
    tree = subprocess.run(["git", "rev-parse", "HEAD:bench"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    paths = {"zpq": args.binary, "zpq_prev": args.prev, "duckdb20": args.duckdb, "polars": args.polars}
    inputs = sorted({p for c in cases for p in c["inputs"]})
    return {
        "schema": "zpq-bench-v1", "lane": args.lane, "reps": args.reps, "timeout_s": args.timeout,
        "host": socket.gethostname().split(".")[0], "kernel": os.uname().release, "sha": sha,
        "bench_tree": tree, "tree_clean": not dirty, "started_ns": time.time_ns(),
        "cpu": next((l.split(":", 1)[1].strip() for l in Path("/proc/cpuinfo").read_text().splitlines()
                     if l.startswith("model name")), "unknown"),
        "placement": {"timing": timing, "housekeeping": housekeeping, "shared": timing == housekeeping},
        "psi": env.psi_available(), "pyarrow_python": args.pyarrow, "smoke": args.smoke,
        "engines": {n: engines.describe(n, paths[n]) for n in names},
        "zpq_check_sha256": engines.sha256(args.zpq_check),
        "cases_sha256": hashlib.sha256(json.dumps(cases, sort_keys=True).encode()).hexdigest(),
        "inputs": {p: {"bytes": os.path.getsize(p), "mtime_ns": os.stat(p).st_mtime_ns} for p in inputs},
        "argv": sys.argv,
    }
