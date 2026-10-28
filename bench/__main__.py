"""The scorecard: zpq against DuckDB 2.0 and Polars 2.0, with the previous zpq as a same-host control.

    python -m bench run --host H --lane cold|hot|engine --binary ZPQ --prev ZPQ_PREV [--suites S,...] [--only ID]
                        [--reps N] [--out DIR] [--resume RUN_DIR]
    python -m bench score RUN_DIR...
    python -m bench replay OLD_RUN_DIR --out DIR      rescore a scorecard-v1 cold run under these rules

`run` on another host ships bench/, zpq-check and the two zpq binaries there over ssh, runs, and copies the run
directory back. Exit: 0 COMPLETE, 2 INCOMPLETE, 3 UNCERTIFIED, 1 on error. The ship bar is reported, not an exit.
"""

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from bench import engines, env, export, lane, replay, requests, score, suites

ROOT = engines.ROOT
EXIT = {"COMPLETE": 0, "INCOMPLETE": 2, "UNCERTIFIED": 3}


def busy():
    """Why this host must not time now: a gate or a full tier is running. Another bench is the timing lock's job."""
    gates = subprocess.run(["pgrep", "-f", "[v]erify.py full|[D]tier=|[z]ig build check"], capture_output=True)
    return "a gate is running" if gates.returncode == 0 else None


def default_python():
    for p in (ROOT / ".venv/bin/python", engines.POLARS):
        if Path(p).is_file():
            return str(p)
    return sys.executable


def run_remote(a):
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    remote = f".cache/zpq-bench/{sha[:12]}"
    stage = Path(tempfile.mkdtemp())
    try:
        (stage / "bin").mkdir()
        shutil.copytree(ROOT / "bench", stage / "bench", ignore=shutil.ignore_patterns("__pycache__", "tests"))
        for name, path in (("zpq", a.binary), ("zpq_prev", a.prev), ("zpq-check", a.zpq_check)):
            shutil.copy2(path, stage / "bin" / name)
        subprocess.run(["ssh", a.host, f"mkdir -p {remote}"], check=True)
        subprocess.run(["rsync", "-a", "--delete", f"{stage}/", f"{a.host}:{remote}/"], check=True)
    finally:
        shutil.rmtree(stage)
    passthrough = sys.argv[2:]
    for flag, value in (("--binary", "bin/zpq"), ("--prev", "bin/zpq_prev"), ("--zpq-check", "bin/zpq-check"),
                        ("--out", "runs")):
        if flag in passthrough:
            i = passthrough.index(flag)
            del passthrough[i:i + 2]
        passthrough += [flag, value]
    code = subprocess.run(["ssh", a.host, f"cd {remote} && {a.remote_python} -m bench run "
                           + " ".join(map(sh_quote, passthrough))]).returncode
    if subprocess.run(["ssh", a.host, f"test -d {remote}/runs"]).returncode == 0:
        out = Path(a.out).expanduser()
        out.mkdir(parents=True, exist_ok=True)
        subprocess.run(["rsync", "-a", f"{a.host}:{remote}/runs/", f"{out}/"], check=True)
    return code


def sh_quote(s):
    return "'" + s.replace("'", "'\"'\"'") + "'"


def run(a):
    if a.host != socket.gethostname().split(".")[0]:
        return run_remote(a)
    if a.resume:
        run_dir = Path(a.resume)
        head = json.loads((run_dir / "header.json").read_text())
    else:
        head = None
    cases = suites.cases(a.suites.split(","), a.only.split(",") if a.only else None)
    names = a.engines.split(",")
    timing, housekeeping = env.placement()
    os.sched_setaffinity(0, set(housekeeping))
    fresh = lane.header(a, cases, names, timing, housekeeping)
    if head:
        same = ("lane", "reps", "engines", "cases_sha256", "bench_tree", "zpq_check_sha256", "inputs", "placement")
        changed = [k for k in same if head.get(k) != fresh.get(k)]
        if changed:
            raise SystemExit(f"bench: cannot resume, changed: {', '.join(changed)}")
    else:
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        run_dir = Path(a.out).expanduser() / f"{fresh['host']}-{fresh['sha'][:12]}-{a.lane}-{stamp}"
        run_dir.mkdir(parents=True)
        (run_dir / "header.json").write_text(json.dumps(fresh, indent=1, sort_keys=True) + "\n")
        (run_dir / "planned.json").write_text(json.dumps([c["id"] for c in cases]) + "\n")
        head = fresh
    if not head["tree_clean"]:
        print("bench: bench/ or tools/check has uncommitted changes: this run is unofficial", file=sys.stderr)
    why = None if a.smoke else busy()
    if why:
        raise SystemExit(f"bench: {why}; this host is not quiet")
    tmp = Path(tempfile.mkdtemp(prefix="zpq-bench-", dir="/dev/shm" if Path("/dev/shm").is_dir() else None))
    with env.timing_lock():
        spawner = lane.Spawner(a.spawn_python, a.memory_max, scope=not a.no_scope)
        judge = lane.Judge(a.zpq_check, a.duckdb, a.pyarrow, housekeeping, None if a.no_cache else a.cache)
        try:
            lane.Lane(run_dir, head, cases, spawner, judge, tmp).run(names)
        finally:
            spawner.close()
            judge.close()
            shutil.rmtree(tmp, ignore_errors=True)
    return show([run_dir])


def show(run_dirs):
    code = 0
    for d in run_dirs:
        d = Path(d)
        head = json.loads((d / "header.json").read_text())
        records = [json.loads(l) for l in (d / "journal.jsonl").read_text().splitlines()] if (
            d / "journal.jsonl").exists() else []
        planned = json.loads((d / "planned.json").read_text()) if (d / "planned.json").exists() else sorted(
            {r["case"] for r in records})
        s = score.score(head, records, planned, max_round=head.get("max_round", 2))
        (d / "score.json").write_text(json.dumps(s, indent=1, sort_keys=True, default=str) + "\n")
        print(score.render(s), end="")
        code = max(code, EXIT[s["verdict"]])
    return code


def main():
    if len(sys.argv) > 1 and sys.argv[1] in ("quick", "arch"):
        from bench import quick
        sys.exit(quick.main(sys.argv[2:]))
    p = argparse.ArgumentParser(prog="bench", description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run")
    r.add_argument("--host", required=True)
    r.add_argument("--lane", required=True, choices=("cold", "hot", "engine"))
    r.add_argument("--binary", default=str(ROOT / "zig-out/bin/zpq"))
    r.add_argument("--prev", required=True, help="the control: the previous zpq build")
    r.add_argument("--suites", default=",".join(suites.SUITES))
    r.add_argument("--only", help="case ids or id prefixes, comma-separated (a smaller experiment)")
    r.add_argument("--engines", default="zpq,zpq_prev,duckdb20,polars")
    r.add_argument("--reps", type=int, default=3)
    r.add_argument("--timeout", type=float, default=120)
    r.add_argument("--out", default=str(Path.home() / "bench/runs"))
    r.add_argument("--resume")
    r.add_argument("--duckdb", default=str(engines.DUCKDB))
    r.add_argument("--polars", default=str(engines.POLARS))
    r.add_argument("--zpq-check", default=str(ROOT / "zig-out/bin/zpq-check"))
    r.add_argument("--pyarrow", default=default_python(), help="a Python with PyArrow (written files are decoded)")
    r.add_argument("--spawn-python", default=sys.executable)
    r.add_argument("--remote-python", default="python3")
    r.add_argument("--memory-max", default="24G")
    r.add_argument("--cache", default=str(Path.home() / ".cache/zpq-check/oracle"))
    r.add_argument("--no-cache", action="store_true")
    r.add_argument("--no-scope", action="store_true", help="engines in our own cgroup (no systemd user manager)")
    r.add_argument("--smoke", action="store_true", help="harness check on a busy host: quietness is recorded, never "
                   "enforced, and the lane is UNCERTIFIED")
    s = sub.add_parser("score")
    s.add_argument("runs", nargs="+")
    x = sub.add_parser("replay")
    x.add_argument("old")
    x.add_argument("--out", required=True)
    for name in ("request", "export"):
        command = sub.add_parser(name, help="prepare an offline request" if name == "request" else "export saved runs")
        command.add_argument("request")
        command.add_argument("--out", required=True)
        if name == "request":
            command.add_argument("--catalog")
            command.add_argument("--inventory")
    a = p.parse_args()
    if a.command in ("request", "export"):
        try:
            if a.command == "request":
                requests.write_packet(a.out, requests.packet(a.request, a.catalog, a.inventory))
            else:
                result = export.export(a.request, a.out)
                print(json.dumps(result["coverage"], sort_keys=True))
        except (ValueError, OSError, KeyError, TypeError) as error:
            p.exit(1, f"bench {a.command}: {error}\n")
        return
    if a.command == "run":
        sys.exit(run(a))
    if a.command == "score":
        sys.exit(show(a.runs))
    sys.exit(show([replay.convert(Path(a.old), Path(a.out))]))


if __name__ == "__main__":
    main()
