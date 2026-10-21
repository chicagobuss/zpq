#!/usr/bin/env python3
"""Torture harness: rebuild every crafted case and check zpq's behaviour against its expected outcome.

Each case (tools/torture/cases_*.py) generates a small file deterministically and lists the zpq invocations to run on
it with the outcome each must have:

  err         exit 1 with a non-empty error on stderr (untrusted bytes must fail cleanly)
  eq:V        exit 0 and the JSON rows on stdout equal V
  err|eq:V    either of the two (metadata contradictions: detect and fail, or ignore the lie and answer right)
  safe        exit 0 or 1 (inspect/schema on garbage); never a signal, trap, hang or OOM
  refuse      exit 2 (an unsupported query refused before any data is read)

Every run is bounded (timeout 60 s, MemoryMax 4G, peak RSS recorded). A signal, a safety-check panic, a timeout or an
exit code outside {0, 1} always fails the case. Writes (`-o`) are read back with pyarrow and must match.

Usage:
  .venv/bin/python -I tools/torture.py [--bin PATH ...] [--only SUBSTR] [--out DIR] [--list]
Default binaries: $ZPQ_SAFE (a ReleaseSafe build) and $ZPQ_FAST (the shipped ReleaseFast binary) when set, else
zig-out/bin/zpq.
"""

import argparse
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import time

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "torture"))

import cases_reader  # noqa: E402
import cases_writer  # noqa: E402
from common import CASES  # noqa: E402

TIMEOUT = 60
MEM = os.environ.get("TORTURE_MEM", "4G")


def bounded(argv, mem=MEM, timeout=TIMEOUT, env=None):
    """Run argv under a memory-capped scope, timeout and /usr/bin/time -v; returns a result dict."""
    tf = f"/tmp/torture-time-{os.getpid()}.txt"
    cmd = ["systemd-run", "--user", "--scope", "-q", "--collect", "-p", f"MemoryMax={mem}", "-p", "MemorySwapMax=0",
           "-p", "CPUQuota=400%", "--", "taskset", "-c", "8-15,24-31", "/usr/bin/time", "-v", "-o", tf,
           "timeout", "-s", "KILL", str(timeout)] + argv
    t0 = time.monotonic()
    p = subprocess.run(cmd, capture_output=True, env=env)
    wall = time.monotonic() - t0
    rss = None
    sig = None
    try:
        txt = open(tf).read()
        m = re.search(r"Maximum resident set size \(kbytes\): (\d+)", txt)
        rss = int(m.group(1)) if m else None
        m = re.search(r"Command terminated by signal (\d+)", txt)
        sig = int(m.group(1)) if m else None
        os.unlink(tf)
    except FileNotFoundError:
        pass
    rc = p.returncode
    return {"rc": rc, "sig": sig, "out": p.stdout, "err": p.stderr, "wall": wall, "rss_kb": rss}


def judge(res, expect, readback=None):
    """Return (ok, verdict text)."""
    err = res["err"].decode(errors="replace")
    if res["sig"] is not None or res["rc"] >= 128 or res["rc"] == 124:
        what = "timeout" if res["rc"] in (124, 137) and res["wall"] >= TIMEOUT - 1 else f"signal/rc {res['rc']}"
        if res["rc"] == 137 and res["wall"] < TIMEOUT - 1:
            what = "killed (OOM at MemoryMax?)"
        return False, f"CRASH {what} sig={res['sig']} {err[-300:]!r}"
    if "panic:" in err or "reached unreachable" in err:
        return False, f"PANIC {err[-400:]!r}"
    if res["rc"] not in (0, 1, 2):
        return False, f"rc {res['rc']} {err[-200:]!r}"
    kinds = expect["kind"].split("|")
    if res["rc"] == 2 and "refuse" in kinds:
        return True, "refused (exit 2)"
    if res["rc"] == 1 and ("err" in kinds or "safe" in kinds):
        if "err" in kinds and not err.strip():
            return False, "exit 1 with an empty error"
        return True, f"err: {err.strip().splitlines()[-1][:160] if err.strip() else ''}"
    if res["rc"] == 0 and "safe" in kinds:
        return True, "ok"
    if res["rc"] == 0 and "eq" in kinds:
        got = parse_rows(res["out"]) if readback is None else readback
        if got == expect["value"]:
            return True, "eq"
        return False, f"WRONG got {str(got)[:300]} want {str(expect['value'])[:300]}"
    return False, f"unexpected rc {res['rc']} (want {expect['kind']}): {err.strip()[-300:]!r} out={res['out'][:200]!r}"


def parse_rows(out):
    txt = out.decode(errors="replace").strip()
    if not txt:
        return []
    rows = []
    for line in txt.splitlines():
        rows.append(json.loads(line))
    return rows


def readback_rows(path):
    import pyarrow.parquet as pq

    t = pq.read_table(path)
    return [dict(r) for r in t.to_pylist()]


def run_case(c, bins, outdir, log):
    cdir = outdir / "cases"
    cdir.mkdir(parents=True, exist_ok=True)
    path = cdir / f"{c.name}.parquet"
    spec = c.build(path)
    results = []
    for binname, binpath in bins:
        for run in spec["runs"]:
            if "check" in run:
                # Writer checks run several commands (e.g. every -j) and compare outputs themselves.
                ctx = {"bin": str(binpath), "path": path, "cdir": cdir, "name": c.name, "bounded": bounded}
                ok, verdict, res = run["check"](ctx)
                rec = {"case": c.name, "bullet": c.bullet, "bin": binname, "label": run["label"], "ok": ok,
                       "verdict": verdict, "rc": res.get("rc"), "rss_kb": res.get("rss_kb"),
                       "wall": round(res.get("wall", 0), 3)}
                results.append(rec)
                log.write(json.dumps(rec) + "\n")
                log.flush()
                print(f"{'PASS' if ok else (f'KNOWN[{c.known}]' if c.known else 'FAIL')} {c.name:44s} {binname:5s} {run['label']:12s} rc={res.get('rc')} "
                      f"rss={res.get('rss_kb')}KB {res.get('wall', 0):.2f}s {verdict[:150]}")
                continue
            argv = [str(binpath)] + [a.replace("{F}", str(path)).replace("{O}", str(cdir / f"{c.name}.out.parquet"))
                                    for a in run["argv"]]
            o = cdir / f"{c.name}.out.parquet"
            if o.exists():
                o.unlink()
            if "needs_out" in run:
                # The run reads zpq's own output: write it first (-j 1) from the case's input.
                bounded([str(binpath), "query", str(path)] + run["needs_out"] + ["-j", "1", "-o", str(o)])
            res = bounded(argv, mem=run.get("mem", MEM), env=run.get("env"))
            readback = None
            if res["rc"] == 0 and "-o" in run["argv"] and "eq" in run["expect"]["kind"]:
                try:
                    readback = run.get("readback", readback_rows)(o)
                except Exception as e:  # noqa: BLE001
                    readback = f"readback failed: {e}"
            ok, verdict = judge(res, run["expect"], readback)
            rec = {"case": c.name, "bullet": c.bullet, "bin": binname, "label": run["label"], "ok": ok,
                   "verdict": verdict, "rc": res["rc"], "rss_kb": res["rss_kb"], "wall": round(res["wall"], 3)}
            results.append(rec)
            log.write(json.dumps(rec) + "\n")
            log.flush()
            mark = "PASS" if ok else (f"KNOWN[{c.known}]" if c.known else "FAIL")
            print(f"{mark} {c.name:44s} {binname:5s} {run['label']:12s} rc={res['rc']} rss={res['rss_kb']}KB "
                  f"{res['wall']:.2f}s {verdict[:150]}")
    if spec.get("cleanup"):
        for p in cdir.glob(f"{c.name}*"):
            p.unlink()
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", action="append", default=[], help="NAME=PATH (repeatable)")
    ap.add_argument("--only", action="append", default=[])
    ap.add_argument("--out", default=os.environ.get("TORTURE_OUT", "/tmp/zpq-torture"))
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--big", action="store_true", help="also run the multi-GB cases (generated on the fly, deleted)")
    a = ap.parse_args()
    if a.list:
        for c in CASES:
            print(f"{c.name:44s} {c.bullet}")
        return 0
    bins = [tuple(b.split("=", 1)) for b in a.bin]
    if not bins:
        for k, n in (("ZPQ_SAFE", "safe"), ("ZPQ_FAST", "fast")):
            if os.environ.get(k):
                bins.append((n, os.environ[k]))
    if not bins:
        bins = [("local", str(HERE.parent / "zig-out" / "bin" / "zpq"))]
    outdir = pathlib.Path(a.out)
    outdir.mkdir(parents=True, exist_ok=True)
    fails = 0
    known = 0
    total = 0
    with open(outdir / "results.jsonl", "a") as log:
        for c in CASES:
            if a.only and not any(s in c.name for s in a.only):
                continue
            if getattr(c, "big", False) and not a.big:
                continue
            for r in run_case(c, bins, outdir, log):
                total += 1
                if not r["ok"] and c.known:
                    known += 1
                else:
                    fails += not r["ok"]
    print(f"\n{total - fails - known}/{total} runs as expected, {known} reproduce known findings, {fails} new failures")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
