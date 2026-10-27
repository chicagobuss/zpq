"""The engine scope's spawner: runs one engine process per request and reports what the controller cannot see.

    spawn.py   (stdin: one JSON request per line; stdout: one JSON reply per line)

It runs inside the engine's cgroup scope, so the scope's pressure counters cover engine work only: the controller,
oracle and validation stay outside. Standalone on purpose (no bench imports): it starts under any interpreter.
Request: {argv, env, stdout, stderr, timeout_s, cpus}. Reply: {rc, signal, timeout, wall_ns, cpu_ns, rss_kib,
read_bytes, busy_ticks, psi: {host: {...}, scope: {...}}} with every PSI counter as an after-minus-before delta.
"""

import json
import os
import signal
import sys
import time
from pathlib import Path

PRESSURE = {"io_some": ("io", "some"), "io_full": ("io", "full"), "memory_some": ("memory", "some"),
            "memory_full": ("memory", "full")}


def pressure(path_of):
    """Cumulative stall microseconds per counter, or None where the kernel has no PSI."""
    out = {}
    for key, (resource, line) in PRESSURE.items():
        try:
            text = path_of(resource).read_text()
        except OSError:
            return None
        out[key] = next(int(f.split("=")[1]) for row in text.splitlines() if row.startswith(line)
                        for f in row.split() if f.startswith("total="))
    return out


def host_psi(resource):
    return Path("/proc/pressure") / resource


def scope_psi():
    group = next(line.split(":", 2)[2] for line in Path("/proc/self/cgroup").read_text().splitlines()
                 if line.startswith("0::"))
    root = Path("/sys/fs/cgroup") / group.strip().lstrip("/")
    return lambda resource: root / f"{resource}.pressure"


def cpu_ticks(cpus):
    """Busy ticks (everything but idle and iowait) summed over `cpus`."""
    total = 0
    for line in Path("/proc/stat").read_text().splitlines():
        f = line.split()
        if f[0].startswith("cpu") and f[0][3:].isdigit() and int(f[0][3:]) in cpus:
            ticks = list(map(int, f[1:9]))
            total += sum(ticks) - ticks[3] - ticks[4]
    return total


def delta(after, before):
    return None if after is None or before is None else {k: after[k] - before[k] for k in after}


def run(req, scope):
    cpus = set(req["cpus"])
    env = {**os.environ, **req.get("env", {})}
    with open(req["stdout"], "wb") as out, open(req["stderr"], "wb") as err:
        host0, scope0, busy0 = pressure(host_psi), pressure(scope), cpu_ticks(cpus)
        start = time.monotonic_ns()
        pid = os.fork()
        if pid == 0:
            try:
                os.setpgid(0, 0)
                os.sched_setaffinity(0, cpus)
                os.dup2(out.fileno(), 1)
                os.dup2(err.fileno(), 2)
                os.execvpe(req["argv"][0], req["argv"], env)
            finally:
                os._exit(127)
        deadline, timed_out = start + int(req["timeout_s"] * 1e9), False
        while True:
            done, status, usage = os.wait4(pid, os.WNOHANG)
            if done:
                break
            if time.monotonic_ns() > deadline:
                timed_out = True
                os.killpg(pid, signal.SIGKILL)
                _, status, usage = os.wait4(pid, 0)
                break
            time.sleep(0.0005)
        wall = time.monotonic_ns() - start
        host1, scope1, busy1 = pressure(host_psi), pressure(scope), cpu_ticks(cpus)
    try:
        os.killpg(pid, signal.SIGKILL)  # a daemonized grandchild must not outlive its query
    except ProcessLookupError:
        pass
    code = os.waitstatus_to_exitcode(status)
    return {"rc": code, "signal": -code if code < 0 else None, "timeout": timed_out, "wall_ns": wall,
            "cpu_ns": int((usage.ru_utime + usage.ru_stime) * 1e9), "rss_kib": usage.ru_maxrss,
            "read_bytes": usage.ru_inblock * 512, "busy_ticks": busy1 - busy0,
            "psi": {"host": delta(host1, host0), "scope": delta(scope1, scope0)}}


def main():
    scope = scope_psi()
    print(json.dumps({"ready": True, "psi": pressure(scope) is not None}), flush=True)
    for line in sys.stdin:
        try:
            reply = run(json.loads(line), scope)
        except Exception as error:  # noqa: BLE001 - reported to the controller, which records the failure
            reply = {"error": f"{type(error).__name__}: {error}"}
        print(json.dumps(reply), flush=True)


if __name__ == "__main__":
    main()
