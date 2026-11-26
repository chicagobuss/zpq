#!/usr/bin/env python3
"""What a CI step takes from its runner: memory, swap, disk and wall time, sampled while the step's command runs.

  resources.py facts                      cores=N and mem=BYTES (physical memory), for $GITHUB_ENV
  resources.py run LABEL -- COMMAND...    run COMMAND, its output echoed, and exit with its code

`run` samples every two seconds and, when the command ends, prints the peaks, appends them and each `zig build
--summary all` step's MaxRSS (the per-compile peaks) to the job summary, and writes them as JSON under ci-metrics/.
Standard library only; Linux, macOS and Windows.
"""
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import threading
import time


def memory():
    """(physical total, in use, swap in use), in bytes."""
    system = platform.system()
    if system == "Linux":
        m = {}
        with open("/proc/meminfo") as f:
            for line in f:
                key, value = line.split(":", 1)
                m[key] = int(value.split()[0]) * 1024
        return m["MemTotal"], m["MemTotal"] - m["MemAvailable"], m["SwapTotal"] - m["SwapFree"]
    if system == "Darwin":
        total = int(subprocess.check_output(["sysctl", "-n", "hw.memsize"]))
        vm = subprocess.check_output(["vm_stat"], text=True)
        page = int(re.search(r"page size of (\d+)", vm).group(1))

        def pages(name):
            found = re.search(re.escape(name) + r":\s+(\d+)", vm)
            return int(found.group(1)) if found else 0

        used = (pages("Pages active") + pages("Pages wired down") + pages("Pages occupied by compressor")) * page
        swap = re.search(r"used = ([\d.]+)M", subprocess.check_output(["sysctl", "-n", "vm.swapusage"], text=True))
        return total, used, int(float(swap.group(1)) * 2**20) if swap else 0
    if system == "Windows":
        import ctypes

        class Status(ctypes.Structure):
            _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong)] + [
                (name, ctypes.c_ulonglong) for name in ("total_phys", "avail_phys", "total_page", "avail_page",
                                                        "total_virtual", "avail_virtual", "avail_ext")]

        s = Status()
        s.dwLength = ctypes.sizeof(Status)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(s))
        used = s.total_phys - s.avail_phys
        # Windows has no swap figure; the commit charge beyond physical use is what the page file can be holding.
        return s.total_phys, used, max(0, (s.total_page - s.avail_page) - used)
    raise SystemExit(f"resources.py: no memory reader for {system}")


def gib(n):
    return f"{n / 2**30:.2f} GiB"


def facts():
    print(f"cores={os.cpu_count()}")
    print(f"mem={memory()[0]}")


def run(label, command):
    total = memory()[0]
    peak = {"mem_used": 0, "swap_used": 0, "disk_used": 0}
    disk_start = shutil.disk_usage(".")
    peak["disk_free_min"] = disk_start.free
    done = threading.Event()

    def sample():
        while True:
            try:
                _, used, swap = memory()
                disk = shutil.disk_usage(".")
                peak["mem_used"] = max(peak["mem_used"], used)
                peak["swap_used"] = max(peak["swap_used"], swap)
                peak["disk_used"] = max(peak["disk_used"], disk.used)
                peak["disk_free_min"] = min(peak["disk_free_min"], disk.free)
            except Exception as e:  # a failed sample must not fail the step it measures
                print(f"resources.py: sample failed: {e}", file=sys.stderr)
            if done.wait(2):
                return

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    start = time.monotonic()
    exe = shutil.which(command[0]) or command[0]
    proc = subprocess.Popen([exe] + command[1:], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    steps = []
    # Bytes through, as the command wrote them: a Windows console encoding can't encode every test's output.
    for raw in proc.stdout:
        sys.stdout.buffer.write(raw)
        sys.stdout.buffer.flush()
        line = raw.decode("utf-8", "replace")
        if "MaxRSS:" in line:
            steps.append(line.strip(" \t\r\n│├└─"))
    code = proc.wait()
    wall = time.monotonic() - start
    done.set()
    sampler.join()

    def rss_bytes(s):
        m = re.search(r"MaxRSS:(\d+)([GMKB])", s)
        return int(m.group(1)) * {"G": 10**9, "M": 10**6, "K": 10**3, "B": 1}[m.group(2)] if m else 0

    steps.sort(key=rss_bytes, reverse=True)
    record = {
        "label": label, "command": command, "exit": code, "wall_s": round(wall, 1), "runner": {
            "os": os.environ.get("RUNNER_OS", platform.system()), "arch": os.environ.get("RUNNER_ARCH", platform.machine()),
            "cores": os.cpu_count(), "mem_total": total, "disk_total": disk_start.total, "disk_free_start": disk_start.free},
        "peak": peak, "steps_by_max_rss": steps,
    }
    os.makedirs("ci-metrics", exist_ok=True)
    with open(os.path.join("ci-metrics", re.sub(r"[^A-Za-z0-9_.-]", "_", label) + ".json"), "w") as f:
        json.dump(record, f, indent=2)
    lines = [
        f"### {label}: exit {code}, {wall / 60:.1f} min",
        "",
        f"| memory peak | swap peak | disk peak used | disk free, least | cores | RAM |",
        f"|---|---|---|---|---|---|",
        f"| {gib(peak['mem_used'])} | {gib(peak['swap_used'])} | {gib(peak['disk_used'])} | "
        f"{gib(peak['disk_free_min'])} | {os.cpu_count()} | {gib(total)} |",
        "",
    ]
    if steps:
        lines += ["<details><summary>Largest steps by MaxRSS</summary>", "", "```"] + steps[:15] + ["```", "</details>", ""]
    text = "\n".join(lines) + "\n"
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as f:
            f.write(text)
    return code


def main(argv):
    if argv[:1] == ["facts"]:
        return facts()
    if len(argv) >= 4 and argv[0] == "run" and argv[2] == "--":
        sys.exit(run(argv[1], argv[3:]))
    raise SystemExit(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
