"""Host identity for a verify summary. Bandwidth and DIMM probes are best-effort."""

from __future__ import annotations

import platform
import hashlib
from importlib.metadata import PackageNotFoundError, version
import socket
import subprocess
from pathlib import Path


def cpu_model() -> str:
    try:
        text = Path("/proc/cpuinfo").read_text()
    except OSError as error:
        return f"unread: {error}"
    for line in text.splitlines():
        if line.lower().startswith("model name"):
            return line.split(":", 1)[1].strip()
    return "unknown"


def _run(argv: list[str], timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(argv, capture_output=True, text=True, timeout=timeout)


def bandwidth(repo: Path, out: Path) -> dict:
    source = repo / "tools/verify/bw.c"
    binary = out / "bw"
    compiled = _run(["gcc", "-O2", "-fopenmp", "-o", str(binary), str(source)])
    if compiled.returncode != 0:
        return {"error": (compiled.stderr or compiled.stdout or "gcc failed").strip()[-500:]}
    ran = _run(["systemd-run", "--user", "--scope", "-p", "MemoryMax=20G", "-q", "--",
                "nice", "-n", "10", str(binary)], timeout=180)
    if ran.returncode != 0:
        return {"error": (ran.stderr or ran.stdout or "bw failed").strip()[-500:]}
    return {"lines": [line for line in ran.stdout.splitlines() if line.strip()]}


def memory_modules() -> dict:
    attempts = []
    for argv in (["dmidecode", "-t", "memory"], ["sudo", "-n", "dmidecode", "-t", "memory"]):
        try:
            ran = _run(argv, timeout=30)
        except (OSError, subprocess.TimeoutExpired) as error:
            attempts.append({"argv": argv, "error": str(error)})
            continue
        text = (ran.stdout or "") + (ran.stderr or "")
        if ran.returncode != 0 or "Permission denied" in text or "Operation not permitted" in text:
            attempts.append({"argv": argv, "error": text.strip()[-300:]})
            continue
        modules = []
        current: dict[str, str] = {}
        for line in ran.stdout.splitlines():
            if not line.strip():
                if current.get("Size") and "No Module" not in current["Size"]:
                    modules.append(current)
                current = {}
                continue
            if ":" in line:
                key, value = line.split(":", 1)
                key, value = key.strip(), value.strip()
                if key in ("Locator", "Bank Locator", "Size", "Type", "Speed", "Manufacturer"):
                    current[key] = value
        if current.get("Size") and "No Module" not in current["Size"]:
            modules.append(current)
        return {"modules": modules, "populated": len(modules)}
    return {"error": "dmidecode unavailable", "attempts": attempts}


def python_environment(repo: Path) -> dict:
    pins = repo / "tools/verify/requirements.txt"
    required = dict(line.split("==") for line in pins.read_text().splitlines() if line and not line.startswith("#"))
    installed = {}
    for name in required:
        try:
            installed[name] = version(name)
        except PackageNotFoundError:
            installed[name] = None
    return {
        "requirements_sha256": hashlib.sha256(pins.read_bytes()).hexdigest(),
        "required": required,
        "installed": installed,
        "mismatches": [f"{name}: need {want}, installed {installed[name] or 'missing'}"
                       for name, want in required.items() if installed[name] != want],
    }


def collect(repo: Path, out: Path, zig_cpu: str, probe: bool) -> dict:
    dirty = subprocess.check_output(["git", "status", "--porcelain"], cwd=repo, text=True)
    diff = subprocess.check_output(["git", "diff", "HEAD", "--binary"], cwd=repo)
    host = {
        "hostname": socket.gethostname(),
        "uname": " ".join(platform.uname()),
        "cpu_model": cpu_model(),
        "logical_cpus": os_cpu_count(),
        "zig_cpu": zig_cpu,
        "python": platform.python_version(),
        "python_environment": python_environment(repo),
        "engine_tree": subprocess.check_output(["git", "rev-parse", "HEAD:src"], cwd=repo, text=True).strip(),
        "working_tree_status": dirty.splitlines(),
        "diff_sha256": hashlib.sha256(diff).hexdigest() if diff else None,
    }
    if probe:
        try:
            host["bw"] = bandwidth(repo, out)
        except (OSError, subprocess.SubprocessError) as error:
            host["bw"] = {"error": str(error)}
        host["dimm"] = memory_modules()
    return host


def os_cpu_count() -> int | None:
    try:
        return len(
            [
                line
                for line in Path("/proc/cpuinfo").read_text().splitlines()
                if line.lower().startswith("processor")
            ]
        )
    except OSError:
        return None
