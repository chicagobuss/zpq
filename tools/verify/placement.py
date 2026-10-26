#!/usr/bin/env python3
"""Prepare revision artifacts once, then reuse the existing per-host transport."""

import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path

import execute
import revision

HERE = Path(__file__).resolve().parent


def host_plan(config, hosts):
    hosts = sorted(set(hosts))
    reference = config.get("reference_host", "[redacted]") if len(hosts) > 1 else None
    if reference:
        policy = config["hosts"].get(reference, {})
        if policy.get("target") != "x86_64-linux-musl" or not policy.get("enabled", True):
            raise ValueError("reference_host must be an enabled x86 Linux build host")
        policy["hash_only"] = True
        hosts = [reference, *[h for h in hosts if h != reference]]
    return hosts, reference


def main():
    parser = argparse.ArgumentParser(prog="verify arch", description=__doc__)
    parser.add_argument("--rev", help="git revision to build; defaults to inventory revision")
    parser.add_argument("--host", action="append", help="SSH alias; repeat or omit for all enabled hosts")
    parser.add_argument("--config", default=os.environ.get(
        "ZPQ_ARCH_CONFIG", "/mnt/fast/zpq-out/arch-matrix/fleet.json"))
    parser.add_argument("--setup-only", action="store_true")
    parser.add_argument("--no-scope", action="store_true", help="caller supplies resource limits")
    args = parser.parse_args()
    try:
        config = json.loads(Path(args.config).expanduser().read_text())
    except (OSError, ValueError) as error:
        parser.error(str(error))
    hosts = args.host or [h for h, v in config["hosts"].items() if v.get("enabled", True)]
    for host in hosts:
        if host not in config["hosts"] or not config["hosts"][host].get("enabled", True):
            parser.error(f"host absent or disabled in inventory: {host}")
    try:
        hosts, reference = host_plan(config, hosts)
        if not hosts or not json.loads((Path(config["data"]) / "manifest.json").read_text()).get("cases"):
            raise ValueError("enabled hosts and a nonempty case list are required")
    except ValueError as error:
        parser.error(str(error))
    try:
        sha = revision.resolve(args.rev or config["revision"])
    except subprocess.CalledProcessError:
        parser.error("invalid engine revision")
    root = revision.CACHE / sha
    binaries = root / "binaries"
    targets = sorted({config["hosts"][h]["target"] for h in hosts})
    for target in targets:
        linux = target.endswith("linux-musl")
        binary = revision.build(sha, target, cpu="baseline", heavy=not args.no_scope,
                                steps=("cli", "lambda") if linux else ("cli",))
        dest = binaries / target
        dest.mkdir(parents=True, exist_ok=True)
        shutil.copy2(binary, dest / "zpq")
        if linux:
            shutil.copy2(binary.parent / "zpq-lambda", dest / "zpq-lambda")
            revision.integration(revision.source(sha), dest, target, not args.no_scope)
        names = ("zpq", "zpq-lambda", "zpq-integration") if linux else ("zpq",)
        (dest / "SHA256SUMS").write_text("".join(f"{execute.file_sha256(dest / n)}  {n}\n" for n in names))
        argv = [os.environ.get("ZPQ_ZIG", "zig"), "build-exe", "-j8", str(HERE / "arch.zig"), "-lc",
                "-O", "ReleaseSafe", "-mcpu", "baseline", "-target", target,
                f"-femit-bin={dest / 'arch-runner'}"]
        tree = revision.source(sha)
        zig_sha = revision.compiler(os.environ.get("ZPQ_ZIG", "zig"), tree)
        rc, _ = execute.launch(argv, revision.environment(zig_sha, dest / f"runner-cache-{zig_sha}"),
                               tree, dest / "runner-build.log", not args.no_scope)
        if rc:
            raise RuntimeError(f"runner build failed: {dest / 'runner-build.log'}")
    config.update(revision=sha, binaries=str(binaries))
    config.pop("reference", None)
    config["source"] = str(root / "source.tar")
    generated = root / f"fleet-{os.urandom(8).hex()}.json"
    result = 0
    for host in hosts:
        config["comparison"] = reference if host != reference else "reference host"
        generated.write_text(json.dumps(config, indent=2) + "\n")
        argv = ["bash", str(HERE / "arch_host.sh"), host, str(generated), str(args.setup_only).lower()]
        rc = subprocess.run(argv).returncode
        if rc:
            result = rc if rc != 2 or result == 0 else result
        if host == reference and not args.setup_only:
            if rc:
                break
            config["reference"] = str((Path(config["out"]) / host / "latest.json").resolve())
        if rc == 3:
            break
    if not reference:
        print("no cross-host comparison")
        if not args.setup_only and result == 0:
            result = 2
    print(f"inventory {generated}")
    return result


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        raise SystemExit(f"verify arch: {error}")
