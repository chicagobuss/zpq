#!/usr/bin/env python3
"""Check shipped, stripped x86_64 Linux binaries: --default PATH --compat PATH.

Skips when qemu-x86_64 is absent; release CI installs qemu-user before invoking it.
"""
import argparse
import json
import os
from pathlib import Path
import resource
import shutil
import struct
import subprocess


def require_stripped(binary):
    data = binary.read_bytes()
    if data[:6] != b"\x7fELF\x02\x01":
        raise SystemExit(f"{binary}: expected a little-endian ELF64 release artifact")
    offset = struct.unpack_from("<Q", data, 40)[0]
    size, count, strings = struct.unpack_from("<HHH", data, 58)
    start, length = struct.unpack_from("<QQ", data, offset + strings * size + 24)
    names = data[start:start + length]
    for i in range(count):
        name_offset = struct.unpack_from("<I", data, offset + i * size)[0]
        name = names[name_offset:].split(b"\0", 1)[0]
        if name == b".symtab" or name.startswith(b".debug_"):
            raise SystemExit(f"{binary}: use the stripped release artifact ({name.decode()} remains)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--default", required=True, type=Path)
    parser.add_argument("--compat", required=True, type=Path)
    args = parser.parse_args()
    qemu = shutil.which("qemu-x86_64")
    if qemu is None:
        print("SKIP cpu-start: qemu-x86_64 is absent (install qemu-user)")
        return
    for binary in (args.default, args.compat):
        require_stripped(binary)
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    root = Path(__file__).resolve().parents[2]
    fixture = root / "ci/fixtures/parquet/full_match.parquet"
    expected = json.loads((root / "tools/check/goldens/release/aggregate.json").read_text())
    commands = [
        ("version", ["--version"]),
        ("inspect", ["inspect", str(fixture)]),
        ("query", ["query", str(fixture), "--aggregate", "count(*) AS n, sum(b) AS s, max(s) AS m"]),
    ]
    env = dict(os.environ)
    env.pop("ZPQ_TEST_CPU_MISSING", None)
    passed = 0
    for cpu, feature in (("Westmere", "AVX"), ("SandyBridge", "AVX2")):
        for variant, binary in (("default", args.default), ("compat", args.compat)):
            for name, argv in commands:
                result = subprocess.run([qemu, "-cpu", cpu, str(binary.resolve()), *argv],
                                        capture_output=True, text=True, timeout=60, env=env)
                if variant == "default":
                    diagnostic = (f"zpq: this CPU lacks {feature}, which this x86-64-v3 build needs; "
                                  "use the x86_64-linux-musl-compat archive, which runs on any x86-64\n")
                    ok = result.returncode == 3 and result.stdout == "" and diagnostic in result.stderr
                else:
                    ok = result.returncode == 0
                    if ok and name == "version":
                        ok = result.stdout.startswith("zpq ")
                    if ok and name == "query":
                        ok = json.loads(result.stdout) == expected
                if not ok:
                    raise SystemExit(f"FAIL {cpu} {variant} {name}: exit {result.returncode}\n"
                                     f"stdout: {result.stdout}\nstderr: {result.stderr}")
                passed += 1
                print(f"PASS {cpu} {variant} {name}", flush=True)
    print(f"cpu-start: {passed} passed, 0 failed")


if __name__ == "__main__":
    main()
