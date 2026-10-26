"""Accept only explicit runtime evidence for this release revision and target."""

import argparse
import json
import re
from pathlib import Path


def proves(doc, sha, target):
    if not isinstance(doc, dict):
        return False
    smoke = doc.get("release_smoke", {})
    if not isinstance(smoke, dict):
        return False
    binaries = smoke.get("binaries", {})
    if not isinstance(binaries, dict):
        return False
    required = ("zpq", "zpq-lambda", "zpq-integration") if "linux" in target else ("zpq",)
    return (doc.get("sha") == sha and doc.get("target") == target
            and type(doc.get("exit")) is int and doc["exit"] == 0
            and smoke.get("status") == "pass" and smoke.get("sha") == sha
            and smoke.get("cli") is True and ("linux" not in target or smoke.get("lambda") is True)
            and all(isinstance(binaries.get(name), str) and re.fullmatch(r"[0-9a-f]{64}", binaries[name])
                    for name in required))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sha", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--summary", action="append", default=[])
    args = parser.parse_args()
    for path in args.summary:
        if proves(json.loads(Path(path).read_text()), args.sha, args.target):
            return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
