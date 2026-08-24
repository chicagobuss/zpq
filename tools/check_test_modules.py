#!/usr/bin/env python3
"""Require every test-bearing src module to be rooted by a test artifact."""

from __future__ import annotations

import re
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
SRC = REPO / "src"
TEST_ROOTS = (SRC / "zpq.zig", SRC / "lambda" / "main.zig")
TEST_DECL = re.compile(r"^test(?:\s|\{)", re.MULTILINE)
ROOTED_IMPORT = re.compile(r'^\s*_\s*=\s*@import\("([^\"]+\.zig)"\);\s*$', re.MULTILINE)


def relative(path: Path) -> str:
    return path.relative_to(REPO).as_posix()


def main() -> int:
    discovered: dict[Path, int] = {}
    for path in SRC.rglob("*.zig"):
        count = len(TEST_DECL.findall(path.read_text(encoding="utf-8")))
        if count:
            discovered[path.resolve()] = count

    rooted = {path.resolve() for path in TEST_ROOTS}
    for root in TEST_ROOTS:
        source = root.read_text(encoding="utf-8")
        for imported in ROOTED_IMPORT.findall(source):
            rooted.add((root.parent / imported).resolve())

    missing = sorted(set(discovered) - rooted)
    if missing:
        print("test-module coverage: unrooted modules:", file=sys.stderr)
        for path in missing:
            print(f"  {relative(path)} ({discovered[path]} tests)", file=sys.stderr)
        print(
            "add each module as a direct `_ = @import(\"...\");` in "
            "src/zpq.zig or src/lambda/main.zig",
            file=sys.stderr,
        )
        return 1

    print(f"test-module coverage: {len(discovered)} modules, {sum(discovered.values())} tests, all rooted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
