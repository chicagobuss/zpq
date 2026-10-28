#!/usr/bin/env python3
"""Reject unverified local TLS archive overrides; missing files use the build's pinned fetcher."""
import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for line in (ROOT / "vendor/boring_tls/prebuilt.sha256").read_text().splitlines():
    if not line or line.startswith("#"):
        continue
    name, expected, url = line.split()
    path = ROOT / "vendor/boring_tls/prebuilt" / name
    if path.exists():
        actual = hashlib.file_digest(path.open("rb"), "sha256").hexdigest()
        if actual != expected:
            raise SystemExit(f"unverified TLS override: {path}: {actual} != {expected}")
        print(f"verified {name} {actual}")
    else:
        print(f"pinned-fetch {name} {expected} {url}")
