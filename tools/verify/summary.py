"""One summary.json per run directory. A second open of the same file is refused."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path


def now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Summary:
    def __init__(self, directory: Path, meta: dict):
        self.directory = Path(directory)
        self.path = self.directory / "summary.json"
        if self.path.exists():
            raise FileExistsError(
                f"{self.path} already exists. A rerun is a new directory; the first record stays."
            )
        self.directory.mkdir(parents=True, exist_ok=True)
        self.data = {
            "schema": 1,
            "run_id": self.directory.name,
            "label": meta.get("label"),
            "rerun_of": meta.get("rerun_of"),
            "tier": meta["tier"],
            "sha": meta["sha"],
            "binary_sha256": meta.get("binary_sha256"),
            "host": meta.get("host") or {},
            "started": now(),
            "finished": None,
            "exit": None,
            "stages": [],
            "failures": [],
            "expected": [],
            "blocked": [],
        }
        self.flush()

    def set_host(self, host: dict) -> None:
        self.data["host"] = host
        self.flush()

    def set_binary(self, sha: str | None) -> None:
        self.data["binary_sha256"] = sha
        self.flush()

    def add_stage(self, record: dict) -> None:
        self.data["stages"].append(record)
        status = record["status"]
        name = record["name"]
        if status == "fail":
            self.data["failures"].append(name)
        elif status == "expected":
            self.data["expected"].append(name)
        elif status == "blocked":
            self.data["blocked"].append(name)
        self.flush()

    def finish(self, exit_code: int) -> None:
        self.data["finished"] = now()
        self.data["exit"] = exit_code
        self.flush()

    def flush(self) -> None:
        temporary = self.path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(self.data, indent=2) + "\n")
        os.replace(temporary, self.path)


def exit_code(summary: Summary) -> int:
    if summary.data["failures"]:
        return 1
    if summary.data["blocked"]:
        return 2
    return 0
