#!/usr/bin/env python3
"""Exercise CLI fixture and case spawning from a different working directory."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


RUNNER = Path(sys.argv.pop(1)).resolve()
ROOT = Path(__file__).resolve().parents[2]


class RunnerPaths(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=ROOT / "zig-out")
        self.addCleanup(self.tmp.cleanup)
        self.cwd = Path(self.tmp.name)
        (self.cwd / "bin").mkdir()
        fake = self.cwd / "bin/zpq"
        fake.write_text("#!/bin/sh\ncase $0 in /*) ;; *) exit 92 ;; esac\n"
                        "if [ \"$1\" = fixture ]; then printf fixture > \"$2\"; else echo answer; fi\n")
        fake.chmod(0o755)
        (self.cwd / "cases.json").write_text(json.dumps({
            "fixtures": [{"name": "main", "zpq": ["fixture", "{path}"]}],
            "cases": [{"id": "relative", "cli": ["query"], "same": "spawn path regression"}],
        }))

    def run_checker(self, binary="bin/zpq", env=None):
        return subprocess.run([str(RUNNER), "--cases", "cases.json", "--zpq", binary,
                               "--work", "work", "--no-cache", "--duckdb", "missing-duckdb"],
                              cwd=self.cwd, env=env, text=True, capture_output=True, timeout=60)

    def test_relative_binary_from_other_cwd(self):
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("1/1 checks match", result.stderr)

    def test_missing_binary_names_path(self):
        result = self.run_checker("bin/missing")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(self.cwd / "bin/missing"), result.stderr)
        self.assertIn("FileNotFound", result.stderr)

    def test_relative_path_preserves_symlink_parent(self):
        (self.cwd / "real/nested").mkdir(parents=True)
        binary = self.cwd / "real/zpq"
        binary.write_bytes((self.cwd / "bin/zpq").read_bytes())
        binary.chmod(0o755)
        (self.cwd / "link").symlink_to("real/nested")
        result = self.run_checker("link/../zpq")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("1/1 checks match", result.stderr)

    def test_missing_timeout_names_executable(self):
        env = dict(os.environ, PATH=str(self.cwd / "bin"))
        result = self.run_checker(env=env)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("cannot start timeout: FileNotFound", result.stderr)


if __name__ == "__main__":
    unittest.main()
