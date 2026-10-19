"""Shared by the repros: locate the CLI, libzpq and the Python binding from the repo root."""
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
ZPQ = os.environ.get("ZPQ_BIN", os.path.join(ROOT, "zig-out", "bin", "zpq"))
sys.path.insert(0, os.path.join(ROOT, "python"))


def cli(*args):
    p = subprocess.run([ZPQ, *args], capture_output=True, text=True)
    return p.returncode, p.stdout.strip(), p.stderr.strip()
