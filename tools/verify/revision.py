"""Build immutable git revisions without a caller-maintained worktree."""

import os
import subprocess
import tarfile
import tempfile
from pathlib import Path

import execute

REPO = Path(__file__).resolve().parents[2]
CACHE = Path(os.environ.get("ZPQ_VERIFY_CACHE", REPO / "zig-out/verify-cache")).expanduser().resolve()


def resolve(rev):
    return subprocess.check_output(
        ["git", "rev-parse", "--verify", "--end-of-options", f"{rev}^{{commit}}"], cwd=REPO, text=True).strip()


def source(sha):
    dest = CACHE / sha / "source"
    if not dest.is_dir():
        dest.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=dest.parent) as tmp:
            archive = Path(tmp) / "source.tar"
            with archive.open("wb") as out:
                subprocess.run(["git", "archive", sha], cwd=REPO, stdout=out, check=True)
            tree = Path(tmp) / "source"
            tree.mkdir()
            with tarfile.open(archive) as tar:
                tar.extractall(tree, filter="data")
            archive.replace(dest.parent / "source.tar")
            tree.rename(dest)
    return dest


def build(sha, target=None, cpu="native", jobs=8, heavy=True):
    tree = source(sha)
    zig = os.environ.get("ZPQ_ZIG", "zig")
    version = subprocess.check_output([zig, "version"], cwd=tree, text=True).strip()
    if version != (tree / ".zig-version").read_text().strip():
        raise RuntimeError(f"{zig}: {version} does not match {tree / '.zig-version'}")
    prefix = tree.parent / (target or "native") / cpu
    log = prefix / "build.log"
    env = dict(os.environ, ZIG_GLOBAL_CACHE_DIR=str(CACHE / "zig"), ZIG_LOCAL_CACHE_DIR=str(tree / ".zig-cache"))
    argv = [zig, "build", "cli", f"-j{jobs}", "-Doptimize=ReleaseFast", "-Dstrip=true",
            f"-Dcpu={cpu}", f"-Drevision={sha}", "--prefix", str(prefix)]
    if target:
        argv.append(f"-Dtarget={target}")
    print(f"== build {sha[:12]} {target or 'native'} cpu={cpu}; log {log}", flush=True)
    rc, _ = execute.launch(argv, env, tree, log, heavy)
    if rc:
        raise RuntimeError(f"revision build failed: {log}")
    return prefix / "bin/zpq"
