"""Build verified git archives with compiler- and flag-specific caches."""

import hashlib
import json
import os
import re
import stat
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


def verify_tree(tree, archive):
    with tarfile.open(archive) as tar:
        members = {m.name: m for m in tar if not m.isdir()}
        actual = set()
        for top in tree.iterdir():
            if top.name in (".zig-cache", "zig-out", "zig-pkg"):
                continue
            paths = (top, *top.rglob("*")) if top.is_dir() and not top.is_symlink() else (top,)
            actual.update(str(p.relative_to(tree)) for p in paths if p.is_file() or p.is_symlink())
        if actual != members.keys():
            raise RuntimeError(f"source cache differs from git archive: {tree}")
        for name, member in members.items():
            path = tree / name
            if member.issym():
                valid = path.is_symlink() and os.readlink(path) == member.linkname
            else:
                valid = not path.is_symlink() and stat.S_ISREG(path.stat().st_mode)
                valid = valid and bool(path.stat().st_mode & 0o111) == bool(member.mode & 0o111)
                with tar.extractfile(member) as handle:
                    valid = valid and execute.file_sha256(path) == hashlib.file_digest(handle, "sha256").hexdigest()
            if not valid:
                raise RuntimeError(f"source cache differs from git archive: {path}")


def source(sha):
    dest = CACHE / sha / "source"
    dest.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=dest.parent) as tmp:
        archive = Path(tmp) / "source.tar"
        with archive.open("wb") as out:
            subprocess.run(["git", "archive", sha], cwd=REPO, stdout=out, check=True)
        if dest.exists():
            verify_tree(dest, archive)
            if execute.file_sha256(dest.parent / "source.tar") != execute.file_sha256(archive):
                raise RuntimeError(f"cached archive differs from git archive: {dest.parent}")
        else:
            tree = Path(tmp) / "source"
            with tarfile.open(archive) as tar:
                tar.extractall(tree, filter="data")
            archive.replace(dest.parent / "source.tar")
            tree.rename(dest)
    return dest


def compiler(zig, tree):
    version = subprocess.check_output([zig, "version"], cwd=tree, text=True).strip()
    if version != (tree / ".zig-version").read_text().strip():
        raise RuntimeError(f"{zig}: {version} does not match {tree / '.zig-version'}")
    env = subprocess.check_output([zig, "env"], cwd=tree, text=True)
    match = re.search(r'\.zig_exe\s*=\s*("(?:[^"\\]|\\.)*")', env)
    if not match:
        raise RuntimeError("zig env did not identify its compiler binary")
    digest = execute.file_sha256(Path(json.loads(match.group(1))))
    if not digest:
        raise RuntimeError("zig env identified a missing compiler binary")
    return digest


def build_key(zig_sha, target, cpu, steps):
    flags = [zig_sha, target, cpu, "ReleaseFast", "strip=true", *steps]
    return hashlib.sha256(json.dumps(flags).encode()).hexdigest()


def environment(zig_sha, local_cache):
    return dict(os.environ, ZIG_GLOBAL_CACHE_DIR=str(CACHE / "zig" / zig_sha),
                ZIG_LOCAL_CACHE_DIR=str(local_cache))


def build(sha, target=None, cpu="native", jobs=8, heavy=True, steps=("cli",)):
    tree = source(sha)
    zig = os.environ.get("ZPQ_ZIG", "zig")
    zig_sha = compiler(zig, tree)
    prefix = tree.parent / "builds" / build_key(zig_sha, target, cpu, steps)
    log = prefix / "build.log"
    env = environment(zig_sha, prefix / "cache")
    argv = [zig, "build", *steps, f"-j{jobs}", "-Doptimize=ReleaseFast", "-Dstrip=true",
            f"-Dcpu={cpu}", f"-Drevision={sha}", "--prefix", str(prefix)]
    if target:
        argv.append(f"-Dtarget={target}")
    print(f"== build {sha[:12]} {target or 'native'} cpu={cpu}; log {log}", flush=True)
    rc, _ = execute.launch(argv, env, tree, log, heavy)
    if rc:
        raise RuntimeError(f"revision build failed: {log}")
    if target is None:
        version_text = subprocess.check_output([str(prefix / "bin/zpq"), "--version"], cwd=tree, text=True)
        if sha not in version_text:
            raise RuntimeError("comparison binary reports the wrong revision")
    (prefix / "build.json").write_text(json.dumps({"sha": sha, "zig_sha256": zig_sha, "command": argv,
        "tree": subprocess.check_output(["git", "rev-parse", f"{sha}^{{tree}}"], cwd=REPO, text=True).strip()}))
    return prefix / "bin/zpq"


def integration(tree, dest, target, heavy, runtime=None):
    options = dest / "integration_opts.zig"
    runtime = runtime or f"release/{target}/bin"
    options.write_text(f'pub const lambda_bin = {json.dumps(f"{runtime}/zpq-lambda")};\n'
                       f'pub const cli_bin = {json.dumps(f"{runtime}/zpq")};\n')
    zig = os.environ.get("ZPQ_ZIG", "zig")
    zig_sha = compiler(zig, tree)
    argv = [zig, "test", "-j8", "--test-no-exec", "-O", "ReleaseFast",
            "-fstrip", "-target", target, "-mcpu", "baseline", f"-femit-bin={dest / 'zpq-integration'}",
            "--dep", "integration_opts", f"-Mroot={tree / 'tests/lambda_integration.zig'}",
            f"-Mintegration_opts={options}"]
    rc, _ = execute.launch(argv, environment(zig_sha, dest / f"integration-cache-{zig_sha}"),
                           tree, dest / "integration-build.log", heavy)
    if rc:
        raise RuntimeError(f"integration build failed: {dest / 'integration-build.log'}")
