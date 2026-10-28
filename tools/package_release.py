#!/usr/bin/env python3
"""Build and stage immutable, deterministic CLI tarballs; never upload or publish."""
import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import struct
import subprocess
import tarfile
import tempfile
import time
import zlib

ROOT = Path(__file__).resolve().parents[1]
TARGETS = ("x86_64-linux-musl", "aarch64-linux-musl", "aarch64-macos", "x86_64-macos")
# Everything but the binary, by archive path. The guides are the ones help and the man page name.
SHARED_FILES = {"LICENSE": ROOT / "LICENSE",
                "THIRD_PARTY_NOTICES.md": ROOT / "THIRD_PARTY_NOTICES.md",
                "share/man/man1/zpq.1": ROOT / "packaging/man/zpq.1",
                "share/bash-completion/completions/zpq": ROOT / "packaging/completions/zpq.bash",
                "share/zsh/site-functions/_zpq": ROOT / "packaging/completions/_zpq",
                "share/fish/vendor_completions.d/zpq.fish": ROOT / "packaging/completions/zpq.fish",
                "share/zpq/tiny.parquet": ROOT / "packaging/fixtures/tiny.parquet",
                **{"share/doc/zpq/" + n: ROOT / "docs" / n
                   for n in ("getting-started.md", "output-format.md", "binary-output.md",
                             "writer-statistics.md", "decode-errors.md", "hardware-execution.md",
                             "execution_knobs.md", "byte_topk.md")}}
# Each target's archives as (name suffix, -Dcpu). Linux x86_64 ships x86-64-v3 (AVX2), which the binary checks at
# startup, and a baseline "-compat" archive for older CPUs; .deb and .rpm come from the unsuffixed archive.
VARIANTS = {"x86_64-linux-musl": (("", "x86_64_v3"), ("-compat", "baseline"))}
PACKAGE_ARCHES = {"x86_64-linux-musl": ("amd64", "x86_64"), "aarch64-linux-musl": ("arm64", "aarch64")}


def sha(path):
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def dangling_references(files):
    """References in the shipped man page and guides to docs the archive lacks: `docs/x.md` (help's spelling, which
    the archive keeps under share/doc/zpq/), relative Markdown links, and the README, which is not shipped."""
    shipped = {Path(n).name for n in files if n.startswith("share/doc/zpq/")}
    missing = []
    for name, path in sorted(files.items()):
        if not name.endswith((".md", ".1")):
            continue
        text = path.read_text().replace("\\-", "-")
        refs = re.findall(r"docs/([\w.-]+)", text) + re.findall(r"\]\((?![a-z]+:)([^)#\s]+)", text)
        missing += [f"{name}: {ref}" for ref in refs if ref.rstrip(".") not in shipped]
        missing += [f"{name}: README"] if "README" in text else []
    return missing


def man_gzip(page):
    """gzip -9n of the man page. Deflate output differs between zlib builds (zlib-ng does), so pin the bytes."""
    deflate = zlib.compressobj(9, zlib.DEFLATED, -zlib.MAX_WBITS, zlib.DEF_MEM_LEVEL, 0)
    body = b"\x1f\x8b\x08\x00" + bytes(4) + b"\x02\xff" + deflate.compress(page) + deflate.flush()
    body += struct.pack("<II", zlib.crc32(page), len(page) & 0xFFFFFFFF)
    lines = [f"{hashlib.sha256(page).hexdigest()}  zpq.1", f"{hashlib.sha256(body).hexdigest()}  zpq.1.gz"]
    pinned = (ROOT / "packaging/man/zpq.1.gz.sha256").read_text().splitlines()
    if lines[0] != pinned[0]:
        raise SystemExit("packaging/man/zpq.1 changed; with a stock-zlib python3, pin its gzip in "
                         "packaging/man/zpq.1.gz.sha256:\n" + "\n".join(lines))
    if lines[1] != pinned[1]:
        raise SystemExit(f"zlib {zlib.ZLIB_RUNTIME_VERSION} gzips the man page differently from "
                         "packaging/man/zpq.1.gz.sha256; run with a python3 linked against stock zlib")
    return body


def write_packages(out, target, content, version, epoch):
    """Write the .deb and .rpm for one Linux target from the tarball's own bytes, with packaging/nfpm.yaml."""
    deb_arch, rpm_arch = PACKAGE_ARCHES[target]
    package_version = version.replace("-", "~", 1)
    names = {"deb": f"zpq_{package_version}_{deb_arch}.deb", "rpm": f"zpq-{package_version}-1.{rpm_arch}.rpm"}
    with tempfile.TemporaryDirectory() as stage:
        stage = Path(stage)
        for filename, body in content.items():
            (stage / filename).parent.mkdir(parents=True, exist_ok=True)
            (stage / filename).write_bytes(body)
        (stage / "share/man/man1/zpq.1.gz").write_bytes(man_gzip(content["share/man/man1/zpq.1"]))
        env = {**os.environ, "ZPQ_VERSION": version, "ZPQ_ARCH": deb_arch, "SOURCE_DATE_EPOCH": str(epoch)}
        for packager, name in names.items():
            subprocess.run(["nfpm", "pkg", "--config", str(ROOT / "packaging/nfpm.yaml"), "--packager", packager,
                            "--target", str(out / name)], cwd=stage, env=env, check=True, timeout=120)
            (out / (name + ".sha256")).write_text(sha(out / name) + "  " + name + "\n")
            print(target, name, sha(out / name), flush=True)


def positive_seconds(value):
    try:
        seconds = int(value)
        if seconds <= 0:
            raise ValueError
        return seconds
    except ValueError:
        raise argparse.ArgumentTypeError("timeout must be a positive number of seconds")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="zig-out/release")
    parser.add_argument("--targets", default=",".join(TARGETS[:3]))
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--build-timeout", type=positive_seconds, default=os.environ.get("ZPQ_RELEASE_BUILD_TIMEOUT", "3600"),
                        help="seconds per build (default: 3600; or ZPQ_RELEASE_BUILD_TIMEOUT)")
    parser.add_argument("--check-docs", action="store_true", help="only check the archive's doc references")
    parser.add_argument("--tag", help="release tag, v<version> or v<version>-<prerelease>; a prerelease suffix is "
                        "built into the binary's and the manifest's version (e.g. v0.5.0-rc2)")
    parser.add_argument("--packages", action="store_true", help="also write .deb and .rpm for Linux targets (nfpm)")
    args = parser.parse_args()
    if missing := dangling_references(SHARED_FILES):
        raise SystemExit("shipped docs reference files the archive lacks:\n  " + "\n  ".join(missing))
    if args.check_docs:
        return
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    subprocess.run(["python3", "tools/verify_tls.py"], cwd=ROOT, check=True, timeout=30)
    subprocess.run(["python3", "tools/generate_cli_docs.py", "--check"], cwd=ROOT, check=True, timeout=30)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, timeout=10).strip()
    status = subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"], cwd=ROOT, timeout=10)
    dirty = bool(status)
    if dirty:
        raise SystemExit("release staging requires a clean tracked tree; commit first")
    epoch = int(subprocess.check_output(["git", "show", "-s", "--format=%ct", "HEAD"], cwd=ROOT, timeout=10))
    base_version = re.search(r'\.version = "([^"]+)"', (ROOT / "build.zig.zon").read_text())[1]
    version, prerelease = base_version, None
    if args.tag:
        tag = re.fullmatch("v" + re.escape(base_version) + r"(?:-([0-9A-Za-z.-]+))?", args.tag)
        if not tag:
            raise SystemExit(f"tag {args.tag} does not name build.zig.zon's version {base_version}")
        version, prerelease = args.tag[1:], tag[1]
    manifest = {"version": version, "revision": revision, "zig": subprocess.check_output(
        ["zig", "version"], text=True, timeout=300).strip(),
        "tls_manifest_sha256": sha(ROOT / "vendor/boring_tls/prebuilt.sha256"),
        "artifacts": {}}
    builds = []
    for target in args.targets.split(","):
        if target not in TARGETS:
            raise SystemExit("unsupported release target: " + target)
        builds += [(target, suffix, cpu) for suffix, cpu in VARIANTS.get(target, (("", "baseline"),))]
    for target, suffix, cpu in builds:
        prefix = out / (target + suffix)
        if not args.skip_build:
            started = time.monotonic()
            try:
                subprocess.run(["zig", "build", "cli", "-j8", "-Doptimize=ReleaseFast", "-Dtarget=" + target,
                                "-Dcpu=" + cpu, "-Dstrip=true", "-Drevision=" + revision, "--prefix", str(prefix)]
                               + (["-Dprerelease=" + prerelease] if prerelease else []),
                               cwd=ROOT, check=True, timeout=args.build_timeout)
            finally:
                print(f"{target + suffix} build elapsed: {time.monotonic() - started:.1f}s", flush=True)
        binary = prefix / "bin/zpq"
        if revision.encode() not in binary.read_bytes():
            raise SystemExit("binary lacks its manifest revision; refusing to package stale output")
        if target == "x86_64-linux-musl":
            version_output = subprocess.check_output([str(binary), "--version"], text=True, timeout=10)
            if version_output != f"zpq {version} ({revision})\n":
                raise SystemExit(f"binary --version {version_output.strip()!r} differs from manifest {version}")
        files = {"bin/zpq": binary, **SHARED_FILES}
        # No version or revision in the name, so .../releases/latest/download/<name> works for every release; both
        # are in MANIFEST.json, release.json and `zpq --version`.
        name = f"zpq-{target}{suffix}.tar.gz"
        artifact = out / name
        metadata = {**{k: v for k, v in manifest.items() if k != "artifacts"}, "target": target, "cpu": cpu,
                    "binary_sha256": sha(binary), "files": {name: sha(p) for name, p in files.items()}}
        with artifact.open("wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as gz:
            with tarfile.open(fileobj=gz, mode="w") as tar:
                content = {name: p.read_bytes() for name, p in files.items()}
                content["MANIFEST.json"] = (json.dumps(metadata, sort_keys=True, indent=2) + "\n").encode()
                for filename, body in sorted(content.items()):
                    entry = tarfile.TarInfo(filename)
                    entry.size, entry.mtime = len(body), epoch
                    entry.mode = 0o755 if filename == "bin/zpq" else 0o644
                    tar.addfile(entry, io.BytesIO(body))
        manifest["artifacts"][target + suffix] = {"file": name, "sha256": sha(artifact), "binary_sha256": sha(binary),
                                                  "cpu": cpu}
        (out / (name + ".sha256")).write_text(sha(artifact) + "  " + name + "\n")
        (out / "release.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(target + suffix, name, manifest["artifacts"][target + suffix]["sha256"], flush=True)
        if args.packages and target in PACKAGE_ARCHES and not suffix:
            write_packages(out, target, content, version, epoch)


if __name__ == "__main__":
    main()
