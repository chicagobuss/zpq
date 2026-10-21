#!/usr/bin/env python3
"""Build and stage immutable, deterministic CLI tarballs; never upload or publish."""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import tarfile

ROOT = Path(__file__).resolve().parents[1]
TARGETS = ("x86_64-linux-musl", "aarch64-linux-musl", "aarch64-macos", "x86_64-macos")
# Everything but the binary, by archive path. The guides are the ones help and the man page name.
SHARED_FILES = {"LICENSE": ROOT / "LICENSE",
                "share/man/man1/zpq.1": ROOT / "packaging/man/zpq.1",
                "share/bash-completion/completions/zpq": ROOT / "packaging/completions/zpq.bash",
                "share/zsh/site-functions/_zpq": ROOT / "packaging/completions/_zpq",
                "share/fish/vendor_completions.d/zpq.fish": ROOT / "packaging/completions/zpq.fish",
                "share/zpq/tiny.parquet": ROOT / "packaging/fixtures/tiny.parquet",
                **{"share/doc/zpq/" + n: ROOT / "docs" / n
                   for n in ("getting-started.md", "output-format.md", "binary-output.md")}}


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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="zig-out/release")
    parser.add_argument("--targets", default=",".join(TARGETS[:3]))
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--check-docs", action="store_true", help="only check the archive's doc references")
    parser.add_argument("--tag", help="release tag, v<version> or v<version>-<prerelease>; a prerelease suffix is "
                        "built into the binary's and the manifest's version (e.g. v0.5.0-rc2)")
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
        ["zig", "version"], text=True, timeout=10).strip(),
        "tls_manifest_sha256": sha(ROOT / "vendor/boring_tls/prebuilt.sha256"),
        "artifacts": {}}
    for target in args.targets.split(","):
        if target not in TARGETS:
            raise SystemExit("unsupported release target: " + target)
        prefix = out / target
        if not args.skip_build:
            subprocess.run(["zig", "build", "cli", "-j8", "-Doptimize=ReleaseFast", "-Dtarget=" + target,
                            "-Dcpu=baseline", "-Dstrip=true", "-Drevision=" + revision, "--prefix", str(prefix)]
                           + (["-Dprerelease=" + prerelease] if prerelease else []),
                           cwd=ROOT, check=True, timeout=1200)
        binary = prefix / "bin/zpq"
        if revision.encode() not in binary.read_bytes():
            raise SystemExit("binary lacks its manifest revision; refusing to package stale output")
        if target == "x86_64-linux-musl":
            version_output = subprocess.check_output([str(binary), "--version"], text=True, timeout=10)
            if version_output != f"zpq {version} ({revision})\n":
                raise SystemExit(f"binary --version {version_output.strip()!r} differs from manifest {version}")
        files = {"bin/zpq": binary, **SHARED_FILES}
        name = f"zpq-{version}-{revision[:12]}-{target}.tar.gz"
        artifact = out / name
        metadata = {**{k: v for k, v in manifest.items() if k != "artifacts"}, "target": target,
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
        manifest["artifacts"][target] = {"file": name, "sha256": sha(artifact), "binary_sha256": sha(binary)}
        (out / (name + ".sha256")).write_text(sha(artifact) + "  " + name + "\n")
        (out / "release.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(target, name, manifest["artifacts"][target]["sha256"], flush=True)


if __name__ == "__main__":
    main()
