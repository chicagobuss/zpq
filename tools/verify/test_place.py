"""Fail-closed placement contracts; all commands use temporary fixtures."""

import contextlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import execute
import placement
import provenance
import release_coverage
import revision
import stages

REPO = Path(__file__).resolve().parents[2]
SHA = "a" * 40
ARM = "aarch64-linux-musl"
MAC = "aarch64-macos"


def executable(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    path.chmod(0o755)
    return path


def proof(bin_dir, target=ARM):
    names = ("zpq", "zpq-lambda", "zpq-integration") if "linux" in target else ("zpq",)
    return {"sha": SHA, "target": target, "exit": 0,
            "release_smoke": {"status": "pass", "sha": SHA, "cli": True, "lambda": "linux" in target,
                              "binaries": {n: execute.file_sha256(bin_dir / n) for n in names}}}


FAKE_CLI = '''#!/bin/bash
case "$*" in
    *--version*) mode=version; echo "zpq 0.5.0 ($RELEASE_SMOKE_REVISION)" ;;
    *'max(s)'*) mode=aggregate; echo '{"n":1600,"s":12000,"m":"k001599-pppppppppppppppppppppppp"}' ;;
    *--query*) mode=sql; echo '{"x":28}' ;;
    *--filter*) mode=write ;;
    *) mode=readback; echo '{"n":200,"s":2900}' ;;
esac
printf '%s\\n' "$mode" >> "$TRACE"
if [ "${FAIL_AT:-}" = "$mode" ]; then exit 7; fi
'''
FAKE_LAMBDA = '''#!/bin/bash
printf '%s\\n' SHIPPED_LAMBDA >> "$TRACE"
if [ "${FAIL_AT:-}" = lambda ]; then exit 7; fi
'''
FAKE_INTEGRATION = '''#!/bin/bash
printf '%s\\n' SHIPPED_INTEGRATION >> "$TRACE"
if [ "${FAIL_AT:-}" = integration ]; then exit 7; fi
"$(dirname "$0")/zpq-lambda"
'''


def bundle(path):
    executable(path / "zpq", FAKE_CLI)
    executable(path / "zpq-lambda", FAKE_LAMBDA)
    executable(path / "zpq-integration", FAKE_INTEGRATION)
    manifest = "".join(f"{execute.file_sha256(path / n)}  {n}\n"
                       for n in ("zpq", "zpq-lambda", "zpq-integration"))
    (path / "SHA256SUMS").write_text(manifest)
    return execute.file_sha256(path / "SHA256SUMS")


class SmokeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        tools = self.root / "tools"
        executable(tools / "uname", '#!/bin/sh\ncase "$1" in -s) echo "$FAKE_OS";; *) echo "$FAKE_ARCH";; esac\n')
        executable(tools / "zig", '#!/bin/sh\necho unexpected-compiler >&2\nexit 99\n')
        self.out = self.root / "release"
        for target in (ARM, MAC, "x86_64-linux-musl"):
            bundle(self.out / target / "bin")
        self.env = dict(os.environ, PATH=f"{tools}:{os.environ['PATH']}", RELEASE_SMOKE_REVISION=SHA,
                        FAKE_OS="Linux", FAKE_ARCH="aarch64", TRACE=str(self.root / "trace"), FAIL_AT="")

    def run_smoke(self, targets, options=(), build=False, **env):
        mode = [] if build else ["--run-only"]
        return subprocess.run(["bash", str(REPO / "tools/release_smoke.sh"), *mode, *options,
                               str(self.out), *targets], env=dict(self.env, **env), text=True, capture_output=True)

    def test_foreign_and_unknown_hosts_block_without_evidence(self):
        for host in (("Linux", "x86_64"), ("OtherOS", "other")):
            with self.subTest(host=host):
                ran = self.run_smoke([ARM], FAKE_OS=host[0], FAKE_ARCH=host[1])
                self.assertEqual(ran.returncode, 2, ran.stderr)
                self.assertIn("NOT RUN aarch64-linux-musl", ran.stdout)
                self.assertNotIn("release smoke: PASS", ran.stdout)

    def test_only_same_sha_complete_native_proof_closes_coverage(self):
        path = self.root / "summary.json"
        bin_dir = self.out / ARM / "bin"
        valid = proof(bin_dir)
        for doc, code in ((valid, 0), ({**valid, "sha": "c" * 40}, 2), ({**valid, "exit": 1}, 2)):
            path.write_text(json.dumps(doc))
            ran = self.run_smoke([ARM], ("--arch-summary", str(path)), FAKE_ARCH="x86_64")
            self.assertEqual(ran.returncode, code, ran.stderr)
        for change in ({"lambda": False}, {"binaries": {}}, {"status": "not run"}, {"sha": "c" * 40}):
            doc = proof(bin_dir)
            doc["release_smoke"].update(change)
            self.assertFalse(release_coverage.proves(doc, SHA, ARM, bin_dir))
        self.assertFalse(release_coverage.proves({**valid, "exit": False}, SHA, ARM, bin_dir))
        self.assertFalse(release_coverage.proves(proof(self.out / MAC / "bin", MAC), SHA, ARM, bin_dir))

    def test_foreign_proof_requires_each_installed_binary_to_match(self):
        path = self.root / "summary.json"
        for target in (ARM, MAC):
            bin_dir = self.out / target / "bin"
            for name in proof(bin_dir, target)["release_smoke"]["binaries"]:
                with self.subTest(target=target, binary=name):
                    doc = proof(bin_dir, target)
                    doc["release_smoke"]["binaries"][name] = "b" * 64
                    path.write_text(json.dumps(doc))
                    run = self.run_smoke([target], ("--arch-summary", str(path)), FAKE_ARCH="x86_64")
                    self.assertEqual(run.returncode, 2, run.stderr)
                    self.assertIn(f"NOT RUN {target}", run.stdout)
                    path.write_text(json.dumps(proof(bin_dir, target)))
                    binary = bin_dir / name
                    content = binary.read_bytes()
                    binary.unlink()
                    self.assertFalse(release_coverage.proves(json.loads(path.read_text()), SHA, target, bin_dir))
                    binary.write_bytes(content + b"\n# replaced build\n")
                    self.assertFalse(release_coverage.proves(json.loads(path.read_text()), SHA, target, bin_dir))
                    binary.write_bytes(content)

    def test_intel_macos_requires_its_named_deferral(self):
        self.assertEqual(self.run_smoke(["x86_64-macos"]).returncode, 2)
        ran = self.run_smoke(["x86_64-macos"], ("--accept-deferred", "x86_64-macos"))
        self.assertEqual(ran.returncode, 0, ran.stderr)
        self.assertIn("NOT RUN x86_64-macos (deferred; accepted by name)", ran.stdout)
        self.assertNotIn("RAN x86_64-macos", ran.stdout)
        self.assertEqual(self.run_smoke([ARM], ("--accept-deferred", ARM)).returncode, 2)

    def test_golden_stdout_never_hides_a_nonzero_smoke_exit(self):
        for point in ("version", "aggregate", "sql", "write", "readback", "integration", "lambda"):
            with self.subTest(point=point):
                ran = self.run_smoke([ARM], FAIL_AT=point)
                self.assertEqual(ran.returncode, 7, ran.stderr)
                self.assertNotIn("release smoke: PASS", ran.stdout)
        ran = self.run_smoke([ARM])
        self.assertEqual(ran.returncode, 0, ran.stderr)
        trace = (self.root / "trace").read_text()
        self.assertIn("SHIPPED_INTEGRATION", trace)
        self.assertIn("SHIPPED_LAMBDA", trace)

    def test_build_mode_compiles_only_the_harness_and_runs_the_installed_lambda(self):
        harness = executable(self.root / 'harness', FAKE_INTEGRATION)
        zig = executable(self.root / 'tools/zig', '''#!/usr/bin/env python3
import json, os, pathlib, shutil, sys
args = sys.argv[1:]
if args == ['version']:
    print('0.17.0')
elif args == ['env']:
    print('.zig_exe = ' + json.dumps(__file__))
elif args[0] == 'build':
    assert 'test-integration' not in args, 'must not rebuild the Lambda for integration'
    with open(os.environ['TRACE'], 'a') as out: out.write('BUILD_BUNDLE\\n')
else:
    assert args[0] == 'test' and '--test-no-exec' in args
    opts = pathlib.Path(next(a.split('=', 1)[1] for a in args if a.startswith('-Mintegration_opts=')))
    prefix = pathlib.Path(os.environ['EXPECTED_PREFIX'])
    for name, binary in (('lambda_bin', 'zpq-lambda'), ('cli_bin', 'zpq')):
        assert f'pub const {name} = {json.dumps(str(prefix / binary))};' in opts.read_text()
    dest = next(a.split('=', 1)[1] for a in args if a.startswith('-femit-bin='))
    shutil.copy2(os.environ['HARNESS_SOURCE'], dest)
''')
        for point, code in (('', 0), ('integration', 7), ('lambda', 7)):
            with self.subTest(point=point):
                run = self.run_smoke([ARM], build=True, ZIG=str(zig), FAIL_AT=point,
                                     EXPECTED_PREFIX=str(self.out / ARM / 'bin'), HARNESS_SOURCE=str(harness))
                self.assertEqual(run.returncode, code, run.stderr)
        trace = (self.root / 'trace').read_text()
        self.assertEqual(trace.count('BUILD_BUNDLE'), 3)
        self.assertIn('SHIPPED_INTEGRATION', trace)
        self.assertIn('SHIPPED_LAMBDA', trace)

    def test_release_stage_never_accepts_elsewhere_or_empty_stdout(self):
        cmd = stages.stage_release_smoke(__import__("verify").dummy_ctx())
        for text, expected in (("elsewhere: verify arch\nrelease smoke: PASS", "blocked"),
                               ("NOT RUN aarch64-macos\nrelease smoke: PASS", "blocked"),
                               ("release smoke: PASS", "fail"),
                               ("RAN x86_64-linux-musl\nNOT RUN x86_64-macos (deferred; accepted by name)", "pass")):
            self.assertEqual(execute.judge(cmd, 0, text)[0], expected)
        self.assertEqual(execute.judge(cmd, 2, "RAN x86_64-linux-musl")[0], "blocked")
        self.assertEqual(execute.judge(cmd, 7, "NOT RUN aarch64-linux-musl")[0], "fail")


class CacheTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        clean_env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        env_patch = patch.dict(os.environ, clean_env, clear=True)
        env_patch.start()
        self.addCleanup(env_patch.stop)
        repo = self.root / "repo"
        repo.mkdir()
        for args in (("init", "-q"), ("config", "user.name", "cache test"),
                     ("config", "user.email", "cache@example.invalid"), ("config", "core.hooksPath", "/dev/null"),
                     ("config", "commit.gpgSign", "false")):
            subprocess.run(["/usr/bin/git", *args], cwd=repo, check=True, capture_output=True)
        (repo / "source.zig").write_text("pub const answer = 42;\n")
        (repo / ".zig-version").write_text("0.17.0\n")
        subprocess.run(["/usr/bin/git", "add", "."], cwd=repo, check=True)
        subprocess.run(["/usr/bin/git", "commit", "-qm", "fixture"], cwd=repo, check=True)
        for name, value in (("REPO", repo), ("CACHE", self.root / "cache")):
            patcher = patch.object(revision, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.sha = revision.resolve("HEAD")

    def test_modified_added_or_missing_sources_cannot_build_under_the_claimed_sha(self):
        tree = revision.source(self.sha)
        for mode in ("modified", "added", "missing", "executable", "symlink"):
            with self.subTest(mode=mode):
                path = tree / "source.zig"
                original = path.read_bytes()
                if mode == "modified": path.write_text("pub const answer = 99;\n")
                elif mode == "added": (tree / "injected.zig").write_text("// injected\n")
                elif mode == "missing": path.unlink()
                elif mode == "executable": path.chmod(0o755)
                else:
                    path.unlink()
                    path.symlink_to(revision.REPO / "source.zig")
                with patch.object(execute, "launch") as launch, self.assertRaises(RuntimeError):
                    revision.build(self.sha)
                launch.assert_not_called()
                if path.is_symlink(): path.unlink()
                path.write_bytes(original)
                path.chmod(0o644)
                (tree / "injected.zig").unlink(missing_ok=True)
        archive = tree.parent / "source.tar"
        archive.write_bytes(b"stale archive")
        with self.assertRaises(RuntimeError): revision.source(self.sha)

    def test_compiler_content_and_flags_change_the_cache_identity(self):
        tree = revision.source(self.sha)
        zig = self.root / "zig"
        script = '#!/bin/sh\ncase "$1" in version) echo 0.17.0;; env) echo \' .{ .zig_exe = "PATH" }\';; esac\n'
        executable(zig, script.replace("PATH", str(zig)))
        before = revision.compiler(str(zig), tree)
        executable(zig, script.replace("PATH", str(zig)) + "# a different binary with the same version\n")
        after = revision.compiler(str(zig), tree)
        self.assertNotEqual(before, after)
        first = revision.environment(before, self.root / before)
        second = revision.environment(after, self.root / after)
        self.assertNotEqual(first["ZIG_GLOBAL_CACHE_DIR"], second["ZIG_GLOBAL_CACHE_DIR"])
        self.assertNotEqual(first["ZIG_LOCAL_CACHE_DIR"], second["ZIG_LOCAL_CACHE_DIR"])
        base = revision.build_key(before, ARM, "baseline", ("cli",))
        for args in ((after, ARM, "baseline", ("cli",)), (before, MAC, "baseline", ("cli",)),
                     (before, ARM, "native", ("cli",)), (before, ARM, "baseline", ("cli", "lambda"))):
            self.assertNotEqual(base, revision.build_key(*args))

    def test_comparison_binary_must_report_the_requested_revision(self):
        revision.source(self.sha)
        with patch.object(revision, "compiler", return_value="b" * 64), \
                patch.object(execute, "launch", return_value=(0, 0)), \
                patch.object(revision.subprocess, "check_output", return_value="zpq wrong-revision"):
            # The source check uses git too; keep it verified before replacing the compiler's response.
            with patch.object(revision, "source", return_value=revision.CACHE / self.sha / "source"):
                with self.assertRaisesRegex(RuntimeError, "wrong revision"):
                    revision.build(self.sha)

    def test_cross_integration_embeds_paths_to_the_shipped_bundle(self):
        tree = revision.source(self.sha)
        dest = self.root / "bundle"
        dest.mkdir()
        with patch.object(execute, "launch", return_value=(0, 0)) as launch, \
                patch.object(revision, "compiler", return_value="b" * 64):
            revision.integration(tree, dest, ARM, False)
        argv = launch.call_args.args[0]
        self.assertIn("--test-no-exec", argv)
        self.assertIn(ARM, argv)
        self.assertFalse(launch.call_args.args[-1])
        text = (dest / "integration_opts.zig").read_text()
        self.assertIn(f'release/{ARM}/bin/zpq-lambda', text)
        self.assertIn(f'release/{ARM}/bin/zpq"', text)


FAKE_SSH = '''#!/usr/bin/env python3
import json, os, pathlib, shlex, subprocess, sys
parts = shlex.split(sys.argv[-1])
if parts[:2] == ['bash', '-c']:
    print(os.environ['FAKE_ROOT'] if parts[2].startswith('cd ') else 'probe fixture')
    sys.exit(0)
if parts and parts[0] == 'uptime':
    print('fixture uptime')
    sys.exit(0)
if len(parts) > 2 and parts[0] == 'bash' and parts[1].endswith('arch_remote.sh'):
    action = parts[2]
    if action in ('setup', 'release'):
        sys.exit(int(os.environ.get('FAKE_RELEASE_EXIT', '0')) if action == 'release' else 0)
    if action == 'run':
        out = pathlib.Path(parts[parts.index('--out') + 1]); out.mkdir(parents=True)
        code = int(os.environ['FAKE_RUNNER_EXIT'])
        doc = {'sha': 'a' * 40, 'target': 'aarch64-linux-musl',
               'exit': int(os.environ.get('FAKE_JSON_EXIT', str(code))),
               'queries': [{'id': 'cell', 'correctness': 'HASH_MISMATCH' if code == 3 else 'pass',
                            'hashes': ['b' * 64], 'error_text': 'expected x; exact output ' + str(out / 'bad.json')}]}
        (out / 'summary.json').write_text(json.dumps(doc))
        (out / 'bad.json').write_text('bad answer')
        (out / 'scratch.parquet').write_bytes(b'scratch')
        (out / 'runner.log').write_text('runner evidence')
        sys.exit(code)
sys.exit(subprocess.run(parts).returncode)
'''
FAKE_RSYNC = '''#!/usr/bin/env python3
import os, pathlib, shutil, sys
source, dest = sys.argv[-2:]
if ':' in source:
    source = source.split(':', 1)[1]
    if os.environ.get('FAKE_COLLECTION_FAIL') == '1': sys.exit(23)
    with open(os.environ['COLLECTION_TRACE'], 'a') as out: out.write(pathlib.Path(source).name + '\\n')
if ':' in dest: dest = dest.split(':', 1)[1]
src = pathlib.Path(source); dst = pathlib.Path(dest)
if src.is_dir():
    dst.mkdir(parents=True, exist_ok=True)
    for p in src.iterdir():
        if '--include=SHA256SUMS' in sys.argv and p.name not in ('SHA256SUMS','zpq','zpq-lambda','zpq-integration'):
            continue
        if p.is_dir(): shutil.copytree(p, dst / p.name, dirs_exist_ok=True)
        else: shutil.copy2(p, dst / p.name)
else:
    # Script shipping passes several sources before one directory destination.
    files = [p for p in sys.argv[1:-1] if not p.startswith('-')]
    for name in files:
        src = pathlib.Path(name.split(':', 1)[-1])
        if dest.endswith('/') or len(files) > 1 or dst.is_dir():
            dst.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst / src.name)
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
'''


class TransportTest(unittest.TestCase):
    def fixture(self, root, code, comparison=None, recorded=None, release_code=0, collect_fail=False):
        tools = root / "tools"
        executable(tools / "ssh", FAKE_SSH)
        executable(tools / "rsync", FAKE_RSYNC)
        binaries = root / "bin"
        bundle(binaries / ARM)
        executable(binaries / ARM / "arch-runner", "#!/bin/sh\nexit 99\n")
        data = root / "data"; data.mkdir()
        (data / "manifest.json").write_text('{"cases": [{"id": "cell"}]}')
        source = root / "source.tar"; source.write_bytes(b"source fixture")
        cache = root / "cache"; cache.mkdir()
        out = root / "results"
        config = {"revision": SHA, "data": str(data), "binaries": str(binaries), "out": str(out),
                  "source": str(source), "comparison": comparison,
                  "hosts": {"fixture": {"root": str(cache), "target": ARM, "duckdb_platform": "linux-arm64"}}}
        path = root / "fleet.json"; path.write_text(json.dumps(config))
        env = dict(os.environ, PATH=f"{tools}:{os.environ['PATH']}", FAKE_ROOT=str(cache),
                   FAKE_RUNNER_EXIT=str(code), COLLECTION_TRACE=str(root / "collection"),
                   FAKE_RELEASE_EXIT=str(release_code), FAKE_COLLECTION_FAIL=str(int(collect_fail)))
        env.pop("FAKE_JSON_EXIT", None)
        if recorded is not None: env["FAKE_JSON_EXIT"] = str(recorded)
        run = subprocess.run(["bash", str(REPO / "tools/verify/arch_host.sh"), "fixture", str(path), "false"],
                             env=env, text=True, capture_output=True)
        summaries = list(out.glob("fixture/*/summary.json"))
        self.assertEqual(len(summaries), 0 if collect_fail else 1, run.stderr + run.stdout)
        return run, summaries[0] if summaries else None, cache

    def test_singleton_process_and_record_preserve_every_runner_failure(self):
        for code in (0, 1, 2, 3):
            with self.subTest(code=code), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                run, path, cache = self.fixture(root, code)
                expected = 2 if code == 0 else code
                self.assertEqual(run.returncode, expected, run.stderr)
                doc = json.loads(path.read_text())
                self.assertEqual(doc['exit'], expected)
                self.assertEqual(doc['cross_host_comparison'], 'no cross-host comparison')
                fetched = (root / "collection").read_text().splitlines()
                self.assertEqual(fetched, ['summary.json', 'bad.json'] if code == 3 else ['summary.json'])
                remote_files = sorted(p.name for p in (cache / 'runs').rglob('*') if p.is_file())
                self.assertEqual(remote_files, ['runner.log', 'summary.json'])

    def test_failed_collection_retains_only_remote_summary_and_logs(self):
        with tempfile.TemporaryDirectory() as tmp:
            run, path, cache = self.fixture(Path(tmp), 0, 'reference host', collect_fail=True)
            self.assertEqual(run.returncode, 2, run.stderr)
            self.assertIsNone(path)
            remote_files = sorted(p.name for p in (cache / 'runs').rglob('*') if p.is_file())
            self.assertEqual(remote_files, ['runner.log', 'summary.json'])

    def test_successful_reference_has_native_artifact_proof_and_cleans_answers(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run, path, cache = self.fixture(root, 0, 'reference host')
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertTrue(release_coverage.proves(json.loads(path.read_text()), SHA, ARM, root / 'bin' / ARM))
            self.assertFalse(list((cache / 'runs').glob('*')))
            self.assertEqual((root / "collection").read_text().splitlines(), ['summary.json'])

    def test_process_and_runner_summary_disagreement_cannot_leave_a_green_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            run, path, _ = self.fixture(Path(tmp), 1, 'reference host', recorded=0)
            self.assertEqual(run.returncode, 1, run.stderr)
            doc = json.loads(path.read_text())
            self.assertEqual(doc['exit'], 1)
            self.assertIn('transport_error', doc)

    def test_failed_release_records_its_exit(self):
        with tempfile.TemporaryDirectory() as tmp:
            run, path, _ = self.fixture(Path(tmp), 0, 'reference host', release_code=7)
            self.assertEqual(run.returncode, 7, run.stderr)
            self.assertEqual(json.loads(path.read_text())['exit'], 7)

    def test_release_runs_shipped_lambda_and_harness_without_any_compiler(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache = root / "cache"; cache.mkdir()
            tools = root / "tools"
            executable(tools / "uname", '#!/bin/sh\ncase "$1" in -s) echo Linux;; *) echo aarch64;; esac\n')
            executable(tools / "systemctl", '#!/bin/sh\nexit 1\n')
            executable(tools / "zig", '#!/bin/sh\necho forbidden-compiler >&2\nexit 99\n')
            engine = cache / "bundle"
            digest = bundle(engine)
            engine.rename(cache / f"engine-{digest}")
            shutil.copy2(REPO / "tools/release_smoke.sh", cache / "release_smoke.sh")
            tree = root / "source"; (tree / "tools").mkdir(parents=True)
            (tree / "tools/placeholder").touch()
            archive = cache / 'source.tar'
            with tarfile.open(archive, 'w') as tar: tar.add(tree, arcname='.')
            source_sha = execute.file_sha256(archive)
            archive.rename(cache / f'source-{source_sha}.tar')
            env = dict(os.environ, PATH=f"{tools}:{os.environ['PATH']}", TRACE=str(root / 'trace'))
            command = ['bash', str(REPO / 'tools/verify/arch_remote.sh'), 'release', str(cache), SHA,
                       ARM, '1G', '0', digest, source_sha, '100000']
            run = subprocess.run(command, env=env, text=True, capture_output=True)
            self.assertEqual(run.returncode, 0, run.stderr)
            trace = (root / 'trace').read_text()
            self.assertIn('SHIPPED_INTEGRATION', trace)
            self.assertIn('SHIPPED_LAMBDA', trace)
            self.assertFalse(list(cache.glob('release.*')))
            self.assertNotIn('forbidden-compiler', run.stderr)
            (cache / f'engine-{digest}/zpq-lambda').write_text('tampered')
            checked = subprocess.run(['bash', str(REPO / 'tools/verify/arch_remote.sh'), 'verify',
                                      str(cache / f'engine-{digest}'), digest], env=env, capture_output=True)
            self.assertNotEqual(checked.returncode, 0)


class PlacementInputTest(unittest.TestCase):
    def test_bandwidth_probe_keeps_the_sixteen_gib_scope(self):
        completed = subprocess.CompletedProcess([], 0, stdout="bandwidth fixture\n", stderr="")
        with patch.object(provenance, '_run', return_value=completed) as run:
            self.assertEqual(provenance.bandwidth(REPO, Path('/tmp/probe-fixture')),
                             {'lines': ['bandwidth fixture']})
        self.assertEqual(run.call_args.args[0][:8],
                         ['systemd-run', '--user', '--scope', '-p', 'MemoryMax=16G', '-q', '--', 'nice'])

    def test_default_reference_is_fixed_without_consulting_the_invoking_hostname(self):
        cfg = {"hosts": {"[redacted]": {"target": "x86_64-linux-musl"}}}
        hosts, reference = placement.host_plan(cfg, ["mac", "arm"])
        self.assertEqual((hosts, reference), (["[redacted]", "arm", "mac"], "[redacted]"))

    def config(self, root, cases):
        (root / 'manifest.json').write_text(json.dumps({'cases': cases}))
        cfg = {'revision': SHA, 'data': str(root), 'out': str(root / 'results'),
               'hosts': {'arm': {'target': ARM}}}
        path = root / 'fleet.json'; path.write_text(json.dumps(cfg))
        return path

    def test_empty_cases_and_invalid_reference_are_exit_two_before_build(self):
        for bad_reference in (False, True):
            with self.subTest(reference=bad_reference), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                path = self.config(root, [] if not bad_reference else [{'id': 'cell'}])
                args = ['verify arch', '--config', str(path), '--host', 'arm']
                if bad_reference:
                    cfg = json.loads(path.read_text())
                    cfg['hosts']['mac'] = {'target': MAC}
                    cfg['reference_host'] = 'missing'
                    path.write_text(json.dumps(cfg)); args += ['--host', 'mac']
                with patch.object(sys, 'argv', args), patch.object(revision, 'build') as build, \
                        contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as stopped:
                    placement.main()
                self.assertEqual(stopped.exception.code, 2)
                build.assert_not_called()

    def test_no_scope_reaches_all_compiles_and_linux_bundle_contains_every_binary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = self.config(root, [{'id': 'cell'}])
            source = root / 'engine'
            bundle(source)
            def fake_integration(tree, dest, target, heavy):
                self.assertFalse(heavy)
                shutil.copy2(source / 'zpq-integration', dest / 'zpq-integration')
            with patch.object(sys, 'argv', ['verify arch', '--config', str(path), '--host', 'arm', '--no-scope']), \
                    patch.object(revision, 'CACHE', root / 'cache'), \
                    patch.object(revision, 'resolve', return_value=SHA), \
                    patch.object(revision, 'compiler', return_value='b' * 64), \
                    patch.object(revision, 'source', return_value=root), \
                    patch.object(revision, 'build', return_value=source / 'zpq') as build, \
                    patch.object(revision, 'integration', side_effect=fake_integration), \
                    patch.object(execute, 'launch', return_value=(0, 0)) as launch, \
                    patch.object(placement.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0)), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(placement.main(), 2)
            self.assertFalse(build.call_args.kwargs['heavy'])
            self.assertEqual(build.call_args.kwargs['steps'], ('cli', 'lambda'))
            self.assertFalse(launch.call_args.args[-1])
            manifest = root / f'cache/{SHA}/binaries/{ARM}/SHA256SUMS'
            names = [line.split()[1] for line in manifest.read_text().splitlines()]
            self.assertEqual(names, ['zpq', 'zpq-lambda', 'zpq-integration'])

    def test_release_workflows_require_all_three_native_platforms(self):
        expected = {('ubuntu-24.04', 'x86_64-linux-musl'), ('ubuntu-24.04-arm', ARM), ('macos-14', MAC)}
        for filename in ('release.yml', 'verify.yml'):
            text = (REPO / '.github/workflows' / filename).read_text()
            self.assertEqual(set(re.findall(r'- runner: (\S+)\n\s+target: (\S+)', text)), expected)
            self.assertIn('--accept-deferred x86_64-macos', text)
        release = (REPO / '.github/workflows/release.yml').read_text()
        self.assertIn('needs: [build, smoke]', release)
        smoke = release[release.index('  smoke:'):release.index('  release:')]
        self.assertIn('needs: build', smoke)
        self.assertIn('actions/download-artifact@v4', smoke)
        self.assertIn('--run-only', smoke)
        self.assertNotIn('setup-zig', smoke)
