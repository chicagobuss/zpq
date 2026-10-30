"""Quick checks must never turn partial, contended or incorrect work into a merge signal."""

import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch

from bench import quick


class Quick(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = self.root / "data"
        self.data.mkdir()
        (self.data / "input").write_bytes(b"sealed input")
        self.case = {"id": "one", "file": "input", "sql": "SELECT count(*) FROM {T}", "output": "rows"}
        self.manifest = {"cases": [self.case], "files": {"input": {
            "bytes": 12, "sha256": quick.digest(self.data / "input")}}}
        self.cfg = {"data": str(self.data), "reps": 1, "budget_seconds": 5, "noise_pct": 10000,
                    "hosts": {"local": {"enabled": True, "root": str(self.root), "target": "test",
                                         "lock": str(self.root / "timing.lock"), "memory_max": "16G"}}}
        for name, sha in (("candidate", "a" * 40), ("base", "b" * 40)):
            self.make_cli(name, sha)
        self.out = self.root / "out"
        self.out.mkdir()
        self.config = self.root / "config.json"
        self.idle = patch("bench.quick.idle", return_value={"quiet": True, "busy_cores": 0})
        self.idle.start()
        self.addCleanup(self.idle.stop)
        # Native fixture runtimes are correctness tests, not measurements of the host.
        self.contention = patch("bench.env.foreign_cpu", return_value={"contended": False})
        self.contention.start()
        self.addCleanup(self.contention.stop)

    def make_cli(self, name, sha, answer='{"n":1}\n', stderr="", rc=0, delay=0, hot_delay=0,
                 hot_answer=None, parquet=b"fixture"):
        path = self.root / (name + ".py")
        counter = self.root / (name + ".count")
        path.write_text(
            "#!" + sys.executable + "\nimport sys,time,pathlib\n"
            "if '--version' in sys.argv:\n print('zpq " + sha[:12] + "');sys.exit(0)\n"
            "p=pathlib.Path(" + repr(str(counter)) + ")\n"
            "n=int(p.read_text()) if p.exists() else 0\np.write_text(str(n+1))\n"
            "time.sleep(" + repr(delay) + " if n==0 else " + repr(hot_delay) + ")\n"
            "if '-o' in sys.argv:\n pathlib.Path(sys.argv[sys.argv.index('-o')+1]).write_bytes(" +
            repr(parquet) + ")\nelse:\n sys.stdout.write(" + repr(answer) + " if n==0 else " +
            repr(answer if hot_answer is None else hot_answer) + ")\n"
            "sys.stderr.write(" + repr(stderr) + ")\nsys.exit(" + str(rc) + ")\n")
        path.chmod(0o755)
        self.cfg["hosts"]["local"][name] = {
            "path": str(path), "sha": sha, "sha256": quick.digest(path), "target": "test",
            "cpu": "baseline", "optimize": "ReleaseFast", "strip": True, "toolchain": "0.17.0"}

    def write_config(self):
        quick.save(self.data / "manifest.json", self.manifest)
        self.cfg["manifest_sha256"] = quick.digest(self.data / "manifest.json")
        quick.save(self.config, self.cfg)
        return self.config

    def run_worker(self):
        self.write_config()
        code = quick.worker(self.config, "local", self.out, time.monotonic() + self.cfg["budget_seconds"])
        return code, quick.read(self.out / "report.json")

    def rows(self):
        return [json.loads(x) for x in (self.out / "attempts.jsonl").read_text().splitlines()]

    def test_all_hash_checks_precede_fixed_paired_samples(self):
        self.cfg["reps"] = 2
        code, report = self.run_worker()
        self.assertEqual(code, 0)
        self.assertEqual([r["phase"] for r in self.rows()], ["hash-first"] * 2 + ["hot-fresh"] * 4)
        self.assertEqual([r["engine"] for r in self.rows()[2:]], ["candidate", "base", "base", "candidate"])
        self.assertTrue(report["hash_pass_complete"])
        self.assertTrue(report["cells"][0]["byte_identical"])
        self.assertEqual(report["selected_timings"], 4)
        self.assertEqual(report["other_hosts"], "unmeasured; this is one host, not fleet acceptance")

    def test_input_checksum_rejection_precedes_native_query(self):
        self.manifest["files"]["input"]["sha256"] = "0" * 64
        code, report = self.run_worker()
        self.assertEqual(code, 3)
        self.assertEqual(report["attempts"], 0)
        self.assertIn("input checksum", report["reason"])
        self.assertFalse((self.root / "candidate.count").exists())

    def test_input_verdict_is_reused_across_workers_without_reading_input(self):
        self.assertEqual(self.run_worker()[0], 0)
        initial = quick.read(self.out / "report.json")["input_verification"][0]
        self.out = self.root / "second-out"
        self.out.mkdir()
        original = quick.digest
        def guarded(path, check=lambda: None):
            self.assertNotEqual(Path(path), self.data / "input", "cached input was read again")
            return original(path, check)
        with patch("bench.quick.digest", side_effect=guarded):
            code, report = self.run_worker()
        self.assertEqual(code, 0)
        self.assertEqual(report["selected_timings"], 2)
        self.assertTrue(report["input_verification"][0]["cache_hit"])
        self.assertFalse(initial["cache_hit"])
        self.assertEqual(initial["verified_at"], report["input_verification"][0]["verified_at"])

    def test_each_input_identity_change_requires_a_new_digest(self):
        for field in ("path", "size", "mtime", "inode"):
            with self.subTest(field=field):
                source = self.root / field
                source.write_bytes(b"input")
                cache = quick.InputHashes(self.root / (field + "-cache"))
                before = source.stat()
                with patch("bench.quick.digest", wraps=quick.digest) as hashing:
                    cache.verify(source, hashlib.sha256(b"input").hexdigest())
                    if field == "path":
                        alias = source.with_suffix(".alias")
                        os.link(source, alias)
                        source = alias
                    elif field == "size":
                        source.write_bytes(b"input extra")
                        os.utime(source, ns=(before.st_atime_ns, before.st_mtime_ns))
                    elif field == "mtime":
                        os.utime(source, ns=(before.st_atime_ns, before.st_mtime_ns + 1_000_000))
                    else:
                        replacement = source.with_suffix(".new")
                        replacement.write_bytes(b"input")
                        os.utime(replacement, ns=(before.st_atime_ns, before.st_mtime_ns))
                        replacement.replace(source)
                        self.assertNotEqual(before.st_ino, source.stat().st_ino)
                    expected = hashlib.sha256(source.read_bytes()).hexdigest()
                    self.assertFalse(cache.verify(source, expected)["cache_hit"])
                    self.assertTrue(cache.verify(source, expected)["cache_hit"])
                    self.assertEqual(hashing.call_count, 2)

    def test_cached_digest_cannot_clear_a_different_expected_pin(self):
        cache = quick.InputHashes(self.root)
        source = self.data / "input"
        cache.verify(source, self.manifest["files"]["input"]["sha256"])
        with patch("bench.quick.digest", side_effect=AssertionError("input was read again")):
            with self.assertRaises(quick.Mismatch):
                cache.verify(source, "0" * 64)

    def test_corrupt_or_other_host_cache_is_rehashed(self):
        for corrupt in (b"{unfinished", b"[]", b"other-host"):
            with self.subTest(corrupt=corrupt):
                cache = quick.InputHashes(self.root)
                row = cache.verify(self.data / "input", self.manifest["files"]["input"]["sha256"])
                path = Path(row["cache_path"])
                if corrupt == b"other-host":
                    entry = quick.read(path)
                    entry["host"] = "different-host"
                    quick.save(path, entry)
                else:
                    path.write_bytes(corrupt)
                self.assertFalse(cache.verify(self.data / "input", row["sha256"])["cache_hit"])

    def test_changing_or_interrupted_hash_never_creates_a_verdict(self):
        cache = quick.InputHashes(self.root)
        source = self.data / "input"
        before = source.stat()
        def mutate(path, check):
            os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns + 1_000_000))
            return self.manifest["files"]["input"]["sha256"]
        with patch("bench.quick.digest", side_effect=mutate):
            with self.assertRaises(quick.Mismatch):
                cache.verify(source, "0" * 64)
        with patch("bench.quick.digest", side_effect=quick.BudgetExpired):
            with self.assertRaises(quick.BudgetExpired):
                cache.verify(source, "0" * 64)
        self.assertFalse(cache.root.exists())

    def test_local_setup_seals_only_requested_files_then_worker_produces_pairs(self):
        self.manifest["cases"].append({**self.case, "id": "unused", "file": "missing"})
        self.manifest["files"]["missing"] = {"bytes": 100, "sha256": "0" * 64}
        self.cfg["only"] = ["one"]
        self.cfg["hosts"]["localhost"] = self.cfg["hosts"]["local"]
        self.write_config()
        out = self.root / "fleet-out"
        def worker(config, host, result):
            return quick.worker(config, host, result, time.monotonic() + 5)
        with patch("bench.quick.supervise", side_effect=worker):
            self.assertEqual(quick.fleet(self.config, "localhost", out), 0)
        setup, report = quick.read(out / "setup.json"), quick.read(out / "report.json")
        self.assertFalse(setup["input_verification"][0]["cache_hit"])
        self.assertTrue(report["input_verification"][0]["cache_hit"])
        self.assertEqual(report["selected_timings"], 2)
        self.assertEqual(report["unrequested_cases"], ["unused"])

    def test_remote_probe_reuses_verdict_and_worker_reuses_object_across_views(self):
        source = self.root / "object"
        source.write_bytes(b"sealed input")
        def local_probe(argv, host, lock, timeout):
            return subprocess.run(argv, check=True, capture_output=True, timeout=timeout)
        expected = self.manifest["files"]["input"]["sha256"]
        with patch("bench.quick.remote", side_effect=local_probe):
            first = quick.remote_input(Path.cwd(), self.root, source, expected, "remote", "lock", 5)
            second = quick.remote_input(Path.cwd(), self.root, source, expected, "remote", "lock", 5)
        self.assertFalse(first["cache_hit"])
        self.assertTrue(second["cache_hit"])
        self.cfg["input_objects"] = {"input": str(source)}
        code, report = self.run_worker()
        self.assertEqual(code, 3)
        self.assertIn("object/view differs", report["reason"])
        self.assertEqual(report["attempts"], 0)
        (self.data / "input").unlink()
        os.link(source, self.data / "input")
        original = quick.digest
        def guarded(path, check=lambda: None):
            self.assertNotEqual(Path(path), source)
            return original(path, check)
        with patch("bench.quick.digest", side_effect=guarded):
            code, report = self.run_worker()
        self.assertEqual(code, 0)
        self.assertTrue(report["input_verification"][0]["cache_hit"])

    def test_binary_checksum_rejection_precedes_native_query(self):
        self.cfg["hosts"]["local"]["candidate"]["sha256"] = "0" * 64
        code, report = self.run_worker()
        self.assertEqual(code, 3)
        self.assertEqual(report["attempts"], 0)
        self.assertIn("binary checksum", report["reason"])

    def test_wrong_build_revision_cannot_establish_reference(self):
        self.cfg["hosts"]["local"]["candidate"]["sha"] = "c" * 40
        code, report = self.run_worker()
        self.assertEqual(code, 3)
        self.assertFalse(report["hash_pass_complete"])

    def test_mismatched_build_tuples_and_unbounded_budget_rejected(self):
        self.write_config()
        for value in (0, 121, float("inf"), float("nan")):
            self.cfg["budget_seconds"] = value
            self.config.write_text(json.dumps(self.cfg))
            with self.assertRaises(ValueError):
                quick.load(self.config, "local")
        self.cfg["budget_seconds"] = 90
        self.cfg["hosts"]["local"]["base"]["cpu"] = "native"
        quick.save(self.config, self.cfg)
        with self.assertRaisesRegex(ValueError, "build tuples"):
            quick.load(self.config, "local")

    def test_raw_output_difference_blocks_before_any_timing(self):
        self.make_cli("base", "b" * 40, answer='{"n":"01001"}\n')
        code, report = self.run_worker()
        self.assertEqual(code, 3)
        self.assertFalse(report["hash_pass_complete"])
        self.assertEqual(report["selected_timings"], 0)
        self.assertTrue(all(r["phase"] == "hash-first" for r in self.rows()))

    def test_native_refusal_identity_is_not_a_speedup(self):
        for name, sha in (("candidate", "a" * 40), ("base", "b" * 40)):
            self.make_cli(name, sha, answer="", stderr="memory budget exceeded\n", rc=1)
        self.case["expected"] = {"all": {"outcome": "refusal", "native_exit": 1,
                                       "stdout_sha256": hashlib.sha256(b"").hexdigest(),
                                       "stderr_sha256": hashlib.sha256(b"memory budget exceeded\n").hexdigest()}}
        code, report = self.run_worker()
        self.assertEqual(code, 0)
        self.assertIsNone(report["cells"][0]["candidate_over_base"])
        self.assertEqual(report["cells"][0]["engines"]["candidate"]["native_outcomes"], ["refusal"])

    def test_refusal_diagnostic_transition_fails(self):
        self.case["expected"] = {"all": {"outcome": "refusal", "native_exit": 1,
                                       "stdout_sha256": hashlib.sha256(b"").hexdigest(),
                                       "stderr_sha256": hashlib.sha256(b"old\n").hexdigest()}}
        self.make_cli("candidate", "a" * 40, answer="", stderr="different\n", rc=1)
        code, report = self.run_worker()
        self.assertEqual(code, 3)
        self.assertEqual(report["selected_timings"], 0)
        self.assertIn("stderr_sha256", report["reason"])

    def test_candidate_only_outcome_case_does_not_run_budget_flag_on_base(self):
        self.case["engines"] = ["candidate"]
        code, report = self.run_worker()
        self.assertEqual(code, 0)
        self.assertFalse((self.root / "base.count").exists())
        self.assertEqual(set(report["cells"][0]["engines"]), {"candidate"})
        self.assertIsNone(report["cells"][0]["candidate_over_base"])

    def test_existing_host_lock_defers_without_queries(self):
        with (self.root / "timing.lock").open("w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            code, report = self.run_worker()
        self.assertEqual(code, 2)
        self.assertEqual(report["state"], "deferred")
        self.assertEqual(report["attempts"], 0)

    def test_busy_host_defers_without_query(self):
        self.idle.stop()
        self.cfg["hosts"]["local"]["idle_command"] = [
            sys.executable, "-c", 'print(\'{"quiet":false,"busy_cores":4}\')']
        code, report = self.run_worker()
        self.assertEqual(code, 2)
        self.assertEqual(report["state"], "deferred")
        self.assertEqual(report["attempts"], 0)
        self.assertFalse((self.root / "candidate.count").exists())

    def test_one_deadline_is_shared_by_hash_pass_of_both_builds(self):
        self.make_cli("base", "b" * 40, delay=0.5)
        self.cfg["budget_seconds"] = 0.3
        start = time.monotonic()
        code, report = self.run_worker()
        self.assertLess(time.monotonic() - start, 0.8)
        self.assertEqual(code, 2)
        self.assertFalse(report["hash_pass_complete"])
        self.assertEqual(report["selected_timings"], 0)
        self.assertFalse((self.out / "active.json").exists())
        self.assertTrue(any(r["status"] == "timeout" for r in self.rows()))

    def test_interrupted_hot_pair_has_no_selected_half_pair_or_ratio(self):
        self.make_cli("base", "b" * 40, hot_delay=0.8)
        self.cfg["budget_seconds"] = 0.5
        code, report = self.run_worker()
        self.assertEqual(code, 2)
        self.assertTrue(report["hash_pass_complete"])
        self.assertEqual(report["selected_timings"], 0)
        self.assertIsNone(report["cells"][0]["candidate_over_base"])
        self.assertIsNone(report["cells"][0]["engines"]["candidate"]["median_ms"])

    def test_repeated_output_change_is_failed_not_flat_or_flagged(self):
        self.make_cli("candidate", "a" * 40, hot_answer='{"n":2}\n')
        code, report = self.run_worker()
        self.assertEqual(code, 3)
        self.assertEqual(report["state"], "failed")
        self.assertEqual(report["selected_timings"], 0)
        self.assertIsNone(report["cells"][0]["candidate_over_base"])

    def test_cross_host_reference_must_bind_sources_inputs_and_raw_hashes(self):
        reference = self.root / "reference.json"
        self.write_config()
        quick.save(reference, {"corpus_manifest_sha256": self.cfg["manifest_sha256"],
                               "query_set_sha256": hashlib.sha256(quick.encode([self.case])).hexdigest(),
                               "candidate": {"sha": "a" * 40}, "baseline": {"sha": "b" * 40},
                               "hashes": {"one": {"candidate": "incorrect"}}})
        self.cfg["reference"] = str(reference)
        code, report = self.run_worker()
        self.assertEqual(code, 3)
        self.assertIn("cross-host", report["reason"])
        self.assertEqual(report["selected_timings"], 0)

    def parquet(self, declared=False):
        self.case["output"] = "parquet"
        if declared:
            self.case["parquet_byte_difference"] = "explicit footer format change"
        self.cfg["parquet_validators"] = {k: [sys.executable, "-c", "pass", "{output}"]
                                          for k in ("duckdb", "pyarrow")}
        self.make_cli("candidate", "a" * 40, parquet=b"v2")
        self.make_cli("base", "b" * 40, parquet=b"v1")

    def test_parquet_requires_both_readback_validators(self):
        self.parquet()
        del self.cfg["parquet_validators"]["pyarrow"]
        code, report = self.run_worker()
        self.assertEqual(code, 3)
        self.assertIn("validator", report["reason"])
        self.assertEqual(report["selected_timings"], 0)

    def test_declared_parquet_change_preserves_literal_byte_difference(self):
        self.parquet(declared=True)
        code, report = self.run_worker()
        self.assertEqual(code, 0)
        self.assertFalse(report["cells"][0]["byte_identical"])
        self.assertEqual(report["cells"][0]["declared_parquet_difference"], "explicit footer format change")
        self.assertTrue(all(r.get("value_validation") == {"duckdb": "pass", "pyarrow": "pass"}
                            for r in self.rows()))

    def test_undeclared_parquet_difference_is_not_normalized_away(self):
        self.parquet()
        code, report = self.run_worker()
        self.assertEqual(code, 3)
        self.assertEqual(report["selected_timings"], 0)

    def test_failed_parquet_validator_stops_hash_pass(self):
        self.parquet(declared=True)
        self.cfg["parquet_validators"]["duckdb"] = [sys.executable, "-c", "raise SystemExit(1)"]
        code, report = self.run_worker()
        self.assertEqual(code, 3)
        self.assertIn("duckdb rejected", report["reason"])

    def test_command_timing_is_not_rounded_to_wait_backoff(self):
        # Popen.wait(timeout) polls at 0.5, 1, 2 ... 32 ms, so an 18 ms child used to read 31.5 ms.
        run = quick.Run(self.out, time.monotonic() + 5)
        best = min(run.command([sys.executable, "-c", "import time; time.sleep(0.018)"],
                               self.out / "o", self.out / "e")[1] for _ in range(3))
        startup = min(run.command([sys.executable, "-c", "pass"], self.out / "o", self.out / "e")[1]
                      for _ in range(3))
        self.assertLess(best - startup, 27_000_000)

    def test_nested_native_process_group_is_drained_at_deadline(self):
        run = quick.Run(self.out, time.monotonic() + 0.3)
        pid = self.root / "child.pid"
        command = ("import subprocess,time,pathlib,sys;"
                   "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']);"
                   "pathlib.Path(sys.argv[1]).write_text(str(p.pid));time.sleep(30)")
        with self.assertRaises(quick.BudgetExpired):
            run.command([sys.executable, "-c", command, str(pid)], self.out / "o", self.out / "e")
        if Path("/proc").exists() and pid.exists():
            # Killed, the nested child is a zombie, dying or already reaped; any other state means it still runs.
            try:
                state = (Path("/proc") / pid.read_text() / "stat").read_text().split()[2]
            except (FileNotFoundError, ProcessLookupError):  # reaped between the open and the read
                state = None
            self.assertIn(state, ("Z", "X", None))
        self.assertFalse((self.out / "active.json").exists())

    def test_partial_counts_and_flags_are_deterministic(self):
        rows = []
        for engine, ns in (("candidate", 2_000_000), ("base", 1_000_000)):
            rows.append({"case": "one", "engine": engine, "phase": "hot-fresh", "rep": 1, "status": "ok",
                         "wall_ns": ns, "native_outcome": "answer", "artifact_sha256": "h"})
        first = quick.cells_for([self.case], rows, 1, True, 0)
        self.assertTrue(first[0]["flagged"])
        self.assertEqual(quick.encode(first), quick.encode(quick.cells_for([self.case], rows, 1, True, 0)))
        partial = quick.cells_for([self.case], rows[:1], 1, True, 0)
        self.assertIsNone(partial[0]["engines"]["candidate"]["median_ms"])

    def test_manifest_subsets_and_paths_cannot_invent_or_escape_inputs(self):
        self.assertEqual(quick.manifest_cases(self.manifest, ["one"]), [self.case])
        with self.assertRaises(ValueError):
            quick.manifest_cases(self.manifest, ["absent"])
        with self.assertRaises(ValueError):
            quick.inside(self.data, "../elsewhere")

    def test_watchdog_retains_denominator_when_worker_cannot_finish(self):
        self.make_cli("base", "b" * 40, delay=2)
        self.cfg["budget_seconds"] = 0.5
        self.cfg["hosts"]["local"]["idle_command"] = [
            sys.executable, "-c", 'print(\'{"quiet":true,"busy_cores":0}\')']
        self.write_config()
        start = time.monotonic()
        code = quick.supervise(self.config, "local", self.out)
        self.assertLess(time.monotonic() - start, 1.5)
        self.assertEqual(code, 2)
        report = quick.read(self.out / "report.json")
        self.assertEqual(report["planned_cases"], 1)
        self.assertFalse(report["coverage_complete"])
        self.assertEqual(report["selected_timings"], 0)
        self.assertIsNone(report["cells"][0]["candidate_over_base"])

    def test_recovery_retains_counts_and_cannot_certify_partial_journal(self):
        self.write_config()
        row = {"case": "one", "engine": "candidate", "phase": "hot-fresh", "rep": 1, "status": "ok",
               "wall_ns": 1, "native_outcome": "answer", "artifact_sha256": "h"}
        (self.out / "attempts.jsonl").write_bytes(quick.encode(row).replace(b"\n", b"") + b"\n{")
        quick.recover(self.config, "local", self.out, time.monotonic(), "watchdog")
        report = quick.read(self.out / "report.json")
        self.assertEqual(report["planned_cases"], 1)
        self.assertEqual(report["attempts"], 1)
        self.assertEqual(report["selected_timings"], 0)
        self.assertEqual(report["state"], "incomplete")

    def test_setup_transfer_lock_survives_exec_and_rejects_competing_holder(self):
        lock = str(self.root / "transfer.lock")
        check = ("import fcntl,sys;f=open(sys.argv[1],'a');"
                 "\ntry: fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)"
                 "\nexcept BlockingIOError: sys.exit(42)")
        done = subprocess.run([sys.executable, "-c", quick.LOCK_EXEC, lock,
                               sys.executable, "-c", check, lock])
        self.assertEqual(done.returncode, 42)
        with open(lock, "a") as held:
            fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
            done = subprocess.run([sys.executable, "-c", quick.LOCK_EXEC, lock,
                                   sys.executable, "-c", "raise SystemExit(99)"])
            self.assertEqual(done.returncode, 75)

    def rival_fixture(self, reps):
        path = self.root / "arch-runner.py"
        path.write_text("#!" + sys.executable + "\nimport sys,pathlib,json\n"
                        "p=pathlib.Path(sys.argv[sys.argv.index('--out')+1]);"
                        "(p/'seconds').write_text(sys.argv[sys.argv.index('--budget-seconds')+1]);"
                        "(p/'summary.json').write_text(json.dumps({'queries':[{'timings':["
                        "{'engine':'zpq','reps':" + str(reps) + "},"
                        "{'engine':'duckdb','reps':" + str(reps) + "}]}]}))\n")
        path.chmod(0o755)
        self.cfg["arch_runner"] = {"path": str(path), "sha256": quick.digest(path)}
        self.cfg["duckdb"] = self.cfg["hosts"]["local"]["base"]

    def test_optional_rivals_get_only_the_remaining_shared_budget(self):
        self.rival_fixture(1)
        _, host, _, _ = quick.load(self.write_config(), "local")
        run = quick.Run(self.out, time.monotonic() + 5.8)
        quick.rivals(run, self.cfg, host, self.data)
        self.assertLessEqual(int((self.out / "rivals/seconds").read_text()), 5)

    def test_missing_optional_rival_repetitions_cannot_be_complete(self):
        self.rival_fixture(0)
        _, host, _, _ = quick.load(self.write_config(), "local")
        run = quick.Run(self.out, time.monotonic() + 5.8)
        with self.assertRaises(quick.BudgetExpired):
            quick.rivals(run, self.cfg, host, self.data)


if __name__ == "__main__":
    unittest.main()
