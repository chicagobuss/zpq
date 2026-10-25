#!/usr/bin/env python3
"""Registry-free checks for the summary writer and the ship bar."""

import contextlib
import argparse
import fcntl
import io
import json
import sys
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import execute
import perf
import provenance
import scorecard_select
import shipbar
import stages
import summary
import verify


def cell(engine, ns, query="q", failures=None, checked=3):
    return {
        "suite": "clickbench",
        "dataset": "hits",
        "query_id": query,
        "engine": engine,
        "median_ns": ns,
        "checked": checked,
        "failures": failures or [],
    }


class EnvironmentTest(unittest.TestCase):
    def test_missing_and_wrong_versions_stop_before_host_probes(self):
        installed = {"duckdb": "1.4.3"}

        def lookup(name):
            if name not in installed:
                raise provenance.PackageNotFoundError(name)
            return installed[name]

        with patch.object(provenance, "version", side_effect=lookup), \
                patch.object(verify, "open_run") as open_run, contextlib.redirect_stderr(io.StringIO()) as err:
            self.assertEqual(verify.main(["quick"]), 2)
        open_run.assert_not_called()
        self.assertIn("duckdb: need 1.5.5, installed 1.4.3", err.getvalue())
        self.assertIn("pyarrow: need 25.0.0, installed missing", err.getvalue())
        self.assertIn("uv pip install", err.getvalue())

    def test_exact_pins_are_recorded(self):
        required = provenance.python_environment(verify.REPO)["required"]
        with patch.object(provenance, "version", side_effect=required.__getitem__):
            actual = provenance.python_environment(verify.REPO)
        self.assertEqual(actual["installed"], required)
        self.assertEqual(actual["mismatches"], [])
        self.assertEqual(len(actual["requirements_sha256"]), 64)


class SummaryTest(unittest.TestCase):
    def test_refuses_to_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory) / "run-a"
            summary.Summary(run, {"tier": "quick", "sha": "abc"})
            with self.assertRaises(FileExistsError):
                summary.Summary(run, {"tier": "quick", "sha": "abc"})

    def test_expected_is_not_a_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            run = summary.Summary(Path(directory) / "run-b", {"tier": "full", "sha": "abc", "rerun_of": "run-a"})
            run.add_stage({"name": "repro-skspill", "status": "expected", "rc": 1})
            run.add_stage({"name": "differential", "status": "fail", "rc": 1})
            self.assertEqual(run.data["expected"], ["repro-skspill"])
            self.assertEqual(run.data["failures"], ["differential"])
            self.assertEqual(run.data["rerun_of"], "run-a")
            self.assertEqual(summary.exit_code(run), 1)
            saved = json.loads((Path(directory) / "run-b" / "summary.json").read_text())
            self.assertEqual(saved["failures"], ["differential"])


class JudgeTest(unittest.TestCase):
    def test_referee_refusals_survive_summary_serialization(self):
        text = "referee: 606/606 checks match\nreferee: expected 120\nreferee: refusals 120\n"
        text += "referee: refusal declarations 40\n"
        counts = stages.parse_referee(text)
        with tempfile.TemporaryDirectory() as directory:
            book = summary.Summary(Path(directory), {"tier": "commit", "sha": "abc"})
            book.add_stage({"name": "differential", "status": "pass", "counts": counts})
            saved = json.loads(book.path.read_text())["stages"][0]["counts"]
        self.assertEqual(saved["refusals"], 120)
        self.assertEqual(saved["expected"], 120)
        self.assertEqual(saved["refusal_declarations"], 40)

    def test_declared_refusal(self):
        cmd = stages.Cmd(
            [],
            {},
            "spill",
            expect="refusal",
            expect_rc=1,
            expect_text="the query needs more memory than --max-memory allows",
        )
        status, _counts = execute.judge(
            cmd, 1, "error: the query needs more memory than --max-memory allows\n"
        )
        self.assertEqual(status, "expected")
        status, _counts = execute.judge(cmd, 0, "rows\n")
        self.assertEqual(status, "fail")

    def test_clickbench_fail_is_not_hidden_by_exit_zero(self):
        cmd = stages.Cmd([], {}, "cb", parse=stages.parse_clickbench)
        status, counts = execute.judge(cmd, 0, "{'PASS': 40, 'FAIL': 2}\n")
        self.assertEqual(counts["fail"], 2)
        self.assertEqual(status, "fail")


class SelectTest(unittest.TestCase):
    def test_missing_input_skips_the_suite_whole(self):
        cases = [
            {"suite": "pdsh", "dataset": "sf1", "query_id": "1", "inputs": ["/no/such/lineitem.parquet"]},
            {"suite": "pdsh", "dataset": "sf1", "query_id": "2", "inputs": ["/no/such/lineitem.parquet"]},
        ]
        decision = scorecard_select.classify_built("pdsh", cases, None)
        self.assertEqual(decision["status"], "skipped")
        self.assertEqual(decision["reason"], "missing inputs")
        self.assertNotIn("queries", decision)

    def test_present_file_runs_and_case_filter_narrows(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "hits.parquet"
            path.write_bytes(b"x")
            cases = [
                {"suite": "clickbench", "dataset": "hits", "query_id": "0", "inputs": [str(path)]},
                {"suite": "clickbench", "dataset": "hits", "query_id": "1", "inputs": [str(path)]},
            ]
            decision = scorecard_select.classify_built("clickbench", cases, ["clickbench/hits/0"])
            self.assertEqual(decision["status"], "ran")
            self.assertEqual(decision["queries"], ["clickbench/hits/0"])

    def test_unknown_case_does_not_run_a_subset(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "hits.parquet"
            path.write_bytes(b"x")
            cases = [{"suite": "clickbench", "dataset": "hits", "query_id": "0", "inputs": [str(path)]}]
            decision = scorecard_select.classify_built("clickbench", cases, ["clickbench/hits/0", "clickbench/hits/99"])
            self.assertEqual(decision["status"], "skipped")
            self.assertEqual(decision["unmatched"], ["clickbench/hits/99"])


class FullTierTest(unittest.TestCase):
    def test_full_refuses_without_base(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            code = verify.main(["full"])
        self.assertEqual(code, 2)
        self.assertIn("--base", err.getvalue())

    def test_full_refuses_a_missing_base_file(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            code = verify.main(["full", "--base", "/no/such/zpq"])
        self.assertEqual(code, 2)
        self.assertIn("not a file", err.getvalue())


class PerfRecordTest(unittest.TestCase):
    def test_busy_timing_lock_leaves_host_profile_alone(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            (home / ".cache").mkdir()
            with (home / ".cache/zpq-bench.lock").open("a+") as held:
                fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with patch.object(perf.Path, "home", return_value=home), patch.object(perf, "subprocess") as calls:
                    with self.assertRaises(BlockingIOError):
                        perf._run(["unused"], home, home / "log", timing_lock=True)
                    calls.run.assert_not_called()
                    calls.check_output.assert_not_called()

    def test_resume_keeps_prior_manifest_and_records_new_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, target = Path(tmp) / "prior", Path(tmp) / "rerun"
            sealed = source / "scorecard/cold"
            sealed.mkdir(parents=True)
            (sealed / "manifest.jsonl").write_text("sealed\n")
            binary = source / "zpq"
            binary.write_bytes(b"binary")
            (source / "perf-spec.json").write_text(json.dumps({"binary": str(binary), "control": None}))
            (source / "suite-filter.json").write_text(json.dumps({
                "suites_requested": ["clickbench"], "suites": [{"suite": "clickbench", "status": "ran"}]
            }))
            ctx = verify.dummy_ctx()
            ctx.run_dir = target
            book = summary.Summary(target, {"tier": "perf", "sha": "abc", "rerun_of": "prior"})
            args = argparse.Namespace(harness="unused", profile="cold", resume=str(source), plan_only=False,
                                      suites=None, cases=None, reps=3, engines="zpq,duckdb20,polars")
            with patch.object(perf, "check_harness", return_value=None), patch.object(perf, "_run", return_value=7):
                perf.run_perf(ctx, book, args)
            record = book.data["stages"][0]
            self.assertEqual(record["sealed"], str(sealed))
            self.assertEqual(record["status"], "fail")
            self.assertEqual(record["resumed_from"], str(source))
            self.assertEqual((sealed / "manifest.jsonl").read_text(), "sealed\n")


class ShipBarTest(unittest.TestCase):
    def test_met_when_majority_and_no_trap(self):
        rows = []
        # One cell of five is 1.1x behind: 20% is the bar's inclusive edge, and 4/5 is a majority.
        for query, zpq in (("a", 80), ("b", 90), ("c", 80), ("d", 90), ("e", 110)):
            rows += [cell("zpq", zpq, query), cell("duckdb20", 100, query), cell("polars", 100, query)]
        result = shipbar.evaluate(rows)
        self.assertTrue(result["met"])
        self.assertIn("SHIP BAR (majority vs each, <=20% behind, none >1.5x): MET\n", shipbar.render(result))

    def test_trap_over_1_5_misses(self):
        rows = []
        for query, zpq in (("a", 80), ("b", 90), ("c", 160)):
            rows += [cell("zpq", zpq, query), cell("duckdb20", 100, query), cell("polars", 100, query)]
        self.assertFalse(shipbar.evaluate(rows)["met"])

    def test_control_does_not_flip_the_bar(self):
        rows = []
        for query, zpq in (("a", 80), ("b", 90), ("c", 80), ("d", 90), ("e", 110)):
            rows += [
                cell("zpq", zpq, query),
                cell("duckdb20", 100, query),
                cell("polars", 100, query),
                cell("zpq_prev", 50, query),
            ]
        result = shipbar.evaluate(rows)
        self.assertTrue(result["met"])
        self.assertGreater(result["control"]["geomean"], 1)
        self.assertIn("not part of the ship bar", shipbar.render(result))


if __name__ == "__main__":
    unittest.main()
