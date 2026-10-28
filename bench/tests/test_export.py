"""Saved-run export must retain unfavorable observations and must never invoke a benchmark."""

import json
import tempfile
import unittest
from pathlib import Path

from bench import export, requests


class Export(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.request = {"schema": "zpq-bench-request/v1", "id": "sample", "requester": "operator",
                        "host": "host", "purpose": "compare candidates", "targets": ["s/d/1"], "controls": [],
                        "lanes": ["cold"], "candidate": {"sha": "a" * 40, "binary_sha256": "a" * 64},
                        "baseline": {"sha": "b" * 40, "binary_sha256": "b" * 64},
                        "expected_engines": ["zpq", "base"], "correctness_receipt": None,
                        "runs": [{"id": "main", "path": str(self.source), "lane": "cold"}]}
        self.request_path = self.root / "packet.json"

    def write(self, path, value, lines=False):
        Path(path).write_bytes(b"".join(requests.encode(x).replace(b"\n", b"") + b"\n" for x in value)
                               if lines else requests.encode(value))

    def save_request(self):
        self.write(self.request_path, self.request)
        return self.request_path

    def row(self, engine="zpq", rep=1, status="ok", case="s/d/1", **extra):
        return {"case": case, "engine": engine, "rep": rep, "round": 0, "status": status,
                "validation": "ok" if status == "ok" else "unchecked", "elapsed_ns": (10 + rep) * 1_000_000,
                "wall_ns": (10 + rep) * 1_000_000, "read_bytes": 1, "psi": None, **extra}

    def native(self, rows=None, planned=None):
        header = {"schema": "zpq-bench-v1", "host": "host", "lane": "cold", "sha": "f" * 40,
                  "reps": 2, "psi": True, "engines": {"zpq": {"sha256": "a" * 64},
                                                         "base": {"sha256": "b" * 64}}}
        self.write(self.source / "header.json", header)
        self.write(self.source / "planned.json", planned or ["s/d/1"])
        rows = rows if rows is not None else [self.row(e, r) for e in ("zpq", "base") for r in (1, 2)]
        self.write(self.source / "journal.jsonl", rows, True)

    def legacy(self, root=None, ms=10):
        root = root or self.source
        root.mkdir(exist_ok=True)
        candidates = {e: {"revision": self.request[k]["sha"], "sha256": self.request[k]["binary_sha256"]}
                      for e, k in (("zpq", "candidate"), ("base", "baseline"))}
        cfg = {"profile": "cold", "reps": 2, "engines": {"zpq": {}, "base": {}}, "candidates": candidates,
               "cases": [{"suite": "s", "dataset": "d", "query_id": "1", "engines": ["zpq", "base"]}]}
        header = {"schema": "zpq-scorecard-v1", "config": cfg, "sources": {"oracle.py": "d" * 64}}
        self.write(root / "manifest.jsonl", [header], True)
        rows = [{**self.row(e, r), "suite": "s", "dataset": "d", "query_id": "1", "wall_ns": ms * 1_000_000,
                 "artifact_sha256": "c" * 64} for e in ("zpq", "base") for r in (1, 2)]
        self.write(root / "results.jsonl", rows, True)

    def result(self):
        return export.make_export(self.save_request())

    def test_native_complete_does_not_promote_canonical_hash_to_raw_identity(self):
        self.native([self.row(e, r, answer_sha256="c" * 64) for e in ("zpq", "base") for r in (1, 2)])
        _, report, cells, audit = self.result()
        self.assertEqual(cells[0]["engines"]["zpq"]["median_ms"], 11.5)
        self.assertIsNone(cells[0]["byte_identical"])
        self.assertEqual(len(audit), 4)
        self.assertIsNone(report["method_compliance"])

    def test_result_contract_names_time_method_environment_and_unknown_checker(self):
        self.native([self.row(e, r, at_ns=1_700_000_000_000_000_000 + r * 1_000_000)
                     for e in ("zpq", "base") for r in (1, 2)])
        report = self.result()[1]
        self.assertTrue(report["start_utc"].endswith("+00:00"))
        self.assertLess(report["start_utc"], report["end_utc"])
        self.assertIn("method", report)
        self.assertIn("environment", report)
        self.assertIsNone(report["checker_sha256"])

    def test_legacy_exact_pair_and_source_module_pins(self):
        self.legacy()
        _, report, cells, _ = self.result()
        self.assertTrue(cells[0]["byte_identical"])
        self.assertIsNone(cells[0]["oracle_byte_identical"])
        self.assertEqual(report["sources"][0]["contract"]["sources"]["oracle.py"], "d" * 64)

    def test_legacy_round_is_repetition_order_not_native_replacement_cohort(self):
        self.legacy()
        path = self.source / "results.jsonl"
        rows = requests.read(path, True)
        for row in rows:
            row["round"] = row["rep"]
        self.write(path, rows, True)
        self.assertEqual(self.result()[2][0]["engines"]["zpq"]["n"], 2)

    def test_failures_never_gain_a_median(self):
        for status in ("wrong", "failed", "timeout"):
            with self.subTest(status=status):
                self.native([self.row("zpq", 1), self.row("zpq", 2, status)])
                cell = self.result()[2][0]
                self.assertEqual(cell["engines"]["zpq"]["state"], "failed")
                self.assertIsNone(cell["engines"]["zpq"]["median_ms"])

    def test_missing_cells_and_rivals_remain_planned(self):
        self.native([self.row(rep=r) for r in (1, 2)], ["s/d/1", "s/d/2"])
        cells = self.result()[2]
        self.assertEqual(len(cells), 2)
        self.assertEqual(cells[0]["engines"]["base"]["state"], "pending")
        self.assertEqual(cells[1]["engines"]["zpq"]["n"], 0)

    def test_warmups_and_environment_exclusions_survive_audit(self):
        self.legacy()
        path = self.source / "results.jsonl"
        rows = requests.read(path, True)
        rows += [{**rows[0], "rep": 0}, {**rows[0], "rep": 3, "env_validity": False}]
        self.write(path, rows, True)
        _, _, cells, audit = self.result()
        self.assertEqual(cells[0]["engines"]["zpq"]["n"], 2)
        self.assertEqual(len(audit), 6)
        self.assertEqual(audit[-2]["disposition"], "warmup")
        self.assertEqual(audit[-1]["disposition"], "excluded-or-incomplete")

    def test_whole_case_confirmation_requires_an_explicit_choice(self):
        self.legacy()
        other = self.root / "confirmation"
        self.legacy(other, ms=30)
        self.request["runs"].append({"id": "confirmation", "path": str(other), "lane": "cold"})
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            self.result()
        self.request["selection"] = {"cold": {"s/d/1": "confirmation"}}
        _, _, cells, audit = self.result()
        self.assertEqual({e["median_ms"] for e in cells[0]["engines"].values()}, {30})
        self.assertEqual(sum(x["disposition"] == "replaced-cohort" for x in audit), 4)

    def test_unknown_selection_and_schema_refuse(self):
        self.native()
        self.request["selection"] = {"cold": {"s/d/2": "main"}}
        with self.assertRaisesRegex(ValueError, "unknown case"):
            self.result()
        self.request.pop("selection")
        header = requests.read(self.source / "header.json")
        header["schema"] = "unknown"
        self.write(self.source / "header.json", header)
        with self.assertRaisesRegex(ValueError, "schema"):
            self.result()

    def test_source_binary_and_revision_mismatch_refuse(self):
        self.legacy()
        for key in ("sha", "binary_sha256"):
            saved = self.request["candidate"][key]
            self.request["candidate"][key] = "f" * len(saved)
            with self.assertRaisesRegex(ValueError, "mismatch"):
                self.result()
            self.request["candidate"][key] = saved

    def test_stored_legacy_metric_is_labeled_and_missing_rival_is_not_recertified(self):
        self.native()
        self.request["expected_engines"].append("rival")
        summary = self.root / "stored.json"
        self.write(summary, [{"suite": "s", "dataset": "d", "query_id": "1", "engine": "rival", "lane": "cold",
                              "legacy_median_ns": 20_000_000, "checked": 3, "run_id": "old-run"}])
        self.request["stored_summaries"] = [str(summary)]
        engine = self.result()[2][0]["engines"]["rival"]
        self.assertEqual((engine["origin"], engine["metric_field"], engine["median_ms"]),
                         ("stored", "legacy_median_ns", 20))

    def test_export_retry_is_identical_and_sources_unchanged(self):
        self.native()
        self.save_request()
        before = {p: p.read_bytes() for p in self.source.iterdir()}
        out = self.root / "out"
        export.export(self.request_path, out)
        first = {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in out.iterdir()}
        export.export(self.request_path, out)
        self.assertEqual(first, {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in out.iterdir()})
        self.assertEqual(before, {p: p.read_bytes() for p in self.source.iterdir()})

    def test_conflicting_output_refuses_before_modifying_package(self):
        self.native()
        export.export(self.save_request(), self.root / "out")
        self.request["purpose"] = "another claim"
        self.save_request()
        existing = {p: p.read_bytes() for p in (self.root / "out").iterdir()}
        with self.assertRaisesRegex(ValueError, "conflicting"):
            export.export(self.request_path, self.root / "out")
        self.assertEqual(existing, {p: p.read_bytes() for p in (self.root / "out").iterdir()})

    def test_source_overlap_and_symlink_outputs_refuse(self):
        self.native()
        with self.assertRaisesRegex(ValueError, "overlaps"):
            export.export(self.save_request(), self.source / "out")
        out = self.root / "out"
        out.mkdir()
        (out / "report.json").symlink_to(self.source / "header.json")
        with self.assertRaisesRegex(ValueError, "conflicting"):
            export.export(self.request_path, out)

    def test_duplicate_repetitions_and_nonfinite_json_refuse(self):
        self.native([self.row(), self.row()])
        with self.assertRaisesRegex(ValueError, "duplicate accepted"):
            self.result()
        path = self.root / "invalid.json"
        for text in ('{"a": 1, "a": 2}', '{"a": NaN}'):
            path.write_text(text)
            with self.assertRaises(ValueError):
                requests.read(path)

    def test_input_plan_deduplicates_shared_objects_and_requires_verification(self):
        digest = "c" * 64
        self.request["controls"] = ["s/d/2"]
        catalog = {"schema": "zpq-corpus-catalog/v1", "objects": [
            {"sha256": digest, "bytes": 10, "source_paths": ["/input"]}], "case_dependencies": [
            {"case": "s/d/1", "objects": [digest]}, {"case": "s/d/2", "objects": [digest]}]}
        inventory = {"host": "host", "inputs": {"data": {"files": [{"path": "/input", "bytes": 10}]}}}
        plan = requests.input_plan(self.request, catalog, inventory)
        self.assertEqual((len(plan["objects"]), plan["required_bytes"], plan["missing_bytes"]), (1, 10, 0))
        self.assertEqual(plan["verification_bytes"], 10)
        self.assertFalse(plan["ready"])
        inventory["inputs"]["data"]["files"][0]["bytes"] = 9
        self.assertEqual(requests.input_plan(self.request, catalog, inventory)["missing_bytes"], 10)
        inventory["inputs"]["data"]["files"] = []
        self.assertEqual(requests.input_plan(self.request, catalog, inventory)["missing_bytes"], 10)

    def test_request_keeps_missing_readiness_evidence_explicit(self):
        packet = requests.packet(self.save_request())
        self.assertIn("correctness_receipt", packet["missing_evidence"])
        self.assertFalse(packet["ready_for_scheduling"])
        self.request["candidate"]["sha"] = "abc"
        with self.assertRaisesRegex(ValueError, "digest"):
            requests.packet(self.save_request())


if __name__ == "__main__":
    unittest.main()
