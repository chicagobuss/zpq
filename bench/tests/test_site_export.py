"""The site export keeps every unfavorable observation and never invents a median."""

import json
import tempfile
import unittest
from pathlib import Path

from bench import site_export


def row(case, engine, rep, status="ok", ns=1_000_000, **extra):
    r = {"case": case, "engine": engine, "rep": rep, "round": 0, "status": status, "validation": "ok",
         "elapsed_ns": ns, "cpu_ns": ns * 2, "rss_kib": 100, "rc": 0, "rows": 3, "bytes_out": 9, "at_ns": 5 + rep,
         "answer_sha256": "a" * 64}
    r.update(extra)
    return r


class SiteExport(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.run = self.root / "run"
        (self.run / "rechecks" / "one").mkdir(parents=True)
        head = {"lane": "hot-fresh", "sha": "c" * 40, "baseline_sha": "b" * 40,
                "engines": {"zpq": {"version": "zpq 0.5.2", "sha256": "1" * 64},
                            "duckdb20": {"version": "v2.0.0", "sha256": "2" * 64}}}
        (self.run / "header.json").write_text(json.dumps(head))
        rows = [row("s/d/1", "zpq", i, ns=n) for i, n in enumerate((30, 10, 20), 1)]
        rows += [row("s/d/1", "duckdb20", i) for i in (1, 2, 3)]
        rows += [row("s/d/2", "zpq", 1), row("s/d/2", "zpq", 2), row("s/d/2", "zpq", 3, status="wrong", validation="wrong")]
        rows += [row("s/d/3", "zpq", 1), row("s/d/3", "zpq", 2), row("s/d/3", "zpq", 3, status="disturbed",
                                                                       reasons=["busy-before"])]
        (self.run / "journal.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
        (self.run / "rechecks" / "one" / "journal.jsonl").write_text(json.dumps(row("s/d/1", "zpq", 9)) + "\n")

    def export(self, name="out", **kw):
        out = self.root / name
        status = site_export.export_lane(self.run, "run-x", out, now_ns=7, **kw)
        return out, status

    def test_a_median_needs_every_repetition_accepted_and_none_failed(self):
        out, status = self.export()
        cells = {(c["query_id"], c["engine"]): c for c in json.loads((out / "composite.json").read_text())}
        done = cells[("1", "zpq")]
        self.assertEqual((done["median_ns"], done["min_ns"], done["max_ns"], done["checked"]), (20, 10, 30, 3))
        wrong = cells[("2", "zpq")]
        self.assertNotIn("median_ns", wrong)
        self.assertNotIn("incomplete", wrong)
        self.assertEqual((wrong["failures"], wrong["checked"], wrong["attempts"]), (["wrong"], 2, 3))
        short = cells[("3", "zpq")]
        self.assertEqual(short["incomplete"], "2/3 accepted")
        self.assertNotIn("median_ns", short)
        self.assertEqual((status["phase"], status["cells"], status["complete"], status["environment_rejections"]),
                         ("finished-incomplete", 4, 2, 1))
        self.assertEqual(status["deferred"], ["s/d/3/zpq"])

    def test_every_journal_row_is_an_attempt_with_its_cohort_and_the_header_pins(self):
        out, _ = self.export()
        attempts = [json.loads(x) for x in (out / "results.jsonl").read_text().splitlines()]
        self.assertEqual(len(attempts), 12)
        ids = {a["attempt_id"] for a in attempts}
        self.assertEqual(len(ids), 12)
        first = next(a for a in attempts if a["query_id"] == "1" and a["engine"] == "zpq" and a["rep"] == 1)
        self.assertEqual((first["selected"], first["env_validity"], first["mode"], first["source_lane"]),
                         (True, True, "hot", "hot-fresh"))
        self.assertEqual((first["candidate_sha"], first["baseline_sha"]), ("c" * 40, "b" * 40))
        self.assertEqual((first["engine_version"], first["binary_sha256"]), ("zpq 0.5.2", "1" * 64))
        self.assertEqual(first["answer_hash"]["content_sha256"], "a" * 64)
        wrong = next(a for a in attempts if a["status"] == "wrong")
        self.assertFalse(wrong["selected"])
        self.assertTrue(wrong["env_validity"], "a wrong answer is a real observation, not an environment rejection")
        ok_in_failed_cell = next(a for a in attempts if a["query_id"] == "2" and a["rep"] == 1)
        self.assertFalse(ok_in_failed_cell["selected"], "no repetition of a cell without a median is selected")
        self.assertFalse(ok_in_failed_cell["env_validity"])
        disturbed = next(a for a in attempts if a["status"] == "disturbed")
        self.assertFalse(disturbed["env_validity"])
        self.assertEqual(disturbed["environment_reasons"], ["busy-before"])

    def test_rechecks_are_kept_verbatim_and_the_same_run_exports_to_the_same_bytes(self):
        a, _ = self.export("a")
        b, _ = self.export("b")
        confirmations = [json.loads(x) for x in (a / "confirmations.jsonl").read_text().splitlines()]
        self.assertEqual([c["rep"] for c in confirmations], [9])
        for name in ("composite.json", "results.jsonl", "confirmations.jsonl", "status.json"):
            self.assertEqual((a / name).read_bytes(), (b / name).read_bytes(), name)

    def test_a_cold_lane_and_explicit_pins_override_the_header(self):
        out, _ = self.export(lane="cold", candidate_sha="d" * 40, baseline_sha="e" * 40, reps=2)
        attempt = json.loads((out / "results.jsonl").read_text().splitlines()[0])
        self.assertEqual((attempt["mode"], attempt["candidate_sha"], attempt["baseline_sha"], attempt["expected_reps"]),
                         ("cold", "d" * 40, "e" * 40, 2))
        cells = {(c["query_id"], c["engine"]): c for c in json.loads((out / "composite.json").read_text())}
        self.assertIn("incomplete", cells[("1", "zpq")], "three accepted repetitions are not the two expected")
        self.assertNotEqual(attempt["cell_id"], json.loads(
            (self.export("hot")[0] / "results.jsonl").read_text().splitlines()[0])["cell_id"])


if __name__ == "__main__":
    unittest.main()
