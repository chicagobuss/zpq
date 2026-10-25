"""Adversarial checks of the referee itself. Run after `zig build referee`.

Requires DUCKDB to name the oracle CLI, and a Python with pyarrow.
"""

import argparse
from decimal import Decimal
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import answer_cmp


ROOT = Path(__file__).resolve().parents[2]
DUCKDB = os.environ.get("DUCKDB", "duckdb")


class DecimalTest(unittest.TestCase):
    def test_triangulation_keeps_decimal_digits_and_reserves_tolerance_for_floats(self):
        import triangulate as tri
        output = '{"d":12345678901234567890123456789.01}\n'
        with patch.object(tri.subprocess, "run", return_value=argparse.Namespace(
            returncode=0, stdout=output, stderr=""
        )):
            got = tri.run_zpq_agg("unused", "sum(d) AS d")[0]["d"]
        self.assertEqual(got, Decimal("12345678901234567890123456789.01"))
        decimal_lane = tri._lane(tri.AggItem("sum", "d", "d", tri.pa.decimal128(38, 2)))
        self.assertFalse(tri.compare_values(got, Decimal("12345678901234567890123456789.02"), decimal_lane))
        float_lane = tri._lane(tri.AggItem("sum", "f", "f", tri.pa.float64()))
        self.assertTrue(tri.compare_values(1, 1.00001, float_lane))

    def test_numeric_json_keeps_all_decimal_digits(self):
        output = '[{"d":12345678901234567890123456789.01}]'
        with patch.object(answer_cmp.subprocess, "run", return_value=argparse.Namespace(
            returncode=0, stdout=output, stderr=""
        )):
            row = answer_cmp.duck_json("unused", "SELECT d")[0]
        self.assertEqual(row["d"], Decimal("12345678901234567890123456789.01"))

    def test_decimal_json_and_strings_are_exact(self):
        rows = answer_cmp.duck_json(DUCKDB, "SELECT '12345678901234567890123456789.01'::DECIMAL(38,2) AS d")
        self.assertEqual(answer_cmp.coerce(rows[0]["d"], "DECIMAL(38,2)"),
                         Decimal("12345678901234567890123456789.01"))


class RowAdapterTest(unittest.TestCase):
    def test_unordered_rows_normalize_decimal_scale_before_sorting(self):
        args = argparse.Namespace(zpq="unused")
        got = [{"d": Decimal("1.10"), "k": "b"}, {"d": Decimal("1.1"), "k": "a"}]
        want = [(Decimal("1.1"), "b"), (Decimal("1.10"), "a")]
        with patch.object(answer_cmp, "load_zpq", return_value=got), \
                patch.object(answer_cmp, "duck_rows", return_value=(want, ["d", "k"], ["exact", None])):
            answer_cmp.cmd_usame(args)

    def test_unordered_float_rows_share_the_canonical_bucket_before_pairing(self):
        args = argparse.Namespace(zpq="unused")
        got = [{"v": Decimal("1.0000001"), "k": "b"}, {"v": Decimal("1.0000002"), "k": "a"}]
        want = [(1.0000002, "b"), (1.0000001, "a")]
        with patch.object(answer_cmp, "load_zpq", return_value=got), \
                patch.object(answer_cmp, "duck_rows", return_value=(want, ["v", "k"], [None, None])):
            answer_cmp.cmd_usame(args)

    def test_dense_rows_follow_oracle_column_names(self):
        args = argparse.Namespace(zpq="unused")
        with patch.object(answer_cmp, "load_zpq", return_value=[{"b": 2, "a": 1}]), \
                patch.object(answer_cmp, "duck_rows", return_value=([(1, 2)], ["a", "b"], [None, None])):
            answer_cmp.cmd_dense(args)

    def test_exact_decimal_scale_has_one_canonical_representation(self):
        self.assertEqual(repr(answer_cmp.canon(Decimal("1.10"), "exact")),
                         repr(answer_cmp.canon(Decimal("1.1"), "exact")))


class RefereeTest(unittest.TestCase):
    def run_cases(self, case, records=(), limit=None, jobs=None):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            cases = {
                "fixture_copy": "COPY (SELECT * FROM (VALUES (1), (2), (2), (NULL)) t(id)) "
                                "TO '{path}' (FORMAT PARQUET)",
                "schema_order": ["id"],
                "cases": [case] if case else [],
            }
            if jobs is not None:
                cases["jobs"] = jobs
            path = tmp / "cases.json"
            path.write_text(json.dumps(cases))
            registry = tmp / "known.json"
            registry.write_text(json.dumps({"records": list(records)}))
            argv = [str(ROOT / "zig-out/bin/referee"), "--cases", str(path), "--divergences", str(registry),
                    "--duckdb", DUCKDB, "--pyarrow", sys.executable, "--work", str(tmp / "work")]
            if limit is not None:
                argv.extend(["--limit", str(limit)])
            return subprocess.run(argv, cwd=ROOT, capture_output=True, text=True)

    def test_file_job_defaults_are_counted_and_a_case_can_override_them(self):
        case = {"id": "jobs", "kind": "sql", "sql": "SELECT count(*) AS c, sum(id) AS s FROM '{path}'"}
        for override, total in ((None, 2), ([1], 1)):
            with self.subTest(override=override):
                current = dict(case, **({"jobs": override} if override else {}))
                result = self.run_cases(current, jobs=[1, 4])
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(f"referee: {total}/{total} checks match", result.stderr)

    def test_extra_missing_changed_duplicate_and_null_rows_fail(self):
        for where in ("id = 1", "id IS NOT NULL", "id <> 2", "id IS NULL"):
            with self.subTest(where=where):
                result = self.run_cases({"id": "extra", "where": where})
                self.assertNotEqual(result.returncode, 0, result.stderr)
                self.assertIn("row mismatch", result.stderr)
        result = self.run_cases({"id": "missing", "filter": "id = 1"})
        self.assertNotEqual(result.returncode, 0, result.stderr)
        self.assertIn("row mismatch", result.stderr)
        result = self.run_cases({"id": "equal"})
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_refusal_requires_the_declared_error_kind(self):
        sql = "SELECT count(*) AS c, sum(id) AS s FROM '{path}' WHERE id = id"
        case = {"id": "refuse", "kind": "sql", "sql": sql, "jobs": [1, 4]}
        record = {"id": "refuse", "engine": "zpq", "class": "documented-refusal", "sql": sql,
                  "error_kind": "BadValue"}
        result = self.run_cases(case, [record])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("referee: refusals 2", result.stderr)
        self.assertIn("referee: expected 2", result.stderr)
        result = self.run_cases(case)
        self.assertNotEqual(result.returncode, 0, result.stderr)
        self.assertIn("unexpected refusal BadValue", result.stderr)
        result = self.run_cases(case, [{**record, "error_kind": "BadOperator"}])
        self.assertNotEqual(result.returncode, 0, result.stderr)
        self.assertIn("refused BadValue, expected BadOperator", result.stderr)

    def test_divergence_metadata_cannot_waive_wrong_values(self):
        record = {"id": "wrong", "engine": "duckdb", "class": "stats-order", "reason": "test"}
        result = self.run_cases({"id": "wrong", "where": "id = 1"}, [record])
        self.assertNotEqual(result.returncode, 0, result.stderr)

    def test_empty_and_limited_runs_do_not_pass(self):
        for case, limit in ((None, None), ({"id": "partial"}, 1)):
            with self.subTest(case=case, limit=limit):
                result = self.run_cases(case, limit=limit)
                self.assertNotEqual(result.returncode, 0, result.stderr)

    def test_unknown_case_kind_does_not_fall_back_to_a_write(self):
        result = self.run_cases({"id": "misspelled-kind", "kind": "aggregate-typo"})
        self.assertNotEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
