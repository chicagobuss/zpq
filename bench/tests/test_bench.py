"""Bench rules that must never drift: stalls, quarantine, the denominator, verdicts, hashing, rank checks.

    .venv/bin/python -m unittest discover -s bench/tests
"""

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from bench import engines, lane, replay, score, stalls, suites

MS = 1_000_000
PSI_ZERO = {"host": {"io_some": 0, "io_full": 0, "memory_some": 0, "memory_full": 0},
            "scope": {"io_some": 0, "io_full": 0, "memory_some": 0, "memory_full": 0}}


def rec(case, engine, rep, ms, rnd=0, status="ok", scope_io_us=0, host_full_us=0, read=100, psi=True):
    p = None
    if psi:
        p = json.loads(json.dumps(PSI_ZERO))
        p["scope"]["io_some"], p["host"]["io_full"] = scope_io_us, host_full_us
    return {"case": case, "engine": engine, "rep": rep, "round": rnd, "status": status, "elapsed_ns": ms * MS,
            "psi": p, "read_bytes": read}


def cohort(case, walls, rnd=0, **kw):
    return [rec(case, e, i + 1, w, rnd, **kw) for e in ("zpq", "duckdb20", "polars", "zpq_prev")
            for i, w in enumerate(walls)]


class Stalls(unittest.TestCase):
    def test_signature_needs_host_evidence(self):
        fastest = rec("c", "zpq", 1, 30)
        # +1 s over a 30 ms rep, the scope stalled for all of it, the same bytes: the device got slower.
        self.assertIsNotNone(stalls.stall_evidence(rec("c", "zpq", 2, 1040, scope_io_us=1_000_000), fastest))
        # The same wall with no stall in the scope is the engine's own slowness: scored.
        self.assertIsNone(stalls.stall_evidence(rec("c", "zpq", 2, 1040), fastest))
        # More bytes read means more I/O by the engine, never the host's fault.
        self.assertIsNone(stalls.stall_evidence(rec("c", "zpq", 2, 1040, scope_io_us=1_000_000, read=500), fastest))

    def test_threshold_is_one_second_and_twice_the_fastest(self):
        fastest = rec("c", "zpq", 1, 3000)
        self.assertIsNone(stalls.stall_evidence(rec("c", "zpq", 2, 8900, scope_io_us=9_000_000), fastest))
        self.assertIsNotNone(stalls.stall_evidence(rec("c", "zpq", 2, 9100, scope_io_us=9_000_000), fastest))

    def test_host_io_full_stalls_whatever_the_wall(self):
        self.assertIsNotNone(stalls.stall_evidence(rec("c", "zpq", 2, 31, host_full_us=1_500_000),
                                                   rec("c", "zpq", 1, 30)))

    def test_without_psi_the_signature_alone_stalls(self):
        fastest = rec("c", "zpq", 1, 30, psi=False)
        self.assertIn("no PSI", stalls.stall_evidence(rec("c", "zpq", 2, 1040, psi=False), fastest))

    def test_disturbed_reps_and_warmups(self):
        records = cohort("a", [30, 31, 29]) + cohort("b", [30, 31, 29])
        records.append({**rec("b", "zpq", 0, 5000, psi=False)})  # a slow warm-up is never judged
        self.assertEqual(stalls.disturbed(records, 0), set())
        records.append(rec("a", "polars", 4, 30, status="disturbed"))
        self.assertEqual(stalls.disturbed(records, 0), {"a"})


class Score(unittest.TestCase):
    def header(self, psi=True):
        return {"lane": "cold", "host": "h", "sha": "0" * 40, "reps": 3, "psi": psi}

    def test_quarantine_keeps_the_denominator_and_two_percent_is_incomplete(self):
        planned = [f"s/d/{i}" for i in range(100)]
        records = [r for c in planned for r in cohort(c, [30, 31, 29])]
        s = score.score(self.header(), records, planned)
        self.assertEqual((s["verdict"], s["ship_bar"]["cases"]), ("COMPLETE", 100))
        for i, c in enumerate(planned[:3]):
            for rnd in range(3):  # stalled in the first run and both re-runs
                records += cohort(c, [30, 2000, 29], rnd=rnd, psi=False)
        s = score.score(self.header(), records, planned)
        self.assertEqual(len(s["quarantined"]), 3)
        self.assertEqual(s["ship_bar"]["cases"], 100)
        self.assertEqual(s["verdict"], "INCOMPLETE")

    def test_a_rerun_that_comes_back_clean_is_scored(self):
        planned = ["s/d/0"]
        records = cohort("s/d/0", [30, 2000, 29], psi=False) + cohort("s/d/0", [30, 31, 29], rnd=1)
        s = score.score(self.header(), records, planned)
        self.assertEqual((s["verdict"], s["quarantined"]), ("COMPLETE", {}))
        self.assertEqual(s["cells"]["s/d/0"]["zpq"]["median_ns"], 30 * MS)

    def test_a_stall_without_a_rerun_yet_is_pending_not_scored(self):
        s = score.score(self.header(), cohort("s/d/0", [30, 2000, 29], psi=False), ["s/d/0"])
        self.assertEqual((s["verdict"], s["pending_rerun"]), ("INCOMPLETE", ["s/d/0"]))

    def test_no_psi_is_uncertified(self):
        planned = ["s/d/0"]
        self.assertEqual(score.score(self.header(psi=False), cohort("s/d/0", [30, 31, 29]), planned)["verdict"],
                         "UNCERTIFIED")

    def test_missing_cells_are_incomplete(self):
        self.assertEqual(score.score(self.header(), cohort("s/d/0", [30, 31, 29]), ["s/d/0", "s/d/1"])["verdict"],
                         "INCOMPLETE")

    def test_a_zpq_failure_is_a_failed_cell_and_a_rival_failure_a_zpq_win(self):
        planned = ["s/d/0", "s/d/1", "s/d/2"]
        records = [r for c in planned for r in cohort(c, [30, 31, 29])]
        for r in records:
            if r["case"] == "s/d/0" and r["engine"] == "zpq":
                r.update(status="failed", reason="sql parse error")
            if r["case"] == "s/d/1" and r["engine"] == "polars":
                r.update(status="wrong")
        bar = score.score(self.header(), records, planned)["ship_bar"]
        self.assertEqual(bar["zpq_failed"], ["s/d/0"])
        self.assertEqual((bar["behind"], bar["over_1_5x"], bar["met"]), (1, 1, False))  # ties are not behind
        self.assertEqual(bar["beats"]["polars"], 1)  # s/d/1: polars was wrong, zpq answered

    def test_majority_and_twenty_percent(self):
        planned = [f"s/d/{i}" for i in range(10)]
        records = []
        for i, c in enumerate(planned):
            for e, ms in (("zpq", 100 if i < 8 else 120), ("duckdb20", 110), ("polars", 110)):
                records += [rec(c, e, k, ms) for k in (1, 2, 3)]
        self.assertTrue(score.score(self.header(), records, planned)["ship_bar"]["met"])
        for r in records:
            if r["engine"] == "zpq" and r["case"] == "s/d/0":
                r["elapsed_ns"] = 120 * MS
        self.assertFalse(score.score(self.header(), records, planned)["ship_bar"]["met"])


class Answers(unittest.TestCase):
    def test_hash_keeps_order_only_where_the_query_orders(self):
        with tempfile.TemporaryDirectory() as d:
            a, b = Path(d, "a"), Path(d, "b")
            a.write_text('[{"k":1,"v":2.50},{"k":2,"v":1}]')
            b.write_text('{"k":2,"v":1}\n{"k":1,"v":2.50}\n')
            self.assertEqual(lane.canonical_hash(a, False), lane.canonical_hash(b, False))
            self.assertNotEqual(lane.canonical_hash(a, True), lane.canonical_hash(b, True))
            b.write_text('[{"k":1,"v":2.5},{"k":2,"v":1}]')  # 2.5 is not 2.50 as text: judged again, not trusted
            self.assertNotEqual(lane.canonical_hash(a, True), lane.canonical_hash(b, True))

    def test_ranks_are_answer_positions(self):
        self.assertEqual(suites.ranks("SELECT a, count(*) AS c FROM t GROUP BY a ORDER BY c DESC LIMIT 10"), [1])
        self.assertEqual(suites.ranks("SELECT UserID, extract(minute FROM toDateTime(EventTime)) AS m, COUNT(*) "
                                      "FROM hits GROUP BY 1, 2 ORDER BY COUNT(*) DESC LIMIT 10"), [2])
        self.assertEqual(suites.ranks("SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC"), [2])
        self.assertEqual(suites.ranks("SELECT SearchPhrase FROM hits ORDER BY EventTime LIMIT 10"), [])
        self.assertEqual(suites.ranks("SELECT a, b FROM t ORDER BY 2, a"), [1, 0])

    def test_a_session_splits_into_its_runs(self):
        out = "__RUN_0\n[{\"x\":1}]\nRun Time (s): real 0.004 user 0.1 sys 0.0\n__RUN_1\n[{\"x\":1}]\n" \
              "Run Time (s): real 0.002 user 0.1 sys 0.0\n"
        with tempfile.TemporaryDirectory() as d:
            answers = [str(Path(d, f"a{i}")) for i in range(2)]
            self.assertEqual(engines.split_session(out, answers), [4 * MS, 2 * MS])
            self.assertEqual(Path(answers[1]).read_text(), '[{"x":1}]')
            with self.assertRaises(ValueError):
                engines.split_session(out, answers + [str(Path(d, "a2"))])


class Replay(unittest.TestCase):
    def test_old_runs_are_quarantined_not_scored(self):
        with tempfile.TemporaryDirectory() as d:
            old = Path(d, "granite-abc-cold")
            old.mkdir()
            cases = [{"suite": "s", "dataset": "d", "query_id": str(i)} for i in range(2)]
            Path(d, "granite-abc-cold.config.json").write_text(json.dumps(
                {"reps": 3, "cases": cases, "engines": {"zpq": {}}, "candidates": {"zpq": {"revision": "f" * 40}}}))
            rows = [{"type": "attempt", "suite": "s", "dataset": "d", "query_id": str(i), "engine": e, "rep": k,
                     "status": "ok", "validation": "ok", "wall_ns": (2000 if (i, k) == (1, 2) else 30) * MS}
                    for i in range(2) for e in ("zpq", "duckdb20", "polars") for k in (1, 2, 3)]
            Path(old, "results.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
            out = replay.convert(old, Path(d, "out"))
            head = json.loads(Path(out, "header.json").read_text())
            records = [json.loads(x) for x in Path(out, "journal.jsonl").read_text().splitlines()]
            s = score.score(head, records, json.loads(Path(out, "planned.json").read_text()), head["max_round"])
            self.assertEqual(list(s["quarantined"]), ["s/d/1"])
            self.assertNotIn("s/d/1", [c for _, c in s["ship_bar"]["worst"]])
            self.assertEqual(s["verdict"], "INCOMPLETE")


@unittest.skipUnless(engines.POLARS.is_file(), "no Polars 2 interpreter")
class PolarsAnswer(unittest.TestCase):
    def test_a_derived_minute_named_event_time_is_not_a_timestamp(self):
        code = ("import sys, polars as pl; sys.path.insert(0, 'bench'); import polars_run as p; "
                "f = pl.DataFrame({'EventTime': pl.Series([28], dtype=pl.Int8), 'n': [3]}); "
                "g = pl.DataFrame({'EventTime': pl.Series([1373896800], dtype=pl.Int64)}).with_columns("
                "(pl.col('EventTime') * 1_000_000).cast(pl.Datetime('us'))); "
                "print(p.answer(pl, f, {})); print(p.answer(pl, g, {}))")
        out = subprocess.check_output([str(engines.POLARS), "-c", code], cwd=engines.ROOT, text=True).split()
        self.assertEqual(out, ['[{"EventTime":28,"n":3}]', '[{"EventTime":1373896800}]'])


if __name__ == "__main__":
    unittest.main()
