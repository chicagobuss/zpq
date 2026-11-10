import json
import unittest

from bench import score


class SweepTests(unittest.TestCase):
    def test_flagged_cells_from_score_json(self):
        fixture = json.loads('''{"cells": {
          "rival": {"zpq": {"median_ns": 121, "min_ns": 115}, "duckdb20": {"median_ns": 100}},
          "control": {"zpq": {"median_ns": 110, "min_ns": 108},
                      "zpq_prev": {"median_ns": 100, "max_ns": 107}},
          "overlap": {"zpq": {"median_ns": 110, "min_ns": 100},
                      "zpq_prev": {"median_ns": 100, "max_ns": 105}},
          "wrong-rival": {"zpq": {"median_ns": 121, "min_ns": 115}, "polars": {"failed": "wrong"}}
        }}''')
        self.assertEqual(score.confirmations(fixture), ["control", "rival"])
