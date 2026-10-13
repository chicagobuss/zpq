"""Readers for the CLI row stream and opt-in diagnostic stream."""
import json


def read_rows(text, **kwargs):
    return [json.loads(line, **kwargs) for line in text.splitlines() if line.strip()]


def read_one(text, **kwargs):
    rows = read_rows(text, **kwargs)
    assert len(rows) == 1, rows
    return rows[0]


def read_summary(text):
    rows = [row for row in read_rows(text) if row.get("type") == "summary"]
    assert len(rows) == 1, rows
    return rows[0]
