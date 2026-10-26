# verify

The gates are `zig build check -Dtier=...` (tools/check/README.md). What remains here is the scorecard and its ship
bar, until it moves in-repo as `bench/`:

```sh
uv pip install --python .venv/bin/python -r tools/verify/requirements.txt
.venv/bin/python tools/verify/verify.py perf --help
```

`perf` runs the scorecard checkout pinned by `tools/verify/HARNESS` (`--harness` selects it) against DuckDB 2.0 and
Polars 2.0 with a `zpq_prev` control, counts only validated answers, and scores the ship bar. Each run writes
`zig-out/verify/<run-id>/summary.json` (override with `--out`) with the host's provenance; a run directory is never
overwritten, and `--resume RUN_DIR` continues a sealed run in a new record. `tools/verify/test_perf.py` checks the run
record, suite selection and ship-bar arithmetic.
