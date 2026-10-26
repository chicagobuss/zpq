# zpq-check

`zig build check -Doptimize=ReleaseSafe` installs `zig-out/bin/zpq-check`, the correctness runner. zpq runs
in-process; the DuckDB 2.0 CLI is the oracle; PyArrow is the strict reader of every file zpq writes
(`readback.py`, which prints what it read and holds no comparison policy). `compare.zig` is the one comparison
policy: NULLs, exact integers and DECIMALs, float tolerance, NaN and infinity tokens, booleans, temporals, and
decimal means rounded half away from zero.

```sh
zpq-check --cases tools/check/cases/agg.json [--cases ...] [--only ID] [--no-cache]
```

Oracle answers are cached under `~/.cache/zpq-check/oracle`, keyed by the DuckDB binary's hash, `TZ`, and the SQL
with every file it names replaced by the hash of its bytes. An entry whose stored key differs, or that is
unreadable, is a miss. `--no-cache` asks DuckDB every time.

## Case files

```json
{
  "fixtures": [{"name": "main", "copy": "COPY (...) TO '{path}' (FORMAT PARQUET)"},
               {"name": "u", "path": "ci/fixtures/parquet/x.parquet"},
               {"recipe": "group_stream", "files": ["group_stream"]},
               {"script": "tools/check/prepare_wire.py", "files": ["flba", "floats"]},
               {"name": "coded", "glob": "coded-*.parquet"}],
  "schema": ["id", "amt"],
  "jobs": [1, 16],
  "axes": {"product": [{"jobs": 1, "force": {"ZPQ_DENSE": "0"}}]},
  "cases": [...]
}
```

A case names a fixture (`main` by default) and a query: `sql` (with `{path}` and `{fixture:NAME}`), or `query`
(`aggregate`, `filter`, `group_by`, `column_order`, `columns`, `select`, `codec`). `emit` says what zpq produces:
`answer` (the CLI's JSON, the default), `answer_parquet`, `rows` (JSONL), `rows_parquet`, `copy` (a re-encoded
file), `values` (integer aggregates as `c,s`) or `mixed_orders`. `jobs`, `max_memory`, `trust_stats`, `scan_all`
and `force` (environment overrides) apply at every point.

`axes` is a list of points (`jobs`, `max_memory` with `""` for none, `force`), or the name of a file preset; with
none, the file's `jobs` are the points. Every point must produce identical bytes, or the same refusal and message.

Then exactly one expectation, checked at the first point:

- `oracle`: `{"compare": MODE, "sql": ...}` plus the mode's fields (`where`, `mean`, `epoch`, `pred`, `py_agg`,
  `names`, `types`, `orders`, `row_count`, `duck_select`). The modes are in `verdict.zig`.
- `refuse`: `{"error": KIND, "diag": SUBSTRING, "binder": true}`, the declared refusal of this exact query.
- `same`: byte parity only, with the reason no independent oracle applies.
- `safe`: refusal kinds that are acceptable answers, still identical at every point.

`typed: {"compare": "product" | "parquet", "jobs": N}` also writes the typed answer and checks it against the JSON.

Empty, limited (`--only`, `--limit`) and failed runs never exit 0.
