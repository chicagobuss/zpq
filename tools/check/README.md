# zpq-check

`zig build check -Doptimize=ReleaseSafe` installs `zig-out/bin/zpq-check`, the correctness runner. zpq runs
in-process; the DuckDB 2.0 CLI is the oracle; PyArrow is the strict reader of every file zpq writes
(`readback.py`, which prints what it read and holds no comparison policy). `compare.zig` is the one comparison
policy: NULLs, exact integers and DECIMALs, float tolerance, NaN and infinity tokens, booleans, temporals, and
decimal means rounded half away from zero.

```sh
zpq-check --cases tools/check/cases/agg.json [--cases ...] [--only ID] [--no-cache]
```

DuckDB runs with `-no-init`, `threads=1`, and only `TZ` (UTC unless set), `LC_ALL=C` and `PATH` in its
environment. Answers are cached under `~/.cache/zpq-check/oracle`, keyed by that configuration, the DuckDB
binary's hash, the SQL verbatim, and the hash of every file a quoted literal names; a named file that cannot be
hashed leaves the question uncached. An entry is used only when its stored key and its answer's digest match and
the answer decodes; anything else asks DuckDB again. `--no-cache` asks DuckDB every time.

## Case files

```json
{
  "fixtures": [{"name": "main", "copy": "COPY (...) TO '{path}' (FORMAT PARQUET)"},
               {"name": "u", "path": "ci/fixtures/parquet/x.parquet"},
               {"recipe": "group_stream", "files": ["group_stream"]},
               {"script": "tools/check/prepare_wire.py", "files": ["flba", "floats"]},
               {"name": "coded", "glob": "coded-*.parquet"},
               {"name": "copy", "zpq": ["query", "{fixture:main}", "-o", "{path}", "--codec", "zstd"]}],
  "schema": ["id", "amt"],
  "jobs": [1, 16],
  "axes": {"product": [{"jobs": 1, "force": {"ZPQ_DENSE": "0"}}]},
  "cases": [...]
}
```

A case names a fixture (`main` by default, or a file path as `input`) and a query: `sql` (with `{path}` and
`{fixture:NAME}`), `query` (`aggregate`, `filter`, `group_by`, `column_order`, `columns`, `select`, `codec`), or
`cli`, the zpq CLI's arguments, one process per point (`{out}` and `{out.SUFFIX}` name the file it writes; `force`
reaches it as environment). `same_as` gives a second command whose outcome must be byte-identical. `emit` says what zpq produces:
`answer` (the CLI's JSON, the default), `answer_parquet`, `rows` (JSONL), `rows_parquet`, `copy` (a re-encoded
file), `values` (integer aggregates as `c,s`) or `mixed_orders`. `jobs`, `max_memory`, `trust_stats`, `scan_all`
and `force` (environment overrides) apply at every point.

`axes` is a list of points (`jobs`, `max_memory` with `""` for none, `force`), or the name of a file preset; with
none, the file's `jobs` are the points. Every point must produce identical bytes, or the same refusal and message.

Then exactly one expectation, checked at the first point (a case with none, two, or an empty one is rejected
when the file loads, as is a file with no cases):

- `oracle`: `{"compare": MODE, "sql": ...}` plus the mode's fields (`where`, `mean`, `epoch`, `pred`, `py_agg`,
  `names`, `types`, `orders`, `row_count`, `duck_select`). The modes are in `verdict.zig`.
- `refuse`: `{"error": KIND, "diag": SUBSTRING, "exit": N, "no_output": true, "binder_sql": SQL}`, the declared
  refusal of this exact query (a CLI refusal's kind is its JSON `code`, its `(Name)`, else `exit N`);
  with `binder_sql`, DuckDB must refuse that SQL with a Binder Error too. A file-level `refuse` map declares
  refusals by case id for cells a matrix generates.
- `same`: byte parity only, with the reason no independent oracle applies.
- `safe`: refusal kinds that are acceptable answers, still identical at every point.

`typed: {"compare": "product" | "parquet", "jobs": N}` also writes the typed answer and checks it against the JSON;
it may stand beside `oracle`.

Some modes judge a written file or an explanation instead: `written_as` (DuckDB reads it as `sql` reads the
inputs; `types` adds PyArrow's), `readers_agree`, `footer` and `plan` (readback.py's view or the `--explain` JSON
`holds` a declared subset: `{"$len": N}`, `{"$list": [...]}`, `{"$all": X}`), `page_index`, `bloom` (filters checked
from the spec's hashing alone), `summary`, and `golden`/`golden_rows` (`--bless` rewrites goldens; review the diff).
`stderr_has` and `max_scans` (plan events in a `--profile` trace) apply to any mode.

Matrices expand parametric cases: every combination of one entry per dimension instantiates `case` (`${VAR}`
interpolates; a list element exactly `${VAR}` splices a list).

Row modes compare canonical values (floats to six significant digits); `agg`, `stats_agg` and `usame` compare
values within the scalar tolerance; `dense`, `stats_select` and `product` compare raw values exactly, a float by
its exact binary value; corpus modes compare each float at its column's width.

Empty, limited (`--only`, `--limit`) and failed runs never exit 0.
