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
  `names`, `types`, `orders`, `row_count`, `duck_select`, `ranks`). The modes are in `verdict.zig`. `tied_rows`
  takes rows in any order where the query does not fix one and any tie at a `LIMIT` cut, but each printed row must
  occur in DuckDB's whole answer at least as often as it is printed, and the `ranks` columns (the ORDER BY
  positions) must match row by row. `written_sql` checks a written file against a query: names, types (an integer
  at another width compares exactly) and the multiset of rows.
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

`zpq-check compare` judges other engines' answers with the same modes, for `bench/`: one JSON request per stdin
line, `{"fixture", "answer", "parquet", "spec"}` with `spec` a case's oracle expectation, and one
`{"ok", "reason"}` line back. It keeps file hashes for the oracle cache between requests, and `--threads N` gives
DuckDB N threads (part of every cache key). `text_numbers` in a spec reads a JSON string in a numeric column as
its number: DuckDB prints HUGEINT that way; zpq never may.

## Tiers

The gates are build.zig steps; CI and the justfile call them.

| Tier | Command | Stages |
|---|---|---|
| edit | `zig build check -Dtier=edit [-Dtest-filter=X]` | core unit tests (ReleaseSafe) |
| commit | `zig build check -Dtier=commit` | fixtures, core unit tests, the runner's self-test, the bench's rules, every commit-tier case file against the cached oracle |
| full | `zig build check -Dtier=full [-Dshard=NAME]` | fixtures, every unit-test binary in Debug, Lambda integration, the self-tests, every case uncached, the corpus, decode errors and hardware parity, ClickBench, path parity, OS contracts, libzpq |
| fleet | `tools/check/fleet.sh` | the full tier's shards (tools/check/shards.json) over the fleet, plus the architecture check |

Each stage runs through `zpq-check stage` under its timeout (a hang fails the stage, its process group killed) and
leaves `zig-out/receipts/NAME.log` and `NAME.json`: the SHA and whether the tree was clean, the host, compiler and
oracle, the case files' tree hash, the fixtures, and the counts its runner printed. `zpq-check verdict` passes a tier
only when every stage has a receipt for HEAD from a clean tree, its log still hashes as recorded, it ran something,
failed nothing and skipped nothing. Unit-test skips are named and never allowed: `tools/check/fixtures.sh` puts
apache/parquet-testing at `tools/check/corpus.pin` and generates the unit tests' large fixtures (`tools/zpq-gen
bench`, `nested`) before anything runs. Contracts skip only by a named need (`--privileged`).

Every test root imports the `tests.gen.zig` build.zig writes beside it, listing the files under it that declare tests,
so no test file goes unrooted.

The architecture check runs every case answers-only (`--answers-only --reuse-fixtures --manifest`) on each fleet host
from a shipped bundle, and `zpq-check manifest-diff` requires the host's answers to equal this host's, case for case.
Placement: granted-mbp runs only with `--host` or `--release`, and a host that is timing (an active `zpq-*` unit, a
held `/mnt/fast/zpq-out/timing.lock` or bench lock `~/.cache/zpq-bench.lock`, a running full gate) is skipped.

The release tier is the GitHub native matrix: each runner builds the CLI as it ships and runs `cases/release.json`
through it, and on Linux the shipped Lambda through the fake runtime (`zig build integration-bin`).
