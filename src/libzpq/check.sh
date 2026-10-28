#!/usr/bin/env bash
# Run the existing libzpq stage, adapting its one hard-coded /tmp C fixture to the assigned scratch directory.
set -euo pipefail
: "${TMPDIR:?set TMPDIR to the assigned output scratch directory}"
scratch=$(mktemp -d "$TMPDIR/libzpq-stage-XXXXXX")
trap 'rm -rf "$scratch"' EXIT
python3 - "$scratch" <<'PY'
import json
from pathlib import Path
import sys
scratch = Path(sys.argv[1]).resolve()
original = Path('tests/libzpq/test_abi.c').read_text()
needle = '"/tmp/libzpq-test-XXXXXX"'
assert original.count(needle) == 1
source = scratch / 'test_abi.c'
source.write_text(original.replace(needle, json.dumps(str(scratch / 'libzpq-test-XXXXXX'))))
runner = Path('tests/libzpq/run.sh').read_text()
assert runner.count('tests/libzpq/test_abi.c') == 2
(scratch / 'run.sh').write_text(runner.replace('tests/libzpq/test_abi.c', '"' + str(source) + '"'))
PY
bash "$scratch/run.sh"
