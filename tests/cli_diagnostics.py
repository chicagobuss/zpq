"""A failed SQL input's borrowed path must remain valid until CLI diagnostics are emitted."""
import json
import subprocess
import sys

cli = sys.argv[1] if len(sys.argv) > 1 else "zig-out/bin/zpq"
for length in (32, 5000, 17000):
    path = "x" * length + ".missing.parquet"
    p = subprocess.run([cli, "query", "--query", f"SELECT * FROM '{path}'", "--summary=json"],
                       capture_output=True, text=True, timeout=30)
    assert p.returncode in (1, 2), (length, p.returncode, p.stderr[-1000:])
    error = json.loads(p.stderr)
    code = "OpenFailed" if length == 32 else "PathTooLong"
    assert error["code"] == code, (length, error)
    if length < 16000:
        assert path in error["message"], (length, error)
    else:
        assert error["message"] == code, (length, error)
    assert p.stdout == "", (length, p.stdout)
print("CLI failed-input path lifetime and diagnostic fallback checks passed")
