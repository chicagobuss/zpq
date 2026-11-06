#!/usr/bin/env python3
"""Write src/core/csv/testdata/duckdb_types.tsv: CSV field texts, the type zpq's CSV reader gives each alone, and
DuckDB 2.0's reading of the text as that type, for the property test in src/core/csv/types.zig.

Each line: text TAB zpq type name TAB DuckDB's value (integers and decimals as text, DOUBLE as its shortest
round-trip repr, DATE as days since the epoch, TIMESTAMP as epoch micro- or nanoseconds, BOOLEAN as true/false).
The empty text is written `\\e`. Deterministic (fixed seed); needs the duckdb 2.0.0 Python module.

python3 tools/csv_type_oracle.py > src/core/csv/testdata/duckdb_types.tsv
"""
import random
import sys

import duckdb

rng = random.Random(0xC5F)
con = duckdb.connect()
con.execute("SET TimeZone='UTC'")
assert duckdb.__version__.startswith("2."), duckdb.__version__


def ints():
    for _ in range(600):
        digits = rng.randint(1, 19)
        v = rng.randint(10 ** (digits - 1) if digits > 1 else 0, 10 ** digits - 1)
        if v > 2**63 - 1:
            continue
        s = str(v) if rng.random() < .6 or v == 0 else f"-{v}"
        yield s, "BIGINT", f"CAST('{s}' AS BIGINT)::VARCHAR"
    for s in ("9223372036854775807", "-9223372036854775808", "0"):
        yield s, "BIGINT", f"CAST('{s}' AS BIGINT)::VARCHAR"


def decimals():
    for _ in range(800):
        int_digits = rng.randint(0, 20)
        scale = rng.randint(1, 18)
        if int_digits + scale > 38:
            continue
        ip = "0" if int_digits == 0 else str(rng.randint(10 ** (int_digits - 1), 10 ** int_digits - 1))
        fp = "".join(rng.choice("0123456789") for _ in range(scale))
        if int_digits == 0 and set(fp) == {"0"}:
            fp = fp[:-1] + "1"
        s = f"{ip}.{fp}" if rng.random() < .6 else f"-{ip}.{fp}"
        p = max(1, int_digits + scale)
        yield s, f"DECIMAL({p},{scale})", f"CAST('{s}' AS DECIMAL({max(p, scale)},{scale}))::VARCHAR"
    # Integers too wide for BIGINT are DECIMAL(p,0).
    for digits in range(20, 39):
        s = str(rng.randint(10 ** (digits - 1), 10 ** digits - 1))
        yield s, f"DECIMAL({digits},0)", f"CAST('{s}' AS DECIMAL({digits},0))::VARCHAR"


def normal(text):
    """Digits without leading/trailing zeros and the power of ten of the first: the number a text says."""
    t = text.lstrip("+-").lower()
    mant, _, exp = t.partition("e")
    whole, _, frac = mant.partition(".")
    digits = (whole + frac).lstrip("0")
    if not digits:
        return ("", 0)
    lead = len(whole + frac) - len((whole + frac).lstrip("0"))
    return (digits.rstrip("0"), int(exp or 0) + len(whole) - lead - 1)


def double_kind(s):
    """zpq's rule: a finite double whose shortest repr says the same number, with at most 15 significant digits."""
    v = float(s)
    if v in (float("inf"), float("-inf")) or len(normal(s)[0]) > 15:
        return "VARCHAR"
    return "DOUBLE" if normal(repr(abs(v))) == normal(s) else "VARCHAR"


def doubles():
    for _ in range(900):
        sig = rng.randint(1, 15)
        mant = str(rng.randint(10 ** (sig - 1), 10 ** sig - 1))
        if len(mant) > 1 and rng.random() < .5:
            k = rng.randint(1, len(mant) - 1)
            mant = mant[:k] + "." + mant[k:]
        # Over the whole exponent range: underflow, subnormals, the normal boundary and overflow.
        exp = rng.choice([rng.randint(-345, 310), rng.randint(-330, -300), rng.randint(300, 310)])
        s = f"{'-' if rng.random() < .3 else ''}{mant}{rng.choice('eE')}{exp}"
        kind = double_kind(s)
        yield s, kind, f"CAST('{s}' AS DOUBLE)" if kind == "DOUBLE" else "'-'"
    for s in ("5e-324", "4.9e-324", "1e-400", "2.2250738585072e-308", "2.225073858507201e-308", "1e308",
              "1.79769313486231e308", "1e309", "123456789012345e-334", "1e-320"):
        kind = double_kind(s)
        yield s, kind, f"CAST('{s}' AS DOUBLE)" if kind == "DOUBLE" else "'-'"
    for s in ("NaN", "inf", "-inf", "Infinity", "-0", "-0.0"):
        yield s, "DOUBLE", f"CAST('{s}' AS DOUBLE)"


def dates():
    for _ in range(400):
        y, m = rng.randint(1, 9999), rng.randint(1, 12)
        d = rng.randint(1, 28)
        s = f"{y:04d}-{m:02d}-{d:02d}"
        yield s, "DATE", f"(CAST('{s}' AS DATE) - DATE '1970-01-01')::VARCHAR"
    for s in ("2024-02-29", "2000-02-29", "1970-01-01", "0001-01-01", "9999-12-31"):
        yield s, "DATE", f"(CAST('{s}' AS DATE) - DATE '1970-01-01')::VARCHAR"


NS_MIN = -(1 << 63)
NS_MAX = (1 << 63) - 1


def epoch_ns(y, mo, d, h, mi, sec, frac, offset_min):
    import datetime
    days = (datetime.date(y, mo, d) - datetime.date(1970, 1, 1)).days
    return ((days * 86400 + h * 3600 + mi * 60 + sec - offset_min * 60) * 10**9 + int((frac + "000000000")[:9] or 0))


def timestamps():
    for _ in range(700):
        y = rng.randint(1678, 2261) if rng.random() < .6 else rng.randint(1, 9999)
        mo, d = rng.randint(1, 12), rng.randint(1, 28)
        h, mi, sec = rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59)
        base = f"{y:04d}-{mo:02d}-{d:02d}{rng.choice(' T')}{h:02d}:{mi:02d}:{sec:02d}"
        frac_len = rng.choice([0, 0, 1, 3, 6, 9])
        frac = "".join(rng.choice("0123456789") for _ in range(frac_len))
        if frac_len > 6:
            frac = frac[:8] + rng.choice("123456789")  # sub-microsecond for certain
        s = base + (f".{frac}" if frac_len else "")
        tz = rng.choice(["", "", "Z", "+02:00", "-05:30"])
        nanos = frac_len > 6
        offset = {"": 0, "Z": 0, "+02:00": 120, "-05:30": -330}[tz]
        if nanos and not NS_MIN <= epoch_ns(y, mo, d, h, mi, sec, frac, offset) <= NS_MAX:
            yield s + tz, "VARCHAR", "'-'"  # nanoseconds outside int64 nanoseconds: no timestamp type
            continue
        if tz:
            if nanos:
                continue  # DuckDB has no nanosecond TIMESTAMPTZ
            s += tz
            yield s, "TIMESTAMP WITH TIME ZONE", f"epoch_us(CAST('{s}' AS TIMESTAMPTZ))::VARCHAR"
        elif nanos:
            yield s, "TIMESTAMP_NS", f"epoch_ns(CAST('{s}' AS TIMESTAMP_NS))::VARCHAR"
        else:
            yield s, "TIMESTAMP", f"epoch_us(CAST('{s}' AS TIMESTAMP))::VARCHAR"


def bools():
    for s in ("true", "false", "TRUE", "False", "tRuE"):
        yield s, "BOOLEAN", f"CAST('{s}' AS BOOLEAN)::VARCHAR"


def strings():
    for s in ("007", "+5", " 42", "42 ", "1e400", "abc", "", "2024-02-30", "01/02/2024", "1,5", "0x1F", "-", "."):
        yield s, "VARCHAR", "'-'"


def main():
    out = sys.stdout
    out.write("# Generated by tools/csv_type_oracle.py with DuckDB " + duckdb.__version__ + "; do not edit.\n")
    for gen in (ints, decimals, doubles, dates, timestamps, bools, strings):
        for text, kind, expr in gen():
            v = con.execute(f"SELECT {expr}").fetchone()[0]
            if kind == "DOUBLE":
                v = repr(float(v)).replace("inf", "inf")
            out.write(f"{text if text else chr(92) + 'e'}\t{kind}\t{v}\n")


if __name__ == "__main__":
    main()
