#!/usr/bin/env python3
"""The messy CSV corpus (REPORT-opus §5) as tools/check fixtures: one file per hazard, plus messy_all.csv, which
combines the ones a reader should survive. Ported from probes/csv/gen_messy.py; the bytes are the same.

Each file is written byte by byte (no csv module), so its hazard is exactly what its name says. tools/check/cases/
csv.json says what zpq must do with each: write the exact values (DuckDB's reading of the text, cast to the exact
type) or refuse at a line and byte.

argv: the paths to write, each `f<i>-<name>.csv` with <name> a hazard below. Deterministic.
"""
import sys
from pathlib import Path

LATE = 250_000  # past every engine's default sniff sample (DuckDB 20,480 rows; Polars 100; PyArrow first 1 MiB block)
PAST = 400_000  # past zpq's 4 MiB sample as well


def write(out, name, data, cases, expect):
    # `out` maps a hazard's name to the path tools/check asked for; one it did not ask for is not written.
    path = out.get(name.removesuffix(".csv"))
    if path is not None:
        Path(path).write_bytes(data)


def lines(rows, nl=b"\n"):
    return nl.join(rows) + nl


def main(out):
    cases = {}

    # Quoting: delimiter, doubled quotes, quoted empty vs unquoted empty, LF and CRLF inside quotes.
    write(out, "quoting.csv", lines([
        b'id,s',
        b'1,"a,b"',
        b'2,"say ""hi"""',
        b'3,""',
        b'4,',
        b'5,"line1\nline2"',
        b'6,"crlf1\r\ncrlf2"',
        b'7,plain',
    ]), cases, {"rows": 7, "cells": [
        [0, "s", "a,b"], [1, "s", 'say "hi"'], [2, "s", ""], [3, "s", None], [4, "s", "line1\nline2"],
        [5, "s", "crlf1\r\ncrlf2"]],
        "note": 'quoted "" is an empty string, an unquoted empty field is NULL; quoted newlines are data'})

    # CRLF everywhere, and a last line with no terminator.
    write(out, "crlf.csv", b"a,b\r\n1,x\r\n2,y\r\n3,z", cases,
          {"rows": 3, "cells": [[2, "b", "z"], [0, "b", "x"]], "note": "no \\r may leak into the last column"})

    # Mixed LF / CRLF line endings.
    write(out, "mixed_eol.csv", b"a,b\n1,x\r\n2,y\n3,z\r\n", cases,
          {"rows": 3, "cells": [[0, "b", "x"], [1, "b", "y"], [2, "b", "z"]]})

    # Backslash-escaped quotes (MySQL/Postgres-text style): not RFC 4180; ambiguous without a dialect option.
    write(out, "backslash.csv", lines([b'id,s', b'1,"a \\"q\\" b"', b'2,"c:\\\\dir"']), cases,
          {"rows": 2, "cells": [], "note": "RFC 4180 has no backslash escape; a reader must not guess silently"})

    # NULL spellings: only the empty unquoted field is NULL by default; the rest are strings unless configured.
    write(out, "nulls.csv", lines([b'i,s', b'1,', b',x', b'3,NULL', b'4,NA', b'5,\\N', b'6,""', b'7,null']), cases,
          {"rows": 7, "cells": [[0, "s", None], [1, "i", None], [2, "s", "NULL"], [3, "s", "NA"],
                                [4, "s", "\\N"], [5, "s", ""], [6, "s", "null"]]})

    # Header hazards.
    write(out, "no_header.csv", lines([b'1,2', b'3,4']), cases,
          {"rows": 2, "note": "all-numeric first row: a sniffer should infer no header"})
    write(out, "dup_header.csv", lines([b'a,a,,b', b'1,2,3,4']), cases,
          {"rows": 1, "note": "duplicate and empty names must be made unique, never silently dropped"})
    write(out, "header_only.csv", b"a,b,c\n", cases, {"rows": 0})
    write(out, "empty.csv", b"", cases, {"rows": 0, "note": "zero bytes: a clean refusal or an empty result"})

    # Late type breaks, past the sniff sample.
    def late(name, header, good, bad, bad_cell):
        rows = [header] + [good(i) for i in range(LATE + 1000)]
        rows[1 + LATE] = bad
        write(out, name, lines(rows), cases, {"rows": LATE + 1000, "cells": [bad_cell],
                                              "note": "the late value must survive exactly or the read must fail"})
    late("late_float.csv", b"id,x", lambda i: b"%d,%d" % (i, i), b"%d,3.5" % LATE, [LATE, "x", "3.5"])
    late("late_overflow.csv", b"id,x", lambda i: b"%d,%d" % (i, i), b"%d,99999999999999999999" % LATE,
         [LATE, "x", "99999999999999999999"])
    late("late_text.csv", b"id,x", lambda i: b"%d,%d" % (i, i), b"%d,abc" % LATE, [LATE, "x", "abc"])
    late("late_baddate.csv", b"id,d", lambda i: b"%d,2024-01-%02d" % (i, i % 28 + 1), b"%d,2024-02-30" % LATE,
         [LATE, "d", "2024-02-30"])
    late("late_bool.csv", b"id,b", lambda i: b"%d,%s" % (i, b"true" if i % 2 else b"false"), b"%d,maybe" % LATE,
         [LATE, "b", "maybe"])
    # Past zpq's own 4 MiB type sample too, so the write must restart (or refuse under --csv-strict).
    rows = [b"id,x"] + [b"%d,%d" % (i, i) for i in range(PAST + 1000)]
    rows[1 + PAST] = b"%d,3.5" % PAST
    write(out, "late_past_sample.csv", lines(rows), cases, {})
    late("late_wide_decimal.csv", b"id,x", lambda i: b"%d,%d.25" % (i, i),
         b"%d,12345678901234567.89" % LATE, [LATE, "x", "12345678901234567.89"])

    # Values whose text a numeric type would change.
    write(out, "lossy_numbers.csv", lines([
        b'zip,amount,big,code',
        b'02134,0.1,12345678901234567.89,1e5',
        b'00501,1.10,98765432109876543.21,-0',
        b'10001,2.000,1.5,NaN',
        b'94110,3,2.5,inf',
    ]), cases, {"rows": 4, "cells": [[0, "zip", "02134"], [1, "amount", "1.10"], [0, "big", "12345678901234567.89"]],
                "note": "leading zeros are text; a DOUBLE silently rounds 17+ significant digits"})

    # Dates, times and time zones.
    write(out, "temporal.csv", lines([
        b'd,ts,tstz,us',
        b'2024-01-02,2024-01-02 03:04:05,2024-01-02T03:04:05Z,01/02/2024',
        b'2024-12-31,2024-12-31 23:59:59.123456789,2024-12-31T23:59:59+02:00,12/31/2024',
        b'1900-01-01,1900-01-01 00:00:00,1900-01-01T00:00:00-05:30,01/01/1900',
    ]), cases, {"rows": 3, "cells": [[1, "ts", "2024-12-31 23:59:59.123456789"], [1, "tstz", "2024-12-31T23:59:59+02:00"]],
                "note": "nanosecond text into a microsecond type truncates; offsets must convert or stay text"})

    # Padded numbers.
    write(out, "whitespace.csv", lines([b'a,b', b' 42 ,x', b'7, y ']), cases,
          {"rows": 2, "cells": [[1, "b", " y "]], "note": "string padding is data"})

    # Encodings: BOM, invalid UTF-8 (Latin-1 bytes, as in ClickBench hits).
    write(out, "bom.csv", b"\xef\xbb\xbfname,v\n\xc3\xa9t\xc3\xa9,1\n", cases,
          {"rows": 1, "cells": [[0, "v", "1"]], "note": "the first column is `name`, not `\\ufeffname`"})
    write(out, "latin1.csv", lines([b'id,s', b'1,caf\xe9', b'2,ok']), cases,
          {"rows": 2, "note": "invalid UTF-8: refuse, or write BYTE_ARRAY without the STRING annotation"})

    # Huge field: 20 MiB quoted text with embedded newlines (DuckDB's default max_line_size is 2 MB).
    big = (b"x" * 1023 + b"\n") * (20 * 1024)
    write(out, "huge_field.csv", b'id,s\n1,"' + big + b'"\n2,small\n', cases,
          {"rows": 2, "cells": [[1, "s", "small"]], "note": "a 20 MiB field must round-trip or be refused"})

    # Malformed rows.
    write(out, "short_row.csv", lines([b'a,b,c', b'1,2,3', b'4,5', b'7,8,9']), cases,
          {"rows": 3, "note": "too few fields: refuse, or NULL-pad only when asked"})
    write(out, "long_row.csv", lines([b'a,b,c', b'1,2,3', b'4,5,6,7', b'7,8,9']), cases,
          {"rows": 3, "note": "too many fields: refuse; never drop the extra value silently"})
    write(out, "unterminated.csv", b'a,b\n1,x\n2,"open\n3,y\n', cases,
          {"rows": None, "note": "a quote open at EOF: refuse"})
    write(out, "stray_quote.csv", lines([b'a,b', b'1,ab"c', b'2,d']), cases,
          {"rows": 2, "cells": [[0, "b", 'ab"c']], "note": "a quote inside an unquoted field: lenient readers keep it"})

    # Everything survivable at once, at size: BOM, CRLF, quotes, embedded newlines, NULL vs "", late breaks,
    # leading zeros and a 3 MiB field.
    rows = [b"\xef\xbb\xbfid,zip,qty,price,day,note,flag"]
    n = LATE + 5000
    for i in range(n):
        note = b'""' if i % 7 == 0 else (b'' if i % 11 == 0 else b'"r%d, ""q"" \r\nline2"' % i)
        rows.append(b"%d,%05d,%d,%d.%02d,2024-03-%02d,%s,%s" % (
            i, i % 100000, i % 1000, i % 5000, i % 100, i % 28 + 1, note, b"t" if i % 2 else b"f"))
    rows[1 + LATE] = b'%d,%05d,7.5,1e400,2024-02-30,"%s",maybe' % (LATE, LATE % 100000, b"y" * (3 << 20))
    write(out, "messy_all.csv", lines(rows, b"\r\n"), cases, {
        "rows": n, "cells": [[7, "note", ""], [11, "note", None], [1, "note", 'r1, "q" \r\nline2'],
                             [123, "zip", "00123"], [LATE, "qty", "7.5"], [LATE, "price", "1e400"],
                             [LATE, "day", "2024-02-30"], [LATE, "flag", "maybe"]],
        "note": "every hazard a reader should survive, late breaks past the sniff sample"})


if __name__ == "__main__":
    # tools/check names each file `f<i>-<name>.csv`.
    main({Path(p).name.split("-", 1)[1].removesuffix(".csv"): p for p in sys.argv[1:]})
