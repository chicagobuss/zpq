# Byte values in text output

JSON text is UTF-8, and a Parquet BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY need not be. Escaping arbitrary bytes into a
JSON string loses them: replacing each byte that is not UTF-8 with U+FFFD prints `ff` and `fe` alike. So zpq picks a
value's JSON form from its column's annotation and, for unannotated bytes, from the value itself. Every form
round-trips to the stored bytes, and no two values print alike: bytes that are not text print as an object, and a
JSON string never equals an object.

| Column | JSON value |
|---|---|
| STRING / UTF8, ENUM, JSON (logical or legacy annotation) | the text, as a JSON string; a value that is not UTF-8 is an error |
| no annotation | the text, as a JSON string, when the bytes are UTF-8; otherwise `{"base64":"..."}` |
| BSON, GEOMETRY, GEOGRAPHY (WKB) | always `{"base64":"..."}` |
| UUID (FLBA(16)) | lowercase canonical `"00010203-0405-0607-0809-0a0b0c0dfeff"` |
| legacy INTERVAL (FLBA(12)) | `{"months":m,"days":d,"millis":ms}`, each an unsigned 32-bit little-endian field |

Base64 is the standard alphabet, padded. Many writers store text without a STRING annotation (ClickBench's
`hits.parquet` among them), which is why unannotated bytes that are UTF-8 print as text. A reader that needs the
bytes back takes a string's UTF-8 encoding, or an object's decoded `base64`.

An annotation zpq does not interpret falls back to the legacy annotation beside it, as the format allows; with none,
its values are unannotated bytes. DECIMAL and FLOAT16 over FLBA are numbers, not bytes.

The form applies wherever a value leaves as text: rows (`--format ndjson` / `json` / `csv`, SQL), GROUP BY keys, min/max
results, libzpq's SQL NDJSON and the Lambda's `agg` response. It is chosen from the column a value passes through
unchanged: a bare column, an alias of one, a GROUP BY key that is a column, or a min/max of one. A computed string
(`CASE ... THEN 'x'`, concatenation) is text. Filters, ordering and min/max compare the stored bytes, unsigned,
whatever the printed form. `--explain` prints names and counts, never values; `zpq inspect` prints statistics bounds
that are not valid STRING text as hex.

In CSV (`--format csv`) a value JSON prints as a string is that field's text, a UUID unquoted; one JSON prints as an
object is that object's JSON text, in a quoted field: `"{""base64"":""/wA=""}"`.

A STRING, ENUM or JSON value that is not valid UTF-8 makes the file invalid. zpq fails with an error naming the column
(exit 1) rather than print a replacement; streamed rows before it may already be on stdout, as with any read error.
A UUID that is not 16 bytes or an INTERVAL that is not 12 is refused the same way. Parquet output (`-o`) writes the
bytes and annotation of a value that passes a column through unchanged and is not affected; a computed string, as a
row, a GROUP BY key or a min/max, is written as STRING, as it prints.
