# Byte values in text output

JSON text is UTF-8, and a Parquet BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY need not be. Escaping arbitrary bytes into a
JSON string loses them: replacing each byte that is not UTF-8 with U+FFFD prints `ff` and `fe` alike. So zpq picks a
column's JSON form from its annotation, and every form round-trips to the stored bytes.

| Column | JSON value |
|---|---|
| STRING / UTF8, ENUM, JSON (logical or legacy annotation) | the text, as a JSON string |
| UUID (FLBA(16)) | lowercase canonical `"00010203-0405-0607-0809-0a0b0c0dfeff"` |
| legacy INTERVAL (FLBA(12)) | `{"months":m,"days":d,"millis":ms}`, each an unsigned 32-bit little-endian field |
| any other BYTE_ARRAY / FLBA: unannotated, BSON, GEOMETRY, GEOGRAPHY, an annotation zpq does not interpret | base64, standard alphabet, padded |

An annotation zpq does not interpret falls back to the legacy annotation beside it, as the format allows, so a future
logical type over a UTF8 column still prints as text. DECIMAL and FLOAT16 over FLBA are numbers, not bytes.

The form applies wherever a value leaves as text: rows (`--format ndjson` / `json`, SQL), GROUP BY keys, min/max
results, libzpq's SQL NDJSON and the Lambda's `agg` response. It is chosen from the column a value passes through
unchanged: a bare column, an alias of one, a GROUP BY key that is a column, or a min/max of one. A computed string
(`CASE ... THEN 'x'`, concatenation) is text. Filters, ordering and min/max compare the stored bytes, unsigned,
whatever the printed form. `--explain` prints names and counts, never values; `zpq inspect` prints statistics bounds
that are not valid STRING text as hex.

A STRING, ENUM or JSON value that is not valid UTF-8 makes the file invalid. zpq fails with an error naming the column
(exit 1) rather than print a replacement; streamed rows before it may already be on stdout, as with any read error.
A UUID that is not 16 bytes or an INTERVAL that is not 12 is refused the same way. Parquet output (`-o`) writes the
bytes and annotation unchanged and is not affected.
