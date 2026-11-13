# Text formats

CSV and JSON tables are read the way their SerDe reads them, as closely as `read_csv` and `read_json` allow. Where we
differ from the SerDe, this page says so.

- [LazySimpleSerDe](#lazysimpleserde)
- [OpenCSVSerde](#opencsvserde)
- [JSON SerDes](#json-serdes)
- [Rules shared by all text tables](#rules-shared-by-all-text-tables)
- [Writing text files](#writing-text-files)
- [Not supported](#not-supported)

## LazySimpleSerDe

| Property | Default | Meaning |
|---|---|---|
| `field.delim`, else `serialization.format` | `\001` | The field delimiter. A number from -128 to 127 is a byte code, so `'1'` is `\001` and `'9'` a tab. |
| `serialization.null.format` | `\N` | The text that reads as NULL. |

Fields are not quoted.

### Timestamps

`timestamp.formats` is not supported the way the SerDe defines it, which is Joda-Time patterns where the longest match
wins. We treat its patterns as hints instead. A `timestamp` field is trimmed and tried with `try_strptime` against
each pattern that strptime can express (`y`, `M`, `d`, `H`, `h`, `m`, `s`, `SSS`, `a`, `E` and quoted text), and the
first match in the list wins. Failing that, it is read as milliseconds since the epoch if `millis` is listed, and
failing that, with the usual cast. As in the SerDe, a field shorter than 8 characters is NULL.

Patterns are separated by `,`, and a `\,` is a comma inside a pattern. We ignore patterns with a time zone, with a
fraction other than `SSS`, or with no year. Without the property, timestamps are read with the usual cast only.

### Escaping with `escape.delim`

A one-byte `escape.delim` makes the next character part of the field, a delimiter included, so `a\,b` is `a,b` and
`\\` is `\`. A `\N` is still NULL. An escaped line break continues the field, where Hive ends the record there.
Writes escape both the escape character and the delimiter in each value.

## OpenCSVSerde

| Property | Default |
|---|---|
| `separatorChar` | `,` |
| `quoteChar` | `"` |
| `escapeChar` | `\`, when the table records none ([see below](#escapes)) |

An empty field is an empty string; this SerDe has no NULL.

A boolean field is true when it reads `true` in any case, and false otherwise. A timestamp field is a number of
milliseconds since the epoch, so `1579059880000` is `2020-01-15 03:44:40`, and a date field a number of days since
the epoch, so `18276` is `2020-01-15`. Text such as `2020-01-15 03:44:40` or `2020-01-15` does not parse.

### Escapes

Without an `escapeChar`, a field is read with `\` as the escape, as the SerDe reads it. The `\` is dropped and the `\`
or quote character after it is kept, in a quoted field or not, so `\` is an empty field, `a\x` is `ax` and `a\\b` is
`a\b`. Outside quotes it does not escape the separator, so `a\,b` is the two fields `a` and `b`. Doubled quotes are
one quote either way.

Because `read_csv` splits the fields first, two cases differ from the SerDe:

- `\"` inside a quoted field ends the quote, so the field and the following lines run together until the next quote.
  The SerDe reads it as `"`.
- An unquoted field starting with `\\` keeps one `\`. The SerDe keeps none.

An `escapeChar` that the table records, such as the `"` of the tables we create, is applied by `read_csv` instead. The
SerDe reads that `"` as a `\` too.

### Line breaks inside quoted fields

The SerDe reads a file line by line, through TextInputFormat, so a line break inside a quoted field splits the record
into two broken rows, and a quote that is never closed, as in `2,"abc`, makes only that field NULL.

We read CSV as `read_csv` does, so a quoted field continues across line breaks. A quoted line break is read as one
value, and the lines after an unclosed quote are read into that one value without an error.

There is one catch. A quoted line break fails a scan of at most twice `threads` files, since such a scan reads each
file with several threads:

```
The parallel scanner does not support null_padding in conjunction with quoted new lines
```

With more files than that, DuckDB already reads one thread per file. `SET hive_csv_parallel = false` reads one thread
per file in every scan.

## JSON SerDes

Both the Hive and the OpenX JSON SerDe are read with `read_json`, one object per line.

With OpenX's `ignore.malformed.json` or `use.null.for.invalid.data` set to `true`, we read the files with
`read_json(ignore_errors := true)`. A line that is not valid JSON becomes a row of NULLs, and a value that does not
fit its column becomes NULL. Either property enables both behaviours.

The other JSON SerDe properties that change how the files are read are not applied. That covers OpenX's `mapping.*`
and `dots.in.keys` and Hive's `timestamp.formats`. The read succeeds and we log a warning, which
`CALL enable_logging(level := 'warning')` shows.

JSON keys are matched to the columns ignoring ASCII case, as both SerDes do, at every nesting level. An OpenX table
with `case.insensitive=false` matches them exactly. Keys of one object that differ only in case are duplicate keys.

## Rules shared by all text tables

Short rows are padded with NULL and extra fields are ignored. A value that does not parse as its column type is NULL,
except in OpenCSVSerde tables, where it fails the query unless `use.null.for.invalid.data` is `true`.

Fields are converted with `TRY_CAST`, or with `CAST` in an OpenCSVSerde table without that property, and OpenCSVSerde
booleans, timestamps and dates as described above. This differs from Hive in a few details: surrounding spaces are
ignored, `'1.5'` in an `int` column is 2 where Hive gives 1, and blank lines are skipped where Hive gives a row of
NULLs.

`skip.header.line.count` lines are skipped at the start of every file.

A line may be up to `hive_csv_max_line_size` bytes long, 8000000 by default, and a longer line fails the query; Hive
itself has no limit. Up to the default, the reader's buffers and the way a file is split between threads are
unchanged. A larger limit makes each thread read at least that many bytes.

As in Hive, a table property overrides the SerDe property of the same name, and keys are case-sensitive. Line counts
and byte codes are DuckDB integer casts, so `'1.5'` is 2.

Partitions are read with the table's SerDe and SerDe properties. A scan that reaches a partition whose own
InputFormat, SerDe or SerDe properties would read its files differently, such as a CSV partition of a Parquet table
or one with another delimiter, fails with an error naming the partition rather than reading it as the table's format.
`hive_scan()` uses plain `read_csv` rules.

## Writing text files

Writes produce files that read back unchanged, which is why we refuse a value we could not read again:

- LazySimpleSerDe values are unquoted, and refused if they hold the delimiter or a line break, or equal the NULL
  string.
- OpenCSVSerde values are all quoted, and refused if they hold a line break. Timestamps and dates are written as
  milliseconds, truncated, and days since the epoch, and an infinite value is refused. A `\` is doubled in a table
  without an `escapeChar`, since such a table reads with the `\` escape.

NULL is written as an empty field. That reads back as an empty string in a string column and as false in a boolean
column. Elsewhere it reads back as NULL only with `use.null.for.invalid.data`; without it, reading the value fails.

## Not supported

Neither for reads nor for writes:

- footer lines (`skip.footer.line.count`);
- header lines in JsonSerDe tables;
- nested columns;
- multi-byte quote or escape characters.

For LazySimpleSerDe, also `serialization.escape.crlf`, an `escape.delim` that is the delimiter or longer than one
byte, a non-UTF-8 `serialization.encoding`, `serialization.last.column.takes.rest`, NUL or line-break delimiters, and
a delimiter that appears in the NULL string. Writes need a single-byte delimiter.
