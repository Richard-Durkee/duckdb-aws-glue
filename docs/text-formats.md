# Text formats

Text tables, which use LazySimpleSerDe, OpenCSVSerde or one of the JSON SerDes, are read the way their SerDe reads
them, as closely as `read_csv` and `read_json` allow. This page describes each SerDe's properties and the cases where
the extension reads or writes differently from the SerDe.

- [LazySimpleSerDe](#lazysimpleserde)
- [OpenCSVSerde](#opencsvserde)
- [Rules for all CSV tables](#rules-for-all-csv-tables)
- [JSON SerDes](#json-serdes)
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
wins. Its patterns are treated as hints instead. A `timestamp` field is trimmed and tried with `try_strptime` against
each pattern that strptime can express (`y`, `M`, `d`, `H`, `h`, `m`, `s`, `SSS`, `a`, `E` and quoted text), and the
first match in the list wins. Failing that, the field is read as milliseconds since the epoch if `millis` is listed,
and failing that, with the usual cast. As in the SerDe, a field shorter than 8 characters is NULL.

Patterns are separated by `,`, and `\,` is a comma inside a pattern. Patterns with a time zone, with a fraction other
than `SSS`, or with no year are ignored. Without the property, timestamps are read with the usual cast only.

### Escaping with `escape.delim`

A one-byte `escape.delim` makes the next character part of the field, a delimiter included, so `a\,b` is `a,b` and
`\\` is `\`. A `\N` is still NULL. An escaped line break continues the field, whereas Hive ends the record there.

## OpenCSVSerde

| Property | Default |
|---|---|
| `separatorChar` | `,` |
| `quoteChar` | `"` |
| `escapeChar` | none; fields are then read with a `\` escape ([see below](#escapes)) |

An empty field is an empty string; this SerDe has no NULL.

A boolean field is true when it reads `true` in any case, and false otherwise. A timestamp field is a number of
milliseconds since the epoch, so `1579059880000` is `2020-01-15 03:44:40`. A date field is a number of days since the
epoch, so `18276` is `2020-01-15`. Text such as `2020-01-15 03:44:40` or `2020-01-15` does not parse.

### Escapes

Without an `escapeChar`, a field is read with `\` as the escape, as the SerDe reads it. The only exception is a table
whose separator or quote character is itself `\`. The escape works as follows:

- The `\` is dropped, and the `\` or quote character after it is kept, in a quoted field or not. So `\` is an empty
  field, `a\x` is `ax`, and `a\\b` is `a\b`.
- Outside quotes, `\` does not escape the separator, so `a\,b` is the two fields `a` and `b`.
- Doubled quotes are one quote either way.

Because `read_csv` splits the fields first, two cases differ from the SerDe:

- `\"` inside a quoted field ends the quote, so the field and the following lines run together until the next quote.
  The SerDe reads it as `"`.
- An unquoted field starting with `\\` keeps one `\`. The SerDe keeps none.

When the table does record an `escapeChar`, `read_csv` applies it. The tables this extension creates record `"`, and
the SerDe reads such a `"` escape the same way it reads `\`.

### Line breaks and unclosed quotes

The SerDe reads a file line by line, through TextInputFormat. A line break inside a quoted field therefore splits the
record into two broken rows, and a quote that is never closed, as in `2,"abc`, makes only that field NULL.

The extension reads CSV as `read_csv` does, so a quoted field continues across line breaks. A quoted line break is
read as part of one value, and the lines after an unclosed quote are read into that one value without an error.

A quoted line break fails a scan of at most twice `threads` files, because such a scan reads each file with several
threads:

```
The parallel scanner does not support null_padding in conjunction with quoted new lines
```

With more files than that, DuckDB reads one thread per file. `SET hive_csv_parallel = false` reads one thread per
file in every scan.

## Rules for all CSV tables

These rules apply to both LazySimpleSerDe and OpenCSVSerde tables.

Short rows are padded with NULL and extra fields are ignored. A value that does not parse as its column type is NULL,
except in OpenCSVSerde tables, where it fails the query unless `use.null.for.invalid.data` is `true`.

Fields are converted with `TRY_CAST`, or with `CAST` in an OpenCSVSerde table without that property. OpenCSVSerde
booleans, timestamps and dates are converted as described above. The conversion differs from Hive's in a few details:

- surrounding spaces are ignored;
- `'1.5'` in an `int` column is 2, where Hive gives 1;
- blank lines are skipped, where Hive gives a row of NULLs.

`skip.header.line.count` lines are skipped at the start of every file.

A line may be up to `hive_csv_max_line_size` bytes long, 8000000 by default, and a longer line fails the query; Hive
itself has no limit. Up to the default, the reader's buffers and the way a file is split between threads are
unchanged. A larger limit makes each thread read at least that many bytes.

As in Hive, a table property overrides the SerDe property of the same name, and keys are case-sensitive. Line counts
and byte codes are DuckDB integer casts, so `'1.5'` is 2. `hive_scan()` uses plain `read_csv` rules instead.

## JSON SerDes

Both the Hive and the OpenX JSON SerDe are read with `read_json`, one object per line.

With OpenX's `ignore.malformed.json` or `use.null.for.invalid.data` set to `true`, the files are read with
`read_json(ignore_errors := true)`. A line that is not valid JSON becomes a row of NULLs, and a value that does not
fit its column becomes NULL. Either property enables both behaviors.

The other JSON SerDe properties that change how the files are read are not applied. These are OpenX's `mapping.*` and
`dots.in.keys`, and Hive's `timestamp.formats`. The read succeeds and a warning is logged, which
`CALL enable_logging(level := 'warning')` shows.

JSON keys are matched to the columns ignoring ASCII case, as both SerDes do, at every nesting level. An OpenX table
with `case.insensitive=false` matches them exactly. Keys of one object that differ only in case are duplicate keys.

## Writing text files

Writes produce files that read back with the same values, with the exception of NULL in OpenCSVSerde tables. A value
that a field cannot hold is refused:

- **LazySimpleSerDe** values are written unquoted. A value is refused if it holds a line break or equals the NULL
  string. Without `escape.delim`, a value that holds the delimiter is refused too; with it, the escape character and
  the delimiter are escaped in each value instead.
- **OpenCSVSerde** values are all quoted, and a value is refused if it holds a line break. Timestamps are written as
  milliseconds since the epoch, truncated, and dates as days since the epoch; an infinite value is refused. In a table
  without an `escapeChar`, which reads with the `\` escape, a `\` is written doubled.

In an OpenCSVSerde table, NULL is written as an empty field. That reads back as an empty string in a string column
and as false in a boolean column. In other columns it reads back as NULL only with `use.null.for.invalid.data`, and
otherwise reading it fails.

## Not supported

These are not supported, for reads or for writes:

- footer lines (`skip.footer.line.count`);
- header lines in JsonSerDe tables;
- nested columns;
- multi-byte quote or escape characters.

For LazySimpleSerDe tables, these are not supported either: `serialization.escape.crlf`, an `escape.delim` that is
the delimiter or longer than one byte, a non-UTF-8 `serialization.encoding`, `serialization.last.column.takes.rest`,
NUL or line-break delimiters, and a delimiter that appears in the NULL string. Writes need a single-byte delimiter.
