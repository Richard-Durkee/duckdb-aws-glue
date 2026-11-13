# Embedding the extension (C++)

## Reading a scan's files with a standard reader

An engine that hands some of a Hive scan's files to machines without this extension (and without Glue access) can
read them there with a standard reader. `functions/hive_reader_call.hpp` gives what that read needs from a bound scan,
of a Glue table or of a `hive_scan()` call, using only core DuckDB types:

- `HiveScanGetReaderCall(get)`: the reader (`read_parquet`, `read_csv`, `read_json` or `read_avro`), its named
  parameters and the scan's data columns as expressions over the reader's columns. The parameters are those the scan
  bound the reader with (a text table's dialect, header lines and NULL spelling, `hive_csv_parallel` and
  `hive_csv_max_line_size` as the planning session had them), plus the data columns as `schema` for Parquet and Avro and
  `hive_partitioning := false`. A column of a SerDe text table is the field read as text and converted with `TRY_CAST`
  (`CAST` without `use.null.for.invalid.data` in an OpenCSVSerde table, `lower(field) = 'true'` for an OpenCSVSerde
  boolean), of the field wrapped in `__internal_barrier`, so the caller's optimizer does not push the conversion into
  `read_csv`, which would convert the text itself and strictly. A field of an OpenCSVSerde table without
  `escapeChar` that holds a `\` is first passed through `regexp_replace`, which drops the escape.
- `HiveScanGetFilePartitionValues(get, path)`: the values of the partition a listed file belongs to, as constants of
  the partition columns' types. The path is the file's as the scan listed it, or another spelling of it that DuckDB's
  `Path` normalizes to it (`s3://b//x` or `s3://b/./x` for `s3://b/x`).

The caller reads a set of the scan's files as `SELECT <columns>, <partition values> FROM <reader>([<files>],
<parameters>)`, one such read per partition, `UNION ALL`ed. Neither function calls Glue or needs a `ClientContext`.
Both return nothing for a scan they do not recognize or that reads a column, a partition key included, as another
type than the table declares (a type the optimizer pushed into the scan), and `HiveScanGetReaderCall` also for a
Parquet table with a `binary` column, whose string columns are decoded file by file, and for a scan that reads with
[file credentials](#file-credentials). Both only work when the caller and this extension are linked into the same
binary.

`EXPLAIN (SQL)` renders a Hive scan as these reads, so the SQL it prints reads the scan's files without this extension
(and `SET debug_verify_statement = 'explain_sql_strict'` runs a query as that SQL). The scan lists its files for it:
a partition the filters pruned is not read, a filter the partition values decided is restated over the reads, and a
scan without files is a relation without rows. A scan no reader call can read (a Parquet table with a `binary`
column, or one whose query reads `$path`, `$file_size`, `$file_modified_time` or another virtual column) is rendered
as the Glue table it reads, by name; a `hive_scan()` call then cannot be rendered.

## File credentials

An extension can give the scans of a Glue table credentials of their own, for example credentials a governance
service vends for that table. It registers a `GlueFileCredentialsCallback`
([glue_file_credentials.hpp](../src/include/catalog/glue_file_credentials.hpp)) as an `ExtensionCallback`, which this
extension asks each time a scan of a Hive table is bound. The callback's extension must be linked into the same binary
as this one (it is found with `dynamic_cast`); one `LOAD`ed as a separate library is not found. The callback answers
with the name of a secret, typically one it created for the transaction (`SecretPersistType::TRANSACTION`). Every file
the scan lists then names that secret in its options (`ExtendedOpenFileInfo::SECRET_OPTION`), and DuckDB opens it with
that secret instead of the one that matches its path. Without a callback, or when it returns no name, the files are read as above.

- The listings themselves are made by path: DuckDB's globs name no secret. A secret of the transaction takes part in
  the path lookup, so a listing under its scope is made with it. So does every other read by path under its scope in
  that transaction, e.g. a `read_parquet` of the table's files or a table without credentials at the same location.
- One query lists a directory once, for every scan of it with or without credentials; only the opens differ.
- A file naming a secret that does not exist (e.g. a transaction secret, after the transaction), or whose scope does
  not cover it, fails to open rather than being read by path.
- `glue_cache_listings` does not keep the listings of a scan with credentials beyond the query.
- A scan with credentials serializes the name of its secret; `EXPLAIN (SQL)` renders it as the Glue table by name.
- INSERT into a table whose scans read with credentials, and `glue_repair_table` of one, are refused: they list or
  write its files by path. The check asks the callback's `GetFileSecret`, as a scan of the table would.

The tests under `test/sql/file_credentials/` give tables credentials with the test extension `glue_test_credentials`
(`test/extension/glue_test_credentials/`), which must share a binary with this extension; `extension_config.cmake`
links both into `unittest`.
