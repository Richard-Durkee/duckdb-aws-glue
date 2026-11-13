# Embedding the extension (C++)

This page is for engines that link the extension into their own binary. Both features below only work when the
caller and this extension are linked into the same binary.

## Reading a scan's files with a standard reader

An engine can hand some of a Hive scan's files to machines that have neither this extension nor Glue access, and read
them there with a standard DuckDB reader. `functions/hive_reader_call.hpp` gives what that read needs from a bound
scan, of a Glue table or of a `hive_scan()` call, using only core DuckDB types. Neither function calls Glue or needs a
`ClientContext`.

### `HiveScanGetReaderCall(get)`

Returns three things:

- the reader, which is `read_parquet`, `read_csv`, `read_json` or `read_avro`;
- its named parameters;
- the scan's data columns, as expressions over the reader's columns.

The parameters are the ones the scan bound the reader with. For a text table they include its dialect, header lines
and NULL spelling, and `hive_csv_parallel` and `hive_csv_max_line_size` as the planning session had them. For Parquet
and Avro they include the data columns as `schema`. Every reader gets `hive_partitioning := false`.

A column of a SerDe text table is the field read as text, wrapped in `__internal_barrier`, and then converted:

- with `TRY_CAST`;
- with `CAST` in an OpenCSVSerde table without `use.null.for.invalid.data`;
- with `lower(field) = 'true'` for an OpenCSVSerde boolean.

The barrier stops the caller's optimizer from pushing the conversion into `read_csv`, which would convert the text
itself, and strictly. In an OpenCSVSerde table without `escapeChar`, a field that holds a `\` first goes through
`regexp_replace`, which drops the escape.

### `HiveScanGetFilePartitionValues(get, path)`

Returns the values of the partition a listed file belongs to, as constants of the partition columns' types. The path
is the file's path as the scan listed it, or another spelling that DuckDB's `Path` normalizes to it. For example,
`s3://b//x` and `s3://b/./x` both work for `s3://b/x`.

### Putting them together

The caller reads a set of the scan's files with one read per partition, combined with `UNION ALL`:

```sql
SELECT <columns>, <partition values> FROM <reader>([<files>], <parameters>)
```

Both functions return nothing for a scan they do not recognize, and for a scan that reads a column, partition keys
included, as a different type than the table declares. That happens when the optimizer pushed a type into the scan.
`HiveScanGetReaderCall` also returns nothing in two more cases:

- a Parquet table with a `binary` column, whose string columns are decoded file by file;
- a scan that reads with [file credentials](#file-credentials).

### `EXPLAIN (SQL)`

`EXPLAIN (SQL)` renders a Hive scan as these reads, so the SQL it prints reads the scan's files without this
extension. `SET debug_verify_statement = 'explain_sql_strict'` runs a query as that SQL.

The scan lists its files for the rendering. A partition the filters pruned is not read, a filter the partition values
decided is restated over the reads, and a scan without files becomes a relation without rows.

A scan that no reader call can read is rendered as the Glue table it reads, by name. That covers a Parquet table with
a `binary` column, and a query that reads `$path`, `$file_size`, `$file_modified_time` or another virtual column. A
`hive_scan()` call cannot be rendered in that case.

## File credentials

An extension can give the scans of a Glue table their own credentials, such as credentials that a governance service
vends for that table. It does this by registering a `GlueFileCredentialsCallback`
([glue_file_credentials.hpp](../src/include/catalog/glue_file_credentials.hpp)) as an `ExtensionCallback`. This
extension asks the callback each time a scan of a Hive table is bound. The callback is found with `dynamic_cast`, so
an extension loaded as a separate library with `LOAD` is not found.

The callback answers with the name of a secret, typically one it created for the transaction
(`SecretPersistType::TRANSACTION`). Every file the scan lists then names that secret in its options
(`ExtendedOpenFileInfo::SECRET_OPTION`), and DuckDB opens the file with that secret instead of the one that matches
its path. Without a callback, or when the callback returns no name, files are read as usual.

The rules that follow from this:

- Listings are made by path, because DuckDB's globs name no secret. A transaction secret takes part in the path
  lookup, so a listing under its scope uses it. So does every other read by path under its scope in that transaction,
  such as a `read_parquet` of the table's files, or a table without credentials at the same location.
- A query lists a directory once, for every scan of it, with or without credentials. Only the file opens differ.
- A file that names a secret that does not exist, such as a transaction secret after its transaction ended, or a
  secret whose scope does not cover the file, fails to open. It is not read by path instead.
- `glue_cache_listings` does not keep the listings of a scan with credentials beyond the query.
- A scan with credentials serializes the name of its secret. `EXPLAIN (SQL)` renders it as the Glue table, by name.
- `INSERT` into a table whose scans read with credentials, and `glue_repair_table` of such a table, are refused,
  since both list or write its files by path. The check asks the callback's `GetFileSecret`, as a scan of the table
  would.

The tests under `test/sql/file_credentials/` give tables credentials with the test extension `glue_test_credentials`
(`test/extension/glue_test_credentials/`). It must share a binary with this extension, so `extension_config.cmake`
links both into `unittest`.
