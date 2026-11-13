# Writing

Glue has no transactions, so DDL takes effect as soon as it runs, files are visible as soon as they are written, and
Glue itself rolls nothing back when a statement fails. We clean up after a failed `INSERT` or `CREATE TABLE ... AS` as
well as we can; [When a write fails](#when-a-write-fails) explains what that covers.

- [Schemas](#schemas)
- [Creating tables](#creating-tables)
- [Inserting](#inserting)
- [Changing tables](#changing-tables)
- [Dropping](#dropping)
- [Transactions and caching](#transactions-and-caching)
- [Recording written files](#recording-written-files)

`DELETE`, `UPDATE` and `MERGE INTO` are not supported.

## Schemas

A DuckDB schema is a Glue database.

```sql
CREATE SCHEMA lake.sales WITH (comment = 'Sales data', location = 's3://bucket/sales', owner = 'team-a');
ALTER SCHEMA lake.sales SET (owner = 'team-b');
ALTER SCHEMA lake.sales RESET (owner);
```

| Key | Stored as |
|---|---|
| `comment` | the database Description |
| `location` | the database LocationUri. Default: `<DEFAULT_LOCATION>/<schema>`, or none without `DEFAULT_LOCATION`. |
| anything else | a database parameter (Hive's `DBPROPERTIES`) |

`ALTER SCHEMA ... SET` merges keys into the database and `RESET` removes them; keys are case-insensitive. When the
catalog was attached with `DEFAULT_LOCATION`, that location still decides where new tables go, even over the
database's own.

## Creating tables

```sql
CREATE TABLE lake.sales.orders (id BIGINT, amount DOUBLE, dt VARCHAR)
PARTITIONED BY (dt)
WITH (format = 'parquet', location = 's3://bucket/sales/orders');
```

### Format

| `format` | Creates |
|---|---|
| `'parquet'` (default) | ParquetHiveSerDe |
| `'csv'` | LazySimpleSerDe, `,`-delimited, no header |
| `'json'` | Hive JsonSerDe, one object per line |
| `'avro'` | AvroSerDe |

CSV tables take three more options:

- `delimiter = '|'` sets the field delimiter (`field.delim`).
- `header = true` writes a header line at the start of every file (`skip.header.line.count`).
- `quote = '"'` or `escape = '\'` switches the table to OpenCSVSerde with `separatorChar`, `quoteChar` and
  `escapeChar`. The escape character defaults to the quote character.

### Location

The table goes to the first of these that exists:

1. `location` in `WITH`
2. `<DEFAULT_LOCATION>/<database>/<table>`
3. `<database LocationUri>/<table>`

If none exists, the statement fails.

### Columns

- Partition keys must be plain column names. They become Glue PartitionKeys and are listed last among the table's
  columns.
- Column types are stored as Hive types. A DuckDB type with no Hive equivalent is refused, for example `UBIGINT`,
  `HUGEINT` and `TIMESTAMP_NS`/`_MS`/`_S`. Hive's `timestamp` is DuckDB's `TIMESTAMP`, in microseconds.
- Generated columns, column defaults and collated columns are refused. A `COLLATE` that comes from the query of a
  `CREATE TABLE ... AS` is refused as well.

### Other `WITH` keys

Unknown keys are stored as Glue table parameters. The exception is other engines' names for options we spell
differently; stored as parameters they would silently do nothing, so we refuse them:

| Refused | Use instead |
|---|---|
| `partitioned_by`, `partitioning`, `partition_by`, `partition_columns`, `partition_keys`, `PartitionKeys` | `PARTITIONED BY (...)` |
| `external_location` | `location` |
| `field_delimiter` | `delimiter` |

### Bucketed tables

With `SET glue_create_bucketed_tables = true`, these options create a bucketed table. That is Hive's
`CLUSTERED BY (...) SORTED BY (...) INTO n BUCKETS`:

```sql
CREATE TABLE lake.sales.users (id BIGINT, name VARCHAR)
WITH (BucketColumns = ['id'], NumberOfBuckets = 8, SortColumns = [{'Column': 'id', 'SortOrder': 1}]);
```

`SortOrder` is 1 for ascending and 0 for descending. Bucket and sort columns must be table columns that are not
partition keys, and `BucketColumns` needs a positive `NumberOfBuckets`. The setting is off by default because DuckDB
reads bucketed tables but does not write them; for the same reason, a `CREATE TABLE ... AS` with these options is
refused before the table is created.

### `CREATE TABLE ... AS`

The Glue table is created when the statement starts executing, before its query runs. Planning alone (`EXPLAIN`,
`PREPARE`) creates nothing.

It is refused when the location already holds data files, such as those a dropped table left behind, since they
would read as rows of the new table. `WITH (overwrite = true)` deletes them instead:

- The files are deleted in one `RemoveFiles` call when the statement starts, before the table is created. They stay
  deleted if the statement fails later.
- If the delete fails, some files may remain, and the table is not created.
- `overwrite` is refused when the location is, holds or lies inside the location of another table, or of a partition
  of one, in the same Glue database. (A partition can lie outside its table's location.) Tables in other databases,
  catalogs or accounts are not checked.
- Files a scan skips, such as `_SUCCESS` and hidden files, are left in place.
- `overwrite` is not stored as a table parameter. `CREATE OR REPLACE` and a `CREATE TABLE` without a query refuse it.

## Inserting

`INSERT INTO` and `CREATE TABLE ... AS` write files in the table's format into the table location, then register any
new partitions in Glue with `BatchCreatePartition`.

```sql
INSERT INTO lake.sales.orders SELECT id, amount, dt FROM staging;
```

- Each partition the statement touches gets one file. Partition columns are not stored in the files.
- A new partition gets a `<key>=<value>` directory. Rows of an existing partition go to its registered location,
  which can be any directory below the table location (such as `<table>/2024/01`).
- The partition keys are the table's last columns, so `INSERT ... VALUES` without a column list must give them
  last.
- Text files use the table's delimiter and NULL string, and at most one header line. In LazySimpleSerDe tables the
  NULL string is `\N` unless `serialization.null.format` says otherwise.

### What is refused

These fail the whole statement:

- a table with bucket columns;
- a table with the OpenX JSON SerDe (`org.openx.data.jsonserde.JsonSerDe`). As in Athena, only the Hive JsonSerDe is
  written, which is what `format = 'json'` creates. OpenX tables can still be read;
- a table whose InputFormat is not one Hive uses for its format. The accepted ones are `MapredParquetInputFormat`,
  `TextInputFormat` or `CombineTextInputFormat` (CSV and JSON), `AvroContainerInputFormat`, or empty. Another
  engine's InputFormat means a file layout DuckDB does not know, and writing into it could corrupt the table.

These fail only the rows of the partition concerned; the rows of other partitions are still written:

- a partition whose location is not below the table location;
- a partition whose location has an empty or `.` segment (`//`, `/./`), which DuckDB cannot write;
- rows whose directory is the location of another partition.

### Compression

Written files use the codec the table records:

| Format | Parameter |
|---|---|
| Parquet | `parquet.compression`, plus `compression_level` for zstd |
| CSV, JSON | the recorded codec, gzip or zstd. Files are named `.csv.gz`, `.json.zst`. |
| Avro | `avro.output.codec` |

A table without its format's codec parameter falls back to `write_compression` if it records one. Any other codec is
an error.

### When a write fails

Each statement gets a new write id (a UUIDv7). Its files are named `<write id>_<file id>.<extension>`.

- A Glue error on a single partition that may pass on retry, such as throttling or an internal error, is retried.
- If Glue still refuses a partition, we delete every file the statement wrote. The partitions it already
  registered stay, empty, so running the statement again does not add its rows twice.
- The transaction records each write when it starts and marks it complete once its partitions are registered. If
  a write is still incomplete when the transaction ends, its statement failed, and the rollback deletes the files
  named after its write id. For `CREATE TABLE ... AS` it also drops the table the statement created. Either way, the
  statement can be run again.
- To find the files, the rollback lists every object below the table location, one LIST request per 1,000 objects.
  If the location contains a glob character (`*`, `?`, `[`), it lists and deletes nothing.
- A table that a `CREATE TABLE ... AS` created is kept if it holds another writer's files, or if another client
  dropped and recreated it while the statement ran.

This cleanup is best effort. A step that fails is logged as a warning, which `CALL enable_logging(level :=
'warning')` shows. If the process dies, nothing is cleaned up at all; the files stay, and their rows appear once a
later `INSERT`, `MSCK REPAIR` or crawler registers their partition. Readers should also note that other queries can
read an insert's files before the insert finishes.

## Changing tables

All of these update the Glue definition with `UpdateTable`. Data files are never rewritten.

### Columns

| Statement | Allowed for |
|---|---|
| `ADD COLUMN` | Appended last. No defaults or collations. |
| `DROP COLUMN` | Not the last data column, a partition key, or a bucket or sort column. |
| `ALTER COLUMN ... TYPE` | Widening only: integer widening (`TINYINT` to `BIGINT`), `FLOAT` to `DOUBLE`, anything to `VARCHAR`. No collations, no `USING`, no partition keys. |
| `RENAME COLUMN` | CSV tables only. Not partition keys, bucket or sort columns. |
| `COMMENT ON COLUMN` | Sets the Glue column comment (`IS NULL` removes it). Comments show in `duckdb_columns()`. Partition keys are not supported yet. |

Existing data files keep their column types, which is why we only allow widening. Renames are limited to CSV
because CSV files are read by position, while Parquet, JSON and Avro files are matched by name, so a renamed column
would read as NULL. To do that deliberately, use `glue_replace_columns`.

### Replacing all columns

`glue_replace_columns` is Hive's `ALTER TABLE ... REPLACE COLUMNS`. It replaces all data columns at once, which can
also rename and reorder them:

```sql
CALL glue_replace_columns('lake.sales.orders', {id: 'BIGINT', name: 'VARCHAR'}, comments := {id: 'order id'});
```

- Types are DuckDB types, stored the way `CREATE TABLE` stores them.
- A column that stays keeps its stored name and may only be widened, as with `ALTER COLUMN ... TYPE`. Names are
  compared case-insensitively.
- As in Hive, comments not given in `comments` are dropped. `keep_comments := true` keeps the comments of columns
  that stay, and a NULL in `comments` then removes one.
- Leave the partition keys out; they are kept. Bucket and sort columns must be listed.
- It returns the columns as stored, with Glue type names.
- Parquet, JSON and Avro files are matched by name, so a renamed column reads as NULL. CSV files are matched by
  position, so a CSV table must keep its number of columns, each column may only be widened, and a new name renames
  the column.

### Table parameters

`ALTER TABLE ... SET (key = 'value', ...)` adds or overwrites Glue table parameters (Hive's `TBLPROPERTIES`). `RESET
(key, ...)` removes them. Everything else in the definition stays as it is.

```sql
ALTER TABLE lake.sales.orders SET ('parquet.compression' = 'ZSTD', compression_level = 4);
```

Values are stored as strings, so `4` becomes `'4'`. The parameters that decide the table's format (`table_type`,
`spark.sql.sources.provider`, `metadata_location`) cannot be changed this way.

## Dropping

`DROP TABLE` and `DROP SCHEMA` delete the Glue entries and leave the data files in S3. Glue drops every table of a
database along with the database, so `DROP SCHEMA` refuses a database that still has tables or views unless you add
`CASCADE`.

## Transactions and caching

DDL and `INSERT` cannot run inside an explicit `BEGIN` transaction. This covers `CREATE`/`ALTER`/`DROP` of schemas,
tables and views, `CREATE TABLE ... AS`, the partition SQL, and the `glue_*` functions that change Glue. Running one
there is an error that aborts the transaction; end it with `COMMIT`, `ABORT` or `ROLLBACK`, and then run the statement
on its own.

For the changing functions (`glue_add_partition`, `glue_drop_partition`, `glue_rename_partition`,
`glue_set_partition_location`, `glue_set_table_location`, `glue_replace_columns`), the error names the function and
says to run it as `CALL glue_replace_columns(...)`. The same error appears for `SELECT * FROM glue_replace_columns(...)`
from a client that wraps every query in its own transaction.

Within one transaction, we fetch a table's definition and partition list from Glue once and reuse them. The files
in those partitions are listed again for each query, and all scans of one query share that listing. In other words:

- a partition added outside the transaction becomes visible after the transaction ends;
- a file added to an existing partition is seen by the next query, or after the next cache clear under
  `glue_cache_listings`.

With `SET glue_metadata_cache_scope = 'connection'`, the cache also serves the connection's later transactions. Each
connection has its own cache, so a write made through another connection stays unseen until something clears this
one's cache, which a write through this connection, a rollback or DETACH does.

## Recording written files

With `SET glue_store_written_files = true`, every `INSERT` or `CREATE TABLE ... AS` into a Hive table adds one row
per file it wrote to the temporary table `temp.main.glue_written_files`. Each row has the statement's `query_id`, the
`table` (`database.table`) and the `file_path`, which makes it a convenient manifest of what a query wrote.

```sql
SET glue_store_written_files = true;
INSERT INTO lake.db.events SELECT ...;
SELECT file_path FROM temp.main.glue_written_files WHERE query_id = (SELECT max(query_id) FROM temp.main.glue_written_files);
-- s3://bucket/db/events/day=2026-11-09/<write id>_<file id>.snappy.parquet
-- s3://bucket/db/events/day=2026-11-10/<write id>_<file id>.snappy.parquet
```

The first write that records files creates the table. Rows are added in the write's transaction, once its partitions
are registered, so a failed write adds none. Like any temporary table, it belongs to the connection.
