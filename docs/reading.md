# Reading Hive tables

Every format is scanned through a custom `MultiFileReader`, `HiveMultiFileReader`, which reads the files of the
partitions Glue registers. This page covers how we pick a reader, which files we read, how partitions are pruned and
listed, and what the optimizer gets to work with. The CSV and JSON SerDe properties have their own page,
[Text formats](text-formats.md).

- [Which format a table has](#which-format-a-table-has)
- [Which reader reads it](#which-reader-reads-it)
- [Which files are read](#which-files-are-read)
- [Partition pruning](#partition-pruning)
- [How partitions are listed](#how-partitions-are-listed)
- [Caching listings](#caching-listings)
- [Row counts and statistics](#row-counts-and-statistics)
- [Schema and column matching](#schema-and-column-matching)
- [Metadata columns](#metadata-columns)
- [Compression](#compression)

## Which format a table has

We read a table's format from its parameters (`table_type`, `spark.sql.sources.provider`, `metadata_location`) and
otherwise from its InputFormat. Hudi tables (`HoodieParquetInputFormat`, ...) and symlink manifest tables
(`SymlinkTextInputFormat`, such as a Delta `_symlink_format_manifest`) register a Parquet or text SerDe, so only the
InputFormat tells them apart from a Hive table. Like Iceberg and Delta, they are listed but neither read nor written.

## Which reader reads it

The SerDe of the Glue table decides the reader:

| SerDe | Reader | Columns matched |
|---|---|---|
| ParquetHiveSerDe | `read_parquet` | by name |
| LazySimpleSerDe, OpenCSVSerde | `read_csv` | by position |
| Hive JsonSerDe, OpenX JsonSerDe | `read_json`, one object per line | by name, ignoring case |
| AvroSerDe | `read_avro`, from the avro extension, which we load on demand | by name |

Other SerDes (ORC, Ion, ...) are not supported.

Two Parquet details are worth knowing. For one, a `BYTE_ARRAY` column without a `STRING` annotation, as Impala and
older Hive writers produce, is read as UTF-8 text when Glue declares the column `string`, and as bytes when Glue
declares it `binary`, whatever the session's `binary_as_string` says; bytes that are not valid UTF-8 are an error in
the first case. In a table that declares a `binary` column or field, such bytes inside an `array`, `struct` or `map`
column keep the escaped form the reader gives a BLOB. Furthermore, a `float` or `double` column may hold NaN wherever
the file does not count its NaN values, since most writers leave `nan_count` out. The min and max of such a column
therefore never skip a row group for a filter that NaN could match.

## Which files are read

The data files are those below the location of every partition Glue lists through `GetPartitions`, at any depth, or
below the table location for an unpartitioned table. Partition locations need not follow the `<key>=<value>` layout.

We skip files and directories named `_*` or `.*`, `*_$folder$` directory markers, and objects the listing reports as
zero bytes. A table with no data files at all, such as one just created, scans as empty.

Locations are read as registered, including a `//` in the path, and `s3a://` and `s3n://` name the same bucket and key
as `s3://` does. When one partition's location lies inside another's, a file belongs to the deepest one. A location
that two or more partitions are registered at is refused when a query reads one of them and the location holds data
files, since Hive engines read such a file once for each partition and DuckDB does not.

A partitioned table with Athena partition projection (`projection.enabled = true`) is refused on read and write
rather than scanned. Athena derives the partitions of such a table from its `projection.*` parameters and ignores the
registered ones, which the table usually has none of, so a scan would silently read nothing. To read it, set
`projection.enabled` to `false` and register its partitions with `ALTER TABLE ... ADD PARTITION` or
`glue_add_partition`.

## Partition pruning

A partition column's values are the values Glue stores for that partition, not the directory names, typed as Glue's
partition keys.

Files are listed lazily, which is what makes pruning pay off. We apply filters on partition columns to the partition
values first, so only the partitions a query actually reads are ever listed; `EXPLAIN` shows the partitions we kept as
`Scanning Files`. This includes filters a join derives from its build side by the time the scan starts. A filter that
the partition values decide is true of every row the scan then reads, so we drop it from the plan, as `read_parquet`
with hive partitioning does.

## How partitions are listed

For the partitions read below the table location, we list whichever way takes fewer rounds of requests:

- the location once, recursively, which is one S3 request per 1000 keys, one after another, with the files matched to
  their partitions by prefix; or
- each partition on its own, several at the same time.

On a tie we list the location when that makes a tenth of the requests, so a table of up to 1000 partitions lists its
location once at least 10 partitions are read, or fewer when they take more than one round. The estimate assumes one
file per partition. `SET hive_partition_listing_threshold = n` replaces it with a fixed rule, which lists the location
from n partitions read.

Partitions at custom locations are always listed on their own. Listings run as tasks on DuckDB's threads, as many at a
time as `SET threads` allows.

## Caching listings

A directory is listed once per query, shared by every scan of it and by the cardinality sample.

With `SET glue_cache_listings = true`, off by default, we keep the listing for the connection's later statements as
well. It is held and cleared like the metadata cache, which means `glue_metadata_cache_scope`, writes through the
extension, rollback and DETACH all apply. Files added any other way, by another writer or a `COPY TO` of the table
location, appear only after the next clear.

Files removed any other way are worse than merely unseen. After a compaction, an overwrite, or dropping and
recreating the table at the same location, scans under the cached listing either fail trying to open the dead paths or
read the superseded files, until the next clear. The setting suits hosts that run one query per connection.

## Row counts and statistics

To estimate a scan's row count, planning lists one directory per table and query and reads the row count of its
largest file, which is the Parquet footer, or the lines of a 64 KiB prefix for CSV and JSON. The directory is the
first partition a scan of the table reads, or the location of an unpartitioned table; when the scan lists the table
location, it is the first page of that listing, which the scan then continues.

Every scan of the table in the query scales that one measurement by the files and bytes of the partitions it reads.
The partitions the listing holds completely count with their own size, and the others with the average of those. A
scan that lists the same directory reuses the listing. Avro tables are not measured, so planning them lists nothing.

An unpartitioned Parquet table of at most `hive_footer_statistics_max_files` files, 16 by default and 25 at most, is
counted exactly instead: its row count is the sum of the row counts in the footers of all its files.

### Partition key statistics

Partition keys have no optimizer statistics by default, as for `read_parquet` with hive partitioning. With
`SET glue_enable_partition_key_statistics = true` they get the bounds and the exact distinct count of the partitions a
scan reads. This makes the group estimate on a key exact, but the optimizer can then push aggregates below a join on
that key and lose the join's run-time partition pruning, so it is off by default.

### Column statistics from Parquet footers

A data column of an unpartitioned Parquet table of at most `hive_footer_statistics_max_files` files, 16 by default,
gets its minimum, maximum and NULL count merged from the footers of all of them. DuckDB's Parquet reader reads those
footers when the scan binds, under its own `maximum_files_for_stats`, so a `DESCRIBE` of such a table lists the table
and reads its footers too. Setting it to `0` turns off both the statistics and the exact row count.

A column gets no statistics when a footer cannot be read, when some file with rows lacks the column, when a file
stores it as a different type than the table declares, or when, in a table of several files, a file has statistics for
it that DuckDB cannot read. Neither do the data columns of partitioned, CSV, JSON and Avro tables. An empty file is
skipped. A table of one file whose statistics DuckDB cannot read fails planning, just as `read_parquet` of that file
does.

This is powerful for join ordering as much as for pruning: the minimum and maximum of a dimension's key let the
optimizer see how much a join on that key keeps.

## Schema and column matching

The schema is Glue's, with the data columns first and the partition keys last, in `PARTITIONED BY` order. We fetch the
Glue table definition and the partition list once per query and table, however often the query scans the table.

Files are matched by column name. A column a file does not have, because it was added after the file was written,
reads as NULL; a column whose type in the file differs from the table's is cast; and file columns Glue does not list
are ignored.

A partition declares its own columns, and those keep the types they had when the partition was created even after the
table's columns change. A scan that reads a column whose partition type cannot be read as the table's type fails,
naming the partition and both types. Partition columns are matched by name, by position for CSV tables, and only the
columns the query reads are checked. Nested types are not checked at all.

A partition type reads as the table's in three cases:

- `ALTER COLUMN ... TYPE` could have changed one into the other, which covers integer widening, `float` to `double`,
  and anything to `string`;
- DuckDB casts it implicitly, such as `int` to `double` or to `decimal`;
- both are decimals.

`bigint` to `int`, `double` to `int` and `string` to `int` do not.

## Metadata columns

Besides `filename`, a scan has three of the metadata columns of Trino's Hive connector, which `SELECT *` leaves out:

| Column | Type | Value |
|---|---|---|
| `"$path"` | `VARCHAR` | the data file, the same as `filename` |
| `"$file_size"` | `BIGINT` | the size the listing reports |
| `"$file_modified_time"` | `TIMESTAMP WITH TIME ZONE` | the modification time the listing reports |

`hive_scan()` has them too.

## Compression

Every CSV and JSON file is read with the codec DuckDB tells from its name (`.gz`, `.zst`), whatever the table records.
So a table whose files use more than one codec is still read correctly, and a stale `write.compression` or
`compressionType` does not matter. The codec a table records is the one writes to it use. Parquet and Avro files carry
their codec themselves.
