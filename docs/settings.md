# Settings

Every setting below is changed with `SET <name> = <value>`; the linked pages explain each one in more detail.

## Metadata and listing

| Setting | Default | What it does |
|---|---|---|
| `glue_metadata_cache_scope` | `'transaction'` | How long table and partition metadata is reused. `'connection'` keeps it across transactions until a write through this connection, a rollback or DETACH clears it. |
| `glue_batch_table_lookup` | `true` | After the first table of a database is looked up, fetch one `GetTables` page of that database instead of calling `GetTable` for each table. |
| `glue_cache_listings` | `false` | Keep S3 listings for later statements of the connection. Files that another writer adds or removes go unseen until the cache is cleared. ([Reading](reading.md)) |
| `glue_get_partitions_segments` | `0` | How many `GetPartitions` requests run in parallel. `0` means 8 against AWS and 1 against a custom `ENDPOINT`. At most 10. ([Partitions](partitions.md)) |
| `hive_partition_listing_threshold` | unset | List the table location once when at least this many partitions are read. Unset: choose whichever needs fewer rounds of requests. ([Reading](reading.md)) |

## Planning

| Setting | Default | What it does |
|---|---|---|
| `hive_footer_statistics_max_files` | `16` | Unpartitioned Parquet tables with up to this many files get exact row counts and column min/max/NULL statistics from the file footers. `0` turns this off. At most 25. |
| `glue_enable_partition_key_statistics` | `false` | Give the optimizer the bounds and distinct counts of partition keys. Group estimates become exact, but run-time join pruning can be lost. |

## CSV

| Setting | Default | What it does |
|---|---|---|
| `hive_csv_parallel` | `true` | Read each file of a scan with several threads when the scan has few files. Set it to `false` for files with line breaks inside quoted fields. ([Text formats](text-formats.md)) |
| `hive_csv_max_line_size` | `8000000` | Longest line, in bytes, a CSV scan accepts. |

## Writing

| Setting | Default | What it does |
|---|---|---|
| `glue_store_written_files` | `false` | Record each file an `INSERT` or `CREATE TABLE ... AS` writes in `temp.main.glue_written_files`. ([Writing](writing.md)) |
| `glue_create_bucketed_tables` | `false` | Allow `CREATE TABLE` to create bucketed tables. DuckDB can read these tables but not write to them. |

## Network and views

| Setting | Default | What it does |
|---|---|---|
| `glue_network_calls_via_duckdb` | `true` | Send Glue API calls through DuckDB's HTTP layer (proxy, certificates, HTTP log). Always on under WebAssembly. |
| `glue_max_retries` | `-1` | Retry limit for Glue API calls, from 0 to 10. `-1` keeps the AWS SDK's own policy. |
| `experimental_glue_view_external_catalog_types` | `[]` | Catalog types, such as `['duckdb', 'iceberg']`, whose tables Glue views may read. Global only. ([Views](views.md)) |
