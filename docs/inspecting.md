# Inspecting tables and logging

## Table types

Every table entry carries its format in `duckdb_tables().tags['table_type']`, which is one of `HIVE`, `ICEBERG`,
`DELTA`, `HUDI`, `SYMLINK` or `UNKNOWN`.

## Glue definitions

Three table functions return what Glue holds for a table, a partition or a database. Each returns one row with the
most useful fields as columns, plus the complete Glue object as a `VARIANT` in `response`.

```sql
SELECT * FROM glue_get_table_response('my_datalake.default.some_table');
SELECT response.StorageDescriptor.Location FROM glue_get_table_response('my_datalake.default.some_table');

SELECT location, response.StorageDescriptor.InputFormat
FROM glue_get_partition_response('my_datalake.default.some_table', {dt: '2024-01-01'});

SELECT * FROM glue_get_database_response('my_datalake.default');
```

| Function | Columns | `response` |
|---|---|---|
| `glue_get_table_response('<table>')` | classification, Glue table type, location, SerDe, columns, partition keys, parameters | the Glue `Table` |
| `glue_get_partition_response('<table>', {...})` | values, location, SerDe, columns, parameters | the Glue `Partition` |
| `glue_get_database_response('<catalog>.<database>')` | description, location, parameters | the Glue `Database` |

The partition is given as for the [partition functions](partitions.md), as a struct with a value for every partition
key.

A partially qualified table name, such as `'default.some_table'`, or `'some_table'` after
`USE my_datalake.default`, is resolved as in a query, through the table's catalog entry. For a table DuckDB cannot
read, such as one with an unsupported column type, give the fully qualified name. An unqualified database name is
resolved through the search path.

## HTTP log

The AWS SDK's Glue calls go through DuckDB's HTTP layer (httpfs), so they honor DuckDB's proxy and certificate
settings and appear in the HTTP log:

```sql
SET redact_http_logs = false; -- header values, including x-amz-target, are redacted by default
CALL enable_logging('HTTP', storage = 'memory');
-- ... run queries ...
SELECT request.type, request.url, request.headers['x-amz-target'], response.status FROM duckdb_logs_parsed('HTTP');
```

`SET glue_network_calls_via_duckdb = false` switches back to the SDK's own HTTP client. Under WebAssembly the SDK has
no client of its own, so the setting has no effect there.

`glue_requests()` lists the Glue requests in the HTTP log, including retries and pages. Each row has the `operation`,
which is the `X-Amz-Target` such as `AWSGlue.GetPartitions`, the `status`, `reason`, `start_time` and `duration_ms`,
and the `connection_id`, `transaction_id` and `query_id` that made the request. Only requests sent through DuckDB's
HTTP layer are logged.

## Scan log

The `GlueScan` log type, at level `INFO`, records what each scan of a Hive table does. Each entry has an `event`, and
a `scan_id` that numbers the scans of a query from 1.

| `event` | Meaning |
|---|---|
| `partitions` | The table's `partitions` were loaded, taking `duration_ms`. |
| `plan` | Of the `partitions`, `partitions_read` are read, with `listings` S3 listings. `detail` is `root` when they are listed in one listing of the table location, and `partitions` otherwise. |
| `listing` | One listing, with `detail` `table`, `root` or `partition`, found `files` data files for `partitions_read` partitions in `duration_ms`. |
| `sample` | The cardinality sample listed `files` files and measured one in `duration_ms`. `files` is NULL when nothing was listed, as for Avro tables, which are not sampled. |

Every scan of the table in a query shares the one sample, and the scan reuses its listing. A `listing` of the same
directory therefore takes next to no time.

```sql
CALL enable_logging(['HTTP', 'GlueScan'], storage = 'memory');
-- ... run queries ...
SELECT operation, count(*), sum(duration_ms) FROM glue_requests() GROUP BY operation;
SELECT "table", scan_id, event, partitions, partitions_read, listings, files, duration_ms, detail
FROM duckdb_logs_parsed('GlueScan');
```

## Retry log

The `GlueRetry` log type, at level `WARNING`, records each retry that the AWS SDK's retry strategy decides on for a
Glue call, before the wait, with either HTTP client. Each entry has:

- `error`, the error that caused the retry, such as `ThrottlingException`;
- `status`, its HTTP status;
- `retry`, the retry number, 1 for the first;
- `request_id`, the request ID of the failed attempt.

`status` is NULL when the request got no response, and `error` and `request_id` are NULL when the SDK has none, as for
a dropped connection. The retry limit is `glue_max_retries`, or 10 by default. A retry that the SDK makes to correct
clock skew does not go through the retry strategy, so it is not logged.

```sql
CALL enable_logging('GlueRetry', storage = 'memory');
-- ... run queries ...
SELECT error, status, count(*) FROM duckdb_logs_parsed('GlueRetry') GROUP BY ALL;
```
