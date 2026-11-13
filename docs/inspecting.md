# Inspecting tables and logging

## Glue definitions

Every table entry carries its format in `duckdb_tables().tags['table_type']` (`HIVE`, `ICEBERG`, `DELTA`, `HUDI`,
`SYMLINK` or `UNKNOWN`). The full Glue definition of a table is available through a table function:

```sql
SELECT * FROM glue_get_table_response('my_datalake.default.some_table');
SELECT response.StorageDescriptor.Location FROM glue_get_table_response('my_datalake.default.some_table');
```

It returns one row with the classification, the Glue table type, location, SerDe, columns, partition keys and
parameters as columns, plus the complete Glue `Table` object as a VARIANT in `response`. A partially qualified name
(`'default.some_table'`, or `'some_table'` after `USE my_datalake.default`) is resolved like in a query, through the
table's catalog entry, so for a table DuckDB can not read (e.g. an unsupported column type) give the fully qualified
name.

`glue_get_partition_response('<table>', {dt: '2024-01-01', ...})` does the same for one partition of a Hive table:
its values, location, SerDe, columns and parameters as columns, plus the complete Glue `Partition` object in
`response`. The partition is given like for the other partition functions, as a struct naming every partition key.

```sql
SELECT location, response.StorageDescriptor.InputFormat
FROM glue_get_partition_response('my_datalake.default.some_table', {dt: '2024-01-01'});
```

`glue_get_database_response('<catalog>.<database>')` does the same for a Glue database (a DuckDB schema): its
description, location and parameters as columns and the complete Glue `Database` object in `response`. An unqualified
`'<database>'` is resolved like in a query, through the search path.

## HTTP transport and logging

The AWS SDK's Glue calls are routed through DuckDB's HTTP layer (httpfs), so they honor DuckDB's proxy and
certificate settings and appear in the HTTP log:

```sql
SET redact_http_logs = false; -- header values (incl. x-amz-target) are redacted by default
CALL enable_logging('HTTP', storage='memory');
-- ... run queries ...
SELECT request.type, request.url, request.headers['x-amz-target'], response.status FROM duckdb_logs_parsed('HTTP');
```

`SET glue_network_calls_via_duckdb = false` switches back to the SDK's own HTTP client (not under WebAssembly,
where the SDK has none).

`glue_requests()` lists the Glue requests in the `HTTP` log, retries and pages included: `operation` (the
`X-Amz-Target`, e.g. `AWSGlue.GetPartitions`), `status`, `reason`, `start_time`, `duration_ms` and the
`connection_id`, `transaction_id` and `query_id` that made them. Only requests sent through DuckDB's HTTP layer are
logged.

The `GlueScan` log type (`INFO`) records what each scan of a Hive table does, one entry per `event`, with the
`scan_id` that numbers the scans of a query from 1:

- `partitions`: the table's `partitions` were loaded, taking `duration_ms`;
- `plan`: of `partitions`, `partitions_read` are read, with `listings` S3 listings; `detail` is `root` when they are
  listed in one listing of the table location, else `partitions`;
- `listing`: one listing (`detail`: `table`, `root` or `partition`) of `partitions_read` partitions found `files`
  data files in `duration_ms`;
- `sample`: the cardinality sample listed `files` files and measured one in `duration_ms` (`files` is NULL when
  nothing was listed: avro tables are not sampled). The scan reuses that listing, so a `listing` of the same
  directory then takes next to no time. Every scan of the table in the query shares the one sample.

```sql
CALL enable_logging(['HTTP', 'GlueScan'], storage = 'memory');
-- ... run queries ...
SELECT operation, count(*), sum(duration_ms) FROM glue_requests() GROUP BY operation;
SELECT "table", scan_id, event, partitions, partitions_read, listings, files, duration_ms, detail
FROM duckdb_logs_parsed('GlueScan');
```

The `GlueRetry` log type (`WARNING`) records each retry the AWS SDK's retry strategy decides on for a Glue API call,
before the wait, with either HTTP client: the `error` that caused it (e.g. `ThrottlingException`), its HTTP `status`,
the `retry` number (1 for the first) and the `request_id` of the failed attempt. `status` is NULL when the request got
no response, `error` and `request_id` when the SDK has none (e.g. a dropped connection). The retry limit is
`glue_max_retries`, or 10 by default. A retry the SDK makes to correct clock skew does not go through the
retry strategy and is not logged.

```sql
CALL enable_logging('GlueRetry', storage = 'memory');
-- ... run queries ...
SELECT error, status, count(*) FROM duckdb_logs_parsed('GlueRetry') GROUP BY ALL;
```
