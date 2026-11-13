# Reading without a catalog: hive_scan

The same scan is available as a table function, for parquet Hive tables that are not (or not yet) registered in
Glue. The schema and the partitions are given as arguments; nothing is looked up in a catalog.

```sql
SELECT * FROM hive_scan('s3://bucket/warehouse/orders',
    schema := {id: 'INTEGER', amount: 'DOUBLE', dt: 'VARCHAR', country: 'VARCHAR'},
    partitions := [
        {dt: '2016-05-14', country: 'IN', location: NULL},
        {dt: '2016-05-16', country: 'IN', location: 's3://bucket/imports/INDIA_16_May_2016'}
    ]);
```

- `schema` (required): a struct of column name to DuckDB type name, data columns and partition columns.
- `format`: `'parquet'` (default), `'csv'`, `'json'` or `'avro'`. For csv, `header := true`, `delim := '|'`,
  `quote := '"'`, `escape := '\'` and `nullstr := '\N'` describe the files (an empty field is NULL unless `nullstr`
  says otherwise, as in `read_csv`); csv columns are matched by position, json keys by name. For json,
  `ignore_errors := true` reads a line that is not valid JSON as a row of NULLs.
- `partitions`: one struct per partition with a value for every partition key and an optional `location`; without
  a location the partition lives at `<root>/<key>=<value>/...`. The partition keys are the struct fields other than
  `location`, in that order, unless `partition_keys := [...]` names them. Without `partitions` the table is
  unpartitioned and all files below the root are read; `partition_keys` without `partitions` is an error, and
  `partitions := []` with `partition_keys` reads no rows.
- A root or partition without data files scans as zero rows.
