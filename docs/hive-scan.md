# Reading without a catalog: hive_scan

`hive_scan` runs the same scan as a Glue table, for Hive tables that are not registered in Glue, or not yet. The
schema and the partitions are given as arguments, and nothing is looked up in a catalog.

```sql
SELECT * FROM hive_scan('s3://bucket/warehouse/orders',
    schema := {id: 'INTEGER', amount: 'DOUBLE', dt: 'VARCHAR', country: 'VARCHAR'},
    partitions := [
        {dt: '2016-05-14', country: 'IN', location: NULL},
        {dt: '2016-05-16', country: 'IN', location: 's3://bucket/imports/INDIA_16_May_2016'}
    ]);
```

## Arguments

| Argument | Meaning |
|---|---|
| `schema` | Required. A struct mapping each column name to a DuckDB type name. List both the data columns and the partition columns. |
| `format` | `'parquet'` (default), `'csv'`, `'json'` or `'avro'`. |
| `partitions` | One struct per partition, with a value for every partition key and an optional `location`. |
| `partition_keys` | The partition key names, in order. Only needed when the struct fields should not decide them. |

CSV files take `header := true`, `delim := '|'`, `quote := '"'`, `escape := '\'` and `nullstr := '\N'`. As in
`read_csv`, an empty field is NULL unless `nullstr` says otherwise. CSV columns are matched by position and JSON keys
by name. For JSON, `ignore_errors := true` reads a line that is not valid JSON as a row of NULLs.

## Partitions

- A partition without a `location` lives at `<root>/<key>=<value>/...`.
- The partition keys are the fields of the struct other than `location`, in that order, unless `partition_keys`
  names them.
- Without `partitions`, the table is unpartitioned and every file below the root is read.
- `partition_keys` without `partitions` is an error, while `partitions := []` together with `partition_keys` reads no
  rows.

A root or partition without data files scans as zero rows.
