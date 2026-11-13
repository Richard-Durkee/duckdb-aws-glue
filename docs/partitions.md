# Partitions

DuckDB has no `ALTER TABLE ... PARTITION` syntax, so the Hive partition statements are available as functions. The
Hive SQL forms also work once the extension's grammar is switched on ([see below](#hive-partition-sql)).

## Functions

A partition is given as a struct with a value for every partition key, such as `{dt: '2016-05-14', country: 'IN'}`.
Glue stores the values as strings, in partition key order. The table name may be partially qualified, as `'db.t'`, or
`'t'` after `USE cat.db`; it is resolved the same way as in a query.

| Function | Hive statement |
|---|---|
| `glue_partitions('cat.db.t')` | `SHOW PARTITIONS` |
| `glue_get_partition_response('cat.db.t', {...})` | `DESCRIBE FORMATTED ... PARTITION (...)` |
| `CALL glue_add_partition('cat.db.t', {...}, location := 's3://...', if_not_exists := false)` | `ALTER TABLE ADD [IF NOT EXISTS] PARTITION (...) [LOCATION ...]` |
| `CALL glue_drop_partition('cat.db.t', {...}, if_exists := false)` | `ALTER TABLE DROP [IF EXISTS] PARTITION (...)` |
| `CALL glue_rename_partition('cat.db.t', {...}, {...})` | `ALTER TABLE PARTITION (...) RENAME TO PARTITION (...)` |
| `CALL glue_set_partition_location('cat.db.t', {...}, 's3://...')` | `ALTER TABLE PARTITION (...) SET LOCATION '...'` |
| `CALL glue_set_table_location('cat.db.t', 's3://...')` | `ALTER TABLE SET LOCATION '...'` |
| `CALL glue_repair_table('cat.db.t')` | `MSCK REPAIR TABLE` |

- `glue_add_partition` without `location` puts the partition at `<table location>/dt=2016-05-14/country=IN`.
- `glue_drop_partition` leaves the partition's data files in S3.
- `glue_rename_partition` changes the partition's values and keeps its location.
- `glue_set_table_location` leaves existing partitions where they are; new partitions go under the new location.
- `glue_get_partition_response` returns the partition's location, SerDe, columns and parameters, plus its complete
  Glue definition in `response` ([Inspecting tables](inspecting.md)).

Run the functions that change Glue with `CALL`, outside a `BEGIN` transaction, since Glue has no transactions
([Writing](writing.md#transactions-and-caching)).

### Listing partitions

`glue_partitions` returns one row per registered partition. Each partition key gets a typed column, and two more
columns follow:

- `location`, the partition's location;
- `partition_values`, the strings Glue stores, with `__HIVE_DEFAULT_PARTITION__` for NULL.

If a partition key is itself named `location` or `partition_values`, the extra column is numbered like a duplicate
result column, as in `location_1`.

A typed column is NULL in two cases: for the NULL partition, and for a value the key's type cannot hold, such as `abc`
that another tool registered on an integer key. A value of the second kind fails every filtered read of the table.
`partition_values` tells the two cases apart, so this query finds such partitions, with one condition per partition
key:

```sql
SELECT location, partition_values
FROM glue_partitions('my_datalake.default.orders')
WHERE (dt IS NULL) <> (partition_values[1] = '__HIVE_DEFAULT_PARTITION__')
   OR (country IS NULL) <> (partition_values[2] = '__HIVE_DEFAULT_PARTITION__');
```

### Repairing a table

`glue_repair_table` registers the partition directories under the table location that Glue does not have yet. It
returns one row per partition it added, with a typed column per partition key and the `location`.

It lists the table location once, and never reuses a listing that `glue_cache_listings` kept. A directory counts as a
partition when it has one `<key>=<value>` component per partition key, in partition key order; keys are compared
case-insensitively. The partition is registered at that directory, with the values unescaped and cast to the key
types, so `year=07` becomes `7`. All new partitions are registered with `BatchCreatePartition`.

A directory is ignored when:

- it does not follow this layout;
- its values do not cast to the key types;
- it is already the location of a partition;
- it holds no file that a scan would read, only `_` or `.` prefixed files, `_$folder$` markers or empty objects.

`glue_repair_table` never removes partitions. It refuses a table whose InputFormat belongs to another engine but
whose SerDe is a Hive SerDe, which `INSERT` refuses as well, since the files of such a table are not laid out the way
Hive writes them.

## Hive partition SQL

The extension registers a grammar extension, `glue_hive_ddl`, that accepts the Hive partition statements. Grammar
extensions are switched on per connection:

```sql
SET active_grammar_extensions = ['glue_hive_ddl'];
ALTER TABLE my_datalake.default.orders ADD IF NOT EXISTS
    PARTITION (dt = '2016-05-14', country = 'IN')
    PARTITION (dt = '2016-06-02', country = 'IN') LOCATION 's3://bucket/imports/INDIA_02_June_2016/';
ALTER TABLE my_datalake.default.orders DROP IF EXISTS PARTITION (dt = '2016-05-14', country = 'IN'), PARTITION (dt = '2016-05-15', country = 'IN');
ALTER TABLE my_datalake.default.orders PARTITION (dt = '2016-05-15', country = 'IN') RENAME TO PARTITION (dt = '2016-05-16', country = 'IN');
ALTER TABLE my_datalake.default.orders PARTITION (dt = '2016-05-16', country = 'IN') SET LOCATION 's3://bucket/other/';
ALTER TABLE my_datalake.default.orders SET LOCATION 's3://bucket/orders_v2/';
```

One statement can chain several actions, as in `ADD PARTITION (...) LOCATION '...' ADD PARTITION (...) ...`. The
statement becomes `CALL glue_alter_table(table, [actions])`, which checks every action against Glue before applying
any of them, so a statement that fails changes nothing. The table name may be partially qualified, as for the
functions.

The checks fetch the partitions they need with `BatchGetPartition`. Consecutive adds go out as one
`BatchCreatePartition` call, and consecutive drops as `BatchDeletePartition` calls of up to 25 partitions each. So
dropping partitions needs both the `glue:BatchGetPartition` and the `glue:BatchDeletePartition` permission.

## How partitions are fetched

`glue_partitions` and the planning of a scan of a partitioned table both page through Glue's `GetPartitions`. A scan
fetches the partitions when planning first needs them, for pruning or for the cardinality estimate, and not when it
binds. So `DESCRIBE`, `CREATE VIEW` and `PREPARE` make no `GetPartitions` call, and every scan of the table in one
query shares a single fetch. The responses include each partition's storage descriptor and columns, which the scan
checks the partitions it reads against ([Reading](reading.md#schema-and-column-matching)).

The pages are requested in parallel with Glue's Segment API: `glue_get_partitions_segments` requests run at the same
time, each over its own segment of the partitions. The default, `0`, means 8 requests against AWS and 1 against a
server given with `ENDPOINT`. Glue accepts at most 10. Moto ignores `Segment` and answers every segment with the
whole table, so partitions that another segment already returned are dropped.

Against AWS, listing the 2526 partitions of a TPC-H SF1 `lineitem` table took about 2.1 s with one request at a time
and about 0.45 s with the default of 8 segments. `SET glue_get_partitions_segments = 1` restores one request at a
time.
