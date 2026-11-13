# Partitions

DuckDB has no `ALTER TABLE ... PARTITION` syntax, so the Hive partition statements are table functions. The
partition is given as a struct naming every partition key; values are stored as strings in Glue, in partition key
order. The table name may be partially qualified (`'db.t'`, or `'t'` after `USE cat.db`); it is resolved like in a
query.

| function | Hive statement |
|----------|------------------|
| `glue_partitions('cat.db.t')` | `SHOW PARTITIONS`: one row per registered partition, a typed column per partition key plus `location` and `partition_values`, the strings Glue stores (`__HIVE_DEFAULT_PARTITION__` for NULL); a typed column is NULL both for the NULL partition and for a value the key's type cannot hold, `partition_values` tells them apart. Both are numbered like a duplicate result column (`location_1`) when a partition key has their name |
| `glue_get_partition_response('cat.db.t', {dt: '2016-05-14', country: 'IN'})` | `DESCRIBE FORMATTED ... PARTITION (...)`: the partition's location, SerDe, columns and parameters, and its complete Glue definition in `response` |
| `CALL glue_add_partition('cat.db.t', {dt: '2016-05-14', country: 'IN'}, location := 's3://...', if_not_exists := false)` | `ALTER TABLE ADD [IF NOT EXISTS] PARTITION (...) [LOCATION ...]`; without `location` the partition lives at `<table location>/dt=2016-05-14/country=IN` |
| `CALL glue_drop_partition('cat.db.t', {dt: '2016-05-14', country: 'IN'}, if_exists := false)` | `ALTER TABLE DROP [IF EXISTS] PARTITION (...)`; the data files stay in S3 |
| `CALL glue_rename_partition('cat.db.t', {dt: '2016-05-14', country: 'IN'}, {dt: '2016-05-15', country: 'IN'})` | `ALTER TABLE PARTITION (...) RENAME TO PARTITION (...)`; changes the values, keeps the location |
| `CALL glue_set_partition_location('cat.db.t', {dt: '2016-05-14', country: 'IN'}, 's3://...')` | `ALTER TABLE PARTITION (...) SET LOCATION '...'` |
| `CALL glue_set_table_location('cat.db.t', 's3://...')` | `ALTER TABLE SET LOCATION '...'`; existing partitions keep their locations, new ones land under the new location |
| `CALL glue_repair_table('cat.db.t')` | `MSCK REPAIR TABLE`: registers the Hive `<key>=<value>` partition directories under the table location that are not in Glue yet; one row per added partition (a typed column per partition key plus `location`) |

The functions that change Glue are called with `CALL`, outside a `BEGIN` transaction, since Glue has no transactions
(see [Writing](writing.md)).

The Hive SQL forms are available as well, through the `glue_hive_ddl` grammar extension the extension registers.
Grammar extensions are switched on per connection:

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

Actions can be chained in one statement (`ADD PARTITION (...) LOCATION '...' ADD PARTITION (...) ...`). The statement
becomes `CALL glue_alter_table(table, [actions])`: every action is checked against Glue before any is applied, so a
statement that fails changes nothing, and consecutive adds go out as one `BatchCreatePartition` call. The checks fetch
the partitions they look at with `BatchGetPartition`, and consecutive drops go out as `BatchDeletePartition` calls of
up to 25 partitions, so dropping partitions needs `glue:BatchGetPartition` and `glue:BatchDeletePartition`. The table
name may be partially qualified; it is resolved like in a query.

`glue_partitions` lists every registered partition with a typed column per partition key, its `location` and
`partition_values`, the strings Glue stores. A typed column is NULL for the NULL partition (Glue stores
`__HIVE_DEFAULT_PARTITION__`) and for a value the key's type cannot hold, such as `abc` registered by another tool on
an integer key; the latter fails every filtered read of the table, and `partition_values` is how to find it, one
condition per partition key:

```sql
SELECT location, partition_values
FROM glue_partitions('my_datalake.default.orders')
WHERE (dt IS NULL) <> (partition_values[1] = '__HIVE_DEFAULT_PARTITION__')
   OR (country IS NULL) <> (partition_values[2] = '__HIVE_DEFAULT_PARTITION__');
```

`glue_repair_table` lists the table location once (never from a listing kept by `glue_cache_listings`) and registers
the partitions Glue does not have yet with `BatchCreatePartition`. A partition is a directory with one `<key>=<value>`
component per partition key, in partition key order (keys compare case-insensitively); it is registered at that
directory, with the values unescaped and cast to the key types (`year=07` is `7`). Directories that do not follow this
scheme, whose values do not cast, that are already the location of a partition, or that hold no file a scan would read
(only `_`/`.` prefixed, `_$folder$` or empty objects) are ignored. It never removes partitions. A table with another
engine's InputFormat over a Hive SerDe (one INSERT refuses too) is refused: its files are not laid out the way Hive
writes them.

Listing the partitions of a table - `glue_partitions`, and planning a scan of a partitioned table - pages through
Glue's `GetPartitions`. A scan fetches them when planning first needs them (pruning, cardinality), not at bind, so
`DESCRIBE`, `CREATE VIEW` and `PREPARE` make no `GetPartitions` call; every scan of the table in a query shares one
fetch. The pages are asked for in parallel with Glue's Segment API:
`glue_get_partitions_segments` requests run at the same time, each over a segment of the partitions that does not
overlap with the others. `0`, the default, uses 8 requests against AWS and 1 against a Glue compatible server given
with `ENDPOINT` (moto ignores `Segment` and answers every segment with the whole table, so the partitions a segment
already returned are dropped); 10 is the maximum Glue accepts. The responses include every partition's storage
descriptor with its columns, which the scan checks the partitions it reads against.

Against AWS, listing the 2526 partitions of a TPC-H SF1 `lineitem` took ~2.1s before and ~0.45s with the default of
8 segments; `SET glue_get_partitions_segments = 1` restores one request at a time.
