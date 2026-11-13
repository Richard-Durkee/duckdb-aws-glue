# DuckDB Glue extension

**Experimental.** This extension attaches an AWS Glue Data Catalog as a DuckDB catalog, so you can query the Hive
tables it lists, write to them, and create or alter tables, partitions and views, all with ordinary DuckDB SQL.

```sql
CREATE SECRET (TYPE S3, PROVIDER credential_chain, REGION 'eu-central-1');
ATTACH '123456789012' AS lake (TYPE GLUE);

SHOW ALL TABLES;
SELECT * FROM lake.sales.orders WHERE dt = '2026-11-01';

CREATE TABLE lake.sales.daily PARTITIONED BY (dt) AS
SELECT dt, count(*) AS orders FROM lake.sales.orders GROUP BY dt;
```

Writes go straight to Glue and S3, and Glue has no transactions. Other engines (Trino, Spark and Hive) each write Hive
tables in slightly different ways, and while we have matched the ones we know of, we likely have not found every
difference yet; please be careful when writing to production tables.

## What works

| Table in Glue | Read | Write |
|---|---|---|
| Hive, Parquet | yes | yes |
| Hive, CSV (LazySimpleSerDe, OpenCSVSerde) | yes | yes |
| Hive, JSON (Hive JsonSerDe) | yes | yes |
| Hive, JSON (OpenX JsonSerDe) | yes | no |
| Hive, Avro | yes (the avro extension is loaded when needed) | yes |
| Hive, bucketed | yes | no |
| Iceberg | through an Iceberg attachment, see [Views](docs/views.md) | no |
| Delta, Hudi, symlink manifests | listed only | no |
| ORC, Ion and other SerDes | no | no |

| Statement | Supported |
|---|---|
| `SELECT` | yes |
| `INSERT INTO`, `CREATE TABLE ... AS` | yes |
| `CREATE` / `ALTER` / `DROP` for schemas, tables and views | yes, with limits ([Writing](docs/writing.md)) |
| Hive partition DDL (`ADD PARTITION`, `MSCK REPAIR`, ...) | yes ([Partitions](docs/partitions.md)) |
| `DELETE`, `UPDATE`, `MERGE INTO` | no |
| DDL or `INSERT` inside `BEGIN ... COMMIT` | no |

## Attach options

```sql
ATTACH '<account id>' AS lake (TYPE GLUE, REGION 'us-east-1', READ_ONLY);
```

| Option | Meaning |
|---|---|
| `SECRET` | The S3 secret that supplies the AWS credentials. Default: the default secret. |
| `REGION` | Region of the catalog. Default: the secret's region. |
| `ENDPOINT` | A different Glue endpoint, such as `http://localhost:5000` for a local moto server. |
| `DEFAULT_LOCATION` | S3 prefix for new databases, and for new tables created without a location. It takes precedence over the database's location. |
| `DEFAULT_SCHEMA` | The Glue database used when a query names no schema. |
| `READ_ONLY` | Refuse every statement and `glue_*` function that would change the catalog. |
| `ICEBERG_CATALOG` | An attached Iceberg catalog over the same Glue catalog. Glue views read their Iceberg tables from it. |

## Documentation

- [Reading Hive tables](docs/reading.md): file listing, partition pruning, statistics and schema evolution
- [Text formats](docs/text-formats.md): how CSV and JSON SerDe properties are read and written
- [Writing](docs/writing.md): `CREATE`, `INSERT`, `ALTER` and `DROP`
- [Partitions](docs/partitions.md): the partition functions and Hive partition SQL
- [Views](docs/views.md)
- [Reading without a catalog: `hive_scan`](docs/hive-scan.md)
- [Inspecting tables and logging](docs/inspecting.md): raw Glue definitions, HTTP and scan logs
- [Settings](docs/settings.md): every setting with its default
- [Embedding (C++)](docs/embedding.md): reading a scan's files elsewhere, per-table credentials
- [Development](docs/development.md): building, tests, CI and benchmarks
