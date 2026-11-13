# Views

A Glue view is a table of type `VIRTUAL_VIEW`. The extension lists every view in a database, but it only queries,
replaces or drops the views it wrote itself.

## Creating views

```sql
CREATE VIEW lake.sales.big_orders AS SELECT * FROM orders WHERE amount > 1000;
```

`CREATE [OR REPLACE] VIEW [IF NOT EXISTS] db.v [(col, ...)] AS SELECT ...` creates a Glue view and marks it as
written by DuckDB. `CREATE SECURE VIEW` is accepted as well. `OR REPLACE` updates an existing view in place.

- Unqualified table names in the `SELECT` refer to the view's own database, so `orders` above is `lake.sales.orders`.
- The view's columns are stored with their DuckDB types, such as `HUGEINT` or `INTEGER`, not Glue types.
- The tables a view reads must exist when the view is created. `CREATE VIEW ... WITH (DEFER_BINDING)` is not
  supported yet.

One naming restriction applies. A column reference in the `SELECT` cannot start with the name of another catalog the
view reads, as in `cat.db.t.col` or `* EXCLUDE (cat.db.t.col)`. It also cannot start with the view's own catalog name
used as a row reference, as in `cat.t`, even when that name is also a table, alias or column. Use a table alias with a
different name instead.

## Views over Iceberg tables

A view can join the Hive and Iceberg tables of its Glue catalog. The extension does not read Iceberg tables itself, so
the Iceberg tables are read through an Iceberg attachment of the same catalog, named by the Glue attach option
`ICEBERG_CATALOG`:

```sql
ATTACH '123456789012' AS lake (TYPE GLUE, ICEBERG_CATALOG 'lake_ice');
ATTACH '123456789012' AS lake_ice (TYPE ICEBERG, ENDPOINT_TYPE GLUE);
CREATE VIEW lake.sales.orders_by_customer AS
SELECT c.name, o.amount FROM lake.sales.customers c JOIN lake_ice.sales.orders o ON c.id = o.id;
```

The view stores two versions of its SQL:

- `ViewExpandedText`, the SQL that runs, names both tables by their Glue names (`sales.customers`, `sales.orders`).
- `ViewOriginalText` keeps the SQL as it was submitted, including the attachment aliases.

When the view is read, its Iceberg references are mapped to the attachment that `ICEBERG_CATALOG` names. Reading an
Iceberg table directly through `lake` is still not supported; use the Iceberg attachment for that.

The attachment that `ICEBERG_CATALOG` names must have catalog type `iceberg`, and its warehouse must match the Glue
attachment's path or known catalog ID. Each mapped table must be registered as Iceberg in Glue. Configure both
attachments with the same region and endpoint, since the warehouse check does not verify those.

## Views over other catalogs

```sql
SET GLOBAL experimental_glue_view_external_catalog_types = ['duckdb', 'iceberg'];
```

With this setting, a view can also read tables of attached catalogs of the listed types. It is empty by default, and
it is global only, so it applies to every connection and `SET SESSION` is refused.

Such a table is stored in the view as `"external:<type>:<alias>".<schema>.<table>`. When the view is read, the table
is read from whichever catalog of that type is attached under that alias. Catalogs that are in memory, temporary,
system or attached hidden are never used.

In a view body that another tool wrote to Glue, a bare table name is read from the view's own Glue database.

## Listing, dropping and limits

`SHOW [ALL] TABLES`, `duckdb_views()`, `information_schema.views` and `duckdb_columns()` list every view of a
database, including views that other engines wrote.

`DROP VIEW [IF EXISTS] db.v` deletes a view. `ALTER VIEW ... RENAME` and `COMMENT ON VIEW` are not supported.
