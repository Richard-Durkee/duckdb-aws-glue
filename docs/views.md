# Views

- `CREATE [OR REPLACE] VIEW [IF NOT EXISTS] db.v [(col, ...)] AS SELECT ...` and `CREATE SECURE VIEW` create a Glue
  view (a table of type `VIRTUAL_VIEW`) marked as written by DuckDB. `OR REPLACE` updates it in place. Unqualified table
  names in the SELECT refer to the view's own database. The view's columns store their DuckDB types (e.g. `HUGEINT`,
  `INTEGER`), not Glue types.
- `CREATE VIEW ... WITH (DEFER_BINDING)` is not supported yet: the tables a view reads must exist when it is created.
- A column in the SELECT can not start with the name of another catalog the view reads (`cat.db.t.col`,
  `* EXCLUDE (cat.db.t.col)`), or with its own catalog's name as a row reference (`cat.t`), even when that name is a
  table, alias or column: use a table alias with another name.
- With `SET experimental_glue_view_external_catalog_types = ['duckdb', 'iceberg']` (empty by default; global only, so
  it applies to every connection and `SET SESSION` is refused), views can also read tables of catalogs of those types, stored as `"external:<type>:<alias>".<schema>.<table>` and read wherever a
  catalog of that type is attached under that alias, never from one in memory, temporary, system or attached hidden. A
  bare table name in a body written to Glue by other means is read from the view's own Glue database.
- A view can join Hive and Iceberg tables of its Glue catalog. Iceberg tables are read through an existing Iceberg attachment, named by the Glue attach option `ICEBERG_CATALOG`:

  ```sql
  ATTACH '123456789012' AS lake (TYPE GLUE, ICEBERG_CATALOG 'lake_ice');
  ATTACH '123456789012' AS lake_ice (TYPE ICEBERG, ENDPOINT_TYPE GLUE);
  CREATE VIEW lake.sales.orders_by_customer AS
  SELECT c.name, o.amount FROM lake.sales.customers c JOIN lake_ice.sales.orders o ON c.id = o.id;
  ```

  `ViewExpandedText`, the executable view SQL, stores both tables by their Glue names (`sales.customers`, `sales.orders`). `ViewOriginalText` keeps the submitted SQL, including its attachment aliases. When the view is read, its Iceberg references are mapped to the attachment named by `ICEBERG_CATALOG`. Direct Iceberg reads through `lake` remain unsupported; use the Iceberg attachment.

  The named attachment must have catalog type `iceberg`. Its warehouse must match the Glue attachment path or known catalog ID, and each mapped table must be registered as Iceberg in Glue. Configure both attachments for the same region and endpoint; these are not verified by the warehouse check.
- `SHOW [ALL] TABLES`, `duckdb_views()`, `information_schema.views` and `duckdb_columns()` list every view of a database,
  including views written by other engines. Only views written by DuckDB can be queried, replaced or dropped.
- `DROP VIEW [IF EXISTS] db.v` deletes the view.
- `ALTER VIEW ... RENAME` and `COMMENT ON VIEW` are not supported.
