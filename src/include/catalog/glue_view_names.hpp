#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {
class ClientContext;
class GlueCatalog;

//! ViewExpandedText names every table <db>.<table>; read in the view's own catalog
struct GlueViewNames {
	//! CREATE VIEW: qualify bare table names with their database. Throws for table functions and for tables outside
	//! the view's Glue catalog. DEFER_BINDING: a missing table goes to the view's database
	static void QualifyForStorage(ClientContext &context, GlueCatalog &view_catalog, const Identifier &view_schema,
	                              QueryNode &query, bool defer_binding);
	//! reading: db.t -> catalog.db.t, as DuckDB binds x.t to a catalog x when one is attached under that name
	static void PinToCatalog(QueryNode &query, const Identifier &catalog_name);
};

} // namespace duckdb
