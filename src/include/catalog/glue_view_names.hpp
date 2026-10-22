#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {
class ClientContext;
class GlueCatalog;

//! ViewExpandedText names tables of the view's own catalog <db>.<table>, of other Glue catalogs
//! "<catalog id>".<db>.<table>, of other kinds of catalogs (experimental_glue_view_enable_external_catalogs)
//! "external:<type>:<alias>".<schema>.<table>; readers map these to attached catalogs
struct GlueViewNames {
	//! CREATE VIEW: qualify bare table names with their database; a table of another catalog, named with its catalog,
	//! goes to its Glue catalog id or "external:<type>:<alias>". Throws for catalog-qualified columns, table functions,
	//! a bare name outside the view's catalog, another region or endpoint, no reported id, and in-memory catalogs.
	//! DEFER_BINDING: a missing bare name goes to the view's database
	static void QualifyForStorage(ClientContext &context, GlueCatalog &view_catalog, const Identifier &view_schema,
	                              QueryNode &query, bool defer_binding);
	//! the catalog ids the stored body names tables by
	static identifier_set_t ReferencedCatalogIds(QueryNode &query);
	//! id -> alias, for the given ids: an attached Glue catalog in the view's region and endpoint, or an attached
	//! catalog with the stored type and alias
	static identifier_tree_t<Identifier> AttachedAliases(GlueCatalog &view_catalog, const identifier_set_t &ids);
	//! reading: ids -> aliases; throws for an id not attached
	static void ResolveForReading(GlueCatalog &view_catalog, const string &view_name,
	                              const identifier_tree_t<Identifier> &aliases, QueryNode &query);
	//! reading: db.t -> catalog.db.t, as DuckDB binds x.t to a catalog x when one is attached under that name
	static void PinToCatalog(QueryNode &query, const Identifier &catalog_name);
};

} // namespace duckdb
