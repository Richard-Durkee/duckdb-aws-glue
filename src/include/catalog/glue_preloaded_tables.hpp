#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/client_context_state.hpp"

#include "core/glue_info.hpp"

namespace duckdb {

//! The Glue definitions given to the connection with glue_preload_table, read instead of calling GetTable. They live
//! with the connection, so the statements after the one that gave a definition use it
class GluePreloadedTables : public ClientContextState {
public:
	//! The definition given for the table, or null
	static shared_ptr<const GlueTableInfo> Find(ClientContext &context, const string &catalog_name,
	                                            const string &database_name, const string &table_name);
	//! Replaces the definition given for the same table earlier
	static void Put(ClientContext &context, const string &catalog_name, shared_ptr<const GlueTableInfo> table);
	//! Drop them all: a mutation can have made every definition given before it stale
	static void Clear(ClientContext &context);

private:
	annotated_mutex lock;
	//! Keyed by catalog, database and table name, case folded (Glue names are case-insensitive)
	unordered_map<string, shared_ptr<const GlueTableInfo>> tables DUCKDB_GUARDED_BY(lock);
};

} // namespace duckdb
