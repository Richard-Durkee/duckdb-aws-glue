#include "catalog/glue_preloaded_tables.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

static constexpr const char *GLUE_PRELOADED_TABLES = "glue_preloaded_tables";

static string PreloadKey(const string &catalog_name, const string &database_name, const string &table_name) {
	return StringUtil::Lower(catalog_name + "\n" + database_name + "\n" + table_name);
}

shared_ptr<const GlueTableInfo> GluePreloadedTables::Find(ClientContext &context, const string &catalog_name,
                                                          const string &database_name, const string &table_name) {
	auto state = context.registered_state->Get<GluePreloadedTables>(GLUE_PRELOADED_TABLES);
	if (!state) {
		return nullptr;
	}
	annotated_lock_guard<annotated_mutex> guard(state->lock);
	auto entry = state->tables.find(PreloadKey(catalog_name, database_name, table_name));
	return entry == state->tables.end() ? nullptr : entry->second;
}

void GluePreloadedTables::Put(ClientContext &context, const string &catalog_name,
                              shared_ptr<const GlueTableInfo> table) {
	auto state = context.registered_state->GetOrCreate<GluePreloadedTables>(GLUE_PRELOADED_TABLES);
	auto key = PreloadKey(catalog_name, table->schema_name, table->name);
	annotated_lock_guard<annotated_mutex> guard(state->lock);
	state->tables[key] = std::move(table);
}

void GluePreloadedTables::Clear(ClientContext &context) {
	auto state = context.registered_state->Get<GluePreloadedTables>(GLUE_PRELOADED_TABLES);
	if (!state) {
		return;
	}
	annotated_lock_guard<annotated_mutex> guard(state->lock);
	state->tables.clear();
}

} // namespace duckdb
