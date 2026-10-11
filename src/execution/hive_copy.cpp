#include "execution/hive_copy.hpp"

#include "duckdb/common/exception.hpp"

#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"

namespace duckdb {

GlueHiveCopy::GlueHiveCopy(PhysicalPlan &physical_plan, vector<LogicalType> types, CopyFunction function,
                           unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality,
                           optional_ptr<GlueSchemaEntry> schema_p, unique_ptr<BoundCreateTableInfo> info_p,
                           GlueTableInfo table_info_p)
    : PhysicalCopyToFile(physical_plan, std::move(types), std::move(function), std::move(bind_data),
                         estimated_cardinality),
      schema(schema_p), info(std::move(info_p)), table_info(std::move(table_info_p)) {
	D_ASSERT((schema != nullptr) == (info != nullptr));
}

unique_ptr<GlobalSinkState> GlueHiveCopy::GetGlobalSinkState(ClientContext &context) const {
	if (info) {
		// CREATE TABLE AS
		annotated_lock_guard<annotated_mutex> guard(create_lock);
		if (!table_created) {
			auto &create_schema = *schema.get_mutable();
			auto &catalog = create_schema.catalog;
			auto created = create_schema.CreateTableFromInfo(catalog.GetCatalogTransaction(context), *info, table_info);
			if (!created) {
				throw CatalogException("Table with name \"%s\" appeared between planning and execution; retry CREATE "
				                       "TABLE IF NOT EXISTS",
				                       table_info.name);
			}
			table_created = true;
		}
	}
	return PhysicalCopyToFile::GetGlobalSinkState(context);
}

string GlueHiveCopy::GetName() const {
	return info ? "GLUE_HIVE_CREATE_TABLE_AS_COPY" : PhysicalCopyToFile::GetName();
}

} // namespace duckdb
