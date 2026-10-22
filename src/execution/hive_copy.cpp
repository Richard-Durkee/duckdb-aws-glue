#include "execution/hive_copy.hpp"

#include "duckdb/common/exception.hpp"

#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_transaction.hpp"

namespace duckdb {

GlueHiveCopy::GlueHiveCopy(PhysicalPlan &physical_plan, vector<LogicalType> types, CopyFunction function,
                           unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality, GlueCatalog &catalog_p,
                           optional_ptr<GlueSchemaEntry> schema_p, unique_ptr<BoundCreateTableInfo> info_p,
                           GlueTableInfo table_info_p, string write_id_p, string location_p)
    : PhysicalCopyToFile(physical_plan, std::move(types), std::move(function), std::move(bind_data),
                         estimated_cardinality),
      catalog(catalog_p), schema(schema_p), info(std::move(info_p)), table_info(std::move(table_info_p)),
      write_id(std::move(write_id_p)), location(std::move(location_p)) {
	D_ASSERT((schema != nullptr) == (info != nullptr));
}

unique_ptr<GlobalSinkState> GlueHiveCopy::GetGlobalSinkState(ClientContext &context) const {
	annotated_lock_guard<annotated_mutex> guard(start_lock);
	if (!write_started) {
		GlueHiveWrite write;
		write.write_id = write_id;
		write.database_name = table_info.database_name;
		write.table_name = table_info.name;
		write.location = location;
		if (info) {
			// CREATE TABLE AS
			auto &create_schema = *schema.get_mutable();
			auto created = create_schema.CreateTableFromInfo(catalog.GetCatalogTransaction(context), *info, table_info);
			if (!created) {
				throw CatalogException("Table with name \"%s\" appeared between planning and execution; retry CREATE "
				                       "TABLE IF NOT EXISTS",
				                       table_info.name);
			}
			write.created_table_schema = schema;
		}
		GlueTransaction::Get(context, catalog).AddHiveWrite(std::move(write));
		write_started = true;
	}
	return PhysicalCopyToFile::GetGlobalSinkState(context);
}

string GlueHiveCopy::GetName() const {
	return info ? "GLUE_HIVE_CREATE_TABLE_AS_COPY" : PhysicalCopyToFile::GetName();
}

} // namespace duckdb
