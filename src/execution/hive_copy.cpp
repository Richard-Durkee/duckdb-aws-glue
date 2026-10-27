#include "execution/hive_copy.hpp"

#include "duckdb/common/exception.hpp"

#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_transaction.hpp"
#include "planning/hive_multi_file_reader.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

//! A CREATE TABLE AS writes only to a location without data files: the files of a dropped table there would read as
//! rows of the new one. Files a scan skips (_SUCCESS, directory markers, empty files) do not count. With 'overwrite'
//! they are returned, to be deleted when the statement commits.
static vector<string> CheckLocation(ClientContext &context, const GlueTableInfo &table_info, const string &location,
                                    bool overwrite) {
	vector<string> replaced;
	auto &fs = FileSystem::GetFileSystem(context);
	auto listing = fs.GlobFileList(location + "/**", FileGlobOptions::ALLOW_EMPTY);
	for (auto &file : listing->Files()) {
		if (!IsDataFile(file, location + "/")) {
			continue;
		}
		if (overwrite) {
			replaced.push_back(file.path);
			continue;
		}
		throw InvalidInputException("CREATE TABLE AS for Hive table '%s.%s' is not supported: its location '%s' "
		                            "already holds files (e.g. '%s'), such as those of a dropped table, which would "
		                            "read as rows of the new table. Delete them, or give the table another location",
		                            table_info.schema_name, table_info.name, location, file.path);
	}
	return replaced;
}

GlueHiveCopy::GlueHiveCopy(PhysicalPlan &physical_plan, vector<LogicalType> types, CopyFunction function,
                           unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality,
                           optional_ptr<GlueSchemaEntry> schema_p, unique_ptr<BoundCreateTableInfo> info_p,
                           GlueTableInfo table_info_p, bool overwrite_location_p, string staging_root_p)
    : PhysicalCopyToFile(physical_plan, std::move(types), std::move(function), std::move(bind_data),
                         estimated_cardinality),
      schema(schema_p), info(std::move(info_p)), table_info(std::move(table_info_p)),
      overwrite_location(overwrite_location_p), staging_root(std::move(staging_root_p)) {
	D_ASSERT((schema != nullptr) == (info != nullptr));
}

unique_ptr<GlobalSinkState> GlueHiveCopy::GetGlobalSinkState(ClientContext &context) const {
	auto &transaction = GlueTransaction::Get(context, *table_catalog.get_mutable());
	{
		annotated_lock_guard<annotated_mutex> guard(create_lock);
		if (!current_write || current_transaction != &transaction || current_write->complete) {
			auto location = table_info.location;
			StringUtil::RTrim(location, "/");
			auto write = make_uniq<GlueTableWrite>();
			write->schema_name = table_info.schema_name;
			write->table_name = table_info.name;
			write->location = location;
			write->staging_root = staging_root;
			write->file_prefix = file_prefix;
			write->started_ms = NumericCast<int64_t>(Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp()));
			if (info) {
				// CREATE TABLE AS: the table is created when the statement commits
				auto &create_schema = *schema.get_mutable();
				if (create_schema.TableExists(context, table_info.name)) {
					throw CatalogException("Table with name \"%s\" appeared between planning and execution; retry "
					                       "CREATE TABLE IF NOT EXISTS",
					                       table_info.name);
				}
				write->replaced_files = CheckLocation(context, table_info, location, overwrite_location);
				write->create_schema = &create_schema;
				write->create_table = make_uniq<GlueTableInfo>(table_info);
			}
			current_write = &transaction.writes.AddWrite(std::move(write));
			current_transaction = &transaction;
		}
	}
	return PhysicalCopyToFile::GetGlobalSinkState(context);
}

GlueTableWrite &GlueHiveCopy::CurrentWrite() const {
	annotated_lock_guard<annotated_mutex> guard(create_lock);
	D_ASSERT(current_write);
	return *current_write;
}

string GlueHiveCopy::GetName() const {
	return info ? "GLUE_HIVE_CREATE_TABLE_AS_COPY" : PhysicalCopyToFile::GetName();
}

} // namespace duckdb
