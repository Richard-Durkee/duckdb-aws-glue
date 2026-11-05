#include "execution/hive_copy.hpp"

#include "duckdb/common/exception.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_transaction.hpp"
#include "core/hive_location.hpp"
#include "planning/hive_multi_file_reader.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

//! 'overwrite' deletes files that must not belong to a table: refuse when a table of the same Glue database reads
//! them. Tables in other databases, catalogs or accounts are not checked.
static void ThrowIfLocationInUse(ClientContext &context, GlueSchemaEntry &schema, const GlueTableInfo &table_info,
                                 const string &location) {
	auto &catalog = schema.catalog.Cast<GlueCatalog>();
	auto target = ComparableLocation(location);
	auto tables = GlueAPI::GetTables(context, catalog, schema.GetDatabaseName(), false, nullptr, true);
	for (auto &table : tables) {
		if (table.location.empty() || !LocationsOverlap(target, ComparableLocation(table.location))) {
			continue;
		}
		throw InvalidInputException("CREATE TABLE AS for Hive table '%s.%s' WITH (overwrite = true) is not "
		                            "supported: its location '%s' overlaps the location '%s' of table '%s.%s', whose "
		                            "files it would delete. Give the table another location",
		                            table_info.schema_name, table_info.name, location, table.location,
		                            table_info.schema_name, table.name);
	}
}

//! A CREATE TABLE AS writes only to a location without data files: the files of a dropped table there would read as
//! rows of the new one. Files a scan skips (_SUCCESS, directory markers, empty files) do not count. With 'overwrite'
//! the data files are returned, to be deleted when the statement commits.
static vector<string> CheckLocation(ClientContext &context, GlueSchemaEntry &schema, const GlueTableInfo &table_info,
                                    const string &location, bool overwrite) {
	// the location is listed with a glob, which would miss its own files and match those of other prefixes
	if (location.find_first_of("*?[") != string::npos) {
		throw InvalidInputException("CREATE TABLE AS for Hive table '%s.%s' is not supported: its location '%s' "
		                            "contains a glob character (*, ? or [)",
		                            table_info.schema_name, table_info.name, location);
	}
	auto &fs = FileSystem::GetFileSystem(context);
	auto listing = fs.GlobFileList(location + "/**", FileGlobOptions::ALLOW_EMPTY);
	vector<string> data_files;
	for (auto &file : listing->Files()) {
		if (!IsDataFile(file, location + "/")) {
			continue;
		}
		if (!overwrite) {
			throw InvalidInputException("CREATE TABLE AS for Hive table '%s.%s' is not supported: its location '%s' "
			                            "already holds files (e.g. '%s'), such as those of a dropped table, which "
			                            "would read as rows of the new table. Delete them, give the table another "
			                            "location, or create it WITH (overwrite = true) to delete them",
			                            table_info.schema_name, table_info.name, location, file.path);
		}
		data_files.push_back(file.path);
	}
	if (!data_files.empty()) {
		ThrowIfLocationInUse(context, schema, table_info, location);
	}
	return data_files;
}

GlueHiveCopy::GlueHiveCopy(PhysicalPlan &physical_plan, vector<LogicalType> types, CopyFunction function,
                           unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality,
                           optional_ptr<GlueSchemaEntry> schema_p, unique_ptr<BoundCreateTableInfo> info_p,
                           GlueTableInfo table_info_p, string staging_root_p)
    : PhysicalCopyToFile(physical_plan, std::move(types), std::move(function), std::move(bind_data),
                         estimated_cardinality),
      schema(schema_p), info(std::move(info_p)), table_info(std::move(table_info_p)),
      staging_root(std::move(staging_root_p)) {
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
			write->table = table_info;
			if (info) {
				// CREATE TABLE AS: the table is created when the statement commits
				auto &create_schema = *schema.get_mutable();
				create_schema.ThrowIfTableExists(context, info->Base());
				auto overwrite = GlueSchemaEntry::ParseCreateTableOptions(context, info->Base()).overwrite;
				write->replaced_files = CheckLocation(context, create_schema, table_info, location, overwrite);
				write->create_schema = &create_schema;
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
