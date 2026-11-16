#include "execution/glue_hive_write.hpp"

#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_schema_set.hpp"
#include "core/hive_location.hpp"
#include "planning/hive_multi_file_reader.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

bool GlueHiveWrite::OwnsTable(const GlueTableInfo &table) const {
	auto table_write_id = table.GetUnfinishedWriteId();
	if (table_write_id == write_id) {
		return true;
	}
	if (!removal_sent || !table_write_id.empty() || table.version_id == removal_version_id) {
		return false;
	}
	// Glue keeps whole seconds, and a table created again starts at the version the dropped one did
	return !removal_create_time.IsValid() || table.create_time_ms <= 0 ||
	       static_cast<idx_t>(table.create_time_ms) == removal_create_time.GetIndex();
}

void GlueHiveWrite::ThrowIfNotOwnTable(const GlueTableInfo &table) const {
	if (!OwnsTable(table)) {
		throw CatalogException("Glue table \"%s.%s\" was dropped and created again by another client while this "
		                       "CREATE TABLE AS ran",
		                       table.schema_name, table.name);
	}
}

void GlueHiveWrite::Undo(ClientContext &context, GlueCatalog &catalog) {
	auto table = database_name + "." + table_name;
	// CREATE TABLE AS: whether the location holds data files of another writer, which a dropped table would leave
	// behind unread. Counted as ClearLocation does, so files a scan skips (_SUCCESS, markers) do not keep the table
	bool others_files = false;
	bool listed = false;
	if (LocationHasGlob(location)) {
		// the listing below would read the location as a pattern
		DUCKDB_LOG_WARNING(context,
		                   "Could not list the location of Hive table '%s' to undo a failed write: '%s' contains a "
		                   "glob character",
		                   table, location);
	} else if (!files_deleted || created_table) {
		try {
			// the database's file system, not one that stops at the interrupt the failed query set to end its pipelines
			auto &fs = DatabaseInstance::GetDatabase(context).GetFileSystem();
			auto prefix = write_id + "_";
			auto data_prefix = location + "/";
			vector<string> written_files;
			// S3 lists by key prefix, not by file name, so the files of the write are found by listing the location
			auto listing = ListLocation(fs, location);
			for (auto &file : listing->GetAllFiles()) {
				auto name = file.path.substr(file.path.find_last_of('/') + 1);
				if (StringUtil::StartsWith(name, prefix)) {
					written_files.push_back(file.path);
				} else if (created_table && IsDataFile(file, data_prefix)) {
					others_files = true;
				}
			}
			listed = true;
			if (!files_deleted && !written_files.empty()) {
				fs.RemoveFiles(written_files);
			}
		} catch (std::exception &ex) {
			DUCKDB_LOG_WARNING(context, "Could not delete the files a failed write left in Hive table '%s': %s", table,
			                   ErrorData(ex).RawMessage());
		}
	}
	if (!created_table) {
		return;
	}
	try {
		GlueTableInfo current;
		if (!GlueAPI::GetTable(client, catalog, database_name, table_name, current)) {
			return;
		}
		if (!OwnsTable(current)) {
			// not created by the statement, or dropped and created again by someone else while it ran: keep it
			DUCKDB_LOG_WARNING(context,
			                   "Kept Hive table '%s' after CREATE TABLE AS failed: it is not the table the statement "
			                   "created",
			                   table);
			return;
		}
		if (!listed || others_files) {
			// another writer's files would be left in a table nobody reads: keep the table
			DUCKDB_LOG_WARNING(context, "Kept Hive table '%s' after CREATE TABLE AS failed: %s", table,
			                   listed ? "it holds files another writer added" : "its location could not be listed");
			return;
		}
		GlueAPI::DeleteTable(context, client, catalog, database_name, table_name);
		// looked up again by name: the schema entry the statement used may have been replaced since
		auto schema = catalog.GetSchemas().GetCachedEntry(database_name);
		if (schema) {
			schema->Cast<GlueSchemaEntry>().tables.RemoveEntry(table_name);
		}
	} catch (std::exception &ex) {
		DUCKDB_LOG_WARNING(context, "Could not drop Hive table '%s' after CREATE TABLE AS failed: %s", table,
		                   ErrorData(ex).RawMessage());
	}
}

} // namespace duckdb
