#include "execution/glue_hive_write.hpp"

#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_schema_set.hpp"
#include "core/hive_location.hpp"
#include "planning/hive_multi_file_reader.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

string GlueHiveWrite::FinalPath(const string &file) const {
	if (staging_root.empty() || !StringUtil::StartsWith(file, staging_root + "/")) {
		return file;
	}
	return location + file.substr(staging_root.size());
}

string GlueHiveWrite::JournalPath() const {
	return staging_root + "/" + STAGING_JOURNAL_NAME;
}

void GlueHiveWrite::WriteJournal(FileSystem &fs) const {
	string contents;
	for (auto &file : files) {
		contents += FinalPath(file) + "\n";
	}
	auto handle = fs.OpenFile(JournalPath(), FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	handle->Write(&contents[0], contents.size());
	handle->Sync();
	handle->Close();
}

void GlueHiveWrite::CheckExistingPartitionLocations(ClientContext &context, GlueCatalog &catalog,
                                                    const vector<GluePartitionInput> &already_existing) const {
	auto check = [&](const GluePartitionInput &partition, const string &registered) {
		if (ComparableLocation(registered) != ComparableLocation(partition.location)) {
			throw IOException("rows of partition [%s] were written to '%s', but the partition is registered at '%s'",
			                  StringUtil::Join(partition.values, ", "), partition.location, registered);
		}
	};
	vector<reference<const GluePartitionInput>> unplanned;
	vector<vector<string>> unplanned_values;
	for (auto &partition : already_existing) {
		auto planned = planned_locations.find(partition.values);
		if (planned != planned_locations.end()) {
			check(partition, planned->second);
		} else {
			unplanned.push_back(partition);
			unplanned_values.push_back(partition.values);
		}
	}
	if (unplanned.empty()) {
		return;
	}
	auto locations = GlueAPI::PartitionLocations(context, catalog, database_name, table_name, unplanned_values);
	for (auto &entry : unplanned) {
		auto &partition = entry.get();
		auto registered = locations.find(partition.values);
		if (registered == locations.end()) {
			throw IOException("partition [%s] existed when its files were registered, but no longer does",
			                  StringUtil::Join(partition.values, ", "));
		}
		check(partition, registered->second);
	}
}

static void RemoveFilesBestEffort(FileSystem &fs, const vector<string> &files) {
	if (files.empty()) {
		return;
	}
	try {
		fs.RemoveFiles(files);
	} catch (std::exception &) {
		for (auto &file : files) {
			fs.TryRemoveFile(file);
		}
	}
}

void GlueHiveWrite::Undo(ClientContext &context, GlueCatalog &catalog) {
	auto table = database_name + "." + table_name;
	// the database's file system, not one that stops at the interrupt the failed query set to end its pipelines
	auto &fs = DatabaseInstance::GetDatabase(context).GetFileSystem();
	// CREATE TABLE AS: whether the location holds data files of another writer, which a dropped table would leave
	// behind unread. Counted as ClearLocation does, so files a scan skips (_SUCCESS, markers) do not keep the table
	bool others_files = false;
	bool listed = false;
	if (complete && !created_table) {
		// the statement finished, so its files are known: deleted by name, without listing the table
		auto known_files = files;
		known_files.insert(known_files.end(), published_files.begin(), published_files.end());
		if (journal_written) {
			known_files.push_back(JournalPath());
		}
		DUCKDB_LOG_INFO(context, "Undoing a failed write into Hive table '%s': deleting its %llu files by name", table,
		                known_files.size());
		try {
			RemoveFilesBestEffort(fs, known_files);
		} catch (std::exception &ex) {
			DUCKDB_LOG_WARNING(context, "Could not delete the files a failed write left in Hive table '%s': %s", table,
			                   ErrorData(ex).RawMessage());
		}
		return;
	}
	// a statement that did not finish: its files are found by their write id. A staged write into an unpartitioned
	// table wrote only below its staging root; one into a partitioned table may also have written new partitions in
	// place, so the whole location is listed
	auto undo_root =
	    !staging_root.empty() && this->table.partition_keys.empty() && !created_table ? staging_root : location;
	if (LocationHasGlob(undo_root)) {
		// the listing below would read the location as a pattern
		DUCKDB_LOG_WARNING(context,
		                   "Could not list the location of Hive table '%s' to undo a failed write: '%s' contains a "
		                   "glob character",
		                   table, undo_root);
	} else {
		DUCKDB_LOG_INFO(context, "Undoing a failed write into Hive table '%s': listing '%s' for its files", table,
		                undo_root);
		try {
			auto prefix = write_id + "_";
			auto data_prefix = location + "/";
			vector<string> written_files;
			// S3 lists by key prefix, not by file name, so the files of the write are found by listing the location
			auto listing = ListLocation(fs, undo_root);
			for (auto &file : listing->GetAllFiles()) {
				auto name = file.path.substr(file.path.find_last_of('/') + 1);
				if (StringUtil::StartsWith(name, prefix)) {
					written_files.push_back(file.path);
				} else if (created_table && IsDataFile(file, data_prefix)) {
					others_files = true;
				}
			}
			listed = true;
			if (journal_written) {
				written_files.push_back(JournalPath());
			}
			written_files.insert(written_files.end(), published_files.begin(), published_files.end());
			RemoveFilesBestEffort(fs, written_files);
		} catch (std::exception &ex) {
			DUCKDB_LOG_WARNING(context, "Could not delete the files a failed write left in Hive table '%s': %s", table,
			                   ErrorData(ex).RawMessage());
		}
	}
	if (!created_table) {
		return;
	}
	if (!listed || others_files) {
		// another writer's files would be left in a table nobody reads: keep the table
		DUCKDB_LOG_WARNING(context, "Kept Hive table '%s' after CREATE TABLE AS failed: %s", table,
		                   listed ? "it holds files another writer added" : "its location could not be listed");
		return;
	}
	try {
		if (created_table_time.IsValid()) {
			auto create_time = GlueAPI::GetTableCreateTime(client, catalog, database_name, table_name);
			if (create_time.IsValid() && create_time.GetIndex() != created_table_time.GetIndex()) {
				// most likely dropped and created again by someone else while the statement ran: keep it
				DUCKDB_LOG_WARNING(context,
				                   "Kept Hive table '%s' after CREATE TABLE AS failed: it was created again by another "
				                   "client while the statement ran",
				                   table);
				return;
			}
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
