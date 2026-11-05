#pragma once

#include "duckdb/common/map.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"

#include "api/glue_api.hpp"
#include "core/glue_info.hpp"

namespace duckdb {
class ClientContext;
class GlueCatalog;
class GlueSchemaEntry;

//! What an INSERT or CREATE TABLE AS into a Hive table writes and the Glue changes that make it visible, applied when
//! its transaction commits and undone when it rolls back
struct GlueHiveWrite {
	string write_id;
	string database_name;
	string table_name;
	//! The files of the write are named <write_id>_..., below this location
	string location;
	//! Staged writes: the hidden directory below the location the files were written to, copied to the location at
	//! commit. Empty: the files were written in place
	string staging_root;
	//! The definition the partitions are registered against: the bound table of an INSERT; for a CREATE TABLE AS the
	//! definition resolved at planning, replaced at commit by the table as created
	GlueTableInfo table;
	//! CREATE TABLE AS: the schema the table is created in at commit
	optional_ptr<GlueSchemaEntry> create_schema;
	//! CREATE TABLE AS with overwrite: the data files at the location before the statement, deleted at commit
	vector<string> replaced_files;
	//! The files the statement wrote, as written (below 'staging_root' when staged); only known once it completes
	vector<string> files;
	//! The partitions to register at commit, at their final locations
	vector<GluePartitionInput> partitions;
	//! The locations the plan saw for the partitions it writes into
	map<vector<string>, string> planned_locations;
	//! Whether the statement completed; an incomplete write is only rolled back
	bool complete = false;

	//! Progress of the commit, so that a failed commit undoes exactly what it did
	vector<string> published_files;
	vector<vector<string>> created_partitions;
	//! The commit created the table, and the client to drop it with
	bool created_table = false;
	std::shared_ptr<Aws::Glue::GlueClient> client;
	//! The CreateTime of the table it created: a table with another CreateTime was created again by someone else, and
	//! is not dropped. Glue keeps whole seconds, so a table dropped and created again within the same second is not
	//! told apart
	optional_idx created_table_time;

public:
	//! The location a written file has once published
	string FinalPath(const string &file) const;
	//! Fails when a partition the files were written for already existed at another location, whose files are the
	//! ones read. A partition the plan saw is compared with the location it had then; one it did not see was
	//! registered by another writer since, and is looked up.
	void CheckExistingPartitionLocations(ClientContext &context, GlueCatalog &catalog,
	                                     const vector<GluePartitionInput> &already_existing) const;
	//! Undo the files of the write and, for CREATE TABLE AS, drop the table its commit created. Best effort: a step
	//! that fails is logged
	void Undo(ClientContext &context, GlueCatalog &catalog);
};

} // namespace duckdb
