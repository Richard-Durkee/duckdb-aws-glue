#pragma once

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"

#include "api/glue_api.hpp"
#include "core/glue_info.hpp"

namespace duckdb {
class GlueCatalog;
class GlueSchemaEntry;
class GlueTransactionCache;

//! One Hive table written by one statement of a transaction: the files it wrote and the Glue changes that make them
//! visible, applied when the transaction commits and undone when it rolls back
struct GlueTableWrite {
	string schema_name;
	string table_name;
	//! the table location, without a trailing '/'
	string location;
	//! staged writes: the hidden directory below the location the files were written to, copied to the location at
	//! commit. Empty: the files were written in place
	string staging_root;
	//! CREATE TABLE AS: the table is created in Glue at commit
	optional_ptr<GlueSchemaEntry> create_schema;
	unique_ptr<GlueTableInfo> create_table;
	//! CREATE TABLE AS with overwrite: the data files at the location before the statement, deleted at commit
	vector<string> replaced_files;
	//! the files the statement wrote, as written (below 'staging_root' when staged)
	vector<string> files;
	//! the partitions to register at commit, at their final locations
	vector<GluePartitionInput> partitions;
	//! whether the statement completed; an incomplete write is only rolled back
	bool complete = false;

	//! progress of the commit, so that a failed commit undoes exactly what it did
	vector<string> published_files;
	vector<vector<string>> created_partitions;
	bool table_created = false;

	//! The location a written file has once published
	string FinalPath(const string &file) const;
};

//! The Hive writes of one transaction on one Glue catalog. A write is visible to no reader until commit: new
//! partitions are not registered, a CTAS table not created and staged files not copied before then. Commit applies
//! the writes in order (delete replaced files, publish staged files, create the table, register partitions with
//! retries) and undoes them all if a step fails; rollback deletes every file written and every Glue entry created.
class GlueWriteTransaction {
public:
	GlueTableWrite &AddWrite(unique_ptr<GlueTableWrite> write);
	bool HasWrites();
	//! Throws after undoing everything when a step fails
	void Commit(ClientContext &context, GlueCatalog &catalog, GlueTransactionCache &cache);
	//! Never throws: what could not be undone is returned
	ErrorData Rollback(ClientContext &context, GlueCatalog &catalog, GlueTransactionCache &cache);

private:
	ErrorData RollbackInternal(ClientContext &context, GlueCatalog &catalog, GlueTransactionCache &cache);

	mutex lock;
	vector<unique_ptr<GlueTableWrite>> writes;
};

} // namespace duckdb
