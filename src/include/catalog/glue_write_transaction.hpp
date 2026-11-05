#pragma once

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"

#include "execution/glue_hive_write.hpp"

namespace duckdb {
class GlueCatalog;
class GlueTransactionCache;

//! The Hive writes of one transaction on one Glue catalog. A write is visible to no reader until commit: new
//! partitions are not registered, a CTAS table not created and staged files not copied before then. Commit applies
//! the writes in order (delete replaced files, publish staged files, create the table, register partitions with
//! retries) and undoes them all if a step fails; rollback deletes every file written and every Glue entry created.
class GlueWriteTransaction {
public:
	GlueHiveWrite &AddWrite(unique_ptr<GlueHiveWrite> write);
	//! The write of an execution, null if it has not started
	optional_ptr<GlueHiveWrite> FindWrite(const string &write_id);
	bool HasWrites();
	//! Throws after undoing everything when a step fails
	void Commit(ClientContext &context, GlueCatalog &catalog, GlueTransactionCache &cache);
	//! Never throws: what could not be undone is returned
	ErrorData Rollback(ClientContext &context, GlueCatalog &catalog, GlueTransactionCache &cache);

private:
	ErrorData RollbackInternal(ClientContext &context, GlueCatalog &catalog, GlueTransactionCache &cache);

	mutex lock;
	vector<unique_ptr<GlueHiveWrite>> writes;
};

} // namespace duckdb
