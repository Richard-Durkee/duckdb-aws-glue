#include "catalog/glue_transaction.hpp"
#include "catalog/glue_catalog.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

namespace {

bool ConnectionScopedMetadataCache(ClientContext &context) {
	Value scope;
	if (context.TryGetCurrentSetting("glue_metadata_cache_scope", scope) && !scope.IsNull()) {
		return StringUtil::Lower(StringValue::Get(scope)) == "connection";
	}
	// matches the registered default of glue_metadata_cache_scope: 'transaction'
	return false;
}

} // namespace

GlueTransaction::GlueTransaction(GlueCatalog &glue_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), glue_catalog(glue_catalog),
      transaction_state(GlueTransactionState::TRANSACTION_NOT_YET_STARTED) {
	if (ConnectionScopedMetadataCache(context)) {
		auto state = context.registered_state->GetOrCreate<GlueMetadataCacheState>("glue_metadata_cache");
		cache = state->GetCache(glue_catalog.GetName().GetIdentifierName(), glue_catalog.GetAttached().oid);
	} else {
		cache = make_shared_ptr<GlueTransactionCache>();
	}
}

GlueTransaction::~GlueTransaction() {
}

void GlueTransaction::Start() {
	transaction_state = GlueTransactionState::TRANSACTION_STARTED;
}

void GlueTransaction::Commit() {
	// a write that has not completed at a commit belongs to a statement that did not finish either
	UndoHiveWrites();
	if (transaction_state == GlueTransactionState::TRANSACTION_STARTED) {
		transaction_state = GlueTransactionState::TRANSACTION_FINISHED;
	}
}

void GlueTransaction::Rollback() {
	UndoHiveWrites();
	if (transaction_state == GlueTransactionState::TRANSACTION_STARTED) {
		transaction_state = GlueTransactionState::TRANSACTION_FINISHED;
	}
}

GlueTransaction &GlueTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<GlueTransaction>();
}

void GlueTransaction::AddHiveWrite(GlueHiveWrite write) {
	lock_guard<mutex> guard(hive_writes_lock);
	hive_writes.push_back(make_uniq<GlueHiveWrite>(std::move(write)));
}

optional_ptr<GlueHiveWrite> GlueTransaction::FindHiveWrite(const string &write_id) {
	lock_guard<mutex> guard(hive_writes_lock);
	for (auto &write : hive_writes) {
		if (write->write_id == write_id) {
			return write.get();
		}
	}
	return nullptr;
}

void GlueTransaction::CompleteHiveWrite(const string &write_id) {
	lock_guard<mutex> guard(hive_writes_lock);
	for (auto it = hive_writes.begin(); it != hive_writes.end(); ++it) {
		if ((*it)->write_id == write_id) {
			hive_writes.erase(it);
			return;
		}
	}
}

void GlueTransaction::UndoHiveWrites() {
	vector<unique_ptr<GlueHiveWrite>> writes;
	{
		lock_guard<mutex> guard(hive_writes_lock);
		writes = std::move(hive_writes);
		hive_writes.clear();
	}
	if (writes.empty()) {
		return;
	}
	auto client_context = context.lock();
	if (!client_context) {
		return;
	}
	for (auto &write : writes) {
		UndoHiveWrite(*client_context, glue_catalog, *write);
	}
}

void GlueTransaction::UndoHiveWrite(ClientContext &context, GlueCatalog &catalog, GlueHiveWrite &write) {
	auto table = write.database_name + "." + write.table_name;
	if (!write.files_deleted) {
		try {
			// the database's file system, not one that stops at the interrupt the failed query set to end its pipelines
			auto &fs = DatabaseInstance::GetDatabase(context).GetFileSystem();
			auto prefix = write.write_id + "_";
			vector<string> written_files;
			auto listing = fs.GlobFileList(write.location + "/**", FileGlobOptions::ALLOW_EMPTY);
			for (auto &file : listing->GetAllFiles()) {
				auto name = file.path.substr(file.path.find_last_of('/') + 1);
				if (StringUtil::StartsWith(name, prefix)) {
					written_files.push_back(file.path);
				}
			}
			if (!written_files.empty()) {
				fs.RemoveFiles(written_files);
			}
		} catch (std::exception &ex) {
			DUCKDB_LOG_WARNING(context, "Could not delete the files a failed write left in Hive table '%s': %s", table,
			                   ErrorData(ex).RawMessage());
		}
	}
	if (write.created_table_schema) {
		try {
			GlueAPI::DeleteTable(context, catalog, write.database_name, write.table_name);
			write.created_table_schema->tables.RemoveEntry(write.table_name);
		} catch (std::exception &ex) {
			DUCKDB_LOG_WARNING(context, "Could not drop Hive table '%s' after CREATE TABLE AS failed: %s", table,
			                   ErrorData(ex).RawMessage());
		}
	}
}

idx_t GlueTransactionCache::NextId() {
	static atomic<idx_t> next_id {0};
	return ++next_id;
}

optional_ptr<GlueTransactionCache> GlueTransactionCache::Of(ClientContext &context, Catalog &catalog) {
	auto transaction = Transaction::TryGet(context, catalog.GetAttached());
	if (!transaction) {
		return nullptr;
	}
	return transaction->Cast<GlueTransaction>().cache.get();
}

void GlueTransactionCache::Invalidate(ClientContext &context, Catalog &catalog) {
	// Get() not Of(): a connection-scoped cache outlives transactions and must be cleared even from a bare mutation
	GlueTransaction::Get(context, catalog).cache->Clear();
	// The scope setting can change between statements, so the active transaction's cache may not be the
	// connection-level one this write would otherwise leave stale: clear those too (all of them, matching
	// the rollback hook — a write through any alias of the same Glue catalog must not be hidden by another's cache)
	auto state = context.registered_state->Get<GlueMetadataCacheState>("glue_metadata_cache");
	if (state) {
		state->ClearAll();
	}
}

void GlueTransactionCache::OnDetach(ClientContext &context, Catalog &catalog) {
	auto state = context.registered_state->Get<GlueMetadataCacheState>("glue_metadata_cache");
	if (state) {
		state->EraseCache(catalog.GetName().GetIdentifierName(), catalog.GetAttached().oid);
	}
}

GlueTransactionCache::InvalidateOnExit::~InvalidateOnExit() {
	// A destructor must not throw: log what went wrong instead, so the swallowed exception is not silent
	try {
		Invalidate(context, catalog);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		DUCKDB_LOG_WARNING(context, "Could not invalidate the Glue metadata cache of catalog '%s': %s",
		                   catalog.GetName().GetIdentifierName(), error.RawMessage());
	} catch (...) {
		DUCKDB_LOG_WARNING(context, "Could not invalidate the Glue metadata cache of catalog '%s': unknown exception",
		                   catalog.GetName().GetIdentifierName());
	}
}

} // namespace duckdb
