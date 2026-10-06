#include "catalog/glue_transaction_manager.hpp"
#include "catalog/glue_catalog.hpp"

#include "duckdb/main/attached_database.hpp"

#include <algorithm>

namespace duckdb {

GlueTransactionManager::GlueTransactionManager(AttachedDatabase &db_p, GlueCatalog &glue_catalog)
    : TransactionManager(db_p), glue_catalog(glue_catalog) {
}

Transaction &GlueTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<GlueTransaction>(glue_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	transaction_sequence[result] = next_sequence++;
	return result;
}

ErrorData GlueTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &glue_transaction = transaction.Cast<GlueTransaction>();
	glue_transaction.Commit();
	lock_guard<mutex> l(transaction_lock);
	transaction_sequence.erase(transaction);
	transactions.erase(transaction);
	FreeRetiredEntries();
	return ErrorData();
}

void GlueTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &glue_transaction = transaction.Cast<GlueTransaction>();
	glue_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transaction_sequence.erase(transaction);
	transactions.erase(transaction);
	FreeRetiredEntries();
}

void GlueTransactionManager::RetireEntry(unique_ptr<CatalogEntry> entry) {
	lock_guard<mutex> l(transaction_lock);
	retired_entries.push_back(RetiredEntry {next_sequence, std::move(entry)});
}

void GlueTransactionManager::FreeRetiredEntries() {
	auto oldest_running = NumericLimits<idx_t>::Maximum();
	for (auto &entry : transaction_sequence) {
		oldest_running = MinValue(oldest_running, entry.second);
	}
	retired_entries.erase(
	    std::remove_if(retired_entries.begin(), retired_entries.end(),
	                   [&](const RetiredEntry &retired) { return retired.retired_at <= oldest_running; }),
	    retired_entries.end());
}

void GlueTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// nothing to checkpoint, all changes are already in Glue
}

} // namespace duckdb
