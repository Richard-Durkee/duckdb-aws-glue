#include "catalog/glue_transaction_manager.hpp"
#include "catalog/glue_catalog.hpp"

#include "duckdb/main/attached_database.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"

#include <algorithm>

namespace duckdb {

GlueTransactionManager::GlueTransactionManager(AttachedDatabase &db_p, GlueCatalog &glue_catalog)
    : TransactionManager(db_p), glue_catalog(glue_catalog) {
}

//! A transaction commits or rolls back after it stopped being the connection's active transaction, but the Glue and S3
//! calls that apply or undo its writes look up secrets through the active transaction: they run in a transaction of
//! their own
//! The undo of a failed statement runs after the error interrupted the connection: interrupts are suppressed meanwhile
struct SuppressInterruptsScope {
	explicit SuppressInterruptsScope(ClientContext &context) : context(context), saved(context.interrupt_state.load()) {
		context.SuppressInterrupts();
	}
	~SuppressInterruptsScope() {
		context.interrupt_state = saved;
	}
	ClientContext &context;
	ClientInterruptState saved;
};

template <class FUNC>
static void RunInOwnTransaction(ClientContext &context, FUNC &&fun) {
	SuppressInterruptsScope suppress(context);
	if (context.transaction.HasActiveTransaction()) {
		fun();
		return;
	}
	context.transaction.BeginTransaction();
	try {
		fun();
	} catch (...) {
		try {
			context.transaction.Rollback(nullptr);
		} catch (...) { // NOLINT
		}
		throw;
	}
	context.transaction.Commit();
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
	ErrorData write_error;
	try {
		if (glue_transaction.writes.HasWrites()) {
			RunInOwnTransaction(context, [&]() {
				glue_transaction.writes.Commit(context, glue_catalog, *glue_transaction.cache);
			});
		}
	} catch (std::exception &ex) {
		write_error = ErrorData(ex);
	}
	glue_transaction.Commit();
	lock_guard<mutex> l(transaction_lock);
	transaction_sequence.erase(transaction);
	transactions.erase(transaction);
	FreeRetiredEntries();
	return write_error;
}

void GlueTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &glue_transaction = transaction.Cast<GlueTransaction>();
	ErrorData write_error;
	auto context = transaction.context.lock();
	if (context && glue_transaction.writes.HasWrites()) {
		try {
			RunInOwnTransaction(*context, [&]() {
				write_error = glue_transaction.writes.Rollback(*context, glue_catalog, *glue_transaction.cache);
			});
		} catch (std::exception &ex) {
			write_error = ErrorData(ex);
		}
		if (write_error.HasError()) {
			DUCKDB_LOG_WARNING(*context, "Undoing the Hive writes of a rolled back transaction failed: %s",
			                   write_error.RawMessage());
		}
	}
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
