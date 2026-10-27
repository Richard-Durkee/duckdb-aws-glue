#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/transaction/transaction.hpp"

#include "api/glue_api.hpp"

#include <functional>

namespace duckdb {
class GlueCatalog;

enum class GlueTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

//! Glue metadata resolved in this transaction: the one place a definition Glue returned is kept (GlueMetadata reads
//! through it). Coalesces concurrent and repeated requests for the same object into one Glue call, remembers a
//! missing object for the query that found it missing, and forgets only what a write changed.
class GlueTransactionCache {
public:
	//! The current transaction's cache, or null when no catalog transaction is active (ATTACH, bare mutations)
	static optional_ptr<GlueTransactionCache> Of(ClientContext &context, Catalog &catalog);
	//! Clears the connection's directory listings too
	static void Invalidate(ClientContext &context, Catalog &catalog);
	//! Forget what a write to one table (or, with an empty 'table_name', one database) changed, in this
	//! transaction's cache and in the connection's caches of every alias. With 'partitions_only' the table
	//! definition stays (a partition write does not change it).
	static void Invalidate(ClientContext &context, Catalog &catalog, const string &database_name,
	                       const string &table_name, bool partitions_only = false);
	//! DETACH erases the catalog's entry from the detaching connection's cache state; the attach-identity
	//! keying remains the safety net for connection states a DETACH cannot reach
	static void OnDetach(ClientContext &context, Catalog &catalog);
	//! Whether glue_metadata_cache_scope is 'connection'
	static bool ConnectionScoped(ClientContext &context);
	//! The id of the running query (0 outside a query): a 'missing' answer is only trusted within it
	static idx_t CurrentQuery(ClientContext &context);
	//! Forget what the enclosing Glue mutation changes when it returns or throws. Without names the whole cache.
	struct InvalidateOnExit {
		InvalidateOnExit(ClientContext &context, Catalog &catalog, string database_name_p = string(),
		                 string table_name_p = string(), bool partitions_only = false)
		    : context(context), catalog(catalog), database_name(std::move(database_name_p)),
		      table_name(std::move(table_name_p)), partitions_only(partitions_only) {
		}
		//! Invalidates; an exception is logged, not rethrown (defined in glue_transaction.cpp for the logger)
		~InvalidateOnExit();
		ClientContext &context;
		Catalog &catalog;
		string database_name;
		string table_name;
		bool partitions_only;
	};

	//! The table, null if Glue does not hold it. 'load' asks Glue (null for a missing table). A held definition is
	//! reused for the whole transaction; with 'this_query_only' only if it was fetched by query 'query'. A missing
	//! answer is reused within query 'query' only (0: never).
	shared_ptr<const GlueTableInfo> GetTable(const string &database_name, const string &table_name,
	                                         const std::function<shared_ptr<const GlueTableInfo>()> &load,
	                                         idx_t query = 0, bool this_query_only = false) {
		shared_ptr<CacheEntry<GlueTableInfo>> entry;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			entry = FindEntry(tables, TableKey(database_name, table_name));
		}
		return Resolve(*entry, load, query, this_query_only);
	}
	//! Whether GetTable would answer from the cache (a definition, or a missing table found by query 'query')
	bool HoldsTable(const string &database_name, const string &table_name, idx_t query) {
		shared_ptr<CacheEntry<GlueTableInfo>> entry;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			auto it = tables.find(StringUtil::Lower(TableKey(database_name, table_name)));
			if (it == tables.end()) {
				return false;
			}
			entry = it->second;
		}
		annotated_lock_guard<annotated_mutex> guard(entry->load_lock);
		return entry->loaded && (entry->value || (query != 0 && entry->query == query));
	}
	//! Replace what is held for the table (a definition an embedding application or a write supplied)
	void PutTable(const string &database_name, const string &table_name, shared_ptr<const GlueTableInfo> table,
	              idx_t query) {
		shared_ptr<CacheEntry<GlueTableInfo>> entry;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			entry = FindEntry(tables, TableKey(database_name, table_name));
		}
		Store(*entry, std::move(table), query);
	}
	shared_ptr<const vector<GluePartitionInfo>>
	GetPartitions(const string &database_name, const string &table_name,
	              const std::function<shared_ptr<const vector<GluePartitionInfo>>()> &load) {
		shared_ptr<CacheEntry<vector<GluePartitionInfo>>> entry;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			entry = FindEntry(partitions, TableKey(database_name, table_name));
		}
		return Resolve(*entry, load, 0, false);
	}
	void PutPartitions(const string &database_name, const string &table_name,
	                   shared_ptr<const vector<GluePartitionInfo>> value, idx_t query) {
		shared_ptr<CacheEntry<vector<GluePartitionInfo>>> entry;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			entry = FindEntry(partitions, TableKey(database_name, table_name));
		}
		Store(*entry, std::move(value), query);
	}
	//! The database, null if Glue does not hold it; as GetTable
	shared_ptr<const GlueDatabaseInfo> GetDatabase(const string &database_name,
	                                               const std::function<shared_ptr<const GlueDatabaseInfo>()> &load,
	                                               idx_t query) {
		shared_ptr<CacheEntry<GlueDatabaseInfo>> entry;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			entry = FindEntry(databases, database_name);
		}
		return Resolve(*entry, load, query, false);
	}
	void Clear() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		tables.clear();
		partitions.clear();
		databases.clear();
	}
	//! Forget one table (its definition unless 'partitions_only', and its partitions), or with an empty
	//! 'table_name' one database and everything held for its tables
	void Forget(const string &database_name, const string &table_name, bool partitions_only) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (!table_name.empty()) {
			auto key = StringUtil::Lower(TableKey(database_name, table_name));
			partitions.erase(key);
			if (!partitions_only) {
				tables.erase(key);
			}
			return;
		}
		auto prefix = StringUtil::Lower(database_name) + "\n";
		databases.erase(StringUtil::Lower(database_name));
		EraseWithPrefix(tables, prefix);
		EraseWithPrefix(partitions, prefix);
	}
	//! Tells cache instances apart (monotonic, never reused): a bulk table fetch warms each instance at most once
	//! (GlueTableSet), and under the default scope a new transaction brings a new instance
	idx_t Id() const {
		return id;
	}

private:
	static idx_t NextId();
	static string TableKey(const string &database_name, const string &table_name) {
		return database_name + "\n" + table_name;
	}

	//! One cached value and the lock that guards loading it. The lock is per entry so the Glue calls for
	//! different objects do not wait on each other (resolving N tables runs their fetches in parallel), while
	//! concurrent requests for the same object share one call.
	template <class T>
	struct CacheEntry {
		annotated_mutex load_lock;
		//! Whether a load completed; a failed (throwing) load leaves it unset so the next reference retries
		bool loaded DUCKDB_GUARDED_BY(load_lock) = false;
		//! Null for an object Glue does not hold
		shared_ptr<const T> value DUCKDB_GUARDED_BY(load_lock);
		//! The query that loaded it
		idx_t query DUCKDB_GUARDED_BY(load_lock) = 0;
	};

	//! Keys fold case (Glue database/table names are case-insensitive). The map lock is only held to find or
	//! create the entry (FindEntry); the entry's own lock is held across 'load' (Resolve) so concurrent references
	//! to the same key fetch once while different keys load in parallel; 'load' must not call back into this cache
	template <class T>
	shared_ptr<CacheEntry<T>> FindEntry(unordered_map<string, shared_ptr<CacheEntry<T>>> &map, const string &key)
	    DUCKDB_REQUIRES(lock) {
		auto &slot = map[StringUtil::Lower(key)];
		if (!slot) {
			slot = make_shared_ptr<CacheEntry<T>>();
		}
		return slot;
	}
	template <class T>
	static void EraseWithPrefix(unordered_map<string, shared_ptr<CacheEntry<T>>> &map, const string &prefix) {
		for (auto it = map.begin(); it != map.end();) {
			if (StringUtil::StartsWith(it->first, prefix)) {
				it = map.erase(it);
			} else {
				++it;
			}
		}
	}
	template <class T>
	static shared_ptr<const T> Resolve(CacheEntry<T> &entry, const std::function<shared_ptr<const T>()> &load,
	                                   idx_t query, bool this_query_only) {
		annotated_lock_guard<annotated_mutex> guard(entry.load_lock);
		bool same_query = query != 0 && entry.query == query;
		if (entry.loaded && (entry.value ? (!this_query_only || same_query) : same_query)) {
			return entry.value;
		}
		auto value = load();
		entry.value = std::move(value);
		entry.query = query;
		entry.loaded = true;
		return entry.value;
	}
	template <class T>
	static void Store(CacheEntry<T> &entry, shared_ptr<const T> value, idx_t query) {
		annotated_lock_guard<annotated_mutex> guard(entry.load_lock);
		entry.value = std::move(value);
		entry.query = query;
		entry.loaded = true;
	}

	annotated_mutex lock;
	unordered_map<string, shared_ptr<CacheEntry<GlueTableInfo>>> tables DUCKDB_GUARDED_BY(lock);
	unordered_map<string, shared_ptr<CacheEntry<vector<GluePartitionInfo>>>> partitions DUCKDB_GUARDED_BY(lock);
	unordered_map<string, shared_ptr<CacheEntry<GlueDatabaseInfo>>> databases DUCKDB_GUARDED_BY(lock);
	const idx_t id = NextId();
};

//! The connection's caches under glue_metadata_cache_scope='connection', one per attached Glue catalog. Lives in
//! the connection's registered state, so two connections never share a cache. A rollback clears the caches held
//! here: a failed statement must not leave metadata it resolved behind.
class GlueMetadataCacheState : public ClientContextState {
public:
	shared_ptr<GlueTransactionCache> GetCache(const string &catalog_name, idx_t attach_oid) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &entry = caches[catalog_name];
		// Keyed by the attach identity, not just the alias: after DETACH + re-ATTACH under the same alias the
		// new attached database must not be served the old catalog's metadata, even where the DETACH hook
		// (EraseCache) could not reach this state. Replacing the entry also keeps one entry per alias.
		if (!entry.cache || entry.attach_oid != attach_oid) {
			entry = {attach_oid, make_shared_ptr<GlueTransactionCache>()};
		}
		return entry.cache;
	}

	void EraseCache(const string &catalog_name, idx_t attach_oid) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto entry = caches.find(catalog_name);
		if (entry != caches.end() && entry->second.attach_oid == attach_oid) {
			caches.erase(entry);
		}
	}

	void ClearAll() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		for (auto &entry : caches) {
			entry.second.cache->Clear();
		}
	}

	void ForgetAll(const string &database_name, const string &table_name, bool partitions_only) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		for (auto &entry : caches) {
			entry.second.cache->Forget(database_name, table_name, partitions_only);
		}
	}

	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override {
		ClearAll();
	}

private:
	struct CatalogCache {
		idx_t attach_oid;
		shared_ptr<GlueTransactionCache> cache;
	};

	annotated_mutex lock;
	unordered_map<string, CatalogCache> caches DUCKDB_GUARDED_BY(lock);
};

//! Glue has no transactional semantics in this extension: every DDL statement is executed against the Glue API
//! immediately. The transaction exists because every attached database needs one, and carries the metadata cache.
class GlueTransaction : public Transaction {
public:
	GlueTransaction(GlueCatalog &glue_catalog, TransactionManager &manager, ClientContext &context);
	~GlueTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	static GlueTransaction &Get(ClientContext &context, Catalog &catalog);

public:
	GlueCatalog &glue_catalog;
	//! Per transaction by default; shared through the connection state under glue_metadata_cache_scope='connection'
	//! (so a prepared statement's two binds share it).
	shared_ptr<GlueTransactionCache> cache;

private:
	GlueTransactionState transaction_state;
};

} // namespace duckdb
