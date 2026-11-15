#include "planning/hive_query_cache.hpp"

#include "core/glue_metrics.hpp"
#include "core/hive_location.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_transaction.hpp"
#include "planning/hive_multi_file_reader.hpp"

namespace duckdb {

static constexpr const char *HIVE_QUERY_CACHE = "glue_hive_query_cache";
static constexpr const char *HIVE_LISTING_CACHE = "glue_listing_cache";

namespace {

//! The directory listings of Glue tables, kept across statements under glue_cache_listings
class HiveListingCache : public ClientContextState {
public:
	//! 'listed' tells whether this call made the listing, rather than finding one an earlier statement made
	shared_ptr<MultiFileList> GetListing(ClientContext &context, const string &directory, bool &listed) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &listing = listings[directory];
		listed = !listing;
		if (!listing) {
			auto &fs = FileSystem::GetFileSystem(context);
			listing = shared_ptr<MultiFileList>(ListLocation(fs, directory));
		}
		return listing;
	}

	void Clear() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		listings.clear();
	}

	void QueryEnd(ClientContext &context) override {
		Value enabled;
		if (!context.TryGetCurrentSetting("glue_cache_listings", enabled) || enabled.IsNull() ||
		    !BooleanValue::Get(enabled)) {
			Clear();
		}
	}

	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override {
		if (!GlueTransactionCache::ConnectionScoped(context)) {
			Clear();
		}
	}

	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override {
		Clear();
	}

private:
	annotated_mutex lock;
	unordered_map<string, shared_ptr<MultiFileList>> listings DUCKDB_GUARDED_BY(lock);
};

} // namespace

void ClearHiveListingCache(ClientContext &context) {
	auto cache = context.registered_state->Get<HiveListingCache>(HIVE_LISTING_CACHE);
	if (cache) {
		cache->Clear();
	}
}

shared_ptr<HiveQueryCache> HiveQueryCache::Get(ClientContext &context) {
	return context.registered_state->GetOrCreate<HiveQueryCache>(HIVE_QUERY_CACHE);
}

string HiveQueryCache::TableKey(const string &catalog, const string &database, const string &table) {
	return catalog + "\x1f" + database + "\x1f" + table;
}

void HiveQueryCache::QueryEnd(ClientContext &context) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	glue_tables.clear();
	samples.clear();
	directory_listings.clear();
	unreported_listings.clear();
	scan_count = 0;
}

idx_t HiveQueryCache::NextScanId() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return ++scan_count;
}

shared_ptr<GlueTableEntry> HiveQueryCache::GetGlueTable(const string &key) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto &entry = glue_tables[key];
	if (!entry) {
		entry = make_shared_ptr<GlueTableEntry>();
	}
	return entry;
}

shared_ptr<HiveTableSampleEntry> HiveQueryCache::GetSample(const string &table) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto &entry = samples[table];
	if (!entry) {
		entry = make_shared_ptr<HiveTableSampleEntry>();
	}
	return entry;
}

shared_ptr<MultiFileList> HiveQueryCache::GetDirectoryListing(ClientContext &context, const string &directory,
                                                              bool cache_in_connection) {
	auto key = directory;
	StringUtil::RTrim(key, "/");
	// The one place a directory listing is made, whichever scan path asks for it (the table, a partition, the root, a
	// sample): a listing found in a cache was made before and is not counted
	if (cache_in_connection) {
		// the connection's listings (glue_cache_listings): kept for later statements, cleared with the metadata cache
		bool listed = false;
		auto listing = context.registered_state->GetOrCreate<HiveListingCache>(HIVE_LISTING_CACHE)
		                   ->GetListing(context, key, listed);
		if (listed) {
			ReportListing(context, key);
		}
		return listing;
	}
	shared_ptr<HiveDirectoryListingEntry> entry;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &slot = directory_listings[key];
		if (!slot) {
			slot = make_shared_ptr<HiveDirectoryListingEntry>();
		}
		entry = slot;
	}
	// lock per directory: GlobFileList fetches the first page right away, and listings of other directories must not
	// wait behind it
	annotated_lock_guard<annotated_mutex> guard(entry->lock);
	if (!entry->listing) {
		auto &fs = FileSystem::GetFileSystem(context);
		entry->listing = shared_ptr<MultiFileList>(ListLocation(fs, key));
		ReportListing(context, key);
	}
	return entry->listing;
}

void HiveQueryCache::ReportListing(ClientContext &context, const string &directory) {
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		unreported_listings.insert(directory);
	}
	GlueMetrics::Add(context, GlueMetrics::LISTINGS, 1);
}

bool HiveQueryCache::ClaimListedFiles(const string &directory) {
	auto key = directory;
	StringUtil::RTrim(key, "/");
	annotated_lock_guard<annotated_mutex> guard(lock);
	return unreported_listings.erase(key) > 0;
}

shared_ptr<const vector<GluePartitionInfo>> GetTablePartitions(ClientContext &context, const HiveScanInfo &info) {
	auto entry = HiveQueryCache::Get(context)->GetGlueTable(
	    HiveQueryCache::TableKey(info.catalog_name, info.database_name, info.table_name));
	// per table, so the Glue calls for different tables do not wait on each other
	annotated_lock_guard<annotated_mutex> guard(entry->lock);
	if (!entry->partitions) {
		auto &catalog = Catalog::GetCatalog(context, Identifier(info.catalog_name)).Cast<GlueCatalog>();
		auto load = [&]() {
			return make_shared_ptr<const vector<GluePartitionInfo>>(
			    GlueAPI::GetPartitions(context, catalog, info.database_name, info.table_name));
		};
		auto cache = GlueTransactionCache::Of(context, catalog);
		entry->partitions = cache ? cache->GetPartitions(info.database_name, info.table_name, load) : load();
	}
	return entry->partitions;
}

} // namespace duckdb
