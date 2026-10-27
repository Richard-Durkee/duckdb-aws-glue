#include "catalog/glue_metadata.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_transaction.hpp"

namespace duckdb {

shared_ptr<const GlueTableInfo> GlueMetadata::GetTable(ClientContext &context, GlueCatalog &catalog,
                                                       const string &database_name, const string &table_name,
                                                       Freshness freshness) {
	auto load = [&]() -> shared_ptr<const GlueTableInfo> {
		GlueTableInfo table;
		if (!GlueAPI::GetTable(context, catalog, database_name, table_name, table)) {
			return nullptr;
		}
		return make_shared_ptr<const GlueTableInfo>(std::move(table));
	};
	auto cache = GlueTransactionCache::Of(context, catalog);
	if (!cache) {
		return load();
	}
	return cache->GetTable(database_name, table_name, load, GlueTransactionCache::CurrentQuery(context),
	                       freshness == Freshness::THIS_QUERY);
}

shared_ptr<const vector<GluePartitionInfo>> GlueMetadata::GetPartitions(ClientContext &context, GlueCatalog &catalog,
                                                                        const string &database_name,
                                                                        const string &table_name) {
	auto load = [&]() {
		return make_shared_ptr<const vector<GluePartitionInfo>>(
		    GlueAPI::GetPartitions(context, catalog, database_name, table_name));
	};
	auto cache = GlueTransactionCache::Of(context, catalog);
	return cache ? cache->GetPartitions(database_name, table_name, load) : load();
}

bool GlueMetadata::GetDatabase(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                               GlueDatabaseInfo &result) {
	auto load = [&]() -> shared_ptr<const GlueDatabaseInfo> {
		GlueDatabaseInfo database;
		if (!GlueAPI::GetDatabase(context, catalog, database_name, database)) {
			return nullptr;
		}
		return make_shared_ptr<const GlueDatabaseInfo>(std::move(database));
	};
	auto cache = GlueTransactionCache::Of(context, catalog);
	auto database = cache ? cache->GetDatabase(database_name, load, GlueTransactionCache::CurrentQuery(context)) : load();
	if (!database) {
		return false;
	}
	result = *database;
	return true;
}

GlueTableInfo GlueMetadata::ProvideTable(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                         const string &table_json) {
	auto table = GlueAPI::ParseTable(table_json, database_name);
	// Get() not Of(): a provided definition must land in the transaction's cache even in a bare statement
	auto &cache = *GlueTransaction::Get(context, catalog).cache;
	cache.PutTable(database_name, table.name, make_shared_ptr<const GlueTableInfo>(table),
	               GlueTransactionCache::CurrentQuery(context));
	return table;
}

idx_t GlueMetadata::ProvidePartitions(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                      const string &table_name, const string &partitions_json) {
	auto partitions = make_shared_ptr<const vector<GluePartitionInfo>>(GlueAPI::ParsePartitions(partitions_json));
	auto count = partitions->size();
	auto &cache = *GlueTransaction::Get(context, catalog).cache;
	cache.PutPartitions(database_name, table_name, std::move(partitions), GlueTransactionCache::CurrentQuery(context));
	return count;
}

} // namespace duckdb
