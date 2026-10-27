#pragma once

#include "duckdb/common/shared_ptr.hpp"

#include "core/glue_info.hpp"

namespace duckdb {
class ClientContext;
class GlueCatalog;

//! The one access path for Glue metadata reads. Every lookup of a table, its partitions or a database goes through
//! the active transaction's GlueTransactionCache, which coalesces duplicate and concurrent requests into one Glue call
//! and keeps the answer until a write changes the object. Outside a catalog transaction the request goes straight to
//! Glue. An embedding application that already holds a definition hands it over with ProvideTable /
//! ProvidePartitions and no Glue call is made for it.
class GlueMetadata {
public:
	enum class Freshness : uint8_t {
		//! What the transaction (or the connection, under glue_metadata_cache_scope='connection') already holds
		CACHED,
		//! Fetched by the running query: the definition a statement is about to rewrite
		THIS_QUERY
	};

	//! The table, null if Glue does not hold it
	static shared_ptr<const GlueTableInfo> GetTable(ClientContext &context, GlueCatalog &catalog,
	                                                const string &database_name, const string &table_name,
	                                                Freshness freshness = Freshness::CACHED);
	//! The partitions registered for the table (GetPartitions, all pages)
	static shared_ptr<const vector<GluePartitionInfo>> GetPartitions(ClientContext &context, GlueCatalog &catalog,
	                                                                 const string &database_name,
	                                                                 const string &table_name);
	//! Returns false if the database does not exist
	static bool GetDatabase(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                        GlueDatabaseInfo &result);

	//! Hand over a Glue Table object the caller already fetched (JSON as the AWS SDK's Table::Jsonize writes it, i.e.
	//! the 'Table' member of a GetTable response); it is used instead of asking Glue for the rest of the transaction
	static GlueTableInfo ProvideTable(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                                  const string &table_json);
	//! Hand over the partitions of a table: a JSON array of Glue Partition objects (the 'Partitions' member of a
	//! GetPartitions response). Returns how many were provided.
	static idx_t ProvidePartitions(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                               const string &table_name, const string &partitions_json);
};

} // namespace duckdb
