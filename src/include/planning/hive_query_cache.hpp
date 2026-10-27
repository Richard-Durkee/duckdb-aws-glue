#pragma once

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/client_context_state.hpp"

#include "core/glue_info.hpp"

#include <future>

namespace duckdb {
struct HiveScanInfo;
struct HiveTableSample;
class BaseFileReader;

//! Whether the plan of the running query has a consumer of the scans' row estimates (decided before optimizing)
enum class HiveSampleDemand : uint8_t { UNKNOWN, NOT_NEEDED, NEEDED };

//! What Glue said about a table the running query scans
struct GlueTableEntry {
	annotated_mutex lock;
	//! Null until the first scan of a partitioned table asks Glue for them
	shared_ptr<const vector<GluePartitionInfo>> partitions DUCKDB_GUARDED_BY(lock);
};

//! The sample of a table, measured once per query for every scan of the table
struct HiveTableSampleEntry {
	annotated_mutex lock;
	//! Null until the first scan of the table measures it. Shared and immutable once set: every scan of the table in
	//! the query (self joins, subqueries, a CTE inlined twice) reads the one sample, after the lock is released
	shared_ptr<const HiveTableSample> sample DUCKDB_GUARDED_BY(lock);
	//! The sample being measured on another thread, when it was started ahead of the first scan asking
	std::shared_future<shared_ptr<const HiveTableSample>> pending DUCKDB_GUARDED_BY(lock);
};

//! What the running query learned about the tables it scans, dropped when the query ends: the partitions of a table
//! (asked for once however often the query scans it), the recursive listings of directories (shared by the estimate and
//! every scan listing the same directory) and the table samples
class HiveQueryCache : public ClientContextState {
public:
	static shared_ptr<HiveQueryCache> Get(ClientContext &context);
	//! The key of a Glue table in GetGlueTable
	static string TableKey(const string &catalog, const string &database, const string &table);

	void QueryEnd(ClientContext &context) override;

	//! 'key' identifies the table (TableKey)
	shared_ptr<GlueTableEntry> GetGlueTable(const string &key);
	//! 'table' identifies the table: its catalog and name, or the location of a hive_scan
	shared_ptr<HiveTableSampleEntry> GetSample(const string &table);
	//! The recursive listing of 'directory', fetched page by page as it is read. With 'cache_in_connection' the
	//! listing is kept beyond the query (glue_cache_listings)
	shared_ptr<MultiFileList> GetDirectoryListing(ClientContext &context, const string &directory,
	                                              bool cache_in_connection = false);

	//! Record whether the plan needs sampled estimates; once needed it stays needed for the query
	void SetSampleDemand(HiveSampleDemand demand);
	HiveSampleDemand GetSampleDemand();
	//! The reader the cardinality sample opened for 'path', kept for the scan that reads the file
	void PutProbeReader(const string &path, shared_ptr<BaseFileReader> reader);
	shared_ptr<BaseFileReader> TakeProbeReader(const string &path);

private:
	annotated_mutex lock;
	unordered_map<string, shared_ptr<GlueTableEntry>> glue_tables DUCKDB_GUARDED_BY(lock);
	unordered_map<string, shared_ptr<HiveTableSampleEntry>> samples DUCKDB_GUARDED_BY(lock);
	unordered_map<string, shared_ptr<MultiFileList>> directory_listings DUCKDB_GUARDED_BY(lock);
	HiveSampleDemand sample_demand DUCKDB_GUARDED_BY(lock) = HiveSampleDemand::UNKNOWN;
	unordered_map<string, shared_ptr<BaseFileReader>> probe_readers DUCKDB_GUARDED_BY(lock);
};

//! The partitions Glue registers for the table of 'info' in this query, fetched once for every scan of the table
shared_ptr<const vector<GluePartitionInfo>> GetTablePartitions(ClientContext &context, const HiveScanInfo &info);
//! Drop the connection's cached directory listings
void ClearHiveListingCache(ClientContext &context);

} // namespace duckdb
