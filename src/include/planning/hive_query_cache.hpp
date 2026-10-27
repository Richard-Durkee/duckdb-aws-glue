#pragma once

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/client_context_state.hpp"

#include <functional>

#include "core/glue_info.hpp"

namespace duckdb {
struct HiveScanInfo;
class GlueCatalog;
struct HiveTableSample;

//! What Glue said about a table the running query scans
struct GlueTableEntry {
	annotated_mutex lock;
	//! Null until the first scan of the table asks Glue
	shared_ptr<const GlueTableInfo> table_info DUCKDB_GUARDED_BY(lock);
	//! Null until the first scan of a partitioned table asks Glue for them
	shared_ptr<const vector<GluePartitionInfo>> partitions DUCKDB_GUARDED_BY(lock);
	//! Some partitions of the table, looked up for scans that need only those, per lookup (GetTablePartitionSubset)
	unordered_map<string, shared_ptr<const vector<GluePartitionInfo>>> partition_subsets DUCKDB_GUARDED_BY(lock);
	//! The kinds of those lookups Glue failed in this query for a reason that may not last beyond it (an expired token,
	//! a table that is gone, a timeout), which the query does not ask again
	unordered_set<string> failed_lookups DUCKDB_GUARDED_BY(lock);
};

//! The sample of a table, measured once per query for every scan of the table
struct HiveTableSampleEntry {
	annotated_mutex lock;
	//! Null until the first scan of the table measures it. Shared and immutable once set: every scan of the table in
	//! the query (self joins, subqueries, a CTE inlined twice) reads the one sample, after the lock is released
	shared_ptr<const HiveTableSample> sample DUCKDB_GUARDED_BY(lock);
};

//! What the running query learned about the tables it scans, dropped when the query ends: the Glue definition and
//! partitions of a table (asked for once however often the query scans it), the recursive listings of directories
//! (shared by the estimate and every scan listing the same directory) and the table samples
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

private:
	annotated_mutex lock;
	unordered_map<string, shared_ptr<GlueTableEntry>> glue_tables DUCKDB_GUARDED_BY(lock);
	unordered_map<string, shared_ptr<HiveTableSampleEntry>> samples DUCKDB_GUARDED_BY(lock);
	unordered_map<string, shared_ptr<MultiFileList>> directory_listings DUCKDB_GUARDED_BY(lock);
};

//! The partitions Glue registers for the table of 'info' in this query, fetched once for every scan of the table
shared_ptr<const vector<GluePartitionInfo>> GetTablePartitions(ClientContext &context, const HiveScanInfo &info);
//! Whether all partitions of the table of 'info' are known already, to this query or the transaction cache
bool TablePartitionsCached(ClientContext &context, const HiveScanInfo &info);
//! Looks up some partitions of a table in Glue into 'result': false when Glue did not answer in full (BatchGetPartition
//! left keys unprocessed, or Glue throttled the lookup or failed it on its side after the retries). An
//! InvalidInputException when Glue refuses this lookup (a value or an expression it does not accept), a
//! PermissionException when it denies the principal the kind of lookup (AccessDeniedException, a plain 403), an
//! InvalidTypeException when it can not compare a value the table stores with it (InvalidStateException), and any
//! other exception of a Glue error (an expired token, a table that is gone) when it fails for another reason
using PartitionSubsetLoader = std::function<bool(GlueCatalog &catalog, vector<GluePartitionInfo> &result)>;
//! Looks up the partitions with exactly these 'values', or more, into 'result': false, or an exception, when it can not
using PartitionValuesProbe =
    std::function<bool(GlueCatalog &catalog, const vector<vector<string>> &values, vector<GluePartitionInfo> &result)>;
//! How GetTablePartitionSubset checks a partition the lookup found in a directory whose name has an escape ('%3A'):
//! that name is also the directory of the value spelled as it is, or as it decodes, whose partition, when registered at
//! the same location, may own its files
struct PartitionSubsetProbes {
	//! The values the lookup asks Glue for by value (BatchGetPartition), whose partitions it finds if they exist; null
	//! for a lookup by expression, all of whose partitions the scan needs
	const vector<vector<string>> *values = nullptr;
	//! Values the lookup asks for alongside, other spellings of the directories of 'values': their partitions are
	//! not part of the subset
	vector<vector<string>> extra;
	//! Looks up the partitions of those spellings the lookup did not ask for; all partitions are fetched without one
	PartitionValuesProbe probe;
	//! How many requests 'probe' sends for these values, one after another. With more than one, all partitions are
	//! fetched instead, which answers the check in the one listing the scan without a lookup needs. Null when 'probe'
	//! sends its requests at the same time.
	std::function<idx_t(const vector<vector<string>> &values)> sequential_requests;
};
//! Some partitions of the table of 'info', 'load'ed once per query (and per transaction cache) for every scan asking
//! with the same 'key', 'subset' set when they are returned. All partitions of the table instead, 'subset' false, when
//! they are known already, or when a partition looked up is not stored in the directory its values name below the
//! table location, shares its location with another, or is stored under an escaped name that a partition with another
//! value (the name as it is, or decoded) is registered at too, can not be asked for or would take several requests
//! one after another ('probes'): a partition left out could be registered there too, and the files of a location
//! belong to the first partition registered with it, as when all partitions are fetched. Null when Glue refuses the
//! lookup or does not answer it, so that the caller can try another one or fetch all.
//! What the connection learns is kept for it, by the Glue catalog (GlueCatalog::AttachIdentity) rather than its alias:
//! a lookup Glue refuses (a value or an expression it does not accept) is not asked again, a 'lookup' kind Glue denies
//! the principal, can not evaluate for a stored value, throttles, fails on its side or leaves unprocessed is not asked
//! again for the table, a table whose partitions the locations do not tell apart is fetched in full right away, and so
//! is one for a 'lookup' kind whose escaped directories could not be checked (a value no probe can ask for, or one Glue
//! refuses, denies or throttles). A lookup that fails for another reason is not asked again in the query.
shared_ptr<const vector<GluePartitionInfo>>
GetTablePartitionSubset(ClientContext &context, const HiveScanInfo &info, const string &key,
                        const PartitionSubsetLoader &load, bool &subset, const string &lookup = string(),
                        const PartitionSubsetProbes &probes = PartitionSubsetProbes());
//! Drop the connection's cached directory listings
void ClearHiveListingCache(ClientContext &context);
//! The partitions of the table of 'info' with exactly these 'values' (BatchGetPartition), looked up once per query for
//! every scan asking for the same ones, as GetTablePartitionSubset: 'pinned' tells whether they are only those
shared_ptr<const vector<GluePartitionInfo>> GetPinnedTablePartitions(ClientContext &context, const HiveScanInfo &info,
                                                                     const vector<vector<string>> &values,
                                                                     bool &pinned);

} // namespace duckdb
