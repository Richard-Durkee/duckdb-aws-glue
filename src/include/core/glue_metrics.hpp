#pragma once

#include "duckdb/common/common.hpp"

#include <chrono>

namespace duckdb {

class ClientContext;
class ExtensionLoader;

//! Per-query Glue figures, reported in the query profile next to DuckDB's own. They are only counted while profiling is
//! enabled.
struct GlueMetrics {
	//! Requests the AWS SDK sent to the Glue API, one per attempt, so the retries are included. A request DuckDB's HTTP
	//! transport repeats inside one attempt is counted once. Requests to other services are not counted. They are also
	//! counted in httpfs.requests, which this is a subset of.
	static constexpr const char *API_REQUESTS = "glue.api_requests";
	//! Glue API requests the AWS SDK sent again after a failed attempt. Counted by the retry strategy of the Glue
	//! client, so only Glue calls are in it
	static constexpr const char *API_RETRIES = "glue.api_retries";
	//! Time the Glue API requests took, in microseconds, summed over requests that ran in parallel
	static constexpr const char *API_REQUEST_TIME = "glue.api_request_time";
	//! Partitions loaded from the Glue catalog for the scans of the query
	static constexpr const char *PARTITIONS_LOADED = "glue.partitions_loaded";
	//! Partitions of the scans of the query whose data files were listed, after pruning, each counted once per scan
	//! (the lists a scan derives from one another by pruning share the count). Absent for a table without partitions
	static constexpr const char *PARTITIONS_READ = "glue.partitions_read";
	//! Directory listings of tables made by the query, whichever scan path asked for them. One served from a cache of
	//! an earlier statement (glue_cache_listings) or of this query is not made again and not counted
	static constexpr const char *LISTINGS = "glue.listings";
	//! Data files found by those listings, reported once the scan has read a listing (a root listing is reported in
	//! full, including the files of partitions pruned)
	static constexpr const char *FILES_LISTED = "glue.files_listed";

	static void Register(ExtensionLoader &loader);
	//! Adds `amount` to the figure `key` of the query running on `context`. Does nothing unless profiling is enabled.
	//! Safe to call from any thread.
	static void Add(ClientContext &context, const char *key, idx_t amount);
};

//! Counts one Glue API request and its duration when it goes out of scope, so a failed request counts too.
class GlueApiRequestMetrics {
public:
	explicit GlueApiRequestMetrics(optional_ptr<ClientContext> context);
	~GlueApiRequestMetrics();

private:
	optional_ptr<ClientContext> context;
	std::chrono::steady_clock::time_point start;
};

} // namespace duckdb
