#include "core/glue_metrics.hpp"

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/profiler/metric_info.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {

void GlueMetrics::Register(ExtensionLoader &loader) {
	loader.RegisterMetric({API_REQUESTS, "uint64",
	                       "Requests the AWS SDK sent to the Glue API, retries included. A request the HTTP transport "
	                       "repeats by itself is counted once. They are also counted in httpfs.requests",
	                       ""});
	loader.RegisterMetric(
	    {API_RETRIES, "uint64", "Glue API requests the AWS SDK sent again after a failed attempt", ""});
	loader.RegisterMetric(
	    {API_REQUEST_TIME, "uint64", "Time the Glue API requests took, added up over all threads", "microseconds"});
	loader.RegisterMetric({PARTITIONS_LOADED, "uint64", "Partitions loaded from the Glue catalog", ""});
	loader.RegisterMetric({PARTITIONS_READ, "uint64",
	                       "Partitions of Glue tables whose data files the scans listed, each counted once", ""});
	loader.RegisterMetric({LISTINGS, "uint64", "Directory listings made to find the data files of Glue tables", ""});
	loader.RegisterMetric({FILES_LISTED, "uint64", "Data files of Glue tables found by those listings", ""});
}

void GlueMetrics::Add(ClientContext &context, const char *key, idx_t amount) {
	auto &profiler = QueryProfiler::Get(context);
	if (!profiler.IsEnabled()) {
		return;
	}
	profiler.AddToMetricCounter(key, amount);
}

GlueApiRequestMetrics::GlueApiRequestMetrics(optional_ptr<ClientContext> context_p)
    : context(context_p), start(std::chrono::steady_clock::now()) {
}

GlueApiRequestMetrics::~GlueApiRequestMetrics() {
	if (!context) {
		return;
	}
	auto elapsed = std::chrono::steady_clock::now() - start;
	auto micros = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
	try {
		GlueMetrics::Add(*context, GlueMetrics::API_REQUESTS, 1);
		GlueMetrics::Add(*context, GlueMetrics::API_REQUEST_TIME, static_cast<idx_t>(MaxValue<int64_t>(micros, 0)));
	} catch (...) { // NOLINT: a destructor must not throw, and a lost figure must not fail the request
	}
}

} // namespace duckdb
