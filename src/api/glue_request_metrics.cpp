#include "api/glue_request_metrics.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {

static constexpr const char *GLUE_TARGET_PREFIX = "AWSGlue.";
static constexpr const char *GLUE_OPERATIONS[] = {
    "BatchCreatePartition", "BatchDeletePartition", "BatchGetPartition", "CreateDatabase", "CreatePartition",
    "CreateTable",          "DeleteDatabase",       "DeletePartition",   "DeleteTable",    "GetDatabase",
    "GetDatabases",         "GetPartition",         "GetPartitions",     "GetTable",       "GetTables",
    "UpdateDatabase",       "UpdatePartition",      "UpdateTable"};

//! GetPartitions -> get_partitions
static string SnakeCase(const string &operation) {
	string result;
	for (auto c : operation) {
		if (c >= 'A' && c <= 'Z') {
			if (!result.empty()) {
				result += '_';
			}
			result += StringUtil::CharacterToLower(c);
		} else {
			result += c;
		}
	}
	return result;
}

void GlueRequestMetrics::Register(ExtensionLoader &loader) {
	loader.RegisterMetric({"glue.requests", "uint64", "Glue requests the query made, retries included", "requests"});
	loader.RegisterMetric(
	    {"glue.request_time", "double", "Wall time of the query's Glue requests, summed over requests", "seconds"});
	for (auto operation : GLUE_OPERATIONS) {
		auto name = SnakeCase(operation);
		loader.RegisterMetric({"glue." + name + ".requests", "uint64",
		                       StringUtil::Format("Glue %s requests the query made, retries included", operation),
		                       "requests"});
		loader.RegisterMetric({"glue." + name + ".request_time", "double",
		                       StringUtil::Format("Wall time of the query's Glue %s requests", operation), "seconds"});
	}
}

void GlueRequestMetrics::Record(ClientContext &context, const string &target, idx_t elapsed_ns) {
	if (!StringUtil::StartsWith(target, GLUE_TARGET_PREFIX)) {
		return;
	}
	auto &profiler = QueryProfiler::Get(context);
	if (!profiler.IsEnabled()) {
		return;
	}
	auto name = "glue." + SnakeCase(target.substr(strlen(GLUE_TARGET_PREFIX)));
	profiler.AddToMetricCounter("glue.requests", 1);
	profiler.AddToMetricTiming("glue.request_time", elapsed_ns);
	profiler.AddToMetricCounter(name + ".requests", 1);
	profiler.AddToMetricTiming(name + ".request_time", elapsed_ns);
}

} // namespace duckdb
