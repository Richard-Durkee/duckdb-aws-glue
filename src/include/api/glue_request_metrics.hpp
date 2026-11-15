#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

class ClientContext;
class ExtensionLoader;

//! Profiler metrics of a query's Glue requests, in total and per operation; every attempt counts, retries included
struct GlueRequestMetrics {
	static void Register(ExtensionLoader &loader);
	//! 'target' is the request's x-amz-target header, e.g. 'AWSGlue.GetPartitions'; other services are not recorded
	static void Record(ClientContext &context, const string &target, idx_t elapsed_ns);
};

} // namespace duckdb
