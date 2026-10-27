#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/logging/log_type.hpp"

#include <chrono>

namespace duckdb {
class DatabaseInstance;
struct GlueTraceRequest;

//! One HTTP request to the Glue (or STS) endpoint, logged by the HTTP bridge: with glue_network_calls_via_duckdb off
//! the SDK's own transport sends the requests and nothing is logged
class GlueRequestLogType : public LogType {
public:
	static constexpr const char *NAME = "GlueRequest";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	GlueRequestLogType();

	static LogicalType GetLogType();
	//! 'operation' is the X-Amz-Target header (AWSGlue.GetTable, ...); 'status' is invalid for a request that did not
	//! reach the service. 'phase' is set while glue_trace is on.
	static string ConstructLogMessage(const GlueTraceRequest &request);
};

//! What a scan of a Glue Hive table spends its time on, one entry per 'event': 'partitions' (loading them),
//! 'plan' (the listings to do), 'listing' (one of them) and 'sample' (the cardinality sample). Fields an event does
//! not set are NULL.
struct GlueScanEvent {
	GlueScanEvent(string table_p, string event_p) : table(std::move(table_p)), event(std::move(event_p)) {
	}

	string table;
	string event;
	optional_idx partitions;
	optional_idx partitions_read;
	optional_idx listings;
	optional_idx files;
	double duration_ms = -1;
	string detail;
};

class GlueScanLogType : public LogType {
public:
	static constexpr const char *NAME = "GlueScan";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	GlueScanLogType();

	static LogicalType GetLogType();
	static string ConstructLogMessage(const GlueScanEvent &event);
};

void RegisterGlueLogTypes(DatabaseInstance &db);

class GlueLogTimer {
public:
	GlueLogTimer() : start(std::chrono::steady_clock::now()) {
	}
	double ElapsedMillis() const {
		return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
	}

private:
	std::chrono::steady_clock::time_point start;
};

} // namespace duckdb
