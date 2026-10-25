#include "core/glue_log_types.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

constexpr LogLevel GlueRequestLogType::LEVEL;
constexpr LogLevel GlueScanLogType::LEVEL;

static Value OptionalCount(optional_idx value) {
	return value.IsValid() ? Value::UBIGINT(value.GetIndex()) : Value(LogicalType::UBIGINT);
}

static Value OptionalString(const string &value) {
	return value.empty() ? Value(LogicalType::VARCHAR) : Value(value);
}

GlueRequestLogType::GlueRequestLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

LogicalType GlueRequestLogType::GetLogType() {
	child_list_t<LogicalType> children = {{"operation", LogicalType::VARCHAR},
	                                      {"status", LogicalType::UBIGINT},
	                                      {"duration_ms", LogicalType::DOUBLE},
	                                      {"response_bytes", LogicalType::UBIGINT},
	                                      {"error", LogicalType::VARCHAR}};
	return LogicalType::STRUCT(children);
}

string GlueRequestLogType::ConstructLogMessage(const string &operation, optional_idx status, double duration_ms,
                                               idx_t response_bytes, const string &error) {
	child_list_t<Value> children = {{"operation", OptionalString(operation)},
	                                {"status", OptionalCount(status)},
	                                {"duration_ms", Value::DOUBLE(duration_ms)},
	                                {"response_bytes", Value::UBIGINT(response_bytes)},
	                                {"error", OptionalString(error)}};
	return Value::STRUCT(std::move(children)).ToString();
}

GlueScanLogType::GlueScanLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

LogicalType GlueScanLogType::GetLogType() {
	child_list_t<LogicalType> children = {
	    {"table", LogicalType::VARCHAR},      {"event", LogicalType::VARCHAR},
	    {"partitions", LogicalType::UBIGINT}, {"partitions_read", LogicalType::UBIGINT},
	    {"listings", LogicalType::UBIGINT},   {"files", LogicalType::UBIGINT},
	    {"duration_ms", LogicalType::DOUBLE}, {"detail", LogicalType::VARCHAR}};
	return LogicalType::STRUCT(children);
}

string GlueScanLogType::ConstructLogMessage(const GlueScanEvent &event) {
	child_list_t<Value> children = {
	    {"table", Value(event.table)},
	    {"event", Value(event.event)},
	    {"partitions", OptionalCount(event.partitions)},
	    {"partitions_read", OptionalCount(event.partitions_read)},
	    {"listings", OptionalCount(event.listings)},
	    {"files", OptionalCount(event.files)},
	    {"duration_ms", event.duration_ms < 0 ? Value(LogicalType::DOUBLE) : Value::DOUBLE(event.duration_ms)},
	    {"detail", OptionalString(event.detail)}};
	return Value::STRUCT(std::move(children)).ToString();
}

void RegisterGlueLogTypes(DatabaseInstance &db) {
	auto &log_manager = db.GetLogManager();
	log_manager.RegisterLogType(make_uniq<GlueRequestLogType>());
	log_manager.RegisterLogType(make_uniq<GlueScanLogType>());
}

} // namespace duckdb
