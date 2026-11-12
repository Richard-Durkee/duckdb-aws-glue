#pragma once

#include "core/glue_info.hpp"

#include "duckdb/common/types/interval.hpp"

namespace duckdb {

class ClientContext;

enum class ProjectionType : uint8_t { ENUM, INTEGER, DATE, INJECTED };

//! A projected value of a partition key: the value the partition holds and how it is written in the location
struct ProjectedValue {
	string value;
	string path;
};

//! How the values of one partition key are projected from the projection.<key>.* table properties
struct ProjectedPartitionKey {
	string name;
	ProjectionType type = ProjectionType::ENUM;
	//! ENUM
	vector<string> values;
	//! INTEGER: the inclusive range, the step and the zero-padded width (0 for no padding)
	int64_t min = 0;
	int64_t max = 0;
	int64_t step = 1;
	idx_t digits = 0;
	//! DATE: the range bounds as the table writes them, the format as a strftime format, the step, and the type of the
	//! column (a DATE or TIMESTAMP key holds the value as such, a string key the formatted value)
	string range_start;
	string range_end;
	string format;
	interval_t interval {0, 0, 0};
	LogicalTypeId value_type = LogicalTypeId::VARCHAR;
};

//! The partitions of a Glue table that projects them from its properties (projection.enabled) instead of registering
//! them in Glue: every combination of the projected values of its partition keys
class PartitionProjection {
public:
	//! The table's projection, or null when it does not project its partitions; throws for an invalid configuration
	static shared_ptr<const PartitionProjection> FromTable(const GlueTableInfo &table);

	const vector<ProjectedPartitionKey> &Keys() const {
		return keys;
	}
	//! The number of values of an integer key, saturated at the largest idx_t
	idx_t IntegerCount(idx_t key_index) const;
	//! The projected values of a key that is not injected; an integer key takes only those in [lo, hi]
	vector<ProjectedValue> Values(ClientContext &context, idx_t key_index,
	                              int64_t lo = NumericLimits<int64_t>::Minimum(),
	                              int64_t hi = NumericLimits<int64_t>::Maximum()) const;
	//! A partition for every combination of one value of each key
	vector<GluePartitionInfo> Combine(ClientContext &context, const vector<vector<ProjectedValue>> &values) const;
	//! Throws for a scan that does not filter an injected key with = or IN
	[[noreturn]] void ThrowMissingInjectedFilter(idx_t key_index) const;
	//! The location of the partition whose keys are written as 'paths'
	string Location(const vector<const string *> &paths) const;
	//! The directory every partition a write derives is below: the table location, or the template's text before its
	//! first placeholder; throws for a template whose placeholder is not inside a directory
	string WriteRoot() const;
	static constexpr idx_t DEFAULT_MAX_PARTITIONS = 100000;
	static idx_t MaxPartitions(ClientContext &context);
	const string &TableName() const {
		return table_name;
	}

private:
	string table_name;
	vector<ProjectedPartitionKey> keys;
	//! storage.location.template, empty for <location>/<key>=<value>/...
	string location_template;
	string root_location;
};

//! Where a write puts a row of a projected table: the partition the scan derives for the row's values. Nothing is
//! registered in Glue, and a value the projection does not derive fails the write.
class ProjectedPartitionWriter {
public:
	ProjectedPartitionWriter(ClientContext &context, shared_ptr<const PartitionProjection> projection,
	                         const vector<LogicalType> &types);

	string Location(const vector<Value> &values) const;

private:
	shared_ptr<const PartitionProjection> projection;
	//! Per key, the path of each projected value by its text in the column's type; empty for an injected key
	vector<unordered_map<string, string>> paths;
};

} // namespace duckdb
