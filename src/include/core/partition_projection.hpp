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
	//! The most partitions a scan projects; more fail the query
	static constexpr idx_t MAX_PARTITIONS = 1000000;

	//! Whether the table has projection.enabled set to true
	static bool IsEnabled(const GlueTableInfo &table);
	//! The table's projection, or null when it does not project its partitions; throws for an invalid configuration
	static shared_ptr<const PartitionProjection> FromTable(const GlueTableInfo &table);

	const vector<ProjectedPartitionKey> &Keys() const {
		return keys;
	}
	//! The projected values of a key that is not injected
	vector<ProjectedValue> Values(ClientContext &context, idx_t key_index) const;
	//! A partition for every combination of one value of each key
	vector<GluePartitionInfo> Combine(const vector<vector<ProjectedValue>> &values) const;
	//! Throws for a scan that does not filter an injected key with = or IN
	[[noreturn]] void ThrowMissingInjectedFilter(idx_t key_index) const;

private:
	string Location(const vector<const ProjectedValue *> &values) const;

private:
	string table_name;
	vector<ProjectedPartitionKey> keys;
	//! storage.location.template, empty for <location>/<key>=<value>/...
	string location_template;
	string root_location;
};

} // namespace duckdb
