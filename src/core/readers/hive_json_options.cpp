#include "core/readers/hive_json_options.hpp"

#include "core/serde_policy.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

HiveJSONOptions HiveJSONOptions::FromSerDeProperties(const SerDeProperties &properties) {
	HiveJSONOptions result;
	result.ignore_errors = properties.GetBool(SerDeOption::IGNORE_ERRORS);
	return result;
}

void HiveJSONOptions::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "ignore_errors", ignore_errors);
}

HiveJSONOptions HiveJSONOptions::Deserialize(Deserializer &deserializer) {
	HiveJSONOptions result;
	result.ignore_errors = deserializer.ReadProperty<bool>(100, "ignore_errors");
	return result;
}

} // namespace duckdb
