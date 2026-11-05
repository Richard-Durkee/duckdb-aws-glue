#include "core/readers/hive_json_options.hpp"

#include "core/serde_policy.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

HiveJSONOptions HiveJSONOptions::FromSerDeProperties(const SerDeProperties &properties) {
	HiveJSONOptions result;
	result.ignore_errors = properties.GetBool(SerDeOption::IGNORE_ERRORS);
	result.case_insensitive_keys = properties.GetBool(SerDeOption::CASE_INSENSITIVE_KEYS);
	return result;
}

void HiveJSONOptions::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "ignore_errors", ignore_errors);
	serializer.WriteProperty(101, "case_insensitive_keys", case_insensitive_keys);
}

HiveJSONOptions HiveJSONOptions::Deserialize(Deserializer &deserializer) {
	HiveJSONOptions result;
	result.ignore_errors = deserializer.ReadProperty<bool>(100, "ignore_errors");
	result.case_insensitive_keys = deserializer.ReadProperty<bool>(101, "case_insensitive_keys");
	return result;
}

} // namespace duckdb
